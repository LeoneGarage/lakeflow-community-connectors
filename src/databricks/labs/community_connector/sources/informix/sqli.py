"""Bounded pure-Python Informix SQLI socket and message primitives.

The normal ASC username/password prefix, six-byte session header, socket/TLS
handling, response grouping, simple command request, and SQ_LODATA dispatcher
are bytecode-backed. Generic ASC tail, DESCRIBE/tuple and bind layouts remain
fail-closed where the recovered evidence is incomplete.
"""

from __future__ import annotations

import io
import ipaddress
import os
import re
import socket
import ssl
import struct
import threading
import time
from dataclasses import dataclass, field
from datetime import date, timedelta
from enum import Enum, auto
from typing import BinaryIO, Protocol

from databricks.labs.community_connector.sources.informix.cdc_protocol import (
    ColumnDescriptor,
    decode_packed_decimal,
    decode_value,
)

SQ_COMMAND = 1
SQ_PREPARE = 2
SQ_CURNAME = 3
SQ_ID = 4
SQ_OPEN = 6
SQ_BIND = 5
SQ_EXECUTE = 7
SQ_NFETCH = 9
SQ_CLOSE = 10
SQ_RELEASE = 11
SQ_EOT = 12
SQ_ERR = 13
SQ_DONE = 15
SQ_NDESCRIBE = 22
SQ_WANTDONE = 49
SQ_DBOPEN = 36
SQ_INFO = 81
SQ_PROTOCOLS = 126
SQ_EXIT = 56
SQ_LODATA = 97
SQ_RET_TYPE = 100
SQ_ASSOC = 100
SQ_ASCBINARY = 101
SQ_ASCENV = 106
SQ_ASCPINFO = 107
SQ_ASCMISC_60 = 116
SQ_ASCEOT = 127
LO_READ = 0
LO_READ_WITH_SEEK = 1
LO_WRITE = 2
TRANSFER_BUFFER_SIZE = 32000
SQLI_PROTOCOL = 60
INTERNAL_VERSION = 316
PROTOCOL_OFFER = bytes.fromhex("ff fc 7f fc 3c 8c aa 97 06")
MAX_PACKET = 16 * 1024 * 1024


def _protocol_feature(bits: bytes, feature: int) -> bool:
    index, offset = divmod(feature, 8)
    return index < len(bits) and bool(bits[index] & (1 << (7 - offset)))


class SqliProtocolError(RuntimeError):
    pass


class SqliUnsupportedAuthentication(SqliProtocolError):
    pass


class SqliRedirect(SqliUnsupportedAuthentication):
    def __init__(self, detail: str) -> None:
        super().__init__("Informix requested a connection redirect")
        self.detail = detail


class SqliDescriptorNotImplemented(SqliProtocolError):
    pass


# Backward-compatible precise name used by callers from the first pure port.
SqliAuthenticationNotImplemented = SqliUnsupportedAuthentication


class ConnectionState(Enum):
    NEW = auto()
    SOCKET_OPEN = auto()
    ASC_SENT = auto()
    ACCEPTED = auto()
    AUTHENTICATED = auto()
    DATABASE_OPEN = auto()
    POISONED = auto()
    CLOSED = auto()


@dataclass(frozen=True)
class AscAccept:
    version: str
    cap_1: int
    cap_2: int
    cap_3: int
    warnings: int


@dataclass(frozen=True)
class ResultColumn:
    name: str
    position: int
    type_code: int
    extended_id: int
    encoded_length: int


@dataclass(frozen=True)
class ResultDescription:
    statement_type: int
    statement_id: int
    tuple_size: int
    columns: tuple[ResultColumn, ...]


class CdcTransport(Protocol):
    def execute(self, sql: str, parameters: tuple = ()) -> list[tuple]: ...

    def read_lodata(self, descriptor: int, requested: int) -> bytes: ...

    def close(self) -> None: ...


class AuthenticationProvider(Protocol):
    """Noninteractive, secret-backed PAM response provider."""

    def respond(self, style: int, challenge: str) -> str | bytes: ...


@dataclass(frozen=True)
class PasswordAuthenticationProvider:
    password: str
    echo_response: str | None = None

    def respond(self, style: int, challenge: str) -> str:
        del challenge
        if style == 1:
            return self.password
        if style == 2 and self.echo_response is not None:
            return self.echo_response
        raise SqliUnsupportedAuthentication(f"no noninteractive PAM response for style {style}")


@dataclass(frozen=True)
class TypedBind:
    native_type: int
    value: int | str | None


def encode_bind(values: tuple[TypedBind, ...], encoding: str = "utf-8") -> bytes:
    if len(values) > 1024:
        raise ValueError("too many SQLI bind values")
    out = io.BytesIO()
    out.write(struct.pack(">hh", SQ_BIND, len(values)))
    for bind in values:
        out.write(struct.pack(">h", bind.native_type))
        if bind.native_type > 18 and bind.native_type not in {52, 53}:
            raise ValueError("extended bind types require owner/type metadata")
        if bind.value is None:
            out.write(struct.pack(">hh", -1, 0))
            continue
        if bind.native_type == 2:
            raw = struct.pack(">i", int(bind.value))
        elif bind.native_type in {52, 53}:
            raw = struct.pack(">q", int(bind.value))
        elif bind.native_type == 0:
            raw = str(bind.value).encode(encoding)
        elif bind.native_type == 13:
            text = str(bind.value).encode(encoding)
            if len(text) > 255:
                raise ValueError("VARCHAR bind exceeds one-byte encoded length")
            raw = bytes([len(text)]) + text
        else:
            raise ValueError(f"unsupported internal bind type {bind.native_type}")
        if len(raw) > 32767:
            raise ValueError("bind value exceeds signed-smallint length")
        out.write(struct.pack(">hh", 0, len(raw)))
        out.write(raw)
        if len(raw) & 1:
            out.write(b"\0")
    return out.getvalue()


def decode_pam_challenge(payload: bytes, encoding: str = "utf-8") -> tuple[int, str]:
    stream = io.BytesIO(payload)
    message_type, length = read_smallint(stream), read_smallint(stream)
    if length < 0 or length > 512:
        raise SqliUnsupportedAuthentication("PAM challenge exceeds noninteractive bound")
    raw = read_exact(stream, length)
    if length & 1:
        read_exact(stream, 1)
    if stream.read(1):
        raise SqliProtocolError("PAM challenge has trailing bytes")
    return message_type, raw.decode(encoding)


def encode_pam_response(response: str, encoding: str = "utf-8") -> bytes:
    raw = response.encode(encoding)
    if len(raw) > 512:
        raise SqliUnsupportedAuthentication("PAM response exceeds 512-byte bound")
    return (
        struct.pack(">hh", 130, len(raw))
        + raw
        + (b"\0" if len(raw) & 1 else b"")
        + struct.pack(">h", SQ_EOT)
    )


def parse_redirect_detail(detail: str, allowlist: set[tuple[str, int]]) -> tuple[str, str, int]:
    tokens = re.split(r"[:=|]", detail)
    if (len(tokens) not in {4, 5} or not tokens[-1].isdigit() or any(not token for token in tokens)
            or any(ord(char) < 0x20 or ord(char) == 0x7f for char in detail)):
        raise SqliUnsupportedAuthentication("redirect detail has unsafe grammar")
    server, host, port = tokens[-3], tokens[-2], int(tokens[-1])
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", server) or not re.fullmatch(
        r"[A-Za-z0-9.-]+", host
    ):
        raise SqliUnsupportedAuthentication("redirect detail has unsafe identifiers")
    if not server or not host or not 1 <= port <= 65535 or (host, port) not in allowlist:
        raise SqliUnsupportedAuthentication("redirect target is not explicitly allow-listed")
    return server, host, port


def materialize_blob_chunks(
    chunks: list[bytes], per_value_limit: int, remaining_batch_bytes: int
) -> bytes:
    output = bytearray()
    limit = min(per_value_limit, remaining_batch_bytes)
    for payload in chunks:
        if len(payload) < 2:
            raise SqliProtocolError("truncated TEXT/BYTE chunk")
        size = struct.unpack_from(">h", payload)[0]
        if size < 0 or len(payload) != 2 + size + (size & 1):
            raise SqliProtocolError("invalid TEXT/BYTE chunk length or padding")
        if len(output) + size > limit:
            raise SqliProtocolError("TEXT/BYTE materialization exceeds configured bound")
        output.extend(payload[2 : 2 + size])
    return bytes(output)


def encode_char(value: str | bytes, encoding: str = "utf-8") -> bytes:
    raw = value.encode(encoding) if isinstance(value, str) else bytes(value)
    if len(raw) > 0x7FFF:
        raise ValueError("SQLI CHAR exceeds signed-smallint length")
    return struct.pack(">h", len(raw)) + raw + (b"\0" if len(raw) & 1 else b"")


def encode_asc_char(value: str | bytes, encoding: str = "utf-8") -> bytes:
    raw = value.encode(encoding) if isinstance(value, str) else bytes(value)
    if not raw.endswith(b"\0"):
        raw += b"\0"
    if len(raw) > 0x7FFF:
        raise ValueError("ASC string exceeds signed-smallint length")
    return struct.pack(">H", len(raw)) + raw


def decode_char(stream: BinaryIO, encoding: str = "utf-8", maximum: int = 1 << 20) -> str:
    length = read_smallint(stream)
    if length < 0 or length > maximum:
        raise SqliProtocolError(f"invalid SQLI CHAR length {length}")
    raw = read_exact(stream, length)
    if length & 1:
        read_exact(stream, 1)
    return raw.decode(encoding)


def encode_session_packet(sl_type: int, payload: bytes, protocol: int = SQLI_PROTOCOL) -> bytes:
    total = len(payload) + 6
    if total > 0xFFFF:
        raise ValueError("SQLI session-layer packet exceeds uint16 length")
    if not 0 <= sl_type <= 0xFF or not 0 <= protocol <= 0xFF:
        raise ValueError("session type/protocol must fit uint8")
    return struct.pack(">HBBH", total, sl_type, protocol, 0) + payload


def read_session_packet(stream: BinaryIO, maximum: int = 0xFFFF) -> tuple[int, int, bytes]:
    total, sl_type, protocol, flags = struct.unpack(">HBBH", read_exact(stream, 6))
    if total < 6 or total > maximum:
        raise SqliProtocolError(f"invalid session-layer length {total}")
    if flags not in {0, 0x1000}:
        raise SqliProtocolError(f"unsupported session-layer flags 0x{flags:04x}")
    return sl_type, protocol, read_exact(stream, total - 6)


def decode_asc_response(packet: bytes) -> bytes:
    """Validate accept/reject/redirect session state and ASC binary markers.

    Returns the undecoded capability body for an accepted connection. Its full
    field layout is intentionally left to the forthcoming DecodeAscBinary port.
    """

    sl_type, protocol, payload = read_session_packet(io.BytesIO(packet))
    if protocol != SQLI_PROTOCOL:
        raise SqliProtocolError(f"server selected unsupported SQLI protocol {protocol}")
    if sl_type == 13:
        raise SqliRedirect("unparsed redirect")
    if sl_type == 3:
        raise SqliUnsupportedAuthentication(
            "Informix rejected normal username/password authentication"
        )
    if sl_type != 2:
        raise SqliProtocolError(f"unexpected ASC session-layer type {sl_type}")
    if len(payload) < 4:
        raise SqliProtocolError("truncated ASC accepted body")
    assoc, binary = struct.unpack_from(">hh", payload)
    if (assoc, binary) != (SQ_ASSOC, SQ_ASCBINARY):
        raise SqliProtocolError(f"invalid ASC accepted markers {(assoc, binary)}")
    return payload[4:]


def decode_asc_accept(packet: bytes, encoding: str = "utf-8") -> AscAccept:
    sl_type, protocol, payload = read_session_packet(io.BytesIO(packet))
    if protocol != SQLI_PROTOCOL:
        raise SqliProtocolError(f"server selected unsupported SQLI protocol {protocol}")
    if sl_type == 3:
        raise SqliUnsupportedAuthentication("Informix rejected authentication")
    if sl_type not in {2, 13} or len(payload) < 4:
        raise SqliProtocolError(f"unexpected ASC session-layer type {sl_type}")
    if struct.unpack_from(">hh", payload) != (SQ_ASSOC, SQ_ASCBINARY):
        raise SqliProtocolError("invalid ASC accepted markers")
    body = memoryview(payload[4:])
    cursor = _BufferCursor(body)
    cursor.skip(4)
    cursor.skip(cursor.smallint_length())
    if cursor.smallint() != 108:
        raise SqliProtocolError("ASC response is missing marker 108")
    cursor.skip(12)
    version_text = cursor.bytes(cursor.smallint_length()).decode("ascii", "strict").rstrip("\0")
    cursor.skip(cursor.smallint_length())
    cursor.skip(cursor.smallint_length())
    cap_1, cap_2, cap_3 = cursor.int32(), cursor.int32(), cursor.int32()
    cursor.skip(2)
    cursor.skip(cursor.smallint_length())
    cursor.skip(cursor.smallint_length())
    cursor.skip(24)
    result_type = cursor.smallint()
    warnings = 0
    if result_type == 102:
        cursor.skip(6)
        service_error, os_error, warnings = cursor.smallint(), cursor.smallint(), cursor.smallint()
        if service_error:
            raise SqliUnsupportedAuthentication(
                f"Informix authentication failed ({service_error}/{os_error})"
            )
        # Live IDS appends bounded ASC server metadata blocks before ASCEOT.
        # Their contents are informational; type/protocol/service status above
        # controls acceptance. Require the exact final marker and consume all.
        trailing = cursor.bytes(cursor.remaining)
        if len(trailing) < 2 or trailing[-2:] != struct.pack(">h", SQ_ASCEOT):
            raise SqliProtocolError("ASC INITRESP metadata is missing final ASCEOT")
    elif result_type == 103:
        cursor.smallint()
        detail = cursor.char(encoding)
        if cursor.remaining:
            raise SqliProtocolError("ASC redirect has trailing bytes")
        raise SqliRedirect(detail)
    elif result_type != 127:
        raise SqliProtocolError(f"unknown ASC result type {result_type}")
    if cursor.remaining:
        raise SqliProtocolError(f"ASC response has {cursor.remaining} trailing bytes")
    if sl_type == 13:
        raise SqliProtocolError("redirect packet did not contain SQ_ASCDBLIST")
    return AscAccept(version_text, cap_1, cap_2, cap_3, warnings)


class _BufferCursor:
    def __init__(self, data: memoryview):
        self.data, self.offset = data, 0

    @property
    def remaining(self) -> int:
        return len(self.data) - self.offset

    def bytes(self, size: int) -> bytes:
        if size < 0 or size > self.remaining:
            raise SqliProtocolError(f"bounded field length {size} exceeds {self.remaining}")
        result = bytes(self.data[self.offset : self.offset + size])
        self.offset += size
        return result

    def skip(self, size: int) -> None:
        self.bytes(size)

    def smallint(self) -> int:
        return struct.unpack(">h", self.bytes(2))[0]

    def smallint_length(self) -> int:
        value = self.smallint()
        if value < 0:
            raise SqliProtocolError(f"negative nested length {value}")
        return value

    def int32(self) -> int:
        return struct.unpack(">i", self.bytes(4))[0]

    def char(self, encoding: str) -> str:
        size = self.smallint_length()
        raw = self.bytes(size)
        return raw.rstrip(b"\0").decode(encoding)


def encode_normal_auth_prefix(
    username: str,
    password: str,
    server_name: str,
    database: str,
    encoding: str = "utf-8",
) -> bytes:
    """Encode the fully recovered fixed ASC normal-password portion.

    The returned bytes stop immediately before SQ_ASCENV because the exact
    ASCPINFO/ASCMISC tail remains unrecovered. Password bytes are plaintext;
    callers must place this packet inside TLS.
    """

    out = io.BytesIO()
    out.write(struct.pack(">hhi", SQ_ASSOC, SQ_ASCBINARY, 61))
    out.write(encode_asc_char(b"IEEEM\0"))
    out.write(struct.pack(">h", 108))
    out.write(b"sqlexec\0\0\0\0\0")
    out.write(encode_asc_char(b"9.280\0"))
    out.write(encode_asc_char(b"RDS#R000000\0"))
    out.write(encode_asc_char(b"sqli\0"))
    out.write(struct.pack(">iii", INTERNAL_VERSION, 0, 0))
    out.write(struct.pack(">h", 1))
    out.write(encode_asc_char(username, encoding))
    out.write(encode_asc_char(password, encoding))
    out.write(b"ol\0\0\0\0\0\0")
    out.write(struct.pack(">i", 61))
    out.write(b"tlitcp\0\0")
    out.write(struct.pack(">ihhi", 1, 104, 11, 3))
    out.write(encode_asc_char(server_name, encoding))
    out.write(struct.pack(">h", 0))
    out.write(struct.pack(">hhhh", 0, 0, 0, 0))
    return out.getvalue()


def encode_asc_environment(properties: dict[str, str], encoding: str = "utf-8") -> bytes:
    if len(properties) > 64:
        raise ValueError("too many ASC environment entries")
    out = io.BytesIO()
    out.write(struct.pack(">hh", SQ_ASCENV, len(properties)))
    for key, value in sorted(properties.items()):
        for item in (key, value):
            raw = item.encode(encoding)
            if not raw or len(raw) > 1023 or b"\0" in raw:
                raise ValueError("invalid ASC environment key/value")
            out.write(struct.pack(">h", len(raw) + 1))
            out.write(raw + b"\0")
    return out.getvalue()


def encode_asc_tail(
    properties: dict[str, str],
    hostname: str = "python-client",
    process_id: int = 0,
    thread_id: int = 0,
    cwd: str = "",
    diagnostic: str = "Thread[id:0, name:lakeflow, path:python]",
    encoding: str = "utf-8",
) -> bytes:
    out = io.BytesIO()
    out.write(encode_asc_environment(properties, encoding))
    out.write(struct.pack(">hiii", SQ_ASCPINFO, 0, process_id, thread_id))
    out.write(encode_asc_char(hostname, encoding))
    out.write(struct.pack(">h", 0))
    out.write(encode_asc_char(cwd, encoding))
    raw = diagnostic.encode("ascii")
    out.write(struct.pack(">hhiih", SQ_ASCMISC_60, 10 + len(raw) + 1, 0, 0, len(raw) + 1))
    out.write(raw + b"\0")
    out.write(struct.pack(">h", SQ_ASCEOT))
    return out.getvalue()


def encode_normal_auth_request(
    username: str,
    password: str,
    server_name: str,
    database: str,
    properties: dict[str, str],
    encoding: str = "utf-8",
) -> bytes:
    prefix = encode_normal_auth_prefix(username, password, server_name, database, encoding)
    tail = encode_asc_tail(
        properties,
        hostname=socket.gethostname(),
        process_id=os.getpid(),
        thread_id=threading.get_ident() & 0x7FFFFFFF,
        cwd=os.getcwd(),
        encoding=encoding,
    )
    return encode_session_packet(1, prefix + tail)


def _encode_sql_text(sql: str, encoding: str, long_length: bool) -> bytes:
    raw = sql.encode(encoding)
    maximum = 1 << 20 if long_length else 0x7FFF
    if not raw or len(raw) > maximum or b"\0" in raw:
        raise ValueError("invalid bounded SQL text")
    prefix = struct.pack(">i", len(raw)) if long_length else struct.pack(">h", len(raw))
    return prefix + raw + (b"\0" if len(raw) & 1 else b"")


def encode_simple_command(
    sql: str, encoding: str = "utf-8", long_sql_length: bool = False
) -> bytes:
    if "\0" in sql:
        raise ValueError("SQL command contains NUL")
    return (
        struct.pack(">hh", SQ_COMMAND, 0)
        + _encode_sql_text(sql, encoding, long_sql_length)
        + struct.pack(">hhhh", SQ_NDESCRIBE, SQ_EXECUTE, SQ_RELEASE, SQ_EOT)
    )


def encode_prepare(
    sql: str,
    encoding: str = "utf-8",
    parameter_count: int = 0,
    long_sql_length: bool = False,
) -> bytes:
    if not 0 <= parameter_count <= 1024:
        raise ValueError("parameter_count must be in [0, 1024]")
    return (
        struct.pack(">hh", SQ_PREPARE, parameter_count)
        + _encode_sql_text(sql, encoding, long_sql_length)
        + struct.pack(">hhh", SQ_NDESCRIBE, SQ_WANTDONE, SQ_EOT)
    )


def encode_cursor_open(
    statement_id: int, cursor_name: str, encoding: str = "utf-8", binds: bytes = b""
) -> bytes:
    return (
        struct.pack(">hhh", SQ_ID, statement_id, SQ_CURNAME)
        + encode_char(cursor_name, encoding)
        + binds
        + struct.pack(">hh", SQ_OPEN, SQ_EOT)
    )


def encode_fetch(statement_id: int, buffer_size: int = 32767) -> bytes:
    if not 1 <= buffer_size <= 32767:
        raise ValueError("fetch buffer_size must be in [1, 32767]")
    return struct.pack(">hhhihh", SQ_ID, statement_id, SQ_NFETCH, buffer_size, 0, SQ_EOT)


def encode_variable_fetch(description: ResultDescription, buffer_size: int = 32767) -> bytes:
    """Encode JDBC's two-phase variable-row RET_TYPE + NFETCH request."""

    if not 1 <= buffer_size <= 32767:
        raise ValueError("fetch buffer_size must be in [1, 32767]")
    result = bytearray(
        struct.pack(
            ">hhhhh", SQ_ID, description.statement_id, SQ_RET_TYPE, 1, len(description.columns)
        )
    )
    for column in description.columns:
        kind = column.type_code & 0xFF
        if kind > 18 and kind not in {23, 52, 53}:
            raise SqliDescriptorNotImplemented(
                f"variable result type {kind} requires an extended SQ_RET_TYPE name"
            )
        result.extend(struct.pack(">hi", kind, column.encoded_length))
    result.extend(struct.pack(">hihh", SQ_NFETCH, buffer_size, 0, SQ_EOT))
    return bytes(result)


def encode_fixed_open_fetch(
    statement_id: int,
    cursor_name: str,
    binds: bytes = b"",
    encoding: str = "utf-8",
    buffer_size: int = 4096,
) -> bytes:
    if not 1 <= buffer_size <= 32767:
        raise ValueError("fetch buffer_size must be in [1, 32767]")
    return (
        struct.pack(">hhh", SQ_ID, statement_id, SQ_CURNAME)
        + encode_char(cursor_name, encoding)
        + binds
        + struct.pack(">hhhhihh", SQ_OPEN, SQ_ID, statement_id, SQ_NFETCH, buffer_size, 0, SQ_EOT)
    )


def encode_close_release(statement_id: int, release: bool = False) -> bytes:
    operation = SQ_RELEASE if release else SQ_CLOSE
    return struct.pack(">hhhh", SQ_ID, statement_id, operation, SQ_EOT)


def encode_protocol_offer() -> bytes:
    return (
        struct.pack(">hh", SQ_PROTOCOLS, len(PROTOCOL_OFFER))
        + PROTOCOL_OFFER
        + b"\0"
        + struct.pack(">h", SQ_EOT)
    )


def encode_secondary_info(properties: dict[str, str], encoding: str = "utf-8") -> bytes:
    encoded = [
        (encode_char(k, encoding), encode_char(v, encoding)) for k, v in sorted(properties.items())
    ]
    max_key = max((len(item[0]) - 2 for item in encoded), default=0)
    max_value = max((len(item[1]) - 2 for item in encoded), default=0)
    total = 6 + sum(4 + len(key) - 2 + len(value) - 2 for key, value in encoded)
    if total > 0x7FFF:
        raise ValueError("secondary environment exceeds signed-smallint size")
    return (
        struct.pack(">hhhhh", SQ_INFO, 6, total, max_key, max_value)
        + b"".join(key + value for key, value in encoded)
        + struct.pack(">hhh", 0, 0, SQ_EOT)
    )


def encode_dbopen(database: str, encoding: str = "utf-8") -> bytes:
    return (
        struct.pack(">h", SQ_DBOPEN)
        + encode_char(database, encoding)
        + struct.pack(">hh", 0, SQ_EOT)
    )


def encode_lodata_read(descriptor: int, requested: int) -> bytes:
    if descriptor < -1 or descriptor > 0x7FFFFFFF:
        raise ValueError("SmartLOB descriptor must be >= -1 and fit signed int32")
    if requested < 1 or requested > 0x7FFFFFFF:
        raise ValueError("requested must be in [1, 2^31-1]")
    wire_descriptor = ((descriptor + 32768) % 65536) - 32768
    return struct.pack(
        ">hhhih", SQ_LODATA, LO_READ, wire_descriptor, requested, TRANSFER_BUFFER_SIZE
    )


def decode_lodata_response(payload: bytes, target: BinaryIO | None = None) -> bytes:
    if len(payload) < 6:
        raise SqliProtocolError("truncated SQ_LODATA response")
    operation, size = struct.unpack_from(">hi", payload)
    if operation not in {LO_READ, LO_READ_WITH_SEEK}:
        raise SqliProtocolError(f"unexpected SQ_LODATA response operation {operation}")
    if size < 0:
        raise SqliProtocolError(f"SQ_LODATA server/ISAM error {size}")
    if size == 0:
        if payload[6:] != b"\0\0":
            raise SqliProtocolError("zero-length SQ_LODATA response lacks its terminator")
        return b""
    cursor = 6
    data = bytearray()
    while len(data) < size:
        if cursor + 2 > len(payload):
            raise SqliProtocolError("truncated SQ_LODATA chunk length")
        chunk_size = struct.unpack_from(">h", payload, cursor)[0]
        cursor += 2
        if chunk_size <= 0 or len(data) + chunk_size > size:
            raise SqliProtocolError(f"invalid SQ_LODATA chunk size {chunk_size}")
        padded = chunk_size + (chunk_size & 1)
        if cursor + padded > len(payload):
            raise SqliProtocolError("truncated SQ_LODATA chunk")
        data.extend(payload[cursor : cursor + chunk_size])
        cursor += padded
    if cursor != len(payload):
        raise SqliProtocolError("SQ_LODATA response has trailing bytes")
    decoded = bytes(data)
    if target is not None:
        target.write(decoded)
    return decoded


def read_exact(stream: BinaryIO, size: int) -> bytes:
    if size < 0 or size > MAX_PACKET:
        raise SqliProtocolError(f"unsafe exact-read size {size}")
    chunks = bytearray()
    while len(chunks) < size:
        chunk = stream.read(size - len(chunks))
        if not chunk:
            raise SqliProtocolError(f"truncated SQLI stream: needed {size}, got {len(chunks)}")
        chunks.extend(chunk)
    return bytes(chunks)


def read_smallint(stream: BinaryIO) -> int:
    return struct.unpack(">h", read_exact(stream, 2))[0]


def read_int32(stream: BinaryIO) -> int:
    return struct.unpack(">i", read_exact(stream, 4))[0]


def _connector_sql(sql: str, parameters: tuple) -> str:
    """Bind only connector-owned scalar values using strict SQL literals."""

    if sql.count("?") != len(parameters):
        raise ValueError("SQL placeholder count does not match parameters")
    pieces = sql.split("?")
    result = [pieces[0]]
    for value, suffix in zip(parameters, pieces[1:]):
        if isinstance(value, bool):
            literal = "1" if value else "0"
        elif isinstance(value, int):
            literal = str(value)
        elif isinstance(value, str):
            if "\0" in value or len(value.encode("utf-8")) > 32767:
                raise ValueError("unsafe connector SQL string literal")
            literal = "'" + value.replace("'", "''") + "'"
        else:
            raise TypeError(f"unsupported connector SQL value {type(value).__name__}")
        result.extend((literal, suffix))
    return "".join(result)


def _typed_bind(value: object) -> TypedBind:
    if isinstance(value, TypedBind):
        return value
    if isinstance(value, bool):
        return TypedBind(2, int(value))
    if isinstance(value, int):
        return TypedBind(2 if -(1 << 31) <= value < (1 << 31) else 52, value)
    if isinstance(value, str):
        return TypedBind(13, value)
    if value is None:
        raise TypeError("SQL NULL requires an internal TypedBind with an explicit native type")
    raise TypeError(f"unsupported internal bind value {type(value).__name__}")


def _decode_result_value(
    data: bytes, column: ResultColumn, encoding: str, pad_varchar: bool = False
) -> object:
    kind = column.type_code & 0xFF
    if kind == 0:  # CHAR
        return data[: column.encoded_length].decode(encoding).rstrip()
    if kind == 1:
        value = struct.unpack(">h", data[:2])[0]
        return None if value == -(1 << 15) else value
    if kind in {2, 6}:
        value = struct.unpack(">i", data[:4])[0]
        return None if value == -(1 << 31) else value
    if kind == 3:
        if data[:8] == b"\xff" * 8:
            return None
        return struct.unpack(">d", data[:8])[0]
    if kind == 4:
        if data[:4] == b"\xff" * 4:
            return None
        return struct.unpack(">f", data[:4])[0]
    if kind in {5, 8}:
        precision, scale = (column.encoded_length >> 8) & 0xFF, column.encoded_length & 0xFF
        expected = (precision + (scale & 1) + 3) // 2
        return decode_packed_decimal(data[:expected], precision, scale)
    if kind == 7:
        value = struct.unpack(">i", data[:4])[0]
        return None if value == -(1 << 31) else date(1899, 12, 31) + timedelta(days=value)
    if kind in {13, 16}:
        if pad_varchar:
            return data[: column.encoded_length].decode(encoding).rstrip(" \0")
        if not data:
            raise SqliProtocolError("truncated variable VARCHAR")
        size = data[0]
        if size + 1 > len(data):
            raise SqliProtocolError("VARCHAR length exceeds tuple column")
        return data[1 : size + 1].decode(encoding)
    if kind in {17, 18}:
        value = struct.unpack(">q", data[:8])[0]
        return None if value == -(1 << 63) else value
    if kind == 10:
        # Unlike DECIMAL, DATETIME's start/end qualifier is carried in the
        # descriptor's extended-id field.  encoded_length describes its packed
        # storage and must not be truncated into a synthetic qualifier.
        value, _ = decode_value(
            memoryview(data),
            ColumnDescriptor("datetime", "DATETIME", length=column.extended_id),
        )
        return value
    if kind == 23:
        if not data:
            raise SqliProtocolError("truncated BOOLEAN result")
        return bool(data[0])
    raise SqliDescriptorNotImplemented(f"ordinary Informix result type {kind} is unsupported")


def _fixed_result_size(column: ResultColumn, remaining: bytes) -> int:
    kind = column.type_code & 0xFF
    widths = {1: 2, 2: 4, 3: 8, 4: 4, 6: 4, 7: 4, 17: 8, 18: 8, 23: 1}
    if kind == 0:
        return column.encoded_length
    if kind in widths:
        return widths[kind]
    if kind in {5, 8}:
        precision, scale = (column.encoded_length >> 8) & 0xFF, column.encoded_length & 0xFF
        return (precision + (scale & 1) + 3) // 2
    if kind == 10:
        total, fraction = (column.encoded_length >> 8) & 0xFF, column.encoded_length & 0xFF
        return (total + (fraction & 1) + 3) // 2
    raise SqliDescriptorNotImplemented(f"ordinary Informix result type {kind} is unsupported")


@dataclass
class InformixSqliClient:
    hostname: str
    port: int
    database: str
    user: str
    password: str
    server_name: str | None = None
    db_locale: str | None = None
    client_locale: str | None = None
    tls: bool = True
    ssl_context: ssl.SSLContext | None = None
    ca_file: str | None = None
    pad_varchar: bool = False
    connect_timeout: float = 10.0
    socket_timeout: float = 30.0
    authentication_mode: str = "password"
    authentication_provider: AuthenticationProvider | None = None
    pam_max_rounds: int = 16
    login_timeout: float = 30.0
    redirect_enabled: bool = False
    redirect_allowlist: frozenset[tuple[str, int]] = frozenset()
    redirect_max: int = 3
    state: ConnectionState = field(default=ConnectionState.NEW, init=False)
    remove_64k_limit: bool = field(default=False, init=False)
    large_tuple_size: bool = field(default=False, init=False)
    long_row_id: bool = field(default=False, init=False)
    _socket: socket.socket | None = field(default=None, init=False, repr=False)
    _input: BinaryIO | None = field(default=None, init=False, repr=False)
    _output: BinaryIO | None = field(default=None, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def connect(self) -> "InformixSqliClient":
        if not self.tls:
            raise SqliUnsupportedAuthentication(
                "Normal Informix ASC authentication sends the password directly; "
                "the pure-Python client requires TLS"
            )
        if not self.server_name or not self.db_locale or not self.client_locale:
            raise SqliUnsupportedAuthentication(
                "server_name, DB_LOCALE and CLIENT_LOCALE are required; locale discovery, "
                "GSS and private-server authentication are unsupported"
            )
        deadline = time.monotonic() + self.login_timeout
        host, port, server = self.hostname, self.port, self.server_name
        visited: set[tuple[str, str, int]] = set()
        redirects = 0
        while True:
            self._reset_connection_state()
            redirected = redirects > 0
            self._validate_redirect_destination(host, port, redirected=redirected)
            addresses = self._resolved_addresses(host, port) if redirected else (host,)
            identity = (server, ",".join(addresses), port)
            if identity in visited:
                raise SqliUnsupportedAuthentication("Informix redirect loop detected")
            visited.add(identity)
            try:
                self._connect_once(
                    host, port, server, deadline, addresses[0] if redirected else None
                )
                self.hostname, self.port, self.server_name = host, port, server
                return self
            except SqliRedirect as redirect:
                self._reset_connection_state()
                if not self.redirect_enabled:
                    raise SqliUnsupportedAuthentication(
                        "Informix redirect is disabled"
                    ) from redirect
                redirects += 1
                if redirects > self.redirect_max:
                    raise SqliUnsupportedAuthentication(
                        "Informix redirect limit exceeded"
                    ) from redirect
                server, host, port = parse_redirect_detail(
                    redirect.detail, set(self.redirect_allowlist)
                )
            except Exception:
                self._poison()
                raise

    def _connect_once(
        self,
        host: str,
        port: int,
        server: str,
        deadline: float,
        validated_address: str | None = None,
    ) -> None:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise SqliUnsupportedAuthentication("Informix login deadline exceeded")
        raw = socket.create_connection(
            (validated_address or host, port), min(self.connect_timeout, remaining)
        )
        raw.settimeout(min(self.socket_timeout, max(0.001, deadline - time.monotonic())))
        raw.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        context = self.ssl_context or ssl.create_default_context(cafile=self.ca_file)
        self._socket = context.wrap_socket(raw, server_hostname=host)
        self._input = self._socket.makefile("rb", buffering=4096)
        self._output = self._socket.makefile("wb", buffering=4096)
        self.state = ConnectionState.SOCKET_OPEN
        properties = {
            "CLIENT_LOCALE": self.client_locale,
            "CLNT_PAM_CAPABLE": "1",
            "DBPATH": ".",
            "DB_LOCALE": self.db_locale,
            "IFX_UPDDESC": "1",
            "NODEFDAC": "no",
        }
        try:
            request = encode_normal_auth_request(
                self.user,
                self.password,
                server,
                self.database,
                properties,
                self._encoding,
            )
            self._output.write(request)
            self._output.flush()
            self.state = ConnectionState.ASC_SENT
            header = read_exact(self._input, 6)
            total = struct.unpack_from(">H", header)[0]
            response = header + read_exact(self._input, total - 6)
            self.asc_accept = decode_asc_accept(response, self._encoding)
            self.state = ConnectionState.ACCEPTED
            self._output.write(encode_protocol_offer())
            self._output.flush()
            self.server_protocols = self._read_protocol_offer()
            self.remove_64k_limit = bool(self.server_protocols[7] & 0x02)
            self.large_tuple_size = bool(self.server_protocols[8] & 0x04)
            self.long_row_id = bool(self.server_protocols[8] & 0x02)
            pam_advertised = _protocol_feature(self.server_protocols, 44)
            if self.authentication_mode == "pam":
                if not pam_advertised:
                    raise SqliUnsupportedAuthentication(
                        "PAM requested but server did not advertise it"
                    )
                self._authenticate_pam(deadline)
            self._output.write(encode_secondary_info(properties, self._encoding))
            self._output.flush()
            self._read_status_group()
            self.state = ConnectionState.AUTHENTICATED
            self._output.write(encode_dbopen(self.database, self._encoding))
            self._output.flush()
            self._read_status_group(require_done=True)
            self.state = ConnectionState.DATABASE_OPEN
        except SqliRedirect:
            raise

    def _authenticate_pam(self, deadline: float) -> None:
        if self._input is None or self._output is None or self.authentication_provider is None:
            raise SqliUnsupportedAuthentication("PAM requires a noninteractive response provider")
        # JDBC's sendACK(); flip() emits both markers.  Informix waits for the
        # EOT before it starts the PAM conversation.
        self._output.write(struct.pack(">hh", 128, SQ_EOT))  # SQ_ACK, SQ_EOT
        self._output.flush()
        for _ in range(self.pam_max_rounds):
            if time.monotonic() >= deadline:
                raise SqliUnsupportedAuthentication("PAM login deadline exceeded")
            code = read_smallint(self._input)
            if code == SQ_ASCEOT:  # SQ_ACCEPT aliases ASCEOT numerically
                if read_smallint(self._input) != SQ_EOT:
                    raise SqliProtocolError("PAM accept is missing EOT")
                return
            if code == SQ_EXIT:
                self._output.write(struct.pack(">hh", SQ_EXIT, SQ_EOT))
                self._output.flush()
                raise SqliUnsupportedAuthentication("Informix rejected PAM authentication")
            if code != 129:
                raise SqliProtocolError(f"unexpected PAM message {code}")
            style = read_smallint(self._input)
            length = read_smallint(self._input)
            if length < 0 or length > 512:
                raise SqliProtocolError("invalid PAM challenge length")
            raw = read_exact(self._input, length)
            if length & 1:
                read_exact(self._input, 1)
            if read_smallint(self._input) != SQ_EOT:
                raise SqliProtocolError("PAM challenge is missing EOT")
            if style not in {1, 2, 3, 4}:
                raise SqliProtocolError(f"unsupported PAM challenge style {style}")
            if style in {3, 4}:
                continue
            challenge = raw.decode(self._encoding, "strict")
            response = self.authentication_provider.respond(style, challenge)
            if isinstance(response, bytes):
                response_raw = response
                if len(response_raw) > 512:
                    raise SqliUnsupportedAuthentication("PAM response exceeds 512-byte bound")
                packet = (struct.pack(">hh", 130, len(response_raw)) + response_raw
                          + (b"\0" if len(response_raw) & 1 else b"") + struct.pack(">h", SQ_EOT))
            else:
                packet = encode_pam_response(response, self._encoding)
            self._output.write(packet)
            self._output.flush()
        raise SqliUnsupportedAuthentication("PAM round limit exceeded")

    def _resolved_addresses(self, host: str, port: int) -> tuple[str, ...]:
        try:
            values = {
                item[4][0]
                for item in socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
            }
        except OSError as exc:
            raise SqliUnsupportedAuthentication("redirect destination cannot be resolved") from exc
        if not values:
            raise SqliUnsupportedAuthentication("redirect destination has no addresses")
        if len(values) != 1:
            raise SqliUnsupportedAuthentication(
                "redirect destination must resolve to exactly one stable address"
            )
        return tuple(sorted(values))

    def _validate_redirect_destination(self, host: str, port: int, redirected: bool) -> None:
        if not redirected:
            return
        if (host, port) not in self.redirect_allowlist:
            raise SqliUnsupportedAuthentication("redirect target is not explicitly allow-listed")
        for value in self._resolved_addresses(host, port):
            address = ipaddress.ip_address(value)
            unsafe = (address.is_loopback or address.is_link_local or address.is_multicast
                      or address.is_unspecified or address.is_private)
            if redirected and unsafe and (str(address), port) not in self.redirect_allowlist:
                raise SqliUnsupportedAuthentication("redirect resolved to a non-public address")

    def _reset_connection_state(self) -> None:
        self.close()
        self.remove_64k_limit = self.large_tuple_size = self.long_row_id = False
        for name in ("asc_accept", "server_protocols"):
            if hasattr(self, name):
                delattr(self, name)
        self.state = ConnectionState.NEW

    @property
    def _encoding(self) -> str:
        locale = (self.client_locale or "en_US.utf8").rsplit(".", 1)[-1].lower()
        aliases = {
            "819": "iso8859-1",
            "iso8859-1": "iso8859-1",
            "iso-8859-1": "iso8859-1",
            "utf8": "utf-8",
            "utf-8": "utf-8",
            "57372": "utf-8",
        }
        try:
            return aliases[locale]
        except KeyError as exc:
            raise SqliProtocolError(
                f"Unsupported Informix locale codeset {locale!r}; configure a verified alias"
            ) from exc

    def _require_open(self) -> tuple[BinaryIO, BinaryIO]:
        if (
            self.state != ConnectionState.DATABASE_OPEN
            or self._input is None
            or self._output is None
        ):
            raise SqliProtocolError("SQLI database session is not open")
        return self._input, self._output

    def _read_protocol_offer(self) -> bytes:
        if self._input is None:
            raise SqliProtocolError("SQLI input is unavailable")
        code = read_smallint(self._input)
        if code != SQ_PROTOCOLS:
            raise SqliProtocolError(f"expected SQ_PROTOCOLS, received {code}")
        size = read_smallint(self._input)
        if size < 5 or size > 1024:
            raise SqliProtocolError(f"invalid enhanced protocol size {size}")
        value = read_exact(self._input, size)
        if size & 1:
            read_exact(self._input, 1)
        if read_smallint(self._input) != SQ_EOT:
            raise SqliProtocolError("enhanced protocol response is missing EOT")
        return value

    def _read_status_group(self, require_done: bool = False) -> None:
        if self._input is None:
            raise SqliProtocolError("SQLI input is unavailable")
        saw_done = False
        while True:
            code = read_smallint(self._input)
            if code == SQ_EOT:
                if require_done and not saw_done:
                    raise SqliProtocolError("DBOPEN response reached EOT without SQ_DONE")
                return
            if code == 55:  # SQ_COST
                read_int32(self._input)
                read_int32(self._input)
                continue
            if code == 99:  # SQ_XACTSTAT
                read_exact(self._input, 6)
                continue
            if code == SQ_DONE:
                self._read_done()
                saw_done = True
                continue
            if code == SQ_CLOSE:
                continue
            if code == SQ_ERR:
                error = self._read_error()
                if error[0] != 100:
                    raise SqliProtocolError(
                        f"Informix SQL error {error[0]}/{error[1]} at {error[2]}: {error[3]}"
                    )
                continue
            raise SqliDescriptorNotImplemented(
                f"status response message {code} requires an unrecovered handler body"
            )

    def _read_done(self) -> tuple[int, int, int, int]:
        if self._input is None:
            raise SqliProtocolError("SQLI input is unavailable")
        warnings = read_smallint(self._input)
        if self.long_row_id:
            rows, row_id = struct.unpack(">qq", read_exact(self._input, 16))
        else:
            rows, row_id = read_int32(self._input), read_int32(self._input)
        serial = read_int32(self._input)
        return warnings, rows, row_id, serial

    def _read_error(self) -> tuple[int, int, int, str]:
        if self._input is None:
            raise SqliProtocolError("SQLI input is unavailable")
        sqlcode, isamcode = read_smallint(self._input), read_smallint(self._input)
        statement_offset = (
            read_int32(self._input) if self.remove_64k_limit else read_smallint(self._input)
        )
        message = "" if sqlcode == -368 else decode_char(self._input, self._encoding)
        return sqlcode, isamcode, statement_offset, message

    def execute(self, sql: str, parameters: tuple = ()) -> list[tuple]:
        _, output_stream = self._require_open()
        # Connector queries are fixed templates. Literalizing their bounded scalar
        # values avoids depending on the separate input-parameter descriptor that
        # JDBC uses to choose VARCHAR bind widths.
        sql = _connector_sql(sql, parameters)
        typed: tuple[TypedBind, ...] = ()
        with self._lock:
            try:
                output_stream.write(
                    encode_prepare(sql, self._encoding, len(typed), self.remove_64k_limit)
                )
                output_stream.flush()
                description, rows, _ = self._read_query_group(None)
                if description is None:
                    return rows
                cursor_name = f"lc_{description.statement_id}"
                binds = encode_bind(typed, self._encoding) if typed else b""
                variable = not self.pad_varchar and any(
                    (column.type_code & 0xFF) in {13, 16, 40, 41, 43, 45, 46}
                    for column in description.columns
                )
                if variable:
                    output_stream.write(
                        encode_cursor_open(
                            description.statement_id, cursor_name, self._encoding, binds
                        )
                    )
                    output_stream.flush()
                    self._read_query_group(description)
                    output_stream.write(encode_variable_fetch(description))
                    output_stream.flush()
                    _, batch, exhausted = self._read_query_group(description)
                    rows.extend(batch)
                    if exhausted:
                        return self._close_query(description, rows, output_stream)
                else:
                    output_stream.write(
                        encode_fixed_open_fetch(
                            description.statement_id,
                            cursor_name,
                            binds,
                            self._encoding,
                            4096,
                        )
                    )
                    output_stream.flush()
                    _, batch, exhausted = self._read_query_group(description)
                    rows.extend(batch)
                    if exhausted:
                        return self._close_query(description, rows, output_stream)
                while True:
                    output_stream.write(encode_fetch(description.statement_id))
                    output_stream.flush()
                    _, batch, exhausted = self._read_query_group(description)
                    rows.extend(batch)
                    if exhausted:
                        break
                output_stream.write(encode_close_release(description.statement_id))
                output_stream.flush()
                self._read_query_group(description)
                output_stream.write(encode_close_release(description.statement_id, release=True))
                output_stream.flush()
                self._read_query_group(description)
                return rows
            except Exception:
                self._poison()
                raise

    def _close_query(self, description, rows, output_stream):
        output_stream.write(encode_close_release(description.statement_id))
        output_stream.flush()
        self._read_query_group(description)
        output_stream.write(encode_close_release(description.statement_id, release=True))
        output_stream.flush()
        self._read_query_group(description)
        return rows

    def _read_query_group(
        self, description: ResultDescription | None
    ) -> tuple[ResultDescription | None, list[dict[str, object]], bool]:
        if self._input is None:
            raise SqliProtocolError("SQLI input is unavailable")
        rows, exhausted = [], False
        while True:
            code = read_smallint(self._input)
            if code == SQ_EOT:
                return description, rows, exhausted
            if code == 8:
                if description is not None:
                    raise SqliProtocolError("duplicate SQ_DESCRIBE")
                description = self._read_description()
            elif code == 14:
                if description is None:
                    raise SqliProtocolError("SQ_TUPLE arrived before SQ_DESCRIBE")
                rows.append(self._read_tuple(description))
            elif code == SQ_ERR:
                error = self._read_error()
                if error[0] == 100:
                    exhausted = True
                else:
                    raise SqliProtocolError(
                        f"Informix SQL error {error[0]}/{error[1]} at {error[2]}: {error[3]}"
                    )
            elif code == 55:
                read_exact(self._input, 8)
            elif code == 99:
                read_exact(self._input, 6)
            elif code == SQ_DONE:
                _, affected_rows, _, _ = self._read_done()
                # A forward-only fetch that has no more tuples is reported as
                # SQ_DONE(rows=0) by IDS 15 (JDBC maps the accompanying zero
                # row id to its SQLSTATE-02000/end-of-data warning).
                if description is not None and (affected_rows == 0 or not rows):
                    exhausted = True
            elif code == SQ_CLOSE:
                # Informix may close a forward-only cursor in the same group as
                # the final tuple instead of emitting SQLCODE 100.  JDBC tracks
                # this as SQ_CLOSERecvd and does not issue another NFETCH.
                exhausted = True
            else:
                raise SqliProtocolError(f"unknown query response message {code}")

    def _read_description(self) -> ResultDescription:
        if self._input is None:
            raise SqliProtocolError("SQLI input is unavailable")
        statement_type, statement_id = read_smallint(self._input), read_smallint(self._input)
        read_int32(self._input)  # optimizer row estimate
        tuple_size = (
            read_int32(self._input) if self.large_tuple_size else read_smallint(self._input)
        )
        count = read_smallint(self._input)
        name_size = read_int32(self._input)
        tuple_limit = MAX_PACKET if self.large_tuple_size else 32767
        if tuple_size < 0 or tuple_size > tuple_limit or count < 0 or count > 1024:
            raise SqliProtocolError("unsafe result description bounds")
        raw = []
        for _ in range(count):
            read_int32(self._input)
            position = read_int32(self._input)
            type_code = read_smallint(self._input)
            extended_id = read_int32(self._input)
            decode_char(self._input, self._encoding)
            decode_char(self._input, self._encoding)
            read_smallint(self._input)
            read_smallint(self._input)
            read_int32(self._input)
            encoded_length = read_int32(self._input)
            if (
                position < 0
                or position > tuple_size
                or encoded_length < 0
                # Variable-width descriptors carry their declared maximum,
                # which may exceed the current tuple's encoded size.
                or encoded_length > tuple_limit
            ):
                raise SqliProtocolError("invalid result column position/length")
            raw.append((position, type_code, extended_id, encoded_length))
        if name_size < 0 or name_size > 1 << 20:
            raise SqliProtocolError(f"unsafe descriptor name blob length {name_size}")
        names_raw = read_exact(self._input, name_size)
        if name_size & 1:
            read_exact(self._input, 1)
        names = [item.decode(self._encoding) for item in names_raw.rstrip(b"\0").split(b"\0")]
        if len(names) != count or len(set(names)) != count:
            raise SqliProtocolError("descriptor names do not match column count")
        columns = tuple(ResultColumn(name, *descriptor) for name, descriptor in zip(names, raw))
        if any(b.position < a.position for a, b in zip(columns, columns[1:])):
            raise SqliProtocolError("result column positions are not monotonic")
        return ResultDescription(statement_type, statement_id, tuple_size, columns)

    def _read_tuple(self, description: ResultDescription) -> dict[str, object]:
        if self._input is None:
            raise SqliProtocolError("SQLI input is unavailable")
        read_smallint(self._input)  # tuple warning
        size = read_int32(self._input)
        tuple_limit = MAX_PACKET if self.large_tuple_size else 32767
        if size < 0 or size > tuple_limit:
            raise SqliProtocolError(f"unsafe tuple payload length {size}")
        payload = read_exact(self._input, size)
        if size & 1:
            read_exact(self._input, 1)
        result = {}
        variable_layout = not self.pad_varchar and any(
            (column.type_code & 0xFF) in {13, 16, 40, 41, 43, 45, 46}
            for column in description.columns
        )
        cursor = 0
        for index, column in enumerate(description.columns):
            kind = column.type_code & 0xFF
            start = cursor if variable_layout else column.position
            if variable_layout and kind in {13, 16}:
                if start >= len(payload):
                    raise SqliProtocolError("truncated variable VARCHAR tuple")
                limit = start + 1 + payload[start]
            elif variable_layout and kind in {40, 41, 43, 45, 46}:
                if start + 5 > len(payload):
                    raise SqliProtocolError("truncated complex variable tuple")
                limit = start + 5 + struct.unpack_from(">i", payload, start + 1)[0]
            elif variable_layout:
                limit = start + _fixed_result_size(column, payload[start:])
            else:
                limit = (
                    description.columns[index + 1].position
                    if index + 1 < len(description.columns)
                    else len(payload)
                )
            if not 0 <= start <= limit <= len(payload):
                raise SqliProtocolError(
                    f"tuple column {column.name!r} slice [{start}:{limit}] "
                    f"exceeds payload size {len(payload)}"
                )
            result[column.name] = _decode_result_value(
                payload[start:limit], column, self._encoding, self.pad_varchar
            )
            cursor = limit
        return result

    def read_lodata(self, descriptor: int, requested: int) -> bytes:
        input_stream, output_stream = self._require_open()
        with self._lock:
            try:
                output_stream.write(encode_lodata_read(descriptor, requested))
                output_stream.write(struct.pack(">h", SQ_EOT))
                output_stream.flush()
                chunks = bytearray()
                while True:
                    code = read_smallint(input_stream)
                    if code == SQ_EOT:
                        return bytes(chunks)
                    if code == SQ_LODATA:
                        operation, size = read_smallint(input_stream), read_int32(input_stream)
                        if operation not in {LO_READ, LO_READ_WITH_SEEK}:
                            raise SqliProtocolError(
                                f"unexpected SQ_LODATA response operation {operation}"
                            )
                        if size < 0:
                            raise SqliProtocolError(f"SQ_LODATA server/ISAM error {size}")
                        if size < 1:
                            read_smallint(input_stream)
                            continue
                        if len(chunks) + size > requested:
                            raise SqliProtocolError(
                                "SQ_LODATA response exceeds requested bound"
                            )
                        remaining = size
                        while remaining:
                            chunk_size = read_smallint(input_stream)
                            if chunk_size <= 0 or chunk_size > remaining:
                                raise SqliProtocolError(
                                    f"invalid SQ_LODATA chunk size {chunk_size}"
                                )
                            chunks.extend(read_exact(input_stream, chunk_size))
                            if chunk_size & 1:
                                read_exact(input_stream, 1)
                            remaining -= chunk_size
                        continue
                    if code == SQ_ERR:
                        raise SqliDescriptorNotImplemented(
                            "SQ_ERR received; exact error-body decoder is not yet recovered"
                        )
                    raise SqliProtocolError(f"unexpected SQLI message {code} during LODATA")
            except Exception:
                self._poison()
                raise

    def _poison(self) -> None:
        self.close()
        self.state = ConnectionState.POISONED

    def close(self) -> None:
        for stream in (self._input, self._output):
            if stream is not None:
                try:
                    stream.close()
                except OSError:
                    pass
        if self._socket is not None:
            try:
                self._socket.close()
            except OSError:
                pass
        self._input = self._output = None
        self._socket = None
        self.state = ConnectionState.CLOSED


__all__ = [
    "AscAccept",
    "AuthenticationProvider",
    "CdcTransport",
    "ConnectionState",
    "InformixSqliClient",
    "PasswordAuthenticationProvider",
    "SqliDescriptorNotImplemented",
    "SqliProtocolError",
    "SqliRedirect",
    "SqliUnsupportedAuthentication",
    "TypedBind",
    "decode_char",
    "decode_asc_accept",
    "decode_asc_response",
    "decode_lodata_response",
    "decode_pam_challenge",
    "encode_asc_environment",
    "encode_asc_char",
    "encode_asc_tail",
    "encode_bind",
    "encode_char",
    "encode_close_release",
    "encode_cursor_open",
    "encode_dbopen",
    "encode_fetch",
    "encode_variable_fetch",
    "encode_fixed_open_fetch",
    "encode_lodata_read",
    "encode_normal_auth_prefix",
    "encode_normal_auth_request",
    "encode_protocol_offer",
    "encode_pam_response",
    "encode_prepare",
    "encode_secondary_info",
    "encode_session_packet",
    "encode_simple_command",
    "materialize_blob_chunks",
    "parse_redirect_detail",
    "read_session_packet",
]
