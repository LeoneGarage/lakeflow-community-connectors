"""SQLI wire values: result decoding, fetch-buffer encoding and SQL text for the client."""

from __future__ import annotations

import errno
import io
import ipaddress
import logging
import os
import re
import socket
import ssl
import struct
import sys
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, localcontext
from enum import Enum, auto
from typing import BinaryIO, Protocol

from databricks.labs.community_connector.sources.informix.cdc_protocol import (
    ColumnDescriptor,
    apply_null_byte,
    datetime_size,
    decode_interval,
    decode_packed_decimal,
    decode_value,
    interval_layout,
)


def add_informix_exception_note(error: BaseException, note: str) -> None:
    # Docstring: Attach secondary diagnostics on every supported Python version.

    add_note = getattr(error, "add_note", None)
    if add_note is not None:
        add_note(note)
        return
    # Python 3.10 does not render PEP 678 notes, but exceptions have a writable
    # attribute dictionary. Preserve the same inspection contract for callers
    # and log the diagnostic so it remains visible in worker output.
    notes = getattr(error, "__notes__", None)
    if notes is None:
        notes = []
        error.__notes__ = notes
    notes.append(note)
    logging.getLogger(__name__).warning("%s", note)


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
SQ_INSERTDONE = 94
SQ_ASSOC = 100
SQ_ASCBINARY = 101
SQ_ASCENV = 106
SQ_ASCPINFO = 107
SQ_ASCMISC_60 = 116
SQ_ASCEOT = 127
LO_READ = 0
LO_READ_WITH_SEEK = 1
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


# These dataclasses turn off the generated __repr__ and __eq__: nothing prints them or
# compares them by value, a repr would expose the password fields below, and the
# merged deployment pickles every generated method.
@dataclass(frozen=True, eq=False, repr=False)
class InformixDatetimeLiteral:
    # Docstring: A canonical partial DATETIME value paired with its native qualifier.

    value: str
    qualifier: int


class SqliTransportError(SqliProtocolError):
    # Docstring: A connection-specific SQLI failure that is safe to retry on another address.
    pass


class SqliSessionRejected(SqliProtocolError):
    # Docstring: Informix rejected the SQLI session before authentication diagnostics.
    pass


class SqliSqlError(SqliProtocolError):
    # Docstring: An error the server reported for a statement (SQ_ERR); ``code`` is its SQL
    # error code.

    code = 0


def _sql_error(error: tuple, during: str = "") -> SqliSqlError:
    # Docstring: The SqliSqlError for a statement's SQ_ERR ``error`` (code, ISAM code,
    # offset, text).

    failure = SqliSqlError(
        f"Informix SQL error {error[0]}/{error[1]} at {error[2]}{during}: {error[3]}"
    )
    failure.code = int(error[0])
    return failure


def informix_locale_encoding(locale: str | None) -> str:
    # Docstring: Return the verified Python codec for an Informix locale codeset.

    codeset = (locale or "en_US.utf8").rsplit(".", 1)[-1].lower()
    aliases = {
        "819": "iso8859-1",
        "iso8859-1": "iso8859-1",
        "iso-8859-1": "iso8859-1",
        "utf8": "utf-8",
        "utf-8": "utf-8",
        "57372": "utf-8",
    }
    try:
        return aliases[codeset]
    except KeyError as exc:
        raise SqliProtocolError(
            f"Unsupported Informix locale codeset {codeset!r}; configure a verified alias"
        ) from exc


def _decode_text(raw: bytes, encoding: str) -> str:
    # Docstring: Decode a result string, turning a codeset mismatch into an actionable error.
    #
    # Informix sends text as the database holds it, in its codeset, so ``encoding`` is
    # the DB_LOCALE codec (see InformixSqliClient._text_encoding). A strict decode
    # failure means the value's bytes are not text in that codeset: written by a client
    # that sent another codeset unconverted, or DB_LOCALE naming another codeset than the
    # database's (the connector checks that when it connects).

    try:
        return raw.decode(encoding)
    except UnicodeDecodeError as exc:
        raise SqliProtocolError(
            f"Could not decode an Informix result string as {encoding!r}, the codeset of "
            "DB_LOCALE: the database holds a value that is not text in its codeset, "
            "written by a client that did not convert it, or DB_LOCALE does not name the "
            "database's locale"
        ) from exc


class SqliUnsupportedAuthentication(SqliProtocolError):
    pass


class SqliRedirect(SqliUnsupportedAuthentication):
    def __init__(self, detail: str) -> None:
        super().__init__("Informix requested a connection redirect")
        self.detail = detail


class SqliInvalidCaFile(SqliProtocolError):
    # Docstring: ssl.ca.file read, but holds no certificate that loads: no retry helps.
    pass


class SqliDescriptorNotImplemented(SqliProtocolError):
    pass


class _DeadlineReader:
    # Docstring: Apply one absolute login deadline to every underlying stream read.

    def __init__(
        self, stream: BinaryIO, connection: socket.socket, deadline: float, maximum: float
    ) -> None:
        self._stream = stream
        self._connection = connection
        self._deadline: float | None = deadline
        self._maximum = maximum

    def read(self, size: int = -1) -> bytes:
        if self._deadline is not None:
            remaining = self._deadline - time.monotonic()
            if remaining <= 0:
                raise SqliUnsupportedAuthentication("Informix login deadline exceeded")
            self._connection.settimeout(min(self._maximum, remaining))
        return self._stream.read(size)

    def finish(self) -> None:
        self._deadline = None
        self._connection.settimeout(self._maximum)

    def close(self) -> None:
        self._stream.close()


class ConnectionState(Enum):
    NEW = auto()
    SOCKET_OPEN = auto()
    ASC_SENT = auto()
    ACCEPTED = auto()
    AUTHENTICATED = auto()
    DATABASE_OPEN = auto()
    POISONED = auto()
    CLOSED = auto()


@dataclass(frozen=True, eq=False, repr=False)
class AscAccept:
    version: str
    cap_1: int
    cap_2: int
    cap_3: int
    warnings: int


@dataclass(frozen=True, eq=False, repr=False)
class ResultColumn:
    name: str
    position: int
    type_code: int
    extended_id: int
    encoded_length: int
    extended_owner: str = ""
    extended_name: str = ""


@dataclass(frozen=True, eq=False, repr=False)
class ResultDescription:
    statement_type: int
    statement_id: int
    tuple_size: int
    columns: tuple[ResultColumn, ...]


class AuthenticationProvider(Protocol):
    # Docstring: Noninteractive, secret-backed PAM response provider.

    def respond(self, style: int, challenge: str) -> str | bytes: ...


@dataclass(frozen=True, eq=False, repr=False)
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


def redirect_host_key(host: str) -> str:
    # Docstring: A redirect host as allow-list entries and redirect targets are compared: a
    # name case-folded without its trailing dot (DNS names are case-insensitive), an IP
    # address in its canonical form, an IPv6 one without its brackets.
    name = host.strip()
    if name.startswith("[") and name.endswith("]"):
        name = name[1:-1]
    try:
        return ipaddress.ip_address(name).compressed
    except ValueError:
        return name.rstrip(".").casefold()


def parse_redirect_detail(detail: str, allowlist: set[tuple[str, int]]) -> tuple[str, str, int]:
    tokens = re.split(r"[:=|]", detail)
    if (
        len(tokens) not in {4, 5}
        or not tokens[-1].isdigit()
        or any(not token for token in tokens)
        or any(ord(char) < 0x20 or ord(char) == 0x7F for char in detail)
    ):
        raise SqliUnsupportedAuthentication("redirect detail has unsafe grammar")
    server, host, port = tokens[-3], tokens[-2], int(tokens[-1])
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", server) or not re.fullmatch(r"[A-Za-z0-9.-]+", host):
        raise SqliUnsupportedAuthentication("redirect detail has unsafe identifiers")
    if (
        not server
        or not host
        or not 1 <= port <= 65535
        or (redirect_host_key(host), port) not in allowlist
    ):
        raise SqliUnsupportedAuthentication("redirect target is not explicitly allow-listed")
    return server, host, port


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


def read_exact(stream: BinaryIO, size: int) -> bytes:
    if size < 0 or size > MAX_PACKET:
        raise SqliProtocolError(f"unsafe exact-read size {size}")
    chunks = bytearray()
    while len(chunks) < size:
        chunk = stream.read(size - len(chunks))
        if not chunk:
            raise SqliTransportError(f"truncated SQLI stream: needed {size}, got {len(chunks)}")
        chunks.extend(chunk)
    return bytes(chunks)


def read_smallint(stream: BinaryIO) -> int:
    return struct.unpack(">h", read_exact(stream, 2))[0]


def decode_char(stream: BinaryIO, encoding: str = "utf-8", maximum: int = 1 << 20) -> str:
    length = read_smallint(stream)
    if length < 0 or length > maximum:
        raise SqliProtocolError(f"invalid SQLI CHAR length {length}")
    raw = read_exact(stream, length)
    if length & 1:
        read_exact(stream, 1)
    return _decode_text(raw, encoding)


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
        return _decode_text(raw.rstrip(b"\0"), encoding)


def decode_asc_accept(packet: bytes, encoding: str = "utf-8") -> AscAccept:
    sl_type, protocol, payload = read_session_packet(io.BytesIO(packet))
    if protocol != SQLI_PROTOCOL:
        raise SqliProtocolError(f"server selected unsupported SQLI protocol {protocol}")
    if sl_type == 3:
        raise SqliSessionRejected(
            "Informix rejected the SQLI session before returning a diagnostic; "
            "inspect the server log (connection exhaustion commonly reports SQLCODE -25571)"
        )
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
    # Latin-1 decodes any bytes: the version is only reported, never worth failing a connect.
    version_text = cursor.bytes(cursor.smallint_length()).decode("latin-1").rstrip("\0")
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


def encode_normal_auth_prefix(
    username: str,
    password: str,
    server_name: str,
    database: str,
    encoding: str = "utf-8",
) -> bytes:
    # Docstring: Encode the fixed ASC normal-password portion before the recovered ASC tail.
    #
    # Password bytes are plaintext; callers must place this packet inside TLS.

    del database  # part of the public signature; the prefix does not carry it
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
    # The host's name and working directory are for the server's session list only: one
    # the login's codec can't spell, or a working directory that is gone, failed the login.
    try:
        cwd = os.getcwd()
    except OSError:
        cwd = ""

    def spelled(text: str) -> str:
        return text.encode(encoding, "replace").decode(encoding)

    tail = encode_asc_tail(
        properties,
        hostname=spelled(socket.gethostname()),
        process_id=os.getpid(),
        thread_id=threading.get_ident() & 0x7FFFFFFF,
        cwd=spelled(cwd),
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


def _effective_result_kind(column: ResultColumn) -> int:
    kind = column.type_code & 0xFF
    extended_owner = column.extended_owner.strip().upper()
    extended_name = column.extended_name.strip().upper()
    if kind == 40 and (extended_owner, extended_name) == ("INFORMIX", "LVARCHAR"):
        return 43
    if kind == 41 and (extended_owner, extended_name) == ("INFORMIX", "BOOLEAN"):
        return 45
    return kind


def encode_variable_fetch(
    description: ResultDescription, buffer_size: int = 32767, encoding: str = "utf-8"
) -> bytes:
    # Docstring: Encode the two-phase variable-row RET_TYPE + NFETCH request.

    if not 1 <= buffer_size <= 32767:
        raise ValueError("fetch buffer_size must be in [1, 32767]")
    result = bytearray(
        struct.pack(
            ">hhhhh", SQ_ID, description.statement_id, SQ_RET_TYPE, 1, len(description.columns)
        )
    )
    for column in description.columns:
        native_kind = column.type_code & 0xFF
        kind = _effective_result_kind(column)
        if kind > 18 and kind not in {23, 43, 45, 52, 53}:
            raise SqliDescriptorNotImplemented(
                f"variable result type {kind} requires an extended SQ_RET_TYPE name"
            )
        result.extend(struct.pack(">h", kind))
        if kind == 43:
            result.extend(encode_char("", encoding))
            result.extend(encode_char("", encoding))
        elif native_kind in {40, 41} and column.extended_id:
            if not column.extended_owner or not column.extended_name:
                raise SqliDescriptorNotImplemented(
                    f"extended result type {native_kind} is missing owner/type metadata"
                )
            result.extend(encode_char(column.extended_owner, encoding))
            result.extend(encode_char(column.extended_name, encoding))
        result.extend(struct.pack(">i", column.encoded_length))
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


# What a socket raises when the connection is lost mid-operation: a reset or a close
# without TLS close_notify (an NLB idle reset, a PrivateLink drop, a server crash), a write
# to a closed peer, a read timeout. read_exact raises SqliTransportError only for a clean
# end of stream.
_CONNECTION_LOSS = (ConnectionError, TimeoutError, ssl.SSLError)
_UNREACHABLE_ERRNOS = frozenset({errno.ENETUNREACH, errno.EHOSTUNREACH, errno.ENETDOWN})


# What no retry helps: a certificate that failed verification, a login refused (bad
# credentials, a login deadline, a redirect not allowed), a type the client can't decode,
# an ssl.ca.file path that names no file (see _ca_file_misnamed).
# Errors in the statement itself, which a new session reads the same way: a syntax error
# (a snapshot.filter's), a table or column not there, a privilege missing, a refused
# newline, a value that does not convert or overflows. Retried as a lost connection, each
# reconnected and failed again before the read did. Any other SQL error, a locked row's
# among them, is still retried on a new session.
_STATEMENT_SQL_ERRORS = frozenset(
    {-201, -206, -217, -272, -282, -329, -387, -1213, -1226, -1262, -1263}
)
CONFIGURATION_ERRORS = (
    ssl.SSLCertVerificationError,
    SqliUnsupportedAuthentication,
    SqliDescriptorNotImplemented,
    SqliInvalidCaFile,
    FileNotFoundError,
    IsADirectoryError,
    NotADirectoryError,
)


def statement_sql_error(error: BaseException) -> bool:
    # Docstring: Whether ``error`` is one in the statement itself (_STATEMENT_SQL_ERRORS),
    # which a new session reads the same way.

    return isinstance(error, SqliSqlError) and error.code in _STATEMENT_SQL_ERRORS


def reconnectable_error(error: BaseException) -> bool:
    # Docstring: Whether an in-place reconnect may retry ``error``: a lost connection, or a
    # connect refused, reset or timed out, a TLS handshake cut short, a name lookup or a
    # route that failed while the network or the listener recovers. connect() raises these
    # as they are. Not a certificate that failed verification, a login refused (bad
    # credentials, a login deadline, a redirect not allowed) or a type it can't decode:
    # those are configuration, and fail the same way again.

    if isinstance(error, CONFIGURATION_ERRORS) or statement_sql_error(error):
        return False
    return isinstance(error, (SqliProtocolError, socket.gaierror, *_CONNECTION_LOSS)) or (
        isinstance(error, OSError) and error.errno in _UNREACHABLE_ERRNOS
    )


def _raise_connection_loss(error: BaseException) -> None:
    # Docstring: Raise ``error`` as the SqliTransportError it is when it is a lost
    # connection, so the readers' reconnect loops, which catch SqliProtocolError, retry it
    # in place as they do a truncated stream. The socket error is kept as its cause, where
    # the timeout checks look (_is_timeout_error).

    if isinstance(error, _CONNECTION_LOSS):
        raise SqliTransportError(f"SQLI connection lost: {error!r}") from error


def read_int32(stream: BinaryIO) -> int:
    return struct.unpack(">i", read_exact(stream, 4))[0]


def _retained_size(value: object, seen: set[int] | None = None) -> int:
    # Docstring: Estimate retained decoded-result memory without double counting objects.

    if seen is None:
        seen = set()
    identity = id(value)
    if identity in seen:
        return 0
    seen.add(identity)
    size = sys.getsizeof(value)
    if isinstance(value, dict):
        return size + sum(
            _retained_size(key, seen) + _retained_size(item, seen) for key, item in value.items()
        )
    if isinstance(value, (list, tuple, set, frozenset)):
        return size + sum(_retained_size(item, seen) for item in value)
    return size


_DATETIME_FIELD_NAMES = {
    0: "YEAR",
    2: "MONTH",
    4: "DAY",
    6: "HOUR",
    8: "MINUTE",
    10: "SECOND",
    11: "FRACTION(1)",
    12: "FRACTION(2)",
    13: "FRACTION(3)",
    14: "FRACTION(4)",
    15: "FRACTION(5)",
}


def _partial_datetime_sql(value: str, qualifier: int) -> str:
    start, end = (qualifier >> 8) & 0xF, qualifier & 0xF
    fields = [code for code in (0, 2, 4, 6, 8, 10) if start <= code <= min(end, 10)]
    if end > 10:
        fields.append(end)
    expected = [_DATETIME_FIELD_NAMES[code] for code in fields]
    if start == 6 and "=" not in value:
        base_fields = [code for code in fields if code <= 10]
        main, separator, fraction = value.partition(".")
        components = main.split(":")
        if (
            # pylint: disable-next=too-many-boolean-expressions
            len(components) != len(base_fields)
            or any(len(component) != 2 or not component.isdigit() for component in components)
            or (end > 10 and (not separator or len(fraction) != end - 10 or not fraction.isdigit()))
            or (end <= 10 and separator)
        ):
            raise ValueError("TIME string does not match its Informix DATETIME qualifier")
        return f"DATETIME({value}) {expected[0]} TO {expected[-1]}"
    parts = value.split(":")
    if len(parts) != len(expected):
        raise ValueError("partial Informix DATETIME does not match its qualifier")
    values: list[str] = []
    for part, name in zip(parts, expected, strict=True):
        prefix = name + "="
        if not part.startswith(prefix):
            raise ValueError("partial Informix DATETIME does not match its qualifier")
        component = part[len(prefix) :]
        width = 4 if name == "YEAR" else int(name[-2]) if name.startswith("FRACTION") else 2
        if len(component) != width or not component.isdigit():
            raise ValueError("invalid partial Informix DATETIME component")
        values.append(component)

    payload = values[0]
    for previous, code, component in zip(fields[:-1], fields[1:], values[1:], strict=True):
        if code >= 11:
            separator = "."
        elif previous == 4 and code == 6:
            separator = " "
        elif code <= 4:
            separator = "-"
        else:
            separator = ":"
        payload += separator + component
    # TO even for one field: Informix takes DATETIME(2024) YEAR TO YEAR, and fails
    # DATETIME(2024) YEAR with -201 (checked on 14.10).
    return f"DATETIME({payload}) {expected[0]} TO {expected[-1]}"


# pylint: disable-next=too-many-branches,too-many-statements
def _connector_sql(sql: str, parameters: tuple, encoding: str = "utf-8") -> str:
    # Docstring: Bind only connector-owned scalar values using strict SQL literals.
    #
    # ``encoding`` is the one the statement goes out in (the database codeset's): a
    # string literal's bound counts its bytes there, quotes doubled.

    result: list[str] = []
    parameter_index = 0
    index = 0
    quote = None
    while index < len(sql):
        character = sql[index]
        following = sql[index + 1] if index + 1 < len(sql) else ""
        if quote:
            result.append(character)
            if character == quote:
                if following == quote:
                    result.append(following)
                    index += 1
                else:
                    quote = None
            index += 1
            continue
        if character in {"'", '"'}:
            quote = character
            result.append(character)
            index += 1
            continue
        if character == "-" and following == "-":
            end = sql.find("\n", index + 2)
            end = len(sql) if end < 0 else end
            result.append(sql[index:end])
            index = end
            continue
        if character == "/" and following == "*":
            end = sql.find("*/", index + 2)
            if end < 0:
                raise ValueError("unterminated connector SQL block comment")
            result.append(sql[index : end + 2])
            index = end + 2
            continue
        if character != "?":
            result.append(character)
            index += 1
            continue
        if parameter_index >= len(parameters):
            raise ValueError("SQL placeholder count does not match parameters")
        value = parameters[parameter_index]
        if isinstance(value, InformixDatetimeLiteral):
            literal = _partial_datetime_sql(value.value, value.qualifier)
        elif isinstance(value, bool):
            # A BOOLEAN's literals: Informix has no cast from an integer to one.
            literal = "'t'" if value else "'f'"
        elif isinstance(value, int):
            literal = str(value)
        elif isinstance(value, Decimal):
            if not value.is_finite():
                raise ValueError("Informix DECIMAL parameters must be finite")
            # A DECIMAL(p) with no scale decodes with the zeros of its unused digits (0 as
            # 0E-162, 1 as 1 with 32 zeros). The literal is the number without them, and
            # in exponent form when it is still longer than a DECIMAL's 32 digits.
            # Informix 14.10 took the long forms too (probed: 164 characters of zeros,
            # and 1 with 42 zeros); the short one says the same and reads as a number.
            if not value:
                literal = "0"
            else:
                with localcontext() as decimal_context:
                    decimal_context.prec = 80
                    plain = value.normalize()
                literal = format(plain, "f")
                if sum(character.isdigit() for character in literal) > 32:
                    literal = format(plain, "E")
            if len(literal) > 32767:
                raise ValueError("Informix DECIMAL parameter exceeds SQL literal bound")
        elif isinstance(value, datetime):
            # A decoded year-1 value carries UTC (see cdc_protocol's datetime rendering):
            # an Informix DATETIME has no zone, so UTC is its wall clock. Any other
            # zone is not one the connector made.
            if value.tzinfo is not None:
                if value.utcoffset():
                    raise ValueError("Informix DATETIME parameters must be timezone-free")
                value = value.replace(tzinfo=None)
            fraction = value.microsecond // 10
            # Each part by itself: strftime's %Y does not pad a year below 1000 on
            # Linux, and a year-1 value would read 1-01-01.
            literal = (
                f"DATETIME({value.year:04d}-{value.month:02d}-{value.day:02d} "
                f"{value.hour:02d}:{value.minute:02d}:{value.second:02d}.{fraction:05d}) "
                "YEAR TO FRACTION(5)"
            )
        elif isinstance(value, date):
            # DATE(string) is interpreted through the session's DBDATE setting.
            # MDY uses numeric components and is therefore locale-independent.
            literal = f"MDY({value.month},{value.day},{value.year})"
        elif isinstance(value, str):
            escaped = value.replace("'", "''")
            # Counted as sent: in UTF-8 a Latin-1 database's accented value was twice its
            # size, and before doubling its quotes a value of many was half of it.
            if "\0" in value:
                # A key or bound value holding one, under string.null.byte=keep.
                raise ValueError(
                    "Informix SQL cannot be given a string holding a NUL byte, as a key or "
                    "index value this read pages on does: set snapshot.scan=cursor, which "
                    "reads the table without paging on them"
                )
            if len(escaped.encode(encoding)) > 32767:
                raise ValueError("unsafe connector SQL string literal")
            literal = "'" + escaped + "'"
        else:
            raise TypeError(f"unsupported connector SQL value {type(value).__name__}")
        result.append(literal)
        parameter_index += 1
        index += 1
    if parameter_index != len(parameters):
        raise ValueError("SQL placeholder count does not match parameters")
    return "".join(result)


def datetime_order_preserving_cast(column: str, qualifier: int) -> str | None:
    # Docstring: Return a fixed-width SQL text expression for chunking a DATETIME column.
    #
    # Incremental snapshot chunks a table with ``WHERE key > <value>`` ordered by
    # that key. Informix DATETIME keyset literals do not round-trip for every
    # qualifier, so instead of comparing the DATETIME natively this renders the
    # value as its canonical zero-padded text and the connector compares strings.
    #
    # Keyset pagination is complete as long as the SAME expression drives
    # ``ORDER BY``, the ``WHERE`` bound, and the stored cursor, and the string
    # order is a deterministic total order that agrees between Informix and
    # Python. It does not need to be chronological. Within one column every value
    # shares the declared qualifier, so casting the column IN ITS OWN qualifier
    # range yields a fixed-width, zero-padded string (``YYYY-MM-DD HH:MM:SS.fffff``
    # truncated to the qualifier's fields). Fixed width makes byte order stable
    # across collations, so a DATETIME key column of any contiguous qualifier can
    # be chunked — including HOUR-anchored TIME-of-day and DATE-only ranges, and
    # such a column combined lexicographically with non-DATETIME key columns.
    #
    # ``qualifier`` uses the connector's decoded layout: start field in bits
    # 8..11 and end field in bits 0..3, with field codes 0=YEAR, 2=MONTH, 4=DAY,
    # 6=HOUR, 8=MINUTE, 10=SECOND, 11..15=FRACTION(1..5).
    #
    # Returns ``None`` for a malformed qualifier so the caller can fall back
    # rather than risk skipped or duplicated chunk rows.

    start, end = (qualifier >> 8) & 0xF, qualifier & 0xF
    field_starts = {0, 2, 4, 6, 8, 10}
    if start not in field_starts or end < start or end > 15:
        return None
    # Canonical text fields present for this qualifier: whole date/time fields
    # from start up to min(end, SECOND), plus an optional trailing FRACTION.
    fields = [code for code in (0, 2, 4, 6, 8, 10) if start <= code <= min(end, 10)]
    if end > 10:
        fields.append(end)
    if not fields:
        return None

    def _field_chars(code: int) -> int:
        if code == 0:
            return 4  # YEAR is rendered as four digits
        if code <= 10:
            return 2  # MONTH..SECOND are two digits each
        return code - 10  # FRACTION(k) is k digits

    # First field has no separator; each later field adds one separator char
    # ("-", " ", ":", or ".") plus its digits.
    width = _field_chars(fields[0]) + sum(1 + _field_chars(code) for code in fields[1:])
    return f"CAST({column} AS CHAR({width}))"


def _statement_keyword(sql: str) -> str:
    index = 0
    while True:
        while index < len(sql) and sql[index].isspace():
            index += 1
        if sql.startswith("--", index):
            newline = sql.find("\n", index + 2)
            if newline < 0:
                return ""
            index = newline + 1
            continue
        if sql.startswith("/*", index):
            end = sql.find("*/", index + 2)
            if end < 0:
                return ""
            index = end + 2
            continue
        break
    end = index
    while end < len(sql) and (sql[end].isalnum() or sql[end] == "_"):
        end += 1
    return sql[index:end].upper()


_NON_ROW_STATEMENTS = frozenset(
    {
        "ALTER",
        "BEGIN",
        "COMMIT",
        "CREATE",
        "DELETE",
        "DROP",
        "GRANT",
        "INSERT",
        "MERGE",
        "RENAME",
        "REVOKE",
        "ROLLBACK",
        "SET",
        "TRUNCATE",
        "UPDATE",
    }
)


def _decode_boolean_result(data: bytes) -> bool | None:
    # Docstring: A BOOLEAN result value: one marker byte, a null flag and the marker, or
    # either in a variable envelope.

    if len(data) >= 5:
        envelope_size = struct.unpack_from(">i", data, 1)[0]
        if envelope_size < 0 or envelope_size + 5 != len(data):
            raise SqliProtocolError("BOOLEAN envelope length does not match tuple column")
        null_marker = data[0]
        if null_marker == 1:
            if envelope_size != 0:
                raise SqliProtocolError("null BOOLEAN has a non-empty payload")
            return None
        if null_marker != 0:
            raise SqliProtocolError(f"invalid BOOLEAN envelope null marker {null_marker}")
        data = data[5:]
    if not data or len(data) > 2:
        raise SqliProtocolError(f"BOOLEAN result of {len(data)} bytes")
    if len(data) == 2:
        # A null flag, then the marker, as a change record holds one: read past, a NULL
        # (01 00) landed as False here and as NULL from the stream.
        if data[0] == 1:
            return None
        if data[0] != 0:
            raise SqliProtocolError(f"invalid BOOLEAN null flag {data[0]}")
    marker = struct.unpack_from(">b", data, len(data) - 1)[0]
    if marker == -1:
        return None
    if marker in {1, ord("t"), ord("T")}:
        return True
    if marker in {0, ord("f"), ord("F")}:
        return False
    raise SqliProtocolError(f"invalid BOOLEAN result marker {marker}")


def _datetime_qualifier(column: ResultColumn) -> int:
    # Docstring: A DATETIME result column's qualifier, start << 8 | end. Some descriptors
    # carry it in extended_id. Live Informix 15 ordinary SELECT descriptors instead use
    # extended_id=0 and the syscolumns.collength layout in encoded_length: packed width
    # in the high byte, qualifier nibbles in the low byte (0x130f for YEAR TO FRACTION(5)).

    if column.extended_id:
        return column.extended_id
    encoded = column.encoded_length & 0xFF
    return ((encoded >> 4) << 8) | (encoded & 0x0F)


# pylint: disable-next=too-many-branches
def _decode_result_value_raw(
    data: bytes, column: ResultColumn, encoding: str, pad_varchar: bool = False
) -> object:
    kind = _effective_result_kind(column)
    if kind in {0, 15}:  # CHAR, NCHAR
        # A NULL has no flag of its own: Informix sends it as a NUL followed by spaces
        # (probed on 14.10: 00 20 20 ... for a NULL CHAR(8)), where an empty value is
        # all spaces. Told here, so that it is a NULL under every string.null.byte mode
        # and not an empty string or a lone NUL.
        if column.encoded_length and data[:1] == b"\x00":
            return None
        return _decode_text(data[: column.encoded_length], encoding).rstrip(" ")
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
        # Length-prefixed, padded or not. With IFX_PAD_VARCHAR the server sends each
        # value in a slot of one length byte and the column's full width, the value
        # followed by spaces (probed on 14.10: 02 61 62 20 ... for 'ab' in a
        # VARCHAR(10), 00 20 ... for '', 01 00 00 ... for a NULL): the length byte says
        # where the value ends, so its own trailing spaces are kept and the padding is
        # not. Without it the slot ends with the value.
        if not data:
            raise SqliProtocolError("truncated variable VARCHAR")
        size = data[0]
        if size + 1 > len(data):
            raise SqliProtocolError("VARCHAR length exceeds tuple column")
        if size == 1 and data[1] == 0:
            return None
        return _decode_text(data[1 : size + 1], encoding)
    if kind == 43:
        if len(data) < 5:
            raise SqliProtocolError("truncated variable LVARCHAR")
        marker = data[0]
        size = struct.unpack_from(">i", data, 1)[0]
        if size < 0 or size + 5 != len(data):
            raise SqliProtocolError("LVARCHAR length does not match tuple column")
        if marker == 1:
            if size != 0:
                raise SqliProtocolError("null LVARCHAR has a non-empty payload")
            return None
        if marker != 0:
            raise SqliProtocolError(f"invalid LVARCHAR null marker {marker}")
        return _decode_text(data[5:], encoding)
    if kind in {17, 18}:
        type_name = "INT8" if kind == 17 else "SERIAL8"
        value, consumed = decode_value(memoryview(data), ColumnDescriptor(column.name, type_name))
        if consumed != 10:
            raise SqliProtocolError(f"{type_name} decoder consumed {consumed} bytes")
        return value
    if kind in {52, 53}:
        value = struct.unpack(">q", data[:8])[0]
        return None if value == -(1 << 63) else value
    if kind == 10:
        # In the CDC extended-id layout the shared DATETIME decoder expects.
        value, _ = decode_value(
            memoryview(data),
            ColumnDescriptor("datetime", "DATETIME", length=_datetime_qualifier(column)),
        )
        return value
    if kind == 14:
        return decode_interval(memoryview(data), column.encoded_length)[0]
    if kind == 45:
        return _decode_boolean_result(data)
    raise SqliDescriptorNotImplemented(f"ordinary Informix result type {kind} is unsupported")


def _decode_result_value(
    data: bytes,
    column: ResultColumn,
    encoding: str,
    pad_varchar: bool = False,
    null_byte: str = "keep",
) -> object:
    # The snapshot/query path decodes CHAR/VARCHAR/LVARCHAR itself (below), so apply the
    # NUL sanitizer here as the CDC path does in decode_value. Numeric/metadata results
    # are unaffected -- apply_null_byte only touches strings that contain a NUL.
    try:
        raw = _decode_result_value_raw(data, column, encoding, pad_varchar)
    except (ValueError, ArithmeticError) as error:
        # The shared decoders' CdcProtocolError, a DATE past the calendar, a DECIMAL past
        # its precision: a value that isn't what its descriptor says, so the session's.
        raise SqliProtocolError(f"cannot decode result column {column.name!r}: {error}") from error
    return apply_null_byte(raw, null_byte)


def _fixed_result_size(column: ResultColumn) -> int:
    kind = column.type_code & 0xFF
    widths = {
        1: 2,
        2: 4,
        3: 8,
        4: 4,
        6: 4,
        7: 4,
        17: 10,
        18: 10,
        52: 8,
        53: 8,
    }
    if kind in {0, 15}:
        return column.encoded_length
    if kind in widths:
        return widths[kind]
    if kind in {5, 8}:
        precision, scale = (column.encoded_length >> 8) & 0xFF, column.encoded_length & 0xFF
        return (precision + (scale & 1) + 3) // 2
    if kind == 10:
        # By the qualifier, as the decoder reads it: where extended_id carries that, the
        # low byte of encoded_length is not the fraction's width, and the slice was a
        # byte longer than the value (HOUR TO SECOND as 5 bytes, not 4).
        return datetime_size(_datetime_qualifier(column))
    if kind == 14:
        layout = interval_layout(column.encoded_length)
        if layout is None:
            raise SqliProtocolError(f"invalid INTERVAL qualifier 0x{column.encoded_length:04x}")
        total = (column.encoded_length >> 8) & 0xFF
        return (total + (layout[2] & 1) + 3) // 2
    if kind == 45:
        if column.encoded_length not in {1, 2}:
            raise SqliProtocolError(
                f"BOOLEAN result width must be 1 or 2, got {column.encoded_length}"
            )
        return column.encoded_length
    raise SqliDescriptorNotImplemented(f"ordinary Informix result type {kind} is unsupported")


# How long a TLS connection waits before its first and second retry of reading
# ssl.ca.file (twice this, the second time).
_CA_FILE_RETRY_SECONDS = 0.5
# The FUSE mounts an ssl.ca.file can be read through, where a dropped mount can hide a file.
_FUSE_PATH_PREFIXES = ("/Volumes/", "/Workspace/", "/dbfs/")


def _ca_file_misnamed(error: OSError, path: str | None) -> bool:
    # Docstring: Whether ``error`` reading ssl.ca.file says the path names no file, which
    # no retry finds: a directory, or a file missing off a FUSE mount. Wrapped as a
    # transport loss, each reconnect loop retried a typo for half a minute.

    if isinstance(error, (IsADirectoryError, NotADirectoryError)):
        return True
    return isinstance(error, FileNotFoundError) and not str(path or "").startswith(
        _FUSE_PATH_PREFIXES
    )
