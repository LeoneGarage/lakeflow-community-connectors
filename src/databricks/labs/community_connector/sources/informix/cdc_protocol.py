"""Verified Informix change-stream framing and primitive codecs.

Layouts are ported from IBM ifx-changestream-client 1.1.5 and JDBC bytecode.
The supported packed DECIMAL/MONEY, INT8/SERIAL8, and qualifier-aware DATETIME
paths are implemented below. Types omitted by IBM's native capture-column API
(including INTERVAL, LOB, and complex/opaque values) remain explicit exclusions;
the source API supplies no row payload for a Python decoder to reconstruct.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, localcontext
from typing import Any, Iterable, Sequence

FIXED_HEADER = 16
RECORD_TYPES = {
    1: "BEGIN",
    2: "COMMIT",
    3: "ROLLBACK",
    40: "INSERT",
    41: "DELETE",
    42: "BEFORE_UPDATE",
    43: "AFTER_UPDATE",
    62: "DISCARD",
    119: "TRUNCATE",
    200: "METADATA",
    201: "TIMEOUT",
    202: "ERROR",
}
_OP_TYPES = {40, 41, 42, 43}
CDC_ROUTINE_DATABASE = "syscdcv1"


def cdc_routine(name: str) -> str:
    """Return the cross-database name of an installed Informix CDC routine."""

    if not name.startswith("cdc_") or not name.replace("_", "").isalnum():
        raise CdcProtocolError(f"invalid CDC routine name {name!r}")
    return f"{CDC_ROUTINE_DATABASE}:informix.{name}"


def metadata_column_names(payload: bytes, encoding: str = "utf-8") -> tuple[str, ...]:
    """Extract CDC metadata column names while ignoring commas in type arguments."""

    text = payload.rstrip(b"\0 \t\r\n").decode(encoding)
    fields: list[str] = []
    start = depth = 0
    for index, character in enumerate(text):
        if character == "(":
            depth += 1
        elif character == ")":
            if depth == 0:
                raise CdcProtocolError("unbalanced CDC metadata type")
            depth -= 1
        elif character == "," and depth == 0:
            fields.append(text[start:index].strip())
            start = index + 1
    if depth:
        raise CdcProtocolError("unbalanced CDC metadata type")
    fields.append(text[start:].strip())
    names = tuple(field.split(None, 1)[0] for field in fields if field)
    if not names or len(names) != len(set(names)):
        raise CdcProtocolError("CDC metadata has missing or duplicate column names")
    return names


class CdcProtocolError(ValueError):
    pass


class UnsupportedCdcType(CdcProtocolError):
    pass


class OpenTransactionRecords:
    """Count only data records still held by currently open transactions."""

    def __init__(self) -> None:
        self._sequences: dict[int, list[int]] = {}

    def __bool__(self) -> bool:
        return bool(self._sequences)

    @property
    def buffered(self) -> int:
        return sum(len(values) for values in self._sequences.values())

    def begin(self, tx_id: int) -> None:
        if tx_id in self._sequences:
            raise CdcProtocolError(f"duplicate CDC BEGIN for transaction {tx_id}")
        self._sequences[tx_id] = []

    def append(self, tx_id: int, lsn: int) -> None:
        if tx_id not in self._sequences:
            raise CdcProtocolError(f"CDC data for unknown transaction {tx_id}")
        self._sequences[tx_id].append(lsn)

    def discard(self, tx_id: int, lsn: int) -> None:
        if tx_id not in self._sequences:
            raise CdcProtocolError(f"CDC DISCARD for unknown transaction {tx_id}")
        self._sequences[tx_id] = [value for value in self._sequences[tx_id] if value < lsn]

    def finish(self, tx_id: int) -> None:
        if tx_id not in self._sequences:
            raise CdcProtocolError(f"CDC completion for unknown transaction {tx_id}")
        del self._sequences[tx_id]


def validate_snapshot_arity(after: Sequence[Any] | None, primary_keys: Sequence[str]) -> None:
    if after is not None and len(after) != len(primary_keys):
        raise CdcProtocolError("Snapshot primary-key offset has the wrong arity")


@dataclass(frozen=True)
class ColumnDescriptor:
    name: str
    type_name: str
    length: int = 0
    precision: int | None = None
    scale: int | None = None
    encoding: str = "utf-8"


class CdcFrameParser:
    """Incremental bounded parser preserving partial frames between reads."""

    def __init__(self, max_frame_bytes: int = 16 * 1024 * 1024) -> None:
        if max_frame_bytes < FIXED_HEADER:
            raise ValueError("max_frame_bytes must be at least 16")
        self.max_frame_bytes = max_frame_bytes
        self._buffer = bytearray()

    @property
    def buffered_bytes(self) -> int:
        return len(self._buffer)

    def feed(self, data: bytes | bytearray | memoryview) -> list[bytes]:
        self._buffer.extend(data)
        frames = []
        while len(self._buffer) >= FIXED_HEADER:
            header_size, payload_size = struct.unpack_from(">II", self._buffer)
            if header_size < FIXED_HEADER:
                raise CdcProtocolError(f"invalid CDC header size {header_size}")
            total = header_size + payload_size
            if total < header_size or total > self.max_frame_bytes:
                raise CdcProtocolError(f"CDC frame size {total} exceeds configured bound")
            if len(self._buffer) < total:
                break
            frames.append(bytes(self._buffer[:total]))
            del self._buffer[:total]
        if len(self._buffer) > self.max_frame_bytes:
            raise CdcProtocolError("unterminated CDC frame exceeds configured bound")
        return frames


def decode_frame(frame: bytes, columns_by_label: dict[int, tuple[ColumnDescriptor, ...]]) -> dict:
    if len(frame) < FIXED_HEADER:
        raise CdcProtocolError("truncated CDC fixed header")
    header_size, payload_size, reserved, kind = struct.unpack_from(">IIII", frame)
    total = header_size + payload_size
    if header_size < FIXED_HEADER or total != len(frame):
        raise CdcProtocolError("CDC frame length does not match header")
    op = RECORD_TYPES.get(kind)
    if op is None:
        raise CdcProtocolError(f"unknown CDC record type {kind}")
    header = memoryview(frame)[FIXED_HEADER:header_size]
    payload = memoryview(frame)[header_size:]
    record: dict[str, Any] = {"op": op, "reserved": reserved}
    if kind in _OP_TYPES:
        _require_header(header, 16, op)
        lsn, tx_id, label = struct.unpack_from(">Qii", header)
        record.update(lsn=lsn, tx_id=tx_id, label=label)
        columns = columns_by_label.get(label)
        if columns is None:
            raise CdcProtocolError(f"operation references unknown capture label {label}")
        record["row"] = decode_row(payload, columns)
    elif kind == 1:
        _require_header(header, 24, op)
        lsn, tx_id, timestamp, user_id = struct.unpack_from(">Qiqi", header)
        record.update(lsn=lsn, tx_id=tx_id, timestamp=timestamp, user_id=user_id)
    elif kind == 2:
        _require_header(header, 20, op)
        lsn, tx_id, timestamp = struct.unpack_from(">Qiq", header)
        record.update(lsn=lsn, tx_id=tx_id, timestamp=timestamp)
    elif kind in {3, 62}:
        _require_header(header, 12, op)
        lsn, tx_id = struct.unpack_from(">Qi", header)
        record.update(lsn=lsn, tx_id=tx_id)
    elif kind == 119:
        _require_header(header, 16, op)
        lsn, tx_id, label = struct.unpack_from(">Qii", header)
        record.update(lsn=lsn, tx_id=tx_id, user_id=label, capture_label=label)
    elif kind == 200:
        _require_header(header, 4, op)
        record.update(label=struct.unpack_from(">i", header)[0], metadata=bytes(payload))
    elif kind == 201:
        _require_header(header, 8, op)
        record["lsn"] = struct.unpack_from(">Q", header)[0]
    elif kind == 202:
        _require_header(header, 8, op)
        record["flags"], record["error"] = struct.unpack_from(">ii", header)
    if payload and kind not in _OP_TYPES and kind != 200:
        record["payload"] = bytes(payload)
    return record


def decode_row(payload: memoryview, columns: Iterable[ColumnDescriptor]) -> dict[str, Any]:
    offset, row = 0, {}
    for column in columns:
        try:
            value, consumed = decode_value(payload[offset:], column)
        except CdcProtocolError as error:
            raise CdcProtocolError(
                f"failed to decode CDC column {column.name!r} at payload offset {offset}: {error}"
            ) from error
        if consumed <= 0 or offset + consumed > len(payload):
            raise CdcProtocolError(f"invalid width for CDC column {column.name}")
        row[column.name] = value
        offset += consumed
    if offset != len(payload):
        raise CdcProtocolError(f"CDC row has {len(payload) - offset} trailing bytes")
    return row


def decode_value(data: memoryview, column: ColumnDescriptor) -> tuple[Any, int]:
    kind = column.type_name.upper().split("(", 1)[0].strip()
    if kind in {"SMALLINT", "INT2"}:
        value = _unpack(">h", data)
        return (None if value == -(1 << 15) else value), 2
    if kind in {"INTEGER", "INT", "SERIAL"}:
        value = _unpack(">i", data)
        return (None if value == -(1 << 31) else value), 4
    if kind in {"BIGINT", "BIGSERIAL"}:
        value = _unpack(">q", data)
        return (None if value == -(1 << 63) else value), 8
    if kind == "DATE":
        value = _unpack(">i", data)
        if value == -(1 << 31):
            return None, 4
        # Informix day zero is 1899-12-31. JDBC's internal conversion adds
        # 693594 to a zero-based Julian-day value; Python ordinals are one-based.
        return date(1, 1, 1) + timedelta(days=value + 693594), 4
    if kind in {"FLOAT", "DOUBLE", "DOUBLE PRECISION"}:
        _need(data, 8)
        if bytes(data[:8]) == b"\xff" * 8:
            return None, 8
        value = _unpack(">d", data)
        return value, 8
    if kind in {"REAL", "SMALLFLOAT"}:
        _need(data, 4)
        if bytes(data[:4]) == b"\xff" * 4:
            return None, 4
        value = _unpack(">f", data)
        return value, 4
    if kind in {"BOOLEAN", "BOOL"}:
        _need(data, 2)
        null_flag = data[0]
        marker = struct.unpack_from(">b", data, 1)[0]
        if null_flag == 1:
            return None, 2
        if null_flag != 0:
            raise CdcProtocolError(f"invalid boolean null flag {null_flag}")
        if marker not in {0, 1}:
            raise CdcProtocolError(f"invalid boolean marker {marker}")
        return bool(marker), 2
    if kind in {"CHAR", "NCHAR"}:
        _need(data, column.length)
        return bytes(data[: column.length]).decode(column.encoding).rstrip(" "), column.length
    if kind in {"VARCHAR", "NVARCHAR"}:
        _need(data, 1)
        length = int(data[0])
        if length == 1:
            _need(data, 2)
            if data[1] == 0:
                return None, 2
        _need(data, length + 1)
        return bytes(data[1 : length + 1]).decode(column.encoding), length + 1
    if kind == "LVARCHAR":
        _need(data, 3)
        length = struct.unpack_from(">h", data)[0]
        if length < 0:
            raise CdcProtocolError(f"invalid LVARCHAR length {length}")
        _need(data, length + 2)
        if length == 1 and data[2] == 1:
            return None, 3
        if length == 1 and data[2] == 0:
            return "", 3
        return bytes(data[3 : length + 2]).decode(column.encoding), length + 2
    if kind in {"DECIMAL", "NUMERIC", "MONEY"}:
        if column.precision is None or column.scale is None:
            raise UnsupportedCdcType(f"{kind} requires precision and scale metadata")
        size = (column.precision + (column.scale & 1) + 3) // 2
        _need(data, size)
        return decode_packed_decimal(bytes(data[:size]), column.precision, column.scale), size
    if kind in {"INT8", "SERIAL8"}:
        _need(data, 10)
        sign = struct.unpack_from(">h", data)[0]
        if sign == 0:
            return None, 10
        if sign not in {-1, 1}:
            raise CdcProtocolError(f"invalid {kind} sign word {sign}")
        low = struct.unpack_from(">I", data, 2)[0]
        high = struct.unpack_from(">I", data, 6)[0]
        magnitude = (high << 32) | low
        if magnitude > 1 << 63 or (sign == 1 and magnitude == 1 << 63):
            raise CdcProtocolError(f"{kind} magnitude exceeds signed int64")
        return (-magnitude if sign == -1 else magnitude), 10
    if kind == "DATETIME":
        return _decode_datetime(data, column)
    if kind in {"BYTE", "TEXT", "BLOB", "CLOB", "INTERVAL", "UDT", "ROW", "SET", "LIST"}:
        raise UnsupportedCdcType(f"{kind} is not supported by the IBM capture-column path")
    raise UnsupportedCdcType(f"unsupported CDC type {column.type_name}")


def _unpack(fmt: str, data: memoryview):
    size = struct.calcsize(fmt)
    _need(data, size)
    return struct.unpack_from(fmt, data)[0]


def decode_packed_decimal(data: bytes, precision: int, scale: int) -> Decimal | None:
    if not 1 <= precision <= 38 or not 0 <= scale <= precision:
        raise UnsupportedCdcType(f"invalid DECIMAL({precision},{scale}) metadata")
    expected = (precision + (scale & 1) + 3) // 2
    if len(data) != expected:
        raise CdcProtocolError(f"packed decimal is {len(data)} bytes, expected {expected}")
    if data[:2] == b"\0\0":
        return None
    positive = bool(data[0] & 0x80)
    header = data[0] if positive else data[0] ^ 0x7F
    exponent = (header & 0x7F) - 64
    digits = list(data[1:])
    if any(digit > 99 for digit in digits):
        raise CdcProtocolError("packed decimal contains a base-100 digit above 99")
    if not positive:
        carrying = False
        for index in range(len(digits) - 1, -1, -1):
            digit = digits[index]
            if not carrying and digit == 0:
                continue
            digits[index] = (100 if not carrying else 99) - digit
            carrying = True
    with localcontext() as context:
        context.prec = max(precision + 8, 46)
        value = sum(
            Decimal(digit) * (Decimal(100) ** (exponent - index))
            for index, digit in enumerate(digits)
        )
        if not positive:
            value = -value
        return value.quantize(Decimal(1).scaleb(-scale))


def _decode_datetime(
    data: memoryview, column: ColumnDescriptor
) -> tuple[datetime | str | None, int]:
    qualifier = column.length
    start, end = (qualifier >> 8) & 0xF, qualifier & 0xF
    codes = (0, 2, 4, 6, 8, 10)
    if start not in codes or end not in {*codes, 11, 12, 13, 14, 15} or end < start:
        raise UnsupportedCdcType(f"unsupported DATETIME qualifier {start} TO {end}")
    selected = [code for code in codes if start <= code <= min(end, 10)]
    widths = {0: 4, 2: 2, 4: 2, 6: 2, 8: 2, 10: 2}
    fraction_digits = max(0, end - 10)
    digit_count = sum(widths[code] for code in selected) + fraction_digits
    size = 1 + (digit_count + 1) // 2
    _need(data, size)
    raw = bytes(data[:size])
    if raw[:2] == b"\0\0":
        return None, size
    if not raw[0] & 0x80:
        raise CdcProtocolError("DATETIME packed value must be non-negative")
    groups = raw[1:]
    if any(group > 99 for group in groups):
        raise CdcProtocolError("DATETIME contains a base-100 group above 99")
    digits = "".join(f"{group:02d}" for group in groups)[:digit_count]
    values, position = {}, 0
    for code in selected:
        width = widths[code]
        values[code] = int(digits[position : position + width])
        position += width
    fraction = digits[position:]
    if {0, 2, 4}.issubset(values):
        try:
            return datetime(
                values[0],
                values[2],
                values[4],
                values.get(6, 0),
                values.get(8, 0),
                values.get(10, 0),
                int((fraction + "000000")[:6] or 0),
            ), size
        except ValueError as exc:
            raise CdcProtocolError(f"invalid DATETIME calendar fields {digits}") from exc
    labels = {0: "YEAR", 2: "MONTH", 4: "DAY", 6: "HOUR", 8: "MINUTE", 10: "SECOND"}
    rendered = ":".join(f"{labels[code]}={values[code]:0{widths[code]}d}" for code in selected)
    if fraction:
        rendered += f":FRACTION({fraction_digits})={fraction}"
    return rendered, size


def _need(data: memoryview, size: int) -> None:
    if size < 0 or len(data) < size:
        raise CdcProtocolError(f"truncated CDC value: need {size}, have {len(data)}")


def _require_header(header: memoryview, size: int, op: str) -> None:
    # IBM's record classes consume only the known prefix from ByteBuffer and
    # deliberately tolerate server-version-specific header extensions.
    if len(header) < size:
        raise CdcProtocolError(f"{op} header is {len(header)} bytes, expected at least {size}")


__all__ = [
    "CdcFrameParser",
    "CdcProtocolError",
    "ColumnDescriptor",
    "OpenTransactionRecords",
    "UnsupportedCdcType",
    "decode_frame",
    "metadata_column_names",
    "decode_packed_decimal",
    "decode_row",
    "decode_value",
    "validate_snapshot_arity",
]
