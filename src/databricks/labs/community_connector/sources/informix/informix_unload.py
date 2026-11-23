"""Snapshot sources: staged pages, .unl extract records and splits, and schema widening."""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import math
import os
import sys
import zlib
from datetime import date, datetime
from decimal import Decimal, localcontext
from typing import Any, Callable, Iterator

from pyspark.sql.types import DecimalType, DoubleType, FloatType, IntegerType, LongType

from databricks.labs.community_connector.sources.informix.cdc_protocol import (
    apply_null_byte,
    datetime_from_text,
)
from databricks.labs.community_connector.sources.informix.informix_filters import (
    _INTEGER_RANKS,
    _float32,
    _parse_row_filter,
    _row_filter_kind,
    _unsupported_column,
)
from databricks.labs.community_connector.sources.informix.informix_host_sharing import (
    _FLOAT_KEY_TYPES,
    _VARIABLE_DECIMAL_DEFAULT_TARGET,
    _parse_unload_fields,
    _unload_float,
    _UnloadEmpty,
    _variable_decimal_text,
)
from databricks.labs.community_connector.sources.informix.informix_keys import (
    _DEFAULT_NULL_BYTE_MODE,
    _FLOAT_KINDS,
    _coerce_variable_decimal_values,
    _framework_row,
    _is_variable_scale_decimal,
)
from databricks.labs.community_connector.sources.informix.informix_options import (
    _OFFSET_VERSION,
    _SNAPSHOT_SOURCE_OPTION,
    _UNL_DEFAULT_DELIMITER,
    _UNL_DEFAULT_ESCAPE,
    _UNL_ESCAPE_OPTION,
    _UNL_SCAN_CHUNK_BYTES,
    _UNL_SPLIT_TARGET_BYTES,
    _UNLOAD_FLOAT_TYPES,
    _UNLOAD_INT_TYPES,
    COMMIT_LSN,
    COMMIT_TIME,
    CURSOR,
    OP,
    TX_ID,
    InformixError,
    _m,
)
from databricks.labs.community_connector.sources.informix.informix_registration import Column, Table
from databricks.labs.community_connector.sources.informix.informix_shards import _schema_fingerprint
from databricks.labs.community_connector.sources.informix.informix_transactions import (
    _commit_time_value,
    _refuse_null_key,
    _row_filter_matches,
    _sortable_lsn,
    _strict_lsn,
)
from databricks.labs.community_connector.sources.informix.lakebase_workspace import (
    WorkspaceFilesApi,
    WorkspaceHttpError,
)


def _unload_value(
    text: str, type_name: str, qualifier: int | None = None, scale: int | None = None
) -> Any:
    # Docstring: Convert one UNLOAD field to the Python value the live snapshot would yield.
    #
    # Keyed on the column's Informix ``type_name`` so ``_shape_snapshot`` (which
    # isoformats date/datetime and quantizes decimals) then produces a row
    # byte-identical to a live ``consistent_snapshot`` row. An empty field is NULL.
    # ``qualifier`` is a DATETIME column's qualifier (``Column.length``): with it, every
    # qualifier renders as the live and CDC decoders render it. Exotic/binary types and
    # locale-sensitive date formats are best-effort and must be validated against the
    # count/data-compare tool in live mode. ``scale`` is a DECIMAL or MONEY column's: UNLOAD
    # writes a DECIMAL(12,2) 7.00 as 7.0, which is quantized back to the column's scale.

    if text == "":
        return None
    upper = type_name.upper()
    if isinstance(text, _UnloadEmpty) and upper in {"VARCHAR", "NVARCHAR", "LVARCHAR"}:
        # Read as the space, an empty value landed as a blank, and as a key it then
        # matched none of the row's later changes, which carry the empty string.
        return ""
    if upper in {"CHAR", "NCHAR"}:
        # The live decoders drop CHAR's blank padding; an extract may keep it.
        return text.rstrip(" ")
    if upper == "DATETIME" and qualifier is not None:
        return datetime_from_text(text, qualifier)
    if upper in _UNLOAD_INT_TYPES:
        return int(text)
    if upper.startswith(("DECIMAL", "NUMERIC", "MONEY")):
        value = Decimal(text)
        if not value.is_finite():
            # 470: UNLOAD value {!r} in snapshot.source is not a finite number, which no
            # Informix {} column holds
            raise InformixError(_m(470, text, type_name))
        if scale is None or scale == 0xFF:
            return value
        with localcontext() as decimal_context:
            # Not the default context's 28 digits, which a DECIMAL(32) passes.
            decimal_context.prec = 80
            return value.quantize(Decimal(1).scaleb(-scale))
    if upper in _UNLOAD_FLOAT_TYPES:
        return _unload_float(text, type_name, upper in {"SMALLFLOAT", "REAL"})
    if upper == "DATE":
        try:
            return date.fromisoformat(text)
        except ValueError:
            return datetime.strptime(text, "%m/%d/%Y").date()
    if upper.startswith("DATETIME"):
        normalized = text.replace("T", " ")
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(normalized, fmt)
            except ValueError:
                continue
        return datetime.fromisoformat(text)
    if upper == "BOOLEAN":
        # Anything else is no BOOLEAN: a misaligned field must fail, not read as false.
        flag = text.strip().lower()
        if flag in {"t", "true", "1", "y"}:
            return True
        if flag in {"f", "false", "0", "n"}:
            return False
        raise ValueError(f"invalid BOOLEAN value {text!r}")
    if upper == "INTERVAL":
        # UNLOAD right-aligns an INTERVAL in its field width ("  -5 10:30:00"); the
        # trimmed text is exactly what the snapshot and CDC decoders produce.
        return text.strip()
    return text


_UNL_DEFAULT_ESCAPE_BYTE = b"\\"


def _unl_escape_byte(escape: str, encoding: str) -> bytes:
    # Docstring: Encode the record escape character for the byte-level record splitter.
    #
    # The splitter scans raw bytes for record terminators, so the escape must be a
    # single byte in the extract's encoding (backslash, or an HPL job's ASCII choice).
    # An empty escape (escaping disabled) yields ``b""``: every physical newline then
    # terminates a record.

    if not escape:
        return b""
    encoded = escape.encode(encoding)
    if len(encoded) != 1:
        # 361: Option '{}' must encode to a single byte in {!r}; got {!r}
        raise InformixError(_m(361, _UNL_ESCAPE_OPTION, encoding, escape))
    return encoded


def _unl_escape_run(data: bytes, index: int, marker: int) -> int:
    # Docstring: Count the consecutive ``marker`` bytes immediately before ``data[index]``.

    run = 0
    position = index - 1
    while position >= 0 and data[position] == marker:
        run += 1
        position -= 1
    return run


def _unl_find_record_end(data: bytes, start: int, escape: bytes) -> int:
    # Docstring: Index of the first *unescaped* ``\\n`` in ``data`` at or after ``start``, else -1.
    #
    # UNLOAD writes a newline embedded in a CHAR/VARCHAR value as the escape followed
    # by a literal newline, and a literal escape as the escape doubled, so a newline
    # preceded by an odd run of escape bytes is data while an even run (including
    # none) terminates the record. ``data`` must begin at a record start: the byte
    # before any record is an unescaped newline (or the file start), so a run can never
    # extend before it.

    if not escape:
        return data.find(b"\n", start)
    marker = escape[0]
    while True:
        index = data.find(b"\n", start)
        if index == -1 or _unl_escape_run(data, index, marker) % 2 == 0:
            return index
        start = index + 1


def _unl_split_records(buffer: bytes, escape: bytes) -> tuple[list[bytes], bytes]:
    # Docstring: Split ``buffer`` (which starts at a record start) into its complete records and
    # the trailing partial record still waiting for its terminator.

    if not escape or escape not in buffer:
        # No escape byte anywhere: every newline terminates a record (C-speed split).
        parts = buffer.split(b"\n")
        return parts[:-1], parts[-1]
    records: list[bytes] = []
    cursor = 0
    while True:
        index = _unl_find_record_end(buffer, cursor, escape)
        if index == -1:
            return records, buffer[cursor:]
        records.append(buffer[cursor:index])
        cursor = index + 1


def _unl_escape_run_before(
    read_chunk: Callable[[int, int], bytes], position: int, marker: int
) -> int:
    # Docstring: Count the consecutive ``marker`` bytes ending right before ``position``, reading
    # backwards in small windows (a run is normally zero or one byte long).

    run = 0
    end = position
    window = 64
    while end > 0:
        start = max(0, end - window)
        data = read_chunk(start, end - start)
        if not data:
            break
        trailing = _unl_escape_run(data, len(data), marker)
        run += trailing
        if trailing < len(data):
            break
        end = start
        window = min(window * 2, _UNL_SCAN_CHUNK_BYTES)
    return run


def _unl_next_record_boundary(
    read_chunk: Callable[[int, int], bytes],
    target: int,
    size: int,
    chunk_bytes: int,
    escape: bytes = _UNL_DEFAULT_ESCAPE_BYTE,
) -> int:
    # Docstring: Return the offset just after the first unescaped ``\\n`` at or after ``target``.
    #
    # Reads forward from ``target`` in ``chunk_bytes`` windows (never the whole file) and
    # returns ``size`` when no terminator remains before EOF, so the caller runs the
    # final split to the end of the file. A candidate newline's escape run may begin
    # before the window -- before ``target`` itself when the newline is its first byte
    # -- so a run that reaches the window start is extended by reading backwards.

    marker = escape[0] if escape else None
    offset = target
    while offset < size:
        chunk = read_chunk(offset, min(chunk_bytes, size - offset))
        if not chunk:
            break
        search = 0
        while True:
            index = chunk.find(b"\n", search)
            if index == -1:
                break
            if marker is not None:
                run = _unl_escape_run(chunk, index, marker)
                if run == index:
                    run += _unl_escape_run_before(read_chunk, offset, marker)
                if run % 2:
                    search = index + 1
                    continue
            return offset + index + 1
        offset += len(chunk)
    return size


def _unl_byte_splits(
    read_chunk: Callable[[int, int], bytes],
    size: int,
    *,
    target_bytes: int = _UNL_SPLIT_TARGET_BYTES,
    chunk_bytes: int = _UNL_SCAN_CHUNK_BYTES,
    escape: bytes = _UNL_DEFAULT_ESCAPE_BYTE,
) -> list[tuple[int, int]]:
    # Docstring: Index one `.unl` file into ``[start, end)`` byte splits of ~``target_bytes`` each.
    #
    # Cheap to plan and constant-memory: for each split, seek to ``start + target_bytes``
    # and read forward via ``read_chunk`` only until the next record terminator, cutting
    # the split just after it (the next split resumes there). This reads a small window
    # near each cut point rather than the whole file -- the reason byte splitting is
    # preferred over a row cap, which cannot know row counts without reading every byte.
    #
    # A record terminator is an *unescaped* physical ``\\n``: UNLOAD escapes a newline
    # embedded in a value as ``escape`` + newline, so a newline behind an odd run of
    # escape bytes is data (see _unl_find_record_end). ``_iter_unl_records`` splits the
    # same range on the same rule, so ending a split right after a terminator keeps
    # records from spanning a boundary. A trailing unterminated record is included in the
    # final split; a split whose target lands past the last terminator runs to EOF. An
    # empty file yields no splits.

    target_bytes = max(1, target_bytes)
    splits: list[tuple[int, int]] = []
    split_start = 0
    while split_start < size:
        target = split_start + target_bytes
        if target >= size:
            splits.append((split_start, size))
            break
        boundary = _unl_next_record_boundary(read_chunk, target, size, chunk_bytes, escape)
        if boundary >= size:
            splits.append((split_start, size))
            break
        splits.append((split_start, boundary))
        split_start = boundary
    return splits


def _iter_unl_records(
    read_chunk: Callable[[int, int], bytes],
    start: int,
    end: int,
    *,
    chunk_bytes: int = _UNL_SCAN_CHUNK_BYTES,
    escape: bytes = _UNL_DEFAULT_ESCAPE_BYTE,
    path: str = "",
) -> Iterator[bytes]:
    # Docstring: Yield the raw record bytes (no trailing newline) whose bytes lie in ``[start,
    # end)``.
    #
    # Splits produced by :func:`_unl_byte_splits` begin at a record start and end right
    # after a record terminator, so this reads the range and splits it on unescaped
    # ``\\n`` (see _unl_find_record_end); an escaped newline stays inside its record.
    # Streaming/constant-memory. A final unterminated record (end of file) is yielded.
    # A file that ends before ``end`` fails (479): it is shorter than it was planned at.

    offset = start
    buffer = b""
    while offset < end:
        chunk = read_chunk(offset, min(chunk_bytes, end - offset))
        if not chunk:
            # 479: .unl snapshot file {} ended at byte {}, short of the {} planned; the extract
            # changed under a running snapshot -- run a full refresh
            raise InformixError(_m(479, path, offset, end))
        offset += len(chunk)
        buffer += chunk
        records, buffer = _unl_split_records(buffer, escape)
        yield from records
    if buffer:
        yield buffer


def _unl_file_is_gzip(read_chunk: Callable[[int, int], bytes]) -> bool:
    # Docstring: Whether a `.unl` file is gzip-compressed, by its two magic bytes ``1f 8b``.
    #
    # Authoritative over the file name, so a mislabelled extract is still handled
    # correctly. An empty file (no bytes) is not gzip.

    return read_chunk(0, 2) == b"\x1f\x8b"


def _iter_gzip_unl_records(
    read_chunk: Callable[[int, int], bytes],
    size: int,
    *,
    chunk_bytes: int = _UNL_SCAN_CHUNK_BYTES,
    escape: bytes = _UNL_DEFAULT_ESCAPE_BYTE,
    path: str = "",
) -> Iterator[bytes]:
    # Docstring: Yield raw record bytes from a gzip-compressed `.unl` file (whole file, streaming).
    #
    # Gzip is not seekable, so a compressed file is one non-splittable unit: read the
    # compressed bytes ``[0, size)`` in ``chunk_bytes`` windows, decompress incrementally,
    # split the decompressed stream on unescaped ``\\n`` (see _unl_find_record_end), and
    # yield records. Constant memory -- only one compressed window, the current
    # decompressed run, and the trailing partial record are held. Concatenated gzip
    # members (``cat a.gz b.gz``) are handled by starting a fresh decompressor from each
    # member's trailing ``unused_data``; zero bytes after a member are padding, as gzip
    # takes them. A final unterminated record is yielded. An empty extract (gzip of no
    # rows) yields nothing. A file that ends inside a member fails (478): zlib checks a
    # member's CRC and length only at its trailer, and gives a cut one's rows without error.

    decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
    offset = 0
    tail = b""
    in_member = False
    while offset < size:
        compressed = read_chunk(offset, min(chunk_bytes, size - offset))
        if not compressed:
            # 479: .unl snapshot file {} ended at byte {}, short of the {} planned; the extract
            # changed under a running snapshot -- run a full refresh
            raise InformixError(_m(479, path, offset, size))
        offset += len(compressed)
        while compressed:
            if not in_member:
                compressed = compressed.lstrip(b"\0")
                in_member = bool(compressed)
                if not in_member:
                    break
            tail += decompressor.decompress(compressed)
            if not decompressor.eof:
                break
            # Member finished; its leftover bytes begin the next concatenated member.
            compressed = decompressor.unused_data
            decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
            in_member = False
        records, tail = _unl_split_records(tail, escape)
        yield from records
    if in_member:
        # 478: gzip snapshot file {} ends inside a compressed member, before its trailer: the
        # file is cut short (an incomplete copy or upload); replace it with the whole extract
        raise InformixError(_m(478, path))
    records, tail = _unl_split_records(tail, escape)
    yield from records
    if tail:
        yield tail


class _StaticStagingCredential:
    # Docstring: Fixed-bearer credential for a ``WorkspaceFilesApi`` rebuilt on an executor.
    #
    # The driver mints and refreshes the real staging token; an executor receives just
    # the bearer for one batch in its partition descriptor, so it needs no minting or
    # refresh -- only ``bearer()``, which ``WorkspaceFilesApi`` calls per request.

    def __init__(self, token: str) -> None:
        self._token = token

    def bearer(self) -> str:
        return self._token


def _unl_split_read_chunk(partition: dict) -> Callable[[int, int], bytes]:
    # Docstring: A ``(offset, length) -> bytes`` range reader for one ``unl_split`` descriptor.
    #
    # Ranged GET over the Files REST API when the descriptor carries a token+host
    # (executors have no ambient workspace credential, so the driver passes one per
    # batch); otherwise a local seek/read -- the same fallback shape as the driver's
    # ``_unl_read_chunk``, used offline or when no token could be minted. The batch's
    # token can be revoked or expire after planning; under ``auto`` (``fuse_fallback``) a
    # 401 then moves this split's remaining reads to the mount, while ``rest`` fails.

    token = partition.get("token")
    host = partition.get("host")
    path = partition["path"]
    if not os.path.isabs(path) or ".." in path.split("/"):
        # 117: Option '{}' must not contain traversal
        raise ValueError(_m(117, _SNAPSHOT_SOURCE_OPTION))

    def local(offset: int, length: int) -> bytes:
        with open(path, "rb") as handle:
            handle.seek(offset)
            return handle.read(length)

    if not (token and host):
        return local
    files_api: WorkspaceFilesApi | None = WorkspaceFilesApi(host, _StaticStagingCredential(token))

    def read(offset: int, length: int) -> bytes:
        nonlocal files_api
        if files_api is not None:
            try:
                return files_api.get_range(path, offset, offset + length)
            except WorkspaceHttpError as error:
                if error.status_code != 401 or not partition.get("fuse_fallback"):
                    raise
                # 362: The snapshot-staging token for {} was rejected; reading the split through the
                # FUSE mount
                logging.getLogger(__name__).warning(
                    _m(362, path),
                )
                files_api = None
        return local(offset, length)

    return read


def _partition_column(column: Column) -> dict[str, Any]:
    # Docstring: A column as a partition descriptor carries it; Column.parse reads it back.

    return {
        "name": column.name,
        "type_name": column.type_name,
        "nullable": column.nullable,
        "length": column.length,
        "precision": column.precision,
        "scale": column.scale,
    }


def _descriptor_columns(partition: dict) -> tuple[Column, ...]:
    # Docstring: The columns a partition descriptor carries for shaping rows on an executor.

    raw = partition.get("transform_columns") or partition.get("columns") or ()
    return tuple(Column.parse(column) for column in raw)


def _shape_snapshot(
    row: dict[str, Any],
    lsn: int,
    table: Table | None = None,
    options: dict[str, str] | None = None,
    *,
    commit_time: int | None = None,
) -> dict[str, Any]:
    converted = (
        _coerce_variable_decimal_values(row, table, options or {}) if table is not None else row
    )
    if table is not None and table.primary_keys:
        _refuse_null_key(converted, table)
    result = _framework_row(converted)
    result.update(
        {
            CURSOR: _sortable_lsn(lsn),
            COMMIT_LSN: _sortable_lsn(lsn),
            TX_ID: None,
            OP: "r",
            COMMIT_TIME: _commit_time_value(commit_time),
        }
    )
    return result


def _shape_unl_record(
    raw: bytes,
    table: Table,
    snapshot_lsn: int,
    options: dict[str, str],
    encoding: str,
    *,
    exposed_name: str | None = None,
    delimiter: str = _UNL_DEFAULT_DELIMITER,
    escape: str = _UNL_DEFAULT_ESCAPE,
    commit_time: int | None = None,
    predicate: tuple | None = None,
    null_byte: str = _DEFAULT_NULL_BYTE_MODE,
) -> dict[str, Any] | None:
    # Docstring: Parse, type-convert, and shape one UNLOAD / HPL DELIMITED record -- shared by both
    # serve paths.
    #
    # Fails closed on a per-record column-count mismatch (the cheap misalignment guard
    # that stands in for the absent schema-fingerprint check). ``exposed_name`` names the
    # table in the error when the caller's ``table`` is a shaping-only stand-in.
    # ``delimiter``/``escape`` select the delimited dialect (UNLOAD's ``|``/backslash by
    # default, or an HPL job's configured pair).

    #
    # An extract holds every catalog column, so fields are matched positionally against
    # ``catalog_columns`` under a column selection and only the selected ones converted.
    # ``predicate`` is the table's row_filter from :func:`_row_filter_predicate`, built once
    # per extract by the caller; a record it drops returns None. ``null_byte`` is the
    # connection's string.null.byte mode, applied as the live decoders apply it, before
    # the filter sees the row.

    catalog = table.catalog_columns or table.columns
    # Only an HPL extract (its own delimiter) can end a full record in the delimiter: with
    # UNLOAD's every record does, so a record short of a column must still fail below.
    expected = None if delimiter == _UNL_DEFAULT_DELIMITER else len(catalog)
    fields = _parse_unload_fields(
        raw.decode(encoding, "strict"), delimiter=delimiter, escape=escape, fields=expected
    )
    if len(fields) != len(catalog):
        name = exposed_name if exposed_name is not None else table.exposed_name
        # 363: UNLOAD record in snapshot.source for '{}' has {} fields but the table has {} columns
        raise InformixError(_m(363, name, len(fields), len(catalog)))
    position = {column.name: index for index, column in enumerate(catalog)}
    row = {
        column.name: apply_null_byte(
            _unload_value(
                fields[position[column.name]], column.type_name, column.length, column.scale
            ),
            null_byte,
        )
        for column in table.columns
    }
    if predicate is not None and not _row_filter_matches(predicate, row):
        return None
    return _shape_snapshot(row, snapshot_lsn, table, options, commit_time=commit_time)


def _log_lsn(uniqid: int, used_pages: int, *, page_start: bool = False) -> int:
    # Docstring: Compose an LSN from a ``sysmaster:syslogs`` row: the log file's ``uniqid`` in the
    # high word and the byte offset of ``used_pages`` 4 KiB pages in the low word.
    #
    # ``page_start`` anchors the offset at the start of the last used page instead of
    # its end -- the conservative capture boundary (see ``boundary_lsn``); a log with no
    # used page yet anchors at its beginning either way.

    pages = max(int(used_pages) - 1, 0) if page_start else int(used_pages)
    return (int(uniqid) << 32) + (pages << 12)


def _deep_size(value: Any, seen: set[int] | None = None) -> int:
    # Docstring: Estimate retained Python container/value memory without double counting.

    if seen is None:
        seen = set()
    identity = id(value)
    if identity in seen:
        return 0
    seen.add(identity)
    size = sys.getsizeof(value)
    if isinstance(value, dict):
        return size + sum(
            _deep_size(key, seen) + _deep_size(item, seen) for key, item in value.items()
        )
    if isinstance(value, (list, tuple, set, frozenset)):
        return size + sum(_deep_size(item, seen) for item in value)
    return size


def _skip_json_whitespace(text: str, index: int) -> int:
    # Docstring: Return the first index at or after ``index`` that is not JSON whitespace.
    #
    # Staged pages are written with ``separators=(",",":")`` (no whitespace), but
    # tolerating it keeps the hand-rolled scan robust to a compliant re-serializer.

    length = len(text)
    while index < length and text[index] in " \t\n\r":
        index += 1
    return index


# Header fields that sort *after* ``rows`` in a staged page's canonical
# (``sort_keys=True``) serialization -- see ``_publish_snapshot_stage_page``. They
# are located from the tail by ``_scan_staged_page`` rather than by walking the rows
# array. ``sha256`` is added to the record after the body and re-sorted between
# ``schema_id`` and ``snapshot_lsn``.
_SNAPSHOT_STAGE_TRAILING_KEYS = (
    "schema_id",
    "sha256",
    "snapshot_lsn",
    "table",
    "upper_pk",
    "version",
)


def _staged_page_payload(body: dict[str, Any]) -> tuple[bytes, str]:
    # Docstring: A staged page's bytes and its digest, with the body serialised once.
    #
    # A page is the canonical JSON of ``body`` plus a "sha256" field, in sorted-key order,
    # holding the digest of that canonical JSON. The fields either side of "sha256" are
    # serialised once each, hashed as joined, and the digest spliced between them: byte for
    # byte what serialising the whole record would give, and what _scan_staged_page undoes
    # by cutting out the field and its trailing comma. The body always has fields on both
    # sides (schema_id before it, snapshot_lsn after).

    options: dict[str, Any] = {"allow_nan": False, "separators": (",", ":"), "sort_keys": True}
    before = json.dumps({k: v for k, v in body.items() if k < "sha256"}, **options)[:-1]
    after = json.dumps({k: v for k, v in body.items() if k > "sha256"}, **options)[1:]
    head, tail = before.encode("utf-8"), after.encode("utf-8")
    hasher = hashlib.sha256(head)
    hasher.update(b",")
    hasher.update(tail)
    digest = hasher.hexdigest()
    return head + b',"sha256":"' + digest.encode("ascii") + b'",' + tail, digest


def _scan_staged_page(text: str) -> tuple[dict[str, Any], int, tuple[int, int]]:
    # Docstring: Locate a staged page's header fields without ever walking the ``rows`` array.
    #
    # Returns ``(header, array_start, sha_span)``. ``header`` holds every field except
    # ``rows``; ``array_start`` is the index of the ``rows`` array's opening ``[`` (the
    # single serve pass streams from there and finds the closing ``]`` itself);
    # ``sha_span`` is the ``[start, end)`` of the ``"sha256":"...",`` field including its
    # trailing comma, so the caller can excise it to recover the exact canonical body
    # the digest covers.
    #
    # The rows array is the one field with unbounded, arbitrary content, so it is never
    # scanned. Leading fields (those sorting before ``rows``) are forward-parsed with
    # ``raw_decode`` and are small; the parse stops at the ``rows`` key. Trailing fields
    # (sorting after ``rows``) are found from the tail with ``str.rfind`` on their
    # ``,"key":`` marker: each trailing field is structurally the *last* occurrence of
    # its key in the payload -- any same-named row column lives inside the earlier rows
    # array, and an unescaped ``,"key":`` only ever appears as a JSON key (a string
    # *value* containing that text is escaped as ``,\\"key\\":``) -- so the reverse find
    # lands on the real header field. Those fields occupy the payload's final few
    # hundred bytes, so the reverse finds are effectively free while the multi-hundred-MB
    # rows array is skipped entirely. Raises ``InformixError`` on any structural problem.

    length = len(text)
    index = _skip_json_whitespace(text, 0)
    if index >= length or text[index] != "{":
        # 365: Malformed staged snapshot page
        raise InformixError(_m(365))
    index += 1
    decoder = json.JSONDecoder()
    header: dict[str, Any] = {}
    array_start: int | None = None
    sha_span: tuple[int, int] | None = None
    try:
        # Forward-parse the small leading fields, stopping at the rows array.
        while True:
            index = _skip_json_whitespace(text, index)
            if index < length and text[index] == "}":
                break
            key, index = decoder.raw_decode(text, index)
            if not isinstance(key, str):
                # 365: Malformed staged snapshot page
                raise InformixError(_m(365))
            index = _skip_json_whitespace(text, index)
            if index >= length or text[index] != ":":
                # 365: Malformed staged snapshot page
                raise InformixError(_m(365))
            index = _skip_json_whitespace(text, index + 1)
            if key == "rows":
                if index >= length or text[index] != "[":
                    # 365: Malformed staged snapshot page
                    raise InformixError(_m(365))
                array_start = index
                break
            value, index = decoder.raw_decode(text, index)
            header[key] = value
            index = _skip_json_whitespace(text, index)
            if index < length and text[index] == ",":
                index += 1
            elif index >= length or text[index] != "}":
                # 365: Malformed staged snapshot page
                raise InformixError(_m(365))
        if array_start is None:
            # 365: Malformed staged snapshot page
            raise InformixError(_m(365))
        # Locate the trailing fields from the tail. Each key's last occurrence is its
        # header field (row columns of the same name sit inside the earlier array).
        for key in _SNAPSHOT_STAGE_TRAILING_KEYS:
            marker = f',"{key}":'
            marker_pos = text.rfind(marker)
            if marker_pos < array_start:
                # 365: Malformed staged snapshot page
                raise InformixError(_m(365))
            value, value_end = decoder.raw_decode(text, marker_pos + len(marker))
            header[key] = value
            if key == "sha256":
                # Excise "sha256":"...", (key through trailing comma) to recover the
                # canonical body. sha256 is never the last sorted key (snapshot_lsn,
                # table, upper_pk, version follow), so a trailing comma is present.
                if value_end >= length or text[value_end] != ",":
                    # 365: Malformed staged snapshot page
                    raise InformixError(_m(365))
                sha_span = (marker_pos + 1, value_end + 1)
    except (json.JSONDecodeError, IndexError) as error:
        # 365: Malformed staged snapshot page
        raise InformixError(_m(365)) from error
    if sha_span is None:
        # 365: Malformed staged snapshot page
        raise InformixError(_m(365))
    return header, array_start, sha_span


def _encode_snapshot_stage_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        return {"$informix": "float", "value": repr(value)}
    if isinstance(value, Decimal):
        return {"$informix": "decimal", "value": str(value)}
    if isinstance(value, datetime):
        return {"$informix": "datetime", "value": value.isoformat()}
    if isinstance(value, date):
        return {"$informix": "date", "value": value.isoformat()}
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {
            "$informix": "binary",
            "value": base64.b64encode(bytes(value)).decode("ascii"),
        }
    if isinstance(value, list):
        return [_encode_snapshot_stage_value(item) for item in value]
    if isinstance(value, tuple):
        return {
            "$informix": "tuple",
            "value": [_encode_snapshot_stage_value(item) for item in value],
        }
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        if "$informix" in value:
            # 366: Snapshot row contains reserved staging key '$informix'
            raise InformixError(_m(366))
        return {key: _encode_snapshot_stage_value(item) for key, item in value.items()}
    # 367: Snapshot value of type {} cannot be staged safely
    raise InformixError(_m(367, type(value).__name__))


def _decode_snapshot_stage_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, list):
        return [_decode_snapshot_stage_value(item) for item in value]
    if not isinstance(value, dict):
        # 368: Invalid typed value in staged snapshot page
        raise InformixError(_m(368))
    kind = value.get("$informix")
    if kind is None:
        return {str(key): _decode_snapshot_stage_value(item) for key, item in value.items()}
    if set(value) != {"$informix", "value"} or not isinstance(value["value"], str):
        if kind != "tuple" or set(value) != {"$informix", "value"}:
            # 368: Invalid typed value in staged snapshot page
            raise InformixError(_m(368))
    payload = value["value"]
    try:
        if kind == "decimal":
            return Decimal(payload)
        if kind == "datetime":
            return datetime.fromisoformat(payload)
        if kind == "date":
            return date.fromisoformat(payload)
        if kind == "binary":
            return base64.b64decode(payload, validate=True)
        if kind == "float" and payload in {"nan", "inf", "-inf"}:
            return float(payload)
        if kind == "tuple" and isinstance(payload, list):
            return tuple(_decode_snapshot_stage_value(item) for item in payload)
    except (ValueError, TypeError, ArithmeticError) as error:
        # 368: Invalid typed value in staged snapshot page
        raise InformixError(_m(368)) from error
    # 369: Unknown staged snapshot value type {!r}
    raise InformixError(_m(369, kind))


def _offset(
    commit: int,
    change: int,
    begin: int,
    tx_id: int | None,
    phase: str,
    table: Table,
    schema_id: str,
    pipeline_scope: str,
    *,
    trigger_generation: str | None = None,
    trigger_high_water: int | str | None = None,
    incremental: dict | None = None,
) -> dict:
    offset = {
        "version": _OFFSET_VERSION,
        "commit_lsn": str(commit),
        "change_lsn": str(change),
        "begin_lsn": str(begin),
        "tx_id": tx_id,
        "phase": phase,
        "schema_fingerprint": _schema_fingerprint(table),
        "schema_id": schema_id,
        "pipeline_scope": pipeline_scope,
        "trigger_generation": trigger_generation,
        "trigger_high_water": (str(trigger_high_water) if trigger_high_water is not None else None),
    }
    if incremental is not None:
        offset["incremental"] = incremental
    return offset


def _schema_node_id(table: Table, predecessor: str | None = None) -> str:
    # Docstring: Return the stable identity of one physical-table schema layout.
    #
    # A transition's node also hashes the node it follows. A layout can recur (a column
    # added, then dropped), and with the layout alone its return would reuse the first
    # node's id, whose recorded successor is the change it already undid: the history
    # would cycle between the two and never reach a later schema. An initial node has no
    # predecessor and keeps the layout-only id.

    parts = (table.native_identity, _schema_fingerprint(table))
    identity = "\0".join(parts if predecessor is None else (*parts, predecessor))
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:32]


def _schema_state(
    table: Table,
    start_lsn: int,
    predecessor: str | None = None,
    *,
    schema_id: str | None = None,
) -> dict[str, object]:
    return {
        "id": schema_id or _schema_node_id(table, predecessor),
        "fingerprint": _schema_fingerprint(table),
        "start_lsn": str(start_lsn),
        "predecessor": predecessor,
        "table": {
            "database": table.database,
            "owner": table.owner,
            "name": table.name,
            "incarnation": table.incarnation,
            "primary_keys": list(table.primary_keys),
            "columns": [
                {
                    "name": column.name,
                    "type_name": column.type_name,
                    "nullable": column.nullable,
                    "length": column.length,
                    "precision": column.precision,
                    "scale": column.scale,
                    "cdc_supported": column.cdc_supported,
                }
                for column in table.columns
            ],
            **(
                {}
                if table.column_selection is None
                else {
                    "column_selection": [
                        table.column_selection[0],
                        list(table.column_selection[1]),
                    ]
                }
            ),
            **({} if table.row_filter is None else {"row_filter": table.row_filter}),
            **(
                {}
                if table.column_renames is None
                else {"column_renames": [list(pair) for pair in table.column_renames]}
            ),
            **(
                {}
                if table.column_transforms is None
                else {
                    "column_transforms": [list(pair) for pair in table.column_transforms],
                    "transform_key_id": table.transform_key_id,
                }
            ),
            **(
                {}
                if table.decimal_targets is None
                else {"decimal_targets": [list(pair) for pair in table.decimal_targets]}
            ),
            **({} if table.decimal_default is None else {"decimal_default": table.decimal_default}),
            # Not fingerprinted (the key it gives is): kept so a replayed step is judged by
            # the except list it was taken under, not the live one (see
            # _ensure_compatible_schema_change). An empty one (a bare "*") included: left
            # out, it read as unknown.
            **(
                {}
                if table.key_wildcard_except is None
                else {"key_wildcard_except": list(table.key_wildcard_except)}
            ),
        },
    }


def _table_from_schema_state(state: dict[str, object], default_database: str) -> Table:
    raw = state.get("table")
    if not isinstance(raw, dict):
        # 370: Informix shared CDC schema is missing table metadata
        raise InformixError(_m(370))
    try:
        start_lsn = _strict_lsn(state["start_lsn"], "start_lsn")
    except (KeyError, TypeError, ValueError) as error:
        # 371: Informix shared CDC schema has an invalid start LSN
        raise InformixError(_m(371)) from error
    if start_lsn < 1:
        # 371: Informix shared CDC schema has an invalid start LSN
        raise InformixError(_m(371))
    table = Table.parse(raw, default_database)
    if _schema_fingerprint(table) != state.get("fingerprint"):
        # 372: Informix shared CDC schema fingerprint does not match metadata
        raise InformixError(_m(372))
    return table


_WIDENABLE_STRING_TYPES = frozenset({"CHAR", "NCHAR", "VARCHAR", "NVARCHAR", "LVARCHAR"})


def _string_capacity(column: Column) -> int:
    # VARCHAR and NVARCHAR pack their minimum reserved length into the high byte.
    length = column.length or 0
    return length & 0xFF if column.type_name in {"VARCHAR", "NVARCHAR"} else length


def _spark_type_widened(previous: Any, current: Any) -> bool:
    # Docstring: Whether Delta type widening takes a column from Spark type ``previous`` to
    # ``current`` in place: the changes _widened allows, stated on the landed types.

    if isinstance(previous, IntegerType):
        if isinstance(current, (LongType, DoubleType)):
            return True
        return isinstance(current, DecimalType) and current.precision - current.scale >= 10
    if isinstance(previous, LongType):
        return isinstance(current, DecimalType) and current.precision - current.scale >= 20
    if isinstance(previous, FloatType):
        return isinstance(current, DoubleType)
    if isinstance(previous, DecimalType) and isinstance(current, DecimalType):
        return (
            current.scale >= previous.scale
            and current.precision - current.scale >= previous.precision - previous.scale
        )
    return False


def _widened(previous: Column, current: Column) -> bool:
    # Docstring: Whether ``current`` widens ``previous`` so every old value still fits.
    #
    # A longer CHAR, NCHAR, VARCHAR, NVARCHAR, or LVARCHAR stays StringType, and SMALLINT to
    # INTEGER stays IntegerType, so neither changes what the destination holds. The
    # numeric widenings change the Spark type -- SMALLINT or INTEGER to BIGINT/INT8
    # (IntegerType to LongType), SMALLFLOAT to FLOAT (FloatType to DoubleType), a
    # fixed-scale DECIMAL or MONEY gaining precision without losing integer digits, and
    # an integer to a fixed-scale DECIMAL or MONEY with room for all its digits (10 for
    # SMALLINT and INTEGER, which land as IntegerType; 20 for the 64-bit types) or, for
    # SMALLINT and INTEGER, to FLOAT -- and match what Delta type widening accepts, so
    # the destination widens the column in place when the pipeline sets
    # pipelines.enableTypeWidening. Nullability must not change, and a variable-scale
    # DECIMAL never widens.

    if (previous.name, previous.nullable, previous.cdc_supported) != (
        current.name,
        current.nullable,
        current.cdc_supported,
    ):
        return False
    old_type, new_type = previous.type_name, current.type_name
    if old_type == new_type in _WIDENABLE_STRING_TYPES:
        return _string_capacity(current) >= _string_capacity(previous)
    if old_type in _INTEGER_RANKS and new_type in _INTEGER_RANKS:
        return _INTEGER_RANKS[new_type] > _INTEGER_RANKS[old_type]
    if old_type in _INTEGER_RANKS and new_type in {"DECIMAL", "NUMERIC", "MONEY"}:
        if current.precision is None or current.scale in (None, 0xFF):
            return False
        # Delta's minimum: decimal(10,0) for IntegerType, decimal(20,0) for LongType,
        # with any scale added on top of those integer digits.
        digits = 20 if _INTEGER_RANKS[old_type] == 3 else 10
        return current.precision <= 38 and current.precision - current.scale >= digits
    if old_type in _INTEGER_RANKS and new_type in {"FLOAT", "DOUBLE", "DOUBLE PRECISION"}:
        # IntegerType to DoubleType is exact; Delta doesn't widen LongType to DoubleType.
        return _INTEGER_RANKS[old_type] < 3
    if old_type in {"SMALLFLOAT", "REAL"}:
        return new_type in {"FLOAT", "DOUBLE", "DOUBLE PRECISION"}
    if old_type == new_type in {"DECIMAL", "NUMERIC", "MONEY"}:
        scales = (previous.scale, current.scale)
        if None in (previous.precision, current.precision, *scales) or 0xFF in scales:
            return False
        # Delta's rule: the scale may grow only as far as the precision grows with it.
        return (
            current.precision <= 38
            and current.scale >= previous.scale
            and current.precision - current.scale >= previous.precision - previous.scale
        )
    return False


# String types Informix 14.10 shortens in place: the table keeps its tabid, so the stream
# carries on. LVARCHAR is not one, nor is a narrower number or DATETIME qualifier: Informix
# rebuilds the table under a new tabid for those, which reads as a new table.
_SHORTENABLE_STRING_TYPES = frozenset({"CHAR", "NCHAR", "VARCHAR", "NVARCHAR"})


def _shortened(previous: Column, current: Column) -> bool:
    # Docstring: Whether ``current`` is ``previous`` made shorter and nothing else.
    #
    # The destination column is a string either way, so it stays as it is and is never
    # shrunk. Informix's ALTER truncates existing values longer than the
    # new length without logging a change per row, so those rows keep their old values in
    # the destination until they next change or a full refresh. For a key column that
    # divergence breaks matching, so _ensure_compatible_schema_change rejects it there.

    return (
        (previous.name, previous.type_name, previous.nullable, previous.cdc_supported)
        == (current.name, current.type_name, current.nullable, current.cdc_supported)
        and previous.type_name in _SHORTENABLE_STRING_TYPES
        and _string_capacity(current) < _string_capacity(previous)
    )


def _refuse_float_key_mid_copy(previous: Table, current: Table) -> None:
    # Docstring: Fail on a key column widened to a float while an incremental copy is
    # in progress.
    #
    # SMALLINT and INTEGER widen to FLOAT, a key column too, but a float key can't page
    # the copy (see _incremental_datetime_chunk_exprs), and the copy's next chunk passed
    # its float cursor to SQL, which takes no float: every batch failed after the schema
    # step was recorded. Refused before that step, as the other changes a copy can't take.

    before = {column.name: column.type_name for column in previous.columns}
    for column in current.columns:
        if (
            column.name in current.primary_keys
            and column.type_name in _FLOAT_KEY_TYPES
            and before.get(column.name, column.type_name) not in _FLOAT_KEY_TYPES
        ):
            # 468: Key column {} of '{}' became {} while the table's incremental snapshot was
            # copying, and a float key cannot page the copy; run a full refresh
            raise InformixError(_m(468, column.name, current.exposed_name, column.type_name))


def _filter_parts(node: tuple):
    # Docstring: A row_filter tree's shape, and each number or value compared with a bare
    # column, in order: (shape, None) or (shape, (column, op, literal)), an IN value taken as
    # an "=", a BETWEEN's bounds as ">=" and "<=".

    tag = node[0]
    if tag in ("and", "or", "not"):
        yield (tag,), None
        for child in node[1] if tag != "not" else (node[1],):
            yield from _filter_parts(child)
        yield (")",), None
    elif tag in ("cmp", "in", "between") and isinstance(node[1], str):
        literals = node[3] if tag == "in" else node[3:] if tag == "between" else (node[3],)
        op = node[2] if tag == "cmp" else "="
        ops = (">=", "<=") if tag == "between" else (op,) * len(literals)
        yield (tag, node[1], node[2], len(literals)), None
        for op, literal in zip(ops, literals):
            yield (literal[0],), (node[1], op, literal)
    else:
        yield node, None


def _float_filter_regraded(previous: Table, current: Table) -> tuple | None:
    # Docstring: The column of ``current``'s row_filter whose comparison a widening to FLOAT
    # changes, its numbers that do, and what to write for each instead; or None. An integer
    # compares exactly, a SMALLFLOAT in 32 bits and a FLOAT as doubles, so a number that
    # isn't the same in both can split values the old comparison kept together (a
    # SMALLFLOAT 0.1 equals 0.1; widened, it doesn't).

    if current.row_filter is None:
        return None
    before = {column.name: column for column in previous.columns}
    after = {column.name: _row_filter_kind(column) for column in current.columns}
    found: dict[str, dict] = {}
    for _, part in _filter_parts(_parse_row_filter(current.row_filter)):
        column = part and before.get(part[0])
        if not column or after.get(column.name) != "float" or part[2][0] != "number":
            continue
        value, kind = part[2][1], _row_filter_kind(column)
        exact = float(value)
        if kind == "smallfloat" and _float32(exact) != exact:
            # The SMALLFLOAT the number stood for, exactly: a double, and 32 bits.
            found.setdefault(column.name, {})[value] = repr(_float32(exact))
        elif kind == "number":
            # An integer's values are whole, and of its type's range: only one of those
            # between the number and its double (2.9999999999999999 and 3) compares
            # otherwise. Halfway between the whole numbers around it every comparison
            # splits as the number did, and a double holds it exactly.
            top = 32767 if _INTEGER_RANKS.get(column.type_name) == 1 else 2147483647
            low, high = sorted((value, Decimal(exact)))
            if low != high and max(math.ceil(low), -top) <= min(math.floor(high), top):
                found.setdefault(column.name, {})[value] = str(math.floor(value) + Decimal("0.5"))
    if not found:
        return None
    # Every number of the first such column, so one edit takes them all.
    name, numbers = next(iter(found.items()))
    listed = " and ".join(map(str, numbers))
    if len(numbers) == 1:
        return name, listed, next(iter(numbers.values()))
    return name, listed, " and ".join(f"{new} for {old}" for old, new in numbers.items())


def _number_alike(column: Column | None, op: str, old: tuple, new: tuple) -> bool:
    # Docstring: Whether ``op`` with ``new`` gives every value ``column`` can hold the answer
    # it gives with ``old``: a SMALLFLOAT's number one with the same 32 bits, a FLOAT's the
    # same double, an integer's or fixed-scale DECIMAL's one between the same two values.

    if old == new:
        return True
    if column is None or old[0] != "number" or new[0] != "number":
        return False
    kind = _row_filter_kind(column)
    if kind in _FLOAT_KINDS:
        fit = _float32 if kind == "smallfloat" else float
        return fit(float(old[1])) == fit(float(new[1]))
    if column.type_name in _INTEGER_RANKS:
        scale = 0
    elif kind == "number" and not _is_variable_scale_decimal(column):
        scale = column.scale if column.scale is not None else 2 * (column.type_name == "MONEY")
    else:
        return False
    # In steps of the column's smallest value, where its values are the whole numbers. Past
    # the default context's 28 digits: in a DECIMAL(32,2) it made ...890.12 and ...890.99
    # one number, and a change that moved 87 values passed as no change.
    with localcontext() as context:
        context.prec = max(200, len(old[1].as_tuple().digits), len(new[1].as_tuple().digits))
        a, b = old[1].scaleb(scale), new[1].scaleb(scale)
        if op in ("=", "<>"):
            return a != a.to_integral_value() and b != b.to_integral_value()
    rounding = math.ceil if op in (">=", "<") else math.floor
    return rounding(a) == rounding(b)


def _filter_rewritten_alike(previous: Table, current: Table) -> bool:
    # Docstring: Whether ``current``'s row_filter is ``previous``'s with only numbers compared
    # with a column changed, each to one that gives every value it can hold, as ``previous``
    # types it, the same answer (_number_alike): the rows already landed match as they did.

    if previous.row_filter is None or current.row_filter is None:
        return False
    columns = {column.name: column for column in previous.columns}
    old = list(_filter_parts(_parse_row_filter(previous.row_filter)))
    new = list(_filter_parts(_parse_row_filter(current.row_filter)))
    return len(old) == len(new) and all(
        a[0] == b[0]
        and (a[1] == b[1] or bool(a[1]) and _number_alike(columns.get(a[1][0]), *a[1][1:], b[1][2]))
        for a, b in zip(old, new)
    )


def _decimal_types_changed(previous: Table, current: Table) -> bool:
    # Docstring: Whether a variable-scale DECIMAL column both schemas have lands as another
    # type now.
    #
    # Only those columns: one added since takes the options as they are, and one dropped
    # leaves nothing to compare. A column with no entry is the default, so a table that
    # left the default for another type is a change (its entries were none before).

    def variable(table: Table) -> set[str]:
        return {column.name for column in table.columns if _is_variable_scale_decimal(column)}

    default = _variable_decimal_text(_VARIABLE_DECIMAL_DEFAULT_TARGET)
    before, after = dict(previous.decimal_targets or ()), dict(current.decimal_targets or ())
    return any(
        before.get(name, default) != after.get(name, default)
        for name in variable(previous) & variable(current)
    )


def _selection_cleanup_only(previous: Table, current: Table) -> bool:
    # Docstring: Whether the selection only changed by names that leave the ingested columns
    # as they were.
    #
    # That is a name whose column the table no longer has, or, under exclude_columns, a
    # column of a type the connector leaves out anyway: listing one only silences its
    # warning, and taking it out again leaves it out all the same. Such an exclude_columns
    # entry may also be the whole list, so the selection appears or goes. include_columns
    # can't name such a column, so a change there stays strict.

    before, after = previous.column_selection, current.column_selection
    catalog = {column.name: column for column in current.catalog_columns or current.columns}

    def needless(name: str) -> bool:
        column = catalog.get(name)
        return column is None or _unsupported_column(column)

    if before is None or after is None:
        selection = before or after
        return (
            selection is not None
            and selection[0] == "exclude"
            and all(needless(name) for name in selection[1])
        )
    if before[0] != after[0]:
        return False
    changed = set(before[1]) ^ set(after[1])
    if before[0] == "include":
        return not changed & catalog.keys()
    return all(needless(name) for name in changed)


def _transforms_cleanup_only(previous: Table, current: Table) -> bool:
    # Docstring: Whether the transforms only lost entries for columns the table no longer has.

    before, after = set(previous.column_transforms or ()), set(current.column_transforms or ())
    catalog = {column.name for column in current.catalog_columns or current.columns}
    return after <= before and all(name not in catalog for name, _ in before - after)


def _renames_cleanup_only(previous: Table, current: Table) -> bool:
    # Docstring: Whether the renames only lost entries for columns the table no longer has.

    before, after = set(previous.column_renames or ()), set(current.column_renames or ())
    catalog = {column.name for column in current.catalog_columns or current.columns}
    return after <= before and all(source not in catalog for source, _ in before - after)
