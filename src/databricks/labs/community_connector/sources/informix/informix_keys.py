"""The bridge protocol and key handling: unique-index keys, PAD SPACE and descending order, key
overrides, and decimal coercion.
"""

from __future__ import annotations

import contextlib
import json
import math
import re
from datetime import date, datetime, timezone
from decimal import ROUND_DOWN, Decimal, InvalidOperation, localcontext
from typing import Any, Callable, Iterable, Iterator, Protocol, Sequence

from databricks.labs.community_connector.sources.informix.informix_options import (
    _APPEND_INGESTION_OPTION,
    _BARE_IDENTIFIER,
    _DAEMON_RESERVATION_OPTION,
    _DEFAULT_CDC_MAX_RECORDS,
    _DEFAULT_DAEMON_CONNECTION_RESERVATION,
    _DEFAULT_KEYLESS_SNAPSHOT_PAGE_SIZE,
    _DEFAULT_MAX_CONCURRENT_CONNECTIONS,
    _DEFAULT_MAX_RECORDS_PER_BATCH,
    _DEFAULT_SNAPSHOT_PAGE_SIZE,
    _DEFAULT_STATE_GC_INTERVAL_HOURS,
    _DEFAULT_STATE_GC_RETENTION_DAYS,
    _SNAPSHOT_READER_THREADS_OPTION,
    _SNAPSHOT_SCAN_CURSOR,
    _SNAPSHOT_SCAN_KEYSET,
    _SNAPSHOT_SCAN_OPTION,
    _SNAPSHOT_SCANS,
    _UNL_GZIP_RECORDS_OPTION,
    _UNL_SPLIT_BYTES_OPTION,
    _UNL_SPLIT_TARGET_BYTES,
    InformixError,
    _m,
)
from databricks.labs.community_connector.sources.informix.informix_registration import (
    Column,
    ConnectionCapacityUnavailable,
    Table,
)


class InformixBridge(Protocol):
    # Docstring: Injectable boundary around pure-Python SQLI metadata, snapshots, and CDC.

    def list_tables(self) -> list[dict[str, Any]]: ...

    def get_table(self, identity: str) -> dict[str, Any]: ...

    def key_index_directions(self, identity: str, columns: Sequence[str]) -> list[bool] | None: ...

    def column_defaults(self, identity: str, columns: Sequence[str]) -> list[str]: ...

    def current_lsn(self) -> int: ...

    def minimum_lsn(self) -> int: ...

    def current_utc_time(self) -> int | None: ...

    def prepare_initial_capture(self, identities: Sequence[str]) -> int: ...

    def validate_initial_lsn(self, capture: dict[str, Any], start_lsn: int) -> None: ...

    def snapshot_page(
        self,
        identity: str,
        columns: Sequence[str],
        primary_keys: Sequence[str],
        after: Sequence[Any] | None,
        limit: int,
        max_bytes: int | None = None,
        skip: int = 0,
        snapshot_filter: str | None = None,
        key_descending: Sequence[bool] | None = None,
        chunk_exprs: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]: ...

    def consistent_snapshot(
        self,
        identity: str,
        columns: Sequence[str],
        primary_keys: Sequence[str],
        page_size: int,
        max_rows: int,
        max_bytes: int,
        datetime_primary_key: bool = False,
        page_consumer: Callable[[int, int, list[dict[str, Any]]], None] | None = None,
        snapshot_filter: str | None = None,
        isolation: str = "REPEATABLE READ",
        key_descending: Sequence[bool] | None = None,
    ) -> tuple[int, list[dict[str, Any]]]: ...

    def snapshot_chunk(
        self,
        identity: str,
        columns: Sequence[str],
        primary_keys: Sequence[str],
        after: Sequence[Any] | None,
        limit: int,
        max_bytes: int | None = None,
        chunk_exprs: dict[str, str] | None = None,
        snapshot_filter: str | None = None,
        key_descending: Sequence[bool] | None = None,
        isolation: str = "REPEATABLE READ",
    ) -> tuple[int, list[dict[str, Any]]]: ...

    def max_primary_key(
        self,
        identity: str,
        primary_keys: Sequence[str],
        chunk_exprs: dict[str, str] | None = None,
        snapshot_filter: str | None = None,
        key_descending: Sequence[bool] | None = None,
    ) -> list[Any] | None: ...

    def read_changes(
        self,
        tables: Sequence[dict[str, Any]],
        start_lsn: int,
        timeout_seconds: int,
        max_records: int,
        budget: int | None = None,
        until: int | None = None,
        seconds: float | None = None,
    ) -> list[dict[str, Any]]: ...

    def release_connection(self) -> None:
        # Docstring: Close a finite poll's source connection and release its capacity lease.
        ...

    def boundary_lsn(self) -> int: ...

    def buffered_logging(self) -> bool: ...

    def table_indexes(self, identity: str) -> list: ...

    def key_rows(
        self, identity, columns, keys, values, isolation, limit=None, present=(), skip=0
    ) -> list[dict[str, Any]]: ...

    def rows_by_values(
        self, identity, columns, keys, value_sets, isolation, limit=None
    ) -> list[dict[str, Any]]: ...


def _serialized_sqli_operation(method: Callable[..., Any]) -> Callable[..., Any]:
    # Docstring: Keep lease-loss shutdown from closing a transport during an operation.

    def serialized(self: Any, *args: Any, **kwargs: Any) -> Any:
        lock = getattr(self, "_operation_lock", None)
        if lock is None:  # injected bridges constructed without __init__
            return method(self, *args, **kwargs)
        with lock:
            lease_lost = getattr(self, "_connection_lease_lost", None)

            def lost() -> bool:
                # Whether the lease is lost. A shutdown that gave up waiting for this
                # lock leaves the loss standing for an operation to see (see
                # _poison_connection_slot_lease): the one that sees it after the shutdown
                # is over takes it down, so that the next can connect afresh.
                if lease_lost is None or not lease_lost.is_set():
                    return False
                if getattr(self, "_lease_loss_unseen", False):
                    self._lease_loss_unseen = False
                    lease_lost.clear()
                return True

            if lost():
                # 12: Informix connection-capacity lease was lost before the SQLI operation
                raise ConnectionCapacityUnavailable(_m(12))
            depth = self.__dict__.get("_operation_depth", 0)
            self._operation_depth = depth + 1
            try:
                result = method(self, *args, **kwargs)
                if not depth:
                    self._end_ansi_transaction()
            except BaseException as error:
                if not depth:
                    # Failed after its statements ran (a refused value, a nonzero cdc_*
                    # status), the operation left their transaction open, its locks held.
                    with contextlib.suppress(Exception):
                        self._end_ansi_transaction(rollback=True)
                if lost():
                    # 13: Informix connection-capacity lease was lost during the SQLI operation
                    raise ConnectionCapacityUnavailable(_m(13)) from error
                raise
            finally:
                self._operation_depth = depth
            if lost():
                # 13: Informix connection-capacity lease was lost during the SQLI operation
                raise ConnectionCapacityUnavailable(_m(13))
            return result

    return serialized


def _resolve_daemon_reservation(options: dict[str, str], slot_count: int) -> int:
    # Docstring: Effective CDC-daemon slot reservation (the ``0`` = auto case resolved).
    #
    # Resolution only -- range validation stays in ``_daemon_connection_reservation`` and
    # the constructor. Extracted so ``_resolve_snapshot_reader_threads`` can key its default
    # off the same value without duplicating the auto formula.

    reservation = int(
        options.get(_DAEMON_RESERVATION_OPTION, str(_DEFAULT_DAEMON_CONNECTION_RESERVATION))
    )
    if reservation == 0:
        return min(max(1, slot_count // 3), slot_count - 1)
    return reservation


def _resolve_snapshot_reader_threads(options: dict[str, str]) -> int:
    # Docstring: Number of snapshot-drain workers the pool spawns for these options.
    #
    # Shared by the connector (worker count) and the bridge (reservation band) so the two
    # never drift. An explicit ``snapshot.reader.threads`` wins; otherwise the count is one
    # below the CDC-daemon reservation. Keeping it strictly below the reservation guarantees
    # the snapshot-drain floor (``daemon.connection.reservation - threads``) never collapses
    # to 0, so a low slot always stays reachable by consumer bootstrap reads even while the
    # drains hold their slots for whole scans -- otherwise the drains grab the lowest slots
    # and, holding them for the entire (uncapped) backfill, starve consumer/CDC reads on a
    # small pool. Keyless drains serialize through this pool, so a flat single worker made
    # every keyless table's initial scan queue behind the last, stalling the later tables on
    # multi-table pipelines; one-below-reservation parallelizes them once the pool is large
    # enough to spare a drain band and still keep a consumer floor.

    explicit = options.get(_SNAPSHOT_READER_THREADS_OPTION)
    if explicit is not None:
        return max(1, int(explicit))
    slot_count = int(
        options.get("max.concurrent.connections", str(_DEFAULT_MAX_CONCURRENT_CONNECTIONS))
    )
    return max(1, _resolve_daemon_reservation(options, slot_count) - 1)


def _option_bool(options: dict[str, str], name: str, default: bool) -> bool:
    value = options.get(name)
    if value is None or not value.strip():
        # A blank is a setting not made.
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes"}:
        return True
    if normalized in {"0", "false", "no"}:
        return False
    # 62: Option '{}' must be one of: 1, true, yes, 0, false, no
    raise ValueError(_m(62, name))


def _pad_varchar_option(options: dict[str, str]) -> bool:
    # Docstring: padVarchar, under either spelling: Spark lowercases option keys, and a
    # copy of the options into a plain dict (a drain or shard reader's) keeps only that
    # form. An invalid value is reported under the option's documented name.
    value = options.get("padvarchar", options.get("padVarchar"))
    return _option_bool({} if value is None else {"padVarchar": value}, "padVarchar", False)


def _bridge_config(options: dict[str, str]) -> dict[str, Any]:
    required = ("hostname", "database", "user", "password", "server")
    missing = [name for name in required if not options.get(name)]
    if missing:
        # 61: Missing required Informix option(s): {}
        raise ValueError(_m(61, (", ".join(missing))))
    if _BARE_IDENTIFIER.fullmatch(str(options["database"])) is None:
        # The database goes into every statement as it is, before the colon: it cannot be
        # quoted there as an owner, table or column name is.
        # 447: Option 'database' must be a plain Informix database name (letters, digits,
        # underscores and $, not starting with a digit): {!r}
        raise ValueError(_m(447, options["database"]))
    return {
        "hostname": options["hostname"],
        "port": int(options.get("port") or 9088),
        "database": options["database"],
        "user": options["user"],
        "password": options["password"],
        "server": options.get("server"),
        "db_locale": (
            # Spark hands option keys to the Python Data Source lowercased (its
            # JVM CaseInsensitiveStringMap stores them that way), so a user's
            # ``DB_LOCALE`` arrives as ``db_locale``. Check the lowercased form
            # first; keep the original-case and dotted spellings as fallbacks.
            options.get("db_locale")
            or options.get("DB_LOCALE")
            or options.get("db.locale")
            or "en_US.819"
        ),
        "client_locale": (
            options.get("client_locale")
            or options.get("CLIENT_LOCALE")
            or options.get("client.locale")
            or "en_US.utf8"
        ),
        "tls": _option_bool(options, "encrypt", True),
        "ca_file": options.get("ssl.ca.file"),
        # As with the locales above (see _pad_varchar_option).
        "pad_varchar": _pad_varchar_option(options),
        "cdc_timeout": int(options.get("cdc.timeout", "5")),
        "cdc_max_records": int(options.get("cdc.max.records", str(_DEFAULT_CDC_MAX_RECORDS))),
        "stop_logging_on_close": False,
    }


def _snapshot_scan_columns(raw: object) -> list[tuple[str, bool | None]] | None:
    # Docstring: The columns ``snapshot.scan=keyset(a, b desc)`` names for a keyless table's
    # snapshot to page by, each with its direction (True is descending, None is unsaid), or
    # None for any other value. An empty list or a malformed column is no such value.

    match = re.fullmatch(r"keyset\s*\((.*)\)", str(raw).strip(), re.IGNORECASE | re.DOTALL)
    if match is None:
        return None
    columns: list[tuple[str, bool | None]] = []
    for part in match.group(1).split(","):
        words = part.split()
        direction = words[1].lower() if len(words) == 2 else None
        if not 1 <= len(words) <= 2 or direction not in (None, "asc", "desc"):
            return None
        columns.append((words[0], None if direction is None else direction == "desc"))
    # A column named twice names no index, and would be both NULL and not in one read.
    if len({name.casefold() for name, _direction in columns}) != len(columns):
        return None
    return columns


def _snapshot_scan_value(raw: object) -> str:
    # Docstring: Normalise and validate a ``snapshot.scan`` value; see ``_SNAPSHOT_SCAN_OPTION``.
    #
    # Shared by construction (connection scope), registration (table scope) and the
    # snapshot read itself, so a malformed value fails before any read.

    value = str(raw).strip().lower()
    if _snapshot_scan_columns(raw) is not None:
        return _SNAPSHOT_SCAN_KEYSET
    if value not in _SNAPSHOT_SCANS:
        # 63: Option '{}' must be one of: {}, {}
        raise ValueError(
            _m(
                63,
                _SNAPSHOT_SCAN_OPTION,
                f"{_SNAPSHOT_SCAN_KEYSET}, keyset(column [asc|desc], ...)",
                _SNAPSHOT_SCAN_CURSOR,
            )
        )
    return value


class _Descending:
    # Docstring: A key value of a descending index column, ordered as the index orders it.

    __slots__ = ("value",)

    def __init__(self, value: Any) -> None:
        self.value = value

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _Descending) and self.value == other.value

    def __lt__(self, other: "_Descending") -> bool:
        return self.value > other.value

    def __le__(self, other: "_Descending") -> bool:
        return self.value >= other.value

    __hash__ = None  # type: ignore[assignment]


def _framework_value(value: Any) -> Any:
    # The shared Spark Python Data Source parser accepts ISO strings for DateType
    # and TimestampType, but rejects a native datetime.date.  Normalize both
    # temporal Python objects at the connector boundary for consistent snapshot
    # and CDC behavior.
    if isinstance(value, (date, datetime)):
        if value.year <= 1 and isinstance(value, datetime) and value.tzinfo is None:
            # A year-1 timestamp built here rather than decoded (a transform's TIMESTAMP
            # fallback) is tagged UTC as the decoders tag theirs (see cdc_protocol): naive,
            # next to datetime.min, it overflows PySpark's time zone conversion.
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    return value


def _index_scan_key(row: dict[str, Any], keys: Sequence[str]) -> tuple:
    # Docstring: A row's index key as an index-ordered snapshot compares it: the values the
    # connector emits, a character one without the padding a CHAR carries. A row with a NULL
    # in an index column has the empty key: SQL compares a NULL with nothing, so those rows
    # are one group, read by IS NULL ahead of the rest, and the empty key sorts first.

    values = tuple(
        value.rstrip(" ") if isinstance(value, str) else value
        for value in (_framework_value(row[key]) for key in keys)
    )
    return () if None in values else values


def _index_scan_key_text(values: Sequence[Any]) -> str:
    # Docstring: Key values as the scan's moved-row index names them.
    #
    # A number by its value, as Informix compares it: as text, the 7 a scan recorded and
    # the 7.00 of the same row after its key column was widened to DECIMAL(12,2) were two
    # keys, and the stream missed the row's entry. An integer keeps its form, and so does
    # one an INTEGER key widened to FLOAT decodes as (7.0).

    def value(item: Any) -> Any:
        if isinstance(item, float) and math.isfinite(item) and item.is_integer():
            return int(item)
        if isinstance(item, Decimal) and item.is_finite():
            if item == item.to_integral_value():
                return int(item)
            with localcontext() as context:
                context.prec = 80
                return format(item.normalize(), "f")
        return item

    return json.dumps([value(item) for item in values], default=str)


_VARIABLE_DECIMAL_DEFAULT = "decimal(38,18)"

_DECIMAL_TYPE_RE = re.compile(r"^decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$")


def _parse_variable_decimal_type(spec: str, *, option: str) -> tuple[str, int | None, int | None]:
    # Docstring: Normalize a variable-DECIMAL target-type spec.
    #
    # Accepts ``string``, ``double``, ``integer`` or ``decimal(p,s)`` and returns a
    # ``(kind, precision, scale)`` tuple where ``kind`` is one of ``string``,
    # ``double``, ``integer`` or ``decimal``.  ``precision``/``scale`` are populated
    # only for ``decimal``.

    normalized = spec.strip().lower()
    if normalized in {"string", "double", "integer"}:
        return normalized, None, None
    match = _DECIMAL_TYPE_RE.match(normalized)
    if match:
        precision, scale = int(match.group(1)), int(match.group(2))
        if not (1 <= precision <= 38 and 0 <= scale <= precision):
            # 338: Option '{}' decimal(p,s) requires 1<=p<=38 and 0<=s<=p, got decimal({},{})
            raise ValueError(_m(338, option, precision, scale))
        return "decimal", precision, scale
    # 339: Option '{}' must be one of: string, double, integer, decimal(p,s); got {!r}
    raise ValueError(_m(339, option, spec))


def _variable_decimal_column_overrides(
    options: dict[str, str],
) -> dict[str, tuple[str, int | None, int | None]]:
    # Docstring: Parse ``decimal.variable.column.type`` into a per-column target map.

    raw = options.get("decimal.variable.column.type", "").strip()
    if not raw:
        return {}
    overrides: dict[str, tuple[str, int | None, int | None]] = {}
    # Entries are comma-separated, but decimal(p,s) also contains a comma; split
    # only on commas that are not inside parentheses.
    for entry in re.split(r",(?![^(]*\))", raw):
        entry = entry.strip()
        if not entry:
            continue
        name, sep, spec = entry.partition(":")
        name = name.strip()
        if not sep or not name:
            # 340: Option 'decimal.variable.column.type' entries must be 'column:type' pairs; got
            # {!r}
            raise ValueError(_m(340, entry))
        if name in overrides:
            # The later entry won, quietly.
            # 459: Option 'decimal.variable.column.type' names column {} more than once
            raise ValueError(_m(459, name))
        overrides[name] = _parse_variable_decimal_type(spec, option="decimal.variable.column.type")
    return overrides


def _variable_decimal_target(
    column: Column, options: dict[str, str]
) -> tuple[str, int | None, int | None]:
    # Docstring: Resolve the target type for one variable-scale DECIMAL/NUMERIC column.
    #
    # A per-column ``decimal.variable.column.type`` entry wins over the global
    # ``decimal.variable.type``; the default is ``decimal(38,18)``.

    overrides = _variable_decimal_column_overrides(options)
    if column.name in overrides:
        return overrides[column.name]
    return _parse_variable_decimal_type(
        options.get("decimal.variable.type", _VARIABLE_DECIMAL_DEFAULT),
        option="decimal.variable.type",
    )


def _is_variable_scale_decimal(column: Column) -> bool:
    name = column.type_name.split("(", 1)[0].strip()
    return name in {"DECIMAL", "NUMERIC"} and column.scale == 0xFF


def _coerce_variable_decimal_value(
    value: Any,
    column: Column,
    target: tuple[str, int | None, int | None],
) -> Any:
    # Docstring: Coerce one variable-scale DECIMAL value to its resolved target type.

    kind, precision, scale = target
    if not isinstance(value, Decimal):
        try:
            value = Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError) as error:
            # 355: column '{}' value {!r} is not a valid decimal
            raise InformixError(_m(355, column.name, value)) from error
    if kind == "string":
        # The packed form keeps the zeros of its unused base-100 digits (0 arrives as
        # 0E-134, 5 as 5.0000): the text is the number without them. A zero is "0"
        # whatever its sign, and the default context's 28 digits would round a
        # DECIMAL(32).
        if not value:
            return "0"
        with localcontext() as decimal_context:
            decimal_context.prec = 80
            return format(value.normalize(), "f")
    if kind == "double":
        return float(value)
    if kind == "integer":
        # Truncate toward zero (ANSI DECIMAL(p,0) semantics) then bounds-check.
        truncated = int(value.to_integral_value(rounding=ROUND_DOWN))
        if not -(1 << 63) <= truncated < (1 << 63):
            # 356: column '{}' value {} exceeds the 64-bit range of integer
            raise InformixError(_m(356, column.name, (format(value, "f"))))
        return truncated
    # decimal(p,s): quantize to the target scale and confirm it fits precision.
    quantum = Decimal(1).scaleb(-(scale or 0))
    try:
        # Not under the default context: its 28 digits cannot hold eleven integer digits
        # at scale 18, which decimal(38,18) can.
        with localcontext() as decimal_context:
            decimal_context.prec = 80
            quantized = value.quantize(quantum, rounding=ROUND_DOWN)
    except InvalidOperation as error:
        # 357: column '{}' value {} cannot be represented as decimal({},{})
        raise InformixError(_m(357, column.name, (format(value, "f")), precision, scale)) from error
    digits = quantized.as_tuple()
    integer_digits = max(len(digits.digits) + digits.exponent, 0)
    allowed_digits = (precision or 0) - (scale or 0)
    if integer_digits > allowed_digits:
        # 358: column '{}' value {} exceeds decimal({},{}) (integer digits {} > {})
        raise InformixError(
            _m(
                358,
                column.name,
                (format(value, "f")),
                precision,
                scale,
                integer_digits,
                allowed_digits,
            )
        )
    return quantized


def _coerce_variable_decimal_values(
    row: dict[str, Any], table: Table, options: dict[str, str]
) -> dict[str, Any]:
    variable_columns = [column for column in table.columns if _is_variable_scale_decimal(column)]
    if not variable_columns:
        return row
    result = dict(row)
    for column in variable_columns:
        value = result.get(column.name)
        if value is None:
            continue
        target = _variable_decimal_target(column, options)
        result[column.name] = _coerce_variable_decimal_value(value, column, target)
    return result


def _framework_row(row: dict[str, Any]) -> dict[str, Any]:
    return {name: _framework_value(value) for name, value in row.items()}


def _index_scan_row_key(
    row: dict[str, Any], table: Table, options: dict[str, str], names: Sequence[str] | None = None
) -> str:
    # Docstring: The primary.keys values of a source row, as a staged row holds them; or
    # those of ``names``, the key columns a scan recorded.
    names = table.primary_keys if names is None else names
    # Only the key columns are shaped: a wide row costs no more than a narrow one.
    shaped = _framework_row(
        _coerce_variable_decimal_values({name: row[name] for name in names}, table, options)
    )
    return _index_scan_key_text([shaped[name] for name in names])


def _index_scan_lookups(value_sets: Iterable[tuple]) -> list[tuple]:
    # Docstring: The sets of values to read rows again by, each once as Informix compares
    # them: a string without its trailing spaces.
    #
    # Two sets that differ only in those ('abc' and 'abc ') were two SELECTs of one
    # statement, each of which returned the other's row too: every row came back twice, and
    # the scan never settled them (444).

    unique: dict[tuple, tuple] = {}
    for values in value_sets:
        key = tuple(value.rstrip(" ") if isinstance(value, str) else value for value in values)
        unique.setdefault(key, values)
    return list(unique.values())


def _index_scan_staged_key(row: dict[str, Any], names: Sequence[str]) -> str:
    # Docstring: The same of a staged row: _index_scan_row_key of the row it was shaped from.
    return _index_scan_key_text([row[name] for name in names])


class _ScanReads:
    # Docstring: The bridge as an index-ordered scan reads it: each read retried in place
    # on a dropped connection (_snapshot_read_with_reconnect), as the other snapshot reads
    # and the CDC polls are. A read advances nothing until it returns: the scan samples
    # the log around its reads, and sorts a poll's changes after it.

    # The clock too: its reconnect, outside its own best-effort catch, failed a scan at the
    # finish line on a blip after every page was read.
    _READS = frozenset(
        {
            "boundary_lsn",
            "current_lsn",
            "current_utc_time",
            "key_rows",
            "read_changes",
            "rows_by_values",
            "snapshot_chunk",
        }
    )

    # The reads whose rows' primary.keys values are checked after them (see read).
    _ROWS = frozenset({"key_rows", "rows_by_values", "snapshot_chunk"})

    def __init__(self, connector: Any, table: Table) -> None:
        self._connector, self._table = connector, table

    def __getattr__(self, name: str) -> Any:
        attribute = getattr(self._connector._bridge, name)
        if name not in self._READS:
            return attribute

        def read(*args: Any, **kwargs: Any) -> Any:
            result = self._connector._snapshot_read_with_reconnect(
                lambda: attribute(*args, **kwargs), table=self._table, operation=f"scan {name}"
            )
            # The keys of the rows read too, where primary.keys gives the table some: a moved
            # row is found by them and taken out of the pages, and a value string.null.byte
            # changed found another row (the bridge checks the index columns it pages by).
            unaltered = getattr(self._connector._bridge, "_unaltered", None)
            if unaltered is not None and self._table.primary_keys and name in self._ROWS:
                unaltered(self._table.primary_keys, self._table.identity)
            return result

        return read


def _moved_rows_of_page(moved: dict[str, Any] | None, page_index: int) -> dict[str, Any] | None:
    # Docstring: What a staged page leaves out, of the rows its index-ordered scan read again.
    #
    # ``moved`` is the manifest's record of them (see _index_scan_snapshot): the rows of a
    # table keyed by primary.keys that changed index value during the scan are left out of
    # the pages read under an index value they had, and are in the page that read holds.
    # None for a page that holds none of them.
    listed = moved and moved["pages"].get(str(page_index))
    return {"keys": moved["keys"], "rows": listed} if listed else None


def _without_moved_rows(
    rows: Iterator[dict[str, Any]], left_out: dict[str, Any] | None
) -> Iterator[dict[str, Any]]:
    # Docstring: A staged page's rows without those _moved_rows_of_page names.
    if not left_out:
        return rows
    skipped, names = frozenset(left_out["rows"]), left_out["keys"]
    return (row for row in rows if _index_scan_staged_key(row, names) not in skipped)


class _PadSpace:
    # Docstring: A key's text compared as Informix compares CHAR, VARCHAR and LVARCHAR: as
    # if the shorter were padded with blanks.
    #
    # Python compares by prefix, so "a\n" came after "a", where Informix puts it first (a
    # newline is below a blank), and "a " after "a", which Informix holds equal. An
    # incremental copy that took a key for past its bound stopped there, and the rows
    # after it were never copied. By code point, which is Informix's byte order in ISO
    # 8859-1 and UTF-8 databases; NCHAR and NVARCHAR, which collate by locale, are not
    # paged in Python at all (_incremental_datetime_chunk_exprs).

    __slots__ = ("text",)

    def __init__(self, text: str) -> None:
        self.text = text

    def _compare(self, other: object) -> int:
        other_text = other.text if isinstance(other, _PadSpace) else other
        if not isinstance(other_text, str):
            return NotImplemented  # type: ignore[return-value]
        width = max(len(self.text), len(other_text))
        mine, theirs = self.text.ljust(width), other_text.ljust(width)
        return (mine > theirs) - (mine < theirs)

    def __eq__(self, other: object) -> bool:
        result = self._compare(other)
        return result is not NotImplemented and result == 0

    def __hash__(self) -> int:
        return hash(self.text.rstrip(" "))

    # Against what isn't text (a NULL key's _NullLowest) NotImplemented goes back, so the
    # reflected operation decides.
    def __lt__(self, other: object) -> bool:
        result = self._compare(other)
        return result if result is NotImplemented else result < 0

    def __le__(self, other: object) -> bool:
        result = self._compare(other)
        return result if result is NotImplemented else result <= 0

    def __gt__(self, other: object) -> bool:
        result = self._compare(other)
        return result if result is NotImplemented else result > 0

    def __ge__(self, other: object) -> bool:
        result = self._compare(other)
        return result if result is NotImplemented else result >= 0


def _index_scan_order(key: Sequence[Any], descending: Sequence[bool]) -> tuple:
    # Text blank-padded, as Informix orders it (see _PadSpace).
    return tuple(
        _Descending(_PadSpace(value) if isinstance(value, str) else value)
        if down
        else (_PadSpace(value) if isinstance(value, str) else value)
        for value, down in zip(key, descending)
    )


def _index_scan_plain(order: Sequence[Any]) -> list:
    def plain(value: Any) -> Any:
        if isinstance(value, _Descending):
            value = value.value
        return value.text if isinstance(value, _PadSpace) else value

    return [plain(value) for value in order]


def _state_gc_settings(options: dict[str, str]) -> tuple[int, float]:
    # Docstring: state.gc.retention.days and state.gc.interval.hours, checked.

    try:
        retention_days = int(
            options.get("state.gc.retention.days", _DEFAULT_STATE_GC_RETENTION_DAYS)
        )
        interval_hours = float(
            options.get("state.gc.interval.hours", _DEFAULT_STATE_GC_INTERVAL_HOURS)
        )
    except (TypeError, ValueError) as error:
        # 320: Options 'state.gc.retention.days' and 'state.gc.interval.hours' must be numeric
        raise ValueError(_m(320)) from error
    if retention_days < 1 or not math.isfinite(interval_hours) or interval_hours <= 0:
        # 321: Option 'state.gc.retention.days' must be >= 1 and 'state.gc.interval.hours' must
        # be > 0
        raise ValueError(_m(321))
    if interval_hours >= retention_days * 24:
        # 460: Option 'state.gc.interval.hours' ({}) must be shorter than
        # 'state.gc.retention.days' ({} days): a sweep could then remove a live table's state
        raise ValueError(_m(460, options.get("state.gc.interval.hours"), retention_days))
    return retention_days, interval_hours


def _state_gc_project_settings(options: dict[str, str]) -> None:
    # Docstring: Refuse state.gc.drop.unused.projects=true.
    #
    # With it on, a reader listed every Lakebase project in the workspace, connected to each
    # auto-named connector project, waking its endpoint, and deleted those idle past
    # state.gc.project.retention.days: other servers' and environments' projects too, where
    # the identity could. The Lakebase project cleanup tool does that now, on request, with
    # a dry run first; set on a pipeline, the option fails rather than going unheeded.

    try:
        drop = _option_bool(options, "state.gc.drop.unused.projects", False)
    except ValueError as error:
        # 323: Option 'state.gc.drop.unused.projects' must be true or false
        raise ValueError(_m(323)) from error
    if drop:
        # 324: Option 'state.gc.drop.unused.projects' no longer deletes Lakebase projects from
        # a pipeline's reads; remove it, and run the Lakebase project cleanup tool (see the
        # README) to find and delete unused projects
        raise ValueError(_m(324))


# The whole-number options a table may set, as (name, default, minimum, maximum) the way
# the reads take them (InformixLakeflowConnect._table_int_option).
_TABLE_INT_OPTIONS = (
    ("snapshot.page.size", _DEFAULT_SNAPSHOT_PAGE_SIZE, 1, None),
    ("keyless.snapshot.page.size", _DEFAULT_KEYLESS_SNAPSHOT_PAGE_SIZE, 1, None),
    ("snapshot.max.rows", 0, 0, None),
    ("snapshot.max.bytes", 0, 0, None),
    ("cdc.timeout", 5, 1, None),
    ("cdc.max.records", _DEFAULT_CDC_MAX_RECORDS, 1, 256),
    ("max.records.per.batch", _DEFAULT_MAX_RECORDS_PER_BATCH, 1, None),
    (_UNL_GZIP_RECORDS_OPTION, 0, 0, None),
    (_UNL_SPLIT_BYTES_OPTION, _UNL_SPLIT_TARGET_BYTES, 1, None),
)


def _parsed_option(options: dict[str, str], name: str, default: Any, kind: type = int) -> Any:
    # Docstring: ``options[name]`` (or ``default``) as ``kind``, failing with the option's
    # name rather than Python's "invalid literal for int()".

    try:
        return kind(options.get(name, default) if options.get(name) not in (None, "") else default)
    except (TypeError, ValueError) as error:
        noun = "a whole number" if kind is int else "a number"
        raise ValueError(f"Option '{name}' must be {noun}") from error


def _validate_nonneg_float_option(options: dict[str, str], name: str) -> None:
    # Docstring: Raise if ``name`` is present and not a number >= 0. Absent means the default.

    if name not in options:
        return
    try:
        value = float(options[name])
        # nan and inf passed "< 0" and failed later, inf as a sleep of nan.
        if not math.isfinite(value) or value < 0:
            raise ValueError
    except (TypeError, ValueError) as error:
        # 64: Option '{}' must be a number >= 0
        raise ValueError(_m(64, name)) from error


# How a decoded string containing an embedded NUL (\x00) is sanitized. A NUL is genuine
# content -- the decoders already trim real (space) padding -- but many downstream
# consumers cannot store it, so the default converts such a value to NULL (honest and
# detectable) rather than silently stripping it. "empty" strips the NUL characters;
# "keep" passes them through unchanged.
_NULL_BYTE_OPTION = "string.null.byte"
_NULL_BYTE_MODES = {"null", "empty", "keep"}
_DEFAULT_NULL_BYTE_MODE = "null"


def _null_byte_mode(options: dict[str, str]) -> str:
    # Docstring: Resolve the ``string.null.byte`` option to ``null`` (default), ``empty``, or
    # ``keep``.

    raw = options.get(_NULL_BYTE_OPTION)
    if raw is None:
        return _DEFAULT_NULL_BYTE_MODE
    mode = raw.strip().lower()
    if mode == "empty_string":
        mode = "empty"
    if not mode:
        return _DEFAULT_NULL_BYTE_MODE
    if mode not in _NULL_BYTE_MODES:
        # 65: Option '{}' must be one of: null, empty, keep
        raise ValueError(_m(65, _NULL_BYTE_OPTION))
    return mode


def _append_only_value(raw: str) -> str:
    # Docstring: Normalize the tri-state ``append.only.ingestion`` option.
    #
    # ``true``/``false`` force append (or not) for every capturable table; ``auto``
    # (also the default when the option is unset) applies append only to keyless
    # tables and leaves keyed tables on their normal CDC/snapshot path.

    normalized = (raw or "").strip().lower()
    if normalized in {"1", "true", "yes"}:
        return "true"
    if normalized in {"0", "false", "no"}:
        return "false"
    if normalized == "auto":
        return "auto"
    # 66: Option '{}' must be one of: true, false, auto (or 1/yes, 0/no)
    raise ValueError(_m(66, _APPEND_INGESTION_OPTION))


_PRIMARY_KEYS_OPTION = "primary.keys"
# ``primary.keys: "*"`` keys a table on every column, in catalog column order, so the whole
# row is its identity. Meant for a keyless table whose rows are unique as a whole, such as
# a junction or lookup table. It is shorthand for listing every column, except that the key
# follows the schema: an added column joins the key. For a table already streaming changes,
# the schema-transition check accepts exactly that growth -- the old key followed by the
# appended nullable columns -- so no full refresh is needed. Rows written before the ALTER
# hold NULL in the new column at the source, and the target gives them NULL when it gains
# the column, so their updates and deletes still match by the longer key. The check fails
# closed instead when an added column has a default (existing rows would carry it at the
# source but NULL in the target) or while an incremental copy is still in progress (its
# stored chunk position is laid out by the old key).
#
# A dropped column leaves the key the same way: the check accepts the old key
# minus exactly the dropped columns. Informix returns records logged before the ALTER
# without the column, and the target keeps it as an ordinary column, so changes match by
# the shorter key (probe-verified: AUTO CDC accepts a key that shrinks between updates).
# Only the first read of a new update may shrink it -- a running update still merges on
# the key it started with -- and never in the same step as a growth. Rows that differed
# only in the dropped column become one key, like exact duplicates.
#
# Nullable columns join the key like any other. On the keyed CDC path a delete carries a
# NULL key value through, and the pipeline's AUTO CDC delete flow matches it (see
# _shape_delete), so updates and deletes of a row holding NULL apply normally -- under a
# wildcard every update changes the key and deletes the old row. What a NULL can still
# break is keyset snapshot paging, which compares key values in SQL and in Python;
# snapshot.scan=cursor avoids it, and the per-page sort an all-column key usually costs.
# On the append path with snapshot.mode=cdc_only nothing pages by key, so a NULL there is
# harmless; its default, incremental, takes initial's snapshot, which pages by key like any
# other.
_PRIMARY_KEYS_WILDCARD = "*"
# ``"* except(c1, c2)"`` keys the table on every column but the ones named, which suits a
# table whose rows are unique as a whole bar a few columns that change or are too wide to
# key on (an audit timestamp, a free-text note). It follows the schema like the bare
# wildcard: a column added later joins the key unless it is named, and one dropped leaves
# it. Every name must be a column of the table, so a misspelt one fails instead of
# keeping its column in the key (see _ensure_excepted_columns_exist). EXCEPT and the
# names are matched case-insensitively (see _wildcard_key).
_PRIMARY_KEYS_WILDCARD_EXCEPT = re.compile(r"\*\s*except\s*\((?P<names>.*)\)", re.I | re.S)
# When a table has no primary-key constraint, promote a UNIQUE index to stand in as the
# primary key, so a physically key-less table becomes keyed (resumable keyset snapshots and
# CDC dedup) instead of falling back to the one-shot key-less drain. Default on. An explicit
# ``primary.keys`` option still wins over the discovered index. Set false to keep the old
# behaviour (only a real primary key makes a table keyed).
_PROMOTE_UNIQUE_INDEX_OPTION = "primary.key.from.unique.index"
# Per-table opt-in: when a table has no primary key and no all-NOT-NULL unique index, allow a
# UNIQUE index that includes nullable column(s) to stand in as the key. The operator asserts
# those columns are effectively unique for every row. A row that does hold NULL in one of
# them is still updated and deleted correctly: as for a ``primary.keys`` override, a delete
# carries the NULL through and the pipeline's delete flow matches it (see _shape_delete).
# What a NULL can still break is keyset snapshot paging, which compares key values and can
# skip the row or fail; snapshot.scan=cursor avoids that. Default false (stay key-less).
_ALLOW_NULLABLE_INDEX_OPTION = "allow.nullable.index"

# The options the connector takes from a table's own options only, never from the
# connection's (see InformixLakeflowConnect._refuse_connection_table_options).
_TABLE_ONLY_OPTIONS = (
    # Which table the configuration is for: on the connection, the reads read it while the
    # metadata read the pipeline's source_table.
    "qualified_source_table",
    "decimal.variable.type",
    "decimal.variable.column.type",
    "snapshot.filter",
    "row_filter",
    "include_columns",
    "exclude_columns",
    "transform_columns",
    "rename_columns",
    "snapshot.isolation",
    "primary.keys",
    "allow.nullable.index",
)


class _DescendingKey:
    # Docstring: Order one key-column value descending inside a tuple comparison.
    #
    # Index-native paging over a mixed-direction key compares tuples in the
    # index's order, not Python's all-ascending default. Wrapping the values of
    # the DESC columns flips just those positions; ASC columns stay bare. A
    # wrapper is only ever compared against another wrapper at the same tuple
    # position (same column, same type), so comparing the underlying values is
    # well-defined.

    __slots__ = ("value",)

    def __init__(self, value: Any) -> None:
        self.value = value

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _DescendingKey) and self.value == other.value

    def __ne__(self, other: object) -> bool:
        return not self.__eq__(other)

    def __lt__(self, other: "_DescendingKey") -> bool:
        return other.value < self.value

    def __le__(self, other: "_DescendingKey") -> bool:
        return other.value <= self.value

    def __gt__(self, other: "_DescendingKey") -> bool:
        return other.value > self.value

    def __ge__(self, other: "_DescendingKey") -> bool:
        return other.value >= self.value

    def __hash__(self) -> int:
        return hash(self.value)


class _NullLowest:
    # Docstring: A NULL key value where Informix sorts it: below every value, so first in
    # an ascending column and, wrapped in _DescendingKey, last in a descending one.
    #
    # Python refuses to compare None with a value, so a page of a nullable key
    # (allow.nullable.index, primary.keys) holding a NULL failed its bound check with a
    # TypeError.

    __slots__ = ()

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _NullLowest)

    def __ne__(self, other: object) -> bool:
        return not isinstance(other, _NullLowest)

    def __hash__(self) -> int:
        return 0

    def __lt__(self, other: object) -> bool:
        return not isinstance(other, _NullLowest)

    def __le__(self, other: object) -> bool:
        return True

    def __gt__(self, other: object) -> bool:
        return False

    def __ge__(self, other: object) -> bool:
        return isinstance(other, _NullLowest)


_NULL_LOWEST = _NullLowest()


def _order_key(values: Sequence[Any], descending: Sequence[bool]) -> tuple:
    # Docstring: Map a key tuple to a comparison key honoring per-column direction.
    #
    # Empty or all-ascending ``descending`` returns the tuple unchanged, so the
    # common (all-ascending lexicographic) case keeps Python's native order and is
    # unaffected. Otherwise DESC columns are wrapped so tuple comparison reproduces
    # the index's own scan order.

    # A decoded year-1 DATETIME carries UTC and every other one no zone, and Python
    # refuses to compare the two: the zone is dropped, the wall clock being the value.
    values = [
        value.replace(tzinfo=None)
        if isinstance(value, datetime) and value.tzinfo is not None
        else value
        for value in values
    ]
    # Text in Informix's order: blank-padded (see _PadSpace); a NULL below every value.
    values = [
        _PadSpace(value) if isinstance(value, str) else (_NULL_LOWEST if value is None else value)
        for value in values
    ]
    if not descending or not any(descending):
        return tuple(values)
    return tuple(
        _DescendingKey(value) if is_desc else value for value, is_desc in zip(values, descending)
    )


def _select_unique_index_key(
    indexes: list[tuple[str, list[int]]],
    column_by_number: dict[int, str],
    nullable_by_name: dict[str, bool],
    allow_nullable: bool = False,
) -> list[tuple[str, bool]]:
    # Docstring: Choose a UNIQUE index to stand in for an absent primary key.
    #
    # ``indexes`` is ``(idxname, [part1..part16])`` per unique index; the parts are
    # ``syscolumns`` column numbers in key order, terminated by a ``0`` (a negative number
    # is a descending column -- still part of the key, so its magnitude names the column).
    #
    # By default only indexes whose every column is NOT NULL are eligible: a primary key must
    # be non-null for keyset pagination (``WHERE key > ?``) and CDC dedup to be sound, exactly
    # as a real primary key is, and a NULL key value would silently drop or mis-order rows.
    # ``allow_nullable`` relaxes that -- the operator asserts the columns are effectively
    # unique and non-null -- so an index with a nullable column can stand in. An index
    # referencing a column not present in the projection is skipped either way.
    #
    # Among the eligible indexes, choose the one with the fewest columns; break ties by index
    # name in ascending lexical order (deterministic and independent of catalog row order).
    # Returns the ordered key as ``(column, descending)`` pairs -- ``descending`` records the
    # per-column index direction so paging can match the index's own order -- or ``[]`` when
    # no unique index qualifies (the table stays key-less).

    candidates: list[tuple[int, str, list[tuple[str, bool]]]] = []
    for idxname, parts in indexes:
        columns: list[tuple[str, bool]] = []
        eligible = True
        for part in parts:
            number = int(part)
            if number == 0:
                break  # key columns are contiguous; the rest of the parts are padding
            name = column_by_number.get(abs(number))
            if name is None:
                eligible = False  # references a column we cannot name -> unusable
                break
            if not allow_nullable and nullable_by_name.get(name, True):
                eligible = False  # nullable column -> not a safe primary key unless allowed
                break
            columns.append((name, number < 0))  # negative part number == descending column
        if eligible and columns:
            candidates.append((len(columns), str(idxname), columns))
    if not candidates:
        return []
    return min(candidates, key=lambda candidate: (candidate[0], candidate[1]))[2]


def _parse_key_columns(raw: str, option: str = _PRIMARY_KEYS_OPTION) -> list[str]:
    # Docstring: Parse a column-list option -- a comma-separated list or a JSON array.

    text = (raw or "").strip()
    if not text:
        return []
    if text.startswith("["):
        try:
            values = json.loads(text)
        except json.JSONDecodeError as error:
            # 67: Option '{}' is not a valid JSON array: {!r}
            raise ValueError(_m(67, option, text)) from error
        items = [str(value).strip() for value in values]
    else:
        items = [item.strip() for item in text.split(",")]
    return [item for item in items if item]


def _key_override(
    table_options: dict[str, str],
) -> tuple[list[str], tuple[str, ...]] | None:
    # Docstring: The ``primary.keys`` option, parsed and checked, as (keys, excepted): the
    # column names, or ``["*"]`` for the wildcard with the names its ``except(...)`` leaves
    # out (none for a bare ``*``); None when unset.
    #
    # Parsed once per table resolve and shared by _requested_key and the override itself,
    # so a malformed value fails before any column is left out. The except form is matched
    # before the comma split, since its own list holds commas; a JSON array may hold it as
    # its one element.

    raw = table_options.get(_PRIMARY_KEYS_OPTION)
    if raw is None:
        return None
    text = str(raw).strip()
    if text.startswith("["):
        items = _parse_key_columns(text)
        text = items[0] if len(items) == 1 else text
    match = _PRIMARY_KEYS_WILDCARD_EXCEPT.fullmatch(text)
    if match:
        excepted = _parse_key_columns(match.group("names"))
        if not excepted:
            # 68: Option '{}' names no columns in '{} except(...)'
            raise ValueError(_m(68, _PRIMARY_KEYS_OPTION, _PRIMARY_KEYS_WILDCARD))
        if len(excepted) != len({name.casefold() for name in excepted}):
            # 69: Option '{}' excepts a column twice
            raise ValueError(_m(69, _PRIMARY_KEYS_OPTION))
        return [_PRIMARY_KEYS_WILDCARD], tuple(excepted)
    keys = _parse_key_columns(raw)
    if not keys:
        # 70: Option '{}' is set but lists no columns
        raise ValueError(_m(70, _PRIMARY_KEYS_OPTION))
    if _PRIMARY_KEYS_WILDCARD in keys and keys != [_PRIMARY_KEYS_WILDCARD]:
        # 71: Option '{}' must be '{}' alone to key on every column, '{} except(c1, c2)' to key on
        # every column but those, or a list of column names
        raise ValueError(
            _m(71, _PRIMARY_KEYS_OPTION, _PRIMARY_KEYS_WILDCARD, _PRIMARY_KEYS_WILDCARD)
        )
    if len(keys) != len(set(keys)):
        # 72: Option '{}' lists a duplicate column
        raise ValueError(_m(72, _PRIMARY_KEYS_OPTION))
    return keys, ()


# row_filter keeps only the rows matching a predicate -- in snapshots, file-sourced
# snapshots, and the change stream alike -- and is named as in Lakeflow Connect. The
# connector parses it itself so the same predicate can be rendered to SQL for snapshots
# and evaluated in Python on CDC before- and after-images and on file records. That
# limits it to a grammar whose meaning is identical both ways: comparisons, IN, BETWEEN,
# LIKE, IS [NOT] NULL, AND/OR/NOT and parentheses, over numeric, string, BOOLEAN, DATE,
# and YEAR-anchored DATETIME columns, with SQL's three-valued NULL logic. Strings compare
# by code point with trailing spaces ignored, which is Informix's CHAR/VARCHAR order for
# ISO 8859-1 and UTF-8 code sets; NCHAR and NVARCHAR collate by locale, so they take
# equality tests and LIKE only. LIKE follows what Informix 14.10 was measured to do on
# every string type: case-sensitive, trailing blanks of the value and of the pattern
# ignored, and an escape character (backslash unless ESCAPE names another) that makes
# the next character literal. The SQL always spells the ESCAPE out, so the server's
# DEFAULTESCCHAR setting cannot change the meaning. FLOAT and SMALLFLOAT compare as
# Informix compares them (see _float_literal). An UPDATE that moves a
# row into the filter is upserted and one that moves it out is deleted, so a keyed
# target holds exactly the matching rows.
_ROW_FILTER_KEYWORDS = frozenset(
    {
        "AND",
        "OR",
        "NOT",
        "IN",
        "BETWEEN",
        "LIKE",
        "ESCAPE",
        "IS",
        "NULL",
        "TRUE",
        "FALSE",
        "DATE",
        "TIMESTAMP",
    }
)
_ROW_FILTER_LIKE_ESCAPE = "\\"

_ROW_FILTER_TOKEN = re.compile(
    r"\s*(?:(?P<number>-?\d+(?:\.\d+)?(?:[eE][+-]?\d{1,3}(?!\w))?)|(?P<string>'(?:[^']|'')*')"
    r"|(?P<op><>|!=|<=|>=|=|<|>)|(?P<punct>[(),])|(?P<word>[^\W\d][\w$]*)"
    # A column name in double quotes: one that holds "+" or ".", or is a word of SQL.
    r'|(?P<quoted>"(?:[^"]|"")+"))',
    re.UNICODE,
)
_ROW_FILTER_TEMPORAL = re.compile(
    r"(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,6}))?)?)?"
)

# Deterministic functions shared by row_filter and transform_columns: name -> (input
# kinds, literal argument kinds, result kind, filter). An argument kind ending in "?"
# is optional; "same" means the kind of the function's input. row_filter renders each
# function to Informix SQL for snapshots and evaluates it in Python on change images and
# file records, so the two must agree: every filter function's Python twin below was
# measured against Informix 14.10 (see the README). MASK, HASH, and NULLIFY run only in
# Python, where transform_columns applies them, so they are not filter functions.
# Functions whose result depends on the clock, the session, or the database locale's
# collation (TODAY, CURRENT, USER) are deliberately absent: a filter on them would
# change a row's membership without any change event to carry it.
_STRING_KINDS = ("string", "nstring")
_VALUE_KINDS = ("string", "nstring", "number", "bool", "date", "timestamp")
# FLOAT and SMALLFLOAT: a row_filter compares them bare, as Informix does (see
# _float_literal), and of the functions only NVL, COALESCE, HASH and NULLIFY in a transform
# take them; the numeric ones' twins were not checked against Informix on binary floats.
_FLOAT_KINDS = ("float", "smallfloat")
_DATE_KINDS = ("date", "timestamp")
_EXPRESSION_FUNCTIONS = {
    "UPPER": (_STRING_KINDS, (), "same", True),
    "LOWER": (_STRING_KINDS, (), "same", True),
    "TRIM": (_STRING_KINDS, (), "same", True),
    "LTRIM": (_STRING_KINDS, (), "same", True),
    "RTRIM": (_STRING_KINDS, (), "same", True),
    "LENGTH": (_STRING_KINDS, (), "number", True),
    "CHAR_LENGTH": (_STRING_KINDS, (), "number", True),
    "SUBSTR": (_STRING_KINDS, ("number", "number?"), "same", True),
    "REPLACE": (_STRING_KINDS, ("string", "string?"), "same", True),
    "LPAD": (_STRING_KINDS, ("number", "string"), "same", True),
    "RPAD": (_STRING_KINDS, ("number", "string"), "same", True),
    "NVL": (_VALUE_KINDS, ("same",), "same", True),
    "COALESCE": (_VALUE_KINDS, ("same",), "same", True),
    "ABS": (("number",), (), "number", True),
    "MOD": (("number",), ("number",), "number", True),
    "ROUND": (("number",), ("number?",), "number", True),
    "TRUNC": (("number",), ("number?",), "number", True),
    "YEAR": (_DATE_KINDS, (), "number", True),
    "MONTH": (_DATE_KINDS, (), "number", True),
    "DAY": (_DATE_KINDS, (), "number", True),
    "WEEKDAY": (_DATE_KINDS, (), "number", True),
    "MASK": (_STRING_KINDS, ("number?", "string?"), "string", False),
    "HASH": ((*_VALUE_KINDS, *_FLOAT_KINDS, "other"), (), "string", False),
    "NULLIFY": ((*_VALUE_KINDS, *_FLOAT_KINDS, "other"), (), "same", False),
}


def _row_filter_literal_text(literal: tuple) -> str:
    kind, value = literal
    if kind == "number":
        # Not under the default context: its 28 digits would round a longer literal, as
        # any fixed precision would one longer than it.
        with localcontext() as decimal_context:
            decimal_context.prec = max(200, len(value.as_tuple().digits))
            return format(value.normalize(), "f")
    if kind == "string":
        return "'" + value.replace("'", "''") + "'"
    if kind == "bool":
        return "TRUE" if value else "FALSE"
    if kind == "date":
        return f"DATE '{value.isoformat()}'"
    return f"TIMESTAMP '{value.isoformat(sep=' ')}'"


def _expression_text(operand: str | tuple) -> str:
    # Docstring: Canonical text of an operand: a column name or NAME(operand, literals).

    if isinstance(operand, str):
        # Delimited only where it would not parse back bare, a name with "+" or "." or a
        # keyword of the grammar: those could not be written before, so every filter
        # that could keeps its text, and its fingerprint.
        bare = _BARE_IDENTIFIER.fullmatch(operand) and operand.upper() not in _ROW_FILTER_KEYWORDS
        return operand if bare else '"' + operand.replace('"', '""') + '"'

    _, name, arguments = operand
    parts = [_expression_text(arguments[0])]
    parts += [_row_filter_literal_text(literal) for literal in arguments[1:]]
    return f"{name}({', '.join(parts)})"
