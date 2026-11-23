"""The bridge's reads: keys, log positions and snapshots."""

from __future__ import annotations

import contextlib
import logging
import time
from typing import Any, Sequence

from databricks.labs.community_connector.sources.informix.cdc_protocol import (
    cdc_routine,
    validate_snapshot_arity,
)
from databricks.labs.community_connector.sources.informix.informix_host_sharing import (
    _expect_zero,
    _field,
    _is_timeout_error,
    _sysmaster_database_name,
    _transport_closed,
)
from databricks.labs.community_connector.sources.informix.informix_keys import (
    _serialized_sqli_operation,
)
from databricks.labs.community_connector.sources.informix.informix_options import (
    _DEFAULT_CDC_READ_TIMEOUT_SECONDS,
    _DEFAULT_SNAPSHOT_READ_TIMEOUT_SECONDS,
    _IDENTIFIER,
    _INDEX_PARTS,
    _SNAPSHOT_ISOLATION_SQL,
    _UTC_TIME_REPROBE_SECONDS,
    _VALIDATE_LSN_RETRY_BASE_SECONDS,
    _VALIDATE_LSN_RETRY_MAX_RETRIES,
    _VALIDATE_LSN_RETRY_MAX_SECONDS,
    InformixError,
    _join_identity,
    _m,
    _split_identity,
    _sql_name,
)
from databricks.labs.community_connector.sources.informix.informix_unload import (
    _deep_size,
    _log_lsn,
)
from databricks.labs.community_connector.sources.informix.sqli_codec import (
    InformixDatetimeLiteral,
    SqliTransportError,
    add_informix_exception_note,
    reconnectable_error,
)


class _BridgeReadMixin:
    # Docstring: The bridge's reads: keys, log positions and snapshots.

    @_serialized_sqli_operation
    def table_indexes(self, identity):
        # Docstring: A table's indexes on plain columns, each ``(columns, descending, nunique,
        # nrows)`` with the columns in key order and the catalog's estimates of the index's
        # distinct keys and the table's rows, both 0 until statistics are run.

        self._ensure_connected()
        _database, owner, name = _split_identity(identity)
        max_bytes = self._metadata_max_bytes()
        by_number = {
            int(_field(row, "colno", 1)): str(_field(row, "colname", 0))
            for row in self.transport.execute(
                "SELECT c.colname, c.colno FROM systables t JOIN syscolumns c ON t.tabid = c.tabid "
                "WHERE t.tabtype = 'T' AND t.owner = ? AND t.tabname = ?",
                (owner, name),
                max_result_bytes=max_bytes,
            )
        }
        indexes = []
        for row in self.transport.execute(
            f"SELECT {_INDEX_PARTS}, i.nunique, t.nrows FROM systables t JOIN sysindexes i "
            "ON t.tabid = i.tabid WHERE t.tabtype = 'T' AND t.owner = ? AND t.tabname = ?",
            (owner, name),
            max_result_bytes=max_bytes,
        ):
            parts = [int(_field(row, f"part{position + 1}", position)) for position in range(16)]
            parts = parts[: parts.index(0)] if 0 in parts else parts
            if parts and all(abs(part) in by_number for part in parts):
                indexes.append(
                    (
                        [by_number[abs(part)] for part in parts],
                        [part < 0 for part in parts],
                        int(_field(row, "nunique", 16) or 0),
                        int(_field(row, "nrows", 17) or 0),
                    )
                )
        return indexes

    @_serialized_sqli_operation
    def buffered_logging(self) -> bool:
        # Docstring: Whether the database logs buffered: a commit is then visible to readers
        # before its log records are written, so before CDC can hand it over.

        self._ensure_connected()
        row = self.transport.execute(
            "SELECT is_buff_log FROM sysmaster:sysdatabases WHERE name = ?",
            (_sysmaster_database_name(self.config["database"]),),
        )[0]
        return bool(int(_field(row, "is_buff_log", 0) or 0))

    @_serialized_sqli_operation
    def key_rows(self, identity, columns, keys, values, isolation, limit=None, present=(), skip=0):
        # Docstring: Every row whose ``keys`` columns equal ``values``, or the first ``limit``
        # of them after the first ``skip``, read at ``isolation``. Without ``values``, every
        # row with a NULL in one of ``keys`` and none in the ``present`` columns.

        self._ensure_connected()
        database, owner, name = _split_identity(identity)
        for identifier in (database, owner, name, *columns, *keys, *present):
            if not _IDENTIFIER.fullmatch(identifier):
                # 43: Unsafe snapshot identifier {!r}
                raise InformixError(_m(43, identifier))
        with self._repeatable_read_transaction(isolation):
            rows = self.transport.execute(
                f"SELECT {f'SKIP {int(skip)} ' if skip else ''}"
                f"{f'FIRST {int(limit)} ' if limit else ''}"
                f"{','.join(_sql_name(column) for column in columns)} "
                f"FROM {database}:{_sql_name(owner)}.{_sql_name(name)} WHERE "
                + (
                    "("
                    + " OR ".join(f"{_sql_name(key)} IS NULL" for key in keys)
                    + ")"
                    + "".join(f" AND {_sql_name(key)} IS NOT NULL" for key in present)
                    if values is None
                    else " AND ".join(f"{_sql_name(key)} = ?" for key in keys)
                ),
                tuple(values or ()),
            )
            self._unaltered(keys, identity)
        return [
            dict(row) if isinstance(row, dict) else dict(zip(columns, row, strict=True))
            for row in rows
        ]

    @_serialized_sqli_operation
    def rows_by_values(self, identity, columns, keys, value_sets, isolation, limit=None):
        # Docstring: Every row whose ``keys`` columns equal one of ``value_sets``, a None
        # in one standing for a NULL, read at ``isolation`` in one query; for a single
        # set, the first ``limit`` of its rows. The sets must differ: a row is returned
        # once for each set it matches.

        self._ensure_connected()
        database, owner, name = _split_identity(identity)
        for identifier in (database, owner, name, *columns, *keys):
            if not _IDENTIFIER.fullmatch(identifier):
                # 43: Unsafe snapshot identifier {!r}
                raise InformixError(_m(43, identifier))
        if not value_sets:
            return []
        # One SELECT a set of values, joined by UNION ALL: Informix finds each by the
        # index. Asked for as one SELECT with the sets joined by OR, it reads the whole
        # table (probed on 14.10: SEQUENTIAL SCAN for the OR, INDEX PATH with a lower
        # index filter for each branch of the UNION ALL, a NULL's included). A NULL is
        # asked for as one: no value equals it. The limit is for a single set, which
        # may be an index value alone; a set that names a row by its key is small.
        select = (
            f"SELECT {f'FIRST {int(limit)} ' if limit and len(value_sets) == 1 else ''}"
            f"{','.join(_sql_name(column) for column in columns)} "
            f"FROM {database}:{_sql_name(owner)}.{_sql_name(name)} WHERE "
        )
        with self._repeatable_read_transaction(isolation):
            rows = self.transport.execute(
                " UNION ALL ".join(
                    select
                    + "("
                    + " AND ".join(
                        f"{_sql_name(key)} " + ("IS NULL" if value is None else "= ?")
                        for key, value in zip(keys, values, strict=True)
                    )
                    + ")"
                    for values in value_sets
                ),
                tuple(value for values in value_sets for value in values if value is not None),
            )
            self._unaltered(keys, identity)
        return [
            dict(row) if isinstance(row, dict) else dict(zip(columns, row, strict=True))
            for row in rows
        ]

    @_serialized_sqli_operation
    def key_index_directions(self, identity, columns):
        # Docstring: Return the per-column DESC flags of an index whose key columns exactly
        # match ``columns`` in order, or ``None`` if no index matches.
        #
        # A ``primary.keys`` override names columns, not an index, so on its own it
        # pages all-ascending -- which sorts when the real key index is
        # mixed-direction. This finds any index (unique, duplicate, or the PK's)
        # whose ordered key columns equal the override and reports its directions,
        # so the override can page in that index's own order. A negative catalog
        # part number is a descending column. ``None`` leaves paging ascending.

        self._ensure_connected()
        database, owner, name = _split_identity(identity)
        for identifier in (database, owner, name, *columns):
            if not _IDENTIFIER.fullmatch(identifier):
                # 36: Unsafe key-index identifier {!r}
                raise InformixError(_m(36, identifier))
        max_bytes = self._metadata_max_bytes()
        column_rows = self.transport.execute(
            "SELECT c.colname, c.colno FROM systables t JOIN syscolumns c ON t.tabid = c.tabid "
            "WHERE t.tabtype = 'T' AND t.owner = ? AND t.tabname = ?",
            (owner, name),
            max_result_bytes=max_bytes,
        )
        by_number = {
            int(_field(row, "colno", 1)): str(_field(row, "colname", 0)) for row in column_rows
        }
        index_rows = self.transport.execute(
            f"SELECT {_INDEX_PARTS} FROM systables t JOIN sysindexes i ON t.tabid = i.tabid "
            "WHERE t.tabtype = 'T' AND t.owner = ? AND t.tabname = ?",
            (owner, name),
            max_result_bytes=max_bytes,
        )
        target = list(columns)
        for row in index_rows:
            names: list[str] = []
            directions: list[bool] = []
            for position in range(16):
                part = int(_field(row, f"part{position + 1}", position))
                if part == 0:
                    break  # key columns are contiguous; the rest is padding
                column_name = by_number.get(abs(part))
                if column_name is None:
                    names = []  # references a column we cannot name; skip this index
                    break
                names.append(column_name)
                directions.append(part < 0)
            if names == target:
                return directions
        return None

    @_serialized_sqli_operation
    def current_lsn(self) -> int:
        self._ensure_connected()
        uniqid, used = self._current_log_position()
        return _log_lsn(uniqid, used)

    @_serialized_sqli_operation
    def boundary_lsn(self) -> int:
        # Docstring: A capture boundary that is never ahead of the log's true write position.
        #
        # ``syslogs.used`` is a page count, so :meth:`current_lsn` names the end of the
        # page currently being written -- up to one page ahead of the last record on
        # disk. A transaction committing in the rest of that page *after* the sample
        # would carry an LSN at or below such a boundary and be discarded by ``_recover``
        # as already captured, while the snapshot read that follows the sample may or
        # may not have seen it: a stale or missing row with nothing left to correct it.
        # Anchoring the boundary at the *start* of the current page keeps it at or below
        # every commit that follows the sample. The cost is at most one page of log
        # re-delivered, which keyed targets dedup by key and LSN. Positions used as a
        # stop bound (the AvailableNow high-water mark, log-file comparisons) keep
        # :meth:`current_lsn`, where overshooting is harmless.

        self._ensure_connected()
        uniqid, used = self._current_log_position()
        return _log_lsn(uniqid, used, page_start=True)

    def _current_log_position(self) -> tuple[int, int]:
        row = self.transport.execute(
            "SELECT uniqid, used FROM sysmaster:syslogs WHERE is_current = 1"
        )[0]
        return int(_field(row, "uniqid", 0)), int(_field(row, "used", 1))

    @_serialized_sqli_operation
    def minimum_lsn(self) -> int:
        self._ensure_connected()
        row = self.transport.execute("SELECT MIN(uniqid) AS uniqid FROM sysmaster:syslogs")[0]
        return int(_field(row, "uniqid", 0)) << 32

    @_serialized_sqli_operation
    def current_utc_time(self) -> int | None:
        # Docstring: Source-server current time as UTC epoch seconds, matching the CDC commit-time
        # units, for stamping snapshot rows' ``_informix_commit_time``.
        #
        # Best-effort: a source that does not support ``DBINFO('utc_current')`` yields NULL
        # commit-time on snapshot rows rather than failing the read. A reconnect that fails
        # raises, for the caller's reconnect loop (an index-ordered scan's); the snapshot's
        # stamp (_source_utc_time) takes that for None.

        if time.monotonic() < getattr(self, "_utc_time_retry_at", 0.0):
            return None
        for attempt in range(2):
            self._ensure_connected()
            try:
                row = self.transport.execute(
                    "SELECT DBINFO('utc_current') AS utc FROM sysmaster:sysdual"
                )[0]
                return int(_field(row, "utc", 0))
            except Exception as error:
                lease_lost = getattr(self, "_connection_lease_lost", None)
                if lease_lost is not None and lease_lost.is_set():
                    # The slot's lease went: that is for the operation guard to report, not
                    # a probe the source refused.
                    raise
                # The failed statement poisoned the SQLI client: drop the transport,
                # keeping the slot, so the next statement reconnects rather than failing on
                # it. A dropped connection is asked again at once: an index-ordered scan's
                # time of a key's last read, or of its end, was lost to one. A source that
                # answered with an error isn't asked again for a while: the error may mean
                # it can't run the probe, or only a lock timeout.
                self.reset_transport()
                if not isinstance(error, (SqliTransportError, OSError)):
                    self._utc_time_retry_at = time.monotonic() + _UTC_TIME_REPROBE_SECONDS
                    return None
                if attempt:
                    return None
        return None

    @_serialized_sqli_operation
    def prepare_initial_capture(self, identities: Sequence[str]) -> int:
        # Docstring: Enable full-row logging for every table, then capture one shared boundary.
        #
        # The boundary is :meth:`boundary_lsn` -- never ahead of the true log position --
        # because the snapshot that follows is sequenced against it.

        self._ensure_connected()

        if not identities:
            # 37: Initial CDC preparation requires at least one table
            raise InformixError(_m(37))
        enabled = []
        try:
            for identity in identities:
                _expect_zero(
                    self.transport.execute(
                        f"EXECUTE FUNCTION {cdc_routine('cdc_set_fullrowlogging')}(?, 1)",
                        (identity,),
                    ),
                    f"cdc_set_fullrowlogging({identity})",
                )
                enabled.append(identity)
        except Exception as error:
            # 38: Initial CDC preparation was partially applied; full-row logging remains enabled
            # for {!r}. Correct the failure and rerun preparation.
            raise InformixError(_m(38, enabled)) from error
        return self.boundary_lsn()

    @_serialized_sqli_operation
    def validate_initial_lsn(self, capture: dict[str, Any], start_lsn: int) -> None:
        # Docstring: Validate CDC registration/activation without reading LODATA records.
        #
        # Retries on a socket-read *timeout*, which under a startup storm (many flows
        # opening CDC sessions against syscdcv1 at once) is transient contention rather
        # than a real validation failure. Each attempt reconnects a fresh transport;
        # bounded attempts with short backoff ride out the storm, and exhausting them
        # re-raises so a genuinely-unreachable source still fails honestly. A non-timeout
        # failure (a nonzero cdc_* return, a negative session id) is a real error and is
        # never retried. Idempotent: each attempt opens its own CDC session, and the
        # server reclaims an abandoned one on its own.

        attempt = 0
        while True:
            try:
                self._validate_initial_lsn_once(capture, start_lsn)
                return
            except Exception as error:  # pylint: disable=broad-except
                if not _is_timeout_error(error) or attempt >= _VALIDATE_LSN_RETRY_MAX_RETRIES:
                    raise
                lease_lost = getattr(self, "_connection_lease_lost", None)
                if lease_lost is not None and lease_lost.is_set():
                    # The connection slot is gone: another reader may hold it already.
                    raise
                delay = min(
                    _VALIDATE_LSN_RETRY_BASE_SECONDS * (2**attempt),
                    _VALIDATE_LSN_RETRY_MAX_SECONDS,
                )
                # 39: Informix CDC initial-LSN validation timed out ({}); resetting the transport
                # and retrying (attempt {}/{}) after {:.1f}s. Raise cdc.read.timeout.seconds or
                # reduce concurrent CDC sessions if this recurs.
                logging.getLogger(__name__).warning(
                    _m(39, error, attempt + 1, _VALIDATE_LSN_RETRY_MAX_RETRIES, delay),
                    exc_info=True,
                )
                self.reset_transport()
                time.sleep(delay)
                attempt += 1

    def _validate_initial_lsn_once(self, capture: dict[str, Any], start_lsn: int) -> None:
        # Docstring: One attempt of :meth:`validate_initial_lsn` (which owns the retry policy).

        self._ensure_connected()

        # The CDC control-plane calls below (open/start/activate/end/close) run on
        # the transport's default socket timeout unless raised here. syscdcv1
        # teardown can stall under many concurrent CDC sessions, so give them the
        # configured cdc.read.timeout.seconds budget instead of the ~30s default.
        set_socket_timeout = getattr(self.transport, "set_socket_timeout", None)
        previous_socket_timeout = getattr(self.transport, "socket_timeout", None)
        cdc_timeout = float(
            self.options.get("cdc.read.timeout.seconds", str(_DEFAULT_CDC_READ_TIMEOUT_SECONDS))
        )
        if set_socket_timeout is not None:
            set_socket_timeout(max(float(previous_socket_timeout or 30), cdc_timeout))

        try:
            server_row = self.transport.execute(
                "SELECT env_value FROM sysmaster:sysenv WHERE env_name='INFORMIXSERVER'"
            )[0]
            server = str(_field(server_row, "env_value", 0))
            session_row = self.transport.execute(
                f"EXECUTE FUNCTION {cdc_routine('cdc_opensess')}(?, 0, 1, 1, 1, 1)",
                (server,),
            )[0]
            session = int(_field(session_row, "session_id", 0))
            if session < 0:
                # 40: cdc_opensess failed with Informix error {}
                raise InformixError(_m(40, session))
        except BaseException:
            # No session to tear down, but the timeout is put back, as below: the client
            # keeps it for every later connection.
            if set_socket_timeout is not None and previous_socket_timeout is not None:
                set_socket_timeout(float(previous_socket_timeout))
            raise
        native = capture["identity"]
        started = False
        primary_error: BaseException | None = None
        try:
            _expect_zero(
                self.transport.execute(
                    f"EXECUTE FUNCTION {cdc_routine('cdc_startcapture')}(?, 0, ?, ?, ?)",
                    (session, native, ",".join(capture["columns"]), 1),
                ),
                "cdc_startcapture",
            )
            started = True
            _expect_zero(
                self.transport.execute(
                    f"EXECUTE FUNCTION {cdc_routine('cdc_activatesess')}(?, ?)",
                    (session, start_lsn),
                ),
                "cdc_activatesess",
            )
        except BaseException as error:
            primary_error = error
            raise
        finally:
            if primary_error is None or not _is_timeout_error(primary_error):
                self._teardown_validation_session(
                    started,
                    session,
                    native,
                    primary_error,
                    set_socket_timeout,
                    previous_socket_timeout,
                )
            else:
                # The socket is mid-read on a timed-out CDC session. Skip teardown --
                # issuing it on a dead socket would only time out again, tripling this
                # attempt's cost. validate_initial_lsn resets the transport and retries;
                # the server reclaims the abandoned session on its own. The timeout is
                # still put back: the client keeps it for every later connection.
                if set_socket_timeout is not None and previous_socket_timeout is not None:
                    set_socket_timeout(float(previous_socket_timeout))

    def _teardown_validation_session(
        self,
        started: bool,
        session: int,
        native: str,
        primary_error: BaseException | None,
        set_socket_timeout,
        previous_socket_timeout,
    ) -> None:
        # Docstring: Best-effort teardown for :meth:`_validate_initial_lsn_once`.
        #
        # The purpose of the validation -- proving the CDC boundary registers and
        # activates -- is already met once activation succeeds (``primary_error`` is
        # None). cdc_endcapture/cdc_closesess are best-effort: the server reclaims an
        # abandoned CDC session on its own, and the validation returns no value derived
        # from them. A teardown that merely *times out* under heavy syscdcv1 load must not
        # fail an otherwise-successful validation -- doing so was observed wedging a flow
        # (e.g. tw101) purely on a slow teardown. Nor one on a dropped connection, which
        # ends the session with it, as a poll's teardown takes one (_settle_poll_cleanup):
        # raised as 42, it was taken for a refused schema transition and recorded as one.
        # Poison the transport so the next use reconnects rather than resuming a
        # half-torn-down session, and treat it as non-fatal. Any other cleanup failure, or
        # any failure alongside a real primary error, is still surfaced.

        cleanup_errors = []
        # A failed primary call that closed the client ended the CDC session with it.
        if started and not _transport_closed(self.transport):
            try:
                _expect_zero(
                    self.transport.execute(
                        f"EXECUTE FUNCTION {cdc_routine('cdc_endcapture')}(?, 0, ?)",
                        (session, native),
                    ),
                    "cdc_endcapture",
                )
            except Exception as error:
                cleanup_errors.append(error)
        if not _transport_closed(self.transport):
            # A failed call closes the SQLI connection, and the CDC session ends with it:
            # cdc_closesess would then fail only for want of a connection.
            try:
                _expect_zero(
                    self.transport.execute(
                        f"EXECUTE FUNCTION {cdc_routine('cdc_closesess')}(?)", (session,)
                    ),
                    "cdc_closesess",
                )
            except Exception as error:
                cleanup_errors.append(error)
        if set_socket_timeout is not None and previous_socket_timeout is not None:
            try:
                set_socket_timeout(float(previous_socket_timeout))
            except Exception as error:
                cleanup_errors.append(error)
        if cleanup_errors and primary_error is None:
            if all(_is_timeout_error(e) or reconnectable_error(e) for e in cleanup_errors):
                # The socket is mid-read on a half-torn-down CDC session; drop the
                # transport so the next operation reconnects instead of resuming it,
                # keeping the connection slot (this is not a capacity problem).
                reset_transport = getattr(self, "reset_transport", None)
                if callable(reset_transport):
                    # pylint: disable-next=not-callable
                    reset_transport()
                # 41: Informix CDC validation succeeded but session teardown timed out or lost its
                # connection; treating as non-fatal and reconnecting. Raise cdc.read.timeout.seconds
                # or reduce concurrent CDC sessions if this recurs.
                logging.getLogger(__name__).warning(
                    _m(41),
                    exc_info=cleanup_errors[0],
                )
                return
            # 42: Initial CDC validation cleanup failed
            raise InformixError(_m(42)) from cleanup_errors[0]
        if cleanup_errors and primary_error is not None:
            for error in cleanup_errors:
                add_informix_exception_note(
                    primary_error,
                    f"Initial Informix CDC validation cleanup also failed: {error}",
                )

    @_serialized_sqli_operation
    # pylint: disable-next=too-many-locals
    def snapshot_page(
        self,
        identity,
        columns,
        primary_keys,
        after,
        limit,
        max_bytes=None,
        skip=0,
        chunk_exprs=None,
        snapshot_filter=None,
        key_descending=None,
    ):
        self._ensure_connected()
        database, owner, name = _split_identity(identity)
        for identifier in (database, owner, name, *columns, *primary_keys):
            if not _IDENTIFIER.fullmatch(identifier):
                # 43: Unsafe snapshot identifier {!r}
                raise InformixError(_m(43, identifier))
        if int(skip) < 0:
            # 44: Snapshot SKIP offset cannot be negative
            raise InformixError(_m(44))
        # chunk_exprs maps a primary-key column to an order-preserving SQL text
        # expression used for keyset comparison and ordering (see
        # datetime_order_preserving_cast). The rendered value is projected under
        # a "__chunk_<key>" alias so the caller can track the cursor as a string.
        chunk_exprs = dict(chunk_exprs or {})
        for key in chunk_exprs:
            if key not in primary_keys:
                # 45: Chunk expression names unknown key {!r}
                raise InformixError(_m(45, key))
        # Page in the key index's own column directions when the key is mixed
        # (some column DESC): then ORDER BY matches the index and a forward scan
        # serves it without a sort. A chunked (DATETIME) key keeps the default
        # ascending order -- its order-preserving cast has no direction-aware index
        # to match -- and an all-ascending key is unaffected.
        descending = list(key_descending or [])
        direction_aware = any(descending) and not chunk_exprs

        def is_descending(index: int) -> bool:
            return direction_aware and index < len(descending) and descending[index]

        def order_term(key: str) -> str:
            # Names are validated, but one with "+" or "." must be sent delimited.
            return chunk_exprs.get(key) or _sql_name(key)

        def order_clause(index: int, key: str) -> str:
            return f"{order_term(key)} DESC" if is_descending(index) else order_term(key)

        alias_names = [f"__chunk_{key}" for key in primary_keys if key in chunk_exprs]
        projection = [_sql_name(column) for column in columns] + [
            f"{chunk_exprs[key]} AS {_sql_name(f'__chunk_{key}')}"
            for key in primary_keys
            if key in chunk_exprs
        ]
        window = f"SKIP {int(skip)} " if skip else ""
        # {+FIRST_ROWS(n)} asks the optimizer to stream the page in key order from
        # the index rather than sort the table: the keyset predicate below is the
        # OR-form `(k1 > ?) OR (k1 = ? AND k2 > ?)`, which optimizers often fail to
        # recognize as an index range start, and a composite `ORDER BY k1, k2` then
        # tips into a full scan + top-sort on a large table -- observed timing out
        # per page even under the extended snapshot budget. It is a `{+ }` comment,
        # so a server with external directives disabled ignores it rather than
        # erroring; UPDATE STATISTICS on the source is the complementary fix that
        # steers the cost model to the same index-ordered plan on its own.
        sql = (
            f"SELECT {{+FIRST_ROWS({int(limit)})}} {window}FIRST {int(limit)} "
            f"{','.join(projection)} "
            f"FROM {database}:{_sql_name(owner)}.{_sql_name(name)}"
        )
        parameters: list[Any] = []
        predicates = [f"({snapshot_filter})"] if snapshot_filter else []
        if after is not None:
            validate_snapshot_arity(after, primary_keys)
            for key, value in zip(primary_keys, after):
                if value is None:
                    # A NULL key value (allow.nullable.index, primary.keys) went into a "= ?"
                    # or "> ?" and failed inside the SQL binder; "= NULL" would match no
                    # row anyway. Paging by key cannot go past one.
                    # 461: The snapshot of {} pages by its key and reached a NULL in key
                    # column {}; paging by key can't go past it: set snapshot.scan: cursor
                    # for this table, and run a full refresh of it, as a snapshot already
                    # begun goes on as it began
                    raise InformixError(_m(461, _join_identity(owner, name), key))
            bound_after = list(after)
            if any(isinstance(value, str) for value in after):
                datetimes = self._paging_column_facts(owner, name)[0]
                for index, key in enumerate(primary_keys):
                    # DATETIME keys chunked by expression compare as strings, so
                    # they must not be rewritten into a native DATETIME literal.
                    if (
                        key not in chunk_exprs
                        and key in datetimes
                        and isinstance(bound_after[index], str)
                    ):
                        bound_after[index] = InformixDatetimeLiteral(
                            bound_after[index], datetimes[key]
                        )
            # Guarded leading-column seek bound. The OR-form keyset below is
            # frequently not recognized by the optimizer as an index range start,
            # so the scan begins at the index head and re-reads then discards the
            # whole already-copied prefix every page -- cost grows with the cursor
            # and a large table eventually times a page out (tw305). This redundant
            # bound gives the optimizer a concrete value to seek to, and unlike the
            # {+FIRST_ROWS} directive it is honored where external directives are
            # disabled. It is added as a separate conjunct and does not touch the
            # ORDER BY or the OR-form, so the index-direction (ASC/DESC) ordered
            # iteration is unchanged; it only reuses is_descending(0) to pick the
            # matching comparator (>= for ASC, <= for DESC).
            #
            # It is result-preserving only under guards, so it is omitted otherwise
            # rather than risk silently dropping rows:
            #   * leading key must be a plain indexed column, not an order-preserving
            #     cast (chunk_exprs): no index serves the cast, and its ordering can
            #     diverge from the column's, so the bound would neither seek nor stay
            #     provably redundant;
            #   * the leading cursor value must be non-NULL: `k1 >= NULL` is UNKNOWN
            #     for every row and would return an empty page (a nullable leading
            #     column under allow.nullable.index can resume from NULL). The bound
            #     keeps the rows whose leading value IS NULL exactly when the OR-form
            #     does (a nullable descending column, below), so it never drops a row
            #     the OR-form keeps.
            #
            # Informix sorts NULL lowest: first in an ascending column, so behind any
            # cursor value, but last in a descending one, so still to read after a page
            # that ended on a value. "< ?" alone passed over them. A descending column the
            # catalog holds nullable (allow.nullable.index, primary.keys) adds "OR IS NULL";
            # a NOT NULL one, every real primary key, keeps the plain comparison.
            nullable = (
                self._paging_column_facts(owner, name)[1]
                if any(is_descending(index) for index in range(len(primary_keys)))
                else frozenset()
            )

            def comparison_term(index: int, comparison: str) -> str:
                key = primary_keys[index]
                term = f"{order_term(key)} {comparison} ?"
                if is_descending(index) and key in nullable:
                    return f"({term} OR {order_term(key)} IS NULL)"
                return term

            if primary_keys and primary_keys[0] not in chunk_exprs and bound_after[0] is not None:
                predicates.append(comparison_term(0, "<=" if is_descending(0) else ">="))
                parameters.append(bound_after[0])
            clauses = []
            for index in range(len(primary_keys)):
                prefix = " AND ".join(
                    f"{order_term(previous)} = ?" for previous in primary_keys[:index]
                )
                # "Next in scan order" is a larger value for an ASC column and a
                # smaller one for a DESC column, so the keyset comparison flips to
                # match the index direction.
                term = comparison_term(index, "<" if is_descending(index) else ">")
                clauses.append(f"({prefix + ' AND ' if prefix else ''}{term})")
                parameters.extend(bound_after[: index + 1])
            predicates.append("(" + " OR ".join(clauses) + ")")
        if predicates:
            sql += " WHERE " + " AND ".join(predicates)
        if primary_keys:
            sql += " ORDER BY " + ",".join(
                order_clause(index, key) for index, key in enumerate(primary_keys)
            )
        rows = self.transport.execute(
            sql,
            tuple(parameters),
            max_result_bytes=(
                (max_bytes or None)
                if max_bytes is not None
                else (int(self.options.get("snapshot.max.bytes", "0")) or None)
            ),
        )
        self._unaltered(primary_keys, identity)
        return [
            dict(row)
            if isinstance(row, dict)
            else dict(zip(list(columns) + alias_names, row, strict=True))
            for row in rows
        ]

    def _paging_column_facts(self, owner: str, name: str) -> tuple[dict[str, int], frozenset]:
        # Docstring: The table's DATETIME columns with their qualifiers, and its nullable
        # columns, described once per bridge.
        #
        # A keyset page needs the first to bind a string cursor value as a DATETIME literal
        # and the second to keep a descending key column's NULLs. Describing the table on
        # every page cost catalog queries inside each page's transaction. The connector
        # pages with key metadata it read when the snapshot began, so caching these adds
        # no staleness of its own.

        cache = getattr(self, "_paging_column_cache", None)
        if cache is None:
            cache = self._paging_column_cache = {}
        if (owner, name) not in cache:
            table = self._describe_table(owner, name)
            cache[(owner, name)] = (
                {
                    str(column["name"]): int(column["length"])
                    for column in table["columns"]
                    if column["type_name"] == "DATETIME"
                },
                frozenset(
                    str(column["name"]) for column in table["columns"] if column.get("nullable")
                ),
            )
        return cache[(owner, name)]

    def _unaltered(self, keys, identity) -> None:
        # Docstring: Refuse the last read's rows when string.null.byte changed a value of the
        # ``keys`` they page on or are looked up by: Informix holds 'a\x00z', and paging on
        # from 'az' skipped every row up to it ('ab' too), or, descending, read it again.

        altered = sorted(set(keys) & getattr(self.transport, "altered", set()))
        if altered:
            # 486: Informix table '{}' holds a NUL byte in key or index column {}, which
            # string.null.byte={} changes, so the read can't page on or look up the value
            # Informix holds; set snapshot.scan=cursor to read the table without paging on it
            raise InformixError(_m(486, identity, ", ".join(altered), self._null_byte))

    @contextlib.contextmanager
    def _repeatable_read_transaction(self, isolation: str = "REPEATABLE READ"):
        # Docstring: Run a snapshot transaction at ``isolation`` with the snapshot socket timeout.
        #
        # Yields ``execute_command``. Commits on success and rolls back on any
        # error, preserving the primary error if rollback also fails. Isolates
        # the ANSI/non-ANSI transaction handling shared by the blocking
        # consistent snapshot and per-chunk incremental snapshot reads.
        #
        # ``isolation`` is the SQL clause after ``SET ISOLATION TO`` and must be one
        # of the fixed clauses in ``_SNAPSHOT_ISOLATION_SQL`` -- it is interpolated
        # into a statement, so it is guarded here against anything else even though
        # the connector already maps it from a validated token.

        if isolation not in _SNAPSHOT_ISOLATION_SQL.values():
            # 46: Unsupported snapshot isolation {!r}
            raise InformixError(_m(46, isolation))
        execute_command = getattr(self.transport, "execute_command", self.transport.execute)
        # A database's mode never changes, so a non-ANSI one is probed once. An ANSI one
        # is probed every time: the probe opens the implicit transaction the COMMIT
        # WORK below ends.
        is_ansi = getattr(self, "_is_ansi_database", None)
        if is_ansi is not False:
            ansi_rows = self.transport.execute(
                "SELECT is_ansi FROM sysmaster:sysdatabases WHERE name = ?",
                (_sysmaster_database_name(self.config["database"]),),
            )
            if len(ansi_rows) != 1:
                # 47: Could not determine transaction mode for database {!r}
                raise InformixError(_m(47, self.config["database"]))
            is_ansi = self._is_ansi_database = bool(int(_field(ansi_rows[0], "is_ansi", 0)))
        if is_ansi:
            # The catalog SELECT starts an implicit transaction in an ANSI
            # database. End it before establishing the snapshot isolation.
            execute_command("COMMIT WORK")
        execute_command(f"SET ISOLATION TO {isolation}")
        if not is_ansi:
            execute_command("BEGIN WORK")
        set_socket_timeout = getattr(self.transport, "set_socket_timeout", None)
        previous_socket_timeout = getattr(self.transport, "socket_timeout", None)
        if set_socket_timeout is not None:
            set_socket_timeout(
                max(
                    float(previous_socket_timeout or 30),
                    float(
                        self.options.get(
                            "snapshot.read.timeout.seconds",
                            str(_DEFAULT_SNAPSHOT_READ_TIMEOUT_SECONDS),
                        )
                    ),
                )
            )
        try:
            yield execute_command
            execute_command("COMMIT WORK")
        except BaseException as primary_error:
            try:
                # A client that closed on the failure ended the transaction with its session:
                # sent there, the rollback only failed, as a note on every such error.
                if not _transport_closed(self.transport):
                    execute_command("ROLLBACK WORK")
            except Exception as cleanup_error:
                add_informix_exception_note(
                    primary_error,
                    f"Informix snapshot rollback also failed: {cleanup_error}",
                )
            raise
        finally:
            if set_socket_timeout is not None and previous_socket_timeout is not None:
                try:
                    set_socket_timeout(float(previous_socket_timeout))
                except Exception:
                    # 48: Failed to restore Informix snapshot socket timeout
                    logging.getLogger(__name__).warning(
                        _m(48),
                        exc_info=True,
                    )

    @_serialized_sqli_operation
    def snapshot_chunk(
        self,
        identity,
        columns,
        primary_keys,
        after,
        limit,
        max_bytes=None,
        chunk_exprs=None,
        snapshot_filter=None,
        key_descending=None,
        isolation="REPEATABLE READ",
    ):
        # Docstring: Read one PK-ordered chunk in its own repeatable-read transaction.
        #
        # Returns ``(chunk_lsn, rows)`` where ``chunk_lsn`` is the source LSN
        # captured immediately before the chunk SELECT. The chunk is read at
        # REPEATABLE READ so its values are consistent as-of ``chunk_lsn``; the
        # transaction spans a single page, so no long-lived read view is held.
        # ``chunk_exprs`` optionally maps primary-key columns to order-preserving
        # SQL text expressions for keyset comparison (e.g. DATETIME-as-string).
        # ``key_descending`` optionally carries the per-column key index direction
        # so a mixed-direction key pages in the index's own order.

        self._ensure_connected()
        with self._repeatable_read_transaction(isolation):
            # boundary_lsn, not current_lsn: the stamp must not exceed the true log
            # position, or a change committing right after the sample (but before
            # this SELECT) sequences below a row that predates it.
            chunk_lsn = self.boundary_lsn()
            rows = self.snapshot_page(
                identity,
                columns,
                primary_keys,
                after,
                limit,
                max_bytes,
                chunk_exprs=chunk_exprs,
                snapshot_filter=snapshot_filter,
                key_descending=key_descending,
            )
            return chunk_lsn, rows

    @_serialized_sqli_operation
    def max_primary_key(
        self, identity, primary_keys, chunk_exprs=None, snapshot_filter=None, key_descending=None
    ):
        # Docstring: Return the largest primary-key tuple, or ``None`` for an empty table.
        #
        # "Largest" is the last tuple in the key's paging order. For a plain-column
        # key that order follows the index's per-column direction (see
        # ``key_descending``); for an all-ascending key it is ordinary descending.
        #
        # When ``chunk_exprs`` maps a key to an order-preserving SQL expression,
        # that key is ordered and returned as its rendered string form so the
        # upper bound matches the values chunk pages compare against.
        #
        # A plain-column key resolves the maximum tuple one column at a time (see
        # ``_max_key_stepwise``) so every read is index-servable regardless of the
        # key index's per-column direction. A chunked (DATETIME) key keeps the
        # single composite read: its equality prefix would have to match an
        # order-preserving cast expression that no index serves, so a stepwise scan
        # would not avoid a sort there.

        self._ensure_connected()
        database, owner, name = _split_identity(identity)
        for identifier in (database, owner, name, *primary_keys):
            if not _IDENTIFIER.fullmatch(identifier):
                # 49: Unsafe max-key identifier {!r}
                raise InformixError(_m(49, identifier))
        chunk_exprs = dict(chunk_exprs or {})
        table_ref = f"{database}:{_sql_name(owner)}.{_sql_name(name)}"
        if primary_keys and not chunk_exprs:
            return self._max_key_stepwise(
                table_ref, list(primary_keys), snapshot_filter, list(key_descending or []), identity
            )
        select_terms = [
            f"{chunk_exprs[key]} AS {_sql_name(f'__chunk_{key}')}"
            if key in chunk_exprs
            else _sql_name(key)
            for key in primary_keys
        ]
        result_names = [f"__chunk_{key}" if key in chunk_exprs else key for key in primary_keys]
        order = ",".join(f"{chunk_exprs.get(key) or _sql_name(key)} DESC" for key in primary_keys)
        # {+FIRST_ROWS(1)} steers the optimizer to minimize time-to-first-row so a
        # DESC ordering is served by an index read (row 1 returns immediately)
        # rather than a full scan + top-sort. It is a `{+ }` comment, so a server
        # with external directives disabled ignores it rather than erroring. It
        # cannot help when a key column is chunked to an order-preserving
        # expression (no index serves that ordering); that path stays sort-bound.
        sql = f"SELECT {{+FIRST_ROWS(1)}} FIRST 1 {','.join(select_terms)} FROM {table_ref}"
        if snapshot_filter:
            sql += f" WHERE ({snapshot_filter})"
        sql += f" ORDER BY {order}"
        # In its own transaction, as each chunk and the stepwise read are: bare, it read at
        # whatever isolation the session last had and, in an ANSI database, left its implicit
        # transaction, and the shared locks of a sort-bound scan, open to the next one.
        with self._repeatable_read_transaction():
            rows = self.transport.execute(sql, ())
            self._unaltered(primary_keys, identity)
        if not rows:
            return None
        row = rows[0]
        if isinstance(row, dict):
            return [row[name] for name in result_names]
        return list(row)

    def _max_key_stepwise(
        self,
        table_ref: str,
        primary_keys: list[str],
        snapshot_filter: str | None,
        descending: list[bool],
        identity: str = "",
    ) -> list[Any] | None:
        # Docstring: Resolve the maximum key tuple one column at a time.
        #
        # Each step reads ``FIRST 1`` of one key column under an equality filter
        # that pins the already-resolved higher-order columns, ordered to return
        # that column's *last* value in the key's paging order: for an ASC column
        # that is its max (``ORDER BY col DESC``), for a DESC column its min
        # (``ORDER BY col ASC``) -- so the tuple is the last row a forward index
        # scan in the key's own order would reach. A single-column order within an
        # equality prefix is served by a directional sub-range scan of the key
        # index -- no whole-table sort -- and it holds for any mix of ASC/DESC
        # column directions, where a composite ``ORDER BY`` matches neither scan
        # direction and forces a sort. Prior columns are pinned with ``?``
        # parameters, exactly as the snapshot keyset does, so no key value is
        # rendered back into SQL text.
        #
        # All steps run inside one REPEATABLE READ view so the columns resolve
        # against a single point in time -- identical to what the equivalent
        # one-statement composite read would return -- and that view also carries
        # the snapshot read-timeout budget instead of the bare transport default.

        with self._repeatable_read_transaction():
            resolved: list[Any] = []
            for index, key in enumerate(primary_keys):
                predicates = [f"({snapshot_filter})"] if snapshot_filter else []
                params: list[Any] = []
                for previous in range(index):
                    # A nullable key column (allow.nullable.index, primary.keys) can resolve
                    # to NULL, which no "= ?" matches and the SQL binder refuses.
                    if resolved[previous] is None:
                        predicates.append(f"{_sql_name(primary_keys[previous])} IS NULL")
                        continue
                    predicates.append(f"{_sql_name(primary_keys[previous])} = ?")
                    params.append(resolved[previous])
                where = f" WHERE {' AND '.join(predicates)}" if predicates else ""
                # The last value in paging order is the min of a DESC column, the
                # max of an ASC one; fetch it by ordering opposite the index.
                fetch_order = "ASC" if index < len(descending) and descending[index] else "DESC"
                term = _sql_name(key)
                sql = (
                    f"SELECT {{+FIRST_ROWS(1)}} FIRST 1 {term} "
                    f"FROM {table_ref}{where} ORDER BY {term} {fetch_order}"
                )
                rows = self.transport.execute(sql, tuple(params))
                self._unaltered([key], identity or table_ref)
                if not rows:
                    # No row at the first column means the table (or filtered set)
                    # is empty. A later column cannot come up empty under the
                    # frozen read view once a higher column resolved, so an empty
                    # result at any step is the empty-table answer.
                    return None
                row = rows[0]
                resolved.append(row[key] if isinstance(row, dict) else row[0])
            return resolved

    @_serialized_sqli_operation
    # pylint: disable-next=too-many-locals,too-many-statements
    def consistent_snapshot(
        self,
        identity,
        columns,
        primary_keys,
        page_size,
        max_rows,
        max_bytes,
        datetime_primary_key=False,
        page_consumer=None,
        snapshot_filter=None,
        isolation="REPEATABLE READ",
        key_descending=None,
    ):
        # Docstring: Read one bounded point-in-time snapshot at ``isolation``.
        #
        # ``isolation`` defaults to REPEATABLE READ, which freezes the scanned rows
        # so the snapshot is exactly the committed state as-of ``snapshot_lsn``. A
        # non-locking level (e.g. COMMITTED READ LAST COMMITTED) avoids locking the
        # table but no longer freezes it, so rows committed during the scan may be
        # captured here and replayed by CDC -- harmless for a keyed table (the sink
        # de-duplicates), a duplicate-row risk for a key-less one.

        self._ensure_connected()

        # Informix SQLI does not expose parameter binding, so keyset pagination
        # must render continuation values as SQL literals. Some valid partial
        # DATETIME values are rejected when parsed back by Informix (-1263), a float
        # key has no exact literal and a BOOLEAN key no order. A table with such a key
        # is read as one with no key is: through one forward-only cursor. Paging it by
        # position (SKIP) would run a statement a page, and under an isolation that takes
        # no locks each sees the rows as they then are: a row deleted below the position
        # shifts the rest down, and the first row not yet read is never read.
        unordered = not primary_keys or bool(datetime_primary_key)

        self._ensure_connected()
        with self._repeatable_read_transaction(isolation):
            # boundary_lsn, not current_lsn: see snapshot_chunk.
            snapshot_lsn = self.boundary_lsn()
            rows: list[dict[str, Any]] = []
            retained_bytes = _deep_size(rows) if max_bytes else 0
            total_rows = 0
            page_index = 0

            if unordered:
                database, owner, name = _split_identity(identity)
                for identifier in (database, owner, name, *columns):
                    if not _IDENTIFIER.fullmatch(identifier):
                        # 43: Unsafe snapshot identifier {!r}
                        raise InformixError(_m(43, identifier))
                execute_pages = getattr(self.transport, "execute_pages", None)
                if not callable(execute_pages):
                    # 51: Configured Informix SQLI transport does not implement execute_pages()
                    raise InformixError(_m(51))

                def consume(page: list[dict[str, Any]]) -> None:
                    nonlocal retained_bytes, total_rows, page_index
                    if max_rows and total_rows + len(page) > max_rows:
                        # 52: Initial snapshot exceeds snapshot.max.rows={}
                        raise InformixError(_m(52, max_rows))
                    if max_bytes:
                        retained_bytes += _deep_size(page)
                        if retained_bytes > max_bytes:
                            # 53: Initial snapshot exceeds snapshot.max.bytes={}
                            raise InformixError(_m(53, max_bytes))
                    if page_consumer is None:
                        rows.extend(page)
                    else:
                        page_consumer(snapshot_lsn, page_index, page)
                    total_rows += len(page)
                    page_index += 1

                execute_pages(
                    f"SELECT {','.join(_sql_name(column) for column in columns)} "
                    f"FROM {database}:{_sql_name(owner)}.{_sql_name(name)}"
                    + (f" WHERE ({snapshot_filter})" if snapshot_filter else ""),
                    (),
                    page_size,
                    consume,
                )
                return snapshot_lsn, rows

            after = None
            while True:
                remaining_rows = max_rows - total_rows if max_rows else page_size
                page_capacity = min(page_size, remaining_rows)
                remaining_bytes = max_bytes - retained_bytes if max_bytes else None
                if remaining_bytes is not None and remaining_bytes <= 0:
                    # 53: Initial snapshot exceeds snapshot.max.bytes={}
                    raise InformixError(_m(53, max_bytes))
                page = self.snapshot_page(
                    identity,
                    columns,
                    primary_keys,
                    after,
                    page_capacity + 1,
                    # 0, no limit: None would take the connection's, which a table's
                    # snapshot.max.bytes=0 turned off.
                    0 if remaining_bytes is None else remaining_bytes,
                    snapshot_filter=snapshot_filter,
                    # The key index's own directions, so that it serves each page's order.
                    **({"key_descending": list(key_descending)} if key_descending else {}),
                )
                has_more = len(page) > page_capacity
                if has_more and max_rows and remaining_rows == 0:
                    # 52: Initial snapshot exceeds snapshot.max.rows={}
                    raise InformixError(_m(52, max_rows))
                accepted = page[:page_capacity]
                if max_bytes:
                    retained_bytes += _deep_size(accepted)
                if max_bytes and retained_bytes > max_bytes:
                    # 53: Initial snapshot exceeds snapshot.max.bytes={}
                    raise InformixError(_m(53, max_bytes))
                if page_consumer is None:
                    rows.extend(accepted)
                else:
                    page_consumer(snapshot_lsn, page_index, accepted)
                total_rows += len(accepted)
                if not has_more:
                    break
                after = [accepted[-1][key] for key in primary_keys]
                page_index += 1
            return snapshot_lsn, rows
