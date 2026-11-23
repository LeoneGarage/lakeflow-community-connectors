"""Shared CDC sessions: shards, their catalog, and projecting a transaction onto a table's rows."""

from __future__ import annotations

import contextlib
import dataclasses
import hashlib
import itertools
import logging
import math
import os
import stat
import tempfile
import threading
import time
from collections import deque
from dataclasses import dataclass, replace
from typing import Any, Callable, Iterable, Iterator

from databricks.labs.community_connector.sources.informix.informix_filters import (
    _TRANSFORM_COLUMNS_OPTION,
    _apply_column_renames,
    _apply_column_selection,
    _apply_column_transforms,
    _ensure_selected_keys,
    _leave_out_unsupported_columns,
    _RowFilterParser,
)
from databricks.labs.community_connector.sources.informix.informix_keys import (
    _NULL_BYTE_OPTION,
    _PRIMARY_KEYS_OPTION,
    _coerce_variable_decimal_values,
    _framework_value,
    _is_variable_scale_decimal,
)
from databricks.labs.community_connector.sources.informix.informix_options import (
    _CDC_SPILL_SCHEMA,
    _DEFAULT_CDC_MAX_RECORDS,
    _DEFAULT_SHARED_CDC_BUFFER,
    _SHARED_CDC_IDLE_EXIT_SECONDS,
    _SHARED_CDC_IDLE_POLL_SECONDS,
    _SHARED_CDC_SESSION_OPTION,
    _SHARED_CDC_SUBSCRIBER_TTL_SECONDS,
    _SNAPSHOT_SCHEMA_REFRESH_INTERVAL_SECONDS,
    COMMIT_LSN,
    COMMIT_TIME,
    CURSOR,
    OP,
    TX_ID,
    InformixError,
    _m,
)
from databricks.labs.community_connector.sources.informix.informix_registration import (
    Column,
    Table,
    UnsupportedChangeError,
)
from databricks.labs.community_connector.sources.informix.informix_transactions import (
    CommittedTransaction,
    Transaction,
    TransactionBuffer,
    _apply_row_filter,
    _before_lsn,
    _commit_time_value,
    _key,
    _lsn,
    _normalise_record,
    _operation,
    _record_matches,
    _refuse_null_key,
    _row_filter_matches,
    _row_filter_predicate,
    _shape_change,
    _shape_tombstone,
    _sortable_lsn,
)


def _shape_delete(row, table, record, tx):
    # Docstring: Shape a delete-channel row: the key values, with every other column NULL.
    #
    # A NULL key value passes through when its column is declared nullable. Such a key can
    # only come from a ``primary.keys`` override (the wildcard included) or a nullable-index
    # promotion, and the pipeline's AUTO CDC delete flow matches NULL key values, for SCD1
    # and SCD2 alike.
    #
    # A NOT NULL key column -- every real catalog primary key -- can never legitimately
    # hold NULL, so a NULL there still fails closed as a corrupt or incomplete before-image.
    # So does a key column missing from the image entirely: the CDC decoder sets every
    # captured column, using None for a SQL NULL, so an absent column is never a NULL.

    if row is None:
        # 352: CDC delete has no before image; full-row logging is required
        raise InformixError(_m(352))
    result = {column.name: None for column in table.columns}
    nullable: set[str] | None = None
    for pk in table.primary_keys:
        if pk not in row:
            # 353: CDC delete on '{}' has no value for key column {}; the before image is incomplete
            raise InformixError(_m(353, table.exposed_name, pk))
        value = row[pk]
        if value is None:
            if nullable is None:
                nullable = {column.name for column in table.columns if column.nullable}
            if pk not in nullable:
                # 354: CDC delete on '{}' has NULL in NOT NULL key column {}; the before image is
                # corrupt or incomplete, or '{}=null' turned a string holding a NUL byte into NULL
                raise InformixError(_m(354, table.exposed_name, pk, _NULL_BYTE_OPTION))
        result[pk] = _framework_value(value)
    result.update(
        {
            CURSOR: _sortable_lsn(_lsn(record)),
            COMMIT_LSN: _sortable_lsn(tx.commit_lsn),
            TX_ID: tx.tx_id,
            OP: "d",
            COMMIT_TIME: _commit_time_value(tx.commit_time),
        }
    )
    return result


def _project_transaction(
    tx: CommittedTransaction,
    table: Table,
    deletes: bool,
    tombstones: bool = False,
    encoding: str = "latin-1",
    options: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    # Docstring: Project one committed transaction onto one output channel.
    #
    # ``deletes`` selects the delete channel: key-only ``d`` rows for DELETEs and for the
    # old key of a key-changing UPDATE. Otherwise this is the upsert channel, which emits
    # INSERT/UPDATE after-images as ``c``/``u``. On the keyed CDC path the upsert channel
    # deliberately skips DELETEs -- the pipeline applies deletes only through the
    # separate delete flow, and a ``d`` row on the upsert view would be merged as an
    # upsert. ``tombstones`` (the append path with ``append.delete.tombstones`` on) adds
    # each DELETE's full before-image as a ``d`` row instead: the append destination
    # lands rows as-is and never reads ``_informix_op``, so the delete stays in the
    # change log rather than vanishing. On a keyed table forced onto the append path, a
    # key-changing UPDATE is a delete of the old key too: with ``tombstones`` on it lands
    # the before-image as a ``d`` row ahead of the new key's ``u`` row, sequenced at the
    # before-image's own log position so the two never tie on the cursor. A keyless table
    # has no key to tell which row an UPDATE replaced, so with ``tombstones`` on every
    # UPDATE of one lands its before-image as a ``d`` row the same way, also when nothing
    # changed: the log then nets to the table, an update being one row out and one in.
    # Without ``tombstones`` its UPDATEs land only their after-image. TRUNCATE is
    # unrepresentable on either channel.

    #
    # With a row_filter, membership comes from the images: a change whose rows never match
    # is dropped, an UPDATE that moves a row out of the filter is a delete of its
    # before-image, and one that moves a row in is an upsert of its after-image alone --
    # its old key was never landed, so there is no old key to delete.
    #
    # An UPDATE changes the key as the key lands: a variable-scale DECIMAL key column is
    # coerced to its target type (``options``), which can round 12.1 and 12.2 to one 12,
    # and a delete of the old key at the update's LSN then tied with its upsert and could
    # remove the row.

    predicate = _row_filter_predicate(table, encoding)
    coerced = any(
        _is_variable_scale_decimal(column)
        for column in table.columns
        if column.name in table.primary_keys
    )

    def landed_key(row):
        if coerced and row is not None:
            # The key columns alone: another column's value, which a delete never lands,
            # could fail its coercion.
            row = _coerce_variable_decimal_values(
                {name: row.get(name) for name in table.primary_keys}, table, options or {}
            )
        # NaN equals nothing, itself included: a FLOAT key left NaN read as changed, and its
        # delete tied with its upsert. The target's merge takes NaN for NaN.
        return tuple(
            "NaN" if isinstance(value, float) and math.isnan(value) else value
            for value in _key(row, table)
        )

    output = []
    for record in tx.records:
        if not _record_matches(record, table):
            continue
        op = _operation(record)
        if op == "TRUNCATE":
            # 348: TRUNCATE on {} cannot be represented as keyed Lakeflow deletes
            raise UnsupportedChangeError(_m(348, table.exposed_name))
        before = record.get("before", record.get("row") if op == "DELETE" else None)
        after = record.get("after", record.get("row") if op == "INSERT" else None)
        entered = False
        if predicate is not None:
            # A missing image counts as in the filter, so the record reaches the shaping
            # that fails closed on it, as it does without a filter, instead of dropping or
            # reclassifying the change.
            was_in = op in {"UPDATE", "DELETE"} and (
                before is None or _row_filter_matches(predicate, before)
            )
            is_in = op in {"INSERT", "UPDATE"} and (
                after is None or _row_filter_matches(predicate, after)
            )
            if not (was_in or is_in):
                continue
            if op == "UPDATE" and not is_in:
                op = "DELETE"
            entered = op == "UPDATE" and not was_in
        key_changed = op == "UPDATE" and not entered and landed_key(before) != landed_key(after)
        if deletes:
            if op == "DELETE" or key_changed:
                output.append(_shape_delete(before, table, record, tx))
        elif op in {"INSERT", "UPDATE"}:
            if tombstones and (
                key_changed or (op == "UPDATE" and not entered and not table.primary_keys)
            ):
                output.append(_shape_tombstone(before, table, record, tx, lsn=_before_lsn(record)))
            if after is not None and table.primary_keys:
                _refuse_null_key(after, table)
            output.append(_shape_change(after, record, tx, "u" if op == "UPDATE" else "c"))
        elif op == "DELETE" and tombstones:
            output.append(_shape_tombstone(before, table, record, tx))
    return output


def _spilled_page_rows(read_page, part, page_index, tx, table, options, **shape):
    # Docstring: One spilled page's rows on one channel, shaped as _read_stream shapes an
    # in-memory transaction. ``tx`` is (tx_id, begin, commit, restart, commit time), ``shape``
    # the deletes, tombstones and encoding _project_transaction takes.

    _header, records = read_page(
        part["native_identity"],
        part["table"],
        part["pipeline_scope"],
        _CDC_SPILL_SCHEMA,
        part["run"],
        page_index,
    )
    discarded = part["discarded"]
    buffer = Transaction(int(tx[0]), int(tx[1]))
    for record in records:
        record = _normalise_record(record)
        if not any(low <= record["lsn"] <= high for low, high in discarded):
            buffer.append(record)
    if buffer.pending_before is not None:
        # A page never ends between an UPDATE's images, so this transaction committed with
        # one unpaired, as an in-memory one fails at its COMMIT.
        # 141: Transaction {} committed with unpaired BEFORE_UPDATE
        raise InformixError(_m(141, buffer.tx_id))
    committed = CommittedTransaction(
        int(tx[0]), int(tx[1]), int(tx[2]), int(tx[3]), tuple(buffer.records), tx[4]
    )
    for row in _project_transaction(committed, table, **shape, options=options):
        yield _coerce_variable_decimal_values(row, table, options)


_HOST_SHARD_TABLE_COLUMN_FIELDS = ("columns", "catalog_columns", "unsupported_columns")


def _wire_table(table: Table) -> dict[str, Any]:
    # Docstring: ``table`` as plain data. The merged deployment defines Table inside a
    # function, so it can't be pickled by name between processes; Table(**fields) rebuilds it.

    data = {}
    for item in dataclasses.fields(table):
        value = getattr(table, item.name)
        if item.name in _HOST_SHARD_TABLE_COLUMN_FIELDS:
            value = [dataclasses.asdict(column) for column in value]
        data[item.name] = value
    return data


class _SpilledRows:
    # Docstring: A batch that is one spilled transaction: its rows on one channel, shaped
    # from the staged pages only when iterated (the simple reader), or served page by page
    # from executors through ``descriptors`` (the partitioned reader), so the driver never
    # holds them.

    def __init__(self, read_page, parts, tx, table, options, **shape) -> None:
        self._read_page = read_page
        self.parts = parts
        self.tx = (tx.tx_id, tx.begin_lsn, tx.commit_lsn, tx.restart_lsn, tx.commit_time)
        self.table = table
        self.shape = shape
        self._options = options
        self._rows: Iterator[dict[str, Any]] | None = None

    def __iter__(self) -> "_SpilledRows":
        return self

    def __next__(self) -> dict[str, Any]:
        if self._rows is None:
            self._rows = itertools.chain.from_iterable(
                _spilled_page_rows(
                    self._read_page, part, page, self.tx, self.table, self._options, **self.shape
                )
                for part in self.parts
                for page in range(part["pages"])
            )
        return next(self._rows)

    def descriptors(self, extra: dict[str, Any]) -> list[dict[str, Any]]:
        table = _wire_table(self.table)
        return [
            {
                "kind": "spill",
                "part": part,
                "page_index": page,
                "tx": list(self.tx),
                "table": table,
                **self.shape,
                **extra,
            }
            for part in self.parts
            for page in range(part["pages"])
        ]


@dataclass(frozen=True, eq=False, repr=False)
class _SharedCdcSnapshot:
    # Docstring: A coherent (schema, log position, transactions) view a consumer poll reads
    # from a shard in place of touching Informix itself.

    table: "Table"
    fingerprint: str
    min_lsn: int
    current_lsn: int
    committed: list
    caught_up: bool
    open_begin: int | None
    # Where the daemon's latest read went idle (its TIMEOUT LSN), or None.
    reached_lsn: int | None = None
    # Where that idle read started (the shard's restart LSN), or None.
    read_start_lsn: int | None = None


def _table_id(table: Table) -> int | None:
    # Docstring: The Informix table ID a catalog read recorded as ``table.incarnation``.

    try:
        return int(table.incarnation)
    except (TypeError, ValueError):
        return None


class _ShardCatalog:
    # Docstring: The catalog tables one shard's daemon reads under, re-read only when changed.
    #
    # Describing every subscribed table before every read cost two or three catalog
    # queries a table each cycle. Instead the daemon keeps each table as last read, with
    # its systables.version, and every _SNAPSHOT_SCHEMA_REFRESH_INTERVAL_SECONDS asks for all
    # their versions in one query. Only the tables whose version moved, or whose table ID
    # no longer names them, are read again, together in one batch (get_tables). Informix
    # bumps the version on every change to a table's definition, and on UPDATE STATISTICS,
    # which just costs a needless re-read. A new subscriber is read at once, and a failed
    # read forces a check before the next one.
    #
    # A change waits for the next check without misreading rows. Informix rejects ALTER
    # TABLE while full-row logging is on, which every read switches on, so a column change
    # needs the pipelines stopped (see the README), and the restarted update gets a new
    # shard, read afresh. A captured column changed anyway still fails the read that meets
    # it, which compares the layout Informix reports with the catalog
    # (_assert_capture_layout). What a check does pick up is an index or key constraint
    # created or dropped while the table is captured, which Informix allows and which can
    # change the key a table is merged by; consumers see it at the next check rather than
    # the next read. A bridge without table_versions re-reads every table at each check,
    # and one without get_tables reads them one at a time.
    #
    # Used by the shard's daemon thread alone, so it needs no lock; a consumer only ever
    # calls force_check, a single assignment.

    def __init__(self) -> None:
        # identity -> (systables.version, or None when unknown, and the catalog table)
        self.entries: dict[str, tuple[int | None, Table]] = {}
        self.next_check = 0.0

    def force_check(self) -> None:
        self.next_check = 0.0

    def tables(self, bridge, identities, now: float, check_seconds: float) -> dict[str, Table]:
        # Docstring: The catalog table of each subscribed (identity, table) pair that still
        # names one, re-read as the class describes.

        wanted = dict(identities)
        for identity in [identity for identity in self.entries if identity not in wanted]:
            del self.entries[identity]
        if now >= self.next_check:
            self.next_check = now + check_seconds
            check = wanted
        else:
            check = {
                identity: table
                for identity, table in wanted.items()
                if identity not in self.entries
            }
        if check:
            self._refresh(bridge, check)
        return {
            identity: self.entries[identity][1] for identity in wanted if identity in self.entries
        }

    def _refresh(self, bridge, check: dict[str, Table]) -> None:
        # The versions come first: a definition changed between this query and the reads
        # below leaves an older version on the newer table, which the next check re-reads.
        table_versions = getattr(bridge, "table_versions", None)
        known = {
            identity: self.entries[identity][1] if identity in self.entries else table
            for identity, table in check.items()
        }
        versions = None
        if table_versions is not None:
            versions = table_versions(
                [tabid for tabid in map(_table_id, known.values()) if tabid is not None]
            )
        stale: dict[str, int | None] = {}
        for identity, table in known.items():
            version = None
            found = versions.get(_table_id(table)) if versions is not None else None
            if found is not None and found[:2] == (
                table.owner.rstrip(" "),
                table.name.rstrip(" "),
            ):
                version = found[2]
            entry = self.entries.get(identity)
            if entry is None or version is None or entry[0] != version:
                stale[identity] = version
        if not stale:
            return
        logical = {identity: check[identity].identity for identity in stale}
        get_tables = getattr(bridge, "get_tables", None)
        if get_tables is not None:
            described = get_tables(sorted(set(logical.values())))
        else:
            described = {name: bridge.get_table(name) for name in sorted(set(logical.values()))}
        for identity, version in stale.items():
            raw = described.get(logical[identity])
            try:
                fresh = (
                    None
                    if raw is None or raw.get("unreadable")
                    else Table.parse(raw, check[identity].database)
                )
            except (InformixError, ValueError):
                fresh = None  # unreadable (see _describe_tables): the same, for that table alone
            if fresh is None:
                # The name no longer names a table: the subscriber is left to its own
                # direct read, which reports that.
                self.entries.pop(identity, None)
                continue
            if fresh.incarnation != known[identity].incarnation:
                # Re-created under a new table ID, whose version wasn't asked for: the
                # next check reads it again with its version.
                version = None
            self.entries[identity] = (version, fresh)


def _schema_fingerprint(table: Table) -> str:
    parts: tuple[Any, ...] = (
        table.database,
        table.owner,
        table.name,
        table.incarnation,
        table.primary_keys,
        tuple(
            (
                column.name,
                column.type_name,
                column.nullable,
                column.length,
                column.precision,
                column.scale,
                column.cdc_supported,
            )
            for column in table.columns
        ),
    )
    # Appended only when set, so a table using neither keeps the fingerprint every
    # existing checkpoint was written with.
    if table.column_selection is not None:
        parts += (("column_selection", table.column_selection),)
    if table.row_filter is not None:
        parts += (("row_filter", table.row_filter),)
    if table.column_renames is not None:
        parts += (("column_renames", table.column_renames),)
    if table.column_transforms is not None:
        parts += (("column_transforms", table.column_transforms, table.transform_key_id),)
    if table.decimal_targets:
        parts += (("decimal_targets", table.decimal_targets),)
    return hashlib.sha256(repr(parts).encode("utf-8")).hexdigest()


class _SharedCdcShard:
    # Docstring: One shard of the sharded daemon-reader CDC pool.
    #
    # A single daemon thread owns the Informix I/O for this shard's tables -- it reads
    # the shared CDC session, fetches schema, and samples the log bounds -- and publishes
    # the result here. Consumer streaming polls read the fanned-out transactions and the
    # schema/log position from memory rather than connecting themselves. All mutable
    # state is guarded by ``condition``.

    def __init__(self) -> None:
        self.condition = threading.Condition()
        # identity -> {table, capture, checkpoint, floor, last_seen}
        self.subscribers: dict[str, dict[str, Any]] = {}
        self.buffers: dict[str, deque] = {}
        # highest commit_lsn appended per table: dedups re-delivery and lets a lagging
        # or late subscriber replay its own history exactly once.
        self.cursors: dict[str, int] = {}
        self.shared_restart: int | None = None
        # coherent snapshot, published atomically at the end of each read cycle
        self.schema: dict[str, "Table"] = {}
        self.min_lsn: int | None = None
        self.current_lsn: int | None = None
        self.reached_lsn: int | None = None
        self.read_start_lsn: int | None = None
        self.caught_up = False
        self.open_begins: dict[str, int | None] = {}
        self.ready = False
        # daemon configuration, set by the first subscriber
        self.reader_factory: Callable[[], Any] | None = None
        self.timeout_seconds = 5
        self.max_records = _DEFAULT_CDC_MAX_RECORDS
        self.buffer_cap = _DEFAULT_SHARED_CDC_BUFFER
        self.subscriber_ttl = _SHARED_CDC_SUBSCRIBER_TTL_SECONDS
        # The daemon's catalog tables, re-read only when they change; daemon-thread only.
        self.catalog = _ShardCatalog()

    def configure(
        self, *, reader_factory, timeout_seconds, max_records, buffer_cap, subscriber_ttl
    ):
        # Every poll configures the shard with its own table's cdc.timeout and
        # cdc.max.records, which are table options. Taken as given, the shared session
        # flipped between the tables' values with the order of their polls. It keeps the
        # shortest timeout and the largest record budget any of them asked for: neither
        # makes a read wrong, only an idle read shorter or a read larger.
        with self.condition:
            if self.reader_factory is None:
                self.reader_factory = reader_factory
            if not self.__dict__.get("_read_settings_given"):
                self._read_settings_given = True
                self.timeout_seconds = timeout_seconds
                self.max_records = max_records
            else:
                self.timeout_seconds = min(self.timeout_seconds, timeout_seconds)
                self.max_records = max(self.max_records, max_records)
            self.buffer_cap = buffer_cap
            self.subscriber_ttl = subscriber_ttl

    # ---- consumer side ------------------------------------------------------

    def subscribe(self, identity, table, capture, checkpoint_commit_lsn, floor_lsn, owner=None):
        # ``owner`` names the flow asking (each connector instance has its own). Returns
        # False when another flow holds the table's place: this one then reads directly,
        # and must not take a view either, which would cut the other's buffer at its own
        # checkpoint.
        with self.condition:
            existing = self.subscribers.get(identity)
            expired = (
                existing is not None
                and time.monotonic() - existing["last_seen"] > self.subscriber_ttl
            )
            if (
                existing is not None
                and owner is not None
                and existing.get("owner") not in (None, owner)
                and not expired
            ):
                # A second flow reading the same table with the same layout (two
                # destinations, say). The table has one buffer, cut at one checkpoint:
                # served to both, the flow further on dropped transactions the other had
                # yet to read, which it then never saw, or each start-over wiped the
                # other's coverage. The first keeps the shard until it stops polling.
                return False
            own = existing is not None and (
                expired or (owner is not None and existing.get("owner") == owner)
            )
            if own and (
                existing["capture"] != capture
                or _schema_fingerprint(existing["table"]) != _schema_fingerprint(table)
            ):
                # This flow's own place, or one whose flow stopped polling, under a table that
                # changed since: a schema or key change the capture does not show too. Taken
                # for another flow's, it read directly until the place expired, and the
                # place's buffer, its checkpoint frozen, grew to the cap and held the shard.
                self._restart_table_locked(identity)
                existing = None
            elif (
                existing is not None
                and existing["capture"] == capture
                and _schema_fingerprint(existing["table"]) != _schema_fingerprint(table)
            ):
                # Another flow reading the table under other options (a schema change
                # changes the capture): the shard keeps the first one's table, buffer and
                # coverage, and this one reads directly.
                return False
            elif (
                existing is not None
                and existing["capture"] != capture
                and existing["table"].column_selection != table.column_selection
            ):
                # Another flow reading the table with other columns (include_columns or
                # exclude_columns): its capture differs for good, not because the table
                # changed. Taking it for a schema change, each flow dropped the other's
                # buffer and coverage at every poll, and neither was ever served. The
                # shard keeps the first one's, and this one reads directly.
                return False
            elif existing is not None and existing["capture"] != capture:
                # The table's columns changed (its consumer moved to a new schema): what
                # was buffered under the old capture lacks the new columns, so drop it and
                # treat the table as new, re-read from its floor under the new capture.
                self._restart_table_locked(identity)
                existing = None
            elif existing is not None and (
                checkpoint_commit_lsn < existing["checkpoint"] or floor_lsn < existing["floor"]
            ):
                # The checkpoint or floor moved back (a consumer replaying from an earlier
                # commit, or an open transaction older than its floor): the buffer may lack
                # what lies above the new floor and the cursor would skip re-reading it, so
                # start the table over from its floor, on direct reads until covered again.
                self.buffers.pop(identity, None)
                self.cursors.pop(identity, None)
                existing = None
            entry = {
                "table": table,
                "capture": capture,
                "checkpoint": checkpoint_commit_lsn,
                "floor": floor_lsn,
                "last_seen": time.monotonic(),
                # The lowest LSN a daemon read that captured this table was activated
                # at, or None until one has run. The table is *covered* -- its buffer
                # is authoritative from its floor on -- only once that read start is
                # at or below its floor (see _covered). Preserved across re-subscribes;
                # a TTL eviction drops the entry and so resets it.
                "covered_from": None if existing is None else existing.get("covered_from"),
                "owner": owner,
            }
            self.subscribers[identity] = entry
            # Drop what the consumer has durably consumed, as a view does. A consumer on
            # direct reads (the shard not ready, or the table not yet covered) gets no view,
            # so without this its buffer could reach the cap and stop the shard reading.
            buf = self.buffers.setdefault(identity, deque())
            while buf and buf[0].commit_lsn <= checkpoint_commit_lsn:
                buf.popleft()
            # Lower the shared read cursor to admit a newly subscribed or lagging
            # table's history. It only moves backward here; the daemon advances it
            # forward as it reads, but never past the floor of a table it has not yet
            # covered (see ingest). Activating too low is merely wasteful (re-read then
            # discarded per cursor / _recover); too high would skip a laggard's changes.
            if self.shared_restart is None:
                self.shared_restart = floor_lsn
            elif not self._covered(entry):
                self.shared_restart = min(self.shared_restart, floor_lsn)
            self.condition.notify_all()
            return True

    def _restart_table_locked(self, identity) -> None:
        # Docstring: Drop a table's buffer and cursor, so it is re-read from its floor as a
        # new one, and read its catalog entry at the daemon's next cycle, not the next
        # scheduled check. The caller makes its subscriber entry anew.

        self.buffers.pop(identity, None)
        self.cursors.pop(identity, None)
        self.catalog.force_check()

    def poll(self, identity, table, capture, checkpoint_commit_lsn, floor_lsn, owner=None):
        # Docstring: Subscribe and take the view in one hold of the lock, so no other flow's
        # subscribe cuts the buffer between them; None where the flow reads directly.

        with self.condition:
            if not self.subscribe(
                identity, table, capture, checkpoint_commit_lsn, floor_lsn, owner
            ):
                return None
            return self.snapshot(identity, checkpoint_commit_lsn)

    @staticmethod
    def _covered(entry) -> bool:
        # Docstring: Whether a daemon read that captured this subscriber started at or below
        # its floor, so its buffer holds every transaction above that floor.

        covered_from = entry.get("covered_from")
        return covered_from is not None and covered_from <= entry["floor"]

    def snapshot(self, identity, checkpoint_commit_lsn):
        # Docstring: Return a coherent view for one table, or None to fall back to a direct
        # read (daemon not ready yet, or this table is unknown to the daemon).

        with self.condition:
            if not self.ready or identity not in self.schema:
                return None
            entry = self.subscribers.get(identity)
            if entry is None or not self._covered(entry):
                # No daemon read that captured this table has started at or below its
                # floor yet, so its buffer may have a gap right after its checkpoint. A
                # view is taken as authoritative and would skip that gap silently;
                # keep the consumer on direct reads until a covering read is ingested.
                return None
            table = self.schema.get(identity)
            if table is None or self.min_lsn is None or self.current_lsn is None:
                return None
            if self.current_lsn < checkpoint_commit_lsn:
                # The consumer moved on by a direct read past the shard's last read: its log
                # position would look reinitialized (284) or behind a trigger boundary (295).
                return None
            buf = self.buffers.get(identity)
            if buf is None:
                return None
            # Drop durably-consumed transactions; keep the rest (peek, don't pop) so a
            # poll that consumes only part of its budget re-serves the remainder.
            while buf and buf[0].commit_lsn <= checkpoint_commit_lsn:
                buf.popleft()
            committed = list(buf)
            # ``committed`` is the entire retained buffer, so nothing is withheld:
            # the consumer is caught up exactly when the daemon reached end-of-log.
            caught_up = self.caught_up
            return _SharedCdcSnapshot(
                table=table,
                fingerprint=_schema_fingerprint(table),
                min_lsn=self.min_lsn,
                current_lsn=self.current_lsn,
                committed=committed,
                caught_up=caught_up,
                open_begin=self.open_begins.get(identity),
                reached_lsn=self.reached_lsn if caught_up else None,
                read_start_lsn=self.read_start_lsn if caught_up else None,
            )

    # ---- daemon side --------------------------------------------------------

    def live_subscribers(self):
        # Docstring: Drop subscribers past the TTL (a stopped flow must not pin the shard) and
        # return the current set. Caller holds the lock.

        now = time.monotonic()
        for identity in list(self.subscribers):
            if now - self.subscribers[identity]["last_seen"] > self.subscriber_ttl:
                self._evict_locked(identity)
        return dict(self.subscribers)

    def evict(self, identity):
        # Docstring: Drop one subscriber and its state; it re-subscribes on its next poll.

        with self.condition:
            self._evict_locked(identity)

    def _evict_locked(self, identity):
        self.subscribers.pop(identity, None)
        for store in (self.buffers, self.cursors, self.schema, self.open_begins):
            store.pop(identity, None)

    def ingest(self, raw, schema_map, min_lsn, current_lsn, read_start, captures=None):
        # Docstring: Assemble one read's records into transactions, fan them out per table, and
        # publish the coherent snapshot. Returns whether the shard is caught up.
        #
        # ``schema_map`` names the tables the read captured and ``read_start`` the LSN
        # it was activated at; together they record which subscribers it covered.
        # ``captures`` maps each to the capture the read used: a subscriber that has
        # re-subscribed with another since gets nothing from this read.

        buffer = TransactionBuffer()
        committed = []
        timed_out = False
        reached = None
        for record in raw:
            if _operation(record) == "TIMEOUT":
                timed_out = True
                reached = _lsn(record)
            done = buffer.feed(record)
            if done is not None:
                committed.append(done)
        committed.sort(key=lambda tx: (tx.commit_lsn, tx.tx_id))
        with self.condition:
            stale = {
                identity
                for identity, capture in (captures or {}).items()
                if (self.subscribers.get(identity) or {}).get("capture") != capture
            }
            # A subscriber started over (or joined) while this read was in flight, with a
            # floor below where the read began, gets nothing from it: a partial delivery
            # would move its cursor past the transactions the covering read must add.
            stale |= {
                identity
                for identity, sub in self.subscribers.items()
                if not self._covered(sub) and read_start > sub["floor"]
            }
            for tx in committed:
                for identity, sub in self.subscribers.items():
                    if identity in stale or tx.commit_lsn <= self.cursors.get(identity, 0):
                        continue
                    if any(
                        _record_matches(record, sub["table"])
                        for record in itertools.chain(tx.records, tx.spilled)
                    ):
                        self.buffers.setdefault(identity, deque()).append(tx)
                        self.cursors[identity] = tx.commit_lsn
            # A subscriber is covered once a read that captured it was activated at or
            # below its floor: from then on its buffer holds everything above the floor.
            for identity in schema_map:
                entry = self.subscribers.get(identity)
                if entry is None or identity in stale:
                    continue
                previous = entry.get("covered_from")
                entry["covered_from"] = (
                    read_start if previous is None else min(previous, read_start)
                )
            # Resume the next read from the oldest open BEGIN so open transactions are
            # recaptured; per-table cursors above dedup the resulting re-delivery.
            global_open = min((item.begin_lsn for item in buffer.open.values()), default=None)
            if global_open is not None:
                advanced = global_open
            elif timed_out and reached is not None:
                # Idle with nothing open: everything the shard captures up to where the
                # log reader stopped has been delivered, so resume from there. Resuming
                # from the last commit instead rescans the same stretch of a busy log on
                # every poll and can time out before ever passing it. A position held back
                # short of where the read started (a quiet log, see _shared_cdc_snapshot)
                # resumes from the start: the read had nothing to deliver short of it.
                advanced = max(
                    read_start, reached, committed[-1].commit_lsn if committed else reached
                )
            elif committed:
                advanced = committed[-1].commit_lsn
            else:
                advanced = self.shared_restart
            # Never advance past the floor of a table this read did not cover. A
            # subscriber that joined (or re-joined after TTL eviction) while the read
            # was in flight lowered shared_restart to its floor; overwriting that here
            # left its log between the floor and this read's end unread forever, and
            # its first view then skipped that range silently.
            pending = [
                entry["floor"] for entry in self.subscribers.values() if not self._covered(entry)
            ]
            candidates = [value for value in (advanced, *pending) if value is not None]
            if candidates:
                self.shared_restart = min(candidates)
            open_begins: dict[str, int | None] = {}
            for identity, sub in self.subscribers.items():
                begins = [
                    item.begin_lsn
                    for item in buffer.open.values()
                    if any(
                        _record_matches(record, sub["table"])
                        for record in itertools.chain(item.records, item.spilling)
                    )
                ]
                open_begins[identity] = min(begins) if begins else None
            self.open_begins = open_begins
            self.schema = dict(schema_map)
            self.min_lsn = min_lsn
            # current_lsn was sampled before the read, which can then reach further, into the
            # next logical log: a view must not report a log position older than the commits
            # it hands out, or a consumer that took them reads as being ahead of the log.
            self.current_lsn = max(
                current_lsn, reached or 0, committed[-1].commit_lsn if committed else 0
            )
            self.caught_up = timed_out and not buffer.open
            self.reached_lsn = reached if self.caught_up else None
            self.read_start_lsn = read_start if self.caught_up else None
            self.ready = True
            self.condition.notify_all()
        return self.caught_up

    def mark_unready(self):
        with self.condition:
            self.ready = False


# Serialization-safe registry (a bare module global becomes a function local in the
# merged deployment; a mutated dict survives, as with _lakebase_waiter_lock).
_SHARED_CDC_SHARDS: dict[tuple, _SharedCdcShard] = {}
_SHARED_CDC_THREADS: dict[tuple, threading.Thread] = {}
_SHARED_CDC_REGISTRY_LOCK: dict[str, threading.Lock] = {}


def _shared_cdc_registry_lock() -> threading.Lock:
    lock = _SHARED_CDC_REGISTRY_LOCK.get("lock")
    if lock is None:
        lock = _SHARED_CDC_REGISTRY_LOCK.setdefault("lock", threading.Lock())
    return lock


def _ensure_key_columns_exist(table: Table) -> None:
    # Docstring: Fail when a carried key names a column the catalog no longer has.
    #
    # A primary.keys override or a nullable-index promotion is carried as listed, so a
    # key column dropped at the source would otherwise outlive the refresh and every row
    # would then key on NULL.

    known = {column.name for column in table.columns}
    missing = [key for key in table.primary_keys if key not in known]
    if missing:
        # 373: Key column(s) {} of '{}' no longer exist at the source; run a full refresh
        raise InformixError(_m(373, (", ".join(missing)), table.exposed_name))


def _wildcard_key(columns: Iterable[Column], excepted: Iterable[str]) -> list[str]:
    # Docstring: The names of ``columns`` a wildcard key covers: all of them, in order, but
    # the ``excepted`` ones of a "* except(...)" key.
    #
    # Matched ignoring case, as Informix folds an undelimited identifier to lower case, so
    # "* except(Note)" leaves out a column catalogued as ``note``.

    folded = {name.casefold() for name in excepted or ()}
    return [column.name for column in columns if column.name.casefold() not in folded]


def _ensure_excepted_columns_exist(table: Table, excepted: Iterable[str]) -> None:
    # Docstring: Fail when a "* except(...)" key names a column the table doesn't have.
    #
    # Checked against every catalog column, those the column selection or an unsupported
    # type leaves out included, and ignoring case as _wildcard_key matches, so a typo fails
    # instead of quietly keeping the column it meant in the key. After a named column is
    # dropped at the source, take it out of the list: the key stays as it was, so the
    # schema transition accepts that edit. A column still to be added can't be named
    # ahead; add it to the list once it exists, before the pipeline restarts.

    known = {column.name.casefold() for column in table.catalog_columns or table.columns}
    missing = [name for name in excepted or () if name.casefold() not in known]
    if missing:
        # 374: Option '{}' excepts column(s) that '{}' does not have: {}
        raise InformixError(_m(374, _PRIMARY_KEYS_OPTION, table.exposed_name, (", ".join(missing))))


def _carry_key_override(fresh: Table, overridden: Table) -> Table:
    # Docstring: Carry a ``primary.keys`` override from ``overridden`` onto a re-read ``fresh``
    # table.
    #
    # An explicit key list is preserved as-is, with its adopted key-index directions. A
    # wildcard key is re-expanded to ``fresh``'s columns, less any its except(...) names,
    # keeping the directions only when the key is unchanged. A name whose column was
    # dropped fails, as it does when the option is applied.

    keys = overridden.primary_keys
    descending = overridden.key_descending
    if overridden.key_wildcard:
        _ensure_excepted_columns_exist(fresh, overridden.key_wildcard_except)
        keys = tuple(_wildcard_key(fresh.columns, overridden.key_wildcard_except))
        if not keys:
            # 192: Option '{}' excepts every column of '{}', leaving no key
            raise InformixError(_m(192, _PRIMARY_KEYS_OPTION, fresh.exposed_name))
        if keys != overridden.primary_keys:
            descending = ()
    return replace(
        fresh,
        primary_keys=keys,
        key_override=True,
        key_wildcard=overridden.key_wildcard,
        key_wildcard_except=overridden.key_wildcard_except,
        key_descending=descending,
        # As the option was applied: a key a nullable index gave the table before it,
        # which the catalog's re-read no longer has, still makes the option's key not its
        # only one.
        key_only_from_option=overridden.key_only_from_option and not fresh.primary_keys,
    )


def _recorded_transforms(recorded: tuple | None) -> tuple | None:
    # Docstring: Parse recorded (column, canonical) transforms back to (column, text, operand).

    if recorded is None:
        return None
    return tuple(
        (
            name,
            canonical,
            _RowFilterParser(canonical, _TRANSFORM_COLUMNS_OPTION, "transform").parse_expression(),
        )
        for name, canonical in recorded
    )


def _carry_table_options(fresh: Table, previous: Table) -> Table:
    # Docstring: Re-apply ``previous``'s option-derived state to ``fresh``, re-read from
    # the catalog.
    #
    # A catalog read knows nothing of table options, so without this a refresh would
    # revert a column selection, a primary.keys override, a nullable-index promotion, or a
    # row_filter, change the fingerprint, and read as drift. The selection runs first so a
    # wildcard key re-expands over the selected columns only.

    fresh = _apply_column_selection(fresh, previous.column_selection)
    # The key the carry below gives ``fresh``: the override or promotion carried, else
    # the catalog's; a wildcard keys on whatever is ingested.
    if previous.key_wildcard:
        key = None
    elif previous.key_override or previous.key_from_nullable_index:
        key = previous.primary_keys
    else:
        key = fresh.primary_keys
    fresh = _leave_out_unsupported_columns(fresh, key)
    if previous.key_override:
        # A wildcard key is re-expanded to the fresh columns instead of kept as listed.
        fresh = _carry_key_override(fresh, previous)
    elif previous.key_from_nullable_index and not fresh.primary_keys and fresh.nullable_index_key:
        # The catalog re-derives the table as key-less (no all-NOT-NULL index), so without
        # this the promoted key -- and its directions -- would be dropped and the
        # key-order guard would trip mid-copy. Promoted again from this read, as the
        # option does: copied from the earlier one, an index dropped or replaced since, or
        # a primary key added, read as no change, and the key drift went unseen.
        fresh = replace(
            fresh,
            primary_keys=fresh.nullable_index_key,
            key_descending=fresh.nullable_index_descending,
            key_from_nullable_index=True,
        )
    _ensure_key_columns_exist(fresh)
    _ensure_selected_keys(fresh)
    fresh = _apply_row_filter(fresh, previous.row_filter)
    fresh = _apply_column_renames(fresh, previous.column_renames)
    if previous.decimal_targets is not None:
        # Those of the columns still there: a dropped column takes its entry with it, and
        # one the table gained takes the table-wide type, which the catalog doesn't know.
        known = {column.name for column in previous.columns}
        entries = dict(previous.decimal_targets)
        targets = []
        for column in fresh.columns:
            if not _is_variable_scale_decimal(column):
                continue
            if column.name in entries:
                targets.append((column.name, entries[column.name]))
            elif column.name not in known and previous.decimal_default is not None:
                targets.append((column.name, previous.decimal_default))
        fresh = replace(
            fresh, decimal_targets=tuple(sorted(targets)), decimal_default=previous.decimal_default
        )
    carried = _recorded_transforms(previous.column_transforms)
    return _apply_column_transforms(fresh, carried, None, key_id=previous.transform_key_id)


def _shard_schema_map(
    bridge, identities, catalog: _ShardCatalog | None = None, check_seconds: float = 0
) -> tuple[dict[str, Table], list[str]]:
    # Docstring: Each subscriber's catalog table with its options carried, as
    # _refresh_table_schema does, for one shard read: ``(identity -> table, unfit)``.
    #
    # The catalog tables come from ``catalog``, which re-reads only what changed (see
    # _ShardCatalog); without one, every table is read now. Without the carry an overridden
    # table's fingerprint never matches its checkpoint, so it would always fall back to a
    # direct read and never benefit from sharding -- and its PK-change detection would use
    # the wrong keys. A table whose options no longer fit its catalog (a selected, renamed,
    # or transformed column gone, a carried key column dropped), or whose name no longer
    # names a table, is listed as unfit instead of failing the whole read: its own direct
    # read reports the error, and the shard keeps serving the others.

    catalog = _ShardCatalog() if catalog is None else catalog
    fresh_tables = catalog.tables(bridge, identities, time.monotonic(), check_seconds)
    schema_map: dict[str, Table] = {}
    unfit: list[str] = []
    for identity, table in identities:
        fresh = fresh_tables.get(identity)
        if fresh is None:
            # 152: Shared CDC shard leaves {} to direct reads: the catalog no longer has it
            logging.getLogger(__name__).warning(
                _m(152, identity),
            )
            unfit.append(identity)
            continue
        try:
            schema_map[identity] = _carry_table_options(fresh, table)
        except (InformixError, ValueError):
            # 153: Shared CDC shard leaves {} to direct reads: its table options no longer fit the
            # catalog
            logging.getLogger(__name__).warning(
                _m(153, identity),
                exc_info=True,
            )
            unfit.append(identity)
    return schema_map, unfit


def _close_shared_cdc_reader(reader) -> None:
    if reader is None:
        return
    try:
        reader.close()
    except Exception:  # noqa: BLE001 - best-effort slot release
        # 154: shared CDC reader close failed
        logging.getLogger(__name__).debug(_m(154), exc_info=True)


class _HostShardEndpoints:
    # Docstring: This process's part in each host-shared shard: the lock and listener of a
    # shard it leads, or its connection to the leader of one it follows.
    #
    # Never serialized: a worker that unpickles a reader starts with no roles of its own.

    def __init__(self, directory: str | None = None) -> None:
        self.lock = threading.Lock()
        self.directory = directory
        # key -> {"lock_fd", "listener", "reader_factory"}: the shards this process leads.
        self.leading: dict[tuple, dict[str, Any]] = {}
        self.clients: dict[tuple, Any] = {}
        self.client_locks: dict[tuple, threading.Lock] = {}
        # (key, identity) -> (checkpoint commit LSN, floor, capture, transactions) a follower
        # holds from its earlier polls, so the leader sends each transaction only once.
        self.mirrors: dict[tuple, tuple[int, int, dict, tuple]] = {}
        # Set when this process can't lock or serve a shard at all, so it shares in
        # process mode instead of reading directly on every poll.
        self.unavailable: str | None = None

    def __getstate__(self) -> dict[str, object]:
        return {}

    def __setstate__(self, state: dict[str, object]) -> None:
        del state
        self.__init__()

    def paths(self, key: tuple) -> tuple[str, str, str]:
        # Docstring: The shard's (lock, socket, auth key) paths, short enough for AF_UNIX.
        #
        # Whoever can write the directory could serve forged CDC rows, so one that isn't
        # this user's alone raises, which turns host sharing off in this process.

        directory = self.directory
        if directory is None:
            name = f"informix-cdc-shards-{os.getuid()}"
            directory = os.path.join(tempfile.gettempdir(), name)
            # A socket path must fit AF_UNIX's sun_path (104 bytes on macOS and BSD, 108 on
            # Linux); a long TMPDIR would otherwise turn host sharing off. /tmp is short.
            if len(os.fsencode(os.path.join(directory, "0" * 24 + ".sock"))) > 100:
                directory = os.path.join("/tmp", name)
        os.makedirs(directory, mode=0o700, exist_ok=True)
        info = os.lstat(directory)
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            # 143: {} is not a directory only this user can access
            raise PermissionError(_m(143, directory))
        digest = hashlib.sha256(repr(key).encode()).hexdigest()[:24]
        base = os.path.join(directory, digest)
        return f"{base}.lock", f"{base}.sock", f"{base}.key"


_HOST_SHARD_ENDPOINTS = _HostShardEndpoints()


def _host_shard_stop_leading(
    endpoints: _HostShardEndpoints, key: tuple, *, locked: bool = False
) -> None:
    # Docstring: Give up leading a shard nobody reads any more: close its listener, remove
    # its socket and key files, and release its lock. A follower that polls later finds
    # no leader and leads it itself. The lock file stays: removing it could let two
    # processes lock different files of the same name. ``locked``: the caller holds the
    # endpoints lock.

    with contextlib.nullcontext() if locked else endpoints.lock:
        leading = endpoints.leading.pop(key, None)
        if leading is None:
            return
        with contextlib.suppress(Exception):
            leading["listener"].close()
        with contextlib.suppress(Exception):
            _lock_path, socket_path, key_path = endpoints.paths(key)
            for path in (socket_path, key_path):
                with contextlib.suppress(OSError):
                    os.unlink(path)
        with contextlib.suppress(OSError):
            os.close(leading["lock_fd"])


def _retire_shared_cdc_shard(key: tuple, shard: _SharedCdcShard) -> bool:
    # Docstring: Take an idle shard out of the registry, and give up leading it for other
    # processes; False when a subscriber has come back. A consumer that asks for the
    # shard afterwards gets a new one, with a new thread.

    # The lead is given up under the locks the shard leaves the registry under, the
    # endpoints lock first, as _host_shard_own_poll takes it: a poll that found this
    # process leading between the two made a new shard and thread here, while another
    # process took the lock and led the shard too.
    # A shard already out of the registry (given up, see _host_shard_accept) leaves the
    # lead alone: it is a newer shard's, if this process leads the key again.
    endpoints = _HOST_SHARD_ENDPOINTS
    with endpoints.lock, _shared_cdc_registry_lock():
        with shard.condition:
            if shard.live_subscribers():
                return False
        if _SHARED_CDC_SHARDS.get(key) is shard:
            _SHARED_CDC_SHARDS.pop(key, None)
            _SHARED_CDC_THREADS.pop(key, None)
            _host_shard_stop_leading(endpoints, key, locked=True)
    return True


# pylint: disable-next=too-many-statements
def _run_shared_cdc_shard(shard: _SharedCdcShard, key: tuple | None = None) -> None:
    # Docstring: Daemon loop for one shard: read the shared CDC session, fetch schema + log
    # bounds, and publish. Holds one connection slot only across consecutive productive
    # reads; releases it before any idle wait so consumer bootstrap can use the slot.
    # Ends once the shard has had no subscriber for _SHARED_CDC_IDLE_EXIT_SECONDS.

    logger = logging.getLogger(__name__)
    reader = None
    idle_since: float | None = None
    try:
        while True:
            with shard.condition:
                subscribers = shard.live_subscribers()
                have_subscribers = bool(subscribers)
            if have_subscribers:
                idle_since = None
            elif key is not None:
                idle_since = idle_since or time.monotonic()
                if (
                    time.monotonic() - idle_since >= _SHARED_CDC_IDLE_EXIT_SECONDS
                    and _retire_shared_cdc_shard(key, shard)
                ):
                    return
            with shard.condition:
                throttled = have_subscribers and any(
                    len(buf) >= shard.buffer_cap for buf in shard.buffers.values()
                )
                restart = shard.shared_restart
                identities = [(identity, sub["table"]) for identity, sub in subscribers.items()]
            if not have_subscribers or throttled or restart is None or shard.reader_factory is None:
                _close_shared_cdc_reader(reader)
                reader = None
                with shard.condition:
                    shard.condition.wait(timeout=_SHARED_CDC_IDLE_POLL_SECONDS)
                continue
            if reader is None:
                reader = shard.reader_factory()
            try:
                schema_map, unfit = _shard_schema_map(
                    reader._bridge,
                    identities,
                    shard.catalog,
                    _SNAPSHOT_SCHEMA_REFRESH_INTERVAL_SECONDS,
                )
                for identity in unfit:
                    # Its consumer falls back to a direct read, which reports the error;
                    # evicting it keeps its floor from pinning the shared restart.
                    shard.evict(identity)
                if not schema_map:
                    _close_shared_cdc_reader(reader)
                    reader = None
                    with shard.condition:
                        shard.condition.wait(timeout=_SHARED_CDC_IDLE_POLL_SECONDS)
                    continue
                captures = {identity: subscribers[identity]["capture"] for identity in schema_map}
                min_lsn = reader._bridge.minimum_lsn()
                current_lsn = reader._bridge.current_lsn()
                raw = reader._read_changes_with_reconnect(
                    list(captures.values()),
                    restart,
                    shard.timeout_seconds,
                    shard.max_records,
                    table=subscribers[next(iter(schema_map))]["table"],
                )
                caught_up = shard.ingest(raw, schema_map, min_lsn, current_lsn, restart, captures)
            except Exception:  # noqa: BLE001 - surfaced by degrading to direct reads
                # 155: Shared CDC shard read failed; consumers fall back to direct reads until it
                # recovers
                logger.warning(
                    _m(155),
                    exc_info=True,
                )
                shard.mark_unready()
                # Whatever failed, the next read starts from a fresh look at the catalog.
                shard.catalog.force_check()
                _close_shared_cdc_reader(reader)
                reader = None
                time.sleep(_SHARED_CDC_IDLE_POLL_SECONDS)
                continue
            if caught_up:
                _close_shared_cdc_reader(reader)
                reader = None
                with shard.condition:
                    shard.condition.wait(timeout=_SHARED_CDC_IDLE_POLL_SECONDS)
    finally:
        _close_shared_cdc_reader(reader)


def _shared_cdc_shard(
    key, *, reader_factory, timeout_seconds, max_records, buffer_cap, subscriber_ttl
) -> _SharedCdcShard:
    with _shared_cdc_registry_lock():
        shard = _SHARED_CDC_SHARDS.get(key)
        if shard is None:
            shard = _SHARED_CDC_SHARDS.setdefault(key, _SharedCdcShard())
        shard.configure(
            reader_factory=reader_factory,
            timeout_seconds=timeout_seconds,
            max_records=max_records,
            buffer_cap=buffer_cap,
            subscriber_ttl=subscriber_ttl,
        )
        thread = _SHARED_CDC_THREADS.get(key)
        if thread is None or not thread.is_alive():
            thread = threading.Thread(
                target=_run_shared_cdc_shard,
                args=(shard, key),
                daemon=True,
                name=f"informix-shared-cdc-{key[-2]}-{key[-1]}",
            )
            _SHARED_CDC_THREADS[key] = thread
            thread.start()
        return shard


# Host-shared shards (cdc.shared.session=host, the default). Serverless SDP runs each
# streaming flow's driver-side reader in its own Python process, all in one container on
# the driver host, so a process-local shard serves only its own flow's table and every
# flow opens its own CDC session. In host mode the first process to lock a shard's lock
# file leads it: it runs the shard and its reader thread exactly as process mode does,
# and serves the other flow processes' polls over a Unix socket in the local temp
# directory. A follower that can't reach the leader tries to take the lock, which the
# kernel releases when the leader's process exits, and otherwise reads directly for that
# poll, as any flow does while its shard isn't ready. A new leader starts an empty shard,
# which its subscribers cover again from their own checkpoints, as after a restart.
_SHARED_CDC_SESSION_HOST = "host"
_SHARED_CDC_SESSION_PROCESS = "process"
# How long a follower waits to connect to the leader, or for it to answer one poll,
# before reading directly.
_HOST_SHARD_REPLY_SECONDS = 60.0
# Consecutive accept failures after which a leader gives its shard up to another process.
_HOST_SHARD_ACCEPT_FAILURES = 8


def _shared_cdc_mode(options: dict[str, str]) -> str:
    # Docstring: ``cdc.shared.session`` as host (the default) or process.
    #
    # ``host`` shares the shards between the flow processes on one host; ``process``
    # keeps one shard registry per process.

    value = options.get(_SHARED_CDC_SESSION_OPTION, _SHARED_CDC_SESSION_HOST).strip().lower()
    if value in (_SHARED_CDC_SESSION_HOST, _SHARED_CDC_SESSION_PROCESS):
        return value
    # 142: Option '{}' must be 'host' or 'process'
    raise ValueError(_m(142, _SHARED_CDC_SESSION_OPTION))


def _unwire_table(data: dict[str, Any]) -> Table:
    fields = dict(data)
    for name in _HOST_SHARD_TABLE_COLUMN_FIELDS:
        fields[name] = tuple(Column(**column) for column in fields[name])
    return Table(**fields)


def _unwire_json_table(data: dict[str, Any]) -> Table:
    # Docstring: _unwire_table for a table that crossed JSON (a partition descriptor), which
    # turned its tuples into lists.

    def tuples(value: Any) -> Any:
        return tuple(tuples(item) for item in value) if isinstance(value, list) else value

    return _unwire_table(
        {
            name: value if name in _HOST_SHARD_TABLE_COLUMN_FIELDS else tuples(value)
            for name, value in data.items()
        }
    )


def _wire_view(view: "_SharedCdcSnapshot") -> dict[str, Any]:
    # Docstring: A shard view as plain data; its records already are (decoded CDC values).

    return {
        "table": _wire_table(view.table),
        "fingerprint": view.fingerprint,
        "min_lsn": view.min_lsn,
        "current_lsn": view.current_lsn,
        "committed": [
            (
                tx.tx_id,
                tx.begin_lsn,
                tx.commit_lsn,
                tx.restart_lsn,
                tx.records,
                tx.commit_time,
                tx.spilled,
            )
            for tx in view.committed
        ],
        "caught_up": view.caught_up,
        "open_begin": view.open_begin,
        "reached_lsn": view.reached_lsn,
        "read_start_lsn": view.read_start_lsn,
    }


def _unwire_view(data: dict[str, Any]) -> "_SharedCdcSnapshot":
    return _SharedCdcSnapshot(
        table=_unwire_table(data["table"]),
        fingerprint=data["fingerprint"],
        min_lsn=data["min_lsn"],
        current_lsn=data["current_lsn"],
        committed=[CommittedTransaction(*tx) for tx in data["committed"]],
        caught_up=data["caught_up"],
        open_begin=data["open_begin"],
        reached_lsn=data["reached_lsn"],
        read_start_lsn=data.get("read_start_lsn"),
    )


def _host_shard_local_poll(
    key, reader_factory, config, identity, table, capture, commit, floor, owner=None
):
    # Docstring: Subscribe to and read this process's own shard, as process mode does;
    # None for a flow the shard does not serve because another holds the table's place.

    shard = _shared_cdc_shard(key, reader_factory=reader_factory, **config)
    return shard.poll(identity, table, capture, commit, floor, owner)


def _host_shard_own_poll(endpoints, key, reader_factory, config, *poll):
    # Docstring: _host_shard_local_poll of this process's own shard: within the process
    # where the host can't share, else as its leader; None, to read directly, once this
    # process leads it no more.
    #
    # The leader takes the shard under the endpoints lock, which an idle shard's retirement
    # holds while it leaves the registry and gives the lead up (_retire_shared_cdc_shard).
    # Taken after a check without it, a poll between the two made a new shard and reader
    # thread in a process that then let its lock go, beside the leader another process
    # became: two sessions reading one shard's tables.

    if endpoints.unavailable:
        return _host_shard_local_poll(key, reader_factory, config, *poll)
    with endpoints.lock:
        if key not in endpoints.leading:
            return None
        shard = _shared_cdc_shard(key, reader_factory=reader_factory, **config)
    return shard.poll(*poll)
