"""Pipeline registration: reader hooks, replay ranges, the driver's captured context, and the
merged file's pickle trimming.
"""

from __future__ import annotations

import contextlib
import dis
import itertools
import json
import logging
import operator
import os
import random
import secrets
import sys
import threading
import time
import types
from dataclasses import dataclass
from typing import Any

from databricks.labs.community_connector.sources.informix.cdc_protocol import frame_log_file
from databricks.labs.community_connector.sources.informix.informix_options import (
    _DEFAULT_CONNECTION_WAIT_TIMEOUT_SECONDS,
    _IDENTIFIER,
    _INTERNAL_COLUMNS,
    _OFFSET_ADVISORY_FIELDS,
    _PIPELINE_SCOPE,
    _REPLAY_CONNECTION_WAIT_BUDGET_OPTION,
    _REPLAY_END_SEED_OPTION,
    _REPLAY_INITIAL_END_OPTION,
    _REPLAY_REESTABLISHED,
    _REPLAY_RESNAPSHOT_OPTION,
    _REPLAY_STOP_LSN_OPTION,
    _REPLAY_STOP_PK_OPTION,
    InformixError,
    _join_identity,
    _m,
    _sql_identifier,
)
from databricks.labs.community_connector.sources.informix.lakebase_workspace import (
    _CAPTURED_WORKSPACE,
    capture_workspace_credentials,
    pipeline_events,
    pipeline_update,
    pipeline_updates,
)

_CAPACITY_ATTEMPT_MAX_SECONDS = 60.0
# Fraction of the sweep gap a maximally-ranked waiter keeps. A *blocking* waiter
# (a triggered flow, or a latency-tolerant snapshot/incremental copy) never gives
# up, so the per-attempt budget above cannot prioritise it -- for those readers
# the only lever is how often they re-sweep, because that decides who reaches a
# freed slot first. Measured in production: every ranked offset belonged to a
# steady-state CDC flow and every backlog streak to a blocking bulk-copy flow, so
# the two never met and ranking changed nothing for the readers with the most
# outstanding work. Scaling the gap rather than adding passes keeps the metadata
# rate bounded: at the floor a top-ranked waiter sweeps ~2x as often as an
# unranked one, not tens of times.
_SLOT_SWEEP_RANK_FLOOR = 0.5
_SHARED_STATE_OPEN_ATTEMPTS = 8
# Ceiling on the jittered backoff between reopen attempts after the Volume's FUSE
# mount drops (ENOTCONN). A remount settles in seconds, so the eight attempts span
# roughly half a minute in total -- long enough to ride out a blip, short enough
# that a genuine outage still surfaces as a failed flow rather than a silent hang.
_SHARED_STATE_MOUNT_RETRY_MAX_SECONDS = 8.0
# Bound on cached per-table registration lookups shared across reader instances
# in one process. Entries are single resolved tables (keyed by source identity +
# exposed name), so this can be generous -- a pipeline registers one entry per
# configured table.
_MAX_REGISTRATION_TABLE_CACHES = 512
# Predecessor hops a same-position channel start may follow back to its first schema.
_MAX_SCHEMA_DESCENT = 256
_INFORMIX_REGISTRATION_CONTEXT: dict[str, Any] = {"scope": None, "request": None, "reset_at": None}
# How old the driver's captured workspace credential may get before the driver, packing a
# registration, captures it again: the ambient token lasts about an hour.
_CREDENTIAL_RECAPTURE_SECONDS = 900.0
_CREDENTIAL_CAPTURED: dict[str, float] = {}
# An index-ordered snapshot records its progress beside its staged pages, so that a restart
# after a failure continues from the last page instead of the table's first row. The
# record of an earlier update is used only while it is this recent: its pages are swept
# after snapshot.staging.retention.days, and the log it needs is not kept for ever.
_SCAN_PROGRESS_VERSION = 1
_SCAN_RESUME_SECONDS = 24 * 3600


def _update_purpose(update: dict[str, Any]) -> str:
    # Docstring: What an update of the pipelines API was started for (see _update_request).

    if update.get("cause") == "RETRY_ON_FAILURE":
        return "continue"
    # Tables named by name or by source file (an editor's "run file with full refresh").
    selected = any(
        update.get(name) for name in ("full_refresh_selection", "full_refresh_selection_by_path")
    )
    # Only a refresh with no selection of any kind resets every table.
    partial = selected or any(
        update.get(name) for name in ("refresh_selection", "refresh_selection_by_path")
    )
    if update.get("full_refresh") and not partial:
        return "full_refresh"
    return "selective_refresh" if selected or update.get("full_refresh") else "continue"


def _update_request(pipeline_id: str, update_id: str) -> str | None:
    # Docstring: What this update was started for: ``"full_refresh"`` when a full refresh of
    # the whole pipeline was asked for, ``"selective_refresh"`` for one of some tables,
    # ``"continue"`` otherwise, None when the pipelines API cannot say.
    #
    # An automatic retry of a failed full refresh is itself marked a full refresh, with
    # the cause RETRY_ON_FAILURE: it carries on with what the failed update began, and so
    # counts as "continue". A selective refresh is told apart because its tables left out
    # keep their checkpoints, which a whole one resets: a delete reader takes this update's
    # start itself only in a whole one (_full_refresh_delete_seed).

    update = pipeline_update(pipeline_id, update_id)
    return None if update is None else _update_purpose(update)


# Where a full refresh's update has emptied every target and checkpoint: past RESETTING.
_RESET_STATES = frozenset({"SETTING_UP_TABLES", "RUNNING", "STOPPING", "COMPLETED"})


def _update_reset(pipeline_id: str, update: dict[str, Any]) -> bool:
    # Docstring: Whether an update's events show it past RESETTING, which empties the targets.

    for event in pipeline_events(pipeline_id, str(update.get("start_time"))) or ():
        origin = event.get("origin") if isinstance(event.get("origin"), dict) else {}
        if origin.get("update_id") != update.get("update_id"):
            continue
        details = event.get("details") if isinstance(event.get("details"), dict) else {}
        progress = details.get("update_progress")
        state = progress.get("state") if isinstance(progress, dict) else None
        if state in _RESET_STATES:
            return True
        if state in {"CANCELED", "FAILED"}:
            return False
    return False


def _last_reset(pipeline_id: str) -> float | None:
    # Docstring: When the pipeline's latest full refresh of every table that emptied the
    # targets was created, in seconds since the epoch, or None when the pipelines API can't
    # say or lists none.
    #
    # One stopped as early as it started emptied nothing: one that completed did, and one
    # stopped or failed did if its events show it past RESETTING. A snapshot floor written
    # before it names rows no target holds (_floor_predates_reset).

    refreshes = 0
    for update in pipeline_updates(pipeline_id) or ():
        if _update_purpose(update) != "full_refresh":
            continue
        try:
            created = int(update["creation_time"]) / 1000
        except (KeyError, TypeError, ValueError):
            return None
        if update.get("state") == "COMPLETED" or _update_reset(pipeline_id, update):
            return created
        refreshes += 1
        if refreshes >= 3:
            return None
    return None


def _optional_int(value: Any) -> int | None:
    return None if value is None or value == "" else int(value)


# These dataclasses generate only the methods the connector uses: none is printed,
# and only Column is compared by value (the schema-change check compares column
# tuples). The merged deployment pickles every generated method, so the rest are off.
@dataclass(frozen=True, repr=False)
class Column:
    name: str
    type_name: str
    nullable: bool = True
    length: int | None = None
    precision: int | None = None
    scale: int | None = None
    cdc_supported: bool = True

    @classmethod
    def parse(cls, raw: dict[str, Any]) -> "Column":
        return cls(
            name=str(raw["name"]),
            type_name=str(raw.get("type_name") or raw.get("type") or "VARCHAR").upper(),
            nullable=bool(raw.get("nullable", True)),
            length=_optional_int(raw.get("length")),
            precision=_optional_int(raw.get("precision")),
            scale=_optional_int(raw.get("scale")),
            cdc_supported=bool(raw.get("cdc_supported", True)),
        )


@dataclass(frozen=True, eq=False, repr=False)
class Table:
    database: str
    owner: str
    name: str
    columns: tuple[Column, ...]
    primary_keys: tuple[str, ...]
    incarnation: str | None = None
    # True when primary_keys came from the primary.keys option rather than the
    # catalog, so a schema refresh preserves them instead of reverting to the
    # catalog's (empty, for a physically keyless table). Not part of the schema
    # fingerprint -- only the resulting primary_keys are.
    key_override: bool = False
    # True when that override is the ``primary.keys: "*"`` wildcard. A schema refresh then
    # re-expands the key to the refreshed columns instead of preserving the old list, so an
    # added column joins the key; the schema transition accepts that growth (see
    # _PRIMARY_KEYS_WILDCARD). Not fingerprinted either.
    key_wildcard: bool = False
    # The columns a ``"* except(...)"`` wildcard leaves out of the key, kept so a refresh
    # re-expands it the same way; empty for a bare ``*``. Not fingerprinted: the resolved
    # primary_keys are, so changing the list changes the key and needs a full refresh.
    # None where no primary.keys option set it, or a recorded schema doesn't say: an empty
    # list is a bare "*" (see _ensure_compatible_schema_change).
    key_wildcard_except: tuple[str, ...] | None = None
    # Per-column key index direction, parallel to primary_keys: True == DESC.
    # Empty means all-ascending (the common case) -- paging then uses the default
    # lexicographic order untouched. A mixed/descending key carries the full
    # per-column list so snapshot paging and the key bound match the index's own
    # scan order instead of forcing a sort. Derived from the same index as the
    # keys, so it is not fingerprinted separately. Unknown for a primary.keys
    # override (no matching index), so it is cleared to () there.
    key_descending: tuple[bool, ...] = ()
    # A nullable-column UNIQUE index the connector may promote to the key when the
    # table sets allow.nullable.index (columns + parallel directions). A catalog
    # fact, populated only when no PK and no all-NOT-NULL unique index exist; not
    # fingerprinted. ``key_from_nullable_index`` records that the current
    # primary_keys came from it, so a schema refresh preserves the promotion.
    nullable_index_key: tuple[str, ...] = ()
    nullable_index_descending: tuple[bool, ...] = ()
    key_from_nullable_index: bool = False
    # True when the table has no key of its own and primary_keys is the primary.keys
    # option's. The key then says how the target merges, not how the table is best read:
    # its snapshot pages by an index, as a keyless table's does (see _scan_index). Not
    # fingerprinted.
    key_only_from_option: bool = False
    # The include_columns / exclude_columns selection as (mode, sorted names), or None.
    # ``columns`` then holds only the selected columns, and ``catalog_columns`` every
    # catalog column, which a positional `.unl` extract still needs. The selection and
    # the canonical row_filter text are fingerprinted when set, so changing either on a
    # running stream fails closed rather than silently diverging from rows already landed.
    column_selection: tuple[str, tuple[str, ...]] | None = None
    catalog_columns: tuple[Column, ...] = ()
    # Columns of a type the connector can't read, left out of ``columns`` (which sets
    # ``catalog_columns`` too) so the rest of the table is ingested; see
    # _leave_out_unsupported_columns. Re-derived from the catalog on every resolve, so
    # neither fingerprinted nor kept in schema history: a table without such columns keeps
    # its fingerprint, and one added at the source later is left out without a transition.
    unsupported_columns: tuple[Column, ...] = ()
    row_filter: str | None = None
    # rename_columns as sorted (source, destination) pairs, or None. Internally every
    # column keeps its source name; only what the connector reports -- the schema, the
    # rows, the key -- uses the destination names. Fingerprinted when set, like the
    # selection, since rows already landed carry the old names.
    column_renames: tuple[tuple[str, str], ...] | None = None
    # transform_columns as sorted (column, canonical expression) pairs, or None, and the
    # id of the HASH key they use; fingerprinted like the renames.
    column_transforms: tuple[tuple[str, str], ...] | None = None
    transform_key_id: str | None = None
    # The Spark types decimal.variable.type and decimal.variable.column.type give its
    # variable-scale DECIMAL columns, as sorted (column, type) pairs for those that differ
    # from the default: () when every one is the default, None for a schema recorded
    # before the types were. Fingerprinted when not empty, like the transforms, since rows
    # already landed and staged pages hold values of the old type.
    decimal_targets: tuple[tuple[str, str], ...] | None = None
    # decimal.variable.type itself when it is not the default, for a column the table gains
    # later (_carry_table_options).
    decimal_default: str | None = None

    @property
    def exposed_name(self) -> str:
        return _join_identity(self.owner, self.name)

    @property
    def identity(self) -> str:
        return _join_identity(self.database, self.owner, self.name)

    @property
    def native_identity(self) -> str:
        return f"{self.database}:{_sql_identifier(self.owner)}.{_sql_identifier(self.name)}"

    @classmethod
    # pylint: disable-next=too-many-locals
    def parse(cls, raw: dict[str, Any], default_database: str) -> "Table":
        database = str(raw.get("database") or default_database)
        owner = str(raw["owner"])
        name = str(raw["name"])
        for part in (database, owner, name):
            if not _IDENTIFIER.fullmatch(part):
                # 126: Unsafe Informix identifier returned by metadata: {!r}
                raise InformixError(_m(126, part))
        columns = tuple(Column.parse(c) for c in raw.get("columns", ()))
        pks = tuple(str(v) for v in raw.get("primary_keys", ()))
        column_names = tuple(column.name for column in columns)
        for column_name in column_names:
            if not _IDENTIFIER.fullmatch(column_name):
                # 127: Unsafe Informix column identifier returned by metadata: {!r}
                raise InformixError(_m(127, column_name))
        normalized_names = tuple(column_name.casefold() for column_name in column_names)
        if len(normalized_names) != len(set(normalized_names)):
            # 128: Duplicate column names for {}.{}.{}
            raise InformixError(_m(128, database, owner, name))
        reserved = {column.casefold() for column in _INTERNAL_COLUMNS}
        collisions = sorted(
            column_name for column_name in column_names if column_name.casefold() in reserved
        )
        if collisions:
            # 129: Source columns collide with reserved Informix metadata columns: {!r}
            raise InformixError(_m(129, collisions))
        if len(pks) != len(set(pks)):
            # 130: Duplicate primary-key columns for {}.{}.{}
            raise InformixError(_m(130, database, owner, name))
        known = {c.name for c in columns}
        if not columns or any(pk not in known for pk in pks):
            raise InformixError(f"Invalid metadata for {database}.{owner}.{name}")
        incarnation_value = raw.get("incarnation")
        incarnation = None if incarnation_value is None else str(incarnation_value)
        key_descending = tuple(bool(v) for v in raw.get("key_descending", ()))
        if key_descending and len(key_descending) != len(pks):
            # 131: Key direction metadata for {}.{}.{} does not match its {} primary-key column(s)
            raise InformixError(_m(131, database, owner, name, len(pks)))
        nullable_index_raw = list(raw.get("nullable_index_key", ()))
        nullable_index_key = tuple(str(pair[0]) for pair in nullable_index_raw)
        nullable_index_descending = tuple(bool(pair[1]) for pair in nullable_index_raw)
        for column_name in nullable_index_key:
            if column_name not in known:
                # 132: Nullable-index candidate for {}.{}.{} names unknown column {!r}
                raise InformixError(_m(132, database, owner, name, column_name))
        # Only schema-history state carries these two; a catalog description never does.
        selection_raw = raw.get("column_selection")
        selection = None
        if selection_raw is not None:
            mode, names = selection_raw
            if mode not in {"include", "exclude"}:
                # 133: Invalid column selection metadata for {}.{}.{}
                raise InformixError(_m(133, database, owner, name))
            selection = (str(mode), tuple(str(value) for value in names))
        row_filter = raw.get("row_filter")
        transforms_raw = raw.get("column_transforms")
        transforms = (
            None
            if transforms_raw is None
            else tuple((str(name), str(expression)) for name, expression in transforms_raw)
        )
        renames_raw = raw.get("column_renames")
        renames = (
            None
            if renames_raw is None
            else tuple((str(source), str(target)) for source, target in renames_raw)
        )
        return cls(
            database,
            owner,
            name,
            columns,
            pks,
            incarnation,
            key_descending=key_descending,
            nullable_index_key=nullable_index_key,
            nullable_index_descending=nullable_index_descending,
            column_selection=selection,
            row_filter=None if row_filter is None else str(row_filter),
            column_renames=renames,
            column_transforms=transforms,
            transform_key_id=(
                None if raw.get("transform_key_id") is None else str(raw["transform_key_id"])
            ),
            decimal_targets=(
                None
                if raw.get("decimal_targets") is None
                else tuple((str(name), str(target)) for name, target in raw["decimal_targets"])
            ),
            decimal_default=(
                None if raw.get("decimal_default") is None else str(raw["decimal_default"])
            ),
            key_wildcard_except=(
                None
                if raw.get("key_wildcard_except") is None
                else tuple(str(name) for name in raw["key_wildcard_except"])
            ),
        )


class _StateValidationCoordinator:
    # Docstring: Process-local exact leases whose synchronization state is never serialized.

    def __init__(self) -> None:
        self.condition = threading.Condition()
        self.table_caches: dict[tuple[str, ...], "Table"] = {}
        # (table identity, left-out column names) pairs already warned about, so a table
        # resolved on every read warns once per process rather than on each microbatch.
        self.unsupported_column_warnings: set[tuple[str, tuple[str, ...]]] = set()

    def __getstate__(self) -> dict[str, object]:
        # Spark workers must establish their own process-local validation state.
        return {}

    def __setstate__(self, state: dict[str, object]) -> None:
        del state
        self.__init__()


_STATE_VALIDATION_COORDINATOR = _StateValidationCoordinator()


def _sleep_with_backoff(deadline: float, delay: float) -> float:
    # Docstring: Sleep with jitter without crossing the caller's absolute deadline.

    remaining = deadline - time.monotonic()
    if remaining > 0:
        time.sleep(min(delay * random.uniform(0.8, 1.2), remaining))
    return min(delay * 1.5, 2.0)


# The framework stream readers the Informix wrapper below instruments: the simple
# reader (readBetweenOffsets replay) and the partitioned proxy, which is the default
# for CDC-capable tables and replays through get_partitions(start, end) instead.
_WRAPPED_STREAM_READER_NAMES = frozenset(
    {"LakeflowStreamReader", "LakeflowPartitionedStreamReader"}
)


def _snapshot_position(offset: Any) -> tuple | None:
    # Docstring: Where a snapshot-phase offset points: (snapshot LSN, page, record); else None.

    if not isinstance(offset, dict) or offset.get("phase") != "snapshot":
        return None
    position = offset.get("snapshot") or {}
    return (
        str(offset.get("snapshot_lsn")),
        int(position.get("page_index", 0)),
        int(position.get("record_index", 0)),
    )


def _replay_keeps_floor(options: Any) -> bool:
    # Docstring: Whether this read replays a batch that established a boundary: an initial
    # range, a first batch Spark logged, or auto_snapshot's resnapshot of an expired
    # checkpoint.
    #
    # Such a replay may establish a newer boundary than the batch had (an incremental
    # copy's, a cdc_only start, a snapshot taken again), but Spark goes on from the end it
    # logged, so the stream reads on from below that boundary. Published as the delete
    # floor, the newer boundary started the delete flow above it, and the deletes between
    # were never applied. The floor the logged batch published stays: it is that batch's
    # boundary, at or below where the stream goes on.

    return bool(options) and (
        options.get(_REPLAY_INITIAL_END_OPTION) is not None
        or options.get(_REPLAY_RESNAPSHOT_OPTION) is not None
    )


def _open_state_connection(connection: Any) -> Any:
    # Docstring: ``connection`` while it is open, rolled back first when a failed statement
    # left its transaction aborted, else None.
    #
    # The state statements commit only when they succeed, so one that fails (a lock or
    # statement timeout, a permission error) leaves the transaction aborted, and every
    # statement after it on the connection failed with InFailedSqlTransaction until a
    # rollback. The best-effort callers swallow the first error, so the next mandatory
    # operation failed instead. 3 is libpq's PQTRANS_INERROR, which psycopg2 and psycopg
    # both report as the transaction status.

    if connection is None:
        return None
    try:
        if connection.closed:
            return None
        if getattr(getattr(connection, "info", None), "transaction_status", None) == 3:
            connection.rollback()
    except Exception:  # pylint: disable=broad-except
        # Gone, or a rollback that failed: a new connection serves instead.
        with contextlib.suppress(Exception):
            connection.close()
        return None
    return connection


def _range_emits_nothing(start: Any, end: Any) -> bool:
    # Docstring: Whether ``[start, end)`` is a range that emitted no rows by what it is, so
    # that replayed or planned again it emits none either.
    #
    # Spark may persist bootstrap coordination progress as a range of its own: an end of
    # advisory fields alone, from a start with no Informix position, advances none and holds
    # no records. A table.migration cutover seed (``handoff_seeded`` on the end and not the
    # start) adopted a parked position and emitted no rows: replayed as a read from it, it
    # was taken for the committed seed and dropped the token, then fell short of the
    # position; after Restore it served the pages or extract the position names.

    # So is a floor-seeded delete position's wait, the floor replaced: it starts again with
    # no checkpoint (_confirmed_floor_seed), and its end had no position to replay to (1).
    positional = {"commit_lsn", "change_lsn", "begin_lsn", "tx_id", "incremental"}
    if (
        isinstance(end, dict)
        and end
        and set(end) <= _OFFSET_ADVISORY_FIELDS
        and (not positional.intersection(start or {}) or bool((start or {}).get("floor_seed")))
    ):
        return True
    # A delete position's seed at a floor or a full refresh's own start, from none, holds no
    # rows; so does one that started again from a seeded position, ending idle at or below
    # it, or on another seed (a batch that reads on keeps its mark or drops it). Replayed
    # before the upsert reader had published, each read again: from no position it waited
    # until its deadline, or read on from a lower floor and emitted deletes its batch never
    # had, or fell short of its end (5).
    if (
        isinstance(end, dict)
        and end.get("floor_seed")
        and (
            not positional.intersection(start or {})
            or end["floor_seed"] != (start or {}).get("floor_seed", end["floor_seed"])
        )
    ):
        return True
    if (
        isinstance(start, dict)
        and start.get("floor_seed")
        and isinstance(end, dict)
        and not end.get("floor_seed")
    ):
        if (
            end.get("tx_id") is None
            and end.get("commit_lsn") is not None
            and start.get("commit_lsn") is not None
            and int(end["commit_lsn"]) <= int(start["commit_lsn"])
        ):
            return True
    return bool((end or {}).get("handoff_seeded")) and not (start or {}).get("handoff_seeded")


def _resnapshot_range(start: Any, end: Any) -> bool:
    # Docstring: Whether ``start`` is a stream position below ``end``'s LSN, as auto_snapshot's
    # resnapshot of an expired checkpoint starts: where ``end`` is a staged snapshot's, that
    # batch is the only one to reach a snapshot's boundary from below it.
    #
    # Measured from where the stream restarts, as _checkpoint_requires_snapshot measures:
    # the begin of a transaction open at the checkpoint, whose commit can lie at or past
    # the new boundary.

    try:
        restart = int(start.get("begin_lsn") or start["commit_lsn"])
        return start.get("phase") == "stream" and restart < int(end["commit_lsn"])
    except (AttributeError, KeyError, TypeError, ValueError):
        return False


class SchemaTransitionUnavailable(InformixError):
    # Docstring: The owning upsert reader has not published this schema transition yet.
    pass


# pylint: disable-next=too-many-branches,too-many-locals,too-many-statements
def _replay_between_offsets(start, end, read, options, connector_options, staged_start=None):
    # Docstring: Replay ``[start, end)`` by reading to ``end`` and stopping there.
    #
    # Shared by the simple reader's ``readBetweenOffsets`` and the partitioned reader's
    # ``get_partitions`` recompute, so both replay paths honour one contract: the read
    # is bounded at ``end.commit_lsn`` (and, mid-incremental-copy, at
    # ``end.incremental.last_pk``), and an under-read fails closed rather than letting
    # Spark commit an ``end`` the rows do not reach.
    #
    # The unbounded alternative (``read(start)`` with the reached offset discarded) is
    # not reproducible: it ends at whichever transaction boundary the row budget falls
    # on, so the same start LSN over the same log can end in different places --
    # measured directly, budget 10 ended at LSN 107 and budget 2 at 104 on identical
    # data. Spark commits its own ``end`` regardless, so a fresh read that stops earlier
    # than the original silently drops everything in between; that is how six tables
    # lost 151 rows.
    #
    # ``read(start)`` performs one read and returns ``(records, reached_offset)``; the
    # bounds are injected into ``options`` (the dict that read consults, restored on
    # exit) because read_table has no ``end`` parameter and thirty connectors
    # implement it. ``connector_options`` supplies the connection-wait default.
    # ``staged_start(end)``, the simple reader's, gives the start that serves an initial
    # range from the snapshot its first batch staged, or None (_staged_first_batch_start).

    positional_keys = {
        "commit_lsn",
        "change_lsn",
        "begin_lsn",
        "tx_id",
        "incremental",
    }
    # Spark may persist bootstrap coordination progress as a range of its own.
    # Such a range advances no Informix position and contains no records to
    # replay. Treat it as empty, while continuing to fail closed if either side
    # identifies a positional CDC range.
    if _range_emits_nothing(start, end):
        return iter(())

    stop = (end or {}).get("commit_lsn")
    if stop is None:
        # 1: Informix cannot replay an offset range whose end has no commit_lsn
        raise InformixError(_m(1))
    end_position = _snapshot_position(end)
    if end_position is not None and _snapshot_position(start) == end_position:
        # A snapshot range that served nothing (a retry yield at one page): reading would
        # serve that page, which the batch did not hold.
        return iter(())
    # The first incremental range starts at {} and still emits a chunk. Bound it
    # whenever either side identifies an in-progress copy; a start-side block with
    # no end-side block is the final range and is represented by JSON null below. A
    # copy that finished in its first batch leaves neither side a block, so an initial
    # range ending in the stream phase drains to max_pk too (only a copy reads it).
    initial_range = not positional_keys.intersection(start or {})
    replaying_chunk = (
        isinstance((start or {}).get("incremental"), dict)
        or isinstance((end or {}).get("incremental"), dict)
        or (initial_range and (end or {}).get("phase") == "stream")
    )
    # ``null`` is meaningful: the copy finished inside this range, so the replay
    # must drain to max_pk instead of stopping at a cursor.
    end_copy = (end or {}).get("incremental")
    if isinstance(end_copy, dict) and end_copy.get("last_pk") is None:
        # In the copy, with no chunk copied yet: null would drain it to max_pk.
        stop_pk = json.dumps({"before_first_chunk": True})
    else:
        stop_pk = json.dumps((end_copy or {}).get("last_pk"))
    previous = options.get(_REPLAY_STOP_LSN_OPTION)
    previous_pk = options.get(_REPLAY_STOP_PK_OPTION)
    previous_wait_budget = options.get(_REPLAY_CONNECTION_WAIT_BUDGET_OPTION)
    previous_end = options.get(_REPLAY_INITIAL_END_OPTION)
    previous_seed = options.get(_REPLAY_END_SEED_OPTION)
    options[_REPLAY_STOP_LSN_OPTION] = str(stop)
    options[_REPLAY_END_SEED_OPTION] = json.dumps(
        [(end or {}).get("floor_seed"), (end or {}).get("tx_id") is not None]
    )
    if initial_range:
        options[_REPLAY_INITIAL_END_OPTION] = json.dumps(end, sort_keys=True)
    if replaying_chunk:
        options[_REPLAY_STOP_PK_OPTION] = stop_pk
    try:
        replay_start = start
        reestablished = False
        if end_position is not None and _snapshot_position(start) is None:
            # A staged snapshot's first batch (an initial one, or auto_snapshot's replacing an
            # expired checkpoint): serve it from the snapshot Spark logged, from page 0,
            # rather than drain another one (a later update would), so the batch and the
            # pages after it come from one snapshot.
            position = {k: v for k, v in end["snapshot"].items() if k != "record_index"}
            replay_start = {**end, "snapshot": {**position, "page_index": 0, "last_pk": []}}
        elif (
            staged_start is not None
            and end.get("phase") == "stream"
            and (initial_range or _resnapshot_range(start, end))
        ):
            # One whose snapshot staged a page or none ended in the stream phase: the
            # connector recognises it by the snapshot's manifest. So is auto_snapshot's
            # resnapshot of an expired checkpoint, which starts from that checkpoint.
            replay_start = staged_start(end) or replay_start
        record_parts = []
        configured_wait = float(
            options.get(
                "connection.wait.timeout.seconds",
                connector_options.get(
                    "connection.wait.timeout.seconds",
                    str(_DEFAULT_CONNECTION_WAIT_TIMEOUT_SECONDS),
                ),
            )
        )
        replay_wait_deadline = time.monotonic() + configured_wait
        step_wait = 0.1
        # Spark cannot checkpoint an advisory yield while replaying: it has already
        # fixed this range's end. Advance bootstrap coordination counters inside
        # this call until the reader obtains a real source position. This is
        # especially important for the delete flow's deferred schema node fallback
        # after a restart.
        while True:
            remaining_wait = replay_wait_deadline - time.monotonic()
            if remaining_wait <= 0:
                # 2: Informix replay did not produce a positional end LSN before its connection wait
                # deadline
                raise InformixError(_m(2))
            options[_REPLAY_CONNECTION_WAIT_BUDGET_OPTION] = repr(remaining_wait)
            try:
                records, reached = read(replay_start)
                if isinstance(reached, dict):
                    # Marked on the read that re-established the range; later polls of it
                    # read on from there.
                    reestablished = bool(reached.pop(_REPLAY_REESTABLISHED, False)) or reestablished
            except SchemaTransitionUnavailable:
                # The range needs a schema step the upsert reader records on its own read
                # (a column changed while the pipeline was stopped). A replay cannot yield
                # for it, so it waits in place, the read having given its slot back.
                if replay_wait_deadline - time.monotonic() <= 0:
                    raise
                step_wait = _sleep_with_backoff(replay_wait_deadline, step_wait)
                continue
            record_parts.append(records)
            if isinstance(reached, dict) and reached.get("commit_lsn") is not None:
                position = _snapshot_position(reached)
                if (
                    position is not None
                    and position != _snapshot_position(replay_start)
                    and (
                        end_position is None
                        or (position[0] == end_position[0] and position[1:] < end_position[1:])
                    )
                ):
                    # The simple reader serves a staged snapshot a page per read, and a batch
                    # the partitioned reader logged can span several: read on to its end.
                    replay_start = reached
                    continue
                if (
                    isinstance(replay_start, dict)
                    and (
                        # An initial range starts from no position (a copy's first batch).
                        replay_start.get("commit_lsn") is None
                        or int(replay_start["commit_lsn"]) < int(reached["commit_lsn"])
                    )
                    and int(reached["commit_lsn"]) < int(stop)
                ):
                    # One poll can go idle short of the committed end; the original
                    # read then advanced across the quiet log in several batches.
                    # Keep reading from where this one stopped, as it did. A chunked
                    # range too, whose CDC a direct poll may cap short of what the shared
                    # reader delivered: its chunk reached the committed cursor in the
                    # first poll, so the next ones read none.
                    replay_start = reached
                    continue
                break
            advisory_yield = (
                # Or a floor-seeded delete position whose floor was replaced: it starts
                # again as a reader with no checkpoint (_confirmed_floor_seed).
                (initial_range or bool((start or {}).get("floor_seed")))
                and isinstance(reached, dict)
                and bool(reached)
                and set(reached) <= _OFFSET_ADVISORY_FIELDS
            )
            if not advisory_yield:
                # 3: Informix replay did not produce a positional end LSN
                raise InformixError(_m(3))
            if reached == replay_start:
                # At the maximum schema fallback count, an unchanged advisory
                # offset means the owning upsert flow has not published its
                # current channel start yet. The read has released its connection
                # slot, so wait briefly and try again without extending the shared
                # deadline.
                remaining_wait = replay_wait_deadline - time.monotonic()
                if remaining_wait <= 0:
                    # 2: Informix replay did not produce a positional end LSN before its connection
                    # wait deadline
                    raise InformixError(_m(2))
                time.sleep(min(random.uniform(0.05, 0.2), remaining_wait))
            replay_start = reached
        # A replay with one read that returned rows hands back those rows as they came, so a
        # spilled transaction (_SpilledRows) still reaches get_partitions as pages after reads
        # that went idle. An idle read returns an empty list iterator, which reports its
        # size; a part that reports none (a generator) may hold rows, so it is kept.
        filled = [part for part in record_parts if operator.length_hint(part, -1) != 0]
        records = (
            filled[0] if len(filled) == 1 else itertools.chain.from_iterable(filled or record_parts)
        )
        try:
            reached_lsn = int(reached["commit_lsn"])
            stop_lsn = int(stop)
        except (KeyError, TypeError, ValueError) as error:
            # 4: Informix replay returned an invalid end LSN
            raise InformixError(_m(4)) from error
        # The first full-refresh range normally starts at {}, but the framework
        # may add retry bookkeeping before replaying it (for example
        # ``schema_node_fallback_retry_count``). Such metadata does not turn the
        # range into a checkpointed CDC range. Re-executing an initial range
        # legitimately establishes a newer snapshot/CDC boundary than the one
        # latestOffset planned before cancellation; its snapshot rows represent
        # that newer state and replaying them is safe. A checkpointed range,
        # however, must land on its exact transaction boundary. In both cases an
        # under-read is always destructive.
        # A range the read re-established is held to the same terms as an initial one.
        initial_terms = initial_range or reestablished
        wrong_lsn = reached_lsn < stop_lsn or (not initial_terms and reached_lsn != stop_lsn)
        if wrong_lsn:
            # 5: Informix replay did not reach its committed end LSN {}; reached {}
            raise InformixError(_m(5, stop_lsn, reached_lsn))
        # Every page of a snapshot shares its LSN, so the page is checked on its own, and a
        # replay still in a snapshot the logged end has left falls short of it too.
        if _snapshot_position(reached) != end_position:
            # 428: Informix replay did not reach its committed snapshot position {}; reached {}
            raise InformixError(_m(428, end_position, _snapshot_position(reached)))
        # An end that names a transaction is reached only by consuming that transaction.
        # The read lands on its bound without one only for an end the original read
        # reached by going idle, so landing there for this end means the replay never
        # saw the transaction.
        expected_tx = (end or {}).get("tx_id")
        if (
            not initial_terms
            and not replaying_chunk
            and expected_tx is not None
            and str(reached.get("tx_id")) != str(expected_tx)
        ):
            # 6: Informix replay did not reach its committed end transaction {} at LSN {}; reached
            # {}
            raise InformixError(_m(6, expected_tx, stop_lsn, reached.get("tx_id")))
        expected_incremental = (end or {}).get("incremental")
        reached_incremental = reached.get("incremental")
        if isinstance(expected_incremental, dict):
            expected_pk = expected_incremental.get("last_pk")
            # A copy the replay seeded again -- an initial range's, or a re-established one --
            # is another snapshot's, read in full up to the logged cursor: its own last key
            # need not be that cursor, its max_pk having been sampled again.
            if not initial_terms and (
                not isinstance(reached_incremental, dict)
                or reached_incremental.get("last_pk") != expected_pk
            ):
                # 7: Informix replay did not reach its committed incremental snapshot cursor {!r}
                raise InformixError(_m(7, expected_pk))
        elif (replaying_chunk or reestablished) and isinstance(reached_incremental, dict):
            # 8: Informix replay did not finish the incremental snapshot range that Spark has
            # committed
            raise InformixError(_m(8))
        return records
    finally:
        # Scoped strictly to this replay: a normal read that inherited the bound
        # would stop short forever.
        if previous is None:
            options.pop(_REPLAY_STOP_LSN_OPTION, None)
        else:
            options[_REPLAY_STOP_LSN_OPTION] = previous
        if previous_pk is None:
            options.pop(_REPLAY_STOP_PK_OPTION, None)
        else:
            options[_REPLAY_STOP_PK_OPTION] = previous_pk
        if previous_wait_budget is None:
            options.pop(_REPLAY_CONNECTION_WAIT_BUDGET_OPTION, None)
        else:
            options[_REPLAY_CONNECTION_WAIT_BUDGET_OPTION] = previous_wait_budget
        if previous_end is None:
            options.pop(_REPLAY_INITIAL_END_OPTION, None)
        else:
            options[_REPLAY_INITIAL_END_OPTION] = previous_end
        if previous_seed is None:
            options.pop(_REPLAY_END_SEED_OPTION, None)
        else:
            options[_REPLAY_END_SEED_OPTION] = previous_seed


def _replay_reader_range(reader, start, end):
    # Docstring: The simple stream reader's readBetweenOffsets, for the merged file's
    # readers and the wheel's alike: ``[start, end)`` replayed bounded at ``end``, and a
    # staged snapshot's first batch served from its stage (_staged_first_batch_start,
    # which tells a delete flow's connector by its own options).

    connector = reader.lakeflow_connect
    staged = getattr(connector, "_staged_first_batch_start", None)

    def staged_start(end_offset):
        return staged(reader.options.get("tableName"), end_offset, reader.options)

    return _replay_between_offsets(
        start,
        end,
        reader.read,
        reader.options,
        getattr(connector, "options", {}),
        staged_start if callable(staged) else None,
    )


def _pipeline_update(spark_session: Any | None) -> tuple[str | None, str | None]:
    # Docstring: The pipeline and update the Spark session runs, or (None, None) outside a
    # pipeline. A serverless (Spark Connect) session outside one raises CONFIG_NOT_AVAILABLE
    # for these settings, default or not. Any other failure is raised: a pipeline given a
    # random scope for a passing one would not find its update's state.

    if spark_session is None:
        return None, None
    try:
        conf = spark_session.conf
        return (
            conf.get("spark.pipelines.pipelineId", None),
            conf.get("spark.pipelines.updateId", None),
        )
    except (AttributeError, TypeError):
        return None, None
    except Exception as exc:  # pylint: disable=broad-except
        condition = getattr(exc, "getCondition", getattr(exc, "getErrorClass", lambda: ""))
        if "CONFIG_NOT_AVAILABLE" in f"{condition()} {exc}":
            return None, None
        raise


def _registration_identity(spark_session: Any | None) -> tuple[str, str | None, float | None]:
    # Docstring: The scope of this registration, from the pipeline and update the Spark
    # session runs under (a random one outside a pipeline), what the update was started for
    # (see _update_request), and when the pipeline's targets were last reset (_last_reset).

    pipeline_id, update_id = _pipeline_update(spark_session)
    if not pipeline_id or not update_id:
        return secrets.token_hex(16), None, None
    registration_scope = f"{pipeline_id}_@_{update_id}".lower()
    if _PIPELINE_SCOPE.fullmatch(registration_scope) is None:
        # 9: Spark pipeline and update IDs must be canonical UUID strings
        raise ValueError(_m(9))
    return registration_scope, _update_request(pipeline_id, update_id), _last_reset(pipeline_id)


def _driver_registration(spark_session: Any | None = None) -> dict[str, Any]:
    # Docstring: What the driver knows and no other process can find out, for a connector
    # registered by its class from an installed package: the registration's scope, what
    # the update was started for, and the workspace credentials.
    #
    # The merged single file carries these in its own globals, which Spark pickles to
    # every process with the code. A class registered from a package is pickled by name,
    # and each process imports the module afresh, so they travel with the class instead
    # (see InformixDataSource) and with each connector instance (see __getstate__). A
    # process that was handed them hands the same on.

    # The update this session runs, when it says: a driver kept warm between updates, or
    # one that registers again, must gather anew for the new update. With no session to
    # ask, a process that was handed these hands the same on.
    pipeline_id, update_id = _pipeline_update(spark_session)
    running = f"{pipeline_id}_@_{update_id}".lower() if pipeline_id and update_id else None
    held = _INFORMIX_REGISTRATION_CONTEXT["scope"]
    now = time.monotonic()
    if held is None or (running is not None and running != held):
        capture_workspace_credentials()
        _CREDENTIAL_CAPTURED.update(at=now, pid=os.getpid())
        scope, request, reset_at = _registration_identity(spark_session)
        _INFORMIX_REGISTRATION_CONTEXT["request"] = request
        _INFORMIX_REGISTRATION_CONTEXT["reset_at"] = reset_at
        _INFORMIX_REGISTRATION_CONTEXT["scope"] = scope
    elif (
        _CREDENTIAL_CAPTURED.get("pid") == os.getpid()
        and now - _CREDENTIAL_CAPTURED["at"] >= _CREDENTIAL_RECAPTURE_SECONDS
    ):
        # The same update, or a session outside a pipeline, packing a registration again
        # (a flow restarted, say, or a connector pickled for a worker, which asks with no
        # session): the token captured when it began lasts about an hour, and a process
        # first reaching the control plane after that failed with it. Captured again here,
        # in the process that captured it (the merged file's globals carry the capture
        # time to every process with its code, but not the process); a process that was
        # handed the credentials hands the same on. A failed capture keeps the one held.
        capture_workspace_credentials()
        _CREDENTIAL_CAPTURED["at"] = now
    return {
        "scope": _INFORMIX_REGISTRATION_CONTEXT["scope"],
        "request": _INFORMIX_REGISTRATION_CONTEXT["request"],
        "reset_at": _INFORMIX_REGISTRATION_CONTEXT["reset_at"],
        "workspace": dict(_CAPTURED_WORKSPACE),
    }


def _install_registration(state: dict[str, Any]) -> None:
    # Docstring: Take up, in this process, what _driver_registration gathered on the driver.
    #
    # Whatever the process held before: Spark keeps its Python workers for the next
    # update, whose scope, request and credentials are other ones, and a worker that kept
    # the earlier update's would read under its scope and with a token since expired.

    if state.get("scope"):
        _INFORMIX_REGISTRATION_CONTEXT["scope"] = state["scope"]
        _INFORMIX_REGISTRATION_CONTEXT["request"] = state.get("request")
        _INFORMIX_REGISTRATION_CONTEXT["reset_at"] = state.get("reset_at")
        # Whole: merged in, an earlier update's token stayed where this one carried none.
        _CAPTURED_WORKSPACE.clear()
    if state.get("workspace"):
        _CAPTURED_WORKSPACE.update(state["workspace"])


def _informix_available_now_base(base: type, spark_session: Any | None = None) -> type:
    # Docstring: Wrap the generated reader base without changing the shared adapter source.

    original_base = getattr(base, "_informix_original_base", base)
    registration_scope, update_request, reset_at = _registration_identity(spark_session)
    _INFORMIX_REGISTRATION_CONTEXT["request"] = update_request
    _INFORMIX_REGISTRATION_CONTEXT["reset_at"] = reset_at
    # Schema discovery constructs connector instances before stream readers
    # exist. Publish this registration's immutable scope early so those
    # instances can share the registration-scoped catalog snapshot.
    _INFORMIX_REGISTRATION_CONTEXT["scope"] = registration_scope

    class InformixAvailableNowBase(original_base):
        _informix_available_now_wrapper = True
        _informix_original_base = original_base

        def __init_subclass__(cls, **kwargs):
            super().__init_subclass__(**kwargs)
            # Both framework stream readers are instrumented. Matching only the
            # simple reader left the partitioned proxy -- the default for
            # CDC-capable tables -- without the registration-scope setter and the
            # AvailableNow preparation hook.
            if cls.__name__ not in _WRAPPED_STREAM_READER_NAMES:
                return

            original_init = cls.__init__

            def initialize(reader, *args, **kwargs) -> None:
                original_init(reader, *args, **kwargs)
                setter = getattr(reader.lakeflow_connect, "set_registration_scope", None)
                if setter is not None:
                    setter(registration_scope)
                setter = getattr(reader.lakeflow_connect, "set_update_request", None)
                if setter is not None:
                    setter(update_request, reset_at)
                options = getattr(reader, "options", {})
                table = options.get("tableName", "<unknown>")
                is_delete = getattr(reader.lakeflow_connect, "_is_delete_flow", None)
                role = (
                    "delete"
                    if (
                        is_delete()
                        if callable(is_delete)
                        else options.get("isDeleteFlow") == "true"
                    )
                    else "upsert"
                )
                # 10: Informix CDC reader initialized: scope={} table={} role={}
                logging.getLogger(__name__).info(
                    _m(10, registration_scope, table, role),
                )

            def prepare_for_trigger(reader) -> None:
                prepare = getattr(
                    reader.lakeflow_connect, "prepare_for_trigger_available_now", None
                )
                if prepare is not None:
                    prepare()

            def read_between_offsets(reader, start, end):
                # Docstring: Replay ``[start, end)`` bounded at ``end`` (see
                # _replay_between_offsets).
                #
                # The shared implementation is ``return self.read(start)[0]``: an
                # unbounded read whose returned offset is discarded. Spark commits its
                # own ``end`` regardless, so if the fresh read stops earlier than the
                # original did, everything in between is dropped silently. The bound
                # travels through the reader's options, which the framework copies into
                # table_options on every read.

                return _replay_reader_range(reader, start, end)

            cls.prepareForTriggerAvailableNow = prepare_for_trigger
            if cls.__name__ == "LakeflowStreamReader":
                # Only the simple reader replays through readBetweenOffsets; the
                # partitioned proxy replays through get_partitions(start, end), which
                # bounds its recompute with the same _replay_between_offsets contract.
                cls.readBetweenOffsets = read_between_offsets
            cls.__init__ = initialize

    return InformixAvailableNowBase


def _lines_only(code: types.CodeType) -> types.CodeType:
    # Docstring: ``code`` with a location table that keeps each instruction's line and
    # drops its columns, as ``python -X no_debug_ranges`` compiles it.
    #
    # Python 3.11 and later record a column range for nearly every instruction, about one
    # byte of table per two of bytecode, and a pickled function carries it. A traceback
    # uses the columns only to underline part of a source line. The table is rebuilt from
    # ``co_lines()`` as runs of up to eight instructions, each a "no column" entry (code
    # 13, or 15 for an instruction without a line) with its line's distance from the one
    # before. The result is kept only if it reads back to the same lines. The format is
    # CPython's own and may change in a later release: a table the replace refuses, or one
    # that fails to read back, leaves the code as it was.
    if not hasattr(code, "co_positions"):  # Python 3.10: its table has lines only
        return code
    try:
        table = bytearray()
        previous = code.co_firstlineno
        for start, end, line in code.co_lines():
            units = (end - start) // 2
            while units > 0:
                run = min(units, 8)
                units -= run
                if line is None:
                    table.append(0xF8 | (run - 1))
                    continue
                table.append(0xE8 | (run - 1))
                delta, previous = line - previous, line
                value = (-delta << 1) | 1 if delta < 0 else delta << 1
                while value >= 64:
                    table.append(0x40 | (value & 63))
                    value >>= 6
                table.append(value)
        plain = code.replace(co_linetable=bytes(table))
        return plain if list(plain.co_lines()) == list(code.co_lines()) else code
    except (ValueError, TypeError, SystemError):
        return code


def _shorten_merged_definitions(namespace: dict[str, Any]) -> None:
    # Docstring: In the merged single file, strip ``register_lakeflow_source.<locals>.``
    # from every class's qualname, give each function and code object its name as its
    # qualname, drop function annotations and docstrings, and keep lines but not columns
    # in each code object's location table (see _lines_only).
    #
    # The merge nests the whole connector inside register_lakeflow_source, so Spark
    # pickles every function and class by value. Each function carries that prefix twice
    # (on the function and on its code object, nested code included) and an annotations
    # dict nothing reads at run time: about 67 KB of the 1 MiB the planner accepts. A
    # function's docstring goes along as its __doc__ beside the copy in its code, and a
    # class's (a dataclass's is its whole signature) in its dict: some 11 KB more.
    # Tracebacks still show each function's name, file and line. Imported as a module,
    # nothing carries the prefix, so this changes nothing.
    prefix = "register_lakeflow_source.<locals>."
    seen: set[int] = set()
    codes: dict[int, types.CodeType] = {}

    def shorten(code: types.CodeType) -> types.CodeType:
        if id(code) not in codes:
            consts = tuple(
                shorten(c) if isinstance(c, types.CodeType) else c for c in code.co_consts
            )
            if not hasattr(code, "co_qualname"):  # Python 3.10: only the function carries one
                codes[id(code)] = code.replace(co_consts=consts)
            else:
                # Its qualname as its name: nothing a reader sees shows it (a traceback names
                # co_name), and as one interned string the pickle writes it once for both,
                # where the class and enclosing names of every method, lambda and
                # comprehension came to some 32 KB.
                codes[id(code)] = _lines_only(
                    code.replace(co_consts=consts, co_qualname=sys.intern(code.co_name))
                )
        return codes[id(code)]

    def visit(value: Any) -> None:
        if id(value) in seen:
            return
        seen.add(id(value))
        if isinstance(value, (staticmethod, classmethod)):
            visit(value.__func__)
        elif isinstance(value, property):
            for accessor in (value.fget, value.fset, value.fdel):
                visit(accessor)
        elif isinstance(value, types.FunctionType):
            if prefix in value.__qualname__ or prefix in getattr(value.__code__, "co_qualname", ""):
                code = shorten(value.__code__)
                # The docstring's other copy, the code's first constant, unless the body
                # loads that constant itself. Replaced after shorten, which keys its cache by
                # the id of a code object the merged source keeps: a copy made before it was
                # freed at once, and its id, reused, handed another function's code back.
                if (
                    value.__doc__ is not None
                    and code.co_consts
                    and code.co_consts[0] == value.__doc__
                    and not any(
                        ins.opcode in dis.hasconst and ins.arg == 0
                        for ins in dis.get_instructions(code)
                    )
                ):
                    code = code.replace(co_consts=(None, *code.co_consts[1:]))
                value.__code__ = code
                # Its name alone: cloudpickle writes a function's own copy of it, beside its
                # code's (which tracebacks and the code keep whole), and the class prefix of
                # every method came to some 13 KB. Pickling by reference looks a function up
                # by it and checks that the object found is this one, so it can't mistake it.
                value.__qualname__ = value.__name__
                value.__annotations__ = {}
                value.__doc__ = None
            # Shortened or not: a hook made from code already shortened (a reader's
            # wrapped __init__) closes over a definition that was not.
            for cell in value.__closure__ or ():
                with contextlib.suppress(ValueError):  # an unfilled cell
                    visit(cell.cell_contents)
        elif isinstance(value, type):
            if value.__qualname__.startswith(prefix):
                value.__qualname__ = value.__qualname__.removeprefix(prefix)
                with contextlib.suppress(AttributeError, TypeError):
                    value.__doc__ = None
                # Its bases too: the connector's methods are spread over mixins.
                for member in [*vars(value).values(), *value.__bases__]:
                    visit(member)
        elif isinstance(value, dict):
            for member in list(value.values()):
                visit(member)
        elif isinstance(value, (list, tuple, set, frozenset)):
            for member in list(value):
                visit(member)

    for value in list(namespace.values()):
        visit(value)


# In the deployable merged module the shared trigger base has already been
# defined and the shared reader is defined after this connector. Shadow its base
# with an Informix-owned wrapper that installs the callback at class creation.
# In normal package imports that global is absent, so this block is inert.
class _ShortenedRegistrations:
    # Docstring: The Spark session the merged source registers its data source with, whose
    # dataSource.register shortens the data source first (_shorten_merged_definitions).
    #
    # The shortening at the end of informix.py runs before the definitions merged after it,
    # the framework's data source and readers, exist; they kept their prefixes,
    # annotations, docstrings and columns, some 5 KB of the planner's limit. Everything
    # else is the session's.

    def __init__(self, session: Any) -> None:
        self._session = session

    def __getattr__(self, name: str) -> Any:
        return getattr(self._session, name)

    @property
    def dataSource(self) -> Any:  # noqa: N802  # pylint: disable=invalid-name
        registration = self._session.dataSource

        class Registration:
            def __getattr__(self, name: str) -> Any:
                return getattr(registration, name)

            def register(self, data_source: Any) -> Any:
                _shorten_merged_definitions({"data_source": data_source})
                return registration.register(data_source)

        return Registration()


try:
    # This name is local to register_lakeflow_source() after merging; globals()
    # cannot see it. In a normal package import it is intentionally undefined.
    _generated_trigger_base = SupportsTriggerAvailableNow  # type: ignore[name-defined] # noqa: F821
except NameError:
    _generated_trigger_base = None
else:
    # A second registration in one process (a driver kept between updates) finds the
    # wrapper the first one installed: take the base it wraps. Kept, the first one's
    # wrapper went into the pickle with its hooks and what they close over, and passed
    # the planner's limit (~13 KB).
    _generated_trigger_base = getattr(
        _generated_trigger_base, "_informix_original_base", _generated_trigger_base
    )
    # Avoid assigning this name in register_lakeflow_source(): doing so would
    # make the imported PySpark base an uninitialized local throughout that
    # function. Replace the generated module global instead.
    globals()["SupportsTriggerAvailableNow"] = _informix_available_now_base(
        _generated_trigger_base, locals().get("spark")
    )
    # This block runs while the pipeline module executes on the driver, which is
    # the only place an ambient workspace identity is resolvable. Every process a
    # reader later runs in has none, so Lakebase provisioning would have no way to
    # reach the control plane unless the credential is captured here and carried
    # along. Doing it in the merged-deployment branch is deliberate: a plain
    # package import is not a driver and has nothing to capture.
    capture_workspace_credentials()
    # When and where, so that the driver captures again as a package's does (see
    # _driver_registration): it was never, and the token lasts about an hour.
    _CREDENTIAL_CAPTURED.update(at=time.monotonic(), pid=os.getpid())
    # The merged source's own parameter: the framework's data source, merged after this
    # file, registers through it and is shortened then (_ShortenedRegistrations).
    if locals().get("spark") is not None:
        spark = _ShortenedRegistrations(locals()["spark"])


class ConnectionCapacityUnavailable(InformixError):
    # Docstring: No connector-managed Informix connection slot is currently available.
    pass


class SharedStateAccessUnavailable(InformixError):
    # Docstring: A transient permissions failure prevented access to shared state.
    pass


class LogRetentionError(InformixError):
    # Docstring: The requested restart LSN is no longer retained by Informix.
    pass


def _stale_capture_label_error(frame: bytes, error: Exception, start_lsn: int) -> InformixError:
    # Docstring: Explain an undecodable CDC frame in terms the operator can act on.
    #
    # Informix can emit log records that predate this capture's registration while
    # still tagging them with the session's capture label. ``decode_frame`` trusts
    # the label to select column descriptors, so such a record is decoded with the
    # wrong layout and fails -- observed in production as a bare
    # ``UnicodeDecodeError`` on a byte belonging to a foreign row's binary field,
    # with nothing to tie it to the log position that caused it.
    #
    # Fail closed rather than skipping the frame. A frame that cannot be decoded
    # under its declared layout cannot be classified: it may be a foreign record
    # (safe to drop) or a real change this reader must not lose, and nothing in the
    # frame distinguishes the two. Silently skipping would risk dropping
    # replicated changes, and continuing would risk ingesting a foreign row as if
    # it were data. Naming the log file lets the operator confirm the checkpoint
    # sits in reused log territory, which is the actual remedy.

    log_file = frame_log_file(frame)
    location = f"logical log {log_file}" if log_file is not None else "an unidentified logical log"
    # 11: Unable to decode an Informix CDC record from {} under the layout registered for this
    # capture: {}. The source replayed a log record that does not match this table's captured
    # columns, which happens when the restart position ({}, logical log {}) lies in a log file whose
    # space Informix has since reused for other tables. The record cannot be classified, so it is
    # neither skipped nor ingested. Run a full refresh to re-establish a restart position in the
    # live log.
    return InformixError(_m(11, location, error, start_lsn, start_lsn >> 32))


def _cdc_text_error(frame: bytes, error: Exception, start_lsn: int) -> InformixError:
    # Docstring: Explain a CDC record whose text would not decode.
    #
    # Two causes look alike here: text that is not in the database's codeset, the one CDC
    # text is decoded in (a client that wrote another codeset unconverted), and a record
    # of another table decoded under this capture's layout (see
    # _stale_capture_label_error). Named alone, the second sent an operator to a full
    # refresh that could not help with the first.

    log_file = frame_log_file(frame)
    location = f"logical log {log_file}" if log_file is not None else "an unidentified logical log"
    encoding = getattr(error.__cause__, "encoding", "the DB_LOCALE codeset")
    # 454: Unable to decode text in an Informix CDC record from {} as {}, the codeset of
    # DB_LOCALE: {}. Either the table holds a value that is not text in the database's
    # codeset, written by a client that did not convert it. Or the record is not of this
    # capture, which happens when the restart position ({}, logical log {}) lies in a log
    # file whose space Informix has since reused for other tables: then run a full refresh.
    return InformixError(_m(454, location, encoding, error, start_lsn, start_lsn >> 32))


class UnsupportedChangeError(InformixError):
    # Docstring: A source operation cannot be represented by the Lakeflow interface.
    pass
