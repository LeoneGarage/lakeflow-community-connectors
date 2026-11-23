"""Incremental chunks, snapshots, and the index-ordered scan's progress files."""

from __future__ import annotations

import contextlib
import json
import logging
import os
import secrets
import time
from typing import Any, Iterator, Sequence

from databricks.labs.community_connector.sources.informix.cdc_protocol import (
    validate_snapshot_arity,
)
from databricks.labs.community_connector.sources.informix.informix_bridge import (
    PurePythonInformixBridge,
)
from databricks.labs.community_connector.sources.informix.informix_filters import (
    _snapshot_filter,
    _snapshot_predicate,
)
from databricks.labs.community_connector.sources.informix.informix_host_sharing import (
    _LITERAL_UNSAFE_KEY_TYPES,
    _AsyncStagePipeline,
    _cdc_capable,
)
from databricks.labs.community_connector.sources.informix.informix_keys import (
    _is_variable_scale_decimal,
    _order_key,
    _snapshot_scan_columns,
    _snapshot_scan_value,
)
from databricks.labs.community_connector.sources.informix.informix_offsets import (
    _validated_offset,
    _with_backlog_streak,
)
from databricks.labs.community_connector.sources.informix.informix_options import (
    _DEFAULT_KEYLESS_SNAPSHOT_PAGE_SIZE,
    _DEFAULT_SNAPSHOT_PAGE_SIZE,
    _INDEX_SCAN_AUTO_GROUP_ROWS,
    _INDEX_SCAN_KEY_TYPES,
    _PIPELINE_SCOPE,
    _REPLAY_BEFORE_FIRST_CHUNK,
    _REPLAY_DRAIN_TO_MAX_PK,
    _REPLAY_MAX_CHUNK_FETCHES,
    _REPLAY_STOP_PK_OPTION,
    _RESNAPSHOT_OPTION,
    _SNAPSHOT_SCAN_CURSOR,
    _SNAPSHOT_SCAN_KEYSET,
    _SNAPSHOT_SCAN_OPTION,
    _UNL_SNAPSHOT_MODES,
    InformixError,
    _m,
)
from databricks.labs.community_connector.sources.informix.informix_registration import (
    _SCAN_PROGRESS_VERSION,
    _SCAN_RESUME_SECONDS,
    Table,
    _replay_keeps_floor,
)
from databricks.labs.community_connector.sources.informix.informix_shards import (
    _schema_fingerprint,
    _SpilledRows,
)
from databricks.labs.community_connector.sources.informix.informix_transactions import (
    _dropped_mount_error,
    _filtered_pages,
    _remove_tree,
    _strict_lsn,
)
from databricks.labs.community_connector.sources.informix.informix_unload import (
    _decode_snapshot_stage_value,
    _deep_size,
    _encode_snapshot_stage_value,
    _offset,
    _shape_snapshot,
)


class _ScanMixin:
    # Docstring: Incremental chunks, snapshots, and the index-ordered scan's progress files.

    def _read_incremental_step(
        self, table: Table, checkpoint: dict, options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        incremental = dict(checkpoint["incremental"])
        exact = incremental.get("exact")
        replaying = self._replay_stop_lsn(options) is not None
        if exact and not replaying:
            # The pages of chunks whose windows are closed: the batches that read them, and
            # those that closed them, are committed, so no replay serves or judges by them.
            self._cleanup_consumed_snapshot_stage_pages(
                table,
                *exact,
                int(incremental["boundary_lsn"]),
                min([window[0] for window in incremental["windows"]] + [incremental["chunks"]]),
            )
        # Stream a bounded batch of committed changes from the current cursor.
        cdc_rows, end = self._read_stream(table, checkpoint, options, deletes=False)
        if exact:
            # Less the windows the stream has read through (see _drop_copied_changes).
            incremental["windows"] = end["incremental"]["windows"]
        if isinstance(cdc_rows, _SpilledRows):
            # A transaction that spilled is a batch of its own, served from its pages: listed
            # here with a chunk, its rows were read back onto the driver whole. The next
            # batch reads the chunk.
            return cdc_rows, {**end, "incremental": incremental}
        output = list(cdc_rows)
        # The stream reader may return a freshly built offset that drops the
        # incremental block; the schema it used is authoritative for chunking.
        live = self._refresh_table_schema(table, None)
        if (
            end.get("schema_fingerprint") != _schema_fingerprint(live)
            and self._replay_stop_lsn(options) is None
        ):
            # A step to a generation the live layout has moved past reads nothing under it
            # (see _read_stream), so the copy keeps its cursor until a read reaches the live
            # schema: a chunk read now would land rows under columns that are not current.
            # A replay reads the chunk its batch read, under the live layout, to the cursor
            # Spark logged (none past a metadata-only step, whose cursor did not move).
            return iter(output), _with_backlog_streak(
                {**end, "incremental": incremental}, checkpoint, True
            )
        table = live
        # A replay repeats a batch an earlier update logged, its change-log range and copy
        # chunk alike, so it reads with the bound that batch had: the change log it repeats
        # began before that bound was captured, and carries every row inserted above it.
        # Captured again, a bound lowered by rows deleted at the table's top since ended the
        # copy short of the cursor Spark logged, and the replay failed on every restart.
        # Not a copy that follows the stream: its bound rides in one offset with the stream's
        # position, and a bound moved would leave rows the stream dropped to the copy uncopied,
        # or those it kept copied again.
        if not (replaying or exact) and self._incremental_bound_is_foreign(incremental):
            # A bound from an earlier update was captured before this update's CDC
            # start, which would orphan rows inserted in between. Recapture it --
            # and only it, since the cursor still names rows this table holds. After the
            # stream read, so a schema change the copy can't take is refused first (a key
            # widened to FLOAT, 468, recorded for the delete reader): read before it, the
            # bound's query met the changed table and failed on the float, or worse.
            # 232: Informix incremental snapshot bound for '{}' was captured by scope={} but this
            # update is scope={}; recapturing the snapshot bound so rows above a stale bound are not
            # orphaned between the snapshot and the change stream
            logging.getLogger(__name__).warning(
                _m(232, table.exposed_name, incremental.get("scope"), self._registration_scope),
            )
            # In the expressions the copy pages by, the checkpoint's own (see
            # _next_incremental_chunk), not those today's options would give: with
            # snapshot.incremental.datetime.as.string turned off mid-copy the bound was
            # left stale, and turned on, recaptured in another space than the cursor.
            incremental = self._rebound_incremental_block(
                table, incremental, dict(incremental.get("chunk_exprs") or {})
            )
        if not incremental["done"]:
            chunk_rows, incremental = (
                self._copy_chunk(table, incremental, options, int(end["commit_lsn"]))
                if exact
                else self._next_incremental_chunk(table, incremental, options)
            )
            output.extend(chunk_rows)
        # A copy that follows the stream keeps its block until its last window is closed in
        # a batch that is committed: a replay of the batch that closed it judges by its page.
        # A replay, which can read in several polls, drops it as the batch it repeats did.
        keep = exact and (
            self._replay_stop_pk(options, table.primary_keys) is not _REPLAY_DRAIN_TO_MAX_PK
            if replaying
            else incremental["windows"] or checkpoint["incremental"]["windows"]
        )
        if incremental["done"] and not keep:
            if exact and not replaying:
                self._snapshot_cleanup_executor().submit(
                    self._delete_snapshot_page,
                    os.path.join(
                        self._snapshot_stage_namespace(table, *exact),
                        "runs",
                        str(incremental["boundary_lsn"]),
                    ),
                )
            end.pop("incremental", None)
        else:
            end = dict(end)
            end["incremental"] = incremental
            # An unfinished incremental copy is *exact* proof of outstanding work,
            # stronger than the CDC row-budget signal _read_stream just applied:
            # the chunk cursor has not yet reached the snapshot's upper bound, so
            # rows demonstrably remain. Without this the backlog signal is blind
            # for the whole bulk-copy phase -- which is precisely when a flow has
            # the most to catch up (observed after a full refresh: every reader
            # was in the incremental phase and none recorded a streak).
            #
            # Idempotent with the call inside _read_stream: both derive the new
            # value from ``checkpoint``, not from ``end``. A copy that has read its last
            # chunk and waits for the stream to pass its windows has no rows left: what
            # the stream found stands.
            if not incremental["done"]:
                end = _with_backlog_streak(end, checkpoint, True)
        return iter(output), end

    # pylint: disable-next=too-many-locals,too-many-statements
    def _next_incremental_chunk(
        self, table: Table, incremental: dict, options: dict[str, str]
    ) -> tuple[list[dict[str, Any]], dict]:
        # The cursor and bound were paged in the key ordering recorded at seed
        # time. If the table's key index direction now differs -- e.g. an
        # all-ascending checkpoint resumed under mixed-direction paging, or vice
        # versa -- the persisted cursor is not comparable in the new order, and
        # resuming would silently skip rows. Fail closed: the copy must restart.
        seeded_descending = tuple(bool(v) for v in (incremental.get("key_descending") or ()))
        if seeded_descending != tuple(table.key_descending):
            # 237: Incremental snapshot for '{}' was seeded with a different key ordering than the
            # table's current key index; the paged cursor cannot be compared across the change. Run
            # a full refresh.
            raise InformixError(_m(237, table.exposed_name))
        page_size = self._table_int_option(
            options, "snapshot.page.size", _DEFAULT_SNAPSHOT_PAGE_SIZE, minimum=1
        )
        chunk_exprs = dict(incremental.get("chunk_exprs") or {})
        # Columns chunked by an order-preserving expression are compared and
        # cursored on the rendered "__chunk_<key>" string, not the raw value.
        cursor_names = [
            f"__chunk_{key}" if key in chunk_exprs else key for key in table.primary_keys
        ]
        # The cursor/bound comparisons below must run in the key index's own scan
        # order. For a mixed-direction key that is not Python's all-ascending tuple
        # order, so wrap the DESC columns to flip just those positions. A chunked
        # key compares on the always-ascending "__chunk_" strings, and an
        # all-ascending key is left exactly as before.
        order_desc = () if chunk_exprs else tuple(table.key_descending)

        def order_key(values: Sequence[Any]) -> tuple:
            return _order_key(values, order_desc)

        last_pk = (
            None
            if incremental.get("last_pk") is None
            else _decode_snapshot_stage_value(incremental["last_pk"])
        )
        max_pk = tuple(_decode_snapshot_stage_value(incremental["max_pk"]))
        # The cursor and the bound are compared against keys read from the
        # refreshed table, which _read_incremental_step may have re-read between
        # the CDC read and this chunk. A cursor of the wrong arity would compare
        # mismatched tuples: every value still exists, so rows would be skipped
        # with no error. The blocking path validates this in snapshot_page; do the
        # same here rather than rely on the schema guards upstream.
        if last_pk is not None:
            validate_snapshot_arity(last_pk, table.primary_keys)
        validate_snapshot_arity(max_pk, table.primary_keys)
        # A replay must reproduce the whole committed key range, so it stops at
        # the cursor ``end`` recorded rather than after one page. ``null`` means
        # the copy finished inside that range, which leaves max_pk as the bound.
        replay_stop_pk = self._replay_stop_pk(options, table.primary_keys)
        if replay_stop_pk is _REPLAY_BEFORE_FIRST_CHUNK:
            return [], dict(incremental)
        replaying_chunk = replay_stop_pk is not None
        bound = max_pk
        if replay_stop_pk is not None and replay_stop_pk is not _REPLAY_DRAIN_TO_MAX_PK:
            # min() in the key's own paging order, not Python's default.
            bound = replay_stop_pk if order_key(replay_stop_pk) < order_key(max_pk) else max_pk

        if (
            replaying_chunk
            and last_pk is not None
            and replay_stop_pk is not _REPLAY_DRAIN_TO_MAX_PK
            and order_key(tuple(last_pk)) >= order_key(bound)
        ):
            # A replay already at its committed cursor, as a second poll of a range is,
            # has no chunk rows left to read.
            return [], dict(incremental)

        def cursor_key(row: dict[str, Any]) -> tuple:
            return tuple(row[name] for name in cursor_names)

        chunk_lsn: int | None = None
        bounded: list[dict[str, Any]] = []
        passed_upper_bound = False
        has_more = False
        cursor = last_pk
        # One fetch for an ordinary read. A replay keeps paging until it reaches
        # the committed cursor, because rows inserted into that range since the
        # original read can push it beyond a single page -- and stopping short is
        # the truncation this bound exists to prevent.
        for _ in range(_REPLAY_MAX_CHUNK_FETCHES):
            # Ask for one row more than the page keeps, so "the page filled" and
            # "rows remain" are distinguishable rather than inferred: the same
            # probe the blocking consistent_snapshot loop uses.
            fetch_lsn, raw = self._snapshot_read_with_reconnect(
                lambda cursor=cursor: self._bridge.snapshot_chunk(
                    table.identity,
                    [column.name for column in table.columns],
                    list(table.primary_keys),
                    cursor,
                    page_size + 1,
                    # The table's limit: left out, the bridge took the connection's.
                    max_bytes=self._table_int_option(options, "snapshot.max.bytes", 0, minimum=0),
                    chunk_exprs=chunk_exprs or None,
                    snapshot_filter=_snapshot_predicate(incremental.get("snapshot_filter"), table),
                    key_descending=list(table.key_descending),
                ),
                table=table,
                operation="chunk read",
            )
            try:
                fetch_lsn = _strict_lsn(fetch_lsn, "chunk_lsn")
            except ValueError as error:
                # 238: Incremental snapshot chunk for '{}' returned an invalid LSN
                raise InformixError(_m(238, table.exposed_name)) from error
            # Every row in this batch is stamped with the LSN of the fetch that
            # read it, so a multi-fetch replay keeps each row's own read point.
            if chunk_lsn is None:
                chunk_lsn = fetch_lsn
            has_more = len(raw) > page_size
            # Rows beyond the snapshot's captured upper bound are left to the
            # change stream (maximum-key semantics). A replay's own bound
            # is tracked separately: stopping there says nothing about whether the
            # snapshot is finished.
            page: list[dict[str, Any]] = []
            stopped_at_bound = False
            for row in raw[:page_size]:
                key = cursor_key(row)
                if order_key(key) > order_key(max_pk):
                    passed_upper_bound = True
                    break
                if order_key(key) > order_key(bound):
                    stopped_at_bound = True
                    break
                page.append(row)
            bounded.extend(page)
            if page:
                cursor = cursor_key(page[-1])
            reached_bound = cursor is not None and order_key(cursor) >= order_key(bound)
            if not replaying_chunk or passed_upper_bound or stopped_at_bound:
                break
            if reached_bound or not page or not has_more:
                break
        else:
            # 239: Replayed incremental snapshot range for '{}' did not reach its committed cursor
            # within {} chunk reads
            raise InformixError(_m(239, table.exposed_name, _REPLAY_MAX_CHUNK_FETCHES))
        last_cursor = cursor_key(bounded[-1]) if bounded else None
        to_cursor = replaying_chunk and replay_stop_pk is not _REPLAY_DRAIN_TO_MAX_PK
        if to_cursor:
            # The key-ordered scan went past the replay's bound, reached it, or reached the
            # end of the table: every row of the range that still exists was read. A chunk
            # holds no lock across batches, so the rows at its end may have been deleted
            # since its batch, and the replay stands at the cursor Spark logged regardless.
            last_cursor = bound
        # Strip the "__chunk_<key>" helper aliases before shaping so they never
        # reach the emitted row or the destination schema.
        chunk_aliases = [f"__chunk_{key}" for key in chunk_exprs]
        cleaned = [
            {key: value for key, value in row.items() if key not in chunk_aliases}
            for row in bounded
        ]
        stamp = chunk_lsn if chunk_lsn is not None else 0
        commit_time = self._source_utc_time()
        shaped = [
            _shape_snapshot(row, stamp, table, options, commit_time=commit_time)
            for row in self._landed_rows(table, cleaned)
        ]
        result = dict(incremental)
        result["chunk_lsn"] = str(stamp)
        if last_cursor is not None:
            result["last_pk"] = _encode_snapshot_stage_value(list(last_cursor))
        # Only the snapshot's own bound ends the copy. A replay stops at the
        # committed cursor, which normally sits well below max_pk, so treating
        # that stop as completion would abandon the rest of the copy: the flow
        # would leave the incremental phase with most of the table uncopied. A row
        # past max_pk, or the end of the table, says the same only for a replay of
        # the batch that finished the copy, which drains to max_pk as that batch did.
        reached_max_key = last_cursor is not None and order_key(last_cursor) >= order_key(max_pk)
        if to_cursor:
            done = reached_max_key
        else:
            done = passed_upper_bound or not has_more or reached_max_key
        result["done"] = done
        return shaped, result

    def _copy_chunk(
        self, table: Table, incremental: dict, options: dict[str, str], streamed: int
    ) -> tuple[list[dict[str, Any]], dict]:
        # Docstring: The next chunk of a copy that follows the stream, read once the stream
        # is at ``streamed`` and staged as one immutable page; a replay's served from it.
        #
        # The page records the chunk's window: its keys, and the log positions it was read
        # between, from the later of its read's start and ``streamed`` (the stream dropped
        # the changes to its keys up to there, which the chunk holds) to the log's end after
        # the read. The stream judges each change to those keys by it (_drop_copied_changes).
        # Read again, a chunk holds other rows than the page the stream judges by.

        scope, schema_id = incremental["exact"]
        lsn, number = int(incremental["boundary_lsn"]), incremental["chunks"]
        stop = self._replay_stop_pk(options, table.primary_keys)
        if stop is None:
            rows, result = self._next_incremental_chunk(table, incremental, options)
            after = self._snapshot_read_with_reconnect(
                self._bridge.current_lsn, table=table, operation="chunk end"
            )
            meta = {
                "window": [
                    number,
                    incremental["last_pk"],
                    result["max_pk"] if result["done"] else result["last_pk"],
                    str(max(int(result["chunk_lsn"]), streamed)),
                    str(after),
                ],
                "last_pk": result["last_pk"],
                "done": result["done"],
            }
            # In place of a page a read of this chunk staged for a batch never logged.
            self._delete_snapshot_page(
                os.path.join(
                    self._snapshot_stage_namespace(table, scope, schema_id),
                    "runs",
                    str(lsn),
                    f"page-{number:08d}",
                )
            )
            self._publish_snapshot_stage_page(
                table, scope, schema_id, lsn, number, rows, json.dumps(meta)
            )
        elif stop is _REPLAY_BEFORE_FIRST_CHUNK or (
            stop is not _REPLAY_DRAIN_TO_MAX_PK
            and incremental["last_pk"] is not None
            and tuple(_decode_snapshot_stage_value(incremental["last_pk"])) == stop
        ):
            # The batch replayed read no chunk: its cursor stayed.
            return [], incremental
        else:
            record = self._read_snapshot_stage_page_record(table, scope, schema_id, lsn, number)
            meta = json.loads(record["lower_pk"])
            rows = _decode_snapshot_stage_value(record["rows"])
        return rows, {
            **incremental,
            "last_pk": meta["last_pk"],
            "done": meta["done"],
            "chunks": number + 1,
            "windows": [*incremental["windows"], meta["window"]],
        }

    @staticmethod
    def _replay_stop_pk(
        options: dict[str, str], primary_keys: tuple[str, ...]
    ) -> object | tuple | None:
        # Docstring: The chunk cursor a replayed read must stop at, or None if unbounded.
        #
        # JSON ``null`` returns a sentinel rather than None: both use max_pk as the
        # numerical bound, but the replay must keep fetching pages until it reaches
        # that bound while an ordinary read intentionally fetches only one page.
        #
        # Validated rather than trusted, for the same reason as the LSN bound: a
        # malformed value would silently widen the replay back to the behaviour
        # being fixed.

        raw = options.get(_REPLAY_STOP_PK_OPTION)
        if raw is None:
            return None
        try:
            decoded = json.loads(raw)
        except ValueError as error:
            # 240: Informix replay stop cursor is not valid JSON
            raise InformixError(_m(240)) from error
        if decoded is None:
            return _REPLAY_DRAIN_TO_MAX_PK
        if decoded == {"before_first_chunk": True}:
            return _REPLAY_BEFORE_FIRST_CHUNK
        if not isinstance(decoded, list):
            # 241: Informix replay stop cursor must be a list or null
            raise InformixError(_m(241))
        cursor = _decode_snapshot_stage_value(decoded)
        validate_snapshot_arity(cursor, primary_keys)
        return tuple(cursor)

    def _served_snapshot_table(
        self, table: Table, effective_start: dict, end_offset: dict | None
    ) -> Table:
        # Docstring: ``table`` for a served snapshot batch, failing 243 on drift: checked by
        # its start's fingerprint, or a first batch's by the one its end pinned.
        if effective_start and effective_start.get("phase") not in (None, "snapshot"):
            return table
        expected = (effective_start or end_offset or {}).get("schema_fingerprint")
        return self._snapshot_schema_held(table, expected) if expected else table

    def _snapshot_schema_held(self, table: Table, expected: str | None) -> Table:
        # Docstring: ``table`` for a snapshot read on from its checkpoint, failing 243 where the
        # schema or a fingerprinted option no longer gives the snapshot's ``expected``.
        #
        # Staged serve reads immutable, self-describing pages from the Volume, so it needs
        # no live Informix schema. Re-query it only every _snapshot_schema_refresh_interval
        # seconds to catch mid-snapshot drift; between refreshes, validate the pinned
        # fingerprint against the cached table (no connection, no catalog query). The first
        # resume microbatch always refreshes (no timestamp yet). Both readers check here:
        # the partitioned one's continuations carried the fingerprint on unchecked, and
        # served the rest of a snapshot under changed options (initial_only, whose stream
        # never reads, to its end).

        now = time.monotonic()
        last = self._snapshot_schema_refreshed_at.get(table.identity)
        if last is None or now - last >= self._snapshot_schema_refresh_interval:
            table = self._refresh_table_schema(table, expected)
            self._snapshot_schema_refreshed_at[table.identity] = now
        elif expected != _schema_fingerprint(table):
            # 243: Informix schema changed for '{}' during ingestion; run a full refresh before
            # reading additional snapshot or CDC records
            raise InformixError(_m(243, table.exposed_name))
        return table

    # pylint: disable-next=too-many-branches,too-many-locals,too-many-statements
    def _read_snapshot(
        self,
        table: Table,
        start: dict | None,
        options: dict[str, str],
        *,
        pipeline_scope_override: str | None = None,
        allow_keyless: bool = False,
    ):
        checkpoint = _validated_offset(start) if start else None
        pipeline_scope = pipeline_scope_override or self._pipeline_scope()
        # Hand the bridge the same progress callback the drain worker installed, so a worker
        # blocked acquiring its slot (before any page is staged) can keep the consumer's
        # liveness watchdog alive while the pool is turning over. None for non-drain reads,
        # where slot-wait liveness is off anyway. Constructs the bridge without connecting.
        self._bridge._slot_wait_liveness = self._drain_progress
        if checkpoint and checkpoint.get("schema_fingerprint") is None:
            # 242: Informix snapshot checkpoint for '{}' predates schema-safe offsets; run a full
            # refresh
            raise InformixError(_m(242, table.exposed_name))
        expected_fingerprint = checkpoint.get("schema_fingerprint") if checkpoint else None
        if checkpoint:
            table = self._snapshot_schema_held(table, expected_fingerprint)
        else:
            table = self._refresh_table_schema(table, expected_fingerprint)
        if not _cdc_capable(table) and not (
            allow_keyless and all(column.cdc_supported for column in table.columns)
        ):
            # 244: Table '{}' is no longer CDC-capable after metadata refresh; run a full refresh
            # after restoring its primary key and supported schema
            raise InformixError(_m(244, table.exposed_name))
        # The key columns the drain pages by: none for a keyless table or a cursor scan,
        # which then reads through one unordered forward-only cursor.
        scan_keys = () if self._snapshot_cursor_scan(options) else table.primary_keys
        if scan_keys:
            page_size = self._table_int_option(
                options, "snapshot.page.size", _DEFAULT_SNAPSHOT_PAGE_SIZE, minimum=1
            )
        else:
            # A keyless initial snapshot pages positionally and stages larger pages by
            # default; its own knob so it never changes keyed keyset chunk sizing. A
            # cursor scan of a keyed table pages the same way, so it shares the knob.
            page_size = self._table_int_option(
                options,
                "keyless.snapshot.page.size",
                _DEFAULT_KEYLESS_SNAPSHOT_PAGE_SIZE,
                minimum=1,
            )
        if checkpoint:
            # Resolve the staging scope from the *checkpoint*, not this update's
            # registration scope. The staged pages and manifest were written under the
            # update scope that created them (``{pipeline_id}_@_{update_id}``), which the
            # checkpoint records; the current registration scope carries a fresh update_id
            # after a stop/start. Locating the pages by their creating scope -- together
            # with the schema_id and snapshot_lsn the checkpoint also pins -- lets a
            # resumed read find them instead of missing the manifest and forcing a full
            # refresh mid-load. Full-refresh semantics are untouched: a full refresh clears
            # the Spark checkpoint, so it arrives with no offset and takes the else branch,
            # where the update-scoped initialization record establishes a fresh boundary
            # and re-snapshots. A legacy offset without a stored scope falls back to the
            # registration scope (the prior behaviour).
            # For a handoff relocation the pages live under the *capturing* flow's scope,
            # carried on stage_scope; the offset's own pipeline_scope is already the new
            # flow's. stage_scope drives the manifest and page reads; pipeline_scope drives
            # the emitted offsets. Absent stage_scope, the two coincide (unchanged).
            pipeline_scope, stage_scope = self._offset_scopes(checkpoint)
            high_water = int(checkpoint["snapshot_lsn"])
            schema_id = str(checkpoint["schema_id"])
            manifest = self._read_snapshot_stage_manifest(table, stage_scope, schema_id)
            if manifest is None:
                if self._table_snapshot_source(options) is not None:
                    # A .unl serve stages no pages: its checkpoint is a split position the
                    # partitioned reader's windows advance, and the simple reader can only
                    # serve an extract whole.
                    # 423: Informix file snapshot of '{}' is part-way through a serve in split
                    # windows, which only stream.partitioned=true can continue; set it back to true
                    # until the snapshot has finished, and no full refresh is needed
                    raise InformixError(_m(423, table.exposed_name))
                # 245: Incremental snapshot state for '{}' is missing; run a full refresh
                raise InformixError(_m(245, table.exposed_name))
            if int(manifest["snapshot_lsn"]) != high_water:
                # 246: Incremental snapshot boundary mismatch for '{}'
                raise InformixError(_m(246, table.exposed_name))
            page_index = int(checkpoint["snapshot"].get("page_index", -1))
            if page_index == 0 and int(manifest["page_count"]) == 0:
                # The replay of an empty table's first batch (_staged_first_batch_start):
                # it served nothing and went on to the stream, as it does here.
                return iter(()), self._naming_scan(
                    _offset(
                        high_water,
                        high_water,
                        high_water,
                        None,
                        "stream",
                        table,
                        schema_id,
                        pipeline_scope,
                    ),
                    table,
                    stage_scope,
                    schema_id,
                    manifest,
                )
            return self._staged_snapshot_result(
                table,
                pipeline_scope,
                schema_id,
                manifest,
                page_index,
                checkpoint,
                stage_scope=stage_scope,
            )
        elif self._table_snapshot_source(options) and (
            # The extract is the initial snapshot of the modes that take one from it. Under
            # another (a connection-scope snapshot.source), a blocking snapshot that
            # incremental or auto_snapshot falls back to served the extract on the driver,
            # an auto_snapshot resnapshot an old one, while a chunked copy read the table.
            self._table_snapshot_mode(table, options) in _UNL_SNAPSHOT_MODES
            or self._replayed_unl_boundary(table, options) is not None
        ):
            # File source: serve the initial snapshot from Informix UNLOAD (`.unl`) files
            # on a Volume instead of a live scan. Serial driver fallback used when the
            # partitioned reader is off; the partitioned path fans the same splits across
            # executors (see get_partitions/read_partition). Both pin the boundary via
            # _pin_unl_boundary and hand back the shaped rows with a stream-phase offset --
            # the same transition a completed live snapshot makes, so the CDC stream
            # resumes at the boundary and _recover discards anything at/below it. The whole
            # extract serves in one microbatch (rows stream lazily, so driver memory stays
            # bounded); a restart before the offset commits simply replays it (idempotent).
            replayed = self._replayed_unl_boundary(table, options)
            if replayed is not None:
                table, schema_id, snapshot_lsn, pipeline_scope = replayed
            else:
                table, schema_id, snapshot_lsn = self._pin_unl_boundary(
                    table, options, pipeline_scope
                )
            rows = self._serve_unl_snapshot(table, options, snapshot_lsn)
            return rows, _offset(
                snapshot_lsn,
                snapshot_lsn,
                snapshot_lsn,
                None,
                "stream",
                table,
                schema_id,
                pipeline_scope,
            )
        else:
            consistent_snapshot = getattr(self._bridge, "consistent_snapshot", None)
            if not callable(consistent_snapshot):
                # 247: Configured Informix bridge does not implement consistent_snapshot()
                raise InformixError(_m(247))
            if pipeline_scope_override is None:
                # An index-ordered scan an earlier update began is carried on under that
                # update's scope, unless this update is a full refresh (see above). The
                # reader decides; the drain worker it hands the scan to is given the scope.
                pipeline_scope = self._adopted_scan_scope(table, pipeline_scope, options)
            # Validate the complete snapshot contract before _initial_lsn()
            # enables full-row logging or publishes durable initialization.
            high_water = self._initial_lsn(table, scope=pipeline_scope)
            schema_id = self._snapshot_schema_ids[(pipeline_scope, table.identity)]
            manifest = self._read_snapshot_stage_manifest(table, pipeline_scope, schema_id)
            if manifest is not None:
                page_count = int(manifest["page_count"])
                if page_count == 0:
                    snapshot_lsn = int(manifest["snapshot_lsn"])
                    return iter(()), self._naming_scan(
                        _offset(
                            snapshot_lsn,
                            snapshot_lsn,
                            snapshot_lsn,
                            None,
                            "stream",
                            table,
                            schema_id,
                            pipeline_scope,
                        ),
                        table,
                        pipeline_scope,
                        schema_id,
                        manifest,
                    )
                return self._staged_snapshot_result(table, pipeline_scope, schema_id, manifest, 0)
            if not self._is_snapshot_drain_worker():
                # Hand the long drain to the bounded daemon pool. Free this reader's
                # connection slot first so the wait holds none, then serve the pages the
                # daemon stages -- exactly as a resumed snapshot does. At most
                # ``snapshot.reader.threads`` drains run at once in a process, so streaming
                # readers keep the remaining slots. ``_initial_lsn`` above already established
                # the durable boundary; the daemon reads it rather than redoing it.
                self._release_worker_connection()
                manifest = self._drain_via_snapshot_daemon(
                    table, options, pipeline_scope, schema_id, allow_keyless
                )
                page_count = int(manifest["page_count"])
                if page_count == 0:
                    snapshot_lsn = int(manifest["snapshot_lsn"])
                    return iter(()), self._naming_scan(
                        _offset(
                            snapshot_lsn,
                            snapshot_lsn,
                            snapshot_lsn,
                            None,
                            "stream",
                            table,
                            schema_id,
                            pipeline_scope,
                        ),
                        table,
                        pipeline_scope,
                        schema_id,
                        manifest,
                    )
                return self._staged_snapshot_result(table, pipeline_scope, schema_id, manifest, 0)
            # A drain worker drains inline below, on the slot floor of its band.
            max_rows = self._table_int_option(options, "snapshot.max.rows", 0, minimum=0)
            expected_columns = {column.name for column in table.columns}
            page_count = 0
            row_count = 0
            shaped_bytes = 0
            previous_upper_pk: list[Any] | None = None
            first_upper_pk: str | None = None
            max_bytes = self._table_int_option(options, "snapshot.max.bytes", 0, minimum=0)
            # Capture the source's UTC time once, at drain start, and bake it into every
            # staged page's `r` rows as their commit time -- so the whole snapshot shares one
            # as-of instant and the served (immutable) pages carry it without re-deriving.
            commit_time = self._source_utc_time()

            def stage_page(
                snapshot_lsn: int,
                page_index: int,
                page_rows: list[dict[str, Any]],
            ) -> None:
                nonlocal page_count, row_count, shaped_bytes, previous_upper_pk, first_upper_pk
                if not page_rows:
                    return
                for index, row in enumerate(page_rows):
                    if not isinstance(row, dict) or set(row) != expected_columns:
                        # 248: Consistent snapshot row {} does not exactly match table columns for
                        # '{}'
                        raise InformixError(_m(248, row_count + index, table.exposed_name))
                try:
                    shaped = [
                        _shape_snapshot(row, snapshot_lsn, table, options, commit_time=commit_time)
                        for row in page_rows
                    ]
                except Exception as error:
                    if isinstance(error, InformixError):
                        raise
                    # 249: Cannot materialize consistent snapshot rows for '{}'
                    raise InformixError(_m(249, table.exposed_name)) from error
                # Not the page of rows an index-ordered scan read again: the pages they
                # are left out of counted them already.
                if max_bytes and not scanned.get("moved_rows_page"):
                    shaped_bytes += _deep_size(shaped)
                    if shaped_bytes > max_bytes:
                        # 250: Initial shaped snapshot exceeds snapshot.max.bytes={}
                        raise InformixError(_m(250, max_bytes))
                upper_pk = self._publish_snapshot_stage_page(
                    table,
                    pipeline_scope,
                    schema_id,
                    snapshot_lsn,
                    page_index,
                    shaped,
                    previous_upper_pk,
                )
                if page_index == 0:
                    first_upper_pk = json.dumps(upper_pk, allow_nan=False, sort_keys=True)
                previous_upper_pk = [page_rows[-1][key] for key in table.primary_keys]
                page_count = max(page_count, page_index + 1)
                row_count += len(page_rows)
                # A staged page is forward progress: reset the drain pool's liveness
                # watchdog so a large-but-progressing drain is never mistaken for a stall.
                # No-op when no pool set the callback.
                if self._drain_progress is not None:
                    self._drain_progress()

            # A positional REST drain (keyless, or keyed under snapshot.scan=cursor)
            # overlaps the next page fetch with the previous page's shape+upload through
            # a bounded background uploader; the counters stage_page owns are then read
            # only after the pipeline is joined below.
            scanned: dict[str, Any] = {}
            scan_index = self._scan_index(table, options)
            snapshot_lsn = None
            if scan_index is not None:
                # The scan records its progress and goes on from it after a failure. Not
                # with a byte limit on the snapshot, whose count is not kept.
                resume = progress = None
                if not max_bytes:
                    resume = self._scan_resume_state(
                        table, pipeline_scope, schema_id, high_water, scan_index
                    )
                    if resume:
                        commit_time = resume["commit_time"]
                        page_count, row_count = resume["staged"], resume["rows"]
                        if page_count:
                            # What staging the next page needs of those staged: the first
                            # page's upper key for the manifest, the last one's as the next
                            # page's lower key. A table with no key has empty ones.
                            # Their headers alone: a whole page parsed is its every row,
                            # past serverless's worker memory for a wide table.
                            first_upper_pk, previous_upper_pk = (
                                self._stream_staged_page(
                                    table, pipeline_scope, schema_id, resume["boundary"], at
                                )[0]["upper_pk"]
                                if table.primary_keys
                                else []
                                for at in (0, page_count - 1)
                            )
                            previous_upper_pk = _decode_snapshot_stage_value(previous_upper_pk)
                            first_upper_pk = json.dumps(
                                first_upper_pk, allow_nan=False, sort_keys=True
                            )
                    progress = self._scan_progress_writer(
                        table, pipeline_scope, schema_id, scan_index, commit_time, high_water
                    )
                snapshot_lsn = self._index_scan_snapshot(
                    table,
                    options,
                    scan_index,
                    page_size,
                    max_rows,
                    stage_page,
                    lambda lsn, index: self._stream_staged_page(
                        table, pipeline_scope, schema_id, lsn, index
                    )[1],
                    scanned,
                    resume,
                    progress,
                )
            # None when there is no index to page by, or the scan gave way to this read.
            index_scan = snapshot_lsn is not None
            pipeline = (
                _AsyncStagePipeline(stage_page)
                if not index_scan and self._should_pipeline_staging(scan_keys)
                else None
            )
            page_consumer = pipeline.submit if pipeline is not None else stage_page
            landed = self._landed_predicate(table)
            if landed is not None:
                # The limit counts the rows that land, so the bridge reads with none.
                page_consumer = _filtered_pages(page_consumer, landed, max_rows)
            try:
                snapshot_lsn, rows = (
                    (snapshot_lsn, [])
                    if index_scan
                    else consistent_snapshot(  # pylint: disable=not-callable
                        table.identity,
                        [c.name for c in table.columns],
                        scan_keys,
                        page_size,
                        0 if landed is not None else max_rows,
                        self._table_int_option(options, "snapshot.max.bytes", 0, minimum=0),
                        datetime_primary_key=any(
                            column.name in scan_keys
                            and column.type_name in _LITERAL_UNSAFE_KEY_TYPES
                            for column in table.columns
                        ),
                        page_consumer=page_consumer,
                        snapshot_filter=_snapshot_predicate(_snapshot_filter(options), table),
                        isolation=self._snapshot_isolation(options),
                        # A key index with a descending column: each page in its order.
                        **(
                            {"key_descending": list(table.key_descending)}
                            if any(table.key_descending)
                            and list(scan_keys) == list(table.primary_keys)
                            else {}
                        ),
                    )
                )
                if pipeline is not None:
                    # Finish and join every queued upload so page_count/row_count are
                    # complete before the manifest is published, re-raising any error.
                    pipeline.close()
            except BaseException:
                if pipeline is not None:
                    pipeline.shutdown()
                raise
            try:
                snapshot_lsn = _strict_lsn(snapshot_lsn, "snapshot_lsn")
            except ValueError as error:
                # 251: Consistent snapshot bridge returned an invalid snapshot LSN
                raise InformixError(_m(251)) from error
            if snapshot_lsn < high_water:
                # 252: Consistent snapshot LSN {} precedes prepared LSN {} for '{}'
                raise InformixError(_m(252, snapshot_lsn, high_water, table.exposed_name))
            try:
                # Retried on a drop: the bridge's own session sat idle while another read the
                # pages, and a reset lost the whole staged snapshot.
                current_lsn = _strict_lsn(
                    self._snapshot_read_with_reconnect(
                        self._bridge.current_lsn, table=table, operation="end"
                    ),
                    "current_lsn",
                )
            except ValueError as error:
                # 253: Informix bridge returned an invalid current source LSN
                raise InformixError(_m(253)) from error
            if snapshot_lsn > current_lsn:
                # 254: Consistent snapshot LSN {} exceeds current source LSN {} for '{}'
                raise InformixError(_m(254, snapshot_lsn, current_lsn, table.exposed_name))
            if not isinstance(rows, list):
                # 255: Consistent snapshot bridge must return rows as a list
                raise InformixError(_m(255))
            if max_rows and len(rows) > max_rows and landed is None:
                # 52: Initial snapshot exceeds snapshot.max.rows={}
                raise InformixError(_m(52, max_rows))
            if rows:
                if page_count:
                    # 256: Consistent snapshot bridge returned rows after consuming pages
                    raise InformixError(_m(256))
                if landed is not None:
                    _filtered_pages(stage_page, landed, max_rows)(snapshot_lsn, 0, rows)
                else:
                    stage_page(snapshot_lsn, 0, rows)
            retained_bytes = _deep_size(rows) if max_bytes else 0
            if max_bytes and retained_bytes > max_bytes:
                # 53: Initial snapshot exceeds snapshot.max.bytes={}
                raise InformixError(_m(53, max_bytes))
            table = self._refresh_table_schema(table, _schema_fingerprint(table))
            self._publish_snapshot_boundary(
                table,
                schema_id,
                high_water,
                snapshot_lsn,
                pipeline_scope,
                floor=not _replay_keeps_floor(options),
                resnapshot=_RESNAPSHOT_OPTION in options,
            )
            # The rows a scan read again are in the count twice, once for each page that
            # holds them: the manifest counts the rows the pages serve.
            scanned.pop("moved_rows_page", None)
            row_count -= scanned.pop("left_out_rows", 0)
            manifest = self._publish_snapshot_stage_manifest(
                table,
                pipeline_scope,
                schema_id,
                snapshot_lsn,
                page_count,
                row_count,
                first_upper_pk,
                scanned,
            )
            if page_count == 0:
                return iter(()), self._naming_scan(
                    _offset(
                        snapshot_lsn,
                        snapshot_lsn,
                        snapshot_lsn,
                        None,
                        "stream",
                        table,
                        schema_id,
                        pipeline_scope,
                    ),
                    table,
                    pipeline_scope,
                    schema_id,
                    manifest,
                )
            return self._staged_snapshot_result(table, pipeline_scope, schema_id, manifest, 0)

    def _with_connection_scan(self, table_options: dict[str, str]) -> dict[str, str]:
        # Docstring: ``table_options`` with the connection's snapshot.scan where they set
        # none, as the snapshot reads it (_scan_index): checked on the table options alone,
        # keyset(...) set on the connection passed the check that refuses it on a keyed
        # table.

        scan = self.options.get(_SNAPSHOT_SCAN_OPTION)
        if scan is None or _SNAPSHOT_SCAN_OPTION in table_options:
            return table_options
        return {**table_options, _SNAPSHOT_SCAN_OPTION: scan}

    def _scan_index(self, table, options) -> tuple[list[str], list[bool]] | None:
        # Docstring: The index a keyless table's snapshot pages by, as its columns and their
        # directions, or None when it reads through the one cursor instead. A table with a
        # key of its own pages by that key on the append path, and follows its changes as a
        # keyless one does; merged by its key, it returns None and pages by the key as before.
        #
        # ``snapshot.scan=cursor`` asks for the cursor. ``snapshot.scan=keyset(a, b desc)``
        # names the columns to page by, and may give their directions. Otherwise the table is
        # paged by the index with the most distinct keys among those on ingested columns of
        # a type whose values Python orders as Informix does (rows with a NULL in one of
        # them are read first, as one group). It cannot be when
        # there is none, with a filter (the scan and the stream would have to agree on it row
        # by row), or on a
        # database that logs buffered (a commit is visible there before CDC can hand it
        # over). With named columns the snapshot then fails; otherwise the table falls back
        # to the cursor, with a warning that its change log can hold the overlap.

        raw = options.get(
            _SNAPSHOT_SCAN_OPTION, self.options.get(_SNAPSHOT_SCAN_OPTION, _SNAPSHOT_SCAN_KEYSET)
        )
        requested = _snapshot_scan_value(raw)
        named = _snapshot_scan_columns(raw)
        own_key = bool(table.primary_keys) and not table.key_only_from_option
        if own_key and not self._append_only_table(table, options):
            # Merged by its key, which takes out what the snapshot and the stream both hold.
            return None
        bridge = self._bridge
        indexes = getattr(bridge, "table_indexes", None)
        if requested == _SNAPSHOT_SCAN_CURSOR or not (named or callable(indexes)):
            return None
        # Whether the scan will follow the table's changes (see _index_scan_snapshot): only
        # then does buffered logging matter, or a fallback leave an overlap to warn of.
        follows = not table.primary_keys or self._append_only_table(table, options)
        listed = (
            indexes(table.identity)  # pylint: disable=not-callable
            if callable(indexes)
            else []
        )
        # Not a DECIMAL(p) with no scale: it is staged as decimal.variable.type shapes it
        # (a string, a double), and the scan compares staged rows with rows as read.
        usable = {
            column.name.lower(): column.name
            for column in table.columns
            if column.type_name in _INDEX_SCAN_KEY_TYPES and not _is_variable_scale_decimal(column)
        }
        chosen = None
        if named:
            # The columns the option names, in the directions it gives; one it leaves unsaid
            # takes the direction of an index that starts with these columns, else ascending.
            for name, _direction in named:
                if name.lower() not in usable:
                    # 433: The index-ordered snapshot does not support {} on '{}'; set
                    # snapshot.scan=cursor to read the table without index order
                    raise InformixError(
                        _m(
                            433,
                            f"column {name}, which is not an ingested column of integer, "
                            "decimal, date or character type,",
                            table.exposed_name,
                        )
                    )
            names = [usable[name.lower()] for name, _direction in named]
            found = next(
                (
                    directions
                    for columns, directions, *_counts in listed
                    if columns[: len(names)] == names
                ),
                [False] * len(names),
            )
            chosen = (
                None,
                names,
                [found[at] if down is None else down for at, (_name, down) in enumerate(named)],
            )
        if own_key and not named and all(name.lower() in usable for name in table.primary_keys):
            # A key of its own on the append path: one row to a value, so each change is
            # judged by its own key's page. A key of a type the scan does not compare (a
            # DATETIME, say) leaves the table's other indexes, as for primary.keys.
            chosen = (
                None,
                list(table.primary_keys),
                list(table.key_descending)
                if len(table.key_descending) == len(table.primary_keys)
                else [False] * len(table.primary_keys),
            )
        for columns, directions, nunique, *rows in () if named or chosen else listed:
            # The catalog's count of distinct keys, which is 0 until statistics are run:
            # among equals, more columns make smaller groups.
            rank = (nunique, len(columns))
            # A table merged by its key can page by that key instead, so an index is chosen
            # for it only on statistics that show few rows to a value: all rows of one are
            # read together, and too many fail the snapshot.
            count = rows[0] if rows else 0
            few = follows or (nunique and count and count / nunique <= _INDEX_SCAN_AUTO_GROUP_ROWS)
            if (
                few
                and all(column.lower() in usable for column in columns)
                and (chosen is None or rank > chosen[0])
            ):
                chosen = (rank, columns, directions)
        if table.row_filter is not None or _snapshot_filter(options):
            reason = "a row or snapshot filter"
        elif chosen is None:
            reason = None
        elif follows and bridge.buffered_logging():
            reason = "a database with buffered logging"
        else:
            return list(chosen[1]), list(chosen[2])
        if named:
            # 433: The index-ordered snapshot does not support {} on '{}'; set snapshot.scan=cursor
            # to read the table without index order
            raise InformixError(_m(433, reason, table.exposed_name))
        if not follows:
            # A merged target takes the overlap out by its key: it pages by the key as before.
            return None
        if own_key:
            # 492: Informix snapshot of '{}', which lands as a change log, pages by its key
            # without following its changes because it has {}; a row that changes during
            # the snapshot can then be in its change log twice
            logging.getLogger(__name__).warning(
                _m(492, table.exposed_name, reason or "no key or index of a type it can page by")
            )
            return None
        # 437: Informix snapshot of '{}', which has no key of its own, reads without index
        # order because it has {}; a row that changes during the snapshot can then be in
        # its change log twice, or not at all
        logging.getLogger(__name__).warning(
            _m(437, table.exposed_name, reason or "no index the snapshot can page by")
        )
        return None

    # ---- progress of an index-ordered snapshot ------------------------------------------
    #
    # The scan stages a page at a time and publishes its manifest only at the end, so a
    # failure before that left nothing to carry on from: the next attempt read the table
    # from its first row. The scan now writes, beside the staged pages:
    #
    #   snapshot-scan/<pipeline>.json                         which update's scan is under way
    #   snapshot-scan/<scope>/<schema>/<snapshot LSN>/head.json   the index and the rows' time
    #   .../<position>.json                                    one per page read, in order
    #
    # A restart under the same update, or a later update that was not started as a full
    # refresh, takes the scan up after the last page recorded: it runs under the first
    # update's scope, as a snapshot that is already being served in a later update does,
    # reads the table's changes again from the snapshot LSN and goes on from the last index
    # value. A full refresh, an update the pipelines API says nothing about, a record that
    # is too old or one whose log is gone discards the record instead, so that no later
    # update takes those pages up.

    def _scan_progress_root(self, table) -> str:
        connection_key, table_key = self._table_state_keys(table)
        return os.path.join(
            self._snapshot_staging_location, connection_key, table_key, "snapshot-scan"
        )

    def _scan_pointer_path(self, table, scope: str) -> str:
        return os.path.join(self._scan_progress_root(table), f"{scope.partition('_@_')[0]}.json")

    def _scan_progress_directory(self, table, scope: str, schema_id: str, boundary: int) -> str:
        return os.path.join(self._scan_progress_root(table), scope, schema_id, str(boundary))

    @staticmethod
    def _read_progress_file(path: str) -> dict[str, Any] | None:
        # A dropped mount is not a missing record: read as one, it discarded a scan's
        # progress or cut its resume short. It goes to the read's mount retry instead.
        try:
            with open(path, encoding="utf-8") as handle:
                value = json.load(handle)
        except (OSError, ValueError) as error:
            if _dropped_mount_error(error) is not None:
                raise
            return None
        return value if isinstance(value, dict) else None

    @staticmethod
    def _progress_file_exists(path: str) -> bool:
        # Not os.path.exists, which reads a dropped mount as a missing file.
        try:
            os.stat(path)
        except FileNotFoundError:
            return False
        return True

    @classmethod
    def _latest_progress_file(cls, directory: str) -> str:
        # Docstring: The file of the last page recorded in ``directory``, else its head.
        #
        # Pages are recorded in order with no gaps, so the last is found by probing: a
        # scan of thousands of pages costs a few dozen lookups on the Volume, not one for
        # every file.

        def page(number: int) -> str:
            return os.path.join(directory, f"{number:08d}.json")

        exists = cls._progress_file_exists
        if not exists(page(0)):
            return os.path.join(directory, "head.json")
        low, high = 0, 1
        while exists(page(high)):
            low, high = high, high * 2
        while high - low > 1:
            middle = (low + high) // 2
            if exists(page(middle)):
                low = middle
            else:
                high = middle
        return page(low)

    @staticmethod
    def _write_progress_file(path: str, value: dict[str, Any]) -> None:
        # Its directory is made when the scan begins: a page then costs a write and a rename.
        candidate = f"{path}.{secrets.token_hex(8)}"
        with open(candidate, "w", encoding="utf-8") as handle:
            json.dump(value, handle, sort_keys=True)
        os.replace(candidate, path)

    def _discard_scan_progress(self, table, scope: str, why: str | None = None) -> None:
        # Docstring: Remove the record of the scan under way for this pipeline, with its
        # pages' records, so that no update takes it up.

        path = self._scan_pointer_path(table, scope)
        pointer = self._read_progress_file(path)
        if pointer is None:
            return
        recorded = pointer.get("scope")
        # Missing alone counts as removed: under a dropped mount the record stayed for a
        # later update to take up, so the failure goes to the caller.
        if isinstance(recorded, str) and _PIPELINE_SCOPE.fullmatch(recorded) is not None:
            _remove_tree(os.path.join(self._scan_progress_root(table), recorded))
        with contextlib.suppress(FileNotFoundError):
            os.remove(path)
        if why:
            # 440: Informix snapshot of '{}' discards the index-ordered scan an
            # earlier update of this pipeline began ({}); its staged pages are not
            # used
            logging.getLogger(__name__).info(_m(440, table.exposed_name, why))

    def _adopted_scan_scope(self, table, scope: str, options: dict[str, str]) -> str:
        # Docstring: The scope a snapshot with no checkpoint runs under: that of an earlier
        # update of this pipeline whose index-ordered scan it continues, else its own.
        #
        # Taken only where the scan can go on from what it recorded: under that scope the
        # snapshot reads that update's initialization, which names the table's layout then,
        # so a changed layout failed every restart (316); and a scan that cannot resume
        # (another index, a cursor scan, a byte limit) started again beside the pages that
        # one staged, and on a quiet log at the same boundary met them (218).

        pointer = self._read_progress_file(self._scan_pointer_path(table, scope))
        recorded = None if pointer is None else pointer.get("scope")
        if pointer is None or recorded == scope:
            return scope
        directory = None
        why = None
        if self._update_request is None:
            why = "the pipelines API does not say what this update was started for"
        elif self._update_request != "continue":
            why = "this update is a full refresh"
        elif (
            pointer.get("version") != _SCAN_PROGRESS_VERSION
            or pointer.get("table") != table.native_identity
            or not isinstance(recorded, str)
            or _PIPELINE_SCOPE.fullmatch(recorded) is None
            or recorded.partition("_@_")[0] != scope.partition("_@_")[0]
        ):
            why = "its record is not one this version reads"
        else:
            try:
                directory = self._scan_progress_directory(
                    table, recorded, str(pointer["schema_id"]), int(pointer["boundary"])
                )
                touched = os.path.getmtime(self._latest_progress_file(directory))
                head = self._read_progress_file(os.path.join(directory, "head.json")) or {}
                initialization = self._read_immutable_head(
                    self._immutable_namespace(table, "initialization", recorded)
                )
                layout = ((initialization or {}).get("schema") or {}).get("fingerprint")
                index = self._scan_index(table, options)
                if layout != _schema_fingerprint(table):
                    why = "the table's columns or options have changed since"
                elif index is None or [head.get("keys"), head.get("descending")] != [
                    list(part) for part in index
                ]:
                    why = "the index it paged by is not the one this snapshot pages by"
                elif self._table_int_option(options, "snapshot.max.bytes", 0, minimum=0):
                    why = "a snapshot with snapshot.max.bytes set does not go on from one"
                elif time.time() - touched > min(
                    _SCAN_RESUME_SECONDS,
                    # Within half the staging retention: the sweep takes pages older than
                    # it, and at one day took those of a scan being taken up.
                    self._snapshot_staging_retention_seconds / 2
                    if self._snapshot_staging_retention_seconds > 0
                    else _SCAN_RESUME_SECONDS,
                ):
                    why = "it was last written to too long ago"
                elif self._bridge.minimum_lsn() > int(pointer["initial"]):
                    # That update's capture began there, and its changes are read again.
                    why = "the log it began in is no longer kept"
            except (KeyError, OSError, TypeError, ValueError) as error:
                if _dropped_mount_error(error) is not None:
                    raise
                why = "its record cannot be read"
        if why is None:
            return recorded
        self._discard_scan_progress(table, scope, why)
        return scope

    def _scan_resume_state(self, table, scope, schema_id, high_water, index):
        # Docstring: What an index-ordered scan under ``scope`` has recorded, for the scan to
        # go on from, or None when there is nothing it can go on from.

        pointer = self._read_progress_file(self._scan_pointer_path(table, scope))
        if (
            pointer is None
            or pointer.get("version") != _SCAN_PROGRESS_VERSION
            or pointer.get("scope") != scope
            or pointer.get("schema_id") != schema_id
            or pointer.get("table") != table.native_identity
        ):
            return None
        try:
            boundary = int(pointer["boundary"])
            directory = self._scan_progress_directory(table, scope, schema_id, boundary)
            head = self._read_progress_file(os.path.join(directory, "head.json")) or {}
            keys, descending = index
            if (
                boundary < high_water
                or head.get("keys") != list(keys)
                or head.get("descending") != list(descending)
                # The changes since the snapshot LSN are read again: they must still be there.
                or self._bridge.minimum_lsn() > boundary
            ):
                return None
            pages = []
            staged = rows = 0
            while True:
                entry = self._read_progress_file(os.path.join(directory, f"{len(pages):08d}.json"))
                if entry is None:
                    break
                if entry["index"] is not None:
                    if int(entry["index"]) != staged:
                        return None
                    staged += 1
                rows += int(entry["rows"])
                pages.append(entry)
                if entry["bound"] is None:
                    break
        except (KeyError, TypeError, ValueError):
            return None
        # A page staged after the last one recorded is read again: its file gives way, and
        # so do any after it, or staging that page again conflicted with what was there.
        runs = os.path.join(
            self._snapshot_stage_namespace(table, scope, schema_id), "runs", str(boundary)
        )
        try:
            names = os.listdir(runs)
        except FileNotFoundError:
            names = []
        for name in names:
            if name.startswith("page-") and name[5:].isdigit() and int(name[5:]) >= staged:
                # Not shutil.rmtree ignoring errors, which left the page on a dropped mount,
                # and staging it again then conflicted (218): the mount retry takes it.
                with contextlib.suppress(FileNotFoundError):
                    PurePythonInformixBridge._remove_connection_slot_tree(os.path.join(runs, name))
        # 439: Informix snapshot of '{}' continues the index-ordered scan an earlier
        # update of this pipeline began: {} staged pages with {} rows are kept
        logging.getLogger(__name__).info(_m(439, table.exposed_name, staged, rows))
        return {
            "boundary": boundary,
            "commit_time": head.get("commit_time"),
            "pages": pages,
            "staged": staged,
            "rows": rows,
        }

    def _scan_progress_writer(self, table, scope, schema_id, index, commit_time, initial):
        # Docstring: ``(begin, page)``: record that a scan under ``scope`` began at a
        # snapshot LSN, and each page it has read. A failure to record costs the scan
        # nothing but its restart point, and is reported once.

        keys, descending = index
        failed = []

        def record(path, value):
            if failed:
                return
            try:
                self._write_progress_file(path, value)
            except OSError as error:
                failed.append(error)
                # 441: Cannot record the progress of the index-ordered snapshot of
                # '{}' ({}); a restart reads the table from its first row again
                logging.getLogger(__name__).warning(_m(441, table.exposed_name, error))

        def begin(boundary: int) -> None:
            directory = self._scan_progress_directory(table, scope, schema_id, boundary)
            try:
                _remove_tree(directory)
            except OSError as error:
                # Its pages left beside this scan's, a resume took them for its own.
                failed.append(error)
                # 441: Cannot record the progress of the index-ordered snapshot of
                # '{}' ({}); a restart reads the table from its first row again
                logging.getLogger(__name__).warning(_m(441, table.exposed_name, error))
                return
            try:
                os.makedirs(directory, mode=0o700, exist_ok=True)
            except OSError:
                pass  # the write below reports it
            record(
                os.path.join(directory, "head.json"),
                {
                    "commit_time": commit_time,
                    "descending": list(descending),
                    "keys": list(keys),
                    "table": table.native_identity,
                },
            )
            record(
                self._scan_pointer_path(table, scope),
                {
                    "boundary": boundary,
                    "initial": initial,
                    "schema_id": schema_id,
                    "scope": scope,
                    "table": table.native_identity,
                    "version": _SCAN_PROGRESS_VERSION,
                },
            )

        def page(boundary: int, position: int, entry: dict[str, Any]) -> None:
            directory = self._scan_progress_directory(table, scope, schema_id, boundary)
            record(os.path.join(directory, f"{position:08d}.json"), entry)

        return begin, page
