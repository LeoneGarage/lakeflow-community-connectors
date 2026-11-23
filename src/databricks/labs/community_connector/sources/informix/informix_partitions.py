"""Partitions, delete reads, the retry offsets of capacity, Volume and schema waits, and key
options.
"""

from __future__ import annotations

import contextlib
import json
import logging
import math
import random
import time
from dataclasses import replace
from typing import Any, Callable, Iterable, Iterator, Sequence

from databricks.labs.community_connector.sources.informix.informix_bridge import (
    PurePythonInformixBridge,
)
from databricks.labs.community_connector.sources.informix.informix_filters import (
    _TRANSFORM_HASH_KEY_OPTION,
    _apply_column_selection,
    _column_renames,
    _column_selection,
    _column_transforms,
    _leave_out_unsupported_columns,
)
from databricks.labs.community_connector.sources.informix.informix_host_sharing import (
    _cdc_streamable,
    _database_encoding,
    _options_set,
)
from databricks.labs.community_connector.sources.informix.informix_keys import (
    _ALLOW_NULLABLE_INDEX_OPTION,
    _PRIMARY_KEYS_OPTION,
    _PRIMARY_KEYS_WILDCARD,
    _append_only_value,
    _key_override,
    _moved_rows_of_page,
    _option_bool,
    _without_moved_rows,
)
from databricks.labs.community_connector.sources.informix.informix_offsets import (
    _backlog_rank,
    _backlog_rank_value,
    _backlog_streak,
    _capacity_pressure,
    _capacity_retry_count,
    _checked_advisory_fields,
    _dropped_mount_retry_count,
    _schema_transition_retry_count,
    _shared_state_retry_count,
    _trigger_boundary_retry_count,
    _unl_position,
    _validated_offset,
)
from databricks.labs.community_connector.sources.informix.informix_options import (
    _APPEND_DELETE_TOMBSTONES_OPTION,
    _APPEND_INGESTION_OPTION,
    _CAPACITY_ATTEMPT_BASE_SECONDS,
    _CAPACITY_ATTEMPT_PER_MISS_SECONDS,
    _CAPACITY_ATTEMPT_PER_RANK_SECONDS,
    _CAPACITY_RETRY_MAX_RETRIES,
    _CAPACITY_RETRY_MIN_DELAY_SECONDS,
    _DEFAULT_CAPACITY_RETRY_MAX_DELAY_SECONDS,
    _DROPPED_MOUNT_MAX_RETRIES,
    _HANDOFF_CHANNEL_DELETE,
    _MAX_EMBEDDED_PARTITION_BYTES,
    _OFFSET_ADVISORY_FIELDS,
    _REPLAY_CONNECTION_WAIT_BUDGET_OPTION,
    _REPLAY_STOP_LSN_OPTION,
    _SHARED_STATE_ACCESS_MAX_RETRIES,
    _TRANSIENT_VOLUME_IN_PROCESS_ATTEMPTS,
    _TRANSIENT_VOLUME_IN_PROCESS_MAX_SECONDS,
    InformixError,
    _m,
)
from databricks.labs.community_connector.sources.informix.informix_registration import (
    _CAPACITY_ATTEMPT_MAX_SECONDS,
    Column,
    ConnectionCapacityUnavailable,
    SchemaTransitionUnavailable,
    SharedStateAccessUnavailable,
    Table,
    _range_emits_nothing,
    _replay_between_offsets,
)
from databricks.labs.community_connector.sources.informix.informix_shards import (
    _ensure_excepted_columns_exist,
    _spilled_page_rows,
    _SpilledRows,
    _unwire_json_table,
    _wildcard_key,
)
from databricks.labs.community_connector.sources.informix.informix_transactions import (
    _dropped_mount_error,
    _renamed_rows,
    _transformed_rows,
)
from databricks.labs.community_connector.sources.informix.informix_unload import (
    _descriptor_columns,
    _partition_column,
)


class _PartitionMixin:
    # Docstring: Partitions, delete reads, the retry offsets of capacity, Volume and schema
    # waits, and key options.

    # pylint: disable-next=too-many-locals
    def _served_partitions(
        self,
        table: Table,
        table_options: dict[str, str],
        start_offset: dict | None,
        end_offset: dict | None,
        effective_start: dict,
    ) -> list[dict] | None:
        # Docstring: The partitions of a ``.unl`` split window or staged page batch, else None.
        #
        # Run through _plan_with_mount_retry: like latest_offset's plan it reads only the
        # Volume and Lakebase, so a dropped mount is retried in place. The embedded replay in
        # get_partitions stays outside, as a real read that handles the mount itself.

        if self._unl_snapshot_serve(table_options, effective_start, table.identity, table):
            # File source: fan this microbatch's split window across executors -- one
            # unl_split descriptor per byte split in [start_split, start_split+window). The
            # boundary was pinned by latest_offset; take snapshot_lsn from end_offset and
            # the resume point from the start offset's split index (0 on the first read).
            snapshot_lsn = int((end_offset or {})["commit_lsn"])
            # A batch re-planned after a restart has had no latest_offset to check drift
            # (_served_offset): checked here, a first window by the fingerprint its end pinned.
            table = self._served_snapshot_table(table, effective_start, end_offset)
            start_split, start_record = _unl_position(effective_start)
            splits = self._unl_all_splits(table, table_options)
            end_split, end_record = _unl_position(end_offset or {}, terminal=len(splits))
            # The window Spark logged decides, not the gzip record-window option as it is now
            # (a re-plan after a restart may see it changed): a record window ends inside its
            # file or finishes it, and executors serve a window of several files.
            driver_window = (end_split == start_split and end_record > start_record) or (
                self._unl_gzip_driver_window(table_options, splits, start_split, start_record)
                and end_split <= start_split + 1
            )
            # A batch re-planned after a restart reaches here without its latest_offset, so
            # the extract is checked against its pinned manifest here as well.
            end_checkpoint = _validated_offset(end_offset or {})
            window_paths = (
                # Within the splits: files removed since make the check below fail (269).
                {splits[start_split][0]}
                if driver_window and start_split < len(splits)
                else {split[0] for split in splits[start_split:end_split]}
            )
            self._verify_unl_extract(
                table,
                table_options,
                # Where the serve pinned the extract: the start's scopes, or for the first
                # window the end's, written by the latest_offset that pinned it.
                self._offset_scopes(effective_start or end_checkpoint)[1],
                str(end_checkpoint["schema_id"]),
                snapshot_lsn,
                window_paths,
            )
            if driver_window:
                encoded = self._partition_embedded_rows.pop(
                    self._partition_offset_key(start_offset), None
                )
                if encoded is None:
                    # Re-planning an uncommitted batch (e.g. after a driver restart):
                    # replay exactly the records between start and the logged end.
                    rows = self._replay_unl_gzip_window(
                        table,
                        table_options,
                        snapshot_lsn,
                        splits[start_split],
                        start_record,
                        stop_record=end_record if end_split == start_split else None,
                    )
                    encoded = self._encode_embedded_output(
                        rows, table_options, lambda: table.columns
                    )
                return self._embedded_descriptors(table, encoded)
            # The window is whatever latest_offset logged: up to pages-per-batch splits,
            # ending early before a gzip split that is served in record windows.
            return self._unl_split_descriptors(
                table,
                table_options,
                snapshot_lsn,
                start_split=start_split,
                window=end_split - start_split,
                all_splits=splits,
            )
        batch = self._snapshot_page_batch(table, effective_start, table_options)
        cached = self._partition_page_batches.pop(self._partition_offset_key(start_offset), None)
        checked_by = effective_start
        if batch is None and cached is None:
            batch = self._bootstrap_page_batch(
                table, effective_start, end_offset or {}, table_options
            )
            # The first batch re-planned after a restart (or a resnapshot's): checked by the
            # fingerprint its end pinned, not its start's.
            checked_by = {}
        if batch is not None:
            # As for a .unl window. A batch its own read just staged (cached) needs no check.
            table = self._served_snapshot_table(table, checked_by, end_offset)
        batch = batch or cached
        if batch is not None:
            scope, schema_id, snapshot_lsn, start_page, end_page, page_count = batch
            # The pages Spark logged for the batch: a re-plan after a restart computes its
            # window afresh, and pages.per.batch may have changed since.
            logged = (end_offset or {}).get("snapshot")
            if (end_offset or {}).get("phase") == "stream":
                end_page = page_count
            elif isinstance(logged, dict) and str((end_offset or {}).get("snapshot_lsn")) == str(
                snapshot_lsn
            ):
                end_page = min(int(logged.get("page_index", end_page)), page_count)
            # The rows an index-ordered scan read again are left out of the pages: an
            # executor has no manifest, so a page's descriptor names those it leaves out.
            noted = self.__dict__.get("_moved_rows", {})
            lookup = (table.identity, scope, schema_id, int(snapshot_lsn))
            if lookup in noted:
                moved = noted[lookup]
            else:
                # A batch whose manifest was read elsewhere: read it here. Without it
                # the pages cannot be told to leave their moved rows out.
                manifest = self._read_snapshot_stage_manifest(table, scope, schema_id)
                if manifest is None or int(manifest["snapshot_lsn"]) != int(snapshot_lsn):
                    # 228: Invalid staged snapshot manifest for '{}'
                    raise InformixError(_m(228, table.exposed_name))
                moved = self._note_moved_rows(table, scope, schema_id, manifest)
            descriptors = []
            for page_index in range(start_page, end_page):
                left_out = _moved_rows_of_page(moved, page_index)
                descriptors.append(
                    {
                        "kind": "page",
                        "native_identity": table.native_identity,
                        "exposed_name": table.exposed_name,
                        "pipeline_scope": scope,
                        "schema_id": schema_id,
                        "snapshot_lsn": snapshot_lsn,
                        "page_index": page_index,
                        **({"moved": left_out} if left_out else {}),
                        **self._transform_descriptor_fields(table),
                    }
                )
            return descriptors
        return None

    def get_partitions(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
        end_offset: dict | None = None,
    ) -> Sequence[dict]:
        table_options = _options_set(table_options)
        if start_offset is None and end_offset is None:
            # Batch read. Not partitioned here -- raising makes LakeflowBatchReader
            # fall back to read_table (a single non-partitioned batch).
            # 175: Informix does not support partitioned batch reads
            raise NotImplementedError(_m(175))
        self._scope_from_checkpoint(start_offset)
        if start_offset == end_offset or _range_emits_nothing(start_offset, end_offset):
            # The staged pages and .unl splits a plan serves come before the replay below,
            # which would emit nothing for such a range too.
            return []
        # A metadata lookup, as in latest_offset: no capacity wait if the table is not cached
        # (a re-plan after a restart), and the session it opened is given back even when it
        # fails. An embedded replay below takes its own.
        with self._metadata_access():
            table = self._table(table_name, table_options)
        effective_start = self._effective_start_offset(start_offset or {})
        served = self._plan_with_mount_retry(
            self._served_partitions, table, table_options, start_offset, end_offset, effective_start
        )
        if served is not None:
            return served
        encoded = self._partition_embedded_rows.pop(self._partition_offset_key(start_offset), None)
        if encoded is None:
            # F2: a cache miss (Spark re-planning an uncommitted batch after a driver
            # restart) recomputes the read. The recompute must land on exactly the
            # ``end`` Spark already logged: it commits that offset regardless of what
            # the fresh read reaches, so an under-read -- a direct poll bounded by
            # cdc.max.records while the shared daemon is not ready yet, or a daemon
            # buffer that does not yet cover this table -- would silently drop every
            # row between the two. Bound the read at ``end`` and fail closed on a
            # shortfall, the same contract the simple reader's readBetweenOffsets
            # enforces, rather than trusting the recompute to reproduce the batch.
            rows = self._replay_embedded_range(
                table_name, start_offset or {}, end_offset or {}, table_options
            )
            encoded = (
                rows
                if isinstance(rows, _SpilledRows)
                else self._encode_embedded_output(rows, table_options, lambda: table.columns)
            )
        if isinstance(encoded, _SpilledRows):
            return encoded.descriptors(self._transform_descriptor_fields(table))
        return self._embedded_descriptors(table, encoded)

    def _replay_embedded_range(
        self, table_name: str, start: dict, end: dict, table_options: dict[str, str]
    ) -> Iterator[dict]:
        # Docstring: Recompute an embedded microbatch bounded by its committed ``end`` offset.
        #
        # The replay bounds are injected into a private copy of the table options, so
        # the read stops at ``end.commit_lsn`` (and, mid-incremental-copy, at
        # ``end.incremental.last_pk``) and the reached offset is verified before any
        # row is returned; the copy keeps the bound from leaking into later reads.

        options = dict(table_options)
        return _replay_between_offsets(
            start,
            end,
            lambda replay_start: self._embedded_read(table_name, replay_start, options),
            options,
            self.options,
        )

    def read_partition(
        self, table_name: str, partition: dict, table_options: dict[str, str]
    ) -> Iterator[dict]:
        table_options = _options_set(table_options)
        rows = self._read_partition_rows(partition, table_options)
        return self._output_rows(
            rows,
            table_options,
            lambda: _descriptor_columns(partition),
            # Embedded rows were transformed on the driver (see _encode_embedded_output).
            transform=not partition.get("transformed"),
        )

    def _output_rows(
        self,
        rows: Iterator[dict],
        table_options: dict[str, str],
        columns: Callable[[], Iterable[Column]] | None = None,
        *,
        transform: bool = True,
    ) -> Iterator[dict]:
        # Docstring: Rows as they leave the connector: transformed, then renamed.
        #
        # ``columns`` yields the table's columns on demand. A transform needs them only when
        # NVL or COALESCE replaces a NULL in a numeric column (see _fallback_value), so the
        # lookup -- the reader's cached table, or descriptor metadata on an executor -- waits
        # until then.

        if transform:
            rows = self._transformed_output(rows, table_options, columns)
        return _renamed_rows(rows, _column_renames(table_options))

    def _transformed_output(
        self,
        rows: Iterator[dict],
        table_options: dict[str, str],
        columns: Callable[[], Iterable[Column]] | None,
    ) -> Iterator[dict]:
        # Docstring: ``rows`` with transform_columns applied (source names).

        transforms = _column_transforms(table_options)
        if not transforms:
            return rows
        key = self.options.get(_TRANSFORM_HASH_KEY_OPTION) or table_options.get(
            _TRANSFORM_HASH_KEY_OPTION
        )
        context = {
            "hash_key": key.encode("utf-8") if key else None,
            "encoding": self._source_encoding(table_options),
            "columns": columns,
            # The table options alone, as the schema reads them (see _number_for_column).
            "options": table_options,
        }
        return _transformed_rows(rows, transforms, context)

    def _encode_embedded_output(
        self,
        rows: Iterable[dict[str, Any]],
        table_options: dict[str, str],
        columns: Callable[[], Iterable[Column]],
    ) -> list[Any]:
        # Docstring: Rows for an embedded partition descriptor, transformed here on the driver.
        #
        # The descriptor travels from the driver through Spark to an executor. Encoded as
        # read, it carried the raw values transform_columns is there to keep out of Spark;
        # transformed here, it carries what the destination gets, and the executor only
        # renames (the descriptor says ``transformed``).

        return self._encode_embedded_rows(self._transformed_output(rows, table_options, columns))

    def _source_encoding(self, table_options: dict[str, str]) -> str:
        # Docstring: The database code set's Python encoding: the connection's DB_LOCALE, the
        # one the SQLI client and the CDC capture use. LENGTH counts bytes in it and a CHAR
        # operand is re-padded to its byte width, on the snapshot, file, and CDC paths alike.
        # DB_LOCALE is no table option, so ``table_options`` is not read.

        del table_options
        return _database_encoding(self.options)

    @staticmethod
    def _transform_descriptor_fields(table: Table) -> dict[str, Any]:
        # Docstring: Column metadata a partition descriptor carries so an executor can type a
        # transform's NVL fallback (see _fallback_value) without a catalog connection.

        if not table.column_transforms:
            return {}
        transformed = {name for name, _ in table.column_transforms}
        return {
            "transform_columns": [
                _partition_column(column) for column in table.columns if column.name in transformed
            ]
        }

    @staticmethod
    def _embedded_descriptor(_table: Table, encoded) -> dict[str, Any]:
        # Its rows are transformed already (see _encode_embedded_output).
        return {"kind": "embedded", "rows": encoded, "transformed": True}

    def _embedded_descriptors(self, table: Table, encoded: list[Any]) -> list[dict[str, Any]]:
        # Docstring: An embedded microbatch as descriptors of at most
        # _MAX_EMBEDDED_PARTITION_BYTES of row JSON each, in row order.

        if len(json.dumps(encoded)) <= _MAX_EMBEDDED_PARTITION_BYTES:
            return [self._embedded_descriptor(table, encoded)]
        chunks: list[list[Any]] = [[]]
        size = 0
        for row in encoded:
            row_size = len(json.dumps(row))
            if chunks[-1] and size + row_size > _MAX_EMBEDDED_PARTITION_BYTES:
                chunks.append([])
                size = 0
            chunks[-1].append(row)
            size += row_size
        return [self._embedded_descriptor(table, chunk) for chunk in chunks]

    def _read_partition_rows(
        self, partition: dict, table_options: dict[str, str]
    ) -> Iterator[dict]:
        kind = partition.get("kind")
        if kind == "embedded":
            # F1: reverse the staging-codec encoding get_partitions applied, so the
            # exact read_table rows (Decimal / bytes / etc.) are yielded to the
            # framework -- identical to what the simple reader produces.
            yield from self._decode_embedded_rows(partition.get("rows", []))
            return
        if kind == "page":
            # Executor side: read the staged page by identity only -- no Table and no
            # Informix catalog connection.
            _header, rows = self._stream_staged_page_for(
                partition["native_identity"],
                partition["exposed_name"],
                partition["pipeline_scope"],
                partition["schema_id"],
                int(partition["snapshot_lsn"]),
                int(partition["page_index"]),
            )
            yield from _without_moved_rows(rows, partition.get("moved"))
            return
        if kind == "unl_split":
            # Executor side: read one `.unl` byte split by descriptor only -- no Table
            # and no Informix connection; shape rows identically to the serial path.
            yield from self._read_unl_split(partition, table_options)
            return
        if kind == "spill":
            # Executor side: one staged page of a spilled transaction, shaped here by the
            # table the descriptor carries -- no catalog lookup and no Informix connection.
            yield from _spilled_page_rows(
                self._stream_staged_page_for,
                partition["part"],
                int(partition["page_index"]),
                partition["tx"],
                _unwire_json_table(partition["table"]),
                table_options,
                deletes=bool(partition["deletes"]),
                tombstones=bool(partition["tombstones"]),
                encoding=str(partition["encoding"]),
            )
            return
        # 176: Unknown Informix partition descriptor kind: {!r}
        raise InformixError(_m(176, kind))

    def read_table_deletes(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        table_options = _options_set(table_options)
        self._scope_from_checkpoint(start_offset)
        rows, offset = self._read_deletes(table_name, start_offset, table_options)

        def columns():
            return self._table(table_name, table_options).columns

        return self._output_rows(rows, table_options, columns), offset

    def _read_deletes(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        # Docstring: ``read_table_deletes`` under source column names.

        # The interface passes None on the first call of the first run, and a batch
        # read always does.
        start_offset = start_offset or {}
        effective_start = self._effective_start_offset(start_offset)
        snapshot_mode = self._snapshot_mode(table_options)
        # An uncheckpointed delete reader cannot do useful source work until its
        # paired upsert reader publishes the boundary it is actually using.  The
        # table-state key depends only on the configured native identity, so test
        # Lakebase before resolving Informix metadata: _table() opens Informix and
        # would otherwise let coordination waiters consume every scarce slot from
        # the upsert readers that must unblock them.
        # table.migration is excluded too: a delete cutover seeds from its own parked token
        # (see _read_handoff_deletes), which does not depend on the upsert boundary, so
        # it must reach the attempt rather than parking here to wait for that boundary.
        if (
            not effective_start
            and snapshot_mode not in {"initial_only", "recovery"}
            and not self._table_migration(table_options)
            and not self._upsert_channel_start_exists(table_name, table_options)
        ):
            if (
                # With a snapshot floor to start at, it need not wait (_delete_floor_seed).
                not self._delete_floor_recorded(table_name, table_options)
                # Nor in a whole full refresh, whose start it can take (_full_refresh_delete_seed).
                and not self._full_refresh_delete_start(table_options, snapshot_mode)
            ):
                return iter(()), self._schema_node_fallback_offset(start_offset)
        replaying = self._replay_stop_lsn(table_options) is not None
        replay_wait_budget = self._replay_connection_wait_budget(table_options)
        with self._capacity_attempt(
            start_offset, replaying=replaying, replay_wait_budget=replay_wait_budget
        ):
            return self._read_table_deletes_attempt(
                table_name, start_offset, effective_start, table_options, snapshot_mode
            )

    def _read_table_deletes_attempt(
        self,
        table_name: str,
        start_offset: dict,
        effective_start: dict,
        table_options: dict[str, str],
        snapshot_mode: str,
        attempt_index: int = 0,
    ) -> tuple[Iterator[dict], dict]:
        # See _read_table_attempt: re-runs this attempt after a transient
        # shared-state failure, bounded by attempt_index.
        def read_again():
            return self._read_table_deletes_attempt(
                table_name,
                start_offset,
                effective_start,
                table_options,
                snapshot_mode,
                attempt_index + 1,
            )

        try:
            try:
                migration = self._table_migration(table_options)
                if not migration and effective_start.get("handoff_seeded"):
                    # First post-Restore delete read still carrying the marker: drop the
                    # delete-channel token the cutover left behind, one-shot and best-effort
                    # (see _drop_orphaned_handoff_tokens).
                    self._drop_orphaned_handoff_tokens(
                        self._table(table_name, table_options), [_HANDOFF_CHANNEL_DELETE]
                    )
                    # One-shot, on a copy (see _read_table_attempt).
                    effective_start = {
                        k: v for k, v in effective_start.items() if k != "handoff_seeded"
                    }
                if migration:
                    # Capture parks the delete position and freezes; cutover seeds from it
                    # (keyed relocation keeps the delete channel) or, for the SCD1->append
                    # switch, there is no delete flow so this is never reached.
                    return self._read_handoff_deletes(
                        table_name, effective_start, table_options, start_offset
                    )
                elif snapshot_mode == "initial_only":
                    result = (iter(()), dict(effective_start))
                elif snapshot_mode == "recovery" and (
                    not effective_start or effective_start.get("phase") != "stream"
                ):
                    # 177: snapshot.mode=recovery requires an existing stream checkpoint
                    raise InformixError(_m(177))
                else:
                    result = self._read_table_deletes(
                        table_name,
                        effective_start,
                        table_options,
                        snapshot_mode=snapshot_mode,
                        bootstrap_offset=start_offset,
                    )
                return self._reset_retry_counts(result, start_offset)
            except (SharedStateAccessUnavailable, OSError) as error:
                return self._volume_failure_result(
                    error, read_again, table_options, attempt_index, start_offset
                )
            except SchemaTransitionUnavailable:
                if self._replay_stop_lsn(table_options) is not None:
                    raise
                return iter(()), self._schema_transition_retry_offset(start_offset)
            except ConnectionCapacityUnavailable:
                # Continuous always yields (bounded by the consecutive-miss cap);
                # only a triggered flow raises, where an exhausted wait is a
                # genuine end-of-data failure rather than transient contention.
                if self._trigger_available_now:
                    raise
                if self._replay_stop_lsn(table_options) is not None:
                    raise
                return iter(()), self._capacity_retry_offset(start_offset)
        finally:
            self._release_worker_connection()

    @staticmethod
    def _shared_state_retry_offset(start_offset: dict) -> dict:
        result = dict(start_offset)
        retry_count = _shared_state_retry_count(result)
        if retry_count >= _SHARED_STATE_ACCESS_MAX_RETRIES:
            # 178: Informix shared state exhausted its checkpoint retry limit
            raise SharedStateAccessUnavailable(_m(178))
        time.sleep(random.uniform(0.05, min(0.1 * (2 ** min(retry_count, 5)), 1.5)))
        result["shared_state_retry_count"] = retry_count + 1
        return result

    def _dropped_mount_retry_offset(self, start_offset: dict, error: OSError) -> dict:
        # Docstring: Yield an empty batch for a dropped Volume mount, bounded by a fail-loud cap.
        #
        # Kept separate from ``_shared_state_retry_offset`` because the two count
        # different things at different cadences: that one bounds a fast in-process
        # retry loop, this one bounds consecutive *microbatches*. Sharing a counter
        # would either let a dropped mount stall for ~100 minutes (see
        # ``_DROPPED_MOUNT_MAX_RETRIES``) or cut the in-process retries far too short.
        #
        # Yielding is still the right first response -- a remount settles in seconds
        # and the checkpoint is untouched -- but it must not be the *only* response.
        # A mount that never returns otherwise leaves every flow yielding empty
        # batches with the pipeline reporting RUNNING and committing zero-row Delta
        # versions on schedule, which is indistinguishable from an idle source. This
        # was observed in production taking all 60 flows offline for 13 minutes with
        # no ERROR or WARN anywhere in the event log; only the offset's retry count
        # revealed it. Failing at the cap turns that into a visible flow failure.
        #
        # ``_reset_retry_counts`` drops the key on the next read that reaches the
        # Volume, so the count only ever measures a current run of consecutive
        # failures and a mount that flaps does not accumulate toward the cap.

        result = dict(start_offset)
        retry_count = _dropped_mount_retry_count(result)
        if retry_count >= self._dropped_mount_max_retries():
            # 179: Informix snapshot staging Volume mount was not connected for {} consecutive
            # reads; this flow yielded empty batches throughout and replicated nothing. The Volume's
            # FUSE mount is down rather than the source being idle: check the Volume's availability.
            logging.getLogger(__name__).error(
                _m(179, retry_count),
            )
            # 180: Informix snapshot staging Volume mount has not been connected for {} consecutive
            # reads; the staging Volume is unavailable
            raise InformixError(_m(180, retry_count)) from error
        result["dropped_mount_retry_count"] = retry_count + 1
        return result

    def _dropped_mount_max_retries(self) -> int:
        # Docstring: Consecutive dropped-mount yields tolerated before the flow fails.
        #
        # Configurable because the right bound depends on the microbatch interval:
        # the cap counts reads, not seconds, so a pipeline with a long trigger
        # interval covers the same wall-clock outage in fewer retries. Defaults to
        # ``_DROPPED_MOUNT_MAX_RETRIES``.

        value = int(
            self.options.get(
                "dropped.mount.max.retries",
                str(_DROPPED_MOUNT_MAX_RETRIES),
            )
        )
        if value < 1:
            # 181: Option 'dropped.mount.max.retries' must be >= 1
            raise ValueError(_m(181))
        return value

    def _capacity_retry_max_delay(self) -> float:
        # Docstring: Maximum jittered delay before a continuous reader retries after a miss.
        #
        # A positive value is the inter-read backoff and also caps each acquisition
        # attempt (a short, miss-scaled budget). ``0`` changes both for a continuous
        # flow: the acquisition attempt blocks the full ``connection.wait.timeout.seconds``
        # (maximum chance to claim a slot) and there is no inter-read delay before
        # the next attempt. A continuous flow still yields rather than fails on
        # exhaustion, bounded by ``capacity.retry.max.retries``. For a triggered
        # flow the value is irrelevant: it always blocks fully and raises on
        # exhaustion.

        value = float(
            self.options.get(
                "capacity.retry.max.delay.seconds",
                str(_DEFAULT_CAPACITY_RETRY_MAX_DELAY_SECONDS),
            )
        )
        if not math.isfinite(value) or value < 0:
            # 166: Option 'capacity.retry.max.delay.seconds' must be >= 0
            raise ValueError(_m(166))
        return value

    @staticmethod
    def _offset_is_incremental_reader(start_offset: dict) -> bool:
        # Docstring: Report whether this read is an in-progress incremental snapshot.
        #
        # The incremental strategy interleaves primary-key snapshot chunks with the
        # live CDC stream, so its offset carries ``phase == "stream"`` *and* an
        # ``incremental`` block while the copy is still in flight; the block is
        # dropped once the snapshot completes and the reader becomes pure CDC. This
        # window is the incremental *reader* phase, distinct from steady-state
        # stream even though both report ``phase == "stream"``.

        return bool(start_offset) and "incremental" in start_offset

    def _offset_is_stream_phase(self, start_offset: dict) -> bool:
        # Docstring: Report whether this read is steady-state CDC for capacity purposes.
        #
        # A first read has no checkpoint and begins with a snapshot; the blocking
        # ``snapshot`` phase carries ``phase != "stream"``. Only ``phase ==
        # "stream"`` is CDC. The incremental *reader* phase also reports ``phase ==
        # "stream"`` but is a bulk copy interleaved with CDC (see
        # :meth:`_offset_is_incremental_reader`); whether it counts as stream for
        # capacity is governed by ``snapshot.incremental.blocking`` -- when that is
        # true (the default) it is treated as non-stream so it blocks for a slot
        # like the consistent snapshot, and when false it is treated as stream and
        # uses the continuous yield budget. The capacity retry offset preserves both
        # ``phase`` and the ``incremental`` block across misses, so this stays
        # correct while a starved reader retries.

        if not start_offset or start_offset.get("phase") != "stream":
            return False
        if self._offset_is_incremental_reader(start_offset):
            return not self._incremental_snapshot_blocking()
        return True

    def _incremental_snapshot_blocking(self) -> bool:
        # Docstring: Whether the incremental snapshot reader phase blocks for a slot.
        #
        # When true (the default), an in-progress incremental snapshot read blocks
        # the full ``connection.wait.timeout.seconds`` to patiently claim a slot,
        # like the consistent snapshot -- bulk copying is latency-tolerant and
        # benefits from pressing for a slot rather than yielding empty. When false,
        # it uses the continuous miss-scaled yield budget and ``capacity.retry.max.delay.seconds``
        # inter-read backoff instead, freeing the worker between attempts at the
        # cost of a slower copy under contention. A triggered flow always blocks
        # regardless of this option.

        return _option_bool(self.options, "snapshot.incremental.blocking", True)

    def _effective_capacity_retry_delay(self, start_offset: dict) -> float:
        # Docstring: Per-read capacity retry delay, treating non-stream reads as ``0``.
        #
        # Snapshot reads (and, by default, the incremental snapshot reader phase)
        # are bulk, latency-tolerant work, so they should patiently wait out
        # contention for a slot: forcing the delay to ``0`` there makes each attempt
        # block the full ``connection.wait.timeout.seconds`` with no inter-read
        # backoff. Only the steady-state ``stream`` phase uses the configured
        # ``capacity.retry.max.delay.seconds``, where prompt yield-and-retry is the
        # right backpressure. Whether the incremental reader phase counts as stream
        # here is governed by ``snapshot.incremental.blocking`` (see
        # :meth:`_offset_is_stream_phase`). The phase override only applies to a
        # positive configured delay; a configured ``0`` already means "block fully
        # in every phase", so there is nothing to override.

        configured = self._capacity_retry_max_delay()
        if configured and not self._offset_is_stream_phase(start_offset):
            return 0.0
        return configured

    def _retried_result(self, result: tuple[Iterator[dict], dict], start_offset: dict):
        # Docstring: Return a retried read's result without disturbing its retry bookkeeping.
        #
        # A nested attempt may itself have given up and yielded, in which case its
        # offset carries the advisory counter that bounds the retry. Passing that
        # through ``_reset_retry_counts`` -- which exists to clear the counters after a
        # read that actually reached the source -- strips it, so the count oscillates
        # instead of climbing and the fail-loud caps can never be reached. Observed
        # directly while building this: successive reads reported 1, absent, 1, absent.

        _, end_offset = result
        if _OFFSET_ADVISORY_FIELDS & set(end_offset):
            return result
        return self._reset_retry_counts(result, start_offset)

    def _volume_failure_result(
        self,
        error: Exception,
        read_again: Callable[[], Any],
        options: dict[str, str],
        attempt_index: int,
        start_offset: dict,
        *,
        batch: bool = False,
    ) -> tuple[Iterator[dict], dict]:
        # Docstring: Answer a read whose shared-state Volume access failed.
        #
        # A dropped mount (ENOTCONN) can surface from any shared-state helper, not only the
        # one that retries open(2), and from that one wrapped in SharedStateAccessUnavailable
        # once its own retries run out, so it is classified by the error's cause chain. Any
        # other OSError is a real fault and keeps propagating. Both are retried in place
        # first: a remount settles in seconds, and yielding is unavailable during a replay
        # (see _retry_transient_volume_read). A dropped mount then yields under
        # dropped.mount.max.retries with a warning, anything else under the shared-state
        # retry limit.

        dropped = _dropped_mount_error(error)
        if dropped is None and not isinstance(error, SharedStateAccessUnavailable):
            raise error
        retried = self._retry_transient_volume_read(read_again, options, attempt_index)
        if retried is not None:
            return self._retried_result(retried, start_offset)
        if batch:
            # No checkpoint to yield to: an empty batch is an empty snapshot.
            raise error
        if dropped is None:
            return iter(()), self._shared_state_retry_offset(start_offset)
        # 173: Informix snapshot staging Volume mount is not connected; yielding an empty
        # batch and retrying this checkpoint
        logging.getLogger(__name__).warning(_m(173), exc_info=True)
        return iter(()), self._dropped_mount_retry_offset(start_offset, dropped)

    def _retry_transient_volume_read(
        self, attempt: Callable[[], Any], options: dict[str, str], attempt_index: int
    ):
        # Docstring: Re-run a read whose shared-state access failed transiently.
        #
        # Called from the handlers that would otherwise answer a dropped mount by
        # yielding an empty batch with an unchanged position. For a normal read that
        # is correct and cheap -- the framework calls again and nothing is lost. During
        # a replay it is silently destructive: the caller has already committed the
        # range's end offset and reads an empty batch as proof the range held nothing,
        # so the rows vanish. Six tables lost 151 rows this way.
        #
        # Retrying in place removes the dilemma whenever the mount returns, which in
        # production it did within seconds. If it never returns the two cases genuinely
        # differ, so they diverge: a normal read yields (this returns None and the
        # caller falls back), while a replay raises, because failing a batch Spark will
        # retry beats under-delivering one it will not.
        #
        # Performs one further attempt per call. The caller re-enters through the read
        # path, which calls back here on a repeat failure, so ``attempt_index`` bounds
        # the chain; returning None at the bound hands the decision back.

        if attempt_index + 1 >= _TRANSIENT_VOLUME_IN_PROCESS_ATTEMPTS:
            # 182: Informix shared state stayed inaccessible across {} in-process attempts
            logging.getLogger(__name__).warning(
                _m(182, _TRANSIENT_VOLUME_IN_PROCESS_ATTEMPTS),
            )
            if self._replay_stop_lsn(options) is not None:
                # 183: Informix shared state is inaccessible while replaying a committed offset
                # range; failing this batch rather than returning fewer rows than the range holds
                raise InformixError(_m(183))
            return None
        delay = min(0.25 * (2**attempt_index), _TRANSIENT_VOLUME_IN_PROCESS_MAX_SECONDS)
        time.sleep(random.uniform(delay / 2, delay))
        return attempt()

    @staticmethod
    def _replay_stop_lsn(options: dict[str, str]) -> int | None:
        # Docstring: The LSN a replayed read must stop at, or None for an ordinary read.
        #
        # Validated rather than trusted: the value reaches the read through the
        # options dict, and a malformed one would silently become an unbounded read --
        # exactly the behaviour being fixed.

        raw = options.get(_REPLAY_STOP_LSN_OPTION)
        if raw is None:
            return None
        try:
            stop = int(raw)
        except (TypeError, ValueError) as error:
            # 184: Option '{}' must be an integer LSN, got {!r}
            raise InformixError(_m(184, _REPLAY_STOP_LSN_OPTION, raw)) from error
        if stop < 0:
            # 185: Option '{}' must not be negative
            raise InformixError(_m(185, _REPLAY_STOP_LSN_OPTION))
        return stop

    @staticmethod
    def _replay_connection_wait_budget(options: dict[str, str]) -> float | None:
        raw = options.get(_REPLAY_CONNECTION_WAIT_BUDGET_OPTION)
        if raw is None:
            return None
        try:
            budget = float(raw)
        except (TypeError, ValueError) as error:
            # 186: Option '{}' must be a numeric duration, got {!r}
            raise InformixError(_m(186, _REPLAY_CONNECTION_WAIT_BUDGET_OPTION, raw)) from error
        if not math.isfinite(budget) or budget < 0:
            # 187: Option '{}' must be a finite non-negative duration
            raise InformixError(_m(187, _REPLAY_CONNECTION_WAIT_BUDGET_OPTION))
        return budget

    def _capacity_retry_max_retries(self) -> int:
        # Docstring: Consecutive capacity misses a continuous flow tolerates before failing.
        #
        # A continuous reader that never gets a slot yields empty batches, which is
        # indistinguishable from an idle source. This bound makes a permanently
        # undersized endpoint fail loudly instead of replicating nothing forever;
        # acquiring a slot resets the count, so it only ever measures a current run
        # of misses. Defaults to ``_CAPACITY_RETRY_MAX_RETRIES``.

        value = int(
            self.options.get(
                "capacity.retry.max.retries",
                str(_CAPACITY_RETRY_MAX_RETRIES),
            )
        )
        if value < 1:
            # 188: Option 'capacity.retry.max.retries' must be >= 1
            raise ValueError(_m(188))
        return value

    def _capacity_attempt_budget(
        self,
        start_offset: dict,
        *,
        replaying: bool = False,
        replay_wait_budget: float | None = None,
    ) -> float | None:
        # Docstring: Seconds this read may spend acquiring a slot, or None to block fully.
        #
        # The bounded budget is a continuous-mode concept: a continuous flow is a
        # permanent standing consumer, so instead of pinning a worker for the whole
        # ``connection.wait.timeout.seconds`` it spends a short budget, yields an
        # empty batch, and is called again. A triggered flow is the opposite -- it
        # drains and terminates, so it must block for the full wait to actually get
        # a slot, and only fail if that genuinely exhausts. It therefore never uses
        # the yield budget, regardless of ``capacity.retry.max.delay.seconds``
        # (which governs only the continuous yield/backoff and is irrelevant to a
        # triggered read). Returning a short budget to a triggered flow makes it
        # give up in under a second under contention and fail the update, which is
        # the bug this guard prevents.
        #
        # A replay also blocks for the full wait. Spark has already committed the
        # range's end and cannot accept an empty capacity-yield in its place, so a
        # short attempt only creates retry churn and increases the chance of repeated
        # failure under the same contention that caused the first miss.
        #
        # A continuous flow blocks fully whenever its *effective* delay is ``0``
        # (see :meth:`_effective_capacity_retry_delay`): either the configured
        # ``capacity.retry.max.delay.seconds`` is ``0``, or this read is a
        # non-stream (snapshot/incremental) read, which is treated as ``0`` so bulk
        # copying patiently waits out contention for a slot. Blocking fully gives
        # the read the maximum chance to claim a slot before giving up; it still
        # yields rather than fails when the wait exhausts (only a triggered flow
        # raises), bounded by ``capacity.retry.max.retries`` consecutive misses.
        # The guard is an ``or`` (triggered is *always* full-block, plus any read
        # whose effective delay is zero); the earlier bug used ``and`` and so let a
        # triggered flow with a positive delay fall through to the short budget.
        #
        # Otherwise (a continuous stream read with a positive delay) the budget
        # scales with ``capacity_pressure`` -- an acquisition-pressure signal that
        # rises on a miss and *decays* rather than resets on a served read -- so a
        # reader that has been contended presses harder than one that just arrived,
        # and a reader that is intermittently served keeps some of that earned
        # patience instead of dropping straight back to the least-patient budget on
        # every success. (This is deliberately distinct from ``capacity_retry_count``,
        # which counts strictly consecutive misses and resets on success because it
        # is the failure guard.) It is jittered so equally-aged readers do not
        # converge on one give-up instant and re-storm the Volume together on the
        # next round.

        if replaying:
            return replay_wait_budget
        if self._trigger_available_now or not self._effective_capacity_retry_delay(start_offset):
            return None
        pressure = _capacity_pressure(start_offset)
        # A flow that is further behind presses harder for the same pressure, so
        # scarce slots skew toward the readers with the most to catch up. The rank
        # is additive with pressure rather than multiplicative: it biases the
        # budget without letting one very stale flow monopolise the pool, and a
        # missing rank (hints disabled, unpublished, or stale) contributes zero,
        # which leaves this identical to the pre-hint behaviour.
        rank = _backlog_rank_value(start_offset)
        budget = min(
            _CAPACITY_ATTEMPT_BASE_SECONDS
            + pressure * _CAPACITY_ATTEMPT_PER_MISS_SECONDS
            + rank * _CAPACITY_ATTEMPT_PER_RANK_SECONDS,
            _CAPACITY_ATTEMPT_MAX_SECONDS,
        )
        return random.uniform(budget * 0.5, budget)

    @contextlib.contextmanager
    def _capacity_attempt(
        self,
        start_offset: dict,
        *,
        replaying: bool = False,
        replay_wait_budget: float | None = None,
    ):
        # Docstring: Publish this read's acquisition budget and rank, then restore both.
        #
        # The rank is published even when there is no budget. A blocking reader --
        # a triggered flow, or the latency-tolerant snapshot/incremental copy -- has
        # no budget by design, and it is exactly the reader that carries a proven
        # backlog while bulk-copying, so gating the rank on the budget left those
        # readers unprioritised (observed in production: ranks only ever appeared on
        # steady-state CDC offsets, streaks only on blocking copy offsets).

        budget = self._capacity_attempt_budget(
            start_offset,
            replaying=replaying,
            replay_wait_budget=replay_wait_budget,
        )
        # Take the stronger of the persisted rank and one derived from the streak.
        # Only the capacity-miss path writes backlog_rank, so a *blocking* reader's
        # offset carries a streak but no rank -- reading the key alone would leave
        # every bulk-copy reader at zero, which is the gap this exists to close.
        rank = max(
            _backlog_rank_value(start_offset),
            _backlog_rank(0, _backlog_streak(start_offset)),
        )
        # This thread's, for the bridge to read (_call_context).
        context = self._call_state()
        previous = getattr(context, "attempt_budget", None)
        previous_rank = getattr(context, "attempt_rank", 0)
        if budget is not None:
            context.attempt_budget = budget
        if rank > 0:
            context.attempt_rank = rank
        try:
            yield
        finally:
            context.attempt_budget, context.attempt_rank = previous, previous_rank

    def _capacity_retry_offset(self, start_offset: dict) -> dict:
        # Docstring: Yield an empty continuous batch when no connection slot is free.
        #
        # The returned offset must differ from ``start_offset``: the framework calls
        # a reader repeatedly until the offset stops changing, so returning it
        # unchanged would signal exhaustion and stop the stream. Bumping the retry
        # count keeps the stream live, and ``_reset_retry_counts`` clears the key on
        # the next read that actually acquires a slot, so a reader that gets served
        # starts again from zero and the count only ever measures a current run of
        # consecutive misses.
        #
        # Only the terminal case is logged: individual retries are the expected
        # steady state, and logging each would bury the driver log.

        result = dict(start_offset)
        retry_count = _capacity_retry_count(result)
        if retry_count >= self._capacity_retry_max_retries():
            # 189: Informix connection capacity was unavailable for {} consecutive continuous reads;
            # this flow yielded empty batches throughout and replicated nothing. Raise
            # max.concurrent.connections, reduce the number of flows, or use a triggered pipeline.
            logging.getLogger(__name__).error(
                _m(189, retry_count),
            )
            # 190: Informix connection capacity exhausted its continuous retry limit; raise
            # max.concurrent.connections or reduce the number of flows
            raise ConnectionCapacityUnavailable(_m(190))
        # Non-stream reads use an effective delay of 0 (no inter-read backoff),
        # matching the full-block acquisition they get above. min() keeps a
        # configured delay below the floor -- notably 0, which means "block fully,
        # no spacing" -- from inverting the bounds.
        delay = self._effective_capacity_retry_delay(start_offset)
        time.sleep(random.uniform(min(_CAPACITY_RETRY_MIN_DELAY_SECONDS, delay), delay))
        result["capacity_retry_count"] = retry_count + 1
        # Pressure rises with every miss and feeds the acquisition budget. Unlike
        # the consecutive-miss count above it decays rather than resets on a
        # served read (see _reset_retry_counts), so an intermittently-served but
        # contended flow keeps pressing rather than snapping back to the base
        # budget on each success.
        result["capacity_pressure"] = _capacity_pressure(result) + 1
        # Carry the backlog rank so the next attempt of this flow can press harder
        # when it is badly behind, and so the value is visible in the offset that
        # every batch logs. Two signals feed it (see _backlog_rank): the published
        # hint, which is comparable across readers but over-states a quiet table's
        # lag because it is a global log position, and this flow's own run of reads
        # that ended at their row budget, which is first-hand proof that work
        # remains for *this* table. The streak survives a capacity miss because it
        # describes the backlog, not the attempt, and a miss reads nothing that
        # could have drained it.
        backlog = self._estimated_backlog(start_offset)
        streak = _backlog_streak(start_offset)
        if streak > 0:
            result["backlog_streak"] = streak
        if backlog is not None or streak > 0:
            result["backlog_rank"] = _backlog_rank(backlog or 0, streak)
        return result

    def _estimated_backlog(self, start_offset: dict) -> int | None:
        # Docstring: Estimate how far this flow trails the source, without a connection.
        #
        # A waiting reader cannot call ``current_lsn()`` -- that needs the very slot
        # it is waiting for -- so it subtracts its own checkpoint (which survives a
        # capacity yield inside the offset) from the log position a slot *holder*
        # published. Returns None whenever no trustworthy hint exists, so callers
        # keep their unprioritised behaviour rather than ranking on a guess.
        #
        # The published position is the server's global log position, so this
        # over-states the backlog of a quiet table: the log advances because *other*
        # tables are written. It measures staleness rather than true per-table lag,
        # which is why callers should compare it on a log scale rather than treat
        # the difference as a row count.

        try:
            checkpoint = start_offset.get("commit_lsn")
            if checkpoint is None:
                return None
            # Deliberately does NOT go through self._bridge_instance. A waiter is
            # precisely the reader that has no bridge: _release_worker_connection
            # clears the instance after every read so a closed bridge is never
            # retained, so requiring one here made this unreachable in production
            # (the rank never appeared in any offset). The hint depends only on
            # the endpoint identity, which comes from options.
            hint = PurePythonInformixBridge._read_connection_backlog_hint_at(self.options)
            if hint is None:
                return None
            return max(0, hint - int(checkpoint))
        except Exception:
            # Advisory signal: never let estimation failure affect a read.
            # 191: Could not estimate Informix backlog from hint
            logging.getLogger(__name__).debug(_m(191), exc_info=True)
            return None

    @staticmethod
    def _trigger_boundary_retry_offset(start_offset: dict) -> dict:
        result = dict(start_offset)
        retry_count = _trigger_boundary_retry_count(result)
        time.sleep(random.uniform(0.05, 0.2))
        result["trigger_boundary_retry_count"] = retry_count + 1
        return result

    @staticmethod
    def _schema_transition_retry_offset(start_offset: dict) -> dict:
        # Docstring: Yield an empty batch while the upsert reader publishes the transition.
        #
        # The offset must differ from ``start_offset`` or the framework reads the
        # unchanged offset as exhaustion and stops the stream, so bump a counter the
        # way the sibling coordination retries do. Unlike the capacity retry this
        # carries no retry cap: the wait is on the paired upsert reader for the
        # *same* table, which either publishes (this reader proceeds) or fails its
        # own flow with a real error. Capping the count would convert a slow schema
        # transition into a delete-channel failure while the upsert side is still
        # healthy, and the whole point of yielding is that the slot is free for that
        # publication to happen. A triggered update is bounded in time instead (see
        # _ensure_schema_transition_wait_bounded): there a failed upsert flow does not
        # end the update, and this reader would otherwise yield forever.

        result = dict(start_offset)
        retry_count = _schema_transition_retry_count(result)
        time.sleep(random.uniform(0.05, 0.2))
        result["schema_transition_retry_count"] = retry_count + 1
        return result

    @staticmethod
    def _effective_start_offset(start_offset: dict) -> dict:
        # Shared with _retried_result: a field that is bookkeeping here must be
        # bookkeeping there too, and duplicating the list let them drift.
        retry_fields = _OFFSET_ADVISORY_FIELDS
        if start_offset and set(start_offset) <= retry_fields:
            # Checked as _validated_offset checks them, which never sees them once dropped.
            _checked_advisory_fields(start_offset)
            return {}
        return start_offset

    def _reset_retry_counts(
        self, result: tuple[Iterator[dict], dict], start_offset: dict
    ) -> tuple[Iterator[dict], dict]:
        rows, end_offset = result
        if {
            "shared_state_retry_count",
            "dropped_mount_retry_count",
            "trigger_boundary_retry_count",
            "schema_transition_retry_count",
            "schema_node_fallback_retry_count",
            "capacity_retry_count",
            "capacity_pressure",
            "backlog_rank",
        } & set(start_offset):
            end_offset = dict(end_offset)
            end_offset.pop("shared_state_retry_count", None)
            # This read reached the Volume, so the mount is back: the fail-loud
            # guard must start over rather than carry a flapping mount's history
            # toward the cap.
            end_offset.pop("dropped_mount_retry_count", None)
            # Kept where this read raised it: a resnapshot wait (_resnapshot_retry_offset)
            # returns, not raises past this reset as the other waits do. Dropped, its count
            # went 1, absent, 1, and from a start of advisory fields alone the end was {},
            # a range a replay cannot take (1).
            if _trigger_boundary_retry_count(end_offset) <= _trigger_boundary_retry_count(
                start_offset
            ):
                end_offset.pop("trigger_boundary_retry_count", None)
            end_offset.pop("schema_transition_retry_count", None)
            # Deliberately NOT reset here, unlike every counter above. Those reset
            # because the read succeeded at the thing they counted failures of; a
            # bootstrap that still has no boundary has not succeeded and must keep
            # accumulating, or the count pins at 1 on every retry and the schema-node
            # fallback never becomes eligible. A read that *does* find a boundary
            # returns a freshly built _offset(), which never carries the counter, so
            # it cannot leak onto a real checkpoint either way.
            # Dropping the key resets the consecutive-miss failure guard: this
            # read got a slot, so the reader is no longer starved and the
            # fail-loud counter must start over.
            end_offset.pop("capacity_retry_count", None)
            # Pressure decays rather than resets: a served-but-contended reader
            # keeps some of the elevated budget it earned while starved so it does
            # not snap back to the base budget (and a likely immediate re-miss) on
            # every success. Halving reaches the base budget after a few
            # uncontended reads; drop the key once it decays to zero so a fully
            # recovered flow carries no residual state.
            pressure = _capacity_pressure(start_offset) // 2
            if pressure > 0:
                end_offset["capacity_pressure"] = pressure
            else:
                end_offset.pop("capacity_pressure", None)
            # The rank is a per-attempt observation, not accumulated state: this
            # read got a slot, so any bias it earned while waiting is spent. The
            # next miss re-derives it from a fresh hint.
            end_offset.pop("backlog_rank", None)
            # backlog_streak is deliberately NOT dropped here. It describes the
            # source, not the attempt: a served read has just measured whether it
            # could drain the log, and _with_backlog_streak has already set the
            # fresh value on this end offset (clearing it outright when the read
            # reached the log's end). Popping it would discard that measurement and
            # make a permanently backlogged flow look caught up on every read that
            # happened to get a slot.
        return rows, end_offset

    def _append_only_mode(self, table_options: dict[str, str]) -> str:
        # Docstring: Resolve ``append.only.ingestion`` to ``'true'``, ``'false'``, or ``'auto'``.
        #
        # ``table_options`` override the connection options; when the option is set
        # nowhere the mode defaults to ``'auto'``.

        for source in (table_options, self.options):
            if _APPEND_INGESTION_OPTION in source:
                return _append_only_value(source[_APPEND_INGESTION_OPTION])
        return "auto"

    def _append_delete_tombstones(self, table_options: dict[str, str]) -> bool:
        # Docstring: Resolve ``append.delete.tombstones``; default ``True``.
        #
        # ``table_options`` override the connection options, exactly like
        # ``append.only.ingestion``. Only consulted for a table on the append path: it
        # decides whether a source DELETE lands on the upsert channel as a ``d`` row
        # carrying the full before-image, or is dropped.

        if _APPEND_DELETE_TOMBSTONES_OPTION in table_options:
            return _option_bool(table_options, _APPEND_DELETE_TOMBSTONES_OPTION, True)
        return _option_bool(self.options, _APPEND_DELETE_TOMBSTONES_OPTION, True)

    def _append_only_table(self, table: Table, table_options: dict[str, str]) -> bool:
        # Docstring: Report whether this read should use the append-only path.
        #
        # ``true`` forces append for any capturable table (a caller may want append
        # semantics even for an insert-only keyed table); ``false`` forces the normal
        # CDC/snapshot path; ``auto`` (the default) appends only keyless tables and
        # leaves keyed tables on CDC. A table whose columns cannot be captured always
        # falls back to snapshot, since append cannot represent rows it cannot read.

        if not _cdc_streamable(table):
            return False
        mode = self._append_only_mode(table_options)
        if mode == "auto":
            return not table.primary_keys
        return mode == "true"

    def _apply_primary_key_override(
        self,
        table: Table,
        table_options: dict[str, str],
        override: tuple[list[str], tuple[str, ...]] | None = None,
    ) -> Table:
        # Docstring: Override a table's primary key with the connector ``primary.keys`` option.
        #
        # When set (per table), treat the table as keyed on those columns -- even a
        # physically keyless one -- so it is read as cdc_with_deletes with
        # keyset-paginated snapshots and reported with those keys (which the pipeline
        # also uses as the destination merge key). The caller MUST guarantee the
        # columns are unique for the row: a non-unique override corrupts upserts,
        # delete identification, and snapshot pagination.
        #
        # The override adopts the per-column direction of an index whose key columns
        # exactly match it (see ``_override_key_directions``), so an override that
        # names a mixed-direction index's columns still pages in that index's own
        # order rather than forcing an all-ascending sort. When no index matches,
        # paging stays ascending.
        #
        # ``"*"`` (or ``["*"]``) keys the table on every column in catalog order, nullable
        # ones included, and ``"* except(c1, c2)"`` on every column but those; see
        # ``_PRIMARY_KEYS_WILDCARD``. ``override`` is the option already parsed by
        # _key_override, which runs here when the caller didn't.

        override = _key_override(table_options) if override is None else override
        if override is None:
            return table
        keys, excepted = override
        wildcard = keys == [_PRIMARY_KEYS_WILDCARD]
        if wildcard:
            _ensure_excepted_columns_exist(table, excepted)
            keys = _wildcard_key(table.columns, excepted)
            if not keys:
                # 192: Option '{}' excepts every column of '{}', leaving no key
                raise InformixError(_m(192, _PRIMARY_KEYS_OPTION, table.exposed_name))
        column_names = {column.name for column in table.columns}
        missing = [key for key in keys if key not in column_names]
        if missing:
            # 193: Option '{}' names column(s) not present in '{}': {}
            raise InformixError(
                _m(193, _PRIMARY_KEYS_OPTION, table.exposed_name, (", ".join(missing)))
            )
        if tuple(keys) == table.primary_keys:
            # The override restates the current key; keep its directions (already
            # reflecting the key index) rather than looking them up again. It is now
            # owned by the override, not a nullable-index promotion.
            if (
                table.key_override
                and table.key_wildcard == wildcard
                and table.key_wildcard_except == excepted
            ):
                return table
            return replace(
                table,
                key_override=True,
                key_wildcard=wildcard,
                key_wildcard_except=excepted,
                key_from_nullable_index=False,
            )
        directions = self._override_key_directions(table.identity, tuple(keys))
        return replace(
            table,
            primary_keys=tuple(keys),
            key_override=True,
            key_wildcard=wildcard,
            key_wildcard_except=excepted,
            key_descending=tuple(directions or ()),
            key_from_nullable_index=False,
            key_only_from_option=table.key_only_from_option or not table.primary_keys,
        )

    def _override_key_directions(self, identity: str, keys: tuple[str, ...]) -> list[bool] | None:
        # Docstring: Memoized key-index direction lookup for a ``primary.keys`` override.
        #
        # The override is applied on every ``_table`` call (cache hits included), so
        # the catalog lookup must run at most once per table+key, not per microbatch.

        cache = self._override_direction_cache
        cache_key = (identity, keys)
        if cache_key not in cache:
            found = self._bridge.key_index_directions(identity, list(keys))
            cache[cache_key] = tuple(found) if found else ()
        cached = cache[cache_key]
        return list(cached) if cached else None

    def _apply_nullable_index_promotion(self, table: Table, table_options: dict[str, str]) -> Table:
        # Docstring: Promote a nullable-column UNIQUE index to the key when the table opts in.
        #
        # Applies only when the table is otherwise key-less (no primary key and no
        # all-NOT-NULL unique index qualified) and the per-table ``allow.nullable.index``
        # option is set. The operator thereby asserts the index's columns are unique and
        # non-null for every row. ``key_from_nullable_index`` marks the result so a schema
        # refresh preserves the promotion (the catalog re-derives it as key-less). An
        # explicit ``primary.keys`` override still wins -- it is applied afterward.

        if table.primary_keys or not table.nullable_index_key:
            return table
        if not _option_bool(table_options, _ALLOW_NULLABLE_INDEX_OPTION, False):
            return table
        return replace(
            table,
            primary_keys=table.nullable_index_key,
            key_descending=table.nullable_index_descending,
            key_from_nullable_index=True,
        )

    def _requested_key(
        self,
        table: Table,
        table_options: dict[str, str],
        override: tuple[list[str], tuple[str, ...]] | None,
    ) -> tuple[str, ...] | None:
        # Docstring: The key the table options will give ``table``, ahead of applying them.
        #
        # In _apply_key_options' precedence: the primary.keys list (``override``, from
        # _key_override), a nullable index the table opts into promoting, then the catalog
        # key. None for the primary.keys "*" wildcard, with or without except(...), which
        # keys on whatever columns are ingested.

        if override is not None:
            keys, _ = override
            return None if keys == [_PRIMARY_KEYS_WILDCARD] else tuple(keys)
        if (
            not table.primary_keys
            and table.nullable_index_key
            and _option_bool(table_options, _ALLOW_NULLABLE_INDEX_OPTION, False)
        ):
            return table.nullable_index_key
        return table.primary_keys

    def _ingested_columns(
        self,
        table: Table,
        table_options: dict[str, str],
        override: tuple[list[str], tuple[str, ...]] | None,
    ) -> Table:
        # Docstring: Narrow ``table`` to the columns it ingests: the column selection, then
        # the columns of a type the connector can't read left out, unless one is in its key.
        #
        # ``override`` is the primary.keys option from _key_override. Opens no Informix
        # connection, unlike the key options, which can query the override's index
        # directions.

        table = _apply_column_selection(table, _column_selection(table_options))
        return _leave_out_unsupported_columns(
            table, self._requested_key(table, table_options, override)
        )

    def _apply_key_options(
        self,
        table: Table,
        table_options: dict[str, str],
        override: tuple[list[str], tuple[str, ...]] | None = None,
    ) -> Table:
        # Docstring: Apply the per-table key options in precedence order: an explicit
        # ``primary.keys`` override wins over a nullable-index promotion, which wins
        # over the catalog-derived key. Promotion runs first so the override, applied
        # last, still takes precedence. ``override`` is as for _apply_primary_key_override.

        return self._apply_primary_key_override(
            self._apply_nullable_index_promotion(table, table_options), table_options, override
        )
