"""The connector's Lakeflow interface: lifecycle, discovery, read_table and the partitioned
reader's offsets.
"""

from __future__ import annotations

import contextlib
import functools
import itertools
import json
import logging
import random
import threading
import time
from typing import Any, Callable, Iterable, Iterator

from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from databricks.labs.community_connector.sources.informix.informix_bridge import (
    PurePythonInformixBridge,
)
from databricks.labs.community_connector.sources.informix.informix_filters import (
    _operand_functions,
    _row_filter_kind,
    _snapshot_filter,
    _snapshot_predicate,
    _spark_type,
    _transform_spark_type,
)
from databricks.labs.community_connector.sources.informix.informix_host_sharing import (
    _cdc_capable,
    _client_encoding,
    _DeferredStagedPage,
    _ensure_materializable,
    _options_set,
    _refuse_code_loading_options,
)
from databricks.labs.community_connector.sources.informix.informix_keys import (
    _ALLOW_NULLABLE_INDEX_OPTION,
    _PROMOTE_UNIQUE_INDEX_OPTION,
    _STRING_KINDS,
    _TABLE_INT_OPTIONS,
    _TABLE_ONLY_OPTIONS,
    InformixBridge,
    _null_byte_mode,
    _option_bool,
)
from databricks.labs.community_connector.sources.informix.informix_offsets import (
    _validated_offset,
    _with_backlog_streak,
)
from databricks.labs.community_connector.sources.informix.informix_options import (
    _DEFAULT_PARTITIONED_SNAPSHOT_PAGES_PER_BATCH,
    _HANDOFF_CHANNEL_DELETE,
    _HANDOFF_CHANNEL_UPSERT,
    _IDENTIFIER,
    _IS_DELETE_FLOW_OPTION,
    _PARTITIONED_SNAPSHOT_PAGES_PER_BATCH_OPTION,
    _PARTITIONED_STREAM_OPTION,
    _PIPELINE_SCOPE,
    _REPLAY_REESTABLISHED,
    _REPLAY_RESNAPSHOT_OPTION,
    _REPLAY_STOP_PK_OPTION,
    _RESNAPSHOT_OPTION,
    _SCAN_CACHES,
    _SNAPSHOT_BOUNDARY_LSN_OPTION,
    _SNAPSHOT_RECONNECT_BASE_SECONDS,
    _SNAPSHOT_RECONNECT_MAX_RETRIES,
    _SNAPSHOT_RECONNECT_MAX_SECONDS,
    _SNAPSHOT_SOURCE_OPTION,
    _TRANSIENT_VOLUME_IN_PROCESS_ATTEMPTS,
    _TRANSIENT_VOLUME_IN_PROCESS_MAX_SECONDS,
    _UNL_SNAPSHOT_MODES,
    COMMIT_LSN,
    COMMIT_TIME,
    CURSOR,
    OP,
    TX_ID,
    InformixError,
    _join_identity,
    _m,
    _split_identity,
)
from databricks.labs.community_connector.sources.informix.informix_registration import (
    _INFORMIX_REGISTRATION_CONTEXT,
    _MAX_REGISTRATION_TABLE_CACHES,
    _STATE_VALIDATION_COORDINATOR,
    ConnectionCapacityUnavailable,
    SharedStateAccessUnavailable,
    Table,
    _driver_registration,
    _generated_trigger_base,
    _install_registration,
    _resnapshot_range,
)
from databricks.labs.community_connector.sources.informix.informix_shards import (
    _recorded_transforms,
    _schema_fingerprint,
    _SpilledRows,
)
from databricks.labs.community_connector.sources.informix.informix_transactions import (
    _dropped_mount_error,
    _transform_may_null,
)
from databricks.labs.community_connector.sources.informix.informix_unload import (
    _decode_snapshot_stage_value,
    _encode_snapshot_stage_value,
    _offset,
    _unl_escape_byte,
)
from databricks.labs.community_connector.sources.informix.sqli_codec import (
    CONFIGURATION_ERRORS,
    SqliProtocolError,
    add_informix_exception_note,
    statement_sql_error,
)

# The bridge's class, so that tests can put another in its place. Held in a dict: the
# merged single file turns module globals into function locals, which ``global`` can't
# rebind.
_BRIDGE_FACTORY: dict[str, Callable[[dict[str, str]], InformixBridge]] = {
    "factory": PurePythonInformixBridge
}


def set_bridge_factory(factory: Callable[[dict[str, str]], InformixBridge]) -> None:
    # Docstring: Set the process-wide bridge factory (primarily for deterministic tests).

    _BRIDGE_FACTORY["factory"] = factory


class _ReaderMixin:
    # Docstring: The connector's Lakeflow interface: lifecycle, discovery, read_table and the
    # partitioned reader's offsets.

    def set_update_request(self, request: str | None, reset_at: float | None = None) -> None:
        # Docstring: Install what this update was started for (see _update_request) and when
        # the pipeline's targets were last reset (see _last_reset).

        self._update_request = request
        self._update_reset_at = reset_at

    def set_registration_scope(self, scope: str) -> None:
        # Docstring: Install the scope shared by every reader serialized from one registration.

        if not isinstance(scope, str) or _PIPELINE_SCOPE.fullmatch(scope) is None:
            # 167: Informix registration scope has an invalid identity
            raise ValueError(_m(167))
        if self._registration_scope != scope:
            self._snapshot_high_water.clear()
            self._snapshot_schema_ids.clear()
            self._trigger_boundaries.clear()
            self._schema_transition_waits.clear()
            self._activity_touched.clear()
            self._cleaned_update_scopes.clear()
            self._noted_upsert_starts.clear()
            self._cleaned_snapshot_stages.clear()
            self._snapshot_cleanup_watermark.clear()
            self._snapshot_schema_refreshed_at.clear()
            self._partition_embedded_rows.clear()
            self._partition_page_batches.clear()
            self._unl_gzip_reader = None
            self._unl_gzip_window_memo = None
            # The tables' source names are the update's pipeline spec, which can rename one.
            self.__dict__.pop("_metadata_sources", None)
            self._retention_swept.clear()
            self._spill_runs.clear()
            self._spill_runs_listed.clear()
            self._trigger_available_now = False
            for name in _SCAN_CACHES:
                self.__dict__.pop(name, None)
            # The staging client is the update's too: its token was minted for the update
            # the earlier scope names, and the new update revokes earlier updates' tokens.
            self._staging_files_api = None
            self._staging_transport_resolved = False
        self._registration_scope = scope

    def _pipeline_scope(self, checkpoint: dict[str, Any] | None = None) -> str:
        scope = self._registration_scope
        if scope is None and checkpoint:
            scope = checkpoint.get("pipeline_scope")
        if not isinstance(scope, str) or _PIPELINE_SCOPE.fullmatch(scope) is None:
            # 168: Informix reader has no registration or checkpoint scope
            raise InformixError(_m(168))
        return scope

    def _offset_scopes(self, offset: dict[str, Any]) -> tuple[str, str]:
        # Docstring: The scope an offset was written under, and the one its staged state is in.
        #
        # A snapshot resumed in a later update finds its manifest, pages and extract pin under
        # the scope that wrote them, which _cleanup_previous_update_scopes keeps; a legacy
        # offset without one falls back to the registration scope. A handoff relocation's
        # state stays under the capturing flow's scope, carried on stage_scope.
        flow = offset.get("pipeline_scope")
        if not isinstance(flow, str) or _PIPELINE_SCOPE.fullmatch(flow) is None:
            flow = self._pipeline_scope(offset)
        stage = offset.get("stage_scope")
        if not isinstance(stage, str) or _PIPELINE_SCOPE.fullmatch(stage) is None:
            stage = flow
        return flow, stage

    def prepare_for_trigger_available_now(self) -> None:
        # Docstring: Freeze stream high-water marks when Spark selects AvailableNow.

        self._trigger_available_now = True
        self._trigger_boundaries.clear()

    def close(self) -> None:
        # Docstring: Close the live SQLI transport, if one was opened.

        # Stop the background page-cleanup pool without blocking on in-flight
        # deletes -- cleanup is best-effort, so a few pages may be left for the
        # retention sweep. Nulling it lets a later read lazily start a fresh pool.
        executor = self._snapshot_cleanup_executor_instance
        if executor is not None:
            self._snapshot_cleanup_executor_instance = None
            executor.shutdown(wait=False)
        self._reset_lakebase_connection()
        with self._metadata_session_lock:
            bridge = self._bridge_instance
            if bridge is None:
                return
            try:
                release = getattr(bridge, "release_connection", None)
                if release is not None:
                    release()
                    return
                transport = getattr(bridge, "transport", None)
                close = getattr(transport, "close", None)
                if close is not None:
                    close()
            finally:
                # Never retain a closed or partially closed bridge.  The bridge's
                # release path also stops its lease heartbeat and frees its
                # connector-managed connection-capacity slot; its own Lakebase
                # connection, which serves slot operations, closes with it.
                self._bridge_instance = None
                self._reset_lakebase_connection(bridge)

    def __enter__(self) -> Any:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if exc_value is None:
            self.close()
            return
        try:
            self.close()
        except Exception as close_error:
            add_informix_exception_note(exc_value, f"Informix cleanup also failed: {close_error}")

    def __getstate__(self) -> dict[str, Any]:
        # Docstring: Exclude live SQLI state when Spark serializes the data source.
        #
        # Schema and metadata discovery run before Spark ships the reader to a
        # Python worker, so ``_bridge_instance`` can contain a socket, buffered
        # streams, and a thread lock.  None of those objects is transferable or
        # valid in another process.  The worker reconstructs a fresh bridge from
        # the immutable connection options on its first source operation.

        self.close()
        state = self.__dict__.copy()
        state["_bridge_instance"] = None
        state["_metadata_session_lock"] = None
        state["_call_context"] = None
        # Driver-only; never ship materialized embedded rows to a worker. The worker
        # gets its rows from the partition descriptor, not from this cache.
        state["_partition_embedded_rows"] = {}
        # An open gzip record iterator is a live generator, and a window memo holds rows;
        # neither belongs on a worker.
        state["_unl_gzip_reader"] = None
        state["_unl_gzip_window_memo"] = None
        # close() dropped the Lakebase connection; the provisioned state and the Files
        # API client hold an API session and a lock, so a worker resolves its own.
        state["_lakebase_state"] = None
        state["_staging_files_api"] = None
        state["_staging_transport_resolved"] = False
        state["_partition_page_batches"] = {}
        state["_spill_runs"] = {}
        state["_spill_runs_listed"] = {}
        # What this process has read of snapshot manifests and scan records: a worker
        # gets a page's moved rows from its descriptor, and reads a manifest itself.
        for name in _SCAN_CACHES:
            state.pop(name, None)
        if _INFORMIX_REGISTRATION_CONTEXT["scope"]:
            # Captured again here when old, on the driver (see _driver_registration).
            registration = _driver_registration()
            if _generated_trigger_base is None:
                # Registered by class from a package: an executor imports this module
                # afresh and is handed only readers, so what the driver gathered goes with
                # them. The merged file's globals go with its code.
                state["_driver_registration"] = registration
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        carried = state.pop("_driver_registration", None)
        if carried:
            _install_registration(carried)
        self.__dict__.update(state)
        self._metadata_session_lock = threading.RLock()
        self._call_context = threading.local()

    @property
    def _bridge(self) -> InformixBridge:
        if self._bridge_instance is None:
            _refuse_code_loading_options(self.options)
            self._bridge_instance = _BRIDGE_FACTORY["factory"](self.options)
        bridge, context = self._bridge_instance, self._call_state()
        if getattr(bridge, "_call_context", None) is not context:
            bridge._call_context = context
        return bridge

    def _call_state(self) -> threading.local:
        # Docstring: This connector's per-thread call context (see __init__), made on first
        # use where the connector was built without __init__.
        context = self.__dict__.get("_call_context")
        if context is None:
            context = self._call_context = threading.local()
        return context

    @contextlib.contextmanager
    def _metadata_access(self) -> Iterator[None]:
        # Docstring: Bypass connection capacity for a catalog lookup, then give the session back.
        #
        # Registration and reader setup read only metadata. Queued for a slot behind running
        # reads they would stall startup, and a wait that gave up would fail it.

        # The bypass is this thread's (_call_context), so a read on another thread claims
        # its slot as ever. Under the metadata lock, re-entrant: two lookups at once let
        # the first one's exit close the second one's session.
        with self._metadata_session_lock:
            context = self._call_state()
            previous = getattr(context, "bypass_capacity", False)
            context.bypass_capacity = True
            try:
                yield
            finally:
                try:
                    self._release_worker_connection()
                finally:
                    context.bypass_capacity = previous

    def list_tables(self) -> list[str]:
        with self._metadata_access():
            return sorted(self._table_map())

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        table_options = _options_set(table_options)
        with self._metadata_access():
            table = self._registration_table(table_name, table_options)
            _ensure_materializable(table, self._with_connection_scan(table_options))
            renames = dict(table.column_renames or ())
            transforms = {
                name: operand
                for name, _, operand in _recorded_transforms(table.column_transforms) or ()
            }
            # A NOT NULL column can still land a NULL two ways. The delete flow sends a
            # key and nothing else. And under string.null.byte=null a character value
            # that holds a NUL byte is a NULL. Spark plans on what the schema says cannot
            # be NULL, so it says what can.
            # Only a character column's values can hold one: INTERVAL, partial DATETIME and
            # decimals as strings are text the connector writes. A transform whose outermost
            # function is NVL or COALESCE replaces any NULL, the NUL byte's included.
            deletes = self._is_delete_flow()
            nulled = _null_byte_mode(self.options) == "null"
            fields = []
            for column in table.columns:
                operand = transforms.get(column.name)
                kind = (
                    _transform_spark_type(operand, column, table_options)
                    if operand is not None
                    else _spark_type(column, table_options)
                )
                filled = operand is not None and _operand_functions(operand)[:1] in (
                    ["NVL"],
                    ["COALESCE"],
                )
                fields.append(
                    StructField(
                        renames.get(column.name, column.name),
                        kind,
                        (deletes and column.name not in table.primary_keys)
                        or not filled
                        and (
                            column.nullable
                            or (operand is not None and _transform_may_null(operand))
                            # By the column as read: a transform of such a value gets the NULL.
                            or (nulled and _row_filter_kind(column) in _STRING_KINDS)
                        ),
                    )
                )
            fields.extend(
                (
                    StructField(CURSOR, StringType(), False),
                    StructField(COMMIT_LSN, StringType(), False),
                    StructField(TX_ID, LongType(), True),
                    StructField(OP, StringType(), False),
                    StructField(COMMIT_TIME, TimestampType(), True),
                )
            )
            return StructType(fields)

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        table_options = _options_set(table_options)
        self._refuse_connection_table_options(table_options)
        # A table's whole-number options failed only when the stream first read them.
        for name, default, minimum, maximum in _TABLE_INT_OPTIONS:
            self._table_int_option(table_options, name, default, minimum=minimum, maximum=maximum)
        with self._metadata_access():
            table = self._registration_table(table_name, table_options)
            _ensure_materializable(table, self._with_connection_scan(table_options))
            # The pipeline reads every table's metadata through this one connector. Two of
            # its tables naming one Informix table (qualified_source_table) would share
            # every record and staged page kept for it, which are the Informix table's
            # and the update's: one flow served the other's differently filtered pages,
            # or deleted pages and spills the other had yet to read.
            sources = self.__dict__.setdefault("_metadata_sources", {})
            other = sources.setdefault(table.native_identity, table_name)
            if other != table_name:
                # 462: Tables '{}' and '{}' of this pipeline both read Informix table {},
                # and two flows of one table would share its change and snapshot state;
                # read each Informix table through one source_table per pipeline
                raise InformixError(_m(462, other, table_name, table.exposed_name))
            # Only consulted on the append path (tombstones) or at snapshot time
            # (scan), but validated for every table so a malformed value fails at
            # registration rather than on a later read.
            self._append_delete_tombstones(table_options)
            self._snapshot_cursor_scan(table_options)
            self._check_table_read_options(table, table_options)
            if self._append_only_table(table, table_options):
                # Report a cursor even though no primary key is exposed: the
                # cursor is what makes the framework treat this as an
                # incremental stream. Without it the flow would re-read the
                # whole table into an append target every microbatch and
                # accumulate duplicates without bound.
                result = {
                    "primary_keys": [],
                    "cursor_field": CURSOR,
                    "ingestion_type": "append",
                }
            elif not _cdc_capable(table):
                result = {
                    "primary_keys": [],
                    "cursor_field": None,
                    "ingestion_type": "snapshot",
                }
            else:
                renames = dict(table.column_renames or ())
                result = {
                    "primary_keys": [renames.get(key, key) for key in table.primary_keys],
                    "cursor_field": CURSOR,
                    "ingestion_type": "cdc_with_deletes",
                }
            return result

    def _check_table_read_options(self, table: Table, table_options: dict[str, str]) -> None:
        # Docstring: Fail at registration on a table option its reads would fail on: they
        # were read only when the flow first used them, after the pipeline had started.

        mode = self._snapshot_mode(table_options)
        migration = self._table_migration(table_options)
        _option_bool(table_options, "snapshot.incremental.datetime.as.string", True)
        _option_bool(table_options, _ALLOW_NULLABLE_INDEX_OPTION, False)
        self._append_only_mode(table_options)
        _snapshot_predicate(_snapshot_filter(table_options), table)
        self._snapshot_isolation(table_options)
        self._validate_unl_source_options(table_options)
        self._table_snapshot_source(table_options)
        _, escape = self._unl_field_format(table_options)
        _unl_escape_byte(escape, _client_encoding(self.options))
        append = self._append_only_table(table, table_options)
        if migration and not append:
            # A migrating table records and stops whatever its mode (see _read_handoff).
            return
        if not migration:
            # A cutover seeds from the parked token and reads no extract.
            self._check_unl_source_mode(table, table_options, append)
        if append:
            configured = table_options.get("snapshot.mode", self.options.get("snapshot.mode"))
            # incremental too for a table with a key, whose copy follows the stream (see
            # _begin_incremental).
            allowed = {"initial", "cdc_only", "incremental" if table.primary_keys else "initial"}
            if configured is not None and mode not in allowed:
                if migration:
                    # The cutover to append-only recorded its seed, and the restore then
                    # failed on this, after the migration had moved the table.
                    # 471: table.migration cuts '{}' over to append-only, which accepts only
                    # snapshot.mode=initial or snapshot.mode=cdc_only (or incremental for a table
                    # with a key), and its restore would fail on the snapshot.mode in force: set one
                    # of them in the table's configuration (an incremental copy in progress still
                    # finishes)
                    raise ValueError(_m(471, table.exposed_name))
                # 274: Append-only table '{}' supports only snapshot.mode=initial or
                # snapshot.mode=cdc_only, or snapshot.mode=incremental when it has a key
                raise ValueError(_m(274, table.exposed_name))
        elif not _cdc_capable(table) and mode in {"cdc_only", "recovery"}:
            # 170: snapshot.mode={} requires a CDC-capable table; '{}' is snapshot-only
            raise ValueError(_m(170, mode, table.exposed_name))

    def _check_unl_source_mode(
        self, table: Table, table_options: dict[str, str], append: bool
    ) -> None:
        # Docstring: Fail a table's snapshot.source, or snapshot.boundary.lsn, that its reads
        # would never use.
        #
        # Only snapshot.mode initial and initial_only take a snapshot from the extract (an
        # append-only table without a key, initial, its default). Set with another, the option did
        # nothing: an append-only table's cdc_only skipped the extract, a keyed table's
        # default incremental copied the live table from today's log position instead of
        # loading the extract from its boundary. Checked on the table's own configuration;
        # one set on the connection applies only to the tables whose mode reads it.

        source = bool(str(table_options.get(_SNAPSHOT_SOURCE_OPTION) or "").strip())
        if not source and _SNAPSHOT_BOUNDARY_LSN_OPTION not in table_options:
            return
        option = _SNAPSHOT_SOURCE_OPTION if source else _SNAPSHOT_BOUNDARY_LSN_OPTION
        mode = self._table_snapshot_mode(table, table_options)
        if not source and self._table_snapshot_source(table_options) is None:
            # 472: Option '{}' of table '{}' is never read: the table has no snapshot.source;
            # set one, or remove the option
            raise ValueError(_m(472, option, table.exposed_name))
        if not append and not _cdc_capable(table):
            # 473: Option '{}' of table '{}' is never read: the table is snapshot-only, read
            # whole from Informix; remove the option
            raise ValueError(_m(473, option, table.exposed_name))
        if append and mode == "cdc_only":
            # 474: Option '{}' of table '{}' is never read: snapshot.mode=cdc_only reads no
            # snapshot; set snapshot.mode=initial, or leave it unset on an append-only table
            # without a key
            raise ValueError(_m(474, option, table.exposed_name))
        if (not append or mode == "incremental") and mode not in _UNL_SNAPSHOT_MODES:
            # 475: Option '{}' of table '{}' is never read: snapshot.mode={} takes no snapshot
            # from it; set snapshot.mode=initial or snapshot.mode=initial_only
            raise ValueError(_m(475, option, table.exposed_name, mode))

    def _refuse_connection_table_options(self, table_options: dict[str, str]) -> None:
        # Docstring: Fail a table option set on the connection rather than in the table's
        # configuration.
        #
        # The pipeline asks for a table's metadata with the table's configuration alone,
        # while its reads get the connection's options as well. A table option the connector
        # takes from the table's options only, set on the connection, then reached the reads
        # and not the metadata: primary.keys keyed the stream of a table registered without
        # a key, so its deletes were dropped, and rename_columns renamed the columns but not
        # the keys merged by. Set the same in both, it is the table's own.

        for name in _TABLE_ONLY_OPTIONS:
            if name in self.options and table_options.get(name) != self.options[name]:
                # 453: Option '{}' is a table option but is set on the connection: set it in
                # the table_configuration of each table it is for
                raise ValueError(_m(453, name))

    def _registration_table(self, table_name: str, table_options: dict[str, str]) -> Table:
        # Docstring: Resolve one configured table with a targeted catalog lookup.
        #
        # The framework asks for one specific table at a time (the pipeline spec's
        # exact ``owner.name``), so query just that table rather than scanning and
        # parsing the whole catalog. This keeps an unrelated unsupported table from
        # breaking registration for tables that are configured, and avoids reading
        # a large catalog wholesale. Resolved tables are cached per table in the
        # cross-instance coordinator so repeated registrations reuse them.

        exposed = table_options.get("qualified_source_table", table_name)
        key = self._registration_table_cache_key()
        if key is None:
            return self._table(table_name, table_options, refresh=True)
        coordinator = _STATE_VALIDATION_COORDINATOR
        cache_key = (*key, exposed)
        with coordinator.condition:
            # Held across the catalog lookup on purpose: concurrent registrations of one
            # table share a single lookup instead of each querying Informix.
            table = coordinator.table_caches.get(cache_key)
            if table is None:
                table = self._resolve_registration_table(exposed)
                coordinator.table_caches[cache_key] = table
                while len(coordinator.table_caches) > _MAX_REGISTRATION_TABLE_CACHES:
                    del coordinator.table_caches[next(iter(coordinator.table_caches))]
        if self._tables is None:
            self._tables = {}
        self._tables[exposed] = table
        return self._apply_table_options(table, table_options)

    def _resolve_registration_table(self, exposed: str) -> Table:
        # Docstring: Resolve a single configured table by exposed ``owner.name`` via a
        # targeted catalog query, applying the same table filters ``_table_map`` would.
        # Only this one table is read and parsed, so an unrelated unsupported table
        # elsewhere in the catalog cannot fail it.

        parts = _split_identity(exposed)
        if len(parts) != 2 or not all(_IDENTIFIER.fullmatch(part) for part in parts):
            # 169: Unknown or excluded Informix table '{}'
            raise ValueError(_m(169, exposed))
        database = self.options.get("database", "")
        table = Table.parse(
            self._bridge.get_table(_join_identity(database, parts[0], parts[1])),
            database,
        )
        if not self._selected(table):
            # 169: Unknown or excluded Informix table '{}'
            raise ValueError(_m(169, exposed))
        return table

    def _registration_table_cache_key(self) -> tuple[str, ...] | None:
        scope = self._registration_scope
        hostname = self.options.get("hostname")
        server = self.options.get("server")
        if scope is None or not hostname or not server:
            # Injected test bridges and incomplete discovery configurations do
            # not have a stable source identity and must remain instance-local.
            return None
        # As the state keys hold them: a table cached under one database name's case was
        # handed to a connection naming it in another, whose own lookups then differed.
        return (
            scope,
            hostname.strip().rstrip(".").casefold(),
            str(int(self.options.get("port") or 9088)),
            server.strip(),
            self.options.get("database", "").strip(),
            self.options.get("table.include.list", self.options.get("tables", "")),
            self.options.get("table.exclude.list", ""),
            # Discovery keys a table by its unique index only when this is on.
            str(_option_bool(self.options, _PROMOTE_UNIQUE_INDEX_OPTION, True)),
        )

    def _scope_from_checkpoint(self, offset: Any) -> None:
        # Docstring: With no registration (the connector used outside a pipeline), take the
        # scope a resumed checkpoint names, so that every step of the read runs under it.
        #
        # Each step asks for the scope on its own, and a few had the checkpoint to fall back
        # to: the others, earlier on the same read, failed it with 168.

        scope = offset.get("pipeline_scope") if isinstance(offset, dict) else None
        if (
            self._registration_scope is None
            and isinstance(scope, str)
            and _PIPELINE_SCOPE.fullmatch(scope)
        ):
            self.set_registration_scope(scope)

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        table_options = _options_set(table_options)
        self._scope_from_checkpoint(start_offset)
        rows, offset = self._read_upserts(table_name, start_offset, table_options)

        def columns():
            return self._table(table_name, table_options).columns

        return self._output_rows(rows, table_options, columns), offset

    def _read_upserts(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        # Docstring: ``read_table`` under source column names; rename_columns is applied
        # only where rows leave the connector.

        # A batch read passes None, a stream its offset ({} at first). A batch has no
        # checkpoint to try again from: the empty batch a stream yields on a capacity or
        # Volume failure, read by apply_changes_from_snapshot (a snapshot-only table),
        # deleted every row of the target, so a batch read raises instead.
        batch = start_offset is None
        if not batch:
            self._keep_staging_credential()
        start_offset = start_offset or {}
        effective_start = self._effective_start_offset(start_offset)
        snapshot_mode = self._snapshot_mode(table_options)
        self._publish_checkpoint_channel_start_before_capacity(
            table_name, effective_start, table_options
        )
        replaying = self._replay_stop_lsn(table_options) is not None
        replay_wait_budget = self._replay_connection_wait_budget(table_options)
        with self._capacity_attempt(
            start_offset, replaying=replaying, replay_wait_budget=replay_wait_budget
        ):
            return self._read_table_attempt(
                table_name,
                start_offset,
                effective_start,
                table_options,
                snapshot_mode,
                batch=batch,
            )

    # pylint: disable-next=too-many-branches,too-many-statements
    def _read_table_attempt(
        self,
        table_name: str,
        start_offset: dict,
        effective_start: dict,
        table_options: dict[str, str],
        snapshot_mode: str,
        attempt_index: int = 0,
        *,
        batch: bool = False,
    ) -> tuple[Iterator[dict], dict]:
        # Re-runs this attempt from the top after a transient shared-state failure.
        # Recursion rather than a loop because the failure surfaces from inside the
        # handlers below, and the retry has to redo the shared-state work that failed
        # rather than resume part-way through it. Bounded by attempt_index.
        def read_again():
            return self._read_table_attempt(
                table_name,
                start_offset,
                effective_start,
                table_options,
                snapshot_mode,
                attempt_index + 1,
                batch=batch,
            )

        try:
            try:
                table = self._table(table_name, table_options)
                migration = self._table_migration(table_options)
                if not migration and self._replay_stop_lsn(table_options) is None:
                    # A replay is skipped too: an initial range starts from no offset, so
                    # it would retain no scope and drop the one Spark's logged end names.
                    # The read that follows from that end cleans up, keeping it.
                    #
                    # Skip obsolete-scope cleanup for the whole migration window. On a
                    # same-pipeline relocation the *capturing* flow's scope looks like an
                    # ordinary previous update, and the cutover-seed read (empty checkpoint,
                    # so no stage_scope yet to trigger the in-method guard) would otherwise
                    # delete its still-needed snapshot manifest before Restore serves it.
                    # After Restore (table.migration cleared) the stage_scope guard inside
                    # _cleanup_previous_update_scopes protects it until the serve completes.
                    self._cleanup_previous_update_scopes(table, effective_start)
                    if effective_start.get("handoff_seeded"):
                        # First post-Restore read still carrying the marker: drop any token
                        # the cutover run left behind (see _drop_orphaned_handoff_tokens).
                        # Both channels, unconditionally. Once table.migration is cleared the
                        # delete reader can no longer seed from its token (that path is
                        # migration-only), so a lingering delete token is always dead here.
                        # The delete reader drops its own token only when its offset carries
                        # the marker, which never happens if the cutover delete flow arrived
                        # with a non-empty checkpoint (it re-parked instead of seeding), so
                        # its token would otherwise be orphaned with no TTL. This upsert read
                        # is guaranteed to fire post-Restore, so dropping both here is the
                        # reliable one-shot.
                        self._drop_orphaned_handoff_tokens(
                            table, [_HANDOFF_CHANNEL_UPSERT, _HANDOFF_CHANNEL_DELETE]
                        )
                        # Strip the marker so this is one-shot: nothing post-Restore reads
                        # it, and a quiet (no-advance) read would otherwise return it and
                        # re-drop every poll until the stream advances off it. A copy: the
                        # caller's offset is Spark's, and the partition cache is keyed by it.
                        effective_start = {
                            k: v for k, v in effective_start.items() if k != "handoff_seeded"
                        }
                self._touch_table_state(table)
                self._maybe_sweep_stale_snapshot_stages(table)
                _ensure_materializable(table, self._with_connection_scan(table_options))
                # A staged snapshot's checkpoint too, mid-serve: it names the snapshot's LSN
                # and schema, which its first batch published for the update that took it.
                if (
                    _cdc_capable(table)
                    and effective_start.get("phase") in ("stream", "snapshot")
                    and effective_start.get("schema_fingerprint") is not None
                ):
                    checkpoint = _validated_offset(effective_start)
                    self._publish_upsert_channel_start(
                        table,
                        int(checkpoint.get("begin_lsn") or checkpoint["commit_lsn"]),
                        str(checkpoint["schema_id"]),
                        str(checkpoint["schema_fingerprint"]),
                        self._pipeline_scope(checkpoint),
                    )
                if self._append_only_table(table, table_options):
                    # Append-only has exactly two states: establish the boundary once,
                    # then stream. _read_stream needs no primary key, so a key-less
                    # table follows the identical CDC path from here on.
                    if effective_start and migration:
                        # Whatever the phase: a capture parks it and a committed seed drops
                        # the token. A seed from a keyed capture can be mid-snapshot. The same
                        # offset back, undecayed, as _read_handoff's below.
                        return self._append_migration_record_and_stop(table, effective_start)
                    if effective_start and effective_start.get("phase") == "stream":
                        # After Restore, a handoff seed can carry an in-flight incremental
                        # copy (phase=="stream" with an "incremental" block, only ever from a
                        # keyed capture that was mid-copy); continue the copy from its cursor
                        # rather than treating it as pure CDC, or the un-copied rows are lost.
                        if self._offset_is_incremental_reader(effective_start):
                            result = self._read_incremental(table, effective_start, table_options)
                        else:
                            # An initial snapshot staged pages, as a keyed one does below.
                            self._cleanup_completed_snapshot_stage(table, effective_start)
                            result = self._read_stream(
                                table, effective_start, table_options, deletes=False
                            )
                    elif effective_start and effective_start.get("phase") == "snapshot":
                        # An in-flight snapshot finishes whatever snapshot.mode says now, as a
                        # keyed one does below: a seed from a keyed capture mid-snapshot, or a
                        # mode changed or unset mid-snapshot, would otherwise start CDC at the
                        # boundary (cdc_only) and drop the pages not yet served.
                        result = self._read_snapshot(
                            table, effective_start, table_options, allow_keyless=True
                        )
                    else:
                        result = self._read_append_only(table, effective_start, table_options)
                elif migration:
                    # Records an offset and stops at it, the same offset back: decayed batch
                    # by batch (_reset_retry_counts), a busy flow's pressure made each an
                    # extra batch that rewrote the token.
                    return self._read_handoff(table, effective_start)
                elif not _cdc_capable(table):
                    if snapshot_mode in {"cdc_only", "recovery"}:
                        # 170: snapshot.mode={} requires a CDC-capable table; '{}' is snapshot-only
                        raise ValueError(_m(170, snapshot_mode, table.exposed_name))
                    result = self._read_snapshot_only(table, table_options)
                elif snapshot_mode == "recovery" and (
                    not effective_start or effective_start.get("phase") != "stream"
                ):
                    # 171: snapshot.mode=recovery requires an existing stream checkpoint; use
                    # snapshot.mode=initial for a new or snapshot-phase table
                    raise InformixError(_m(171))
                elif snapshot_mode == "recovery":
                    checkpoint = _validated_offset(effective_start)
                    current_table = self._refresh_table_schema(table, None)
                    if checkpoint.get("schema_fingerprint") != _schema_fingerprint(current_table):
                        # 172: snapshot.mode=recovery cannot rebuild schema history for '{}' because
                        # the source schema differs from the checkpoint; run a full refresh
                        raise InformixError(_m(172, table.exposed_name))
                    # Recovery explicitly repairs missing immutable schema
                    # history. Ordinary microbatches trust the validated
                    # checkpoint and avoid rereading this Volume record.
                    self._record_current_schema(
                        current_table,
                        int(checkpoint["commit_lsn"]),
                        str(checkpoint["schema_id"]),
                        str(checkpoint["schema_fingerprint"]),
                        self._pipeline_scope(checkpoint),
                        owner=True,
                    )
                    # A copy in flight goes on, as under any mode (below).
                    read = (
                        self._read_incremental
                        if self._offset_is_incremental_reader(checkpoint)
                        else functools.partial(self._read_stream, deletes=False)
                    )
                    result = read(current_table, checkpoint, table_options)
                elif (
                    snapshot_mode == "auto_snapshot"
                    and effective_start.get("phase") == "stream"
                    and self._checkpoint_requires_snapshot(effective_start)
                ):
                    replaying = self._replay_stop_lsn(table_options) is not None
                    result = self._read_incremental(
                        table,
                        None,
                        # A replayed range from a start the log no longer holds cannot be
                        # reproduced; it resnapshots, as an initial range does. Spark goes on
                        # from the end it logged, so one with no copy cursor drains the copy.
                        {
                            _REPLAY_STOP_PK_OPTION: "null",
                            _REPLAY_RESNAPSHOT_OPTION: "1",
                            _RESNAPSHOT_OPTION: "1",
                            **table_options,
                        }
                        if replaying
                        else {_RESNAPSHOT_OPTION: "1", **table_options},
                        pipeline_scope_override=self._resnapshot_scope(table, effective_start),
                    )
                    if replaying:
                        result = result[0], {**result[1], _REPLAY_REESTABLISHED: True}
                elif self._offset_is_incremental_reader(effective_start):
                    # A copy in flight finishes whatever snapshot.mode says now, as a snapshot
                    # does: under another mode its cursor was dropped, or its stream stopped
                    # (initial_only), and the rows it had yet to copy were never read.
                    result = self._read_incremental(table, effective_start, table_options)
                elif snapshot_mode == "initial_only" and effective_start.get("phase") == "stream":
                    self._cleanup_completed_snapshot_stage(table, effective_start)
                    result = (iter(()), dict(effective_start))
                elif snapshot_mode == "cdc_only" and not effective_start:
                    result = self._read_cdc_only(table, table_options)
                elif snapshot_mode in {"incremental", "auto_snapshot"} and not effective_start:
                    result = self._read_incremental(table, None, table_options)
                elif not effective_start or effective_start.get("phase", "snapshot") == "snapshot":
                    result = self._read_snapshot(table, effective_start, table_options)
                else:
                    self._cleanup_completed_snapshot_stage(table, effective_start)
                    result = self._read_stream(table, effective_start, table_options, deletes=False)
                return self._reset_retry_counts(result, start_offset)
            except (SharedStateAccessUnavailable, OSError) as error:
                return self._volume_failure_result(
                    error, read_again, table_options, attempt_index, start_offset, batch=batch
                )
            except ConnectionCapacityUnavailable:
                # Continuous flows are permanent, so capacity pressure is the
                # steady state rather than a fault: always yield and let the
                # framework call again, bounded by the consecutive-miss cap in
                # _capacity_retry_offset so a permanently starved flow still
                # fails loudly instead of silently replicating nothing. Only a
                # triggered flow raises here -- it drains and releases its slot,
                # so an exhausted wait is a genuine end-of-data failure and
                # yielding an empty batch would make AvailableNow stop early. A batch
                # read raises too: it has no next call to wait for (see _read_upserts).
                if self._trigger_available_now or batch:
                    raise
                if self._replay_stop_lsn(table_options) is not None:
                    raise
                return iter(()), self._capacity_retry_offset(start_offset)
        finally:
            self._release_worker_connection()

    # ------------------------------------------------------------------
    # SupportsPartitionedStream -- partitioned snapshot-page reader
    # ------------------------------------------------------------------
    # Default on (see _PARTITIONED_STREAM_OPTION). Only the staged-snapshot
    # page-serve phase fans out: a microbatch spans up to pages-per-batch staged
    # pages, each read+decoded on its own Spark executor via read_partition, and so
    # is the bootstrap microbatch's page 0. Every other phase (live CDC/incremental
    # stream, transitions, deletes) is embedded partitions whose rows the driver
    # computed via read_table / read_table_deletes -- byte-for-byte the simple
    # reader's output, just wrapped. The delicate phase machine (read_table,
    # _read_stream, _read_snapshot, transitions, _recover) is reused verbatim; only
    # its (rows, offset) output is reshaped into the partitioned contract.

    def _partitioned_stream_enabled(self) -> bool:
        return _option_bool(self.options, _PARTITIONED_STREAM_OPTION, True)

    def _partitioned_pages_per_batch(self) -> int:
        return int(
            self.options.get(
                _PARTITIONED_SNAPSHOT_PAGES_PER_BATCH_OPTION,
                str(_DEFAULT_PARTITIONED_SNAPSHOT_PAGES_PER_BATCH),
            )
        )

    def _is_delete_flow(self) -> bool:
        # Spark hands option keys on lowercased. Its own options answer to either form,
        # but a plain-dict copy of them, as a drain or shard reader gets, keeps only the
        # lowercased one: read by the documented name alone, a delete flow's copy would
        # take itself for the upsert flow.
        options = self.options
        value = options.get(_IS_DELETE_FLOW_OPTION)
        if value is None:
            value = options.get(_IS_DELETE_FLOW_OPTION.lower())
        return value == "true"

    @staticmethod
    def _partition_offset_key(start_offset: dict | None) -> str:
        return json.dumps(start_offset or {}, sort_keys=True)

    @staticmethod
    def _encode_embedded_rows(rows: Iterable[dict[str, Any]]) -> list[Any]:
        # F1: the framework JSON-serializes each partition descriptor
        # (InputPartition(json.dumps(descriptor))), but read_table rows carry native
        # Decimal / bytes / etc. that json cannot encode. Round-trip every row through
        # the staging codec -- the same lossless JSON-safe form staged pages use -- so
        # the descriptor serializes and read_partition reconstructs the exact rows.
        return [_encode_snapshot_stage_value(row) for row in rows]

    @staticmethod
    def _decode_embedded_rows(rows: Iterable[Any]) -> Iterator[dict[str, Any]]:
        for row in rows:
            yield _decode_snapshot_stage_value(row)

    def is_partitioned(self, table_name: str) -> bool:
        # Docstring: Gate the partitioned reader (default on).
        #
        # True only for tables with a bounded staged-snapshot -> stream lifecycle:
        # keyed CDC tables and append-only tables. Their snapshot serves immutable
        # staged pages that fan out across executors, and every microbatch is bounded
        # so the embedded fallback never ships an unbounded batch. Snapshot-only
        # (non-CDC, non-append) tables drain unbounded and stay on the simple reader.
        # Cheap and exception-safe: any doubt returns False -> simpleStreamReader.

        if not self._partitioned_stream_enabled():
            return False
        # Shared CDC is always on, so no shared-session check is needed here. The lookup is
        # the registration one, outside connection capacity: queued for a slot among a
        # starting pipeline's flows, a wait that gave up would leave the table on the simple
        # reader for the whole stream.
        # Read as a table's options are everywhere else (_options_set).
        options = _options_set(self.options)
        attempt = 0
        while True:
            try:
                with self._metadata_access():
                    table = self._registration_table(table_name, options)
                return bool(_cdc_capable(table) or self._append_only_table(table, options))
            except (SqliProtocolError, OSError) as error:
                # A dropped connection, or a session the server refused (-25571, its
                # connections used up while a pipeline starts), on a stream runner that
                # starts cold: taken for a failed lookup, it left the flow on the simple
                # reader for the whole update, and a file snapshot part-way through a serve
                # then failed with 423, which asks for stream.partitioned=true. Retried as a
                # snapshot read is; a source that stays down fails the flow, as its reads
                # would. Any OSError, a file's too (ssl.ca.file on a dropped mount gave
                # ENOTCONN, which failed the update at once), but not a certificate, a login
                # or a type no retry helps.
                # Nor a missing database or privilege (statement_sql_error), which slept
                # through every retry before the table's registration failed.
                if (
                    isinstance(error, CONFIGURATION_ERRORS)
                    or statement_sql_error(error)
                    or attempt >= _SNAPSHOT_RECONNECT_MAX_RETRIES
                ):
                    raise
                reset = getattr(self._bridge, "reset_transport", None)
                if reset is not None:
                    reset()
                time.sleep(
                    min(
                        _SNAPSHOT_RECONNECT_BASE_SECONDS * (2**attempt),
                        _SNAPSHOT_RECONNECT_MAX_SECONDS,
                    )
                )
                attempt += 1
            except Exception:  # pylint: disable=broad-except
                # 174: Informix reads '{}' with the simple stream reader: its table lookup failed
                logging.getLogger(__name__).warning(
                    _m(174, table_name),
                    exc_info=True,
                )
                return False

    def _snapshot_page_batch(
        self, table: Table, effective_start: dict, table_options: dict[str, str]
    ) -> tuple[str, str, int, int, int, int] | None:
        # Docstring: Resolve a staged-snapshot page-serve microbatch, else None.
        #
        # Returns ``(stage_scope, schema_id, snapshot_lsn, start_page, end_page,
        # page_count)`` for a snapshot-phase checkpoint whose staged manifest exists,
        # where ``stage_scope`` is the scope the pages are stored under (the capturing
        # flow's, for a handoff relocation; otherwise the offset's own scope),
        # mirroring the resume resolution in :meth:`_read_snapshot`. Deterministic
        # given the start offset + immutable manifest + pages-per-batch, so
        # ``latest_offset`` and ``get_partitions`` agree without sharing state. None
        # means "not a page-serve" -> the embedded path. Never a page-serve for the
        # delete channel (deletes stage no pages).

        if self._is_delete_flow():
            return None
        if self._table_migration(table_options):
            # Migration is record-and-stop: a snapshot-phase offset while table.migration is
            # set must reach the embedded _read_handoff (park the token on capture, drop it
            # on the committed seed) rather than being served here. After Restore
            # (table.migration cleared) this returns a batch and serves the pages -- under
            # stage_scope for a relocation -- as normal.
            return None
        if not effective_start or effective_start.get("phase") not in (None, "snapshot"):
            return None
        try:
            checkpoint = _validated_offset(effective_start)
        except Exception:  # pylint: disable=broad-except
            return None
        snapshot = checkpoint.get("snapshot")
        if not isinstance(snapshot, dict) or "page_index" not in snapshot:
            return None
        # _validated_offset (above) already verified, for a snapshot-phase offset,
        # that schema_id is 32-hex, snapshot_lsn is present and non-negative, and
        # snapshot.page_index is a non-negative int -- so these reads cannot raise. A
        # malformed offset raises inside _validated_offset and is caught above,
        # returning None -> the embedded path.
        schema_id = str(checkpoint["schema_id"])
        snapshot_lsn = int(checkpoint["snapshot_lsn"])
        start_page = int(snapshot["page_index"])
        stage_scope = self._offset_scopes(checkpoint)[1]
        # Staged pages live under the scope that created them. For a handoff relocation
        # that is the *capturing* flow's scope, carried on stage_scope while the offset's
        # own pipeline_scope is already the new flow's; otherwise stage_scope == the
        # offset scope and behaviour is unchanged. The returned scope is the *stage*
        # scope -- it drives the manifest read, the page reads (driver + executor), and
        # consumed-page cleanup. The emitted offset's scope is resolved separately from
        # the checkpoint in _snapshot_page_batch_offset.
        manifest = self._read_snapshot_stage_manifest(table, stage_scope, schema_id)
        if manifest is None or int(manifest["snapshot_lsn"]) != snapshot_lsn:
            return None
        self._note_moved_rows(table, stage_scope, schema_id, manifest)
        page_count = int(manifest["page_count"])
        if start_page < 0 or start_page >= page_count:
            return None
        end_page = min(start_page + self._partitioned_pages_per_batch(), page_count)
        return (stage_scope, schema_id, snapshot_lsn, start_page, end_page, page_count)

    def _note_moved_rows(
        self, table: Table, scope: str, schema_id: str, manifest: dict[str, Any]
    ) -> dict[str, Any] | None:
        # Docstring: Keep, for the page descriptors, what a snapshot's manifest records of
        # the rows its index-ordered scan read again (see _index_scan_snapshot); returns it.
        #
        # Every way a batch of staged pages comes about has just read the manifest, so
        # planning the batch's descriptors need not read it once more.
        moved = (manifest.get("index_scan") or {}).get("moved")
        self.__dict__.setdefault("_moved_rows", {})[
            (table.identity, scope, schema_id, int(manifest["snapshot_lsn"]))
        ] = moved
        return moved

    def _bootstrap_page_batch(
        self, table: Table, effective_start: dict, end_offset: dict, table_options: dict[str, str]
    ) -> tuple[str, str, int, int, int, int] | None:
        # Docstring: Resolve a table's first microbatch as a serve of staged page 0, else None.
        #
        # The first microbatch drains the snapshot to the stage, then serves page 0 from it:
        # an immutable page an executor can stream like every later page. Embedded in the
        # descriptor instead, the executor parses the whole page at once, which passes
        # serverless's 1 GB Python limit for a wide table. Recognised from the (start, end)
        # pair alone, so a batch re-planned after a driver restart is served the same way:
        # an empty start, and the end _staged_snapshot_result emits after page 0 of the
        # manifest pinned at that LSN. Same tuple as _snapshot_page_batch. So is
        # auto_snapshot's resnapshot of an expired checkpoint, whose start is that checkpoint:
        # re-planned in a later update, it took another snapshot, newer than the end Spark
        # goes on from.

        if not end_offset or "incremental" in end_offset:
            return None
        if effective_start and not _resnapshot_range(effective_start, end_offset):
            return None
        if self._is_delete_flow() or self._table_migration(table_options):
            return None
        try:
            end = _validated_offset(end_offset)
        except Exception:  # pylint: disable=broad-except
            return None
        lsn = int(end["commit_lsn"])
        # _staged_snapshot_result pins every LSN of either end at the snapshot boundary.
        if {int(end["begin_lsn"]), int(end["change_lsn"])} != {lsn}:
            return None
        if end.get("tx_id") is not None:
            return None
        if end["phase"] == "snapshot":
            snapshot = end.get("snapshot")
            if "stage_scope" in end or not isinstance(snapshot, dict):
                return None
            if snapshot.get("page_index") != 1 or int(end.get("snapshot_lsn", -1)) != lsn:
                return None
        scope, schema_id = str(end["pipeline_scope"]), str(end["schema_id"])
        manifest = self._read_snapshot_stage_manifest(table, scope, schema_id)
        if manifest is None or int(manifest["snapshot_lsn"]) != lsn:
            return None
        self._note_moved_rows(table, scope, schema_id, manifest)
        page_count = int(manifest["page_count"])
        # A snapshot-phase end leaves pages to serve; a stream end served the only page, or
        # none for an empty table. Recognising that here keeps a re-plan in a later update
        # from falling through to a replay, which would snapshot the table afresh.
        if (page_count > 1) != (end["phase"] == "snapshot") or page_count < 0:
            return None
        return (scope, schema_id, lsn, 0, min(1, page_count), page_count)

    def _staged_first_batch_start(
        self, table_name: str, end_offset: dict, table_options: dict[str, str]
    ) -> dict | None:
        # Docstring: The start that replays a one-page or empty staged snapshot's first
        # batch from its stage, else None.
        #
        # The simple reader's counterpart of _bootstrap_page_batch. A first batch that
        # staged more than one page ends in the snapshot phase, and _replay_between_offsets
        # serves it from page 0 on its own; one that staged one page or none ends in the
        # stream phase, like a first batch that staged nothing. Replayed in a later update
        # from no position, it took another snapshot, newer than the end Spark goes on
        # from: the changes between the two came once more on top of it, a keyless table's
        # inserts twice and its deletes of rows the new snapshot lacks. Recognised as
        # _bootstrap_page_batch does, by the manifest pinned at the end's LSN.

        if not isinstance(end_offset, dict) or end_offset.get("phase") != "stream":
            return None
        if "incremental" in end_offset or "staged_scope" in end_offset:
            return None
        table_options = _options_set(table_options)
        if (
            self._is_delete_flow()
            or self._table_migration(table_options)
            or self._table_snapshot_source(table_options)
        ):
            return None
        try:
            end = _validated_offset(end_offset)
        except Exception:  # pylint: disable=broad-except
            return None
        lsn = int(end["commit_lsn"])
        if {int(end["begin_lsn"]), int(end["change_lsn"])} != {lsn} or end.get("tx_id") is not None:
            return None
        scope, schema_id = str(end["pipeline_scope"]), str(end["schema_id"])
        with self._metadata_access():
            table = self._table(table_name, table_options)
            manifest = self._read_snapshot_stage_manifest(table, scope, schema_id)
        if manifest is None or int(manifest["snapshot_lsn"]) != lsn:
            return None
        if int(manifest["page_count"]) > 1:
            return None
        # The offset the first batch's read began the serve from: what the stream end
        # adds for the scan is the serve's to add again.
        start = {k: v for k, v in end_offset.items() if k not in ("scan_scope", "scan_schema")}
        start.update(
            {
                "phase": "snapshot",
                "snapshot_lsn": str(lsn),
                "snapshot": {"last_pk": [], "page_index": 0},
            }
        )
        return start

    def _embedded_read(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        # Source names: read_partition renames every partition's rows as they leave.
        if self._is_delete_flow():
            return self._read_deletes(table_name, start_offset, table_options)
        return self._read_upserts(table_name, start_offset, table_options)

    def _served_offset(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict,
        effective_start: dict,
    ) -> dict | None:
        # Docstring: The end of a ``.unl`` split window or staged page batch, else None.
        #
        # Both serves plan from metadata without a read, so they run under _metadata_access:
        # no wait behind running reads for a slot, and the session a catalog lookup opened
        # is given back whether planning succeeds or fails; the ``.unl`` boundary pin takes a
        # slot (see below). A
        # delete flow or a stream-phase offset never serves, so it skips the lookup, which
        # for a delete reader would take a slot its upsert reader may need. A dropped Volume
        # mount is retried in place (_plan_with_mount_retry).

        unl = self._unl_snapshot_serve(table_options, effective_start, table_name)
        if not unl and (
            self._is_delete_flow()
            or not effective_start
            or effective_start.get("phase") not in (None, "snapshot")
            # Migration records and stops on the read path, which owns a seed's marker.
            or self._table_migration(table_options)
        ):
            return None
        if not self._is_delete_flow():
            # Planned here rather than read, so published here, as _read_upserts does.
            self._publish_checkpoint_channel_start_before_capacity(
                table_name, effective_start, table_options
            )

        def plan() -> dict | None:
            if unl and not effective_start:
                # The first window pins the extract's boundary, which opens CDC sessions on
                # the source. Planned as metadata, the first windows of a pipeline's .unl
                # tables opened them all at once, past max.concurrent.connections: it takes a
                # slot as a read does, and a continuous flow that gets none yields, as a read
                # does.
                try:
                    with self._capacity_attempt(start_offset):
                        return serve(self._table(table_name, table_options))
                except ConnectionCapacityUnavailable:
                    if self._trigger_available_now:
                        raise
                    return self._capacity_retry_offset(start_offset)
                finally:
                    self._release_worker_connection()
            with self._metadata_access():
                table = self._table(table_name, table_options)
                expected = effective_start.get("schema_fingerprint") if effective_start else None
                if expected and effective_start.get("phase") == "snapshot":
                    table = self._snapshot_schema_held(table, expected)
                batch = None
                if not unl:
                    batch = self._snapshot_page_batch(table, effective_start, table_options)
                    if batch is None:
                        # The read path serves it, and drops a restored seed's tokens itself.
                        return None
                if effective_start.get("handoff_seeded"):
                    # The first post-Restore read of a seed taken mid-snapshot is served
                    # here, not by _read_table_attempt, so drop the cutover's tokens here.
                    self._drop_orphaned_handoff_tokens(
                        table, [_HANDOFF_CHANNEL_UPSERT, _HANDOFF_CHANNEL_DELETE]
                    )
                if batch is not None:
                    return self._snapshot_page_batch_offset(table, effective_start, batch)
                return serve(table)

        def serve(table: Table) -> dict:
            # File source: serve the extract as a window of splits per microbatch. Leaving
            # the embedded cache untouched steers the following get_partitions to the
            # unl_split fan-out instead of the driver-side embedded path. A gzip record
            # window is the exception: its rows were read here on the driver, so they are
            # cached for get_partitions exactly as an embedded read's are.
            end, rows = self._unl_snapshot_offset(table, table_options, effective_start)
            if rows is not None:
                self._partition_embedded_rows.clear()
                self._partition_embedded_rows[self._partition_offset_key(start_offset)] = (
                    self._encode_embedded_output(rows, table_options, lambda: table.columns)
                )
            return end

        return self._plan_with_mount_retry(plan)

    @staticmethod
    def _plan_with_mount_retry(  # pylint: disable=inconsistent-return-statements
        plan: Callable[..., Any], *args: Any
    ) -> Any:
        # Docstring: Plan a staged-page or ``.unl`` microbatch, retrying a dropped mount in place.
        #
        # The plan reads the manifest, staged pages and extract files (and, for a ``.unl``
        # boundary pin, the log bounds), so running it again is safe. It cannot answer a
        # dropped Volume mount with an empty batch: returning the start offset from
        # latest_offset would end a triggered update early, and get_partitions must serve
        # the range latest_offset logged. So it tries again with backoff, and the last
        # attempt's error fails the batch, which Spark plans again from the same offsets.

        for attempt_index in itertools.count():
            try:
                return plan(*args)
            except Exception as error:  # pylint: disable=broad-except
                transient = isinstance(error, SharedStateAccessUnavailable) or _dropped_mount_error(
                    error
                )
                if not transient or attempt_index + 1 >= _TRANSIENT_VOLUME_IN_PROCESS_ATTEMPTS:
                    raise
                delay = min(0.25 * (2**attempt_index), _TRANSIENT_VOLUME_IN_PROCESS_MAX_SECONDS)
                time.sleep(random.uniform(delay / 2, delay))

    def latest_offset(
        self,
        table_name: str,
        table_options: dict[str, str],
        start_offset: dict | None = None,
    ) -> dict:
        table_options = _options_set(table_options)
        start_offset = start_offset or {}
        self._scope_from_checkpoint(start_offset)
        effective_start = self._effective_start_offset(start_offset)
        end = self._served_offset(table_name, table_options, start_offset, effective_start)
        if end is not None:
            return end
        # A first batch: from no position, or auto_snapshot's resnapshot of an expired
        # checkpoint, which starts from it. Read on the driver, that one's page 0 was cached
        # here and then served from the stage as well (_bootstrap_page_batch).
        self._defer_staged_rows = not effective_start or effective_start.get("phase") == "stream"
        try:
            rows, next_offset = self._embedded_read(table_name, start_offset, table_options)
        finally:
            self._defer_staged_rows = False
        # F1: cache the JSON-safe encoded rows (they are the descriptor payload).
        # F6: keep only this microbatch's entry, so a latest_offset call Spark never
        # follows with get_partitions (speculative discovery, or an empty start==end
        # batch) cannot leak driver memory. A later cache miss recomputes -- safe
        # because partitioned mode requires the shared CDC session, whose reads are
        # idempotent for a committed start.
        self._partition_embedded_rows.clear()
        self._partition_page_batches.clear()
        if isinstance(rows, _DeferredStagedPage):
            # get_partitions serves page 0 from the stage, so the driver never reads it.
            self._partition_page_batches[self._partition_offset_key(start_offset)] = rows.page_batch
            return next_offset

        def columns():
            return self._table(table_name, table_options).columns

        self._partition_embedded_rows[self._partition_offset_key(start_offset)] = (
            # A spilled transaction stays on its pages, which get_partitions hands to executors.
            rows
            if isinstance(rows, _SpilledRows)
            else self._encode_embedded_output(rows, table_options, columns)
        )
        return next_offset

    def _snapshot_page_batch_offset(
        self,
        table: Table,
        effective_start: dict,
        batch: tuple[str, str, int, int, int, int],
    ) -> dict:
        # Docstring: Build the end offset for a page-serve microbatch spanning [start,end) pages.
        #
        # Mirrors :meth:`_staged_snapshot_result`'s offset construction: a
        # snapshot-phase offset that advances ``page_index`` to ``end_page`` and
        # carries the boundary page's ``upper_pk`` as ``last_pk``, or the stream-phase
        # transition offset once the last page is served. The schema fingerprint is
        # carried forward from the checkpoint rather than recomputed from a live
        # catalog read -- staged pages are frozen as-of ``snapshot_lsn`` so a
        # mid-snapshot schema change cannot invalidate them, and drift is still caught
        # at the snapshot->CDC transition by ``_read_stream``. In the (overwhelming)
        # no-drift case this is byte-identical to what the simple reader commits.

        stage_scope, schema_id, snapshot_lsn, start_page, end_page, page_count = batch
        expected_fingerprint = effective_start.get("schema_fingerprint")
        # The flow owns its own scope for the emitted offsets; stage_scope (where the
        # staged pages physically live) drives only the page reads and cleanup, and is
        # carried forward on the non-terminal offset until the snapshot completes.
        flow_scope = self._offset_scopes(effective_start)[0]
        # Pages below this batch's first page are committed -- schedule their async
        # cleanup under the scope that stores them, exactly as the serial serve does.
        self._cleanup_consumed_snapshot_stage_pages(
            table, stage_scope, schema_id, snapshot_lsn, start_page
        )
        if end_page >= page_count:
            # Snapshot complete: transition to the stream phase under the flow's own
            # scope and drop stage_scope -- the offset is now an ordinary stream offset.
            end = _offset(
                snapshot_lsn,
                snapshot_lsn,
                snapshot_lsn,
                None,
                "stream",
                table,
                schema_id,
                flow_scope,
            )
            if expected_fingerprint is not None:
                end["schema_fingerprint"] = expected_fingerprint
            return self._naming_scan(end, table, stage_scope, schema_id)
        # Non-terminal: last_pk is the boundary page's upper_pk. Read only that one
        # page's header; its row generator is discarded, so no rows are decoded.
        header, _rows = self._stream_staged_page(
            table, stage_scope, schema_id, snapshot_lsn, end_page - 1
        )
        last_pk = header.get("upper_pk")
        end = _offset(
            snapshot_lsn, snapshot_lsn, snapshot_lsn, None, "snapshot", table, schema_id, flow_scope
        )
        if expected_fingerprint is not None:
            end["schema_fingerprint"] = expected_fingerprint
        end.update(
            {
                "snapshot_lsn": str(snapshot_lsn),
                "snapshot": {"last_pk": last_pk, "page_index": end_page},
            }
        )
        if stage_scope != flow_scope:
            end["stage_scope"] = stage_scope
        return _with_backlog_streak(end, effective_start, True)
