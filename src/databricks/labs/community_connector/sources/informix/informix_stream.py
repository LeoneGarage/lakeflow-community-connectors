"""The CDC stream: spills, shared sessions, snapshot drain workers and stream batches."""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import math
import os
import secrets
import time
import types
from typing import Any, Callable, Sequence

from databricks.labs.community_connector.sources.informix.informix_bridge import (
    PurePythonInformixBridge,
)
from databricks.labs.community_connector.sources.informix.informix_host_sharing import (
    _capture_descriptor,
    _database_encoding,
    _host_shard_poll,
    _recover,
    _snapshot_drain_pool,
    _transaction_batch,
)
from databricks.labs.community_connector.sources.informix.informix_keys import (
    _PROMOTE_UNIQUE_INDEX_OPTION,
    _coerce_variable_decimal_values,
    _null_byte_mode,
    _option_bool,
    _resolve_snapshot_reader_threads,
    _snapshot_scan_value,
)
from databricks.labs.community_connector.sources.informix.informix_offsets import (
    _validated_offset,
    _with_backlog_streak,
)
from databricks.labs.community_connector.sources.informix.informix_options import (
    _CDC_SPILL_LIST_SECONDS,
    _CDC_SPILL_RECORD_TYPE,
    _CDC_SPILL_RECORDS_OPTION,
    _CDC_SPILL_SCHEMA,
    _CONNECTION_ATTEMPT_BUDGET_OPTION,
    _CONNECTION_ATTEMPT_RANK_OPTION,
    _DAEMON_SLOT_MARKER_OPTION,
    _DEFAULT_CDC_MAX_RECORDS,
    _DEFAULT_CDC_SPILL_RECORDS,
    _DEFAULT_CONNECTION_WAIT_TIMEOUT_SECONDS,
    _DEFAULT_MAX_CONCURRENT_CONNECTIONS,
    _DEFAULT_MAX_RECORDS_PER_BATCH,
    _DEFAULT_SHARED_CDC_BUFFER,
    _DEFAULT_SNAPSHOT_ISOLATION,
    _LAKEBASE_RECONNECT_BASE_SECONDS,
    _LAKEBASE_RECONNECT_MAX_RETRIES,
    _LAKEBASE_RECONNECT_MAX_SECONDS,
    _PIPELINE_SCOPE,
    _REPLAY_CONNECTION_WAIT_BUDGET_OPTION,
    _REPLAY_UNBOUNDED_ROWS,
    _SHARED_CDC_BUFFER_OPTION,
    _SHARED_CDC_THREADS_OPTION,
    _SHARED_STATE_WAIT_SECONDS,
    _SHARED_VIEW_WAIT_MARGIN_SECONDS,
    _SHARED_VIEW_WAIT_POLL_SECONDS,
    _SHARED_VIEW_WAIT_TIMEOUTS,
    _SNAPSHOT_DAEMON_SLOT_MARKER_OPTION,
    _SNAPSHOT_DRAIN_STALL_GRACE_SECONDS,
    _SNAPSHOT_ISOLATION_OPTION,
    _SNAPSHOT_ISOLATION_SQL,
    _SNAPSHOT_RECONNECT_BASE_SECONDS,
    _SNAPSHOT_RECONNECT_MAX_RETRIES,
    _SNAPSHOT_RECONNECT_MAX_SECONDS,
    _SNAPSHOT_SCAN_CURSOR,
    _SNAPSHOT_SCAN_KEYSET,
    _SNAPSHOT_SCAN_OPTION,
    _SPILL_DELETE_OPS,
    _SPILL_RUN_NAME,
    _T,
    InformixError,
    _m,
)
from databricks.labs.community_connector.sources.informix.informix_registration import (
    ConnectionCapacityUnavailable,
    LogRetentionError,
    Table,
)
from databricks.labs.community_connector.sources.informix.informix_shards import (
    _SHARED_CDC_SESSION_HOST,
    _project_transaction,
    _schema_fingerprint,
    _shared_cdc_mode,
    _shared_cdc_shard,
    _SpilledRows,
)
from databricks.labs.community_connector.sources.informix.informix_transactions import (
    CommittedTransaction,
    _CdcSpill,
    _is_path,
    _record_matches,
    _spill_run_consumed,
    _spill_scope,
)
from databricks.labs.community_connector.sources.informix.informix_unload import _offset
from databricks.labs.community_connector.sources.informix.lakebase_state import (
    connection_lost_error_types,
    delete_spill_run,
    list_spill_runs,
    merge_spill_run,
    publish_state_record,
)
from databricks.labs.community_connector.sources.informix.sqli_codec import (
    SqliProtocolError,
    reconnectable_error,
)


class _StreamMixin:
    # Docstring: The CDC stream: spills, shared sessions, snapshot drain workers and stream
    # batches.

    def _cdc_spill(self, captures: Sequence[dict[str, Any]]) -> _CdcSpill | None:
        # Docstring: The spill writer for one poll over ``captures``, or None when
        # cdc.spill.records is 0. Pages go to the staging Volume under the pipeline's spill
        # scope (see _spill_scope), in a "cdc-spill" stage the stale-stage sweep also clears.

        threshold = int(self.options.get(_CDC_SPILL_RECORDS_OPTION, _DEFAULT_CDC_SPILL_RECORDS))
        scope = _spill_scope(getattr(self, "_registration_scope", None))
        if (
            threshold <= 0
            or not getattr(self, "_snapshot_staging_location", None)
            # A reader without a scope can't name pages another process finds: it keeps
            # every transaction in memory instead of failing the read.
            or scope is None
        ):
            return None
        natives = {str(c["logical_identity"]): str(c["identity"]) for c in captures}
        # Each table's run is named by its own capture, so the upsert and delete channels,
        # whose shards capture different sets of tables, name the same pages.
        # And by string.null.byte, applied before a change is spilled: a restart under
        # another one took up the pages of the first.
        null_byte = _null_byte_mode(self.options)
        layouts = {
            str(c["logical_identity"]): hashlib.sha256(
                json.dumps([c, null_byte], sort_keys=True, default=str).encode("utf-8")
            ).hexdigest()[:12]
            for c in captures
        }
        # Whether each run this poll writes is consumed already, asked once per run.
        consumed: dict[str, bool] = {}

        def write(native: str, name: str, run: str, page_index: int, records: list) -> None:
            target = types.SimpleNamespace(
                native_identity=native, exposed_name=name, primary_keys=()
            )
            self._maybe_sweep_stale_snapshot_stages(target)
            if run not in consumed:
                # A poll writes a run from its first page, whose first record is the
                # table's first change in the transaction.
                first = records[0]["lsn"] if page_index == 0 and records else None
                consumed[run] = self._register_spill_run(native, scope, run, first)
            if consumed[run]:
                # Every channel has committed past this transaction, so no batch reads these
                # pages: a poll that reads it again (a shard reading below it for another
                # table) writes none.
                return
            # The upsert and delete channels read the same transaction under the same
            # capture and scope, so they name the same pages with the same contents: a page
            # the other channel has written already is kept, not encoded and written again.
            # Both are written atomically, so one that exists is whole.
            namespace = self._snapshot_stage_namespace_for(native, scope, _CDC_SPILL_SCHEMA)
            page = f"page-{page_index:08d}"
            try:
                if _is_path(os.path.join(namespace, "runs", run, page, "data.json.gz")):
                    return
            except OSError:
                # Written over REST, the page needs no mount, which only says whether it
                # is there already: a dropped one failed the shard's poll. It is written
                # again, with the same contents.
                if self._staging_transport() is None:
                    raise
            self._publish_snapshot_stage_page(
                target, scope, _CDC_SPILL_SCHEMA, run, page_index, records, None
            )

        def delete(native: str, run: str) -> None:
            # A rolled-back transaction: its pages and its record go.
            path = os.path.join(
                self._snapshot_stage_namespace_for(native, scope, _CDC_SPILL_SCHEMA), "runs", run
            )
            with contextlib.suppress(OSError):
                PurePythonInformixBridge._remove_connection_slot_tree(path)
            key = self._spill_run_key(native, scope, run)
            self._spill_run_state(
                run,
                lambda: delete_spill_run(
                    self._lakebase_connection(), self._lakebase_state_namespace(), key
                ),
            )

        def commit(native: str, run: str, commit_lsn: int) -> None:
            # The commit LSN finishes the run for each channel whose checkpoint reaches it.
            record = {**self._spill_run_record(native, scope, run), "commit_lsn": str(commit_lsn)}
            key = self._spill_run_key(native, scope, run)
            merged = self._spill_run_state(
                run,
                lambda: merge_spill_run(
                    self._lakebase_connection(), self._lakebase_state_namespace(), key, record
                ),
            )
            if merged is not None and _spill_run_consumed(merged):
                # The last channel finished the run while this poll wrote it (it asked at
                # its first page), so the pages written since have no reader.
                path = os.path.join(
                    self._snapshot_stage_namespace_for(native, scope, _CDC_SPILL_SCHEMA),
                    "runs",
                    run,
                )
                self._snapshot_cleanup_executor().submit(self._delete_spill_pages, path)

        return _CdcSpill(threshold, write, delete, layouts, scope, natives, commit)

    def _spill_run_key(self, native_identity: str, scope: str, run: str) -> str:
        # Docstring: A run's Lakebase record key, under its table's state prefix so the
        # table-state GC removes it with the table.

        return "/".join(
            (*self._table_state_keys_for(native_identity), _CDC_SPILL_SCHEMA, scope, run)
        )

    @staticmethod
    def _spill_run_record(native_identity: str, scope: str, run: str) -> dict[str, Any]:
        return {
            "format_version": 1,
            "table": native_identity,
            "spill_scope": scope,
            "run": run,
            "begin_lsn": run.split("-")[1],
            "done": {},
        }

    def _spill_run_state(self, run: str, thunk):
        # Docstring: One Lakebase update of a run's record, or None when it fails.
        #
        # Best-effort: without the record the pages are written all the same, and only their
        # cleanup falls to the retention sweep.

        try:
            return self._state_op_with_reconnect(thunk, operation="spilled-run record")
        except Exception:  # pylint: disable=broad-except
            # 429: Cannot update spilled CDC run {} in Lakebase; its pages are left for the
            # retention sweep
            logging.getLogger(__name__).warning(_m(429, run), exc_info=True)
            return None

    def _register_spill_run(self, native_identity: str, scope: str, run: str, first=None) -> bool:
        # Docstring: Record a run a poll writes in Lakebase, with the LSN of its first change
        # when known; report whether it is consumed.

        record = self._spill_run_record(native_identity, scope, run)
        if first is not None:
            record["first_lsn"] = str(first)
        key = self._spill_run_key(native_identity, scope, run)
        elected = self._spill_run_state(
            run,
            lambda: publish_state_record(
                self._lakebase_connection(),
                self._lakebase_state_namespace(),
                key,
                record,
                record_type=_CDC_SPILL_RECORD_TYPE,
            ),
        )
        return elected is not None and _spill_run_consumed(elected)

    def _note_spilled_runs(self, spilled, tx: CommittedTransaction, deletes: bool) -> None:
        # Docstring: Remember the runs of a spilled transaction this channel serves, by commit.

        channel = "delete" if deletes else "upsert"
        for part in spilled:
            runs = self._spill_runs.setdefault((str(part["native_identity"]), channel), {})
            entry = (str(part["pipeline_scope"]), tx.begin_lsn, tx.commit_lsn, None)
            runs[str(part["run"])] = entry

    # pylint: disable-next=too-many-locals
    def _settle_spilled_runs(
        self, table: Table, checkpoint: dict, options, deletes: bool, current_lsn=None
    ) -> None:
        # Docstring: Mark the spilled runs this channel has committed past, in Lakebase, and
        # delete the pages of each run whose last channel this is.
        #
        # Spark gives a reader only a start it has committed, and _recover never serves a
        # transaction at or below it, so a run is finished for this channel once the start's
        # commit LSN reaches its transaction's commit: the commit of a run this reader served,
        # or the commit LSN a poll recorded at the COMMIT. The restart LSN proves nothing, past
        # the BEGIN or the first change alike: Informix hands a channel a transaction whose
        # BEGIN is below it when the first captured change came later, and a stream started at
        # a snapshot or delete-bootstrap boundary restarts at the boundary while a shared shard
        # reading from below it still hands over a transaction open across it. So a run no
        # poll saw commit (an update stopped inside it) waits for a later COMMIT or ROLLBACK,
        # or for its record to expire, unless its first change names a logical log ahead of
        # the source's current one: the source reinitialized its log, and every checkpoint
        # from before that fails (error 284). ``current_lsn`` samples that position, and is
        # called only after a listing, so after every listed record existed: an earlier
        # sample can name an older log than a live run's first change. A read served from a
        # shard view passes none, since the view's position predates the read that published
        # it, so there such a run waits for a direct read or its expiry. Runs are listed from
        # Lakebase at the first read and every _CDC_SPILL_LIST_SECONDS, which also expires the
        # table's old records of every pipeline, their stale pages with them, so a channel that
        # read a transaction in memory finishes the other shard's run too. Not called on a replay,
        # whose later polls start at positions Spark has not committed. The table's channels
        # are the upsert one and, unless it is append-only, the delete one: the connector
        # already serves one of each per table per pipeline.

        scope = _spill_scope(getattr(self, "_registration_scope", None))
        if scope is None or not getattr(self, "_snapshot_staging_location", None):
            return
        native = table.native_identity
        channel = "delete" if deletes else "upsert"
        pending = self._spill_runs.setdefault((native, channel), {})
        now = time.monotonic()
        due = now - self._spill_runs_listed.get((native, channel), -math.inf)
        due = due >= _CDC_SPILL_LIST_SECONDS
        if not due and not pending:
            return
        namespace = self._lakebase_state_namespace()
        # Set while the sampler runs: it is an Informix query, so its errors (a lost capacity
        # lease included) go to the read like any bridge call's, not to the Lakebase handler.
        sampling = False
        try:
            if due:
                prefix = "/".join((*self._table_state_keys_for(native), _CDC_SPILL_SCHEMA))
                retention = self._snapshot_staging_retention_seconds
                listed, expired = self._state(
                    list_spill_runs,
                    namespace,
                    prefix,
                    scope,
                    channel,
                    retention,
                    operation="spilled-run listing",
                )
                self._spill_runs_listed[(native, channel)] = now
                for record in expired:
                    # Only a record of this table, with a scope and run of the shapes the
                    # connector writes, names pages to delete.
                    run_scope, run = str(record.get("spill_scope")), str(record.get("run"))
                    if (
                        record.get("table") == native
                        and _PIPELINE_SCOPE.fullmatch(run_scope)
                        and _SPILL_RUN_NAME.fullmatch(run)
                    ):
                        path = os.path.join(
                            self._snapshot_stage_namespace_for(
                                native, run_scope, _CDC_SPILL_SCHEMA
                            ),
                            "runs",
                            run,
                        )
                        self._snapshot_cleanup_executor().submit(
                            self._remove_stale_stage_run, path, time.time() - retention
                        )
                runs = {
                    str(record["run"]): (
                        scope,
                        int(record["begin_lsn"]),
                        int(record["commit_lsn"]) if record.get("commit_lsn") else None,
                        int(record["first_lsn"]) if record.get("first_lsn") else None,
                    )
                    for record in listed
                    if _SPILL_RUN_NAME.fullmatch(str(record.get("run")))
                }
                for run, entry in list(pending.items()):
                    # A listed run gone since (rolled back, finished, expired) is dropped; a
                    # served one keeps the commit it was served with.
                    if entry[2] is None and run not in runs:
                        del pending[run]
                for run, entry in runs.items():
                    if run not in pending or pending[run][2] is None:
                        pending[run] = entry
            commit = int(checkpoint["commit_lsn"])
            log = None
            if (
                due
                and current_lsn is not None
                and any(entry[2] is None and entry[3] is not None for entry in pending.values())
            ):
                sampling = True
                log = int(current_lsn()) >> 32
                sampling = False
            finished = [
                (run, entry)
                for run, entry in pending.items()
                if (entry[2] is not None and entry[2] <= commit)
                or (entry[2] is None and None not in (entry[3], log) and entry[3] >> 32 > log)
            ]
            if not finished:
                return
            required = (
                ["upsert"] if self._append_only_table(table, options) else ["upsert", "delete"]
            )
            for run, (run_scope, *_) in finished:
                record = {
                    **self._spill_run_record(native, run_scope, run),
                    "done": {channel: True},
                    "required": required,
                }
                key = self._spill_run_key(native, run_scope, run)
                marked = self._state(
                    merge_spill_run,
                    namespace,
                    key,
                    record,
                    operation="spilled-run settlement",
                )
                del pending[run]
                if _spill_run_consumed(marked):
                    path = os.path.join(
                        self._snapshot_stage_namespace_for(native, run_scope, _CDC_SPILL_SCHEMA),
                        "runs",
                        run,
                    )
                    self._snapshot_cleanup_executor().submit(self._delete_spill_pages, path)
        except Exception:  # pylint: disable=broad-except
            if sampling:
                raise
            # 430: Cannot update the spilled CDC runs of '{}' in Lakebase; trying again on the
            # next batch
            logging.getLogger(__name__).warning(_m(430, table.exposed_name), exc_info=True)

    def _delete_spill_pages(self, path: str) -> None:
        # Docstring: Delete a consumed run's pages, then the run once nothing else is in it.
        #
        # A candidate page another writer is still renaming into place keeps the run, for
        # the retention sweep.

        try:
            names = os.listdir(path)
        except OSError:
            return
        for name in names:
            if name.startswith("page-"):
                self._delete_snapshot_page(os.path.join(path, name))
        with contextlib.suppress(OSError):
            os.rmdir(path)

    def _snapshot_read_with_reconnect(self, thunk, *, table: Table, operation: str):
        # Docstring: Run one snapshot page or key-bound read, retrying in place on a drop.
        #
        # A connection dropped mid-read raises ``SqliTransportError`` ("truncated
        # SQLI stream ..."), a subclass of ``SqliProtocolError``; the enclosing
        # repeatable-read transaction's rollback then fails because the session is
        # already gone. The read has advanced no offset -- the incremental cursor is
        # committed only after the read returns, and each read (``max_primary_key``,
        # ``snapshot_chunk``, the keyless one-shot ``snapshot_page``) opens its own
        # repeatable-read transaction -- so the dead transport can be reset and the
        # identical read reissued under the connection slot already held. Bounded
        # attempts with short backoff absorb a transient blip; exhausting them
        # re-raises so a genuinely-down source still fails honestly. This is the
        # snapshot counterpart of ``_read_changes_with_reconnect``.
        #
        # A deliberate lease-loss shutdown (``ConnectionCapacityUnavailable`` or a
        # set ``_connection_lease_lost``) is never retried; it propagates.

        attempt = 0
        while True:
            try:
                return thunk()
            except ConnectionCapacityUnavailable:
                raise
            except (SqliProtocolError, OSError) as error:
                if not reconnectable_error(error):
                    raise
                lease_lost = getattr(self._bridge, "_connection_lease_lost", None)
                if lease_lost is not None and lease_lost.is_set():
                    # The drop is a deliberate lease-loss shutdown, not a blip.
                    raise
                if attempt >= _SNAPSHOT_RECONNECT_MAX_RETRIES:
                    raise
                delay = min(
                    _SNAPSHOT_RECONNECT_BASE_SECONDS * (2**attempt),
                    _SNAPSHOT_RECONNECT_MAX_SECONDS,
                )
                # 278: Informix snapshot {} for '{}' lost its connection ({}); resetting the
                # transport and retrying the same read (attempt {}/{}) after {:.1f}s
                logging.getLogger(__name__).warning(
                    _m(
                        278,
                        operation,
                        table.exposed_name,
                        error,
                        attempt + 1,
                        _SNAPSHOT_RECONNECT_MAX_RETRIES,
                        delay,
                    ),
                    exc_info=True,
                )
                reset = getattr(self._bridge, "reset_transport", None)
                if reset is not None:
                    reset()
                time.sleep(delay)
                attempt += 1

    def _state(self, function: Callable[..., _T], *args, operation: str, **kwargs) -> _T:
        # Docstring: ``function(state connection, *args, **kwargs)``, through
        # _state_op_with_reconnect: the connection is fetched again on each attempt.

        return self._state_op_with_reconnect(
            lambda: function(self._lakebase_connection(), *args, **kwargs), operation=operation
        )

    def _state_op_with_reconnect(self, thunk: Callable[[], _T], *, operation: str) -> _T:
        # Docstring: Run one Lakebase state read/write, retrying in place on a dropped connection.
        #
        # A state connection reaped while idle (typically across a long wait for
        # connection capacity) is handed back by ``_lakebase_connection`` because
        # psycopg leaves ``.closed`` False until a query fails; the next
        # ``cursor.execute`` then raises ``OperationalError``/``InterfaceError``.
        # The operation committed nothing (the failure precedes ``commit``) and is
        # idempotent, so the dead handle is dropped and the identical operation
        # reissued on a fresh connection. ``thunk`` must re-fetch
        # ``self._lakebase_connection()`` each call so the retry uses the reopened
        # handle. Bounded attempts with short backoff absorb a transient reap;
        # exhausting them re-raises. Postgres counterpart of
        # ``_snapshot_read_with_reconnect``.
        #
        # Only a genuinely-dead connection is retried: after the error the cached
        # handle must report closed. A failure on a still-live connection (e.g. a
        # serialization conflict, also an ``OperationalError`` subclass) is not a
        # drop and propagates unchanged.

        connection_lost = connection_lost_error_types()
        if not connection_lost:
            return thunk()
        attempt = 0
        while True:
            try:
                return thunk()
            except Exception as error:
                connection = getattr(self, "_lakebase_conn", None)
                try:
                    still_alive = connection is not None and not connection.closed
                except Exception:  # pragma: no cover - a broken handle counts as dead
                    still_alive = False
                if (
                    still_alive
                    or not isinstance(error, connection_lost)
                    or attempt >= _LAKEBASE_RECONNECT_MAX_RETRIES
                ):
                    if still_alive:
                        # The failed operation's transaction, aborted, or left open by an
                        # error raised after a statement ran: rolled back here, on the thread
                        # that ran it, it neither holds its locks nor commits with the next
                        # operation on the connection.
                        with contextlib.suppress(Exception):
                            connection.rollback()
                    raise
                delay = min(
                    _LAKEBASE_RECONNECT_BASE_SECONDS * (2**attempt),
                    _LAKEBASE_RECONNECT_MAX_SECONDS,
                )
                # 279: Informix Lakebase state {} lost its connection ({}); reconnecting and
                # retrying the same operation (attempt {}/{}) after {:.1f}s
                logging.getLogger(__name__).warning(
                    _m(279, operation, error, attempt + 1, _LAKEBASE_RECONNECT_MAX_RETRIES, delay),
                    exc_info=True,
                )
                self._reset_lakebase_connection()
                time.sleep(delay)
                attempt += 1

    def _snapshot_cursor_scan(self, options: dict[str, str]) -> bool:
        # Docstring: Whether ``snapshot.scan=cursor`` asks for an unordered one-pass snapshot.
        #
        # The table option wins over connection scope; the default is ``keyset``. See
        # ``_SNAPSHOT_SCAN_OPTION``.

        raw = options.get(
            _SNAPSHOT_SCAN_OPTION, self.options.get(_SNAPSHOT_SCAN_OPTION, _SNAPSHOT_SCAN_KEYSET)
        )
        return _snapshot_scan_value(raw) == _SNAPSHOT_SCAN_CURSOR

    def _snapshot_isolation(self, options: dict[str, str]) -> str:
        # Docstring: Resolve the ``snapshot.isolation`` table option to its SQL clause.
        #
        # Defaults to COMMITTED READ LAST COMMITTED (non-locking; see the README
        # duplicate warning). Tokens are whitespace/hyphen-insensitive; an unknown
        # value fails loudly rather than silently falling back.

        token = (
            str(options.get(_SNAPSHOT_ISOLATION_OPTION, _DEFAULT_SNAPSHOT_ISOLATION))
            .strip()
            .lower()
            .replace("-", "_")
            .replace(" ", "_")
        )
        try:
            return _SNAPSHOT_ISOLATION_SQL[token]
        except KeyError:
            # 160: Option '{}' must be one of {}; got {!r}
            raise ValueError(
                _m(160, _SNAPSHOT_ISOLATION_OPTION, sorted(_SNAPSHOT_ISOLATION_SQL), token)
            ) from None

    def _is_snapshot_drain_worker(self) -> bool:
        # Docstring: Whether this reader is a drain-pool worker, which drains its table inline.

        return self.options.get(_SNAPSHOT_DAEMON_SLOT_MARKER_OPTION) == "true"

    def _snapshot_reader_thread_count(self) -> int:
        return _resolve_snapshot_reader_threads(self.options)

    def _snapshot_drain_reader_factory(self) -> Callable[[], Any]:
        # The daemon worker gets its OWN options copy. Capacity accounting is left intact --
        # the drain must count against the same connection budget so
        # ``snapshot.reader.threads`` bounds the slots it holds.
        reader_options = dict(self.options)
        # The drain runs on a daemon thread and holds its slot for the whole scan, but it
        # is bootstrap work that must finish to unblock its consumer. Floor it one band
        # below the CDC daemon (not at the CDC floor), so a saturated CDC daemon cannot
        # starve it -- it always has slots the CDC daemon is locked out of. The marker also
        # makes the worker's ``_read_snapshot`` drain inline rather than recurse into the
        # pool (see _is_snapshot_drain_worker).
        reader_options[_SNAPSHOT_DAEMON_SLOT_MARKER_OPTION] = "true"
        # The worker waits for its slot using its own ``connection.wait.timeout.seconds`` as
        # a *per-turnover* window, not a hard cap: it resets every time the pool claims a slot
        # (see the slot-wait liveness in ``_acquire_connection_slot``), so it waits out a busy
        # backfill and only gives up when the pool is wedged. It is also the consumer's
        # per-progress idle window, so the single knob governs both. Drop any inherited
        # per-read attempt budget so it cannot cap the slot wait below that window.
        reader_options.pop(_CONNECTION_ATTEMPT_BUDGET_OPTION, None)
        reader_options.pop(_REPLAY_CONNECTION_WAIT_BUDGET_OPTION, None)
        # And the backlog rank of the read that happened to start it: kept, the daemon
        # swept for a slot at that read's pace for as long as it lived.
        reader_options.pop(_CONNECTION_ATTEMPT_RANK_OPTION, None)
        cls = type(self)
        return lambda: cls(reader_options)

    def _drain_via_snapshot_daemon(
        self,
        table: Table,
        options: dict[str, str],
        pipeline_scope: str,
        schema_id: str,
        allow_keyless: bool,
    ) -> dict[str, Any]:
        # Docstring: Enqueue this table's drain on the bounded pool and wait -- holding no slot
        # or state connection -- for the daemon to stage it and publish the manifest,
        # then return that manifest.

        namespace = self._lakebase_state_namespace()
        # This update's pool, with its one budget of drains, whichever update's scope the
        # snapshot is staged under.
        registration_scope = self._pipeline_scope()
        factory = self._snapshot_drain_reader_factory()
        pool = _snapshot_drain_pool(
            (namespace, registration_scope),
            reader_factory=factory,
            thread_count=self._snapshot_reader_thread_count(),
        )
        job_key = (namespace, pipeline_scope, table.native_identity, schema_id)
        pool.submit(
            job_key,
            table=table,
            options=dict(options),
            pipeline_scope=pipeline_scope,
            allow_keyless=allow_keyless,
            registration_scope=registration_scope,
            # Not the pool's, the first flow's to ask: a worker built from another flow's
            # options read a table this one left unset with that flow's setting.
            reader_factory=factory,
        )
        # The wait is a liveness watchdog, not a total-duration cap: it trips only after
        # the worker reports no progress for a whole ``connection.wait.timeout.seconds``
        # window. "Progress" is a staged page *or* -- while the worker is still acquiring
        # its slot -- a turnover ping from ``_acquire_connection_slot``, so a drain that is
        # legitimately queued behind a busy-but-live pool never trips here; the worker's own
        # slot-wait watchdog owns the wedged-capacity case and raises a precise
        # ConnectionCapacityUnavailable. The consumer waits one extra grace beyond the
        # worker's window so, on a genuine wedge, that precise error (recorded by the worker
        # when it gives up) surfaces instead of this generic message racing it out -- which
        # then means only one thing: the worker went dark without recording an error.
        idle_timeout = float(
            self.options.get(
                "connection.wait.timeout.seconds",
                str(_DEFAULT_CONNECTION_WAIT_TIMEOUT_SECONDS),
            )
        )
        # Hold no Lakebase connection across the drain wait: the caller's manifest
        # probe left this connection idle inside a read transaction, which Lakebase
        # reaps as an idle-in-transaction timeout over a multi-minute drain. The
        # daemon stages on its own connection; the post-wait read reopens fresh.
        self._reset_lakebase_connection()
        if not pool.wait(job_key, idle_timeout + _SNAPSHOT_DRAIN_STALL_GRACE_SECONDS):
            # 280: Shared snapshot drain of '{}' made no progress for {:g}s and appears stalled: the
            # drain worker stopped reporting progress without failing. Check the source for a hung
            # query, or raise connection.wait.timeout.seconds.
            raise InformixError(_m(280, table.exposed_name, idle_timeout))
        manifest = self._read_snapshot_stage_manifest(table, pipeline_scope, schema_id)
        if manifest is None:
            # 281: Shared snapshot drain of '{}' completed without publishing a manifest
            raise InformixError(_m(281, table.exposed_name))
        return manifest

    def _shared_cdc_thread_count(self) -> int:
        explicit = self.options.get(_SHARED_CDC_THREADS_OPTION)
        if explicit is not None:
            return max(1, int(explicit))
        # Threads are per channel and the upsert/delete channels shard independently, so K
        # threads means up to 2K daemon slot-holders. Default K to a quarter of the pool so
        # total daemon demand stays near half the pool, leaving room for consumer bootstrap
        # reads and the snapshot drain. The old default (= max.concurrent.connections)
        # targeted 2N holders against an N-slot pool and pinned the entire daemon
        # reservation band, starving bootstrap/drain reads on a small pool.
        slot_count = int(
            self.options.get("max.concurrent.connections", str(_DEFAULT_MAX_CONCURRENT_CONNECTIONS))
        )
        return max(1, slot_count // 4)

    def _shared_cdc_reader_factory(self) -> Callable[[], Any]:
        # The daemon reader gets its OWN options copy: the connector mutates options
        # mid-read (capacity hints), so a shared dict would race. It reads
        # through its bridge and _read_changes_with_reconnect, never _read_stream, so it
        # can't recurse into the shards.
        reader_options = dict(self.options)
        # The shard reads on a daemon thread, so floor its slot above the daemon
        # reservation to leave headroom for consumer bootstrap reads.
        reader_options[_DAEMON_SLOT_MARKER_OPTION] = "true"
        # The daemon waits for its slot on the turnover watchdog (see the liveness path in
        # _acquire_connection_slot), using ``connection.wait.timeout.seconds`` as a
        # per-turnover window rather than a hard cap, so it holds its queue ticket long
        # enough for the round-robin rotation to reach it. Drop any per-read attempt budget
        # inherited from the parent's options so a short consumer budget cannot cap that
        # wait below the window and send the daemon back to the end of the queue.
        reader_options.pop(_CONNECTION_ATTEMPT_BUDGET_OPTION, None)
        reader_options.pop(_REPLAY_CONNECTION_WAIT_BUDGET_OPTION, None)
        # And the backlog rank of the read that happened to start it: kept, the daemon
        # swept for a slot at that read's pace for as long as it lived.
        reader_options.pop(_CONNECTION_ATTEMPT_RANK_OPTION, None)
        cls = type(self)
        # The shard is keyed by this reader's scope, and its reads spill under it.
        scope = self._registration_scope

        def factory() -> Any:
            reader = cls(reader_options)
            if scope is not None:
                reader.set_registration_scope(scope)
            return reader

        return factory

    # pylint: disable-next=too-many-locals
    def _shared_cdc_snapshot(self, table, checkpoint, options, deletes, pipeline_scope):
        # Docstring: Subscribe this table to its shard and return the daemon's coherent view, or
        # None to fall back to a direct read (daemon not ready, or a schema transition).

        channel = "delete" if deletes else "upsert"
        threads = self._shared_cdc_thread_count()
        shard_id = int(hashlib.sha256(table.native_identity.encode()).hexdigest(), 16) % threads
        # By what the shard's leader decodes, spills and reads the catalog under, too: a
        # flow of the same database through a connection with others got the leader's. And
        # by the user it captures as, so no flow reads through another user's session.
        key = (
            self._lakebase_state_namespace()
            + ":"
            + hashlib.sha256(
                repr(
                    [
                        self.options.get("user"),
                        self.options.get(_CDC_SPILL_RECORDS_OPTION),
                        _null_byte_mode(self.options),
                        _database_encoding(self.options),
                        getattr(self, "_snapshot_staging_location", None),
                        self.options.get("lakebase.project.id"),
                        self.options.get("lakebase.database"),
                        _option_bool(self.options, _PROMOTE_UNIQUE_INDEX_OPTION, True),
                    ]
                ).encode()
            ).hexdigest()[:12],
            pipeline_scope,
            channel,
            shard_id,
        )
        capture = _capture_descriptor(table, _database_encoding(self.options))
        checkpoint_commit = int(checkpoint["commit_lsn"])
        floor = int(checkpoint.get("begin_lsn") or checkpoint["commit_lsn"])
        config = {
            "timeout_seconds": self._table_int_option(options, "cdc.timeout", 5, minimum=1),
            "max_records": self._table_int_option(
                options, "cdc.max.records", _DEFAULT_CDC_MAX_RECORDS, minimum=1, maximum=256
            ),
            "buffer_cap": int(
                self.options.get(_SHARED_CDC_BUFFER_OPTION, str(_DEFAULT_SHARED_CDC_BUFFER))
            ),
            "subscriber_ttl": float(
                self.options.get("cdc.shared.state.wait.seconds", str(_SHARED_STATE_WAIT_SECONDS))
            ),
        }
        reader_factory = self._shared_cdc_reader_factory()
        # Names this flow to the shard: two flows reading the table with one layout (two
        # destinations, say) share its buffer only through the first, the other reading
        # directly. Kept through pickling, so the flow's reader is the same owner wherever
        # it runs; a connector built without __init__ gets one here.
        owner = self.__dict__.setdefault("_shard_owner", secrets.token_hex(8))
        replay_stop = self._replay_stop_lsn(options)
        deadline = (
            time.monotonic()
            + _SHARED_VIEW_WAIT_TIMEOUTS * config["timeout_seconds"]
            + _SHARED_VIEW_WAIT_MARGIN_SECONDS
        )
        while True:
            if _shared_cdc_mode(self.options) == _SHARED_CDC_SESSION_HOST:
                view = _host_shard_poll(
                    key,
                    reader_factory,
                    config,
                    table.native_identity,
                    table,
                    capture,
                    checkpoint_commit,
                    floor,
                    owner=owner,
                )
            else:
                shard = _shared_cdc_shard(key, reader_factory=reader_factory, **config)
                view = shard.poll(
                    table.native_identity, table, capture, checkpoint_commit, floor, owner
                )
            # A fingerprint mismatch means a schema transition is in flight; defer to the
            # direct-read path, which owns the transition/replay logic. So does a view of
            # another layout than this reader's: two flows can read one table under different
            # options with the same capture, and the shard keeps the first one's table.
            if (
                view is None
                or view.fingerprint != str(checkpoint.get("schema_fingerprint"))
                or view.fingerprint != _schema_fingerprint(table)
            ):
                return None
            if not (self._trigger_available_now or replay_stop is not None) or view.committed:
                return view
            # A bounded read -- an AvailableNow update, or a replay -- ends at the first read
            # that moves it nowhere. A view that cannot move this consumer on (the shard's
            # scan has not reached it yet, or went idle where the consumer already is)
            # yields to a direct read, which waits on the log itself -- unless it shows the
            # read done. A replay is done once the shard went idle at or past its stop. Not
            # a checkpoint the log no longer retains: the read reports that first.
            reached = view.reached_lsn if view.caught_up else None
            if (
                reached is None
                or reached > checkpoint_commit
                or replay_stop is not None
                or floor < view.min_lsn
            ):
                moved_on = reached is not None and reached > checkpoint_commit
                at_stop = replay_stop is not None and reached is not None and reached >= replay_stop
                return view if moved_on or at_stop else None
            # An AvailableNow read takes the update's frozen boundary here when this process
            # has not read it yet, the upsert reader publishing it from this view's sample as
            # it would on taking the view. The boundary is current_lsn, the end of the log
            # page being written, which an idle read never reaches; and on a quiet log the
            # server's CDC reader, shared by every session, reports a TIMEOUT position held
            # dozens of pages back until the next checkpoint, even to a session started past
            # it. A direct read, started from the checkpoint, is done when the server reports
            # no position past it. The shard's read is as good once the log had reached the
            # boundary when it started (its sample, current_lsn, is at or past it) and it
            # went idle where it started or short of it. A scan stopped short, its timeout
            # spent on other tables' log, ends past where it started instead -- even where
            # that is where this consumer now is, the quiet advance having taken it there
            # before the shard's next read. Within the boundary's page is done as well.
            bound, _ = self._shared_trigger_boundary(
                table, checkpoint, current_lsn=view.current_lsn
            )
            started = view.read_start_lsn
            if view.current_lsn >= bound and (
                reached >= bound - (1 << 12) or (started is not None and reached <= started)
            ):
                return view
            # Otherwise the shard's next read, started later, is waited for.
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            time.sleep(min(_SHARED_VIEW_WAIT_POLL_SECONDS, remaining))

    def _read_stream(self, table: Table, start: dict, options: dict[str, str], deletes: bool):
        # Docstring: One stream batch (see _read_stream_batch). Until the stream is past the
        # end of a keyless table's index-ordered scan, its offset names the update that took
        # the snapshot, whose manifest says which changes to drop (_drop_scanned_changes).

        rows, end = self._read_stream_batch(table, start, options, deletes)
        live = self.__dict__.get("_index_scan_live", {})
        # Only once this process has looked for the manifest: until then the offset keeps
        # what it carries.
        if isinstance(end, dict) and table.identity in live:
            scan = live[table.identity]
            end = {k: v for k, v in end.items() if k not in ("scan_scope", "scan_schema")}
            if scan and int(end.get("commit_lsn", 0)) < scan[2]:
                end["scan_scope"], end["scan_schema"] = scan[0], scan[1]
        return rows, end

    # pylint: disable-next=too-many-branches,too-many-locals,too-many-statements
    def _read_stream_batch(self, table: Table, start: dict, options: dict[str, str], deletes: bool):
        checkpoint = _validated_offset(start)
        pipeline_scope = self._pipeline_scope(checkpoint)
        # A daemon reader owns this shard's Informix I/O: it supplies the schema, log
        # bounds, and change records, so a steady-state poll touches Informix for
        # nothing. A None view (daemon not ready yet, or a schema transition in flight)
        # transparently falls back to the direct-read path below.
        shared_view = self._shared_cdc_snapshot(table, checkpoint, options, deletes, pipeline_scope)
        if shared_view is not None:
            table = shared_view.table
        else:
            table = self._refresh_table_schema(table, None)
        fingerprint = _schema_fingerprint(table)
        checkpoint_fingerprint = checkpoint.get("schema_fingerprint")
        checkpoint_schema_id = str(checkpoint["schema_id"])
        if checkpoint_fingerprint is None:
            # 282: Informix checkpoint for '{}' predates schema-safe offsets; run a full refresh
            # before resuming CDC
            raise InformixError(_m(282, table.exposed_name))
        restart = int(checkpoint.get("begin_lsn") or checkpoint["commit_lsn"])
        minimum = shared_view.min_lsn if shared_view is not None else self._bridge.minimum_lsn()
        if restart < minimum:
            # 283: Restart LSN {} is older than minimum retained LSN {}; resnapshot required
            raise LogRetentionError(_m(283, restart, minimum))
        # A restart position in a *higher log file* than the server's current one
        # means the source reinitialized its logical log under this checkpoint, so
        # the position will never be reached again. Without this the reader
        # activates CDC at an unreachable LSN and returns nothing on every poll
        # while re-returning the same checkpoint -- a permanent silent stall that
        # reports RUNNING and replicates no rows. Observed in production on the
        # delete channel, whose bootstrap path already validates both bounds (see
        # _initial_lsn) while this resume path validated only the lower one.
        #
        # Compare log-file numbers rather than raw LSNs. ``current_lsn`` derives
        # its byte offset from ``syslogs.used``, a sampled page count, so a
        # transaction committed between that sample and this read legitimately sits
        # a few pages beyond it -- a raw ``>`` comparison would reject that benign
        # race. Only a lower *uniqid* is unambiguous: log files advance
        # monotonically, so a checkpoint naming a file above the current one cannot
        # be explained by sampling lag.
        #
        # Fail loudly rather than advancing: the checkpoint's position is unrelated
        # to the current log, so no safe resume point can be inferred from it and
        # only a full refresh can re-establish one.
        current = shared_view.current_lsn if shared_view is not None else self._bridge.current_lsn()
        if (restart >> 32) > (current >> 32):
            # 284: Restart LSN {} for '{}' names logical log {}, which is ahead of the source's
            # current log {}, so the source reinitialized its logical log; this checkpoint can never
            # be resumed. Run a full refresh; changes committed before the reinitialization are no
            # longer in the log.
            raise LogRetentionError(
                _m(284, restart, table.exposed_name, restart >> 32, current >> 32)
            )
        capture_table = table
        transition_table = table
        transition_lsn: int | None = None
        capture_schema_id = checkpoint_schema_id
        transition_schema_id = checkpoint_schema_id
        projection_schema_transition = False
        # The live layout this flow saw on its first read of the update, carried on every
        # offset the update writes. A whole-row key shrinks only by a drop the update's
        # destination already merges without, one made before it started: the update's first
        # read may take one, and so may a later read while the live layout is still the one
        # that read saw. An offset written without reading (a delete seed, a metadata-only
        # step) then leaves a drop made while the pipeline was stopped to its next read.
        first_read = checkpoint.get("pipeline_scope") != pipeline_scope
        update_layout = fingerprint if first_read else checkpoint.get("update_fingerprint")
        if checkpoint_fingerprint != fingerprint:
            (
                _previous_table,
                transition_table,
                transition_lsn,
                transition_schema_id,
            ) = self._schema_transition(
                table,
                checkpoint_schema_id,
                int(checkpoint["commit_lsn"]),
                owner=not deletes,
                # An incremental copy still in progress keeps its chunk position in the
                # offset, laid out by the old key, so a wildcard key may only grow once
                # the copy has finished and dropped that block.
                allow_key_extension=not isinstance(checkpoint.get("incremental"), dict),
                copying=isinstance(checkpoint.get("incremental"), dict),
                # Every offset records the update that wrote it, so a checkpoint from an
                # earlier update marks this update's first read: the destination's merge
                # key was read at its start and already reflects a dropped column.
                new_update=first_read or update_layout == fingerprint,
            )
            # Informix capture registration is a projection, not a raw decoder
            # tied to the catalog layout at ``start_lsn``.  For an appended
            # nullable column, registering the expanded descriptor at the old
            # checkpoint safely spans the DDL: records written before the ALTER
            # contain NULL for the appended field, while records written after
            # it contain the logged value.  Live Informix 15 validation covers
            # both transactions in one replay session.  The same holds for the
            # other changes _ensure_compatible_schema_change accepts: live
            # Informix 14.10 returned records logged before a CHAR/VARCHAR
            # widening, numeric widenings (SMALLINT/INTEGER -> BIGINT/INT8, a
            # widened key included, SMALLFLOAT -> FLOAT, DECIMAL and MONEY
            # precision), and a column drop in the current layout, so they decode
            # with the new descriptor.
            #
            # Previously this method captured with the predecessor descriptor
            # only up to a restart-time ``current_lsn`` and then advanced the
            # checkpoint to that sampled boundary.  A transaction committed
            # after the DDL but before restart could fall on the wrong side of
            # that artificial boundary and be skipped.  Schema history is still
            # published above so the independently checkpointed delete reader
            # selects the same successor generation, but it is not a data
            # boundary: replay the successor projection from the retained Spark
            # checkpoint itself.
            capture_table = transition_table
            capture_schema_id = transition_schema_id
            transition_lsn = None
            projection_schema_transition = True
        max_rows = self._table_int_option(
            options, "max.records.per.batch", _DEFAULT_MAX_RECORDS_PER_BATCH, minimum=1
        )
        stop_lsn: int | None = None
        trigger_high_water: int | None = None
        trigger_generation: str | None = None
        # A replay reproduces a committed [start, end) range bounded by its end offset
        # (``replay_stop`` below). It must NOT consult the shared trigger boundary: that
        # boundary is per update scope, so after a driver restart a replay -- planned in a
        # fresh scope -- would bound the replayed range by a boundary of its own update
        # instead of the end Spark logged. The replay's own committed end LSN is the
        # correct stop and is applied below, so skip the live handshake while replaying.
        # A normal (non-replay) read has no replay stop and takes the boundary as before.
        replay_stop = self._replay_stop_lsn(options)
        if replay_stop is None:
            self._settle_spilled_runs(
                table,
                checkpoint,
                options,
                deletes,
                None if shared_view is not None else self._bridge.current_lsn,
            )
        if self._trigger_available_now and replay_stop is None:
            stop_lsn, trigger_generation = self._shared_trigger_boundary(
                table,
                checkpoint,
                current_lsn=shared_view.current_lsn if shared_view is not None else None,
            )
            trigger_high_water = stop_lsn
        if transition_lsn is not None:
            stop_lsn = transition_lsn if stop_lsn is None else min(stop_lsn, transition_lsn)
        # A replay must land on the offset Spark already committed, so it reads to
        # that LSN and stops there -- the same bounded read the trigger boundary
        # above performs, reached through the one channel the connector owns.
        #
        # Without a bound a replay is not reproducible: the read stops at whichever
        # transaction boundary the row budget happens to fall on, so the same start
        # LSN over the same log can end in different places. Measured directly --
        # budget 10 ended at LSN 107, budget 2 at 104, identical data. The framework
        # discards the offset a replay reaches and commits its own `end` regardless,
        # so an unbounded short read silently drops everything in between; that is
        # how six tables lost 151 rows. Bounding the read removes the divergence at
        # its source rather than detecting it afterwards, which is all a comparison
        # could do -- and failing there just turns lost rows into a retry loop,
        # because the next attempt is no more reproducible than the last.
        # (``replay_stop`` was resolved above, before the trigger-boundary block.)
        if replay_stop is not None:
            stop_lsn = replay_stop if stop_lsn is None else min(stop_lsn, replay_stop)
            # The committed range may hold more rows than a normal batch admits, and
            # truncating it is the very divergence being removed. The LSN bound caps
            # the read instead, so the budget must not also cut it short.
            max_rows = _REPLAY_UNBOUNDED_ROWS
        past_step = projection_schema_transition and (
            _schema_fingerprint(capture_table) != fingerprint
        )
        if shared_view is not None:
            # The daemon already assembled and fanned out this shard's transactions and
            # re-validated the schema; take them as-is (no Informix I/O, no re-fetch).
            committed, caught_up, open_begin, reached = (
                shared_view.committed,
                shared_view.caught_up,
                shared_view.open_begin,
                shared_view.reached_lsn,
            )
        elif (past_step and replay_stop is None) or (
            replay_stop is not None and replay_stop <= int(checkpoint["commit_lsn"])
        ):
            # A step to a generation the live schema has moved past: commit its schema and
            # read nothing under it. Its columns are not the live ones -- a column added
            # since would go uncaptured in every change read here, and one dropped since
            # cannot be captured -- so the changes wait for the step to the live schema,
            # which captures them with the live layout from this same position. A replay
            # that ends where it starts (such a step, or a metadata-only step to the live
            # schema) has no transaction to read either: none commits after its start and
            # at or before its stop.
            committed, caught_up, open_begin, reached = [], False, None, None
        else:
            # A replay of a step that read changes reads its committed range, every change
            # of which was committed while the step's generation was live. It captures with
            # the live layout, which spans the ALTERs since, and is shaped by it below.
            raw_records = self._read_changes_with_reconnect(
                [
                    _capture_descriptor(
                        table if past_step else capture_table, _database_encoding(self.options)
                    )
                ],
                restart,
                self._table_int_option(options, "cdc.timeout", 5, minimum=1),
                self._table_int_option(
                    options, "cdc.max.records", _DEFAULT_CDC_MAX_RECORDS, minimum=1, maximum=256
                ),
                table=table,
            )
            table = self._refresh_table_schema(table, fingerprint)
            committed, caught_up, open_begin, reached = _transaction_batch(raw_records)
        recovered = self._drop_scanned_changes(
            table, checkpoint, options, _recover(committed, checkpoint), reached
        )
        # A copy that follows the stream (see _copy_chunk): ``marks`` holds what its windows
        # had judged before each transaction, and after the last.
        copy, marks = checkpoint.get("incremental"), None
        if not deletes and isinstance(copy, dict) and copy.get("exact"):
            recovered, marks = self._drop_copied_changes(table, copy, options, recovered)
        output: list[dict[str, Any]] = []
        end = dict(start)
        end["pipeline_scope"] = pipeline_scope
        if projection_schema_transition:
            # A metadata-only poll still has to commit the successor schema so
            # Spark can evolve the target before the next batch.  Keep every
            # positional field unchanged: schema discovery is not evidence that
            # CDC reached a newer LSN.
            end["schema_id"] = capture_schema_id
            end["schema_fingerprint"] = _schema_fingerprint(capture_table)
        consumed = 0
        crossed_transition = False
        crossed_trigger_boundary = False
        # Set when the row budget, rather than the end of the log, ended this read.
        # It is the one proof a reader has that work remains for *this* table, so
        # it is what the backlog rank is built from.
        row_budget_reached = False
        # Delete handling on the upsert channel follows the destination. A keyed table's
        # upsert view merges every row it receives, so its DELETEs are skipped here and
        # applied only through the separate delete channel (``deletes=True``). An append
        # destination lands rows as-is and never reads _informix_op, so a DELETE on the
        # append path is emitted as a full-before-image ``d`` tombstone unless the
        # operator switched tombstones off for this table. So is the old key of a
        # key-changing UPDATE on a keyed table forced to append. A transaction is never
        # split across batches, so such a tombstone always ships with its ``u`` row.
        # Shaped by the generation the read captured, which a reader a step behind the live
        # schema registers (capture_table): the live table can key or type a column the
        # captured records lack. A schema record keeps the table's options, filter and
        # transforms included, so the projection loses none of them. A replay of a step
        # the live schema moved past captured with the live layout, and this update's
        # destination keys by it: the step's own key can name a column dropped since.
        shape_table = table if past_step else capture_table
        tombstones = (
            not deletes
            and self._append_only_table(shape_table, options)
            and self._append_delete_tombstones(options)
        )
        for tx in recovered:
            if transition_lsn is not None and tx.commit_lsn > transition_lsn:
                if tx.begin_lsn < transition_lsn:
                    # 285: Transaction {} spans schema transition LSN {} for '{}'; keep source
                    # writes quiesced until schema transition completes or run a full refresh
                    raise InformixError(_m(285, tx.tx_id, transition_lsn, table.exposed_name))
                crossed_transition = True
                break
            if stop_lsn is not None and tx.commit_lsn > stop_lsn:
                crossed_trigger_boundary = True
                break
            spilled = [part for part in tx.spilled if _record_matches(part, shape_table)]
            if spilled:
                self._note_spilled_runs(spilled, tx, deletes)
            if deletes:
                # The delete channel shapes nothing from a run of inserts: it skips its pages.
                spilled = [
                    part
                    for part in spilled
                    if part.get("ops") is None or _SPILL_DELETE_OPS.intersection(part["ops"])
                ]
            elif not tombstones:
                # Nor does an upsert channel without tombstones from a run of DELETEs. A
                # TRUNCATE is not a DELETE, so it still reaches the shaping that rejects it.
                spilled = [part for part in spilled if part.get("ops") != ["DELETE"]]
            if spilled and output:
                # A spilled transaction is a batch of its own: it waits for the next one.
                row_budget_reached = True
                break
            projected = _project_transaction(
                tx,
                shape_table,
                deletes,
                tombstones=tombstones,
                encoding=self._source_encoding(options),
                options=options,
            )
            projected = [
                _coerce_variable_decimal_values(row, shape_table, options) for row in projected
            ]
            # Never split a transaction.  A single large transaction is
            # accepted; subsequent complete transactions wait for next poll.
            if output and len(output) + len(projected) > max_rows:
                row_budget_reached = True
                break
            output.extend(projected)
            consumed += 1
            end = _offset(
                tx.commit_lsn,
                tx.commit_lsn,
                tx.restart_lsn,
                tx.tx_id,
                "stream",
                capture_table,
                capture_schema_id,
                pipeline_scope,
                trigger_generation=checkpoint.get("trigger_generation"),
                trigger_high_water=checkpoint.get("trigger_high_water"),
            )
            if spilled:
                # Its changes stay on the staged pages until the batch is served. All of
                # them went there once it spilled, so nothing was projected above.
                output = _SpilledRows(
                    self._stream_staged_page_for,
                    spilled,
                    tx,
                    shape_table,
                    options,
                    deletes=deletes,
                    tombstones=tombstones,
                    encoding=self._source_encoding(options),
                )
                # Work remains for this table only if a transaction follows it.
                row_budget_reached = tx is not recovered[-1]
                break
            if len(output) >= max_rows:
                row_budget_reached = True
                break
        if (
            transition_lsn is not None
            and crossed_transition
            and open_begin is not None
            and open_begin < transition_lsn
        ):
            # 286: An open transaction beginning at LSN {} spans schema transition LSN {} for '{}';
            # keep source writes quiesced until schema transition completes or run a full refresh
            raise InformixError(_m(286, open_begin, transition_lsn, table.exposed_name))
        if transition_lsn is not None and (
            trigger_high_water is None or transition_lsn <= trigger_high_water
        ):
            if (caught_up and consumed == len(recovered)) or (
                crossed_transition and (open_begin is None or open_begin >= transition_lsn)
            ):
                end = _offset(
                    transition_lsn,
                    transition_lsn,
                    transition_lsn,
                    None,
                    "stream",
                    transition_table,
                    transition_schema_id,
                    pipeline_scope,
                    trigger_generation=checkpoint.get("trigger_generation"),
                    trigger_high_water=checkpoint.get("trigger_high_water"),
                )
        drained = (
            caught_up
            and open_begin is None
            and consumed == len(recovered)
            and not crossed_transition
        )
        # Caught up only counts once Informix's log reader is past the frozen boundary:
        # a poll can go idle short of it, and stopping there would leave the changes in
        # between for a later update. An unknown position keeps the old reading.
        reached_trigger_boundary = trigger_generation is not None and (
            crossed_trigger_boundary
            or (drained and (reached is None or reached >= trigger_high_water))
        )
        # A triggered or replayed read that went idle with nothing left to emit moves its
        # checkpoint to where the log reader stopped (capped at the read's bound), so the
        # next read continues from there rather than rescanning -- and, below the
        # boundary, so AvailableNow sees progress and keeps reading. Everything this
        # table needs up to that position was delivered: an open transaction keeps the
        # offset at its BEGIN instead (drained is False), and Informix delivers a
        # transaction's BEGIN with its first captured change, as the commit-based
        # offsets above already rely on. A continuous read keeps its checkpoint: idle
        # polls there would otherwise each become an empty microbatch.
        quiet_bound = None
        if trigger_high_water is not None:
            quiet_bound = trigger_high_water
        if replay_stop is not None:
            quiet_bound = replay_stop if quiet_bound is None else min(quiet_bound, replay_stop)
        if quiet_bound is not None and drained and reached is not None and transition_lsn is None:
            if (
                # A metadata-only step keeps its position for Spark to evolve the target first;
                # a replay's end is already logged, so it lands there through a step too.
                (not projection_schema_transition or replay_stop is not None)
                and min(reached, quiet_bound) > int(end["commit_lsn"])
            ):
                quiet = min(reached, quiet_bound)
                end = _offset(
                    quiet,
                    quiet,
                    quiet,
                    None,
                    "stream",
                    capture_table,
                    capture_schema_id,
                    pipeline_scope,
                    trigger_generation=checkpoint.get("trigger_generation"),
                    trigger_high_water=checkpoint.get("trigger_high_water"),
                )
        # A replay of a range whose original read went idle lands on that idle end even
        # when the log has since moved on. The replay then delivers a commit beyond its
        # bound instead of going idle, so it is not drained, but the crossing commit
        # proves the log reader passed the bound, and every transaction committed below
        # it was consumed in commit order before the loop broke. A transaction still open
        # with its BEGIN below the bound keeps the replay short, as before: the original
        # read went idle with nothing open, so the replay cannot account for it, and a
        # retry lands once it commits. _replay_between_offsets refuses this landing for
        # an end that names a transaction the replay did not reach.
        if replay_stop is not None and crossed_trigger_boundary and transition_lsn is None:
            if (open_begin is None or open_begin >= replay_stop) and replay_stop > int(
                end["commit_lsn"]
            ):
                end = _offset(
                    replay_stop,
                    replay_stop,
                    replay_stop,
                    None,
                    "stream",
                    capture_table,
                    capture_schema_id,
                    pipeline_scope,
                    trigger_generation=checkpoint.get("trigger_generation"),
                    trigger_high_water=checkpoint.get("trigger_high_water"),
                )
        if reached_trigger_boundary:
            end = dict(end)
            end["trigger_generation"] = trigger_generation
            end["trigger_high_water"] = str(trigger_high_water)
        if update_layout is not None:
            end["update_fingerprint"] = update_layout
        if marks is not None:
            # A window the stream has read through is closed, each of its changes judged; an
            # open one keeps what it has judged, for the batch that goes on in it.
            upto = max(int(end["commit_lsn"]), (reached or 0) if drained else 0)
            states, windows = marks[consumed], []
            for window in copy["windows"]:
                state = states[window[0]]
                if int(window[4]) > upto:
                    windows.append([*window[:5], state] if state else window[:5])
                elif 0 in state.values():
                    # 493: Informix incremental copy of '{}', which lands as a change log, cannot
                    # tell which changes made to a key while one of its chunks was read the chunk
                    # holds: the key's staged row fits none of them; run a full refresh
                    raise InformixError(_m(493, table.exposed_name))
            end = {**end, "incremental": {**copy, "windows": windows}}
        end = _with_backlog_streak(end, start, row_budget_reached)
        return iter(output), end
