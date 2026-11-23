"""Snapshot staging: pages, manifests and their sweeps, and the start of an incremental snapshot."""

from __future__ import annotations

import concurrent.futures
import contextlib
import gzip
import hashlib
import hmac
import json
import logging
import os
import secrets
import time
import weakref
from typing import Any, Iterator

from databricks.labs.community_connector.sources.informix.informix_bridge import (
    PurePythonInformixBridge,
)
from databricks.labs.community_connector.sources.informix.informix_filters import (
    _snapshot_filter,
    _snapshot_predicate,
)
from databricks.labs.community_connector.sources.informix.informix_host_sharing import (
    _BOOLEAN_KEY_TYPES,
    _FLOAT_KEY_TYPES,
    _cdc_capable,
    _DeferredStagedPage,
)
from databricks.labs.community_connector.sources.informix.informix_keys import (
    _moved_rows_of_page,
    _option_bool,
    _without_moved_rows,
)
from databricks.labs.community_connector.sources.informix.informix_offsets import (
    _validated_offset,
    _with_backlog_streak,
)
from databricks.labs.community_connector.sources.informix.informix_options import (
    _MAX_SNAPSHOT_STAGE_PAGE_BYTES,
    _REPLAY_INITIAL_END_OPTION,
    _RESNAPSHOT_OPTION,
    _SNAPSHOT_CLEANUP_THREADS,
    _SNAPSHOT_SCAN_CURSOR,
    _SNAPSHOT_SCAN_OPTION,
    _SNAPSHOT_STAGE_VERSION,
    _SNAPSHOT_STAGING_PIPELINE_OPTION,
    _SNAPSHOT_STAGING_TOKEN_LIFETIME_OPTION,
    _STAGING_PROVIDERS,
    _STAGING_TOKEN_SWEEP_CLAIM_SECONDS,
    InformixError,
    _m,
    _sql_name,
)
from databricks.labs.community_connector.sources.informix.informix_registration import (
    _SCAN_RESUME_SECONDS,
    Table,
    _replay_keeps_floor,
)
from databricks.labs.community_connector.sources.informix.informix_shards import _schema_fingerprint
from databricks.labs.community_connector.sources.informix.informix_transactions import (
    _created_since,
    _dropped_mount_error,
    _is_path,
    _open_state_file,
    _staging_subdirectories,
    _strict_lsn,
    _unchanged_directories,
)
from databricks.labs.community_connector.sources.informix.informix_unload import (
    _decode_snapshot_stage_value,
    _encode_snapshot_stage_value,
    _offset,
    _scan_staged_page,
    _skip_json_whitespace,
    _staged_page_payload,
)
from databricks.labs.community_connector.sources.informix.lakebase_state import claim_staging_sweep
from databricks.labs.community_connector.sources.informix.lakebase_workspace import (
    _DEFAULT_STAGING_TOKEN_LIFETIME_SECONDS,
    _STAGING_TOKEN_MIN_LIFETIME_SECONDS,
    StagingCredentialProvider,
    WorkspaceHttpError,
    _workspace_credentials,
)
from databricks.labs.community_connector.sources.informix.sqli_codec import (
    datetime_order_preserving_cast,
)


class _StagingMixin:
    # Docstring: Snapshot staging: pages, manifests and their sweeps, and the start of an
    # incremental snapshot.

    def _should_pipeline_staging(self, scan_keys: tuple[str, ...]) -> bool:
        # Docstring: Whether to overlap this drain's page fetch with the page upload.
        #
        # Only for a positional drain over the REST transport -- ``scan_keys`` empty: a
        # keyless table, or a keyed one on ``snapshot.scan=cursor``. Keyset drains are
        # out of scope, and pipelining FUSE writes would raise the mount's write
        # frequency -- the exact condition the REST transport exists to avoid.

        if scan_keys:
            return False
        if not _option_bool(self.options, _SNAPSHOT_STAGING_PIPELINE_OPTION, True):
            return False
        return self._staging_transport() is not None

    def _build_staging_credential_provider(
        self, *, required: bool
    ) -> StagingCredentialProvider | None:
        # Docstring: Build the Files-API credential provider, or ``None`` to fall back to FUSE.
        #
        # In ``auto`` mode with neither explicit workspace credentials nor a Databricks
        # runtime there is no ambient identity to mint from, so it returns ``None``
        # without probing the SDK -- which also keeps offline tests on FUSE. ``rest``
        # mode (``required``) always attempts resolution and lets a failure propagate.

        explicit = bool(
            self.options.get("lakebase.workspace.token")
            and self.options.get("lakebase.workspace.host")
        )
        if not required and not explicit and not os.environ.get("DATABRICKS_RUNTIME_VERSION"):
            return None
        try:
            host, token = _workspace_credentials(self.options)
        except Exception:  # noqa: BLE001 - any resolution failure falls back to FUSE in auto
            if required:
                raise
            return None
        self._staging_workspace_host = host
        pipeline_id, _, update_id = self._pipeline_scope().partition("_@_")
        pipeline_id = pipeline_id.strip() or "default"
        lifetime = self._table_int_option(
            self.options,
            _SNAPSHOT_STAGING_TOKEN_LIFETIME_OPTION,
            _DEFAULT_STAGING_TOKEN_LIFETIME_SECONDS,
            minimum=_STAGING_TOKEN_MIN_LIFETIME_SECONDS,
        )
        update_id = update_id.strip()
        # The provider outlives this reader (one a process and update), so what it keeps
        # of the reader is a weak reference, replaced by each reader that takes the
        # provider up: it then asks through one that is in use, never one closed since,
        # whose state connection it would open again with nobody left to close it.
        reader = weakref.ref(self)

        def sweeps_earlier_updates() -> bool:
            # One process an update revokes the earlier updates' tokens: the first to
            # take the update's claim in Lakebase.
            current = reader()
            if current is None:
                return True
            root = current._table_state_keys_for("")[0]
            claim = f"{root}/staging-token-sweep/{pipeline_id}/{update_id}"
            return current._state_op_with_reconnect(
                lambda: claim_staging_sweep(
                    current._lakebase_connection(),
                    current._lakebase_state_namespace(),
                    claim,
                    _STAGING_TOKEN_SWEEP_CLAIM_SECONDS,
                ),
                operation="staging token sweep claim",
            )

        # One provider, and so one token, for this process and update: a drain worker
        # makes a reader a table and a shard daemon one a burst, and each minting its own
        # left a day-long token behind every time. An explicit lakebase.workspace.token is
        # an identity of its own, as a digest: two connections with different ones shared
        # the first one's token. The runtime's own credential is not, as the driver
        # captures it afresh every 15 minutes; the provider takes each capture up, so that
        # a rejected token is replaced through one that has not expired (refresh).
        identity = (
            hashlib.sha256(self.options["lakebase.workspace.token"].encode("utf-8")).hexdigest()
            if explicit
            else ""
        )
        key = (host, pipeline_id, update_id, lifetime, identity)
        with _STAGING_PROVIDERS.lock:
            provider = _STAGING_PROVIDERS.get(key)
            if provider is None:
                provider = _STAGING_PROVIDERS[key] = StagingCredentialProvider(
                    host,
                    token,
                    pipeline_id,
                    lifetime_seconds=lifetime,
                    update_id=update_id,
                )
            else:
                provider.take_bootstrap(token)
            provider.sweep_claim(sweeps_earlier_updates if update_id else None)
        return provider

    def _publish_snapshot_stage_page(
        self,
        table: Table,
        pipeline_scope: str,
        schema_id: str,
        snapshot_lsn: int,
        page_index: int,
        rows: list[dict[str, Any]],
        lower_pk: list[Any] | None,
    ) -> Any:
        # Docstring: Stage one page; returns the encoded upper_pk its header records.

        namespace = os.path.join(
            self._snapshot_stage_namespace(table, pipeline_scope, schema_id),
            "runs",
            str(snapshot_lsn),
        )
        encoded_rows = _encode_snapshot_stage_value(rows)
        body = {
            "lower_pk": _encode_snapshot_stage_value(lower_pk),
            "page_index": page_index,
            "pipeline_scope": pipeline_scope,
            "row_count": len(rows),
            "rows": encoded_rows,
            "schema_id": schema_id,
            "snapshot_lsn": str(snapshot_lsn),
            "table": table.native_identity,
            # A chunk of a copy can hold no rows.
            "upper_pk": rows
            and _encode_snapshot_stage_value([rows[-1][key] for key in table.primary_keys]),
            "version": _SNAPSHOT_STAGE_VERSION,
        }
        payload, digest = _staged_page_payload(body)
        record = {**body, "sha256": digest}
        if len(payload) > _MAX_SNAPSHOT_STAGE_PAGE_BYTES:
            # 217: Staged snapshot page {} for '{}' exceeds {} decoded bytes; reduce
            # snapshot.page.size, or keyless.snapshot.page.size for a keyless table or
            # snapshot.scan=cursor
            raise InformixError(
                _m(217, page_index, table.exposed_name, _MAX_SNAPSHOT_STAGE_PAGE_BYTES)
            )
        compressed = gzip.compress(payload, compresslevel=6, mtime=0)
        page_name = f"page-{page_index:08d}"
        files_api = self._staging_transport()
        if files_api is not None:
            # REST transport: write the payload straight to the Volume over the Files
            # API, never touching the FUSE mount. Head election lives in Postgres, so a
            # single overwrite PUT is sufficient -- no candidate/atomic-rename fence is
            # needed (a page's content is a pure function of its committed rows, so a
            # re-published page is byte-identical). Reads still go through FUSE.
            try:
                files_api.put(os.path.join(namespace, page_name, "data.json.gz"), compressed)
                return body["upper_pk"]
            except WorkspaceHttpError as error:
                # After a 401 nothing could recover, FUSE writes this page too.
                if not self._staging_fell_back(error):
                    raise
        # No filesystem capability probe: the exclusive-create and atomic-rename
        # guarantees it checked were needed by the head-election protocol, which
        # Postgres now performs. Writing a page needs only ordinary directory
        # creation, and a failure surfaces from the write itself.
        os.makedirs(namespace, mode=0o700, exist_ok=True)
        target = os.path.join(namespace, page_name)
        candidate = os.path.join(namespace, f"candidate-{secrets.token_hex(16)}")
        try:
            os.mkdir(candidate, mode=0o700)
            descriptor = os.open(
                os.path.join(candidate, "data.json.gz"),
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(compressed)
                handle.flush()
                os.fsync(handle.fileno())
            candidate_descriptor = os.open(
                candidate,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
            )
            try:
                os.fsync(candidate_descriptor)
            finally:
                os.close(candidate_descriptor)
            os.rename(candidate, target)
        except OSError:
            if not _is_path(target, directory=True):
                raise
            existing = self._read_snapshot_stage_page_record(
                table, pipeline_scope, schema_id, snapshot_lsn, page_index
            )
            if existing != record:
                # 218: Conflicting immutable snapshot page {} for '{}'
                raise InformixError(_m(218, page_index, table.exposed_name))
        finally:
            try:
                PurePythonInformixBridge._remove_connection_slot_tree(candidate)
            except FileNotFoundError:
                pass
            except OSError:
                # 219: Cannot remove Informix snapshot-page candidate: {}
                logging.getLogger(__name__).warning(
                    _m(219, candidate),
                    exc_info=True,
                )
        return body["upper_pk"]

    def _read_staged_page_payload(
        self,
        table: Table,
        pipeline_scope: str,
        schema_id: str,
        snapshot_lsn: int,
        page_index: int,
    ) -> bytes:
        # Docstring: Open, size-guard, and decompress one staged page into its raw JSON bytes.
        #
        # Both the whole-record reader (used by the staging conflict check) and the
        # streaming serve reader go through this, so the file-open/decompress/ceiling
        # logic lives in one place.

        return self._read_staged_page_payload_for(
            table.native_identity,
            table.exposed_name,
            pipeline_scope,
            schema_id,
            snapshot_lsn,
            page_index,
        )

    def _read_staged_page_payload_for(
        self,
        native_identity: str,
        exposed_name: str,
        pipeline_scope: str,
        schema_id: str,
        snapshot_lsn: int,
        page_index: int,
    ) -> bytes:
        # Docstring: Identity-string form of :meth:`_read_staged_page_payload`.
        #
        # Uses only the page's identity strings + the staging location, so it runs on
        # a Spark executor (the partitioned reader's ``read_partition``) with no live
        # Informix ``Table`` and therefore no catalog connection.

        path = os.path.join(
            self._snapshot_stage_namespace_for(native_identity, pipeline_scope, schema_id),
            "runs",
            str(snapshot_lsn),
            f"page-{page_index:08d}",
            "data.json.gz",
        )
        try:
            descriptor = _open_state_file(self._snapshot_staging_location, path)
            metadata = os.fstat(descriptor)
            if metadata.st_size > _MAX_SNAPSHOT_STAGE_PAGE_BYTES:
                os.close(descriptor)
                # 220: Compressed staged snapshot page {} for '{}' is too large
                raise InformixError(_m(220, page_index, exposed_name))
            with os.fdopen(descriptor, "rb") as raw_handle:
                with gzip.GzipFile(fileobj=raw_handle, mode="rb") as handle:
                    payload = handle.read(_MAX_SNAPSHOT_STAGE_PAGE_BYTES + 1)
        except (OSError, EOFError) as error:
            if _dropped_mount_error(error) is not None:
                raise  # unwrapped, so a read's dropped-mount retry sees it
            # 221: Cannot read staged snapshot page {} for '{}'
            raise InformixError(_m(221, page_index, exposed_name)) from error
        if len(payload) > _MAX_SNAPSHOT_STAGE_PAGE_BYTES:
            # 222: Staged snapshot page {} for '{}' exceeds its decoded size bound
            raise InformixError(_m(222, page_index, exposed_name))
        return payload

    def _read_snapshot_stage_page_record(
        self,
        table: Table,
        pipeline_scope: str,
        schema_id: str,
        snapshot_lsn: int,
        page_index: int,
    ) -> dict[str, Any]:
        payload = self._read_staged_page_payload(
            table, pipeline_scope, schema_id, snapshot_lsn, page_index
        )
        try:
            record = json.loads(payload)
            # Free the decompressed page bytes (up to the 256 MB ceiling) before
            # building the canonical form and the row graph, so they do not stack
            # on top of the peak. Matters for a wide table's large page.
            del payload
        except (UnicodeError, json.JSONDecodeError) as error:
            # 221: Cannot read staged snapshot page {} for '{}'
            raise InformixError(_m(221, page_index, table.exposed_name)) from error
        if not isinstance(record, dict):
            # 223: Invalid staged snapshot page {} for '{}'
            raise InformixError(_m(223, page_index, table.exposed_name))
        digest = record.get("sha256")
        body = {key: value for key, value in record.items() if key != "sha256"}
        # Hash the canonical body, then release that (also up to ~256 MB) before the
        # validation so it does not coexist with the row graph the caller decodes.
        computed_digest = hashlib.sha256(
            json.dumps(body, allow_nan=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
        ).hexdigest()
        del body
        if (
            # pylint: disable-next=too-many-boolean-expressions
            record.get("version") != _SNAPSHOT_STAGE_VERSION
            or record.get("table") != table.native_identity
            or record.get("pipeline_scope") != pipeline_scope
            or record.get("schema_id") != schema_id
            or record.get("page_index") != page_index
            or isinstance(record.get("row_count"), bool)
            or record.get("row_count") != len(record.get("rows", []))
            or not isinstance(digest, str)
            or not hmac.compare_digest(digest, computed_digest)
        ):
            # 223: Invalid staged snapshot page {} for '{}'
            raise InformixError(_m(223, page_index, table.exposed_name))
        return record

    def _stream_staged_page(
        self,
        table: Table,
        pipeline_scope: str,
        schema_id: str,
        snapshot_lsn: int,
        page_index: int,
    ) -> tuple[dict[str, Any], Iterator[dict[str, Any]]]:
        # Docstring: Read a staged page without ever materializing its whole row graph.
        #
        # The blocker for wide pages is that ``json.loads`` builds the entire page's
        # Python object graph (multiple GB) before a row is served, OOM-ing the
        # worker. Here the page is parsed structurally instead: the small header
        # fields are decoded, the huge ``rows`` array is *located* (not parsed), and
        # the rows are streamed one at a time by the returned generator, so peak
        # memory is the ~256 MB page text plus a single row rather than the whole
        # graph.
        #
        # Integrity is still verified over the exact same bytes the writer hashed.
        # The writer stores ``sha256(json.dumps(body))`` and the page is
        # ``json.dumps({**body, "sha256": digest})`` with identical ``json.dumps``
        # settings, so the page bytes with the ``"sha256":"..."`` field excised are
        # byte-for-byte that canonical body -- hashed here via a ``memoryview`` (no
        # copy). ``json.dumps`` defaults to ``ensure_ascii=True``, so the page is
        # ASCII and character offsets equal byte offsets, letting the text scan and
        # the byte hash share indices.
        #
        # Returns ``(header, rows)`` where ``header`` holds every field except
        # ``rows`` (already validated for version/identity/scope/schema/page and
        # integrity) and ``rows`` is a single-use generator of decoded rows that also
        # enforces the row count.

        return self._stream_staged_page_for(
            table.native_identity,
            table.exposed_name,
            pipeline_scope,
            schema_id,
            snapshot_lsn,
            page_index,
        )

    def _stream_staged_page_for(
        self,
        native_identity: str,
        exposed_name: str,
        pipeline_scope: str,
        schema_id: str,
        snapshot_lsn: int,
        page_index: int,
    ) -> tuple[dict[str, Any], Iterator[dict[str, Any]]]:
        # Docstring: Identity-string form of :meth:`_stream_staged_page`.
        #
        # Takes the page's identity strings instead of a ``Table`` so it runs on a
        # Spark executor (the partitioned reader's ``read_partition``) without a live
        # Informix catalog lookup. Integrity, structural scan, and row-count checks
        # are identical.

        payload = self._read_staged_page_payload_for(
            native_identity, exposed_name, pipeline_scope, schema_id, snapshot_lsn, page_index
        )

        def invalid() -> InformixError:
            # 223: Invalid staged snapshot page {} for '{}'
            return InformixError(_m(223, page_index, exposed_name))

        try:
            text = payload.decode("ascii")
        except UnicodeError as error:
            raise invalid() from error
        try:
            header, array_start, sha_span = _scan_staged_page(text)
        except InformixError as error:
            raise invalid() from error
        # Integrity over the exact canonical body: the page bytes with the sha256
        # field excised are byte-identical to what the writer hashed. memoryview
        # slices avoid copying the (up to 256 MB) payload.
        stored_digest = header.get("sha256")
        digest = hashlib.sha256()
        view = memoryview(payload)
        digest.update(view[: sha_span[0]])
        digest.update(view[sha_span[1] :])
        computed_digest = digest.hexdigest()
        view.release()
        del payload
        if not isinstance(stored_digest, str) or not hmac.compare_digest(
            computed_digest, stored_digest
        ):
            raise invalid()
        expected_row_count = header.get("row_count")
        expected_identity = (
            _SNAPSHOT_STAGE_VERSION,
            native_identity,
            pipeline_scope,
            schema_id,
            page_index,
        )
        actual_identity = (
            header.get("version"),
            header.get("table"),
            header.get("pipeline_scope"),
            header.get("schema_id"),
            header.get("page_index"),
        )
        if (
            actual_identity != expected_identity
            or isinstance(expected_row_count, bool)
            or not isinstance(expected_row_count, int)
        ):
            raise invalid()

        def stream_rows() -> Iterator[dict[str, Any]]:
            row_decoder = json.JSONDecoder()
            position = _skip_json_whitespace(text, array_start + 1)
            count = 0
            # array_end is just past the matching ']'; a non-empty array has an
            # element here, an empty one has the ']' itself.
            if text[position] != "]":
                while True:
                    row, position = row_decoder.raw_decode(text, position)
                    if not isinstance(row, dict):
                        # 224: Invalid staged snapshot rows for '{}'
                        raise InformixError(_m(224, exposed_name))
                    count += 1
                    yield _decode_snapshot_stage_value(row)
                    position = _skip_json_whitespace(text, position)
                    if position < len(text) and text[position] == ",":
                        position = _skip_json_whitespace(text, position + 1)
                    elif position < len(text) and text[position] == "]":
                        break
                    else:
                        # 224: Invalid staged snapshot rows for '{}'
                        raise InformixError(_m(224, exposed_name))
            if count != expected_row_count:
                # 225: Staged snapshot row count mismatch for '{}'
                raise InformixError(_m(225, exposed_name))

        return header, stream_rows()

    def _snapshot_cleanup_executor(self) -> concurrent.futures.ThreadPoolExecutor:
        # Docstring: Lazily start the background pool that deletes consumed staged pages.
        #
        # Created on first use rather than in ``__init__`` and never pickled (see
        # ``close``/``__getstate__``): it is a driver-only, non-serializable object.

        executor = self._snapshot_cleanup_executor_instance
        if executor is None:
            executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=_SNAPSHOT_CLEANUP_THREADS,
                thread_name_prefix="informix-page-cleanup",
            )
            self._snapshot_cleanup_executor_instance = executor
        return executor

    @staticmethod
    def _delete_snapshot_page(path: str) -> None:
        # Docstring: Remove one consumed staged page. Best-effort: a leak only wastes space.

        try:
            PurePythonInformixBridge._remove_connection_slot_tree(path)
        except FileNotFoundError:
            pass
        except OSError:
            # 226: Cannot remove consumed Informix snapshot page: {}
            logging.getLogger(__name__).warning(
                _m(226, path),
                exc_info=True,
            )

    def _cleanup_consumed_snapshot_stage_pages(
        self,
        table: Table,
        pipeline_scope: str,
        schema_id: str,
        snapshot_lsn: int,
        before_page_index: int,
    ) -> None:
        # Docstring: Schedule deletion of pages the reader has advanced past.
        #
        # Incremental and off the serve path. A per-stream high-water mark makes
        # this schedule only the pages consumed since the last call, so it is O(1)
        # amortized per microbatch instead of the old O(page_index) rescan that
        # re-walked every earlier page each time -- at thousands of pages that
        # quadratic pile of FUSE lookups, not decode, dominated serve time. The
        # deletes run on a background pool so the microbatch never blocks on the
        # Volume. The mark lives on the reader instance, so a fresh stream (or a
        # restart) starts at zero and schedules every earlier page once; that
        # catch-up is still asynchronous. Deletion stays best-effort: a leaked page
        # only wastes space and is reclaimed by the retention sweep.

        key = (pipeline_scope, schema_id, snapshot_lsn)
        already_scheduled = self._snapshot_cleanup_watermark.get(key, 0)
        if before_page_index <= already_scheduled:
            return
        self._snapshot_cleanup_watermark[key] = before_page_index
        run = os.path.join(
            self._snapshot_stage_namespace(table, pipeline_scope, schema_id),
            "runs",
            str(snapshot_lsn),
        )
        executor = self._snapshot_cleanup_executor()
        for page_index in range(already_scheduled, before_page_index):
            path = os.path.join(run, f"page-{page_index:08d}")
            executor.submit(self._delete_snapshot_page, path)

    def _maybe_sweep_stale_snapshot_stages(self, table: Table) -> None:
        # Docstring: Submit a one-shot background sweep of this table's stale staged snapshot runs.
        #
        # The consumed-page and completed-stage cleanups reclaim staging in the normal
        # lifecycle, but a crash, an abandoned resnapshot, or a superseded schema/scope can
        # leave a whole ``runs/<snapshot_lsn>`` subtree behind. This is the age-based safety
        # net that ``snapshot.staging.retention.days`` promises. It runs off the serve path
        # (on the same background pool as page deletion) and at most once per table per
        # reader instance -- so it never blocks a microbatch and never rescans -- and it is
        # keyed on file mtime, so an active run (recent page writes) is never swept.

        location = self._snapshot_staging_location
        retention = self._snapshot_staging_retention_seconds
        if not location or retention <= 0:
            return
        identity = table.native_identity
        if identity in self._retention_swept:
            return
        self._retention_swept.add(identity)
        connection_key, table_key = self._table_state_keys_for(identity)
        self._snapshot_cleanup_executor().submit(
            self._sweep_table_staging,
            os.path.join(location, connection_key, table_key),
            float(retention),
        )

    @classmethod
    def _sweep_stale_snapshot_stages(cls, root: str, retention_seconds: float) -> None:
        # Docstring: Delete ``runs/<snapshot_lsn>`` subtrees under a table's staging root whose
        # newest page is older than the retention window.
        #
        # Best-effort and idempotent, and safe to run concurrently with serving: an active
        # run's pages carry a recent mtime so it is never selected, and a path that is
        # missing or removed by a racing cleanup is skipped. Runs on a background thread.

        cutoff = time.time() - retention_seconds
        emptied = []
        for scope in _staging_subdirectories(root):
            schemas = _staging_subdirectories(scope)
            runs = [os.path.join(schema, "runs") for schema in schemas]
            emptied += _unchanged_directories(cutoff, *runs, *schemas, scope)
            for path in runs:
                for run in _staging_subdirectories(path):
                    cls._remove_stale_stage_run(run, cutoff)
        for path in emptied:
            with contextlib.suppress(OSError):  # not empty
                os.rmdir(path)

    @classmethod
    def _sweep_stale_scan_progress(cls, root: str, retention_seconds: float) -> None:
        # Docstring: Delete the index-ordered scans' progress (``snapshot-scan/``) under a
        # table's staging root once it is older than the retention window and the scan
        # resume window both.
        #
        # A finished scan's progress goes when its stream commits (_discard_scan_progress),
        # but that of a dropped table, a deleted pipeline, or a stream outside a pipeline
        # (whose random scope no later run resumes) was kept for good. A record still young
        # enough for a restart to continue from is never selected.

        cutoff = time.time() - max(float(retention_seconds), float(_SCAN_RESUME_SECONDS))
        emptied = []
        for scope in _staging_subdirectories(root):
            schemas = _staging_subdirectories(scope)
            emptied += _unchanged_directories(cutoff, *schemas, scope)
            for schema in schemas:
                for boundary in _staging_subdirectories(schema):
                    cls._remove_stale_stage_run(boundary, cutoff)
        for path in emptied:
            with contextlib.suppress(OSError):  # not empty
                os.rmdir(path)
        try:
            with os.scandir(root) as entries:
                pointers = [
                    entry.path
                    for entry in entries
                    if entry.name.endswith(".json")
                    and entry.is_file(follow_symlinks=False)
                    and entry.stat(follow_symlinks=False).st_mtime < cutoff
                ]
        except OSError:
            return  # nothing recorded there, or unreadable
        for pointer in pointers:
            with contextlib.suppress(FileNotFoundError):
                try:
                    os.remove(pointer)
                except OSError:
                    # 227: Cannot remove stale Informix snapshot run: {}
                    logging.getLogger(__name__).warning(_m(227, pointer), exc_info=True)

    @staticmethod
    def _remove_stale_stage_run(run: str, cutoff: float) -> None:
        # Docstring: Delete one staged run whose newest page is older than ``cutoff``.

        try:
            if _created_since(run, cutoff):
                return
        except OSError:
            return  # vanished or unreadable -- nothing to reclaim
        try:
            PurePythonInformixBridge._remove_connection_slot_tree(run)
        except FileNotFoundError:
            pass
        except OSError:
            # 227: Cannot remove stale Informix snapshot run: {}
            logging.getLogger(__name__).warning(_m(227, run), exc_info=True)

    def _maybe_sweep_connection_staging(self, interval_seconds: float) -> None:
        # Docstring: Sweep every table's stale staging under this connection's staging root, at
        # most once per interval across the readers staging there.
        #
        # The per-table sweep reaches only the tables a reader serves, so the pages of a
        # dropped table or a deleted pipeline would stay. The reader that takes the root's
        # claim in Lakebase runs the same age-based sweep over every table's root, in the
        # background, with its own retention.

        location = self._snapshot_staging_location
        retention = self._snapshot_staging_retention_seconds
        if not location or retention <= 0:
            return
        connection_key = self._table_state_keys_for("")[0]
        root = os.path.normpath(os.path.join(location, connection_key))
        # Each staging root has its own claim: two connections to one database can stage
        # under different locations, and each is swept by a reader that stages there.
        claim = f"{connection_key}/staging-sweep/{hashlib.sha256(root.encode()).hexdigest()[:16]}"
        try:
            claimed = self._state(
                claim_staging_sweep,
                self._lakebase_state_namespace(),
                claim,
                interval_seconds,
                operation="staging sweep claim",
            )
        except Exception:  # pylint: disable=broad-except
            # 431: Cannot claim the connection-wide staging sweep in Lakebase; a later read
            # tries again
            logging.getLogger(__name__).warning(_m(431), exc_info=True)
            return
        if claimed:
            self._snapshot_cleanup_executor().submit(
                self._sweep_connection_staging, root, retention
            )

    @classmethod
    def _sweep_connection_staging(cls, root: str, retention_seconds: float) -> None:
        for table in _staging_subdirectories(root):
            cls._sweep_table_staging(table, retention_seconds)

    @classmethod
    def _sweep_table_staging(cls, root: str, retention_seconds: float) -> None:
        # Docstring: The age-based sweep of one table's staging root: its staged runs and
        # its index-ordered scans' progress.
        cls._sweep_stale_snapshot_stages(
            os.path.join(root, "snapshot-page-data"), float(retention_seconds)
        )
        cls._sweep_stale_scan_progress(
            os.path.join(root, "snapshot-scan"), float(retention_seconds)
        )

    def _read_snapshot_stage_manifest(
        self, table: Table, pipeline_scope: str, schema_id: str
    ) -> dict[str, Any] | None:
        record = self._read_immutable_head(
            self._snapshot_manifest_namespace(table, pipeline_scope, schema_id)
        )
        if record is None:
            return None
        if (
            # pylint: disable-next=too-many-boolean-expressions
            record.get("record_type") != "snapshot-page-manifest"
            or record.get("table") != table.native_identity
            or record.get("scope") != pipeline_scope
            or record.get("schema_id") != schema_id
            or isinstance(record.get("page_count"), bool)
            or not isinstance(record.get("page_count"), int)
            or int(record["page_count"]) < 0
            or isinstance(record.get("row_count"), bool)
            or not isinstance(record.get("row_count"), int)
            or int(record["row_count"]) < 0
        ):
            # 228: Invalid staged snapshot manifest for '{}'
            raise InformixError(_m(228, table.exposed_name))
        _strict_lsn(record.get("snapshot_lsn"), "snapshot_lsn")
        return record

    def _publish_snapshot_stage_manifest(
        self,
        table: Table,
        pipeline_scope: str,
        schema_id: str,
        snapshot_lsn: int,
        page_count: int,
        row_count: int,
        first_upper_pk: str | None = None,
        scanned: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        # scanned is what a keyless table's index-ordered scan recorded for the stream.
        # first_upper_pk is page 0's header upper_pk as sorted-key JSON text, so the first
        # microbatch's end offset can be built without reading the page. Text, because the
        # jsonb column reorders nested keys and the offset must match the page's byte for byte.
        recorded = {} if first_upper_pk is None else {"first_upper_pk": first_upper_pk}
        self._publish_immutable_head(
            self._snapshot_manifest_namespace(table, pipeline_scope, schema_id),
            {
                **recorded,
                **(scanned or {}),
                "created_at": time.time(),
                "page_count": page_count,
                "row_count": row_count,
                "schema_id": schema_id,
                "scope": pipeline_scope,
                "snapshot_lsn": str(snapshot_lsn),
                "table": table.native_identity,
            },
            record_type="snapshot-page-manifest",
        )
        manifest = self._read_snapshot_stage_manifest(table, pipeline_scope, schema_id)
        if (
            manifest is None
            or int(manifest["snapshot_lsn"]) != snapshot_lsn
            or int(manifest["page_count"]) != page_count
            or int(manifest["row_count"]) != row_count
        ):
            # 229: Conflicting staged snapshot manifest for '{}'
            raise InformixError(_m(229, table.exposed_name))
        return manifest

    def _staged_snapshot_result(
        self,
        table: Table,
        pipeline_scope: str,
        schema_id: str,
        manifest: dict[str, Any],
        page_index: int,
        checkpoint: dict | None = None,
        *,
        stage_scope: str | None = None,
    ) -> tuple[Iterator[dict], dict]:
        # stage_scope is where the pages physically live (the capturing flow's scope for
        # a handoff relocation); pipeline_scope is this flow's own scope, stamped on the
        # emitted offsets. They coincide for every non-handoff serve.
        stage_scope = stage_scope or pipeline_scope
        page_count = int(manifest["page_count"])
        snapshot_lsn = _strict_lsn(manifest["snapshot_lsn"], "snapshot_lsn")
        if page_index < 0 or page_index >= page_count:
            # 230: Invalid staged snapshot page index {} for '{}'
            raise InformixError(_m(230, page_index, table.exposed_name))

        def read_page() -> tuple[dict[str, Any], Iterator[dict[str, Any]]]:
            # Stream the page: the header (all fields except rows) is parsed and
            # integrity-verified eagerly, and rows are yielded one at a time by the
            # returned generator, so the whole-page object graph is never materialized.
            header, rows = self._stream_staged_page(
                table, stage_scope, schema_id, snapshot_lsn, page_index
            )
            if _strict_lsn(header.get("snapshot_lsn"), "snapshot_lsn") != snapshot_lsn:
                # 231: Staged snapshot boundary mismatch for '{}'
                raise InformixError(_m(231, table.exposed_name))
            return header, rows

        # An index-ordered scan of a table keyed by primary.keys leaves the rows that
        # changed index value during it out of the pages, and reads them once more into a
        # page of their own (see _index_scan_snapshot).
        moved = self._note_moved_rows(table, stage_scope, schema_id, manifest)

        def unmoved(rows: Iterator[dict[str, Any]]) -> Iterator[dict[str, Any]]:
            return _without_moved_rows(rows, _moved_rows_of_page(moved, page_index))

        if self._defer_staged_rows and page_index == 0:
            # The caller (the partitioned reader's first microbatch, or a drain worker)
            # needs only the end offset; page 0 is read here only if its rows are iterated
            # after all. The boundary cursor comes from the manifest, or from the page
            # header for a manifest an earlier connector version published; a one-page
            # snapshot needs neither.
            recorded = manifest.get("first_upper_pk")
            if page_count == 1:
                header = {}
            elif isinstance(recorded, str):
                header = {"upper_pk": json.loads(recorded)}
            else:
                header = read_page()[0]
            rows = _DeferredStagedPage(
                lambda: unmoved(read_page()[1]),
                (stage_scope, schema_id, snapshot_lsn, 0, 1, page_count),
            )
        else:
            header, rows = read_page()
            rows = unmoved(rows)
        # The shared reader replays a batch from its start offset. Receiving
        # page_index=N as that start means Spark has advanced beyond every page
        # below N; retries continue from N, so those earlier immutable pages are
        # no longer required. Keep the current page and manifest intact.
        self._cleanup_consumed_snapshot_stage_pages(
            table,
            stage_scope,
            schema_id,
            snapshot_lsn,
            page_index,
        )
        next_page = page_index + 1
        if next_page < page_count:
            # The stored ``upper_pk`` is the encoded PK of the page's last row --
            # identical to encoding decoded[-1]'s key columns -- so the resume cursor
            # matches the whole-decode path without holding any row in memory.
            last_pk = header.get("upper_pk")
            end = _offset(
                snapshot_lsn,
                snapshot_lsn,
                snapshot_lsn,
                None,
                "snapshot",
                table,
                schema_id,
                pipeline_scope,
            )
            end.update(
                {
                    "snapshot_lsn": str(snapshot_lsn),
                    "snapshot": {"last_pk": last_pk, "page_index": next_page},
                }
            )
            # Carry stage_scope forward so the next microbatch still finds the pages under
            # the capturing scope; it is dropped at the stream transition (else branch),
            # after which the offset is an ordinary new-scope stream offset.
            if stage_scope != pipeline_scope:
                end["stage_scope"] = stage_scope
            # Staged pages remain, so work outstanding is a certainty rather than
            # an estimate. Carrying the streak here keeps a mid-snapshot reader
            # ranked above an idle one while it competes for slots; the streak is
            # cleared when the final page is served (the branch below).
            end = _with_backlog_streak(end, checkpoint or {}, True)
        else:
            end = _offset(
                snapshot_lsn,
                snapshot_lsn,
                snapshot_lsn,
                None,
                "stream",
                table,
                schema_id,
                pipeline_scope,
            )
            end = self._naming_scan(end, table, stage_scope, schema_id, manifest)
        return rows, end

    def _naming_scan(
        self,
        end: dict[str, Any],
        table: Table,
        stage_scope: str,
        schema_id: str,
        manifest: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        # Docstring: The stream offset a snapshot ends on, naming the scope its pages were
        # staged under when it was an index-ordered scan the stream has changes to drop
        # for.
        #
        # The scan's manifest stays under that scope, and the stream needs it until it is
        # past the scan's end (see _read_stream, which keeps the name until then). An
        # offset without the name loses it when the flow's scope changes before the
        # stream's first read: a table.migration relocation, whether it serves the pages
        # itself or takes the table over right after them, looks for the manifest under
        # its own scope, finds none and replays the changes the pages hold, and the
        # cleanup of earlier scopes removes the manifest.
        if manifest is None:
            manifest = self._read_snapshot_stage_manifest(table, stage_scope, schema_id)
        if manifest and manifest.get("index_scan"):
            end["scan_scope"], end["scan_schema"] = stage_scope, schema_id
        if end.get("pipeline_scope") and stage_scope != end["pipeline_scope"]:
            # Pages staged under another flow's scope (a table.migration relocation): the
            # first stream read cleans them up from there. Without the name it looked
            # under the flow's own scope, and the last batch's pages waited for the
            # retention sweep.
            end["staged_scope"] = stage_scope
        return end

    def _incremental_datetime_chunk_exprs(
        self, table: Table, options: dict[str, str]
    ) -> tuple[dict[str, str], bool]:
        # Docstring: Return ``(chunk_exprs, viable)`` for incremental chunking.
        #
        # ``chunk_exprs`` maps each DATETIME primary-key column to a fixed-width
        # SQL text expression so keyset pagination can compare strings instead of
        # DATETIME literals. Any contiguous DATETIME qualifier is supported,
        # including HOUR-anchored TIME-of-day and DATE-only ranges, and a DATETIME
        # key column combined with non-DATETIME key columns in a composite key.
        # ``viable`` is False when the table has a DATETIME key that cannot be
        # string-chunked (option off or a malformed qualifier), signalling the
        # caller to fall back to the blocking snapshot rather than risk skipped
        # rows.

        if any(
            column.name in table.primary_keys
            and (
                column.type_name == "INTERVAL"
                or column.type_name in _FLOAT_KEY_TYPES
                or column.type_name in _BOOLEAN_KEY_TYPES
                # Ordered by the locale's collation, which a Python comparison of the
                # cursor and the bound can't follow: past the bound too early, the copy
                # ended with rows left.
                or column.type_name in {"NCHAR", "NVARCHAR"}
            )
            for column in table.columns
        ):
            # An INTERVAL key has no order-preserving string cast (a sign and an
            # unpadded leading field), a float key has no exact literal for the keyset
            # bound, a BOOLEAN key has no order, and an NCHAR or NVARCHAR one the locale's,
            # so each is copied by the blocking snapshot, whose positional paging never
            # renders a key back into SQL or compares one in Python.
            return {}, False
        datetime_keys = [
            column
            for column in table.columns
            if column.name in table.primary_keys and column.type_name == "DATETIME"
        ]
        if not datetime_keys:
            return {}, True
        enabled = _option_bool(
            options,
            "snapshot.incremental.datetime.as.string",
            _option_bool(self.options, "snapshot.incremental.datetime.as.string", True),
        )
        if not enabled:
            return {}, False
        exprs: dict[str, str] = {}
        for column in datetime_keys:
            expr = datetime_order_preserving_cast(_sql_name(column.name), int(column.length or 0))
            if expr is None:
                return {}, False
            exprs[column.name] = expr
        return exprs, True

    def _read_incremental(
        self,
        table: Table,
        start: dict | None,
        options: dict[str, str],
        *,
        pipeline_scope_override: str | None = None,
    ) -> tuple[Iterator[dict], dict]:
        # Docstring: Interleave a PK-chunked snapshot with the live CDC stream.
        #
        # Unlike the blocking consistent snapshot, CDC begins immediately at the
        # captured boundary and the table is copied in primary-key chunks, one
        # per microbatch, interleaved with change events. Each chunk is read in
        # its own short repeatable-read transaction and its READ rows are stamped
        # with the LSN captured just before that chunk's SELECT. Correctness for
        # keyed (apply_changes) targets follows from LSN sequencing: any
        # concurrent or later change to a chunked key commits at a higher LSN and
        # supersedes the READ row, so no in-memory deduplication window is needed.

        checkpoint = _validated_offset(start) if start else None
        # A truncated destination discards the offset, and with it the whole
        # incremental block, so this branch is also what a full refresh takes.
        if checkpoint is None:
            return self._begin_incremental(
                table, None, options, pipeline_scope_override=pipeline_scope_override
            )
        return self._read_incremental_step(table, checkpoint, options)

    def _begin_incremental(
        self,
        table: Table,
        checkpoint: dict | None,
        options: dict[str, str],
        *,
        pipeline_scope_override: str | None = None,
    ) -> tuple[Iterator[dict], dict]:
        if checkpoint is not None:
            # 233: Informix incremental snapshot for '{}' resumed without its progress state; run a
            # full refresh
            raise InformixError(_m(233, table.exposed_name))
        pipeline_scope = pipeline_scope_override or self._pipeline_scope()
        table = self._refresh_table_schema(table, None)
        if not _cdc_capable(table):
            # 234: Table '{}' is not CDC-capable; incremental snapshot requires a primary key and
            # CDC-supported columns
            raise InformixError(_m(234, table.exposed_name))
        # On the append path nothing merges what the copy and the stream both hold, so the
        # copy follows the stream there (see _drop_copied_changes): one paged by a NOT NULL
        # key of the table's own that the index-ordered snapshot pages by too, so that each
        # change has one row to compare with. Any other takes the snapshot initial takes.
        exact = self._append_only_table(table, options)
        if exact:
            index = self._scan_index(table, options)
            keys = list(table.primary_keys)
            if (
                table.key_only_from_option
                or index is None
                or index[0] != keys
                or any(column.nullable for column in table.columns if column.name in keys)
            ):
                return self._read_snapshot(
                    table, None, options, pipeline_scope_override=pipeline_scope_override
                )
            # A replay of the copy's first batch serves the chunk that batch staged, after the
            # stream from its boundary: Spark goes on from the end it logged.
            logged = json.loads(options.get(_REPLAY_INITIAL_END_OPTION) or "{}")
            copy = logged.get("incremental")
            if isinstance(copy, dict) and copy.get("exact"):
                lsn = copy["boundary_lsn"]
                logged.update(
                    commit_lsn=lsn,
                    change_lsn=lsn,
                    begin_lsn=lsn,
                    tx_id=None,
                    incremental={
                        **copy,
                        "last_pk": None,
                        "done": False,
                        "chunks": 0,
                        "windows": [copy["seed"]],
                    },
                )
                return self._read_incremental_step(table, _validated_offset(logged), options)
        # snapshot.scan=cursor reads without key order, so there are no key ranges to
        # chunk by: take the blocking consistent snapshot, which honours the cursor scan.
        # So does a table keyed by primary.keys alone whose snapshot pages by an index: the
        # chunks follow a unique key, and an index value has many rows.
        by_index = table.key_only_from_option and self._scan_index(table, options) is not None
        if by_index or self._snapshot_cursor_scan(options):
            # 235: Informix incremental snapshot for '{}' using the blocking consistent snapshot
            # because {}={} reads without key order
            logging.getLogger(__name__).info(
                _m(
                    235,
                    table.exposed_name,
                    _SNAPSHOT_SCAN_OPTION,
                    "keyset over an index" if by_index else _SNAPSHOT_SCAN_CURSOR,
                ),
            )
            return self._read_snapshot(
                table, None, options, pipeline_scope_override=pipeline_scope_override
            )
        # DATETIME primary keys cannot use native keyset (WHERE pk > ?)
        # pagination reliably. When every DATETIME key column has an
        # order-preserving string cast we chunk by that string instead;
        # otherwise fall back to the blocking consistent snapshot.
        chunk_exprs, viable = self._incremental_datetime_chunk_exprs(table, options)
        if not viable:
            # 236: Informix incremental snapshot for '{}' falling back to the blocking consistent
            # snapshot because a DATETIME, INTERVAL or floating-point primary-key column cannot be
            # chunked by an exact key bound
            logging.getLogger(__name__).info(
                _m(236, table.exposed_name),
            )
            return self._read_snapshot(
                table, None, options, pipeline_scope_override=pipeline_scope_override
            )
        table, schema_id, incremental = self._seed_incremental_block(
            table,
            pipeline_scope,
            chunk_exprs,
            _snapshot_filter(options),
            floor=not _replay_keeps_floor(options),
            resnapshot=_RESNAPSHOT_OPTION in options,
        )
        if exact:
            # Where its chunks are staged, how many it has read, and the windows still open.
            # The first is the bound's: its read found no row above max_pk, so it holds every
            # key there, as a chunk with none, from the boundary to the log's end after it.
            seed = [
                -1,
                incremental["max_pk"],
                None,
                incremental["boundary_lsn"],
                str(
                    self._snapshot_read_with_reconnect(
                        self._bridge.current_lsn, table=table, operation="key bound end"
                    )
                ),
            ]
            incremental.update(
                exact=[pipeline_scope, schema_id], chunks=0, windows=[seed], seed=seed
            )
        high_water = int(incremental["boundary_lsn"])
        seed = _offset(
            high_water,
            high_water,
            high_water,
            None,
            "stream",
            table,
            schema_id,
            pipeline_scope,
            incremental=incremental,
        )
        # An empty table needs no chunks; behave as a plain stream from here.
        if incremental["done"]:
            seed.pop("incremental", None)
            return iter(()), seed
        return self._read_incremental_step(table, _validated_offset(seed), options)

    def _seed_incremental_block(
        self,
        table: Table,
        pipeline_scope: str,
        chunk_exprs: dict[str, str],
        snapshot_filter: str | None,
        *,
        floor: bool = True,
        resnapshot: bool = False,
    ) -> tuple[Table, str, dict]:
        # Docstring: Capture a fresh, self-describing incremental snapshot block.
        #
        # The correctness of an incremental copy rests on one ordering: ``max_pk``
        # is read at or after the CDC start LSN. A row above ``max_pk`` is then
        # necessarily inserted later, commits above the start LSN, and so is
        # carried by the change stream that begins there. Reverse the order and
        # rows inserted in between fall above the key bound *and* below the stream
        # start -- owned by neither, and lost silently.
        #
        # Production hit exactly that (tw214): a cancelled update published
        # max_pk=W0056332 at 22:41:58, the restart published a fresh, higher
        # initial_lsn at 22:46:29, and the copy ran with the OLD key bound against
        # the NEW stream start. Four rows inserted in between were never emitted.
        # The bound cannot have been captured fresh -- rows above it already
        # existed when the copy began.
        #
        # ``max_pk`` rides in the Spark checkpoint while the LSN is published
        # durably per scope, so nothing tied the pair together. Stamping the block
        # with the scope that captured it makes the pair checkable wherever it is
        # later resumed; see ``_incremental_bound_is_foreign``.

        high_water = self._initial_lsn(table, scope=pipeline_scope)
        schema_id = self._snapshot_schema_ids[(pipeline_scope, table.identity)]
        table = self._refresh_table_schema(table, _schema_fingerprint(table))
        self._publish_snapshot_boundary(
            table,
            schema_id,
            high_water,
            high_water,
            pipeline_scope,
            floor=floor,
            resnapshot=resnapshot,
        )
        # Strictly after the boundary above, never before it.
        max_pk = self._snapshot_read_with_reconnect(
            lambda: self._bridge.max_primary_key(
                table.identity,
                list(table.primary_keys),
                chunk_exprs=chunk_exprs or None,
                snapshot_filter=_snapshot_predicate(snapshot_filter, table),
                key_descending=list(table.key_descending),
            ),
            table=table,
            operation="key bound read",
        )
        incremental = {
            "started": True,
            "last_pk": None,
            "max_pk": (None if max_pk is None else _encode_snapshot_stage_value(list(max_pk))),
            "done": max_pk is None,
            "chunk_exprs": chunk_exprs,
            "snapshot_filter": snapshot_filter,
            # Provenance: which scope captured this block, and against which
            # boundary. A resumed block that disagrees with either describes a
            # different copy and cannot be trusted.
            "scope": pipeline_scope,
            "boundary_lsn": str(high_water),
            # The key ordering the cursor and bound were paged in. If the table's
            # key index direction later differs (e.g. an all-ascending checkpoint
            # resumed under mixed-direction paging), the cursor is not comparable
            # across the change and resuming would silently skip rows -- so the
            # copy must restart. See the guard in _next_incremental_chunk.
            "key_descending": [bool(v) for v in table.key_descending],
        }
        return table, schema_id, incremental

    def _rebound_incremental_block(
        self, table: Table, incremental: dict, chunk_exprs: dict[str, str]
    ) -> dict:
        # Docstring: Recapture ``max_pk`` when the block was bounded by another update.
        #
        # The snapshot bound is only sound if it was read at or after the CDC start
        # LSN this update is streaming from: a row above ``max_pk`` then necessarily
        # commits above that LSN, so the change stream carries it. A bound inherited
        # from an earlier update inverts that -- it was read before this update's
        # boundary, so rows inserted in between sit above the key bound and below
        # the stream start, owned by neither side.
        #
        # Production hit exactly that (tw214). A cancelled update bounded the copy
        # at W0056332 at 22:41:58; the restart published a fresh, higher boundary at
        # 22:46:29 and copied with the OLD bound. Four rows inserted in between were
        # never emitted. The bound provably could not have been captured fresh --
        # rows above it already existed when that copy began.
        #
        # Only the bound is recaptured. ``last_pk`` is kept: it names rows this
        # destination still holds, because the framework only discards the offset
        # when it truncates the table, and a discarded offset takes the whole
        # ``incremental`` block with it (start is None, which re-seeds from
        # scratch). Resetting the cursor here would re-read a prefix that is still
        # present -- measured as a 5x throughput drop across restarts -- and protect
        # nothing.

        max_pk = self._snapshot_read_with_reconnect(
            lambda: self._bridge.max_primary_key(
                table.identity,
                list(table.primary_keys),
                chunk_exprs=chunk_exprs or None,
                snapshot_filter=_snapshot_predicate(incremental.get("snapshot_filter"), table),
                key_descending=list(table.key_descending),
            ),
            table=table,
            operation="key bound read",
        )
        rebound = dict(incremental)
        rebound["max_pk"] = None if max_pk is None else _encode_snapshot_stage_value(list(max_pk))
        rebound["scope"] = self._registration_scope
        if max_pk is None:
            # The table is empty as of this boundary; the change stream owns it.
            rebound["done"] = True
        return rebound

    def _incremental_bound_is_foreign(self, incremental: dict) -> bool:
        # Docstring: True when this block's bound was captured by a different update.

        # The reader's own registration, read directly rather than through
        # _pipeline_scope: that helper falls back to the checkpoint's own
        # pipeline_scope field, which _read_stream rewrites to the reading scope
        # on every batch (see end["pipeline_scope"] there). Comparing the stamp
        # against a rewritten field would let a foreign block vouch for itself.
        scope = self._registration_scope
        if scope is None:
            # No registration to compare against, so the stamp cannot be checked.
            return False
        return incremental.get("scope") != scope
