"""Pure-Python, serverless-capable Informix snapshot and CDC connector.

The SQLI, CDC framing, codec, transaction, and Lakeflow paths are covered by
source-local regression tests. Informix 15 and serverless Lakeflow pipelines
have validated authentication, queries, discovery, snapshots, and CDC.
"""

from __future__ import annotations

import concurrent.futures
import contextlib
import fnmatch
import hashlib
import logging
import math
import secrets
import threading
import time
from typing import Any, Callable

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.interface.supports_partition import (
    SupportsPartitionedStream,
)
from databricks.labs.community_connector.sources.informix.informix_bridge import (
    PurePythonInformixBridge,
)
from databricks.labs.community_connector.sources.informix.informix_deletes import (
    _DeleteChannelMixin,
)
from databricks.labs.community_connector.sources.informix.informix_host_sharing import (
    _client_encoding,
    _database_encoding,
    _patterns,
    _refuse_code_loading_options,
    _unl_char,
    _validate_unl_field_format,
)
from databricks.labs.community_connector.sources.informix.informix_index_scan import _IndexScanMixin
from databricks.labs.community_connector.sources.informix.informix_keys import (
    _PROMOTE_UNIQUE_INDEX_OPTION,
    InformixBridge,
    _append_only_value,
    _null_byte_mode,
    _option_bool,
    _pad_varchar_option,
    _parsed_option,
    _snapshot_scan_value,
    _state_gc_project_settings,
    _state_gc_settings,
    _validate_nonneg_float_option,
)
from databricks.labs.community_connector.sources.informix.informix_options import (
    _APPEND_DELETE_TOMBSTONES_OPTION,
    _APPEND_INGESTION_OPTION,
    _CAPACITY_RETRY_MAX_RETRIES,
    _CDC_SPILL_RECORDS_OPTION,
    _CONNECTION_STARTUP_JITTER_OPTION,
    _DAEMON_RESERVATION_OPTION,
    _DEFAULT_CAPACITY_RETRY_MAX_DELAY_SECONDS,
    _DEFAULT_CDC_MAX_RECORDS,
    _DEFAULT_CDC_SPILL_RECORDS,
    _DEFAULT_CONNECTION_WAIT_TIMEOUT_SECONDS,
    _DEFAULT_DAEMON_CONNECTION_RESERVATION,
    _DEFAULT_KEYLESS_SNAPSHOT_PAGE_SIZE,
    _DEFAULT_MAX_CONCURRENT_CONNECTIONS,
    _DEFAULT_MAX_RECORDS_PER_BATCH,
    _DEFAULT_PARTITIONED_SNAPSHOT_PAGES_PER_BATCH,
    _DEFAULT_SHARED_CDC_BUFFER,
    _DEFAULT_SNAPSHOT_PAGE_SIZE,
    _DEFAULT_SNAPSHOT_READ_TIMEOUT_SECONDS,
    _DEFAULT_SNAPSHOT_STAGE_RETENTION_DAYS,
    _DEFAULT_SNAPSHOT_STAGING_TRANSPORT,
    _DROPPED_MOUNT_MAX_RETRIES,
    _IDENTIFIER,
    _IMMUTABLE_STATE_VERSION,
    _PARTITIONED_SNAPSHOT_PAGES_PER_BATCH_OPTION,
    _PARTITIONED_STREAM_OPTION,
    _PIPELINE_SCOPE,
    _SHARED_CDC_BUFFER_OPTION,
    _SHARED_CDC_THREADS_OPTION,
    _SHARED_STATE_WAIT_SECONDS,
    _SNAPSHOT_READER_THREADS_OPTION,
    _SNAPSHOT_SCAN_OPTION,
    _SNAPSHOT_SCHEMA_REFRESH_INTERVAL_SECONDS,
    _SNAPSHOT_STAGING_PIPELINE_OPTION,
    _SNAPSHOT_STAGING_TOKEN_LIFETIME_OPTION,
    _SNAPSHOT_STAGING_TRANSPORT_OPTION,
    _SNAPSHOT_STAGING_TRANSPORTS,
    _TABLE_MIGRATION_OPTION,
    _UNL_DEFAULT_ESCAPE,
    _UNL_ESCAPE_OPTION,
    _UNL_GZIP_RECORDS_OPTION,
    _UNL_SPLIT_BYTES_OPTION,
    _UNL_SPLIT_TARGET_BYTES,
    InformixError,
    _join_identity,
    _m,
    _split_identity,
    _sql_identifier,
)
from databricks.labs.community_connector.sources.informix.informix_partitions import _PartitionMixin
from databricks.labs.community_connector.sources.informix.informix_reader import (
    _ReaderMixin,
    set_bridge_factory,
)
from databricks.labs.community_connector.sources.informix.informix_registration import (
    _INFORMIX_REGISTRATION_CONTEXT,
    LogRetentionError,
    SchemaTransitionUnavailable,
    SharedStateAccessUnavailable,
    Table,
    UnsupportedChangeError,
    _shorten_merged_definitions,
)
from databricks.labs.community_connector.sources.informix.informix_scan import _ScanMixin
from databricks.labs.community_connector.sources.informix.informix_schema import _SchemaMixin
from databricks.labs.community_connector.sources.informix.informix_shards import _shared_cdc_mode
from databricks.labs.community_connector.sources.informix.informix_staging import _StagingMixin
from databricks.labs.community_connector.sources.informix.informix_stream import _StreamMixin
from databricks.labs.community_connector.sources.informix.informix_transactions import (
    TransactionBuffer,
    _redirect_allowlist,
    _strict_lsn,
    _validated_volume_location,
)
from databricks.labs.community_connector.sources.informix.informix_unl import _UnlMixin
from databricks.labs.community_connector.sources.informix.informix_unload import (
    _table_from_schema_state,
    _unl_escape_byte,
)
from databricks.labs.community_connector.sources.informix.lakebase_state import (
    collect_stale_table_state,
    delete_obsolete_scoped_state_records,
    publish_state_record,
    read_state_record,
    touch_table_activity,
)
from databricks.labs.community_connector.sources.informix.lakebase_state_store import (
    validate_lakebase_options,
)
from databricks.labs.community_connector.sources.informix.lakebase_workspace import (
    _DEFAULT_STAGING_TOKEN_LIFETIME_SECONDS,
    _STAGING_TOKEN_MIN_LIFETIME_SECONDS,
    WorkspaceFilesApi,
)


class InformixLakeflowConnect(  # pylint: disable=too-many-ancestors
    _ReaderMixin,
    _PartitionMixin,
    _DeleteChannelMixin,
    _StagingMixin,
    _ScanMixin,
    _IndexScanMixin,
    _UnlMixin,
    _StreamMixin,
    _SchemaMixin,
    LakeflowConnect,
    SupportsPartitionedStream,
):
    # Docstring: Pure-Python connector live-validated on disposable Informix 15.
    #
    # Normal auth, queries, discovery, snapshots, transactional CDC, and
    # serverless Lakeflow pipeline execution have been exercised.

    # pylint: disable-next=too-many-branches,too-many-statements
    def __init__(self, options: dict[str, str]) -> None:
        # A parameter left empty is one not set: a form sends "" for a field nobody
        # filled in, and a number or flag read from "" would fail here or, later, on a
        # worker. The same rule as a table's options (_options_set): only "" counts. A value of
        # blanks is something typed, judged as given; it was dropped here but judged in a
        # table's options, so the same blank was unset at read time and an error at
        # registration.
        #
        # On a copy: the caller's mapping is left as it was, and a read-only one is no
        # longer left with its empty values (its deletes failed, quietly).
        options = options.copy() if callable(getattr(options, "copy", None)) else dict(options)
        with contextlib.suppress(TypeError, AttributeError):
            for name in [
                name for name, value in options.items() if isinstance(value, str) and value == ""
            ]:
                del options[name]
        # Under the names Spark hands them over in, lowercased, as a table's options are
        # (_options_set): constructed directly, "Bridge.Factory" or "Primary.Keys" passed the
        # refusals that look for the lowercase name, and every other option read as unset.
        with contextlib.suppress(TypeError, AttributeError):
            if any(isinstance(name, str) and name != name.lower() for name in options):
                options = {
                    name.lower() if isinstance(name, str) else name: value
                    for name, value in options.items()
                }
        super().__init__(options)
        # The Volume now stores exactly one thing: snapshot page payloads. It has
        # no default to fall back on, because there is no longer a shared state
        # location to borrow one from.
        staging_location = options.get("snapshot.staging.location", "").strip()
        if not staging_location:
            # 158: Missing required Informix option: snapshot.staging.location
            raise ValueError(_m(158))
        # Checked here rather than where state is first read: this runs while the
        # pipeline is being set up, so a missing password fails the update
        # immediately instead of surfacing on the first microbatch that needs state.
        if not options.get("lakebase.password", "").strip():
            # 159: Missing required Informix option: lakebase.password
            raise ValueError(_m(159))
        self._snapshot_staging_location = _validated_volume_location(
            staging_location, "snapshot.staging.location", options
        )
        # Validate numeric configuration without opening a connection.
        for name, default, minimum in (
            ("snapshot.page.size", str(_DEFAULT_SNAPSHOT_PAGE_SIZE), 1),
            (
                "keyless.snapshot.page.size",
                str(_DEFAULT_KEYLESS_SNAPSHOT_PAGE_SIZE),
                1,
            ),
            (
                "snapshot.read.timeout.seconds",
                str(_DEFAULT_SNAPSHOT_READ_TIMEOUT_SECONDS),
                1,
            ),
            ("snapshot.max.rows", "0", 0),
            ("snapshot.max.bytes", "0", 0),
            (
                "snapshot.staging.retention.days",
                str(_DEFAULT_SNAPSHOT_STAGE_RETENTION_DAYS),
                1,
            ),
            (
                _SNAPSHOT_STAGING_TOKEN_LIFETIME_OPTION,
                str(_DEFAULT_STAGING_TOKEN_LIFETIME_SECONDS),
                _STAGING_TOKEN_MIN_LIFETIME_SECONDS,
            ),
            ("metadata.max.bytes", str(64 << 20), 0),
            ("max.records.per.batch", str(_DEFAULT_MAX_RECORDS_PER_BATCH), 1),
            ("cdc.timeout", "5", 1),
            ("cdc.max.records", str(_DEFAULT_CDC_MAX_RECORDS), 1),
            ("cdc.max.frame.bytes", str(16 << 20), 16),
            ("cdc.max.transaction.records", "100000", 1),
            (_CDC_SPILL_RECORDS_OPTION, str(_DEFAULT_CDC_SPILL_RECORDS), 0),
            ("cdc.max.poll.records", "200000", 1),
            ("cdc.max.poll.bytes", "0", 0),
            ("cdc.read.bytes", "32000", 128),
            ("authentication.pam.max.rounds", "16", 1),
            (
                "max.concurrent.connections",
                str(_DEFAULT_MAX_CONCURRENT_CONNECTIONS),
                1,
            ),
            (
                "connection.wait.timeout.seconds",
                str(_DEFAULT_CONNECTION_WAIT_TIMEOUT_SECONDS),
                1,
            ),
            ("redirect.max", "3", 0),
            ("cdc.shared.state.wait.seconds", str(_SHARED_STATE_WAIT_SECONDS), 1),
            (_SHARED_CDC_BUFFER_OPTION, str(_DEFAULT_SHARED_CDC_BUFFER), 1),
            (
                _PARTITIONED_SNAPSHOT_PAGES_PER_BATCH_OPTION,
                str(_DEFAULT_PARTITIONED_SNAPSHOT_PAGES_PER_BATCH),
                1,
            ),
            (_UNL_SPLIT_BYTES_OPTION, str(_UNL_SPLIT_TARGET_BYTES), 1),
            (_UNL_GZIP_RECORDS_OPTION, "0", 0),
            # Read only when a fault comes: a bad value must not wait for one.
            ("dropped.mount.max.retries", str(_DROPPED_MOUNT_MAX_RETRIES), 1),
            ("capacity.retry.max.retries", str(_CAPACITY_RETRY_MAX_RETRIES), 1),
        ):
            try:
                value = int(options.get(name, default))
            except (TypeError, ValueError) as error:
                raise ValueError(f"Option '{name}' must be a whole number") from error
            if value < minimum:
                raise ValueError(f"Option '{name}' must be >= {minimum}")
        # After the check: before it, a retention of "abc" failed with int()'s own error.
        self._snapshot_staging_retention_seconds = (
            int(
                options.get(
                    "snapshot.staging.retention.days",
                    str(_DEFAULT_SNAPSHOT_STAGE_RETENTION_DAYS),
                )
            )
            * 24
            * 60
            * 60
        )
        # The state project's capacity, checked here rather than when a worker first
        # provisions it.
        try:
            low = float(options.get("lakebase.autoscale.min.cu", 0.5))
            high = float(options.get("lakebase.autoscale.max.cu", 2.0))
            suspend = int(options.get("lakebase.suspend.timeout.seconds", 60))
        except (TypeError, ValueError) as error:
            raise ValueError(
                "Options 'lakebase.autoscale.min.cu', 'lakebase.autoscale.max.cu' and "
                "'lakebase.suspend.timeout.seconds' must be numbers"
            ) from error
        if not (math.isfinite(low) and math.isfinite(high) and 0 < low <= high):
            raise ValueError(
                "Options 'lakebase.autoscale.min.cu' and 'lakebase.autoscale.max.cu' must be "
                "positive, with the minimum no greater than the maximum"
            )
        if suspend < 0:
            raise ValueError("Option 'lakebase.suspend.timeout.seconds' must be >= 0")
        # Startup jitter is a float (fractional seconds), validated here rather than in the
        # integer loop above; 0 disables it.
        _validate_nonneg_float_option(options, _CONNECTION_STARTUP_JITTER_OPTION)
        # Read as a float where a CDC session is set up, which a negative value passed as
        # the 30 s floor and text failed only then.
        if "cdc.read.timeout.seconds" in options:
            try:
                cdc_read_timeout = float(options["cdc.read.timeout.seconds"])
            except (TypeError, ValueError) as error:
                raise ValueError("Option 'cdc.read.timeout.seconds' must be a number") from error
            if not math.isfinite(cdc_read_timeout) or cdc_read_timeout <= 0:
                raise ValueError("Option 'cdc.read.timeout.seconds' must be > 0")
        # Shared-reader thread count defaults to a quarter of the connection budget, so it is
        # validated here rather than in the static-default loop above.
        if (
            _SHARED_CDC_THREADS_OPTION in options
            and _parsed_option(options, _SHARED_CDC_THREADS_OPTION, 1) < 1
        ):
            raise ValueError(f"Option '{_SHARED_CDC_THREADS_OPTION}' must be >= 1")
        if (
            _SNAPSHOT_READER_THREADS_OPTION in options
            and _parsed_option(options, _SNAPSHOT_READER_THREADS_OPTION, 1) < 1
        ):
            raise ValueError(f"Option '{_SNAPSHOT_READER_THREADS_OPTION}' must be >= 1")
        # Validate boolean capacity options eagerly (they are otherwise only
        # checked on first read, deep inside a running flow).
        _option_bool(options, "snapshot.incremental.blocking", True)
        _option_bool(options, "connection.backlog.hint.enabled", True)
        _state_gc_settings(options)
        # Read when the first connection is made, deep inside a read: checked here instead.
        _option_bool(options, "encrypt", True)
        _option_bool(options, "redirect.enabled", False)
        _database_encoding(options)
        _client_encoding(options)
        if str(options.get("authentication.mode", "password")).lower() not in {"password", "pam"}:
            # 14: Authentication mode {!r} is unsupported; use password or pam.
            raise InformixError(_m(14, options.get("authentication.mode")))
        _shared_cdc_mode(options)
        # Validate the .unl / HPL DELIMITED field format eagerly (single-character
        # delimiter and optionally-empty escape, which must differ), rather than only
        # on the first file-source read deep inside a running flow.
        _validate_unl_field_format(options)
        _option_bool(options, _PROMOTE_UNIQUE_INDEX_OPTION, True)
        _option_bool(options, _SNAPSHOT_STAGING_PIPELINE_OPTION, True)
        _option_bool(options, _PARTITIONED_STREAM_OPTION, True)
        # Read only as a bridge or the Lakebase state is first made, or a table's state is
        # touched, deep inside a read: checked here instead.
        _refuse_code_loading_options(options)
        _redirect_allowlist(options.get("redirect.allowlist", ""))
        _pad_varchar_option(options)
        validate_lakebase_options(options)
        _state_gc_project_settings(options)
        self._snapshot_mode({})
        _option_bool(options, _TABLE_MIGRATION_OPTION, False)
        _option_bool(options, "snapshot.incremental.datetime.as.string", True)
        # The record splitter scans an extract's bytes for the escape: one byte in its
        # codeset, CLIENT_LOCALE's ("é" passed here under UTF-8 and failed split planning).
        _unl_escape_byte(
            _unl_char(
                options.get(_UNL_ESCAPE_OPTION, _UNL_DEFAULT_ESCAPE),
                _UNL_ESCAPE_OPTION,
                allow_empty=True,
            ),
            _client_encoding(options),
        )
        staging_transport = (
            str(
                options.get(_SNAPSHOT_STAGING_TRANSPORT_OPTION, _DEFAULT_SNAPSHOT_STAGING_TRANSPORT)
            )
            .strip()
            .lower()
        )
        if staging_transport not in _SNAPSHOT_STAGING_TRANSPORTS:
            # 160: Option '{}' must be one of {}; got {!r}
            raise ValueError(
                _m(
                    160,
                    _SNAPSHOT_STAGING_TRANSPORT_OPTION,
                    sorted(_SNAPSHOT_STAGING_TRANSPORTS),
                    staging_transport,
                )
            )
        self._validate_unl_source_options(options)
        _null_byte_mode(options)
        if _APPEND_INGESTION_OPTION in options:
            _append_only_value(options[_APPEND_INGESTION_OPTION])
        if _APPEND_DELETE_TOMBSTONES_OPTION in options:
            _option_bool(options, _APPEND_DELETE_TOMBSTONES_OPTION, True)
        if _SNAPSHOT_SCAN_OPTION in options:
            _snapshot_scan_value(options[_SNAPSHOT_SCAN_OPTION])
        if int(options.get("cdc.max.records", str(_DEFAULT_CDC_MAX_RECORDS))) > 256:
            # 161: Option 'cdc.max.records' must be <= 256
            raise ValueError(_m(161))
        if int(options.get("cdc.read.bytes", "32000")) > 32767:
            # 162: Option 'cdc.read.bytes' must be <= 32767
            raise ValueError(_m(162))
        if (
            int(
                options.get(
                    "max.concurrent.connections",
                    str(_DEFAULT_MAX_CONCURRENT_CONNECTIONS),
                )
            )
            > 9999
        ):
            # 163: Option 'max.concurrent.connections' must be <= 9999
            raise ValueError(_m(163))
        slot_count = int(
            options.get(
                "max.concurrent.connections",
                str(_DEFAULT_MAX_CONCURRENT_CONNECTIONS),
            )
        )
        daemon_reservation = _parsed_option(
            options, _DAEMON_RESERVATION_OPTION, _DEFAULT_DAEMON_CONNECTION_RESERVATION
        )
        # Reject rather than clamp: at slot_count a daemon could never claim a slot,
        # stranding the sharded CDC session and the snapshot drain permanently.
        if not 0 <= daemon_reservation < slot_count:
            # 17: Option '{}' must be >= 0 and < max.concurrent.connections ({})
            raise ValueError(_m(17, _DAEMON_RESERVATION_OPTION, slot_count))
        port = _parsed_option(options, "port", 9088)
        if not 1 <= port <= 65535:
            # 164: Option 'port' must be between 1 and 65535
            raise ValueError(_m(164))
        login_timeout = _parsed_option(options, "authentication.login.timeout", 30.0, float)
        if not math.isfinite(login_timeout) or login_timeout <= 0:
            # 165: Option 'authentication.login.timeout' must be > 0
            raise ValueError(_m(165))
        capacity_delay = _parsed_option(
            options,
            "capacity.retry.max.delay.seconds",
            _DEFAULT_CAPACITY_RETRY_MAX_DELAY_SECONDS,
            float,
        )
        if not math.isfinite(capacity_delay) or capacity_delay < 0:
            # 166: Option 'capacity.retry.max.delay.seconds' must be >= 0
            raise ValueError(_m(166))
        self._bridge_instance: InformixBridge | None = None
        # This flow's name to a shared CDC shard (see _shared_cdc_snapshot).
        self._shard_owner = secrets.token_hex(8)
        self._tables: dict[str, Table] | None = None
        self._tables_complete = False
        # Memoizes the key-index directions adopted for a primary.keys override,
        # keyed by (identity, key columns). The override runs on every _table call
        # (including cache hits), so the catalog lookup must happen at most once
        # per table+key rather than per microbatch.
        self._override_direction_cache: dict[tuple[str, tuple[str, ...]], tuple[bool, ...]] = {}
        self._snapshot_high_water: dict[tuple[str, str], int] = {}
        self._snapshot_schema_ids: dict[tuple[str, str], str] = {}
        self._trigger_available_now = False
        self._trigger_boundaries: dict[str, tuple[int, str, str]] = {}
        # When a delete reader first yielded for a schema transition, per (identity,
        # checkpoint schema id); see _ensure_schema_transition_wait_bounded.
        self._schema_transition_waits: dict[tuple[str, str], float] = {}
        self._activity_touched: dict[tuple[str, str], float] = {}
        self._cleaned_update_scopes: set[tuple[str, str]] = set()
        # The lowest upsert start this reader published under each channel-start and
        # own-start record key, this registration.
        self._noted_upsert_starts: dict[str, tuple] = {}
        self._cleaned_snapshot_stages: set[str] = set()
        # Snapshot-page staging transport, resolved once on first use: None means the
        # FUSE mount; a WorkspaceFilesApi means the Files REST API. See _staging_transport.
        self._staging_transport_resolved = False
        self._staging_files_api: WorkspaceFilesApi | None = None
        self._staging_rest_required = False
        self._staging_workspace_host = ""
        self._registration_scope: str | None = _INFORMIX_REGISTRATION_CONTEXT["scope"]
        # What this update was started for (see _update_request).
        self._update_request: str | None = _INFORMIX_REGISTRATION_CONTEXT.get("request")
        # When the pipeline's targets were last reset (see _last_reset).
        self._update_reset_at: float | None = _INFORMIX_REGISTRATION_CONTEXT.get("reset_at")
        self._metadata_session_lock = threading.RLock()
        # What the call under way tells the bridge -- a metadata lookup's capacity bypass,
        # a read's slot-wait budget and backlog rank -- per thread: set on the options it
        # shares with the bridge, a lookup on one thread reached a read on another.
        self._call_context = threading.local()
        # Set by a snapshot-drain worker to a callback that reports a staged page to the
        # pool, resetting the consumer's liveness watchdog. None on a normal reader.
        self._drain_progress: Callable[[], None] | None = None
        # Set while a first microbatch's staged page 0 may be served from an executor (see
        # _staged_snapshot_result): the serve then returns its rows unread.
        self._defer_staged_rows = False
        # Consumed-page cleanup runs off the serve path on a background pool, and a
        # per-stream high-water mark (keyed by scope/schema/snapshot_lsn) makes it
        # incremental: each microbatch schedules only the pages newly advanced past,
        # not a rescan of every earlier page. The executor is lazily created (and
        # excluded from pickling) because it is a driver-only, non-serializable object.
        self._snapshot_cleanup_executor_instance: concurrent.futures.ThreadPoolExecutor | None = (
            None
        )
        self._snapshot_cleanup_watermark: dict[tuple[str, str, int], int] = {}
        # Per-stream monotonic timestamp of the last live schema refresh, keyed by table
        # identity, so a resumed snapshot re-queries Informix for drift only every
        # _snapshot_schema_refresh_interval seconds rather than every microbatch. The
        # interval is an instance attribute so tests can force per-page (0) refreshes.
        self._snapshot_schema_refreshed_at: dict[str, float] = {}
        self._snapshot_schema_refresh_interval = float(_SNAPSHOT_SCHEMA_REFRESH_INTERVAL_SECONDS)
        # Driver-only bridge between the partitioned reader's latest_offset and
        # get_partitions for an *embedded* (non-page-serve) microbatch: the rows the
        # driver already materialized, keyed by the start offset. Excluded from pickling
        # (driver-only) and cleared on scope change. A cache miss recomputes via read_table,
        # so correctness never depends on it -- it only avoids a second driver read.
        # Values are codec-encoded (JSON-safe) rows -- the exact descriptor payload
        # get_partitions emits and read_partition decodes; see _encode_embedded_rows.
        self._partition_embedded_rows: dict[str, list[Any]] = {}
        # The same bridge for a first microbatch that serves staged page 0 from an
        # executor: its page-serve tuple, so get_partitions needs no manifest read.
        self._partition_page_batches: dict[str, tuple] = {}
        # Driver-only gzip record-window state (see _UNL_GZIP_RECORDS_OPTION): the open
        # record iterator for the gzip file being served, as [key, next_record_index,
        # iterator], and the last window read, so a repeated latest_offset for the same
        # start returns the same rows instead of advancing the iterator again. Never
        # pickled; losing either only costs a reopen-and-skip.
        self._unl_gzip_reader: list[Any] | None = None
        self._unl_gzip_window_memo: tuple[Any, Any] | None = None
        # Extract-file fingerprints already sampled by this process, keyed by
        # (path, size, mtime_ns): any change to a file's size or exact mtime re-samples it.
        self._unl_fingerprint_cache: dict[tuple[str, int, int], str] = {}
        # Tables whose stale-stage retention sweep has already been submitted by this
        # reader instance, so the age-based safety net runs at most once per table per
        # instance (off the serve path) instead of every microbatch.
        self._retention_swept: set[str] = set()
        # Driver-only: the spilled runs each (table, channel) has yet to finish, as run ->
        # (spill scope, BEGIN LSN, commit LSN or None, first change LSN or None), and when each
        # pair last listed its runs from Lakebase (see _settle_spilled_runs).
        self._spill_runs: dict[tuple[str, str], dict[str, tuple]] = {}
        self._spill_runs_listed: dict[tuple[str, str], float] = {}

    def _touch_table_state(self, table: Table) -> None:
        # Docstring: Mark a pipeline/table active and opportunistically run daily state GC and
        # the connection-wide staging sweep.

        scope = self._pipeline_scope()
        if "_@_" not in scope:
            return
        pipeline_id, _, update_id = scope.partition("_@_")
        if not pipeline_id or not update_id:
            return
        retention_days, interval_hours = _state_gc_settings(self.options)
        cache_key = (table.identity, pipeline_id)
        now = time.monotonic()
        # The touch keeps a table's state from the sweep, which deletes what was not
        # touched within the retention: it comes at least four times a retention, whatever
        # the sweep's interval. At the interval alone, a day's interval against a day's
        # retention let a sibling table's sweep take a live table's state between touches.
        touch_seconds = min(interval_hours * 3600, retention_days * 86400 / 4)
        if now - self._activity_touched.get(cache_key, -math.inf) < touch_seconds:
            return
        prefix = self._immutable_namespace(table)
        pipeline_key = hashlib.sha256(pipeline_id.encode()).hexdigest()[:32]
        namespace = self._lakebase_state_namespace()
        self._state(
            touch_table_activity,
            namespace,
            f"{prefix}/activity/{pipeline_key}",
            prefix,
            pipeline_id,
            touch_seconds,
            operation="table-activity touch",
        )
        self._activity_touched[cache_key] = now
        deleted = self._state(
            collect_stale_table_state,
            namespace,
            "state-gc/table-activity",
            retention_days,
            interval_hours,
            operation="stale-state GC",
        )
        if deleted:
            # 322: Collected {} stale Informix Lakebase state records
            logging.getLogger(__name__).info(_m(322, deleted))
        # At the interval, as documented: the touch's quarter-retention cadence is the state
        # sweep's need, not the staging sweep's, which keeps a retention of its own.
        self._maybe_sweep_connection_staging(interval_hours * 3600)

    def _cleanup_previous_update_scopes(
        self, table: Table, checkpoint: dict[str, Any] | None = None
    ) -> None:
        # Docstring: Clean obsolete scopes without deleting state referenced by Spark.

        if checkpoint:
            # A committed offset is what a restart goes on from now: the record of the
            # scan that led to it must not send a later update back to its pages.
            forgotten = self.__dict__.setdefault("_scan_progress_forgotten", set())
            if table.identity not in forgotten:
                with contextlib.suppress(Exception):
                    self._discard_scan_progress(table, self._pipeline_scope())
                    forgotten.add(table.identity)
        if checkpoint and (checkpoint.get("stage_scope") or checkpoint.get("scan_scope")):
            # A handoff relocation is still serving staged pages that live under the
            # capturing flow's scope (carried on stage_scope). That scope's records --
            # the snapshot manifest above all -- must survive until the serve completes,
            # so skip obsolete-scope cleanup while it is present. Not cached, so normal
            # cleanup resumes on the first read after the stream transition drops it.
            return
        current = self._pipeline_scope()
        cache_key = (table.identity, current)
        if cache_key in self._cleaned_update_scopes or "_@_" not in current:
            return
        pipeline_id, _, update_id = current.partition("_@_")
        if not pipeline_id or not update_id:
            return
        retained_scope = None
        if checkpoint:
            candidate = checkpoint.get("pipeline_scope")
            if (
                isinstance(candidate, str)
                and candidate.startswith(f"{pipeline_id}_@_")
                and _PIPELINE_SCOPE.fullmatch(candidate) is not None
            ):
                retained_scope = candidate
        else:
            # No offset yet, but an earlier update's index-ordered scan may be on record
            # for this read to take up (see _adopted_scan_scope): it runs under that
            # update's scope, from the starting position recorded there. Deleting that
            # record sent the scan to a new starting position, past its own snapshot LSN,
            # and so back to the table's first row. The record unreadable (a dropped mount,
            # which _read_progress_file raises), nothing is deleted: a later read cleans up.
            try:
                pointer = self._read_progress_file(self._scan_pointer_path(table, current))
            except Exception:  # pylint: disable=broad-except
                return
            candidate = None if pointer is None else pointer.get("scope")
            if (
                isinstance(candidate, str)
                and candidate != current
                and candidate.startswith(f"{pipeline_id}_@_")
                and _PIPELINE_SCOPE.fullmatch(candidate) is not None
            ):
                retained_scope = candidate
        deleted = self._state(
            delete_obsolete_scoped_state_records,
            self._lakebase_state_namespace(),
            self._immutable_namespace(table),
            f"{pipeline_id}_@_",
            current,
            retained_scope,
            operation="obsolete-scope cleanup",
        )
        self._cleaned_update_scopes.add(cache_key)
        if deleted:
            # 326: Removed {} obsolete Informix update-scoped records for table={}
            logging.getLogger(__name__).info(
                _m(326, deleted, table.exposed_name),
            )

    def _lakebase_state_namespace(self) -> str:
        # Docstring: Namespace state records by the Informix *database*, not the endpoint.
        #
        # Slots are per server process because capacity is a property of that
        # process, but a table's state belongs to one database on it, so two
        # databases on one server never share a record.

        identity = "\0".join(
            (
                "v2",
                self.options.get("hostname", "").strip().rstrip(".").casefold(),
                str(int(self.options.get("port") or 9088)),
                self.options.get("server", "").strip(),
                self.options.get("database", "").strip(),
            )
        )
        return hashlib.sha256(identity.encode()).hexdigest()[:24]

    def _read_immutable_head(self, namespace: str) -> dict[str, object] | None:
        # Docstring: Return the elected record for a key, or None when unwritten.

        return self._state(
            read_state_record,
            self._lakebase_state_namespace(),
            namespace,
            operation="head read",
        )

    def _publish_immutable_head(
        self,
        namespace: str,
        record: dict[str, object],
        *,
        record_type: str = "generic",
    ) -> dict[str, object]:
        # Docstring: Elect exactly one immutable record for a key and return the winner.
        #
        # ``INSERT ... ON CONFLICT DO NOTHING RETURNING`` is the election. The
        # return value is always the *elected* record, which may be another
        # writer's: a record is shared truth for its key, so a loser must adopt the
        # winner rather than assume its own value took effect.

        return self._state(
            publish_state_record,
            self._lakebase_state_namespace(),
            namespace,
            {
                **record,
                "format_version": _IMMUTABLE_STATE_VERSION,
                "record_type": record_type,
            },
            record_type=record_type,
            operation="head publish",
        )

    @staticmethod
    def _validate_immutable_record_header(
        record: dict[str, object], expected_type: str, context: str
    ) -> None:
        if (
            not isinstance(record.get("format_version"), int)
            or isinstance(record.get("format_version"), bool)
            or record.get("format_version") != _IMMUTABLE_STATE_VERSION
            or record.get("record_type") != expected_type
        ):
            # 327: Unsupported or mismatched Informix immutable {} record for {}
            raise InformixError(_m(327, expected_type, context))

    def _validate_schema_node_winner(
        self,
        winner: dict[str, object],
        expected_schema: dict[str, object],
        table: Table,
    ) -> None:
        self._validate_immutable_record_header(winner, "schema-node", table.exposed_name)
        schema = winner.get("schema")
        if (
            # pylint: disable-next=too-many-boolean-expressions
            not isinstance(schema, dict)
            or schema.get("id") != expected_schema.get("id")
            or schema.get("fingerprint") != expected_schema.get("fingerprint")
            or (
                schema.get("predecessor") is not None
                and expected_schema.get("predecessor") is not None
                and schema.get("predecessor") != expected_schema.get("predecessor")
            )
            or _table_from_schema_state(schema, table.database).native_identity
            != _table_from_schema_state(expected_schema, table.database).native_identity
        ):
            # 328: Conflicting immutable schema-node winner for '{}'
            raise InformixError(_m(328, table.exposed_name))
        if (
            _table_from_schema_state(schema, table.database).native_identity
            != table.native_identity
        ):
            # 329: Immutable schema-node table mismatch for '{}'
            raise InformixError(_m(329, table.exposed_name))

    @staticmethod
    def _immutable_lsn(record: dict[str, object], name: str, context: str) -> int:
        try:
            return _strict_lsn(record.get(name), name)
        except (TypeError, ValueError) as error:
            # 330: Invalid {} in Informix immutable record for {}
            raise InformixError(_m(330, name, context)) from error

    def _table_map(self, refresh: bool = False) -> dict[str, Table]:
        if self._tables is None or not self._tables_complete or refresh:
            result: dict[str, Table] = {}
            unreadable: dict[tuple, str] = {}
            database = self.options.get("database", "")
            for raw in self._bridge.list_tables():
                # A table the connector can't read (a column type, or a name, it does not
                # take) failed the whole list, the tables the include and exclude lists
                # leave out too. Asked for by name, it fails with its own error.
                try:
                    if raw.get("unreadable"):
                        raise InformixError(raw["unreadable"])
                    table = Table.parse(raw, database)
                except (InformixError, ValueError) as error:
                    # Selected by the names a Table is (see _selected).
                    owner, name = str(raw.get("owner")), str(raw.get("name"))
                    at = str(raw.get("database") or database)
                    named = _join_identity(owner, name)
                    native = f"{at}:{_sql_identifier(owner)}.{_sql_identifier(name)}"
                    if self._selected_names((_join_identity(at, owner, name), named, native)):
                        unreadable[owner, name] = str(error)
                        # 485: Informix table '{}' is left out of the table list: {}
                        logging.getLogger(__name__).warning(_m(485, named, error))
                    continue
                if self._selected(table):
                    if table.exposed_name in result:
                        # 331: Duplicate exposed table name {}
                        raise InformixError(_m(331, table.exposed_name))
                    result[table.exposed_name] = table
            self._tables = result
            self._unreadable = unreadable
            self._tables_complete = True
        return self._tables

    def _selected(self, table: Table) -> bool:
        return self._selected_names((table.identity, table.exposed_name, table.native_identity))

    def _selected_names(self, names: tuple) -> bool:
        include = _patterns(self.options.get("table.include.list") or self.options.get("tables"))
        exclude = _patterns(self.options.get("table.exclude.list"))
        return (
            not include or any(fnmatch.fnmatchcase(n, p) for n in names for p in include)
        ) and not any(fnmatch.fnmatchcase(n, p) for n in names for p in exclude)

    def _table(self, name: str, options: dict[str, str], refresh: bool = False) -> Table:
        exposed = options.get("qualified_source_table", name)
        if self._tables is not None and exposed in self._tables and not refresh:
            return self._apply_table_options(self._tables[exposed], options)
        parts = _split_identity(exposed)
        if len(parts) == 2 and all(_IDENTIFIER.fullmatch(part) for part in parts):
            database = self.options.get("database", "")
            table = Table.parse(
                self._bridge.get_table(_join_identity(database, parts[0], parts[1])),
                database,
            )
            if not self._selected(table):
                # 169: Unknown or excluded Informix table '{}'
                raise ValueError(_m(169, exposed))
            if self._tables is None:
                self._tables = {}
            self._tables[exposed] = table
            return self._apply_table_options(table, options)
        tables = self._table_map(refresh=refresh)
        if exposed not in tables:
            # One the list left out, by what it can't read (a name too odd to look up).
            unreadable = getattr(self, "_unreadable", {}).get(tuple(parts))
            # 169: Unknown or excluded Informix table '{}'
            raise ValueError(unreadable or _m(169, exposed))
        return self._apply_table_options(tables[exposed], options)


# Conventional alias used by some connector loaders.
InformixConnect = InformixLakeflowConnect

__all__ = [
    "InformixConnect",
    "InformixLakeflowConnect",
    "InformixBridge",
    "InformixError",
    "SharedStateAccessUnavailable",
    "SchemaTransitionUnavailable",
    "PurePythonInformixBridge",
    "LogRetentionError",
    "TransactionBuffer",
    "UnsupportedChangeError",
    "set_bridge_factory",
]


# Last, as when this was one module: in the merged file every definition above is local to
# register_lakeflow_source by now. It keeps a few as module globals (the AvailableNow
# reader base), so both namespaces are visited.
_shorten_merged_definitions({**globals(), **locals()})
