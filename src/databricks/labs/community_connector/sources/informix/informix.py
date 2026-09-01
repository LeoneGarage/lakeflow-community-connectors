"""Pure-Python, serverless-capable Informix snapshot and CDC connector.

The SQLI, CDC framing, codec, transaction, and Lakeflow paths are covered by
source-local regression tests. Informix 15 and serverless Lakeflow pipelines
have validated authentication, queries, discovery, snapshots, and CDC.
"""

from __future__ import annotations

import base64
import contextlib
import errno
import fnmatch
import gzip
import hashlib
import hmac
import importlib
import itertools
import json
import logging
import math
import os
import random
import re
import secrets
import stat
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta
from decimal import ROUND_DOWN, Decimal, InvalidOperation
from typing import Any, Callable, Iterable, Iterator, Protocol, Sequence

from databricks.labs.community_connector.interface import LakeflowConnect
from databricks.labs.community_connector.sources.informix.cdc_protocol import (
    CdcFrameParser,
    CdcProtocolError,
    ColumnDescriptor,
    OpenTransactionRecords,
    cdc_routine,
    decode_frame,
    frame_log_file,
    metadata_column_names,
    validate_snapshot_arity,
)
from databricks.labs.community_connector.sources.informix.lakebase_state import (
    LakebaseState,
    acquire_slot,
    capture_workspace_credentials,
    collect_stale_table_state,
    delete_obsolete_scoped_state_records,
    heartbeat_slot,
    publish_backlog_hint,
    publish_connection_limit,
    publish_minimum_lsn_state_record,
    publish_state_record,
    read_backlog_hints,
    read_state_record,
    release_slot,
    seed_slots,
    touch_table_activity,
)
from databricks.labs.community_connector.sources.informix.sqli import (
    InformixDatetimeLiteral,
    InformixSqliClient,
    PasswordAuthenticationProvider,
    SqliProtocolError,
    add_informix_exception_note,
    datetime_order_preserving_cast,
    informix_locale_encoding,
)
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CURSOR = "_informix_change_lsn"
COMMIT_LSN = "_informix_commit_lsn"
TX_ID = "_informix_tx_id"
OP = "_informix_op"
_INTERNAL_COLUMNS = (CURSOR, COMMIT_LSN, TX_ID, OP)
_LSN_DECIMAL_WIDTH = 20
_OFFSET_VERSION = 10
# Informix undelimited identifiers: the first character is a "letter" or
# underscore and later characters add digits and "$", where the "letter" class
# is defined by the database locale -- under the default en_US.819 (Latin-1) that
# includes accented letters such as e-acute or n-tilde. Match Unicode letters so a
# legitimately accented owner/table/column name is not rejected. ``+`` and ``.``
# are also permitted in later positions -- both occur in real Informix owner names
# (e.g. a "firstname.lastname" login owner) -- while the class still excludes the
# injection-unsafe characters (quotes, spaces, ``;`` ...) so a validated identifier
# cannot break out of its SQL context. Characters beyond the bare-safe set are sent
# as delimited identifiers (see _sql_identifier / _join_identity).
_IDENTIFIER = re.compile(r"^[^\W\d][\w$+.]*$", re.UNICODE)
# Identifiers safe to interpolate *bare* (undelimited): a locale letter or
# underscore followed by letters/digits/underscore/"$". Anything the validator
# admits beyond this (e.g. "+" or ".") is not a valid undelimited identifier and
# must be sent delimited -- wrapped in double quotes, which the server honors
# because the connection sets DELIMIDENT.
_BARE_IDENTIFIER = re.compile(r"^[^\W\d][\w$]*$", re.UNICODE)


def _sql_identifier(identifier: str) -> str:
    """Render a (already validated) identifier for interpolation into SQL.

    Bare when it is a valid undelimited identifier; otherwise delimited with
    double quotes, doubling any embedded quote. The identifier must already have
    passed ``_IDENTIFIER`` validation, so the delimited branch only ever escapes
    admitted characters such as "+" and ".".
    """

    if _BARE_IDENTIFIER.fullmatch(identifier):
        return identifier
    return '"' + identifier.replace('"', '""') + '"'


def _join_identity(*components: str) -> str:
    """Join identity components with ``.``, delimiting any component that is not a
    valid undelimited identifier so the result round-trips through
    ``_split_identity`` even when a component itself contains ``.``.
    """

    return ".".join(_sql_identifier(component) for component in components)


def _split_identity(identity: str) -> list[str]:
    """Split a ``.``-joined identity into its components -- the inverse of
    ``_join_identity``.

    A double-quoted component is taken whole (so it may contain ``.``) with its
    doubled quotes unescaped; bare components split on ``.`` as before.
    """

    parts: list[str] = []
    index, length = 0, len(identity)
    while True:
        if index < length and identity[index] == '"':
            cursor, buffer = index + 1, []
            while cursor < length:
                if identity[cursor] == '"':
                    if cursor + 1 < length and identity[cursor + 1] == '"':
                        buffer.append('"')
                        cursor += 2
                        continue
                    break
                buffer.append(identity[cursor])
                cursor += 1
            else:
                raise InformixError(f"Unterminated quoted identifier in {identity!r}")
            parts.append("".join(buffer))
            index = cursor + 1
        else:
            dot = identity.find(".", index)
            end = length if dot == -1 else dot
            parts.append(identity[index:end])
            index = end
        if index >= length:
            return parts
        if identity[index] != ".":
            raise InformixError(f"Malformed identity {identity!r}")
        index += 1


_DATA_OPS = {"INSERT", "BEFORE_UPDATE", "AFTER_UPDATE", "DELETE", "TRUNCATE"}
_DEFAULT_SNAPSHOT_PAGE_SIZE = 20000
_DEFAULT_SNAPSHOT_READ_TIMEOUT_SECONDS = 300
# CDC session setup/teardown (cdc_opensess/startcapture/activatesess/endcapture/
# closesess) runs on the transport's default socket timeout unless raised here.
# syscdcv1 teardown can stall under many concurrent CDC sessions, so give these
# control-plane calls more room than the 30s default without adopting the much
# larger snapshot read budget.
_DEFAULT_CDC_READ_TIMEOUT_SECONDS = 60
_DEFAULT_MAX_RECORDS_PER_BATCH = 10000
# Informix's own per-session ceiling, passed straight to cdc_opensess. Each poll
# pays a connection slot plus a full CDC session open/activate/close, so a small
# window amortises that fixed cost over very little log progress. Request the
# largest window the source accepts and let the soft-boundary loop finish any
# transaction still open past it.
_DEFAULT_CDC_MAX_RECORDS = 256
_SNAPSHOT_MODES = frozenset(
    {
        "incremental",
        "initial",
        "initial_only",
        "cdc_only",
        "auto_snapshot",
        "recovery",
    }
)
_DEFAULT_SNAPSHOT_MODE = "incremental"
_UNSUPPORTED_SNAPSHOT_MODES = frozenset({"configuration_based", "custom"})
# Opt-in append-only ingestion, for a table Informix can capture but this connector
# cannot merge -- in practice one without a primary key.
#
# Deliberately a connector option rather than something inferred from a missing key.
# Append-only is only *correct* for a genuinely insert-only table: an UPDATE at the
# source appends a second row and a DELETE appends nothing, leaving the deleted row
# present. Nothing in the schema distinguishes an append-only log from a mutable table
# that merely lacks a key, so inferring it would silently corrupt the latter.
#
# It cannot be read from ``scd_type`` either, even though the pipeline layer already
# maps ``scd_type=APPEND_ONLY`` onto an append flow: ``get_table_configuration``
# strips scd_type/primary_keys/sequence_by before the connector sees any options, so
# the value is structurally unavailable here. To append a *keyed* table, set both --
# this option (``true``) so the connector streams incrementally, and
# ``scd_type: APPEND_ONLY`` so the destination appends instead of a keyed merge.
#
# Accepts ``true`` / ``false`` / ``auto``. ``auto`` (also the default when the option
# is unset) appends only keyless tables and leaves keyed tables on their normal
# CDC/snapshot path -- equivalent, for a keyless table, to ``true`` + a keyless
# append destination. ``true`` forces append for every capturable table; ``false``
# forces the normal CDC/snapshot path even for a keyless table.
_APPEND_INGESTION_OPTION = "append.only.ingestion"
# Sharded daemon-reader CDC, on by default. Set ``cdc.shared.session=false`` to opt out,
# in which case every CDC-capable table runs its own Informix CDC session per poll -- one
# connection slot and a full opensess/startcapture/activatesess/endcapture cycle each
# microbatch, per table.
#
# On (the default), the tables are partitioned into K shards
# (``cdc.shared.reader.threads``, default = ``max.concurrent.connections``). A bounded
# pool of K driver-resident daemon threads -- one per shard -- each holds a single
# shared CDC session multiplexing just its shard's subset of tables, reads the log
# once, and fans the assembled transactions out to per-table in-memory buffers. The
# daemon also supplies the schema and log position each streaming poll needs, so in
# steady state a consumer's poll reads entirely from memory and touches Informix for
# nothing. Sharding is by stable hash of the native identity, so a table maps to the
# same shard on every driver and across restarts without coordination.
#
# Concurrent CDC sessions drop from ~N to K (bounding the syscdcv1 teardown that stalls
# under many concurrent sessions), reads are guaranteed parallel across the K daemon
# threads, and the driver thread count is bounded by K. Both the upsert and delete
# channels are sharded independently (the shard is keyed by channel).
_SHARED_CDC_SESSION_OPTION = "cdc.shared.session"
_SHARED_CDC_THREADS_OPTION = "cdc.shared.reader.threads"
_SHARED_CDC_BUFFER_OPTION = "cdc.shared.buffer.max.records"
# Safety ceiling on buffered-but-unconsumed transactions per table in a shard. When a
# shard's slowest buffer reaches it, that shard's daemon defers new reads (throttle to
# the slowest reader) until the backlog drains, bounding driver memory.
_DEFAULT_SHARED_CDC_BUFFER = 200000
# How long a shard retains a subscriber after its last poll before dropping it from the
# capture set and floor computation, so a stopped flow cannot pin the shard forever.
_SHARED_CDC_SUBSCRIBER_TTL_SECONDS = 300.0
# Idle back-off between reads once a shard has drained its log, so a caught-up daemon
# does not busy-spin (and releases its connection slot for consumer bootstrap).
_SHARED_CDC_IDLE_POLL_SECONDS = 1.0
# Snapshot-drain fairness. An ``initial``-mode snapshot drains the whole table through
# one REPEATABLE READ transaction, holding one connection slot for the entire scan; with
# many tables this can consume every slot and starve the streaming/CDC readers waiting
# for one. Two strategies, selected by ``snapshot.shared.session``:
#
#   true (default) -- Model C, a bounded daemon pool. The drain moves off the microbatch
#   thread onto ``snapshot.reader.threads`` (K) driver-resident daemon workers; the
#   consumer waits (holding no slot) for the daemon to stage the table and publish its
#   manifest, then serves the staged pages exactly as a resumed snapshot does. At most K
#   drains run at once, so at most K slots are held by snapshots -- keep
#   K < max.concurrent.connections and a floor always remains for streaming readers.
#
#   false -- Model A, a reservation floor. A snapshot-phase read acquires its slot above
#   ``snapshot.connection.reservation``, so that many low slots are guaranteed reachable
#   by non-snapshot readers and can never all be held by drains at once. Off by default
#   (reservation 0), so this mode bounds the starvation only once the reservation is set;
#   cheap, and it leaves the drain on the microbatch thread.
_SNAPSHOT_SHARED_SESSION_OPTION = "snapshot.shared.session"
_SNAPSHOT_READER_THREADS_OPTION = "snapshot.reader.threads"
_DEFAULT_SNAPSHOT_READER_THREADS = 1
_SNAPSHOT_RESERVATION_OPTION = "snapshot.connection.reservation"
# Isolation level for the monolithic ``snapshot.mode=initial`` full-table snapshot. A
# table option: the isolation trade-off is per table, not per connection. Maps a small
# validated token to a fixed SQL clause (so the value can never be SQL-injected). The
# default is COMMITTED READ LAST COMMITTED -- it reads the last-committed image of a
# locked row instead of taking held shared locks, so the scan does not lock the table
# against writers; the cost is that rows inserted DURING the scan can be captured by both
# the snapshot and the CDC stream, which a key-less table cannot de-duplicate (see the
# README warning). REPEATABLE READ restores the exactly-once, table-locking behaviour.
_SNAPSHOT_ISOLATION_OPTION = "snapshot.isolation"
_DEFAULT_SNAPSHOT_ISOLATION = "committed_read_last_committed"
_SNAPSHOT_ISOLATION_SQL = {
    "committed_read_last_committed": "COMMITTED READ LAST COMMITTED",
    "repeatable_read": "REPEATABLE READ",
}
# Private per-read marker: set on a reader's options while it performs a snapshot-phase
# read, so the lazy slot acquisition can apply the reservation floor. Mirrors how the
# channel and attempt-budget options are threaded to the bridge.
_SNAPSHOT_DRAIN_MARKER_OPTION = "_informix.snapshot.drain"
# Idle back-off for a snapshot-drain worker with no queued jobs, so it does not busy-spin.
_SNAPSHOT_DRAIN_IDLE_SECONDS = 1.0
# Safety cap on how long a consumer waits for the daemon to publish a snapshot manifest
# before failing loudly, so a silently-wedged worker cannot block a flow forever.
_SNAPSHOT_DRAIN_WAIT_SECONDS = 3600.0
# What an append-only table does about rows that predate the flow.
#
# ``stream`` (default): begin at the server's current log position and capture only
# what arrives after. No history, but no size ceiling and no failure mode.
#
# ``snapshot``: read the table once through one forward-only cursor under REPEATABLE
# READ, stage bounded pages, then continue CDC from the snapshot LSN. No ordering or
# positional pagination is required because the transaction and cursor remain open.
_UUID_TEXT = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
_PIPELINE_SCOPE = re.compile(rf"(?:[0-9a-f]{{32}}|{_UUID_TEXT}_@_{_UUID_TEXT})")
_DEFAULT_STATE_GC_RETENTION_DAYS = 30
_DEFAULT_STATE_GC_INTERVAL_HOURS = 24.0
_DEFAULT_STATE_GC_PROJECT_RETENTION_DAYS = 90
_IMMUTABLE_STATE_VERSION = 1
_SNAPSHOT_STAGE_VERSION = 1
_SHARED_STATE_WAIT_SECONDS = 300
_MAX_SNAPSHOT_STAGE_PAGE_BYTES = 256 << 20
_DEFAULT_SNAPSHOT_STAGE_RETENTION_DAYS = 4
# Bootstrap reads a delete reader spends waiting for *this* update's scoped
# initialization record before it will consider the scope-independent schema node.
#
# The scoped record is authoritative: it was published by this update against the
# current logical log. The schema node is keyed by table identity and schema
# fingerprint alone, so it survives every update and its start_lsn may belong to a
# previous log incarnation -- a stale uniqid-22 boundary against a reinitialized log
# that had only reached uniqid 10 is what sent a delete reader to byte 0 of the oldest
# retained log, where it decoded another table's row as its own.
#
# Sized to cover the window in which the owning upsert reader publishes: it enables
# full-row logging, captures one LSN, and publishes before taking its snapshot, so a
# handful of microbatch intervals is ample. Too low reintroduces the race; too high
# delays recovery of the resumed-upsert case the fallback exists for.
_SCHEMA_NODE_FALLBACK_RETRIES = 5
_HEAD_LEASE_SECONDS = 120.0
_DEFAULT_MAX_CONCURRENT_CONNECTIONS = 16
# Slots reserved for the upsert channel, which the delete channel may not claim.
# A floor rather than a partition: upserts may still use every unreserved slot,
# so an idle delete channel strands nothing. Both channels replay the same log
# range, so their throughput needs are symmetric and reserving only chooses which
# channel falls behind. Upsert starvation merely slows an update, but a delete
# reader held back until its checkpoint predates log retention forces a
# re-snapshot, so this defaults to 0 (no reservation) and is opt-in.
_DEFAULT_UPSERT_CONNECTION_RESERVATION = 0
# Slots a snapshot-phase (Model A) read may not claim, guaranteeing them to the
# streaming/CDC readers so a long initial drain cannot starve them. Same floor mechanism
# as the upsert reservation. The configured default is 0, but in Model A a 0 (unset) is
# interpreted as floor(max.concurrent.connections / 3), rounded up to at least 1 whenever
# the pool has 2+ slots (0 only for a single-slot pool, which cannot spare one) -- so a
# long inline drain leaves streaming readers about two-thirds of the pool without any
# tuning; set a positive value to override, and Model C (the default) ignores it entirely.
_DEFAULT_SNAPSHOT_CONNECTION_RESERVATION = 0
# The framework strips ``isDeleteFlow`` before it reaches the connector, so the
# reader publishes the channel to its own bridge through this private option --
# the same mechanism ``_informix.bypass.connection.capacity`` already uses.
_CONNECTION_CHANNEL_OPTION = "_informix.connection.channel"
# Slots the background daemons (sharded CDC readers, the snapshot drain pool) may not
# claim, guaranteeing them to consumer bootstrap/snapshot/incremental reads. Unlike a
# consumer read, a daemon never releases per microbatch (a busy CDC shard re-reads in a
# tight loop, a drain holds its slot for the whole scan), so without this the daemons --
# whose default thread count equals the pool size, across two channels -- can pin every
# slot and starve a fresh bootstrap read. Configured default 0, interpreted as
# floor(max.concurrent.connections / 3) (>= 1 whenever the pool has 2+ slots).
_DAEMON_RESERVATION_OPTION = "daemon.connection.reservation"
_DEFAULT_DAEMON_CONNECTION_RESERVATION = 0
# Private marker set on a daemon reader's options so its lazy slot acquisition applies the
# daemon reservation floor. Set by the CDC reader factory only.
_DAEMON_SLOT_MARKER_OPTION = "_informix.daemon.slot"
# Private marker for the snapshot-drain daemon reader. Unlike the CDC daemon (deferrable,
# floored highest), a drain is bootstrap work that must make progress to unblock its
# append-only consumer, so it is floored *below* the CDC daemon -- into a band the CDC
# daemon cannot claim -- rather than sharing the CDC floor and being starved by it.
_SNAPSHOT_DAEMON_SLOT_MARKER_OPTION = "_informix.snapshot.daemon.slot"
# Private, published per read by a yielding continuous reader: how long this
# attempt may spend acquiring a slot before the reader gives up and returns empty.
_CONNECTION_ATTEMPT_BUDGET_OPTION = "_informix.connection.attempt.budget.seconds"
# Remaining connection-acquisition budget shared by every internal read in one
# readBetweenOffsets() call. Without this, each advisory bootstrap retry starts a
# fresh connection.wait.timeout.seconds window and multiplies the configured cap.
_REPLAY_CONNECTION_WAIT_BUDGET_OPTION = "_informix.replay.connection.wait.budget.seconds"
# Carries the reader's backlog rank to the bridge for the duration of one read, so
# acquisition can prioritise by sweep cadence. Separate from the budget option
# because a *blocking* reader has no budget at all yet still needs a rank.
_CONNECTION_ATTEMPT_RANK_OPTION = "_informix.connection.attempt.backlog.rank"
_DEFAULT_CONNECTION_WAIT_TIMEOUT_SECONDS = 10 * 60
_CONNECTION_SLOT_LEASE_SECONDS = 120
_CONNECTION_SLOT_HEARTBEAT_SECONDS = 30
# How long a lease-loss handler waits for the in-flight operation before closing
# the transport anyway. Bounded because the alternative risks pairing with a
# releaser that is joining the heartbeat thread; the loss is already published, so
# the operation's result is discarded regardless of who wins.
_POISON_OPERATION_LOCK_SECONDS = 30.0
# How long a release waits for the heartbeat thread to exit. Short because it
# normally returns immediately, and a renewal that lands late is rejected by the
# epoch guard rather than doing harm.
_CONNECTION_SLOT_JOIN_SECONDS = 3.0
# Sweep cadence when slots live in Postgres. The Volume intervals (0.2-0.5s
# triggered, 1-5s continuous) were sized for ~2ms directory metadata calls; a
# Postgres round trip is ~11ms measured, and 60 flows sweeping at 0.2s would
# offer ~300 queries/s against an endpoint whose floor is 0.5 CU. These are wide
# enough to keep steady-state load modest while still noticing a freed slot
# inside the 1-5s a production slot is typically held.
_LAKEBASE_SWEEP_MIN_SECONDS = 0.5
_LAKEBASE_SWEEP_MAX_SECONDS = 2.0
# The backlog hint describes the *endpoint's* log position, not one table's, so
# all publishers share a single row. The hints table is keyed per table because
# state records reuse it; this constant is the endpoint-wide key.
_LAKEBASE_ENDPOINT_HINT_KEY = "__endpoint__"
# A waiter holds no slot and has no bridge, so it cannot borrow a bridge's
# Postgres connection. These caches give the waiter path one state handle and one
# connection per endpoint per process, keyed by endpoint identity.
_LAKEBASE_WAITER_STATE: dict[str, Any] = {}
_LAKEBASE_WAITER_CONNECTION: dict[str, Any] = {}
# Created on demand rather than at import, and held in a dict rather than a bare
# module global. Two constraints meet here. Spark cloudpickles the reader class to
# its workers, and a lock alive at serialization time makes that fail outright
# ("cannot pickle '_thread.lock' object"), so the lock cannot exist at import.
# And the merged single-file deployment nests this whole module inside a function,
# which turns module globals into that function's locals -- a rebinding helper
# declaring ``global`` would then address a name nothing ever assigns and raise
# ``NameError``. Mutating a dict needs no rebinding, so it survives both forms.
_LAKEBASE_WAITER_LOCK: dict[str, threading.Lock] = {}


def _lakebase_waiter_lock() -> threading.Lock:
    lock = _LAKEBASE_WAITER_LOCK.get("lock")
    if lock is None:
        # setdefault resolves the race: concurrent callers may each build a lock,
        # but every one of them returns whichever landed first.
        lock = _LAKEBASE_WAITER_LOCK.setdefault("lock", threading.Lock())
    return lock


_CONNECTION_LIMIT_CONFIG_VERSION = 1
_CONNECTION_LIMIT_WIDTH = 5
# The three fence lifecycle stages, as fixed child names under
# ``fences/<slot>/``. They are distinct names because they legitimately
# coexist: a reclaimer holds ``fencing`` while it renames the displaced slot to
# ``expired``, and ``expired`` is later renamed to ``fenced`` for quarantine.
# Collapsing them onto one name would make those renames collide with the live
# reservation and wedge the slot permanently.
_CONNECTION_FENCE_RESERVATION = "fencing"
_CONNECTION_FENCE_TOMBSTONE = "expired"
_CONNECTION_FENCE_QUARANTINE = "fenced"
# How many older generations a waiter will fall back to, so one missed
# publication (the holder crashed mid-write) does not blind every waiter.
_CONNECTION_HINT_STALE_BUCKETS = 2
# Freshness comes from the *bucket* encoded in the generation directory name, never
# from file mtime and no longer from a timestamp stored inside a record. A Volume
# surfaces a transient sub-second mtime for 1-3s after a mutation, so mtime cannot
# distinguish a fresh hint from a stale one -- but the bucket is derived from the
# publisher's clock at publication and is immutable thereafter, which is exactly what
# the stored ``at`` field provided. A waiter walks back a bounded number of buckets
# from its own current one, so the bucket distance *is* the age, in quanta.
_CONNECTION_HINT_MAX_AGE_SECONDS = 45.0
# The hint payload is the log position, and it is carried in a marker *filename*
# rather than in file contents:
#
#     hints/gen-<bucket>/lsn-<20-digit-zero-padded>
#
# Reading it is then a single ``listdir`` of a directory holding one small entry --
# no open(2), no read(2), no gzip, no JSON parse. This matters because UC Volume FUSE
# commits a large read-ahead window per *open file handle* regardless of how few bytes
# are actually read (ES-1604921/ES-1614902), so the cheapest content read is no
# content read. It also removes the partial-record hazard the write-then-rename dance
# existed to avoid: a name either exists in full or does not exist.
#
# Zero-padded to a fixed width so the name is fixed-length and lexically ordered,
# matching _LSN_DECIMAL_WIDTH used elsewhere for the same reason.
_CONNECTION_HINT_LSN_PREFIX = "lsn-"
# Ranks are bounded so the value is cheap to compare and safe to persist in an
# offset. Eight log-scaled levels span the whole plausible backlog range.
_CONNECTION_BACKLOG_RANK_LEVELS = 8
# Consecutive reads that filled their row budget, above which a flow is treated as
# maximally behind. A read that truncates has *proved* work remains, which is the
# one backlog fact a reader establishes without asking the server anything -- the
# published hint is a global log position and so measures staleness rather than
# this table's lag. Four sustains the signal across a brief burst without letting
# one busy read dominate: a genuinely backlogged flow truncates every read, while
# a flow that truncated once and then drained falls straight back to zero.
_CONNECTION_BACKLOG_TRUNCATION_LEVELS = 4
_CONNECTION_FENCE_STAGES = (
    _CONNECTION_FENCE_RESERVATION,
    _CONNECTION_FENCE_TOMBSTONE,
    _CONNECTION_FENCE_QUARANTINE,
)
_SHARED_STATE_ACCESS_MAX_RETRIES = 200
# Consecutive dropped-mount yields a continuous flow tolerates before failing.
#
# Deliberately far smaller than _SHARED_STATE_ACCESS_MAX_RETRIES even though both
# guard the same Volume, because the two retry at completely different cadences.
# An exhausted _open_state_entry_with_retry raises SharedStateAccessUnavailable
# and is retried from _shared_state_retry_offset, which sleeps 0.05-1.5s itself,
# so 200 of those is a couple of minutes. A dropped mount instead yields an empty
# batch and waits for the framework to call the reader again, so its cadence is
# the microbatch interval -- roughly 30s in production -- and reusing 200 there
# means ~100 minutes of a green, RUNNING pipeline replicating nothing.
#
# Sized for the real failure it was observed against: production mount drops
# recovered within seconds, so ten consecutive microbatches (~5 minutes) is well
# clear of a genuine remount while still surfacing an outage inside the window an
# operator would plausibly notice. A mount that is still gone after five minutes
# is an infrastructure fault, and failing the flow reports it where a silent
# retry cannot.
_DROPPED_MOUNT_MAX_RETRIES = 10
# Option carrying the LSN a replayed read must stop at.
#
# readBetweenOffsets(start, end) has to reproduce a range Spark already committed,
# but read_table's signature has no end parameter and thirty connectors implement
# it, so widening the interface to pass one is out of proportion to the fix. The
# framework builds table_options from the reader's own options dict, and the
# connector owns both the reader subclass and this option namespace -- so the
# bound travels the one channel already available. Private by the leading
# underscore convention: set per read by the replay path, never by a user.
_REPLAY_STOP_LSN_OPTION = "_informix.replay.stop.lsn"
# Option carrying the snapshot-chunk cursor a replayed read must stop at.
#
# The LSN bound above covers only half of what a microbatch emits during the
# incremental copy: the same read also returns a snapshot chunk, and a chunk is
# bounded by its keyset cursor, which no LSN constrains. Spark discards the
# offset a replay returns and commits ``end`` regardless, so a replay whose
# chunk covers less of the key range than ``end`` claims leaves the shortfall to
# be skipped by the next read -- silent row loss with the gap on a chunk
# boundary. ``end`` already carries the missing bound as
# incremental.last_pk, so it travels the same options channel as the LSN.
#
# Encoded as JSON because the value is the staged form of a key tuple (a list),
# not a scalar. Two distinguishable states matter: a cursor to stop at, and
# JSON ``null`` meaning "the copy finished inside this range" -- which must
# drain to max_pk rather than stop early. Absent means an ordinary read.
_REPLAY_STOP_PK_OPTION = "_informix.replay.stop.pk"
# Distinguish JSON null (replay the copy-completing range through max_pk) from
# an absent option (ordinary one-page incremental read).
_REPLAY_DRAIN_TO_MAX_PK = object()
# Row budget for a bounded replay: the LSN bound is what limits the read, so the
# budget must not truncate it first. Large rather than None to keep the existing
# integer comparisons intact.
_REPLAY_UNBOUNDED_ROWS = 1 << 62
# Chunk fetches one replayed range may take before the read gives up.
#
# A replay pages until it reaches the committed cursor rather than stopping at
# snapshot.page.size, because rows inserted into the range since the original
# read can push it past one page. That loop runs while holding a connection
# slot, so it cannot be unbounded -- an unbounded wait while holding a slot is
# the deadlock shape avoided elsewhere in this module. In the ordinary case the
# range is exactly the one page the original read produced, so the loop runs
# once; this cap exists for the pathological case, and is a hard error rather
# than a silent truncation because truncation is the bug being fixed.
_REPLAY_MAX_CHUNK_FETCHES = 64
# In-process retries for a transient shared-state failure before the read gives up.
#
# Yielding an empty batch and waiting to be called again is the cheaper response
# for a normal read -- it frees the worker -- but it is unavailable during a
# replay, where the caller has already promised these rows and reads the empty
# batch as proof the range held none. Retrying in place serves both: a remount
# settles in seconds, so most outages clear inside the loop and never reach either
# decision. Mirrors _open_state_entry_with_retry, which already does exactly this
# for ENOTCONN on open(2).
#
# Sized to stay well inside one microbatch: five jittered sleeps of 0.25-4s is
# under 8s worst case. The read holds its connection slot throughout, which is
# acceptable only because the wait is on a filesystem rather than on another
# reader -- waiting on a sibling while holding a slot is the deadlock fixed in
# 65dac41, and this must not become that.
# Offset keys that carry scheduling bookkeeping rather than a stream position.
# Every other key comes from _offset() and answers "where is this reader"; these
# are layered on afterwards by the retry helpers and _with_backlog_streak. Their
# presence is what marks an offset as a yield rather than a completed read.
_OFFSET_ADVISORY_FIELDS = frozenset(
    {
        "shared_state_retry_count",
        "dropped_mount_retry_count",
        "trigger_boundary_retry_count",
        "schema_transition_retry_count",
        "schema_node_fallback_retry_count",
        "capacity_retry_count",
        "capacity_pressure",
        "backlog_rank",
        "backlog_streak",
    }
)
_TRANSIENT_VOLUME_IN_PROCESS_ATTEMPTS = 6
_TRANSIENT_VOLUME_IN_PROCESS_MAX_SECONDS = 8.0
# Continuous-mode capacity backpressure. Every flow in a continuous pipeline runs
# permanently, so N flows are N standing consumers of a fixed slot count and most
# are waiting at any instant. Blocking for the whole connection.wait.timeout.seconds
# pins one worker process per waiter and then fails the streaming query, so a
# continuous reader instead spends a short bounded budget trying to claim a slot
# and returns an empty batch when it cannot.
#
# The budget grows with the reader's consecutive-miss count, so a reader that has
# gone longest without a slot presses hardest. Without that, every reader would
# give up after the same interval and the unlucky ones would keep losing.
#
# The ceiling is bounded because each pass of the acquisition loop tries every
# candidate slot, so one pass costs one mkdir per slot and the budget sets how
# much Volume metadata a single starved read may spend before giving up. Measured
# at 16 slots, a 60s budget is roughly 22 passes and 368 mkdir calls for a read
# that may still return nothing. Endpoint listing was already engineered off this
# hot path because metadata cost dominated here, so the ceiling exists to stop a
# large budget reintroducing that load through a different door -- worst of all
# when the endpoint is most contended, since that is when misses accumulate.
_CAPACITY_RETRY_MAX_RETRIES = 500
# A CDC poll that drops its connection mid-read is retried in place: the dead
# transport is reset and the same read reissued under the slot already held.
# This is transient (network blip / NLB idle reset on a PrivateLink path), so a
# few bounded attempts with short backoff recover it without surfacing a stream
# failure. Applies identically to triggered (AvailableNow) and continuous flows
# because the retry never returns an empty-below-high-water batch.
_CDC_RECONNECT_MAX_RETRIES = 4
_CDC_RECONNECT_BASE_SECONDS = 0.5
_CDC_RECONNECT_MAX_SECONDS = 8.0
# Ceiling on the jittered pause a starved continuous reader takes before yielding,
# and on the budget it spends acquiring a slot.
#
# Held at 5.0 with a 0.1 floor. These were widened to 0.5-8.0 to cut how often
# starved readers touched the shared Volume, because UC Volume FUSE disconnects
# with ENOTCONN under sustained write frequency (Databricks ES-1604921 /
# ES-1614902: the prefetch mechanism is not memory-constraint aware, and
# "increasing the frequency of writes to UC Volume paths triggers the issue
# earlier") -- observed nine times in thirteen hours. Slots are Postgres rows now.
# A measured 247-266 req/s at 120 concurrent connections held latency flat with no
# errors, so the ~11-17 sweeps/s those waiters generate is not a load the endpoint
# notices, and there is no daemon left to protect.
#
# What the wider values did cost was latency: with light contention a freed slot
# could sit idle up to 8s before a waiter re-swept for it. The narrower range
# returns that responsiveness. The floor stays well below the ceiling because the
# jitter's other job is decorrelating equally-aged readers so they do not converge
# on one give-up instant and re-storm together, which needs a wide range.
_DEFAULT_CAPACITY_RETRY_MAX_DELAY_SECONDS = 5.0
# Floor on that same jittered pause, so the sleep cannot draw arbitrarily close to
# zero and let a reader re-sweep almost immediately.
_CAPACITY_RETRY_MIN_DELAY_SECONDS = 0.1
_CAPACITY_ATTEMPT_BASE_SECONDS = 1.0
_CAPACITY_ATTEMPT_PER_MISS_SECONDS = 0.5
# Per backlog rank level, added to the budget so a further-behind flow waits
# longer per attempt. Kept at the per-miss weight so eight rank levels can add at
# most 3.5s -- enough to bias which reader wins a contended slot, far too little
# to let a stale flow hold a worker for the full wait timeout.
_CAPACITY_ATTEMPT_PER_RANK_SECONDS = 0.5
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
_METADATA_SESSION_IDLE_SECONDS = 1.0
# Bound on cached per-table registration lookups shared across reader instances
# in one process. Entries are single resolved tables (keyed by source identity +
# exposed name), so this can be generous -- a pipeline registers one entry per
# configured table.
_MAX_REGISTRATION_TABLE_CACHES = 512
_LAST_CANDIDATE_CLEANUP: dict[str, float] = {}
_LAST_RETIRED_CONNECTION_CLEANUP: dict[str, float] = {}
_RETIRED_CONNECTION_CLEANUP_CURSOR: dict[str, str] = {}
# Newest observed lease renewal per slot path, keyed absolutely so slots of
# different endpoints cannot collide. Process-local and purely an optimisation:
# see _connection_slot_held for why a cached renewal is safe to trust and what
# it deliberately does not cover. Bounded because a long-lived worker may sweep
# several endpoints over its lifetime; slot paths per endpoint are already
# bounded by max.concurrent.connections.
_CONNECTION_SLOT_LEASE_OBSERVED: dict[str, float] = {}
_INFORMIX_REGISTRATION_CONTEXT: dict[str, Any] = {"scope": None}


class _StateValidationCoordinator:
    """Process-local exact leases whose synchronization state is never serialized."""

    def __init__(self) -> None:
        self.condition = threading.Condition()
        self.validated: set[str] = set()
        self.claims: dict[str, tuple[object, float]] = {}
        self.table_caches: dict[tuple[str, ...], "Table"] = {}
        self.connection_limits: set[tuple[str, int]] = set()

    def __getstate__(self) -> dict[str, object]:
        # Spark workers must establish their own process-local validation state.
        return {}

    def __setstate__(self, state: dict[str, object]) -> None:
        del state
        self.__init__()


_STATE_VALIDATION_COORDINATOR = _StateValidationCoordinator()


class _ConnectionMaintenanceCoordinator:
    """Throttle best-effort connection artifact cleanup per worker process."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.last_started: dict[str, float] = {}
        self.running: set[str] = set()

    def __getstate__(self) -> dict[str, object]:
        return {}

    def __setstate__(self, state: dict[str, object]) -> None:
        del state
        self.__init__()


def _sleep_with_backoff(deadline: float, delay: float) -> float:
    """Sleep with jitter without crossing the caller's absolute deadline."""

    remaining = deadline - time.monotonic()
    if remaining > 0:
        time.sleep(min(delay * random.uniform(0.8, 1.2), remaining))
    return min(delay * 1.5, 2.0)


def _informix_available_now_base(base: type, spark_session: Any | None = None) -> type:
    """Wrap the generated reader base without changing the shared adapter source."""

    original_base = getattr(base, "_informix_original_base", base)
    pipeline_id: str | None = None
    update_id: str | None = None
    if spark_session is not None:
        try:
            pipeline_id = spark_session.conf.get("spark.pipelines.pipelineId", None)
            update_id = spark_session.conf.get("spark.pipelines.updateId", None)
        except (AttributeError, TypeError):
            pass
    if not pipeline_id or not update_id:
        registration_scope = secrets.token_hex(16)
    else:
        registration_scope = f"{pipeline_id}_@_{update_id}".lower()
        if _PIPELINE_SCOPE.fullmatch(registration_scope) is None:
            raise ValueError("Spark pipeline and update IDs must be canonical UUID strings")
    # Schema discovery constructs connector instances before stream readers
    # exist. Publish this registration's immutable scope early so those
    # instances can share the registration-scoped catalog snapshot.
    _INFORMIX_REGISTRATION_CONTEXT["scope"] = registration_scope

    class InformixAvailableNowBase(original_base):
        _informix_available_now_wrapper = True
        _informix_original_base = original_base

        def __init_subclass__(cls, **kwargs):
            super().__init_subclass__(**kwargs)
            if cls.__name__ != "LakeflowStreamReader":
                return

            original_init = cls.__init__

            def initialize(reader, *args, **kwargs) -> None:
                original_init(reader, *args, **kwargs)
                setter = getattr(reader.lakeflow_connect, "set_registration_scope", None)
                if setter is not None:
                    setter(registration_scope)
                options = getattr(reader, "options", {})
                table = options.get("tableName", "<unknown>")
                role = "delete" if options.get("isDeleteFlow") == "true" else "upsert"
                logging.getLogger(__name__).info(
                    "Informix CDC reader initialized: scope=%s table=%s role=%s",
                    registration_scope,
                    table,
                    role,
                )

            def prepare_for_trigger(reader) -> None:
                prepare = getattr(
                    reader.lakeflow_connect, "prepare_for_trigger_available_now", None
                )
                if prepare is not None:
                    prepare()

            def read_between_offsets(reader, start, end):
                """Replay ``[start, end)`` by reading to ``end`` and stopping there.

                The shared implementation is ``return self.read(start)[0]``: an
                unbounded read whose returned offset is discarded. Spark commits its
                own ``end`` regardless, so if the fresh read stops earlier than the
                original did, everything in between is dropped silently -- how six
                tables lost 151 rows.

                An unbounded read is not reproducible. It ends at whichever
                transaction boundary the row budget falls on, so the same start LSN
                over the same log can end in different places: measured directly,
                budget 10 ended at LSN 107 and budget 2 at 104 on identical data.
                Comparing the reached offset against ``end`` can only detect that
                after the fact, and failing there converts dropped rows into a retry
                loop, since the next attempt is no more reproducible than the last.

                Passing the bound down is what makes the replay deterministic:
                ``_read_stream`` already stops cleanly at an arbitrary LSN for the
                AvailableNow trigger boundary, and this reuses that path. The bound
                travels through the reader's options, which the framework copies into
                table_options, because read_table has no ``end`` parameter and thirty
                connectors implement it.

                A microbatch taken during the incremental copy emits a snapshot
                chunk alongside the CDC rows, and the LSN bound does not constrain
                it -- a chunk is bounded by its keyset cursor. ``end`` carries that
                second bound as incremental.last_pk, so it is passed down the same
                channel; see _REPLAY_STOP_PK_OPTION.
                """

                positional_keys = {
                    "commit_lsn",
                    "change_lsn",
                    "begin_lsn",
                    "tx_id",
                    "incremental",
                }
                # Spark may persist bootstrap coordination progress as a range of
                # its own. Such a range advances no Informix position and contains
                # no records to replay. Treat it as empty, while continuing to fail
                # closed if either side identifies a positional CDC range.
                advisory_only_end = (
                    isinstance(end, dict) and bool(end) and set(end) <= _OFFSET_ADVISORY_FIELDS
                )
                if advisory_only_end and not positional_keys.intersection(start or {}):
                    return iter(())

                stop = (end or {}).get("commit_lsn")
                if stop is None:
                    raise InformixError(
                        "Informix cannot replay an offset range whose end has no commit_lsn"
                    )
                # The first incremental range starts at {} and still emits a chunk.
                # Bound it whenever either side identifies an in-progress copy; a
                # start-side block with no end-side block is the final range and is
                # represented by JSON null below.
                replaying_chunk = isinstance((start or {}).get("incremental"), dict) or isinstance(
                    (end or {}).get("incremental"), dict
                )
                # ``null`` is meaningful: the copy finished inside this range, so the
                # replay must drain to max_pk instead of stopping at a cursor.
                stop_pk = json.dumps(((end or {}).get("incremental") or {}).get("last_pk"))
                previous = reader.options.get(_REPLAY_STOP_LSN_OPTION)
                previous_pk = reader.options.get(_REPLAY_STOP_PK_OPTION)
                previous_wait_budget = reader.options.get(_REPLAY_CONNECTION_WAIT_BUDGET_OPTION)
                reader.options[_REPLAY_STOP_LSN_OPTION] = str(stop)
                if replaying_chunk:
                    reader.options[_REPLAY_STOP_PK_OPTION] = stop_pk
                try:
                    initial_range = not positional_keys.intersection(start or {})
                    replay_start = start
                    record_parts = []
                    connector_options = getattr(reader.lakeflow_connect, "options", {})
                    configured_wait = float(
                        reader.options.get(
                            "connection.wait.timeout.seconds",
                            connector_options.get(
                                "connection.wait.timeout.seconds",
                                str(_DEFAULT_CONNECTION_WAIT_TIMEOUT_SECONDS),
                            ),
                        )
                    )
                    replay_wait_deadline = time.monotonic() + configured_wait
                    # Spark cannot checkpoint an advisory yield while executing
                    # readBetweenOffsets(): it has already fixed this range's end.
                    # Advance bootstrap coordination counters inside this call
                    # until the reader obtains a real source position. This is
                    # especially important for the delete flow's deferred schema
                    # node fallback after a restart.
                    while True:
                        remaining_wait = replay_wait_deadline - time.monotonic()
                        if remaining_wait <= 0:
                            raise InformixError(
                                "Informix replay did not produce a positional end LSN "
                                "before its connection wait deadline"
                            )
                        reader.options[_REPLAY_CONNECTION_WAIT_BUDGET_OPTION] = repr(remaining_wait)
                        records, reached = reader.read(replay_start)
                        record_parts.append(records)
                        if isinstance(reached, dict) and reached.get("commit_lsn") is not None:
                            break
                        advisory_yield = (
                            initial_range
                            and isinstance(reached, dict)
                            and bool(reached)
                            and set(reached) <= _OFFSET_ADVISORY_FIELDS
                        )
                        if not advisory_yield:
                            raise InformixError(
                                "Informix replay did not produce a positional end LSN"
                            )
                        if reached == replay_start:
                            # At the maximum schema fallback count, an unchanged
                            # advisory offset means the owning upsert flow has not
                            # published its current channel start yet. The read has
                            # released its connection slot, so wait briefly and try
                            # again without extending the shared deadline.
                            remaining_wait = replay_wait_deadline - time.monotonic()
                            if remaining_wait <= 0:
                                raise InformixError(
                                    "Informix replay did not produce a positional end LSN "
                                    "before its connection wait deadline"
                                )
                            time.sleep(min(random.uniform(0.05, 0.2), remaining_wait))
                        replay_start = reached
                    records = itertools.chain.from_iterable(record_parts)
                    try:
                        reached_lsn = int(reached["commit_lsn"])
                        stop_lsn = int(stop)
                    except (KeyError, TypeError, ValueError) as error:
                        raise InformixError(
                            "Informix replay returned an invalid end LSN"
                        ) from error
                    # The first full-refresh range normally starts at {}, but the
                    # framework may add retry bookkeeping before replaying it (for
                    # example ``schema_node_fallback_retry_count``). Such metadata
                    # does not turn the range into a checkpointed CDC range.
                    # Re-executing an initial range
                    # legitimately establishes a newer snapshot/CDC boundary than
                    # the one latestOffset planned before cancellation; its snapshot
                    # rows represent that newer state and replaying them is safe. A
                    # checkpointed range, however, must land on its exact transaction
                    # boundary. In both cases an under-read is always destructive.
                    wrong_lsn = reached_lsn < stop_lsn or (
                        not initial_range and reached_lsn != stop_lsn
                    )
                    if wrong_lsn:
                        raise InformixError(
                            "Informix replay did not reach its committed end LSN "
                            f"{stop_lsn}; reached {reached_lsn}"
                        )
                    expected_incremental = (end or {}).get("incremental")
                    reached_incremental = reached.get("incremental")
                    if isinstance(expected_incremental, dict):
                        expected_pk = expected_incremental.get("last_pk")
                        if (
                            not isinstance(reached_incremental, dict)
                            or reached_incremental.get("last_pk") != expected_pk
                        ):
                            raise InformixError(
                                "Informix replay did not reach its committed incremental "
                                f"snapshot cursor {expected_pk!r}"
                            )
                    elif replaying_chunk and isinstance(reached_incremental, dict):
                        raise InformixError(
                            "Informix replay did not finish the incremental snapshot range "
                            "that Spark has committed"
                        )
                    return records
                finally:
                    # Scoped strictly to this replay: a normal read that inherited
                    # the bound would stop short forever.
                    if previous is None:
                        reader.options.pop(_REPLAY_STOP_LSN_OPTION, None)
                    else:
                        reader.options[_REPLAY_STOP_LSN_OPTION] = previous
                    if previous_pk is None:
                        reader.options.pop(_REPLAY_STOP_PK_OPTION, None)
                    else:
                        reader.options[_REPLAY_STOP_PK_OPTION] = previous_pk
                    if previous_wait_budget is None:
                        reader.options.pop(_REPLAY_CONNECTION_WAIT_BUDGET_OPTION, None)
                    else:
                        reader.options[_REPLAY_CONNECTION_WAIT_BUDGET_OPTION] = previous_wait_budget

            cls.prepareForTriggerAvailableNow = prepare_for_trigger
            cls.readBetweenOffsets = read_between_offsets
            cls.__init__ = initialize

    return InformixAvailableNowBase


# In the deployable merged module the shared trigger base has already been
# defined and the shared reader is defined after this connector. Shadow its base
# with an Informix-owned wrapper that installs the callback at class creation.
# In normal package imports that global is absent, so this block is inert.
try:
    # This name is local to register_lakeflow_source() after merging; globals()
    # cannot see it. In a normal package import it is intentionally undefined.
    _generated_trigger_base = SupportsTriggerAvailableNow  # type: ignore[name-defined]  # noqa: F821
except NameError:
    _generated_trigger_base = None
else:
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


class InformixError(RuntimeError):
    """Base error raised by this connector."""


class ConnectionCapacityUnavailable(InformixError):
    """No connector-managed Informix connection slot is currently available."""


class SharedStateAccessUnavailable(InformixError):
    """A transient permissions failure prevented access to shared state."""


class TriggerBoundaryUnavailable(InformixError):
    """The owning upsert reader has not published this update's boundary yet."""


class SchemaTransitionUnavailable(InformixError):
    """The owning upsert reader has not published this schema transition yet."""


class LogRetentionError(InformixError):
    """The requested restart LSN is no longer retained by Informix."""


def _stale_capture_label_error(frame: bytes, error: Exception, start_lsn: int) -> InformixError:
    """Explain an undecodable CDC frame in terms the operator can act on.

    Informix can emit log records that predate this capture's registration while
    still tagging them with the session's capture label. ``decode_frame`` trusts
    the label to select column descriptors, so such a record is decoded with the
    wrong layout and fails -- observed in production as a bare
    ``UnicodeDecodeError`` on a byte belonging to a foreign row's binary field,
    with nothing to tie it to the log position that caused it.

    Fail closed rather than skipping the frame. A frame that cannot be decoded
    under its declared layout cannot be classified: it may be a foreign record
    (safe to drop) or a real change this reader must not lose, and nothing in the
    frame distinguishes the two. Silently skipping would risk dropping
    replicated changes, and continuing would risk ingesting a foreign row as if
    it were data. Naming the log file lets the operator confirm the checkpoint
    sits in reused log territory, which is the actual remedy.
    """

    log_file = frame_log_file(frame)
    location = f"logical log {log_file}" if log_file is not None else "an unidentified logical log"
    return InformixError(
        f"Unable to decode an Informix CDC record from {location} under the layout "
        f"registered for this capture: {error}. The source replayed a log record that "
        f"does not match this table's captured columns, which happens when the restart "
        f"position ({start_lsn}, logical log {start_lsn >> 32}) lies in a log file whose "
        "space Informix has since reused for other tables. The record cannot be "
        "classified, so it is neither skipped nor ingested. Run a full refresh to "
        "re-establish a restart position in the live log."
    )


class UnsupportedChangeError(InformixError):
    """A source operation cannot be represented by the Lakeflow interface."""


class InformixBridge(Protocol):
    """Injectable boundary around pure-Python SQLI metadata, snapshots, and CDC."""

    def list_tables(self) -> list[dict[str, Any]]: ...

    def get_table(self, identity: str) -> dict[str, Any]: ...

    def current_lsn(self) -> int: ...

    def minimum_lsn(self) -> int: ...

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
    ) -> tuple[int, list[dict[str, Any]]]: ...

    def max_primary_key(
        self,
        identity: str,
        primary_keys: Sequence[str],
        chunk_exprs: dict[str, str] | None = None,
        snapshot_filter: str | None = None,
    ) -> list[Any] | None: ...

    def read_changes(
        self,
        tables: Sequence[dict[str, Any]],
        start_lsn: int,
        timeout_seconds: int,
        max_records: int,
    ) -> list[dict[str, Any]]: ...

    def release_connection(self) -> None:
        """Close a finite poll's source connection and release its capacity lease."""
        ...


def _serialized_sqli_operation(method: Callable[..., Any]) -> Callable[..., Any]:
    """Keep lease-loss shutdown from closing a transport during an operation."""

    def serialized(self: "PurePythonInformixBridge", *args: Any, **kwargs: Any) -> Any:
        lock = getattr(self, "_operation_lock", None)
        if lock is None:  # injected bridges constructed without __init__
            return method(self, *args, **kwargs)
        with lock:
            lease_lost = getattr(self, "_connection_lease_lost", None)
            if lease_lost is not None and lease_lost.is_set():
                raise ConnectionCapacityUnavailable(
                    "Informix connection-capacity lease was lost before the SQLI operation"
                )
            try:
                result = method(self, *args, **kwargs)
            except BaseException as error:
                if lease_lost is not None and lease_lost.is_set():
                    raise ConnectionCapacityUnavailable(
                        "Informix connection-capacity lease was lost during the SQLI operation"
                    ) from error
                raise
            if lease_lost is not None and lease_lost.is_set():
                raise ConnectionCapacityUnavailable(
                    "Informix connection-capacity lease was lost during the SQLI operation"
                )
            return result

    return serialized


class PurePythonInformixBridge:
    """Pure-Python SQLI/CDC bridge validated against disposable Informix 15.

    Tests can inject ``transport.factory=module:callable`` without changing
    Lakeflow or CDC decoding code.
    """

    def __init__(self, options: dict[str, str]) -> None:
        self.options = options
        authentication = options.get("authentication.mode", "password").lower()
        if authentication not in {"password", "pam"}:
            raise InformixError(
                f"Authentication mode {authentication!r} is unsupported; use password or pam."
            )
        self.config = _bridge_config(options)
        factory_path = options.get("transport.factory")
        if factory_path:
            self.transport = _load_factory(factory_path)(options)
        else:
            provider_path = options.get("authentication.provider.factory")
            provider = (
                _load_factory(provider_path)(options)
                if provider_path
                else PasswordAuthenticationProvider(
                    self.config["password"], options.get("authentication.pam.echo.response")
                )
            )
            self.transport = InformixSqliClient(
                self.config["hostname"],
                self.config["port"],
                self.config["database"],
                self.config["user"],
                self.config["password"],
                server_name=self.config["server"],
                db_locale=self.config["db_locale"],
                client_locale=self.config["client_locale"],
                tls=self.config["tls"],
                ca_file=self.config["ca_file"],
                pad_varchar=self.config["pad_varchar"],
                authentication_mode=authentication,
                authentication_provider=provider,
                pam_max_rounds=int(options.get("authentication.pam.max.rounds", "16")),
                login_timeout=float(options.get("authentication.login.timeout", "30")),
                redirect_enabled=_option_bool(options, "redirect.enabled", False),
                redirect_allowlist=_redirect_allowlist(options.get("redirect.allowlist", "")),
                redirect_max=int(options.get("redirect.max", "3")),
            )
        self._connected = False
        self._connect_lock = threading.Lock()
        self._operation_lock = threading.RLock()
        self._connection_lease_lost = threading.Event()
        self._connection_slot: str | None = None
        self._connection_slot_token: str | None = None
        self._connection_slot_heartbeat_stop: threading.Event | None = None
        self._connection_slot_heartbeat: threading.Thread | None = None

    def _ensure_connected(self) -> None:
        # Tests and injected bridges built with ``object.__new__`` already
        # supply a ready transport and intentionally bypass ``__init__``.
        if getattr(self, "_connected", True):
            return
        with self._connect_lock:
            if self._connected:
                return
            bypass_capacity = self.options.get("_informix.bypass.connection.capacity") == "true"
            if not bypass_capacity:
                budget = self.options.get(_CONNECTION_ATTEMPT_BUDGET_OPTION)
                self._acquire_connection_slot(float(budget) if budget else None)
            try:
                self._connect_transport()
                self._connected = True
            except Exception:
                self._release_connection_slot()
                raise

    def _connect_transport(self) -> None:
        connect = getattr(self.transport, "connect", None)
        if connect:
            connect()

    @staticmethod
    def _connection_identity_from(source: dict[str, Any]) -> str:
        """Identify the Informix endpoint whose capacity is being shared.

        Deliberately excludes the database name: capacity is a property of the
        server process, so every database on one server draws from one pool.

        Takes the mapping so a holder (which has a parsed ``config``) and a waiter
        (which has only ``options``) derive the *same* identity. They must: the
        holder publishes the backlog hint and the waiter reads it, so a divergence
        here would leave every hint silently unreadable rather than failing
        loudly.
        """

        return "\0".join(
            (
                str(source.get("hostname", "")).strip().rstrip(".").casefold(),
                str(int(source.get("port", 9088) or 9088)),
                str(source.get("server", "")).strip().casefold(),
            )
        )

    def _connection_identity(self) -> str:
        return self._connection_identity_from(getattr(self, "config", None) or self.options)

    def _lakebase(self) -> Any:
        """Return this process's Lakebase state handle, provisioning on first use."""

        existing = getattr(self, "_lakebase_state", None)
        if existing is not None:
            return existing
        state = LakebaseState(self.options, self._connection_identity())
        state.provision()
        self._lakebase_state = state
        return state

    def _lakebase_connection(self) -> Any:
        """Return a per-bridge Postgres connection, reconnecting if it has died.

        One connection per bridge rather than a pool: a bridge serves a single
        table's reads sequentially, and the endpoint permits 450 connections
        (measured), which is ample for a pipeline's flows.
        """

        connection = getattr(self, "_lakebase_conn", None)
        if connection is not None:
            try:
                if not connection.closed:
                    return connection
            except Exception:
                pass
        connection = self._lakebase().connect()
        self._lakebase_conn = connection
        return connection

    def _lakebase_namespace(self) -> str:
        """Namespace every row by the Informix endpoint sharing the capacity."""

        return hashlib.sha256(self._connection_identity().encode()).hexdigest()[:24]

    @staticmethod
    def _revocation_name(path: str) -> str:
        """Return the revocation marker name for one artifact.

        Nested fence stages share basenames across slots (every slot has a
        ``fencing`` child), so the marker must include the slot to stay unique;
        deriving it from the basename alone would let one slot's revocation
        appear to revoke every other slot's same-stage fence.
        """

        base = os.path.basename(path)
        if base in _CONNECTION_FENCE_STAGES:
            slot = os.path.basename(os.path.dirname(path))
            return f"revoked-{slot}-{base}"
        return f"revoked-{base}"

    def _connection_hints_enabled(self) -> bool:
        return self.options.get("connection.backlog.hint.enabled", "true") != "false"

    def _publish_connection_backlog_hint(self) -> None:
        """Record the endpoint's current log position for waiters to read.

        Called by a reader that still holds a slot, which is the only participant
        able to ask the server anything. One monotonic upsert: ``GREATEST`` makes
        concurrent publications converge, so every holder may publish and the
        value can never regress -- no per-quantum election is needed.

        Advisory. Every failure is swallowed, because a missing hint only degrades
        waiters to the unprioritised behaviour they had before hints existed and
        must never fail a read that otherwise succeeded.
        """

        if not self._connection_hints_enabled():
            return
        try:
            publish_backlog_hint(
                self._lakebase_connection(),
                self._lakebase_namespace(),
                _LAKEBASE_ENDPOINT_HINT_KEY,
                self.current_lsn(),
            )
        except Exception:
            logging.getLogger(__name__).debug(
                "Informix backlog hint publication failed", exc_info=True
            )

    @classmethod
    def _read_connection_backlog_hint_at(cls, options: dict[str, str]) -> int | None:
        """Return the published endpoint log position from Postgres, or None.

        Called by a *waiter*, which holds no slot and has no bridge, so it cannot
        reuse a bridge's connection. The handle is cached per process and keyed by
        endpoint identity; a waiter reads once per sweep, so one shared connection
        per process is enough.

        Advisory, so every failure yields None and the caller falls back to
        unprioritised waiting rather than failing.
        """

        identity = cls._connection_identity_from(options)
        namespace = hashlib.sha256(identity.encode()).hexdigest()[:24]
        try:
            with _lakebase_waiter_lock():
                state = _LAKEBASE_WAITER_STATE.get(namespace)
                if state is None:
                    state = LakebaseState(options, identity)
                    state.provision()
                    _LAKEBASE_WAITER_STATE[namespace] = state
                connection = _LAKEBASE_WAITER_CONNECTION.get(namespace)
                if connection is None or getattr(connection, "closed", False):
                    connection = state.connect()
                    _LAKEBASE_WAITER_CONNECTION[namespace] = connection
                hints = read_backlog_hints(
                    connection, namespace, max_age_seconds=_CONNECTION_HINT_MAX_AGE_SECONDS
                )
            return hints.get(_LAKEBASE_ENDPOINT_HINT_KEY)
        except Exception:
            with _lakebase_waiter_lock():
                # Drop a connection that may be broken so the next read reopens.
                _LAKEBASE_WAITER_CONNECTION.pop(namespace, None)
            logging.getLogger(__name__).debug(
                "Informix Lakebase backlog hint read failed", exc_info=True
            )
            return None

    def _connection_channel_is_delete(self) -> bool:
        return self.options.get(_CONNECTION_CHANNEL_OPTION) == "delete"

    def _upsert_connection_reservation(self, slot_count: int) -> int:
        reservation = int(
            self.options.get(
                "upsert.connection.reservation",
                str(_DEFAULT_UPSERT_CONNECTION_RESERVATION),
            )
        )
        if not 0 <= reservation < slot_count:
            # At slot_count the delete channel could never claim a slot at all,
            # which starves it permanently rather than merely throttling it.
            raise ValueError(
                "Option 'upsert.connection.reservation' must be >= 0 and < "
                f"max.concurrent.connections ({slot_count})"
            )
        return reservation

    def _snapshot_connection_reservation(self, slot_count: int) -> int:
        reservation = int(
            self.options.get(
                _SNAPSHOT_RESERVATION_OPTION,
                str(_DEFAULT_SNAPSHOT_CONNECTION_RESERVATION),
            )
        )
        if not 0 <= reservation < slot_count:
            # At slot_count a snapshot drain could never claim a slot at all, which
            # deadlocks the drain rather than merely deferring it behind streams.
            raise ValueError(
                f"Option '{_SNAPSHOT_RESERVATION_OPTION}' must be >= 0 and < "
                f"max.concurrent.connections ({slot_count})"
            )
        # Model A with an unset/zero reservation reserves a third of the pool by default,
        # so a long inline drain always leaves the streaming/CDC readers roughly
        # two-thirds of capacity rather than potentially every slot. Reserve at least one
        # slot whenever the pool can spare one (>= 2 slots), rounding floor(slot_count/3)
        # up to 1; clamp to slot_count - 1 so a single-slot pool reserves 0 -- reserving
        # its only slot would floor the drain out of every slot and deadlock it.
        if reservation == 0 and not _option_bool(
            self.options, _SNAPSHOT_SHARED_SESSION_OPTION, True
        ):
            return min(max(1, slot_count // 3), slot_count - 1)
        return reservation

    def _daemon_connection_reservation(self, slot_count: int) -> int:
        reservation = int(
            self.options.get(
                _DAEMON_RESERVATION_OPTION,
                str(_DEFAULT_DAEMON_CONNECTION_RESERVATION),
            )
        )
        if not 0 <= reservation < slot_count:
            # At slot_count a daemon could never claim a slot at all, stranding the
            # sharded CDC session and the snapshot drain permanently.
            raise ValueError(
                f"Option '{_DAEMON_RESERVATION_OPTION}' must be >= 0 and < "
                f"max.concurrent.connections ({slot_count})"
            )
        # A 0 (unset) reservation reserves a third of the pool by default (rounded up to
        # at least 1 whenever the pool has 2+ slots, 0 only for a single-slot pool), so a
        # fresh consumer bootstrap read always has slots the daemons cannot pin. Set a
        # positive value to give the daemons more of the pool (down to reserving a single
        # slot); a single-slot pool reserves nothing because it cannot spare one.
        if reservation == 0:
            return min(max(1, slot_count // 3), slot_count - 1)
        return reservation

    def _snapshot_daemon_connection_reservation(self, slot_count: int) -> int:
        """Floor for the snapshot-drain daemon: one band below the CDC daemon.

        The CDC daemon is deferrable and floored at ``daemon.connection.reservation``;
        a drain is bootstrap work that must make progress, so it sits ``snapshot.reader
        .threads`` slots lower, giving it that many slots the CDC daemon cannot claim.
        The band below it (``daemon.connection.reservation - snapshot.reader.threads``
        slots) stays reachable only by consumer bootstrap reads at floor 0. When the
        pool cannot spare a private drain band the floor collapses to 0, where the drain
        still reaches the CDC-free low slots (consumer reads release per microbatch, so
        those slots churn) -- it simply loses its dedicated headroom.
        """

        daemon_reservation = self._daemon_connection_reservation(slot_count)
        threads = max(
            1,
            int(
                self.options.get(
                    _SNAPSHOT_READER_THREADS_OPTION, str(_DEFAULT_SNAPSHOT_READER_THREADS)
                )
            ),
        )
        return max(0, daemon_reservation - threads)

    def _connection_sweep_rank_scale(self) -> float:
        """Shrink the gap between acquisition sweeps for a further-behind reader.

        The connector publishes the rank alongside the attempt budget, so this
        needs no offset access of its own. Returns 1.0 when no rank is present,
        which leaves the cadence byte-identical to the pre-rank behaviour.

        Linear between 1.0 (unranked) and ``_SLOT_SWEEP_RANK_FLOOR`` (top rank).
        Bounded deliberately: a freed slot goes to whoever sweeps next, so halving
        the gap is enough to bias that race, while an unbounded shrink would
        reintroduce exactly the metadata storm the wide interval exists to prevent.
        """

        raw = self.options.get(_CONNECTION_ATTEMPT_RANK_OPTION)
        if not raw:
            return 1.0
        try:
            rank = int(raw)
        except (TypeError, ValueError):
            return 1.0
        top = _CONNECTION_BACKLOG_RANK_LEVELS - 1
        if rank <= 0 or top <= 0:
            return 1.0
        fraction = min(1.0, rank / top)
        return 1.0 - (1.0 - _SLOT_SWEEP_RANK_FLOOR) * fraction

    def _acquire_connection_slot(self, budget_seconds: float | None = None) -> None:
        """Claim a capacity slot from Postgres.

        Structurally the same wait loop as the Volume path -- the same timeout,
        the same rank-scaled cadence, the same reservation floor -- but each pass
        is a single atomic statement instead of a per-slot ``mkdir`` sweep, so
        there is no fence to check afterwards and no partial claim to unwind.
        """

        namespace = self._lakebase_namespace()
        slot_count = int(
            self.options.get(
                "max.concurrent.connections",
                str(_DEFAULT_MAX_CONCURRENT_CONNECTIONS),
            )
        )
        reservation = self._upsert_connection_reservation(slot_count)
        floor = reservation if self._connection_channel_is_delete() else 0
        # Model A: a snapshot-phase read holds its slot for the whole drain, so push it
        # above the snapshot reservation, leaving those low slots reachable by the
        # streaming/CDC readers. Only when the daemon pool (Model C) is off -- in shared
        # mode the daemon bounds concurrency instead and this marker is never set.
        if self.options.get(_SNAPSHOT_DRAIN_MARKER_OPTION) == "true":
            floor = max(floor, self._snapshot_connection_reservation(slot_count))
        # The sharded CDC daemon draws from the same pool but, unlike a consumer read,
        # never releases per microbatch: a busy CDC shard re-reads in a tight loop. With
        # its default thread count equal to the pool size and two channels, it can pin
        # every slot and starve a fresh consumer bootstrap read (which then times out and
        # fails the query). Floor it above the daemon reservation so those low slots stay
        # reachable by consumer reads.
        if self.options.get(_DAEMON_SLOT_MARKER_OPTION) == "true":
            floor = max(floor, self._daemon_connection_reservation(slot_count))
        # The snapshot-drain daemon also holds its slot for the whole scan, but it is
        # bootstrap work that must finish to unblock its consumer -- so it floors *below*
        # the CDC daemon into a band the CDC daemon cannot claim, rather than sharing the
        # CDC floor and being starved by a saturated CDC daemon.
        if self.options.get(_SNAPSHOT_DAEMON_SLOT_MARKER_OPTION) == "true":
            floor = max(floor, self._snapshot_daemon_connection_reservation(slot_count))
        connection_wait_timeout = float(
            self.options.get(
                "connection.wait.timeout.seconds",
                str(_DEFAULT_CONNECTION_WAIT_TIMEOUT_SECONDS),
            )
        )
        if budget_seconds is not None:
            connection_wait_timeout = min(connection_wait_timeout, budget_seconds)
        sweep_scale = self._connection_sweep_rank_scale()
        sweep_min = _LAKEBASE_SWEEP_MIN_SECONDS * sweep_scale
        sweep_max = _LAKEBASE_SWEEP_MAX_SECONDS * sweep_scale
        connection = self._lakebase_connection()
        # Capacity is the row count, so the rows must exist before the first
        # claim. Seeding is idempotent and only ever adds rows.
        seed_slots(connection, namespace, slot_count)
        publish_connection_limit(
            connection, namespace, slot_count, reservation, _CONNECTION_LIMIT_CONFIG_VERSION
        )
        owner = f"{secrets.token_hex(16)}"
        scope = self.options.get("_informix.pipeline.scope") or None
        deadline = time.monotonic() + connection_wait_timeout
        while True:
            slot = acquire_slot(
                connection,
                namespace,
                owner,
                slot_count=slot_count,
                floor=floor,
                scope=scope,
                lease_seconds=_CONNECTION_SLOT_LEASE_SECONDS,
            )
            if slot is not None:
                self._connection_slot = f"slot-{slot.slot_id:04d}"
                self._connection_slot_token = owner
                self._lakebase_slot = slot
                stop = threading.Event()
                heartbeat = threading.Thread(
                    target=self._heartbeat_connection_slot,
                    args=(slot, stop, self._poison_connection_slot_lease),
                    daemon=True,
                    name="informix-lakebase-slot-heartbeat",
                )
                self._connection_slot_heartbeat_stop = stop
                self._connection_slot_heartbeat = heartbeat
                heartbeat.start()
                return
            if time.monotonic() >= deadline:
                raise ConnectionCapacityUnavailable(
                    "Timed out waiting for an Informix connection-capacity slot after "
                    f"{connection_wait_timeout:g} seconds"
                )
            time.sleep(random.uniform(sweep_min, sweep_max))

    def _heartbeat_connection_slot(
        self,
        slot: Any,
        stop: threading.Event,
        on_lease_lost: Callable[[], None] | None = None,
    ) -> None:
        """Renew a Postgres lease until released, poisoning it if renewal fails.

        Uses its own connection: the bridge's connection is busy serving reads,
        and psycopg connections are not safe to share across threads.
        """

        def report_lease_lost() -> None:
            # Mirrors the Volume heartbeat: a lease whose owner is already
            # releasing must not be poisoned, or the releaser (which sets ``stop``
            # then joins this thread) and the poisoner (which wants the owner's
            # operation lock) would wait on each other.
            if stop.is_set() or on_lease_lost is None:
                return
            on_lease_lost()

        connection: Any = None
        first_failure: float | None = None
        try:
            while not stop.wait(_CONNECTION_SLOT_HEARTBEAT_SECONDS):
                try:
                    if connection is None or connection.closed:
                        connection = self._lakebase().connect()
                    if not heartbeat_slot(connection, slot):
                        # The guard rejected us: the lease was reclaimed and now
                        # belongs to someone else. Nothing to retry.
                        report_lease_lost()
                        return
                    first_failure = None
                except Exception:
                    now = time.monotonic()
                    if first_failure is None:
                        first_failure = now
                    try:
                        if connection is not None:
                            connection.close()
                    except Exception:
                        pass
                    connection = None
                    # A transient error must not drop a healthy lease, but one
                    # that outlasts the lease means another reader may already
                    # have reclaimed the slot, so the transport has to go.
                    if now - first_failure >= _CONNECTION_SLOT_LEASE_SECONDS:
                        logging.getLogger(__name__).warning(
                            "Informix Lakebase slot renewal failed for %.0fs; "
                            "treating the lease as lost: %s",
                            now - first_failure,
                            slot,
                            exc_info=True,
                        )
                        report_lease_lost()
                        return
                    logging.getLogger(__name__).debug(
                        "Informix Lakebase slot renewal failed; will retry", exc_info=True
                    )
        finally:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass

    def _release_connection_slot(self) -> None:
        """Free a Postgres lease, tolerating a lease already reclaimed."""

        slot = getattr(self, "_lakebase_slot", None)
        # Stop the heartbeat before releasing, and clear the handles first so a
        # re-entrant release cannot signal the same thread twice. Without this the
        # thread outlives the lease it was renewing: the slot is freed, another
        # reader claims it and bumps the epoch, and the stale heartbeat's next
        # renewal is rejected -- which it correctly reports as a lost lease,
        # poisoning a transport whose owner had already finished with it.
        stop = getattr(self, "_connection_slot_heartbeat_stop", None)
        heartbeat = getattr(self, "_connection_slot_heartbeat", None)
        self._connection_slot_heartbeat_stop = None
        self._connection_slot_heartbeat = None
        if stop is not None:
            stop.set()
        if heartbeat is not None and heartbeat is not threading.current_thread():
            # Returns as soon as the thread exits, so an ordinary release pays
            # nothing. The bound matters only if a renewal is mid-flight, and the
            # epoch guard makes a late renewal harmless anyway.
            heartbeat.join(timeout=_CONNECTION_SLOT_JOIN_SECONDS)
        if slot is None:
            return
        self._lakebase_slot = None
        try:
            release_slot(self._lakebase_connection(), slot)
        except Exception:
            # Releasing is best effort: the lease expires on its own, so a failed
            # release costs one slot for one lease period rather than a stranded
            # slot as it did on the Volume.
            logging.getLogger(__name__).warning(
                "Informix could not release Lakebase connection slot %s; "
                "its lease will expire instead",
                slot,
                exc_info=True,
            )

    def _poison_connection_slot_lease(self) -> None:
        """Abandon a lease that can no longer be renewed, closing the transport.

        Called from the heartbeat when renewal is rejected or fails persistently.
        The reader has provably lost its capacity slot, so continuing to use the
        SQLI connection would exceed the configured concurrency: another reader
        already owns the slot. Nothing here is Volume-specific -- it is about the
        Informix transport and the operation in flight on it.
        """

        lease_lost = getattr(self, "_connection_lease_lost", None)
        if lease_lost is not None:
            # Publish loss before waiting. The active operation can finish its
            # bounded socket work, but its wrapper must discard the result.
            lease_lost.set()
        operation_lock = getattr(self, "_operation_lock", None)
        context: Any = contextlib.nullcontext()
        if operation_lock is not None:
            # Bound the wait for the active operation. The lease is already
            # published as lost, so the operation's wrapper will discard its result
            # regardless; what remains is closing the transport and freeing the
            # slot. Waiting without a bound risks pairing with a releaser that is
            # joining this thread. The guard in the heartbeat covers the ordinary
            # release, and this covers the residue: a reader whose operation
            # outlives the lease still gets its transport closed and slot freed
            # here rather than leaving the lease poisoned indefinitely.
            if operation_lock.acquire(timeout=_POISON_OPERATION_LOCK_SECONDS):
                context = contextlib.ExitStack()
                context.callback(operation_lock.release)
            else:
                logging.getLogger(__name__).warning(
                    "Informix connection-slot lease loss could not take the operation "
                    "lock within %.0fs; closing the transport while an operation is "
                    "still in flight: %s",
                    _POISON_OPERATION_LOCK_SECONDS,
                    getattr(self, "_connection_slot", None),
                )
        with context:
            with self._connect_lock:
                logging.getLogger(__name__).error(
                    "Informix connection-slot lease renewal failed persistently; "
                    "closing SQLI transport after the active operation exits"
                )
                self._connected = False
                try:
                    close = getattr(self.transport, "close", None)
                    if close is not None:
                        close()
                except Exception:
                    logging.getLogger(__name__).warning(
                        "Failed to close SQLI transport after connection-slot lease loss",
                        exc_info=True,
                    )
                finally:
                    self._release_connection_slot()
                    if lease_lost is not None:
                        lease_lost.clear()

    @classmethod
    def _remove_connection_slot_tree(cls, path: str) -> None:
        parent, name = os.path.split(path)
        parts = os.path.normpath(path).split(os.sep)
        if len(parts) >= 5 and parts[1] == "Volumes":
            root = os.path.join(os.sep, *parts[1:5])
            descriptor = _open_state_directory(root, parent)
        else:
            flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(parent or ".", flags)
        try:
            info = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
            if not stat.S_ISDIR(info.st_mode):
                os.unlink(name, dir_fd=descriptor)
                return
            _remove_candidate_tree_at(descriptor, name)
            os.rmdir(name, dir_fd=descriptor)
        finally:
            os.close(descriptor)

    def release_connection(self) -> None:
        """Release a finite worker poll's SQLI session without discarding metadata."""

        operation_lock = getattr(self, "_operation_lock", None)
        context = operation_lock if operation_lock is not None else contextlib.nullcontext()
        with context:
            with self._connect_lock:
                if not self._connected:
                    self._release_connection_slot()
                    return
                # Publish the backlog hint here: this is the last point at which
                # the connection is still live *and* the slot is still held, which
                # is exactly the position a waiter cannot reach. Advisory, so it
                # must never turn a completed read into a failed one.
                slot = getattr(self, "_connection_slot", None)
                if slot is not None:
                    self._publish_connection_backlog_hint()
                try:
                    close = getattr(self.transport, "close", None)
                    if close:
                        close()
                finally:
                    self._connected = False
                    self._release_connection_slot()

    def reset_transport(self) -> None:
        """Drop a dead SQLI transport but keep the connection slot held.

        Used to recover from a mid-read connection drop: close the (already
        broken) transport and mark the bridge disconnected so the next
        ``_ensure_connected`` reconnects, *without* releasing and re-acquiring
        the connection slot. Releasing the slot here would reintroduce capacity
        contention on what is a transient network blip, and the caller is
        retrying the same read in place under the slot it already owns.
        """

        operation_lock = getattr(self, "_operation_lock", None)
        context = operation_lock if operation_lock is not None else contextlib.nullcontext()
        with context:
            with self._connect_lock:
                if not self._connected:
                    return
                try:
                    close = getattr(self.transport, "close", None)
                    if close:
                        close()
                except Exception:
                    logging.getLogger(__name__).warning(
                        "Failed to close SQLI transport during reset; reconnecting anyway",
                        exc_info=True,
                    )
                finally:
                    self._connected = False

    @_serialized_sqli_operation
    def list_tables(self) -> list[dict[str, Any]]:
        self._ensure_connected()
        maximum = int(self.options.get("metadata.max.bytes", str(64 << 20)))
        columns = self.transport.execute(
            "SELECT c.colname, c.coltype, c.collength, c.colno, t.tabid, "
            "c.extended_id, x.name AS extended_name, x.owner AS extended_owner, "
            "t.owner, t.tabname "
            "FROM systables t JOIN syscolumns c ON t.tabid = c.tabid "
            "LEFT JOIN sysxtdtypes x ON x.extended_id = c.extended_id "
            "WHERE t.tabtype = 'T' AND t.owner NOT MATCHES 'sys*' "
            "AND t.tabname NOT MATCHES 'sys*' "
            "ORDER BY t.owner, t.tabname, c.colno",
            max_result_bytes=maximum or None,
        )
        keys = self.transport.execute(
            "SELECT i.part1,i.part2,i.part3,i.part4,i.part5,i.part6,i.part7,i.part8,"
            "i.part9,i.part10,i.part11,i.part12,i.part13,i.part14,i.part15,i.part16,"
            "t.owner,t.tabname "
            "FROM systables t JOIN sysconstraints x ON t.tabid=x.tabid "
            "JOIN sysindexes i ON x.idxname=i.idxname AND x.tabid=i.tabid "
            "WHERE t.tabtype='T' AND x.constrtype='P' "
            "AND t.owner NOT MATCHES 'sys*' AND t.tabname NOT MATCHES 'sys*' "
            "ORDER BY t.owner,t.tabname",
            max_result_bytes=maximum or None,
        )
        columns_by_table: dict[tuple[str, str], list[Any]] = {}
        for row in columns:
            identity = (
                str(_field(row, "owner", 8)),
                str(_field(row, "tabname", 9)),
            )
            columns_by_table.setdefault(identity, []).append(row)
        keys_by_table = {
            (str(_field(row, "owner", 16)), str(_field(row, "tabname", 17))): [row] for row in keys
        }
        result = []
        retained_bytes = (
            _deep_size(result) + _deep_size(columns) + _deep_size(keys) if maximum else 0
        )
        for (owner, name), table_columns in columns_by_table.items():
            table = self._table_from_catalog_rows(
                owner, name, table_columns, keys_by_table.get((owner, name), [])
            )
            if maximum:
                retained_bytes += _deep_size(table)
            if maximum and retained_bytes > maximum:
                raise InformixError(
                    f"Informix metadata discovery exceeded metadata.max.bytes={maximum}"
                )
            result.append(table)
        return result

    @_serialized_sqli_operation
    def get_table(self, identity: str) -> dict[str, Any]:
        self._ensure_connected()
        parts = _split_identity(identity)
        if len(parts) != 3:
            raise InformixError(f"Invalid logical table identity {identity!r}")
        return self._describe_table(parts[1], parts[2])

    def _assert_capture_layout(self, capture: dict[str, Any], encoding: str) -> None:
        """Fail before decoding rows when catalog metadata changed mid-session."""

        native = str(capture["identity"])
        try:
            database, qualified = native.split(":", 1)
            owner, name = _split_identity(qualified)
        except ValueError as error:
            raise InformixError(f"Invalid native table identity {native!r}") from error
        refreshed = _capture_descriptor(
            Table.parse(self._describe_table(owner, name), database), encoding
        )

        def layout(value: dict[str, Any]) -> dict[str, tuple[Any, ...]]:
            return {
                str(column["name"]): (
                    str(column["type_name"]),
                    int(column.get("length") or 0),
                    column.get("precision"),
                    column.get("scale"),
                    str(column.get("encoding") or "utf-8"),
                )
                for column in value["descriptors"]
            }

        capture_columns = list(capture["columns"])
        refreshed_columns = list(refreshed["columns"])
        refreshed_layout = layout(refreshed)
        capture_layout = layout(capture)
        prefix_is_unchanged = refreshed_columns[: len(capture_columns)] == capture_columns and all(
            refreshed_layout.get(column) == capture_layout[column] for column in capture_columns
        )
        if not prefix_is_unchanged:
            raise InformixError(
                f"Informix schema changed for {native!r} during CDC; "
                "run a full refresh before decoding additional records"
            )

    def _describe_table(self, owner: str, name: str) -> dict[str, Any]:
        columns = self.transport.execute(
            "SELECT c.colname, c.coltype, c.collength, c.colno, t.tabid, "
            "c.extended_id, x.name AS extended_name, x.owner AS extended_owner "
            "FROM systables t JOIN syscolumns c ON t.tabid = c.tabid "
            "LEFT JOIN sysxtdtypes x ON x.extended_id = c.extended_id "
            "WHERE t.tabtype = 'T' AND t.owner = ? AND t.tabname = ? ORDER BY c.colno",
            (owner, name),
            max_result_bytes=(int(self.options.get("metadata.max.bytes", str(64 << 20))) or None),
        )
        keys = self.transport.execute(
            "SELECT i.part1,i.part2,i.part3,i.part4,i.part5,i.part6,i.part7,i.part8,"
            "i.part9,i.part10,i.part11,i.part12,i.part13,i.part14,i.part15,i.part16 "
            "FROM systables t JOIN sysconstraints x ON t.tabid=x.tabid "
            "JOIN sysindexes i ON x.idxname=i.idxname AND x.tabid=i.tabid "
            "WHERE t.tabtype='T' AND x.constrtype='P' AND t.owner=? AND t.tabname=?",
            (owner, name),
            max_result_bytes=(int(self.options.get("metadata.max.bytes", str(64 << 20))) or None),
        )
        return self._table_from_catalog_rows(owner, name, columns, keys)

    def _table_from_catalog_rows(
        self, owner: str, name: str, columns: list[Any], keys: list[Any]
    ) -> dict[str, Any]:
        parsed_columns = [_catalog_column(row) for row in columns]
        for column in parsed_columns:
            if column["type_name"] in {"INT8", "SERIAL8"}:
                column["cdc_supported"] = True
            if column["type_name"] == "DATETIME":
                start, end = (column["length"] >> 8) & 0xF, column["length"] & 0xF
                column["cdc_supported"] = (
                    start in {0, 2, 4, 6, 8, 10}
                    and end in {0, 2, 4, 6, 8, 10, 11, 12, 13, 14, 15}
                    and end >= start
                )
        by_number = {
            int(_field(row, "colno", 3)): str(_field(row, "colname", 0)) for row in columns
        }
        primary_keys = []
        if keys:
            for position in range(16):
                column_number = int(_field(keys[0], f"part{position + 1}", position))
                if column_number > 0:
                    primary_keys.append(by_number[column_number])
        try:
            tabid = int(_field(columns[0], "tabid", 4))
        except (IndexError, KeyError, TypeError, ValueError) as error:
            raise InformixError(
                f"Informix catalog metadata for {owner}.{name} is missing tabid"
            ) from error
        if tabid <= 0:
            raise InformixError(
                f"Informix catalog metadata for {owner}.{name} has invalid tabid {tabid}"
            )
        return {
            "database": self.config["database"],
            "owner": owner,
            "name": name,
            "columns": parsed_columns,
            "primary_keys": primary_keys,
            "incarnation": str(tabid),
        }

    @_serialized_sqli_operation
    def current_lsn(self) -> int:
        self._ensure_connected()
        row = self.transport.execute(
            "SELECT uniqid, used FROM sysmaster:syslogs WHERE is_current = 1"
        )[0]
        return (int(_field(row, "uniqid", 0)) << 32) + (int(_field(row, "used", 1)) << 12)

    @_serialized_sqli_operation
    def minimum_lsn(self) -> int:
        self._ensure_connected()
        row = self.transport.execute("SELECT MIN(uniqid) AS uniqid FROM sysmaster:syslogs")[0]
        return int(_field(row, "uniqid", 0)) << 32

    @_serialized_sqli_operation
    def prepare_initial_capture(self, identities: Sequence[str]) -> int:
        """Enable full-row logging for every table, then capture one shared LSN."""

        self._ensure_connected()

        if not identities:
            raise InformixError("Initial CDC preparation requires at least one table")
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
            raise InformixError(
                "Initial CDC preparation was partially applied; full-row logging remains "
                f"enabled for {enabled!r}. Correct the failure and rerun preparation."
            ) from error
        return self.current_lsn()

    @_serialized_sqli_operation
    def validate_initial_lsn(self, capture: dict[str, Any], start_lsn: int) -> None:
        """Validate CDC registration/activation without reading LODATA records."""

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
            raise InformixError(f"cdc_opensess failed with Informix error {session}")
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
            cleanup_errors = []
            cleanup_timed_out = False
            if started:
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
                    cleanup_timed_out = cleanup_timed_out or _is_timeout_error(error)
            try:
                _expect_zero(
                    self.transport.execute(
                        f"EXECUTE FUNCTION {cdc_routine('cdc_closesess')}(?)", (session,)
                    ),
                    "cdc_closesess",
                )
            except Exception as error:
                cleanup_errors.append(error)
                cleanup_timed_out = cleanup_timed_out or _is_timeout_error(error)
            if set_socket_timeout is not None and previous_socket_timeout is not None:
                try:
                    set_socket_timeout(float(previous_socket_timeout))
                except Exception as error:
                    cleanup_errors.append(error)
            # The purpose of this method -- proving the CDC boundary registers and
            # activates -- is already met once activation succeeds (primary_error is
            # None). cdc_endcapture/cdc_closesess are best-effort teardown: the server
            # reclaims an abandoned CDC session on its own, and this method returns no
            # value derived from them. A teardown that merely *times out* under heavy
            # syscdcv1 load (many tables opening CDC sessions at once) must not fail an
            # otherwise-successful validation -- doing so was observed wedging a flow
            # (e.g. tw101) purely on a slow teardown. Poison the transport so the next
            # use reconnects rather than resuming a half-torn-down session, and treat it
            # as non-fatal. A non-timeout cleanup failure, or any failure alongside a
            # real primary error, is still surfaced.
            if cleanup_errors and primary_error is None:
                if cleanup_timed_out and all(_is_timeout_error(e) for e in cleanup_errors):
                    # The socket is mid-read on a half-torn-down CDC session; drop the
                    # transport so the next operation reconnects instead of resuming it,
                    # keeping the connection slot (this is not a capacity problem).
                    reset_transport = getattr(self, "reset_transport", None)
                    if callable(reset_transport):
                        reset_transport()
                    logging.getLogger(__name__).warning(
                        "Informix CDC validation succeeded but session teardown timed out; "
                        "treating as non-fatal and reconnecting. Raise cdc.read.timeout.seconds "
                        "or reduce concurrent CDC sessions if this recurs.",
                        exc_info=cleanup_errors[0],
                    )
                    return
                raise InformixError("Initial CDC validation cleanup failed") from cleanup_errors[0]
            if cleanup_errors and primary_error is not None:
                for error in cleanup_errors:
                    add_informix_exception_note(
                        primary_error,
                        f"Initial Informix CDC validation cleanup also failed: {error}",
                    )

    @_serialized_sqli_operation
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
    ):
        self._ensure_connected()
        database, owner, name = _split_identity(identity)
        for identifier in (database, owner, name, *columns, *primary_keys):
            if not _IDENTIFIER.fullmatch(identifier):
                raise InformixError(f"Unsafe snapshot identifier {identifier!r}")
        if int(skip) < 0:
            raise InformixError("Snapshot SKIP offset cannot be negative")
        # chunk_exprs maps a primary-key column to an order-preserving SQL text
        # expression used for keyset comparison and ordering (see
        # datetime_order_preserving_cast). The rendered value is projected under
        # a "__chunk_<key>" alias so the caller can track the cursor as a string.
        chunk_exprs = dict(chunk_exprs or {})
        for key in chunk_exprs:
            if key not in primary_keys:
                raise InformixError(f"Chunk expression names unknown key {key!r}")

        def order_term(key: str) -> str:
            return chunk_exprs.get(key, key)

        alias_names = [f"__chunk_{key}" for key in primary_keys if key in chunk_exprs]
        projection = list(columns) + [
            f"{chunk_exprs[key]} AS __chunk_{key}" for key in primary_keys if key in chunk_exprs
        ]
        window = f"SKIP {int(skip)} " if skip else ""
        sql = (
            f"SELECT {window}FIRST {int(limit)} {','.join(projection)} "
            f"FROM {database}:{_sql_identifier(owner)}.{_sql_identifier(name)}"
        )
        parameters: list[Any] = []
        predicates = [f"({snapshot_filter})"] if snapshot_filter else []
        if after is not None:
            validate_snapshot_arity(after, primary_keys)
            bound_after = list(after)
            if any(isinstance(value, str) for value in after):
                table = self._describe_table(owner, name)
                descriptors = {str(column["name"]): column for column in table["columns"]}
                for index, key in enumerate(primary_keys):
                    descriptor = descriptors[key]
                    # DATETIME keys chunked by expression compare as strings, so
                    # they must not be rewritten into a native DATETIME literal.
                    if (
                        key not in chunk_exprs
                        and descriptor["type_name"] == "DATETIME"
                        and isinstance(bound_after[index], str)
                    ):
                        bound_after[index] = InformixDatetimeLiteral(
                            bound_after[index], int(descriptor["length"])
                        )
            clauses = []
            for index, key in enumerate(primary_keys):
                prefix = " AND ".join(
                    f"{order_term(previous)} = ?" for previous in primary_keys[:index]
                )
                clauses.append(f"({prefix + ' AND ' if prefix else ''}{order_term(key)} > ?)")
                parameters.extend(bound_after[: index + 1])
            predicates.append("(" + " OR ".join(clauses) + ")")
        if predicates:
            sql += " WHERE " + " AND ".join(predicates)
        if primary_keys:
            sql += " ORDER BY " + ",".join(order_term(key) for key in primary_keys)
        rows = self.transport.execute(
            sql,
            tuple(parameters),
            max_result_bytes=(
                (max_bytes or None)
                if max_bytes is not None
                else (int(self.options.get("snapshot.max.bytes", "0")) or None)
            ),
        )
        return [
            dict(row)
            if isinstance(row, dict)
            else dict(zip(list(columns) + alias_names, row, strict=True))
            for row in rows
        ]

    @contextlib.contextmanager
    def _repeatable_read_transaction(self, isolation: str = "REPEATABLE READ"):
        """Run a snapshot transaction at ``isolation`` with the snapshot socket timeout.

        Yields ``execute_command``. Commits on success and rolls back on any
        error, preserving the primary error if rollback also fails. Isolates
        the ANSI/non-ANSI transaction handling shared by the blocking
        consistent snapshot and per-chunk incremental snapshot reads.

        ``isolation`` is the SQL clause after ``SET ISOLATION TO`` and must be one
        of the fixed clauses in ``_SNAPSHOT_ISOLATION_SQL`` -- it is interpolated
        into a statement, so it is guarded here against anything else even though
        the connector already maps it from a validated token.
        """

        if isolation not in _SNAPSHOT_ISOLATION_SQL.values():
            raise InformixError(f"Unsupported snapshot isolation {isolation!r}")
        execute_command = getattr(self.transport, "execute_command", self.transport.execute)
        ansi_rows = self.transport.execute(
            "SELECT is_ansi FROM sysmaster:sysdatabases WHERE name = ?",
            (self.config["database"],),
        )
        if len(ansi_rows) != 1:
            raise InformixError(
                f"Could not determine transaction mode for database " f"{self.config['database']!r}"
            )
        is_ansi = bool(int(_field(ansi_rows[0], "is_ansi", 0)))
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
                    logging.getLogger(__name__).warning(
                        "Failed to restore Informix snapshot socket timeout",
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
    ):
        """Read one PK-ordered chunk in its own repeatable-read transaction.

        Returns ``(chunk_lsn, rows)`` where ``chunk_lsn`` is the source LSN
        captured immediately before the chunk SELECT. The chunk is read at
        REPEATABLE READ so its values are consistent as-of ``chunk_lsn``; the
        transaction spans a single page, so no long-lived read view is held.
        ``chunk_exprs`` optionally maps primary-key columns to order-preserving
        SQL text expressions for keyset comparison (e.g. DATETIME-as-string).
        """

        self._ensure_connected()
        with self._repeatable_read_transaction():
            chunk_lsn = self.current_lsn()
            rows = self.snapshot_page(
                identity,
                columns,
                primary_keys,
                after,
                limit,
                max_bytes,
                chunk_exprs=chunk_exprs,
                snapshot_filter=snapshot_filter,
            )
            return chunk_lsn, rows

    @_serialized_sqli_operation
    def max_primary_key(self, identity, primary_keys, chunk_exprs=None, snapshot_filter=None):
        """Return the largest primary-key tuple, or ``None`` for an empty table.

        When ``chunk_exprs`` maps a key to an order-preserving SQL expression,
        that key is ordered and returned as its rendered string form so the
        upper bound matches the values chunk pages compare against.
        """

        self._ensure_connected()
        database, owner, name = _split_identity(identity)
        for identifier in (database, owner, name, *primary_keys):
            if not _IDENTIFIER.fullmatch(identifier):
                raise InformixError(f"Unsafe max-key identifier {identifier!r}")
        chunk_exprs = dict(chunk_exprs or {})
        select_terms = [
            f"{chunk_exprs[key]} AS __chunk_{key}" if key in chunk_exprs else key
            for key in primary_keys
        ]
        result_names = [f"__chunk_{key}" if key in chunk_exprs else key for key in primary_keys]
        order = ",".join(f"{chunk_exprs.get(key, key)} DESC" for key in primary_keys)
        sql = (
            f"SELECT FIRST 1 {','.join(select_terms)} "
            f"FROM {database}:{_sql_identifier(owner)}.{_sql_identifier(name)}"
        )
        if snapshot_filter:
            sql += f" WHERE ({snapshot_filter})"
        sql += f" ORDER BY {order}"
        rows = self.transport.execute(sql, ())
        if not rows:
            return None
        row = rows[0]
        if isinstance(row, dict):
            return [row[name] for name in result_names]
        return list(row)

    @_serialized_sqli_operation
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
    ):
        """Read one bounded point-in-time snapshot at ``isolation``.

        ``isolation`` defaults to REPEATABLE READ, which freezes the scanned rows
        so the snapshot is exactly the committed state as-of ``snapshot_lsn``. A
        non-locking level (e.g. COMMITTED READ LAST COMMITTED) avoids locking the
        table but no longer freezes it, so rows committed during the scan may be
        captured here and replayed by CDC -- harmless for a keyed table (the sink
        de-duplicates), a duplicate-row risk for a key-less one.
        """

        self._ensure_connected()

        # Informix SQLI does not expose parameter binding, so keyset pagination
        # must render continuation values as SQL literals. Some valid partial
        # DATETIME values are rejected when parsed back by Informix (-1263).
        # Within this snapshot transaction, positional pagination is stable and
        # avoids converting DATETIME keys back into SQL.
        positional_pagination = bool(datetime_primary_key)

        self._ensure_connected()
        with self._repeatable_read_transaction(isolation):
            snapshot_lsn = self.current_lsn()
            rows: list[dict[str, Any]] = []
            retained_bytes = _deep_size(rows) if max_bytes else 0
            total_rows = 0
            page_index = 0

            if not primary_keys:
                database, owner, name = _split_identity(identity)
                for identifier in (database, owner, name, *columns):
                    if not _IDENTIFIER.fullmatch(identifier):
                        raise InformixError(f"Unsafe snapshot identifier {identifier!r}")
                execute_pages = getattr(self.transport, "execute_pages", None)
                if not callable(execute_pages):
                    raise InformixError(
                        "Configured Informix SQLI transport does not implement execute_pages()"
                    )

                def consume(page: list[dict[str, Any]]) -> None:
                    nonlocal retained_bytes, total_rows, page_index
                    if max_rows and total_rows + len(page) > max_rows:
                        raise InformixError(
                            f"Initial snapshot exceeds snapshot.max.rows={max_rows}"
                        )
                    if max_bytes:
                        retained_bytes += _deep_size(page)
                        if retained_bytes > max_bytes:
                            raise InformixError(
                                f"Initial snapshot exceeds snapshot.max.bytes={max_bytes}"
                            )
                    if page_consumer is None:
                        rows.extend(page)
                    else:
                        page_consumer(snapshot_lsn, page_index, page)
                    total_rows += len(page)
                    page_index += 1

                execute_pages(
                    f"SELECT {','.join(columns)} "
                    f"FROM {database}:{_sql_identifier(owner)}.{_sql_identifier(name)}"
                    + (f" WHERE ({snapshot_filter})" if snapshot_filter else ""),
                    (),
                    page_size,
                    consume,
                )
                return snapshot_lsn, rows

            after = None
            skip = 0
            while True:
                remaining_rows = max_rows - total_rows if max_rows else page_size
                page_capacity = min(page_size, remaining_rows)
                remaining_bytes = max_bytes - retained_bytes if max_bytes else None
                if remaining_bytes is not None and remaining_bytes <= 0:
                    raise InformixError(f"Initial snapshot exceeds snapshot.max.bytes={max_bytes}")
                page = self.snapshot_page(
                    identity,
                    columns,
                    primary_keys,
                    after,
                    page_capacity + 1,
                    remaining_bytes,
                    skip=skip if positional_pagination else 0,
                    snapshot_filter=snapshot_filter,
                )
                has_more = len(page) > page_capacity
                if has_more and max_rows and remaining_rows == 0:
                    raise InformixError(f"Initial snapshot exceeds snapshot.max.rows={max_rows}")
                accepted = page[:page_capacity]
                if max_bytes:
                    retained_bytes += _deep_size(accepted)
                if max_bytes and retained_bytes > max_bytes:
                    raise InformixError(f"Initial snapshot exceeds snapshot.max.bytes={max_bytes}")
                if page_consumer is None:
                    rows.extend(accepted)
                else:
                    page_consumer(snapshot_lsn, page_index, accepted)
                total_rows += len(accepted)
                if not has_more:
                    break
                if positional_pagination:
                    skip += len(accepted)
                else:
                    after = [accepted[-1][key] for key in primary_keys]
                page_index += 1
            return snapshot_lsn, rows

    @_serialized_sqli_operation
    def read_changes(self, tables, start_lsn, timeout_seconds, max_records):
        self._ensure_connected()
        set_socket_timeout = getattr(self.transport, "set_socket_timeout", None)
        previous_socket_timeout = getattr(self.transport, "socket_timeout", None)
        server_row = self.transport.execute(
            "SELECT env_value FROM sysmaster:sysenv WHERE env_name='INFORMIXSERVER'"
        )[0]
        server = str(_field(server_row, "env_value", 0))
        session_row = self.transport.execute(
            f"EXECUTE FUNCTION {cdc_routine('cdc_opensess')}(?, 0, ?, ?, 1, 1)",
            (server, timeout_seconds, max_records),
        )[0]
        session = int(_field(session_row, "session_id", 0))
        if session < 0:
            raise InformixError(f"cdc_opensess failed with Informix error {session}")
        labels: dict[int, tuple[ColumnDescriptor, ...]] = {}
        captures = []
        try:
            if set_socket_timeout is not None:
                set_socket_timeout(max(float(previous_socket_timeout or 30), timeout_seconds + 5.0))
            for label, capture in enumerate(tables, 1):
                native = capture["identity"]
                columns = tuple(capture["columns"])
                _expect_zero(
                    self.transport.execute(
                        f"EXECUTE FUNCTION {cdc_routine('cdc_set_fullrowlogging')}(?, 1)",
                        (native,),
                    ),
                    "cdc_set_fullrowlogging",
                )
                _expect_zero(
                    self.transport.execute(
                        f"EXECUTE FUNCTION {cdc_routine('cdc_startcapture')}(?, 0, ?, ?, ?)",
                        (session, native, ",".join(columns), label),
                    ),
                    "cdc_startcapture",
                )
                captures.append(native)
                labels[label] = tuple(_column_descriptor(c) for c in capture["descriptors"])
            _expect_zero(
                self.transport.execute(
                    f"EXECUTE FUNCTION {cdc_routine('cdc_activatesess')}(?, ?)",
                    (session, start_lsn),
                ),
                "cdc_activatesess",
            )
            parser = CdcFrameParser(int(self.options.get("cdc.max.frame.bytes", str(16 << 20))))
            records = []
            budget_records = 0
            open_transactions = OpenTransactionRecords()
            max_transaction_records = int(self.options.get("cdc.max.transaction.records", "100000"))
            max_poll_records = int(self.options.get("cdc.max.poll.records", "200000"))
            max_poll_bytes = int(self.options.get("cdc.max.poll.bytes", "0"))
            requested = int(self.options.get("cdc.read.bytes", "32000"))
            empty_reads = 0
            timed_out = False
            retained_bytes = 0
            truncated = False
            completed_transactions = 0
            metadata_labels: set[int] = set()

            def _stop_at_poll_bound(bound: str) -> None:
                # The poll bounds cap what one poll may retain, not how much the
                # stream may contain. An interleaved long-running transaction
                # keeps this loop reading far past cdc.max.records, so under a
                # bulk load the bound is reached routinely rather than
                # exceptionally. End the poll and return the transactions that
                # already completed: they advance the checkpoint, and the next
                # poll resumes from the oldest still-open BEGIN.
                if not completed_transactions:
                    # Nothing completed, so there is no progress to checkpoint.
                    # Returning empty would leave the flow retrying the same
                    # bound forever, so surface the stall instead.
                    raise InformixError(
                        f"A CDC poll exceeded {bound} before any transaction completed"
                    )
                logging.getLogger(__name__).warning(
                    "Informix CDC poll reached %s after %d completed transactions; "
                    "ending the poll early and resuming from the oldest open transaction",
                    bound,
                    completed_transactions,
                )

            # cdc.max.records is a soft boundary: once crossed, finish every
            # transaction already observed so a transaction larger than the
            # native boundary can make progress. An idle open transaction is
            # still bounded by Informix's TIMEOUT control frame.
            while (
                (budget_records < max_records or open_transactions)
                and empty_reads < 1
                and not timed_out
                and not truncated
            ):
                chunk = self.transport.read_lodata(session, requested)
                if not chunk:
                    empty_reads += 1
                    continue
                empty_reads = 0
                for frame in parser.feed(chunk):
                    try:
                        record = decode_frame(frame, labels)
                    except CdcProtocolError as error:
                        raise _stale_capture_label_error(frame, error, start_lsn) from error
                    if max_poll_bytes:
                        retained_bytes += len(frame) + _deep_size(record)
                    if max_poll_bytes and retained_bytes > max_poll_bytes:
                        _stop_at_poll_bound(f"cdc.max.poll.bytes={max_poll_bytes}")
                        truncated = True
                        break
                    if record["op"] == "TIMEOUT":
                        # Informix represents an idle CDC timeout as a real,
                        # non-empty protocol frame. Treat it as the terminal
                        # condition for this finite poll instead of waiting for
                        # max_records timeout frames.
                        timed_out = True
                    label = record.get("label", record.get("capture_label"))
                    if record["op"] == "METADATA" and label in labels:
                        if label in metadata_labels:
                            raise InformixError(
                                f"Informix emitted a second CDC metadata layout for capture "
                                f"label {label}; run a full refresh before continuing"
                            )
                        descriptors = {column.name: column for column in labels[label]}
                        encoding = next(iter(descriptors.values())).encoding
                        names = metadata_column_names(record["metadata"], encoding)
                        if set(names) != set(descriptors):
                            raise InformixError(
                                f"CDC metadata for capture label {label} does not match "
                                "the requested columns"
                            )
                        self._assert_capture_layout(tables[label - 1], encoding)
                        labels[label] = tuple(descriptors[name] for name in names)
                        metadata_labels.add(label)
                    if label and 1 <= label <= len(tables):
                        record["table"] = tables[label - 1]["logical_identity"]
                    if len(records) >= max_poll_records:
                        _stop_at_poll_bound(f"cdc.max.poll.records={max_poll_records}")
                        truncated = True
                        break
                    records.append(record)
                    if record["op"] not in {"METADATA", "TIMEOUT"}:
                        budget_records += 1
                    if record["op"] == "COMMIT":
                        completed_transactions += 1
                    if record["op"] == "BEGIN":
                        open_transactions.begin(int(record["tx_id"]))
                    elif record["op"] in _DATA_OPS:
                        open_transactions.append(int(record["tx_id"]), int(record["lsn"]))
                    elif record["op"] == "DISCARD":
                        open_transactions.discard(int(record["tx_id"]), int(record["lsn"]))
                    elif record["op"] in {"COMMIT", "ROLLBACK"}:
                        open_transactions.finish(int(record["tx_id"]))
                    if open_transactions.buffered > max_transaction_records:
                        raise InformixError(
                            "An open CDC transaction exceeded "
                            f"cdc.max.transaction.records={max_transaction_records}"
                        )
                    if timed_out:
                        # Ignore anything following the terminal timeout in this
                        # session. The checkpoint remains at the last complete
                        # transaction, so a later poll safely replays it.
                        break
            if parser.buffered_bytes and not timed_out and not truncated:
                # A truncated poll abandons the rest of the chunk deliberately,
                # so a partial trailing frame is expected rather than corruption.
                raise InformixError("CDC read ended with an incomplete native frame")
            return records
        finally:
            cleanup_errors = []
            lease_lost = getattr(self, "_connection_lease_lost", None)
            if lease_lost is None or not lease_lost.is_set():
                for native in captures:
                    try:
                        _expect_zero(
                            self.transport.execute(
                                f"EXECUTE FUNCTION {cdc_routine('cdc_endcapture')}(?, 0, ?)",
                                (session, native),
                            ),
                            "cdc_endcapture",
                        )
                    except Exception as error:  # preserve an active CDC failure
                        cleanup_errors.append(error)
                try:
                    _expect_zero(
                        self.transport.execute(
                            f"EXECUTE FUNCTION {cdc_routine('cdc_closesess')}(?)", (session,)
                        ),
                        "cdc_closesess",
                    )
                except Exception as error:  # preserve an active CDC failure
                    cleanup_errors.append(error)
                if set_socket_timeout is not None and previous_socket_timeout is not None:
                    try:
                        set_socket_timeout(float(previous_socket_timeout))
                    except Exception as error:
                        cleanup_errors.append(error)
            active_error = sys.exc_info()[1]
            if cleanup_errors:
                if active_error is None:
                    raise InformixError("Informix CDC session cleanup failed") from cleanup_errors[
                        0
                    ]
                for error in cleanup_errors:
                    add_informix_exception_note(
                        active_error, f"Informix CDC cleanup also failed: {error}"
                    )


_bridge_factory: Callable[[dict[str, str]], InformixBridge] = PurePythonInformixBridge


def set_bridge_factory(factory: Callable[[dict[str, str]], InformixBridge]) -> None:
    """Set the process-wide bridge factory (primarily for deterministic tests)."""

    global _bridge_factory
    _bridge_factory = factory


def _bridge_config(options: dict[str, str]) -> dict[str, Any]:
    required = ("hostname", "database", "user", "password", "server")
    missing = [name for name in required if not options.get(name)]
    if missing:
        raise ValueError(f"Missing required Informix option(s): {', '.join(missing)}")
    return {
        "hostname": options["hostname"],
        "port": int(options.get("port", "9088")),
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
        "pad_varchar": _option_bool(options, "padVarchar", False),
        "cdc_timeout": int(options.get("cdc.timeout", "5")),
        "cdc_max_records": int(options.get("cdc.max.records", str(_DEFAULT_CDC_MAX_RECORDS))),
        "stop_logging_on_close": False,
    }


def _option_bool(options: dict[str, str], name: str, default: bool) -> bool:
    value = options.get(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes"}:
        return True
    if normalized in {"0", "false", "no"}:
        return False
    raise ValueError(f"Option '{name}' must be one of: 1, true, yes, 0, false, no")


def _append_only_value(raw: str) -> str:
    """Normalize the tri-state ``append.only.ingestion`` option.

    ``true``/``false`` force append (or not) for every capturable table; ``auto``
    (also the default when the option is unset) applies append only to keyless
    tables and leaves keyed tables on their normal CDC/snapshot path.
    """

    normalized = (raw or "").strip().lower()
    if normalized in {"1", "true", "yes"}:
        return "true"
    if normalized in {"0", "false", "no"}:
        return "false"
    if normalized == "auto":
        return "auto"
    raise ValueError(
        f"Option '{_APPEND_INGESTION_OPTION}' must be one of: true, false, auto (or 1/yes, 0/no)"
    )


_PRIMARY_KEYS_OPTION = "primary.keys"


def _parse_key_columns(raw: str) -> list[str]:
    """Parse the primary.keys option -- a comma-separated list or a JSON array."""

    text = (raw or "").strip()
    if not text:
        return []
    if text.startswith("["):
        try:
            values = json.loads(text)
        except json.JSONDecodeError as error:
            raise ValueError(
                f"Option '{_PRIMARY_KEYS_OPTION}' is not a valid JSON array: {text!r}"
            ) from error
        items = [str(value).strip() for value in values]
    else:
        items = [item.strip() for item in text.split(",")]
    return [item for item in items if item]


def _snapshot_filter(options: dict[str, str]) -> str | None:
    """Return a validated connector-owned snapshot WHERE predicate."""

    value = options.get("snapshot.filter")
    if value is None:
        return None
    predicate = value.strip()
    if not predicate:
        return None
    if len(predicate) > 8192:
        raise ValueError("Option 'snapshot.filter' cannot exceed 8192 characters")
    if any(ord(character) < 32 for character in predicate):
        raise ValueError("Option 'snapshot.filter' cannot contain control characters")
    if ";" in predicate or "--" in predicate or "/*" in predicate or "*/" in predicate:
        raise ValueError(
            "Option 'snapshot.filter' must be one SQL predicate without comments or semicolons"
        )
    return predicate


def _validated_volume_location(location: str, option_name: str, options: dict[str, str]) -> str:
    if not os.path.isabs(location):
        raise ValueError(f"Option '{option_name}' must be an absolute path")
    normalized = os.path.normpath(location)
    if normalized != location.rstrip("/") or any(
        part in {".", ".."} for part in location.split("/")
    ):
        raise ValueError(f"Option '{option_name}' must not contain traversal")
    if options.get("hostname"):
        parts = normalized.split("/")
        if len(parts) < 5 or parts[1] != "Volumes" or any(not part for part in parts[2:5]):
            raise ValueError(
                f"Option '{option_name}' must be a Unity Catalog Volume path under "
                "/Volumes/<catalog>/<schema>/<volume>"
            )
    return normalized


def _newest_created_entry_mtime(path: str, recursive: bool = False) -> float:
    """Return the newest storage-assigned timestamp without trusting directory updates.

    Considers only the creation timestamps of the *files* found beneath ``path``.
    Every artifact this inspects is created with O_EXCL and never rewritten, so a
    file's mtime is its creation time.

    Directory mtimes are excluded because their meaning is filesystem-specific:
    a local POSIX filesystem advances them when entries are *removed*, while a
    Volume settles them to a creation-like whole second but briefly surfaces a
    transient post-mutation value first. Either way, cleaning an artifact's
    contents could make the artifact look newly created to a caller sampling at
    the wrong moment, keeping a stale lease alive. The scanned directory's own
    timestamp is used only when it contains no files at all, where it bounds the
    age rather than extending it.
    """

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    root = os.open(path, flags)
    newest: float | None = None
    root_mtime = os.fstat(root).st_mtime
    stack = [root]
    try:
        while stack:
            descriptor = stack.pop()
            try:
                with os.scandir(descriptor) as entries:
                    for entry in entries:
                        metadata = entry.stat(follow_symlinks=False)
                        if not stat.S_ISDIR(metadata.st_mode):
                            created_at = metadata.st_mtime
                            newest = created_at if newest is None else max(newest, created_at)
                        elif recursive:
                            stack.append(os.open(entry.name, flags, dir_fd=descriptor))
            finally:
                os.close(descriptor)
        return root_mtime if newest is None else newest
    except BaseException:
        for descriptor in stack:
            os.close(descriptor)
        raise


def _remove_candidate_tree_at(parent_descriptor: int, name: str) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    root = os.open(name, flags, dir_fd=parent_descriptor)
    stack: list[tuple[int, os.ScandirIterator, str | None]] = [(root, os.scandir(root), None)]
    try:
        while stack:
            descriptor, entries, child_name = stack[-1]
            try:
                entry = next(entries)
            except StopIteration:
                entries.close()
                os.close(descriptor)
                stack.pop()
                if stack and child_name is not None:
                    os.rmdir(child_name, dir_fd=stack[-1][0])
                continue
            metadata = entry.stat(follow_symlinks=False)
            if stat.S_ISDIR(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode):
                child = os.open(entry.name, flags, dir_fd=descriptor)
                stack.append((child, os.scandir(child), entry.name))
            else:
                os.unlink(entry.name, dir_fd=descriptor)
    finally:
        while stack:
            descriptor, entries, _ = stack.pop()
            entries.close()
            os.close(descriptor)


def _namespace_storage_now(namespace_descriptor: int) -> float | None:
    """Return a storage-assigned wall clock for lease decisions, or None.

    Writes and removes a short-lived probe file, reading its storage-assigned
    mtime so lease comparisons use the filesystem's clock rather than a possibly
    skewed worker clock. Returns None when the probe cannot be written.
    """

    probe = f".now-{secrets.token_hex(8)}"
    try:
        descriptor = os.open(
            probe,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            mode=0o600,
            dir_fd=namespace_descriptor,
        )
    except OSError:
        return None
    try:
        now = os.fstat(descriptor).st_mtime
    finally:
        os.close(descriptor)
        try:
            os.unlink(probe, dir_fd=namespace_descriptor)
        except OSError:
            pass
    return now


def _fsync_directory_path(path: str) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError as error:
        if error.errno in {errno.EINVAL, errno.ENOTSUP, errno.EACCES}:
            return
        raise
    try:
        try:
            os.fsync(descriptor)
        except OSError as error:
            if error.errno not in {errno.EINVAL, errno.ENOTSUP}:
                raise
    finally:
        os.close(descriptor)


def _dropped_mount_error(error: BaseException) -> OSError | None:
    """Return the ENOTCONN in ``error``'s cause chain, if a dropped mount caused it.

    Snapshot page staging is the only remaining Volume user, but it still touches
    the mount from several calls -- open, makedirs, rename, listdir -- and only
    ``os.open`` goes through the retry helper. When the mount drops, whichever
    call runs first raises ENOTCONN, so no single site can be the place this is
    handled; production saw it surface from two different sites in consecutive
    updates.

    Classifying at the read boundary instead covers every site at once. The
    caller converts this into the same empty-batch checkpoint retry it already
    uses for a transiently inaccessible Volume, which is exactly what a remount
    needs: the mount returns within seconds and the next read proceeds.
    """

    seen: set[int] = set()
    cursor: BaseException | None = error
    while cursor is not None and id(cursor) not in seen:
        seen.add(id(cursor))
        if isinstance(cursor, OSError) and cursor.errno == errno.ENOTCONN:
            return cursor
        cursor = cursor.__cause__ or cursor.__context__
    return None


def _open_state_entry_with_retry(path: str, flags: int, *, dir_fd: int | None = None) -> int:
    """Open one state entry, retrying transient Volume permission and mount failures."""

    last_error: OSError | None = None
    for attempt in range(_SHARED_STATE_OPEN_ATTEMPTS):
        try:
            return os.open(path, flags, dir_fd=dir_fd)
        except OSError as error:
            if error.errno not in {errno.EPERM, errno.EACCES, errno.ENOTCONN}:
                raise
            if error.errno == errno.ENOTCONN:
                # The Volume's FUSE mount dropped: the filesystem itself is
                # briefly gone rather than this entry being denied. Observed in
                # production taking down 23 flows at once, every one failing on
                # the shared-state root, while the mount was healthy again
                # moments later. A remount settles on a scale of seconds, not the
                # milliseconds a permission blip needs, so this backs off harder
                # (and skips the symlink check below, which exists only to
                # disambiguate EPERM and cannot stat an absent filesystem).
                last_error = error
                if attempt + 1 < _SHARED_STATE_OPEN_ATTEMPTS:
                    delay = min(0.25 * (2**attempt), _SHARED_STATE_MOUNT_RETRY_MAX_SECONDS)
                    time.sleep(random.uniform(delay / 2, delay))
                continue
            # Some filesystems report EPERM rather than ELOOP for O_NOFOLLOW.
            # Preserve the security distinction between a transiently denied
            # Volume entry and a replaceable symlink.
            try:
                entry = os.stat(path, dir_fd=dir_fd, follow_symlinks=False)
            except OSError:
                entry = None
            if entry is not None and stat.S_ISLNK(entry.st_mode):
                raise InformixError(
                    f"Informix shared-state path traverses symlink: '{path}'"
                ) from error
            last_error = error
            if attempt + 1 < _SHARED_STATE_OPEN_ATTEMPTS:
                delay = min(0.01 * (2**attempt), 0.25)
                time.sleep(random.uniform(delay / 2, delay))
    raise SharedStateAccessUnavailable(
        f"Informix shared state is temporarily inaccessible: '{path}'"
    ) from last_error


def _open_state_directory(root: str, path: str) -> int:
    """Open a directory beneath root without following a replaceable symlink."""

    if os.path.commonpath((root, path)) != root:
        raise InformixError(f"Informix shared-state path escapes '{root}': '{path}'")
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = _open_state_entry_with_retry(root, flags)
    try:
        relative = os.path.relpath(path, root)
        for part in () if relative == "." else relative.split(os.sep):
            try:
                child = _open_state_entry_with_retry(part, flags, dir_fd=descriptor)
            except OSError as error:
                if error.errno in {errno.ELOOP, errno.ENOTDIR}:
                    raise InformixError(
                        f"Informix shared-state path traverses symlink or non-directory: '{path}'"
                    ) from error
                raise
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _open_state_file(root: str, path: str) -> int:
    parent = os.path.dirname(path)
    directory = _open_state_directory(root, parent)
    try:
        try:
            return _open_state_entry_with_retry(
                os.path.basename(path),
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=directory,
            )
        except OSError as error:
            if error.errno == errno.ELOOP:
                raise InformixError(
                    f"Informix shared-state path traverses symlink '{path}'"
                ) from error
            raise
    finally:
        os.close(directory)


def _makedirs_durable(root: str, path: str) -> None:
    if os.path.commonpath((root, path)) != root:
        raise InformixError(f"Informix shared-state path escapes '{root}': '{path}'")
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(root, flags)
    except OSError as error:
        if error.errno in {errno.ELOOP, errno.ENOTDIR}:
            raise InformixError(
                f"Informix shared-state root traverses symlink or non-directory: '{root}'"
            ) from error
        if error.errno != errno.ENOENT:
            raise
        parts = root.split(os.sep)
        if len(parts) >= 5 and parts[1] == "Volumes":
            anchor = os.path.join(os.sep, *parts[1:5])
        else:
            anchor = os.path.dirname(root)
            while not os.path.isdir(anchor):
                parent = os.path.dirname(anchor)
                if parent == anchor:
                    raise InformixError(
                        f"Cannot find an existing parent for shared-state root '{root}'"
                    )
                anchor = parent
        descriptor = os.open(anchor, flags)
        root_parts = tuple(
            part for part in os.path.relpath(root, anchor).split(os.sep) if part != "."
        )
    else:
        root_parts = ()
    path_parts = tuple(part for part in os.path.relpath(path, root).split(os.sep) if part != ".")
    try:
        for part in (*root_parts, *path_parts):
            try:
                child = os.open(part, flags, dir_fd=descriptor)
            except FileNotFoundError:
                try:
                    os.mkdir(part, mode=0o700, dir_fd=descriptor)
                except FileExistsError:
                    pass
                os.fsync(descriptor)
                child = os.open(part, flags, dir_fd=descriptor)
            except OSError as error:
                if error.errno in {errno.ELOOP, errno.ENOTDIR}:
                    raise InformixError(
                        "Informix shared-state path traverses symlink or "
                        f"non-directory: '{path}'"
                    ) from error
                raise
            os.close(descriptor)
            descriptor = child
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _cleanup_bucket_number(name: str) -> int | None:
    if re.fullmatch(r"[0-9]{1,20}", name) is None:
        return None
    return int(name)


def _redirect_allowlist(value: str) -> frozenset[tuple[str, int]]:
    result: set[tuple[str, int]] = set()
    for item in filter(None, (part.strip() for part in value.split(","))):
        host, separator, port_text = item.rpartition(":")
        if not separator or not host or not port_text.isdigit() or not 1 <= int(port_text) <= 65535:
            raise ValueError("redirect.allowlist entries must be exact host:numeric-port pairs")
        result.add((host, int(port_text)))
    return frozenset(result)


@dataclass(frozen=True)
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


@dataclass(frozen=True)
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
    def parse(cls, raw: dict[str, Any], default_database: str) -> "Table":
        database = str(raw.get("database") or default_database)
        owner = str(raw["owner"])
        name = str(raw["name"])
        for part in (database, owner, name):
            if not _IDENTIFIER.fullmatch(part):
                raise InformixError(f"Unsafe Informix identifier returned by metadata: {part!r}")
        columns = tuple(Column.parse(c) for c in raw.get("columns", ()))
        pks = tuple(str(v) for v in raw.get("primary_keys", ()))
        column_names = tuple(column.name for column in columns)
        for column_name in column_names:
            if not _IDENTIFIER.fullmatch(column_name):
                raise InformixError(
                    f"Unsafe Informix column identifier returned by metadata: {column_name!r}"
                )
        normalized_names = tuple(column_name.casefold() for column_name in column_names)
        if len(normalized_names) != len(set(normalized_names)):
            raise InformixError(f"Duplicate column names for {database}.{owner}.{name}")
        reserved = {column.casefold() for column in _INTERNAL_COLUMNS}
        collisions = sorted(
            column_name for column_name in column_names if column_name.casefold() in reserved
        )
        if collisions:
            raise InformixError(
                f"Source columns collide with reserved Informix metadata columns: {collisions!r}"
            )
        if len(pks) != len(set(pks)):
            raise InformixError(f"Duplicate primary-key columns for {database}.{owner}.{name}")
        known = {c.name for c in columns}
        if not columns or any(pk not in known for pk in pks):
            raise InformixError(f"Invalid metadata for {database}.{owner}.{name}")
        incarnation_value = raw.get("incarnation")
        incarnation = None if incarnation_value is None else str(incarnation_value)
        return cls(database, owner, name, columns, pks, incarnation)


@dataclass
class Transaction:
    tx_id: int
    begin_lsn: int
    records: list[dict[str, Any]] = field(default_factory=list)
    pending_before: dict[str, Any] | None = None
    last_lsn: int = field(init=False)

    def __post_init__(self) -> None:
        self.last_lsn = self.begin_lsn

    def advance(self, record: dict[str, Any]) -> int:
        lsn = _lsn(record)
        if lsn < self.last_lsn:
            raise InformixError(
                f"CDC LSN regressed in transaction {self.tx_id}: {lsn} < {self.last_lsn}"
            )
        self.last_lsn = lsn
        return lsn

    def append(self, record: dict[str, Any]) -> None:
        self.advance(record)
        op = _operation(record)
        if op == "BEFORE_UPDATE":
            if self.pending_before is not None:
                raise InformixError(f"Unpaired BEFORE_UPDATE in transaction {self.tx_id}")
            self.pending_before = record
            return
        if op == "AFTER_UPDATE":
            if self.pending_before is None:
                raise InformixError(
                    f"AFTER_UPDATE without BEFORE_UPDATE in transaction {self.tx_id}"
                )
            merged = dict(record)
            merged["op"] = "UPDATE"
            merged["before"] = self.pending_before.get("before", self.pending_before.get("row"))
            merged["after"] = record.get("after", record.get("row"))
            self.pending_before = None
            self.records.append(merged)
            return
        self.records.append(record)

    def discard(self, lsn: int) -> None:
        # DISCARD carries the rollback cutoff, not the forward position of the
        # DISCARD control record. It may legitimately precede the latest data LSN.
        self.records = [r for r in self.records if _lsn(r) < lsn]
        if self.pending_before is not None and _lsn(self.pending_before) >= lsn:
            self.pending_before = None


@dataclass(frozen=True)
class CommittedTransaction:
    tx_id: int
    begin_lsn: int
    commit_lsn: int
    restart_lsn: int
    records: tuple[dict[str, Any], ...]


class TransactionBuffer:
    """Buffering for interleaved Informix transactions."""

    def __init__(self) -> None:
        self.open: dict[int, Transaction] = {}

    def feed(self, raw: dict[str, Any]) -> CommittedTransaction | None:
        record = _normalise_record(raw)
        op = _operation(record)
        if op in {"TIMEOUT", "METADATA"}:
            return None
        if op == "ERROR":
            detail = record.get("message") or record.get("payload") or ""
            raise InformixError(
                f"Informix CDC error {record.get('error', 'unknown')} "
                f"with flags {record.get('flags', 'unknown')}: {detail}"
            )
        tx_id = _tx_id(record)
        if op == "BEGIN":
            if tx_id in self.open:
                raise InformixError(f"Duplicate CDC BEGIN for transaction {tx_id}")
            self.open[tx_id] = Transaction(tx_id, _lsn(record))
            return None
        tx = self.open.get(tx_id)
        if tx is None:
            raise InformixError(f"CDC {op} for unknown transaction {tx_id}")
        if op in _DATA_OPS:
            tx.append(record)
            return None
        if op == "DISCARD":
            tx.discard(_lsn(record))
            return None
        if op == "ROLLBACK":
            tx.advance(record)
            del self.open[tx_id]
            return None
        if op != "COMMIT":
            raise InformixError(f"Unknown Informix CDC operation {op!r}")
        if tx.pending_before is not None:
            raise InformixError(f"Transaction {tx_id} committed with unpaired BEFORE_UPDATE")
        end = tx.advance(record)
        del self.open[tx_id]
        restart = min((item.begin_lsn for item in self.open.values()), default=end)
        return CommittedTransaction(tx_id, tx.begin_lsn, end, restart, tuple(tx.records))


@dataclass(frozen=True)
class _SharedCdcSnapshot:
    """A coherent (schema, log position, transactions) view a consumer poll reads
    from a shard in place of touching Informix itself."""

    table: "Table"
    fingerprint: str
    min_lsn: int
    current_lsn: int
    committed: list
    caught_up: bool
    open_begin: int | None


class _SharedCdcShard:
    """One shard of the sharded daemon-reader CDC pool.

    A single daemon thread owns the Informix I/O for this shard's tables -- it reads
    the shared CDC session, fetches schema, and samples the log bounds -- and publishes
    the result here. Consumer streaming polls read the fanned-out transactions and the
    schema/log position from memory rather than connecting themselves. All mutable
    state is guarded by ``condition``.
    """

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
        self.caught_up = False
        self.open_begins: dict[str, int | None] = {}
        self.ready = False
        # daemon configuration, set by the first subscriber
        self.reader_factory: Callable[[], Any] | None = None
        self.timeout_seconds = 5
        self.max_records = _DEFAULT_CDC_MAX_RECORDS
        self.buffer_cap = _DEFAULT_SHARED_CDC_BUFFER
        self.subscriber_ttl = _SHARED_CDC_SUBSCRIBER_TTL_SECONDS

    def configure(
        self, *, reader_factory, timeout_seconds, max_records, buffer_cap, subscriber_ttl
    ):
        with self.condition:
            if self.reader_factory is None:
                self.reader_factory = reader_factory
            self.timeout_seconds = timeout_seconds
            self.max_records = max_records
            self.buffer_cap = buffer_cap
            self.subscriber_ttl = subscriber_ttl

    # ---- consumer side ------------------------------------------------------

    def subscribe(self, identity, table, capture, checkpoint_commit_lsn, floor_lsn):
        with self.condition:
            existing = self.subscribers.get(identity)
            self.subscribers[identity] = {
                "table": table,
                "capture": capture,
                "checkpoint": checkpoint_commit_lsn,
                "floor": floor_lsn,
                "last_seen": time.monotonic(),
            }
            self.buffers.setdefault(identity, deque())
            # Lower the shared read cursor to admit a newly subscribed or lagging
            # table's history. It only moves backward here; the daemon advances it
            # forward as it reads. Activating too low is merely wasteful (re-read then
            # discarded per cursor / _recover); too high would skip a laggard's changes.
            if self.shared_restart is None:
                self.shared_restart = floor_lsn
            elif existing is None:
                self.shared_restart = min(self.shared_restart, floor_lsn)
            self.condition.notify_all()

    def snapshot(self, identity, checkpoint_commit_lsn):
        """Return a coherent view for one table, or None to fall back to a direct
        read (daemon not ready yet, or this table is unknown to the daemon)."""

        with self.condition:
            if not self.ready or identity not in self.schema:
                return None
            table = self.schema.get(identity)
            if table is None or self.min_lsn is None or self.current_lsn is None:
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
            )

    # ---- daemon side --------------------------------------------------------

    def live_subscribers(self):
        """Drop subscribers past the TTL (a stopped flow must not pin the shard) and
        return the current set. Caller holds the lock."""

        now = time.monotonic()
        for identity in list(self.subscribers):
            if now - self.subscribers[identity]["last_seen"] > self.subscriber_ttl:
                del self.subscribers[identity]
                for store in (self.buffers, self.cursors, self.schema, self.open_begins):
                    store.pop(identity, None)
        return dict(self.subscribers)

    def ingest(self, raw, schema_map, min_lsn, current_lsn):
        """Assemble one read's records into transactions, fan them out per table, and
        publish the coherent snapshot. Returns whether the shard is caught up."""

        buffer = TransactionBuffer()
        committed = []
        timed_out = False
        for record in raw:
            if _operation(record) == "TIMEOUT":
                timed_out = True
            done = buffer.feed(record)
            if done is not None:
                committed.append(done)
        committed.sort(key=lambda tx: (tx.commit_lsn, tx.tx_id))
        with self.condition:
            for tx in committed:
                for identity, sub in self.subscribers.items():
                    if tx.commit_lsn <= self.cursors.get(identity, 0):
                        continue
                    if any(_record_matches(record, sub["table"]) for record in tx.records):
                        self.buffers.setdefault(identity, deque()).append(tx)
                        self.cursors[identity] = tx.commit_lsn
            # Resume the next read from the oldest open BEGIN so open transactions are
            # recaptured; per-table cursors above dedup the resulting re-delivery.
            global_open = min((item.begin_lsn for item in buffer.open.values()), default=None)
            if global_open is not None:
                self.shared_restart = global_open
            elif committed:
                self.shared_restart = committed[-1].commit_lsn
            open_begins: dict[str, int | None] = {}
            for identity, sub in self.subscribers.items():
                begins = [
                    item.begin_lsn
                    for item in buffer.open.values()
                    if any(_record_matches(record, sub["table"]) for record in item.records)
                ]
                open_begins[identity] = min(begins) if begins else None
            self.open_begins = open_begins
            self.schema = dict(schema_map)
            self.min_lsn = min_lsn
            self.current_lsn = current_lsn
            self.caught_up = timed_out and not buffer.open
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
                args=(shard,),
                daemon=True,
                name=f"informix-shared-cdc-{key[-2]}-{key[-1]}",
            )
            _SHARED_CDC_THREADS[key] = thread
            thread.start()
        return shard


def _close_shared_cdc_reader(reader) -> None:
    if reader is None:
        return
    try:
        reader.close()
    except Exception:  # noqa: BLE001 - best-effort slot release
        logging.getLogger(__name__).debug("shared CDC reader close failed", exc_info=True)


def _run_shared_cdc_shard(shard: _SharedCdcShard) -> None:
    """Daemon loop for one shard: read the shared CDC session, fetch schema + log
    bounds, and publish. Holds one connection slot only across consecutive productive
    reads; releases it before any idle wait so consumer bootstrap can use the slot."""

    logger = logging.getLogger(__name__)
    reader = None
    try:
        while True:
            with shard.condition:
                subscribers = shard.live_subscribers()
                have_subscribers = bool(subscribers)
                throttled = have_subscribers and any(
                    len(buf) >= shard.buffer_cap for buf in shard.buffers.values()
                )
                restart = shard.shared_restart
                captures = [sub["capture"] for sub in subscribers.values()]
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
                schema_map = {}
                for identity, table in identities:
                    fresh = Table.parse(reader._bridge.get_table(table.identity), table.database)
                    if table.key_override:
                        # Preserve the subscriber's primary.keys override (and its
                        # fingerprint) on the daemon's schema, exactly as
                        # _refresh_table_schema does. Without this an overridden table's
                        # fingerprint never matches its checkpoint, so it would always
                        # fall back to a direct read and never benefit from sharding --
                        # and its PK-change detection would use the wrong keys.
                        fresh = replace(fresh, primary_keys=table.primary_keys, key_override=True)
                    schema_map[identity] = fresh
                min_lsn = reader._bridge.minimum_lsn()
                current_lsn = reader._bridge.current_lsn()
                raw = reader._read_changes_with_reconnect(
                    captures,
                    restart,
                    shard.timeout_seconds,
                    shard.max_records,
                    table=identities[0][1],
                )
                caught_up = shard.ingest(raw, schema_map, min_lsn, current_lsn)
            except Exception:  # noqa: BLE001 - surfaced by degrading to direct reads
                logger.warning(
                    "Shared CDC shard read failed; consumers fall back to direct reads "
                    "until it recovers",
                    exc_info=True,
                )
                shard.mark_unready()
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


class _SnapshotDrainPool:
    """Bounded pool that drains ``initial``-mode snapshots off the microbatch thread.

    A fixed number of driver-resident daemon workers (``snapshot.reader.threads``) pull
    drain jobs off a shared queue; each builds a private reader, runs the ordinary inline
    snapshot drain (which stages the pages and publishes the manifest durably), then
    closes the reader to release its connection slot. Because at most K workers drain at
    once, at most K slots are ever held by snapshots -- the fairness guarantee that keeps
    streaming readers from being starved. A consumer subscribes a job and waits, holding
    no slot, for its result. All state is guarded by ``condition``.
    """

    def __init__(self) -> None:
        self.condition = threading.Condition()
        self.queue: deque = deque()
        # Job keys queued or running: dedups concurrent subscribers so one table drains
        # exactly once even when several flows (or retries) ask for it at the same time.
        self.pending: set = set()
        self.jobs: dict[tuple, dict[str, Any]] = {}
        # job key -> None on success, or the Exception to re-raise to the waiter.
        self.results: dict[tuple, Any] = {}
        self.reader_factory: Callable[[], Any] | None = None
        self.thread_count = _DEFAULT_SNAPSHOT_READER_THREADS

    def configure(self, *, reader_factory, thread_count):
        with self.condition:
            if self.reader_factory is None:
                self.reader_factory = reader_factory
            self.thread_count = thread_count

    def submit(self, job_key, *, table, options, pipeline_scope, allow_keyless):
        with self.condition:
            # Drop a prior terminal result so a re-subscribe after a failed drain (or a
            # process restart that left no manifest) enqueues a fresh attempt.
            self.results.pop(job_key, None)
            if job_key not in self.pending:
                self.jobs[job_key] = {
                    "table": table,
                    "options": options,
                    "pipeline_scope": pipeline_scope,
                    "allow_keyless": allow_keyless,
                }
                self.queue.append(job_key)
                self.pending.add(job_key)
                self.condition.notify_all()

    def wait(self, job_key, timeout):
        deadline = time.monotonic() + timeout
        with self.condition:
            while job_key in self.pending:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self.condition.wait(timeout=min(remaining, _SNAPSHOT_DRAIN_IDLE_SECONDS))
            result = self.results.get(job_key)
        if isinstance(result, BaseException):
            raise result
        return True


# Serialization-safe registry (a bare module global becomes a function local in the
# merged deployment; a mutated dict survives, as with the shared-CDC registry).
_SNAPSHOT_DRAIN_POOLS: dict[tuple, _SnapshotDrainPool] = {}
_SNAPSHOT_DRAIN_THREADS: dict[tuple, list] = {}
_SNAPSHOT_DRAIN_REGISTRY_LOCK: dict[str, threading.Lock] = {}


def _snapshot_drain_registry_lock() -> threading.Lock:
    lock = _SNAPSHOT_DRAIN_REGISTRY_LOCK.get("lock")
    if lock is None:
        lock = _SNAPSHOT_DRAIN_REGISTRY_LOCK.setdefault("lock", threading.Lock())
    return lock


def _snapshot_drain_pool(key, *, reader_factory, thread_count) -> _SnapshotDrainPool:
    with _snapshot_drain_registry_lock():
        pool = _SNAPSHOT_DRAIN_POOLS.get(key)
        if pool is None:
            pool = _SNAPSHOT_DRAIN_POOLS.setdefault(key, _SnapshotDrainPool())
        pool.configure(reader_factory=reader_factory, thread_count=thread_count)
        threads = _SNAPSHOT_DRAIN_THREADS.setdefault(key, [])
        threads[:] = [thread for thread in threads if thread.is_alive()]
        index = len(threads)
        while len(threads) < thread_count:
            thread = threading.Thread(
                target=_run_snapshot_drain_worker,
                args=(pool,),
                daemon=True,
                name=f"informix-snapshot-drain-{key[-1]}-{index}",
            )
            threads.append(thread)
            index += 1
            thread.start()
        return pool


def _close_snapshot_drain_reader(reader) -> None:
    if reader is None:
        return
    try:
        reader.close()
    except Exception:  # noqa: BLE001 - best-effort slot release
        logging.getLogger(__name__).debug("snapshot drain reader close failed", exc_info=True)


def _run_snapshot_drain_worker(pool: _SnapshotDrainPool) -> None:
    """Daemon loop: pull one drain job, run the inline snapshot drain under a private
    reader (staging pages and publishing the manifest durably), then close the reader to
    release its slot. A failure is recorded on the job so the waiting consumer re-raises
    and its next microbatch re-subscribes -- the same recover-by-restart the inline drain
    already relies on."""

    logger = logging.getLogger(__name__)
    while True:
        with pool.condition:
            while not pool.queue:
                pool.condition.wait(timeout=_SNAPSHOT_DRAIN_IDLE_SECONDS)
            job_key = pool.queue.popleft()
            job = pool.jobs.get(job_key)
            factory = pool.reader_factory
        result: Any = None
        reader = None
        if job is not None and factory is not None:
            try:
                reader = factory()
                # shared mode is off on the copy, so this drains inline and returns the
                # staged page-0 result; the daemon ignores the rows -- it only needs the
                # durable manifest the drain publishes as a side effect.
                reader._read_snapshot(
                    job["table"],
                    None,
                    job["options"],
                    pipeline_scope_override=job["pipeline_scope"],
                    allow_keyless=job["allow_keyless"],
                )
            except Exception as error:  # noqa: BLE001 - surfaced to the waiting consumer
                logger.warning(
                    "Informix snapshot drain job failed; the consumer will re-raise and "
                    "retry on its next microbatch",
                    exc_info=True,
                )
                result = error
            finally:
                _close_snapshot_drain_reader(reader)
        with pool.condition:
            pool.jobs.pop(job_key, None)
            pool.pending.discard(job_key)
            pool.results[job_key] = result
            pool.condition.notify_all()


class InformixLakeflowConnect(LakeflowConnect):
    """Pure-Python connector live-validated on disposable Informix 15.

    Normal auth, queries, discovery, snapshots, transactional CDC, and
    serverless Lakeflow pipeline execution have been exercised.
    """

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        # The Volume now stores exactly one thing: snapshot page payloads. It has
        # no default to fall back on, because there is no longer a shared state
        # location to borrow one from.
        staging_location = options.get("snapshot.staging.location", "").strip()
        if not staging_location:
            raise ValueError("Missing required Informix option: snapshot.staging.location")
        # Checked here rather than where state is first read: this runs while the
        # pipeline is being set up, so a missing password fails the update
        # immediately instead of surfacing on the first microbatch that needs state.
        if not options.get("lakebase.password", "").strip():
            raise ValueError("Missing required Informix option: lakebase.password")
        self._snapshot_staging_location = _validated_volume_location(
            staging_location, "snapshot.staging.location", options
        )
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
        # Validate numeric configuration without opening a connection.
        for name, default, minimum in (
            ("snapshot.page.size", str(_DEFAULT_SNAPSHOT_PAGE_SIZE), 1),
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
            ("metadata.max.bytes", str(64 << 20), 0),
            ("max.records.per.batch", str(_DEFAULT_MAX_RECORDS_PER_BATCH), 1),
            ("cdc.timeout", "5", 1),
            ("cdc.max.records", str(_DEFAULT_CDC_MAX_RECORDS), 1),
            ("cdc.max.frame.bytes", str(16 << 20), 16),
            ("cdc.max.transaction.records", "100000", 1),
            ("cdc.max.poll.records", "200000", 1),
            ("cdc.max.poll.bytes", "0", 0),
            ("cdc.read.bytes", "32000", 1),
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
        ):
            if int(options.get(name, default)) < minimum:
                raise ValueError(f"Option '{name}' must be >= {minimum}")
        # Shared-reader thread count defaults to the connection budget, so it is
        # validated here rather than in the static-default loop above.
        if _SHARED_CDC_THREADS_OPTION in options and int(options[_SHARED_CDC_THREADS_OPTION]) < 1:
            raise ValueError(f"Option '{_SHARED_CDC_THREADS_OPTION}' must be >= 1")
        if (
            _SNAPSHOT_READER_THREADS_OPTION in options
            and int(options[_SNAPSHOT_READER_THREADS_OPTION]) < 1
        ):
            raise ValueError(f"Option '{_SNAPSHOT_READER_THREADS_OPTION}' must be >= 1")
        # Validate boolean capacity options eagerly (they are otherwise only
        # checked on first read, deep inside a running flow).
        _option_bool(options, "snapshot.incremental.blocking", True)
        _option_bool(options, _SHARED_CDC_SESSION_OPTION, True)
        _option_bool(options, _SNAPSHOT_SHARED_SESSION_OPTION, True)
        if _APPEND_INGESTION_OPTION in options:
            _append_only_value(options[_APPEND_INGESTION_OPTION])
        if int(options.get("cdc.max.records", str(_DEFAULT_CDC_MAX_RECORDS))) > 256:
            raise ValueError("Option 'cdc.max.records' must be <= 256")
        if int(options.get("cdc.read.bytes", "32000")) > 32767:
            raise ValueError("Option 'cdc.read.bytes' must be <= 32767")
        if (
            int(
                options.get(
                    "max.concurrent.connections",
                    str(_DEFAULT_MAX_CONCURRENT_CONNECTIONS),
                )
            )
            > 9999
        ):
            raise ValueError("Option 'max.concurrent.connections' must be <= 9999")
        slot_count = int(
            options.get(
                "max.concurrent.connections",
                str(_DEFAULT_MAX_CONCURRENT_CONNECTIONS),
            )
        )
        reservation = int(
            options.get(
                "upsert.connection.reservation",
                str(_DEFAULT_UPSERT_CONNECTION_RESERVATION),
            )
        )
        # Reject rather than clamp: at slot_count the delete channel could never
        # claim a slot, starving it permanently instead of throttling it.
        if not 0 <= reservation < slot_count:
            raise ValueError(
                "Option 'upsert.connection.reservation' must be >= 0 and < "
                f"max.concurrent.connections ({slot_count})"
            )
        snapshot_reservation = int(
            options.get(
                _SNAPSHOT_RESERVATION_OPTION,
                str(_DEFAULT_SNAPSHOT_CONNECTION_RESERVATION),
            )
        )
        # Reject rather than clamp: at slot_count a snapshot drain could never claim a
        # slot, deadlocking the drain instead of merely deferring it behind streams.
        if not 0 <= snapshot_reservation < slot_count:
            raise ValueError(
                f"Option '{_SNAPSHOT_RESERVATION_OPTION}' must be >= 0 and < "
                f"max.concurrent.connections ({slot_count})"
            )
        daemon_reservation = int(
            options.get(
                _DAEMON_RESERVATION_OPTION,
                str(_DEFAULT_DAEMON_CONNECTION_RESERVATION),
            )
        )
        # Reject rather than clamp: at slot_count a daemon could never claim a slot,
        # stranding the sharded CDC session and the snapshot drain permanently.
        if not 0 <= daemon_reservation < slot_count:
            raise ValueError(
                f"Option '{_DAEMON_RESERVATION_OPTION}' must be >= 0 and < "
                f"max.concurrent.connections ({slot_count})"
            )
        port = int(options.get("port", "9088"))
        if not 1 <= port <= 65535:
            raise ValueError("Option 'port' must be between 1 and 65535")
        login_timeout = float(options.get("authentication.login.timeout", "30"))
        if not math.isfinite(login_timeout) or login_timeout <= 0:
            raise ValueError("Option 'authentication.login.timeout' must be > 0")
        capacity_delay = float(
            options.get(
                "capacity.retry.max.delay.seconds",
                str(_DEFAULT_CAPACITY_RETRY_MAX_DELAY_SECONDS),
            )
        )
        if not math.isfinite(capacity_delay) or capacity_delay < 0:
            raise ValueError("Option 'capacity.retry.max.delay.seconds' must be >= 0")
        self._bridge_instance: InformixBridge | None = None
        self._tables: dict[str, Table] | None = None
        self._tables_complete = False
        self._snapshot_high_water: dict[tuple[str, str], int] = {}
        self._snapshot_schema_ids: dict[tuple[str, str], str] = {}
        self._trigger_available_now = False
        self._trigger_boundaries: dict[str, tuple[int, str, str]] = {}
        self._activity_touched: dict[tuple[str, str], float] = {}
        self._cleaned_update_scopes: set[tuple[str, str]] = set()
        self._cleaned_snapshot_stages: set[str] = set()
        self._registration_scope: str | None = _INFORMIX_REGISTRATION_CONTEXT["scope"]
        self._metadata_session_lock = threading.RLock()
        self._metadata_release_timer: threading.Timer | None = None
        self._metadata_session_generation = 0

    def set_registration_scope(self, scope: str) -> None:
        """Install the scope shared by every reader serialized from one registration."""

        if not isinstance(scope, str) or _PIPELINE_SCOPE.fullmatch(scope) is None:
            raise ValueError("Informix registration scope has an invalid identity")
        if self._registration_scope != scope:
            self._snapshot_high_water.clear()
            self._snapshot_schema_ids.clear()
            self._trigger_boundaries.clear()
            self._activity_touched.clear()
            self._cleaned_update_scopes.clear()
            self._cleaned_snapshot_stages.clear()
            self._trigger_available_now = False
        self._registration_scope = scope

    def _pipeline_scope(self, checkpoint: dict[str, Any] | None = None) -> str:
        scope = self._registration_scope
        if scope is None and checkpoint:
            scope = checkpoint.get("pipeline_scope")
        if not isinstance(scope, str) or _PIPELINE_SCOPE.fullmatch(scope) is None:
            raise InformixError("Informix reader has no registration or checkpoint scope")
        return scope

    def prepare_for_trigger_available_now(self) -> None:
        """Freeze stream high-water marks when Spark selects AvailableNow."""

        self._trigger_available_now = True
        self._trigger_boundaries.clear()

    def close(self) -> None:
        """Close the live SQLI transport, if one was opened."""

        with self._metadata_session_lock:
            self._metadata_session_generation += 1
            timer = self._metadata_release_timer
            self._metadata_release_timer = None
            if timer is not None:
                timer.cancel()
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
                # connector-managed connection-capacity slot.
                self._bridge_instance = None

    def __enter__(self) -> "InformixLakeflowConnect":
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
        """Exclude live SQLI state when Spark serializes the data source.

        Schema and metadata discovery run before Spark ships the reader to a
        Python worker, so ``_bridge_instance`` can contain a socket, buffered
        streams, and a thread lock.  None of those objects is transferable or
        valid in another process.  The worker reconstructs a fresh bridge from
        the immutable connection options on its first source operation.
        """

        self.close()
        state = self.__dict__.copy()
        state["_bridge_instance"] = None
        state["_metadata_session_lock"] = None
        state["_metadata_release_timer"] = None
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._metadata_session_lock = threading.RLock()
        self._metadata_release_timer = None

    def _begin_metadata_session(self) -> None:
        self._metadata_session_generation += 1
        timer = self._metadata_release_timer
        self._metadata_release_timer = None
        if timer is not None:
            timer.cancel()

    def _defer_metadata_session_release(self) -> None:
        generation = self._metadata_session_generation
        timer = threading.Timer(
            _METADATA_SESSION_IDLE_SECONDS,
            self._finish_metadata_session,
            args=(generation,),
        )
        timer.daemon = True
        self._metadata_release_timer = timer
        timer.start()

    def _finish_metadata_session(self, generation: int) -> None:
        with self._metadata_session_lock:
            if generation != self._metadata_session_generation:
                return
            self._metadata_release_timer = None
            self._release_worker_connection()

    @property
    def _bridge(self) -> InformixBridge:
        if self._bridge_instance is None:
            factory_path = self.options.get("bridge.factory")
            factory = _load_factory(factory_path) if factory_path else _bridge_factory
            self._bridge_instance = factory(self.options)
        return self._bridge_instance

    def list_tables(self) -> list[str]:
        previous_capacity_mode = self.options.get("_informix.bypass.connection.capacity")
        self.options["_informix.bypass.connection.capacity"] = "true"
        try:
            return sorted(self._table_map())
        finally:
            try:
                self._release_worker_connection()
            finally:
                if previous_capacity_mode is None:
                    self.options.pop("_informix.bypass.connection.capacity", None)
                else:
                    self.options["_informix.bypass.connection.capacity"] = previous_capacity_mode

    def get_table_schema(self, table_name: str, table_options: dict[str, str]) -> StructType:
        previous_capacity_mode = self.options.get("_informix.bypass.connection.capacity")
        self.options["_informix.bypass.connection.capacity"] = "true"
        try:
            table = self._registration_table(table_name, table_options)
            _ensure_materializable(table, table_options)
            fields = [
                StructField(
                    column.name,
                    _spark_type(column, table_options),
                    column.nullable,
                )
                for column in table.columns
            ]
            fields.extend(
                (
                    StructField(CURSOR, StringType(), False),
                    StructField(COMMIT_LSN, StringType(), False),
                    StructField(TX_ID, LongType(), True),
                    StructField(OP, StringType(), False),
                )
            )
            return StructType(fields)
        finally:
            try:
                self._release_worker_connection()
            finally:
                if previous_capacity_mode is None:
                    self.options.pop("_informix.bypass.connection.capacity", None)
                else:
                    self.options["_informix.bypass.connection.capacity"] = previous_capacity_mode

    def read_table_metadata(self, table_name: str, table_options: dict[str, str]) -> dict:
        with self._metadata_session_lock:
            self._begin_metadata_session()
            previous_capacity_mode = self.options.get("_informix.bypass.connection.capacity")
            self.options["_informix.bypass.connection.capacity"] = "true"
            try:
                table = self._registration_table(table_name, table_options)
                _ensure_materializable(table, table_options)
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
                    result = {
                        "primary_keys": list(table.primary_keys),
                        "cursor_field": CURSOR,
                        "ingestion_type": "cdc_with_deletes",
                    }
            except BaseException:
                self._release_worker_connection()
                raise
            finally:
                if previous_capacity_mode is None:
                    self.options.pop("_informix.bypass.connection.capacity", None)
                else:
                    self.options["_informix.bypass.connection.capacity"] = previous_capacity_mode
            self._release_worker_connection()
            return result

    def _registration_table(self, table_name: str, table_options: dict[str, str]) -> Table:
        """Resolve one configured table with a targeted catalog lookup.

        The framework asks for one specific table at a time (the pipeline spec's
        exact ``owner.name``), so query just that table rather than scanning and
        parsing the whole catalog. This keeps an unrelated unsupported table from
        breaking registration for tables that are configured, and avoids reading
        a large catalog wholesale. Resolved tables are cached per table in the
        cross-instance coordinator so repeated registrations reuse them.
        """

        exposed = table_options.get("qualified_source_table", table_name)
        key = self._registration_table_cache_key()
        if key is None:
            return self._table(table_name, table_options, refresh=True)
        coordinator = _STATE_VALIDATION_COORDINATOR
        cache_key = (*key, exposed)
        with coordinator.condition:
            table = coordinator.table_caches.get(cache_key)
            if table is None:
                table = self._resolve_registration_table(exposed)
                coordinator.table_caches[cache_key] = table
                while len(coordinator.table_caches) > _MAX_REGISTRATION_TABLE_CACHES:
                    del coordinator.table_caches[next(iter(coordinator.table_caches))]
            if self._tables is None:
                self._tables = {}
            self._tables[exposed] = table
            return self._apply_primary_key_override(table, table_options)

    def _resolve_registration_table(self, exposed: str) -> Table:
        """Resolve a single configured table by exposed ``owner.name`` via a
        targeted catalog query, applying the same eligibility and selection
        filters ``_table_map`` would. Only this one table is read and parsed, so
        an unrelated unsupported table elsewhere in the catalog cannot fail it.
        """

        parts = _split_identity(exposed)
        if len(parts) != 2 or not all(_IDENTIFIER.fullmatch(part) for part in parts):
            raise ValueError(f"Unknown or excluded Informix table '{exposed}'")
        database = self.options.get("database", "")
        table = Table.parse(
            self._bridge.get_table(_join_identity(database, parts[0], parts[1])),
            database,
        )
        if not _eligible(table) or not self._selected(table):
            raise ValueError(f"Unknown or excluded Informix table '{exposed}'")
        return table

    def _registration_table_cache_key(self) -> tuple[str, ...] | None:
        scope = self._registration_scope
        hostname = self.options.get("hostname")
        server = self.options.get("server")
        if scope is None or not hostname or not server:
            # Injected test bridges and incomplete discovery configurations do
            # not have a stable source identity and must remain instance-local.
            return None
        return (
            scope,
            hostname.strip().rstrip(".").casefold(),
            self.options.get("port", "9088"),
            server.strip().casefold(),
            self.options.get("database", "").strip().casefold(),
            self.options.get("table.include.list", self.options.get("tables", "")),
            self.options.get("table.exclude.list", ""),
        )

    def read_table(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        effective_start = self._effective_start_offset(start_offset)
        snapshot_mode = self._snapshot_mode(table_options)
        self._publish_checkpoint_channel_start_before_capacity(
            table_name, effective_start, table_options
        )
        replaying = self._replay_stop_lsn(table_options) is not None
        replay_wait_budget = self._replay_connection_wait_budget(table_options)
        # Model A only, and only for the monolithic full-table drain -- the `initial` /
        # `initial_only` read that holds one connection for the whole scan. That is the
        # sole reader that can starve streaming readers, so only it takes the reservation
        # floor. Everything else releases its slot after each microbatch: a keyed table's
        # default `incremental` / `auto_snapshot` snapshot reads a bounded chunk per
        # microbatch and frees the slot between them, and stream/CDC reads are one bounded
        # poll each -- none of them hold long enough to starve anyone, so they may freely
        # borrow the reserved slots (floor 0). In shared mode (Model C) the daemon bounds
        # drain concurrency instead, so no read takes this floor.
        reserve_snapshot_slot = (
            not self._snapshot_shared_enabled()
            and snapshot_mode in ("initial", "initial_only")
            and effective_start.get("phase") != "stream"
        )
        with contextlib.ExitStack() as stack:
            stack.enter_context(
                self._capacity_attempt(
                    start_offset,
                    replaying=replaying,
                    replay_wait_budget=replay_wait_budget,
                    reserve_snapshot_slot=reserve_snapshot_slot,
                )
            )
            return self._read_table_attempt(
                table_name, start_offset, effective_start, table_options, snapshot_mode
            )

    def _read_table_attempt(
        self,
        table_name: str,
        start_offset: dict,
        effective_start: dict,
        table_options: dict[str, str],
        snapshot_mode: str,
        attempt_index: int = 0,
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
            )

        try:
            try:
                table = self._table(table_name, table_options)
                self._cleanup_previous_update_scopes(table, effective_start)
                self._touch_table_state(table)
                _ensure_materializable(table, table_options)
                if (
                    _cdc_capable(table)
                    and effective_start.get("phase") == "stream"
                    and effective_start.get("schema_fingerprint") is not None
                ):
                    checkpoint = _validated_offset(effective_start)
                    self._publish_upsert_channel_start(
                        table,
                        int(checkpoint.get("begin_lsn") or checkpoint["commit_lsn"]),
                        str(checkpoint["schema_id"]),
                        str(checkpoint["schema_fingerprint"]),
                        self._pipeline_scope(),
                    )
                if self._append_only_table(table, table_options):
                    # Append-only has exactly two states: establish the boundary once,
                    # then stream. _read_stream needs no primary key, so a key-less
                    # table follows the identical CDC path from here on.
                    if effective_start and effective_start.get("phase") == "stream":
                        result = self._read_stream(
                            table, effective_start, table_options, deletes=False
                        )
                    else:
                        result = self._read_append_only(table, effective_start, table_options)
                elif not _cdc_capable(table):
                    if snapshot_mode in {"cdc_only", "recovery"}:
                        raise ValueError(
                            f"snapshot.mode={snapshot_mode} requires a CDC-capable table; "
                            f"'{table.exposed_name}' is snapshot-only"
                        )
                    result = self._read_snapshot_only(table, effective_start, table_options)
                elif snapshot_mode == "recovery" and (
                    not effective_start or effective_start.get("phase") != "stream"
                ):
                    raise InformixError(
                        "snapshot.mode=recovery requires an existing stream checkpoint; "
                        "use snapshot.mode=initial for a new or snapshot-phase table"
                    )
                elif snapshot_mode == "recovery":
                    checkpoint = _validated_offset(effective_start)
                    current_table = self._refresh_table_schema(table, None)
                    if checkpoint.get("schema_fingerprint") != _schema_fingerprint(current_table):
                        raise InformixError(
                            f"snapshot.mode=recovery cannot rebuild schema history for "
                            f"'{table.exposed_name}' because the source schema differs from "
                            "the checkpoint; run a full refresh"
                        )
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
                    result = self._read_stream(
                        current_table, checkpoint, table_options, deletes=False
                    )
                elif snapshot_mode == "initial_only" and effective_start.get("phase") == "stream":
                    self._cleanup_completed_snapshot_stage(table, effective_start)
                    result = (iter(()), dict(effective_start))
                elif snapshot_mode == "cdc_only" and not effective_start:
                    result = self._read_cdc_only(table, table_options)
                elif (
                    snapshot_mode == "auto_snapshot"
                    and effective_start.get("phase") == "stream"
                    and self._checkpoint_requires_snapshot(effective_start)
                ):
                    result = self._read_incremental(
                        table,
                        None,
                        table_options,
                        pipeline_scope_override=self._resnapshot_scope(table, effective_start),
                    )
                elif snapshot_mode in {"incremental", "auto_snapshot"} and not effective_start:
                    result = self._read_incremental(table, None, table_options)
                elif (
                    snapshot_mode in {"incremental", "auto_snapshot"}
                    and "incremental" in effective_start
                ):
                    result = self._read_incremental(table, effective_start, table_options)
                elif not effective_start or effective_start.get("phase", "snapshot") == "snapshot":
                    result = self._read_snapshot(table, effective_start, table_options)
                else:
                    self._cleanup_completed_snapshot_stage(table, effective_start)
                    result = self._read_stream(table, effective_start, table_options, deletes=False)
                return self._reset_retry_counts(result, start_offset)
            except SharedStateAccessUnavailable:
                retried = self._retry_transient_volume_read(
                    read_again, table_options, attempt_index
                )
                if retried is not None:
                    return self._retried_result(retried, start_offset)
                return iter(()), self._shared_state_retry_offset(start_offset)
            except OSError as error:
                # A dropped Volume mount can surface from any of the shared-state
                # helpers, not only the one that retries open(2), so classify it
                # here and reuse the transient-Volume retry. Any other OSError is
                # a real fault and must keep propagating.
                dropped = _dropped_mount_error(error)
                if dropped is None:
                    raise
                # Retry in place first: a remount settles in seconds, and yielding
                # is unavailable during a replay (see _retry_transient_volume_read).
                retried = self._retry_transient_volume_read(
                    read_again, table_options, attempt_index
                )
                if retried is not None:
                    return self._retried_result(retried, start_offset)
                logging.getLogger(__name__).warning(
                    "Informix snapshot staging Volume mount is not connected; yielding an "
                    "empty batch and retrying this checkpoint",
                    exc_info=True,
                )
                return iter(()), self._dropped_mount_retry_offset(start_offset, dropped)
            except ConnectionCapacityUnavailable:
                # Continuous flows are permanent, so capacity pressure is the
                # steady state rather than a fault: always yield and let the
                # framework call again, bounded by the consecutive-miss cap in
                # _capacity_retry_offset so a permanently starved flow still
                # fails loudly instead of silently replicating nothing. Only a
                # triggered flow raises here -- it drains and releases its slot,
                # so an exhausted wait is a genuine end-of-data failure and
                # yielding an empty batch would make AvailableNow stop early.
                if self._trigger_available_now:
                    raise
                if self._replay_stop_lsn(table_options) is not None:
                    raise
                return iter(()), self._capacity_retry_offset(start_offset)
        finally:
            self._release_worker_connection()

    def read_table_deletes(
        self, table_name: str, start_offset: dict, table_options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        effective_start = self._effective_start_offset(start_offset)
        snapshot_mode = self._snapshot_mode(table_options)
        # An uncheckpointed delete reader cannot do useful source work until its
        # paired upsert reader publishes the boundary it is actually using.  The
        # table-state key depends only on the configured native identity, so test
        # Lakebase before resolving Informix metadata: _table() opens Informix and
        # would otherwise let coordination waiters consume every scarce slot from
        # the upsert readers that must unblock them.
        if (
            not effective_start
            and snapshot_mode not in {"initial_only", "recovery"}
            and not self._upsert_channel_start_exists(table_name, table_options)
        ):
            return iter(()), self._schema_node_fallback_offset(start_offset)
        replaying = self._replay_stop_lsn(table_options) is not None
        replay_wait_budget = self._replay_connection_wait_budget(table_options)
        # Declare the channel before anything can connect. The slot is taken by
        # the first call that reaches _ensure_connected -- which is the schema
        # refresh, well before read_changes -- so setting this any later would
        # let the first acquisition land outside the delete channel's band.
        previous_channel = self.options.get(_CONNECTION_CHANNEL_OPTION)
        self.options[_CONNECTION_CHANNEL_OPTION] = "delete"
        try:
            with contextlib.ExitStack() as stack:
                stack.enter_context(
                    self._capacity_attempt(
                        start_offset,
                        replaying=replaying,
                        replay_wait_budget=replay_wait_budget,
                    )
                )
                return self._read_table_deletes_attempt(
                    table_name, start_offset, effective_start, table_options, snapshot_mode
                )
        finally:
            if previous_channel is None:
                self.options.pop(_CONNECTION_CHANNEL_OPTION, None)
            else:
                self.options[_CONNECTION_CHANNEL_OPTION] = previous_channel

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
                if snapshot_mode == "initial_only":
                    result = (iter(()), dict(effective_start))
                elif snapshot_mode == "recovery" and (
                    not effective_start or effective_start.get("phase") != "stream"
                ):
                    raise InformixError(
                        "snapshot.mode=recovery requires an existing stream checkpoint"
                    )
                else:
                    result = self._read_table_deletes(
                        table_name,
                        effective_start,
                        table_options,
                        snapshot_mode=snapshot_mode,
                        bootstrap_offset=start_offset,
                    )
                return self._reset_retry_counts(result, start_offset)
            except SharedStateAccessUnavailable:
                retried = self._retry_transient_volume_read(
                    read_again, table_options, attempt_index
                )
                if retried is not None:
                    return self._retried_result(retried, start_offset)
                return iter(()), self._shared_state_retry_offset(start_offset)
            except OSError as error:
                # A dropped Volume mount can surface from any of the shared-state
                # helpers, not only the one that retries open(2), so classify it
                # here and reuse the transient-Volume retry. Any other OSError is
                # a real fault and must keep propagating.
                dropped = _dropped_mount_error(error)
                if dropped is None:
                    raise
                retried = self._retry_transient_volume_read(
                    read_again, table_options, attempt_index
                )
                if retried is not None:
                    return self._retried_result(retried, start_offset)
                logging.getLogger(__name__).warning(
                    "Informix snapshot staging Volume mount is not connected; yielding an "
                    "empty batch and retrying this checkpoint",
                    exc_info=True,
                )
                return iter(()), self._dropped_mount_retry_offset(start_offset, dropped)
            except TriggerBoundaryUnavailable:
                if self._replay_stop_lsn(table_options) is not None:
                    raise
                return iter(()), self._trigger_boundary_retry_offset(start_offset)
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
            raise SharedStateAccessUnavailable(
                "Informix shared state exhausted its checkpoint retry limit"
            )
        time.sleep(random.uniform(0.05, min(0.1 * (2 ** min(retry_count, 5)), 1.5)))
        result["shared_state_retry_count"] = retry_count + 1
        return result

    def _dropped_mount_retry_offset(self, start_offset: dict, error: OSError) -> dict:
        """Yield an empty batch for a dropped Volume mount, bounded by a fail-loud cap.

        Kept separate from ``_shared_state_retry_offset`` because the two count
        different things at different cadences: that one bounds a fast in-process
        retry loop, this one bounds consecutive *microbatches*. Sharing a counter
        would either let a dropped mount stall for ~100 minutes (see
        ``_DROPPED_MOUNT_MAX_RETRIES``) or cut the in-process retries far too short.

        Yielding is still the right first response -- a remount settles in seconds
        and the checkpoint is untouched -- but it must not be the *only* response.
        A mount that never returns otherwise leaves every flow yielding empty
        batches with the pipeline reporting RUNNING and committing zero-row Delta
        versions on schedule, which is indistinguishable from an idle source. This
        was observed in production taking all 60 flows offline for 13 minutes with
        no ERROR or WARN anywhere in the event log; only the offset's retry count
        revealed it. Failing at the cap turns that into a visible flow failure.

        ``_reset_retry_counts`` drops the key on the next read that reaches the
        Volume, so the count only ever measures a current run of consecutive
        failures and a mount that flaps does not accumulate toward the cap.
        """

        result = dict(start_offset)
        retry_count = _dropped_mount_retry_count(result)
        if retry_count >= self._dropped_mount_max_retries():
            logging.getLogger(__name__).error(
                "Informix snapshot staging Volume mount was not connected for %d "
                "consecutive reads; this flow yielded empty batches throughout and "
                "replicated nothing. The Volume's FUSE mount is down rather than "
                "the source being idle: check the Volume's availability.",
                retry_count,
            )
            raise InformixError(
                f"Informix snapshot staging Volume mount has not been connected for "
                f"{retry_count} consecutive reads; the staging Volume is unavailable"
            ) from error
        result["dropped_mount_retry_count"] = retry_count + 1
        return result

    def _dropped_mount_max_retries(self) -> int:
        """Consecutive dropped-mount yields tolerated before the flow fails.

        Configurable because the right bound depends on the microbatch interval:
        the cap counts reads, not seconds, so a pipeline with a long trigger
        interval covers the same wall-clock outage in fewer retries. Defaults to
        ``_DROPPED_MOUNT_MAX_RETRIES``.
        """

        value = int(
            self.options.get(
                "dropped.mount.max.retries",
                str(_DROPPED_MOUNT_MAX_RETRIES),
            )
        )
        if value < 1:
            raise ValueError("Option 'dropped.mount.max.retries' must be >= 1")
        return value

    def _capacity_retry_max_delay(self) -> float:
        """Maximum jittered delay before a continuous reader retries after a miss.

        A positive value is the inter-read backoff and also caps each acquisition
        attempt (a short, miss-scaled budget). ``0`` changes both for a continuous
        flow: the acquisition attempt blocks the full ``connection.wait.timeout.seconds``
        (maximum chance to claim a slot) and there is no inter-read delay before
        the next attempt. A continuous flow still yields rather than fails on
        exhaustion, bounded by ``capacity.retry.max.retries``. For a triggered
        flow the value is irrelevant: it always blocks fully and raises on
        exhaustion.
        """

        value = float(
            self.options.get(
                "capacity.retry.max.delay.seconds",
                str(_DEFAULT_CAPACITY_RETRY_MAX_DELAY_SECONDS),
            )
        )
        if not math.isfinite(value) or value < 0:
            raise ValueError("Option 'capacity.retry.max.delay.seconds' must be >= 0")
        return value

    @staticmethod
    def _offset_is_incremental_reader(start_offset: dict) -> bool:
        """Report whether this read is an in-progress incremental snapshot.

        The incremental strategy interleaves primary-key snapshot chunks with the
        live CDC stream, so its offset carries ``phase == "stream"`` *and* an
        ``incremental`` block while the copy is still in flight; the block is
        dropped once the snapshot completes and the reader becomes pure CDC. This
        window is the incremental *reader* phase, distinct from steady-state
        stream even though both report ``phase == "stream"``.
        """

        return bool(start_offset) and "incremental" in start_offset

    def _offset_is_stream_phase(self, start_offset: dict) -> bool:
        """Report whether this read is steady-state CDC for capacity purposes.

        A first read has no checkpoint and begins with a snapshot; the blocking
        ``snapshot`` phase carries ``phase != "stream"``. Only ``phase ==
        "stream"`` is CDC. The incremental *reader* phase also reports ``phase ==
        "stream"`` but is a bulk copy interleaved with CDC (see
        :meth:`_offset_is_incremental_reader`); whether it counts as stream for
        capacity is governed by ``snapshot.incremental.blocking`` -- when that is
        true (the default) it is treated as non-stream so it blocks for a slot
        like the consistent snapshot, and when false it is treated as stream and
        uses the continuous yield budget. The capacity retry offset preserves both
        ``phase`` and the ``incremental`` block across misses, so this stays
        correct while a starved reader retries.
        """

        if not start_offset or start_offset.get("phase") != "stream":
            return False
        if self._offset_is_incremental_reader(start_offset):
            return not self._incremental_snapshot_blocking()
        return True

    def _incremental_snapshot_blocking(self) -> bool:
        """Whether the incremental snapshot reader phase blocks for a slot.

        When true (the default), an in-progress incremental snapshot read blocks
        the full ``connection.wait.timeout.seconds`` to patiently claim a slot,
        like the consistent snapshot -- bulk copying is latency-tolerant and
        benefits from pressing for a slot rather than yielding empty. When false,
        it uses the continuous miss-scaled yield budget and ``capacity.retry.max.delay.seconds``
        inter-read backoff instead, freeing the worker between attempts at the
        cost of a slower copy under contention. A triggered flow always blocks
        regardless of this option.
        """

        return _option_bool(self.options, "snapshot.incremental.blocking", True)

    def _effective_capacity_retry_delay(self, start_offset: dict) -> float:
        """Per-read capacity retry delay, treating non-stream reads as ``0``.

        Snapshot reads (and, by default, the incremental snapshot reader phase)
        are bulk, latency-tolerant work, so they should patiently wait out
        contention for a slot: forcing the delay to ``0`` there makes each attempt
        block the full ``connection.wait.timeout.seconds`` with no inter-read
        backoff. Only the steady-state ``stream`` phase uses the configured
        ``capacity.retry.max.delay.seconds``, where prompt yield-and-retry is the
        right backpressure. Whether the incremental reader phase counts as stream
        here is governed by ``snapshot.incremental.blocking`` (see
        :meth:`_offset_is_stream_phase`). The phase override only applies to a
        positive configured delay; a configured ``0`` already means "block fully
        in every phase", so there is nothing to override.
        """

        configured = self._capacity_retry_max_delay()
        if configured and not self._offset_is_stream_phase(start_offset):
            return 0.0
        return configured

    def _retried_result(self, result: tuple[Iterator[dict], dict], start_offset: dict):
        """Return a retried read's result without disturbing its retry bookkeeping.

        A nested attempt may itself have given up and yielded, in which case its
        offset carries the advisory counter that bounds the retry. Passing that
        through ``_reset_retry_counts`` -- which exists to clear the counters after a
        read that actually reached the source -- strips it, so the count oscillates
        instead of climbing and the fail-loud caps can never be reached. Observed
        directly while building this: successive reads reported 1, absent, 1, absent.
        """

        _, end_offset = result
        if _OFFSET_ADVISORY_FIELDS & set(end_offset):
            return result
        return self._reset_retry_counts(result, start_offset)

    def _retry_transient_volume_read(
        self, attempt: Callable[[], Any], options: dict[str, str], attempt_index: int
    ):
        """Re-run a read whose shared-state access failed transiently.

        Called from the handlers that would otherwise answer a dropped mount by
        yielding an empty batch with an unchanged position. For a normal read that
        is correct and cheap -- the framework calls again and nothing is lost. During
        a replay it is silently destructive: the caller has already committed the
        range's end offset and reads an empty batch as proof the range held nothing,
        so the rows vanish. Six tables lost 151 rows this way.

        Retrying in place removes the dilemma whenever the mount returns, which in
        production it did within seconds. If it never returns the two cases genuinely
        differ, so they diverge: a normal read yields (this returns None and the
        caller falls back), while a replay raises, because failing a batch Spark will
        retry beats under-delivering one it will not.

        Performs one further attempt per call. The caller re-enters through the read
        path, which calls back here on a repeat failure, so ``attempt_index`` bounds
        the chain; returning None at the bound hands the decision back.
        """

        if attempt_index + 1 >= _TRANSIENT_VOLUME_IN_PROCESS_ATTEMPTS:
            logging.getLogger(__name__).warning(
                "Informix shared state stayed inaccessible across %d in-process attempts",
                _TRANSIENT_VOLUME_IN_PROCESS_ATTEMPTS,
            )
            if self._replay_stop_lsn(options) is not None:
                raise InformixError(
                    "Informix shared state is inaccessible while replaying a committed "
                    "offset range; failing this batch rather than returning fewer rows "
                    "than the range holds"
                )
            return None
        delay = min(0.25 * (2**attempt_index), _TRANSIENT_VOLUME_IN_PROCESS_MAX_SECONDS)
        time.sleep(random.uniform(delay / 2, delay))
        return attempt()

    @staticmethod
    def _replay_stop_lsn(options: dict[str, str]) -> int | None:
        """The LSN a replayed read must stop at, or None for an ordinary read.

        Validated rather than trusted: the value reaches the read through the
        options dict, and a malformed one would silently become an unbounded read --
        exactly the behaviour being fixed.
        """

        raw = options.get(_REPLAY_STOP_LSN_OPTION)
        if raw is None:
            return None
        try:
            stop = int(raw)
        except (TypeError, ValueError) as error:
            raise InformixError(
                f"Option '{_REPLAY_STOP_LSN_OPTION}' must be an integer LSN, got {raw!r}"
            ) from error
        if stop < 0:
            raise InformixError(f"Option '{_REPLAY_STOP_LSN_OPTION}' must not be negative")
        return stop

    @staticmethod
    def _replay_connection_wait_budget(options: dict[str, str]) -> float | None:
        raw = options.get(_REPLAY_CONNECTION_WAIT_BUDGET_OPTION)
        if raw is None:
            return None
        try:
            budget = float(raw)
        except (TypeError, ValueError) as error:
            raise InformixError(
                f"Option '{_REPLAY_CONNECTION_WAIT_BUDGET_OPTION}' must be a numeric "
                f"duration, got {raw!r}"
            ) from error
        if not math.isfinite(budget) or budget < 0:
            raise InformixError(
                f"Option '{_REPLAY_CONNECTION_WAIT_BUDGET_OPTION}' must be a finite "
                "non-negative duration"
            )
        return budget

    def _capacity_retry_max_retries(self) -> int:
        """Consecutive capacity misses a continuous flow tolerates before failing.

        A continuous reader that never gets a slot yields empty batches, which is
        indistinguishable from an idle source. This bound makes a permanently
        undersized endpoint fail loudly instead of replicating nothing forever;
        acquiring a slot resets the count, so it only ever measures a current run
        of misses. Defaults to ``_CAPACITY_RETRY_MAX_RETRIES``.
        """

        value = int(
            self.options.get(
                "capacity.retry.max.retries",
                str(_CAPACITY_RETRY_MAX_RETRIES),
            )
        )
        if value < 1:
            raise ValueError("Option 'capacity.retry.max.retries' must be >= 1")
        return value

    def _capacity_attempt_budget(
        self,
        start_offset: dict,
        *,
        replaying: bool = False,
        replay_wait_budget: float | None = None,
    ) -> float | None:
        """Seconds this read may spend acquiring a slot, or None to block fully.

        The bounded budget is a continuous-mode concept: a continuous flow is a
        permanent standing consumer, so instead of pinning a worker for the whole
        ``connection.wait.timeout.seconds`` it spends a short budget, yields an
        empty batch, and is called again. A triggered flow is the opposite -- it
        drains and terminates, so it must block for the full wait to actually get
        a slot, and only fail if that genuinely exhausts. It therefore never uses
        the yield budget, regardless of ``capacity.retry.max.delay.seconds``
        (which governs only the continuous yield/backoff and is irrelevant to a
        triggered read). Returning a short budget to a triggered flow makes it
        give up in under a second under contention and fail the update, which is
        the bug this guard prevents.

        A replay also blocks for the full wait. Spark has already committed the
        range's end and cannot accept an empty capacity-yield in its place, so a
        short attempt only creates retry churn and increases the chance of repeated
        failure under the same contention that caused the first miss.

        A continuous flow blocks fully whenever its *effective* delay is ``0``
        (see :meth:`_effective_capacity_retry_delay`): either the configured
        ``capacity.retry.max.delay.seconds`` is ``0``, or this read is a
        non-stream (snapshot/incremental) read, which is treated as ``0`` so bulk
        copying patiently waits out contention for a slot. Blocking fully gives
        the read the maximum chance to claim a slot before giving up; it still
        yields rather than fails when the wait exhausts (only a triggered flow
        raises), bounded by ``capacity.retry.max.retries`` consecutive misses.
        The guard is an ``or`` (triggered is *always* full-block, plus any read
        whose effective delay is zero); the earlier bug used ``and`` and so let a
        triggered flow with a positive delay fall through to the short budget.

        Otherwise (a continuous stream read with a positive delay) the budget
        scales with ``capacity_pressure`` -- an acquisition-pressure signal that
        rises on a miss and *decays* rather than resets on a served read -- so a
        reader that has been contended presses harder than one that just arrived,
        and a reader that is intermittently served keeps some of that earned
        patience instead of dropping straight back to the least-patient budget on
        every success. (This is deliberately distinct from ``capacity_retry_count``,
        which counts strictly consecutive misses and resets on success because it
        is the failure guard.) It is jittered so equally-aged readers do not
        converge on one give-up instant and re-storm the Volume together on the
        next round.
        """

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
        reserve_snapshot_slot: bool = False,
    ):
        """Publish this read's acquisition budget and rank, then restore both.

        The rank is published even when there is no budget. A blocking reader --
        a triggered flow, or the latency-tolerant snapshot/incremental copy -- has
        no budget by design, and it is exactly the reader that carries a proven
        backlog while bulk-copying, so gating the rank on the budget left those
        readers unprioritised (observed in production: ranks only ever appeared on
        steady-state CDC offsets, streaks only on blocking copy offsets).
        """

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
        previous = self.options.get(_CONNECTION_ATTEMPT_BUDGET_OPTION)
        previous_rank = self.options.get(_CONNECTION_ATTEMPT_RANK_OPTION)
        previous_drain = self.options.get(_SNAPSHOT_DRAIN_MARKER_OPTION)
        if budget is not None:
            self.options[_CONNECTION_ATTEMPT_BUDGET_OPTION] = repr(budget)
        if rank > 0:
            self.options[_CONNECTION_ATTEMPT_RANK_OPTION] = str(rank)
        # Marks the lazy slot acquisition as a snapshot-phase read so it applies the
        # Model A reservation floor. Set only when the daemon pool is off (see caller).
        if reserve_snapshot_slot:
            self.options[_SNAPSHOT_DRAIN_MARKER_OPTION] = "true"
        try:
            yield
        finally:
            if previous is None:
                self.options.pop(_CONNECTION_ATTEMPT_BUDGET_OPTION, None)
            else:
                self.options[_CONNECTION_ATTEMPT_BUDGET_OPTION] = previous
            if previous_rank is None:
                self.options.pop(_CONNECTION_ATTEMPT_RANK_OPTION, None)
            else:
                self.options[_CONNECTION_ATTEMPT_RANK_OPTION] = previous_rank
            if previous_drain is None:
                self.options.pop(_SNAPSHOT_DRAIN_MARKER_OPTION, None)
            else:
                self.options[_SNAPSHOT_DRAIN_MARKER_OPTION] = previous_drain

    def _capacity_retry_offset(self, start_offset: dict) -> dict:
        """Yield an empty continuous batch when no connection slot is free.

        The returned offset must differ from ``start_offset``: the framework calls
        a reader repeatedly until the offset stops changing, so returning it
        unchanged would signal exhaustion and stop the stream. Bumping the retry
        count keeps the stream live, and ``_reset_retry_counts`` clears the key on
        the next read that actually acquires a slot, so a reader that gets served
        starts again from zero and the count only ever measures a current run of
        consecutive misses.

        Only the terminal case is logged: individual retries are the expected
        steady state, and logging each would bury the driver log.
        """

        result = dict(start_offset)
        retry_count = _capacity_retry_count(result)
        if retry_count >= self._capacity_retry_max_retries():
            logging.getLogger(__name__).error(
                "Informix connection capacity was unavailable for %d consecutive "
                "continuous reads; this flow yielded empty batches throughout and "
                "replicated nothing. Raise max.concurrent.connections, reduce the "
                "number of flows, or use a triggered pipeline.",
                retry_count,
            )
            raise ConnectionCapacityUnavailable(
                "Informix connection capacity exhausted its continuous retry limit; "
                "raise max.concurrent.connections or reduce the number of flows"
            )
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
        """Estimate how far this flow trails the source, without a connection.

        A waiting reader cannot call ``current_lsn()`` -- that needs the very slot
        it is waiting for -- so it subtracts its own checkpoint (which survives a
        capacity yield inside the offset) from the log position a slot *holder*
        published. Returns None whenever no trustworthy hint exists, so callers
        keep their unprioritised behaviour rather than ranking on a guess.

        The published position is the server's global log position, so this
        over-states the backlog of a quiet table: the log advances because *other*
        tables are written. It measures staleness rather than true per-table lag,
        which is why callers should compare it on a log scale rather than treat
        the difference as a row count.
        """

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
            logging.getLogger(__name__).debug(
                "Could not estimate Informix backlog from hint", exc_info=True
            )
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
        """Yield an empty batch while the upsert reader publishes the transition.

        The offset must differ from ``start_offset`` or the framework reads the
        unchanged offset as exhaustion and stops the stream, so bump a counter the
        way the sibling coordination retries do. Unlike the capacity retry this
        carries no failure cap: the wait is on the paired upsert reader for the
        *same* table, which either publishes (this reader proceeds) or fails its
        own flow with a real error. Capping here would convert a slow schema
        transition into a delete-channel failure while the upsert side is still
        healthy, and the whole point of yielding is that the slot is free for that
        publication to happen.
        """

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
            if "shared_state_retry_count" in start_offset:
                _shared_state_retry_count(start_offset)
            if "dropped_mount_retry_count" in start_offset:
                _dropped_mount_retry_count(start_offset)
            if "trigger_boundary_retry_count" in start_offset:
                _trigger_boundary_retry_count(start_offset)
            if "schema_transition_retry_count" in start_offset:
                _schema_transition_retry_count(start_offset)
            if "schema_node_fallback_retry_count" in start_offset:
                _schema_node_fallback_retry_count(start_offset)
            if "capacity_pressure" in start_offset:
                _capacity_pressure(start_offset)
            if "backlog_rank" in start_offset:
                _backlog_rank_value(start_offset)
            if "backlog_streak" in start_offset:
                _backlog_streak(start_offset)
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
        """Resolve ``append.only.ingestion`` to ``'true'``, ``'false'``, or ``'auto'``.

        ``table_options`` override the connection options; when the option is set
        nowhere the mode defaults to ``'auto'``.
        """

        for source in (table_options, self.options):
            if _APPEND_INGESTION_OPTION in source:
                return _append_only_value(source[_APPEND_INGESTION_OPTION])
        return "auto"

    def _append_only_table(self, table: Table, table_options: dict[str, str]) -> bool:
        """Report whether this read should use the append-only path.

        ``true`` forces append for any capturable table (a caller may want append
        semantics even for an insert-only keyed table); ``false`` forces the normal
        CDC/snapshot path; ``auto`` (the default) appends only keyless tables and
        leaves keyed tables on CDC. A table whose columns cannot be captured always
        falls back to snapshot, since append cannot represent rows it cannot read.
        """

        if not _cdc_streamable(table):
            return False
        mode = self._append_only_mode(table_options)
        if mode == "auto":
            return not table.primary_keys
        return mode == "true"

    def _apply_primary_key_override(self, table: Table, table_options: dict[str, str]) -> Table:
        """Override a table's primary key with the connector ``primary.keys`` option.

        When set (per table), treat the table as keyed on those columns -- even a
        physically keyless one -- so it is read as cdc_with_deletes with
        keyset-paginated snapshots and reported with those keys (which the pipeline
        also uses as the destination merge key). The caller MUST guarantee the
        columns are unique for the row: a non-unique override corrupts upserts,
        delete identification, and snapshot pagination.
        """

        raw = table_options.get(_PRIMARY_KEYS_OPTION)
        if raw is None:
            return table
        keys = _parse_key_columns(raw)
        if not keys:
            raise ValueError(f"Option '{_PRIMARY_KEYS_OPTION}' is set but lists no columns")
        if len(keys) != len(set(keys)):
            raise ValueError(f"Option '{_PRIMARY_KEYS_OPTION}' lists a duplicate column")
        column_names = {column.name for column in table.columns}
        missing = [key for key in keys if key not in column_names]
        if missing:
            raise InformixError(
                f"Option '{_PRIMARY_KEYS_OPTION}' names column(s) not present in "
                f"'{table.exposed_name}': {', '.join(missing)}"
            )
        if table.key_override and tuple(keys) == table.primary_keys:
            return table
        return replace(table, primary_keys=tuple(keys), key_override=True)

    def _snapshot_mode(self, table_options: dict[str, str]) -> str:
        mode = (
            str(
                table_options.get(
                    "snapshot.mode",
                    self.options.get("snapshot.mode", _DEFAULT_SNAPSHOT_MODE),
                )
            )
            .strip()
            .lower()
        )
        if mode in _UNSUPPORTED_SNAPSHOT_MODES:
            raise ValueError(
                f"snapshot.mode={mode} is an external extension mechanism and is not "
                "supported by the Python Informix connector"
            )
        if mode not in _SNAPSHOT_MODES:
            allowed = ", ".join(sorted(_SNAPSHOT_MODES))
            raise ValueError(f"Unsupported snapshot.mode={mode!r}; supported values are {allowed}")
        return mode

    @staticmethod
    def _schema_node_fallback_exhausted(start_offset: dict[str, Any]) -> bool:
        """Report whether the scope-independent schema-node fallback may be used yet.

        A delete reader with no offset at all is bootstrapping, which is exactly the
        state a **full refresh** produces: Lakeflow discards the checkpoint, so the
        first read of every flow arrives with ``start_offset`` empty. In that state the
        owning upsert reader is concurrently publishing this update's scoped
        ``initialization`` record, and the delete reader will normally find it within a
        few retries.

        The schema-node fallback exists for a different situation -- an upsert reader
        that *resumed* a checkpoint publishes no scoped record at all, so from the
        second update onward the delete channel would stall forever without it. But
        ``schema-nodes`` is keyed by table identity and schema fingerprint alone, so it
        is never scope-cleaned and its ``start_lsn`` can belong to a **previous log
        incarnation**: observed in production as a boundary at uniqid 22 while the
        source's reinitialized log had only reached uniqid 10. Taking that stale
        boundary during a refresh is how the delete channel ended up resuming at byte 0
        of the oldest retained log and decoding another table's row as its own.

        So the fallback is deferred rather than removed: prefer this update's fresh
        record for a bounded number of retries, and only consult the schema node once
        waiting has demonstrably not produced one. The scoped record is authoritative
        because it was published by this update against the current log; the schema node
        is a last resort whose age cannot be verified.
        """

        return _schema_node_fallback_retry_count(start_offset) >= _SCHEMA_NODE_FALLBACK_RETRIES

    @staticmethod
    def _schema_node_fallback_offset(start_offset: dict[str, Any]) -> dict[str, Any]:
        """Carry the bootstrap retry count so the fallback eventually becomes eligible.

        Advisory and non-positional, like the other retry counters: it rides an offset
        that has no position at all (bootstrap returns an empty offset), so it cannot
        move a checkpoint. Without it every retry would look like the first and the
        fallback would never be reached, permanently stalling the resumed-upsert case
        this defers rather than removes.
        """

        count = _schema_node_fallback_retry_count(start_offset)
        if count >= _SCHEMA_NODE_FALLBACK_RETRIES:
            return dict(start_offset)
        result = dict(start_offset)
        result["schema_node_fallback_retry_count"] = count + 1
        return result

    def _checkpoint_requires_snapshot(self, checkpoint: dict[str, Any]) -> bool:
        validated = _validated_offset(checkpoint)
        restart = int(validated.get("begin_lsn") or validated["commit_lsn"])
        return restart < self._bridge.minimum_lsn()

    def _resnapshot_scope(self, table: Table, checkpoint: dict[str, Any]) -> str:
        del table
        _validated_offset(checkpoint)
        return self._pipeline_scope()

    def _read_table_deletes(
        self,
        table_name: str,
        start_offset: dict,
        table_options: dict[str, str],
        *,
        snapshot_mode: str = "initial",
        bootstrap_offset: dict | None = None,
    ) -> tuple[Iterator[dict], dict]:
        # ``start_offset`` has already been through _effective_start_offset, which
        # collapses an all-advisory offset to {} so a retry looks like a fresh
        # bootstrap positionally. The bootstrap deferral counter lives in exactly such
        # an offset, so it has to be read from the raw one instead or it would reset to
        # zero on every retry and the fallback would never become eligible.
        bootstrap_offset = start_offset if bootstrap_offset is None else bootstrap_offset
        table = self._table(table_name, table_options)
        self._touch_table_state(table)
        _ensure_materializable(table, table_options)
        if self._append_only_table(table, table_options):
            # The framework does not open a delete flow for ingestion_type="append",
            # so reaching here means the two sides disagree about this table. Returning
            # an empty batch would look like "no deletes" forever; fail loudly instead.
            raise ValueError(
                f"Table '{table_name}' is configured for append-only ingestion "
                f"({_APPEND_INGESTION_OPTION}), which has no delete channel"
            )
        if not _cdc_capable(table):
            raise ValueError(
                f"Table '{table_name}' lacks a primary key or has columns unsupported "
                "by Informix CDC and is snapshot-only"
            )
        if (
            snapshot_mode == "auto_snapshot"
            and start_offset.get("phase") == "stream"
            and self._checkpoint_requires_snapshot(start_offset)
        ):
            checkpoint = _validated_offset(start_offset)
            scope = self._resnapshot_scope(table, checkpoint)
            high_water = self._initial_lsn(table, owner=False, wait=False, scope=scope)
            if high_water is None:
                return iter(()), dict(start_offset)
            schema_id = self._snapshot_schema_ids[(scope, table.identity)]
            return iter(()), _offset(
                high_water,
                high_water,
                high_water,
                None,
                "stream",
                table,
                schema_id,
                scope,
            )
        # Lakeflow checkpoints this independently from read_table(). The
        # upsert reader owns initialization and publishes the table boundary
        # through durable shared state; delete readers only consume it.
        if not start_offset:
            channel_start = self._read_upsert_channel_start(table, self._pipeline_scope())
            if channel_start is None:
                # The two Lakeflow flows have independent Spark checkpoints.  A
                # delete flow with no checkpoint must therefore wait until the
                # upsert reader publishes the position it is *actually* using in
                # this update.  Inferring from current_lsn or a scope-independent
                # schema node can start ahead of a resumed upsert checkpoint and
                # permanently skip old-key deletes after a cancelled refresh.
                return iter(()), self._schema_node_fallback_offset(bootstrap_offset)
            high_water, schema_id, fingerprint = channel_start
            if fingerprint != _schema_fingerprint(table):
                raise InformixError(
                    f"Informix upsert/delete bootstrap schema mismatch for "
                    f"'{table.exposed_name}'"
                )
            minimum = self._bridge.minimum_lsn()
            if high_water < minimum:
                logging.getLogger(__name__).warning(
                    "Informix upsert start boundary %s for '%s' precedes the minimum "
                    "retained LSN %s; the delete channel is waiting for a resnapshot",
                    high_water,
                    table.exposed_name,
                    minimum,
                )
                return iter(()), self._schema_node_fallback_offset(bootstrap_offset)
            current = self._bridge.current_lsn()
            if (high_water >> 32) > (current >> 32):
                logging.getLogger(__name__).warning(
                    "Informix upsert start boundary %s for '%s' is ahead of the "
                    "source's current logical log %s; the delete channel is waiting "
                    "for a new snapshot generation",
                    high_water,
                    table.exposed_name,
                    current,
                )
                return iter(()), self._schema_node_fallback_offset(bootstrap_offset)
            return iter(()), _offset(
                high_water,
                high_water,
                high_water,
                None,
                "stream",
                table,
                schema_id,
                self._pipeline_scope(),
            )
        if start_offset.get("phase") == "snapshot":
            snapshot_checkpoint = _validated_offset(start_offset)
            expected = snapshot_checkpoint.get("schema_fingerprint")
            if expected is None:
                raise InformixError(
                    f"Informix delete checkpoint for '{table.exposed_name}' predates "
                    "schema-safe offsets; run a full refresh"
                )
            table = self._refresh_table_schema(table, expected)
            high_water = int(start_offset.get("snapshot_lsn", start_offset.get("commit_lsn", 0)))
            start_offset = _offset(
                high_water,
                high_water,
                high_water,
                None,
                "stream",
                table,
                str(snapshot_checkpoint["schema_id"]),
                self._pipeline_scope(snapshot_checkpoint),
            )
        return self._read_stream(table, start_offset, table_options, deletes=True)

    def _release_worker_connection(self) -> None:
        # sys.exception() is Python 3.11+, while the connector supports 3.10.
        primary_error = sys.exc_info()[1]
        if self._bridge_instance is None:
            return
        release = getattr(self._bridge_instance, "release_connection", None)
        if not callable(release):
            contract_error = InformixError(
                "Configured Informix bridge does not implement release_connection()"
            )
            if primary_error is None:
                raise contract_error
            add_informix_exception_note(primary_error, str(contract_error))
            return
        try:
            release()
        except Exception as cleanup_error:
            if primary_error is None:
                raise
            add_informix_exception_note(
                primary_error,
                f"Releasing the Informix worker connection also failed: {cleanup_error}",
            )

    def _reset_lakebase_connection(self) -> None:
        """Drop the cached Lakebase state connection so the next use reopens fresh.

        A psycopg connection left open across a long, idle wait -- worse, left
        idle inside the implicit read transaction that a bare SELECT starts --
        is reaped by Lakebase's ``idle_in_transaction_session_timeout``, and
        ``.closed`` stays False until the next query fails, so
        ``_lakebase_connection`` cannot tell it is dead and hands it back. The
        snapshot-drain consumer blocks for the whole drain without touching
        state, so it closes the connection here and lets the post-wait manifest
        read reopen a guaranteed-live one.
        """

        connection = getattr(self, "_lakebase_conn", None)
        self._lakebase_conn = None
        if connection is not None:
            try:
                connection.close()
            except Exception:  # pragma: no cover - best-effort teardown
                pass

    def _read_cdc_only(self, table: Table, options: dict[str, str]) -> tuple[Iterator[dict], dict]:
        """Establish a schema-safe CDC boundary without reading existing rows."""

        pipeline_scope = self._pipeline_scope()
        table = self._refresh_table_schema(table, None)
        if not _cdc_capable(table):
            raise ValueError(
                f"snapshot.mode=cdc_only requires a CDC-capable table; "
                f"'{table.exposed_name}' is snapshot-only"
            )
        high_water = self._initial_lsn(table, scope=pipeline_scope)
        if high_water is None:  # owner=True always publishes or raises
            raise InformixError(f"Unable to establish no-data boundary for '{table.exposed_name}'")
        schema_id = self._snapshot_schema_ids[(pipeline_scope, table.identity)]
        self._publish_snapshot_boundary(table, schema_id, high_water, high_water, pipeline_scope)
        return iter(()), _offset(
            high_water,
            high_water,
            high_water,
            None,
            "stream",
            table,
            schema_id,
            pipeline_scope,
        )

    def _table_state_keys(self, table: Table) -> tuple[str, str]:
        """Return the (connection, table) key pair that identifies a table's state.

        The single source of truth for both. ``_shared_table_state_root`` joins
        these under the shared state location; snapshot staging joins the same two
        under the staging location. Deriving them here rather than recovering them
        by relpath is what lets staging stand alone once the shared state location
        is gone.
        """

        namespace = "\0".join(
            (
                "v2",
                self.options.get("hostname", "").strip().rstrip(".").casefold(),
                str(int(self.options.get("port", "9088"))),
                self.options.get("server", "").strip(),
                self.options.get("database", "").strip(),
            )
        )
        return (
            hashlib.sha256(namespace.encode()).hexdigest()[:24],
            hashlib.sha256(table.native_identity.encode()).hexdigest()[:24],
        )

    def _snapshot_stage_namespace(self, table: Table, pipeline_scope: str, schema_id: str) -> str:
        # Computed directly from the table's identity rather than recovered by
        # relpath against the shared state location, so staging no longer depends
        # on that location existing at all. The resulting path is byte-identical
        # to what the relpath form produced.
        connection_key, table_key = self._table_state_keys(table)
        return os.path.join(
            self._snapshot_staging_location,
            connection_key,
            table_key,
            "snapshot-page-data",
            pipeline_scope,
            schema_id,
        )

    def _snapshot_manifest_namespace(
        self, table: Table, pipeline_scope: str, schema_id: str
    ) -> str:
        """Record key for a staged snapshot's manifest.

        The manifest is metadata -- page count, row count, snapshot LSN -- so it
        lives with the rest of the state rather than beside the page payloads it
        describes. Keying it by identity instead of by staging path means moving
        or repointing the staging Volume does not orphan it.
        """

        connection_key, table_key = self._table_state_keys(table)
        return "/".join(
            (
                connection_key,
                table_key,
                "snapshot-page-manifest",
                pipeline_scope,
                schema_id,
            )
        )

    def _cleanup_completed_snapshot_stage(self, table: Table, checkpoint: dict[str, Any]) -> None:
        validated = _validated_offset(checkpoint)
        scope = self._pipeline_scope(validated)
        schema_id = str(validated["schema_id"])
        path = self._snapshot_stage_namespace(table, scope, schema_id)
        if path in self._cleaned_snapshot_stages:
            return
        try:
            PurePythonInformixBridge._remove_connection_slot_tree(path)
        except FileNotFoundError:
            pass
        except OSError:
            logging.getLogger(__name__).warning(
                "Cannot remove completed Informix snapshot staging data: %s",
                path,
                exc_info=True,
            )
            return
        self._cleaned_snapshot_stages.add(path)

    def _publish_snapshot_stage_page(
        self,
        table: Table,
        pipeline_scope: str,
        schema_id: str,
        snapshot_lsn: int,
        page_index: int,
        rows: list[dict[str, Any]],
        lower_pk: list[Any] | None,
    ) -> None:
        namespace = os.path.join(
            self._snapshot_stage_namespace(table, pipeline_scope, schema_id),
            "runs",
            str(snapshot_lsn),
        )
        # No filesystem capability probe: the exclusive-create and atomic-rename
        # guarantees it checked were needed by the head-election protocol, which
        # Postgres now performs. Writing a page needs only ordinary directory
        # creation, and a failure surfaces from the write itself.
        os.makedirs(namespace, mode=0o700, exist_ok=True)
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
            "upper_pk": _encode_snapshot_stage_value([rows[-1][key] for key in table.primary_keys]),
            "version": _SNAPSHOT_STAGE_VERSION,
        }
        canonical = json.dumps(body, allow_nan=False, separators=(",", ":"), sort_keys=True).encode(
            "utf-8"
        )
        record = {
            **body,
            "sha256": hashlib.sha256(canonical).hexdigest(),
        }
        payload = json.dumps(record, allow_nan=False, separators=(",", ":"), sort_keys=True).encode(
            "utf-8"
        )
        if len(payload) > _MAX_SNAPSHOT_STAGE_PAGE_BYTES:
            raise InformixError(
                f"Staged snapshot page {page_index} for '{table.exposed_name}' exceeds "
                f"{_MAX_SNAPSHOT_STAGE_PAGE_BYTES} decoded bytes; reduce snapshot.page.size"
            )
        compressed = gzip.compress(payload, compresslevel=6, mtime=0)
        page_name = f"page-{page_index:08d}"
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
            if not os.path.isdir(target):
                raise
            existing = self._read_snapshot_stage_page_record(
                table, pipeline_scope, schema_id, snapshot_lsn, page_index
            )
            if existing != record:
                raise InformixError(
                    f"Conflicting immutable snapshot page {page_index} for "
                    f"'{table.exposed_name}'"
                )
        finally:
            try:
                PurePythonInformixBridge._remove_connection_slot_tree(candidate)
            except FileNotFoundError:
                pass
            except OSError:
                logging.getLogger(__name__).warning(
                    "Cannot remove Informix snapshot-page candidate: %s",
                    candidate,
                    exc_info=True,
                )

    def _read_snapshot_stage_page_record(
        self,
        table: Table,
        pipeline_scope: str,
        schema_id: str,
        snapshot_lsn: int,
        page_index: int,
    ) -> dict[str, Any]:
        path = os.path.join(
            self._snapshot_stage_namespace(table, pipeline_scope, schema_id),
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
                raise InformixError(
                    f"Compressed staged snapshot page {page_index} for "
                    f"'{table.exposed_name}' is too large"
                )
            with os.fdopen(descriptor, "rb") as raw_handle:
                with gzip.GzipFile(fileobj=raw_handle, mode="rb") as handle:
                    payload = handle.read(_MAX_SNAPSHOT_STAGE_PAGE_BYTES + 1)
            if len(payload) > _MAX_SNAPSHOT_STAGE_PAGE_BYTES:
                raise InformixError(
                    f"Staged snapshot page {page_index} for '{table.exposed_name}' "
                    "exceeds its decoded size bound"
                )
            record = json.loads(payload)
        except (OSError, EOFError, UnicodeError, json.JSONDecodeError) as error:
            raise InformixError(
                f"Cannot read staged snapshot page {page_index} for " f"'{table.exposed_name}'"
            ) from error
        if not isinstance(record, dict):
            raise InformixError(
                f"Invalid staged snapshot page {page_index} for '{table.exposed_name}'"
            )
        digest = record.get("sha256")
        body = {key: value for key, value in record.items() if key != "sha256"}
        canonical = json.dumps(body, allow_nan=False, separators=(",", ":"), sort_keys=True).encode(
            "utf-8"
        )
        if (
            record.get("version") != _SNAPSHOT_STAGE_VERSION
            or record.get("table") != table.native_identity
            or record.get("pipeline_scope") != pipeline_scope
            or record.get("schema_id") != schema_id
            or record.get("page_index") != page_index
            or isinstance(record.get("row_count"), bool)
            or record.get("row_count") != len(record.get("rows", []))
            or not isinstance(digest, str)
            or not hmac.compare_digest(digest, hashlib.sha256(canonical).hexdigest())
        ):
            raise InformixError(
                f"Invalid staged snapshot page {page_index} for '{table.exposed_name}'"
            )
        return record

    def _cleanup_consumed_snapshot_stage_pages(
        self,
        table: Table,
        pipeline_scope: str,
        schema_id: str,
        snapshot_lsn: int,
        before_page_index: int,
    ) -> None:
        """Best-effort delete pages acknowledged by an advanced start offset."""

        run = os.path.join(
            self._snapshot_stage_namespace(table, pipeline_scope, schema_id),
            "runs",
            str(snapshot_lsn),
        )
        for page_index in range(before_page_index):
            path = os.path.join(run, f"page-{page_index:08d}")
            try:
                PurePythonInformixBridge._remove_connection_slot_tree(path)
            except FileNotFoundError:
                continue
            except OSError:
                logging.getLogger(__name__).warning(
                    "Cannot remove consumed Informix snapshot page: %s",
                    path,
                    exc_info=True,
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
            raise InformixError(f"Invalid staged snapshot manifest for '{table.exposed_name}'")
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
    ) -> dict[str, Any]:
        self._publish_immutable_head(
            self._snapshot_manifest_namespace(table, pipeline_scope, schema_id),
            {
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
            raise InformixError(f"Conflicting staged snapshot manifest for '{table.exposed_name}'")
        return manifest

    def _staged_snapshot_result(
        self,
        table: Table,
        pipeline_scope: str,
        schema_id: str,
        manifest: dict[str, Any],
        page_index: int,
        checkpoint: dict | None = None,
    ) -> tuple[Iterator[dict], dict]:
        page_count = int(manifest["page_count"])
        snapshot_lsn = _strict_lsn(manifest["snapshot_lsn"], "snapshot_lsn")
        if page_index < 0 or page_index >= page_count:
            raise InformixError(
                f"Invalid staged snapshot page index {page_index} for " f"'{table.exposed_name}'"
            )
        record = self._read_snapshot_stage_page_record(
            table, pipeline_scope, schema_id, snapshot_lsn, page_index
        )
        if _strict_lsn(record.get("snapshot_lsn"), "snapshot_lsn") != snapshot_lsn:
            raise InformixError(f"Staged snapshot boundary mismatch for '{table.exposed_name}'")
        decoded = _decode_snapshot_stage_value(record.get("rows"))
        if not isinstance(decoded, list) or not all(isinstance(row, dict) for row in decoded):
            raise InformixError(f"Invalid staged snapshot rows for '{table.exposed_name}'")
        if int(record["row_count"]) != len(decoded):
            raise InformixError(f"Staged snapshot row count mismatch for '{table.exposed_name}'")
        # The shared reader replays a batch from its start offset. Receiving
        # page_index=N as that start means Spark has advanced beyond every page
        # below N; retries continue from N, so those earlier immutable pages are
        # no longer required. Keep the current page and manifest intact.
        self._cleanup_consumed_snapshot_stage_pages(
            table,
            pipeline_scope,
            schema_id,
            snapshot_lsn,
            page_index,
        )
        next_page = page_index + 1
        if next_page < page_count:
            last_pk = _encode_snapshot_stage_value([decoded[-1][key] for key in table.primary_keys])
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
        return iter(decoded), end

    @staticmethod
    def _datetime_primary_key(table: Table) -> bool:
        return any(
            column.name in table.primary_keys and column.type_name == "DATETIME"
            for column in table.columns
        )

    def _incremental_datetime_chunk_exprs(
        self, table: Table, options: dict[str, str]
    ) -> tuple[dict[str, str], bool]:
        """Return ``(chunk_exprs, viable)`` for incremental chunking.

        ``chunk_exprs`` maps each DATETIME primary-key column to a fixed-width
        SQL text expression so keyset pagination can compare strings instead of
        DATETIME literals. Any contiguous DATETIME qualifier is supported,
        including HOUR-anchored TIME-of-day and DATE-only ranges, and a DATETIME
        key column combined with non-DATETIME key columns in a composite key.
        ``viable`` is False when the table has a DATETIME key that cannot be
        string-chunked (option off or a malformed qualifier), signalling the
        caller to fall back to the blocking snapshot rather than risk skipped
        rows.
        """

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
            expr = datetime_order_preserving_cast(column.name, int(column.length or 0))
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
        """Interleave a PK-chunked snapshot with the live CDC stream.

        Unlike the blocking consistent snapshot, CDC begins immediately at the
        captured boundary and the table is copied in primary-key chunks, one
        per microbatch, interleaved with change events. Each chunk is read in
        its own short repeatable-read transaction and its READ rows are stamped
        with the LSN captured just before that chunk's SELECT. Correctness for
        keyed (apply_changes) targets follows from LSN sequencing: any
        concurrent or later change to a chunked key commits at a higher LSN and
        supersedes the READ row, so no in-memory deduplication window is needed.
        """

        checkpoint = _validated_offset(start) if start else None
        # A truncated destination discards the offset, and with it the whole
        # incremental block, so this branch is also what a full refresh takes.
        if checkpoint is None:
            return self._begin_incremental(
                table, None, options, pipeline_scope_override=pipeline_scope_override
            )
        incremental = checkpoint.get("incremental")
        if isinstance(incremental, dict) and self._incremental_bound_is_foreign(incremental):
            # A bound from an earlier update was captured before this update's CDC
            # start, which would orphan rows inserted in between. Recapture it --
            # and only it, since the cursor still names rows this table holds.
            logging.getLogger(__name__).warning(
                "Informix incremental snapshot bound for '%s' was captured by scope=%s "
                "but this update is scope=%s; recapturing the snapshot bound so rows "
                "above a stale bound are not orphaned between the snapshot and the "
                "change stream",
                table.exposed_name,
                incremental.get("scope"),
                self._registration_scope,
            )
            chunk_exprs, viable = self._incremental_datetime_chunk_exprs(table, options)
            if viable:
                checkpoint = dict(checkpoint)
                checkpoint["incremental"] = self._rebound_incremental_block(
                    table, incremental, chunk_exprs
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
            raise InformixError(
                f"Informix incremental snapshot for '{table.exposed_name}' resumed "
                "without its progress state; run a full refresh"
            )
        pipeline_scope = pipeline_scope_override or self._pipeline_scope()
        table = self._refresh_table_schema(table, None)
        if not _cdc_capable(table):
            raise InformixError(
                f"Table '{table.exposed_name}' is not CDC-capable; incremental "
                "snapshot requires a primary key and CDC-supported columns"
            )
        # DATETIME primary keys cannot use native keyset (WHERE pk > ?)
        # pagination reliably. When every DATETIME key column has an
        # order-preserving string cast we chunk by that string instead;
        # otherwise fall back to the blocking consistent snapshot.
        chunk_exprs, viable = self._incremental_datetime_chunk_exprs(table, options)
        if not viable:
            logging.getLogger(__name__).info(
                "Informix incremental snapshot for '%s' falling back to the blocking "
                "consistent snapshot because a DATETIME primary-key column cannot be "
                "chunked as an order-preserving string",
                table.exposed_name,
            )
            return self._read_snapshot(
                table, None, options, pipeline_scope_override=pipeline_scope_override
            )
        table, schema_id, incremental = self._seed_incremental_block(
            table, pipeline_scope, chunk_exprs, _snapshot_filter(options)
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
    ) -> tuple[Table, str, dict]:
        """Capture a fresh, self-describing incremental snapshot block.

        The correctness of an incremental copy rests on one ordering: ``max_pk``
        is read at or after the CDC start LSN. A row above ``max_pk`` is then
        necessarily inserted later, commits above the start LSN, and so is
        carried by the change stream that begins there. Reverse the order and
        rows inserted in between fall above the key bound *and* below the stream
        start -- owned by neither, and lost silently.

        Production hit exactly that (tw214): a cancelled update published
        max_pk=W0056332 at 22:41:58, the restart published a fresh, higher
        initial_lsn at 22:46:29, and the copy ran with the OLD key bound against
        the NEW stream start. Four rows inserted in between were never emitted.
        The bound cannot have been captured fresh -- rows above it already
        existed when the copy began.

        ``max_pk`` rides in the Spark checkpoint while the LSN is published
        durably per scope, so nothing tied the pair together. Stamping the block
        with the scope that captured it makes the pair checkable wherever it is
        later resumed; see ``_incremental_bound_is_foreign``.
        """

        high_water = self._initial_lsn(table, scope=pipeline_scope)
        schema_id = self._snapshot_schema_ids[(pipeline_scope, table.identity)]
        table = self._refresh_table_schema(table, _schema_fingerprint(table))
        self._publish_snapshot_boundary(table, schema_id, high_water, high_water, pipeline_scope)
        # Strictly after the boundary above, never before it.
        max_pk = self._bridge.max_primary_key(
            table.identity,
            list(table.primary_keys),
            chunk_exprs=chunk_exprs or None,
            snapshot_filter=snapshot_filter,
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
        }
        return table, schema_id, incremental

    def _rebound_incremental_block(
        self, table: Table, incremental: dict, chunk_exprs: dict[str, str]
    ) -> dict:
        """Recapture ``max_pk`` when the block was bounded by another update.

        The snapshot bound is only sound if it was read at or after the CDC start
        LSN this update is streaming from: a row above ``max_pk`` then necessarily
        commits above that LSN, so the change stream carries it. A bound inherited
        from an earlier update inverts that -- it was read before this update's
        boundary, so rows inserted in between sit above the key bound and below
        the stream start, owned by neither side.

        Production hit exactly that (tw214). A cancelled update bounded the copy
        at W0056332 at 22:41:58; the restart published a fresh, higher boundary at
        22:46:29 and copied with the OLD bound. Four rows inserted in between were
        never emitted. The bound provably could not have been captured fresh --
        rows above it already existed when that copy began.

        Only the bound is recaptured. ``last_pk`` is kept: it names rows this
        destination still holds, because the framework only discards the offset
        when it truncates the table, and a discarded offset takes the whole
        ``incremental`` block with it (start is None, which re-seeds from
        scratch). Resetting the cursor here would re-read a prefix that is still
        present -- measured as a 5x throughput drop across restarts -- and protect
        nothing.
        """

        max_pk = self._bridge.max_primary_key(
            table.identity,
            list(table.primary_keys),
            chunk_exprs=chunk_exprs or None,
            snapshot_filter=incremental.get("snapshot_filter"),
        )
        rebound = dict(incremental)
        rebound["max_pk"] = None if max_pk is None else _encode_snapshot_stage_value(list(max_pk))
        rebound["scope"] = self._registration_scope
        if max_pk is None:
            # The table is empty as of this boundary; the change stream owns it.
            rebound["done"] = True
        return rebound

    def _incremental_bound_is_foreign(self, incremental: dict) -> bool:
        """True when this block's bound was captured by a different update."""

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

    def _read_incremental_step(
        self, table: Table, checkpoint: dict, options: dict[str, str]
    ) -> tuple[Iterator[dict], dict]:
        incremental = dict(checkpoint["incremental"])
        # Stream a bounded batch of committed changes from the current cursor.
        cdc_rows, end = self._read_stream(table, checkpoint, options, deletes=False)
        output = list(cdc_rows)
        # The stream reader may return a freshly built offset that drops the
        # incremental block; the schema it used is authoritative for chunking.
        table = self._refresh_table_schema(table, end.get("schema_fingerprint"))
        if not incremental["done"]:
            chunk_rows, incremental = self._next_incremental_chunk(table, incremental, options)
            output.extend(chunk_rows)
        if incremental["done"]:
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
            # value from ``checkpoint``, not from ``end``.
            end = _with_backlog_streak(end, checkpoint, True)
        return iter(output), end

    def _next_incremental_chunk(
        self, table: Table, incremental: dict, options: dict[str, str]
    ) -> tuple[list[dict[str, Any]], dict]:
        page_size = self._table_int_option(
            options, "snapshot.page.size", _DEFAULT_SNAPSHOT_PAGE_SIZE, minimum=1
        )
        chunk_exprs = dict(incremental.get("chunk_exprs") or {})
        # Columns chunked by an order-preserving expression are compared and
        # cursored on the rendered "__chunk_<key>" string, not the raw value.
        cursor_names = [
            f"__chunk_{key}" if key in chunk_exprs else key for key in table.primary_keys
        ]
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
        replaying_chunk = replay_stop_pk is not None
        bound = max_pk
        if replay_stop_pk is not None and replay_stop_pk is not _REPLAY_DRAIN_TO_MAX_PK:
            bound = min(max_pk, replay_stop_pk)

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
        for fetch in range(_REPLAY_MAX_CHUNK_FETCHES):
            # Ask for one row more than the page keeps, so "the page filled" and
            # "rows remain" are distinguishable rather than inferred: the same
            # probe the blocking consistent_snapshot loop uses.
            fetch_lsn, raw = self._bridge.snapshot_chunk(
                table.identity,
                [column.name for column in table.columns],
                list(table.primary_keys),
                cursor,
                page_size + 1,
                chunk_exprs=chunk_exprs or None,
                snapshot_filter=incremental.get("snapshot_filter"),
            )
            try:
                fetch_lsn = _strict_lsn(fetch_lsn, "chunk_lsn")
            except ValueError as error:
                raise InformixError(
                    f"Incremental snapshot chunk for '{table.exposed_name}' returned "
                    "an invalid LSN"
                ) from error
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
                if key > max_pk:
                    passed_upper_bound = True
                    break
                if key > bound:
                    stopped_at_bound = True
                    break
                page.append(row)
            bounded.extend(page)
            if page:
                cursor = cursor_key(page[-1])
            reached_bound = cursor is not None and tuple(cursor) >= bound
            if not replaying_chunk or passed_upper_bound or stopped_at_bound:
                break
            if reached_bound or not page or not has_more:
                break
        else:
            raise InformixError(
                f"Replayed incremental snapshot range for '{table.exposed_name}' did not "
                f"reach its committed cursor within {_REPLAY_MAX_CHUNK_FETCHES} chunk reads"
            )
        last_cursor = cursor_key(bounded[-1]) if bounded else None
        # Strip the "__chunk_<key>" helper aliases before shaping so they never
        # reach the emitted row or the destination schema.
        chunk_aliases = [f"__chunk_{key}" for key in chunk_exprs]
        cleaned = [
            {key: value for key, value in row.items() if key not in chunk_aliases}
            for row in bounded
        ]
        stamp = chunk_lsn if chunk_lsn is not None else 0
        shaped = [_shape_snapshot(row, stamp, table, options) for row in cleaned]
        result = dict(incremental)
        result["chunk_lsn"] = str(stamp)
        if last_cursor is not None:
            result["last_pk"] = _encode_snapshot_stage_value(list(last_cursor))
        # Only the snapshot's own bound ends the copy. A replay stops at the
        # committed cursor, which normally sits well below max_pk, so treating
        # that stop as completion would abandon the rest of the copy: the flow
        # would leave the incremental phase with most of the table uncopied.
        reached_max_key = last_cursor is not None and last_cursor >= max_pk
        if replaying_chunk:
            done = passed_upper_bound or reached_max_key
        else:
            done = passed_upper_bound or not has_more or reached_max_key
        result["done"] = done
        return shaped, result

    @staticmethod
    def _replay_stop_pk(
        options: dict[str, str], primary_keys: tuple[str, ...]
    ) -> object | tuple | None:
        """The chunk cursor a replayed read must stop at, or None if unbounded.

        JSON ``null`` returns a sentinel rather than None: both use max_pk as the
        numerical bound, but the replay must keep fetching pages until it reaches
        that bound while an ordinary read intentionally fetches only one page.

        Validated rather than trusted, for the same reason as the LSN bound: a
        malformed value would silently widen the replay back to the behaviour
        being fixed.
        """

        raw = options.get(_REPLAY_STOP_PK_OPTION)
        if raw is None:
            return None
        try:
            decoded = json.loads(raw)
        except ValueError as error:
            raise InformixError("Informix replay stop cursor is not valid JSON") from error
        if decoded is None:
            return _REPLAY_DRAIN_TO_MAX_PK
        if not isinstance(decoded, list):
            raise InformixError("Informix replay stop cursor must be a list or null")
        cursor = _decode_snapshot_stage_value(decoded)
        validate_snapshot_arity(cursor, primary_keys)
        return tuple(cursor)

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
        if checkpoint and checkpoint.get("schema_fingerprint") is None:
            raise InformixError(
                f"Informix snapshot checkpoint for '{table.exposed_name}' predates "
                "schema-safe offsets; run a full refresh"
            )
        expected_fingerprint = checkpoint.get("schema_fingerprint") if checkpoint else None
        table = self._refresh_table_schema(table, expected_fingerprint)
        if not _cdc_capable(table) and not (
            allow_keyless and all(column.cdc_supported for column in table.columns)
        ):
            raise InformixError(
                f"Table '{table.exposed_name}' is no longer CDC-capable after metadata refresh; "
                "run a full refresh after restoring its primary key and supported schema"
            )
        page_size = self._table_int_option(
            options, "snapshot.page.size", _DEFAULT_SNAPSHOT_PAGE_SIZE, minimum=1
        )
        if checkpoint:
            pipeline_scope = self._pipeline_scope(checkpoint)
            high_water = int(checkpoint["snapshot_lsn"])
            schema_id = str(checkpoint["schema_id"])
            manifest = self._read_snapshot_stage_manifest(table, pipeline_scope, schema_id)
            if manifest is None:
                raise InformixError(
                    f"Incremental snapshot state for '{table.exposed_name}' is missing; "
                    "run a full refresh"
                )
            if int(manifest["snapshot_lsn"]) != high_water:
                raise InformixError(
                    f"Incremental snapshot boundary mismatch for '{table.exposed_name}'"
                )
            page_index = int(checkpoint["snapshot"].get("page_index", -1))
            table = self._refresh_table_schema(table, expected_fingerprint)
            return self._staged_snapshot_result(
                table, pipeline_scope, schema_id, manifest, page_index, checkpoint
            )
        else:
            consistent_snapshot = getattr(self._bridge, "consistent_snapshot", None)
            if not callable(consistent_snapshot):
                raise InformixError(
                    "Configured Informix bridge does not implement consistent_snapshot()"
                )
            # Validate the complete snapshot contract before _initial_lsn()
            # enables full-row logging or publishes durable initialization.
            high_water = self._initial_lsn(table, scope=pipeline_scope)
            schema_id = self._snapshot_schema_ids[(pipeline_scope, table.identity)]
            manifest = self._read_snapshot_stage_manifest(table, pipeline_scope, schema_id)
            if manifest is not None:
                page_count = int(manifest["page_count"])
                if page_count == 0:
                    snapshot_lsn = int(manifest["snapshot_lsn"])
                    return iter(()), _offset(
                        snapshot_lsn,
                        snapshot_lsn,
                        snapshot_lsn,
                        None,
                        "stream",
                        table,
                        schema_id,
                        pipeline_scope,
                    )
                return self._staged_snapshot_result(table, pipeline_scope, schema_id, manifest, 0)
            if self._snapshot_shared_enabled():
                # Model C: hand the long drain to the bounded daemon pool. Free this
                # reader's connection slot first so the wait holds none, then serve the
                # pages the daemon stages -- exactly as a resumed snapshot does. At most
                # ``snapshot.reader.threads`` drains run at once, so streaming readers
                # keep the remaining slots. ``_initial_lsn`` above already established
                # the durable boundary; the daemon reads it rather than redoing it.
                self._release_worker_connection()
                manifest = self._drain_via_snapshot_daemon(
                    table, options, pipeline_scope, schema_id, allow_keyless
                )
                page_count = int(manifest["page_count"])
                if page_count == 0:
                    snapshot_lsn = int(manifest["snapshot_lsn"])
                    return iter(()), _offset(
                        snapshot_lsn,
                        snapshot_lsn,
                        snapshot_lsn,
                        None,
                        "stream",
                        table,
                        schema_id,
                        pipeline_scope,
                    )
                return self._staged_snapshot_result(table, pipeline_scope, schema_id, manifest, 0)
            # Model A: drain inline below. The reservation floor applied at slot
            # acquisition keeps low slots reachable by streaming readers throughout.
            max_rows = self._table_int_option(options, "snapshot.max.rows", 0, minimum=0)
            expected_columns = {column.name for column in table.columns}
            page_count = 0
            row_count = 0
            shaped_bytes = 0
            previous_upper_pk: list[Any] | None = None
            max_bytes = self._table_int_option(options, "snapshot.max.bytes", 0, minimum=0)

            def stage_page(
                snapshot_lsn: int,
                page_index: int,
                page_rows: list[dict[str, Any]],
            ) -> None:
                nonlocal page_count, row_count, shaped_bytes, previous_upper_pk
                if not page_rows:
                    return
                for index, row in enumerate(page_rows):
                    if not isinstance(row, dict) or set(row) != expected_columns:
                        raise InformixError(
                            f"Consistent snapshot row {row_count + index} does not "
                            f"exactly match table columns for '{table.exposed_name}'"
                        )
                try:
                    shaped = [
                        _shape_snapshot(row, snapshot_lsn, table, options) for row in page_rows
                    ]
                except Exception as error:
                    if isinstance(error, InformixError):
                        raise
                    raise InformixError(
                        f"Cannot materialize consistent snapshot rows for "
                        f"'{table.exposed_name}'"
                    ) from error
                if max_bytes:
                    shaped_bytes += _deep_size(shaped)
                    if shaped_bytes > max_bytes:
                        raise InformixError(
                            f"Initial shaped snapshot exceeds " f"snapshot.max.bytes={max_bytes}"
                        )
                self._publish_snapshot_stage_page(
                    table,
                    pipeline_scope,
                    schema_id,
                    snapshot_lsn,
                    page_index,
                    shaped,
                    previous_upper_pk,
                )
                previous_upper_pk = [page_rows[-1][key] for key in table.primary_keys]
                page_count = max(page_count, page_index + 1)
                row_count += len(page_rows)

            snapshot_lsn, rows = consistent_snapshot(
                table.identity,
                [c.name for c in table.columns],
                table.primary_keys,
                page_size,
                max_rows,
                self._table_int_option(options, "snapshot.max.bytes", 0, minimum=0),
                datetime_primary_key=any(
                    column.name in table.primary_keys and column.type_name == "DATETIME"
                    for column in table.columns
                ),
                page_consumer=stage_page,
                snapshot_filter=_snapshot_filter(options),
                isolation=self._snapshot_isolation(options),
            )
            try:
                snapshot_lsn = _strict_lsn(snapshot_lsn, "snapshot_lsn")
            except ValueError as error:
                raise InformixError(
                    "Consistent snapshot bridge returned an invalid snapshot LSN"
                ) from error
            if snapshot_lsn < high_water:
                raise InformixError(
                    f"Consistent snapshot LSN {snapshot_lsn} precedes prepared LSN "
                    f"{high_water} for '{table.exposed_name}'"
                )
            try:
                current_lsn = _strict_lsn(self._bridge.current_lsn(), "current_lsn")
            except ValueError as error:
                raise InformixError(
                    "Informix bridge returned an invalid current source LSN"
                ) from error
            if snapshot_lsn > current_lsn:
                raise InformixError(
                    f"Consistent snapshot LSN {snapshot_lsn} exceeds current source LSN "
                    f"{current_lsn} for '{table.exposed_name}'"
                )
            if not isinstance(rows, list):
                raise InformixError("Consistent snapshot bridge must return rows as a list")
            if max_rows and len(rows) > max_rows:
                raise InformixError(f"Initial snapshot exceeds snapshot.max.rows={max_rows}")
            if rows:
                if page_count:
                    raise InformixError(
                        "Consistent snapshot bridge returned rows after consuming pages"
                    )
                stage_page(snapshot_lsn, 0, rows)
            retained_bytes = _deep_size(rows) if max_bytes else 0
            if max_bytes and retained_bytes > max_bytes:
                raise InformixError(f"Initial snapshot exceeds snapshot.max.bytes={max_bytes}")
            table = self._refresh_table_schema(table, _schema_fingerprint(table))
            self._publish_snapshot_boundary(
                table, schema_id, high_water, snapshot_lsn, pipeline_scope
            )
            manifest = self._publish_snapshot_stage_manifest(
                table,
                pipeline_scope,
                schema_id,
                snapshot_lsn,
                page_count,
                row_count,
            )
            if page_count == 0:
                return iter(()), _offset(
                    snapshot_lsn,
                    snapshot_lsn,
                    snapshot_lsn,
                    None,
                    "stream",
                    table,
                    schema_id,
                    pipeline_scope,
                )
            return self._staged_snapshot_result(table, pipeline_scope, schema_id, manifest, 0)

    def _read_append_only(self, table: Table, start: dict | None, options: dict[str, str]):
        """Begin an append-only flow, then hand off to the ordinary CDC stream.

        Only the *first* read reaches here. It establishes a stream-phase offset and
        every later read is routed straight to :meth:`_read_stream`, which needs no
        changes: it derives its restart position from ``begin_lsn``/``commit_lsn``
        and never consults ``table.primary_keys``.

        Two startup behaviours, selected by ``snapshot.mode``:

        ``cdc_only`` (also the append-only default when snapshot.mode is unset)
            Start at the server's current log position and emit nothing now. Rows
            that predate the flow are not captured. No size ceiling, no memory
            spike, and no failure mode -- which is why it is the default. An
            append-only table is usually one that is actively being appended to, so
            the interesting rows are the arriving ones.

        ``initial``
            Read the table once under REPEATABLE READ and continue from that
            snapshot's LSN, so history is captured and the handoff is gapless: the
            snapshot is consistent as-of one LSN and the stream resumes from exactly
            that LSN. Because a key-less table has no stable monotonic cursor, this
            uses one forward-only cursor and one REPEATABLE READ transaction. Fetched
            rows are written to immutable staged pages and become visible only after
            the completed snapshot manifest is published.
        """

        _ensure_materializable(table, options)
        table = self._refresh_table_schema(table, None)
        pipeline_scope = self._pipeline_scope()
        configured_mode = options.get("snapshot.mode", self.options.get("snapshot.mode"))
        snapshot_mode = (
            "cdc_only" if configured_mode is None else str(configured_mode).strip().lower()
        )
        if snapshot_mode not in {"initial", "cdc_only"}:
            raise ValueError(
                f"Append-only table '{table.exposed_name}' supports only "
                "snapshot.mode=initial or snapshot.mode=cdc_only"
            )

        if snapshot_mode == "initial":
            return self._read_snapshot(table, start, options, allow_keyless=True)

        # The boundary is durable and shared, exactly as for a keyed table, so a
        # restart before the first checkpoint commits resumes from the same position
        # instead of skipping or re-reading whatever the log had reached by then.
        boundary = self._initial_lsn(table, scope=pipeline_scope)
        if boundary is None:
            raise InformixError(
                f"Could not establish an append-only start boundary for " f"'{table.exposed_name}'"
            )
        schema_id = self._snapshot_schema_ids[(pipeline_scope, table.identity)]

        rows: Iterator[dict] = iter(())

        # phase="stream" so the next read goes straight to _read_stream. tx_id is None
        # and change_lsn equals commit_lsn because no transaction has been read yet --
        # the same shape the keyed snapshot-to-stream transition produces.
        return rows, _offset(
            boundary,
            boundary,
            boundary,
            None,
            "stream",
            table,
            schema_id,
            pipeline_scope,
        )

    def _read_snapshot_only(self, table: Table, start: dict | None, options: dict[str, str]):
        _ensure_materializable(table, options)
        table = self._refresh_table_schema(table, None)
        fingerprint = _schema_fingerprint(table)
        # PK-less tables cannot be seek-paginated safely.  Read exactly once;
        # returning None signals non-checkpointable full refresh semantics.
        limit = self._table_int_option(options, "snapshot.max.rows", 0, minimum=0)
        if limit == 0:
            limit = 100000
        rows = self._bridge.snapshot_page(
            table.identity,
            [c.name for c in table.columns],
            (),
            None,
            limit + 1,
            self._table_int_option(options, "snapshot.max.bytes", 0, minimum=0),
            snapshot_filter=_snapshot_filter(options),
        )
        table = self._refresh_table_schema(table, fingerprint)
        if len(rows) > limit:
            raise InformixError(
                f"Snapshot-only table {table.exposed_name} exceeds snapshot.max.rows={limit}"
            )
        lsn = self._bridge.current_lsn()
        return iter(_shape_snapshot(row, lsn, table, options) for row in rows), None

    def _read_changes_with_reconnect(
        self, captures, restart, timeout_seconds, max_records, *, table: Table
    ):
        """Run one bounded CDC poll, retrying in place on a mid-read drop.

        A connection dropped during ``read_changes`` raises ``SqliProtocolError``
        (e.g. ``SQ_LODATA server/ISAM error -1`` when the socket dies mid-poll).
        The read has not advanced any offset, so it is safe to reset the dead
        transport and reissue the identical poll under the connection slot the
        reader already holds. Bounded attempts with short backoff absorb a
        transient blip; exhausting them re-raises so a genuinely-down source
        still fails honestly.

        This retries synchronously inside the single ``latestOffset`` call rather
        than yielding an empty batch, so it is correct for a triggered
        (AvailableNow) flow too: an empty batch below the frozen high-water mark
        would let AvailableNow complete the batch and skip the un-read changes,
        whereas retrying in place either returns the real records or raises.

        A lost connection-capacity lease (``ConnectionCapacityUnavailable``) is
        not a transport drop and is never retried here; it propagates to the
        capacity-aware handler.
        """

        attempt = 0
        while True:
            try:
                return self._bridge.read_changes(captures, restart, timeout_seconds, max_records)
            except ConnectionCapacityUnavailable:
                raise
            except SqliProtocolError as error:
                lease_lost = getattr(self._bridge, "_connection_lease_lost", None)
                if lease_lost is not None and lease_lost.is_set():
                    # The drop is a deliberate lease-loss shutdown, not a blip.
                    raise
                if attempt >= _CDC_RECONNECT_MAX_RETRIES:
                    raise
                delay = min(
                    _CDC_RECONNECT_BASE_SECONDS * (2**attempt),
                    _CDC_RECONNECT_MAX_SECONDS,
                )
                logging.getLogger(__name__).warning(
                    "Informix CDC poll for '%s' lost its connection (%s); resetting the "
                    "transport and retrying the same read (attempt %d/%d) after %.1fs",
                    table.exposed_name,
                    error,
                    attempt + 1,
                    _CDC_RECONNECT_MAX_RETRIES,
                    delay,
                    exc_info=True,
                )
                reset = getattr(self._bridge, "reset_transport", None)
                if reset is not None:
                    reset()
                time.sleep(delay)
                attempt += 1

    def _shared_cdc_enabled(self) -> bool:
        return _option_bool(self.options, _SHARED_CDC_SESSION_OPTION, True)

    def _snapshot_isolation(self, options: dict[str, str]) -> str:
        """Resolve the ``snapshot.isolation`` table option to its SQL clause.

        Defaults to COMMITTED READ LAST COMMITTED (non-locking; see the README
        duplicate warning). Tokens are whitespace/hyphen-insensitive; an unknown
        value fails loudly rather than silently falling back.
        """

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
            raise ValueError(
                f"Option '{_SNAPSHOT_ISOLATION_OPTION}' must be one of "
                f"{sorted(_SNAPSHOT_ISOLATION_SQL)}; got {token!r}"
            ) from None

    def _snapshot_shared_enabled(self) -> bool:
        return _option_bool(self.options, _SNAPSHOT_SHARED_SESSION_OPTION, True)

    def _snapshot_reader_thread_count(self) -> int:
        return max(
            1,
            int(
                self.options.get(
                    _SNAPSHOT_READER_THREADS_OPTION,
                    str(_DEFAULT_SNAPSHOT_READER_THREADS),
                )
            ),
        )

    def _snapshot_drain_reader_factory(self) -> Callable[[], "InformixLakeflowConnect"]:
        # The daemon worker gets its OWN options copy with shared mode cleared, so its
        # ``_read_snapshot`` drains inline (Model A path) rather than recursing back into
        # the pool. Capacity accounting is left intact -- the drain must count against the
        # same connection budget so ``snapshot.reader.threads`` bounds the slots it holds.
        reader_options = dict(self.options)
        reader_options[_SNAPSHOT_SHARED_SESSION_OPTION] = "false"
        # The drain runs on a daemon thread and holds its slot for the whole scan, but it
        # is bootstrap work that must finish to unblock its consumer. Floor it one band
        # below the CDC daemon (not at the CDC floor), so a saturated CDC daemon cannot
        # starve it -- it always has slots the CDC daemon is locked out of.
        reader_options[_SNAPSHOT_DAEMON_SLOT_MARKER_OPTION] = "true"
        # Wait for a slot as patiently as the consumer waits for the drain. The worker runs
        # inside the consumer's ``pool.wait(snapshot.drain.wait.seconds)`` window, so giving
        # up at the shorter ``connection.wait.timeout.seconds`` (default 600s) fails the
        # query while the consumer would still have waited. Under a contended pool the drain
        # is the most vulnerable reader -- it must acquire *and hold* a slot for the whole
        # scan -- but consumer reads release their slot each microbatch, so a patient drain
        # wins a slot as capacity churns instead of dying prematurely. Drop any inherited
        # per-read attempt budget so it cannot cap the wait below the drain window.
        # connection.wait.timeout.seconds is int-validated, while snapshot.drain.wait.seconds
        # is a float; coerce to a whole-second integer string.
        reader_options["connection.wait.timeout.seconds"] = str(
            int(
                float(
                    self.options.get(
                        "snapshot.drain.wait.seconds", str(_SNAPSHOT_DRAIN_WAIT_SECONDS)
                    )
                )
            )
        )
        reader_options.pop(_CONNECTION_ATTEMPT_BUDGET_OPTION, None)
        reader_options.pop(_REPLAY_CONNECTION_WAIT_BUDGET_OPTION, None)
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
        """Enqueue this table's drain on the bounded pool and wait -- holding no slot
        or state connection -- for the daemon to stage it and publish the manifest,
        then return that manifest."""

        namespace = self._lakebase_state_namespace()
        pool = _snapshot_drain_pool(
            (namespace, pipeline_scope),
            reader_factory=self._snapshot_drain_reader_factory(),
            thread_count=self._snapshot_reader_thread_count(),
        )
        job_key = (namespace, pipeline_scope, table.native_identity, schema_id)
        pool.submit(
            job_key,
            table=table,
            options=dict(options),
            pipeline_scope=pipeline_scope,
            allow_keyless=allow_keyless,
        )
        wait_seconds = float(
            self.options.get("snapshot.drain.wait.seconds", str(_SNAPSHOT_DRAIN_WAIT_SECONDS))
        )
        # Hold no Lakebase connection across the drain wait: the caller's manifest
        # probe left this connection idle inside a read transaction, which Lakebase
        # reaps as an idle-in-transaction timeout over a multi-minute drain. The
        # daemon stages on its own connection; the post-wait read reopens fresh.
        self._reset_lakebase_connection()
        if not pool.wait(job_key, wait_seconds):
            raise InformixError(
                f"Timed out after {wait_seconds:g}s waiting for the shared snapshot drain "
                f"of '{table.exposed_name}'; raise snapshot.drain.wait.seconds or "
                "snapshot.reader.threads"
            )
        manifest = self._read_snapshot_stage_manifest(table, pipeline_scope, schema_id)
        if manifest is None:
            raise InformixError(
                f"Shared snapshot drain of '{table.exposed_name}' completed without "
                "publishing a manifest"
            )
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

    def _shared_cdc_reader_factory(self, channel: str) -> Callable[[], "InformixLakeflowConnect"]:
        # The daemon reader gets its OWN options copy: the connector mutates options
        # mid-read (channel, capacity hints), so a shared dict would race. Shared mode
        # is cleared on the copy so the reader never recurses into this path.
        reader_options = dict(self.options)
        reader_options[_CONNECTION_CHANNEL_OPTION] = channel
        reader_options[_SHARED_CDC_SESSION_OPTION] = "false"
        # The shard reads on a daemon thread, so floor its slot above the daemon
        # reservation to leave headroom for consumer bootstrap reads.
        reader_options[_DAEMON_SLOT_MARKER_OPTION] = "true"
        cls = type(self)
        return lambda: cls(reader_options)

    def _shared_cdc_snapshot(self, table, checkpoint, options, deletes, pipeline_scope):
        """Subscribe this table to its shard and return the daemon's coherent view, or
        None to fall back to a direct read (daemon not ready, or a schema transition)."""

        channel = "delete" if deletes else "upsert"
        threads = self._shared_cdc_thread_count()
        shard_id = int(hashlib.sha256(table.native_identity.encode()).hexdigest(), 16) % threads
        key = (self._lakebase_state_namespace(), pipeline_scope, channel, shard_id)
        capture = _capture_descriptor(table, _client_encoding(self.options))
        checkpoint_commit = int(checkpoint["commit_lsn"])
        floor = int(checkpoint.get("begin_lsn") or checkpoint["commit_lsn"])
        shard = _shared_cdc_shard(
            key,
            reader_factory=self._shared_cdc_reader_factory(channel),
            timeout_seconds=self._table_int_option(options, "cdc.timeout", 5, minimum=1),
            max_records=self._table_int_option(
                options, "cdc.max.records", _DEFAULT_CDC_MAX_RECORDS, minimum=1, maximum=256
            ),
            buffer_cap=int(
                self.options.get(_SHARED_CDC_BUFFER_OPTION, str(_DEFAULT_SHARED_CDC_BUFFER))
            ),
            subscriber_ttl=float(
                self.options.get("cdc.shared.state.wait.seconds", str(_SHARED_STATE_WAIT_SECONDS))
            ),
        )
        shard.subscribe(table.native_identity, table, capture, checkpoint_commit, floor)
        view = shard.snapshot(table.native_identity, checkpoint_commit)
        if view is None:
            return None
        # A fingerprint mismatch means a schema transition is in flight; defer to the
        # direct-read path, which owns the transition/replay logic.
        if str(checkpoint.get("schema_fingerprint")) != view.fingerprint:
            return None
        return view

    def _read_stream(self, table: Table, start: dict, options: dict[str, str], deletes: bool):
        checkpoint = _validated_offset(start)
        pipeline_scope = self._pipeline_scope()
        # In shared-session mode a daemon reader owns this shard's Informix I/O: it
        # supplies the schema, log bounds, and change records, so a steady-state poll
        # touches Informix for nothing. A None view (daemon not ready yet, or a schema
        # transition in flight) transparently falls back to the direct-read path below.
        shared_view = None
        if self._shared_cdc_enabled():
            shared_view = self._shared_cdc_snapshot(
                table, checkpoint, options, deletes, pipeline_scope
            )
        if shared_view is not None:
            table = shared_view.table
        else:
            table = self._refresh_table_schema(table, None)
        fingerprint = _schema_fingerprint(table)
        checkpoint_fingerprint = checkpoint.get("schema_fingerprint")
        checkpoint_schema_id = str(checkpoint["schema_id"])
        if checkpoint_fingerprint is None:
            raise InformixError(
                f"Informix checkpoint for '{table.exposed_name}' predates schema-safe offsets; "
                "run a full refresh before resuming CDC"
            )
        restart = int(checkpoint.get("begin_lsn") or checkpoint["commit_lsn"])
        minimum = shared_view.min_lsn if shared_view is not None else self._bridge.minimum_lsn()
        if restart < minimum:
            raise LogRetentionError(
                f"Restart LSN {restart} is older than minimum retained LSN "
                f"{minimum}; resnapshot required"
            )
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
            raise LogRetentionError(
                f"Restart LSN {restart} for '{table.exposed_name}' names logical log "
                f"{restart >> 32}, which is ahead of the source's current log "
                f"{current >> 32}, so the source reinitialized its logical log; this "
                "checkpoint can never be resumed. Run a full refresh; changes committed "
                "before the reinitialization are no longer in the log."
            )
        capture_table = table
        transition_table = table
        transition_lsn: int | None = None
        capture_schema_id = checkpoint_schema_id
        transition_schema_id = checkpoint_schema_id
        projection_schema_transition = False
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
            )
            # Informix capture registration is a projection, not a raw decoder
            # tied to the catalog layout at ``start_lsn``.  For an appended
            # nullable column, registering the expanded descriptor at the old
            # checkpoint safely spans the DDL: records written before the ALTER
            # contain NULL for the appended field, while records written after
            # it contain the logged value.  Live Informix 15 validation covers
            # both transactions in one replay session.
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
        if self._trigger_available_now:
            stop_lsn, trigger_generation = self._shared_trigger_boundary(
                table,
                checkpoint,
                owner=not deletes,
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
        replay_stop = self._replay_stop_lsn(options)
        if replay_stop is not None:
            stop_lsn = replay_stop if stop_lsn is None else min(stop_lsn, replay_stop)
            # The committed range may hold more rows than a normal batch admits, and
            # truncating it is the very divergence being removed. The LSN bound caps
            # the read instead, so the budget must not also cut it short.
            max_rows = _REPLAY_UNBOUNDED_ROWS
        if shared_view is not None:
            # The daemon already assembled and fanned out this shard's transactions and
            # re-validated the schema; take them as-is (no Informix I/O, no re-fetch).
            committed, caught_up, open_begin = (
                shared_view.committed,
                shared_view.caught_up,
                shared_view.open_begin,
            )
        else:
            raw_records = self._read_changes_with_reconnect(
                [_capture_descriptor(capture_table, _client_encoding(self.options))],
                restart,
                self._table_int_option(options, "cdc.timeout", 5, minimum=1),
                self._table_int_option(
                    options, "cdc.max.records", _DEFAULT_CDC_MAX_RECORDS, minimum=1, maximum=256
                ),
                table=table,
            )
            table = self._refresh_table_schema(table, fingerprint)
            committed, caught_up, open_begin = _transaction_batch(raw_records)
        recovered = _recover(committed, checkpoint)
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
        for tx in recovered:
            if transition_lsn is not None and tx.commit_lsn > transition_lsn:
                if tx.begin_lsn < transition_lsn:
                    raise InformixError(
                        f"Transaction {tx.tx_id} spans schema transition LSN "
                        f"{transition_lsn} for '{table.exposed_name}'; keep source writes "
                        "quiesced until schema transition completes or run a full refresh"
                    )
                crossed_transition = True
                break
            if stop_lsn is not None and tx.commit_lsn > stop_lsn:
                crossed_trigger_boundary = True
                break
            projected = _project_transaction(tx, table, deletes)
            projected = [_coerce_variable_decimal_values(row, table, options) for row in projected]
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
            if len(output) >= max_rows:
                row_budget_reached = True
                break
        if (
            transition_lsn is not None
            and crossed_transition
            and open_begin is not None
            and open_begin < transition_lsn
        ):
            raise InformixError(
                f"An open transaction beginning at LSN {open_begin} spans schema transition "
                f"LSN {transition_lsn} for '{table.exposed_name}'; keep source writes "
                "quiesced until schema transition completes or run a full refresh"
            )
        if (
            transition_lsn is not None
            and (trigger_high_water is None or transition_lsn <= trigger_high_water)
            and (
                (caught_up and consumed == len(recovered))
                or (crossed_transition and (open_begin is None or open_begin >= transition_lsn))
            )
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
        reached_trigger_boundary = trigger_generation is not None and (
            crossed_trigger_boundary or (caught_up and consumed == len(recovered))
        )
        if reached_trigger_boundary:
            end = dict(end)
            end["trigger_generation"] = trigger_generation
            end["trigger_high_water"] = str(trigger_high_water)
        end = _with_backlog_streak(end, start, row_budget_reached)
        return iter(output), end

    def _record_current_schema(
        self,
        table: Table,
        checkpoint_lsn: int,
        checkpoint_schema_id: str,
        checkpoint_fingerprint: str,
        pipeline_scope: str,
        *,
        owner: bool,
    ) -> None:
        if not owner:
            return
        existing = self._read_immutable_head(
            self._immutable_namespace(table, "schema-nodes", checkpoint_schema_id)
        )
        if existing is not None:
            self._validate_immutable_record_header(existing, "schema-node", table.exposed_name)
            schema = existing.get("schema")
            predecessor = schema.get("predecessor") if isinstance(schema, dict) else None
            if (
                not isinstance(schema, dict)
                or schema.get("id") != checkpoint_schema_id
                or schema.get("fingerprint") != checkpoint_fingerprint
                or checkpoint_fingerprint != _schema_fingerprint(table)
                or (
                    predecessor is not None
                    and (
                        not isinstance(predecessor, str)
                        or not re.fullmatch(r"[0-9a-f]{32}", predecessor)
                    )
                )
            ):
                raise InformixError(
                    f"Checkpoint schema {checkpoint_schema_id} conflicts with immutable "
                    f"history for '{table.exposed_name}'"
                )
            if (
                _table_from_schema_state(schema, table.database).native_identity
                != table.native_identity
            ):
                raise InformixError(
                    f"Invalid immutable schema-node state for '{table.exposed_name}'"
                )
            return
        minimum, current = self._bridge.minimum_lsn(), self._bridge.current_lsn()
        if not minimum <= checkpoint_lsn <= current:
            raise InformixError(
                f"Cannot rebuild immutable schema state for '{table.exposed_name}' from "
                f"checkpoint LSN {checkpoint_lsn}; retained/current range is [{minimum}, {current}]"
            )
        self._bridge.validate_initial_lsn(
            _capture_descriptor(table, _client_encoding(self.options)), checkpoint_lsn
        )
        authoritative = self._find_immutable_schema_record(
            table, checkpoint_schema_id, pipeline_scope
        )
        if authoritative is None:
            if checkpoint_fingerprint != _schema_fingerprint(table):
                raise InformixError(
                    f"Schema history for checkpoint node {checkpoint_schema_id} is missing "
                    f"for '{table.exposed_name}' and cannot be reconstructed after a schema "
                    "change; run a full refresh"
                )
            authoritative = {
                "created_at": time.time(),
                "schema": _schema_state(table, checkpoint_lsn, schema_id=checkpoint_schema_id),
            }
        # Schema nodes are global for a physical table and schema ID. Pipeline
        # scope belongs to initialization/trigger records, not this shared node.
        authoritative = {
            "created_at": authoritative.get("created_at", time.time()),
            "schema": authoritative["schema"],
        }
        winner = self._publish_immutable_head(
            self._immutable_namespace(table, "schema-nodes", checkpoint_schema_id),
            authoritative,
            record_type="schema-node",
        )
        self._validate_immutable_record_header(winner, "schema-node", table.exposed_name)
        schema = winner.get("schema")
        if (
            not isinstance(schema, dict)
            or schema.get("id") != checkpoint_schema_id
            or _table_from_schema_state(schema, table.database).native_identity
            != table.native_identity
            or schema.get("fingerprint") != _schema_fingerprint(table)
        ):
            raise InformixError(
                f"Checkpoint schema {checkpoint_schema_id} conflicts with immutable history "
                f"for '{table.exposed_name}'"
            )

    def _shared_trigger_boundary(
        self,
        table: Table,
        checkpoint: dict[str, Any],
        *,
        owner: bool,
        current_lsn: int | None = None,
    ) -> tuple[int, str]:
        # One pipeline update owns exactly one immutable per-table boundary.
        # Upsert and delete checkpoints may legitimately have different prior
        # generations after retries or cancellation, so predecessor identity
        # must not partition their current-update coordination.
        scope = self._pipeline_scope(checkpoint)
        cached = self._trigger_boundaries.get(table.identity)
        if cached is not None:
            high_water, generation, cached_scope = cached
            if cached_scope == scope:
                # Once Spark checkpoints this trigger's generation it invokes
                # latestOffset again to prove that AvailableNow is exhausted.
                # Keep returning the same frozen boundary in this reader
                # instance; a new pipeline update constructs a new reader and
                # legitimately advances from generation as its predecessor.
                if high_water < int(checkpoint.get("commit_lsn", 0)):
                    raise InformixError(
                        f"Cached trigger boundary {high_water} precedes checkpoint LSN "
                        f"{checkpoint.get('commit_lsn')} for '{table.exposed_name}'"
                    )
                return high_water, generation
            self._trigger_boundaries.pop(table.identity, None)
        namespace = self._immutable_namespace(table, "triggers", "scopes", scope)
        deadline = time.monotonic() + int(
            self.options.get("cdc.shared.state.wait.seconds", str(_SHARED_STATE_WAIT_SECONDS))
        )
        delay = 0.1
        while True:
            trigger = self._read_immutable_head(namespace)
            if isinstance(trigger, dict):
                self._validate_immutable_record_header(trigger, "trigger", table.exposed_name)
                generation = trigger.get("generation")
                try:
                    high_water = self._immutable_lsn(trigger, "high_water", table.exposed_name)
                except InformixError as error:
                    raise InformixError(
                        f"Invalid shared trigger boundary for '{table.exposed_name}'"
                    ) from error
                if not isinstance(generation, str) or not re.fullmatch(r"[0-9a-f]{32}", generation):
                    raise InformixError(
                        f"Invalid shared trigger generation for '{table.exposed_name}'"
                    )
                if trigger.get("scope") != scope or high_water < int(
                    checkpoint.get("commit_lsn", 0)
                ):
                    raise InformixError(
                        f"Invalid immutable trigger identity for '{table.exposed_name}'"
                    )
                self._trigger_boundaries[table.identity] = (
                    high_water,
                    generation,
                    scope,
                )
                return high_water, generation
            if owner:
                candidate_high_water = (
                    current_lsn if current_lsn is not None else self._bridge.current_lsn()
                )
                if candidate_high_water < int(checkpoint["commit_lsn"]):
                    raise InformixError(
                        f"Current LSN {candidate_high_water} precedes checkpoint LSN "
                        f"{checkpoint['commit_lsn']} for '{table.exposed_name}'"
                    )
                self._publish_immutable_head(
                    namespace,
                    {
                        "created_at": time.time(),
                        "generation": secrets.token_hex(16),
                        "high_water": str(candidate_high_water),
                        "scope": scope,
                    },
                    record_type="trigger",
                )
                if time.monotonic() >= deadline:
                    raise InformixError(
                        f"Timed out waiting for the upsert reader to publish a triggered "
                        f"boundary for '{table.exposed_name}' in update scope '{scope}'"
                    )
                delay = _sleep_with_backoff(deadline, delay)
                continue
            raise TriggerBoundaryUnavailable(
                f"The upsert reader has not published the triggered boundary for "
                f"'{table.exposed_name}' in update scope '{scope}' yet"
            )

    def _schema_transition(
        self,
        table: Table,
        checkpoint_schema_id: str,
        checkpoint_lsn: int,
        *,
        owner: bool,
    ) -> tuple[Table, Table, int, str]:
        namespace = self._immutable_namespace(table, "schemas", checkpoint_schema_id)
        deadline = time.monotonic() + int(
            self.options.get("cdc.shared.state.wait.seconds", str(_SHARED_STATE_WAIT_SECONDS))
        )
        delay = 0.1
        while True:
            previous_record = self._read_immutable_head(
                self._immutable_namespace(table, "schema-nodes", checkpoint_schema_id)
            )
            previous = previous_record.get("schema") if previous_record else None
            if not isinstance(previous, dict):
                raise InformixError(
                    f"Schema history for checkpoint node {checkpoint_schema_id} is missing for "
                    f"'{table.exposed_name}'; run a full refresh"
                )
            self._validate_immutable_record_header(
                previous_record, "schema-node", table.exposed_name
            )
            if previous.get("id") != checkpoint_schema_id:
                raise InformixError(
                    f"Immutable schema node identity mismatch for '{table.exposed_name}'"
                )
            previous_table = _table_from_schema_state(previous, table.database)
            _ensure_additive_schema_change(previous_table, table)
            transition_record = self._read_immutable_head(namespace)
            if transition_record is None and owner:
                transition = self._bridge.current_lsn()
                if transition < checkpoint_lsn:
                    raise InformixError(
                        f"Current LSN {transition} precedes checkpoint LSN "
                        f"{checkpoint_lsn} for '{table.exposed_name}'"
                    )
                self._bridge.validate_initial_lsn(
                    _capture_descriptor(table, _client_encoding(self.options)),
                    transition,
                )
                node = _schema_state(table, transition, predecessor=checkpoint_schema_id)
                transition_record = self._publish_immutable_head(
                    namespace,
                    {"created_at": time.time(), "schema": node},
                    record_type="schema-transition",
                )
            if transition_record is not None:
                self._validate_immutable_record_header(
                    transition_record, "schema-transition", table.exposed_name
                )
                current = transition_record.get("schema")
                if not isinstance(current, dict):
                    raise InformixError("Invalid immutable Informix schema transition")
                transition = self._immutable_lsn(current, "start_lsn", table.exposed_name)
                minimum, now = self._bridge.minimum_lsn(), self._bridge.current_lsn()
                if not minimum <= transition <= now:
                    raise InformixError(
                        f"Schema transition LSN {transition} for '{table.exposed_name}' is "
                        f"outside retained/current range [{minimum}, {now}]"
                    )
                # The transition LSN records when this immutable successor was
                # first observed; it is not a replay boundary.  Another update
                # can therefore retain the transition after this checkpoint has
                # advanced beyond that observation.  The identity and additive
                # successor checks below make it safe to adopt the metadata while
                # continuing replay from checkpoint_lsn.
                target_table = _table_from_schema_state(current, table.database)
                if (
                    not isinstance(current.get("id"), str)
                    or not re.fullmatch(r"[0-9a-f]{32}", str(current["id"]))
                    or current.get("predecessor") != checkpoint_schema_id
                ):
                    raise InformixError(
                        f"Invalid immutable schema identity for '{table.exposed_name}'"
                    )
                _ensure_additive_schema_change(previous_table, target_table)
                if _schema_fingerprint(target_table) != _schema_fingerprint(table):
                    _ensure_additive_schema_change(target_table, table)
                schema_winner = self._publish_immutable_head(
                    self._immutable_namespace(table, "schema-nodes", str(current["id"])),
                    transition_record,
                    record_type="schema-node",
                )
                self._validate_schema_node_winner(schema_winner, current, table)
                return previous_table, target_table, transition, str(current["id"])
            if not owner:
                # Never wait here while holding a connection slot. Reaching this
                # method already acquired one (the schema refresh in _read_stream
                # connects before this call), so a delete reader that slept until
                # the deadline would occupy a slot for the whole wait while doing
                # no source work -- and the upsert reader it is waiting for needs
                # a slot of its own to publish (current_lsn above). Because both
                # channels can claim every slot when upsert.connection.reservation
                # is 0 (the default), enough waiting delete readers hold the entire
                # pool and the publication can never happen: a genuine deadlock,
                # broken only by this deadline expiring and failing the flow.
                #
                # Yield instead, exactly as the other two non-owner coordination
                # points do (_shared_table_lsn returns None via wait=False, and
                # _shared_trigger_boundary raises TriggerBoundaryUnavailable). The
                # caller converts this into an unchanged-offset empty batch, which
                # releases the slot and lets the owning upsert reader acquire one.
                raise SchemaTransitionUnavailable(
                    f"The upsert reader has not published the schema transition for "
                    f"'{table.exposed_name}' from schema node {checkpoint_schema_id} yet"
                )
            if time.monotonic() >= deadline:
                raise InformixError(
                    f"Timed out waiting for the upsert reader to publish schema transition "
                    f"state for '{table.exposed_name}'"
                )
            delay = _sleep_with_backoff(deadline, delay)

    def _refresh_table_schema(self, table: Table, expected_fingerprint: str | None) -> Table:
        refreshed = Table.parse(self._bridge.get_table(table.identity), table.database)
        if table.key_override:
            # Keep the primary.keys override (and therefore the fingerprint) across a
            # schema refresh instead of reverting to the catalog's keys. Only applies
            # when the caller was already overridden, so a genuine catalog key change
            # on a normal table is still detected as drift.
            refreshed = replace(refreshed, primary_keys=table.primary_keys, key_override=True)
        _ensure_materializable(refreshed)
        fingerprint = _schema_fingerprint(refreshed)
        if expected_fingerprint is not None and expected_fingerprint != fingerprint:
            raise InformixError(
                f"Informix schema changed for '{table.exposed_name}' during ingestion; "
                "run a full refresh before reading additional snapshot or CDC records"
            )
        if self._tables is not None:
            self._tables[table.exposed_name] = refreshed
        return refreshed

    def _table_int_option(
        self,
        table_options: dict[str, str],
        name: str,
        default: int,
        *,
        minimum: int,
        maximum: int | None = None,
    ) -> int:
        value = int(table_options.get(name, self.options.get(name, str(default))))
        if value < minimum:
            raise ValueError(f"Option '{name}' must be >= {minimum}")
        if maximum is not None and value > maximum:
            raise ValueError(f"Option '{name}' must be <= {maximum}")
        return value

    def _initial_lsn(
        self,
        table: Table,
        *,
        owner: bool = True,
        wait: bool = True,
        scope: str | None = None,
    ) -> int | None:
        """Return one durable per-table boundary shared by upsert and delete readers."""

        scope = scope or self._pipeline_scope()
        cache_key = (scope, table.identity)
        if cache_key not in self._snapshot_high_water:
            shared = self._shared_table_lsn(table, owner=owner, wait=wait, scope=scope)
            if shared is None:
                return None
            value, schema_id = shared
            minimum = self._bridge.minimum_lsn()
            if value < minimum:
                raise LogRetentionError(
                    f"Configured initial LSN {value} is older than minimum retained LSN "
                    f"{minimum}; choose a retained boundary after enabling full-row logging"
                )
            current = self._bridge.current_lsn()
            if value > current:
                raise InformixError(
                    f"Configured initial LSN {value} is newer than current Informix LSN "
                    f"{current}; choose a position captured after enabling full-row logging"
                )
            self._snapshot_high_water[cache_key] = value
            self._snapshot_schema_ids[cache_key] = schema_id
        return self._snapshot_high_water[cache_key]

    def _publish_upsert_channel_start(
        self,
        table: Table,
        start_lsn: int,
        schema_id: str,
        fingerprint: str,
        scope: str,
    ) -> None:
        """Publish the upsert reader's effective start for an unseeded delete flow.

        Lakeflow checkpoints the upsert and delete flows independently.  The
        incoming upsert offset is the only authoritative evidence of where a
        resumed update starts; source ``current_lsn`` and global schema history
        can both be later. A later poll can discover an older open transaction,
        so publishers atomically lower the shared boundary and never raise it.
        """

        namespace = self._immutable_namespace(table, "channel-starts", scope, "upsert")
        winner = publish_minimum_lsn_state_record(
            self._lakebase_connection(),
            self._lakebase_state_namespace(),
            namespace,
            {
                "created_at": time.time(),
                "fingerprint": fingerprint,
                "format_version": _IMMUTABLE_STATE_VERSION,
                "record_type": "channel-start",
                "schema_id": schema_id,
                "scope": scope,
                "start_lsn": str(start_lsn),
                "table": table.native_identity,
            },
            record_type="channel-start",
        )
        self._validate_immutable_record_header(winner, "channel-start", table.exposed_name)
        winner_start = self._immutable_lsn(winner, "start_lsn", table.exposed_name)
        same_position_schema_change = winner_start == start_lsn and (
            winner.get("schema_id") != schema_id or winner.get("fingerprint") != fingerprint
        )
        compatible_successor = same_position_schema_change and self._schema_is_direct_successor(
            table, schema_id, fingerprint, winner.get("schema_id")
        )
        if (
            winner.get("scope") != scope
            or winner.get("table") != table.native_identity
            or winner_start > start_lsn
            or (same_position_schema_change and not compatible_successor)
        ):
            raise InformixError(
                f"Conflicting Informix upsert start boundary for '{table.exposed_name}' "
                f"in update scope '{scope}'"
            )

    def _schema_is_direct_successor(
        self,
        table: Table,
        schema_id: str,
        fingerprint: str,
        predecessor_id: Any,
    ) -> bool:
        """Return whether a schema node safely replaces one at the same position."""

        if not isinstance(predecessor_id, str):
            return False
        record = self._read_immutable_head(
            self._immutable_namespace(table, "schema-nodes", schema_id)
        )
        schema = record.get("schema") if isinstance(record, dict) else None
        return bool(
            isinstance(schema, dict)
            and schema.get("id") == schema_id
            and schema.get("fingerprint") == fingerprint
            and schema.get("predecessor") == predecessor_id
        )

    def _read_upsert_channel_start(self, table: Table, scope: str) -> tuple[int, str, str] | None:
        record = self._read_immutable_head(
            self._immutable_namespace(table, "channel-starts", scope, "upsert")
        )
        if record is None:
            return None
        self._validate_immutable_record_header(record, "channel-start", table.exposed_name)
        schema_id = record.get("schema_id")
        fingerprint = record.get("fingerprint")
        if (
            record.get("scope") != scope
            or record.get("table") != table.native_identity
            or not isinstance(schema_id, str)
            or not re.fullmatch(r"[0-9a-f]{32}", schema_id)
            or not isinstance(fingerprint, str)
            or not re.fullmatch(r"[0-9a-f]{64}", fingerprint)
        ):
            raise InformixError(
                f"Invalid Informix upsert start boundary for '{table.exposed_name}'"
            )
        return (
            self._immutable_lsn(record, "start_lsn", table.exposed_name),
            schema_id,
            fingerprint,
        )

    def _upsert_channel_start_exists(self, table_name: str, table_options: dict[str, str]) -> bool:
        """Check current-scope delete coordination without opening Informix."""

        exposed = table_options.get("qualified_source_table", table_name)
        # Preserve eager configuration/schema failures. Registration commonly
        # leaves the table in this process-local cache, so validation here costs
        # no source connection. An explicitly append-only table must always reach
        # the normal path, which raises that it has no delete channel.
        if table_options.get(_APPEND_INGESTION_OPTION, "false").lower() == "true":
            return True
        cached = self._tables.get(exposed) if self._tables is not None else None
        if cached is not None:
            _ensure_materializable(cached, table_options)
            if self._append_only_table(cached, table_options) or not _cdc_capable(cached):
                return True
        identity = self._native_table_state_identity(exposed)
        if identity is None:
            return True
        native_identity, connection_key, table_key = identity
        record_key = "/".join(
            (
                connection_key,
                table_key,
                "channel-starts",
                self._pipeline_scope(),
                "upsert",
            )
        )
        return (
            read_state_record(self._lakebase_connection(), connection_key, record_key) is not None
        )

    def _native_table_state_identity(self, exposed: str) -> tuple[str, str, str] | None:
        """Derive native and Lakebase table identities without opening Informix."""

        parts = _split_identity(exposed)
        if len(parts) == 2:
            database = self.options.get("database", "")
            owner, name = parts
        elif len(parts) == 3:
            database, owner, name = parts
        else:
            # Preserve the normal metadata path and its user-facing validation
            # for connector-specific aliases that are not native identities.
            return None
        if not database or not all(_IDENTIFIER.fullmatch(part) for part in (database, owner, name)):
            return None
        native_identity = f"{database}:{_sql_identifier(owner)}.{_sql_identifier(name)}"
        connection_key = self._lakebase_state_namespace()
        table_key = hashlib.sha256(native_identity.encode()).hexdigest()[:24]
        return native_identity, connection_key, table_key

    def _publish_checkpoint_channel_start_before_capacity(
        self,
        table_name: str,
        effective_start: dict[str, Any],
        table_options: dict[str, str],
    ) -> None:
        """Publish an authoritative resume checkpoint before waiting for a slot.

        A canceled microbatch leaves its positional end in Spark's checkpoint but
        no commit marker. The next update must replay that range, and replay may
        wait for capacity for minutes. Publishing only after acquiring capacity
        leaves the paired delete reader unable to initialize for that entire wait.
        The incoming positional checkpoint already fixes the safe resume boundary,
        schema id, and fingerprint, so publishing it requires Lakebase only.
        """

        if (
            effective_start.get("phase") != "stream"
            or effective_start.get("schema_fingerprint") is None
        ):
            return
        checkpoint = _validated_offset(effective_start)
        exposed = table_options.get("qualified_source_table", table_name)
        identity = self._native_table_state_identity(exposed)
        if identity is None:
            return
        native_identity, connection_key, table_key = identity
        scope = self._pipeline_scope()
        start_lsn = int(checkpoint.get("begin_lsn") or checkpoint["commit_lsn"])
        schema_id = str(checkpoint["schema_id"])
        fingerprint = str(checkpoint["schema_fingerprint"])
        record_key = "/".join(
            (
                connection_key,
                table_key,
                "channel-starts",
                scope,
                "upsert",
            )
        )
        winner = publish_minimum_lsn_state_record(
            self._lakebase_connection(),
            connection_key,
            record_key,
            {
                "created_at": time.time(),
                "fingerprint": fingerprint,
                "format_version": _IMMUTABLE_STATE_VERSION,
                "record_type": "channel-start",
                "schema_id": schema_id,
                "scope": scope,
                "start_lsn": str(start_lsn),
                "table": native_identity,
            },
            record_type="channel-start",
        )
        winner_start = self._immutable_lsn(winner, "start_lsn", exposed)
        if (
            winner.get("format_version") != _IMMUTABLE_STATE_VERSION
            or winner.get("record_type") != "channel-start"
            or winner.get("scope") != scope
            or winner.get("table") != native_identity
            or winner_start > start_lsn
        ):
            raise InformixError(
                f"Conflicting Informix upsert start boundary for '{exposed}' "
                f"in update scope '{scope}'"
            )

    def _shared_table_lsn(
        self,
        table: Table,
        *,
        owner: bool,
        wait: bool = True,
        scope: str | None = None,
    ) -> tuple[int, str] | None:
        scope = scope or self._pipeline_scope()
        namespace = self._immutable_namespace(table, "initialization", scope)
        deadline = time.monotonic() + int(
            self.options.get("cdc.shared.state.wait.seconds", str(_SHARED_STATE_WAIT_SECONDS))
        )
        delay = 0.1
        while True:
            record = self._read_immutable_head(namespace)
            if record is not None:
                self._validate_immutable_record_header(record, "initialization", table.exposed_name)
                if record.get("table") != table.native_identity or record.get("scope") != scope:
                    raise InformixError("Informix immutable initialization table mismatch")
                node = record.get("schema")
                if (
                    not isinstance(node, dict)
                    or not isinstance(node.get("id"), str)
                    or not re.fullmatch(r"[0-9a-f]{32}", str(node.get("id")))
                    or node.get("fingerprint") != _schema_fingerprint(table)
                ):
                    raise InformixError("Invalid Informix immutable initialization schema")
                value = self._immutable_lsn(record, "initial_lsn", table.exposed_name)
                if (
                    self._immutable_lsn(node, "start_lsn", table.exposed_name) != value
                    or _table_from_schema_state(node, table.database).native_identity
                    != table.native_identity
                    or node.get("predecessor") is not None
                ):
                    raise InformixError("Invalid Informix immutable initialization schema")
                if not owner:
                    snapshot = self._read_immutable_head(
                        self._immutable_namespace(table, "snapshots", scope, str(node["id"]))
                    )
                    if snapshot is None:
                        record = None
                    else:
                        self._validate_immutable_record_header(
                            snapshot, "snapshot", table.exposed_name
                        )
                        if (
                            snapshot.get("scope") != scope
                            or snapshot.get("schema_id") != node["id"]
                            or self._immutable_lsn(snapshot, "initial_lsn", table.exposed_name)
                            != value
                        ):
                            raise InformixError(
                                f"Invalid immutable snapshot boundary for '{table.exposed_name}'"
                            )
                        value = self._immutable_lsn(snapshot, "snapshot_lsn", table.exposed_name)
                if record is not None:
                    self._bridge.validate_initial_lsn(
                        _capture_descriptor(table, _client_encoding(self.options)), value
                    )
                    if owner:
                        schema_winner = self._publish_immutable_head(
                            self._immutable_namespace(table, "schema-nodes", str(node["id"])),
                            {
                                "created_at": record.get("created_at", time.time()),
                                "schema": node,
                            },
                            record_type="schema-node",
                        )
                        self._validate_schema_node_winner(schema_winner, node, table)
                    return value, str(node["id"])
            if owner:
                value = self._bridge.prepare_initial_capture([table.native_identity])
                self._bridge.validate_initial_lsn(
                    _capture_descriptor(table, _client_encoding(self.options)), value
                )
                self._publish_immutable_head(
                    namespace,
                    {
                        "created_at": time.time(),
                        "initial_lsn": str(value),
                        "schema": _schema_state(table, value),
                        "scope": scope,
                        "table": table.native_identity,
                    },
                    record_type="initialization",
                )
                continue
            if not wait:
                return None
            if time.monotonic() >= deadline:
                role = "upsert initialization" if owner else "the table's upsert reader"
                raise InformixError(
                    f"Timed out waiting for {role} to publish shared CDC state for "
                    f"'{table.exposed_name}' at '{namespace}'"
                )
            delay = _sleep_with_backoff(deadline, delay)

    def _publish_snapshot_boundary(
        self,
        table: Table,
        schema_id: str,
        initial_lsn: int,
        snapshot_lsn: int,
        pipeline_scope: str,
    ) -> None:
        winner = self._publish_immutable_head(
            self._immutable_namespace(table, "snapshots", pipeline_scope, schema_id),
            {
                "created_at": time.time(),
                "initial_lsn": str(initial_lsn),
                "scope": pipeline_scope,
                "schema_id": schema_id,
                "snapshot_lsn": str(snapshot_lsn),
            },
            record_type="snapshot",
        )
        self._validate_immutable_record_header(winner, "snapshot", table.exposed_name)
        if self._immutable_lsn(winner, "initial_lsn", table.exposed_name) != initial_lsn:
            raise InformixError(
                f"Conflicting immutable snapshot boundary for '{table.exposed_name}'"
            )
        if (
            winner.get("scope") != pipeline_scope
            or winner.get("schema_id") != schema_id
            or self._immutable_lsn(winner, "snapshot_lsn", table.exposed_name) < initial_lsn
        ):
            raise InformixError(f"Invalid immutable snapshot boundary for '{table.exposed_name}'")
        # Publish only once the strategy has finalized its safe handoff:
        # incremental snapshots pass initial_lsn == snapshot_lsn, while a
        # blocking consistent snapshot may finish later and must seed deletes
        # from that later snapshot position.
        self._publish_upsert_channel_start(
            table,
            snapshot_lsn,
            schema_id,
            _schema_fingerprint(table),
            pipeline_scope,
        )

    def _schema_node_delete_boundary(self, table: Table) -> tuple[int | None, str | None]:
        """Return a scope-independent bootstrap boundary for a delete reader.

        ``schema-nodes`` is keyed by schema id alone, so unlike the
        update-scoped ``initialization`` and ``snapshots`` namespaces it is
        never removed by ``_cleanup_previous_update_scopes``. The node for the
        table's current layout is committed before any checkpoint that
        references it, which makes its ``start_lsn`` a durable position the
        upsert reader has already validated.

        Returns ``(None, None)`` whenever a usable node is absent so the caller
        keeps its existing "wait for the upsert reader" behaviour. This runs
        only on the bootstrap path, never on a checkpointed microbatch.
        """

        schema_id = _schema_node_id(table)
        record = self._read_immutable_head(
            self._immutable_namespace(table, "schema-nodes", schema_id)
        )
        if record is None:
            return None, None
        self._validate_immutable_record_header(record, "schema-node", table.exposed_name)
        schema = record.get("schema")
        if (
            not isinstance(schema, dict)
            or schema.get("id") != schema_id
            or schema.get("fingerprint") != _schema_fingerprint(table)
            or _table_from_schema_state(schema, table.database).native_identity
            != table.native_identity
        ):
            return None, None
        boundary = self._immutable_lsn(schema, "start_lsn", table.exposed_name)
        minimum = self._bridge.minimum_lsn()
        if boundary < minimum:
            # The recorded position has aged out of the logical log. Starting
            # there would fail the retention check in _read_stream, and silently
            # advancing to `minimum` could skip deletes committed in between, so
            # decline and let the operator resnapshot.
            logging.getLogger(__name__).warning(
                "Informix schema-node boundary %s for '%s' precedes the minimum retained "
                "LSN %s; the delete channel cannot bootstrap from it",
                boundary,
                table.exposed_name,
                minimum,
            )
            return None, None
        current = self._bridge.current_lsn()
        if boundary > current:
            # A boundary ahead of the server's current position means the logical
            # log was reinitialized (observed in production: the log reset to
            # uniqid 0-4 while stored boundaries sat at uniqid 6 and 12). The
            # position will not be reached again, so this cannot be waited out.
            #
            # Warn rather than decline silently. The retention branch above
            # already warns for the symmetric case, and without this the delete
            # channel stalls permanently with no operator-visible signal: the
            # caller returns an empty offset for Lakeflow to retry, so the flow
            # reports RUNNING forever while replicating no deletes.
            #
            # This record can never be repaired in place: the node id derives from
            # the table identity and schema fingerprint alone, so a full refresh
            # recomputes the same id, finds the write-once head already present,
            # and leaves this start_lsn untouched. Declining would therefore stall
            # the delete channel until an operator noticed.
            #
            # Bootstrap from the server's *current* position, which is safe here in a
            # way it is NOT safe for the retention branch above. That
            # branch declines because its boundary was once valid and deletes
            # committed between it and ``minimum`` were readable, so advancing
            # would silently skip them. Here the boundary belongs to a previous log
            # incarnation: nothing in the *current* log has ever been consumed by
            # this channel, so starting anywhere in it cannot skip a readable delete.
            #
            # Deliberately NOT the oldest retained position. minimum_lsn is
            # MIN(uniqid) << 32 -- byte 0 of the oldest surviving log, which is the
            # next log Informix recycles. Resuming there decodes whatever table's
            # rows have since reused that space: observed in production as a
            # UnicodeDecodeError on 0xf0 partway through a foreign row. current_lsn
            # has the same "nothing consumed yet" property with none of that
            # fragility. Re-reading is harmless because the delete channel
            # is idempotent -- ``apply_as_deletes`` keyed on the primary key and
            # ordered by ``sequence_by`` drops a delete whose sequence precedes the
            # row currently holding that key, so a replayed delete can neither
            # remove a newer row nor apply twice.
            #
            # Deletes that existed only in the pre-reset log are unrecoverable by
            # any reader, so warn: this recovers the channel, it does not recover
            # that window.
            logging.getLogger(__name__).warning(
                "Informix schema-node boundary %s for '%s' is ahead of the current LSN %s, "
                "so the source's logical log was reinitialized. Bootstrapping the delete "
                "channel from the current LSN instead (not the oldest retained LSN %s, "
                "which is byte 0 of the log Informix recycles next); deletes committed "
                "before the reinitialization are no longer in the log and cannot be "
                "replicated, so run a full refresh if the destination must match the "
                "source exactly.",
                boundary,
                table.exposed_name,
                current,
                minimum,
            )
            return current, schema_id
        return boundary, schema_id

    def _find_immutable_schema_record(
        self, table: Table, schema_id: str, scope: str
    ) -> dict[str, object] | None:
        initialization = self._read_immutable_head(
            self._immutable_namespace(table, "initialization", scope)
        )
        if initialization is not None:
            self._validate_immutable_record_header(
                initialization, "initialization", table.exposed_name
            )
            schema = initialization.get("schema")
            if isinstance(schema, dict) and schema.get("id") == schema_id:
                return {
                    "created_at": initialization.get("created_at", time.time()),
                    "schema": schema,
                }
        # A checkpoint is emitted only after its exact schema-node head is
        # committed. Do not scan unbounded, unrelated transition history to
        # compensate for an externally removed historical node.
        return None

    def _lakebase_connection(self) -> Any:
        """Return this connector's Postgres connection, provisioning on first use.

        The connector and its bridge each keep their own connection: the bridge's
        serves slot operations while a read is in flight, and sharing one across
        both would serialise state reads behind slot maintenance.
        """

        connection = getattr(self, "_lakebase_conn", None)
        if connection is not None:
            try:
                if not connection.closed:
                    return connection
            except Exception:
                pass
        state = getattr(self, "_lakebase_state", None)
        if state is None:
            identity = "\0".join(
                (
                    self.options.get("hostname", "").strip().rstrip(".").casefold(),
                    str(int(self.options.get("port", "9088"))),
                    self.options.get("server", "").strip().casefold(),
                )
            )
            state = LakebaseState(self.options, identity)
            state.provision()
            self._lakebase_state = state
        connection = state.connect()
        self._lakebase_conn = connection
        return connection

    def _immutable_namespace(self, table: Table, *parts: str) -> str:
        """Build the record key identifying one piece of per-table state.

        Keys are ``<connection>/<table>/<part>/...`` where both leading
        components are hashes of the Informix identity, so a key is reproducible
        by any worker without coordination. A component that is not already
        restricted to a safe character set is hashed, which keeps a scope or
        schema id containing a separator from forging a different key.
        """

        connection_key, table_key = self._table_state_keys(table)
        safe = [connection_key, table_key]
        for part in parts:
            value = str(part)
            if not re.fullmatch(r"[A-Za-z0-9_.@-]+", value):
                value = hashlib.sha256(value.encode()).hexdigest()
            safe.append(value)
        return "/".join(safe)

    def _touch_table_state(self, table: Table) -> None:
        """Mark a pipeline/table active and opportunistically run daily state GC."""

        scope = self._pipeline_scope()
        if "_@_" not in scope:
            return
        pipeline_id, _, update_id = scope.partition("_@_")
        if not pipeline_id or not update_id:
            return
        try:
            retention_days = int(
                self.options.get("state.gc.retention.days", _DEFAULT_STATE_GC_RETENTION_DAYS)
            )
            interval_hours = float(
                self.options.get("state.gc.interval.hours", _DEFAULT_STATE_GC_INTERVAL_HOURS)
            )
        except (TypeError, ValueError) as error:
            raise ValueError(
                "Options 'state.gc.retention.days' and 'state.gc.interval.hours' must be numeric"
            ) from error
        if retention_days < 1 or not math.isfinite(interval_hours) or interval_hours <= 0:
            raise ValueError(
                "Option 'state.gc.retention.days' must be >= 1 and "
                "'state.gc.interval.hours' must be > 0"
            )
        cache_key = (table.identity, pipeline_id)
        now = time.monotonic()
        touch_seconds = interval_hours * 3600
        if now - self._activity_touched.get(cache_key, -math.inf) < touch_seconds:
            return
        prefix = self._immutable_namespace(table)
        pipeline_key = hashlib.sha256(pipeline_id.encode()).hexdigest()[:32]
        connection = self._lakebase_connection()
        namespace = self._lakebase_state_namespace()
        touch_table_activity(
            connection,
            namespace,
            f"{prefix}/activity/{pipeline_key}",
            prefix,
            pipeline_id,
            touch_seconds,
        )
        self._activity_touched[cache_key] = now
        deleted = collect_stale_table_state(
            connection,
            namespace,
            "state-gc/table-activity",
            retention_days,
            interval_hours,
        )
        if deleted:
            logging.getLogger(__name__).info(
                "Collected %s stale Informix Lakebase state records", deleted
            )
        drop_projects = self.options.get("state.gc.drop.unused.projects", "false").lower()
        if drop_projects not in {"true", "false"}:
            raise ValueError("Option 'state.gc.drop.unused.projects' must be true or false")
        if drop_projects == "true":
            try:
                project_retention_days = int(
                    self.options.get(
                        "state.gc.project.retention.days",
                        _DEFAULT_STATE_GC_PROJECT_RETENTION_DAYS,
                    )
                )
            except (TypeError, ValueError) as error:
                raise ValueError(
                    "Option 'state.gc.project.retention.days' must be an integer"
                ) from error
            if project_retention_days < retention_days:
                raise ValueError(
                    "Option 'state.gc.project.retention.days' must be >= state.gc.retention.days"
                )
            state = getattr(self, "_lakebase_state", None)
            if state is not None:
                state.collect_unused_projects(project_retention_days, interval_hours)

    def _cleanup_previous_update_scopes(
        self, table: Table, checkpoint: dict[str, Any] | None = None
    ) -> None:
        """Clean obsolete scopes without deleting state referenced by Spark."""

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
        deleted = delete_obsolete_scoped_state_records(
            self._lakebase_connection(),
            self._lakebase_state_namespace(),
            self._immutable_namespace(table),
            f"{pipeline_id}_@_",
            current,
            retained_scope,
        )
        self._cleaned_update_scopes.add(cache_key)
        if deleted:
            logging.getLogger(__name__).info(
                "Removed %s obsolete Informix update-scoped records for table=%s",
                deleted,
                table.exposed_name,
            )

    def _lakebase_state_namespace(self) -> str:
        """Namespace state records by the Informix *database*, not the endpoint.

        Slots are per server process because capacity is a property of that
        process, but a table's state belongs to one database on it, so two
        databases on one server never share a record.
        """

        identity = "\0".join(
            (
                "v2",
                self.options.get("hostname", "").strip().rstrip(".").casefold(),
                str(int(self.options.get("port", "9088"))),
                self.options.get("server", "").strip(),
                self.options.get("database", "").strip(),
            )
        )
        return hashlib.sha256(identity.encode()).hexdigest()[:24]

    def _read_immutable_head(self, namespace: str) -> dict[str, object] | None:
        """Return the elected record for a key, or None when unwritten."""

        return read_state_record(
            self._lakebase_connection(), self._lakebase_state_namespace(), namespace
        )

    def _publish_immutable_head(
        self,
        namespace: str,
        record: dict[str, object],
        *,
        record_type: str = "generic",
    ) -> dict[str, object]:
        """Elect exactly one immutable record for a key and return the winner.

        ``INSERT ... ON CONFLICT DO NOTHING RETURNING`` is the election. The
        return value is always the *elected* record, which may be another
        writer's: a record is shared truth for its key, so a loser must adopt the
        winner rather than assume its own value took effect.
        """

        return publish_state_record(
            self._lakebase_connection(),
            self._lakebase_state_namespace(),
            namespace,
            {
                **record,
                "format_version": _IMMUTABLE_STATE_VERSION,
                "record_type": record_type,
            },
            record_type=record_type,
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
            raise InformixError(
                f"Unsupported or mismatched Informix immutable {expected_type} record "
                f"for {context}"
            )

    def _validate_schema_node_winner(
        self,
        winner: dict[str, object],
        expected_schema: dict[str, object],
        table: Table,
    ) -> None:
        self._validate_immutable_record_header(winner, "schema-node", table.exposed_name)
        schema = winner.get("schema")
        if (
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
            raise InformixError(
                f"Conflicting immutable schema-node winner for '{table.exposed_name}'"
            )
        if (
            _table_from_schema_state(schema, table.database).native_identity
            != table.native_identity
        ):
            raise InformixError(f"Immutable schema-node table mismatch for '{table.exposed_name}'")

    @staticmethod
    def _immutable_lsn(record: dict[str, object], field: str, context: str) -> int:
        try:
            return _strict_lsn(record.get(field), field)
        except (TypeError, ValueError) as error:
            raise InformixError(
                f"Invalid {field} in Informix immutable record for {context}"
            ) from error

    @staticmethod
    def _fsync_directory(path: str) -> None:
        _fsync_directory_path(path)

    def _table_map(self, refresh: bool = False) -> dict[str, Table]:
        if self._tables is None or not self._tables_complete or refresh:
            result: dict[str, Table] = {}
            database = self.options.get("database", "")
            for raw in self._bridge.list_tables():
                table = Table.parse(raw, database)
                if _eligible(table) and self._selected(table):
                    if table.exposed_name in result:
                        raise InformixError(f"Duplicate exposed table name {table.exposed_name}")
                    result[table.exposed_name] = table
            self._tables = result
            self._tables_complete = True
        return self._tables

    def _selected(self, table: Table) -> bool:
        include = _patterns(self.options.get("table.include.list") or self.options.get("tables"))
        exclude = _patterns(self.options.get("table.exclude.list"))
        names = (table.identity, table.exposed_name, table.native_identity)
        return (
            not include or any(fnmatch.fnmatchcase(n, p) for n in names for p in include)
        ) and not any(fnmatch.fnmatchcase(n, p) for n in names for p in exclude)

    def _table(self, name: str, options: dict[str, str], refresh: bool = False) -> Table:
        exposed = options.get("qualified_source_table", name)
        if self._tables is not None and exposed in self._tables and not refresh:
            return self._apply_primary_key_override(self._tables[exposed], options)
        parts = _split_identity(exposed)
        if len(parts) == 2 and all(_IDENTIFIER.fullmatch(part) for part in parts):
            database = self.options.get("database", "")
            table = Table.parse(
                self._bridge.get_table(_join_identity(database, parts[0], parts[1])),
                database,
            )
            if not _eligible(table) or not self._selected(table):
                raise ValueError(f"Unknown or excluded Informix table '{exposed}'")
            if self._tables is None:
                self._tables = {}
            self._tables[exposed] = table
            return self._apply_primary_key_override(table, options)
        tables = self._table_map(refresh=refresh)
        if exposed not in tables:
            raise ValueError(f"Unknown or excluded Informix table '{exposed}'")
        return self._apply_primary_key_override(tables[exposed], options)


# Conventional alias used by some connector loaders.
InformixConnect = InformixLakeflowConnect


def _load_factory(path: str) -> Callable[[dict[str, str]], InformixBridge]:
    module_name, separator, attribute = path.partition(":")
    if not separator:
        module_name, separator, attribute = path.rpartition(".")
    if not module_name or not attribute:
        raise ValueError("bridge.factory must be 'module:callable'")
    factory = getattr(importlib.import_module(module_name), attribute)
    if not callable(factory):
        raise TypeError(f"Bridge factory {path!r} is not callable")
    return factory


_CATALOG_TYPES = {
    0: "CHAR",
    1: "SMALLINT",
    2: "INTEGER",
    3: "FLOAT",
    4: "SMALLFLOAT",
    5: "DECIMAL",
    6: "SERIAL",
    7: "DATE",
    8: "MONEY",
    10: "DATETIME",
    11: "BYTE",
    12: "TEXT",
    13: "VARCHAR",
    14: "INTERVAL",
    15: "NCHAR",
    16: "NVARCHAR",
    17: "INT8",
    18: "SERIAL8",
    19: "SET",
    20: "MULTISET",
    21: "LIST",
    22: "ROW",
    23: "COLLECTION",
    40: "UDT_VAR",
    41: "UDT_FIXED",
    43: "LVARCHAR",
    45: "BOOLEAN",
    52: "BIGINT",
    53: "BIGSERIAL",
    101: "BLOB",
    102: "CLOB",
}


def _field(row: Any, name: str, index: int) -> Any:
    if isinstance(row, dict):
        if name in row:
            return row[name]
        lowered = {str(key).lower(): value for key, value in row.items()}
        if name.lower() in lowered:
            return lowered[name.lower()]
        # Informix names unaliased routine results after the full expression.
        # Lifecycle calls return a single scalar, so retain the positional
        # fallback used for tuple rows when a stable label is unavailable.
        return tuple(row.values())[index]
    return row[index]


def _catalog_column(row: Any) -> dict[str, Any]:
    name = str(_field(row, "colname", 0))
    raw_type = int(_field(row, "coltype", 1))
    length = int(_field(row, "collength", 2))
    base_type = raw_type & 0xFF
    type_name = _CATALOG_TYPES.get(base_type)
    if type_name is None:
        raise InformixError(f"Unsupported Informix catalog coltype {base_type} for {name}")
    if type_name in {"UDT_VAR", "UDT_FIXED"}:
        if isinstance(row, dict):
            lowered = {str(key).lower(): value for key, value in row.items()}
            extended_name = str(lowered.get("extended_name") or "").strip().upper()
            extended_owner = str(lowered.get("extended_owner") or "").strip().upper()
        else:
            extended_name = str(row[6] or "").strip().upper() if len(row) > 6 else ""
            extended_owner = str(row[7] or "").strip().upper() if len(row) > 7 else ""
        builtin_extended_types = {
            ("UDT_VAR", "INFORMIX", "LVARCHAR"): "LVARCHAR",
            ("UDT_FIXED", "INFORMIX", "BOOLEAN"): "BOOLEAN",
        }
        type_name = builtin_extended_types.get(
            (type_name, extended_owner, extended_name), type_name
        )
    if type_name == "DATETIME":
        # syscolumns.collength stores the packed width in its high byte and
        # start/end qualifier nibbles in its low byte.  CDC descriptors use
        # the native extended-id layout instead: start in bits 8..11 and end in
        # bits 0..3.  For example, live YEAR TO FRACTION(5) is 0x130f in the
        # catalog and must become 0x000f for the native row decoder.
        encoded_qualifier = length & 0xFF
        length = ((encoded_qualifier >> 4) << 8) | (encoded_qualifier & 0x0F)
    precision = scale = None
    if type_name in {"DECIMAL", "MONEY"}:
        precision, scale = (length >> 8) & 0xFF, length & 0xFF
    unsupported = {
        "BYTE",
        "TEXT",
        "BLOB",
        "CLOB",
        "INTERVAL",
        "NCHAR",
        "SET",
        "MULTISET",
        "LIST",
        "ROW",
        "COLLECTION",
        "UDT_VAR",
        "UDT_FIXED",
    }
    cdc_supported = type_name not in unsupported
    if type_name == "DATETIME":
        cdc_supported = _datetime_qualifier_supported(length)
    return {
        "name": name,
        "type_name": type_name,
        "nullable": not bool(raw_type & 0x100),
        "length": length,
        "precision": precision,
        "scale": scale,
        "cdc_supported": cdc_supported,
    }


def _column_descriptor(raw: dict[str, Any]) -> ColumnDescriptor:
    return ColumnDescriptor(
        name=str(raw["name"]),
        type_name=str(raw["type_name"]),
        length=int(raw.get("length") or 0),
        precision=_optional_int(raw.get("precision")),
        scale=_optional_int(raw.get("scale")),
        encoding=str(raw.get("encoding") or "utf-8"),
    )


def _expect_zero(rows: list[Any], operation: str) -> None:
    if not rows:
        raise InformixError(f"{operation} returned no status")
    status = int(_field(rows[0], "status", 0))
    if status != 0:
        raise InformixError(f"{operation} failed with status {status}")


def _is_timeout_error(error: BaseException | None) -> bool:
    """Report whether ``error`` (or anything it wraps) is a socket read timeout.

    A SQLI socket read timeout surfaces as a bare ``TimeoutError`` (``socket.timeout``
    is an alias of it on Python 3.10+); it may also travel as the ``__cause__`` of a
    wrapping connector error, so walk the cause chain rather than checking the top
    exception alone.
    """

    seen: set[int] = set()
    while error is not None and id(error) not in seen:
        if isinstance(error, TimeoutError):
            return True
        seen.add(id(error))
        error = error.__cause__ or error.__context__
    return False


def _optional_int(value: Any) -> int | None:
    return None if value is None or value == "" else int(value)


def _strict_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not (
        isinstance(value, int) or (isinstance(value, str) and re.fullmatch(r"[+-]?\d+", value))
    ):
        raise ValueError(f"{name} must be an integer, not {value!r}")
    return int(value)


def _strict_timestamp(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError(f"{name} must be a timestamp, not {value!r}")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite, not {value!r}")
    return result


def _strict_lsn(value: object, name: str) -> int:
    result = _strict_int(value, name)
    if not 0 <= result < 1 << 64:
        raise ValueError(f"{name} is outside the unsigned 64-bit LSN domain")
    return result


def _strict_tx_id(value: object, name: str = "tx_id") -> int:
    result = _strict_int(value, name)
    if not -(1 << 31) <= result < 1 << 32:
        raise ValueError(f"{name} is outside the native 32-bit transaction-ID domain")
    return result + (1 << 32) if result < 0 else result


def _patterns(value: str | None) -> tuple[str, ...]:
    return tuple(part.strip() for part in (value or "").split(",") if part.strip())


def _eligible(table: Table) -> bool:
    return not (
        table.owner.lower().startswith("sys")
        or table.name.lower().startswith("sys")
        or table.database.lower() == "syscdcv1"
    )


def _cdc_streamable(table: Table) -> bool:
    """Report whether Informix can capture this table's changes at all.

    Split from :func:`_cdc_capable` because the two questions are different and
    conflating them made every key-less table snapshot-only. ``cdc_startcapture``
    takes ``(session, table, columns, full_row_logging)`` -- no key -- so the
    logical log records inserts, updates, and deletes for any table with full-row
    logging enabled. A primary key is needed to *merge* a change into the
    destination, not to read it.

    Missing/placeholder values are unsafe in Lakeflow table rows, especially for
    binary and complex types, so a table containing uncaptured columns is still
    excluded here rather than silently nulling those values.
    """

    return all(column.cdc_supported for column in table.columns)


def _cdc_capable(table: Table) -> bool:
    """Report whether this table can be merged into a keyed destination.

    Requires a primary key on top of :func:`_cdc_streamable`: both the upsert
    channel's ``apply_changes`` target and the delete channel's row identification
    need one, as does snapshot keyset pagination.
    """

    return bool(table.primary_keys) and _cdc_streamable(table)


def _snapshot_unsupported_columns(table: Table) -> tuple[Column, ...]:
    """Return types whose ordinary SQLI row representation is not implemented."""

    unsupported = {
        "BYTE",
        "TEXT",
        "BLOB",
        "CLOB",
        "INTERVAL",
        "NCHAR",
        "SET",
        "MULTISET",
        "LIST",
        "ROW",
        "COLLECTION",
        "UDT_VAR",
        "UDT_FIXED",
    }
    return tuple(
        column
        for column in table.columns
        if column.type_name in unsupported
        or (
            column.type_name == "DATETIME" and not _datetime_qualifier_supported(column.length or 0)
        )
    )


def _datetime_qualifier_supported(qualifier: int) -> bool:
    start, end = (qualifier >> 8) & 0xF, qualifier & 0xF
    fields = {0, 2, 4, 6, 8, 10}
    return start in fields and end in {*fields, 11, 12, 13, 14, 15} and end >= start


_VARIABLE_DECIMAL_DEFAULT = "decimal(38,18)"
_DECIMAL_TYPE_RE = re.compile(r"^decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$")


def _parse_variable_decimal_type(spec: str, *, option: str) -> tuple[str, int | None, int | None]:
    """Normalize a variable-DECIMAL target-type spec.

    Accepts ``string``, ``double``, ``integer`` or ``decimal(p,s)`` and returns a
    ``(kind, precision, scale)`` tuple where ``kind`` is one of ``string``,
    ``double``, ``integer`` or ``decimal``.  ``precision``/``scale`` are populated
    only for ``decimal``.
    """

    normalized = spec.strip().lower()
    if normalized in {"string", "double", "integer"}:
        return normalized, None, None
    match = _DECIMAL_TYPE_RE.match(normalized)
    if match:
        precision, scale = int(match.group(1)), int(match.group(2))
        if not (1 <= precision <= 38 and 0 <= scale <= precision):
            raise ValueError(
                f"Option '{option}' decimal(p,s) requires 1<=p<=38 and 0<=s<=p, got "
                f"decimal({precision},{scale})"
            )
        return "decimal", precision, scale
    raise ValueError(
        f"Option '{option}' must be one of: string, double, integer, decimal(p,s); got {spec!r}"
    )


def _variable_decimal_column_overrides(
    options: dict[str, str],
) -> dict[str, tuple[str, int | None, int | None]]:
    """Parse ``decimal.variable.column.type`` into a per-column target map."""

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
            raise ValueError(
                "Option 'decimal.variable.column.type' entries must be "
                f"'column:type' pairs; got {entry!r}"
            )
        overrides[name] = _parse_variable_decimal_type(spec, option="decimal.variable.column.type")
    return overrides


def _variable_decimal_target(
    column: Column, options: dict[str, str]
) -> tuple[str, int | None, int | None]:
    """Resolve the target type for one variable-scale DECIMAL/NUMERIC column.

    A per-column ``decimal.variable.column.type`` entry wins over the global
    ``decimal.variable.type``; the default is ``decimal(38,18)``.
    """

    overrides = _variable_decimal_column_overrides(options)
    if column.name in overrides:
        return overrides[column.name]
    return _parse_variable_decimal_type(
        options.get("decimal.variable.type", _VARIABLE_DECIMAL_DEFAULT),
        option="decimal.variable.type",
    )


def _validate_variable_decimal_options(options: dict[str, str]) -> None:
    """Eagerly validate the variable-DECIMAL options so bad specs fail fast."""

    _parse_variable_decimal_type(
        options.get("decimal.variable.type", _VARIABLE_DECIMAL_DEFAULT),
        option="decimal.variable.type",
    )
    _variable_decimal_column_overrides(options)


def _variable_decimal_spark_type(kind: str, precision: int | None, scale: int | None):
    if kind == "string":
        return StringType()
    if kind == "double":
        return DoubleType()
    if kind == "integer":
        return LongType()
    return DecimalType(precision, scale)


def _is_variable_scale_decimal(column: Column) -> bool:
    name = column.type_name.split("(", 1)[0].strip()
    return name in {"DECIMAL", "NUMERIC"} and column.scale == 0xFF


def _ensure_materializable(table: Table, options: dict[str, str] | None = None) -> None:
    unsupported = _snapshot_unsupported_columns(table)
    if unsupported:
        details = ", ".join(f"{column.name} ({column.type_name})" for column in unsupported)
        raise InformixError(
            f"Table '{table.exposed_name}' contains columns that the pure-Python SQLI "
            f"snapshot decoder cannot materialize: {details}"
        )
    options = options or {}
    _validate_variable_decimal_options(options)
    for column in table.columns:
        if column.type_name in {"DECIMAL", "NUMERIC", "MONEY"}:
            variable_scale = column.type_name in {"DECIMAL", "NUMERIC"} and (column.scale == 0xFF)
            valid_fixed = (
                column.precision is not None
                and column.scale is not None
                and 1 <= column.precision <= 38
                and 0 <= column.scale <= column.precision
            )
            # A variable-scale DECIMAL(p) always maps to a materializable target
            # (string, double, integer, or decimal(p,s)) chosen by the
            # decimal.variable.type / decimal.variable.column.type options.
            if valid_fixed or variable_scale:
                continue
            raise InformixError(
                f"Table '{table.exposed_name}' has invalid {column.type_name} metadata for "
                f"column {column.name}: precision={column.precision}, scale={column.scale}"
            )


def _spark_type(column: Column, options: dict[str, str] | None = None):
    name = column.type_name.split("(", 1)[0].strip()
    if name in {"SMALLINT", "INT2"}:
        # The framework row converter does not support Spark ShortType. Widen
        # Informix's signed 16-bit value to IntegerType without losing data.
        return IntegerType()
    if name in {"INTEGER", "INT", "SERIAL"}:
        return IntegerType()
    if name in {"BIGINT", "INT8", "BIGSERIAL", "SERIAL8"}:
        return LongType()
    if name in {"REAL", "SMALLFLOAT"}:
        return FloatType()
    if name in {"FLOAT", "DOUBLE", "DOUBLE PRECISION"}:
        return DoubleType()
    if name in {"DECIMAL", "NUMERIC", "MONEY"}:
        precision = (
            column.precision if column.precision is not None else (19 if name == "MONEY" else 38)
        )
        scale = column.scale if column.scale is not None else (2 if name == "MONEY" else 0)
        if name in {"DECIMAL", "NUMERIC"} and scale == 0xFF:
            # Variable-scale DECIMAL(p) is decimal floating point: p is the count
            # of significant digits and the exponent floats, so the magnitude is
            # not bounded by p. Map it to the target chosen by the
            # decimal.variable.type / decimal.variable.column.type options.
            kind, target_precision, target_scale = _variable_decimal_target(column, options or {})
            return _variable_decimal_spark_type(kind, target_precision, target_scale)
        if 1 <= precision <= 38 and 0 <= scale <= precision:
            return DecimalType(precision, scale)
        return StringType()
    if name == "DATE":
        return DateType()
    if name.startswith("DATETIME") or name == "TIMESTAMP":
        start, end = ((column.length or 0) >> 8) & 0xF, (column.length or 0) & 0xF
        return TimestampType() if start == 0 and end >= 4 else StringType()
    if name in {"BOOLEAN", "BOOL"}:
        return BooleanType()
    if name in {"BYTE", "BLOB", "BINARY", "VARBINARY"}:
        return BinaryType()
    return StringType()


def _operation(record: dict[str, Any]) -> str:
    return str(record.get("op") or record.get("operation") or record.get("type") or "").upper()


def _lsn(record: dict[str, Any]) -> int:
    value = record.get("lsn", record.get("sequence", record.get("sequence_id")))
    if value is None:
        raise InformixError(f"CDC record has no LSN: {record!r}")
    try:
        return _strict_lsn(value, "CDC record LSN")
    except ValueError as error:
        raise InformixError(f"CDC record has an invalid LSN: {record!r}") from error


def _tx_id(record: dict[str, Any]) -> int:
    value = record.get("tx_id", record.get("transaction_id"))
    if value is None:
        raise InformixError(f"CDC record has no transaction ID: {record!r}")
    try:
        return _strict_tx_id(value, "CDC transaction ID")
    except ValueError as error:
        raise InformixError(f"CDC record has an invalid transaction ID: {record!r}") from error


def _normalise_record(raw: dict[str, Any]) -> dict[str, Any]:
    record = dict(raw)
    record["op"] = _operation(record)
    if record["op"] not in {"METADATA", "ERROR"}:
        record["lsn"] = _lsn(record)
    if record["op"] not in {"TIMEOUT", "METADATA", "ERROR"}:
        record["tx_id"] = _tx_id(record)
    return record


def _committed_transactions(records: Sequence[dict[str, Any]]) -> list[CommittedTransaction]:
    return _transaction_batch(records)[0]


def _transaction_batch(
    records: Sequence[dict[str, Any]],
) -> tuple[list[CommittedTransaction], bool, int | None]:
    buffer = TransactionBuffer()
    result = []
    timed_out = False
    for record in records:
        if _operation(record) == "TIMEOUT":
            timed_out = True
        committed = buffer.feed(record)
        if committed is not None:
            result.append(committed)
    # Informix may interleave active transactions without returning their records
    # in one globally monotonic LSN sequence. Transaction.advance() still validates
    # monotonicity within each transaction. Order completed transactions by their
    # commit position before the caller advances its checkpoint, otherwise a higher
    # commit returned first could checkpoint past a lower commit returned later.
    result.sort(key=lambda tx: (tx.commit_lsn, tx.tx_id))
    # Open transactions are intentionally discarded.  The returned offset
    # remains before their BEGIN so a finite next call safely replays them.
    open_begin = min((tx.begin_lsn for tx in buffer.open.values()), default=None)
    return result, timed_out and not buffer.open, open_begin


def _recover(
    transactions: Sequence[CommittedTransaction], checkpoint: dict[str, Any]
) -> list[CommittedTransaction]:
    commit = int(checkpoint["commit_lsn"])
    # Offsets are transaction-atomic. Replaying from the oldest open BEGIN can
    # reproduce transactions already checkpointed, but records from a newly
    # committed transaction must never be filtered using another transaction's
    # commit/change LSN.
    return [tx for tx in transactions if tx.commit_lsn > commit]


def _client_encoding(options: dict[str, str]) -> str:
    # Spark lowercases option keys, so ``CLIENT_LOCALE`` arrives as
    # ``client_locale``; check that first, then the original-case / dotted forms.
    locale = (
        options.get("client_locale")
        or options.get("CLIENT_LOCALE")
        or options.get("client.locale")
        or "en_US.utf8"
    )
    return informix_locale_encoding(locale)


def _capture_descriptor(table: Table, encoding: str = "utf-8") -> dict[str, Any]:
    return {
        "identity": table.native_identity,
        "logical_identity": table.identity,
        "columns": [column.name for column in table.columns if column.cdc_supported],
        "descriptors": [
            {
                "name": column.name,
                "type_name": column.type_name,
                "length": column.length or column.precision or 0,
                "precision": column.precision,
                "scale": column.scale,
                "encoding": encoding,
            }
            for column in table.columns
            if column.cdc_supported
        ],
    }


def _project_transaction(
    tx: CommittedTransaction, table: Table, deletes: bool
) -> list[dict[str, Any]]:
    output = []
    for record in tx.records:
        if not _record_matches(record, table):
            continue
        op = _operation(record)
        if op == "TRUNCATE":
            raise UnsupportedChangeError(
                f"TRUNCATE on {table.exposed_name} cannot be represented as keyed Lakeflow deletes"
            )
        before = record.get("before", record.get("row") if op == "DELETE" else None)
        after = record.get("after", record.get("row") if op == "INSERT" else None)
        if deletes:
            if op == "DELETE":
                output.append(_shape_delete(before, table, record, tx))
            elif op == "UPDATE" and _key(before, table) != _key(after, table):
                output.append(_shape_delete(before, table, record, tx))
        elif op in {"INSERT", "UPDATE"}:
            output.append(_shape_change(after, record, tx, "u" if op == "UPDATE" else "c"))
    return output


def _record_matches(record: dict[str, Any], table: Table) -> bool:
    identity = str(record.get("table") or record.get("identity") or "")
    return not identity or identity in {
        table.name,
        table.exposed_name,
        table.identity,
        table.native_identity,
    }


def _key(row: dict[str, Any] | None, table: Table) -> tuple[Any, ...]:
    if row is None:
        raise InformixError(f"Missing before/after image for {table.exposed_name}")
    return tuple(row.get(pk) for pk in table.primary_keys)


def _shape_change(row, record, tx, op):
    if row is None:
        raise InformixError("CDC upsert has no after image")
    result = _framework_row(row)
    result.update(
        {
            CURSOR: _sortable_lsn(_lsn(record)),
            COMMIT_LSN: _sortable_lsn(tx.commit_lsn),
            TX_ID: tx.tx_id,
            OP: op,
        }
    )
    return result


def _shape_delete(row, table, record, tx):
    if row is None:
        raise InformixError("CDC delete has no before image; full-row logging is required")
    result = {column.name: None for column in table.columns}
    for pk in table.primary_keys:
        if row.get(pk) is None:
            raise InformixError(f"CDC delete has no primary-key value for {pk}")
        result[pk] = _framework_value(row[pk])
    result.update(
        {
            CURSOR: _sortable_lsn(_lsn(record)),
            COMMIT_LSN: _sortable_lsn(tx.commit_lsn),
            TX_ID: tx.tx_id,
            OP: "d",
        }
    )
    return result


def _coerce_variable_decimal_value(
    value: Any,
    column: Column,
    target: tuple[str, int | None, int | None],
) -> Any:
    """Coerce one variable-scale DECIMAL value to its resolved target type."""

    kind, precision, scale = target
    if not isinstance(value, Decimal):
        try:
            value = Decimal(str(value))
        except (InvalidOperation, ValueError, TypeError) as error:
            raise InformixError(
                f"column '{column.name}' value {value!r} is not a valid decimal"
            ) from error
    if kind == "string":
        return format(value, "f")
    if kind == "double":
        return float(value)
    if kind == "integer":
        # Truncate toward zero (ANSI DECIMAL(p,0) semantics) then bounds-check.
        truncated = int(value.to_integral_value(rounding=ROUND_DOWN))
        if not -(1 << 63) <= truncated < (1 << 63):
            raise InformixError(
                f"column '{column.name}' value {format(value, 'f')} exceeds the 64-bit "
                "range of integer"
            )
        return truncated
    # decimal(p,s): quantize to the target scale and confirm it fits precision.
    quantum = Decimal(1).scaleb(-(scale or 0))
    try:
        quantized = value.quantize(quantum, rounding=ROUND_DOWN)
    except InvalidOperation as error:
        raise InformixError(
            f"column '{column.name}' value {format(value, 'f')} cannot be represented as "
            f"decimal({precision},{scale})"
        ) from error
    digits = quantized.as_tuple()
    integer_digits = max(len(digits.digits) + digits.exponent, 0)
    if integer_digits > (precision or 0) - (scale or 0):
        raise InformixError(
            f"column '{column.name}' value {format(value, 'f')} exceeds decimal("
            f"{precision},{scale}) (integer digits {integer_digits} > {(precision or 0) - (scale or 0)})"
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


def _shape_snapshot(
    row: dict[str, Any],
    lsn: int,
    table: Table | None = None,
    options: dict[str, str] | None = None,
) -> dict[str, Any]:
    converted = (
        _coerce_variable_decimal_values(row, table, options or {}) if table is not None else row
    )
    result = _framework_row(converted)
    result.update(
        {CURSOR: _sortable_lsn(lsn), COMMIT_LSN: _sortable_lsn(lsn), TX_ID: None, OP: "r"}
    )
    return result


def _validate_shaped_rows(
    rows: list[dict[str, Any]],
    table: Table,
    options: dict[str, str],
    *,
    context: str,
    primary_keys_only: bool = False,
) -> None:
    """Reject values that the declared Spark schema cannot materialize."""

    columns = (
        [column for column in table.columns if column.name in table.primary_keys]
        if primary_keys_only
        else table.columns
    )
    for row_index, row in enumerate(rows):
        for column in columns:
            value = row.get(column.name)
            if value is None:
                if not column.nullable:
                    raise InformixError(
                        f"{context} row {row_index} has null for non-nullable column "
                        f"'{column.name}'"
                    )
                continue
            spark_type = _spark_type(column, options)
            valid = False
            if isinstance(spark_type, IntegerType):
                valid = (
                    isinstance(value, int)
                    and not isinstance(value, bool)
                    and -(1 << 31) <= value < 1 << 31
                )
            elif isinstance(spark_type, LongType):
                valid = (
                    isinstance(value, int)
                    and not isinstance(value, bool)
                    and -(1 << 63) <= value < 1 << 63
                )
            elif isinstance(spark_type, (FloatType, DoubleType)):
                valid = isinstance(value, (int, float)) and not isinstance(value, bool)
            elif isinstance(spark_type, DecimalType):
                if isinstance(value, Decimal) and value.is_finite():
                    normalized = value.normalize()
                    digits = len(normalized.as_tuple().digits)
                    exponent = normalized.as_tuple().exponent
                    integer_digits = max(digits + exponent, 0)
                    fractional_digits = max(-exponent, 0)
                    valid = (
                        integer_digits <= spark_type.precision - spark_type.scale
                        and fractional_digits <= spark_type.scale
                    )
            elif isinstance(spark_type, StringType):
                valid = isinstance(value, str)
            elif isinstance(spark_type, DateType):
                if isinstance(value, str):
                    try:
                        date.fromisoformat(value)
                        valid = True
                    except ValueError:
                        pass
            elif isinstance(spark_type, TimestampType):
                if isinstance(value, str):
                    try:
                        parsed = datetime.fromisoformat(value)
                        # A naive wall-clock value or an explicit UTC offset both
                        # materialize cleanly. The connector attaches UTC to a year-1
                        # DATETIME so PySpark's internal astimezone() does not overflow
                        # on a value adjacent to datetime.min; a non-UTC offset never
                        # originates here.
                        offset = parsed.utcoffset()
                        valid = offset is None or offset == timedelta(0)
                    except ValueError:
                        pass
            elif isinstance(spark_type, BooleanType):
                valid = isinstance(value, bool)
            elif isinstance(spark_type, BinaryType):
                valid = isinstance(value, (bytes, bytearray))
            if not valid:
                raise InformixError(
                    f"{context} row {row_index} value for column '{column.name}' "
                    f"cannot materialize as {type(spark_type).__name__}"
                )


def _sortable_lsn(value: int) -> str:
    lsn = int(value)
    if not 0 <= lsn < 1 << 64:
        raise InformixError(f"Informix LSN {lsn} is outside the unsigned 64-bit decimal domain")
    return f"{lsn:0{_LSN_DECIMAL_WIDTH}d}"


def _framework_row(row: dict[str, Any]) -> dict[str, Any]:
    return {name: _framework_value(value) for name, value in row.items()}


def _framework_value(value: Any) -> Any:
    # The shared Spark Python Data Source parser accepts ISO strings for DateType
    # and TimestampType, but rejects a native datetime.date.  Normalize both
    # temporal Python objects at the connector boundary for consistent snapshot
    # and CDC behavior.
    return value.isoformat() if isinstance(value, (date, datetime)) else value


def _deep_size(value: Any, seen: set[int] | None = None) -> int:
    """Estimate retained Python container/value memory without double counting."""

    if seen is None:
        seen = set()
    identity = id(value)
    if identity in seen:
        return 0
    seen.add(identity)
    size = sys.getsizeof(value)
    if isinstance(value, dict):
        return size + sum(
            _deep_size(key, seen) + _deep_size(item, seen) for key, item in value.items()
        )
    if isinstance(value, (list, tuple, set, frozenset)):
        return size + sum(_deep_size(item, seen) for item in value)
    return size


def _encode_snapshot_stage_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        return {"$informix": "float", "value": repr(value)}
    if isinstance(value, Decimal):
        return {"$informix": "decimal", "value": str(value)}
    if isinstance(value, datetime):
        return {"$informix": "datetime", "value": value.isoformat()}
    if isinstance(value, date):
        return {"$informix": "date", "value": value.isoformat()}
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {
            "$informix": "binary",
            "value": base64.b64encode(bytes(value)).decode("ascii"),
        }
    if isinstance(value, list):
        return [_encode_snapshot_stage_value(item) for item in value]
    if isinstance(value, tuple):
        return {
            "$informix": "tuple",
            "value": [_encode_snapshot_stage_value(item) for item in value],
        }
    if isinstance(value, dict) and all(isinstance(key, str) for key in value):
        if "$informix" in value:
            raise InformixError("Snapshot row contains reserved staging key '$informix'")
        return {key: _encode_snapshot_stage_value(item) for key, item in value.items()}
    raise InformixError(f"Snapshot value of type {type(value).__name__} cannot be staged safely")


def _decode_snapshot_stage_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, list):
        return [_decode_snapshot_stage_value(item) for item in value]
    if not isinstance(value, dict):
        raise InformixError("Invalid typed value in staged snapshot page")
    kind = value.get("$informix")
    if kind is None:
        return {str(key): _decode_snapshot_stage_value(item) for key, item in value.items()}
    if set(value) != {"$informix", "value"} or not isinstance(value["value"], str):
        if kind != "tuple" or set(value) != {"$informix", "value"}:
            raise InformixError("Invalid typed value in staged snapshot page")
    payload = value["value"]
    try:
        if kind == "decimal":
            return Decimal(payload)
        if kind == "datetime":
            return datetime.fromisoformat(payload)
        if kind == "date":
            return date.fromisoformat(payload)
        if kind == "binary":
            return base64.b64decode(payload, validate=True)
        if kind == "float" and payload in {"nan", "inf", "-inf"}:
            return float(payload)
        if kind == "tuple" and isinstance(payload, list):
            return tuple(_decode_snapshot_stage_value(item) for item in payload)
    except (ValueError, TypeError, ArithmeticError) as error:
        raise InformixError("Invalid typed value in staged snapshot page") from error
    raise InformixError(f"Unknown staged snapshot value type {kind!r}")


def _offset(
    commit: int,
    change: int,
    begin: int,
    tx_id: int | None,
    phase: str,
    table: Table,
    schema_id: str,
    pipeline_scope: str,
    *,
    trigger_generation: str | None = None,
    trigger_high_water: int | str | None = None,
    incremental: dict | None = None,
) -> dict:
    offset = {
        "version": _OFFSET_VERSION,
        "commit_lsn": str(commit),
        "change_lsn": str(change),
        "begin_lsn": str(begin),
        "tx_id": tx_id,
        "phase": phase,
        "schema_fingerprint": _schema_fingerprint(table),
        "schema_id": schema_id,
        "pipeline_scope": pipeline_scope,
        "trigger_generation": trigger_generation,
        "trigger_high_water": (str(trigger_high_water) if trigger_high_water is not None else None),
    }
    if incremental is not None:
        offset["incremental"] = incremental
    return offset


def _schema_fingerprint(table: Table) -> str:
    layout = repr(
        (
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
    ).encode("utf-8")
    return hashlib.sha256(layout).hexdigest()


def _schema_state(
    table: Table,
    start_lsn: int,
    predecessor: str | None = None,
    *,
    schema_id: str | None = None,
) -> dict[str, object]:
    return {
        "id": schema_id or _schema_node_id(table),
        "fingerprint": _schema_fingerprint(table),
        "start_lsn": str(start_lsn),
        "predecessor": predecessor,
        "table": {
            "database": table.database,
            "owner": table.owner,
            "name": table.name,
            "incarnation": table.incarnation,
            "primary_keys": list(table.primary_keys),
            "columns": [
                {
                    "name": column.name,
                    "type_name": column.type_name,
                    "nullable": column.nullable,
                    "length": column.length,
                    "precision": column.precision,
                    "scale": column.scale,
                    "cdc_supported": column.cdc_supported,
                }
                for column in table.columns
            ],
        },
    }


def _schema_node_id(table: Table) -> str:
    """Return the stable identity of one physical-table schema layout."""

    identity = "\0".join((table.native_identity, _schema_fingerprint(table)))
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:32]


def _table_from_schema_state(state: dict[str, object], default_database: str) -> Table:
    raw = state.get("table")
    if not isinstance(raw, dict):
        raise InformixError("Informix shared CDC schema is missing table metadata")
    try:
        start_lsn = _strict_lsn(state["start_lsn"], "start_lsn")
    except (KeyError, TypeError, ValueError) as error:
        raise InformixError("Informix shared CDC schema has an invalid start LSN") from error
    if start_lsn < 1:
        raise InformixError("Informix shared CDC schema has an invalid start LSN")
    table = Table.parse(raw, default_database)
    if _schema_fingerprint(table) != state.get("fingerprint"):
        raise InformixError("Informix shared CDC schema fingerprint does not match metadata")
    return table


def _ensure_additive_schema_change(previous: Table, current: Table) -> None:
    if (
        previous.database,
        previous.owner,
        previous.name,
        previous.incarnation,
        previous.primary_keys,
    ) != (
        current.database,
        current.owner,
        current.name,
        current.incarnation,
        current.primary_keys,
    ):
        raise InformixError(
            f"Informix schema change for '{current.exposed_name}' changed table identity or "
            "primary keys; run a full refresh"
        )
    if len(current.columns) <= len(previous.columns):
        raise InformixError(
            f"Informix schema change for '{current.exposed_name}' is not an additive column "
            "change; run a full refresh"
        )
    if current.columns[: len(previous.columns)] != previous.columns:
        raise InformixError(
            f"Informix schema change for '{current.exposed_name}' modified, removed, or "
            "reordered existing columns; run a full refresh"
        )
    additions = current.columns[len(previous.columns) :]
    if any(not column.nullable or not column.cdc_supported for column in additions):
        raise InformixError(
            f"Informix schema change for '{current.exposed_name}' added a non-nullable or "
            "CDC-unsupported column; run a full refresh"
        )


def _validated_offset(offset: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(offset, dict):
        raise ValueError("Informix offset must be a dictionary")
    result = dict(offset)
    if "shared_state_retry_count" in result:
        result["shared_state_retry_count"] = _shared_state_retry_count(result)
    if "dropped_mount_retry_count" in result:
        result["dropped_mount_retry_count"] = _dropped_mount_retry_count(result)
    if "trigger_boundary_retry_count" in result:
        result["trigger_boundary_retry_count"] = _trigger_boundary_retry_count(result)
    if "schema_transition_retry_count" in result:
        result["schema_transition_retry_count"] = _schema_transition_retry_count(result)
    if "schema_node_fallback_retry_count" in result:
        result["schema_node_fallback_retry_count"] = _schema_node_fallback_retry_count(result)
    if "capacity_retry_count" in result:
        result["capacity_retry_count"] = _capacity_retry_count(result)
    if "capacity_pressure" in result:
        result["capacity_pressure"] = _capacity_pressure(result)
    if "backlog_rank" in result:
        result["backlog_rank"] = _backlog_rank_value(result)
    if "backlog_streak" in result:
        result["backlog_streak"] = _backlog_streak(result)
    if result.get("version") != _OFFSET_VERSION:
        raise ValueError(
            f"Informix offset version {result.get('version')!r} is unsupported; "
            "run a full refresh with this connector version"
        )
    values = {}
    for key in ("commit_lsn", "change_lsn", "begin_lsn"):
        if key not in result:
            raise ValueError(f"Informix stream offset is missing '{key}'")
        values[key] = _strict_lsn(result[key], key)
    if not values["begin_lsn"] <= values["change_lsn"] <= values["commit_lsn"]:
        raise ValueError("Informix offset must satisfy begin_lsn <= change_lsn <= commit_lsn")
    phase = result.get("phase")
    if phase not in {"snapshot", "stream"}:
        raise ValueError("Informix offset phase must be 'snapshot' or 'stream'")
    fingerprint = result.get("schema_fingerprint")
    if fingerprint is not None and (
        not isinstance(fingerprint, str)
        or len(fingerprint) != 64
        or any(character not in "0123456789abcdef" for character in fingerprint)
    ):
        raise ValueError("Informix offset has an invalid schema_fingerprint")
    schema_id = result.get("schema_id")
    if not isinstance(schema_id, str) or not re.fullmatch(r"[0-9a-f]{32}", schema_id):
        raise ValueError(
            "Informix offset has an invalid schema_id; run a full refresh with this "
            "connector version"
        )
    pipeline_scope = result.get("pipeline_scope")
    if not isinstance(pipeline_scope, str) or _PIPELINE_SCOPE.fullmatch(pipeline_scope) is None:
        raise ValueError(
            "Informix offset has an invalid pipeline_scope; run a full refresh with this "
            "connector version"
        )
    trigger_generation = result.get("trigger_generation")
    if trigger_generation is not None and (
        not isinstance(trigger_generation, str)
        or not re.fullmatch(r"[0-9a-f]{32}", trigger_generation)
    ):
        raise ValueError("Informix offset has an invalid trigger_generation")
    trigger_high_water = result.get("trigger_high_water")
    if trigger_generation is None:
        if trigger_high_water is not None:
            raise ValueError("Informix offset trigger_high_water requires trigger_generation")
    elif trigger_high_water is None:
        raise ValueError("Informix offset trigger_generation requires trigger_high_water")
    else:
        parsed_trigger_high_water = _strict_lsn(trigger_high_water, "trigger_high_water")
        result["trigger_high_water"] = str(parsed_trigger_high_water)
    tx_id = result.get("tx_id")
    if tx_id is not None:
        result["tx_id"] = _strict_tx_id(tx_id)
    if phase == "snapshot":
        if "snapshot_lsn" not in result or _strict_lsn(result["snapshot_lsn"], "snapshot_lsn") < 0:
            raise ValueError("Informix snapshot offset has an invalid snapshot_lsn")
        if any(
            values[key] != _strict_lsn(result["snapshot_lsn"], "snapshot_lsn") for key in values
        ):
            raise ValueError(
                "Informix snapshot offset requires snapshot_lsn, begin_lsn, "
                "change_lsn, and commit_lsn to be equal"
            )
        snapshot = result.get("snapshot")
        if not isinstance(snapshot, dict) or not isinstance(snapshot.get("last_pk"), list):
            raise ValueError("Informix snapshot offset is missing snapshot.last_pk")
        page_index = snapshot.get("page_index")
        if isinstance(page_index, bool) or not isinstance(page_index, int) or page_index < 0:
            raise ValueError("Informix snapshot offset has an invalid snapshot.page_index")
    incremental = result.get("incremental")
    if incremental is not None:
        if phase != "stream":
            raise ValueError("Informix incremental snapshot state requires the stream phase")
        if not isinstance(incremental, dict):
            raise ValueError("Informix incremental snapshot state must be a dictionary")
        if not isinstance(incremental.get("started"), bool):
            raise ValueError("Informix incremental snapshot state must record a boolean 'started'")
        chunk_lsn = incremental.get("chunk_lsn")
        if chunk_lsn is not None:
            result["incremental"]["chunk_lsn"] = str(_strict_lsn(chunk_lsn, "chunk_lsn"))
        for key in ("last_pk", "max_pk"):
            value = incremental.get(key)
            if value is not None and not isinstance(value, list):
                raise ValueError(f"Informix incremental snapshot {key} must be a list or null")
        if not incremental["started"] and (
            incremental.get("last_pk") is not None or incremental.get("max_pk") is not None
        ):
            raise ValueError(
                "Informix incremental snapshot state cannot carry bounds before it starts"
            )
        # Provenance stamped by _seed_incremental_block. Absent on offsets written
        # before it existed, which is exactly the "cannot be validated" case the
        # resume path re-seeds rather than trusts.
        boundary_lsn = incremental.get("boundary_lsn")
        if boundary_lsn is not None:
            result["incremental"]["boundary_lsn"] = str(
                _strict_lsn(boundary_lsn, "incremental boundary_lsn")
            )
        stamped_scope = incremental.get("scope")
        if stamped_scope is not None and (
            not isinstance(stamped_scope, str) or _PIPELINE_SCOPE.fullmatch(stamped_scope) is None
        ):
            raise ValueError("Informix incremental snapshot scope has an invalid identity")
        chunk_exprs = incremental.get("chunk_exprs")
        if chunk_exprs is not None and (
            not isinstance(chunk_exprs, dict)
            or not all(
                isinstance(key, str) and isinstance(value, str)
                for key, value in chunk_exprs.items()
            )
        ):
            raise ValueError(
                "Informix incremental snapshot chunk_exprs must map strings to strings"
            )
        snapshot_filter = incremental.get("snapshot_filter")
        if snapshot_filter is not None:
            if not isinstance(snapshot_filter, str):
                raise ValueError("Informix incremental snapshot filter must be a string or null")
            result["incremental"]["snapshot_filter"] = _snapshot_filter(
                {"snapshot.filter": snapshot_filter}
            )
    return result


def _shared_state_retry_count(offset: dict[str, Any]) -> int:
    value = offset.get("shared_state_retry_count", 0)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("Informix offset has an invalid shared_state_retry_count")
    return value


def _dropped_mount_retry_count(offset: dict[str, Any]) -> int:
    value = offset.get("dropped_mount_retry_count", 0)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("Informix offset has an invalid dropped_mount_retry_count")
    return value


def _schema_node_fallback_retry_count(offset: dict[str, Any]) -> int:
    value = offset.get("schema_node_fallback_retry_count", 0)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("Informix offset has an invalid schema_node_fallback_retry_count")
    return value


def _capacity_retry_count(offset: dict[str, Any]) -> int:
    value = offset.get("capacity_retry_count", 0)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("Informix offset has an invalid capacity_retry_count")
    return value


def _capacity_pressure(offset: dict[str, Any]) -> int:
    """Decaying acquisition-pressure signal that feeds the yield budget.

    Distinct from ``capacity_retry_count``: that counts *consecutive* misses and
    resets to zero the instant a read gets a slot, because it is the failure
    guard (a permanently starved flow fails at ``capacity.retry.max.retries``).
    Pressure instead *decays* on success rather than resetting, so a flow that is
    contended but occasionally served keeps some of the elevated budget it earned
    while starved instead of dropping straight back to the least-patient value.
    Optional and backward compatible: an offset written before this field existed
    defaults to zero.
    """

    value = offset.get("capacity_pressure", 0)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("Informix offset has an invalid capacity_pressure")
    return value


def _backlog_streak(offset: dict[str, Any]) -> int:
    """Validate the run of consecutive reads that filled their row budget.

    Optional and backward compatible: an offset written before the streak existed,
    or by a flow that has never truncated, simply has no key and reads as zero.
    """

    value = offset.get("backlog_streak", 0)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("Informix offset has an invalid backlog_streak")
    return value


def _with_backlog_streak(end: dict, start: dict, row_budget_reached: bool) -> dict:
    """Record whether this read ended at its row budget rather than the log's end.

    A read that filled its budget has *proved* more changes are waiting for this
    table, which is the only first-hand backlog evidence a reader has: the
    published hint is the server's global log position, so it measures staleness
    and cannot tell a quiet table from a lagging one. Counting consecutive
    truncations distinguishes a sustained backlog from one busy read, and draining
    the log clears the streak outright so a caught-up flow never keeps priority it
    no longer needs.

    The count saturates so it cannot grow without bound in an offset that a
    permanently backlogged flow persists on every batch.
    """

    if not row_budget_reached:
        if "backlog_streak" not in end:
            return end
        end = dict(end)
        end.pop("backlog_streak", None)
        return end
    end = dict(end)
    end["backlog_streak"] = min(_backlog_streak(start) + 1, _CONNECTION_BACKLOG_TRUNCATION_LEVELS)
    return end


def _backlog_rank(backlog: int, streak: int = 0) -> int:
    """Bucket an estimated backlog onto a small, bounded, log-scaled rank.

    Two signals combine, because each is blind where the other sees.

    ``backlog`` is derived from a published *global* log position, so its
    magnitude reflects whole-server write volume rather than this table's row
    count; a log scale absorbs that bias so only order-of-magnitude differences
    move the rank. It is comparable *across* readers, which is what ordering a
    contended queue needs, but it over-states a quiet table's lag.

    ``streak`` counts consecutive reads that ended at their row budget. That is
    first-hand proof this table has work outstanding, so it discriminates a
    genuinely backlogged flow from a merely stale one -- the discrimination the
    global signal cannot supply (measured in production: every waiter ranked
    identically because all checkpoints were recent). It is only a self
    assessment, so it cannot order one reader against another on its own.

    The streak takes the *high* band of the rank space and the estimate the low
    one, rather than the two being combined by a maximum. A maximum cannot
    separate readers whose estimates have already saturated, which is exactly the
    observed failure: when every waiter's hint-derived estimate lands on the same
    high rank, a proven backlog has to be able to outrank it. Banding guarantees
    that any truncating flow outranks every non-truncating one, while the estimate
    still orders readers within each band.
    """

    proven_floor = _CONNECTION_BACKLOG_RANK_LEVELS // 2
    estimated = 0 if backlog <= 0 else min(proven_floor - 1, backlog.bit_length() // 4)
    if streak <= 0:
        return estimated
    # Scale the streak across the upper band so a saturated streak reaches the top
    # rank, matching what "provably behind on every read" deserves.
    span = _CONNECTION_BACKLOG_RANK_LEVELS - 1 - proven_floor
    return proven_floor + min(span, streak * span // max(1, _CONNECTION_BACKLOG_TRUNCATION_LEVELS))


def _backlog_rank_value(offset: dict[str, Any]) -> int:
    """Validate an optional persisted backlog rank.

    Optional and backward compatible: an offset written before hints existed, or
    by a reader that found no usable hint, simply has no rank and reads as zero.
    """

    value = offset.get("backlog_rank", 0)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("Informix offset has an invalid backlog_rank")
    if value >= _CONNECTION_BACKLOG_RANK_LEVELS:
        raise ValueError("Informix offset has an out-of-range backlog_rank")
    return value


def _trigger_boundary_retry_count(offset: dict[str, Any]) -> int:
    value = offset.get("trigger_boundary_retry_count", 0)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("Informix offset has an invalid trigger_boundary_retry_count")
    return value


def _schema_transition_retry_count(offset: dict[str, Any]) -> int:
    value = offset.get("schema_transition_retry_count", 0)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("Informix offset has an invalid schema_transition_retry_count")
    return value


__all__ = [
    "InformixConnect",
    "InformixLakeflowConnect",
    "InformixBridge",
    "InformixError",
    "SharedStateAccessUnavailable",
    "TriggerBoundaryUnavailable",
    "SchemaTransitionUnavailable",
    "PurePythonInformixBridge",
    "LogRetentionError",
    "TransactionBuffer",
    "UnsupportedChangeError",
    "set_bridge_factory",
]
