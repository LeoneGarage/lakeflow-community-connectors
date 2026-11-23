"""Connection and table options, their defaults, and the connector's numbered messages."""

from __future__ import annotations

import json
import os
import re
import threading
import time
import zipfile
from typing import Any, TypeVar


class _Messages:
    # Docstring: The connector's message templates, read from informix_messages.json on first
    # use; ``_m(index, *values)`` formats one.
    #
    # Kept out of the pickled source: Spark ships this class's code to every worker by value,
    # and the templates are about 32 KB of text. Pickling holds only the file's path, which
    # every serverless worker can read, and each worker reads the file itself. The comment
    # above each ``_m`` call shows its template; lakeflow_test keeps the two in step. A file
    # that cannot be read never masks the error a message describes.
    #
    # Code imported from a zip archive (zipimport) has its path inside the archive, where
    # open() fails; the file is then read from the archive. After a failed read the numbered
    # message is used for a minute before the file is tried again. Every call passes its
    # values here, logging calls included, so the numbered message keeps them.

    def __init__(self) -> None:
        # Source run without a __file__ (the merged file pasted into a notebook cell, or
        # exec'd) looks in the working directory instead of failing to register.
        source = globals().get("__file__") or "_"
        self.path = os.path.join(os.path.dirname(os.path.abspath(source)), "informix_messages.json")
        self.templates: list[str] | None = None
        self.retry_at = 0.0

    def __getstate__(self) -> dict[str, str]:
        return {"path": self.path}

    def __setstate__(self, state: dict[str, str]) -> None:
        self.path, self.templates, self.retry_at = state["path"], None, 0.0

    def __call__(self, index: int, *values: Any) -> str:
        if self.templates is None and time.monotonic() >= self.retry_at:
            try:
                archive, member = self.path, []
                while not os.path.isfile(archive):
                    archive, name = os.path.split(archive)
                    if not name:
                        raise FileNotFoundError(self.path)
                    member.insert(0, name)
                if member:
                    with zipfile.ZipFile(archive) as bundle:
                        text = bundle.read("/".join(member))
                else:
                    with open(archive, "rb") as handle:
                        text = handle.read()
                self.templates = json.loads(text)
            except Exception:  # pylint: disable=broad-except
                self.retry_at = time.monotonic() + 60
        if self.templates is not None:
            try:
                return self.templates[index].format(*values)
            except Exception:  # pylint: disable=broad-except
                pass  # a file from another version: no such template, or other fields
            return f"Informix message {index} {values!r}"
        # The merged file deployed alone: say where the text was looked for.
        return f"Informix message {index} {values!r} ({self.path} not found)"


_m = _Messages()
# What a state operation returns (see _state).
_T = TypeVar("_T")

CURSOR = "_informix_change_lsn"
COMMIT_LSN = "_informix_commit_lsn"
TX_ID = "_informix_tx_id"
OP = "_informix_op"
# Source commit time (TimestampType, UTC): the Informix transaction commit time for
# CDC rows (c/u/d), the snapshot boundary time for snapshot rows (r), and NULL when the
# source did not supply one (e.g. a file-source extract without a manifest boundary_time).
# Appended last and excluded from the schema fingerprint, so adding it to an existing
# target is an additive column: pre-existing rows keep NULL, new rows are populated.
COMMIT_TIME = "_informix_commit_time"
_INTERNAL_COLUMNS = (CURSOR, COMMIT_LSN, TX_ID, OP, COMMIT_TIME)
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
    # Docstring: Render a (already validated) identifier for interpolation into SQL.
    #
    # Bare when it is a valid undelimited identifier; otherwise delimited with
    # double quotes, doubling any embedded quote. The identifier must already have
    # passed ``_IDENTIFIER`` validation, so the delimited branch only ever escapes
    # admitted characters such as "+" and ".".

    if _BARE_IDENTIFIER.fullmatch(identifier):
        return identifier
    return '"' + identifier.replace('"', '""') + '"'


# Words Informix reads as SQL rather than as a name where a column or table name stands
# (a column named "order" or "today" bare in a query): sent delimited, as
# _sql_name sends them. Delimiting a real name is always safe under DELIMIDENT.
_SQL_RESERVED_NAMES = frozenset(
    """all and any as asc avg between by case check constraint count create current date
    datetime day default delete desc distinct drop else end escape exists first for fraction
    from group having hour in index insert interval into is join key last left like limit
    matches max middle min minute month not null of on or order outer primary references
    right second select set skip sum sysdate table then to today top union unique update
    user values when where with year true false sitename dbservername current_role
    default_role rowid rownum cast cross inner full natural using intersect except minus
    connect prior start""".split()
)


def _sql_name(identifier: str) -> str:
    # Docstring: Render a catalog name for SQL text: bare only when Informix reads it back
    # as the same name.
    #
    # Informix folds a bare name to lower case, so a name the catalog holds with upper
    # case (a table or column created delimited, an owner in an ANSI database) was not
    # found bare, and a name that is a word of SQL was read as SQL. Under DELIMIDENT, which
    # every session sets, a delimited name is the catalog's name exactly. Identity strings
    # (_join_identity) keep _sql_identifier's form, which state keys and CDC names use.

    if (
        _BARE_IDENTIFIER.fullmatch(identifier)
        and identifier == identifier.lower()
        and identifier not in _SQL_RESERVED_NAMES
    ):
        return identifier
    return '"' + identifier.replace('"', '""') + '"'


def _join_identity(*components: str) -> str:
    # Docstring: Join identity components with ``.``, delimiting any component that is not a
    # valid undelimited identifier so the result round-trips through
    # ``_split_identity`` even when a component itself contains ``.``.

    return ".".join(_sql_identifier(component) for component in components)


class InformixError(RuntimeError):
    # Docstring: Base error raised by this connector.
    pass


def _split_identity(identity: str) -> list[str]:
    # Docstring: Split a ``.``-joined identity into its components -- the inverse of
    # ``_join_identity``.
    #
    # A double-quoted component is taken whole (so it may contain ``.``) with its
    # doubled quotes unescaped; bare components split on ``.`` as before.

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
                # 0: Unterminated quoted identifier in {!r}
                raise InformixError(_m(0, identity))
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
# A keyless initial snapshot drains positionally (no seek cursor) and stages whole
# pages to the Volume, so a larger default page means fewer manifest entries and
# round trips for the same rows. Kept separate from the keyed paging size so it can
# be tuned without affecting keyset chunks.
_DEFAULT_KEYLESS_SNAPSHOT_PAGE_SIZE = 50000
_DEFAULT_SNAPSHOT_READ_TIMEOUT_SECONDS = 300
# CDC session setup/teardown (cdc_opensess/startcapture/activatesess/endcapture/
# closesess) runs on the transport's default socket timeout unless raised here.
# syscdcv1 teardown can stall under many concurrent CDC sessions, so give these
# control-plane calls more room than the 30s default without adopting the much
# larger snapshot read budget.
_DEFAULT_CDC_READ_TIMEOUT_SECONDS = 60
# How long a bridge leaves DBINFO('utc_current') alone after the source answered it with an
# error, before asking again; until then snapshot rows carry a NULL commit time.
_UTC_TIME_REPROBE_SECONDS = 600.0
_DEFAULT_MAX_RECORDS_PER_BATCH = 10000
# Informix's own per-session ceiling, passed straight to cdc_opensess. Each poll
# pays a connection slot plus a full CDC session open/activate/close, so a small
# window amortises that fixed cost over very little log progress. Request the
# largest window the source accepts and let the soft-boundary loop finish any
# transaction still open past it.
_DEFAULT_CDC_MAX_RECORDS = 256
# A CDC read returns only once the bytes it asked for have arrived, the session's record cap is
# reached, or the session has been idle for cdc.timeout. Under a steady trickle a full-size
# read therefore waits for the cap, however long that takes, and can outlast the socket
# timeout. So a poll starts with a small read, which the first record or two fill, and
# doubles it up to cdc.read.bytes while reads come back full at once (a backlog); a read
# that waited puts it back to the start. 128 bytes is the smallest read Informix accepts (a
# smaller one fails with ISAM error -1); a read may end inside a record, which the frame
# parser carries over to the next.
#
# The read that follows the end of a backlog is the size the backlog earned, and has only
# the trickle to fill it: it waits. Each record arrives within cdc.timeout of the one before
# (or the session goes idle and the read returns), and the read returns at the record cap,
# so it can take up to cdc.timeout for each record it needs. The socket timeout of a read is
# set to that, counting a record for every _CDC_RECORD_MIN_BYTES asked for: under a minute
# for a read at the start size, the whole record cap's worth for a large one.
_CDC_READ_START_BYTES = 128
# The header every CDC record starts with: no record is smaller.
_CDC_RECORD_MIN_BYTES = 16
_CDC_READ_GROW_SECONDS = 0.2
_CDC_READ_SHRINK_SECONDS = 1.0
# An open transaction holding more change records than cdc.spill.records in one poll has
# them written to the staging Volume as pages of up to _CDC_SPILL_PAGE_RECORDS, under a
# "cdc-spill" stage beside the snapshot pages, rather than kept in memory. Its batch is
# served from those pages by executors. 0 keeps every transaction in memory, bounded by
# cdc.max.transaction.records.
_CDC_SPILL_RECORDS_OPTION = "cdc.spill.records"
_DEFAULT_CDC_SPILL_RECORDS = 50_000
_CDC_SPILL_PAGE_RECORDS = 10_000
_CDC_SPILL_SCHEMA = "cdc-spill"
# A run's Lakebase record names the channels that have committed past its transaction;
# the channel that completes the table's set deletes the pages.
_CDC_SPILL_RECORD_TYPE = "cdc-spill-run"
# The shape _spill_run gives a run's name: a record whose run differs names no pages of
# this connector's, and is never made into a path. The transaction ID comes from a signed
# 32-bit header field, so it may carry a minus.
_SPILL_RUN_NAME = re.compile(r"[0-9a-f]{0,12}-[0-9]+--?[0-9]+-[0-9a-f]{12}")
# How often a reader lists its tables' runs from Lakebase again: a channel that read a
# transaction in memory (its shard spilled nothing) finishes the other shard's run so.
_CDC_SPILL_LIST_SECONDS = 300
# The changes a delete channel shapes rows from; a run of none of them has nothing for it.
_SPILL_DELETE_OPS = frozenset({"DELETE", "BEFORE_UPDATE", "AFTER_UPDATE", "TRUNCATE"})
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
# Per-table migration switch. When ``table.migration=true`` the table runs a two-run
# checkpoint handoff (see _read_handoff / _read_append_only) *instead of* its normal read:
# it never streams -- every read records a checkpoint, emits no rows, and stops. While
# still on the old flow it parks its stream (or mid-serve snapshot) offset in Lakebase and
# stops; on the new flow (fresh checkpoint after an SCD1->append switch or a destination
# move) it seeds from that parked offset and stops instead of re-snapshotting. Clearing
# table.migration (the "Restore" step) resumes normal reads from the recorded offset. It
# is orthogonal to snapshot.mode, which the table keeps unchanged throughout.
_TABLE_MIGRATION_OPTION = "table.migration"
# Handoff-token channels. Only the upsert channel's offset is parked: append ingestion
# has no separate delete channel. A DELETE after the boundary reaches the upsert channel,
# where it lands as a full-before-image ``d`` tombstone row by default (see
# ``append.delete.tombstones``) or is dropped when tombstones are switched off. The
# old key of a key-changing UPDATE on a keyed append table is handled the same way.
_HANDOFF_CHANNEL_UPSERT = 0
_HANDOFF_CHANNEL_DELETE = 1
# On a parked upsert token: its flow had a delete flow beside it, which parks its own.
_HANDOFF_KEYED = "handoff_keyed"
_DEFAULT_SNAPSHOT_MODE = "incremental"
# The modes whose snapshot a snapshot.source extract serves: the extract is the table's
# initial snapshot, and no other mode takes one from it.
_UNL_SNAPSHOT_MODES = frozenset({"initial", "initial_only"})
# What _update_request names a full refresh, of the whole pipeline or of some tables.
_FULL_REFRESH_REQUESTS = frozenset({"full_refresh", "selective_refresh"})
# The floor a delete position seeded at a full refresh's own start was taken beside.
_SEED_FLOOR_FIELD = "seed_floor"
_SEED_MARKS = ("floor_seed", _SEED_FLOOR_FIELD)
# How many snapshots a resnapshot's delete floor names besides its own (_floor_scopes).
_KEPT_FLOOR_SCOPES = 16
# The modes whose upsert reader starts a full refresh at the update's initialization
# (_initial_lsn), which a delete reader may then take itself (_full_refresh_delete_seed).
_OWN_START_SNAPSHOT_MODES = frozenset({"incremental", "auto_snapshot", "initial", "cdc_only"})
_UNSUPPORTED_SNAPSHOT_MODES = frozenset({"configuration_based", "custom"})
# Opt-in append-only ingestion, for a table Informix can capture but this connector
# cannot merge -- in practice one without a primary key.
#
# Deliberately a connector option rather than something inferred from a missing key.
# Append output is a *change log*, not current state: an UPDATE at the source appends
# its after-image as a second row (``_informix_op='u'``) and a DELETE appends a
# tombstone row carrying the full before-image (``_informix_op='d'``, see
# ``append.delete.tombstones`` below). On a keyless table that tombstone is a
# change-log record, not an applied delete -- there is no key to pair it with -- so a
# consumer that wants current state filters on ``_informix_op``. Nothing in the schema
# distinguishes an append-only log from a mutable table that merely lacks a key, so
# inferring append from a missing key would silently turn the latter's merge into a log.
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
# Delete tombstones on the append path, on by default. Every CDC session enables
# full-row logging before capture, so a DELETE record always carries its full
# before-image; the append path emits it as a ``d`` row with the usual cursor /
# commit LSN / tx id / commit time columns, keeping the change log lossless (an UPDATE
# already lands as a ``u`` row). On a keyed table forced to append, a key-changing
# UPDATE removes the old key as well, so it also lands its before-image as a ``d`` row,
# ahead of the new key's ``u`` row and sequenced at the before-image's own log position.
# It costs nothing on a genuinely insert-only table,
# which never deletes. Set ``false`` (per table or connection-wide; the table option
# wins) only for an archive-style table where retention purges would otherwise append
# one tombstone per purged row. It has no effect on a table that is not on the append
# path: the keyed upsert channel still skips DELETEs, because the pipeline applies
# deletes only through the separate delete flow and would merge a ``d`` row on the
# upsert view as an upsert.
_APPEND_DELETE_TOMBSTONES_OPTION = "append.delete.tombstones"
# Sharded daemon-reader CDC (see _shared_cdc_mode). ``cdc.shared.session=host``, the
# default, shares each shard between the flow processes on one host through a leader
# process (see the host-shared shards block after _shared_cdc_shard); ``process`` shares
# the shards only within one process.
#
# The tables are partitioned into K shards (``cdc.shared.reader.threads``,
# default a quarter of ``max.concurrent.connections``, at least 1). A bounded
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
# Partitioned stream reader. When enabled (default), the connector advertises the
# SupportsPartitionedStream contract so Spark uses the partitioned DataSourceStreamReader.
# Its one win is the staged-snapshot page-serve phase: instead of serving one page per
# microbatch on the driver, a microbatch spans up to `stream.partitioned.snapshot.pages.
# per.batch` pages, each read+decoded on a separate Spark executor; the first microbatch,
# which drains the snapshot to the stage, serves its page 0 the same way. Every other phase
# (the live CDC/incremental stream, transitions, deletes) degrades to driver-computed rows
# embedded in the descriptor -- byte-for-byte the current output, just wrapped, and split
# across partitions of bounded size when large. Both readers persist the connector's bare
# offset dict, so flipping this option and restarting resumes an existing checkpoint in place
# (no full refresh); serverless SDP was verified to resume across the reader-class swap.
_PARTITIONED_STREAM_OPTION = "stream.partitioned"
_PARTITIONED_SNAPSHOT_PAGES_PER_BATCH_OPTION = "stream.partitioned.snapshot.pages.per.batch"
_DEFAULT_PARTITIONED_SNAPSHOT_PAGES_PER_BATCH = 8
# An embedded partition reaches the executor as one JSON document that the framework parses
# whole, at about seven times its text in Python objects, under serverless's 1 GB Python
# memory limit. A larger embedded microbatch is split across partitions of at most this
# much row text each.
_MAX_EMBEDDED_PARTITION_BYTES = 16 << 20
# The framework tags a delete-channel reader's options with this key (see
# sparkpds/lakeflow_datasource.py). The partitioned reader reads it to route the embedded
# path to read_table_deletes rather than read_table; the delete channel never page-serves.
_IS_DELETE_FLOW_OPTION = "isDeleteFlow"
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
# A shard with no subscriber for this long, or a snapshot-drain worker with no job, ends:
# its thread, and for a shard this process leads its lock and socket, are given back. A
# shard and a drain pool belong to one update of a pipeline, so a process kept warm over
# many updates would otherwise hold one set of each for every update it has seen.
_SHARED_CDC_IDLE_EXIT_SECONDS = 600.0
_SNAPSHOT_DRAIN_IDLE_EXIT_SECONDS = 600.0
# A bounded read whose shard view does not yet show it done waits for the shard's next read,
# holding no connection slot, rather than opening a session of its own: for two of the
# shard's reads (each waits out cdc.timeout about twice) and a margin, re-taking the view
# at this interval, before it reads directly.
_SHARED_VIEW_WAIT_TIMEOUTS = 4
_SHARED_VIEW_WAIT_MARGIN_SECONDS = 5.0
_SHARED_VIEW_WAIT_POLL_SECONDS = 0.5
# Tables per batched catalog query (table_versions, get_tables), well inside the SQLI
# limit of 1024 bind values a statement.
_CATALOG_BATCH = 500
# Pieces of the catalog queries, each spelled once and assembled where a query runs: a
# table's columns, its primary key's index, and its UNIQUE indexes, each with an optional
# select-list tail in place of the braces, and an index's 16 key parts.
_INDEX_PARTS = ",".join(f"i.part{number}" for number in range(1, 17))
_CATALOG_COLUMNS_SQL = (
    "SELECT c.colname, c.coltype, c.collength, c.colno, t.tabid, c.extended_id, "
    "x.name AS extended_name, x.owner AS extended_owner{} FROM systables t "
    "JOIN syscolumns c ON t.tabid = c.tabid "
    "LEFT JOIN sysxtdtypes x ON x.extended_id = c.extended_id WHERE t.tabtype = 'T'"
)
_PRIMARY_KEY_SQL = (
    "SELECT " + _INDEX_PARTS + "{} FROM systables t JOIN sysconstraints x ON t.tabid=x.tabid "
    "JOIN sysindexes i ON x.idxname=i.idxname AND x.tabid=i.tabid "
    "WHERE t.tabtype='T' AND x.constrtype='P'"
)
_UNIQUE_INDEX_SQL = (
    "SELECT i.idxname," + _INDEX_PARTS + "{} FROM systables t JOIN sysindexes i "
    "ON t.tabid=i.tabid WHERE t.tabtype='T' AND i.idxtype='U'"
)
# Snapshot-drain fairness. An ``initial``-mode snapshot drains the whole table through
# one REPEATABLE READ transaction, holding one connection slot for the entire scan; with
# many tables this can consume every slot and starve the streaming/CDC readers waiting
# for one. So the drain moves off the microbatch thread onto ``snapshot.reader.threads``
# (K) driver-resident daemon workers: the consumer waits (holding no slot) for a worker to
# stage the table and publish its manifest, then serves the staged pages exactly as a
# resumed snapshot does. At most K drains run at once in a process, and each claims its
# slot above its own floor (see _snapshot_daemon_connection_reservation), so the lowest
# slots stay with consumer reads.
_SNAPSHOT_READER_THREADS_OPTION = "snapshot.reader.threads"
_DEFAULT_SNAPSHOT_READER_THREADS = 1
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
# How a keyed table's one-pass consistent snapshot reads the source. ``keyset`` (default)
# pages in key order: each page is its own query, ordered by the key and bounded below by
# the previous page's last key. With an index on the key columns that is a cheap index
# walk; without one, every page can scan and sort the table, so the cost grows with pages
# times table size -- the usual case for a primary.keys override, the wildcard especially.
# ``cursor`` reads the table through one forward-only cursor with no ORDER BY, exactly as a
# keyless table is read, while the key still drives the destination merge and the delete
# channel. Skipping key comparisons also sidesteps keyset paging's trouble with NULL keys.
#
# Rows committed during a non-locking scan can reach both the snapshot and the CDC stream;
# for a keyed table that is harmless, because the sink merges by key and CDC replays every
# change after the snapshot LSN. snapshot.isolation=repeatable_read still gives an
# exactly-once scan that locks the table.
#
# With snapshot.mode=incremental or auto_snapshot there are no key ranges to chunk by, so
# a fresh snapshot falls back to the one-pass consistent snapshot, the same fallback a
# DATETIME key that cannot be chunked takes. A chunked copy already in flight finishes as
# it started. A keyless table always reads through the cursor, whatever this says.
_SNAPSHOT_SCAN_OPTION = "snapshot.scan"
_SNAPSHOT_SCAN_KEYSET = "keyset"
_SNAPSHOT_SCAN_CURSOR = "cursor"
# A keyless table has no key to page by, and a change cannot be matched to one of its rows.
# Its snapshot and its change stream then overlap: the snapshot position is the start of a
# log page, so the stream replays every transaction on that page, and a non-locking scan
# also sees changes made while it runs. A keyed target merges the overlap away; a change
# log keeps it as duplicate or missing rows. So under ``keyset`` a keyless table's snapshot
# pages by the table's most selective index, or by the columns ``keyset(a, b desc)`` names,
# and follows the table's changes while it reads, so that the stream can leave out each
# change the scan saw: one that committed before its key's page was read, or, when it
# committed during that read, one the rows staged for the key show the scan saw. See
# _index_scan_snapshot and _drop_scanned_changes. Without an index it can page by, the
# table is read through the cursor instead, as under ``cursor``, with a warning; with named
# columns it fails instead.
_SNAPSHOT_SCANS = frozenset({_SNAPSHOT_SCAN_KEYSET, _SNAPSHOT_SCAN_CURSOR})
# Index key columns it can page by: Python orders their values as Informix does.
_INDEX_SCAN_KEY_TYPES = frozenset(
    "INTEGER INT SMALLINT BIGINT INT8 SERIAL SERIAL8 BIGSERIAL DECIMAL MONEY DATE CHAR VARCHAR "
    "LVARCHAR".split()
)
# Rows read between two looks at the table's changes while the scan runs.
_INDEX_SCAN_FOLLOW_ROWS = 200_000
# How long a poll for changes may go without getting there, which is also how long the scan
# waits at its end for proof that the log was read through it; and how long it waits for
# that on a log nothing has written to since (see follow in _index_scan_snapshot).
_INDEX_SCAN_FOLLOW_SECONDS = 600
_INDEX_SCAN_PROOF_SECONDS = 30
# How long one of the follower's polls may read before it returns for the waits to be checked.
_INDEX_SCAN_POLL_SECONDS = 30
# The records one poll for changes reads in its session: a backlog in a few sessions, not
# one of 256 records each.
_INDEX_SCAN_POLL_RECORDS = 20_000
# The most rows one index key may have: they are read and staged together.
_INDEX_SCAN_KEY_ROWS = 1_000_000
# The most rows to an index value, on the catalog's statistics, for an index to be chosen
# for a table that could page by its key instead.
_INDEX_SCAN_AUTO_GROUP_ROWS = 1_000
_INDEX_SCAN_SETTLE_ATTEMPTS = 5
# The connector's per-process memory of index-ordered scans, kept as needed: the manifests'
# moved rows, the manifests as the stream uses them, which scans the stream is still
# within, and the scan records already discarded. None goes to a worker or outlives the
# registration it was read under.
# How many times a claim of a connection slot reopens a state connection Lakebase
# dropped, as the other state operations do.
_SLOT_CLAIM_RECONNECTS = 3
_SCAN_CACHES = (
    "_moved_rows",
    "_index_scans",
    "_index_scan_live",
    "_scan_progress_forgotten",
    # And what it has found of delete floors, staged snapshots and extract split plans.
    "_rejected_delete_floors",
    "_staged_snapshot_offsets",
    "_unl_split_plans",
)
# How many rows of a table keyed by primary.keys may change index value during the scan:
# each is left out of the pages and read once more at the end, and the manifest names it.
_INDEX_SCAN_MOVED_ROWS = 10_000
# How many changed rows of such a table the scan keeps the index values of, to tell when
# one shows under a second: about 150 bytes a row.
_INDEX_SCAN_TRACKED_ROWS = 2_000_000
# How many of the rows read again one query asks for, and how many times they are read:
# a round reads only the rows that are new or changed since their last read, so it is cheap.
# A query's values go to Informix as text within the statement, which has a size limit:
# a query takes fewer rows when their values are long.
_INDEX_SCAN_LOOKUP_ROWS = 100
_INDEX_SCAN_LOOKUP_CHARS = 16_000
_INDEX_SCAN_ROW_READS = 20

# Where snapshot page payloads are written. ``fuse`` uses the UC Volume FUSE mount
# (today's behaviour). ``rest`` writes them through the Files REST API, which never
# touches the mount and so avoids the ENOTCONN disconnects that recur under sustained
# write frequency, but needs a mintable personal access token (see
# StagingCredentialProvider). ``auto`` (the default) uses REST when a token can be
# minted and falls back to FUSE when the workspace forbids token creation.
_SNAPSHOT_STAGING_TRANSPORT_OPTION = "snapshot.staging.transport"
_SNAPSHOT_STAGING_TRANSPORTS = frozenset({"auto", "rest", "fuse"})
_DEFAULT_SNAPSHOT_STAGING_TRANSPORT = "auto"
# Per-table option: a UC Volume directory of Informix UNLOAD (`.unl`) extract files.
# When set, the table's ``snapshot.mode=initial`` phase reads its history from those
# files instead of a live ``consistent_snapshot()`` scan, then transitions to the CDC
# stream at the connector-pinned boundary exactly as a live snapshot does. The extract
# is the operator's responsibility to align with the boundary (quiesce/extract-then-run);
# the connector pins the boundary itself and does no operator-supplied LSN or fingerprint.
_SNAPSHOT_SOURCE_OPTION = "snapshot.source"
# Per-table operator-supplied CDC boundary for a `.unl` snapshot source: the LSN the
# extract was taken at (captured by the extract step). Resolution order per table:
# this option, then a `_manifest.json` (``{"boundary_lsn": "<X>"}``) in snapshot.source,
# then self-pin (the connector's own registration LSN). A supplied LSN is authoritative
# (initial_lsn == snapshot_lsn == X) so the CDC stream resumes at exactly that position.
_SNAPSHOT_BOUNDARY_LSN_OPTION = "snapshot.boundary.lsn"
_SNAPSHOT_MANIFEST_NAME = "_manifest.json"
# A large `.unl` fans out across executors in byte-bounded pieces: each split targets
# this many bytes and ends at the next record boundary (splits never cross a file).
# Byte splitting -- unlike a row cap -- needs no whole-file scan to plan: the planner
# seeks to each target and reads only a small window to snap to the next newline.
_UNL_SPLIT_TARGET_BYTES = 50 << 20
# Per-table (connection-fallback) override of the split target byte size.
_UNL_SPLIT_BYTES_OPTION = "snapshot.split.bytes"
# Resumable gzip extracts. A `.unl.gz` file cannot be split by byte range -- compressed
# data cannot be decoded from the middle -- so by default it is one split, read whole on an
# executor in one microbatch, and a failure re-reads the whole file. With
# ``snapshot.gzip.records.per.batch`` = N > 0, each gzip file is instead served on the
# driver N records per microbatch, and the offset records how many of the file's records
# are done (``snapshot.record_index``). The driver keeps the file's decompressor open
# between microbatches, so normal progress is still one pass over the file; after a
# restart it reopens the file and skips the recorded records once -- re-decompressing that
# prefix, but never re-ingesting it. Each microbatch's rows pass through the driver and its
# partition descriptor, and a gzip file is no longer read in parallel with other splits, so
# this suits large gzip files rather than many small ones. Plain files are unaffected.
# Clearing the option while a file is part-served finishes that file in one microbatch
# rather than re-reading its served prefix. Applies to the partitioned reader; the serial
# reader serves the whole extract in one microbatch either way.
_UNL_GZIP_RECORDS_OPTION = "snapshot.gzip.records.per.batch"
# Extract identity. A file snapshot resumes by position -- split index, and record index
# inside a gzip file served in record windows -- into a split plan rebuilt from the
# directory on every read, so a file replaced, added or removed after the snapshot started
# would make a resume silently skip or repeat rows. When the boundary is pinned, the
# connector also pins a manifest of the extract: every file's name, size and modification
# time, and a fingerprint hashing the size with the first and last
# _UNL_FINGERPRINT_SAMPLE_BYTES of the file. Every later read compares the directory with
# it -- names and sizes exactly, modification times within _UNL_MTIME_TOLERANCE_SECONDS,
# because a Volume reports an unsettled mtime for a few seconds after a write -- and
# re-checks the fingerprint of each file that read will serve, failing closed with a
# full-refresh error on any difference. Fingerprints are cached per (path, size, exact
# mtime) within a driver process, so a gzip file served over many microbatches is sampled
# once per run. The fingerprint samples rather than hashes whole files, so an edit confined
# to the middle of a file that also keeps its size and modification time escapes it.
_UNL_FINGERPRINT_SAMPLE_BYTES = 1 << 20
_UNL_MTIME_TOLERANCE_SECONDS = 5.0
# Fixed buffer for the streaming range reads and the boundary-snap scan: constant
# memory regardless of `.unl` size (never load a whole file).
_UNL_SCAN_CHUNK_BYTES = 4 << 20
# A `.unl` extract need not be dbaccess UNLOAD: Informix's High-Performance Loader
# (onpload/ipload) DELIMITED output is the same newline-terminated, escaped, delimited
# shape, but with a job-configured field delimiter and escape character -- and, unlike
# UNLOAD, HPL does not honor DBDELIMITER, so an HPL job commonly uses a non-`|`
# delimiter. These per-table (connection-fallback) options let the file serve parse an
# HPL DELIMITED extract whose delimiter/escape differ from UNLOAD's `|`/backslash
# defaults; the defaults leave a plain UNLOAD extract parsed exactly as before. Both are
# a single character; the escape ``none`` disables escape processing for an HPL job that
# writes none. Not "": a form sends that for a field nobody filled in, which then turned
# escaping off on every load of the connection (an empty VARCHAR read as a backslash).
# The record terminator stays a physical newline; HPL's fixed-position,
# COBOL, and Informix-internal binary formats are not supported (they are not
# self-describing) -- configure the HPL job to emit newline-terminated DELIMITED output.
_UNL_DELIMITER_OPTION = "snapshot.unl.delimiter"
_UNL_ESCAPE_OPTION = "snapshot.unl.escape"
_UNL_DEFAULT_DELIMITER = "|"
_UNL_DEFAULT_ESCAPE = "\\"
_UNL_NO_ESCAPE = "none"
# Informix integer/float type names, for mapping UNLOAD text back to Python values.
_UNLOAD_INT_TYPES = frozenset(
    {"INTEGER", "INT", "SMALLINT", "BIGINT", "INT8", "SERIAL", "SERIAL8", "BIGSERIAL"}
)
_UNLOAD_FLOAT_TYPES = frozenset({"FLOAT", "SMALLFLOAT", "REAL", "DOUBLE", "DOUBLE PRECISION"})
_SNAPSHOT_STAGING_TOKEN_LIFETIME_OPTION = "snapshot.staging.token.lifetime.seconds"
# How long an update's claim on revoking earlier updates' staging tokens holds: for good,
# as far as the update goes.
_STAGING_TOKEN_SWEEP_CLAIM_SECONDS = 30 * 86400


class _StagingProviders(dict):
    # Docstring: This process's staging credential providers, by host, pipeline, update and
    # token lifetime. Never shipped: Spark pickles the merged source's globals to every
    # worker, and a provider holds a token and a lock, so it pickles as empty.

    def __init__(self) -> None:
        super().__init__()
        self.lock = threading.Lock()

    def __reduce__(self):
        return (_StagingProviders, ())


_STAGING_PROVIDERS = _StagingProviders()
# Walks a completed snapshot stage's removal may take when background page deletes keep
# cutting it short (see _cleanup_completed_snapshot_stage); each walk removes more.
_SNAPSHOT_STAGE_CLEANUP_ATTEMPTS = 5
# Overlap a keyless positional drain's next page fetch with the previous page's
# shape+upload, through one bounded background uploader. Only engaged for a keyless
# drain writing over the REST transport -- pipelining FUSE writes would raise the
# mount's write frequency, the very thing the REST transport exists to avoid.
_SNAPSHOT_STAGING_PIPELINE_OPTION = "snapshot.staging.pipeline"
_SNAPSHOT_ISOLATION_SQL = {
    "committed_read_last_committed": "COMMITTED READ LAST COMMITTED",
    "repeatable_read": "REPEATABLE READ",
}
# Idle back-off for a snapshot-drain worker with no queued jobs, so it does not busy-spin.
# Also the granularity at which the consumer's liveness watchdog re-checks for progress.
_SNAPSHOT_DRAIN_IDLE_SECONDS = 1.0
# Extra grace the consumer's drain watchdog waits beyond the worker's slot-wait window, so
# a wedged pool surfaces the worker's precise ConnectionCapacityUnavailable (recorded when
# the worker gives up) rather than the consumer's generic "appears stalled" racing it out.
# Comfortably larger than one acquisition sweep so the worker always records first.
_SNAPSHOT_DRAIN_STALL_GRACE_SECONDS = 30.0
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
_IMMUTABLE_STATE_VERSION = 1
_SNAPSHOT_STAGE_VERSION = 1
_SHARED_STATE_WAIT_SECONDS = 300
_MAX_SNAPSHOT_STAGE_PAGE_BYTES = 256 << 20
_DEFAULT_SNAPSHOT_STAGE_RETENTION_DAYS = 4
# Background threads that delete consumed staged pages off the microbatch serve
# path. A small pool drains the one-page-per-microbatch steady state easily and
# clears any post-restart backlog quickly without hammering the Volume.
_SNAPSHOT_CLEANUP_THREADS = 4
# How often a resumed snapshot re-reads the live Informix schema to detect drift.
# Serving immutable staged pages needs no live schema, so a per-microbatch
# get_table query (plus the connection/slot it opens) dominated serve time.
# Checking periodically instead keeps the drift guard -- a mid-snapshot schema
# change is caught within this window and aborts the copy -- while most pages skip
# Informix entirely. The window trades detection latency for throughput; the pages
# served in between are immutable as-of snapshot_lsn, so serving a few extra before
# an abort is harmless. A shared CDC shard's daemon checks its tables' definitions as
# often, for the same reason (see _ShardCatalog).
_SNAPSHOT_SCHEMA_REFRESH_INTERVAL_SECONDS = 60
# Bootstrap deferrals a delete reader records before its advisory offset stops
# changing (see _schema_node_fallback_offset). The upsert reader publishes this
# update's channel-start before taking its snapshot -- it enables full-row logging,
# captures one LSN, and publishes -- and a resumed upsert reader republishes its
# resume position on its first stream read, so a handful of microbatch intervals
# covers the window in which the delete reader would otherwise find nothing. The
# counter is persisted in offsets under its historical name.
_SCHEMA_NODE_FALLBACK_RETRIES = 5
_DEFAULT_MAX_CONCURRENT_CONNECTIONS = 16
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
# One-shot jittered delay a reader sleeps before its *first* connection, so a pipeline that
# starts dozens of flows at once does not open all their Informix CDC sessions in the same
# instant -- the syscdcv1 subsystem can stall under many concurrent session open/teardowns
# (see validate_initial_lsn). Each reader sleeps random.uniform(0, this) once, before it
# acquires a slot (so it holds nothing while waiting), spreading the startup wave over the
# window. 0 (default) disables it; steady-state reads are never delayed (the flag is set
# after the first connect).
_CONNECTION_STARTUP_JITTER_OPTION = "connection.startup.jitter.seconds"
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
# Fair slot queue: a waiter enqueues a ticket so acquisition is first-come per slot
# rather than a raw race, which prevents an unlucky reader being lapped indefinitely
# under contention. The queue interleaves requester classes so a high-volume class (many
# consumer bootstrap/fallback tickets) cannot crowd out a low-volume one (the few
# CDC-daemon tickets) by ticket count alone -- each class is served once per rotation.
# The class is derived from the private slot markers already on a reader's options (see
# _connection_waiter_class).
# Requester classes for the round-robin policy's rotation. The value is only a
# within-round tiebreak -- every class is still served once per round -- so the ordering
# among them does not starve any class. Consumer work is class 0 so that, all else equal,
# the reads that must finish to make progress are not deprioritised.
_SLOT_CLASS_CONSUMER = 0
_SLOT_CLASS_SNAPSHOT_DAEMON = 1
_SLOT_CLASS_CDC_DAEMON = 2
# The rotation modulus for the round-robin policy. Class 3 was the drain on a consumer's
# own thread, now gone; the modulus still counts it so that connectors of either version
# sharing one endpoint rotate alike, and the rotation only ranks classes with waiters.
_SLOT_CLASS_COUNT = 4
# A ticket stays authoritative for this long without a heartbeat. A waiter heartbeats
# every sweep (<= _LAKEBASE_SWEEP_MAX_SECONDS), so this is many sweeps of slack for a
# briefly-stalled live waiter, yet far below the slot lease so a crashed waiter's ticket
# stops blocking the queue quickly.
_LAKEBASE_WAITER_TTL_SECONDS = 15.0
# The backlog hint describes the *endpoint's* log position, not one table's, so
# all publishers share a single row. The hints table is keyed per table because
# state records reuse it; this constant is the endpoint-wide key.
_LAKEBASE_ENDPOINT_HINT_KEY = "__endpoint__"
# A waiter holds no slot and has no bridge, so it cannot borrow a bridge's
# Postgres connection. These caches give the waiter path one state handle and one
# connection per endpoint per process, keyed by endpoint identity.
_LAKEBASE_WAITER_STATE: dict[tuple, Any] = {}
_LAKEBASE_WAITER_CONNECTION: dict[tuple, Any] = {}
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
# How old a backlog hint may be before a waiter ignores it. Lakebase stamps each hint
# with its own clock when it is published and measures the age on that same clock when
# it is read, so clock skew between workers does not change it.
_CONNECTION_HINT_MAX_AGE_SECONDS = 45.0
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
# Set by a replayed read on the offset it reached when the range cannot be reproduced and
# was re-established instead (a start the log no longer retains, under auto_snapshot):
# _replay_between_offsets then holds it to an initial range's terms. Never persisted.
_REPLAY_REESTABLISHED = "_informix.replay.reestablished"
# The end a replayed initial range logged, as JSON: a file snapshot's first batch is
# served again at the boundary that end names (_replayed_unl_boundary).
_REPLAY_INITIAL_END_OPTION = "_informix.replay.initial.end"
# Set on the read that replays auto_snapshot's resnapshot of an expired checkpoint: like an
# initial range's, it leaves the delete floor as the logged batch published it.
_REPLAY_RESNAPSHOT_OPTION = "_informix.replay.resnapshot"
# Set on the read of auto_snapshot's resnapshot of an expired checkpoint, which leaves the
# destination as it is: its boundary keeps an earlier floor still in the log
# (_publish_delete_snapshot_floor). Never persisted.
_RESNAPSHOT_OPTION = "_informix.resnapshot"
# The floor_seed of the end a replayed delete range logged, and whether that end is on a
# transaction, as JSON [seed or null, bool] (see _confirmed_floor_seed).
_REPLAY_END_SEED_OPTION = "_informix.replay.end.seed"
# Distinguish JSON null (replay the copy-completing range through max_pk) from
# an absent option (ordinary one-page incremental read).
_REPLAY_DRAIN_TO_MAX_PK = object()
# A replayed range whose end is in the copy before its first chunk (a seed, or a retry yield
# of one): the replay copies no chunk. Encoded as {"before_first_chunk": true}.
_REPLAY_BEFORE_FIRST_CHUNK = object()
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
# Initial-LSN validation (cdc_opensess/startcapture/activatesess) can have its socket read
# time out under a startup storm -- many flows opening CDC sessions against syscdcv1 at
# once. That is transient contention, not a real validation failure, so a few bounded
# attempts with short backoff -- reconnecting a fresh transport between them -- ride it
# out; exhausting them re-raises so a genuinely-unreachable source still fails. Timeout-
# only: a server error (a nonzero cdc_* return, a negative session id) is a real failure
# and is never retried.
_VALIDATE_LSN_RETRY_MAX_RETRIES = 4
_VALIDATE_LSN_RETRY_BASE_SECONDS = 0.5
_VALIDATE_LSN_RETRY_MAX_SECONDS = 8.0
# The incremental-snapshot page and key-bound reads drop their connection the same
# way (a "truncated SQLI stream" EOF from an idle NLB/PrivateLink reset or a
# server-side session reap), and recover the same way: each read advances no offset
# and opens its own repeatable-read transaction, so the dead transport can be reset
# and the identical read reissued in place. Mirrors the CDC budget above.
_SNAPSHOT_RECONNECT_MAX_RETRIES = 4
_SNAPSHOT_RECONNECT_BASE_SECONDS = 0.5
_SNAPSHOT_RECONNECT_MAX_SECONDS = 8.0
# The Lakebase (Postgres) state connection drops the same way but on the other
# transport: a connection left idle across a long wait for capacity is reaped by
# the server, and because psycopg leaves ``.closed`` False until the next query,
# the cached handle is handed back dead and the next ``execute`` raises
# ``OperationalError`` ("SSL SYSCALL error: EOF detected"). State operations are
# idempotent (monotonic-min upsert, DO-NOTHING election, bare SELECT, idempotent
# DELETE) and commit only after the round-trip succeeds, so a dead connection can
# be dropped and the identical operation reissued on a fresh one. Mirrors the two
# SQLI budgets above.
_LAKEBASE_RECONNECT_MAX_RETRIES = 4
_LAKEBASE_RECONNECT_BASE_SECONDS = 0.5
_LAKEBASE_RECONNECT_MAX_SECONDS = 8.0
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
