# Informix CDC API research and Python port contract

## Scope and evidence

This document specifies a dynamic Informix connector: every eligible user table is discovered at runtime, then filtered by optional configured table patterns. It is based on:

- the Informix CDC API (`syscdcv1` routines) and the Informix SQLI wire protocol;
- the Lakeflow `LakeflowConnect` method and pagination contract.

The protocol is not an HTTP API. Informix exposes a binary change stream through routines installed in the `syscdcv1` database. These routines are invoked over the Informix SQLI connection and return CDC-encoded bytes that require native decoding logic.

## Required source configuration

- Install the Informix CDC API by running `$INFORMIXDIR/etc/syscdcv1.sql`; this creates `syscdcv1` and the `cdc_*` routines.
- The database must use transaction logging and retained logical logs must cover the requested restart LSN.
- The CDC user needs normal metadata/snapshot access to the source database and sufficient rights in `syscdcv1` to open sessions, enable full-row logging, and capture the selected tables. The working deployment grants/uses DBA-level access to `syscdcv1`.
- Full-row logging must remain enabled for watched tables to obtain complete update before-images. Full-row logging is enabled via `cdc_set_fullrowlogging(table, 1)`.
- Connection inputs are hostname, port (default `9088`), database, user, and password. One connector instance/offset partition represents one logical database. `max.concurrent.connections` defaults to `16` (range `1`–`9999`) and bounds concurrent SQLI sessions for the configured hostname, port, and `INFORMIXSERVER` through the Lakebase `conn_slots` table, keyed by that endpoint identity. `connection.wait.timeout.seconds` defaults to `600`; each microbatch blocks for up to that period to acquire a slot, then raises `ConnectionCapacityUnavailable` rather than returning an empty successful batch. The limit can change in place without stopping pipelines: capacity is the number of seeded rows, acquisition bounds `slot_id` by the configured limit (so a lower value takes effect immediately and surplus rows are left in place rather than deleted, because removing a row a live reader holds would silently revoke its lease), and a higher value seeds additional rows idempotently. A claim is one `UPDATE ... FOR UPDATE SKIP LOCKED` that takes the lowest free or expired slot and increments its `epoch`; the epoch is required to renew or release, so a reader stalled past its lease can neither resurrect it nor free a slot its successor now holds — an unreleased lease simply expires. Because that claim is a race, a **fair waiter queue** (`connection.fair.queue.enabled`, default on) sits in front of it: a blocked reader enqueues a ticket in `slot_waiters` carrying its reservation band `[floor, ceiling)`, and the claim then adds a `NOT EXISTS` so a slot is eligible only when no *older, still-heartbeating* waiter is also eligible for it — turning the race into first-come-first-served **per slot**, so no reader is lapped indefinitely under contention. Per-slot (not global) ordering avoids head-of-line blocking across reservation bands: an older delete-channel waiter reserved to high slots does not block a younger consumer from a low slot it could never use. The ticket heartbeats each sweep and is dropped the instant the reader wins or gives up; a crashed or timed-out reader's ticket ages out (≈15s) and is reaped by the next live heartbeat, so it never wedges the queue and the table needs no separate GC. The `epoch` CAS is unchanged, so capacity is still exact — fairness only reorders who wins, never over-issues a slot — and the queue bounds worst-case waiting without creating capacity (a fair waiter on an oversubscribed pool still times out, just in order). Set `connection.fair.queue.enabled=false` to fall back to the unordered race.

Table names use Informix's database-qualified syntax `database:owner.table`; the logical table identifier is `database.owner.table`. Unless `DELIMIDENT` is enabled, arbitrary SQL identifier quoting is not available, so identifiers must be validated and quoted only where Informix permits it.

## Exact native CDC session protocol

The CDC session is established with these calls, in this order:

1. Read the server name:

   ```sql
   SELECT env_value
   FROM sysmaster:sysenv
   WHERE env_name = 'INFORMIXSERVER'
   ```

2. Open a CDC session:

   ```sql
   EXECUTE FUNCTION informix.cdc_opensess(server_name, 0, timeout, max_records, 1, 1)
   ```

   The returned integer is the session ID; a negative value is an Informix error code. `timeout` is seconds and allows an idle read to return a TIMEOUT record. `max_records` limits records returned by a read. Defaults come from `cdc.timeout` and `cdc.max.records`.

3. For every selected table, enable full-row logging:

   ```sql
   EXECUTE FUNCTION informix.cdc_set_fullrowlogging('database:owner.table', 1)
   ```

4. Register every table with a distinct positive integer label:

   ```sql
   EXECUTE FUNCTION informix.cdc_startcapture(
       session_id, 0, 'database:owner.table', 'col1,col2,...', label
   )
   ```

   The `*` projection is resolved through `SELECT FIRST 1 * FROM database:owner.table` and result metadata before registration. BYTE, TEXT, fixed/variable UDT, unknown, and complex columns are excluded from the CDC column descriptor; those values are emitted as unavailable placeholders rather than decoded from the stream. The label-to-table mapping is connector state. A TRUNCATE record is anomalous: its table label is read from `userId`, not the normal `label` field.

5. Activate from a 64-bit sequence position:

   ```sql
   EXECUTE FUNCTION informix.cdc_activatesess(session_id, start_sequence)
   ```

   `0` means Informix's current position. Otherwise `start_sequence` is an LSN encoded as `(log_unique_id << 32) + log_position`.

6. Repeatedly invoke the native CDC record read routine with `(session_id, output_stream, requested_bytes)` parameters. Preserve incomplete bytes between reads. Each CDC record begins with two big-endian 32-bit integers: `header_size`, then `payload_size`; the total record length is their sum. At least 16 bytes must be available before attempting record decoding; the actual decoding follows the native Informix CDC record format.

7. On shutdown, call `cdc_endcapture(session_id, 0, table)` for each table and then `cdc_closesess(session_id)`. Production must normally leave full-row logging enabled (`cdc.stop.logging.on.close=false`); disabling it on every finite Lakeflow poll introduces capture gaps and affects other consumers.

All `cdc_set_fullrowlogging`, `cdc_startcapture`, `cdc_activatesess`, `cdc_endcapture`, and `cdc_closesess` calls return `0` on success. Non-zero results and negative session IDs must fail the batch without advancing its checkpoint.

## Native record and transaction semantics

The native CDC decoder yields BEGIN, INSERT, BEFORE_UPDATE, AFTER_UPDATE, DELETE, TRUNCATE, DISCARD, COMMIT, ROLLBACK, METADATA, TIMEOUT, and ERROR records. Transactional/data records carry the sequence/transaction fields shown below; metadata, timeout, and error use their shorter record-specific headers. Data records carry a capture label.

Interleaved records are grouped by transaction ID:

- BEGIN creates an in-memory transaction holder and records its begin sequence/time/user.
- INSERT, DELETE, BEFORE_UPDATE, AFTER_UPDATE, and TRUNCATE append to that holder.
- BEFORE_UPDATE is paired with the following AFTER_UPDATE and becomes the update's before-image. Do not emit BEFORE_UPDATE independently.
- DISCARD removes buffered records whose sequence is greater than or equal to the DISCARD sequence. This is required for Informix rollback-to-savepoint semantics.
- COMMIT or ROLLBACK closes the transaction. Rolled-back records are never emitted. Empty transactions are normally omitted.
- METADATA/TIMEOUT/ERROR records without a transaction can be delivered independently. TIMEOUT is an idle/liveness indication, not end-of-stream and not an offset advance.

For a committed transaction, operations are emitted in log order. INSERT maps to create (`after` only), paired update maps to update (`before` and `after`), DELETE maps to delete (`before` only), and TRUNCATE is table-wide. Transaction BEGIN/END metadata is optional. A safe Python reader must finish buffering through COMMIT before returning rows: returning uncommitted operations would make later rollback handling impossible.

The source timestamp for transaction-boundary records is the Informix BEGIN/COMMIT time (epoch seconds); data envelope processing time is connector time. Ordering must therefore use LSNs, not timestamps.

## Exact offset and restart rules

### LSN representation

An available Informix LSN is represented by this connector as a non-negative unsigned 64-bit sequence. Its official form is `LSN(loguniq,logpos_hex)`. Ordering compares the 64-bit sequence. The restart position is a four-part transaction-log position:

```json
{
  "commit_lsn": "<decimal sequence>",
  "change_lsn": "<decimal sequence>",
  "begin_lsn": "<decimal sequence>",
  "tx_id": 123
}
```

The CDC offset keys are `commit_lsn`, `change_lsn`, and `begin_lsn`; transaction metadata may add its own fields. Its source partition is `{"databaseName": "<topic/logical name>"}`. The offset map is checkpointed completely by the connector.

### Why three LSNs are required

- `change_lsn` identifies the last delivered operation.
- `commit_lsn` identifies the transaction's commit/end position.
- `begin_lsn` is the oldest transaction BEGIN that must be replayed. Informix transactions can interleave, so resuming only at the last change or commit loses still-open transactions.

At transaction handling start `restartSeq = lowest buffered BEGIN`, or the transaction end sequence when none remain. Positions are monotonically updated. After a normal commit it stores commit=end, change=end, and begin=restart. With transaction metadata disabled, it also advances the final data event's position to the transaction end/restart position because no later transaction-END event will carry that offset.

### Recovery algorithm

On restart, activate the CDC session at `begin_lsn` when available, otherwise `commit_lsn`.

1. If restart/begin is less than the checkpointed commit LSN, enter recovery mode.
2. Rebuild transaction holders by replaying from that earlier BEGIN.
3. Skip transactions whose commit LSN is below the checkpointed commit LSN.
4. If commit equals the checkpointed commit and checkpointed change equals commit, skip the whole transaction.
5. Within a recovered transaction, skip operation records with sequence `<= checkpointed change_lsn`.
6. Leave recovery once a transaction commits after the checkpointed commit LSN, then continue normally.

This yields at-least-once delivery around a Lakeflow checkpoint boundary. Only return an `end_offset` after a complete committed transaction has been decoded and all returned rows are materialized. If a call fails, return nothing/raise and leave the prior offset unchanged. Native and projected batch caps are soft while transactions already observed remain open; reading through commit/rollback prevents a transaction larger than `cdc.max.records` from replaying the same prefix forever. METADATA and TIMEOUT control frames do not consume that native record target. `cdc.max.poll.records` is the default hard poll bound; `cdc.max.poll.bytes` optionally adds recursive retained-byte accounting when set above zero. Recovery skips only whole transactions whose commit LSN is already checkpointed; it never filters records inside a newly committed transaction using another transaction's LSN. Informix TIMEOUT remains a hard terminal condition, and an incomplete transaction is then replayed from the retained checkpoint. When Spark calls `prepareForTriggerAvailableNow()`, the Informix-owned generated base wrapper freezes a current LSN independently for that reader without modifying the shared adapter. Lakeflow checkpoints upsert and delete flows independently, so their AvailableNow boundaries may differ and converge on the next triggered update. Spark does not make that callback in continuous mode, so continuous execution follows new commits without a mode-specific option.

A server-side pre-authentication rejection remains fatal because SQLI rejection type 3 does not itself prove SQLCODE `-25571`; the Informix server log is authoritative. Connection-capacity exhaustion fails visibly after `connection.wait.timeout.seconds`; it is not represented by a retry-only checkpoint offset.

Before resuming, validate retention with:

```sql
SELECT MIN(uniqid) AS uniqid, 0 AS logpage FROM sysmaster:syslogs
```

The minimum available sequence is `(uniqid << 32)`. If the restart LSN is older, incremental continuation is impossible; fail explicitly or perform the configured `auto_snapshot` resnapshot. The approximate current/high-water LSN used before a snapshot is:

```sql
SELECT uniqid, used AS logpage
FROM sysmaster:syslogs
WHERE is_current = 1
```

encoded as `(uniqid << 32) + (logpage << 12)`. This is recorded before snapshot data is read, then streaming begins from it so changes concurrent with the snapshot are not missed.

### Lakeflow pagination shape

The Lakeflow implementation should keep an offset per table and per channel because `read_table()` and `read_table_deletes()` are checkpointed independently:

```json
{
  "version": 10,
  "commit_lsn": "90",
  "change_lsn": "90",
  "begin_lsn": "90",
  "tx_id": null,
  "phase": "snapshot",
  "schema_id": "0123456789abcdef0123456789abcdef",
  "schema_fingerprint": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "pipeline_scope": "94368468-bc2d-4797-868d-0cb9e19a5610_@_5fc90c10-d5d1-489b-afce-1bd9d36544c1",
  "trigger_generation": null,
  "trigger_high_water": null,
  "incremental": {"chunk_lsn": "90", "last_pk": ["..."], "max_pk": ["..."]}
}
```

The `incremental` block is present only during a default `incremental`/`auto_snapshot` copy; a blocking `initial` snapshot instead carries a staged-page index, and a table already in pure CDC omits both. Offset versions are intentionally strict. Version 10 identifies the current fixed-width LSN row encoding, schema identity, update scope, self-contained trigger generation/high-water boundary, and optional AvailableNow capacity retry state; an absent or different version fails with a full-refresh instruction rather than mixing incompatible downstream ordering values.

Each finite call opens/replays a CDC session, reads until at least one complete transaction is available (or timeout), closes it, and returns the exact last committed position. When caught up, return an empty iterator and the unchanged `start_offset`, as required by `LakeflowConnect`.

### Optional sharded daemon-reader CDC

`cdc_opensess` / `cdc_startcapture` accept multiple tables in one session and tag each
record with its source table, so the streaming read can be shared. This is on by default
(`cdc.shared.session`, set `false` to opt out, in which case each CDC-capable table runs
its own session per poll). When on, it partitions tables into K shards (by stable hash of the native
identity; K = `cdc.shared.reader.threads`, default = `max(1, max.concurrent.connections // 4)`
-- per channel, so ~2K daemon slots across both channels, near half the pool) and
runs one driver-resident daemon thread per shard. Each daemon holds a single shared CDC
session over its shard's tables, assembles complete transactions once, and fans them out
to per-table in-memory buffers; it also refreshes each table's schema and samples
`minimum_lsn`/`current_lsn` each cycle. A streaming poll then reads the change records,
schema, and log bounds from its shard and — in steady state — issues no Informix calls of
its own. The daemon activates its shared session at the minimum committed offset across
its shard's tables and relies on the same transaction-atomic `commit_lsn > checkpoint`
recovery as the per-table path, so a lagging or newly added table replays its own history
without duplicating others. This bounds concurrent CDC sessions to K (rather than N) — the
same teardown pressure `cdc.read.timeout.seconds` guards against — at the cost of a
per-shard log-retention floor that tracks that shard's slowest table. A daemon that is not
yet ready, or a table in a schema transition, transparently falls back to a direct
per-table read for that poll.

## Snapshot semantics

Informix snapshot SQLI reads use `snapshot.read.timeout.seconds` (default
`300`) so large staged pages are not constrained by the normal 30-second
transport timeout. The same budget covers the incremental-snapshot key-bound
read that seeds a keyed table.

A snapshot read whose connection is dropped mid-response (a `truncated SQLI
stream` EOF from an idle NLB/PrivateLink reset or a server-side session reap)
is retried in place rather than surfaced as a stream failure: the dead
transport is reset and the identical read reissued under the connection slot
already held, bounded by a few short-backoff attempts before it re-raises. This
mirrors the CDC poll's reconnect and is safe because each snapshot read
(`max_primary_key`, the incremental `snapshot_chunk`, and the keyless one-shot
`snapshot_page`) advances no offset and runs in its own repeatable-read
transaction, so the reissued read is idempotent.

That key bound is read differently depending on the key. For a **plain-column
key**, the connector resolves the maximum tuple one column at a time: it reads
`FIRST 1 <col> ... ORDER BY <col> DESC` for the leading column, then repeats for
each following column under an equality filter pinning the already-resolved
columns (`WHERE c1 = ? [AND c2 = ?] ...`). Every step is a single-column ordered
read within an equality prefix, which the key index serves as a directional
sub-range scan — never a whole-table sort — and it stays index-servable even
when the index mixes ASC/DESC column directions, where a single composite
`ORDER BY c1 DESC, c2 DESC` would match neither scan direction and force a sort.
All steps run in one REPEATABLE READ view so the columns resolve against a
single point in time. For a **DATETIME-chunked key**, the equality prefix would
have to match an order-preserving cast expression that no index serves, so the
connector keeps a single composite `SELECT {+FIRST_ROWS(1)} FIRST 1 ... ORDER BY
<expr> DESC` read; the `{+FIRST_ROWS(1)}` directive asks the optimizer for an
index read over a full scan + top-sort (and is silently ignored where the server
disables external directives). Either way the read gets the snapshot budget
above, because the bare 30-second default would time out and crash-loop the
stream on every restart.

The keyset **page reads** that copy a keyed table carry the same
`{+FIRST_ROWS(n)}` directive. Their predicate is the OR-form
`(k1 > ?) OR (k1 = ? AND k2 > ?)`, which optimizers often do not recognize as an
index range start, so a composite `ORDER BY k1, k2` can tip into a full scan +
top-sort per page — and since each page then rescans and discards the whole
already-copied prefix, cost grows with the cursor until a page times out on a
large table (the tw305 signature). Three things steer it back to an index seek:
the `{+FIRST_ROWS(n)}` directive; `UPDATE STATISTICS HIGH` on the source, which
steers the cost model to the same plan; and a **guarded leading-column seek
bound** — a redundant `k1 >= ?` (`k1 <= ?` when the leading column is DESC)
prepended to the predicate. Unlike the directive, the bound is honored even where
external directives are disabled, and it gives the optimizer a concrete value to
seek to. It is result-preserving because every OR-form disjunct already implies
it (and the OR-form already excludes NULL leading values, so the bound removes
exactly the same rows), and it does not touch the `ORDER BY` or the OR-form, so
mixed-direction iteration (below) is unchanged. It is **guarded** — omitted
rather than risk silently dropping rows — when the leading key is an
order-preserving cast (`chunk_exprs`, which no index serves) or the leading
cursor value is NULL (`k1 >= NULL` is UNKNOWN for every row and would return an
empty page; a nullable leading column under `allow.nullable.index` can resume
from NULL).

**Mixed-direction key indexes.** When the key comes from a discovered index (a
PRIMARY KEY constraint or a promoted UNIQUE index) whose columns are not all
ascending — e.g. `(a ASC, b DESC)` — the connector pages in the index's *own*
column order and direction rather than forcing an all-ascending order. The
`ORDER BY` matches the index (`ORDER BY a, b DESC`), so a forward index scan
serves every page without a sort; the keyset comparison flips per column (`>` for
an ASC column, `<` for a DESC one); the key bound (`max_primary_key`) reads each
column's last value in that order (an ASC column's max, a DESC column's min); and
the driver-side cursor/bound comparisons run in the same order. This is the only
sort-free option for a mixed-direction index, since no all-ascending `ORDER BY`
can be served from it.

A `primary.keys` **override** participates too: although it names columns rather
than an index, the connector looks up an index whose key columns exactly match
the override (in order) and adopts that index's per-column directions, so an
override that restates a mixed-direction index's columns still pages in the
index's own order. The lookup is memoized per table+key. When no index matches
the override columns, paging stays ascending (and, lacking a matching index,
would sort regardless). A DATETIME-chunked key keeps the default ascending order
(its order-preserving cast has no direction-aware index to match), and an
all-ascending key is unaffected.

CDC session setup and teardown while validating the initial
CDC boundary use `cdc.read.timeout.seconds` (default `60`) for the same reason:
`syscdcv1` open/start/activate/end/close calls can stall under many concurrent
CDC sessions and should not fail against the 30-second default.

When the shared reader supplies snapshot page N as its new start offset,
pages below N are acknowledged and removed best-effort. Page N, later pages,
and the manifest remain available for retry. The remaining staging scope is
removed after the checkpoint reaches the stream phase.

The current maximum LSN is discovered before reading data. Eligible tables are discovered, their schema is recorded, and all rows from each table are selected. Snapshot records are read events (`op=r`) in the CDC change envelope. The `snapshot.mode=initial` scan runs in one transaction whose isolation is set by the per-table `snapshot.isolation` option. Its default, COMMITTED READ LAST COMMITTED, reads the last-committed image of a row a concurrent writer has locked rather than holding shared locks, so the scan does not lock the table against writers; because it no longer freezes the table, rows inserted during the scan can be captured by both the snapshot and the change stream — a keyed table de-duplicates these by primary key, but a keyless table cannot and may emit duplicate rows. REPEATABLE READ (`snapshot.isolation=repeatable_read`) restores exactly-once by holding shared locks on every row read (a sequential scan effectively locks the whole table) for the snapshot's duration. Schema locks are taken while metadata is captured regardless of the level.

For Lakeflow, snapshot pagination is deterministic by the complete primary key (`ORDER BY pk`, seek after `last_pk`), at `snapshot.page.size` rows per page (default `20000`). A keyless `initial` snapshot has no seek cursor, so it pages positionally and stages larger pages by default under its own knob, `keyless.snapshot.page.size` (default `50000`); each staged page is still bounded by an internal per-page byte ceiling, and the whole drain by `snapshot.max.rows`/`snapshot.max.bytes`. The production bridge reads all source pages inside one repeatable-read transaction and captures a transactional snapshot LSN for row ordering, but retains only one page in worker memory. Each shaped page is encoded as typed JSON, gzip-compressed, hash-protected, and atomically published under `snapshot.staging.location`, the connector's only Volume. A manifest becomes visible only after every page is durable. Lakeflow checkpoints a page index and `readBetweenOffsets()` replays the exact immutable page, so planning, cancellation, worker replacement, and retry cannot change its rows. Staging contains source values and therefore requires restricted Volume permissions. It is removed after a committed stream-phase checkpoint is observed; abandoned scopes are eligible for best-effort cleanup after `snapshot.staging.retention.days` (default `4`). Active and checkpoint-referenced scopes are retained. After the staged snapshot commits, both channels advance directly to its snapshot LSN because its rows represent the committed state visible at that boundary. The earlier prepared LSN establishes full-row logging and a valid capture window while the transaction starts; it is not replayed after the completed snapshot. The bridge probes `sysmaster:sysdatabases.is_ansi`; non-ANSI databases receive explicit `BEGIN WORK`, while ANSI databases commit the probe's implicit transaction and let the first snapshot statement begin the repeatable-read transaction implicitly. Both branches and their rollback paths have source-local protocol/ordering coverage. `ansi_live_test.py` provides an opt-in regression through the standard connector test configuration and refuses to run unless the target reports `is_ansi=1`; executing it still requires an ANSI-enabled live database. Capture-capable tables without a primary key default to append-only CDC under the `auto` mode (`append.only.ingestion=auto`, the default); set `append.only.ingestion=false` to opt into bounded snapshot-only ingestion instead, or `true` to force append for a keyed table as well. Alternatively, the per-table `primary.keys` option (a comma-separated list or JSON array of column names) overrides the table's key: the connector then treats even a physically keyless table as keyed on those columns — reading it as `cdc_with_deletes` with keyset-paginated snapshots and delete identification, and reporting those keys so the pipeline adopts them as the destination merge key. The declared columns must exist and the operator must guarantee they are unique per row; a non-unique override corrupts upserts, delete identification, and snapshot pagination. Before falling back to keyless, a table with no primary-key constraint is promoted to keyed on a `UNIQUE` index when `primary.key.from.unique.index` is enabled (the default): metadata discovery reads the table's unique indexes and, choosing deterministically — only indexes whose columns are all `NOT NULL` (a primary key must be non-null for keyset pagination and dedup), then the fewest columns, then index name ascending — adopts that index's columns as the primary key. This is catalog-derived (a schema refresh re-derives it) and is overridden by an explicit `primary.keys`; when it fires, the table is keyed rather than append-only, so it precedes the `auto`/`append` classification above. Only indexes whose columns are all `NOT NULL` qualify by default, but the per-table `allow.nullable.index` option relaxes that: when set (and no non-null key exists), a `UNIQUE` index that includes nullable column(s) may stand in as the key, the operator asserting those columns are unique and non-null for every row. This nullable promotion is applied at the connector (the option is table-level) and, like a `primary.keys` override, is preserved across schema refreshes. The precedence is therefore: explicit `primary.keys` → primary-key constraint → all-`NOT NULL` `UNIQUE` index → (if `allow.nullable.index`) nullable `UNIQUE` index → keyless.

Snapshot page payloads reach the staging Volume through one of two transports, selected by `snapshot.staging.transport`. `fuse` writes them through the UC Volume FUSE mount. `rest` writes them through the Files REST API (`PUT /api/2.0/fs/files/Volumes/...`), which never touches the mount and so avoids the FUSE `ENOTCONN` disconnects that recur under sustained write frequency; only page *writes* move — reads and cleanup still use the mount, and a REST-written page is FUSE-readable because it is the same Volume file. Because head election lives in Postgres, a single overwrite `PUT` replaces the FUSE candidate/atomic-rename fence (a page's bytes are a pure function of its committed rows, so a re-published page is byte-identical). The Files API needs a bearer token, but the reader processes that stage pages have no ambient identity — only the driver, at module-load, does. So the connector mints a personal access token from the driver-captured credential, carries it to the reader in the captured-credential dict, and thereafter refreshes it by having the still-valid token mint its successor at half of `snapshot.staging.token.lifetime.seconds` (default `86400`) — no ambient identity is needed after the first mint. Each token is tagged with the pipeline id, stored as a workspace secret for rotation/audit, and the pipeline's prior tokens are revoked on startup. `auto` (the default) uses `rest` when a token can be minted and falls back to `fuse` when the workspace forbids personal-access-token creation, so the feature is safe to leave on where the platform does not support it. On the REST transport, a **keyless** `initial` drain also pipelines by default (`snapshot.staging.pipeline`): a single bounded background uploader overlaps the next page's fetch with the previous page's shape-and-upload, so the connection reads while the Volume write is in flight — roughly halving wall-clock for an I/O-bound keyless drain, holding at most a few pages resident (depth-1 backpressure), and needing no second connection slot because the upload uses the Files API rather than Informix. Page order and the immutable-page contract are unchanged (one worker, in order); a keyed drain is not pipelined, and pipelining is never engaged on the FUSE transport because it would raise the mount's write frequency.

Decoded string values are sanitized for embedded NUL (`\x00`) bytes by the `string.null.byte` option, applied identically in the CDC decoder (`decode_value`) and the snapshot/query decoder (`_decode_result_value`). A NUL is genuine content — the decoders already trim real CHAR padding (trailing spaces) and VARCHAR/LVARCHAR are length-delimited — but many downstream sinks cannot store it. The default `null` converts a string containing any NUL to `NULL`, which is honest and detectable downstream rather than silently altering the value; `empty` strips the NUL characters, preserving the rest; `keep` passes them through. Numeric and metadata results are untouched (sanitization only affects strings that contain a NUL). Because a promoted unique-index or `primary.keys` key column must be non-null, `empty` is the safer mode when text key columns may carry a NUL.

An `initial`-mode snapshot drains the whole table through one repeatable-read
transaction, holding a connection slot for the entire scan; with many tables this
can consume every slot and starve the streaming/CDC readers waiting for one.
`snapshot.shared.session` selects the mitigation. Default `true` (Model C): the
drain runs on a bounded pool of `snapshot.reader.threads` (default
`max(1, daemon.connection.reservation − 1)`, i.e. one below the CDC-daemon reservation)
driver-resident daemon workers. Keyless (append-only, no primary key) tables drain
one-at-a-time through this pool, so a single worker serialized every keyless table's
initial scan and the later tables sat queued behind it. Keeping the default one below
the reservation guarantees the snapshot-drain floor never collapses to 0, so a low slot
stays reachable by consumer reads even while the drains hold their slots for whole scans;
it parallelizes drains once the pool is large enough to spare a drain band and still keep
that consumer floor. `false` (Model A): the drain runs inline but
acquires its slot above `snapshot.connection.reservation`, so that many low slots
are always reachable by non-snapshot readers and can never all be held by drains at
once — the same slot-floor mechanism the delete-channel reservation uses. Only the
monolithic full-table drain (`snapshot.mode` `initial` or `initial_only`, which
holds one connection for the whole scan) takes the floor; every other read releases
its slot after each microbatch — a keyed table's default `incremental` /
`auto_snapshot` snapshot reads one bounded chunk per microbatch, and stream/CDC reads
are one bounded poll each — so they never hold long enough to starve anyone and may
freely borrow the reserved slots. A `0` (unset) reservation is interpreted in Model A
as `floor(max.concurrent.connections / 3)`, rounded up to at least 1 whenever the
pool has 2 or more slots (0 only for a single-slot pool, which cannot spare one), so
a long drain leaves streaming readers about two-thirds of the pool without any
tuning; a positive value overrides it. Under Model C, the consumer
establishes the durable boundary
(`_initial_lsn`), releases its connection slot, and waits — holding no slot — for a
worker to stage the table and publish its manifest, then serves the staged pages
through the ordinary resumed-snapshot path. Each worker builds a private reader
that drains inline (shared mode cleared on its options), stages and publishes the
manifest durably, then closes to release its slot, so at most `snapshot.reader.threads`
slots are held by snapshots at once and the drain is crash-recoverable exactly as
the inline path is (a restart with no manifest re-drains). Keep `snapshot.reader.threads`
below `max.concurrent.connections` so a floor of slots always remains for streaming
readers. The consumer's wait is a **liveness watchdog, not a total-duration deadline**:
the drain reports progress each time it stages a page, and the consumer trips only after
the drain has reported nothing for a whole `connection.wait.timeout.seconds` window. A
large table that keeps staging pages therefore drains for as long as it needs, while a
wedged drain — a hung fetch, or a dead worker thread — stages nothing and trips after one
window. Because progress is tied to *actual staged pages*, a hung fetch cannot masquerade
as liveness; and picking a job up plus each staged page resets the clock, so a job queued
behind other drains is not charged for the wait. The same `connection.wait.timeout.seconds`
also bounds how long the worker waits for its connection slot — the drain must acquire
*and hold* a slot for the whole scan, so on a contended pool it is the most vulnerable
reader. That slot wait is itself a **liveness watchdog** (`snapshot.drain.slot.liveness
.enabled`, default on): the worker resets its window every time the pool *claims* a slot —
turnover proving other work is completing and recycling capacity — so on a small pool
during a large multi-table backfill the single keyless drain worker waits the backfill out
instead of timing out against a pool that is full but healthy. While it waits it also pings
the consumer's watchdog (turnover counts as the running job's own progress), so the
consumer does not declare it stalled meanwhile. The worker gives up only when the pool is
genuinely **wedged** — no slot claimed for a whole window — and then fails with a
`ConnectionCapacityUnavailable` whose message says the pool made no progress, distinct from
the consumer's "stalled" message (which now means only that the worker went dark without
recording an error). None of this creates capacity: a wedged pool still fails, just with a
precise diagnosis. Size `max.concurrent.connections` for the number of tables that snapshot
concurrently, and give the drain room to win a slot (see `daemon.connection.reservation`
and `snapshot.reader.threads`). Set `snapshot.drain.slot.liveness.enabled=false` to revert
the worker to a plain fixed deadline.

The per-table `snapshot.mode` option supports `incremental` (default), `initial`,
`initial_only`, `cdc_only`, `auto_snapshot`, and `recovery`. `initial`
snapshots only without a checkpoint. `initial_only` completes snapshot pages but
does not subsequently poll CDC. `cdc_only` publishes the current schema and LSN
as a completed zero-row snapshot so both channels begin at the same future-only
boundary. `auto_snapshot` uses the incremental PK-chunked strategy, derives a
deterministic replacement pipeline scope from the expired checkpoint identity,
lets the upsert reader publish a new CDC/copy boundary, and makes the delete
reader adopt that boundary while the upsert channel copies existing rows. `recovery` requires a
retained stream checkpoint and an exactly matching schema fingerprint; it may
rebuild a missing schema node but never snapshots data or treats changed schema
as recoverable. To force a fresh snapshot of a table that already has a
checkpoint, request a Lakeflow full refresh for that update. `configuration_based` and `custom` are
rejected because they depend on external framework extensions that cannot preserve the Python
connector's two-reader checkpoint protocol.

Lakeflow creates upsert and delete streams independently and does not transfer offsets between them, so the two channels coordinate through shared state in a Lakebase Postgres endpoint the connector provisions on first use. For each table, only the upsert reader enables full-row logging and publishes initialization, completed-snapshot, triggered-update, and schema-transition boundaries; the delete reader consumes the same records. Each is elected by `INSERT ... ON CONFLICT DO NOTHING RETURNING`, so exactly one writer commits a record and every loser reads back the winner's value. A record cannot be partially visible, so there is no incomplete state to quarantine.

Before Spark serializes the reader classes, the generated source constructs the registration scope as `<spark.pipelines.pipelineId>_@_<spark.pipelines.updateId>` from two canonical driver UUIDs. If either property is unavailable, it falls back to a random 32-character secret scope. Offset version 10 embeds either the staged snapshot page index (blocking `initial` snapshots) or the incremental chunk cursor (default `incremental` snapshots), plus trigger generation and high-water boundary, and every resumed reader migrates its returned checkpoint to the current update scope. Global immutable schema nodes are shared and never update-scoped. A completed consistent snapshot advances both channels directly to its fresh snapshot LSN.

Connection capacity is a fixed set of pre-seeded `conn_slots` rows, so capacity is the row count and cannot be exceeded by any interleaving. A claim is one `UPDATE ... FOR UPDATE SKIP LOCKED` that takes the lowest slot whose lease is free or expired and increments an `epoch`; concurrent claimants skip a row being mutated rather than waiting on it, so N racers claim N distinct slots. That `epoch` is the fencing token: renewal and release are both guarded on `(owner, epoch)`, so a holder stalled past its 120-second lease cannot resurrect it, and — more importantly — cannot free a slot its successor now legitimately holds. A heartbeat renews every 30 seconds on its own connection; a rejected renewal means the lease is provably lost, so the reader closes its SQLI transport rather than exceeding the configured concurrency. A lease nobody releases simply expires.

Background daemons draw from the same pool but, unlike a consumer read, never release their slot per microbatch: a busy CDC shard re-reads in a tight loop and a snapshot drain holds its slot for the whole scan. Because the upsert and delete channels are sharded independently, `cdc.shared.reader.threads` (K) means up to 2K daemon slots; its default of `max(1, max.concurrent.connections // 4)` keeps that near half the pool, but a higher value can otherwise pin every slot and starve a fresh consumer bootstrap read, which then times out and fails the query. `daemon.connection.reservation` (default `floor(max.concurrent.connections / 3)`, at least 1 for a 2+-slot pool) floors the **CDC** daemon's slot acquisition, so those low slots stay reachable by consumer bootstrap/snapshot/incremental reads. The two daemon kinds are not equal, though: the CDC daemon is deferrable, but a **snapshot drain** must finish to unblock its append-only consumer, so it floors one band *below* the CDC daemon — at `daemon.connection.reservation − snapshot.reader.threads` — reserving `snapshot.reader.threads` slots the CDC daemon cannot claim. Sharing the CDC floor instead would let a saturated CDC daemon starve the drain: the drain worker's `get_table` would block on slot acquisition against a band the CDC daemon has fully pinned — and because that wait is now a turnover-based liveness watchdog (`snapshot.drain.slot.liveness.enabled`), a CDC daemon that keeps re-claiming those slots would even keep the drain waiting indefinitely rather than surfacing a failure. The dedicated sub-CDC band avoids that: it reserves `snapshot.reader.threads` slots the CDC daemon can never claim, so the drain always has capacity the CDC daemon is locked out of. When the pool cannot spare a private drain band (drain threads ≥ the reservation) that floor collapses to 0, where the drain still reaches the CDC-free low slots. It all uses the same `slot_id >= floor` mechanism as the delete-channel and snapshot reservations; consumer reads acquire at floor 0 and so may use the whole pool.

Offsets include both a schema-layout fingerprint and a unique schema-node ID. Shared state stores ID-linked schema generations, Informix catalog table IDs, and observation LSNs. On restart, an appended nullable CDC-supported column is accepted only when every existing descriptor and the primary key remain identical. Both readers register the expanded projection and replay it directly from their retained checkpoints. Informix returns `NULL` for the appended columns on pre-DDL records and their logged values on post-DDL records, so no restart-time data boundary is introduced. Other schema changes fail closed and require a full refresh.

Committed state records are never deleted automatically, and no candidate garbage collection is needed: an election that loses commits nothing, so a publisher that dies mid-write leaves nothing to reclaim.

Schema-node IDs are deterministic hashes of physical table identity and schema
fingerprint. The node describes a reusable layout; refresh-specific initial and
snapshot LSNs remain in update-scoped records, so an unchanged full refresh does
not create another global schema node.

AvailableNow coordination uses exactly one immutable high-water boundary per
physical table and current pipeline/update scope. It is deliberately independent
of either channel's prior trigger generation, allowing independently checkpointed
upsert and delete readers to reconverge after cancellation or retry.

## Dynamic table and schema discovery

Use Informix catalog metadata (via catalog queries) to discover all base `TABLE` objects in the configured database. Exclude Informix system catalogs (`sys*` owners/tables), views, synonyms, sequences, and `syscdcv1` internals. Apply include/exclude patterns after normalizing each identity to `database.owner.table`. Expose a stable Lakeflow table name; the least ambiguous choice is `owner.table`, while retaining `database` and `owner` in `table_options` or an internal lookup.

For each table discover, in ordinal order:

- column name, JDBC/Informix native type, length/precision/scale, nullability, and default;
- primary-key columns and their key sequence;
- table owner and database;
- whether the column can be included in native CDC capture.

Refresh metadata before every newly opened CDC stream to capture DDL performed while the connector was offline. A native METADATA record signals schema change. The Python implementation should re-query the affected table metadata before decoding subsequent rows and version/cache the resulting Spark schema.

Recommended Informix-to-Spark mapping follows JDBC semantics: SMALLINT/INTEGER/BIGINT to short/int/long; REAL/FLOAT to float/double; explicit DECIMAL(p,s)/MONEY to fixed DecimalType; variable-scale DECIMAL(p) is decimal floating point (p is a significant-digit count with a floating exponent, so magnitude is not bounded by p), identified by scale sentinel 255, mapped via the `decimal.variable.type` option (`decimal(p,s)` default `decimal(38,18)`, or `string`/`double`/`integer`) with per-column `decimal.variable.column.type` overrides; CHAR/VARCHAR/LVARCHAR/NCHAR/NVARCHAR/CLOB/TEXT to string; DATE to date; DATETIME to timestamp with precision derived from the declaration; INTERVAL to string unless a lossless Spark interval mapping is proven; BOOLEAN to boolean; BYTE/BLOB/binary to binary. Informix DATETIME has no timezone and is interpreted in UTC. Preserve unsupported UDT/complex values as a documented placeholder or reject those columns; silently returning null is unsafe.

Informix requires full-row logging to be disabled for `ALTER TABLE`. Stop every pipeline using the connection first. Application writes must be quiesced before logging is disabled and remain quiesced until the nullable columns are appended and logging is re-enabled; writes may resume immediately after re-enabling logging. On restart the expanded capture projection replays from the retained checkpoint, returning null appended fields for pre-DDL events and their real values for post-DDL events. A backfill only propagates if it produces logged row changes CDC can capture: an `ALTER TABLE ... ADD ... DEFAULT` sets existing rows as part of the DDL, and any `UPDATE` run while full-row logging is disabled is likewise uncaptured — in both cases the destination keeps null while the source is fully populated, because only the schema change was received, not the values. Backfill by re-enabling logging and issuing an `UPDATE` that touches the rows (e.g. `SET col = col`), or by full-refreshing the table to re-snapshot every row with the column populated. Drops, renames, reorders, existing-column changes, non-nullable/unsupported additions, and primary-key changes remain full-refresh operations.

## Lakeflow row and delete contract

The Lakeflow interface does not accept a CDC change envelope as a table row. For each table:

- `get_table_schema()` returns source columns plus a stable internal cursor such as `_informix_change_lsn` (string or decimal(20,0)); optionally include `_informix_commit_lsn`, `_informix_tx_id`, and `_informix_op` if downstream observability needs them. These correspond to conventional CDC source metadata.
- `read_table_metadata()` returns primary keys, cursor field `_informix_change_lsn`, and `ingestion_type="cdc_with_deletes"` for key-bearing tables.
- `read_table()` returns INSERT `after`, UPDATE `after`, and snapshot rows. It must not return DELETE or tombstone records.
- `read_table_deletes()` independently replays the same CDC range and returns DELETE `before` reduced to primary keys plus the cursor. Non-key columns may be null. It must use its own checkpoint channel; sharing a mutable session/offset with `read_table()` would cause one channel to consume events needed by the other.

For Lakeflow SCD Type 2, `sequence_by` controls both event ordering/deduplication and the values and data types of `__START_AT` and `__END_AT`. Sequencing by the default `_informix_change_lsn` therefore produces string validity columns containing fixed-width, zero-padded 20-digit decimal LSNs; their lexical order is their numeric order. A table may instead select a non-null source timestamp such as `updated_at` to obtain timestamp validity columns, but that timestamp then replaces the LSN as the Auto CDC ordering key. It must change on every source mutation and be precise enough to order repeated changes to one primary key. Informix CDC extraction and recovery still checkpoint native LSNs independently of the downstream Auto CDC sequence. Existing targets using the former unpadded LSN encoding, or changing the sequence type, must be recreated or fully refreshed. `__END_AT IS NULL` denotes the current active version.

Updates that change a primary key require delete-old plus insert/upsert-new semantics. If the Lakeflow pipeline cannot derive this from one update event, `read_table_deletes()` must return the old key and `read_table()` the new row at the same change LSN.

Informix TRUNCATE has no row keys. It cannot be represented faithfully by `read_table_deletes()` and must fail with a clear unsupported-operation error unless the surrounding framework gains a table-truncate primitive. Ignoring it leaves stale target rows.

## Delete and tombstone behavior

A native DELETE contains the before-image. A delete envelope contains `before=<old row>`, `after=null`, `op=d`. Tombstone records with a null value are not required or generated.

Lakeflow requires the delete record, not the Kafka tombstone. Do not generate or expose tombstones. A null payload is not a delete and must not advance the delete channel. Full-row logging and a primary/message key are required to construct reliable deletion rows.

## What can be ported to Python

These parts are ordinary control/data logic and should be implemented in Python under the Informix source package:

- configuration validation and safe identifier handling;
- dynamic table filtering and SQL/catalog metadata discovery;
- snapshot high-water capture, snapshot queries, PK seek pagination, and Spark schema/type mapping;
- CDC session lifecycle SQL (`cdc_opensess`, full-row logging, start/activate/end capture, close);
- label-to-table bookkeeping;
- transaction holders, BEFORE/AFTER pairing, DISCARD handling, commit-only emission, rollback suppression, and transaction-safe batch boundaries;
- the three-LSN recovery algorithm, retention validation, Lakeflow offsets, retry/error policy, and separate upsert/delete projections;
- conventional CDC source metadata for downstream compatibility if desired.

No Kafka-based CDC frameworks, message-bus offset stores, or external schema-history stores are required by Lakeflow. Lakeflow checkpoints replace the offset store, and live Informix metadata can replace most schema-history behavior provided DDL/restart boundaries are handled carefully.

## Why ordinary Python SQL is insufficient, and the implemented resolution

The source protocol requires more than calls to the `cdc_*` SQL functions:

- Native CDC record streaming is not a normal SQL result-set fetch exposed by common Python DB-API drivers.
- The binary CDC header, operation records, column descriptors, null/value encodings, temporal/decimal values, UDTs, and metadata must be decoded according to the Informix CDC protocol specification.
- The outer size prefix alone is insufficient to implement a decoder; record headers, metadata descriptors, per-type encodings, SQLI transport, and transaction semantics are also required.

The connector implements that required path directly in Python: SQLI socket/TLS negotiation, bounded normal username/password authentication, SQL query/cursor handling, `SQ_LODATA` streaming, CDC framing and value decoding, transaction/recovery behavior, and Lakeflow shaping. It has no JVM, Kafka, or external CDC framework dependency at execution time and is designed to run on serverless Lakeflow compute.

The implementation targets the Informix SQLI wire protocol and native CDC framing directly. Its supported SQLI, snapshot, transactional CDC, PAM, TLS, and serverless Lakeflow paths have live validation; excluded data types, authentication modes, genuine IDS/HDR-issued redirects, and online DDL without resnapshot remain explicit limitations.

## Delivery guarantees and operational limits

- Guarantee at-least-once, not exactly-once. A failure after rows are returned but before the Lakeflow checkpoint commits causes replay.
- Deduplicate/order on `(commit_lsn, change_lsn)`; timestamps and transaction IDs are not globally monotonic.
- Never run two consumers with the same mutable local state. Native CDC sessions may be separate, but each Lakeflow checkpoint channel must be isolated.
- Preserve backpressure with bounded decoded-record/transaction buffers. A single large transaction can exceed the configured row batch and memory budget; fail explicitly rather than split before COMMIT.
- Low traffic returns unchanged offsets after CDC timeout. The idle protocol does not require heartbeats.
- If retained logs no longer contain `begin_lsn`, resnapshot; resuming from `commit_lsn` can lose interleaved open transactions.
- Do not claim support for TRUNCATE, unsupported UDT/complex/LOB payloads, PK-less tables, or online DDL until their explicit behavior is implemented and tested.

## PAM authentication and connection redirects

PAM and redirects occur at different protocol layers and must not share a continuation path. This section specifies their implementation based on the Informix SQLI wire protocol and the Informix 15 developer image's installed `sqlhosts.demo` and machine notes.

### PAM server fixture

The Informix 15 Linux image contains PAM support and the OS PAM stack. PAM is enabled per SQLI listener by the fifth `sqlhosts` field, not globally. Add a dedicated listener rather than changing the ordinary password listener:

```text
informix       onsoctcp  *informix-primary  9088
informix_pam   onsoctcp  *informix-primary  9090  s=4,pam_serv=informix
```

`s=4` selects PAM challenge/response authentication and `pam_serv=informix` selects `/etc/pam.d/informix`. Keep the normal listener for setup, health checks, and tests which are not exercising PAM. The fixture should create a non-privileged OS user (for example `cdc_pam`) with a deterministic test-only password and install this root-owned mode-0644 service:

```text
auth       required     pam_unix.so
account    required     pam_unix.so
```

The user must separately receive the Informix database/CDC grants; successful PAM authentication does not grant database privileges. Never use this deterministic PAM setup outside the disposable fixture. The image is RHEL 8.10, provides `/etc/pam.d`, and its installed Informix machine notes confirm Linux PAM through the OS `libpam.so`.

The client connection request must continue to advertise `CLNT_PAM_CAPABLE=1` in its primary environment. The reference client adds that environment property unconditionally. The initial connection request may contain the configured password, but the PAM listener does not complete authentication from that field; it advertises PAM in server-version capability bit 44 and starts the exchange below.

### Exact PAM SQLI state machine

PAM begins only after the ASF connection is accepted and the client has completed `SQ_VERSION_REQ`/`SQ_VERSION_REPLY`. It completes before private-server exchange and before the secondary environment request:

```text
ASF accept -> server version -> PAM -> private-server (if any) -> secondary env
```

1. If server-version bit 44 is clear, do not enter PAM mode.
2. Send the normal SQLI ACK (`sendACK()`), flush/flip the protocol buffer.
3. Read one signed big-endian 16-bit SQLI message type:
   - `129` (`SQ_CHALLENGE`): read `message_style:i16`, `message_length:i16`, then exactly `message_length` padded character bytes, followed by normal `SQ_EOT` handling. Reject negative lengths and lengths greater than 512 before allocation/read.
   - `127` (`SQ_ACCEPT`): mark PAM authorized, then consume `SQ_EOT`; authentication is complete.
   - `56` (`SQ_EXIT`): reply with `SQ_EXIT` and fail authentication.
   - anything else: fail as a protocol error; never fall back to password authentication on the same socket.
4. Challenge styles are `1=PAM_PROMPT_ECHO_OFF`, `2=PAM_PROMPT_ECHO_ON`, `3=PAM_ERROR_MSG`, and `4=PAM_TEXT_INFO`. Styles 1 and 2 require a response. Styles 3 and 4 are informational and require no `SQ_RESPONSE`, but the loop must continue reading because another challenge or accept follows.
5. For a required response send `SQ_RESPONSE(130):i16`, `response_length:i16`, encoded response bytes using the negotiated client encoding/padding rules, and `SQ_EOT`. Reject encoded responses over 512 bytes (the wire bytes must be bounded).
6. Repeat with a configurable hard round limit. Success is only `SQ_ACCEPT`; EOF, timeout, `SQ_EXIT`, malformed EOT, an exhausted round limit, or a response-provider failure must close the socket and fail authentication.

The serverless connector cannot use an interactive callback. Its response provider should be deterministic and secret-backed: by default return the configured password only for style 1, optionally return a separately configured value for style 2, and never log either challenge responses or the initial password. Informational/error text may be logged only after control-character stripping and truncation. Synthetic tests must cover multi-round challenges, styles 1--4, empty response, 512/513-byte boundaries, multibyte encoded-length overflow, accept, exit, missing EOT, timeout, and round exhaustion.

### Redirect wire format and client behavior

A redirect is an ASF response, before SQLI version negotiation or PAM. The outer service-layer type is `13` (`SLTYPE_REDIRECT`). Its body has the same initial `SQ_ASSOC(100)`, `SQ_ASCBINARY(101)` and common ASC parameters as an accept/reject. The final ASC item is `SQ_ASCDBLIST(103)`, followed by a length-prefixed character string. IBM stores this as `redSrvDetail` and exposes temporary SQL code `-79998`.

Both IBM redirect parsers tokenize that detail with delimiters `:=|`: discard the leading label/error token, then consume `server_name`, `host`, and `port_or_service`. A representative payload is therefore structurally:

```text
<label>=<server_name>|<host>|<numeric_port>
```

The parser must require exactly three non-empty values after the label, a numeric port in `1..65535`, and no trailing fields. Do not implement a service-name lookup because serverless `/etc/services` is not a connector-controlled contract. Decode with a bounded length and reject NULs/control characters.

On a valid redirect, close the original socket and discard every connection-scoped object: input/output buffers, version/capability bits, PAM state, database/session IDs, prepared statements, cursors, CDC session/SmartLOB state, and partial CDC records. Create a fresh socket to the redirected host/port and restart from the ASF connection request using the redirected server name, original database, credentials, locales, timeout, and TLS policy. PAM, when advertised by the target, consequently runs from its beginning.

Redirects create an SSRF boundary because the server supplies a destination. They must be disabled unless `redirect.enabled=true`; require an explicit hostname/IP and port allowlist; resolve and validate every address (including all DNS results) against the allowlist; reject loopback, link-local, multicast, unspecified, metadata, and private addresses unless explicitly allowed; cap redirects (recommended default 3); retain a visited `(server, canonical-address, port)` set; use one overall login deadline; and apply TLS hostname verification to the redirected hostname. Never inherit a TLS verification exception merely because the original endpoint was trusted.

### Redirect integration fixture

A deterministic ASF responder returns session-layer type 13 with a target address. The client allow-lists that exact private target, closes and discards the responder connection state, reconnects with verified TLS, authenticates from the beginning, and completes a query against Informix.

This is live validation of the complete client redirect path—including wire parsing, opt-in policy, private-address allow-listing, state reset, reconnect, authentication, TLS revalidation, and post-redirect SQL.

Required redirect tests are: valid secondary-to-primary redirect, successful query/CDC after reconnect, redirect then PAM, malformed detail, unallowlisted host/port, DNS rebinding/multiple-address rejection, TLS name mismatch, self-loop, two-node loop, maximum redirects, total-deadline exhaustion, and proof that no original cursor/session/partial CDC bytes survive reconnect.

## Required validation cases

Live validation must cover initial snapshot plus concurrent writes; restart after a committed transaction; restart midway through a multi-operation transaction; two interleaved transactions; rollback and rollback-to-savepoint/DISCARD; insert/update/delete including primary-key update; idle timeout; log-retention expiry; partial native records spanning reads; add/drop column while running and while stopped; unsupported LOB/UDT placeholders; table truncate; and independent `read_table` / `read_table_deletes` checkpoint replay. Simulator tests can validate Lakeflow shaping and offsets, but cannot validate the Informix SmartLOB wire format.
