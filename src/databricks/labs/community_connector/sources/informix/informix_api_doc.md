# Informix CDC API research and Python port contract

## Scope and evidence

This document specifies a dynamic Informix connector: every eligible user table is discovered at runtime, then filtered by optional configured table patterns. It is based on:

- the working embedded implementation in `/Users/leon.eller/work/dev/informix-cdc/informix-cdc/src/datasource/`;
- that implementation's pinned Debezium/pydbzengine version, `3.6.0.Final` / `3.6.0.0`;
- Debezium Informix tag `v3.6.0.Final`, commit `920a040f6f3e6458357ab4e758a9d5aa1e1a03e6`, especially `InformixStreamingChangeEventSource`, `InformixOffsetContext`, `InformixSnapshotChangeEventSource`, `InformixConnection`, `DbzCDCEngine`, and `DbzTransactionEngine`;
- the Lakeflow `LakeflowConnect` method and pagination contract.

The protocol is not an HTTP API. Informix exposes a binary change stream through routines installed in the `syscdcv1` database. Debezium invokes those routines over the Informix SQLI/JDBC connection and uses IBM's change-stream client to decode the returned SmartLOB bytes.

## Required source configuration

- Install the Informix CDC API by running `$INFORMIXDIR/etc/syscdcv1.sql`; this creates `syscdcv1` and the `cdc_*` routines.
- The database must use transaction logging and retained logical logs must cover the requested restart LSN.
- The CDC user needs normal metadata/snapshot access to the source database and sufficient rights in `syscdcv1` to open sessions, enable full-row logging, and capture the selected tables. The working deployment grants/uses DBA-level access to `syscdcv1`.
- Full-row logging must remain enabled for watched tables to obtain complete update before-images. Debezium calls `cdc_set_fullrowlogging(table, 1)` itself.
- Connection inputs are hostname, port (default `9088`), database, user, and password. One connector instance/offset partition represents one logical database.

Table names use Informix's database-qualified syntax `database:owner.table`; Debezium's logical `TableId` is `database.owner.table`. Unless `DELIMIDENT` is enabled, arbitrary SQL identifier quoting is not available, so identifiers must be validated and quoted only where Informix permits it.

## Exact native CDC session protocol

Debezium 3.6 performs these calls in this order:

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

   The returned integer is the session ID; a negative value is an Informix error code. `timeout` is seconds and allows an idle read to return a TIMEOUT record. `max_records` limits records returned by a read. Debezium defaults come from `cdc.timeout` and `cdc.max.records`.

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

   Debezium resolves `*` through `SELECT FIRST 1 * FROM database:owner.table` and JDBC result metadata before registration. It excludes BYTE, TEXT, fixed/variable UDT, unknown, and complex columns from the CDC column descriptor; those values are emitted as unavailable placeholders rather than decoded from the stream. The label-to-table mapping is connector state. A TRUNCATE record is anomalous: its table label is read from `userId`, not the normal `label` field.

5. Activate from a 64-bit sequence position:

   ```sql
   EXECUTE FUNCTION informix.cdc_activatesess(session_id, start_sequence)
   ```

   `0` means Informix's current position. Otherwise `start_sequence` is an LSN encoded as `(log_unique_id << 32) + log_position`.

6. Construct `IfxSmartBlob(connection)` and repeatedly call its driver-specific `IfxLoRead(session_id, output_stream, requested_bytes)`. Preserve incomplete bytes between reads. Each CDC record begins with two big-endian 32-bit integers: `header_size`, then `payload_size`; the total record length is their sum. Debezium waits for at least 16 bytes before attempting a record and delegates the actual record decoding to IBM `CDCRecordBuilder`.

7. On shutdown, call `cdc_endcapture(session_id, 0, table)` for each table and then `cdc_closesess(session_id)`. Production must normally leave full-row logging enabled (`cdc.stop.logging.on.close=false`); disabling it on every finite Lakeflow poll introduces capture gaps and affects other consumers.

All `cdc_set_fullrowlogging`, `cdc_startcapture`, `cdc_activatesess`, `cdc_endcapture`, and `cdc_closesess` calls return `0` on success. Non-zero results and negative session IDs must fail the batch without advancing its checkpoint.

## Native record and transaction semantics

The IBM decoder yields BEGIN, INSERT, BEFORE_UPDATE, AFTER_UPDATE, DELETE, TRUNCATE, DISCARD, COMMIT, ROLLBACK, METADATA, TIMEOUT, and ERROR records. Transactional/data records carry the sequence/transaction fields shown below; metadata, timeout, and error use their shorter record-specific headers. Data records carry a capture label.

Debezium groups interleaved records by transaction ID:

- BEGIN creates an in-memory transaction holder and records its begin sequence/time/user.
- INSERT, DELETE, BEFORE_UPDATE, AFTER_UPDATE, and TRUNCATE append to that holder.
- BEFORE_UPDATE is paired with the following AFTER_UPDATE and becomes the update's before-image. Do not emit BEFORE_UPDATE independently.
- DISCARD removes buffered records whose sequence is greater than or equal to the DISCARD sequence. This is required for Informix rollback-to-savepoint semantics.
- COMMIT or ROLLBACK closes the transaction. Rolled-back records are never emitted. Empty transactions are normally omitted.
- METADATA/TIMEOUT/ERROR records without a transaction can be delivered independently. TIMEOUT is an idle/liveness indication, not end-of-stream and not an offset advance.

For a committed transaction, Debezium emits its operations in log order. INSERT maps to create (`after` only), paired update maps to update (`before` and `after`), DELETE maps to delete (`before` only), and TRUNCATE is table-wide. Transaction BEGIN/END metadata is optional. A safe Python reader must finish buffering through COMMIT before returning rows: returning uncommitted operations would make later rollback handling impossible.

The source timestamp for transaction-boundary records is the Informix BEGIN/COMMIT time (epoch seconds); data envelope processing time is connector time. Ordering must therefore use LSNs, not timestamps.

## Exact offset and restart rules

### LSN representation

An available Informix LSN is represented by this connector as a non-negative unsigned 64-bit sequence; Debezium may additionally use `-1`/`NULL` as an unavailable sentinel. Its official form is `LSN(loguniq,logpos_hex)`. Ordering compares the 64-bit sequence. The restart position is a four-part transaction-log position:

```json
{
  "commit_lsn": "<decimal sequence>",
  "change_lsn": "<decimal sequence>",
  "begin_lsn": "<decimal sequence>",
  "tx_id": 123
}
```

The Debezium Kafka offset keys are `commit_lsn`, `change_lsn`, and `begin_lsn`; transaction metadata may add its own fields. Its source partition is `{"databaseName": "<topic/logical name>"}`. The existing pydbzengine wrapper extracts and checkpoints that complete partition/offset map.

### Why three LSNs are required

- `change_lsn` identifies the last delivered operation.
- `commit_lsn` identifies the transaction's commit/end position.
- `begin_lsn` is the oldest transaction BEGIN that must be replayed. Informix transactions can interleave, so resuming only at the last change or commit loses still-open transactions.

At transaction handling start Debezium computes `restartSeq = lowest buffered BEGIN`, or the transaction end sequence when none remain. It monotonically updates positions. After a normal commit it stores commit=end, change=end, and begin=restart. With transaction metadata disabled, it also advances the final data event's position to the transaction end/restart position because no later transaction-END event will carry that offset.

### Recovery algorithm

On restart, activate the CDC session at `begin_lsn` when available, otherwise `commit_lsn`.

1. If restart/begin is less than the checkpointed commit LSN, enter recovery mode.
2. Rebuild transaction holders by replaying from that earlier BEGIN.
3. Skip transactions whose commit LSN is below the checkpointed commit LSN.
4. If commit equals the checkpointed commit and checkpointed change equals commit, skip the whole transaction.
5. Within a recovered transaction, skip operation records with sequence `<= checkpointed change_lsn`.
6. Leave recovery once a transaction commits after the checkpointed commit LSN, then continue normally.

This yields at-least-once delivery around a Lakeflow checkpoint boundary. Only return an `end_offset` after a complete committed transaction has been decoded and all returned rows are materialized. If a call fails, return nothing/raise and leave the prior offset unchanged. Native and projected batch caps are soft while transactions already observed remain open; reading through commit/rollback prevents a transaction larger than `cdc.max.records` from replaying the same prefix forever. METADATA and TIMEOUT control frames do not consume that native record target. `cdc.max.poll.records` is the default hard poll bound; `cdc.max.poll.bytes` optionally adds recursive retained-byte accounting when set above zero. Recovery skips only whole transactions whose commit LSN is already checkpointed; it never filters records inside a newly committed transaction using another transaction's LSN. Informix TIMEOUT remains a hard terminal condition, and an incomplete transaction is then replayed from the retained checkpoint. When Spark calls `prepareForTriggerAvailableNow()`, the Informix-owned generated base wrapper freezes a current LSN independently for that reader without modifying the shared adapter. Lakeflow checkpoints upsert and delete flows independently, so their AvailableNow boundaries may differ and converge on the next triggered update. Spark does not make that callback in continuous mode, so continuous execution follows new commits without a mode-specific option.

Before resuming, validate retention with:

```sql
SELECT MIN(uniqid) AS uniqid, 0 AS logpage FROM sysmaster:syslogs
```

The minimum available sequence is `(uniqid << 32)`. If the restart LSN is older, incremental continuation is impossible; fail explicitly or perform the configured `when_needed` resnapshot. The approximate current/high-water LSN used before a snapshot is:

```sql
SELECT uniqid, used AS logpage
FROM sysmaster:syslogs
WHERE is_current = 1
```

encoded as `(uniqid << 32) + (logpage << 12)`. Debezium records this before snapshot data is read, then streams from it so changes concurrent with the snapshot are not missed.

### Lakeflow pagination shape

The Lakeflow implementation should keep an offset per table and per channel because `read_table()` and `read_table_deletes()` are checkpointed independently:

```json
{
  "version": 2,
  "commit_lsn": "...",
  "change_lsn": "...",
  "begin_lsn": "...",
  "tx_id": 123,
  "phase": "snapshot|stream",
  "snapshot": {"last_pk": ["..."]}
}
```

Offset versions are intentionally strict. Version 2 identifies the fixed-width LSN row encoding; an absent or different version fails with a full-refresh instruction rather than mixing incompatible downstream ordering values.

Each finite call opens/replays a CDC session, reads until at least one complete transaction is available (or timeout), closes it, and returns the exact last committed position. When caught up, return an empty iterator and the unchanged `start_offset`, as required by `LakeflowConnect`.

## Snapshot semantics

Debezium discovers the current maximum LSN before reading data. It discovers eligible tables, records their schema, and selects all rows from each table. Snapshot records are read events (`op=r`) in Debezium's envelope. The default isolation is REPEATABLE READ; schema locks are taken while metadata is captured. READ COMMITTED/READ UNCOMMITTED reduce locking but do not guarantee a consistent snapshot. EXCLUSIVE holds stronger locks for the full snapshot.

For Lakeflow, snapshot pagination must be deterministic by the complete primary key (`ORDER BY pk`, seek after `last_pk`). The production bridge keeps every page of the bounded initial CDC snapshot inside one repeatable-read transaction, captures a transactional snapshot LSN for row ordering, and retains the earlier prepared LSN as the CDC restart boundary. It probes `sysmaster:sysdatabases.is_ansi`; non-ANSI databases receive explicit `BEGIN WORK`, while ANSI databases commit the probe's implicit transaction and let the first snapshot statement begin the repeatable-read transaction implicitly. Both branches and their rollback paths have source-local protocol/ordering coverage. `ansi_live_test.py` provides an opt-in regression through the standard connector test configuration and refuses to run unless the target reports `is_ansi=1`; executing it still requires an ANSI-enabled live database. Tables without a primary/unique key cannot be safely seek-paginated or deduplicated under CDC; either reject them by default or explicitly run snapshot-only.

Lakeflow creates upsert and delete streams independently and does not transfer the snapshot offset between them. `cdc.shared.state.location` is therefore mandatory and must identify a writable Unity Catalog Volume directory shared by every serverless worker. For each table, only the upsert reader enables full-row logging and atomically publishes its retained, non-future initial LSN. The delete reader waits for and consumes the same record. Valid retained state is reused across retries and full refreshes; expired state is rotated only by an upsert reader. Offsets include a schema-layout fingerprint. Before decoding rows after the session's initial METADATA frame, the bridge re-reads the catalog and compares captured membership, type, width, precision, scale, and encoding; another METADATA frame fails immediately. Metadata is also checked after every page/poll, and any changed fingerprint requires a full refresh.

## Dynamic table and schema discovery

Use Informix/JDBC metadata (what Debezium's `readAllTableNames`/`readSchema` does), or equivalent catalog queries, to discover all base `TABLE` objects in the configured database. Exclude Informix system catalogs (`sys*` owners/tables), views, synonyms, sequences, and `syscdcv1` internals. Apply include/exclude patterns after normalizing each identity to `database.owner.table`. Expose a stable Lakeflow table name; the least ambiguous choice is `owner.table`, while retaining `database` and `owner` in `table_options` or an internal lookup.

For each table discover, in ordinal order:

- column name, JDBC/Informix native type, length/precision/scale, nullability, and default;
- primary-key columns and their key sequence;
- table owner and database;
- whether the column can be included in native CDC capture.

Refresh metadata before every newly opened CDC stream, as Debezium does, to capture DDL performed while the connector was offline. A native METADATA record signals schema change, but Debezium 3.6 emits an ALTER event from its existing table model; it does not itself parse DDL from the record. The Python implementation should instead re-query the affected table metadata before decoding subsequent rows and version/cache the resulting Spark schema.

Recommended Informix-to-Spark mapping follows JDBC semantics: SMALLINT/INTEGER/BIGINT to short/int/long; REAL/FLOAT to float/double; DECIMAL/MONEY to DecimalType where precision is bounded (or string for variable/unbounded decimal); CHAR/VARCHAR/LVARCHAR/NCHAR/NVARCHAR/CLOB/TEXT to string; DATE to date; DATETIME to timestamp with precision derived from the declaration; INTERVAL to string unless a lossless Spark interval mapping is proven; BOOLEAN to boolean; BYTE/BLOB/binary to binary. Informix DATETIME has no timezone and Debezium interprets it in UTC. Preserve unsupported UDT/complex values as a documented placeholder or reject those columns; silently returning null is unsafe.

Schema evolution limitations must be explicit: adding/dropping/reordering captured columns changes the native record layout. Refreshing SQL metadata alone is insufficient unless the decoder/capture registration is also restarted consistently at an LSN boundary.

## Lakeflow row and delete contract

The Lakeflow interface does not accept a Debezium envelope as a table row. For each table:

- `get_table_schema()` returns source columns plus a stable internal cursor such as `_informix_change_lsn` (string or decimal(20,0)); optionally include `_informix_commit_lsn`, `_informix_tx_id`, and `_informix_op` if downstream observability needs them.
- `read_table_metadata()` returns primary keys, cursor field `_informix_change_lsn`, and `ingestion_type="cdc_with_deletes"` for key-bearing tables.
- `read_table()` returns INSERT `after`, UPDATE `after`, and snapshot rows. It must not return DELETE or tombstone records.
- `read_table_deletes()` independently replays the same CDC range and returns DELETE `before` reduced to primary keys plus the cursor. Non-key columns may be null. It must use its own checkpoint channel; sharing a mutable session/offset with `read_table()` would cause one channel to consume events needed by the other.

For Lakeflow SCD Type 2, `sequence_by` controls both event ordering/deduplication and the values and data types of `__START_AT` and `__END_AT`. Sequencing by the default `_informix_change_lsn` therefore produces string validity columns containing fixed-width, zero-padded 20-digit decimal LSNs; their lexical order is their numeric order. A table may instead select a non-null source timestamp such as `updated_at` to obtain timestamp validity columns, but that timestamp then replaces the LSN as the Auto CDC ordering key. It must change on every source mutation and be precise enough to order repeated changes to one primary key. Informix CDC extraction and recovery still checkpoint native LSNs independently of the downstream Auto CDC sequence. Existing targets using the former unpadded LSN encoding, or changing the sequence type, must be recreated or fully refreshed. `__END_AT IS NULL` denotes the current active version.

Updates that change a primary key require delete-old plus insert/upsert-new semantics. If the Lakeflow pipeline cannot derive this from one update event, `read_table_deletes()` must return the old key and `read_table()` the new row at the same change LSN.

Informix TRUNCATE has no row keys. It cannot be represented faithfully by `read_table_deletes()` and must fail with a clear unsupported-operation error unless the surrounding framework gains a table-truncate primitive. Ignoring it leaves stale target rows.

## Delete and tombstone behavior

A native DELETE contains the before-image. Debezium emits a delete envelope with `before=<old row>`, `after=null`, `op=d`, then, only when `tombstones.on.delete=true`, an additional Kafka compaction tombstone with the same key and a null value. The working embedded implementation deliberately sets `tombstones.on.delete=false`.

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
- Debezium-compatible source metadata if compatibility is desired.

No Kafka Connect, Debezium engine, topic routing, JSON envelope builder, Kafka offset store, or Debezium schema-history store is required by Lakeflow. Lakeflow checkpoints replace the offset store, and live Informix metadata can replace most schema-history behavior provided DDL/restart boundaries are handled carefully.

## Why ordinary Python SQL is insufficient, and the implemented resolution

The source protocol requires more than calls to the `cdc_*` SQL functions:

- `IfxSmartBlob.IfxLoRead` is an Informix JDBC extension, not a normal SQL result-set fetch exposed by common Python DB-API drivers.
- `CDCRecordBuilder` and the `ReadableType` implementations decode the binary CDC header, operation records, column descriptors, null/value encodings, temporal/decimal values, UDTs, and metadata. Debezium does **not** contain that decoder; it delegates to `com.ibm.informix:ifx-changestream-client` and the Informix JDBC driver.
- The outer size prefix alone is insufficient to implement a decoder; record headers, metadata descriptors, per-type encodings, SQLI transport, and transaction semantics are also required.

The connector now implements that required path directly in Python: SQLI socket/TLS negotiation, bounded normal username/password authentication, SQL query/cursor handling, `SQ_LODATA` streaming, CDC framing and value decoding, transaction/recovery behavior, and Lakeflow shaping. It has no JVM, JPype, Debezium runtime, or IBM JAR dependency at execution time and is designed to run on serverless Lakeflow compute.

The implementation is derived from IBM JDBC 15.0.1.3, change-stream client 1.1.5, and Debezium Informix 3.6.0.Final bytecode documented below. Its supported SQLI, snapshot, transactional CDC, PAM, TLS, and serverless Lakeflow paths have live validation; excluded data types, authentication modes, genuine IDS/HDR-issued redirects, and online DDL without resnapshot remain explicit limitations.

## Delivery guarantees and operational limits

- Guarantee at-least-once, not exactly-once. A failure after rows are returned but before the Lakeflow checkpoint commits causes replay.
- Deduplicate/order on `(commit_lsn, change_lsn)`; timestamps and transaction IDs are not globally monotonic.
- Never run two consumers with the same mutable local state. Native CDC sessions may be separate, but each Lakeflow checkpoint channel must be isolated.
- Preserve backpressure with bounded decoded-record/transaction buffers. A single large transaction can exceed the configured row batch and memory budget; fail explicitly rather than split before COMMIT.
- Low traffic returns unchanged offsets after CDC timeout. Heartbeats are a Debezium/Kafka convenience, not required for the native API.
- If retained logs no longer contain `begin_lsn`, resnapshot; resuming from `commit_lsn` can lose interleaved open transactions.
- Do not claim support for TRUNCATE, unsupported UDT/complex/LOB payloads, PK-less tables, or online DDL until their explicit behavior is implemented and tested.

## PAM authentication and connection redirects

This section is the implementation contract derived from CFR 0.152 output for IBM Informix JDBC 15.0.1.3 (`IfxSqliConnect`, `IfxSqli`, `Connection`, `ConnectionHeaders`) and the Informix 15 developer image's installed `sqlhosts.demo` and machine notes. PAM and redirects occur at different protocol layers and must not share a continuation path.

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

The client connection request must continue to advertise `CLNT_PAM_CAPABLE=1` in its primary environment. IBM JDBC adds that environment property unconditionally. The initial connection request may contain the configured password, but the PAM listener does not complete authentication from that field; it advertises PAM in server-version capability bit 44 and starts the exchange below.

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
5. For a required response send `SQ_RESPONSE(130):i16`, `response_length:i16`, encoded response bytes using the negotiated client encoding/padding rules, and `SQ_EOT`. IBM's response-type property is not serialized. Reject encoded responses over 512 bytes (the Java driver incorrectly bounds Java character count first; Python must bound the wire bytes).
6. Repeat with a configurable hard round limit. Success is only `SQ_ACCEPT`; EOF, timeout, `SQ_EXIT`, malformed EOT, an exhausted round limit, or a response-provider failure must close the socket and fail authentication.

The serverless connector cannot use an interactive callback. Its response provider should be deterministic and secret-backed: by default return the configured password only for style 1, optionally return a separately configured value for style 2, and never log either challenge responses or the initial password. Informational/error text may be logged only after control-character stripping and truncation. Synthetic tests must cover multi-round challenges, styles 1--4, empty response, 512/513-byte boundaries, multibyte encoded-length overflow, accept, exit, missing EOT, timeout, and round exhaustion.

### Redirect wire format and client behavior

A redirect is an ASF response, before SQLI version negotiation or PAM. The outer service-layer type is `13` (`SLTYPE_REDIRECT`). Its body has the same initial `SQ_ASSOC(100)`, `SQ_ASCBINARY(101)` and common ASC parameters as an accept/reject. The final ASC item is `SQ_ASCDBLIST(103)`, followed by a length-prefixed character string. IBM stores this as `redSrvDetail` and exposes temporary SQL code `-79998`.

Both IBM redirect parsers tokenize that detail with delimiters `:=|`: discard the leading label/error token, then consume `server_name`, `host`, and `port_or_service`. A representative payload is therefore structurally:

```text
<label>=<server_name>|<host>|<numeric_port>
```

The parser must require exactly three non-empty values after the label, a numeric port in `1..65535`, and no trailing fields. Do not implement JDBC's service-name lookup because serverless `/etc/services` is not a connector-controlled contract. Decode with a bounded length and reject NULs/control characters.

On a valid redirect, close the original socket and discard every connection-scoped object: input/output buffers, version/capability bits, PAM state, database/session IDs, prepared statements, cursors, CDC session/SmartLOB state, and partial CDC records. Create a fresh socket to the redirected host/port and restart from the ASF connection request using the redirected server name, original database, credentials, locales, timeout, and TLS policy. PAM, when advertised by the target, consequently runs from its beginning.

Redirects create an SSRF boundary because the server supplies a destination. They must be disabled unless `redirect.enabled=true`; require an explicit hostname/IP and port allowlist; resolve and validate every address (including all DNS results) against the allowlist; reject loopback, link-local, multicast, unspecified, metadata, and private addresses unless explicitly allowed; cap redirects (recommended default 3); retain a visited `(server, canonical-address, port)` set; use one overall login deadline; and apply TLS hostname verification to the redirected hostname. Never inherit a TLS verification exception merely because the original endpoint was trusted.

### Redirect integration fixture

The live fixture at `/Users/leon.eller/work/dev/informix-cdc` uses two orthogonal endpoints. A real Informix PAM listener is exposed at `localhost:9090`. A deterministic ASF responder at `localhost:9191` returns session-layer type 13 with a target of `127.0.0.1:9088`. The client allow-lists that exact private target, closes and discards the responder connection state, reconnects with verified TLS, authenticates from the beginning, and completes a query against Informix.

This is live validation of the complete client redirect path—including wire parsing, opt-in policy, private-address allow-listing, state reset, reconnect, authentication, TLS revalidation, and post-redirect SQL—not a claim that IDS/HDR emitted the redirect. The available IDS/HDR topology did not provide a deterministic server-generated redirect. Client-side `sqlhosts` failover, a proxy, or a connection accepted directly by another member must not be reported as ASF redirect coverage because none proves receipt of type 13.

Required redirect tests are: valid secondary-to-primary redirect, successful query/CDC after reconnect, redirect then PAM, malformed detail, unallowlisted host/port, DNS rebinding/multiple-address rejection, TLS name mismatch, self-loop, two-node loop, maximum redirects, total-deadline exhaustion, and proof that no original cursor/session/partial CDC bytes survive reconnect.

## Required validation cases

Live validation must cover initial snapshot plus concurrent writes; restart after a committed transaction; restart midway through a multi-operation transaction; two interleaved transactions; rollback and rollback-to-savepoint/DISCARD; insert/update/delete including primary-key update; idle timeout; log-retention expiry; partial native records spanning reads; add/drop column while running and while stopped; unsupported LOB/UDT placeholders; table truncate; and independent `read_table` / `read_table_deletes` checkpoint replay. Simulator tests can validate Lakeflow shaping and offsets, but cannot validate the Informix SmartLOB wire format.

## Reverse-engineering appendix: IBM 15.0.1.3 / change-stream 1.1.5

The following results come from `javap -c -p -s -constants` against the exact staged bytecode, not inference from Debezium envelopes. Reproducible inputs and SHA-256 digests are:

| Artifact | SHA-256 |
|---|---|
| `debezium-connector-informix-3.6.0.Final.jar` | `f10fca509b55481639c5c9af12fb5e660bb8dfaf2d903d2497faa383323617cc` |
| `jdbc-15.0.1.3.jar` | `6e1e9fea09385e8d99abdc5ba969c4c36e747675945baf525b855882e078848d` |
| `ifx-changestream-client-1.1.5.jar` | `1a0d17c89c16f4227258b74e41cb255c8d163393267aa95c06bc5a91e2dc31e4` |
| `bson-3.8.0.jar` | `d30b5aeba3ae9b7c68c8a6103b41918c5f7318972007b9b92033ee861762d87e` |

### SmartLOB transport

`com.informix.jdbc.IfxSmartBlob.IfxLoRead(int fd, OutputStream target, int requested)` validates `requested >= 1` and `fd >= -1`, otherwise raises driver error `-79773`. It then delegates directly to `IfxProtocol.executeReadSmBlob(fd,target,requested)` and returns its integer byte count. `IfxSqli.executeReadSmBlob` sets the target stream, requested size, stream mode, and zero byte count, locks the SQLI session, calls `sendLoData`, receives messages, unlocks, and returns `amountRW`.

The SQLI request emitted by `IfxSqli.sendLoData` is an `SQ_LODATA` message consisting of driver-encoded fields: smallint message code `97`, smallint operation (`0` read, `1` read-with-seek, `2` write), the file descriptor truncated to a smallint, requested length as int32, and smallint transfer-buffer size `32000`. Read-with-seek additionally sends a long-int offset and smallint whence `1`. The response handler reads a smallint operation and int32 size. Operation `2` with a negative size records the ISAM error and returns `-1`; a non-positive data response produces zero bytes; positive response chunks are copied/written and accumulated in `amountRW`. This is proprietary SQLI framing beneath CDC framing. A Python DB-API cursor cannot express it; a pure-Python implementation must add these SQLI messages to a compatible Informix wire client.

### CDC outer frame and record constants

Java `ByteBuffer` is used without changing its byte order, so every header field below is big-endian. `IfxCDCRecordBuilder.buildRecord` parses:

```text
offset  size  meaning
0       4     header_size, including this fixed 16-byte prefix
4       4     payload_size
8       4     ignored/reserved by client 1.1.5
12      4     record_type
16      ...   type-specific header: header_size - 16 bytes
header  ...   payload: payload_size bytes
```

The exact type constants from `com.informix.stream.cdc.Constants` are BEGIN `1`, COMMIT `2`, ROLLBACK `3`, INSERT `40`, DELETE `41`, BEFORE_UPDATE `42`, AFTER_UPDATE `43`, DISCARD `62`, TRUNCATE `119`, METADATA `200`, TIMEOUT `201`, and ERROR `202`; fixed prefix size is `16`.

Type-specific headers are:

| Type | Header after byte 16 |
|---|---|
| INSERT/DELETE/BEFORE_UPDATE/AFTER_UPDATE | sequence int64, transaction int32, capture label int32 (16 bytes) |
| BEGIN | sequence int64, transaction int32, time int64, user ID int32 (24 bytes) |
| COMMIT | sequence int64, transaction int32, time int64 (20 bytes) |
| ROLLBACK, DISCARD | sequence int64, transaction int32 (12 bytes) |
| TIMEOUT | sequence int64 (8 bytes) |
| TRUNCATE | sequence int64, transaction int32, user ID int32 (16 bytes); Debezium treats user ID as capture label |
| METADATA | capture label int32 (4 bytes) |
| ERROR | flags int32, error int32 (8 bytes) |

METADATA payload is converted with Java's platform-default charset, trimmed, wrapped as `row(<payload>)`, and parsed by JDBC `ComplexTypeParser`; its child `IfxColumnInfo` descriptors are stored by decimal label. Operation payloads cannot be decoded before the corresponding METADATA record has populated that map.

The builder performs no frame-size, negative-size, overflow, or trailing-byte validation before allocating arrays and reading them. A Python port must add bounds such as `header_size >= 16`, non-negative sizes, checked addition, a configured maximum frame, exact availability, and exact payload consumption.

### Operation payload layout and supported codecs

There is no row-level null bitmap. `IfxCDCOperationRecord.getData` walks METADATA columns in order, creates the corresponding JDBC value with `IfxValue.makeInstance`, consumes each value's fixed/prefixed width, and relies on Informix per-type sentinel encodings. The returned map preserves column order.

| JDBC decoder class | CDC bytes consumed by client 1.1.5 |
|---|---|
| `IfxShort` | 2, signed big-endian; null `-32768` |
| `IfxInteger` | 4, signed big-endian; null `-2147483648` |
| `IfxDate` | 4, same integer encoding; null sentinel as integer, otherwise Informix day count converted after adding calendar epoch constant `693594` |
| `IfxBigInt` | 8, signed big-endian; null `-9223372036854775808` |
| `IfxFloat` | 8, big-endian IEEE-754 bit pattern; driver-specific null bit pattern |
| `IfxSmallFloat` | 4, big-endian IEEE-754 bit pattern; driver-specific null bit pattern |
| `IfxInt8` | 10-byte Informix legacy INT8/decimal representation, not an 8-byte integer |
| `IfxChar` | exactly `columnLength` bytes, decoded using the connection/database encoding |
| `IfxVarChar` | first byte is unsigned length `n`; `(n=1, next=0)` is null, `n=0` is empty, otherwise `n` data bytes; client advances `n + 1` |
| `IfxLvarchar` | first two bytes are big-endian signed span `n`; client advances `n + 2`; `fromCDC` receives `n-1` and interprets the additional marker/prefix documented below |
| `IfxDecimal` (also MONEY metadata mapping) | packed Informix decimal; qualifier is `columnLength`; consumed size is `(precision + (scale & 1) + 3) / 2`, where precision is qualifier high byte and scale is its low byte |
| `IfxDateTime` | `IfxColumnInfo.getNumberOfBytes()` packed-decimal bytes interpreted with the column qualifier |
| `IfxBoolean` | 2 bytes; decoder examines byte at offset + 1: `1` true, `0` false, `-1` null |
| `IfxSmBlob` | 76 bytes: four-byte prefix followed by a 72-byte locator; produces a live JDBC SmartLOB object rather than inline contents |

`IfxLvarchar.fromCDC` proves additional semantics. The minimum encoding has span `n=1` in bytes `0..1`; byte `2` is `1` for null and `0` for empty, yielding wire triples `00 01 01` and `00 01 00`. For nonempty values text starts at offset `3`; the builder advances `n + 2`, and `fromCDC` drops a final zero terminator when present. The corrected Python decoder follows this layout. Exact multibyte conversion and locale aliases remain JDBC behavior.

Any `IfxValue.makeInstance` result outside the classes above causes `IfxStreamException("Unsupported column type: ...")`. In particular, this change-stream client has no branch for INTERVAL, TEXT/BYTE simple LOBs, collection/row UDTs, or arbitrary extended types. Debezium's capture-column exclusion is therefore a correctness requirement, not merely a connector preference.

### SQLI connection and normal password authentication

The serverless blocker below the CDC parser is implemented primarily by `com.informix.asf.Connection`, `ConnectionHeaders`, `IfxDataInputStream`, `IfxDataOutputStream`, `com.informix.jdbc.IfxSqliConnect`, and `IfxSqli`. The normal direct path is considerably smaller than the complete JDBC driver.

`Connection.openSocket` creates either a plain TCP socket or an `SSLSocket`, connects to the configured host/port with `socketConnectionTimeout`, applies `socketTimeout`, `TCP_NODELAY=true`, and configured keepalive, then wraps it in buffered input and a 4096-byte buffered output. TLS is selected by connection property `encrypt`; protocol selection comes from `encryptionProtocols`. Certificate verification/trust-store behavior is controlled by `sslCertificateVerification`, `SSL_TRUSTSTORE`/`trustStore`, and `SSL_TRUSTSTORE_PASSWORD`/`trustStorePassword`. TLS is transport wrapping: the SQLI bytes described below are unchanged.

All SQLI smallints, int32s, long-ints, and bigint values are signed big-endian. In ordinary post-association SQLI messages, a SQLI `CHAR` is the two-byte encoded length plus encoded bytes, followed by one zero padding byte when the encoded array length is odd. Driver conversion arrays themselves include the two-byte length; `writeBytes` deliberately strips that prefix when a surrounding structure already wrote the length. **The initial ASC association payload is an exception:** its length-prefixed strings are written as `uint16 length` plus exactly that many bytes, normally including a terminating NUL, with no even-byte padding. Reusing the ordinary padded-CHAR encoder in ASC shifts every following field after an odd-length username, password, server name, host name, working directory, or environment string and causes the server to abort association.

The first client packet has a six-byte session-layer header:

```text
uint16 total_length = payload_length + 6
uint8  sl_type      = 1                 # SLTYPE_CONREQ
uint8  protocol     = 60                # SQLI 6.00
uint16 flags        = 0                 # request; live accept used 0x1000
payload             = ASC binary request
```

This is the complete outer framing for the association request. `total_length` is the unsigned big-endian length of the whole session packet, including these six bytes. The request's final uint16 is zero. There is no separate ASF association ID, sequence number, or checksum in JDBC 15.0.1.3's direct SQLI path. The driver builds the entire ASC payload in memory, prepends this header, writes header then payload, and flushes; TCP is still a byte stream, so receivers must tolerate arbitrary network segmentation and read exactly `total_length - 6` bytes. The server response uses the same six-byte layout, but its final uint16 is a flags field and is not required to be zero. After association succeeds, JDBC switches to the raw grouped SQLI message stream described below; it does not prepend this session header to protocol-offer, secondary-info, DBOPEN, query, fetch, or LODATA groups.

A metadata-only live proxy confirmed that JDBC performs no hidden read, write, urgent/OOB byte, native preamble, lower ASF packet, or preliminary negotiation before ASC. Its first client write was one complete 457-byte association packet with header `total=457, type=1, protocol=60, flags=0`. About 5 ms later the server returned one 258-byte accept packet with `total=258, type=2, protocol=60, flags=0x1000`. No authentication payload was recorded. A Python decoder that labels the final uint16 “reserved” and requires zero will reject this valid server accept even though the association succeeded. Accept known flags or preserve the field opaquely until their bit meanings are recovered; reject only unsupported bits under an explicit mask, not all nonzero values.

The ASC request begins with smallints `100` (`SQ_ASSOC`) and `101` (`SQ_ASCBINARY`), followed by:

```text
int32   61                         # PF protocol SQLI-with-CSS
ascstr  "IEEEM\0"                 # uint16 length + exact bytes; no padding
smallint 108; bytes[12] "sqlexec\0\0\0\0\0"
ascstr  "9.280\0"                 # JDBC extension version
ascstr  "RDS#R000000\0"
ascstr  "sqli\0"
int32   316                        # client/internal handshake version
int32   0
int32   0
smallint 1                         # internet user type
ascstr  username + NUL
ascstr  password + NUL, or length 0 when absent
bytes[8] "ol\0\0\0\0\0\0"
int32   61
bytes[8] "tlitcp\0\0"
int32   1
smallint 104; smallint 11; int32 (3 | optional flags)
ascstr  server_name + NUL
smallint 0                         # JDBC direct connection omits database from ASC
four smallint zeros
SQ_ASCENV(106) environment block
SQ_ASCPINFO(107) process/host block
SQ_ASCMISC_60(116) diagnostic client block
SQ_ASCEOT(127)
```

The optional bit flags are group preference `0x02000000` and trusted context `0x04000000`. For the required normal username/password path both are zero and the value is exactly `3`. The primary environment block includes negotiated properties such as `DB_LOCALE`, `CLIENT_LOCALE`, `DBDATE`, `GL_DATE`, `DBTIME`, `GL_DATETIME`, and `DELIMIDENT`; each entry is an unpadded ASC string (`uint16 byte_length_including_NUL`, encoded bytes, NUL) in DB encoding. `SQ_ASCPINFO` host and working-directory strings use the same unpadded rule.

`IfxSqliConnect.SetPrimEVars` also injects four entries unconditionally: `NODEFDAC=no`, `DBPATH=.`, `IFX_UPDDESC=1`, and `CLNT_PAM_CAPABLE=1`. A live JDBC run with explicit `CLIENT_LOCALE=en_US.819` and `DB_LOCALE=en_US.819` confirmed that the initial `SQ_ASCENV` contains exactly those six entries. Ordering follows Java `Properties.propertyNames()` and is not semantically significant, but count, lengths, NULs, and values are. A compatibility port should reproduce the four defaults plus explicit compatible locales rather than send locales alone.

A fresh two-connection, in-memory structural proxy compared the actual first packet from successful JDBC with the current shared Python encoder loaded through an explicit repository `PYTHONPATH`, replacing credential values and PID/thread/host/path text with typed placeholders and retaining no raw packets. Before connecting, the Python probe asserted `SQ_ASCENV=106` with count `6`; its emitted packet was `428` bytes, while JDBC emitted `457`. Header type `1`, protocol `60`, request flags `0`, `SQ_ASSOC=100`, `SQ_ASCBINARY=101`, fixed int32 `61`, application/network constants, server name, all six environment names/values/lengths, and tail schema matched.

For that live JDBC run the enumeration order was `DBPATH`, `CLIENT_LOCALE`, `CLNT_PAM_CAPABLE`, `IFX_UPDDESC`, `DB_LOCALE`, `NODEFDAC`; Python sorted the same six keys. A second test forced JDBC's observed order and its 75-byte diagnostic string/85-byte misc block. The remaining packets were `457` bytes for JDBC and `462` for Python. The exact structural mismatch was the database slot: JDBC emitted `smallint 0` (two zero bytes), while Python emitted the seven bytes `testdb\0`. The five-byte size delta accounts exactly for the normalized packet-total difference. Therefore the initial ASC must omit the database; database selection occurs only after authentication/protocol/environment setup through `SQ_DBOPEN=36`. Environment order and diagnostic text were not the cause of `-408`.

The corrected state sequence is: connect socket; send ASC with server name but a zero database slot and all six primary environment entries; consume the accept header/body; negotiate protocols; send secondary information; then send `SQ_DBOPEN` with the actual database name. Recompute the outer ASC total from emitted bytes and do not hard-code the observed JDBC total `457`, because PID/host/path diagnostic lengths vary.

Most importantly, JDBC 15.0.1.3 places the ordinary password directly in this ASC request. There is no password-only encryption or server challenge on this path. Therefore a serverless implementation must require TLS for password authentication unless the network is independently trusted; merely porting the handshake over plain TCP exposes credentials.

The server response starts with the same uint16 total length, then a session-layer type byte. Type `2` accepts, type `3` rejects, and type `13` redirects. The next byte is selected protocol and the next uint16 is the flags field; these are the three bytes skipped without validation by JDBC after reading the type. The live accept used flags `0x1000`. Accepted/rejected bodies must then contain `SQ_ASSOC=100`, `SQ_ASCBINARY=101`, followed by fields decoded by `Connection.DecodeAscBinary`. The apparent `assoc_id` is therefore the `SQ_ASSOC` marker at payload offset 6, not an omitted outer-frame field. An “Invalid ASF assoc_id”/immediate close can result from malformed or shifted ASC fields; adding a guessed association ID, sequence header, or checksum would make framing worse. The decoder populates server version/capabilities (`Cap_1`, `Cap_2`, `Cap_3`), warning bits, service/OS error, error text, and redirect details. A rejection with nonzero service error is fatal. The implemented client rejects redirects by default and reconnects only after the explicit allow-list and security checks described above.

After ASC acceptance, `IfxSqliConnect` requires internal version `316`, constructs `IfxSqli`, negotiates the enhanced protocol, sends secondary environment variables with `SQ_ASCENV`, and opens the configured database. `SQ_DBOPEN=36` is followed by a SQLI CHAR database name and a smallint flags value, then `SQ_EOT=12` and flush. The response dispatcher runs through EOT. Server warning bits initialize logging/ANSI/locale state: `HASLOG=1`, `ANSIMODE=2`, float-to-decimal `8`, read-only `32`, and changed locale `64`. When DB locale was not supplied, JDBC queries `informix.systables` for the `GL_COLLATE` site value and derives DB/client encoding from it. A Python port can instead require explicit, mutually compatible locales initially and add discovery later.

PAM and private-server authentication are separate post-handshake exchanges. PAM is advertised through server/client capability and property `IFX_PAM_CLASS`; it uses `SQ_CHALLENGE=129` and `SQ_RESPONSE=130` with callback-defined prompt/response objects. The Python client implements this exchange with a bounded, non-interactive provider and has been live-validated against Informix PAM. Private-server exchange uses `SQ_ACCEPT=127`, `SQ_ACK=128`, challenge/response handling and remains rejected. The JAR contains no GSSAPI, Kerberos, SPNEGO, or JAAS implementation in this path, so those modes also remain unsupported.

### SQL execution and response dispatcher required by the connector

After connection setup, SQLI is a stream of two-byte message codes and message-specific fields. There is no per-message outer length. Command groups write one or more messages, append `SQ_EOT=12`, flush, then read message codes until server EOT. Unknown codes are fatal desynchronization errors.

For SQL without parameters, the smallest JDBC-compatible sequence used by `IfxSqli.sendCommand` is:

```text
SQ_COMMAND(1), smallint 0, CHAR(sql)
SQ_NDESCRIBE(22)
SQ_EXECUTE(7)
SQ_RELEASE(11)
SQ_EOT(12); flush
```

This path is sufficient for fixed catalog/snapshot queries and CDC routine calls if every literal is encoded and escaped safely. Parameterized execution should ultimately use `SQ_PREPARE=2`, parameter-count smallint, SQL CHAR, `SQ_NDESCRIBE=22`, `SQ_WANTDONE=49`, then statement ID/bind/execute messages. Do not emulate parameters with interpolation for usernames, passwords, identifiers, table names, or user-supplied filters.

`IfxSqli.receiveMessage` sends EOT (`flip`), reads a big-endian smallint code, dispatches it, and repeats until `SQ_EOT=12` or `SQ_EXIT=56`. The subset required for catalog queries, snapshots, and `cdc_*` functions is:

| Code | Handler / required state change |
|---|---|
| `8` SQ_DESCRIBE | statement type/id, estimated rows, tuple size, column count and complete column descriptors |
| `14` SQ_TUPLE | warning smallint then one encoded row according to the descriptor |
| `15` SQ_DONE | affected-row/row-id accounting and completion |
| `13` SQ_ERR | SQL code, ISAM code, statement offset, encoded message; code `100` is no-data/end-of-fetch, other nonzero codes fail |
| `55` SQ_COST | optimizer estimate metadata |
| `94` SQ_INSERTDONE | serial/row completion metadata |
| `99` SQ_XACTSTAT | transaction level/state |
| `10` SQ_CLOSE | cursor closed |
| `97` SQ_LODATA | CDC byte response, integrated into the same dispatcher |
| `12` SQ_EOT | end of response group |

`SQ_DESCRIBE` starts with statement type smallint, statement ID smallint, int32 estimated rows, tuple size (int32 with large-tuple capability, otherwise smallint), column count smallint, then statement flags and one descriptor per column. Each descriptor carries position, Informix type, length/qualifier, nullability/flags, and names used to construct `IfxColumnInfo`. `SQ_TUPLE` then delegates to `IfxRowColumn.readTuple`, which applies those offsets and per-type codecs. This is the reusable ordinary-result decoder needed for metadata, snapshots, and scalar `cdc_*` return values; CDC row payload decoding is a separate format documented above.

For forward SELECT cursors JDBC sends query/open/fetch messages after describe; variable-width rows may force an intermediate receive before fetch. A practical Python implementation should preserve the state machine rather than assume all tuples arrive with the initial execute: prepare/describe, record statement ID and descriptors, open, repeatedly fetch, consume `SQ_TUPLE`, stop on SQL code `100`, then close/release. The connection must serialize request groups with one session lock, exactly as JDBC does, because SQL responses and `SQ_LODATA` share one TCP stream.

The LODATA integration is consequently:

```python
def read_cdc(session_id, requested):
    lock_session()
    try:
        send_smallint(97)          # SQ_LODATA
        send_smallint(0)           # LO_READ
        send_smallint(session_id)  # JDBC truncates fd/session to int16
        send_int32(requested)
        send_smallint(32000)
        send_eot_and_flush()
        while True:
            code = read_smallint()
            if code == 97:
                op = read_smallint()
                size = read_int32()
                consume_lodata_chunks(op, size)
            elif code == 13:
                raise decode_sql_error()
            elif code == 12:
                return accumulated_bytes
            else:
                dispatch_common(code)
    finally:
        unlock_session()
```

The production implementation must use exact reads, bounded lengths, cancellation/socket timeouts, deterministic close, and poison/close the connection after any unknown message, truncated field, impossible length, or output-write failure. Continuing after a framing error risks interpreting payload bytes as message codes.

### Remaining SQLI structures recovered from JDBC bytecode

The following completes the minimal, non-PAM handshake and fixed-query result path. Lengths in the JDBC code are signed Java `short`; a Python implementation must reject negative values and impose a smaller configured ceiling before allocation.

#### ASC request tail

The primary `SQ_ASCENV=106` block in the connection request is:

```text
smallint 106
smallint entry_count
repeat entry_count:
    smallint key_byte_length_plus_NUL
    key bytes; uint8 0
    smallint value_byte_length_plus_NUL
    value bytes in DBENC; uint8 0
```

`SQ_ASCPINFO=107` immediately follows:

```text
smallint 107
int32 0
int32 process_id                    # zero if unavailable
int32 current_thread_id             # zero if it cannot fit/parse
ASCSTR local_hostname + NUL         # uint16 length; exact bytes; no padding
smallint 0
ASCSTR current_working_directory + NUL
```

`SQ_ASCMISC_60=116` is diagnostic rather than authentication data, but the server-facing layout is:

```text
smallint 116
smallint 10 + diagnostic_byte_length + 1
int32 0
int32 0
smallint diagnostic_byte_length + 1
ASCII diagnostic bytes; uint8 0
smallint 127                         # SQ_ASCEOT
```

The diagnostic string is `Thread[id:<id>, name:<name>, path:<driver-location>]`; fixed placeholder values are adequate for a Python client if lengths match. No hash, nonce, password encryption, or challenge appears in these tail blocks.

An offline differential used JDBC `ConnectionHeaders.encodeAscBinary` with dummy credentials and normalized the JVM-generated PID, thread ID, hostname, working directory, and diagnostic string into the Python encoder. The resulting 404-byte requests were identical at every byte from the six-byte session header through `SQ_ASCEOT=127`. This verifies the corrected ASC encoder order and lengths: `100, 101, int32 61`; the fixed application fields; unpadded NUL-terminated ASC strings; four zero smallints; `106` environment; `107` process info; `116` misc with outer length exactly `10 + diagnostic_length_including_NUL`; and `127`.

An earlier replay control regenerated `encodeAscBinary` using the reflected `dbName=testdb` and was rejected. The live structural comparison explains that result: the real `Connection.sendConnectionRequest` path supplies no database to the initial ASC even though `IfxSqliConnect` retains `dbName` for the later DBOPEN. Calling the public encoder with the reflected database did not reproduce the actual call-site arguments. There is no evidence for a missing lower frame or connection-sensitive anti-replay behavior once this call-site distinction is accounted for.

#### Accepted ASC response

After the six-byte session-layer response header and required `100,101` smallints, `DecodeAscBinary` consumes this exact sequence:

```text
skip 4
smallint n1; skip n1
smallint marker                     # must be 108
skip 12
smallint version_len; bytes[version_len] -> VersionNumber
smallint n2; skip n2
smallint n3; skip n3
int32 Cap_1
int32 Cap_2
int32 Cap_3
skip 2
smallint n4; if n4 > 0: skip n4
smallint n5; if n5 > 0: skip n5
skip 24
smallint asc_result_type
```

Result type `102` (`SQ_ASCINITRESP`) then contains `skip 6`, `smallint service_error`, `smallint os_error`, `smallint warnings`, `smallint message_count`, followed by `message_count` repetitions of an ignored non-negative smallint and a SQLI CHAR error message. Type `103` (`SQ_ASCDBLIST`) contains one ignored smallint then a CHAR redirect/server detail. Type `127` ends successfully. Any other marker/result, negative count/length, truncated skip, service error, or trailing structure that cannot be consumed exactly must close the socket.

The decoder uses a fixed 4096-byte scratch buffer without checking response lengths against it. Python must instead require the initial uint16 frame length to be `>= 6` and within a configured handshake maximum, read exactly that body into an isolated buffer, and check every nested length against remaining bytes. This prevents the JDBC implementation's potential oversized-length failure from becoming memory corruption/desynchronization in the port.

#### Enhanced protocol and secondary environment

Immediately after ASC, the client sends `SQ_PROTOCOLS=126`, a nonzero smallint protocol-array length, then that byte array padded to even length and EOT. `IfxSqli.sendProtocols` itself returns after `writePadded`, but `executeProtocols` then calls `receiveMessage`, whose first action is `flip()`: `flip()` calls `sendEOT()`, flushes, and only then reads the response. JDBC 15.0.1.3's exact nine-byte offer is `ff fc 7f fc 3c 8c aa 97 06`, so the exact client request is:

```text
smallint 126
smallint 9
bytes ff fc 7f fc 3c 8c aa 97 06
uint8 0                              # only the odd-length padding byte
smallint 12                          # added by receiveMessage -> flip -> sendEOT
                                        # flush here
```

The live metadata trace observed this as a single 16-byte client write: four bytes of code/length, ten bytes of padded offer, and two bytes of EOT. Thus the Python encoder that appends EOT is correct; omitting it would differ from the effective JDBC call path despite the narrower `sendProtocols` bytecode. The server returns code `126`, a smallint length, and a padded byte array as part of its normal response dispatch. JDBC interprets capability bytes beginning at index 5; unsupported bits only enable optimizations/features. The minimal client should send this exact array and retain the server bytes, but use only capabilities it implements (notably large tuple sizes and variable VARCHAR layout). If the server response is shorter than five bytes, longer than the configured protocol maximum, or advertises an incompatible mandatory layout, fail the connection.

Secondary properties are sent with ordinary `SQ_INFO=81`, subtype `6`:

```text
smallint 81
smallint 6
smallint total_block_length
smallint maximum_padded_key_length
smallint maximum_padded_value_length
repeat properties:
    CHAR key_in_database_encoding
    CHAR value_in_database_encoding
smallint 0
smallint 0
EOT
```

JDBC computes `total_block_length = 6 + sum(4 + even(key_length) + even(value_length))`. The connector does not need arbitrary secondary settings: emit only explicitly supported locale/date settings, reject duplicates, cap count/key/value sizes, and sort keys for deterministic fixtures.

`SQ_DBOPEN=36, CHAR(database), smallint flags, EOT` completes database selection. Successful response state is conveyed through normal `SQ_DONE`, `SQ_XACTSTAT`, `SQ_ERR`, and EOT messages; there is no separate DBOPEN frame. Only after a response group completes without an exception should state transition from `AUTHENTICATED` to `DATABASE_OPEN`. SQL error, disconnect, or unknown message transitions permanently to `BROKEN`.

#### Fixed SQL, describe, fetch, tuple, and release

For the connector's own constant SQL, `SQ_COMMAND` avoids parameter-bind complexity. For any value-bearing call, use a prepared statement; do not substitute a quoted value. The prepare header is exactly `SQ_PREPARE=2`, parameter-count smallint, SQLI CHAR SQL, `SQ_NDESCRIBE=22`, `SQ_WANTDONE=49`, EOT. The response `SQ_DESCRIBE=8` begins:

The term “SQLI CHAR SQL” above is capability-dependent: with feature 62 it is `int32 byte_length + SQL bytes + one zero alignment byte when the complete encoded array length is odd`; without feature 62 it uses the ordinary smallint-length CHAR. This choice must be made after protocol negotiation.

```text
smallint statement_type
smallint statement_id
int32 estimated_rows
(int32 if large-tuple capability else smallint) tuple_size
smallint column_count
int32 descriptor_name_blob_length
```

For each output column it then contains, in order:

```text
int32 unknown/descriptor word retained by JDBC
int32 column_start_position
smallint informix_type
int32 extended_type_id_or_qualifier
CHAR extended_owner_name using DB encoding
CHAR extended_type_name
smallint reference
smallint alignment
int32 source_type
int32 encoded_length
```

After all descriptors, `descriptor_name_blob_length` bytes are read with even padding and decoded in DB encoding as NUL-separated column names. Decimal digits/right-decimal are derived from `(informix_type, extended_id, encoded_length)`. Reject column count above the configured schema maximum, negative tuple/name sizes, non-monotonic or out-of-range start positions, unsupported types, encoded lengths larger than tuple bounds, duplicate names after normalization, and a name count different from `column_count`.

The minimal forward cursor uses the server statement ID (`SQ_ID=4, smallint id`), open/query messages produced for the described SELECT, then fetches with:

```text
SQ_ID(4), smallint statement_id
[SQ_RET_TYPE(100) descriptor block only when variable result types require it]
SQ_NFETCH(9)
int32 requested_tuple_buffer_size
smallint 0
EOT
```

JDBC bounds its normal fetch buffer to at most 32767 unless a negotiated capability permits a larger value. A Python first version should cap it at 32767.

Each `SQ_TUPLE=14` response contains `smallint tuple_warning`, `int32 tuple_payload_length`, exactly that many payload bytes, plus one padding byte when length is odd. Multiple tuple messages may precede EOT. The row decoder uses the describe start positions for fixed layouts. For variable layouts it walks columns from the tuple start: types `40,41,43,45,46` consume `5 + big_endian_int32(bytes at current+1)`; VARCHAR types `13,16` consume `1 + unsigned length byte` when variable-VARCHAR capability is active; complex types consume `4 + big_endian_int32 length`; other types consume descriptor length. Every computed end must be checked against tuple payload length and against the next descriptor position. Scalar values are then decoded by the ordinary JDBC type codecs, not the CDC codec table.

End-of-results is `SQ_ERR=13` with SQL code `100`; JDBC treats it as no-data/warning, not connection failure. General error fields are `smallint sqlcode`, `smallint isamcode`, statement offset (`int32` with remove-64K capability, otherwise smallint), and, except for special code `-368`, a DB-encoded SQLI CHAR message. Negative SQL/ISAM codes fail the operation but do not permit parsing to stop before EOT; drain the bounded response group or close the connection.

Close/release are `SQ_ID=4, id, SQ_CLOSE=10, EOT` followed by `SQ_ID=4, id, SQ_RELEASE=11, EOT`. A statement ID must never be reused locally until release response completes. The connector does not require scroll cursors, positioned update/delete, generated keys, batching, UDTs, TEXT/BYTE streaming, XA, savepoints, trusted context, server groups, or callable OUT parameters; reject those rather than implementing their message variants.

The only required prepared input values for CDC control are strings and signed integers. Their bind descriptors and null/value tuple format remain a high-risk area; until differential fixtures prove them, a minimal implementation can restrict SQL text to connector-generated statements whose identifiers pass strict validation and whose scalar values are encoded by a dedicated literal encoder. Passwords never enter SQL. This exception must not be exposed as a general query API.

### Final dispatcher, cursor, capability, and packed-codec details

#### DBOPEN/query response messages

The exact payloads consumed by JDBC 15.0.1.3 are:

```text
SQ_COST(55):
    int32 estimated_rows            -> sqlerrd[0]
    int32 estimated_cost            -> sqlerrd[3]

SQ_XACTSTAT(99):
    smallint event
    smallint new_transaction_level
    smallint old_transaction_level

SQ_DONE(15):
    smallint server_warnings
    (int64 if long-row-id capability else int32) rows_processed
    (int64 if long-row-id capability else int32) row_id
    int32 serial_value
```

`SQ_DONE` marks DB-open state only when the current statement type is `1`, `12`, or `38`; statement type `31` marks the database closed. For a SELECT with no descriptor and zero row ID it synthesizes SQL code `100`. Rows processed populate `sqlerrd[2]`, row ID `sqlerrd[5]`, and serial `sqlerrd[1]`. During DBOPEN, do not transition state on `SQ_COST` or `SQ_XACTSTAT`; require `SQ_DONE`, no accumulated error, and final EOT. `SQ_CLOSE=10` has no payload in the dispatcher and only marks the cursor closed.

#### Live fixed-width SELECT state machine

A metadata-only live trace of JDBC 15.0.1.3 executing the fixed public query `SELECT FIRST 1 member_id FROM members` recovered the exact grouping after DBOPEN. The negotiated server advertised feature 62. JDBC sent:

```text
# group 1: prepare, 52 bytes for this 37-byte SQL
smallint SQ_PREPARE=2
smallint parameter_count=0
int32   sql_byte_length=37          # feature 62; not smallint
bytes   SQL
uint8   0                           # alignment for the odd SQL length
smallint SQ_NDESCRIBE=22
smallint SQ_WANTDONE=49
smallint SQ_EOT=12                  # receiveMessage -> flip
```

The server's first response group began with `SQ_DESCRIBE=8`, supplied statement ID `0` and the fixed-width result descriptor, and ended at EOT. JDBC then sent OPEN and the initial FETCH in a **single** 42-byte request group:

```text
smallint SQ_ID=4; smallint statement_id=0
smallint SQ_CURNAME=3; CHAR generated_cursor_name   # observed length 18
# SQ_BIND appears here for a prepared statement with input values
smallint SQ_OPEN=6
smallint SQ_ID=4; smallint statement_id=0
smallint SQ_NFETCH=9
int32   tuple_buffer_size=4096
smallint fetch_array_size=0
smallint SQ_EOT=12
```

The server response began with `SQ_TUPLE=14` and completed the row/done group. JDBC did not wait for a standalone OPEN response. It later sent `SQ_ID, 0, SQ_CLOSE=10, EOT` and received EOT, then `SQ_ID, 0, SQ_RELEASE=11, EOT` and received EOT. For variable-width output columns, JDBC is different: after OPEN it first receives the descriptor/open group, then sends FETCH, optionally with `SQ_RET_TYPE=100`; this branch is selected by `desc.hasVariableLengthColumns` (or optimized fetch capability), not unconditionally.

The earlier Python path had two independent mismatches that explained a timeout before the first query response: `encode_prepare` emitted a two-byte SQL length even when feature 62 was active, shifting the SQL and all following codes; and `execute` flushed OPEN and waited for a response before sending the first FETCH even for fixed-width descriptors. The implementation now uses capability-aware SQL encoding and descriptor-driven OPEN/FETCH grouping. Prepared binds belong between CURNAME and OPEN, exactly as shown.

`SQ_COMMAND=1` is a real JDBC path, encoded as `SQ_COMMAND, smallint 0, capability-aware SQL CHAR, SQ_NDESCRIBE, SQ_EXECUTE=7, SQ_RELEASE=11, EOT`. JDBC's `Statement.executeQuery` does not use it for a SELECT; it uses PREPARE and a cursor. Therefore `SQ_COMMAND` should be restricted to non-row command execution until a live result-producing command path proves otherwise. A SELECT probe built with the current Python `encode_simple_command` also inherits the wrong feature-62 SQL length and cannot establish that SQ_COMMAND supports cursor results.

`SQ_INSERTDONE=94`, unnecessary for the connector's read-only/special-function result path, is an 8-byte Informix long-int serial followed by an 8-byte BIGSERIAL only when bigint capability is enabled. Unknown or out-of-order completion messages fail closed.

#### Exact forward cursor open/fetch

For a normal forward-only, non-hold, non-scroll cursor, `IfxSqli.sendQuery` emits:

```text
SQ_ID(4), smallint statement_id
SQ_CURNAME(3), CHAR encoded_cursor_name
[SQ_BIND(5) only when prepared input parameters exist]
[SQ_AUTOFREE(108) only when enabled]
SQ_OPEN(6)
```

For fixed-width results JDBC immediately appends the first fetch to that same group:

```text
SQ_ID(4), smallint statement_id
[SQ_RET_TYPE(100), ... only for negotiated variable result types]
SQ_NFETCH(9)
int32 tuple_buffer_size
smallint fetch_array_size = 0
EOT
```

For variable-width results JDBC receives the open/descriptor response before sending the fetch group above, potentially including `SQ_RET_TYPE`. The optimized-open/fetch capability can also combine additional return-type negotiation, but ordinary fixed-width forward cursors already combine OPEN and first FETCH. Python must branch on the descriptor rather than always use a two-phase open then fetch state machine. Hold (`43`), scroll (`24`), reopen-optimized (`87`), autofree (`108`), and output-type override are unnecessary and should be rejected.

#### Capability bits that change parsing

The nine-byte server protocol response maps through JDBC handlers as follows:

- response byte index 6 bit `0x02` sets feature 54; index 6 bit `0x08` sets feature 52;
- index 7 bits `0x80`, `0x10`, `0x04`, `0x02`, `0x01` set features 56, 59, 61, 62, 63 respectively;
- index 8 bits `0x04`, `0x02` set features 69 and 70.

Feature 62 is `remove-64K-limit`: statement offsets in `SQ_ERR` are int32 rather than smallint, and SQL text produced by `getJavaToIfxCharBytes` uses a **four-byte big-endian length prefix** (`JavaToIfx4BytesChar`) instead of ordinary two-byte CHAR. Larger SQL text is consequently permitted. Feature 69 is `large-tuple-size`: `SQ_DESCRIBE.tuple_size` is int32 rather than smallint. Feature 70 is `long-row-id`: both row-count/row-id fields in `SQ_DONE` are int64. Variable VARCHAR layout is **not** selected by a server feature bit in this driver; it is enabled unless client property `padVarchar` is true. A compatible Python client should use variable VARCHAR and reject `padVarchar=true` initially.

The remaining mapped features enable named parameters (52), savepoints (56), private-server authentication (59), SQL batching (61), and other optional behavior. Only features 62, 69, and 70 alter the minimal response field widths above.

#### Numeric nulls and packed values

The previously unspecified floating null patterns are exact: FLOAT null is eight `ff` bytes and SMALLFLOAT null is four `ff` bytes. These are checked before interpreting IEEE-754 bits; do not treat arbitrary NaNs as null.

Packed DECIMAL/MONEY and DATETIME both use the JDBC `Decimal` machinery. The first two zero bytes mean null. For non-null DECIMAL, byte 0 is sign/exponent: bit `0x80` means positive; a negative value XORs the exponent with `0x7f` and applies Informix's base-100 ten's-complement from the least-significant digit (subtract the first encountered nonzero digit from `100`, then preceding digits from `99`; trailing zero groups remain zero). The unbiased base-100 exponent is `(header & 0x7f) - 64`. Remaining bytes are base-100 digit groups. Precision is the qualifier high byte and scale the low byte; the decoder builds a decimal string from sign, exponent, digit groups, and requested scale. This is sufficient for an independent decoder provided it enforces digits `0..99`, exact encoded length, and precision/scale bounds.

The CDC `IfxInt8` branch consumes the complete ten-byte Informix long-int representation. CFR corrects the earlier javap interpretation: bytes `0..1` are a signed big-endian sign word, bytes `2..5` are the unsigned low 32-bit word, and bytes `6..9` are the unsigned high 32-bit word. A zero sign word is SQL null. Otherwise `magnitude = (uint32(bytes[6:10]) << 32) | uint32(bytes[2:6])`; sign `-1` negates it and sign `1` leaves it positive. `JavaToIfxLongInt` emits exactly this layout. There are no trailer bytes.

DATETIME copies exactly `getNumberOfBytes()` raw bytes, treats leading `00 00` as null, and otherwise passes the bytes plus qualifier to `Decimal.timestampValue`. Qualifier codes are start/end nibbles: YEAR `0`, MONTH `2`, DAY `4`, HOUR `6`, MINUTE `8`, SECOND `10`, fraction levels `11..15`. Calendar/timezone conversion occurs after packed-decimal parsing. Exact field placement for every qualifier combination and the meaning of omitted fields remain fixture-sensitive; initially support the actual catalog qualifiers observed and fail on all others.

CFR recovers JDBC's omitted-date defaults: after unpacking, if any of year/month/day is zero, year zero becomes `1700`, month zero becomes the **current JVM local month**, and day zero becomes `1`; the selected/default local `Calendar` then converts it to epoch milliseconds. This behavior is nondeterministic for time-only/partially qualified values and should not be copied into Lakeflow. `Decimal.timestampStringValue()` renders only fields selected by the qualifier without injecting those calendar defaults, so a Python implementation should decode to a qualifier-aware string/value and apply a separately documented Spark mapping.

### Exhaustive exclusion analysis and Lakeflow policy

Debezium 3.6.0.Final's `InformixStreamingChangeEventSource` explicitly removes native types BYTE `11`, TEXT `12`, UDT variable `40`, UDT fixed `41`, UNKNOWN `49`, and anything for which `IfxTypes.isComplexType` is true. IBM defines complex as SET `19`, MULTISET `20`, LIST `21`, and ROW `22`. This predicate does **not** exclude INTERVAL `14`, INT8 `17`, SERIAL8 `18`, LVARCHAR `43`, BOOLEAN `45`, BIGINT `52`, or BIGSERIAL `53`; however change-stream client 1.1.5's `IfxCDCOperationRecord` has no INTERVAL branch, so an included INTERVAL still fails at decode. These are the native IDs exposed through the catalog/type descriptors used by this connector; they must not be confused with JDBC `java.sql.Types` values. BLOB/CLOB extended types use native extended mappings and are not safe merely because their public IDs are `101/102`; their capture descriptor resolves through unsupported extended/locator behavior.

The working local fixture contains five `DATETIME YEAR TO FRACTION(5)` columns and no INT8, SERIAL8, INTERVAL, LOB, UDT, row, or collection test column. It therefore provides no local fixture evidence for those exclusions.

| Source type | Native CDC policy | Snapshot policy | Lakeflow representation |
|---|---|---|---|
| INT8/SERIAL8 | Experimental only after live ten-byte fixture; otherwise fail table CDC setup | Supported through ordinary SQL numeric conversion | signed long/string if overflow policy requires |
| DATETIME | Support only qualifiers covered by tests; current local priority is YEAR TO FRACTION(5) | Supported | timestamp for year-based values; string for lossy/time-only variants |
| INTERVAL YM/DF | Exclude from native CDC because IBM operation builder lacks a branch | Query/snapshot only | canonical Informix string; do not coerce to an ambiguous duration |
| TEXT/BYTE | Debezium deliberately excludes | Snapshot or post-commit keyed reselect | string/binary, with explicit size limit |
| BLOB/CLOB | Exclude locator payloads | Snapshot or keyed reselect while row exists | binary/string with size limit |
| SET/MULTISET/LIST/ROW | Debezium deliberately excludes | Optional SQL serialization/reselect only | stable JSON/string only with an explicit schema contract |
| fixed/variable UDT | Debezium deliberately excludes | Optional owner/type-specific SQL cast | reject by default; serialized string/binary only by opt-in |
| UNKNOWN | Exclude | Reject | none |

Snapshot fallback alone does not maintain excluded values after updates. The only correct incremental choices are: emit the documented unavailable placeholder; issue a post-commit SELECT by the complete primary key; or reject CDC for that table. A keyed reselect must run after COMMIT, use the same source identity, tolerate a row deleted immediately after commit, and never synthesize a value into DELETE before-images. It weakens point-in-time consistency because the query may observe a later transaction. For that reason Lakeflow should default to placeholders/rejection and expose reselect as an explicit best-effort mode.

#### INT8/SERIAL8

`IfxInt8.fromIfx` invokes `IfxToJavaLongInt` over all ten bytes. Decode `sign=int16(bytes[0:2])`; sign `0` is null, sign `1` or `-1` selects the sign of `(uint32(bytes[6:10]) << 32) | uint32(bytes[2:6])`; reject every other sign. Snapshot and CDC decoding both use this signed-magnitude representation. The `Long.MIN_VALUE` returned internally for null is a Java sentinel, not an on-wire eight-byte sentinel. The magnitude of `-2^63` also needs a checked special case because Java's `l = -l` overflows in the encoder. SERIAL8 has the same value domain but generated-key semantics do not alter a CDC row value.

#### DATETIME and INTERVAL

Both use an Informix qualifier whose high/start and low/end codes cover YEAR `0`, MONTH `2`, DAY `4`, HOUR `6`, MINUTE `8`, SECOND `10`, and FRACTION(1..5) `11..15`. Packed bytes are passed to the driver's decimal temporal parser; leading `00 00` is null. All start/end combinations allowed by Informix must be treated as distinct schemas. Fraction digits must be scaled to nanoseconds without rounding beyond the declared qualifier. YEAR/MONTH/DAY-bearing DATETIME has no timezone; preserve it as a naive source value and apply the connector's documented UTC interpretation only at the Spark boundary. Time-only DATETIME must not invent a source date.

`IfxIntervalYM` and `IfxIntervalDF` delegate to `IfxToJavaInterval(bytes,offset,length,qualifier)` and preserve the qualifier. The operation builder omission, not lack of a JDBC decoder, is the blocker. A future CDC branch can reuse the packed-decimal algorithm, but until live parity tests exist, serialize catalog/snapshot intervals with Informix's canonical `toString` form.

The JDBC conversion constructs `Decimal(raw, offset, length, qualifier, interval=true)`, returns SQL null when its decoded `dec_pos` is `-1`, and otherwise calls `Decimal.intervalValue()`. This proves that INTERVAL shares the packed-decimal container and qualifier dispatch, but does not by itself prove the signed field normalization or every YEAR-MONTH versus DAY-FRACTION rendering rule. Those semantics still require differential fixtures; treating the bytes as a plain integer duration would be incorrect.

#### TEXT/BYTE/BLOB/CLOB locators

Ordinary SQL TEXT/BYTE uses `IfxBlob` descriptors of 56 bytes, or 68 bytes when long-row-id capability is active. The driver reads length triplets from big-endian int32s at offsets 8, 12, and 16 (long-row-id changes one internal descriptor offset), and flags at offset 38; flag bit `1` denotes null. The descriptor identifies server-side blob data and is not the content itself. Smart BLOB/CLOB uses the 76-byte CDC branch previously documented: a four-byte prefix plus 72-byte `IfxLocator`, then additional LODATA/SmartLOB calls.

Ordinary TEXT/BYTE materialization is statement-bound. The driver sends the current statement ID, `SQ_FETCHBLOB=38`, and the padded `IfxBlob.toIfx()` descriptor. Both `SQ_TEXT` and `SQ_BLOB` responses then carry a signed-smallint chunk length followed by padded bytes; the driver loops until exactly that many bytes have been copied to the destination. A Python implementation must reject negative lengths, enforce a cumulative limit, tolerate partial socket reads, and consume padding without including it in the value. This is not the SmartLOB CDC read protocol.

SmartLOB lifecycle is instead a mixture of fast-path calls and LODATA. `IfxLoOpen(locator, flags)` wraps the 72-byte locator as extended type `blob`, invokes `function informix.ifx_lo_open(blob,integer)`, and retains the returned integer file descriptor. `IfxLoRead(fd, ..., requested)` uses `executeReadSmBlob`/LODATA. `IfxLoClose(fd)` invokes `function informix.ifx_lo_close(integer)` and resets the local descriptor to `-1`; `IfxLoRelease(locator)` separately invokes `function informix.ifx_lo_release(blob)`. Therefore the CDC session identifier passed by Debezium to `IfxLoRead` is being used as the SmartLOB descriptor for that server routine; it is not an ordinary TEXT/BYTE descriptor and must not be reused across a reconnect.

Never checkpoint a locator as a value: locator lifetime is connection/session-dependent. Materialize content before returning a batch, enforce per-value and per-batch byte limits, and reject locator reads after CDC session close. For deletes there may be no readable post-commit row/locator, so excluded LOB columns cannot be reconstructed reliably; only keys and supported before-image columns are safe.

#### Complex/UDT metadata and payloads

IBM ordinary SQL represents variable complex values with a four-byte length prefix and recursively parsed `IfxColumnInfo` children. Collections distinguish SET/LIST/MULTISET semantics; ROW preserves named ordered fields; UDTs may require a registered Java `SQLData` class/type map. None of that registry behavior has a portable Python equivalent, and arbitrary binary serialization is not stable across server/type versions.

This also makes “convert all dependencies” an invalid correctness criterion: the Java type-map callback is application code, not information present in the wire value. A Python decoder can only recurse over built-in ROW/collection descriptors whose complete child metadata is available. An opaque fixed/variable UDT cannot be reconstructed generically when its owner-defined serializer, version, or Java `SQLData` mapping is absent. It must be cast by the source query under a user-declared contract or rejected.

Do not flatten these values implicitly. Safe options are a user-supplied SQL cast to LVARCHAR/JSON/BSON, a declared recursive JSON schema for built-in ROW/collections, or table rejection. Preserve LIST order, SET uniqueness, MULTISET multiplicity, named-row field names/order, null elements, and UDT owner/type/version metadata if serialization is ever enabled.

#### Authentication, redirect, TLS, and VARCHAR exclusions

- Normal username/password and bounded non-interactive PAM (`SQ_CHALLENGE/SQ_RESPONSE`) are implemented and require verified TLS. Private-server exchange and trusted context remain rejected.
- No GSSAPI/Kerberos/SPNEGO/JAAS implementation or message path exists in the inspected JDBC JAR. Treat any such configuration as unsupported rather than falling back to password.
- Redirect type `13` contains a server detail string. Redirects are rejected by default. The opt-in implementation parses an exact host/numeric-port target, preserves the TLS policy, validates explicit hostname and private-address allow-list entries, caps redirects, detects loops under one login deadline, discards all old socket/parser/auth state, and restarts the complete ASC handshake without carrying statement/session IDs. This full path is live-validated with the deterministic responder described above.
- The JDBC redirect parser tokenizes the exception/detail text on any of `:`, `=`, or `|`, discards token 1, then assigns tokens 2/3/4 to server name, IP address, and port-or-service. A decimal token 4 becomes the port; otherwise `JnsObject.getServiceByName` resolves it and failure raises `-79759`. Error `-79998` triggers this parse and the outer connection loop calls `connect(...)` again from the beginning. This grammar is brittle and the input is server-controlled, reinforcing reject-by-default; an enabled Python version must require exactly the expected token count and numeric port rather than reproduce local service-name lookup.
- Custom CA support should accept PEM trust anchors through Python's verified `SSLContext`, enforce hostname verification and minimum TLS version, and never reproduce JDBC's optional trust-all manager. Client certificates/mTLS, protocol/cipher overrides, and certificate pinning require explicit configuration and tests.
- `padVarchar=true` selects the fixed/padded ordinary SQL layout; it does not alter the documented native CDC VARCHAR prefix. Reject it initially to keep one result decoder. If added, derive value length from the descriptor, preserve CHAR semantics separately, and test multibyte encodings where character count differs from byte count.
- Arbitrary prepared binds remain outside the connector contract. Internal generated SQL may use narrowly typed integer/string literal encoders with strict bounds and identifier validation; never expose that mechanism as a user query API.

#### Prepared `SQ_BIND` wire envelope

The JDBC bytecode resolves the generic bind envelope, though individual `IfxObject.toIfx()` scalar codecs still need fixtures. It writes `SQ_BIND=5`, signed-smallint value count, then for each value: signed-smallint native type; for extended types above `18` except BIGINT `52` and BIGSERIAL `53`, an owner CHAR (zero smallint when absent) and an extended-type-name CHAR; signed-smallint null marker; signed-smallint encoded-length/precision; and, only for a non-null value, its padded `toIfx()` bytes. Explicit SQL null uses marker `-1`, length `0`. A missing Java argument with a server default uses the metadata type, marker `-2`, length `0`; without such a default it is an error. A non-null value uses marker `0`; DECIMAL type `5` normally places scale in the length/precision slot, while other values use `getEncodedLength()`.

This is enough to reject malformed bind frames and to implement deliberately bounded native INTEGER/CHAR encoders later, but not to claim a general prepared-statement API. TEXT/BYTE binds additionally trigger a separate blob-send phase, opaque extended values need owner/type metadata, and all lengths are signed Java shorts. Connector-generated SQL currently avoids this surface.

#### PAM and private-server exchange details

PAM challenge `SQ_CHALLENGE=129` contains signed-smallint **message style first**, then signed-smallint message length, then padded character bytes; the driver converts it into a callback object and expects EOT. The earlier javap-based field order was reversed; the implemented Python decoder now uses the corrected order. Its `SQ_RESPONSE=130` contains a signed-smallint response length (bounded by the Python implementation to 512 encoded bytes), padded encoded response bytes when non-empty, then EOT. Challenge styles `3` and `4` require no response; styles `1` and `2` invoke the configured non-interactive provider. Multi-round PAM is bounded by a round limit and overall login deadline and has been live-validated against the fixture's Informix PAM listener.

The private-server path is not a password variant. Its challenge names a server-side exchange file; the JDBC implementation opens that local path, reads a destination path plus 256 bytes, writes those bytes to the destination file, receives EOT, and continues an `SQ_ACCEPT=127`/`SQ_ACK=128` exchange. Besides trusting server-selected filesystem paths, this depends on mutable worker-local files and an external private-server mechanism. It is deliberately impossible in the serverless connector sandbox and must be rejected before any path is opened.

### CFR 0.152 source reconstruction audit

The JDBC 15.0.1.3, change-stream 1.1.5, and Debezium Informix 3.6.0 reconstructed Java were cross-checked against this document and the pure-Python `sqli.py`/`cdc_protocol.py`. CFR is substantially easier to audit than isolated javap instructions, but it is still decompiled output; live framing evidence wins where they differ. The audit found these corrections:

- Initial ASC database: `ConnectionHeaders.encodeAscBinary` supports either a database value or `smallint 0`, but the real direct connection call path supplies null and later opens the retained database with `SQ_DBOPEN`. Passing `testdb` to the public encoder was the cause of the five-byte live structural mismatch and earlier failed replay control.
- Session response header: JDBC reads total length and session type, then skips protocol plus the two-byte options field. It does not require options zero; the live accept used `0x1000`. Python's zero-only `reserved` validation is incompatible.
- Protocol EOT: `sendProtocols` stops after the padded array, but `receiveMessage()` immediately invokes `flip()`, which sends EOT and flushes. The effective request includes EOT, as the live 16-byte write proves.
- INT8/SERIAL8: all ten bytes are meaningful signed-magnitude data in the word order documented above. The prior “int64 plus trailer” interpretation and current Python implementation are wrong.
- VARCHAR/LVARCHAR: VARCHAR `(length=1, byte=0)` is null. LVARCHAR null/empty are `00 01 01`/`00 01 00`, and nonempty data begins at byte 3. The Python CDC decoder implements these sentinels and advances by the complete native width.
- PAM challenge: the two fields after `SQ_CHALLENGE` are style then length, not length then style. The corrected Python decoder and non-interactive multi-round state machine are wire-compatible with the live Informix PAM fixture.
- Query execution: negotiated feature 62 changes command/prepare SQL to a four-byte length, and fixed-width cursors combine OPEN plus initial FETCH before the first response. The corrected Python path implements both requirements. `SQ_COMMAND` exists but is not JDBC's SELECT path.
- Ordinary BYTE/TEXT null: `IfxBlob.fromIfx` treats `(tb_flags & 1) == 0` as a present descriptor; null tuple generation sets byte 39 bit `1`. Thus flag bit `1` means null, despite CFR's misleading local variable name `isBlobNull` around the inverse branch.
- SmartLOB byte-array overload: `IfxLoRead(fd, n)` allocates and returns the requested-size array without trimming it to the protocol's returned count. The CDC path uses the `OutputStream` overload, which returns the actual byte count; a Python port must follow that counted behavior and never append an unfilled array tail.
- INTERVAL: `Decimal.intervalValue()` converts YEAR/MONTH fields to total months and DAY/HOUR/MINUTE/SECOND to total seconds plus nanoseconds, then applies the decimal sign and constructs `IntervalYM` or `IntervalDF` according to start qualifier. This confirms the packed-decimal/qualifier model, but native CDC still cannot use it because `IfxCDCOperationRecord` has no interval branch.
- DATETIME: `Decimal.timestampValue()` injects year `1700`, current local month, and day `1` when date fields are omitted/zero, whereas `timestampStringValue()` preserves qualifier-selected fields. This was previously fixture-sensitive; it is now source-confirmed JDBC behavior and an explicit nondeterminism to avoid.
- Complex values: ordinary complex tuples are not simply a four-byte length plus recursive children. `IfxComplex` has a 136-byte base header, `414` bytes of serialized metadata per type entry, a type count/tree level model, serialized-data length, and flags where `0x80000000` is null and `0x20` is a large collection. It then delegates payload alignment and recursion to `IfxComplexInput` plus the JDBC type map. This reinforces rejection/cast-only policy; implementing only a recursive length prefix would desynchronize rows.

The audit also confirmed these branches and boundaries:

- `IfxCDCRecordBuilder` reads `header_size - 16`, payload size, one ignored int32, and record type, then dispatches exactly metadata `200`, insert `40`, delete `41`, before-update `42`, after-update `43`, begin `1`, commit `2`, rollback `3`, discard `62`, truncate `119`, timeout `201`, and error `202`; every other type throws. Not every record has sequence/transaction: metadata has only label, timeout only sequence, and error only flags/error. TRUNCATE's fourth header field is `userID`; Debezium repurposes it for table lookup, while current Python names it `label`. Preserve both the wire name and connector interpretation explicitly.
- Metadata payload is parsed as `row(` + platform-default `new String(payload).trim()` + `)`. A Python port should use the negotiated database encoding explicitly rather than reproduce the JVM-default ambiguity. Operation headers are sequence int64, transaction int32, label int32; missing label metadata causes decode failure rather than an implicit schema guess.
- The operation decoder branches only for the scalar/SmartLOB classes listed earlier. Its DECIMAL size expression simplifies to `(precision + (scale & 1) + 3) / 2`; LVARCHAR passes `wire_length - 1` into `fromCDC` and advances `wire_length + 2`; SmartLOB copies bytes `offset+4..offset+75` into a 72-byte locator.
- Debezium's so-called `reselectColumns` path does not query the source row during streaming. It inserts `UnavailableValuePlaceholderType` into every available before/after map for excluded columns. DELETE has no after map and cannot recover a deleted LOB/UDT. Any actual keyed reselect described above is a new, weaker-consistency connector policy, not behavior inherited from Debezium.
- PAM, private-server file exchange, redirects, ordinary TEXT/BYTE fetch, SmartLOB open/read/close/release, generic bind null/default markers, and the explicit Debezium exclusion predicate match the earlier reconstruction. No GSS/Kerberos/SPNEGO/JAAS branch appears in these reconstructed paths.

Accordingly, the port deliberately rejects complex, INTERVAL, and LOB layouts that are not implemented end to end. The corrected ten-byte INT8/SERIAL8 representation has source-local golden coverage but still needs recorded live snapshot evidence. Supported packed DECIMAL/DATETIME and CDC lifecycle paths have source-local and live validation; unsupported layouts must continue to fail closed.

### Serverless implementation status

The connector implements the documented SQLI transport, transaction/frame parser, supported scalar codecs, metadata handling, and Lakeflow contract in pure Python. Its dependency shape is compatible with serverless execution: no embedded JVM, custom JAR classpath, native Informix client, or worker-local Java bootstrap is required.

Current verification includes the CFR/bytecode audit, the source-local protocol/connector regression suite, and a disposable Informix 15 fixture. The pure-Python client completed normal-password association, enhanced-protocol setup, DBOPEN, feature-62 PREPARE, fixed and variable cursor fetches (including `SQ_RET_TYPE`), table/column/primary-key discovery, and a two-row snapshot containing INTEGER, SMALLINT, VARCHAR, DECIMAL, and DATETIME values. The live DATETIME result confirmed JDBC's decimal-container rule: the descriptor's high byte is total precision, its low byte contains the qualifier, and wire width is `IfxDecimal.decLength(encodedLength)`. The test-only plaintext context remained outside production code; production still requires TLS for password authentication. No credentials were persisted.

The same fixture completed the full CDC lifecycle using cross-database routine names (`syscdcv1:informix.cdc_*`): open session, full-row logging, start capture, activate, repeated `SQ_LODATA`, end capture, and close session. A controlled synthetic member row produced and decoded INSERT, BEFORE_UPDATE, AFTER_UPDATE, DELETE, BEGIN, and COMMIT records with the expected before/after values and LSNs. Live evidence also established that read LODATA responses use operation `LO_READ` plus padded chunks, that record headers may contain extensions beyond the prefix consumed by IBM's classes, and that METADATA order—not catalog/request order—controls operation payload decoding.

### Evidence still missing or requiring fixtures

Bytecode resolves the structural questions above, but the following cannot be certified without controlled records from Informix 15:

- the semantic purpose and allowed values of the reserved outer-header int32 at offset 8;
- server-side limits for header/payload size and whether future CDC versions append header fields;
- prepared-parameter `SQ_BIND` type descriptors and null/value tuple layouts still require transcription and fixtures before exposing a general parameterized SQL API; the bounded connector-generated fixed-SQL path, ASC decode, protocol offer, describe, fetch, tuple, error, release, and LODATA paths are documented above;
- differential fixtures for DECIMAL/MONEY rounding and malformed digits, INT8 signed-magnitude boundary behavior (especially `-2^63`), and exact DATETIME field placement/omitted-field defaults for every qualifier combination;
- exact METADATA type-description grammar emitted for every supported Informix declaration and schema evolution event;
- effects of non-default DB_LOCALE/CLIENT_LOCALE, multibyte VARCHAR length semantics, fixed CHAR padding, and invalid byte sequences;
- whether CDC can actually emit the `IfxSmBlob` locator branch for the connector's registered column set and whether that locator remains readable after the CDC session closes;
- raw frames paired with IBM-decoder results for boundary/null values, partial reads, every record type, rollback-to-savepoint, DDL, and corrupt/oversized input.

These gaps do not prevent implementing the bounded frame/transaction parser, but they do prevent claiming a complete type-compatible Python replacement until differential tests against IBM's decoder pass.
