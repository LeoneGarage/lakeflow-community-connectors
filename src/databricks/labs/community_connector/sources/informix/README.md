# Lakeflow Informix Community Connector

This connector discovers Informix user tables, takes an initial snapshot, and then captures committed changes through the Informix CDC API. The SQLI client, SmartLOB framing, CDC decoding, transaction handling, and checkpoint recovery are implemented in Python. It requires no JVM, JDBC driver, Debezium runtime, JAR staging, or native Informix client and is intended to run on serverless Lakeflow Connect compute.

The pure-Python protocol implementation has completed authentication, query, discovery, snapshot, and end-to-end transactional INSERT/UPDATE/DELETE CDC validation against a disposable Informix 15 fixture. It has also completed a serverless Lakeflow Connect pipeline run over TLS, including multi-table discovery, snapshots, checkpointed CDC flow execution, deletes, and SCD Type 2 materialization. Validate it against your Informix version, topology, security policy, and workload before broader use.

## Prerequisites

- An Informix SQLI endpoint reachable from serverless Lakeflow Connect compute. Configure firewalls, private connectivity, and DNS as appropriate. Protocol redirects are disabled by default and require the explicit controls described below.
- TLS enabled on the SQLI endpoint with a certificate trusted by Python's system CA store. The certificate must match `hostname`.
- Transaction logging enabled for the source database. Retained logical logs must extend back to the oldest connector checkpoint.
- The Informix CDC API installed by an administrator. Run `$INFORMIXDIR/etc/syscdcv1.sql` (commonly with `dbaccess sysadmin`) to create the `syscdcv1` database and `cdc_*` routines.
- A normal username/password account that can read source catalog metadata and selected tables, use the required `syscdcv1` routines, and enable full-row logging. The validated reference setup grants DBA-level access to `syscdcv1`.
- The exact `INFORMIXSERVER` value. `DB_LOCALE` defaults to `en_US.819` and `CLIENT_LOCALE` defaults to `en_US.utf8`; override either when the source requires another locale because the connector does not discover them.

The connector enables full-row logging for captured tables and leaves it enabled when a finite poll ends, avoiding capture gaps between polls. Ensure this operational change is acceptable on the source system.

Initial CDC preparation is automatic. For each table, its upsert reader enables
full-row logging, captures one LSN, and atomically publishes it under
`cdc.shared.state.location`. The independently scheduled delete reader waits
for and uses that exact LSN. The location must be a writable Unity Catalog
Volume directory shared by all serverless workers and dedicated to this
connector deployment.

## Setup

### Connection parameters

| Option | Required | Default | Description |
|---|---:|---:|---|
| `hostname` | Yes | | Informix SQLI hostname or IP address. It is also used for TLS hostname verification. |
| `database` | Yes | | Database to discover, snapshot, and capture. |
| `user` | Yes | | Informix normal-auth user with metadata, snapshot, and CDC privileges. |
| `password` | Yes | | Password for `user`; store it as a secret. |
| `server` | Yes | | Exact `INFORMIXSERVER` name sent during SQLI authentication. |
| `cdc.shared.state.location` | Yes | | Writable shared directory on a Unity Catalog Volume, for example `/Volumes/main/informix_cdc/state`. Stores atomic per-table initialization records used by independently checkpointed upsert and delete readers. |
| `DB_LOCALE` | No | `en_US.819` | Database locale. Set it explicitly when the database uses another locale. |
| `CLIENT_LOCALE` | No | `en_US.utf8` | Client locale. Its codeset controls Python row decoding. |
| `port` | No | `9088` | Informix SQLI port; range `1`–`65535`. |
| `encrypt` | No | `true` | Enables TLS. Truthy values are `1`, `true`, and `yes`, case-insensitively. Disabling TLS fails closed. |
| `ssl.ca.file` | No | system CA store | Path to a PEM CA bundle available on the pipeline worker. Hostname verification remains enabled. |
| `authentication.mode` | No | `password` | `password` or non-interactive `pam`. Other modes fail closed. |
| `authentication.provider.factory` | No | built-in provider | Trusted Python factory in `module:callable` form. It receives all connection options and returns a non-interactive PAM response provider. |
| `authentication.pam.echo.response` | No | `password` | Secret response used by the built-in provider for PAM echo-on prompts. Echo-off prompts use `password`. |
| `authentication.pam.max.rounds` | No | `16` | Maximum PAM challenge rounds before login fails. Each encoded response is limited to 512 bytes. |
| `authentication.login.timeout` | No | `30` | Overall login deadline in seconds, shared by connection, authentication, and all redirect attempts. |
| `redirect.enabled` | No | `false` | Opts into protocol redirects. A redirect still fails unless its destination is explicitly allowed. |
| `redirect.allowlist` | No | empty | Comma-separated exact `host:numeric-port` redirect destinations. See the security rules below. |
| `redirect.max` | No | `3` | Maximum redirects within one login; revisiting a destination is rejected as a loop. |
| `padVarchar` | No | `false` | Enables fixed-width padded decoding for ordinary SQL `VARCHAR`/`NVARCHAR` snapshot and metadata results. Use only when required by the negotiated server tuple format. |
| `table.include.list` | No | all eligible tables | Comma-separated shell-style table patterns. |
| `tables` | No | | Alias for `table.include.list`; ignored when that option is set. |
| `table.exclude.list` | No | none | Comma-separated shell-style patterns excluded after inclusion filtering. |
| `snapshot.page.size` | No | `10000` | Rows per deterministic snapshot page for CDC-capable tables; minimum `1`. |
| `snapshot.max.rows` | No | `100000` | Maximum rows retained by a transactional CDC snapshot or one-shot snapshot-only read. The connector fails instead of returning a partial table. |
| `snapshot.max.bytes` | No | `268435456` | Maximum estimated decoded Python bytes retained while fetching one snapshot query. Set `0` to disable the limit and byte accounting. |
| `metadata.max.bytes` | No | `67108864` | Maximum estimated decoded Python bytes retained by an individual catalog query and by complete discovery. Set `0` to disable the limit and byte accounting. |
| `max.records.per.batch` | No | `10000` | Target maximum projected CDC rows; minimum `1`. A complete transaction may exceed it. |
| `cdc.timeout` | No | `5` | CDC idle-read timeout in seconds; minimum `1`. Zero is rejected because it can select an unbounded native wait. |
| `cdc.shared.state.wait.seconds` | No | `300` | Maximum time a delete reader waits for its table's upsert reader to publish automatic initialization state. |
| `cdc.max.records` | No | `64` | Soft native record target per CDC session; range `1`–`256`. Once reached, records continue until every transaction already observed commits, rolls back, or Informix returns TIMEOUT. |
| `cdc.max.frame.bytes` | No | `16777216` | Maximum accepted native CDC frame size (16 MiB by default; minimum `16`). |
| `cdc.max.transaction.records` | No | `100000` | Maximum records buffered in an open transaction. Exceeding it fails without emitting uncommitted data. |
| `cdc.max.poll.records` | No | `200000` | Hard total decoded-record bound for one native CDC poll, including committed records interleaved with a long-running transaction. |
| `cdc.max.poll.bytes` | No | `0` | Optional estimated retained-byte bound per CDC poll, counting native frames plus recursively sized decoded Python values. The default `0` disables the bound and byte accounting. |
| `cdc.read.bytes` | No | `32000` | Bytes requested in each SmartLOB CDC read. |

Because per-table options are supported, configure the Unity Catalog connection with this exact `externalOptionsAllowList`:

```text
qualified_source_table,snapshot.page.size,snapshot.max.rows,max.records.per.batch,cdc.timeout,cdc.max.records
```

Create the connection from the Lakeflow Community Connector flow on the **Add Data** page, with the Databricks CLI, or with the Databricks SDK for Python. The Unity Catalog connection type must be `COMMUNITY`, and `sourceName` must be `informix`.

### Create a connection with the Databricks CLI

The example uses `jq` so the password comes from an environment variable instead of being written literally into shell history:

```bash
export INFORMIX_PASSWORD='<secret>'

databricks connections create --json "$(jq -n \
  --arg password "$INFORMIX_PASSWORD" \
  '{
    name: "informix_sales",
    connection_type: "COMMUNITY",
    comment: "Informix CDC connection",
    options: {
      sourceName: "informix",
      hostname: "informix.example.internal",
      port: "9088",
      database: "sales",
      user: "cdc_service",
      password: $password,
      server: "informix_prod",
      encrypt: "true",
      "cdc.shared.state.location": "/Volumes/main/informix_cdc/state",
      externalOptionsAllowList: "qualified_source_table,snapshot.page.size,snapshot.max.rows,max.records.per.batch,cdc.timeout,cdc.max.records"
    }
  }')"

unset INFORMIX_PASSWORD
```

Use `--profile <profile-name>` when the CLI should use a non-default Databricks profile. Confirm the result with:

```bash
databricks connections get informix_sales
```

### Update a connection with the Databricks CLI

Pass the complete desired `options` map when updating a connection; do not assume omitted options will be preserved. This TLS example again uses `jq` to keep the password out of the literal command:

```bash
export INFORMIX_PASSWORD='<secret>'
export DATABRICKS_PROFILE='<profile-name>'

databricks connections update informix_sales --json "$(jq -n \
  --arg password "$INFORMIX_PASSWORD" \
  '{
    options: {
      sourceName: "informix",
      hostname: "informix.example.internal",
      port: "9089",
      server: "informix_prod",
      database: "sales",
      user: "cdc_service",
      password: $password,
      encrypt: "true",
      "ssl.ca.file": "/Volumes/catalog/schema/artifacts/informix-ca.pem",
      "cdc.shared.state.location": "/Volumes/main/informix_cdc/state",
      externalOptionsAllowList: "qualified_source_table,snapshot.page.size,snapshot.max.rows,max.records.per.batch,cdc.timeout,cdc.max.records"
    }
  }')" \
  --profile "$DATABRICKS_PROFILE"

unset INFORMIX_PASSWORD DATABRICKS_PROFILE
```

The CA path must be readable by the serverless pipeline. Use the TLS listener's DNS hostname rather than its IP address when certificate hostname verification requires it.

### Create a connection with the Python API

Install or upgrade the Databricks SDK, configure its normal authentication environment, and call the Unity Catalog Connections API:

```python
import os

from databricks.sdk import WorkspaceClient


w = WorkspaceClient()  # Uses the standard Databricks SDK authentication chain.

connection = w.connections.create(
    name="informix_sales",
    connection_type="COMMUNITY",
    comment="Informix CDC connection",
    options={
        "sourceName": "informix",
        "hostname": "informix.example.internal",
        "port": "9088",
        "database": "sales",
        "user": "cdc_service",
        "password": os.environ["INFORMIX_PASSWORD"],
        "server": "informix_prod",
        "encrypt": "true",
        "cdc.shared.state.location": "/Volumes/main/informix_cdc/state",
        "externalOptionsAllowList": (
            "qualified_source_table,snapshot.page.size,snapshot.max.rows,"
            "max.records.per.batch,cdc.timeout,cdc.max.records"
        ),
    },
)

print(connection.full_name or connection.name)
```

Some older `databricks-sdk` releases do not include `COMMUNITY` in their generated `ConnectionType` enum. Passing the API value as the string shown above works with current SDK releases; upgrade the SDK if the installed release rejects it.

Do not hard-code production credentials in scripts, notebooks, pipeline JSON, or source control. Load them from your deployment system's secret store and pass them only while creating the connection. A minimal non-secret configuration shape is:

```json
{
  "hostname": "informix.example.internal",
  "port": "9088",
  "database": "sales",
  "server": "informix_prod",
  "encrypt": "true",
  "table.include.list": "sales.informix.orders,sales.informix.order_items"
}
```

Supply `user` and `password` separately through secret-backed connection properties.

### Authentication, redirects, and TLS

Normal Informix ASC username/password authentication and non-interactive PAM authentication are implemented. Both require verified TLS; `encrypt=false` is rejected. The normal ASC protocol carries the password directly.

TLS always verifies the hostname. It uses Python's system CA store by default, or a PEM CA bundle supplied with `ssl.ca.file`. Insecure/skip-verification mode is not supported. The CA file must be available to each serverless worker; do not rely on an ephemeral local path that is not distributed with the pipeline.

PAM never opens an interactive prompt. The built-in provider returns `password` for echo-off challenges and `authentication.pam.echo.response` for echo-on challenges; when the latter is absent it falls back to `password` for both. Informational PAM messages require no response. Multi-round conversations are supported up to `authentication.pam.max.rounds`, subject to the single overall login deadline and the 512-byte encoded-response limit. An unexpected message style, missing provider, excessive round count, oversized response, rejection, or timeout fails closed.

For a different non-interactive exchange, `authentication.provider.factory` names an installed, administrator-reviewed Python callable as `module:callable`. The callable receives the complete options mapping and returns the response provider. This imports and executes code in the pipeline, so never point it at unreviewed code. A custom factory supersedes the built-in echo-on/password behavior.

Keep `password` and `authentication.pam.echo.response` in secret-backed connection properties. Do not place either in pipeline JSON, source control, logs, provider error messages, or the redirect allowlist. For example, this is a secret-free PAM configuration shape:

```json
{
  "hostname": "informix.example.internal",
  "port": "9090",
  "server": "informix_prod_pam",
  "database": "sales",
  "user": "cdc_service",
  "authentication.mode": "pam",
  "authentication.pam.max.rounds": "8"
}
```

Redirects are an explicit opt-in because the destination is supplied by the server. With `redirect.enabled=true`, every target must exactly match a `host:numeric-port` entry in `redirect.allowlist`; wildcards, service names, and omitted ports are rejected. A hostname must resolve to exactly one stable address. If it resolves to a private, loopback, link-local, multicast, or otherwise non-public address, the exact resolved `IP:numeric-port` must also be allow-listed. These checks prevent a permitted hostname from becoming an unrestricted network pivot.

Each redirect discards the old socket, parser, authentication, and statement state and starts login again. It remains inside `authentication.login.timeout`, cannot exceed `redirect.max`, and cannot revisit the same server/address/port identity. DNS is resolved and checked before connection. TLS is recreated with the original trust policy and hostname verification is performed against the redirected hostname; redirects cannot downgrade encryption or bypass certificate verification.

A secret-free redirect configuration shape is:

```json
{
  "hostname": "informix-router.example.internal",
  "port": "9088",
  "server": "g_informix",
  "database": "sales",
  "user": "cdc_service",
  "redirect.enabled": "true",
  "redirect.allowlist": "informix-a.example.internal:9091,10.20.30.40:9091",
  "redirect.max": "2",
  "authentication.login.timeout": "30"
}
```

GSS/Kerberos, private-server authentication, and automatic server-name or locale discovery remain unsupported. PAM and the full redirect/reconnect security path have live integration coverage. The redirect test uses a deterministic ASF type-13 responder rather than a redirect emitted by IDS/HDR itself. Serverless Lakeflow Connect execution over an Informix TLS listener has been validated separately.

The fixture at `/Users/leon.eller/work/dev/informix-cdc` live-tests PAM against an Informix listener at `localhost:9090`. Its deterministic redirect responder listens at `localhost:9191`, emits ASF session type 13, and redirects to `127.0.0.1:9088`; the test then proves a fresh, allow-listed, TLS-revalidated login and query on the target. This validates the connector's complete redirect parsing, policy, reset, and reconnect path, but is not evidence that IDS/HDR emits redirects itself. Client-side `sqlhosts` failover is likewise not redirect coverage. Fixture credentials are disposable test data and are intentionally not repeated here.

## Supported objects and naming

The connector dynamically discovers all eligible base tables in the configured database, then applies optional filters. It excludes system owners and tables whose names begin with `sys`, plus `syscdcv1` internals. Views and other non-table objects are not exposed.

Pipeline source names use `owner.table`, such as `informix.orders`. Filter patterns are case-sensitive shell-style patterns and can match:

- `database.owner.table`
- `owner.table`
- `database:owner.table`

Include filtering runs before exclusion filtering. Identifiers are limited to a letter or underscore followed by letters, digits, `_`, or `$`.

### CDC type support

A table supports CDC only when it has a primary key and every column has a supported CDC encoding.

| CDC status | Informix types |
|---|---|
| Supported end to end | `SMALLINT`, `INTEGER`/`INT`, `SERIAL`, `INT8`, `SERIAL8`, `BIGINT`, `BIGSERIAL`, `FLOAT`/`DOUBLE`, `REAL`/`SMALLFLOAT`, `DECIMAL`/`NUMERIC`, `MONEY`, `DATE`, supported `DATETIME` qualifiers, `BOOLEAN`, `CHAR`, `VARCHAR`/`NVARCHAR` |
| Rejected before ingestion | `BYTE`, `TEXT`, `BLOB`, `CLOB`, `INTERVAL`, `LVARCHAR`, `NCHAR`, complex and opaque types |
| Excluded from speculative decoding | Unknown catalog types, UDT and complex types such as `ROW`, `SET`, `LIST`, and `MULTISET` |

`INT8` and `SERIAL8` use Informix's complete ten-byte signed-magnitude CDC representation. A `DATETIME` qualifier is CDC-capable when its start and end fields are supported by the native decoder. Values containing `YEAR` through at least `DAY` are exposed as timezone-free Spark timestamps; partial/time-only values are deterministic strings and never acquire the worker's current date.

Informix's native capture-column API and ordinary SQLI snapshot protocol do not yet provide end-to-end materialization for the rejected types. The connector fails during metadata discovery with the exact columns involved instead of advertising a snapshot fallback that later fails or silently returning placeholders. Unknown catalog type codes also fail discovery. Decimal CDC requires valid precision and scale metadata within Spark's 38-digit decimal limit.

## Snapshot, CDC, and deletes

For a CDC-capable table, the production SQLI bridge records the prepared CDC boundary, establishes a repeatable-read transaction, captures the snapshot LSN, and reads the bounded initial snapshot in complete primary-key order before commit. Non-ANSI databases use explicit `BEGIN WORK`; ANSI databases use their implicit transaction after committing the catalog-mode probe. Snapshot rows use the transactional snapshot LSN, while CDC resumes from the earlier prepared boundary; changes preceding the snapshot are therefore ordered before its rows rather than creating false SCD2 history. Injected test bridges may retain seek-pagination behavior.

To validate the ANSI transaction sequence against a live ANSI-mode database, supply the standard `CONNECTOR_TEST_CONFIG_JSON` or `CONNECTOR_TEST_CONFIG_PATH` configuration with `ansi.live.validation=true` and `ansi.test.table=owner.table`, then run `ansi_live_test.py`. The selected table must have a primary key and no more than 10,000 rows.

Only complete committed transactions are emitted. Inserts and update after-images go to the data channel; deletes go to an independently checkpointed delete channel. Delete output contains the primary-key fields and connector metadata. A primary-key update emits the new row and deletes the old key. Rollbacks are suppressed. Transactions are never split: `cdc.max.records` and `max.records.per.batch` are soft targets, and a transaction already observed is read through commit/rollback unless Informix returns TIMEOUT. CDC metadata and timeout control frames do not consume the `cdc.max.records` budget. Record and estimated retained-memory bounds limit each poll. Replay is transaction-atomic: another transaction's checkpoint never removes earlier records from a newly committed interleaved transaction. Continuous runs follow new commits without a mode-specific option.

The framework does not pass snapshot offsets to independently instantiated delete streams. The connector therefore coordinates automatically through atomic per-table JSON records in `cdc.shared.state.location`. Only the upsert reader may enable full-row logging and publish a boundary; its delete reader waits and consumes that boundary. A retained record is safely reused after a full refresh. If it has fallen outside Informix log retention, the upsert reader atomically replaces it with a newly prepared boundary while delete readers wait. Corrupt, mismatched, partial, or inaccessible state fails closed.

Shared-state locks use atomic lock directories and fail closed rather than
reclaiming ownership on elapsed wall-clock time. This avoids two workers
publishing state concurrently after a pause or clock skew. If a worker terminates
while holding a lock, stop every pipeline using the connection, remove the
reported `.lock` directory, and restart. Never remove it while a pipeline is active.
Before its first lock, each worker runs a bounded, concurrent client-side probe
of exclusive directory creation and rename visibility and fails before capture
if those primitives are unavailable. This verifies that worker's mounted Volume
client; deployment validation must still exercise concurrent serverless workers.
Timeout errors include lock owner age, PID, path,
and recovery instructions. Abandoned temporary and released-lock artifacts older
than one hour are removed during later lock acquisition.

Validate the same Volume from multiple Spark Python worker hosts before
production use:

```python
from databricks.labs.community_connector.sources.informix.volume_concurrency_validation import (
    validate_volume_concurrency,
)

validate_volume_concurrency(
    spark,
    "/Volumes/catalog/schema/volume/informix-state",
)
```

The validator fails unless exactly one worker wins the shared `mkdir`, at least
two worker hosts participate, and the subsequent rename is immediately visible.

After stopping every pipeline using the connection, an abandoned lock can be
removed with ownership verification using the exact path and token from the
timeout error:

```python
from databricks.labs.community_connector.sources.informix.informix import (
    recover_shared_state_lock,
)

recover_shared_state_lock(
    "/Volumes/catalog/schema/volume/informix-state",
    "/Volumes/catalog/schema/volume/informix-state/.../table.json.lock",
    "<32-character-token-from-error>",
    acknowledge_pipelines_stopped=True,
)
```

Regenerate the deployable file with `bash src/databricks/labs/community_connector/sources/informix/generate_source.sh`. Informix-owned code wraps the generated reader base and installs the AvailableNow callback at class creation, so this output is identical to the repository's canonical merge command and needs no post-generation patch. The source-local tests use `*_test.py` names so the standard merger excludes them.

Delivery is at least once. A failure after rows are returned but before Lakeflow commits its checkpoint can replay them. `TRUNCATE` cannot be represented by keyed Lakeflow deletes and fails explicitly. Snapshot-only tables are fully reread and fail when they exceed `snapshot.max.rows`.

The connector adds `_informix_change_lsn`, `_informix_commit_lsn`, `_informix_tx_id`, and `_informix_op` to rows. The two LSN columns are fixed-width, zero-padded 20-digit decimal strings so Spark string ordering is identical to numeric LSN ordering. `_informix_change_lsn` is the incremental cursor; operations are `r` (snapshot), `c` (insert), `u` (update), and `d` (delete). Targets created with an older connector that emitted unpadded LSN strings require a full refresh before using this version.

### Checkpoints and log retention

During snapshot, Lakeflow checkpoints the connector offset version, snapshot LSN, last primary-key values, a source-schema fingerprint, a generation-specific schema node ID, and a pipeline scope. A completed consistent snapshot publishes its fresh resume LSN for both independently instantiated channels, so a full refresh never replays older retained transactions or historical `TRUNCATE` records. Streaming checkpoints the version, `commit_lsn`, `change_lsn`, the oldest required `begin_lsn`, fingerprint, schema node ID, pipeline scope, and triggered-update generation; retaining the begin position is necessary for interleaved transactions. Triggered upsert and delete readers use one atomically published per-table high-water LSN rather than sampling independently. The generated source creates one random registration scope before Spark serializes its readers. Every upsert and delete reader receives that same value, records it in its first offset, and thereafter prefers the durable checkpoint value across driver and worker restarts. Snapshot and trigger records are keyed by this scope, isolating concurrently registered pipelines without requiring Lakeflow to expose pipeline or update IDs to Python workers. The node ID distinguishes separate table generations that happen to have identical layouts. Data and delete channels have separate checkpoints and replay the source independently. A missing or unsupported offset version fails closed and requires a full refresh. A changed fingerprint enters the additive schema transition described below. Offset version 6 introduces the durable registration scope; earlier checkpoints require a full refresh. Shared-state version 5 removes boundaries from earlier unscoped formats while preserving schema history.

An idle timeout returns no rows and leaves the checkpoint unchanged. Incomplete or open transactions do not advance it. If the restart LSN predates the minimum retained logical log in `sysmaster:syslogs`, continuation fails and the table must be resnapshotted. Every CDC session validates its initial METADATA frame against a fresh catalog layout before decoding later records; another METADATA frame in that session fails immediately and requires a full refresh.

### Additive schema evolution without full refresh

A normal pipeline restart can evolve an existing CDC table when nullable,
CDC-supported columns are appended at the end of the Informix table. Existing
columns must retain their names, order, types, nullability, widths, precision,
and scale, and the primary key must remain unchanged. Drops, renames, reorders,
type changes, non-nullable additions, unsupported types, and primary-key changes
still require a full refresh.

Informix rejects `ALTER TABLE` while full-row logging is enabled. Stop every
pipeline using this connection, then quiesce source writes before the DDL
sequence and keep them quiesced until the restarted Lakeflow update completes
its schema transition. Disable full-row logging,
apply the additive DDL, immediately re-enable logging, and restart the pipeline
normally—do not request a full refresh. For example:

```sql
EXECUTE FUNCTION syscdcv1:cdc_set_fullrowlogging(
  'testdb:informix.members', 0
);
ALTER TABLE informix.members ADD new_nullable_column VARCHAR(64);
EXECUTE FUNCTION syscdcv1:cdc_set_fullrowlogging(
  'testdb:informix.members', 1
);
```

The shared Volume state stores predecessor-linked schema generations, the
Informix catalog table ID for incarnation detection, and one
transition LSN per additive step. Lagging pipelines advance one recorded schema
at a time instead of skipping intermediate layouts. An incompatible full
refresh creates a new root generation while retaining older descriptors for
other pipelines. Both upsert and delete readers drain the previous descriptor,
checkpoint the transition, and then capture with the expanded descriptor.
Lakeflow retains its checkpoints and Delta adds the new column.
Rows captured through the old descriptor are emitted with `NULL` for the new
column. Resume source writes only after the transition update completes, then
update or backfill the new column if existing rows need values. A transaction
that spans the transition fails closed. Run the connector once after upgrading from a version without
schema history and before applying DDL so the current checkpoint descriptor is
seeded in shared state.

History is bounded by the one-MiB serialized state limit rather than a fixed
number of schema nodes. If that limit is reached, stop all pipelines using the
connection, configure a new shared-state location, and perform full refreshes.

## Table configuration

```json
{
  "pipeline_spec": {
    "connection_name": "informix_sales",
    "object": [
      {
        "table": {
          "source_table": "orders",
          "destination_table": "orders",
          "table_configuration": {
            "qualified_source_table": "informix.orders",
            "snapshot.page.size": "2000",
            "max.records.per.batch": "2000",
            "sequence_by": "_informix_change_lsn"
          }
        }
      }
    ]
  }
}
```

Supported source-specific table options are `qualified_source_table`, `snapshot.page.size`, `snapshot.max.rows`, `max.records.per.batch`, `cdc.timeout`, and `cdc.max.records`. `qualified_source_table` maps the pipeline's logical table name to an Informix `owner.table` name. Standard destination, SCD, key, sequence, and clustering options remain available.

### SCD Type 2 sequencing and validity columns

Set `scd_type` to `SCD_TYPE_2` to retain row history. Lakeflow derives the types and values of `__START_AT` and `__END_AT` from `sequence_by`. The default `_informix_change_lsn` sequence is the safest ordering value, but it produces string validity columns containing zero-padded 20-digit decimal LSNs:

```json
{
  "scd_type": "SCD_TYPE_2",
  "sequence_by": "_informix_change_lsn"
}
```

To produce timestamp validity columns, sequence on a non-null source timestamp instead:

```json
{
  "qualified_source_table": "informix.members",
  "scd_type": "SCD_TYPE_2",
  "sequence_by": "updated_at"
}
```

This changes Lakeflow Auto CDC ordering and deduplication to `updated_at`; it does not merely change the display type. The connector continues using Informix LSNs for native CDC reads, recovery, and source checkpoints. Use a timestamp only when it is updated for every source change and has enough precision to order repeated changes to the same key. Otherwise retain `_informix_change_lsn`. Changing an existing SCD2 target between LSN and timestamp sequencing changes the validity-column schema and requires recreating or fully refreshing that target. A null `__END_AT` is expected for the currently active version.

## Operational guidance

- Start with one small table and verify snapshot, insert, update, delete, restart, rollback, idle timeout, and retention-expiry behavior against the target Informix version before broader use.
- Disposable Informix 15 testing has validated normal-password and PAM authentication, queries, discovery, snapshots, and committed INSERT/UPDATE/DELETE transactions through `syscdcv1`. A deterministic ASF type-13 responder has validated the complete redirect/reconnect security path through a successful target query. A serverless Lakeflow Connect pipeline has validated TLS, multi-table snapshots, checkpointed CDC flows, deletes, and SCD Type 2 materialization. Validate TLS trust, locale behavior, permissions, schemas, data-type boundaries, restart behavior, retention, and IDS/HDR-emitted redirects against the target environment.
- Authentication errors commonly indicate a wrong `server`/locale, unsupported authentication mode or redirect, an untrusted/mismatched TLS certificate, or insufficient `syscdcv1` privileges.
- Schema changes that alter captured column layout are not guaranteed to be safe during active capture. Restart and validate at a clean LSN boundary.
- Ensure source log retention covers downtime and the oldest checkpoint. Otherwise resnapshotting is required.

## References

- [Debezium Informix connector documentation](https://debezium.io/documentation/reference/stable/connectors/informix.html) for Informix-side CDC concepts and prerequisites
- Informix CDC installation script: `$INFORMIXDIR/etc/syscdcv1.sql`
