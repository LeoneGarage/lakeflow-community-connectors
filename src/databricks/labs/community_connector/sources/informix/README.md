# Lakeflow Informix Community Connector

This connector discovers Informix user tables, takes an initial snapshot, and then captures committed changes through the Informix CDC API. The SQLI client, SmartLOB framing, CDC decoding, transaction handling, and checkpoint recovery are implemented in Python. It requires no JVM, database driver, JAR staging, or native Informix client and is intended to run on serverless Lakeflow Connect compute.

The pure-Python protocol implementation has completed authentication, query, discovery, snapshot, and end-to-end transactional INSERT/UPDATE/DELETE CDC validation against a disposable Informix 15 fixture. It has also completed a serverless Lakeflow Connect pipeline run over TLS, including multi-table discovery, snapshots, checkpointed CDC flow execution, deletes, and SCD Type 2 materialization. Validate it against your Informix version, topology, security policy, and workload before broader use.

## Prerequisites

- An Informix SQLI endpoint reachable from serverless Lakeflow Connect compute. Configure firewalls, private connectivity, and DNS as appropriate. Protocol redirects are disabled by default and require the explicit controls described below.
- A reachable SQLI endpoint. TLS is enabled by default and strongly recommended; its certificate must be trusted by Python's system CA store and match `hostname`. Plaintext transport is available only by explicitly setting `encrypt=false`.
- Transaction logging enabled for the source database. Retained logical logs must extend back to the oldest connector checkpoint.
- The Informix CDC API installed by an administrator. Run `$INFORMIXDIR/etc/syscdcv1.sql` (commonly with `dbaccess sysadmin`) to create the `syscdcv1` database and `cdc_*` routines.
- A normal username/password account that can read source catalog metadata and selected tables, use the required `syscdcv1` routines, and enable full-row logging. The validated reference setup grants DBA-level access to `syscdcv1`.
- The exact `INFORMIXSERVER` value. `DB_LOCALE` defaults to `en_US.819` and `CLIENT_LOCALE` defaults to `en_US.utf8`; override either when the source requires another locale because the connector does not discover them.

The connector enables full-row logging for captured tables and leaves it enabled when a finite poll ends, avoiding capture gaps between polls. Ensure this operational change is acceptable on the source system.

Initial CDC preparation is automatic. For each table, its upsert reader enables
full-row logging, captures one LSN, and atomically publishes it as a state
record. The independently scheduled delete reader waits for and uses that exact
LSN. State lives in a Lakebase Postgres endpoint that the connector provisions on
first use, so no shared directory has to be prepared; the only Volume needed is
`snapshot.staging.location`, for snapshot page payloads.

A delete reader that has no offset of its own is bootstrapping, which is exactly the
state a **full refresh** produces. It prefers the record published by *this* update and
waits several reads for it, only then falling back to the scope-independent
`schema-nodes` record. That fallback is necessary — an upsert reader that resumes a
checkpoint publishes no scoped record, so without it the delete channel would stall from
the second update onward — but `schema-nodes` is keyed by table identity and schema
fingerprint alone, so it survives every update and its `start_lsn` can belong to a
**previous logical-log incarnation**. If the source's logical log is reinitialized (a
server rebuild, `oninit -iy`), a stale boundary naming a higher `uniqid` than the server
has reached is detected and the channel restarts from the server's current LSN. Deletes
committed before the reinitialization are no longer in the log and cannot be
replicated — run a full refresh if the destination must match the source exactly.

## Setup

### Connection parameters

| Option | Required | Default | Description |
|---|---:|---:|---|
| `hostname` | Yes | | Informix SQLI hostname or IP address. It is also used for TLS hostname verification. |
| `database` | Yes | | Database to discover, snapshot, and capture. |
| `user` | Yes | | Informix normal-auth user with metadata, snapshot, and CDC privileges. |
| `password` | Yes | | Password for `user`; store it as a secret. |
| `server` | Yes | | Exact `INFORMIXSERVER` name sent during SQLI authentication. |
| `snapshot.staging.location` | Yes | | Writable Unity Catalog Volume directory for gzip-compressed immutable initial-snapshot pages, for example `/Volumes/main/informix_cdc/staging`. This is the only Volume the connector uses; everything else lives in Lakebase. Restrict `READ VOLUME`: staged files contain source row values. Pages older than the checkpoint start page are removed incrementally; remaining staging is removed after the checkpoint reaches CDC. Abandoned scopes use `snapshot.staging.retention.days`. |
| `snapshot.staging.retention.days` | No | `4` | Days to retain abandoned immutable snapshot page scopes before best-effort cleanup; minimum `1`. Active and checkpoint-referenced scopes are retained. |
| `DB_LOCALE` | No | `en_US.819` | Database locale. Set it explicitly when the database uses another locale. |
| `CLIENT_LOCALE` | No | `en_US.utf8` | Client locale. Its codeset controls Python row decoding. |
| `port` | No | `9088` | Informix SQLI port; range `1`–`65535`. |
| `encrypt` | No | `true` | Enables TLS. Boolean values must be `1`, `true`, `yes`, `0`, `false`, or `no`, case-insensitively. Setting `false` uses plaintext TCP and exposes credentials, authentication exchanges, queries, and row data to the network; use it only on a trusted private network. |
| `ssl.ca.file` | No | system CA store | Path to a PEM CA bundle available on the pipeline worker. Hostname verification remains enabled. |
| `authentication.mode` | No | `password` | `password` or non-interactive `pam`. Other modes fail closed. |
| `authentication.provider.factory` | No | built-in provider | Trusted Python factory in `module:callable` form. It receives all connection options and returns a non-interactive PAM response provider. |
| `authentication.pam.echo.response` | No | `password` | Secret response used by the built-in provider for PAM echo-on prompts. Echo-off prompts use `password`. |
| `authentication.pam.max.rounds` | No | `16` | Maximum PAM challenge rounds before login fails. Each encoded response is limited to 512 bytes. |
| `authentication.login.timeout` | No | `30` | Overall login deadline in seconds, shared by connection, authentication, and all redirect attempts. |
| `max.concurrent.connections` | No | `16` | Maximum concurrent SQLI connections to one hostname, port, and `INFORMIXSERVER`, coordinated through the Lakebase `conn_slots` table; range `1`–`9999`. Capacity is the number of seeded rows, so it can be changed without stopping pipelines: acquisition bounds `slot_id` by the configured limit, so lowering it takes effect immediately while surplus rows are left in place rather than deleted (removing a row a live reader holds would silently revoke its lease), and raising it seeds additional rows idempotently. A claim is one `UPDATE ... FOR UPDATE SKIP LOCKED` that takes the lowest free or expired slot and increments an `epoch`; that epoch is required to renew or release, so a reader stalled past its 120s lease cannot resurrect it — and, more importantly, cannot free a slot its successor legitimately holds. A lease nobody releases simply expires. |
| `upsert.connection.reservation` | No | `0` | Connection slots reserved for the upsert channel, which the delete channel may not claim; range `0` to `max.concurrent.connections - 1`. This is a floor, not a partition: beyond the reserved slots both channels compete for everything remaining, so an idle channel never strands capacity. Total capacity is unchanged, so the `max.concurrent.connections` configuration is unaffected. Defaults to `0`, which is exactly the unreserved behaviour. Raise it only with care: both channels replay the same log range, so their throughput needs are symmetric, and where a starved upsert reader merely slows an update, a delete reader held back until its checkpoint predates log retention forces a re-snapshot. A value equal to `max.concurrent.connections` is rejected rather than clamped, because it would starve the delete channel permanently. Like `max.concurrent.connections`, this value is recorded in the endpoint's connection-limit configuration (the Lakebase `conn_limits` row) and must match across **every** pipeline sharing one Informix endpoint: the reservation partitions a single shared slot namespace, so a pipeline configuring a smaller value would let its delete readers claim slots another pipeline reserves for upserts. A mismatch fails at startup with an actionable message rather than silently voiding the reservation. |
| `connection.wait.timeout.seconds` | No | `600` | Maximum time a microbatch blocks while acquiring an Informix connection-capacity slot; minimum `1`. A **triggered** read that exhausts it fails with `ConnectionCapacityUnavailable` rather than returning an empty successful batch: triggered flows drain and terminate, so they release their slots, pressure is transient, and an empty batch is how `AvailableNow` recognises a drained source. A **continuous** read does not wait this long at all — see `capacity.retry.max.delay.seconds`. |
| `capacity.retry.max.delay.seconds` | No | `5` | Enables capacity backpressure for **continuous** pipelines, where every flow is a permanent standing consumer of a fixed slot count so contention is the steady state rather than a fault. Instead of blocking for the whole `connection.wait.timeout.seconds` and then failing, a continuous reader spends a short bounded budget trying to claim a slot and then returns an empty batch, waiting up to this many seconds (jittered) first. With a positive value this also caps each acquisition attempt to a short, miss-scaled budget: the budget grows with the number of consecutive reads that reader has finished without ever getting a slot, so a reader starved longest presses hardest; acquiring a slot resets that count. The default `5` spaces starved readers apart with jittered backoff, floored at `0.1s` so a low draw cannot re-sweep almost immediately. It was briefly `8` with a `0.5s` floor to reduce how often starved readers touched the shared Volume, whose FUSE mount disconnects with `ENOTCONN` under sustained write frequency. Slots are Postgres rows now — a measured 247–266 req/s at 120 concurrent connections held latency flat with no errors — so that traffic is no longer a constraint and the narrower range is restored for faster slot handoff. This value applies only to the steady-state **stream** (CDC) phase: the blocking **snapshot** is bulk, latency-tolerant work, so it always uses an effective delay of `0` regardless of this setting — it blocks the full `connection.wait.timeout.seconds` per attempt to patiently wait out contention for a slot, and the **incremental** snapshot reader phase does the same by default (governed by `snapshot.incremental.blocking`). Setting `0` extends that full-block behavior to the stream phase too for a **continuous** flow: the acquisition attempt blocks the full `connection.wait.timeout.seconds` (maximum chance to claim a slot before giving up) and there is no inter-read delay before the next attempt — prompt re-scan with no spacing, which raises shared-Volume metadata load under heavy contention. Either way a continuous flow **always yields** an empty batch on exhaustion rather than failing, bounded by `capacity.retry.max.retries` consecutive misses; the returned offset carries the count so the stream stays live and never reaches a durable checkpoint. Once that cap is reached the flow logs an error and fails with `ConnectionCapacityUnavailable`, so an undersized endpoint cannot pass unnoticed — prefer a triggered pipeline, a higher `max.concurrent.connections`, or fewer flows when readers outnumber slots. This value is irrelevant to a **triggered** flow, which always blocks the full wait and raises on exhaustion. Must be `>= 0`. |
| `capacity.retry.max.retries` | No | `500` | Consecutive capacity misses a **continuous** flow tolerates before it stops yielding and fails with `ConnectionCapacityUnavailable`. A reader that never claims a slot yields empty batches, which is indistinguishable from an idle source; this bound makes a permanently undersized endpoint fail loudly instead of replicating nothing forever. Acquiring a slot resets the count, so it only ever measures a current run of misses. Minimum `1`. Irrelevant to a triggered flow, which raises on the first exhausted wait. |
| `dropped.mount.max.retries` | No | `10` | Consecutive reads that find the snapshot staging Volume's FUSE mount disconnected (`ENOTCONN`) before the flow stops yielding empty batches and fails with `InformixError`. A dropped mount is normally transient — the connector yields an empty batch and the next read proceeds once the mount returns, leaving the checkpoint untouched — but a mount that never comes back leaves every flow yielding empties while the pipeline reports `RUNNING` and commits zero-row Delta versions on schedule, which is indistinguishable from an idle source. This bound surfaces that outage as a flow failure instead of a silent stall. The count measures **reads, not seconds**, so the wall-clock tolerance is this many microbatch intervals (the default is roughly 5 minutes at a 30s interval); raise it for a long trigger interval. Any read that reaches the Volume resets the count, so a flapping mount does not accumulate toward the cap. Minimum `1`. |
| `lakebase.password` | **Yes** | | Password for `informix_state_user`, the long-lived Postgres role all state access authenticates as. Required, because it is the credential the running pipeline uses for every read and write of state, so nothing in that path depends on a token that can expire. It is the source of truth: a role whose password differs is repaired to match, which invalidates the previous password. The role name is fixed and not configurable — every pipeline sharing an endpoint must agree on which role owns the state tables. |
| `lakebase.admin_user` | No | ambient identity | Postgres role used **only** to provision `informix_state_user`, which requires CREATEROLE. Separate from the state role on purpose, so the credential every reader holds cannot create roles. Defaults to `DATABRICKS_CLIENT_ID` for a service principal, otherwise the workspace user. |
| `lakebase.admin_password` | No | OAuth credential | Password for `lakebase.admin_user`. When unset the admin authenticates with a short-lived OAuth credential minted for the pipeline's identity, which is the usual case. Not the same as `lakebase.password`. |
| `lakebase.project.id` | No | derived | Overrides the project id derived from `hostname`, `port`, and `server`. Set it to share one project across connections that should pool capacity, or to adopt a pre-created project. |
| `lakebase.autoscale.min.cu` | No | `0.5` | Minimum Lakebase compute units; `0.5` is the smallest the API accepts. Applied only when the connector creates the project. |
| `lakebase.autoscale.max.cu` | No | `2` | Maximum Lakebase compute units. Applied only at project creation. |
| `lakebase.suspend.timeout.seconds` | No | `60` | Idle seconds before the endpoint scales to zero compute. Applied only at project creation, because the API accepts it solely through a project's `default_endpoint_settings` and never retroactively. |
| `lakebase.provision.timeout.seconds` | No | `180` | Absolute deadline for project/endpoint provisioning and for waiting on concurrent provisioning. |
| `lakebase.database` | No | `databricks_postgres` | Postgres database holding the state tables. |
| `state.gc.retention.days` | No | `30` | Age in days after which unreferenced per-table state records (activity markers for pipelines that no longer read the table) become eligible for best-effort garbage collection; minimum `1`. |
| `state.gc.interval.hours` | No | `24` | Minimum hours between garbage-collection sweeps for a table, and the window within which a reader touches its activity marker; must be `> 0`. |
| `state.gc.drop.unused.projects` | No | `false` | Whether to also drop Lakebase projects unused within `state.gc.project.retention.days`. Accepts `true` or `false`. |
| `state.gc.project.retention.days` | No | `90` | Age in days after which an unused Lakebase project becomes eligible for removal when `state.gc.drop.unused.projects` is `true`; must be `>=` `state.gc.retention.days`. |
| `connection.backlog.hint.enabled` | No | `true` | Whether readers publish and consume connection-backlog hints. A reader waiting for a connection slot cannot measure how far behind it is: reading the server log position requires a connection, which requires the very slot it is waiting for. A reader that already holds a slot therefore publishes the server log position into the `backlog_hints` table, and a waiter subtracts its own checkpoint from that value to estimate its backlog, then presses proportionally harder (a larger per-attempt acquisition budget) so scarce slots skew toward the readers furthest behind. Publication is one `INSERT ... ON CONFLICT DO UPDATE` that takes `GREATEST(existing, new)`, so a hint can never regress no matter which reader writes last or in what order. A waiter reads only rows updated within the last 45 seconds, so a stale hint is simply invisible rather than misleading. Purely advisory and fail-open: an absent, stale, malformed, or unreadable hint leaves acquisition behaving exactly as it does with hints disabled, so this can never affect correctness or fail a read. Note the published value is the **whole server's** log position, so it over-states the backlog of a low-traffic table (the log advances because other tables are written); it measures staleness rather than a row count and is therefore bucketed on a log scale into 8 ranks. Set to `false` to disable both halves of the protocol. |
| `snapshot.incremental.blocking` | No | `true` | Whether the in-progress **incremental** snapshot reader phase (primary-key chunks interleaved with CDC, before the copy completes) blocks for a connection-capacity slot. Default `true`: it blocks the full `connection.wait.timeout.seconds` per attempt to patiently claim a slot, like the consistent snapshot, since bulk copying is latency-tolerant and benefits from pressing for a slot rather than yielding empty. Set `false` to treat the incremental reader phase as steady-state **stream** instead — it then uses the continuous miss-scaled yield budget and `capacity.retry.max.delay.seconds` inter-read backoff, freeing the worker between attempts at the cost of a slower copy under contention. Only affects **continuous** pipelines; a **triggered** flow always blocks fully. Once the incremental snapshot completes and the reader becomes pure CDC, this option no longer applies. |
| `redirect.enabled` | No | `false` | Opts into protocol redirects. Boolean values are validated strictly; a redirect still fails unless its destination is explicitly allowed. |
| `redirect.allowlist` | No | empty | Comma-separated exact `host:numeric-port` redirect destinations. See the security rules below. |
| `redirect.max` | No | `3` | Maximum redirects within one login; revisiting a destination is rejected as a loop. |
| `padVarchar` | No | `false` | Enables fixed-width padded decoding for ordinary SQL `VARCHAR`/`NVARCHAR` snapshot and metadata results. Boolean values are validated strictly. Use only when required by the negotiated server tuple format. |
| `table.include.list` | No | all eligible tables | Comma-separated shell-style table patterns. |
| `tables` | No | | Alias for `table.include.list`; ignored when that option is set. |
| `table.exclude.list` | No | none | Comma-separated shell-style patterns excluded after inclusion filtering. |
| `decimal.variable.type` | No | `decimal(38,18)` | Per-table option. Target Spark type for variable-scale `DECIMAL(p)`/`NUMERIC(p)` columns: `string`, `double`, `integer` (truncated), or `decimal(p,s)`. Explicit `DECIMAL(p,s)` remains fixed-scale. See [Variable-scale decimals](#variable-scale-decimals). |
| `decimal.variable.column.type` | No | none | Per-table option. Comma-separated `column:type` overrides of `decimal.variable.type` for specific columns, e.g. `agt_no:decimal(9,0),bnk_acct_no:string`. |
| `snapshot.mode` | No | `incremental` | Per-table snapshot policy: `incremental`, `initial`, `initial_only`, `cdc_only`, `auto_snapshot`, or `recovery`. See [Snapshot modes](#snapshot-modes). |
| `snapshot.page.size` | No | `20000` | Rows per immutable staged page for CDC-capable tables; minimum `1`. Pages are read under one repeatable-read transaction and delivered through checkpointed Lakeflow microbatches. |
| `snapshot.filter` | No | none | Per-table Informix SQL predicate appended to snapshot `SELECT` statements, without the `WHERE` keyword. It filters blocking, incremental, append-only initial, and snapshot-only copies. CDC events after the snapshot are not filtered. Semicolons, SQL comments, control characters, and predicates longer than 8,192 characters are rejected. |
| `snapshot.read.timeout.seconds` | No | `300` | SQLI socket read timeout while fetching transactional snapshot pages; minimum `1`. |
| `cdc.read.timeout.seconds` | No | `60` | SQLI socket read timeout for CDC session setup and teardown (open/start/activate/end/close) when validating the initial CDC boundary. Raise it if `syscdcv1` teardown stalls under many concurrent CDC sessions. |
| `snapshot.max.rows` | No | `0` | Optional total row bound for a staged CDC snapshot. `0` disables the bound. Snapshot-only tables remain limited to 100,000 rows when this option is `0`. |
| `append.only.ingestion` | No | `auto` | Tri-state: `auto` (default), `true`, or `false`. `auto` appends only CDC-streamable tables **without** a primary key — the connector reports `ingestion_type=append` with a cursor and streams changes instead of repeatedly reading a bounded snapshot — and leaves keyed tables on their normal CDC path (so keyed tables follow the pipeline `scd_type`, default `SCD_TYPE_1`). For a keyless table `auto` is equivalent to `true`. `true` forces append for every capturable table, including keyed ones; `false` forces the normal CDC/snapshot path, opting a keyless table into bounded snapshot-only ingestion. Append semantics are correct only for genuinely insert-only data: updates append another row and deletes leave old rows present, so use `false` for mutable keyless tables. To append a **keyed** table also set `scd_type: APPEND_ONLY`; the connector cannot read that pipeline setting. Uncapturable types remain snapshot-only, and append-only tables have no delete channel. |
| `primary.keys` | No | *(catalog key)* | Per-table override of the table's primary key — a comma-separated list (or JSON array) of column names. When set, the connector treats the table as **keyed** on those columns even if it is physically keyless: it reports `ingestion_type=cdc_with_deletes`, reads incrementally with keyset-paginated snapshots and delete identification, and reports the keys so the pipeline uses them as the destination merge key too (no separate `primary_keys` needed). The columns must exist in the table. **You must guarantee they are unique per row** — a non-unique override corrupts upserts, delete identification, and snapshot pagination. Leave unset to use the catalog's primary key (or none). |
| `snapshot.max.bytes` | No | `0` | Optional estimated decoded Python byte bound for the complete staged snapshot. The default `0` disables the limit and byte accounting. Each encoded staged page also has a fixed 256 MiB safety bound. |
| `metadata.max.bytes` | No | `67108864` | Maximum estimated decoded Python bytes retained by an individual catalog query and by complete discovery. Set `0` to disable the limit and byte accounting. |
| `max.records.per.batch` | No | `10000` | Target maximum projected CDC rows; minimum `1`. A complete transaction may exceed it. |
| `cdc.timeout` | No | `5` | CDC idle-read timeout in seconds; minimum `1`. Zero is rejected because it can select an unbounded native wait. |
| `cdc.shared.state.wait.seconds` | No | `300` | Maximum time a delete reader waits for its table's upsert reader to publish initialization, trigger-boundary, or schema-transition state. |
| `cdc.max.records` | No | `256` | Soft native record target per CDC session; range `1`–`256`. Defaults to the source's own per-session ceiling because each poll pays a connection slot plus a full CDC session open/activate/close, so a smaller window amortises that fixed cost over less log progress. Once reached, records continue until every transaction already observed commits, rolls back, or Informix returns TIMEOUT. |
| `cdc.max.frame.bytes` | No | `16777216` | Maximum accepted native CDC frame size (16 MiB by default; minimum `16`). |
| `cdc.max.transaction.records` | No | `100000` | Maximum records buffered in an open transaction. Exceeding it fails without emitting uncommitted data. |
| `cdc.max.poll.records` | No | `200000` | Total decoded-record bound for one native CDC poll, including committed records interleaved with a long-running transaction. Reaching it ends the poll and returns the transactions that already committed, so the checkpoint advances and the next poll resumes from the oldest still-open transaction. It fails only when no transaction completed, since then there is no progress to checkpoint. |
| `cdc.max.poll.bytes` | No | `0` | Optional estimated retained-byte bound per CDC poll, counting native frames plus recursively sized decoded Python values. Reaching it truncates the poll on the same terms as `cdc.max.poll.records`. The default `0` disables the bound and byte accounting. |
| `cdc.read.bytes` | No | `32000` | Bytes requested in each SmartLOB CDC read. |

Because per-table options are supported, configure the Unity Catalog connection with this exact `externalOptionsAllowList`:

```text
qualified_source_table,decimal.variable.type,decimal.variable.column.type,snapshot.mode,snapshot.page.size,snapshot.filter,snapshot.max.rows,snapshot.max.bytes,append.only.ingestion,max.records.per.batch,cdc.timeout,cdc.max.records
```

Create the connection from the Lakeflow Community Connector flow on the **Add Data** page, with the Databricks CLI, or with the Databricks SDK for Python. The Unity Catalog connection type must be `COMMUNITY`, and `sourceName` must be `informix`.

### Create a connection with the Databricks CLI

The example uses `jq` so the password comes from an environment variable instead of being written literally into shell history:

```bash
export INFORMIX_PASSWORD='<secret>'
# Password for informix_state_user, the Lakebase role holding connector state.
# Choose it once and supply the same value on every update.
export LAKEBASE_PASSWORD='<secret>'

databricks connections create --json "$(jq -n \
  --arg password "$INFORMIX_PASSWORD" \
  --arg lakebase_password "$LAKEBASE_PASSWORD" \
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
      "snapshot.staging.location": "/Volumes/main/informix_cdc/staging",
      "lakebase.password": $lakebase_password,
      externalOptionsAllowList: "qualified_source_table,decimal.variable.type,decimal.variable.column.type,snapshot.mode,snapshot.page.size,snapshot.filter,snapshot.max.rows,snapshot.max.bytes,append.only.ingestion,max.records.per.batch,cdc.timeout,cdc.max.records,primary.keys"
    }
  }')"

unset INFORMIX_PASSWORD LAKEBASE_PASSWORD
```

Use `--profile <profile-name>` when the CLI should use a non-default Databricks profile. Confirm the result with:

```bash
databricks connections get informix_sales
```

### Update a connection with the Databricks CLI

Pass the complete desired `options` map when updating a connection; do not assume omitted options will be preserved. This TLS example again uses `jq` to keep the password out of the literal command:

```bash
export INFORMIX_PASSWORD='<secret>'
export LAKEBASE_PASSWORD='<secret>'
export DATABRICKS_PROFILE='<profile-name>'

databricks connections update informix_sales --json "$(jq -n \
  --arg password "$INFORMIX_PASSWORD" \
  --arg lakebase_password "$LAKEBASE_PASSWORD" \
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
      "snapshot.staging.location": "/Volumes/main/informix_cdc/staging",
      "lakebase.password": $lakebase_password,
      externalOptionsAllowList: "qualified_source_table,decimal.variable.type,decimal.variable.column.type,snapshot.mode,snapshot.page.size,snapshot.filter,snapshot.max.rows,snapshot.max.bytes,append.only.ingestion,max.records.per.batch,cdc.timeout,cdc.max.records,primary.keys"
    }
  }')" \
  --profile "$DATABRICKS_PROFILE"

unset INFORMIX_PASSWORD LAKEBASE_PASSWORD DATABRICKS_PROFILE
```

The CA path must be readable by the serverless pipeline. Use the TLS listener's DNS hostname rather than its IP address when certificate hostname verification requires it.

### Create a connection with the Python API

Install or upgrade the Databricks SDK, configure its normal authentication environment, and call the Unity Catalog Connections API:

```python
import os
from types import SimpleNamespace

from databricks.sdk import WorkspaceClient


w = WorkspaceClient()  # Uses the standard Databricks SDK authentication chain.

# `connection_type` must be the string "COMMUNITY". The SDK's ConnectionType
# enum does not include it, so pass a small object whose `.value` is the string:
# `w.connections.create` serializes the argument as `connection_type.value`, and
# a bare string (no `.value`) or `ConnectionType("COMMUNITY")` (not an enum
# member) both fail.
connection = w.connections.create(
    name="informix_sales",
    connection_type=SimpleNamespace(value="COMMUNITY"),
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
        "snapshot.staging.location": "/Volumes/main/informix_cdc/staging",
        # Password for informix_state_user, the Lakebase role holding connector
        # state. Required; supply the same value on every update.
        "lakebase.password": os.environ["LAKEBASE_PASSWORD"],
        "externalOptionsAllowList": (
            "qualified_source_table,decimal.variable.type,decimal.variable.column.type,"
            "snapshot.mode,snapshot.page.size,snapshot.filter,snapshot.max.rows,snapshot.max.bytes,"
            "append.only.ingestion,"
            "max.records.per.batch,cdc.timeout,cdc.max.records,primary.keys"
        ),
    },
)

print(connection.full_name or connection.name)
```

To update an existing connection, call `w.connections.update` with the complete
desired `options` map. As with the CLI, an update replaces the options wholesale;
do not assume omitted options are preserved, so include every option the
connection needs — including the required `lakebase.password`:

```python
import os

from databricks.sdk import WorkspaceClient


w = WorkspaceClient()  # Uses the standard Databricks SDK authentication chain.

connection = w.connections.update(
    name="informix_sales",
    options={
        "sourceName": "informix",
        "hostname": "informix.example.internal",
        "port": "9089",
        "database": "sales",
        "user": "cdc_service",
        "password": os.environ["INFORMIX_PASSWORD"],
        "server": "informix_prod",
        "encrypt": "true",
        "ssl.ca.file": "/Volumes/catalog/schema/artifacts/informix-ca.pem",
        "snapshot.staging.location": "/Volumes/main/informix_cdc/staging",
        # Password for informix_state_user, the Lakebase role holding connector
        # state. Required; supply the same value on every update.
        "lakebase.password": os.environ["LAKEBASE_PASSWORD"],
        "externalOptionsAllowList": (
            "qualified_source_table,decimal.variable.type,decimal.variable.column.type,"
            "snapshot.mode,snapshot.page.size,snapshot.filter,snapshot.max.rows,snapshot.max.bytes,"
            "append.only.ingestion,"
            "max.records.per.batch,cdc.timeout,cdc.max.records,primary.keys"
        ),
    },
)

print(connection.full_name or connection.name)
```

The `databricks-sdk` `ConnectionType` enum does not include `COMMUNITY`, so the
typed enum cannot express it. The `SimpleNamespace(value="COMMUNITY")` shim above
supplies the `.value` the SDK serializes without depending on the enum. The
Databricks CLI (`databricks connections create`) takes the string directly and
needs no such workaround.

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

Supply `user`, `password`, and the required `lakebase.password` separately through secret-backed connection properties.

### Authentication, redirects, and TLS

Normal Informix ASC username/password authentication and non-interactive PAM authentication are implemented. The normal ASC protocol carries the password directly. Verified TLS is therefore the default and strongly recommended. Setting `encrypt=false` explicitly opts into plaintext TCP; credentials, PAM exchanges, queries, and row data are then unencrypted and observable or modifiable by the network.

When enabled, TLS always verifies the hostname. It uses Python's system CA store by default, or a PEM CA bundle supplied with `ssl.ca.file`. Insecure/skip-verification mode is not supported. The CA file must be available to each serverless worker; do not rely on an ephemeral local path that is not distributed with the pipeline.

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

Redirects are an explicit opt-in because the destination is supplied by the server. With `redirect.enabled=true`, every target must exactly match a `host:numeric-port` entry in `redirect.allowlist`; wildcards, service names, and omitted ports are rejected. Every address returned for an allowed hostname is validated once and pinned for that connection attempt. The connector tries the validated addresses sequentially only after transport-level failures. If any address is private, loopback, link-local, multicast, or otherwise non-public, its exact `IP:numeric-port` must also be allow-listed. These checks prevent a permitted hostname from becoming an unrestricted network pivot.

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

GSS/Kerberos, private-server authentication, and automatic server-name or locale discovery remain unsupported. PAM has live integration coverage. Redirect parsing, multi-address validation and transport failover, reconnect state, limits, and loop detection have deterministic protocol-test coverage, but the current redirect fixture does not provide a TLS listener and therefore is not live redirect coverage. Serverless Lakeflow Connect execution over an Informix TLS listener has been validated separately.

The fixture at `/Users/leon.eller/work/dev/informix-cdc` live-tests PAM against an Informix listener at `localhost:9090`. Its deterministic redirect responder on `localhost:9191` is plaintext and can be exercised only with the explicit `encrypt=false` opt-in; production redirect validation should use TLS or an independently trusted private network, with every destination explicitly allow-listed. Client-side `sqlhosts` failover is not redirect coverage. Fixture credentials are disposable test data and are intentionally not repeated here.

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
| Live-validated end to end | `SMALLINT`, `INTEGER`/`INT`, `SERIAL`, `INT8`, `SERIAL8`, `BIGINT`, `BIGSERIAL`, `FLOAT`/`DOUBLE`, `REAL`/`SMALLFLOAT`, `DECIMAL`/`NUMERIC`, `MONEY`, `DATE`, supported `DATETIME` qualifiers, `BOOLEAN`, `CHAR`, `VARCHAR`/`NVARCHAR`, `LVARCHAR` |
| Rejected before ingestion | `BYTE`, `TEXT`, `BLOB`, `CLOB`, `INTERVAL`, `NCHAR`, complex and opaque types |
| Excluded from speculative decoding | Unknown catalog types, UDT and complex types such as `ROW`, `SET`, `LIST`, and `MULTISET` |

`INT8` and `SERIAL8` use Informix's complete ten-byte signed-magnitude CDC representation. `LVARCHAR` supports its variable-width ordinary SQLI snapshot envelope and native CDC representation. A `DATETIME` qualifier is CDC-capable when its start and end fields are supported by the native decoder. Values containing `YEAR` through at least `DAY` are exposed as timezone-free Spark timestamps. `HOUR`-based values are SQL TIME strings such as `13:03:36.93100`; other partial values remain qualifier-aware strings and never acquire the worker's current date.

### Variable-scale decimals

Informix encodes an omitted scale in `DECIMAL(p)`/`NUMERIC(p)` as catalog scale byte `255`. This is **decimal floating point**, not scale zero: `p` is the number of *significant digits* (1–32) and the exponent floats, so the magnitude is not bounded by `p`. For example a `DECIMAL(5)` column can hold `114250` (the five significant digits `11425` scaled by `10^1`) or `0.00123`.

Because the exponent floats, no single fixed `DecimalType(precision, scale)` can represent the full range losslessly. The per-table option `decimal.variable.type` chooses the target Spark type:

| `decimal.variable.type` | Spark type | Notes |
| --- | --- | --- |
| `decimal(38,18)` (default) | `DecimalType(38,18)` | 20 integer digits, 18 fractional. Values whose integer part exceeds 20 digits raise a clear error. |
| `decimal(p,s)` | `DecimalType(p,s)` | Any `1<=p<=38`, `0<=s<=p`. Overflowing values raise an error. |
| `string` | `StringType` | Lossless canonical string for any magnitude and scale. |
| `double` | `DoubleType` | Native floating semantics; loses precision beyond ~15–17 significant digits. |
| `integer` | `LongType` | Truncated toward zero; values outside the signed 64-bit range raise an error. |

Use `decimal.variable.column.type` to override the global choice per column, e.g. `agt_no:decimal(9,0),bnk_acct_no:string`. Explicit fixed-scale `DECIMAL(p,s)` columns are unaffected and always map to `DecimalType(p,s)`.

Set `scalar.types.live.validation=true` and `scalar.types.test.table=owner.table` in an opt-in live test configuration to validate snapshot and CDC decoding against a populated, dedicated mutable table containing `INT8`, `SERIAL8`, `DATETIME`, `LVARCHAR`, and `BOOLEAN` columns. Snapshot validation opens separate `padVarchar=false` and `padVarchar=true` sessions so mixed padded VARCHAR and LVARCHAR/BOOLEAN envelope behavior is exercised in both negotiated tuple modes. Provide `scalar.types.expected.snapshot` and `scalar.types.expected.cdc` as JSON objects containing exact non-null wire boundaries: a signed INT8 extreme, a positive generated SERIAL8 value, an LVARCHAR occupying its catalog maximum in the configured `CLIENT_LOCALE` encoding, a DATETIME preserving its declared fractional precision, and a Boolean value. INT8 exercises the shared signed-magnitude extreme without advancing the SERIAL8 generator to exhaustion. Provide corresponding null-sentinel cases in `scalar.types.expected.null.snapshot` and `scalar.types.expected.null.cdc`; each must include every required column and set every nullable scalar column to JSON `null`. Configure committed mutations with `scalar.types.mutation.sql` and `scalar.types.null.mutation.sql`, plus one idempotent `scalar.types.cleanup.sql` statement that removes both mutations. The test commits every statement explicitly in ANSI and non-ANSI databases and preserves all primary, rollback, cleanup, and close failures.

This regression passed against Informix 15.0.FC0DE in an ANSI-mode database over
the verified TLS SQLI listener, covering padded and unpadded snapshots, exact
boundary and null values, committed full-row CDC updates, and cleanup.

A reproducible fixture uses one non-scalar marker so CDC can emit unchanged
boundary/null scalar values through full-row logging:

```sql
CREATE TABLE scalar_types_live (
  id INTEGER NOT NULL PRIMARY KEY,
  int8_value INT8,
  serial8_value SERIAL8 NOT NULL,
  datetime_value DATETIME YEAR TO FRACTION(5),
  lvarchar_value LVARCHAR(16),
  boolean_value BOOLEAN,
  marker INTEGER NOT NULL
);

INSERT INTO scalar_types_live
  (id, int8_value, serial8_value, datetime_value, lvarchar_value, boolean_value, marker)
VALUES
  (1, 9223372036854775807, 0,
   DATETIME(2026-07-21 12:34:56.12345) YEAR TO FRACTION(5),
   'abcdefghijklmnop', 't', 0);
INSERT INTO scalar_types_live
  (id, int8_value, serial8_value, datetime_value, lvarchar_value, boolean_value, marker)
VALUES (2, NULL, 0, NULL, NULL, NULL, 0);
```

Add these test-only entries to `CONNECTOR_TEST_CONFIG_JSON` or the JSON file
referenced by `CONNECTOR_TEST_CONFIG_PATH`; expected objects are JSON-encoded
strings because connection options are strings:

```json
{
  "scalar.types.live.validation": "true",
  "scalar.types.test.table": "informix.scalar_types_live",
  "scalar.types.expected.snapshot": "{\"int8_value\":9223372036854775807,\"serial8_value\":1,\"datetime_value\":\"2026-07-21T12:34:56.12345\",\"lvarchar_value\":\"abcdefghijklmnop\",\"boolean_value\":true}",
  "scalar.types.expected.null.snapshot": "{\"int8_value\":null,\"serial8_value\":2,\"datetime_value\":null,\"lvarchar_value\":null,\"boolean_value\":null}",
  "scalar.types.mutation.sql": "UPDATE scalar_types_live SET marker=1 WHERE id=1",
  "scalar.types.null.mutation.sql": "UPDATE scalar_types_live SET marker=1 WHERE id=2",
  "scalar.types.cleanup.sql": "UPDATE scalar_types_live SET marker=0 WHERE id IN (1,2)",
  "scalar.types.expected.cdc": "{\"int8_value\":9223372036854775807,\"serial8_value\":1,\"datetime_value\":\"2026-07-21T12:34:56.12345\",\"lvarchar_value\":\"abcdefghijklmnop\",\"boolean_value\":true}",
  "scalar.types.expected.null.cdc": "{\"int8_value\":null,\"serial8_value\":2,\"datetime_value\":null,\"lvarchar_value\":null,\"boolean_value\":null}",
  "scalar.types.insert.smoke.sql": "INSERT INTO scalar_types_live (id,int8_value,serial8_value,datetime_value,lvarchar_value,boolean_value,marker) VALUES (9,NULL,42,NULL,NULL,NULL,0)",
  "scalar.types.insert.smoke.cleanup.sql": "DELETE FROM scalar_types_live WHERE id=9"
}
```

Informix's native capture-column API and ordinary SQLI snapshot protocol do not yet provide end-to-end materialization for the rejected types. The connector fails during metadata discovery with the exact columns involved instead of advertising a snapshot fallback that later fails or silently returning placeholders. Unknown catalog type codes also fail discovery. Decimal CDC requires valid precision and scale metadata within Spark's 38-digit decimal limit.

## Snapshot, CDC, and deletes

For a CDC-capable table, the connector defaults to an `incremental` snapshot: it records the prepared CDC boundary, publishes it so the delete channel begins immediately, and then copies existing rows in primary-key chunks interleaved with the live change stream (see [Snapshot modes](#snapshot-modes)). Each chunk is read in its own short repeatable-read transaction and stamped with the LSN captured just before that chunk, so no long-lived read view is held over the whole table.

Selecting `snapshot.mode=initial` uses the earlier blocking strategy instead: the SQLI bridge establishes one repeatable-read transaction, captures a single snapshot LSN, and reads the bounded initial snapshot in complete primary-key order before commit. Non-ANSI databases use explicit `BEGIN WORK`; ANSI databases use their implicit transaction after committing the catalog-mode probe. Snapshot rows use the transactional snapshot LSN, and both channels advance directly to that same boundary because the snapshot already represents all committed state visible there. The earlier prepared LSN ensures full-row logging and a valid capture window while the snapshot is established; it is not replayed after the completed snapshot.

To validate the ANSI transaction sequence against a live ANSI-mode database, supply the standard `CONNECTOR_TEST_CONFIG_JSON` or `CONNECTOR_TEST_CONFIG_PATH` configuration with `ansi.live.validation=true` and `ansi.test.table=owner.table`, then run `ansi_live_test.py`. The selected table must have a primary key and no more than 10,000 rows.

Only complete committed transactions are emitted. Inserts and update after-images go to the data channel; deletes go to an independently checkpointed delete channel. Delete output contains the primary-key fields and connector metadata. A primary-key update emits the new row and deletes the old key. Rollbacks are suppressed. Transactions are never split: `cdc.max.records` and `max.records.per.batch` are soft targets, and a transaction already observed is read through commit/rollback unless Informix returns TIMEOUT. CDC metadata and timeout control frames do not consume the `cdc.max.records` budget. Record and estimated retained-memory bounds limit each poll. Replay is transaction-atomic: another transaction's checkpoint never removes earlier records from a newly committed interleaved transaction. Continuous runs follow new commits without a mode-specific option.

The framework does not pass snapshot offsets to independently instantiated delete streams. The connector therefore coordinates automatically through immutable state records in Lakebase. Only the upsert reader may enable full-row logging and publish a boundary; its delete reader waits and consumes that boundary. Initialization, snapshots, trigger boundaries, schema nodes, and schema transitions are each elected by `INSERT ... ON CONFLICT DO NOTHING RETURNING`: exactly one writer gets a row back and every loser re-reads and validates the winner, which callers depend on because a record is shared truth for its key. A record cannot be partially visible, so there is no incomplete state to quarantine and no candidate to collect. Every committed record carries an immutable-format version and record type. Corrupt, mismatched, or inaccessible committed state fails closed.

Regenerate the deployable file with `bash src/databricks/labs/community_connector/sources/informix/generate_source.sh`. Informix-owned code wraps the generated reader base and installs the AvailableNow callback at class creation, so this output is identical to the repository's canonical merge command and needs no post-generation patch. The source-local tests use `*_test.py` names so the standard merger excludes them.

Delivery is at least once. A failure after rows are returned but before Lakeflow commits its checkpoint can replay them. `TRUNCATE` cannot be represented by keyed Lakeflow deletes and fails explicitly. Snapshot-only tables are fully reread and fail when they exceed `snapshot.max.rows`.

The connector adds `_informix_change_lsn`, `_informix_commit_lsn`, `_informix_tx_id`, and `_informix_op` to rows. Native LSN fields are decoded across their complete unsigned 64-bit wire domain. The two LSN columns are fixed-width, zero-padded 20-digit decimal strings so Spark string ordering is identical to numeric LSN ordering. Native signed int32 transaction-ID bit patterns are normalized to `0..4294967295`, ensuring one stable identifier after the high bit is set. `_informix_change_lsn` is the incremental cursor; operations are `r` (snapshot), `c` (insert), `u` (update), and `d` (delete). Targets created with an older connector that emitted unpadded LSN strings require a full refresh before using this version.

### Checkpoints and log retention

Trigger records use pipeline-scope-specific subtrees, so recovery never reads
another pipeline's trigger records.

On Databricks, the registration scope is
`<spark.pipelines.pipelineId>_@_<spark.pipelines.updateId>`. Both UUIDs are
captured on the driver before reader serialization. If either Spark property is
unavailable, the connector falls back to a random 32-character secret scope.

During a blocking (`initial`) snapshot, Lakeflow checkpoints offset version 10, the snapshot LSN, immutable staged-page index, last primary-key values, source-schema fingerprint, generation-specific schema node ID, and update scope. During an `incremental` snapshot the offset is already a streaming offset that additionally carries the incremental chunk cursor (last and maximum primary-key bounds and the current chunk LSN); the incremental block is dropped once every chunk within the captured key range has been emitted. A completed consistent snapshot publishes its fresh resume LSN for both independently instantiated channels. Streaming offsets checkpoint the `commit_lsn`, `change_lsn`, oldest required `begin_lsn`, fingerprint, schema node ID, current update scope, triggered-update generation, and trigger high-water LSN. This makes restart offsets self-contained: resumed readers coordinate new decisions under the current update scope rather than dereferencing the previous update. Global immutable schema nodes are shared and are not scoped to an update. Triggered readers share one immutable per-table high-water LSN under the current update scope even when their predecessor generations differ. Records are elected rather than assembled, so a publisher that dies mid-write commits nothing and there is no candidate state to sweep, quarantine, or reclaim.

Schema-node IDs are deterministic hashes of the physical Informix table identity
and schema fingerprint. Full refreshes with an unchanged table incarnation and
layout therefore reuse the existing global schema node; their new snapshot and
initial LSNs remain in update-scoped initialization and snapshot records.

Each triggered pipeline update publishes one immutable high-water boundary per
physical table under its current pipeline/update scope. Upsert and delete readers
use that boundary even when cancellation or retry left their predecessor
generations different, then independently checkpoint the shared generation.

An idle timeout returns no rows and leaves the checkpoint unchanged. Incomplete or open transactions do not advance it. If the restart LSN predates the minimum retained logical log in `sysmaster:syslogs`, continuation fails and the table must be resnapshotted. Every CDC session validates its initial METADATA frame against a fresh catalog layout before decoding later records; another METADATA frame in that session fails immediately and requires a full refresh.

### Additive schema evolution without full refresh

A normal pipeline restart can evolve an existing CDC table when nullable,
CDC-supported columns are appended at the end of the Informix table. Existing
columns must retain their names, order, types, nullability, widths, precision,
and scale, and the primary key must remain unchanged. Drops, renames, reorders,
type changes, non-nullable additions, unsupported types, and primary-key changes
still require a full refresh.

Informix rejects `ALTER TABLE` while full-row logging is enabled. Stop every
pipeline using this connection, then quiesce source writes before disabling
logging and keep them quiesced until logging is re-enabled. Apply the additive
DDL, immediately re-enable logging, and then writes may resume even before the
pipeline restarts. Restart normally—do not request a full refresh. For example:

```sql
EXECUTE FUNCTION syscdcv1:cdc_set_fullrowlogging(
  'testdb:informix.members', 0
);
ALTER TABLE informix.members ADD new_nullable_column VARCHAR(64);
EXECUTE FUNCTION syscdcv1:cdc_set_fullrowlogging(
  'testdb:informix.members', 1
);
```

Shared state stores predecessor-linked schema generations, the Informix catalog
table ID for incarnation detection, and one observation LSN per additive step. Lagging pipelines advance one recorded schema
at a time instead of skipping intermediate layouts. An incompatible full
refresh creates a new root generation while retaining older descriptors for
other pipelines. Both upsert and delete readers register the expanded projection
and replay directly from their retained checkpoints. Informix emits `NULL` for
the appended columns on pre-DDL records and their logged values on post-DDL
records. Lakeflow retains its checkpoints and Delta adds the new column without
introducing a restart-time LSN boundary or data-loss window.

Backfilling existing rows only propagates when it produces logged row changes
that CDC can capture. Populating the column outside the change stream does not
reach the destination: an `ALTER TABLE ... ADD ... DEFAULT <value>` sets every
existing row as part of the DDL (not a logged row change), and any `UPDATE`
applied while full-row logging is disabled — including during the
disable-logging window of the DDL sequence above — is likewise not captured.
In both cases the destination keeps `NULL` for those rows even though the source
is fully populated, because the connector only ever received the schema change,
not the values. To backfill, re-enable full-row logging first, then issue an
`UPDATE` that touches the affected rows (for example
`UPDATE informix.members SET new_nullable_column = new_nullable_column`, which
CDC captures as full-row updates and applies), or run a Lakeflow full refresh of
that table to re-snapshot every row with the column already populated. A full
refresh also clears the transient upsert/delete channel skew that a burst of
primary-key updates can leave, where the destination briefly retains an old key
(extra row) before the new key's insert and the old key's delete both land. Run the connector once after upgrading from a version without
schema history and before applying DDL so the current checkpoint descriptor is
seeded in shared state.

The one-MiB limit applies to each immutable state record, not to aggregate
history. Committed schema generations are retained because automatically
deleting one could break a paused pipeline, so schema history grows with the
number of additive steps a connection has taken. If a required historical schema
node is removed, the affected pipeline fails closed and requires a full refresh.

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
            "decimal.variable.type": "decimal(38,18)",
            "snapshot.mode": "initial",
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

Supported source-specific table options are `qualified_source_table`, `decimal.variable.type`, `decimal.variable.column.type`, `snapshot.mode`, `snapshot.page.size`, `snapshot.filter`, `snapshot.max.rows`, `snapshot.max.bytes`, `max.records.per.batch`, `cdc.timeout`, and `cdc.max.records`. `qualified_source_table` maps the pipeline's logical table name to an Informix `owner.table` name. Standard destination, SCD, key, sequence, and clustering options remain available.

### Snapshot modes

`snapshot.mode` is a per-table option. It controls what happens before or while
the table enters CDC; it does not override Lakeflow's durable checkpoint. The
default is `incremental`. It may also be set once at connection scope as the
default for every table, and overridden per table.

For an append-only table without a primary key, an unset mode defaults to
`cdc_only`. Such a table supports `cdc_only` or `initial`: `initial` streams one
unordered forward-only cursor under repeatable read into durable bounded pages,
then continues CDC from the snapshot LSN. Other modes require key-based snapshot
progress and are rejected.

| Mode | Behavior |
|---|---|
| `incremental` | Default. Begin CDC immediately at the captured boundary and copy existing rows in primary-key chunks interleaved with the change stream, one chunk per microbatch. No long-lived repeatable-read transaction is held over the whole table; each chunk is read in its own short repeatable-read transaction and its `r` rows are stamped with the LSN captured just before that chunk. Correctness for keyed (`apply_changes`) targets follows from LSN sequencing — any concurrent or later change to a chunked key commits at a higher `_informix_change_lsn` and supersedes the snapshot row in Auto CDC's sequence-merge, so no in-memory deduplication window is required. Rows inserted beyond the key range captured at snapshot start are left to the change stream. Tables whose primary key includes one or more `DATETIME` columns (alone or combined with non-`DATETIME` key columns in a composite key) are chunked by a fixed-width string cast of each `DATETIME` key column when `snapshot.incremental.datetime.as.string` is true (the default). Any contiguous `DATETIME` qualifier is supported, including `HOUR`-anchored time-of-day ranges. Set the option false to force the blocking `initial` snapshot for such tables. See [Incremental snapshot and append-only targets](#incremental-snapshot-and-append-only-targets). |
| `initial` | If the table has no checkpoint, take a single transactionally consistent (blocking) snapshot under one repeatable-read transaction and continue from its captured Informix LSN. If a checkpoint exists, resume CDC without another snapshot. Use this when you require a single point-in-time snapshot LSN for the whole table rather than per-chunk boundaries. |
| `initial_only` | Take the initial snapshot, checkpoint its boundary, and emit no later CDC records. Both upsert and delete readers then remain exhausted. Snapshot continuation pages still finish normally after a restart. |
| `cdc_only` | Do not copy existing rows. Enable full-row logging, record the current schema and LSN, and stream only transactions after that boundary. It requires CDC-supported column types; keyless tables use append-only ingestion. |
| `auto_snapshot` | Behave like `incremental`, but automatically begin a new PK-chunked snapshot if the checkpoint's restart LSN has fallen out of the retained Informix logical logs. CDC begins immediately at the new deterministic boundary while existing rows are copied in chunks; the independently checkpointed delete reader adopts the same boundary. Other checkpoint or schema errors still fail closed. |
| `recovery` | Rebuild missing immutable schema-history state from an existing stream checkpoint, then resume CDC without copying table data. It is accepted only when the current source schema fingerprint exactly matches the checkpoint and the checkpoint LSN is still retained. If either condition is false, run a full refresh. Do not use this mode to accommodate a real schema change. |

For example, to start CDC at the current source position without ingesting
existing rows:

```json
{
  "source_table": "members",
  "table_configuration": {
    "qualified_source_table": "informix.members",
    "snapshot.mode": "cdc_only"
  }
}
```

Changing `snapshot.mode` does not erase a checkpoint. Use a pipeline full
refresh when deliberately replacing existing destination contents or when
forcing a fresh snapshot of a table that already has a checkpoint.

#### Incremental snapshot and append-only targets

The default `incremental` mode delegates per-key deduplication to the target
flow. For `SCD_TYPE_1` and `SCD_TYPE_2` targets (Lakeflow Auto CDC), rows are
merged by key and ordered by `sequence_by` (`_informix_change_lsn` by default),
so a snapshot `r` row is always superseded by any later change to the same key
and the materialized state is exact.

For `APPEND_ONLY` targets there is no key merge: every emitted row is retained.
Because each chunk is read in its own repeatable-read transaction, its `r` rows
are consistent as-of that chunk's LSN, and no row is ever lost or fabricated.
The only artifact is that a key changed just before its chunk boundary can
appear twice — once as the change event and once as an `r` row carrying the same
value at a nearby LSN. An append-only consumer that reconstructs state by
ordering on `_informix_change_lsn` (the documented contract) still resolves the
correct value. If you require an append changelog with no duplicated snapshot
rows, use `snapshot.mode=initial` for that table.

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

### Lakebase-backed shared state

Connection capacity slots, backlog hints, the capacity limit, and every immutable
per-table state record (schema nodes, initialization, schemas, trigger scopes,
snapshot manifests) live in a Lakebase Autoscaling Postgres endpoint. Snapshot
page *payloads* remain on the Volume named by `snapshot.staging.location`, because
they are large gzip blobs that suit object storage; nothing else uses a Volume.

Coordination needs one primitive: an atomic compare-and-swap. Postgres provides it
directly, so claiming a slot is a single `UPDATE ... FOR UPDATE SKIP LOCKED` and
electing a record is a single `INSERT ... ON CONFLICT DO NOTHING RETURNING`.

Provisioning is self-contained. The first reader that needs state creates the
project, branch, compute endpoint, and schema, keyed by a hash of `hostname`,
`port`, and `server`; every later reader in every later update finds them.

There are two separate identities, with different lifetimes and different rights:

| | Role | Credential | Used for |
| --- | --- | --- | --- |
| **State** | `informix_state_user` (fixed) | `lakebase.password` (**required**) | every ordinary read and write of state |
| **Admin** | `lakebase.admin_user` | `lakebase.admin_password`, or an OAuth credential when unset | creating the state role and resetting its password |

The state role is long-lived and needs no rights beyond its own tables, so nothing
in a running stream depends on a credential that can expire. The admin role needs
`CREATEROLE` and is used only at startup. Keeping them apart is the point: the
credential handed to every reader process is not one that can create roles.

`lakebase.password` is therefore required — without it there is no long-lived
credential for state access at all, and a missing value fails the update while it
is being set up rather than on the first microbatch that needs state. The role
name is deliberately not configurable: every pipeline sharing a Lakebase endpoint
must agree on which role owns the state tables, so naming it per connection could
only misconfigure that.

Startup tries the state login first and only falls back to the admin identity if
it fails. `lakebase.admin_user` defaults to the pipeline's own identity, which the
connector resolves for itself, and `lakebase.admin_password` is only needed where
an OAuth credential cannot be minted.

The configured password is the source of truth: a role whose password does not
match it is repaired to match, which also invalidates the previous password. The
role is never dropped — it owns the state tables, so dropping it would discard the
shared CDC state and force every table to resnapshot. (Postgres refuses the drop
outright for that reason once the tables exist.)

Provisioning needs a workspace credential, and only the pipeline's **driver** has
one. The reader itself runs in processes with no ambient identity at all — no auth
environment variables, no config file, no usable `dbutils` — so the connector
captures the credential when the pipeline module is loaded and carries it to where
provisioning happens. Nothing needs configuring for this; `lakebase.workspace.host`
and `lakebase.workspace.token` exist only as an override for running outside a
Databricks runtime.

Defaults, and the measurements behind them:

| Setting | Default | Rationale |
| --- | --- | --- |
| `lakebase.autoscale.min.cu` | `0.5` | smallest value the API accepts |
| `lakebase.autoscale.max.cu` | `2` | steady-state query cost is ~11 ms, so this is ample headroom |
| `lakebase.suspend.timeout.seconds` | `60` | resume from suspended costs ~3.3 s, and a streaming pipeline never idles that long |

Capacity and suspend settings apply **only when the connector creates the
project**. That is a constraint of the API rather than a choice: it accepts
`suspend_timeout_duration` solely through a project's
`default_endpoint_settings` and does not apply it retroactively to an endpoint
that already exists. Resizing a live endpoint is therefore an operator action
(`databricks postgres update-endpoint`), not something a reader does implicitly.

Requirements and caveats:

- `psycopg2` (preinstalled on Databricks runtimes) or `psycopg`, and permission
  to create Lakebase projects.
- `snapshot.staging.location` is now required and has no default, because the
  shared-state location it used to fall back on no longer exists.
- `lakebase.password` is required. Choose a value once and supply the same one on
  every update; changing it resets the role's password rather than failing.
- **This is a breaking change for an existing deployment.** Durable schema nodes
  previously held on the Volume are not migrated, and `cdc.shared.state.location`
  is no longer accepted. Update the connection and run a full refresh.
- The endpoint bills for compute whenever it is awake, so a pipeline that never
  idles never scales to zero.

## Operational guidance

- Start with one small table and verify snapshot, insert, update, delete, restart, rollback, idle timeout, and retention-expiry behavior against the target Informix version before broader use.
- Disposable Informix 15 testing has validated normal-password and PAM authentication, queries, discovery, snapshots, and committed INSERT/UPDATE/DELETE transactions through `syscdcv1`. Redirect security behavior has deterministic protocol coverage but still requires a TLS-capable live redirect fixture. A serverless Lakeflow Connect pipeline has validated TLS, multi-table snapshots, checkpointed CDC flows, deletes, and SCD Type 2 materialization. Validate TLS trust, locale behavior, permissions, schemas, data-type boundaries, restart behavior, retention, and IDS/HDR-emitted redirects against the target environment.
- Authentication errors commonly indicate a wrong `server`/locale, unsupported authentication mode or redirect, an untrusted/mismatched TLS certificate, or insufficient `syscdcv1` privileges.
- Schema changes that alter captured column layout are not guaranteed to be safe during active capture. Restart and validate at a clean LSN boundary.
- Ensure source log retention covers downtime and the oldest checkpoint. Otherwise resnapshotting is required.

## References

- IBM Informix Change Data Capture API documentation for Informix-side CDC concepts and prerequisites (the `syscdcv1` routines)
- Informix CDC installation script: `$INFORMIXDIR/etc/syscdcv1.sql`
