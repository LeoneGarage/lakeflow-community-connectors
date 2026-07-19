# Lakeflow Informix Community Connector

This connector discovers Informix user tables, takes an initial snapshot, and then captures committed changes through the Informix CDC API. The SQLI client, SmartLOB framing, CDC decoding, transaction handling, and checkpoint recovery are implemented in Python. It requires no JVM, JDBC driver, Debezium runtime, JAR staging, or native Informix client and is intended to run on serverless Lakeflow Connect compute.

The pure-Python protocol implementation has completed authentication, query, discovery, snapshot, and end-to-end transactional INSERT/UPDATE/DELETE CDC validation against a disposable Informix 15 fixture. A run in an actual serverless Lakeflow Connect pipeline is still pending, so treat the connector as pre-production and validate it against your Informix environment before broader use.

## Prerequisites

- An Informix SQLI endpoint reachable from serverless Lakeflow Connect compute. Configure firewalls, private connectivity, and DNS as appropriate. Protocol redirects are disabled by default and require the explicit controls described below.
- TLS enabled on the SQLI endpoint with a certificate trusted by Python's system CA store. The certificate must match `hostname`.
- Transaction logging enabled for the source database. Retained logical logs must extend back to the oldest connector checkpoint.
- The Informix CDC API installed by an administrator. Run `$INFORMIXDIR/etc/syscdcv1.sql` (commonly with `dbaccess sysadmin`) to create the `syscdcv1` database and `cdc_*` routines.
- A normal username/password account that can read source catalog metadata and selected tables, use the required `syscdcv1` routines, and enable full-row logging. The validated reference setup grants DBA-level access to `syscdcv1`.
- The exact `INFORMIXSERVER`, database locale, and client locale values. The connector does not discover these values.

The connector enables full-row logging for captured tables and leaves it enabled when a finite poll ends, avoiding capture gaps between polls. Ensure this operational change is acceptable on the source system.

## Setup

### Connection parameters

| Option | Required | Default | Description |
|---|---:|---:|---|
| `hostname` | Yes | | Informix SQLI hostname or IP address. It is also used for TLS hostname verification. |
| `database` | Yes | | Database to discover, snapshot, and capture. |
| `user` | Yes | | Informix normal-auth user with metadata, snapshot, and CDC privileges. |
| `password` | Yes | | Password for `user`; store it as a secret. |
| `server` | Yes | | Exact `INFORMIXSERVER` name sent during SQLI authentication. |
| `DB_LOCALE` | Yes | | Database locale, for example `en_US.utf8`. |
| `CLIENT_LOCALE` | Yes | | Client locale, for example `en_US.utf8`. Its codeset controls Python row decoding. |
| `port` | No | `9088` | Informix SQLI port. |
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
| `snapshot.page.size` | No | `1000` | Rows per deterministic snapshot page for CDC-capable tables; minimum `1`. |
| `snapshot.max.rows` | No | `100000` | Maximum rows in a one-shot snapshot-only read. The connector fails instead of returning a partial table. |
| `max.records.per.batch` | No | `1000` | Target maximum projected CDC rows; minimum `1`. A complete transaction may exceed it. |
| `cdc.timeout` | No | `5` | CDC idle-read timeout in seconds; minimum `0`. |
| `cdc.max.records` | No | `4096` | Maximum native records requested from a CDC session; minimum `1`. |
| `cdc.max.frame.bytes` | No | `16777216` | Maximum accepted native CDC frame size (16 MiB by default; minimum `16`). |
| `cdc.max.transaction.records` | No | `100000` | Maximum records buffered in an open transaction. Exceeding it fails without emitting uncommitted data. |
| `cdc.read.bytes` | No | `32000` | Bytes requested in each SmartLOB CDC read. |

Because per-table options are supported, configure the Unity Catalog connection with this exact `externalOptionsAllowList`:

```text
source_table,snapshot.page.size,snapshot.max.rows,max.records.per.batch,cdc.timeout,cdc.max.records
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
      DB_LOCALE: "en_US.utf8",
      CLIENT_LOCALE: "en_US.utf8",
      encrypt: "true",
      externalOptionsAllowList: "source_table,snapshot.page.size,snapshot.max.rows,max.records.per.batch,cdc.timeout,cdc.max.records"
    }
  }')"

unset INFORMIX_PASSWORD
```

Use `--profile <profile-name>` when the CLI should use a non-default Databricks profile. Confirm the result with:

```bash
databricks connections get informix_sales
```

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
        "DB_LOCALE": "en_US.utf8",
        "CLIENT_LOCALE": "en_US.utf8",
        "encrypt": "true",
        "externalOptionsAllowList": (
            "source_table,snapshot.page.size,snapshot.max.rows,"
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
  "DB_LOCALE": "en_US.utf8",
  "CLIENT_LOCALE": "en_US.utf8",
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
  "DB_LOCALE": "en_US.utf8",
  "CLIENT_LOCALE": "en_US.utf8",
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
  "DB_LOCALE": "en_US.utf8",
  "CLIENT_LOCALE": "en_US.utf8",
  "redirect.enabled": "true",
  "redirect.allowlist": "informix-a.example.internal:9091,10.20.30.40:9091",
  "redirect.max": "2",
  "authentication.login.timeout": "30"
}
```

GSS/Kerberos, private-server authentication, and automatic server-name or locale discovery remain unsupported. PAM and the full redirect/reconnect security path have live integration coverage. The redirect test uses a deterministic ASF type-13 responder rather than a redirect emitted by IDS/HDR itself, and an actual serverless Lakeflow Connect pipeline run remains pending.

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
| Supported | `SMALLINT`, `INTEGER`/`INT`, `SERIAL`, `INT8`, `SERIAL8`, `FLOAT`/`DOUBLE`, `REAL`/`SMALLFLOAT`, `DECIMAL`/`NUMERIC`, `MONEY`, `DATE`, supported `DATETIME` qualifiers, `BOOLEAN`, `CHAR`/`NCHAR`, `VARCHAR`/`NVARCHAR`, `LVARCHAR` |
| Snapshot-only | `BYTE`, `TEXT`, `BLOB`, `CLOB`, `INTERVAL` |
| Excluded from speculative decoding | Unknown catalog types, UDT and complex types such as `ROW`, `SET`, `LIST`, and `MULTISET` |

`INT8` and `SERIAL8` use Informix's complete ten-byte signed-magnitude CDC representation. A `DATETIME` qualifier is CDC-capable when its start and end fields are supported by the native decoder. Values containing `YEAR` through at least `DAY` are exposed as timezone-free Spark timestamps; partial/time-only values are deterministic strings and never acquire the worker's current date.

Informix's native capture-column API does not provide portable row values for LOB, `INTERVAL`, complex, or opaque UDT columns. The connector does not synthesize those fields, follow session-bound LOB locators, or issue a post-commit reselect that could observe a later transaction. Therefore a table containing one of these columns is snapshot-only. The same restriction avoids pretending excluded fields are available in DELETE before-images. Unknown catalog type codes fail discovery rather than being interpreted speculatively. Decimal CDC requires valid precision and scale metadata within Spark's 38-digit decimal limit.

## Snapshot, CDC, and deletes

For a CDC-capable table, the connector records a pre-snapshot high-water LSN and reads the snapshot in complete primary-key order using seek pagination. CDC then resumes from that high-water mark.

Only complete committed transactions are emitted. Inserts and update after-images go to the data channel; deletes go to an independently checkpointed delete channel. Delete output contains the primary-key fields and connector metadata; non-key fields are null because Lakeflow only needs the key and native CDC cannot guarantee every excluded before-image field. A primary-key update emits the new row and deletes the old key. Rollbacks are suppressed. Transactions are never split to satisfy `max.records.per.batch`, so one large transaction can exceed the row target and consume substantial memory.

Delivery is at least once. A failure after rows are returned but before Lakeflow commits its checkpoint can replay them. `TRUNCATE` cannot be represented by keyed Lakeflow deletes and fails explicitly. Snapshot-only tables are fully reread and fail when they exceed `snapshot.max.rows`.

The connector adds `_informix_change_lsn`, `_informix_commit_lsn`, `_informix_tx_id`, and `_informix_op` to rows. `_informix_change_lsn` is the incremental cursor; operations are `r` (snapshot), `c` (insert), `u` (update), and `d` (delete).

### Checkpoints and log retention

During snapshot, Lakeflow checkpoints the pre-snapshot LSN and last primary-key values. Streaming checkpoints `commit_lsn`, `change_lsn`, and the oldest required `begin_lsn`; retaining the begin position is necessary for interleaved transactions. Data and delete channels have separate checkpoints and replay the source independently.

An idle timeout returns no rows and leaves the checkpoint unchanged. Incomplete or open transactions do not advance it. If the restart LSN predates the minimum retained logical log in `sysmaster:syslogs`, continuation fails and the table must be resnapshotted.

## Table configuration

```json
{
  "pipeline_spec": {
    "connection_name": "informix_sales",
    "object": [
      {
        "table": {
          "source_table": "informix.orders",
          "destination_table": "orders",
          "table_configuration": {
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

Supported source-specific table options are `snapshot.page.size`, `snapshot.max.rows`, `max.records.per.batch`, `cdc.timeout`, and `cdc.max.records`. Standard destination, SCD, key, sequence, and clustering options remain available.

## Operational guidance

- Start with one small table and verify snapshot, insert, update, delete, restart, rollback, idle timeout, and retention-expiry behavior against the target Informix version before broader use.
- Disposable Informix 15 testing has validated normal-password and PAM authentication, queries, discovery, snapshots, and committed INSERT/UPDATE/DELETE transactions through `syscdcv1`. A deterministic ASF type-13 responder has validated the complete redirect/reconnect security path through a successful target query. Validate TLS trust, locale behavior, permissions, schemas, data-type boundaries, restart behavior, retention, and IDS/HDR-emitted redirects against the target environment. An actual serverless Lakeflow Connect pipeline run remains pending.
- Authentication errors commonly indicate a wrong `server`/locale, unsupported authentication mode or redirect, an untrusted/mismatched TLS certificate, or insufficient `syscdcv1` privileges.
- Schema changes that alter captured column layout are not guaranteed to be safe during active capture. Restart and validate at a clean LSN boundary.
- Ensure source log retention covers downtime and the oldest checkpoint. Otherwise resnapshotting is required.

## References

- [Debezium Informix connector documentation](https://debezium.io/documentation/reference/stable/connectors/informix.html) for Informix-side CDC concepts and prerequisites
- Informix CDC installation script: `$INFORMIXDIR/etc/syscdcv1.sql`
