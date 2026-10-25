# Informix connector — analysis & observability tooling

Operational tooling for validating and monitoring a live Informix ingestion
pipeline. **None of this is connector runtime code** — every `.py` here is named
`*_test*.py` so `tools/scripts/merge_python_source.py` excludes it from the
generated `_generated_informix_python_source.py` (verified: the merge picks up
zero of these files). The `.json` is never merged (the merge tool only scans
`*.py`).

## Files

| File | Purpose |
| --- | --- |
| `count_compare_test_tool.py` | Row-count comparison, Informix (SSM→dbaccess) vs pipeline tables. `--json-out <path>` emits per-table counts for dashboard ingestion; `--warehouse-id <id>` picks the SQL warehouse for the pipeline-side counts. |
| `s3_slot_cost_test_tool.py` | Measured S3 request cost for the connection-slot volume prefix, parsed from S3 access logs. `--start`/`--end` are UTC unless `--timezone Australia/Sydney` is given, which reads them as local wall-clock instead. Note: access-log delivery lags ~1h, so a just-closed window undercounts. |
| `s3_slot_model_test_tool.py` | Analytical (upper-bound) S3 request-cost model from the slot-loop cadences. ~150× above the measured FUSE-cached cost. |
| `phase_switch_test_tool.py` | Snapshot→stream timing: last pipeline start → last incremental-snapshot chunk write. `--run-start <utc>`. |
| `ingest_analysis_test_tool.py` | Appends a run's count/data/S3 results (tagged `run_ts`) to the dashboard Delta tables (CLI-based, for local/session use). |
| `informix_analysis_job_test.py` | Databricks **Job notebook**: stop pipeline → counts → per-table data comparison → timing → S3 cost → ingest to Delta tables. Serverless; reads AWS creds from the `informix-analysis` secret scope; ingests via `spark.sql` (no Slack — results surface in the dashboard). The data comparison runs tables concurrently (`COMPARE_WORKERS`, default 8) and parses each source `.unl` on the executors. |
| `keyless_star_probe_test_tool.py` | Live end-to-end probe of keyless tables keyed with `primary.keys: "*"`: SCD1, SCD2 and change-log destinations through DML and schema changes (see below). |
| `analysis_dashboard.json` | Serialized AI/BI dashboard definition (KPIs, trends, per-table view) over the analysis tables. |

## Dashboard & tables

- Dashboard: **Informix Connector Analysis** (`01f18f871e9b19eea51f4a9318760b20`).
- Landing volume: `/Volumes/members/connector_bronze/connector_analysis/<run_ts>/`.
- Delta tables (warehouse `8fff545079c359bb`, profile `demo1`):
  `analysis_count_compare`, `analysis_data_compare`, `analysis_s3_cost`.

## Scheduled job

Databricks Job `Informix Connector Analysis` runs `informix_analysis_job_test.py`
daily (schedule paused until validated). The workspace copy is deployed from this
file; update both together.

### Data comparison parallelism

Each table's comparison is a Spark job (full-outer join on the primary key), but the
tables used to run one at a time, so the cluster idled between them — small tables
cannot fill the executors, and every table paid its own job-launch latency serially.
Comparisons are now submitted from a driver thread pool (`COMPARE_WORKERS`, default 8;
raise it via the environment) so the scheduler packs the per-table jobs together.

Two details that are load-bearing:

- **No scheduler-pool tuning.** The job runs on serverless, i.e. Spark Connect:
  `spark.sparkContext` does not exist and `spark.scheduler.mode` is not a runtime-settable
  conf, so FAIR-pool tagging has nothing to attach to. Each thread submits its plan to
  the server independently and serverless does its own admission control. Do not
  reintroduce `setLocalProperty` here — it cannot work on this compute.
- Results are collected out of order (`as_completed`) and then **re-sorted into
  `CC.TABLES` order**, so `data_compare.json` and the Delta rows do not depend on which
  comparison happened to finish first.

A table that fails is recorded against its own name and does not abort its 29 siblings;
the run raises at the end of the cell with the full list, so a partial comparison can
never be silently ingested as a clean one.

The source `.unl` files are copied from the driver's local disk into
`<run_ts>/source_unl/` on the volume first — executors cannot see driver-local paths —
and parsed with `spark.read.text` plus the tool's own `parse_unload_line` as a UDF, so
the field-splitting rules stay identical to the single-process tool. Informix `UNLOAD`
escapes embedded newlines as a literal `\n` two-character sequence rather than emitting
a raw newline byte, so one row is always one line of text.

### Costing a specific time window

By default the job costs the trailing `S3_WINDOW_HOURS` (1.5h) ending at run time.
Three notebook widgets override that with an explicit window given in **local
wall-clock time**:

| Widget | Default | Meaning |
| --- | --- | --- |
| `window_start` | empty | Local start, `YYYY-MM-DD HH:MM[:SS]` (`T` separator also accepted) |
| `window_end` | empty | Local end, same forms |
| `window_tz` | `Australia/Sydney` | IANA zone the two values are interpreted in |

Both bounds must be supplied together; supplying one alone, or an end at or before
the start, fails fast. An end in the future warns and proceeds.

```bash
databricks jobs run-now 988720389743415 -p demo1 --notebook-params '{
  "window_start": "2026-08-07 09:00",
  "window_end":   "2026-08-07 17:00",
  "window_tz":    "Australia/Sydney"
}'
```

The values are converted to UTC once, up front — S3 access-log key prefixes and the
timestamps inside each log line are both UTC — so a Sydney window correctly spans two
UTC log dates (09:00–17:00 AEST is `2026-08-06-23` … `2026-08-07-07`). `window_hours`,
`cost_per_hour_usd`, and `cost_per_30d_usd` are all derived from the window actually
used rather than from `S3_WINDOW_HOURS`.

`analysis_s3_cost` also records `window_start_local`, `window_end_local`, and
`window_tz` alongside the authoritative UTC columns, so a dashboard reader can tell a
09:00 Sydney window from the 23:00 UTC one it became. Those columns are appended with
`mergeSchema`, so rows written before this change read back `NULL`.

A local time inside a DST transition is resolved with `fold=0` and warns, naming which
case it hit — Sydney has both a skipped hour (October) and a repeated one (April).
Note that a window spanning a transition is 3 wall-clock hours but 2 or 4 real hours,
and the cost rate uses real elapsed time.

Access-log delivery lags roughly an hour, so a just-closed window undercounts; prefer
a window that closed at least an hour ago, and re-measure with an `UPDATE` (not a
second `INSERT`) for the same `run_ts` if you re-run it.

## Keyless whole-row-key probe

`keyless_star_probe_test_tool.py` creates fresh run-suffixed keyless tables in the
demo `testdb`, ingests them with a throwaway serverless pipeline running this
checkout's connector, and checks every destination against a model of the README's
semantics after each step:

- `<run>_s1` SCD type 1, `<run>_s2` SCD type 2, `<run>_log` a change log keyed with
  `"*"` (`append.only.ingestion: "true"`), all `snapshot.mode: initial` with
  `snapshot.scan: cursor`;
- `<run>_hb`, a keyed heartbeat updated before every update, whose value shows the
  update read to the end of the log.

Phases: 1 initial snapshot with duplicates and NULLs; 2 inserts, key-changing and
no-op updates, deletes, one duplicate copy, a transaction; 3 a nullable column added
(the key grows); 4 INTEGER widened to BIGINT; 5 a nullable column dropped (the key
shrinks; rows that differed only in it collide, then one copy is deleted); 6 a NOT
NULL column dropped; 7 a drop and an add in one step (must fail, then a full refresh
recovers); 8 a column added with a default (same); 9 a column dropped while a
continuous update runs (must fail asking for a restart; the restarted update adopts
the shorter key); 10 exact copies (two inserted collapse to one row, updating one copy
removes the shared row, a row updated into a copy of another, one copy of it deleted);
11 widenings (VARCHAR(20) to VARCHAR(40), INTEGER to DECIMAL(12,2), DECIMAL(12,2) to
DECIMAL(14,2), each with a key change on a row from before it); 12 a narrowing,
DECIMAL(14,2) to DECIMAL(10,2) (must fail, then a full refresh recovers).

```bash
.venv/bin/python src/databricks/labs/community_connector/sources/informix/tests/\
keyless_star_probe_test_tool.py --warehouse-id 1a6fe9ed716571e0 \
  [--mode continuous|triggered] [--start-phase N] [--end-phase N] [--keep] \
  [--report out.json]
```

`--mode continuous` (the default) keeps one continuous update running and has each
step wait for its changes to arrive. Informix refuses DDL on a table while a CDC
session captures it (`cdc_set_fullrowlogging` returns -83710, `ALTER` fails with
19816), so a DDL step stops the pipeline first and retries the DDL until Informix has
closed the sessions, a few seconds later; the next step starts a new update, which is
also what a key that shrank needs. `--mode triggered` runs a triggered update per step
instead. `--start-phase N` replays phases 1..N-1 on the model alone, builds the tables
in that state, checks a full refresh of them, and starts at phase N. `--keep` leaves
the pipeline (and its event log) in place; without it the pipeline, its destination
tables and the workspace folder are deleted at the end. Informix tables are never
dropped.

Two behaviours of the demo server shape the probe:

- **Late delivery.** Informix's CDC log reader can sit at a checkpoint for minutes
  before it passes on changes. A triggered update then ends after one empty batch per
  flow. Each step waits up to 15 minutes for the heartbeat: continuous mode on its one
  running update, triggered mode by running more updates, which typically took 2-6.
  Lateness is reported as `lagged`. Each table's upsert and delete flows checkpoint on
  their own, so the heartbeat proves only its own flow caught up: a check that finds a
  destination short re-checks (continuous) or runs more updates (triggered) within the
  same budget before it fails. Wrong rows stay wrong; late ones settle and are reported.
- **Server-wide CDC stalls.** If the heartbeat never arrives, CDC has stopped
  delivering for every session on the server; only restarting Informix clears it. The
  probe records the stall with the server's CDC state (`stalls` in the report),
  restarts the Informix engine inside the container (`onmode -kuy`, then `oninit -w`),
  which keeps `testdb` and every table, resets the probe tables to the rows the stalled
  phase found, checks a full refresh, and reruns that phase, at most three times per
  run. If the phase had already changed the tables' DDL, the probe restarts the
  container instead, which rebuilds `testdb` (dropping every table on the server) and
  starts the demo workload, and recreates its tables as the phase found them.

Phase 9 retries its DDL until it lands between two polls of the running update. If
none of 40 tries gets through, the drop is applied with the pipeline stopped, the
restart check still runs, and the run lists the guard as not reached (`skipped`).

Live results, 2026-10-24:

- **Informix 14.10.FC9W1DE, triggered mode.** Every phase was run, some with
  `--start-phase`. All checks pass except phase 8's "the update ends as FAILED" (fixed
  since, below). Four stalls hit; the probe recovered three by restarting the
  container.
- **14.10.FC13W10DE, continuous mode** (runs 15-18, 21:36-23:07). Every phase passes
  except phase 12's error check, which originally looked only for the connector's
  message (below); phase 8 passes too. No stall or long hold. DML arrived in about
  0.4 minutes, a step after DDL in 2-6 minutes including the new update's start, and
  the settling check matched a lagging destination once. The same server held one
  triggered run's changes for 9 minutes at a checkpoint.

Findings:

- **SCD2 collision (phase 5).** Deleting one of two rows that collided after a drop
  closes every SCD2 version tied for newest. Rows loaded by one snapshot share a start
  position, so both close; the probe models this, and the connector README now says
  so (it said SCD2 "closes only the newest version").
- **Hang on a defaulted key column (phase 8, triggered mode; fixed).** The upsert flows
  failed with the expected "added … with a default … run a full refresh", but each
  table's delete flow waited for the upsert reader's schema-transition record, which
  was never published, and kept running empty batches, so a triggered update never
  ended (a continuous update failed as a whole within two minutes). The delete reader
  now runs the same default check and fails with the same error, and a triggered delete
  reader stops waiting after `cdc.shared.state.wait.seconds`. Rerun live: the triggered
  update fails within two minutes, every flow with the default error.
- **A narrowing fails before the connector sees it (phase 12).** The update fails at
  its start with the pipeline's `CANNOT_UPDATE_TABLE_SCHEMA` ("Failed to merge the
  current and new schemas") for the change-log destination: the narrower type the
  connector reports can't merge into the existing column, and the connector's own
  "changed column … other than by widening it; run a full refresh" never appears. The
  data is safe and a full refresh recovers; the connector README now names this error.
  The probe accepts either.
- **Exact copies (phase 10)** behave as documented: they collapse to one row, and a
  change to one copy at the source removes the shared row from the target while the
  other copy stays at the source.
