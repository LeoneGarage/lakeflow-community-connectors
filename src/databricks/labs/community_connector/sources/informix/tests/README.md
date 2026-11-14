# Informix connector — analysis & observability tooling

Operational tooling for validating and monitoring a live Informix ingestion
pipeline. **None of this is connector runtime code** — every `.py` here is named
`*_test*.py` so `tools/scripts/merge_python_source.py` excludes it from the
generated `_generated_informix_python_source.py` (verified: the merge picks up
zero of these files). The `.json` files are never merged (the merge tool only
scans `*.py`).

## Files

| File | Purpose |
| --- | --- |
| `count_compare_test_tool.py` | Row-count comparison, Informix (SSM→dbaccess) vs pipeline tables. `--json-out <path>` emits per-table counts for dashboard ingestion; `--warehouse-id <id>` picks the SQL warehouse for the pipeline-side counts (default: the workspace's only warehouse, or its Serverless Starter Warehouse) and `--profile` the Databricks CLI profile (default `demo1`). |
| `phase_switch_test_tool.py` | Snapshot→stream timing: pipeline start → last incremental-snapshot chunk write, per table. `--run-start <utc>` (default: the pipeline's newest update), `--pipeline-id <id>` (default: the pipeline named informix-pipeline), `--warehouse-id <id>` and `--profile`, defaulting as in the count compare. Uses the count compare's table list. |
| `informix_cycles_job_test_tool.py` | Databricks **Job notebook** behind the demo1 job `Informix cycles`: each cycle restarts the demo Informix container, runs informix-pipeline as a triggered full refresh, a triggered update and continuous updates, then compares counts and data. Results land in `main.informix_cycles.events` and `main.informix_cycles.cycles`. A cycle that ends in MISMATCH cancels the remaining cycles, leaving the container and the stopped pipeline as they are for diagnosis (`stop_on_mismatch=false` runs them all). `smoke=true` checks everything without restarting the container or running the pipeline. It finds the pipeline by name (`pipeline_name`, default informix-pipeline) and compares the catalog and schema that pipeline writes to, so a recreated pipeline or workspace needs no edit. With `resume_test=true` (the default) the full refresh is stopped on purpose once the index-ordered snapshot of `tw901_claim_note` has recorded a few pages on the staging Volume (`staging_location`), and the triggered update after it must go on from the last page: the snapshot rows' commit time, which is when their scan began, shows whether it did. A cycle whose data matches but whose snapshot was read again from the first row ends `NO RESUME`. A pipeline update that ends FAILED, in any run of a cycle, fails the whole run: the cycle is recorded as `PIPELINE FAILED` with the update's error, the pipeline is stopped, and the notebook raises. |
| `cycles_dashboard.json` | Serialized AI/BI dashboard definition over the two cycles tables: the latest run's status, cycle results and pipeline updates, and a page across all runs. |
| `wheel_build_test_tool.py` | Build step, not tooling: the `build_py` that `pyproject.toml` names under `[tool.setuptools.cmdclass]`. It leaves the connector's `*_test*` modules (`lakeflow_test.py`, `compare_live_data_test_tool.py` and the rest) out of the prebuilt wheel, the same files the merge tool leaves out of the generated source. |
| `keyless_star_probe_test_tool.py` | Live end-to-end probe of keyless tables keyed with `primary.keys: "*"`: SCD1, SCD2 and change-log destinations through DML and schema changes (see below). |
| `orders/` | Live test of a keyless one-to-many child table that holds identical rows: Informix tables and workload, an append-only bronze pipeline, a silver pipeline that rebuilds the table from its change log, and a comparison with the source (see below). |

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
keyless_star_probe_test_tool.py [--warehouse-id <id>] \
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

## Orders test (keyless child table with identical rows)

`orders/` checks that a keyless table's change log and the silver table rebuilt from it
hold every source row the right number of times, while the source changes during the
snapshot. `orders` has a primary key; `order_items` has none, has a non-unique index on
`order_id`, and holds exact duplicates. The workload deletes all of an order's items and
inserts new ones, in one transaction or several, inserts without deleting, and updates
an order's items by `order_id`: all of them, those of one sku, or to the values they
already have. No update changes `order_id`, the column the snapshot pages by.

| File | Purpose |
| --- | --- |
| `orders_setup.sh`, `orders_lib.sh` | Create and seed the two tables in `testdb` (300 orders, about a third of the item rows written two or three times). |
| `orders_workload.sh` | The workload, one transaction a second for the given minutes (default 15). |
| `orders_fingerprint.sh` | Source side of the comparison: `order_items` summed up in 25 buckets. |
| `orders_bronze_test_ingest.py` | Bronze pipeline source: both tables append-only; `order_items` with `snapshot.scan: keyset`, under which the connector picks the index on `order_id`. |
| `orders_silver.sql` | Silver pipeline source: `orders` by AUTO CDC; `order_items` from a count per distinct row (+1 for `r`, `c` and `u` rows, -1 for `d`) expanded back into rows. |
| `orders_bronze_pipeline.json`, `orders_silver_pipeline.json` | Pipeline settings to fill in and create the two pipelines with. |
| `orders_run.sh` | Bronze full refresh during the workload, bronze updates until it has ended and drained, then a silver full refresh. |
| `orders_compare_test_tool.py` | Compares the silver table with the fingerprint; prints `MATCH` or `MISMATCH` and exits 1 on a difference. |

Steps:

1. Copy `orders_lib.sh`, `orders_setup.sh`, `orders_workload.sh` and `orders_fingerprint.sh`
   into the Informix container and run `orders_setup.sh` there.
2. Put `orders_bronze_test_ingest.py` and `orders_silver.sql` in the workspace, fill in the
   two pipeline JSON files and create the pipelines
   (`databricks pipelines create --json @orders_bronze_pipeline.json`).
3. Start `orders_workload.sh 15` in the container and, while it runs,
   `orders_run.sh <bronze id> <silver id> <workload end, UTC HHMMSS> [profile]`.
4. After the workload has ended, save the output of `orders_fingerprint.sh` to a file and
   run `orders_compare_test_tool.py <file> --warehouse-id <id> --silver <catalog>.orders_silver
   --bronze <catalog>.orders_bronze`.

The tables are captured by CDC once the bronze pipeline has run; a restart of the demo
container rebuilds `testdb` without them.
