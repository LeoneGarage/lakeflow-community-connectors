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
| `count_compare_test_tool.py` | Row-count comparison, Informix (SSM→dbaccess) vs pipeline tables. `--json-out <path>` emits per-table counts for dashboard ingestion. |
| `s3_slot_cost_test_tool.py` | Measured S3 request cost for the connection-slot volume prefix, parsed from S3 access logs. `--start`/`--end` are UTC unless `--timezone Australia/Sydney` is given, which reads them as local wall-clock instead. Note: access-log delivery lags ~1h, so a just-closed window undercounts. |
| `s3_slot_model_test_tool.py` | Analytical (upper-bound) S3 request-cost model from the slot-loop cadences. ~150× above the measured FUSE-cached cost. |
| `phase_switch_test_tool.py` | Snapshot→stream timing: last pipeline start → last incremental-snapshot chunk write. `--run-start <utc>`. |
| `ingest_analysis_test_tool.py` | Appends a run's count/data/S3 results (tagged `run_ts`) to the dashboard Delta tables (CLI-based, for local/session use). |
| `informix_analysis_job_test.py` | Databricks **Job notebook**: stop pipeline → counts → per-table data comparison → timing → S3 cost → ingest to Delta tables. Serverless; reads AWS creds from the `informix-analysis` secret scope; ingests via `spark.sql` (no Slack — results surface in the dashboard). The data comparison runs tables concurrently (`COMPARE_WORKERS`, default 8) and parses each source `.unl` on the executors. |
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
