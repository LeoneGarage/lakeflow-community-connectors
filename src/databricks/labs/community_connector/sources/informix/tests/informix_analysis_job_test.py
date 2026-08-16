# Databricks notebook source
# Informix connector analysis — scheduled Databricks Job (serverless).
# Distributed on Spark: the S3 log scan and the data comparison both run across
# executors. Results ingest into the dashboard Delta tables (no Slack).
#
# Steps: capture window -> stop pipeline -> counts (SSM + spark.sql) ->
# data comparison (SSM UNLOAD + Spark join, tool's normalize via UDF) ->
# timing (DESCRIBE HISTORY) -> S3 slot cost (Spark mapPartitions) ->
# ingest to members.connector_bronze.analysis_*.

# COMMAND ----------
import os, sys, json, subprocess, time, datetime as dt, re, pathlib
from collections import Counter

PIPELINE_ID = "4f849629-61aa-4ff2-b5a1-00e672bc9280"
CAT, SCH = "members", "connector_bronze"
CAT_SCH = f"{CAT}.{SCH}"
SCRIPTS = "/Volumes/members/connector_bronze/connector_analysis/_job_scripts"
VOL_BASE = "/Volumes/members/connector_bronze/connector_analysis"
S3_WINDOW_HOURS = 1.5
# UC volume the connector writes connection slots into. The S3 log-scan facts
# (LOG_BUCKET / LOG_PREFIX / SLOT_SUBSTR) are NOT hardcoded — they are derived
# from this volume's physical storage_location in the resolve cell below.
SLOT_VOLUME = "members.connector_bronze.informix_cdc_state"
LOG_BUCKET = LOG_PREFIX = SLOT_SUBSTR = None  # resolved from SLOT_VOLUME below


def log(msg):
    print(f"[{dt.datetime.utcnow().strftime('%H:%M:%S')}] {msg}", flush=True)


# COMMAND ----------
AWS_KEY = dbutils.secrets.get("informix-analysis", "aws_access_key_id")
AWS_SECRET = dbutils.secrets.get("informix-analysis", "aws_secret_access_key")
AWS_REGION = dbutils.secrets.get("informix-analysis", "aws_region")
os.environ["AWS_ACCESS_KEY_ID"] = AWS_KEY
os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET
os.environ["AWS_DEFAULT_REGION"] = AWS_REGION
subprocess.run([sys.executable, "-m", "pip", "install", "-q", "awscli"], check=True)
sys.path.insert(0, SCRIPTS)
import compare_live_data_test_tool as CMP  # provides normalize(), parse_unload_line(), SSM helpers
import count_compare as CC

# COMMAND ----------
# Resolve the S3 log-scan facts from the slot volume's physical location rather
# than hardcoding them. DESCRIBE VOLUME yields storage_location of the form
# s3://<data-bucket>/<metastore-uuid>/volumes/<volume-uuid>; the access-log
# bucket follows databricks-s3-access-logs-<account>-<region> and log keys are
# prefixed <account>/<data-bucket>/.
_storage = spark.sql(f"DESCRIBE VOLUME {SLOT_VOLUME}").first()["storage_location"]
if not _storage.startswith("s3://"):
    raise ValueError(f"{SLOT_VOLUME} is not S3-backed: {_storage}")
_path = _storage[len("s3://") :].rstrip("/")
_data_bucket = _path.split("/")[0]
_volume_uuid = _path.split("/")[-1]
import boto3 as _boto3

_account = _boto3.client("sts").get_caller_identity()["Account"]
LOG_BUCKET = f"databricks-s3-access-logs-{_account}-{AWS_REGION}"
LOG_PREFIX = f"{_account}/{_data_bucket}/"
SLOT_SUBSTR = f"/volumes/{_volume_uuid}/.informix-connection-slots/"
log(f"slot volume {SLOT_VOLUME} -> bucket {LOG_BUCKET} prefix {LOG_PREFIX} slot {SLOT_SUBSTR}")

# COMMAND ----------
# S3 costing window. Defaults to the trailing S3_WINDOW_HOURS ending now, which is
# what the daily schedule wants. Supply window_start/window_end as *local wall-clock*
# times to cost a specific past window instead -- S3 access-log keys and log lines are
# both UTC, so the local values are converted once here and everything downstream
# stays UTC.
dbutils.widgets.text("window_start", "", "Window start (local, YYYY-MM-DD HH:MM)")
dbutils.widgets.text("window_end", "", "Window end (local, YYYY-MM-DD HH:MM)")
dbutils.widgets.text("window_tz", "Australia/Sydney", "Window timezone")

_WINDOW_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M")


def _local_to_utc(text, zone, label):
    """Parse a local wall-clock string and return the naive UTC datetime."""

    for fmt in _WINDOW_FORMATS:
        try:
            naive = dt.datetime.strptime(text.strip(), fmt)
            break
        except ValueError:
            continue
    else:
        raise ValueError(f"{label}={text!r} is not one of the accepted forms {_WINDOW_FORMATS}")
    aware = naive.replace(tzinfo=zone)
    utc = aware.astimezone(dt.timezone.utc).replace(tzinfo=None, microsecond=0)
    # A local time inside a DST transition either occurs twice or never. Both are
    # resolvable (PEP 495 fold=0 takes the pre-transition offset) but the caller
    # almost certainly did not mean an ambiguous instant, so say which case it is
    # rather than silently costing a window an hour from the one requested.
    #
    # Differing fold=0/fold=1 offsets flag *both* cases; round-tripping fold=0 back
    # through the zone is what separates them -- a nonexistent local time does not
    # come back as itself, an ambiguous one does.
    if aware.utcoffset() != aware.replace(fold=1).utcoffset():
        round_trip = utc.replace(tzinfo=dt.timezone.utc).astimezone(zone).replace(tzinfo=None)
        if round_trip != naive:
            log(
                f"WARNING {label}={text} does not exist in {zone} (clocks skip forward); "
                f"it resolves to {utc}Z, local {round_trip}"
            )
        else:
            log(
                f"WARNING {label}={text} occurs twice in {zone} (clocks fall back); "
                f"using the first occurrence, offset {aware.utcoffset()}"
            )
    return utc


now = dt.datetime.utcnow().replace(microsecond=0)
run_ts = now.strftime("%Y-%m-%dT%H:%M:%S")
run_folder = now.strftime("%Y-%m-%dT%H-%M-%S")

_raw_start = dbutils.widgets.get("window_start").strip()
_raw_end = dbutils.widgets.get("window_end").strip()
_raw_tz = dbutils.widgets.get("window_tz").strip() or "Australia/Sydney"
try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
except ImportError as _err:  # pragma: no cover - stdlib since 3.9
    raise RuntimeError("zoneinfo is required to interpret a local analysis window") from _err
try:
    WINDOW_TZ = ZoneInfo(_raw_tz)
except ZoneInfoNotFoundError:
    # Serverless images carry the system tz database, but a slim image may not.
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "tzdata"], check=True)
    import importlib
    import zoneinfo as _zoneinfo

    importlib.reload(_zoneinfo)
    WINDOW_TZ = _zoneinfo.ZoneInfo(_raw_tz)

if bool(_raw_start) != bool(_raw_end):
    raise ValueError("window_start and window_end must be supplied together, or both left empty")
if _raw_start:
    start_utc = _local_to_utc(_raw_start, WINDOW_TZ, "window_start")
    end_utc = _local_to_utc(_raw_end, WINDOW_TZ, "window_end")
    if end_utc <= start_utc:
        raise ValueError(
            f"window_end ({_raw_end}) must be after window_start ({_raw_start}) in {_raw_tz}"
        )
    if end_utc > now:
        log(f"WARNING window_end is {(end_utc - now).total_seconds() / 60:.0f} min in the future")
else:
    end_utc = now
    start_utc = now - dt.timedelta(hours=S3_WINDOW_HOURS)

win_start = start_utc.strftime("%Y-%m-%dT%H:%M")
win_end = end_utc.strftime("%Y-%m-%dT%H:%M")
# Derived from the window actually used, not from S3_WINDOW_HOURS: the per-hour and
# per-30d figures are wrong by the ratio of the two if a custom window is passed.
window_hours = (end_utc - start_utc).total_seconds() / 3600.0
win_start_local = start_utc.replace(tzinfo=dt.timezone.utc).astimezone(WINDOW_TZ)
win_end_local = end_utc.replace(tzinfo=dt.timezone.utc).astimezone(WINDOW_TZ)
_LOCAL_FMT = "%Y-%m-%dT%H:%M %Z"

vol_dir = f"{VOL_BASE}/{run_folder}"
pathlib.Path(vol_dir).mkdir(parents=True, exist_ok=True)
log(f"run_ts={run_ts}  S3 window {win_start}->{win_end}Z ({window_hours:.2f}h)")
log(
    f"  local ({_raw_tz}): {win_start_local.strftime(_LOCAL_FMT)}"
    f" -> {win_end_local.strftime(_LOCAL_FMT)}"
)

# COMMAND ----------
# Stop pipeline (SDK) + capture last start for timing.
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
t0 = time.time()
try:
    w.pipelines.stop_and_wait(PIPELINE_ID)
    log("pipeline IDLE")
except Exception as e:
    log(f"stop: {e}")
run_start = None
for ev in w.pipelines.list_pipeline_events(PIPELINE_ID, max_results=250):
    if getattr(ev, "event_type", "") == "create_update":
        run_start = ev.timestamp
        break
log(f"last pipeline start {run_start}")

# COMMAND ----------
# Build a shared args namespace for the compare tool's SSM/S3 helpers.
CMP.run  # ensure import ok
import argparse

CMP_args = argparse.Namespace(
    aws_profile=None,
    aws_region=AWS_REGION,
    instance_id=None,
    instance_name="informix-cdc",
    container="informix-cdc",
    database="testdb",
    source_owner="informix",
    informix_server="informix_tcp",
    databricks_profile="demo1",
    catalog=CAT,
    schema=SCH,
    s3_bucket="leone-sandbox-metastore",
    s3_prefix="tmp/informix-comparisons",
    tables=list(CC.TABLES),
    workers=20,
    destination_page_size=10000,
    output=None,
)
# ambient creds: drop --profile from aws() (env creds are set above)
CMP.aws = lambda a, *cmd: CMP.run(["aws", *cmd, "--region", a.aws_region])
# The compare tool presigns an S3 PUT via `aws configure export-credentials`, a
# CLI-v2-only subcommand; the job installs awscli v1. Supply the creds directly
# from the secret-scope env vars instead (no session token — static IAM key).
CMP.exported_credentials = lambda a: {
    "AccessKeyId": AWS_KEY,
    "SecretAccessKey": AWS_SECRET,
}


# The compare tool reads the destination via the `databricks` CLI, which is not
# configured on serverless job compute. We are on the driver with a live spark
# session, so route its destination queries through spark.sql instead. (Only
# destination_schema is used here; dest rows come from spark.read.table.)
def _spark_query(_args, statement):
    return [row.asDict() for row in spark.sql(statement).collect()]


CMP.databricks_query = _spark_query
CMP_args.instance_id = CMP.resolve_instance_id(CMP_args)
log(f"informix host {CMP_args.instance_id}")

# COMMAND ----------
# Counts: Informix (one SSM call) + pipeline (spark.sql).
CC.C.aws = CMP.aws
informix = CC.informix_counts(CC.make_args(CMP_args.instance_id))
pipeline = {t: spark.sql(f"SELECT COUNT(*) c FROM {CAT_SCH}.`{t}`").first()["c"] for t in CC.TABLES}
count_rows = [
    {"table_name": t, "informix_count": informix[t], "pipeline_count": pipeline[t]}
    for t in CC.TABLES
    if informix.get(t) is not None
]
json.dump(count_rows, open(f"{vol_dir}/count_compare.json", "w"))
log(
    f"counts: {sum(1 for r in count_rows if r['informix_count']==r['pipeline_count'])}/{len(count_rows)} match"
)

# COMMAND ----------
# Data comparison (Spark-native). One SSM UNLOAD exports every table + primary_keys;
# then per table: source .unl -> DF, dest -> spark.read.table, normalize BOTH with
# the tool's own normalize() via a UDF, full-outer join on PK -> missing/extra/mismatched.
run_id = f"informix-compare-{run_folder}"
out = pathlib.Path(f"/tmp/{run_id}")
out.mkdir(parents=True, exist_ok=True)
CMP.wait_ssm(CMP_args, CMP.send_ssm(CMP_args, CMP.source_export_command(CMP_args, run_id)))
CMP.transfer_source_archive(CMP_args, run_id, out)
src_root = out / run_id
pk_counts = CMP.primary_key_counts(src_root / "primary_keys.unl")
schema_rows = CMP.destination_schema(
    CMP_args
)  # uses databricks CLI on the DRIVER (configured), fine
cols, types = {}, {}
for r in schema_rows:
    if r["column_name"] in CMP.INTERNAL_COLUMNS:
        continue
    cols.setdefault(r["table_name"], []).append(r["column_name"])
    types.setdefault(r["table_name"], {})[r["column_name"]] = r["data_type"].upper()

# Stage the extracted .unl files where executors can read them. The archive lands on
# the driver's local disk, which no executor can see, so parsing it here would pin all
# 30 tables' source text to the driver. Copying to the run's volume folder lets
# spark.read.text fan the parse out instead -- and leaves the raw export beside the
# other artifacts for that run_ts, which the dashboard runbook wants anyway.
SRC_STAGE = f"{vol_dir}/source_unl"
pathlib.Path(SRC_STAGE).mkdir(parents=True, exist_ok=True)
import shutil as _shutil

for _table in CC.TABLES:
    _unl = src_root / f"{_table}.unl"
    if _unl.exists():
        _shutil.copyfile(_unl, f"{SRC_STAGE}/{_table}.unl")
log(f"staged source exports for executors under {SRC_STAGE}")

from pyspark.sql import functions as F, types as T
from pyspark.sql.functions import udf

_normalize = CMP.normalize
norm_udf = udf(lambda v, dtype: _normalize(v, dtype) if v is not None else None, T.StringType())
# Parse on the executors with the tool's own parser, so the field-splitting rules
# (pipe delimiter, backslash escapes) stay identical to the single-process tool.
parse_udf = udf(CMP.parse_unload_line, T.ArrayType(T.StringType()))


def compare_one(table):
    ccols = cols[table]
    ctypes = types[table]
    kc = pk_counts.get(table, 0) or len(ccols)
    key_cols = ccols[:kc] if kc else ccols
    # Source rows from the staged .unl, parsed on the executors. Reading the text as a
    # DataFrame and applying parse_udf keeps the work off the driver; the length filter
    # matches the previous driver-side loop, dropping any line whose field count does
    # not match the destination schema (a trailing blank line, most commonly).
    src_path = f"{SRC_STAGE}/{table}.unl"
    src_df = None
    if pathlib.Path(src_path).exists():
        parsed = (
            spark.read.text(src_path)
            .select(parse_udf(F.col("value")).alias("v"))
            .filter(F.size("v") == len(ccols))
        )
        src_df = parsed.select(*[F.col("v")[i].alias(c) for i, c in enumerate(ccols)])
    dest_df = spark.read.table(f"{CAT_SCH}.`{table}`").select(
        *[F.col(c).cast("string").alias(c) for c in ccols]
    )
    # normalize every column on both sides
    for c in ccols:
        dtype = ctypes[c]
        if src_df is not None:
            src_df = src_df.withColumn(c, norm_udf(F.col(c), F.lit(dtype)))
        dest_df = dest_df.withColumn(c, norm_udf(F.col(c), F.lit(dtype)))
    if src_df is None:
        src_n = 0
    else:
        src_df = src_df.alias("s")
        src_n = src_df.count()
    dest_n = dest_df.count()
    # join on key cols
    if src_df is None:
        return {
            "table": table,
            "source_rows": 0,
            "destination_rows": dest_n,
            "missing_rows": 0,
            "extra_rows": dest_n,
            "mismatched_rows": 0,
            "mismatched_cells": 0,
        }
    s = src_df.select([F.col(c).alias(f"s_{c}") for c in ccols])
    d = dest_df.select([F.col(c).alias(f"d_{c}") for c in ccols])
    cond = [F.col(f"s_{k}").eqNullSafe(F.col(f"d_{k}")) for k in key_cols]
    joined = s.join(d, cond, "full_outer")
    missing = joined.filter(F.col(f"d_{key_cols[0]}").isNull()).count()
    extra = joined.filter(F.col(f"s_{key_cols[0]}").isNull()).count()
    both = joined.filter(
        F.col(f"s_{key_cols[0]}").isNotNull() & F.col(f"d_{key_cols[0]}").isNotNull()
    )
    non_key = [c for c in ccols if c not in key_cols]
    if non_key:
        # Per-column mismatch indicator (0/1); row-level OR for mismatched_rows,
        # total sum for mismatched_cells — matching the tool's definitions.
        cell_flags = [
            F.when(~F.col(f"s_{c}").eqNullSafe(F.col(f"d_{c}")), 1).otherwise(0) for c in non_key
        ]
        row_flag = cell_flags[0]
        for f in cell_flags[1:]:
            row_flag = row_flag + f
        agg = both.select(
            F.sum(F.when(row_flag > 0, 1).otherwise(0)).alias("mm_rows"),
            F.sum(row_flag).alias("mm_cells"),
        ).first()
        mism_rows = int(agg["mm_rows"] or 0)
        mm_cells = int(agg["mm_cells"] or 0)
    else:
        mism_rows = 0
        mm_cells = 0
    return {
        "table": table,
        "source_rows": src_n,
        "destination_rows": dest_n,
        "missing_rows": missing,
        "extra_rows": extra,
        "mismatched_rows": mism_rows,
        "mismatched_cells": mm_cells,
    }


# Compare tables concurrently. Each compare_one is already distributed, but run
# serially the cluster idles between tables: the small tables cannot fill the
# executors, and every table pays its own job-launch latency end to end. Submitting
# from a driver thread pool overlaps the per-table Spark jobs so the scheduler packs
# them together.
#
# The pool threads only *submit* Spark work -- all row processing still happens on the
# executors, and each thread's own memory is a handful of DataFrame objects.
#
# No scheduler-pool tuning here. This job runs on serverless, which is Spark Connect:
# `spark.sparkContext` does not exist, and `spark.scheduler.mode` is not a conf a
# client may set at runtime. Each thread's plan is submitted to the server
# independently and serverless does its own admission control, so FAIR-pool tagging
# has nothing to attach to. An earlier version tried both behind bare excepts, which
# only bought silent dead code.
import concurrent.futures

COMPARE_WORKERS = int(os.environ.get("COMPARE_WORKERS", "8"))
_compare_tables = [t for t in CC.TABLES if t in cols]
data_rows = []
_failures = []
with concurrent.futures.ThreadPoolExecutor(max_workers=COMPARE_WORKERS) as _pool:
    # Keep the table with each future: as_completed returns them out of order, and a
    # failure otherwise cannot be attributed to a table.
    _futures = {_pool.submit(compare_one, t): t for t in _compare_tables}
    for _future in concurrent.futures.as_completed(_futures):
        _table = _futures[_future]
        try:
            data_rows.append(_future.result())
        except Exception as _err:
            # One unreadable table must not lose the other 29 comparisons; record it
            # and let the run finish, then fail loudly below.
            _failures.append((_table, repr(_err)))
            log(f"compare FAILED for {_table}: {_err!r}")
        else:
            log(f"compared {_table} ({len(data_rows)}/{len(_compare_tables)})")
# Restore the deterministic table order the serial loop produced, so the artifact and
# the Delta rows do not depend on which comparison finished first.
_order = {t: i for i, t in enumerate(CC.TABLES)}
data_rows.sort(key=lambda r: _order.get(r["table"], len(_order)))
json.dump(data_rows, open(f"{vol_dir}/data_compare.json", "w"))
log(f"data comparison: {len(data_rows)} tables, {len(_failures)} failed")
if _failures:
    raise RuntimeError(f"data comparison failed for {len(_failures)} table(s): {_failures}")

# COMMAND ----------
# S3 slot cost — distributed scan across executors.
import boto3


def list_log_keys(wstart, wend):
    s3 = boto3.client("s3")
    keys = []
    cur = dt.datetime.strptime(wstart, "%Y-%m-%dT%H:%M").replace(minute=0)
    stop = dt.datetime.strptime(wend, "%Y-%m-%dT%H:%M")
    while cur <= stop:
        pfx = LOG_PREFIX + cur.strftime("%Y-%m-%d-%H")
        tok = None
        while True:
            kw = {"Bucket": LOG_BUCKET, "Prefix": pfx}
            if tok:
                kw["ContinuationToken"] = tok
            resp = s3.list_objects_v2(**kw)
            keys += [o["Key"] for o in resp.get("Contents", [])]
            tok = resp.get("NextContinuationToken")
            if not tok:
                break
        cur += dt.timedelta(hours=1)
    return keys


keys = list_log_keys(win_start, win_end)
log(f"S3: {len(keys)} log objects to scan")
bc_key, bc_secret, bc_region = AWS_KEY, AWS_SECRET, AWS_REGION
wstart_dt = dt.datetime.strptime(win_start, "%Y-%m-%dT%H:%M")
wend_dt = dt.datetime.strptime(win_end, "%Y-%m-%dT%H:%M")
MONN = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "May": 5,
    "Jun": 6,
    "Jul": 7,
    "Aug": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dec": 12,
}
TIME_RE = r"\[(\d{2})/(\w{3})/(\d{4}):(\d{2}):(\d{2}):(\d{2})"
OP_RE = r"\b(?:REST|BATCH|WEBSITE|S3)\.(\w+)\.(\w+)\b"


# Distribute across executors with the DataFrame API (serverless has no `sc`/RDD).
# mapInPandas runs scan_pandas per partition of the keys DataFrame on executors,
# each yielding (op, count) rows; we then groupBy/sum to merge.
from pyspark.sql import functions as _F, types as _T


def scan_pandas(iterator):
    import boto3 as _b, re as _re, gzip as _gz, datetime as _dt, pandas as _pd

    cli = _b.client(
        "s3", aws_access_key_id=bc_key, aws_secret_access_key=bc_secret, region_name=bc_region
    )
    time_re = _re.compile(TIME_RE)
    op_re = _re.compile(OP_RE)
    for pdf in iterator:
        c = Counter()
        for key in pdf["key"]:
            raw = cli.get_object(Bucket=LOG_BUCKET, Key=key)["Body"].read()
            if raw[:2] == b"\x1f\x8b":
                raw = _gz.decompress(raw)
            for line in raw.decode("utf-8", "replace").splitlines():
                if SLOT_SUBSTR not in line:
                    continue
                tm = time_re.search(line)
                if tm:
                    ts = _dt.datetime(
                        int(tm.group(3)),
                        MONN[tm.group(2)],
                        int(tm.group(1)),
                        int(tm.group(4)),
                        int(tm.group(5)),
                        int(tm.group(6)),
                    )
                    if not (wstart_dt <= ts <= wend_dt):
                        continue
                om = op_re.search(line)
                if om:
                    c[f"{om.group(1)}.{om.group(2)}"] += 1
        # Explicit dtypes so the Arrow batch matches the declared schema even when
        # empty (an untyped empty frame yields a ChunkedArray pyarrow rejects).
        yield _pd.DataFrame(
            {
                "op": _pd.Series(list(c.keys()), dtype="object"),
                "n": _pd.Series(list(c.values()), dtype="int64"),
            }
        )


merged = Counter()
if keys:
    parts = max(8, min(200, len(keys) // 50))
    kdf = spark.createDataFrame([(k,) for k in keys], "key string").repartition(parts)
    out_schema = _T.StructType(
        [_T.StructField("op", _T.StringType()), _T.StructField("n", _T.LongType())]
    )
    agg = (
        kdf.mapInPandas(scan_pandas, out_schema).groupBy("op").agg(_F.sum("n").alias("n")).collect()
    )
    for row in agg:
        merged[row["op"]] = int(row["n"])
PUT_LIST, GET_OTHER = 0.005 / 1000, 0.0004 / 1000
total_req = sum(merged.values())
cost = 0.0
for op, n in merged.items():
    verb = op.split(".")[0]
    cost += n * (
        PUT_LIST
        if verb in {"PUT", "COPY", "POST", "LIST"}
        else 0.0
        if verb == "DELETE"
        else GET_OTHER
    )
per_hour = cost / window_hours
s3 = dict(
    total_requests=total_req,
    window_cost_usd=round(cost, 6),
    cost_per_hour_usd=round(per_hour, 6),
    cost_per_30d_usd=round(per_hour * 24 * 30, 2),
    window_hours=round(window_hours, 4),
    window_start_local=win_start_local.strftime(_LOCAL_FMT),
    window_end_local=win_end_local.strftime(_LOCAL_FMT),
    window_tz=_raw_tz,
)
json.dump({"counts": dict(merged), **s3}, open(f"{vol_dir}/s3_cost.json", "w"))
log(f"S3: {total_req} req, ${cost:.4f} window, ${s3['cost_per_30d_usd']}/30d")

# COMMAND ----------
# Ingest into Delta tables (run_ts-tagged).
from pyspark.sql import Row

if count_rows:
    cdf = spark.createDataFrame(
        [
            Row(
                run_ts=run_ts,
                table_name=r["table_name"],
                informix_count=int(r["informix_count"]),
                pipeline_count=int(r["pipeline_count"]),
                diff=int(r["informix_count"]) - int(r["pipeline_count"]),
                matched=int(r["informix_count"]) == int(r["pipeline_count"]),
            )
            for r in count_rows
        ]
    )
    cdf.withColumn("run_ts", cdf.run_ts.cast("timestamp")).write.mode("append").saveAsTable(
        f"{CAT_SCH}.analysis_count_compare"
    )
if data_rows:
    ddf = spark.createDataFrame(
        [
            Row(
                run_ts=run_ts,
                table_name=t["table"],
                source_rows=int(t["source_rows"]),
                destination_rows=int(t["destination_rows"]),
                missing_rows=int(t["missing_rows"]),
                extra_rows=int(t["extra_rows"]),
                mismatched_rows=int(t["mismatched_rows"]),
                mismatched_cells=int(t["mismatched_cells"]),
            )
            for t in data_rows
        ]
    )
    ddf.withColumn("run_ts", ddf.run_ts.cast("timestamp")).write.mode("append").saveAsTable(
        f"{CAT_SCH}.analysis_data_compare"
    )
sdf = spark.createDataFrame(
    [
        Row(
            run_ts=run_ts,
            window_start_utc=win_start,
            window_end_utc=win_end,
            window_hours=float(s3["window_hours"]),
            total_requests=int(s3["total_requests"]),
            window_cost_usd=float(s3["window_cost_usd"]),
            cost_per_hour_usd=float(s3["cost_per_hour_usd"]),
            cost_per_30d_usd=float(s3["cost_per_30d_usd"]),
            # The UTC columns above stay authoritative; these record which local
            # window was requested so a dashboard reader can tell a 09:00 Sydney
            # window from the 23:00 UTC one it became.
            window_start_local=s3["window_start_local"],
            window_end_local=s3["window_end_local"],
            window_tz=s3["window_tz"],
        )
    ]
)
# mergeSchema so the three local columns land on a table written before they existed;
# historical rows read back NULL for them.
(
    sdf.withColumn("run_ts", sdf.run_ts.cast("timestamp"))
    .write.mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(f"{CAT_SCH}.analysis_s3_cost")
)

# COMMAND ----------
dbutils.notebook.exit(
    json.dumps(
        {
            "run_ts": run_ts,
            "count_matched": sum(
                1 for r in count_rows if r["informix_count"] == r["pipeline_count"]
            ),
            "count_tables": len(count_rows),
            "data_clean": all(
                t["missing_rows"] == 0 and t["extra_rows"] == 0 and t["mismatched_rows"] == 0
                for t in data_rows
            )
            if data_rows
            else None,
            "s3_per_30d": s3["cost_per_30d_usd"],
            "s3_window_utc": f"{win_start}Z/{win_end}Z",
            "s3_window_local": f"{s3['window_start_local']}/{s3['window_end_local']}",
            "s3_window_hours": s3["window_hours"],
            "elapsed_s": int(time.time() - t0),
        }
    )
)
