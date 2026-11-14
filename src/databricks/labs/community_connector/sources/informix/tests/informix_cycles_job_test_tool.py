# Databricks notebook source
# Informix restart/run/compare cycles as a Databricks Job (serverless), so they run with no
# laptop attached. Each cycle: restart the demo Informix container and wait for its CDC
# workload; run informix-pipeline as a triggered full refresh and a triggered update of
# RUN_MINUTES each, N continuous updates of RUN_MINUTES each and one more of FINAL_MINUTES;
# compare counts, and if they differ run one more continuous update of EXTRA_MINUTES and
# compare again; then compare the data. The full refresh runs past RUN_MINUTES when the
# keyless tables (DRAIN_TABLES) have not drained by then, so their snapshots never restart.
#
# With resume_test=true (the default) the full refresh is instead stopped on purpose, part-way
# through the index-ordered snapshot of RESUME_TABLE: as soon as that scan has recorded a few
# pages on the staging Volume. The triggered update after it must go on from the last page
# instead of reading the table again, and runs until the keyless tables have drained. Whether
# it did shows in the target: a snapshot row carries the time its scan began, which is before
# the second update started only if that update took the first one's scan up. The outcome is
# part of the cycle's verdict (NO RESUME when the second update started the scan over), next
# to the compares, which show that the resumed snapshot and the stream still add up.
#
# Every pipeline start and end, each cycle's verdict and the final summary go to the run's
# output and to main.informix_cycles.events / main.informix_cycles.cycles. The Informix side
# goes through AWS SSM on the demo host, with keys from the informix-cycles secret scope;
# the compare tools staged next to this notebook do the Informix work exactly as on a laptop.
#
# A pipeline update that ends FAILED, in any run of a cycle, fails the whole run. A failed
# full refresh resets nothing, so the targets and checkpoints of the cycle before would meet
# a rebuilt source; and a later update's failure is a finding of its own, which the runs
# after it would only bury. The cycle is recorded as PIPELINE FAILED with the update's
# error, the pipeline is stopped (which also ends the retry it starts by itself), and the
# notebook raises.
#
# A cycle that ends in MISMATCH or NO RESUME cancels the cycles after it: the next cycle
# would restart the container, which rebuilds testdb and loses the source state the
# mismatch has to be diagnosed against. The pipeline is already stopped for the compares
# and stays stopped. The notebook still exits normally, since a failed task is retried as a
# fresh run that restarts the container. stop_on_mismatch=false runs every cycle regardless.
#
# Parameters: cycles, run_minutes, continuous_runs, final_minutes (0 skips the final run),
# extra_minutes, pipeline_name (default informix-pipeline), stop_on_mismatch (default true),
# resume_test (default true), staging_location (the connection's snapshot.staging.location,
# where the scan's progress is read from; without access to it the resume test is skipped),
# and smoke=true, which checks AWS, SSM, the pipeline API, both compares and the event
# tables without restarting the container or running the pipeline. The pipeline is found by
# name and its tables in the catalog and schema it writes to, so a recreated pipeline or
# workspace needs no edit.

# COMMAND ----------
import datetime as dt
import json
import os
import pathlib
import subprocess
import sys
import time

dbutils.widgets.text("cycles", "5")
dbutils.widgets.text("run_minutes", "5")
dbutils.widgets.text("continuous_runs", "3")
dbutils.widgets.text("final_minutes", "10")
dbutils.widgets.text("extra_minutes", "8")
dbutils.widgets.dropdown("smoke", "false", ["false", "true"])
dbutils.widgets.text("pipeline_name", "informix-pipeline")
dbutils.widgets.dropdown("stop_on_mismatch", "true", ["true", "false"])
dbutils.widgets.dropdown("resume_test", "true", ["true", "false"])
dbutils.widgets.text("staging_location", "/Volumes/members/connector_bronze/informix_cdc_state")
CYCLES = int(dbutils.widgets.get("cycles"))
RUN_SECONDS = int(float(dbutils.widgets.get("run_minutes")) * 60)
CONTINUOUS_RUNS = int(dbutils.widgets.get("continuous_runs"))
FINAL_SECONDS = int(float(dbutils.widgets.get("final_minutes")) * 60)
EXTRA_SECONDS = int(float(dbutils.widgets.get("extra_minutes")) * 60)
SMOKE = dbutils.widgets.get("smoke") == "true"
PIPELINE_NAME = dbutils.widgets.get("pipeline_name")
STOP_ON_MISMATCH = dbutils.widgets.get("stop_on_mismatch") == "true"
RESUME_TEST = dbutils.widgets.get("resume_test") == "true"
STAGING = dbutils.widgets.get("staging_location").rstrip("/")

SCOPE = "informix-cycles"
SCHEMA = "main.informix_cycles"
EVENTS = f"{SCHEMA}.events"
RESULTS = f"{SCHEMA}.cycles"
TERMINAL = {"COMPLETED", "FAILED", "CANCELED"}
# Keyless tables whose one-pass snapshot restarts from its first row if the full-refresh
# run stops mid-drain: that run stops at RUN_MINUTES if both have drained by then, and
# otherwise waits until they have.
DRAIN_TABLES = ("tw900_member_tag", "tw901_claim_note")
# The resume test's table: keyless with a non-unique index, so its snapshot pages by that
# index and records each page it stages; the demo seeds it large enough to take several.
# The full refresh is stopped once this many pages are recorded.
RESUME_TABLE = "tw901_claim_note"
RESUME_PAGES = 2
# What each cycle's resume test found, for its verdict.
RESUME_RESULTS = {}
RUN_ID = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA} COMMENT 'Informix cycle runs (job)'")
spark.sql(
    f"CREATE TABLE IF NOT EXISTS {EVENTS} "
    "(run_id STRING, at TIMESTAMP, cycle INT, kind STRING, message STRING)"
)
spark.sql(
    f"CREATE TABLE IF NOT EXISTS {RESULTS} (run_id STRING, at TIMESTAMP, cycle INT, "
    "verdict STRING, first_count STRING, extra_run BOOLEAN, final_count STRING, "
    "data_compare STRING, details STRING)"
)


def event(cycle, kind, message):
    now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    print(f"{now:%H:%M:%S} [cycle {cycle}] {kind}: {message}", flush=True)
    try:
        spark.createDataFrame(
            [(RUN_ID, now, cycle, kind, message)],
            "run_id string, at timestamp, cycle int, kind string, message string",
        ).write.mode("append").saveAsTable(EVENTS)
    except Exception as error:  # noqa: BLE001 - the run log still has it
        print(f"event write failed: {error!r}", flush=True)


event(
    0,
    "start",
    f"run {RUN_ID}: {CYCLES} cycles, {CONTINUOUS_RUNS} continuous runs of "
    f"{RUN_SECONDS // 60} min, final run {FINAL_SECONDS // 60} min, "
    f"extra run {EXTRA_SECONDS // 60} min, smoke={SMOKE}, stop_on_mismatch={STOP_ON_MISMATCH}, "
    f"resume_test={RESUME_TEST}",
)

# COMMAND ----------
# AWS: the keys from the secret scope, and the v1 CLI the compare tools shell out to.
AWS_KEY = dbutils.secrets.get(SCOPE, "aws_access_key_id").strip()
AWS_SECRET = dbutils.secrets.get(SCOPE, "aws_secret_access_key").strip()
os.environ.update(
    AWS_ACCESS_KEY_ID=AWS_KEY, AWS_SECRET_ACCESS_KEY=AWS_SECRET, AWS_DEFAULT_REGION="us-east-1"
)
subprocess.run([sys.executable, "-m", "pip", "install", "-q", "awscli"], check=True)
os.environ["PATH"] = os.path.dirname(sys.executable) + os.pathsep + os.environ.get("PATH", "")

# A job notebook runs in its own workspace folder, where the two tools are staged.
sys.path.insert(0, os.getcwd())
import compare_live_data_test_tool as CMP  # noqa: E402
import count_compare_test_tool as CC  # noqa: E402

# The count tool must drive SSM through the same module object the patches below land on,
# whichever compare_live_data_test_tool it managed to import.
CC.C = CMP

# Ambient keys, no profile: the tools' --profile flags name laptop profiles.
CMP.aws = lambda a, *command: CMP.run(["aws", *command, "--region", a.aws_region])
# The tool presigns the export upload from `aws configure export-credentials`, a CLI v2
# command; pip installs v1, so hand it the keys directly (static IAM keys, no token).
CMP.exported_credentials = lambda _a: {"AccessKeyId": AWS_KEY, "SecretAccessKey": AWS_SECRET}


# The tools read the destination with the `databricks` CLI, which a job does not have;
# this driver has a Spark session, so they query through it.
def _spark_rows(_args, statement):
    return [row.asDict() for row in spark.sql(statement).collect()]


def _destination_rows(args, table, columns, key_count):
    # One query per table rather than the CLI's OFFSET pages: same rows, same casts.
    del key_count

    def quote(column):
        return "`" + column.replace("`", "``") + "`"

    projection = ",".join(f"CAST({quote(c)} AS STRING) AS {quote(c)}" for c in columns)
    catalog, schema = CMP.validate_identifier(args.catalog), CMP.validate_identifier(args.schema)
    return _spark_rows(args, f"SELECT {projection} FROM {catalog}.{schema}.{quote(table)}")


CMP.databricks_query = _spark_rows
CMP.destination_rows = _destination_rows

HOST = CC.make_args(None)
HOST.instance_id = CMP.resolve_instance_id(HOST)
identity = json.loads(CMP.aws(HOST, "sts", "get-caller-identity", "--output", "json"))
event(0, "aws", f"AWS identity {identity['Arn']}; Informix host {HOST.instance_id}")


def ssm(command, timeout=600):
    return CMP.wait_ssm(HOST, CMP.send_ssm(HOST, command, timeout), timeout)


# COMMAND ----------
# The pipeline, through the REST API as this job's run-as user.
from databricks.sdk import WorkspaceClient  # noqa: E402

WORKSPACE = WorkspaceClient()
API = WORKSPACE.api_client
_named = [
    p.pipeline_id
    for p in WORKSPACE.pipelines.list_pipelines(filter=f"name LIKE '{PIPELINE_NAME}'")
    if p.name == PIPELINE_NAME
]
if len(_named) != 1:
    raise RuntimeError(f"expected one pipeline named {PIPELINE_NAME}, found {len(_named)}")
PIPELINE_ID = _named[0]
BASE = f"/api/2.0/pipelines/{PIPELINE_ID}"
_spec = API.do("GET", BASE)["spec"]
# The destination the pipeline writes to; older pipelines name the schema `target`.
TARGET = f"{_spec['catalog']}.{_spec.get('schema') or _spec['target']}"
event(0, "pipeline", f"pipeline {PIPELINE_NAME} {PIPELINE_ID} writes to {TARGET}")


def _call(method, path, body=None, attempts=6, delay=10):
    # A transient API error must not end a multi-hour unattended run; the pollers call
    # this every 10-15 s, so a minute of retries is nothing. Only idempotent calls go
    # through here (reads, the spec edit, stop); update starts keep their own loop.
    for attempt in range(1, attempts + 1):
        try:
            return API.do(method, path, body=body)
        except Exception as error:  # noqa: BLE001
            if attempt == attempts:
                raise
            print(f"{method} {path} failed (attempt {attempt}): {error}"[:300], flush=True)
            time.sleep(delay)


def _get(path):
    return _call("GET", path)


def pipeline_state():
    return _get(BASE)


def update_info(update_id):
    return _get(f"{BASE}/updates/{update_id}")["update"]


def latest_update():
    updates = pipeline_state().get("latest_updates") or []
    return updates[0]["update_id"] if updates else None


def set_continuous(cycle, continuous):
    spec = pipeline_state()["spec"]
    spec["continuous"] = continuous
    _call("PUT", BASE, body=spec)
    event(cycle, "pipeline", f"pipeline set to continuous={str(continuous).lower()}")


def stop_and_wait(timeout=1200):
    _call("POST", f"{BASE}/stop")
    deadline = time.time() + timeout
    state = None
    while time.time() < deadline:
        state = pipeline_state().get("state")
        if state == "IDLE":
            return
        time.sleep(10)
    # Restarting Informix under a live update would fail it and blame the connector.
    event(0, "stop", f"pipeline still {state} {timeout}s after stop; aborting the run")
    raise RuntimeError(f"pipeline did not reach IDLE within {timeout}s (state {state})")


def start_update(cycle, label, full_refresh=False):
    # A start refused while a failed update is still being retried: stop and try again.
    for attempt in range(1, 6):
        try:
            return API.do("POST", f"{BASE}/updates", body={"full_refresh": full_refresh})[
                "update_id"
            ]
        except Exception as error:  # noqa: BLE001
            event(cycle, "refused", f"{label}: start refused (attempt {attempt}): {error}"[:500])
            stop_and_wait()
            time.sleep(20)
    raise RuntimeError(f"{label}: could not start an update")


def stop_update(cycle, label, update_id):
    # Retry until the update has really ended: one failed stop would leave a continuous
    # update running far past its time.
    state = None
    for attempt in range(1, 21):
        try:
            API.do("POST", f"{BASE}/stop")
        except Exception as error:  # noqa: BLE001
            event(cycle, "stop", f"{label}: stop attempt {attempt} failed: {error}"[:500])
        for _ in range(60):
            state = update_info(update_id)["state"]
            if state in TERMINAL:
                return state
            time.sleep(10)
        event(cycle, "stop", f"{label}: update {update_id} still {state} after a stop; retrying")
    return state


def undrained():
    # A full refresh empties every target before the update runs, and a drained table's
    # pages are served only after its drain finishes, so a target with rows has drained.
    pending = []
    for table in DRAIN_TABLES:
        try:
            rows = spark.sql(f"SELECT COUNT(*) c FROM {TARGET}.`{table}`").first()["c"]
        except Exception:  # noqa: BLE001 - a target not created yet has not drained
            rows = 0
        if not rows:
            pending.append(table)
    return pending


def wait_for_drains(cycle, label, update_id, start, state):
    # Past RUN_MINUTES, keep the full-refresh run going until both keyless tables have
    # drained: stopping it mid-drain would make the next run start their snapshots over.
    pending = undrained() if state == "RUNNING" else list(DRAIN_TABLES)
    if pending:
        event(cycle, "draining", f"{label}: waiting for {', '.join(pending)} to drain")
    while pending and state not in TERMINAL:
        time.sleep(15)
        state = update_info(update_id)["state"]
        if state == "RUNNING":
            pending = undrained()
    if pending and state in TERMINAL:
        # An update that ended between two polls may have drained without one seeing it.
        pending = undrained()
    if not pending:
        minutes = (time.time() - start) / 60
        event(cycle, "drained", f"{label}: {', '.join(DRAIN_TABLES)} drained by {minutes:.1f} min")
    return state


def run_for(cycle, label, update_id, seconds, from_creation=True, until_drained=False):
    info = update_info(update_id)
    start = int(info["creation_time"]) / 1000 if from_creation else time.time()
    deadline = start + seconds
    state = info["state"]
    while time.time() < deadline and state not in TERMINAL:
        time.sleep(15)
        state = update_info(update_id)["state"]
    if until_drained and state not in TERMINAL:
        state = wait_for_drains(cycle, label, update_id, start, state)
    minutes = round((time.time() - start) / 60, 1)
    if state in TERMINAL:
        # "ended before" marks an early end for the dashboard; a full-refresh run waiting
        # for the drains can also end by itself after RUN_MINUTES.
        when = f"before {seconds // 60}" if time.time() < deadline else f"after {minutes}"
        event(cycle, "ended", f"{label}: update {update_id} ended {when} minutes: {state}")
        if state == "FAILED":
            fail_pipeline(cycle, label, update_id)
        return state
    event(
        cycle, "stopping", f"{label}: stopping update {update_id} after {minutes} minutes ({state})"
    )
    state = stop_update(cycle, label, update_id)
    event(cycle, "ended", f"{label}: update {update_id} final state {state}")
    if state == "FAILED":
        fail_pipeline(cycle, label, update_id)
    return state


# COMMAND ----------
# The resume test: stop the full refresh part-way through RESUME_TABLE's snapshot, and see
# whether the update after it took that snapshot up.
import glob  # noqa: E402

_SCAN_POINTER = []


def _scan_progress():
    # (pointer, pages recorded) of the scan of RESUME_TABLE under way for this pipeline, or
    # (None, 0). The connector keeps, on the staging Volume, snapshot-scan/<pipeline>.json
    # naming the update whose scan it is, and one file per page read beside it.
    if not _SCAN_POINTER:
        _SCAN_POINTER.extend(
            path
            for path in glob.glob(f"{STAGING}/*/*/snapshot-scan/{PIPELINE_ID}.json")
            if RESUME_TABLE in pathlib.Path(path).read_text()
        )
    for path in _SCAN_POINTER[:1]:
        try:
            pointer = json.loads(pathlib.Path(path).read_text())
            directory = os.path.join(
                os.path.dirname(path),
                pointer["scope"],
                pointer["schema_id"],
                str(pointer["boundary"]),
            )
            return pointer, sum(1 for name in os.listdir(directory) if name[:8].isdigit())
        except (OSError, ValueError, KeyError):
            # Gone, or being rewritten: the scan ended, or another has just begun.
            del _SCAN_POINTER[:]
    return None, 0


def stop_part_way(cycle, label, update_id):
    # Stop the update once RESUME_TABLE's scan, begun by it, has recorded RESUME_PAGES
    # pages. Returns what the resume check needs, or None when there was nothing to stop
    # part-way (the update goes on as a run that was never stopped).
    if not os.path.isdir(STAGING):
        event(cycle, "resume", f"SKIPPED: cannot read the staging location {STAGING}")
        return None
    del _SCAN_POINTER[:]
    while True:
        state = update_info(update_id)["state"]
        if state in TERMINAL:
            event(cycle, "resume", f"SKIPPED: {label} ended ({state}) before its scan was seen")
            return None
        # Only once the update runs: until then the targets still hold the rows of the run
        # before, which the full refresh has not reset yet, and would pass for a drain.
        if state == "RUNNING" and RESUME_TABLE not in undrained():
            event(cycle, "resume", f"SKIPPED: {RESUME_TABLE} drained before it could be stopped")
            return None
        pointer, pages = _scan_progress()
        if pointer and str(pointer.get("scope", "")).endswith(update_id) and pages >= RESUME_PAGES:
            break
        time.sleep(5)
    state = stop_update(cycle, label, update_id)
    _, pages = _scan_progress()
    event(
        cycle,
        "resume",
        f"{label}: stopped update {update_id} ({state}) with {pages} pages of "
        f"{RESUME_TABLE} recorded",
    )
    return {"pages": pages, "update": update_id, "state": state}


def resume_check(cycle, stopped, update_id):
    # Did the update after the stop take the scan up? Its snapshot rows carry the source's
    # time when their scan began: before this update was created if it went on with the
    # stopped one's scan, after it if it read the table from its first row again.
    created = dt.datetime.fromtimestamp(
        int(update_info(update_id)["creation_time"]) / 1000, dt.timezone.utc
    ).replace(tzinfo=None)
    try:
        row = spark.sql(
            f"SELECT COUNT(*) n, MIN(_informix_commit_time) first, "
            f"MAX(_informix_commit_time) last FROM {TARGET}.`{RESUME_TABLE}` "
            f"WHERE _informix_op = 'r'"
        ).first()
    except Exception as error:  # noqa: BLE001
        result = f"UNKNOWN: cannot read the snapshot rows of {RESUME_TABLE}: {error}"[:300]
        event(cycle, "resume", result)
        return result
    if not row["n"]:
        result = f"UNKNOWN: {RESUME_TABLE} has no snapshot rows yet"
    elif row["last"] < created:
        result = (
            f"PASS: {row['n']} snapshot rows from the scan begun at {row['first']:%H:%M:%S}, "
            f"stopped after {stopped['pages']} pages and taken up by the update created at "
            f"{created:%H:%M:%S}"
        )
    else:
        result = (
            f"FAIL: the update created at {created:%H:%M:%S} read {RESUME_TABLE} from its "
            f"first row again (snapshot rows from a scan begun at {row['last']:%H:%M:%S}), "
            f"although {stopped['pages']} pages were recorded when the full refresh stopped"
        )
    event(cycle, "resume", result)
    return result


# COMMAND ----------
# The cycle's steps.
def save_result(cycle, verdict, first="", extra=False, final="", line="", details=()):
    spark.createDataFrame(
        [
            (
                RUN_ID,
                dt.datetime.now(dt.timezone.utc).replace(tzinfo=None),
                cycle,
                verdict,
                first,
                extra,
                final,
                line,
                json.dumps(details, default=str)[:20000],
            )
        ],
        "run_id string, at timestamp, cycle int, verdict string, first_count string, "
        "extra_run boolean, final_count string, data_compare string, details string",
    ).write.mode("append").saveAsTable(RESULTS)


class PipelineFailed(RuntimeError):
    pass


def fail_pipeline(cycle, label, update_id):
    # An update ended FAILED: record it with its error and fail the run (see the top).
    # The update's own error, for the record: the first one its event log holds.
    try:
        row = spark.sql(
            f"SELECT message, error.exceptions[0].message AS cause "
            f"FROM event_log('{PIPELINE_ID}') "
            f"WHERE origin.update_id = '{update_id}' AND level = 'ERROR' "
            f"ORDER BY timestamp LIMIT 1"
        ).first()
        cause = "" if row is None else f"{row['message']} {(row['cause'] or '')[-600:]}"
    except Exception as error:  # noqa: BLE001 - the failure is reported either way
        cause = f"(its error could not be read: {error})"
    message = f"{label}: update {update_id} FAILED; failing the run. {cause}"[:2000]
    event(cycle, "failed", message)
    save_result(cycle, "PIPELINE FAILED", line=message)
    # Also ends the retry the pipeline starts by itself.
    stop_and_wait()
    event(0, "summary", f"run failed in cycle {cycle}: {label}: a pipeline update failed")
    raise PipelineFailed(message)


def restart_container(cycle):
    event(cycle, "restart", "restarting the Informix container")
    status = ssm(
        "docker restart informix-cdc && docker ps --filter name=informix-cdc --format '{{.Status}}'"
    )
    event(cycle, "restart", f"container: {status.strip()[:200]}")
    deadline = time.time() + 1500
    while time.time() < deadline:
        out = ssm(
            "docker exec informix-cdc sh -c "
            "'ps -ef | grep -v grep | grep -c newhealth_cdc_workload; true' 2>&1; true",
            120,
        )
        count = out.strip().splitlines()[-1] if out.strip() else "?"
        if count.isdigit() and int(count) > 0:
            event(cycle, "restart", f"incremental workload running ({count} processes)")
            return
        time.sleep(30)
    raise RuntimeError("the Informix workload did not start within 25 minutes")


def pipeline_runs(cycle):
    event(cycle, "cycle", "pipeline runs starting")
    set_continuous(cycle, False)
    stopped = None
    for run, full_refresh in ((1, True), (2, False)):
        kind = "triggered full-refresh" if full_refresh else "triggered"
        event(cycle, "starting", f"run {run}: starting {kind} update")
        update_id = start_update(cycle, f"run {run}", full_refresh)
        event(cycle, "started", f"run {run}: started {kind} update {update_id}")
        if full_refresh and RESUME_TEST:
            stopped = stop_part_way(cycle, f"run {run}", update_id)
            if stopped:
                if stopped["state"] == "FAILED":
                    fail_pipeline(cycle, f"run {run}", update_id)
                continue
        # The run after a stopped full refresh has the keyless tables' snapshots to finish.
        run_for(
            cycle,
            f"run {run}",
            update_id,
            RUN_SECONDS,
            until_drained=full_refresh or bool(stopped),
        )
        if stopped:
            RESUME_RESULTS[cycle] = resume_check(cycle, stopped, update_id)
    before = latest_update()
    event(
        cycle, "starting", "run 3: starting continuous update (switching to continuous starts it)"
    )
    set_continuous(cycle, True)
    update_id = None
    for _ in range(12):
        candidate = latest_update()
        if candidate and candidate != before:
            update_id = candidate
            break
        time.sleep(5)
    update_id = update_id or start_update(cycle, "run 3")
    event(cycle, "started", f"run 3: started continuous update {update_id}")
    run_for(cycle, "run 3", update_id, RUN_SECONDS)
    for run in range(4, 3 + CONTINUOUS_RUNS):
        event(cycle, "starting", f"run {run}: starting continuous update")
        update_id = start_update(cycle, f"run {run}")
        event(cycle, "started", f"run {run}: started continuous update {update_id}")
        run_for(cycle, f"run {run}", update_id, RUN_SECONDS)
    if FINAL_SECONDS:
        run = 3 + CONTINUOUS_RUNS
        minutes = FINAL_SECONDS // 60
        event(cycle, "starting", f"run {run}: starting continuous update ({minutes} min)")
        update_id = start_update(cycle, f"run {run}")
        event(cycle, "started", f"run {run}: started continuous update {update_id}")
        run_for(cycle, f"run {run}", update_id, FINAL_SECONDS)


def extra_run(cycle):
    # One more continuous run. An update that fails fails the run, as in any other.
    event(cycle, "starting", "extra run: starting continuous update")
    update_id = start_update(cycle, "extra run")
    event(cycle, "started", f"extra run: started continuous update {update_id}")
    run_for(cycle, "extra run", update_id, EXTRA_SECONDS, False)


def count_compare(cycle):
    informix = CC.informix_counts(HOST)
    pipeline = {}
    for table in CC.TABLES:
        try:
            pipeline[table] = spark.sql(f"SELECT COUNT(*) c FROM {TARGET}.`{table}`").first()["c"]
        except Exception:  # noqa: BLE001 - a missing target table counts as a difference
            pipeline[table] = None
    differing = [
        f"{t} {informix.get(t)} {pipeline.get(t)}"
        for t in CC.TABLES
        if informix.get(t) is None or informix.get(t) != pipeline.get(t)
    ]
    source = sum(v for v in informix.values() if v is not None)
    target = sum(v for v in pipeline.values() if v is not None)
    total = f"TOTAL {source} {target} {target - source:+d} {len(differing)} table(s) not matching"
    event(cycle, "counts", total + (f"; differing: {'; '.join(differing)}" if differing else ""))
    return total, not differing, differing


def data_compare(cycle):
    output = pathlib.Path(f"/tmp/informix-cycles/{RUN_ID}/c{cycle}")
    output.mkdir(parents=True, exist_ok=True)
    argv = sys.argv
    catalog, schema = TARGET.split(".")
    sys.argv = [
        "compare_live_data_test_tool",
        "--output",
        str(output),
        "--catalog",
        catalog,
        "--schema",
        schema,
    ]
    try:
        CMP.main()
    finally:
        sys.argv = argv
    rows = json.loads((output / "comparison.json").read_text())
    keys = (
        "missing_rows",
        "extra_rows",
        "mismatched_rows",
        "duplicate_source_keys",
        "duplicate_destination_keys",
    )
    totals = {k: sum(r.get(k, 0) for r in rows) for k in keys}
    bad = [r["table"] for r in rows if any(r.get(k, 0) for k in keys)]
    line = (
        f"data compare: {len(rows)} tables, source {sum(r['source_rows'] for r in rows)} rows, "
        f"destination {sum(r['destination_rows'] for r in rows)} rows, "
        + ", ".join(f"{k}={v}" for k, v in totals.items())
        + (f", tables with differences: {' '.join(bad)}" if bad else "")
    )
    event(cycle, "data", line)
    details = [
        {
            k: r.get(k)
            for k in ("table", "missing_key_examples", "extra_key_examples", "mismatch_examples")
        }
        for r in rows
        if r["table"] in bad
    ]
    return line, not bad, details


# COMMAND ----------
if SMOKE:
    event(0, "smoke", f"SSM: {ssm('echo ok').strip()}")
    state = pipeline_state()
    event(
        0,
        "smoke",
        f"pipeline {state.get('name')} {state.get('state')} "
        f"continuous={state['spec'].get('continuous')}",
    )
    event(
        0,
        "smoke",
        f"staging location {STAGING}: "
        + ("readable" if os.path.isdir(STAGING) else "not readable, the resume test would skip"),
    )
    total, counts_ok, _ = count_compare(0)
    line, data_ok, _ = data_compare(0)
    event(
        0,
        "summary",
        f"smoke done: counts {'match' if counts_ok else 'differ'}, "
        f"data {'clean' if data_ok else 'differs'}",
    )
    dbutils.notebook.exit(
        json.dumps({"run_id": RUN_ID, "smoke": True, "counts": total, "data": line})
    )

# COMMAND ----------
summary = []
for cycle in range(1, CYCLES + 1):
    event(cycle, "cycle", f"cycle {cycle} of {CYCLES} starting")
    stop_and_wait()
    restart_container(cycle)
    pipeline_runs(cycle)
    stop_and_wait()
    event(cycle, "counts", "comparing counts")
    first, counts_ok, _ = count_compare(cycle)
    final, extra = first, False
    if not counts_ok:
        event(cycle, "counts", f"counts differ: {first}; one more continuous run")
        extra = True
        extra_run(cycle)
        stop_and_wait()
        event(cycle, "counts", "comparing counts after the extra run")
        final, counts_ok, _ = count_compare(cycle)
    event(cycle, "data", "comparing data")
    line, data_ok, details = data_compare(cycle)
    resume = RESUME_RESULTS.get(cycle)
    if not (counts_ok and data_ok):
        verdict = "MISMATCH"
    elif resume and resume.startswith("FAIL"):
        # The data is right, but the stopped snapshot was read again from its first row.
        verdict = "NO RESUME"
    else:
        verdict = "MATCH"
    message = (
        f"CYCLE {cycle} {verdict} | first count: {first}"
        + (" | extra continuous run, then" if extra else "")
        + f" | count compare: {final} | {line}"
        + (f" | resume: {resume}" if resume else "")
    )
    event(cycle, "result", message)
    save_result(cycle, verdict, first, extra, final, line, details)
    summary.append((cycle, verdict))
    if verdict != "MATCH" and STOP_ON_MISMATCH and cycle < CYCLES:
        event(
            cycle,
            "cancel",
            f"cycle {cycle} {verdict}: cancelling cycles {cycle + 1}-{CYCLES} so it can be "
            f"diagnosed; the container is not restarted and pipeline {PIPELINE_NAME} stays "
            f"stopped. The differing tables and key examples are in this cycle's row in "
            f"{RESULTS} (details); a NO RESUME cycle's resume event says what was found.",
        )
        break

mismatched = [c for c, v in summary if v != "MATCH"]
cancelled = len(summary) < CYCLES
event(
    0,
    "summary",
    (
        f"stopped after cycle {len(summary)} of {CYCLES} (not MATCH): "
        if cancelled
        else "all cycles done: "
    )
    + ", ".join(f"cycle {c} {v}" for c, v in summary)
    + (
        f"; cycles that did not end MATCH: {mismatched}"
        if mismatched
        else "; every cycle ended MATCH"
    ),
)
dbutils.notebook.exit(
    json.dumps(
        {
            "run_id": RUN_ID,
            "cycles": summary,
            "mismatched": mismatched,
            "cancelled_after": summary[-1][0] if cancelled else None,
        }
    )
)
