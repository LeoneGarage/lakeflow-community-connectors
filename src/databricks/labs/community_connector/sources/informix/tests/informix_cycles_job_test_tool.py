# Databricks notebook source
# Informix restart/run/compare cycles as a Databricks Job (serverless), so they run with no
# laptop attached. Each cycle: restart the demo Informix container and wait for its CDC
# workload; run informix-pipeline as a triggered full refresh and a triggered update of
# RUN_MINUTES each, N continuous updates of RUN_MINUTES each and one more of FINAL_MINUTES;
# compare counts, and if they differ run one more continuous update of EXTRA_MINUTES and
# compare again; then compare the data. The full refresh runs past RUN_MINUTES when the
# keyless tables (DRAIN_TABLES) have not drained by then, so their snapshots never restart.
#
# Every pipeline start and end, each cycle's verdict and the final summary go to the run's
# output and to main.informix_cycles.events / main.informix_cycles.cycles. The Informix side
# goes through AWS SSM on the demo host, with keys from the informix-cycles secret scope;
# the compare tools staged next to this notebook do the Informix work exactly as on a laptop.
#
# Parameters: cycles, run_minutes, continuous_runs, final_minutes (0 skips the final run),
# extra_minutes, and smoke=true, which checks AWS, SSM, the pipeline API, both compares and
# the event tables without restarting the container or running the pipeline.

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
CYCLES = int(dbutils.widgets.get("cycles"))
RUN_SECONDS = int(float(dbutils.widgets.get("run_minutes")) * 60)
CONTINUOUS_RUNS = int(dbutils.widgets.get("continuous_runs"))
FINAL_SECONDS = int(float(dbutils.widgets.get("final_minutes")) * 60)
EXTRA_SECONDS = int(float(dbutils.widgets.get("extra_minutes")) * 60)
SMOKE = dbutils.widgets.get("smoke") == "true"

PIPELINE_ID = "06fdbf02-06a2-491d-aaba-4cabd7b0d81f"
TARGET = "members.connector_bronze"
SCOPE = "informix-cycles"
SCHEMA = "main.informix_cycles"
EVENTS = f"{SCHEMA}.events"
RESULTS = f"{SCHEMA}.cycles"
TERMINAL = {"COMPLETED", "FAILED", "CANCELED"}
# Keyless tables whose one-pass snapshot restarts from its first row if the full-refresh
# run stops mid-drain: that run stops at RUN_MINUTES if both have drained by then, and
# otherwise waits until they have.
DRAIN_TABLES = ("tw900_member_tag", "tw901_claim_note")
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
    f"extra run {EXTRA_SECONDS // 60} min, smoke={SMOKE}",
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

API = WorkspaceClient().api_client
BASE = f"/api/2.0/pipelines/{PIPELINE_ID}"


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
        return state
    event(
        cycle, "stopping", f"{label}: stopping update {update_id} after {minutes} minutes ({state})"
    )
    state = stop_update(cycle, label, update_id)
    event(cycle, "ended", f"{label}: update {update_id} final state {state}")
    return state


# COMMAND ----------
# The cycle's steps.
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
    for run, full_refresh in ((1, True), (2, False)):
        kind = "triggered full-refresh" if full_refresh else "triggered"
        event(cycle, "starting", f"run {run}: starting {kind} update")
        update_id = start_update(cycle, f"run {run}", full_refresh)
        event(cycle, "started", f"run {run}: started {kind} update {update_id}")
        run_for(cycle, f"run {run}", update_id, RUN_SECONDS, until_drained=full_refresh)
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
    # One more continuous run; an update that fails early starts again for the rest of it.
    deadline = time.time() + EXTRA_SECONDS
    for attempt in range(1, 6):
        event(cycle, "starting", f"extra run: starting continuous update (attempt {attempt})")
        update_id = start_update(cycle, "extra run")
        event(cycle, "started", f"extra run: started continuous update {update_id}")
        state = run_for(cycle, "extra run", update_id, max(60, int(deadline - time.time())), False)
        if state != "FAILED" or time.time() > deadline - 60:
            return


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
    sys.argv = ["compare_live_data_test_tool", "--output", str(output)]
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
    verdict = "MATCH" if counts_ok and data_ok else "MISMATCH"
    message = (
        f"CYCLE {cycle} {verdict} | first count: {first}"
        + (" | extra continuous run, then" if extra else "")
        + f" | count compare: {final} | {line}"
    )
    event(cycle, "result", message)
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
    summary.append((cycle, verdict))

mismatched = [c for c, v in summary if v != "MATCH"]
event(
    0,
    "summary",
    "all cycles done: "
    + ", ".join(f"cycle {c} {v}" for c, v in summary)
    + (
        f"; cycles with mismatched data: {mismatched}"
        if mismatched
        else "; no cycle ended with mismatched data"
    ),
)
dbutils.notebook.exit(json.dumps({"run_id": RUN_ID, "cycles": summary, "mismatched": mismatched}))
