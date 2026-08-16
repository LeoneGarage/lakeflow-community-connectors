#!/usr/bin/env python3
import argparse, concurrent.futures, json, subprocess, time
from datetime import datetime
from zoneinfo import ZoneInfo

SYD = ZoneInfo("Australia/Sydney")
UTC = ZoneInfo("UTC")
TABLES = [
    "archivefulfillment",
    "tw060",
    "tw060_ovhc",
    "tw070",
    "tw070d",
    "tw101",
    "tw101_ovhc",
    "tw109",
    "tw109_ovhc",
    "tw110",
    "tw110_ovhc",
    "tw111",
    "tw111d",
    "tw142_forcedmerge",
    "tw201",
    "tw202",
    "tw202d",
    "tw205",
    "tw208",
    "tw214",
    "tw221",
    "tw221d",
    "tw241",
    "tw241_ovhc",
    "tw295",
    "tw301",
    "tw303",
    "tw304",
    "tw316",
    "tw316_ovhc",
]
WH = "8fff545079c359bb"
_DEFAULT_RUN_START = "2026-08-03T19:52:28.251Z"  # last pipeline update creation_time
_ap = argparse.ArgumentParser(description="Informix snapshot/stream phase timing analysis")
_ap.add_argument(
    "--run-start",
    dest="run_start",
    default=_DEFAULT_RUN_START,
    help="UTC ISO timestamp of the pipeline start (create_update creation_time), "
    "e.g. 2026-08-03T19:52:28.251Z. DESCRIBE HISTORY is filtered to writes at/after this.",
)
RUN_START = _ap.parse_args().run_start
# "large" commit = snapshot chunk. Default snapshot.page.size is large; CDC batches
# are bounded by max.records.per.batch (small). Use a threshold well above CDC batch
# size to separate chunk-writes from streamed changes.
CHUNK_MIN = 500


def api(m, p, pl=None):
    a = ["databricks", "api", m, p, "--profile", "demo1"]
    if pl is not None:
        a += ["--json", json.dumps(pl)]
    return json.loads(subprocess.run(a, capture_output=True, text=True, check=True).stdout)


def q(stmt):
    d = api(
        "post",
        "/api/2.0/sql/statements",
        {"warehouse_id": WH, "wait_timeout": "50s", "statement": stmt},
    )
    sid = d["statement_id"]
    st = d["status"]["state"]
    while st in ("PENDING", "RUNNING"):
        time.sleep(2)
        d = api("get", f"/api/2.0/sql/statements/{sid}")
        st = d["status"]["state"]
    if st != "SUCCEEDED":
        raise RuntimeError(d["status"])
    return d.get("result", {}).get("data_array", [])


def rows_written(op, mj):
    if not mj:
        return 0
    try:
        m = json.loads(mj)
    except:
        return 0
    if op == "MERGE":
        return sum(
            int(m.get(k, "0") or 0)
            for k in ("numTargetRowsInserted", "numTargetRowsUpdated", "numTargetRowsDeleted")
        )
    return sum(
        int(m.get(k, "0") or 0)
        for k in ("numOutputRows", "numInsertedRows", "numUpdatedRows", "numDeletedRows")
    )


def analyze(t):
    hist = q(
        f"SELECT timestamp, operation, operationMetrics FROM (DESCRIBE HISTORY members.connector_bronze.`{t}`) WHERE timestamp >= '{RUN_START}' ORDER BY timestamp"
    )
    last_chunk_ts = None
    max_chunk = 0
    n_chunk = 0
    first_stream_after = None
    rows = []
    for ts, op, mj in hist:
        n = rows_written(op, mj)
        rows.append((ts, n))
    # last commit with a snapshot-sized write
    for ts, n in rows:
        if n >= CHUNK_MIN:
            last_chunk_ts = ts
            max_chunk = max(max_chunk, n)
            n_chunk += 1
    # first non-chunk (stream) write strictly after the last chunk
    if last_chunk_ts is not None:
        for ts, n in rows:
            if ts > last_chunk_ts and n > 0:
                first_stream_after = ts
                break
    return t, last_chunk_ts, n_chunk, max_chunk, first_stream_after


res = {}
with concurrent.futures.ThreadPoolExecutor(max_workers=15) as ex:
    for f in concurrent.futures.as_completed({ex.submit(analyze, t): t for t in TABLES}):
        t, lc, nc, mx, fs = f.result()
        res[t] = (lc, nc, mx, fs)


def syd(z):
    if z is None:
        return "-"
    return (
        datetime.fromisoformat(z.replace("Z", "+00:00")).astimezone(SYD).strftime("%m-%d %H:%M:%S")
    )


print(f"Run started: {syd(RUN_START)} AEST ({RUN_START})\n")
print(
    f"{'table':20} {'chunk commits':>13} {'max chunk rows':>15} {'last chunk write':>18} {'-> first stream':>18}"
)
print("-" * 90)
overall_switch = None
had_incremental = False
for t in TABLES:
    lc, nc, mx, fs = res[t]
    if nc > 0:
        had_incremental = True
    if lc and (overall_switch is None or lc > overall_switch):
        overall_switch = lc
    print(f"{t:20} {nc:>13} {mx:>15,} {syd(lc):>18} {syd(fs):>18}")
print("-" * 90)
if had_incremental:
    print(
        f"LAST table finished incremental (last chunk write): {syd(overall_switch)} AEST ({overall_switch})"
    )
else:
    print(
        "No snapshot-sized commits in this run window: the last run resumed in STREAM phase (no incremental)."
    )
