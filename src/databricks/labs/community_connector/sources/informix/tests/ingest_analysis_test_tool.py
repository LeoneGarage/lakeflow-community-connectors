#!/usr/bin/env python3
"""Ingest one analysis run's results into the dashboard Delta tables.

Reads the artifacts the existing tools produce and appends rows tagged with a
shared run_ts to:
  members.connector_bronze.analysis_count_compare
  members.connector_bronze.analysis_data_compare
  members.connector_bronze.analysis_s3_cost

Usage:
  python3 /tmp/ingest_analysis.py \
      --run-ts 2026-08-03T21:27:00 \
      --count-json /tmp/run_counts.json \
      --data-json /tmp/informix-compare-XXXX/comparison.json \
      --s3-out /tmp/s3_1957_2127.out \
      --s3-window-start 2026-08-03T19:57 --s3-window-end 2026-08-03T21:27

Any of --count-json/--data-json/--s3-out may be omitted; only provided ones ingest.

count-json format (produced by a small tweak to count_compare, or hand-built):
  [{"table_name":"tw060","informix_count":1000,"pipeline_count":1000}, ...]
data-json: the comparison.json from compare_live_data_test_tool.py (list of dicts).
s3-out: the stdout of /tmp/s3_slot_cost.py (parsed for the TOTAL/Per-hour/Per-30d lines).
"""

from __future__ import annotations
import argparse, json, re, subprocess, sys

WH = "8fff545079c359bb"
CAT_SCH = "members.connector_bronze"


def run_sql(stmt: str) -> None:
    payload = {"warehouse_id": WH, "wait_timeout": "50s", "statement": stmt}
    raw = subprocess.run(
        [
            "databricks",
            "api",
            "post",
            "/api/2.0/sql/statements",
            "-p",
            "demo1",
            "--json",
            json.dumps(payload),
        ],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    d = json.loads(raw)
    st = d.get("status", {})
    if st.get("state") != "SUCCEEDED":
        raise RuntimeError(f"SQL failed: {st.get('state')} {st.get('error')}\n{stmt[:300]}")


def sql_str(v):
    if v is None:
        return "NULL"
    return "'" + str(v).replace("'", "''") + "'"


def ingest_counts(run_ts: str, path: str) -> int:
    rows = json.load(open(path))
    values = []
    for r in rows:
        inf = r["informix_count"]
        pipe = r["pipeline_count"]
        diff = inf - pipe
        values.append(
            f"(TIMESTAMP{sql_str(run_ts)}, {sql_str(r['table_name'])}, {inf}, {pipe}, "
            f"{diff}, {str(diff == 0).upper()})"
        )
    run_sql(
        f"INSERT INTO {CAT_SCH}.analysis_count_compare "
        f"(run_ts, table_name, informix_count, pipeline_count, diff, matched) VALUES "
        + ",".join(values)
    )
    return len(values)


def ingest_data(run_ts: str, path: str) -> int:
    rows = json.load(open(path))
    values = []
    for t in rows:
        values.append(
            f"(TIMESTAMP{sql_str(run_ts)}, {sql_str(t['table'])}, {t['source_rows']}, "
            f"{t['destination_rows']}, {t['missing_rows']}, {t['extra_rows']}, "
            f"{t['mismatched_rows']}, {t['mismatched_cells']})"
        )
    run_sql(
        f"INSERT INTO {CAT_SCH}.analysis_data_compare "
        f"(run_ts, table_name, source_rows, destination_rows, missing_rows, extra_rows, "
        f"mismatched_rows, mismatched_cells) VALUES " + ",".join(values)
    )
    return len(values)


def ingest_s3(run_ts: str, out_path: str, wstart: str, wend: str) -> None:
    text = open(out_path).read()

    def grab(pat, cast=float):
        m = re.search(pat, text)
        return cast(m.group(1).replace(",", "")) if m else None

    total_req = grab(r"Slot-prefix S3 requests in window:\s*([\d,]+)", int)
    # "Per hour:  162,177 requests, $0.0791"
    per_hour = grab(r"Per hour:\s*[\d,]+ requests,\s*\$([0-9.]+)")
    per_30d = grab(r"Per 30d:\s*[\d,]+ requests,\s*\$([0-9.]+)")
    window_cost = grab(r"TOTAL\s+[\d,]+\s+([0-9.]+)")
    # window hours from the header "( 1.00 h)" or compute from timestamps
    wh = grab(r"\(([0-9.]+)\s*h\)")
    run_sql(
        f"INSERT INTO {CAT_SCH}.analysis_s3_cost "
        f"(run_ts, window_start_utc, window_end_utc, window_hours, total_requests, "
        f"window_cost_usd, cost_per_hour_usd, cost_per_30d_usd) VALUES "
        f"(TIMESTAMP{sql_str(run_ts)}, {sql_str(wstart)}, {sql_str(wend)}, "
        f"{wh if wh is not None else 'NULL'}, {total_req if total_req is not None else 'NULL'}, "
        f"{window_cost if window_cost is not None else 'NULL'}, "
        f"{per_hour if per_hour is not None else 'NULL'}, "
        f"{per_30d if per_30d is not None else 'NULL'})"
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-ts", required=True, help="UTC ISO, e.g. 2026-08-03T21:27:00")
    ap.add_argument("--count-json")
    ap.add_argument("--data-json")
    ap.add_argument("--s3-out")
    ap.add_argument("--s3-window-start")
    ap.add_argument("--s3-window-end")
    a = ap.parse_args()
    if a.count_json:
        n = ingest_counts(a.run_ts, a.count_json)
        print(f"count_compare: {n} rows")
    if a.data_json:
        n = ingest_data(a.run_ts, a.data_json)
        print(f"data_compare: {n} rows")
    if a.s3_out:
        ingest_s3(a.run_ts, a.s3_out, a.s3_window_start, a.s3_window_end)
        print("s3_cost: 1 row")
    return 0


if __name__ == "__main__":
    sys.exit(main())
