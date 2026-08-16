#!/usr/bin/env python3
"""Empirical S3 request-cost analysis (Option B) for the connection-slot volume.

Parses S3 server access logs for a time window, counts requests hitting the
.informix-connection-slots/ prefix by operation, and prices them at us-east-1
S3 Standard request rates.

The connection-slot volume is not hardcoded: pass the UC volume that holds the
shared CDC state (`--slot-volume catalog.schema.volume`) and this tool resolves
its physical S3 location via `databricks volumes read`. From the returned
`storage_location` (s3://<data-bucket>/<metastore-uuid>/volumes/<volume-uuid>)
it derives, for the log scan:
  - SLOT_KEY_SUBSTR = /volumes/<volume-uuid>/.informix-connection-slots/
  - LOG_PREFIX      = <account>/<data-bucket>/   (server-access-log key prefix)
  - LOG_BUCKET      = databricks-s3-access-logs-<account>-<region>

Usage:
  python3 /tmp/s3_slot_cost.py --start 2026-08-03T09:02 --end 2026-08-03T10:02 \
      --slot-volume members.connector_bronze.informix_cdc_state
Times are UTC. Access-log delivery lags minutes-to-~1h, so run after the window.
"""

from __future__ import annotations
import argparse
import datetime as dt
import gzip
import json
import re
import subprocess
import sys
import tempfile
from collections import Counter

# Sensible defaults for the reference sandbox deployment; every one is derived
# at runtime from --slot-volume via resolve_slot_location() in main(). They are
# module-level so the scan/list helpers can read them after resolution.
LOG_BUCKET = "databricks-s3-access-logs-332745928618-us-east-1"
LOG_PREFIX = "332745928618/leone-sandbox-metastore/"
SLOT_KEY_SUBSTR = "/volumes/58ad2e05-fbe8-49c9-905c-4a9561157a5d/.informix-connection-slots/"
DEFAULT_SLOT_VOLUME = "members.connector_bronze.informix_cdc_state"


def resolve_slot_location(
    volume: str, region: str, databricks_profile: str | None
) -> tuple[str, str, str]:
    """Resolve (log_bucket, log_prefix, slot_key_substr) from a UC volume name.

    Looks up the volume's physical `storage_location` (an s3:// URI of the form
    s3://<data-bucket>/<metastore-uuid>/volumes/<volume-uuid>) and the caller's
    AWS account id, then assembles the three S3 facts the log scan needs. The
    access-log bucket follows Databricks' convention
    databricks-s3-access-logs-<account>-<region>, and log object keys are
    prefixed <account>/<data-bucket>/.
    """
    cmd = ["databricks", "volumes", "read", volume, "--output", "json"]
    if databricks_profile:
        cmd += ["--profile", databricks_profile]
    info = json.loads(subprocess.run(cmd, capture_output=True, text=True, check=True).stdout)
    storage = info["storage_location"]  # s3://bucket/<metastore>/volumes/<vol-uuid>
    if not storage.startswith("s3://"):
        raise ValueError(f"volume {volume} is not S3-backed: {storage}")
    path = storage[len("s3://") :].rstrip("/")
    data_bucket, *rest = path.split("/")
    volume_uuid = rest[-1]
    account = subprocess.run(
        ["aws", "sts", "get-caller-identity", "--query", "Account", "--output", "text"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    log_bucket = f"databricks-s3-access-logs-{account}-{region}"
    log_prefix = f"{account}/{data_bucket}/"
    slot_substr = f"/volumes/{volume_uuid}/.informix-connection-slots/"
    return log_bucket, log_prefix, slot_substr


PRICE_PUT_LIST = 0.005 / 1000
PRICE_GET_OTHER = 0.0004 / 1000
PRICE_DELETE = 0.0

OP_RE = re.compile(r"\b(?:REST|BATCH|WEBSITE|S3)\.(\w+)\.(\w+)\b")
TIME_RE = re.compile(r"\[(\d{2})/(\w{3})/(\d{4}):(\d{2}):(\d{2}):(\d{2})")
MON = {
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


def list_keys(start: dt.datetime, end: dt.datetime) -> list[str]:
    """List access-log object keys delivered within the window's hours.

    No widening: the metastore bucket logs ALL activity, so adjacent hours can
    hold thousands of unrelated objects; the in-line request-time filter in
    scan_key already scopes precisely. Delivery lag past the hour boundary is
    rare and would only drop a few late lines.
    """
    keys: list[str] = []
    cursor = start.replace(minute=0, second=0, microsecond=0)
    stop = end
    while cursor <= stop:
        prefix = LOG_PREFIX + cursor.strftime("%Y-%m-%d-%H")
        out = subprocess.run(
            [
                "aws",
                "s3api",
                "list-objects-v2",
                "--bucket",
                LOG_BUCKET,
                "--prefix",
                prefix,
                "--query",
                "Contents[].Key",
                "--output",
                "json",
            ],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        if out and out != "null":
            batch = json.loads(out)
            if batch:
                keys.extend(batch)
                print(f"  {prefix}: {len(batch)} objects")
        cursor += dt.timedelta(hours=1)
    return keys


def scan_key(key: str, start: dt.datetime, end: dt.datetime) -> Counter:
    counts: Counter = Counter()
    with tempfile.NamedTemporaryFile() as tmp:
        subprocess.run(
            ["aws", "s3", "cp", f"s3://{LOG_BUCKET}/{key}", tmp.name],
            capture_output=True,
            check=True,
        )
        with open(tmp.name, "rb") as handle:
            raw = handle.read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    for line in raw.decode("utf-8", "replace").splitlines():
        if SLOT_KEY_SUBSTR not in line:
            continue
        tmatch = TIME_RE.search(line)
        if tmatch:
            ts = dt.datetime(
                int(tmatch.group(3)),
                MON[tmatch.group(2)],
                int(tmatch.group(1)),
                int(tmatch.group(4)),
                int(tmatch.group(5)),
                int(tmatch.group(6)),
            )
            if not (start <= ts <= end):
                continue
        opmatch = OP_RE.search(line)
        if not opmatch:
            continue
        counts[f"{opmatch.group(1)}.{opmatch.group(2)}"] += 1
        counts["_total"] += 1
    return counts


def price(counts: Counter):
    breakdown, total = {}, 0.0
    for op, n in counts.items():
        if op.startswith("_"):
            continue
        verb = op.split(".")[0]
        if verb in {"PUT", "COPY", "POST", "LIST"}:
            c = n * PRICE_PUT_LIST
        elif verb == "DELETE":
            c = n * PRICE_DELETE
        else:
            c = n * PRICE_GET_OTHER
        breakdown[op] = (n, c)
        total += c
    return total, breakdown


def main() -> int:
    global LOG_BUCKET, LOG_PREFIX, SLOT_KEY_SUBSTR
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--start",
        required=True,
        help="Window start. UTC unless --timezone is given, in which case it is local "
        "wall-clock time in that zone.",
    )
    ap.add_argument("--end", default=None, help="Window end; defaults to now. See --start.")
    ap.add_argument(
        "--timezone",
        default=None,
        help="IANA zone (e.g. Australia/Sydney) that --start/--end are expressed in. "
        "Without it both are read as UTC, which is the historical behaviour.",
    )
    ap.add_argument(
        "--slot-volume",
        default=DEFAULT_SLOT_VOLUME,
        help="UC volume (catalog.schema.volume) holding the connection slots; "
        "its physical S3 location is resolved to derive the log bucket/prefix "
        "and slot key substring.",
    )
    ap.add_argument("--region", default="us-east-1", help="AWS region of the access-log bucket")
    ap.add_argument("--databricks-profile", default="demo1", help="databricks CLI profile")
    args = ap.parse_args()
    LOG_BUCKET, LOG_PREFIX, SLOT_KEY_SUBSTR = resolve_slot_location(
        args.slot_volume, args.region, args.databricks_profile
    )
    print(
        f"Slot volume {args.slot_volume} -> bucket {LOG_BUCKET}\n"
        f"  log prefix {LOG_PREFIX}\n  slot key {SLOT_KEY_SUBSTR}",
        flush=True,
    )
    start = dt.datetime.fromisoformat(args.start)
    end = dt.datetime.fromisoformat(args.end) if args.end else None
    if args.timezone:
        # Access-log key prefixes and the timestamps inside each line are both UTC, so
        # convert once here and leave every downstream comparison on UTC. Only values
        # the caller actually supplied are converted: a defaulted end is already UTC,
        # and running it through the zone would shift it by the offset.
        from zoneinfo import ZoneInfo

        zone = ZoneInfo(args.timezone)
        local_start, local_end = start, end

        def _to_utc(value):
            return value.replace(tzinfo=zone).astimezone(dt.timezone.utc).replace(tzinfo=None)

        start = _to_utc(start)
        if end is not None:
            end = _to_utc(end)
        print(
            f"Window ({args.timezone}): {local_start} -> "
            f"{local_end if local_end is not None else 'now'}",
            flush=True,
        )
    if end is None:
        end = dt.datetime.utcnow()
    if end <= start:
        raise SystemExit(f"--end ({args.end}) must be after --start ({args.start})")
    dur_h = (end - start).total_seconds() / 3600.0
    print(f"Window (UTC): {start} -> {end}  ({dur_h:.2f} h)", flush=True)
    print("Listing access-log objects...", flush=True)
    keys = list_keys(start, end)
    print(f"  total {len(keys)} objects to scan", flush=True)

    import concurrent.futures

    total = Counter()
    done = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as ex:
        futures = [ex.submit(scan_key, k, start, end) for k in keys]
        for fut in concurrent.futures.as_completed(futures):
            total.update(fut.result())
            done += 1
            if done % 200 == 0 or done == len(keys):
                print(
                    f"  scanned {done}/{len(keys)}  (slot reqs so far: {total.get('_total', 0)})",
                    flush=True,
                )

    n = total.get("_total", 0)
    print(f"\nSlot-prefix S3 requests in window: {n}")
    if not n:
        print("  (none found — logs may still be delivering; re-run shortly)")
        return 0
    cost, breakdown = price(total)
    print(f"{'operation':<20}{'count':>12}{'cost $':>14}")
    print("-" * 46)
    for op in sorted(breakdown, key=lambda o: -breakdown[o][0]):
        cnt, c = breakdown[op]
        print(f"{op:<20}{cnt:>12,}{c:>14.6f}")
    print("-" * 46)
    print(f"{'TOTAL':<20}{n:>12,}{cost:>14.6f}")
    if dur_h > 0:
        print(f"\nPer hour:  {n/dur_h:,.0f} requests, ${cost/dur_h:.4f}")
        print(f"Per 30d:   {n/dur_h*24*30:,.0f} requests, ${cost/dur_h*24*30:.2f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
