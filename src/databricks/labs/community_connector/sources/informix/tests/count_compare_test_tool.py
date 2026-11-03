#!/usr/bin/env python3
"""Compare row counts between Informix source tables and Lakeflow pipeline tables.

Reuses the connection helpers from compare_live_data_test_tool (SSM->EC2->dbaccess
for Informix, databricks SQL for the destination). Counts on both sides run in
parallel: one SSM command asks Informix for every table's COUNT(*) at once while
a thread pool issues the Databricks COUNT(*) queries concurrently.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import re
import subprocess
import sys
import time
from typing import Any

sys.path.insert(0, "src")
try:
    # Repo checkout / local dev: the connector package is importable.
    from databricks.labs.community_connector.sources.informix import (  # noqa: E402
        compare_live_data_test_tool as C,
    )
except ModuleNotFoundError:
    # Serverless job / standalone: compare_live_data_test_tool.py is staged next
    # to this file (it has no databricks.labs dependency of its own).
    import compare_live_data_test_tool as C  # noqa: E402

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
    # Keyless: tw900_member_tag is keyed on the whole row in informix-pipeline, and
    # tw901_claim_note is a change log of an insert-only table, so its target holds the
    # table's rows (the compare ignores the _informix_* columns).
    "tw900_member_tag",
    "tw901_claim_note",
]

PROFILE = "demo1"  # Databricks CLI profile of the demo workspace
PIPELINE_NAME = "informix-pipeline"
STARTER_WAREHOUSE = "Serverless Starter Warehouse"


def _databricks_json(profile: str, *command: str):
    done = subprocess.run(
        ["databricks", *command, "--profile", profile, "-o", "json"],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(done.stdout or "null")


def resolve_warehouse(profile: str = PROFILE, warehouse_id: str | None = None) -> str:
    """The given warehouse, else the workspace's only one or its Serverless Starter
    Warehouse, so the tools need no edit when the workspace is recreated."""
    if warehouse_id:
        return warehouse_id
    warehouses = _databricks_json(profile, "warehouses", "list") or []
    starter = [w for w in warehouses if w.get("name") == STARTER_WAREHOUSE]
    for candidates in (warehouses, starter):
        if len(candidates) == 1:
            return candidates[0]["id"]
    names = ", ".join(f"{w.get('name')} ({w.get('id')})" for w in warehouses) or "none"
    raise SystemExit(
        f"Can't choose a SQL warehouse on profile {profile} (found: {names}); pass --warehouse-id"
    )


def resolve_pipeline(
    profile: str = PROFILE, name: str = PIPELINE_NAME, pipeline_id: str | None = None
) -> str:
    """The given pipeline, else the one pipeline named ``name`` on the workspace."""
    if pipeline_id:
        return pipeline_id
    found = _databricks_json(
        profile, "pipelines", "list-pipelines", "--filter", f"name LIKE '{name}'"
    )
    matches = [p["pipeline_id"] for p in found or [] if p.get("name") == name]
    if len(matches) != 1:
        raise SystemExit(
            f"Expected one pipeline named {name} on profile {profile}, found {len(matches)}; "
            "pass --pipeline-id"
        )
    return matches[0]


def resolve_target(profile: str = PROFILE, pipeline_id: str | None = None) -> str:
    """The ``catalog.schema`` the pipeline writes to; older pipelines name it ``target``."""
    spec = _databricks_json(profile, "pipelines", "get", pipeline_id or resolve_pipeline(profile))[
        "spec"
    ]
    return f"{spec['catalog']}.{spec.get('schema') or spec['target']}"


def run_statement(profile: str, warehouse_id: str, statement: str) -> list[list[Any]]:
    """One statement's rows, polling while the warehouse is still starting or running it."""
    body = {"warehouse_id": warehouse_id, "wait_timeout": "50s", "statement": statement}
    result = _databricks_json(
        profile, "api", "post", "/api/2.0/sql/statements", "--json", json.dumps(body)
    )
    while result["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(3)
        result = _databricks_json(
            profile, "api", "get", f"/api/2.0/sql/statements/{result['statement_id']}"
        )
    if result["status"]["state"] != "SUCCEEDED":
        raise RuntimeError(f"{statement}: {result['status']}")
    return result.get("result", {}).get("data_array", [])


def make_args(instance_id: str) -> argparse.Namespace:
    a = argparse.Namespace()
    a.aws_profile = "aws-sandbox-field-eng_databricks-sandbox-admin"
    a.aws_region = "us-east-1"
    a.instance_id = instance_id
    a.instance_name = "informix-cdc"
    a.container = "informix-cdc"
    a.database = "testdb"
    a.source_owner = "informix"
    a.informix_server = "informix_tcp"
    a.databricks_profile = "demo1"
    a.catalog = "members"
    a.schema = "connector_bronze"
    return a


def informix_counts(args: argparse.Namespace) -> dict[str, int]:
    """One SSM call: run COUNT(*) for every table via dbaccess, parse output."""
    owner = C.validate_identifier(args.source_owner)
    stmts = "\n".join(
        f"SELECT '{C.validate_identifier(t)}' tbl, COUNT(*) cnt FROM {owner}.{C.validate_identifier(t)};"
        for t in TABLES
    )
    import base64

    encoded = base64.b64encode((stmts + "\n").encode()).decode()
    env = (
        "-e INFORMIXDIR=/opt/ibm/informix "
        f"-e INFORMIXSERVER={args.informix_server} "
        "-e LD_LIBRARY_PATH=/opt/ibm/informix/lib:/opt/ibm/informix/lib/esql"
    )
    remote = "/tmp/count_compare.sql"
    command = "; ".join(
        [
            "set -e",
            f"printf %s {encoded} | base64 -d > {remote}",
            f"docker cp {remote} {args.container}:{remote}",
            f"docker exec {env} {args.container} /opt/ibm/informix/bin/dbaccess {args.database} {remote}",
        ]
    )
    out = C.wait_ssm(args, C.send_ssm(args, command), 1800)
    # Each SELECT embeds the literal table name, so dbaccess prints a header
    # "tbl  cnt" then a data row "<table>  <number>". Match the data rows
    # directly against the known table names.
    known = set(TABLES)
    counts: dict[str, int] = {}
    for line in out.splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[0] in known and parts[1].isdigit():
            counts[parts[0]] = int(parts[1])
    return counts


def databricks_count(
    table: str, warehouse_id: str, target: str, profile: str = PROFILE
) -> tuple[str, int]:
    rows = run_statement(profile, warehouse_id, f"SELECT COUNT(*) FROM {target}.`{table}`")
    return table, int(rows[0][0])


def main() -> int:
    import argparse as _argparse

    _p = _argparse.ArgumentParser()
    _p.add_argument(
        "--json-out",
        help="Write per-table counts to this path as JSON "
        "([{table_name, informix_count, pipeline_count}, ...]) for dashboard ingestion.",
    )
    _p.add_argument(
        "--warehouse-id",
        help="SQL warehouse that runs the pipeline-side COUNT(*) queries (default: the "
        f"workspace's only warehouse, or its {STARTER_WAREHOUSE}).",
    )
    _p.add_argument(
        "--pipeline-id",
        help=f"Pipeline whose tables to count (default: the one named {PIPELINE_NAME}).",
    )
    _p.add_argument(
        "--profile", default=PROFILE, help=f"Databricks CLI profile (default {PROFILE})."
    )
    _cli = _p.parse_args()
    warehouse_id = resolve_warehouse(_cli.profile, _cli.warehouse_id)
    target = resolve_target(
        _cli.profile, resolve_pipeline(_cli.profile, pipeline_id=_cli.pipeline_id)
    )

    # The sandbox SSO profile is not logged in here, but the ambient default AWS
    # credentials (account 332745928618) can see the informix-cdc host. Patch the
    # tool's aws() helper to drop --profile so every SSM call uses those creds.
    C.aws = lambda args, *command: C.run(["aws", *command, "--region", args.aws_region])

    instance_id = subprocess.run(
        [
            "aws",
            "ec2",
            "describe-instances",
            "--region",
            "us-east-1",
            "--filters",
            "Name=tag:Name,Values=informix-cdc",
            "Name=instance-state-name,Values=running",
            "--query",
            "Reservations[].Instances[].InstanceId",
            "--output",
            "text",
        ],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()[0]
    args = make_args(instance_id)

    t0 = time.monotonic()
    # Run both sides concurrently: Informix as one SSM job, Databricks as a pool.
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(TABLES) + 1) as ex:
        inf_future = ex.submit(informix_counts, args)
        dbx_futures = {
            ex.submit(databricks_count, t, warehouse_id, target, _cli.profile): t for t in TABLES
        }
        dbx: dict[str, int] = {}
        for f in concurrent.futures.as_completed(dbx_futures):
            t, c = f.result()
            dbx[t] = c
        inf = inf_future.result()
    elapsed = time.monotonic() - t0

    print(
        f"\nCompared {len(TABLES)} tables in {elapsed:.1f}s "
        f"(instance {instance_id[:12]}, target {target})\n"
    )
    print(f"{'table':22} {'informix':>12} {'pipeline':>12} {'diff':>10}  status")
    print("-" * 72)
    mism = 0
    for t in TABLES:
        i = inf.get(t)
        p = dbx.get(t)
        if i is None or p is None:
            print(f"{t:22} {str(i):>12} {str(p):>12} {'?':>10}  MISSING")
            mism += 1
            continue
        diff = p - i
        status = "OK" if diff == 0 else "DIFF"
        if diff:
            mism += 1
        print(f"{t:22} {i:>12} {p:>12} {diff:>+10}  {status}")
    print("-" * 72)
    ti, tp = sum(v for v in inf.values() if v is not None), sum(dbx.values())
    print(f"{'TOTAL':22} {ti:>12} {tp:>12} {tp-ti:>+10}  " f"{mism} table(s) not matching")

    if _cli.json_out:
        out = [
            {"table_name": t, "informix_count": inf.get(t), "pipeline_count": dbx.get(t)}
            for t in TABLES
            if inf.get(t) is not None and dbx.get(t) is not None
        ]
        with open(_cli.json_out, "w") as handle:
            json.dump(out, handle)
        print(f"\nWrote {len(out)} table counts to {_cli.json_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
