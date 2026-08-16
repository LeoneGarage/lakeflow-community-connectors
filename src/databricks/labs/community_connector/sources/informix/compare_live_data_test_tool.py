#!/usr/bin/env python3
"""Manually compare AWS-hosted Informix tables with Lakeflow destination tables.

The script creates pipe-delimited Informix UNLOAD files, extracts the matching
Databricks columns as exact strings, compares rows by Informix primary key, and
writes a JSON report. Temporary EC2 and S3 transfer artifacts are removed.

The Informix export and the Databricks reads target different systems, so they
overlap: the destination schema is read while the UNLOAD runs. Each table then
runs its own fetch-then-compare pipeline across ``--workers``, so a table is
compared as soon as it is ready instead of waiting for every other table's fetch
to finish. The only ordering constraint is that destination paging needs each
table's primary-key column count from the export.

The ``_test_`` filename intentionally excludes this standalone developer tool
from the generated single-file connector runtime.
"""

from __future__ import annotations

import argparse
import collections
import concurrent.futures
import datetime as datetime_module
import decimal
import hashlib
import hmac
import json
import pathlib
import shlex
import subprocess
import tarfile
import tempfile
import threading
import time
import urllib.parse
import uuid
from typing import Any


DEFAULT_TABLES = (
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
)
INTERNAL_COLUMNS = {
    "_informix_change_lsn",
    "_informix_commit_lsn",
    "_informix_tx_id",
    "_informix_op",
}
_PROGRESS_LOCK = threading.Lock()


def progress(message: str) -> None:
    with _PROGRESS_LOCK:
        print(message, flush=True)


def run(command: list[str], *, input_text: str | None = None) -> str:
    result = subprocess.run(command, input=input_text, text=True, capture_output=True, check=False)
    if result.returncode:
        raise RuntimeError(
            f"Command failed ({result.returncode}): {shlex.join(command)}\n"
            f"{result.stderr.strip()}"
        )
    return result.stdout


def _profile_flags(args: argparse.Namespace) -> list[str]:
    """Return ``--profile <name>`` unless running on ambient credentials.

    With ``--ambient-creds`` (``aws_profile`` is None) every AWS call uses the
    default credential chain, mirroring how the count-comparison helper drives
    the same host without an SSO profile.
    """

    return ["--profile", args.aws_profile] if args.aws_profile else []


def aws(args: argparse.Namespace, *command: str) -> str:
    return run(["aws", *command, "--region", args.aws_region, *_profile_flags(args)])


def resolve_instance_id(args: argparse.Namespace) -> str:
    """Return the running instance to drive, preferring an explicit --instance-id.

    The sandbox host is periodically rebuilt with a new instance ID, so pinning
    one in a default goes stale silently and surfaces later as an opaque
    ``InvalidInstanceId`` from SSM. Look the current host up by its Name tag
    instead, and keep ``--instance-id`` for the case where several candidates
    exist or a specific host is wanted.
    """

    if args.instance_id:
        return args.instance_id
    raw = aws(
        args,
        "ec2",
        "describe-instances",
        "--filters",
        f"Name=tag:Name,Values={args.instance_name}",
        "Name=instance-state-name,Values=running",
        "--query",
        "Reservations[].Instances[].InstanceId",
        "--output",
        "text",
    )
    candidates = raw.split()
    if not candidates:
        raise RuntimeError(
            f"No running EC2 instance tagged Name={args.instance_name!r} in "
            f"{args.aws_region}; pass --instance-id explicitly."
        )
    if len(candidates) > 1:
        raise RuntimeError(
            f"Multiple running instances tagged Name={args.instance_name!r}: "
            f"{' '.join(candidates)}; pass --instance-id to choose one."
        )
    progress(f"Resolved {args.instance_name} to {candidates[0]}")
    return candidates[0]


def send_ssm(args: argparse.Namespace, command: str, timeout: int = 3600) -> str:
    request = {
        "InstanceIds": [args.instance_id],
        "DocumentName": "AWS-RunShellScript",
        "Parameters": {"commands": [command]},
        "TimeoutSeconds": timeout,
    }
    with tempfile.NamedTemporaryFile("w", suffix=".json") as handle:
        json.dump(request, handle)
        handle.flush()
        return aws(
            args,
            "ssm",
            "send-command",
            "--cli-input-json",
            f"file://{handle.name}",
            "--query",
            "Command.CommandId",
            "--output",
            "text",
        ).strip()


def wait_ssm(args: argparse.Namespace, command_id: str, timeout: int = 3600) -> str:
    deadline = time.monotonic() + timeout
    previous_state = None
    while True:
        raw = aws(
            args,
            "ssm",
            "get-command-invocation",
            "--command-id",
            command_id,
            "--instance-id",
            args.instance_id,
            "--output",
            "json",
        )
        result = json.loads(raw)
        state = result["Status"]
        if state != previous_state:
            progress(f"SSM {command_id}: {state}")
            previous_state = state
        if state == "Success":
            return result.get("StandardOutputContent", "")
        if state in {"Cancelled", "Failed", "TimedOut", "Cancelling"}:
            raise RuntimeError(
                f"SSM command {command_id} ended as {state}: "
                f"{result.get('StandardErrorContent', '').strip()}"
            )
        if time.monotonic() >= deadline:
            raise TimeoutError(f"SSM command {command_id} did not finish in {timeout}s")
        time.sleep(2)


def exported_credentials(args: argparse.Namespace) -> dict[str, str]:
    return json.loads(run(["aws", "configure", "export-credentials", *_profile_flags(args)]))


def presigned_put_url(credentials: dict[str, str], bucket: str, key: str, region: str) -> str:
    now = datetime_module.datetime.now(datetime_module.timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date = now.strftime("%Y%m%d")
    scope = f"{date}/{region}/s3/aws4_request"
    host = f"{bucket}.s3.{region}.amazonaws.com"
    path = "/" + urllib.parse.quote(key, safe="/-_.~")
    query = {
        "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
        "X-Amz-Credential": f"{credentials['AccessKeyId']}/{scope}",
        "X-Amz-Date": amz_date,
        "X-Amz-Expires": "900",
        "X-Amz-SignedHeaders": "host",
    }
    # The session token is only present for temporary (STS/SSO) credentials;
    # static IAM keys presign correctly without it, so only sign it in when set.
    session_token = credentials.get("SessionToken")
    if session_token:
        query["X-Amz-Security-Token"] = session_token
    canonical_query = urllib.parse.urlencode(sorted(query.items()), safe="-_.~")
    canonical_request = "\n".join(
        ["PUT", path, canonical_query, f"host:{host}\n", "host", "UNSIGNED-PAYLOAD"]
    )
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            scope,
            hashlib.sha256(canonical_request.encode()).hexdigest(),
        ]
    )

    def sign(key_bytes: bytes, message: str) -> bytes:
        return hmac.new(key_bytes, message.encode(), hashlib.sha256).digest()

    key_date = sign(("AWS4" + credentials["SecretAccessKey"]).encode(), date)
    key_region = sign(key_date, region)
    key_service = sign(key_region, "s3")
    signing_key = sign(key_service, "aws4_request")
    signature = hmac.new(signing_key, string_to_sign.encode(), hashlib.sha256).hexdigest()
    return f"https://{host}{path}?{canonical_query}&X-Amz-Signature={signature}"


def validate_identifier(value: str) -> str:
    if not value or not value.replace("_", "a").isalnum() or value[0].isdigit():
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return value


def source_export_command(args: argparse.Namespace, run_id: str) -> str:
    tables = tuple(validate_identifier(table) for table in args.tables)
    source_owner = validate_identifier(args.source_owner)
    remote = f"/tmp/{run_id}"
    sql_statements = [
        f"UNLOAD TO '{remote}/{table}.unl' DELIMITER '|' " f"SELECT * FROM {source_owner}.{table};"
        for table in tables
    ]
    table_literals = ",".join(f"'{table}'" for table in tables)
    sql_statements.append(
        f"UNLOAD TO '{remote}/primary_keys.unl' DELIMITER '|' "
        "SELECT t.tabname,i.part1,i.part2,i.part3,i.part4,i.part5,i.part6,"
        "i.part7,i.part8,i.part9,i.part10,i.part11,i.part12,i.part13,i.part14,"
        "i.part15,i.part16 FROM systables t,sysconstraints c,sysindexes i "
        "WHERE c.tabid=t.tabid AND c.constrtype='P' AND i.tabid=t.tabid "
        f"AND i.idxname=c.idxname AND t.tabname IN ({table_literals}) "
        "ORDER BY t.tabname;"
    )
    sql = "\n".join(sql_statements) + "\n"
    encoded_sql = __import__("base64").b64encode(sql.encode()).decode()
    environment = (
        "-e INFORMIXDIR=/opt/ibm/informix "
        f"-e INFORMIXSERVER={shlex.quote(args.informix_server)} "
        "-e LD_LIBRARY_PATH=/opt/ibm/informix/lib:/opt/ibm/informix/lib/esql"
    )
    return "; ".join(
        [
            "set -e",
            f"rm -rf {shlex.quote(remote)} {shlex.quote(remote + '.tar.gz')}",
            f"mkdir -p {shlex.quote(remote)}",
            f"docker exec {shlex.quote(args.container)} rm -rf {shlex.quote(remote)}",
            f"docker exec {shlex.quote(args.container)} mkdir -p {shlex.quote(remote)}",
            f"printf %s {shlex.quote(encoded_sql)} | base64 -d > {shlex.quote(remote + '/export.sql')}",
            f"docker cp {shlex.quote(remote + '/export.sql')} "
            f"{shlex.quote(args.container + ':' + remote + '/export.sql')}",
            f"docker exec {environment} {shlex.quote(args.container)} "
            f"/opt/ibm/informix/bin/dbaccess {shlex.quote(args.database)} "
            f"{shlex.quote(remote + '/export.sql')}",
            f"rm {shlex.quote(remote + '/export.sql')}",
            f"docker cp {shlex.quote(args.container + ':' + remote + '/.')} "
            f"{shlex.quote(remote + '/')}",
            f"tar -C /tmp -czf {shlex.quote(remote + '.tar.gz')} {shlex.quote(run_id)}",
        ]
    )


def transfer_source_archive(args: argparse.Namespace, run_id: str, output: pathlib.Path) -> None:
    key = f"{args.s3_prefix.rstrip('/')}/{run_id}.tar.gz"
    remote_archive = f"/tmp/{run_id}.tar.gz"
    url = presigned_put_url(exported_credentials(args), args.s3_bucket, key, args.aws_region)
    upload = (
        "curl --fail --silent --show-error --request PUT "
        f"--upload-file {shlex.quote(remote_archive)} {shlex.quote(url)}"
    )
    wait_ssm(args, send_ssm(args, upload), 900)
    archive = output / f"{run_id}.tar.gz"
    aws(args, "s3", "cp", f"s3://{args.s3_bucket}/{key}", str(archive))
    with tarfile.open(archive, "r:gz") as handle:
        members = handle.getmembers()
        if any(
            not pathlib.PurePosixPath(member.name).parts
            or pathlib.PurePosixPath(member.name).parts[0] != run_id
            or ".." in pathlib.PurePosixPath(member.name).parts
            for member in members
        ):
            raise RuntimeError("Informix export archive contains an unsafe path")
        handle.extractall(output, members=members)


def databricks_query(args: argparse.Namespace, statement: str) -> list[dict[str, Any]]:
    raw = run(
        [
            "databricks",
            "experimental",
            "aitools",
            "tools",
            "query",
            statement,
            "--profile",
            args.databricks_profile,
        ]
    )
    return json.loads(raw)


def destination_schema(args: argparse.Namespace) -> list[dict[str, Any]]:
    tables = ",".join(f"'{validate_identifier(table)}'" for table in args.tables)
    return databricks_query(
        args,
        "SELECT table_name,column_name,ordinal_position,data_type "
        f"FROM {validate_identifier(args.catalog)}.information_schema.columns "
        f"WHERE table_schema='{validate_identifier(args.schema)}' "
        f"AND table_name IN ({tables}) ORDER BY table_name,ordinal_position",
    )


def destination_rows(
    args: argparse.Namespace, table: str, columns: list[str], key_count: int
) -> list[dict[str, Any]]:
    def quote(column: str) -> str:
        return "`" + column.replace("`", "``") + "`"

    projection = ",".join(
        f"CAST({quote(column)} AS STRING) AS {quote(column)}" for column in columns
    )
    ordering = columns[:key_count] or columns
    order_by = ",".join(quote(column) for column in ordering)
    rows: list[dict[str, Any]] = []
    page_number = 0
    while True:
        page_number += 1
        page = databricks_query(
            args,
            f"SELECT {projection} FROM {validate_identifier(args.catalog)}."
            f"{validate_identifier(args.schema)}.{quote(table)} "
            f"ORDER BY {order_by} LIMIT {args.destination_page_size} "
            f"OFFSET {len(rows)}",
        )
        rows.extend(page)
        progress(
            f"{table}: destination page {page_number} fetched "
            f"({len(page)} rows; {len(rows)} total)"
        )
        if len(page) < args.destination_page_size:
            return rows


def parse_unload_line(line: str) -> list[str]:
    values: list[str] = []
    value: list[str] = []
    escaped = False
    for character in line.rstrip("\n"):
        if escaped:
            value.append({"n": "\n", "r": "\r", "t": "\t"}.get(character, character))
            escaped = False
        elif character == "\\":
            escaped = True
        elif character == "|":
            values.append("".join(value))
            value = []
        else:
            value.append(character)
    if value:
        values.append("".join(value))
    return values


def normalize(value: Any, data_type: str) -> str | None:
    if value is None:
        return None
    text = str(value)
    if not text and data_type != "STRING":
        return None
    if data_type.startswith("DECIMAL"):
        number = decimal.Decimal(text)
        return "0" if number == 0 else format(number.normalize(), "f")
    if data_type in {"INT", "BIGINT", "SMALLINT", "TINYINT"}:
        return str(int(text))
    if data_type in {"FLOAT", "DOUBLE"}:
        return format(decimal.Decimal(text).normalize(), "f")
    if data_type == "TIMESTAMP":
        timestamp = text.replace("T", " ").replace("Z", "+00:00")
        if "." in timestamp:
            head, fraction = timestamp.split(".", 1)
            suffix = ""
            if "+" in fraction:
                fraction, suffix = fraction.split("+", 1)
                suffix = "+" + suffix
            timestamp = f"{head}.{(fraction + '000000')[:6]}{suffix}"
        parsed = datetime_module.datetime.fromisoformat(timestamp)
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(datetime_module.timezone.utc).replace(tzinfo=None)
        return parsed.isoformat(sep=" ", timespec="microseconds")
    if data_type == "DATE":
        try:
            return datetime_module.date.fromisoformat(text).isoformat()
        except ValueError:
            return datetime_module.datetime.strptime(text, "%m/%d/%Y").date().isoformat()
    if data_type == "BOOLEAN":
        return "true" if text.lower() in {"t", "true", "1"} else "false"
    return text


def pipe_value(value: Any) -> str:
    if value is None:
        return "\\N"
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace("|", "\\|")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )


def primary_key_counts(path: pathlib.Path) -> dict[str, int]:
    result = {}
    for line in path.read_text().splitlines():
        values = parse_unload_line(line)
        if not values:
            continue
        result[values[0]] = sum(1 for value in values[1:] if int(value or 0) != 0)
    return result


def compare_table(
    table: str,
    source_file: pathlib.Path,
    destination: list[dict[str, Any]],
    columns: list[str],
    types: dict[str, str],
    key_count: int,
    destination_pipe: pathlib.Path,
) -> dict[str, Any]:
    source = []
    for number, line in enumerate(source_file.read_text().splitlines(True), 1):
        values = parse_unload_line(line)
        if len(values) != len(columns):
            raise RuntimeError(
                f"{table}:{number} has {len(values)} fields; expected {len(columns)}"
            )
        source.append(dict(zip(columns, values)))
    with destination_pipe.open("w") as handle:
        for row in destination:
            handle.write("|".join(pipe_value(row.get(column)) for column in columns) + "|\n")

    key_columns = columns[:key_count]
    key = lambda row: tuple(normalize(row.get(c), types[c]) for c in key_columns)
    source_by_key = {key(row): row for row in source}
    destination_by_key = {key(row): row for row in destination}
    source_keys, destination_keys = set(source_by_key), set(destination_by_key)
    missing = sorted(source_keys - destination_keys, key=repr)
    extra = sorted(destination_keys - source_keys, key=repr)
    mismatched_rows = mismatched_cells = 0
    by_column: collections.Counter[str] = collections.Counter()
    examples = []
    for row_key in sorted(source_keys & destination_keys, key=repr):
        differences = []
        for column in columns:
            left = normalize(source_by_key[row_key].get(column), types[column])
            right = normalize(destination_by_key[row_key].get(column), types[column])
            if left != right:
                mismatched_cells += 1
                by_column[column] += 1
                differences.append(
                    {
                        "column": column,
                        "source": source_by_key[row_key].get(column),
                        "destination": destination_by_key[row_key].get(column),
                    }
                )
        if differences:
            mismatched_rows += 1
            if len(examples) < 5:
                examples.append({"key": row_key, "differences": differences[:10]})
    return {
        "table": table,
        "source_rows": len(source),
        "destination_rows": len(destination),
        "missing_rows": len(missing),
        "extra_rows": len(extra),
        "mismatched_rows": mismatched_rows,
        "mismatched_cells": mismatched_cells,
        "duplicate_source_keys": len(source) - len(source_by_key),
        "duplicate_destination_keys": len(destination) - len(destination_by_key),
        "mismatches_by_column": dict(by_column),
        "missing_key_examples": missing[:5],
        "extra_key_examples": extra[:5],
        "mismatch_examples": examples,
    }


def cleanup(args: argparse.Namespace, run_id: str) -> None:
    key = f"{args.s3_prefix.rstrip('/')}/{run_id}.tar.gz"
    subprocess.run(
        [
            "aws",
            "s3",
            "rm",
            f"s3://{args.s3_bucket}/{key}",
            "--region",
            args.aws_region,
            *_profile_flags(args),
        ],
        capture_output=True,
        text=True,
    )
    remote = f"/tmp/{run_id}"
    command = (
        f"rm -rf {shlex.quote(remote)} {shlex.quote(remote + '.tar.gz')}; "
        f"docker exec {shlex.quote(args.container)} rm -rf {shlex.quote(remote)}"
    )
    try:
        wait_ssm(args, send_ssm(args, command, 300), 300)
    except Exception as error:
        progress(f"Warning: temporary remote cleanup failed: {error}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aws-profile", default="aws-sandbox-field-eng_databricks-sandbox-admin")
    # Drop the profile entirely and use the default credential chain. Handy for
    # unattended runs where no SSO session exists: the ambient IAM keys reach the
    # same SSM host and S3 bucket, and presigning omits the (absent) session token.
    parser.add_argument(
        "--ambient-creds",
        action="store_const",
        const=None,
        dest="aws_profile",
        help="Use default AWS credentials instead of --aws-profile (no SSO required).",
    )
    parser.add_argument("--aws-region", default="us-east-1")
    # Resolved from --instance-name at run time unless given explicitly; the
    # sandbox host is rebuilt periodically, so a pinned ID goes stale.
    parser.add_argument("--instance-id", default=None)
    parser.add_argument("--instance-name", default="informix-cdc")
    parser.add_argument("--container", default="informix-cdc")
    parser.add_argument("--database", default="testdb")
    parser.add_argument("--source-owner", default="informix")
    parser.add_argument("--informix-server", default="informix_tcp")
    parser.add_argument("--databricks-profile", default="demo1")
    parser.add_argument("--catalog", default="members")
    parser.add_argument("--schema", default="connector_bronze")
    parser.add_argument("--s3-bucket", default="leone-sandbox-metastore")
    parser.add_argument("--s3-prefix", default="tmp/informix-comparisons")
    parser.add_argument("--output", type=pathlib.Path)
    parser.add_argument("--workers", type=int, default=20)
    parser.add_argument("--destination-page-size", type=int, default=10000)
    parser.add_argument("--fail-on-difference", action="store_true")
    parser.add_argument("--tables", nargs="+", default=list(DEFAULT_TABLES))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.workers < 1 or args.destination_page_size < 1:
        raise ValueError("--workers and --destination-page-size must be positive")
    # Resolve once, before any SSM call, so every command in this run targets the
    # same host even if the sandbox is rebuilt mid-run.
    args.instance_id = resolve_instance_id(args)
    run_id = f"informix-compare-{uuid.uuid4().hex}"
    output = args.output or pathlib.Path("/tmp") / run_id
    output.mkdir(parents=True, exist_ok=True)
    try:
        # The Informix UNLOAD and the Databricks extraction touch different
        # systems, so run them concurrently. Only the destination row fetch
        # genuinely depends on the source: it needs each table's primary-key
        # column count to build a deterministic ORDER BY for OFFSET paging, and
        # that count comes from the exported primary_keys.unl. Reading the
        # destination schema does not, so it overlaps the export entirely, and
        # each destination table starts fetching as soon as the export lands.
        source_root = output / run_id

        def export_source() -> dict[str, int]:
            progress("Exporting Informix tables...")
            wait_ssm(args, send_ssm(args, source_export_command(args, run_id)))
            progress("Transferring Informix export...")
            transfer_source_archive(args, run_id, output)
            return primary_key_counts(source_root / "primary_keys.unl")

        def read_schema() -> list[dict[str, Any]]:
            progress("Reading destination schema...")
            return destination_schema(args)

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            source_future = pool.submit(export_source)
            schema_future = pool.submit(read_schema)
            # Surface a schema failure without waiting out the whole UNLOAD, but
            # always join the export so its thread cannot outlive cleanup().
            try:
                schema = schema_future.result()
            except BaseException:
                source_future.exception()
                raise
            keys = source_future.result()

        (output / "destination_schema.json").write_text(json.dumps(schema, indent=2))
        columns: dict[str, list[str]] = collections.defaultdict(list)
        types: dict[str, dict[str, str]] = collections.defaultdict(dict)
        for row in schema:
            if row["column_name"] in INTERNAL_COLUMNS:
                continue
            columns[row["table_name"]].append(row["column_name"])
            types[row["table_name"]][row["column_name"]] = row["data_type"].upper()
        missing_schema = sorted(set(args.tables) - set(columns))
        if missing_schema:
            raise RuntimeError(f"Destination tables are missing: {missing_schema}")
        # Compare each table as soon as that table is ready rather than waiting
        # for every extraction to finish. The source UNLOAD lands all files at
        # once, so a table becomes comparable the moment its own destination
        # fetch completes; chaining extract->compare per table lets comparison
        # of finished tables overlap the fetches still in flight.
        progress("Extracting and comparing tables...")
        results_by_table: dict[str, dict[str, Any]] = {}

        def extract_and_compare(table: str) -> dict[str, Any]:
            rows = destination_rows(args, table, columns[table], keys[table])
            progress(f"{table}: destination extraction complete ({len(rows)} rows)")
            return compare_table(
                table,
                source_root / f"{table}.unl",
                rows,
                columns[table],
                types[table],
                keys[table],
                output / f"{table}.destination.unl",
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(extract_and_compare, table): table for table in args.tables}
            for future in concurrent.futures.as_completed(futures):
                table = futures[future]
                result = results_by_table[table] = future.result()
                progress(
                    f"{table}: source={result['source_rows']} destination={result['destination_rows']} "
                    f"missing={result['missing_rows']} extra={result['extra_rows']} "
                    f"mismatched_rows={result['mismatched_rows']}"
                )
        report = [results_by_table[table] for table in sorted(args.tables)]
        report_path = output / "comparison.json"
        report_path.write_text(json.dumps(report, indent=2, default=str))
        progress(f"Report: {report_path}")
        differences = any(
            row["missing_rows"] or row["extra_rows"] or row["mismatched_rows"] for row in report
        )
        return 1 if args.fail_on_difference and differences else 0
    finally:
        cleanup(args, run_id)


if __name__ == "__main__":
    raise SystemExit(main())
