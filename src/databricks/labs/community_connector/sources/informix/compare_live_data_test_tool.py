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
import codecs
import collections
import concurrent.futures
import datetime as datetime_module
import decimal
import hashlib
import hmac
import json
import math
import pathlib
import re
import shlex
import struct
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
    # Keyless: tw900_member_tag is keyed on the whole row in informix-pipeline, and
    # tw901_claim_note is a change log of an insert-only table, so its target holds the
    # table's rows (the compare ignores the _informix_* columns).
    "tw900_member_tag",
    "tw901_claim_note",
)
# Keyless tables the demo pipeline keys on the whole row (primary.keys "*"): their SCD1
# destination holds each distinct row once, so exact duplicates in the source land as one
# row, and their compare is by distinct rows rather than by row counts.
WHOLE_ROW_KEY_TABLES = ("tw221d", "tw900_member_tag")
INTERNAL_COLUMNS = {
    "_informix_change_lsn",
    "_informix_commit_lsn",
    "_informix_tx_id",
    "_informix_op",
    "_informix_commit_time",
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
    """Return ``--profile <name>`` when ``--aws-profile`` names one.

    By default (``aws_profile`` is None) every AWS call uses the default
    credential chain, mirroring how the count-comparison helper drives the same
    host without an SSO profile.
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
        try:
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
        except RuntimeError as error:
            # SSM can briefly answer InvocationDoesNotExist for a command it has just
            # accepted. That is still pending, not a failure: it failed a cycles job run
            # between a container restart and its first workload check.
            if "InvocationDoesNotExist" not in str(error) or time.monotonic() >= deadline:
                raise
            time.sleep(2)
            continue
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
        f"AND t.owner='{source_owner}' "
        "ORDER BY t.tabname;"
    )
    # The parts above are column numbers: their names, to key the compare by.
    sql_statements.append(
        f"UNLOAD TO '{remote}/key_columns.unl' DELIMITER '|' "
        "SELECT t.tabname,c.colno,c.colname FROM systables t,syscolumns c "
        f"WHERE c.tabid=t.tabid AND t.tabname IN ({table_literals}) "
        f"AND t.owner='{source_owner}' "
        "ORDER BY t.tabname,c.colno;"
    )
    # UNLOAD pads an INTERVAL with leading blanks ("  -5 10:30:00"), which the connector
    # lands trimmed as a string: these columns compare trimmed (see normalize).
    sql_statements.append(
        f"UNLOAD TO '{remote}/interval_columns.unl' DELIMITER '|' "
        "SELECT t.tabname,c.colname FROM systables t,syscolumns c "
        f"WHERE c.tabid=t.tabid AND t.tabname IN ({table_literals}) "
        f"AND t.owner='{source_owner}' AND MOD(c.coltype,256)=14;"
    )
    # A DATETIME neither a timestamp (YEAR to DAY or past) nor a time of day (from HOUR)
    # lands as labelled text (YEAR=2024:MONTH=05), which UNLOAD writes as 2024-05: their
    # qualifiers, to compare them alike (see normalize).
    sql_statements.append(
        f"UNLOAD TO '{remote}/datetime_columns.unl' DELIMITER '|' "
        "SELECT t.tabname,c.colname,c.collength FROM systables t,syscolumns c "
        f"WHERE c.tabid=t.tabid AND t.tabname IN ({table_literals}) "
        f"AND t.owner='{source_owner}' AND MOD(c.coltype,256)=10;"
    )
    sql = "\n".join(sql_statements) + "\n"
    encoded_sql = __import__("base64").b64encode(sql.encode()).decode()
    # DBDATE=Y4MD- writes a DATE as ISO, as the README's extract scripts do. A database in
    # another locale than the default en_US.819 names it (--db-locale): dbaccess refuses a
    # mismatched one (-23197), and the files are read in its code set.
    locale = getattr(args, "db_locale", None)
    locale_environment = (
        f"-e DB_LOCALE={shlex.quote(locale)} -e CLIENT_LOCALE={shlex.quote(locale)} "
        if locale
        else ""
    )
    environment = (
        "-e INFORMIXDIR=/opt/ibm/informix "
        "-e DBDATE=Y4MD- "
        f"-e INFORMIXSERVER={shlex.quote(args.informix_server)} "
        f"{locale_environment}"
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
    extract_source_archive(archive, run_id, output)


def extract_source_archive(archive: pathlib.Path, run_id: str, output: pathlib.Path) -> None:
    """Unpack the export's files and directories under ``output/run_id``, and nothing else:
    a link, a device or a FIFO is refused, not only a path outside it, since a later member
    could write through a link to anywhere."""
    with tarfile.open(archive, "r:gz") as handle:
        members = handle.getmembers()
        if any(
            not (member.isfile() or member.isdir())
            or not pathlib.PurePosixPath(member.name).parts
            or pathlib.PurePosixPath(member.name).parts[0] != run_id
            or ".." in pathlib.PurePosixPath(member.name).parts
            for member in members
        ):
            raise RuntimeError("Informix export archive contains an unsafe path")
        # The data filter refuses the same on a Python that has it (3.12, and backports).
        safe = {"filter": "data"} if hasattr(tarfile, "data_filter") else {}
        handle.extractall(output, members=members, **safe)


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


def _key_names(columns: list[str], key: "list[str] | int") -> list[str]:
    # A key given as a count (as callers before key_columns did) is the leading columns.
    return list(columns[:key]) if isinstance(key, int) else list(key)


def destination_rows(
    args: argparse.Namespace, table: str, columns: list[str], key_columns: "list[str] | int"
) -> list[dict[str, Any]]:
    def quote(column: str) -> str:
        return "`" + column.replace("`", "``") + "`"

    projection = ",".join(
        f"CAST({quote(column)} AS STRING) AS {quote(column)}" for column in columns
    )
    ordering = _key_names(columns, key_columns) or columns
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


def iter_unload_records(text: str) -> list[str]:
    """Split an UNLOAD dump into records, ending only on an *unescaped* newline.

    Informix UNLOAD escapes a newline embedded in a CHAR/VARCHAR value as ``\\`` +
    newline, so naive ``splitlines`` would break one record into fragments with the
    wrong field count. This tracks backslash-escape state and keeps an escaped newline
    inside the record; ``parse_unload_line`` then decodes it back to a literal newline.
    """

    records: list[str] = []
    record: list[str] = []
    escaped = False
    for character in text:
        if escaped:
            record.append(character)
            escaped = False
        elif character == "\\":
            record.append(character)
            escaped = True
        elif character == "\n":
            records.append("".join(record))
            record = []
        else:
            record.append(character)
    if record:
        records.append("".join(record))
    return records


class UnloadEmpty(str):
    """A field that was one escaped space: how UNLOAD writes an empty string (``|\\ |``),
    where ``||`` is a NULL and ``| |`` one blank. It is the empty string, which the
    connector lands for it, not the NULL an empty field is."""


def parse_unload_line(line: str) -> list[str]:
    values: list[str] = []
    value: list[str] = []
    escaped = False
    # Whether the field so far is one escaped space and nothing else.
    empty = False

    def field() -> str:
        return UnloadEmpty("") if empty else "".join(value)

    for character in line.rstrip("\n"):
        if escaped:
            empty = character == " " and not value
            value.append({"n": "\n", "r": "\r", "t": "\t"}.get(character, character))
            escaped = False
        elif character == "\\":
            escaped = True
        elif character == "|":
            values.append(field())
            value, empty = [], False
        else:
            empty = False
            value.append(character)
    if value:
        values.append(field())
    return values


def normalize(value: Any, data_type: str) -> str | None:
    if value is None:
        return None
    text = str(value)
    if not text and data_type != "STRING":
        return None
    if data_type in ("FLOAT", "DOUBLE"):
        # UNLOAD writes a float with every digit it has, 9 for a SMALLFLOAT (0.100000001)
        # and 17 for a FLOAT (0.10000000000000001), Spark with as few as name it (0.1):
        # both by the number they name, Spark's FLOAT (a SMALLFLOAT's) through 32 bits.
        number = float(text)
        if data_type == "FLOAT" and math.isfinite(number):
            number = struct.unpack("f", struct.pack("f", number))[0]
        return "0" if number == 0 else repr(number)
    if data_type.startswith("DECIMAL"):
        # Past the default context's 28 digits, which rounded a DECIMAL(32) or (38) value
        # and let two that differ compare equal; and one zero, which a FLOAT also writes
        # as -0.0.
        with decimal.localcontext() as context:
            context.prec = 100
            number = decimal.Decimal(text)
            return "0" if number == 0 else format(number.normalize(), "f")
    if data_type in {"INT", "BIGINT", "SMALLINT", "TINYINT"}:
        return str(int(text))
    if data_type == "INTERVAL":
        return text.strip()
    if data_type.startswith("DATETIME "):
        return partial_datetime_text(text, *map(int, data_type.split()[1:]))
    if data_type == "TIMESTAMP":
        timestamp = text.replace("T", " ").replace("Z", "+00:00")
        if "." in timestamp:
            head, fraction = timestamp.split(".", 1)
            # The zone after the fraction, either sign: a session west of UTC gave
            # "...56.123-05:00", whose "-05" was taken for more of the fraction.
            digits = len(fraction) - len(fraction.lstrip("0123456789"))
            fraction, suffix = fraction[:digits], fraction[digits:]
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


_DATETIME_LABELS = {0: "YEAR", 2: "MONTH", 4: "DAY", 6: "HOUR", 8: "MINUTE", 10: "SECOND"}


def partial_datetime_text(text: str, start: int, end: int) -> str:
    # The connector's labelled text for a DATETIME qualifier ``start`` TO ``end``
    # (YEAR=2024:MONTH=05:FRACTION(3)=123) from UNLOAD's (2024-05); a value already in it
    # (the destination's) as it is.
    if "=" in text:
        return text
    numbers = re.findall(r"\d+", text)
    codes = [code for code in _DATETIME_LABELS if start <= code <= min(end, 10)]
    rendered = ":".join(
        f"{_DATETIME_LABELS[code]}={int(number):0{4 if code == 0 else 2}d}"
        for code, number in zip(codes, numbers)
    )
    if end > 10 and len(numbers) > len(codes):
        rendered += f":FRACTION({end - 10})={numbers[len(codes)]}"
    return rendered


def partial_datetime_type(collength: int) -> str | None:
    # The compare type of a DATETIME column whose syscolumns.collength is ``collength``, or
    # None for a timestamp (YEAR to DAY or past) or a time of day (from HOUR), which
    # compare as they are.
    start, end = (collength >> 4) & 0xF, collength & 0xF
    if (start == 0 and end >= 4) or start == 6:
        return None
    return f"DATETIME {start} {end}"


def source_column_types(source_root: pathlib.Path, types: dict[str, dict[str, str]]) -> None:
    # The source's own types in ``types``, where the destination's don't tell how a column
    # compares: an INTERVAL, and a DATETIME the connector lands as labelled text.
    intervals = source_root / "interval_columns.unl"
    if intervals.exists():
        for line in intervals.read_text(encoding=UNLOAD_ENCODING).splitlines():
            values = parse_unload_line(line)
            if len(values) == 2 and values[1] in types.get(values[0], {}):
                types[values[0]][values[1]] = "INTERVAL"
    datetimes = source_root / "datetime_columns.unl"
    if datetimes.exists():
        for line in datetimes.read_text(encoding=UNLOAD_ENCODING).splitlines():
            values = parse_unload_line(line)
            if len(values) == 3 and values[1] in types.get(values[0], {}):
                partial = partial_datetime_type(int(values[2]))
                if partial:
                    types[values[0]][values[1]] = partial


def locale_encoding(locale: str) -> str:
    # The Python codec of an Informix locale's code set: en_US.819 is Latin-1, en_US.utf8
    # UTF-8, and a code set Python names itself (cp1252) that one.
    codeset = locale.rpartition(".")[2].lower()
    known = {"819": "latin-1", "8859-1": "latin-1", "utf8": "utf-8", "57372": "utf-8"}
    try:
        return known.get(codeset) or codecs.lookup(codeset).name
    except LookupError as error:
        raise SystemExit(f"--db-locale {locale}: no Python codec for code set {codeset}") from error


# The export runs dbaccess with no CLIENT_LOCALE, so it writes in the default client locale,
# en_US.819: Latin-1, whatever the reader's own default encoding is (--db-locale names
# another).
UNLOAD_ENCODING = "latin-1"


def primary_key_counts(path: pathlib.Path) -> dict[str, int]:
    result = {}
    for line in path.read_text(encoding=UNLOAD_ENCODING).splitlines():
        values = parse_unload_line(line)
        if not values:
            continue
        result[values[0]] = sum(1 for value in values[1:] if int(value or 0) != 0)
    return result


def primary_key_columns(
    keys_path: pathlib.Path, columns_path: pathlib.Path
) -> dict[str, list[str]]:
    """Each table's primary-key columns by name, in key order.

    The key's parts are column numbers (negative for a descending column). Taken as a
    count of leading columns, a key on later columns, (col2, col4) say, keyed the compare
    on the wrong ones."""

    names: dict[tuple[str, int], str] = {}
    for line in columns_path.read_text(encoding=UNLOAD_ENCODING).splitlines():
        values = parse_unload_line(line)
        if len(values) >= 3:
            names[(values[0], int(values[1]))] = values[2]
    result = {}
    for line in keys_path.read_text(encoding=UNLOAD_ENCODING).splitlines():
        values = parse_unload_line(line)
        if not values:
            continue
        parts = [abs(int(value or 0)) for value in values[1:]]
        result[values[0]] = [names[(values[0], part)] for part in parts if part]
    return result


def compare_table(
    table: str,
    source_file: pathlib.Path,
    destination: list[dict[str, Any]],
    columns: list[str],
    types: dict[str, str],
    key_columns: "list[str] | int",
    destination_pipe: pathlib.Path,
    whole_row_key: bool = False,
) -> dict[str, Any]:
    key_columns = _key_names(columns, key_columns)
    key_count = len(key_columns)
    source = []
    for number, line in enumerate(
        # Bytes decoded, not read_text: its newline translation turned a value's lone \r
        # into a record end, where UNLOAD (and the connector) end records on \n only.
        iter_unload_records(source_file.read_bytes().decode(UNLOAD_ENCODING)), 1
    ):
        values = parse_unload_line(line)
        if len(values) != len(columns):
            raise RuntimeError(
                f"{table}:{number} has {len(values)} fields; expected {len(columns)}"
            )
        # UNLOAD writes a NULL as an empty field, and an empty string as a backslash and a
        # space (UnloadEmpty): an empty field is a NULL whatever the column's type. Read as
        # an empty string it differed from the destination's NULL in every character
        # column, which a keyless table's whole-row compare counts as a row missing and a
        # row too many. The empty string is "", as the connector lands it; read as the
        # space it decodes to, it differed from the destination's "".
        source.append(
            dict(
                zip(
                    columns,
                    (
                        None if value == "" and not isinstance(value, UnloadEmpty) else value
                        for value in values
                    ),
                )
            )
        )
    with destination_pipe.open("w", encoding="utf-8") as handle:
        for row in destination:
            handle.write("|".join(pipe_value(row.get(column)) for column in columns) + "|\n")

    if key_count == 0:
        # Keyless (append-only) table: there is no primary key to pair rows across
        # sides, so compare full rows as a multiset -- every source row must appear in
        # the destination the same number of times. A value difference surfaces as one
        # missing + one extra row (not a cell-level mismatch, which needs a key), and
        # legitimately duplicated source rows are handled by the multiplicity counts.
        # Keyed on the whole row (whole_row_key), the destination holds each distinct row
        # once, and counted, a source row written twice read as one missing.
        full_row = lambda row: tuple(normalize(row.get(c), types[c]) for c in columns)
        rows = set if whole_row_key else list
        source_counter = collections.Counter(rows(full_row(row) for row in source))
        destination_counter = collections.Counter(rows(full_row(row) for row in destination))
        missing = source_counter - destination_counter
        extra = destination_counter - source_counter
        return {
            "table": table,
            "keyless": True,
            "source_rows": len(source),
            "destination_rows": len(destination),
            "missing_rows": sum(missing.values()),
            "extra_rows": sum(extra.values()),
            "mismatched_rows": 0,
            "mismatched_cells": 0,
            "duplicate_source_keys": 0,
            "duplicate_destination_keys": 0,
            "mismatches_by_column": {},
            "missing_key_examples": [list(row) for row in list(missing)[:5]],
            "extra_key_examples": [list(row) for row in list(extra)[:5]],
            "mismatch_examples": [],
        }

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
    # The default credential chain, as the count comparison uses: the ambient IAM keys
    # reach the same SSM host and S3 bucket with no SSO session to expire mid-run, and
    # presigning omits the (absent) session token. --aws-profile names a profile instead.
    parser.add_argument(
        "--aws-profile",
        default=None,
        help="AWS profile to use (for example an SSO profile); default: ambient credentials.",
    )
    # Kept so existing commands still run: it is now the default.
    parser.add_argument(
        "--ambient-creds",
        action="store_const",
        const=None,
        dest="aws_profile",
        help="Use the default AWS credentials (the default; overrides --aws-profile).",
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
    parser.add_argument(
        "--db-locale",
        default=None,
        help="the database's locale (DB_LOCALE), when not the default en_US.819",
    )
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
    parser.add_argument(
        "--whole-row-key-tables",
        nargs="*",
        default=list(WHOLE_ROW_KEY_TABLES),
        help='keyless tables keyed on the whole row (primary.keys "*"): compared by distinct rows',
    )
    args = parser.parse_args()
    global UNLOAD_ENCODING  # noqa: PLW0603 - the files' code set, read by every parser here
    UNLOAD_ENCODING = locale_encoding(args.db_locale) if args.db_locale else "latin-1"
    return args


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

        def export_source() -> dict[str, list[str]]:
            progress("Exporting Informix tables...")
            wait_ssm(args, send_ssm(args, source_export_command(args, run_id)))
            progress("Transferring Informix export...")
            transfer_source_archive(args, run_id, output)
            return primary_key_columns(
                source_root / "primary_keys.unl", source_root / "key_columns.unl"
            )

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

        (output / "destination_schema.json").write_text(
            json.dumps(schema, indent=2), encoding="utf-8"
        )
        columns: dict[str, list[str]] = collections.defaultdict(list)
        types: dict[str, dict[str, str]] = collections.defaultdict(dict)
        for row in schema:
            if row["column_name"] in INTERNAL_COLUMNS:
                continue
            columns[row["table_name"]].append(row["column_name"])
            types[row["table_name"]][row["column_name"]] = row["data_type"].upper()
        source_column_types(source_root, types)
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
            rows = destination_rows(args, table, columns[table], keys.get(table, []))
            progress(f"{table}: destination extraction complete ({len(rows)} rows)")
            return compare_table(
                table,
                source_root / f"{table}.unl",
                rows,
                columns[table],
                types[table],
                keys.get(table, []),
                output / f"{table}.destination.unl",
                table in args.whole_row_key_tables,
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
        report_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        progress(f"Report: {report_path}")
        # Duplicate keys too: a keyed target with two rows for one key differs from the
        # source, though its other counts can all be 0.
        differences = any(
            row["missing_rows"]
            or row["extra_rows"]
            or row["mismatched_rows"]
            or row.get("duplicate_source_keys")
            or row.get("duplicate_destination_keys")
            for row in report
        )
        return 1 if args.fail_on_difference and differences else 0
    finally:
        cleanup(args, run_id)


if __name__ == "__main__":
    raise SystemExit(main())
