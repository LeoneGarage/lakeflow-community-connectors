"""Generate the silver SDP SQL file over the cycles pipeline's append-only bronze.

Named *_test* so the connector's merge and wheel leave it out.

A table the connector reports a key for gets a streaming table with an explicit schema
(key columns NOT NULL, an informational PRIMARY KEY) kept by AUTO CDC from its change log.
A table with no key gets counts per distinct row, as the orders test's order_items does:
+1 per r, c or u row, -1 per d row, then each distinct row repeated once per copy. A table
keyed only by primary.keys "*" has no key of its own and is keyless here too: the count
views keep its duplicate rows, which a whole-row key would merge.

Inputs:
  --spec     the bronze pipeline source (informix_pipeline_test_ingest.py), for the table
             list and each table's configuration
  --keys     the keys, as silver_keys_job_test_tool.py prints them: a file holding its
             SILVER_KEYS line (the task's logs) or the JSON rows alone
  --columns  information_schema.columns rows (table_name, column_name, ordinal_position,
             full_data_type) of the bronze schema, as JSON; or --profile and
             --bronze-schema to query them with the databricks CLI

  silver_generate_test_tool.py --spec informix_pipeline_test_ingest.py --keys keys.txt \\
      --profile demo1 --bronze-schema members.connector_bronze > informix_silver.sql
"""

import argparse
import ast
import collections
import json
import re
import subprocess
import sys

CONNECTOR_COLUMNS = (
    "_informix_op",
    "_informix_change_lsn",
    "_informix_commit_lsn",
    "_informix_tx_id",
    "_informix_commit_time",
)
EXCEPT = ", ".join(CONNECTOR_COLUMNS)
RESERVED = set(
    """all and any as authorization both case cast check collate column constraint create
    cross current current_date current_time current_timestamp current_user distinct else end
    escape except false fetch filter for foreign from full grant group having in inner
    intersect into is join lateral leading left like natural not null of on only or order
    outer overlaps primary references right select session_user some table then time to
    trailing true union unique unknown user using when where window with""".split()
)


def ident(name: str) -> str:
    if re.fullmatch(r"[a-z_][a-z0-9_]*", name) and name not in RESERVED:
        return name
    return "`" + name.replace("`", "``") + "`"


def pipeline_spec(path: str) -> dict:
    with open(path, encoding="utf-8") as handle:
        tree = ast.parse(handle.read())
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") == "pipeline_spec":
            return ast.literal_eval(node.value)
    raise SystemExit(f"no pipeline_spec in {path}")


def read_keys(path: str) -> dict[str, list[str]]:
    # The task's logs, the JSON `databricks jobs get-run-output` gives (logs in it), or the
    # rows alone.
    with open(path, encoding="utf-8") as handle:
        text = handle.read()
    if text.lstrip().startswith("{"):
        text = json.loads(text).get("logs") or ""
    lines = [line for line in text.splitlines() if line.startswith("SILVER_KEYS ")]
    rows = json.loads(lines[-1][len("SILVER_KEYS ") :] if lines else text)
    return {row["tableName"]: list(row["primary_keys"] or []) for row in rows}


def read_columns(args: argparse.Namespace) -> list[dict]:
    if args.columns:
        with open(args.columns, encoding="utf-8") as handle:
            return json.load(handle)
    catalog, schema = args.bronze_schema.split(".")
    statement = (
        "SELECT table_name, column_name, ordinal_position, full_data_type "
        f"FROM {catalog}.information_schema.columns WHERE table_schema = '{schema}'"
    )
    command = ["databricks", "experimental", "aitools", "tools", "query", statement]
    output = subprocess.run(
        [*command, "--profile", args.profile], check=True, capture_output=True, text=True
    )
    return json.loads(output.stdout)


def keyed_table(table: str, columns: list[tuple[str, str]], keys: list[str]) -> str:
    width = max(len(ident(c)) for c, _ in columns)
    lines = [
        f"  {ident(c).ljust(width)} {t.upper()}{' NOT NULL' if c in keys else ''},"
        for c, t in columns
    ]
    key_list = ", ".join(ident(k) for k in keys)
    lines.append(f"  CONSTRAINT {ident(table + '_pk')} PRIMARY KEY ({key_list})")
    return "\n".join(
        [
            f"-- {table}: primary_keys = [{', '.join(keys)}]",
            f"CREATE OR REFRESH STREAMING TABLE {ident(table)} (",
            *lines,
            ");",
            "",
            f"CREATE FLOW {ident(table + '_cdc')} AS AUTO CDC INTO {ident(table)}",
            f"FROM STREAM(${{bronze_schema}}.{ident(table)})",
            f"KEYS ({key_list})",
            "APPLY AS DELETE WHEN _informix_op = 'd'",
            "SEQUENCE BY _informix_change_lsn",
            f"COLUMNS * EXCEPT ({EXCEPT})",
            "STORED AS SCD TYPE 1;",
        ]
    )


def keyless_table(table: str, why: str) -> str:
    counts = ident(table + "_counts")
    return "\n".join(
        [
            f"-- {table}: {why}",
            f"CREATE OR REFRESH PRIVATE MATERIALIZED VIEW {counts} (",
            "  CONSTRAINT known_changes_only EXPECT (other_changes = 0) ON VIOLATION FAIL UPDATE,",
            "  CONSTRAINT no_orphan_deletes EXPECT (copies >= 0) ON VIOLATION FAIL UPDATE",
            ")",
            "AS SELECT",
            f"  * EXCEPT ({EXCEPT}),",
            "  sum(CASE WHEN _informix_op IN ('r', 'c', 'u') THEN 1"
            " WHEN _informix_op = 'd' THEN -1 ELSE 0 END) AS copies,",
            "  sum(CASE WHEN _informix_op IN ('r', 'c', 'u', 'd') THEN 0 ELSE 1 END)"
            " AS other_changes",
            f"FROM ${{bronze_schema}}.{ident(table)}",
            "GROUP BY ALL",
            "HAVING copies <> 0 OR other_changes <> 0;",
            "",
            f"CREATE OR REFRESH MATERIALIZED VIEW {ident(table)}",
            "AS SELECT inline(array_repeat(struct(* EXCEPT (copies, other_changes)),"
            " CAST(copies AS INT)))",
            f"FROM {counts}",
            "WHERE copies > 0;",
        ]
    )


HEADER = """\
-- GENERATED by silver_generate_test_tool.py: do not edit. Regenerate when a source table's
-- columns or key change, or when informix_pipeline_test_ingest.py's tables do.
-- Silver for informix-pipeline: {count} tables ({keyed} keyed, {keyless} keyless).
-- Keys: the Informix connector's _community_table_metadata (silver_keys_job_test_tool.py),
-- read for each table's configuration without append.only.ingestion.
-- Types: the bronze tables, without the connector's _informix_* columns.
-- ${{bronze_schema}} is the bronze pipeline's catalog.schema, set in this pipeline's
-- configuration (informix_silver_pipeline.json). Bronze is append-only: one change-log
-- row per snapshot row (r) and per change (c, u, d).
--
-- A table with a key: a streaming table with an explicit schema, key columns NOT NULL and
-- an informational PRIMARY KEY, kept by AUTO CDC from the change log. A row both in the
-- snapshot and in the stream merges by its key.
-- A table with no key: how many copies of each distinct row exist now, +1 per r, c or u
-- row and -1 per d row (an UPDATE of a keyless table lands as a d row with the old values
-- and a u row with the new ones), then each distinct row repeated once per copy. Its
-- bronze snapshot pages by an index or by named columns, so neither the snapshot nor the
-- stream holds a change the other has.
"""


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--spec", required=True)
    parser.add_argument("--keys", required=True)
    parser.add_argument("--columns")
    parser.add_argument("--profile", default="demo1")
    parser.add_argument("--bronze-schema", default="members.connector_bronze")
    args = parser.parse_args(argv)
    spec = pipeline_spec(args.spec)
    keys = read_keys(args.keys)
    columns: dict[str, list[tuple[str, str]]] = collections.defaultdict(list)
    for row in sorted(read_columns(args), key=lambda r: int(r["ordinal_position"])):
        if row["column_name"] not in CONNECTOR_COLUMNS:
            columns[row["table_name"]].append((row["column_name"], row["full_data_type"]))
    blocks, keyed, keyless = [], 0, 0
    for entry in spec["objects"]:
        table = entry["table"]["source_table"]
        config = entry["table"].get("table_configuration", {})
        if table not in keys:
            raise SystemExit(f"no key row for {table}")
        if not columns.get(table):
            raise SystemExit(f"no bronze columns for {table}")
        missing = [k for k in keys[table] if k not in dict(columns[table])]
        if missing:
            raise SystemExit(f"{table}: key columns {missing} are not in bronze")
        if config.get("primary.keys", "").strip() == "*":
            why = 'keyless at the source (primary.keys "*"); counts per distinct row'
            blocks.append(keyless_table(table, why))
            keyless += 1
        elif keys[table]:
            blocks.append(keyed_table(table, columns[table], keys[table]))
            keyed += 1
        else:
            blocks.append(keyless_table(table, "no key; counts per distinct row"))
            keyless += 1
    header = HEADER.format(count=len(spec["objects"]), keyed=keyed, keyless=keyless)
    sys.stdout.write(header + "\n" + "\n\n".join(blocks) + "\n")


if __name__ == "__main__":
    main()
