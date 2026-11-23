"""Generate the silver SDP SQL file over the cycles pipeline's append-only bronze.

Named *_test* so the connector's merge and wheel leave it out.

AUTO CDC is used only for a table whose key, as the connector reports it, is a primary key
or a unique index of the Informix table: a streaming table with an explicit schema (key
columns NOT NULL and an informational PRIMARY KEY, when the source has them NOT NULL) kept
by AUTO CDC from its change log. Every other table gets counts per distinct row, as the
orders test's order_items does: +1 per r, c or u row, -1 per d row, then each distinct
row repeated once per copy. That includes a table with no key and one keyed only by its
configuration (primary.keys, "*" or columns no unique index covers), whose rows the
source does not keep unique: the count views keep its duplicates, which a merge by that
key would collapse.

Inputs:
  --spec     the bronze pipeline source (informix_pipeline_test_ingest.py), for the table
             list and each table's configuration
  --keys     the keys, as silver_keys_job_test_tool.py prints them: a file holding its
             SILVER_KEYS line (the task's logs) or the JSON rows alone
  --columns  information_schema.columns rows (table_name, column_name, ordinal_position,
             full_data_type) of the bronze schema, as JSON; or --profile and
             --bronze-schema to query them with the databricks CLI
  --informix-catalog  a directory with the Informix catalog's unique indexes
             (unique_indexes.unl: table, index, 1 if a primary key's, part1..part16) and
             columns (columns.unl: table, colno, name, 1 if NOT NULL), as dbaccess UNLOADs
             them from the statements --catalog-sql prints

  silver_generate_test_tool.py --spec informix_pipeline_test_ingest.py --keys keys.txt \\
      --informix-catalog catalog/ --profile demo1 > informix_silver.sql
"""

import argparse
import ast
import collections
import json
import os
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


# The catalog the generator reads, unloaded by dbaccess in the source database into DIR.
CATALOG_SQL = """\
UNLOAD TO '{dir}/unique_indexes.unl' SELECT t.tabname, i.idxname,
  (SELECT COUNT(*) FROM sysconstraints c
    WHERE c.tabid = t.tabid AND c.idxname = i.idxname AND c.constrtype = 'P'),
  i.part1, i.part2, i.part3, i.part4, i.part5, i.part6, i.part7, i.part8,
  i.part9, i.part10, i.part11, i.part12, i.part13, i.part14, i.part15, i.part16
  FROM systables t, sysindexes i
  WHERE i.tabid = t.tabid AND i.idxtype = 'U' AND t.tabid >= 100 AND t.tabtype = 'T';
UNLOAD TO '{dir}/columns.unl' SELECT t.tabname, c.colno, c.colname,
  CASE WHEN c.coltype >= 256 THEN 1 ELSE 0 END
  FROM systables t, syscolumns c
  WHERE c.tabid = t.tabid AND t.tabid >= 100 AND t.tabtype = 'T';
"""


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


def read_catalog(directory: str) -> dict[str, dict]:
    # Per table: its unique indexes (name, whether a primary key's, column names) and its
    # NOT NULL columns. A part is a column number, negative for a descending column.
    def rows(name):
        with open(os.path.join(directory, name), encoding="latin-1") as handle:
            return [line.rstrip("\n").split("|") for line in handle if line.strip()]

    catalog: dict[str, dict] = collections.defaultdict(lambda: {"unique": [], "not_null": set()})
    names = {}
    for table, colno, name, not_null, *_ in rows("columns.unl"):
        names[(table.strip(), int(colno))] = name.strip()
        nulls = catalog[table.strip()]["not_null"]
        if not_null.strip() == "1":
            nulls.add(name.strip())
    for table, index, primary, *parts in rows("unique_indexes.unl"):
        numbers = [abs(int(part)) for part in parts if part.strip() not in ("", "0")]
        missing = [number for number in numbers if (table.strip(), number) not in names]
        if missing:
            raise SystemExit(
                f"catalog: index {index.strip()} of {table.strip()} names column(s) {missing}, "
                "which columns.unl does not hold"
            )
        columns = [names[(table.strip(), number)] for number in numbers]
        catalog[table.strip()]["unique"].append((index.strip(), float(primary or 0) > 0, columns))
    return catalog


def key_source(keys: list[str], tables: dict) -> tuple[str, bool] | None:
    # The primary key or unique index the key is, as (what it is, whether a primary key's),
    # or None when it is neither.
    for index, primary, columns in sorted(tables["unique"], key=lambda u: not u[1]):
        if keys and set(columns) == set(keys):
            return ("the primary key" if primary else f"unique index {index}"), primary
    return None


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


def keyed_table(
    table: str, columns: list[tuple[str, str]], keys: list[str], source: str, not_null: set
) -> str:
    # A PRIMARY KEY needs its columns NOT NULL; a unique index's nullable column can hold a
    # NULL, which AUTO CDC matches as a key value, so such a key declares no constraint.
    constrained = set(keys) <= not_null
    width = max(len(ident(c)) for c, _ in columns)
    lines = [
        f"  {ident(c).ljust(width)} {t.upper()}{' NOT NULL' if constrained and c in keys else ''},"
        for c, t in columns
    ]
    key_list = ", ".join(ident(k) for k in keys)
    if constrained:
        lines.append(f"  CONSTRAINT {ident(table + '_pk')} PRIMARY KEY ({key_list})")
    else:
        lines[-1] = lines[-1].rstrip(",")
    note = "" if constrained else "; a key column is nullable, so no PRIMARY KEY"
    return "\n".join(
        [
            f"-- {table}: key ({', '.join(keys)}), {source}{note}",
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
-- columns, key or indexes change, or when informix_pipeline_test_ingest.py's tables do.
-- Silver for informix-pipeline: {count} tables ({keyed} keyed, {keyless} keyless).
-- Keys: the Informix connector's _community_table_metadata (silver_keys_job_test_tool.py),
-- read for each table's configuration without append.only.ingestion. A table is keyed
-- here only when that key is a primary key or unique index of the Informix table.
-- Types: the bronze tables, without the connector's _informix_* columns.
-- ${{bronze_schema}} is the bronze pipeline's catalog.schema, set in this pipeline's
-- configuration (informix_silver_pipeline.json). Bronze is append-only: one change-log
-- row per snapshot row (r) and per change (c, u, d).
--
-- A keyed table: a streaming table with an explicit schema, key columns NOT NULL and an
-- informational PRIMARY KEY, kept by AUTO CDC from the change log. A row both in the
-- snapshot and in the stream merges by its key.
-- Any other table: how many copies of each distinct row exist now, +1 per r, c or u
-- row and -1 per d row (an UPDATE of a keyless table lands as a d row with the old values
-- and a u row with the new ones), then each distinct row repeated once per copy. Its
-- bronze snapshot pages by an index or by named columns, so neither the snapshot nor the
-- stream holds a change the other has.
"""


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--spec")
    parser.add_argument("--keys")
    parser.add_argument("--columns")
    parser.add_argument("--profile", default="demo1")
    parser.add_argument("--bronze-schema", default="members.connector_bronze")
    parser.add_argument("--informix-catalog")
    parser.add_argument(
        "--catalog-sql", metavar="DIR", help="print the catalog's UNLOAD statements and exit"
    )
    args = parser.parse_args(argv)
    if args.catalog_sql:
        sys.stdout.write(CATALOG_SQL.format(dir=args.catalog_sql))
        return
    for name in ("spec", "keys", "informix_catalog"):
        if not getattr(args, name):
            parser.error(f"--{name.replace('_', '-')} is required")
    catalog = read_catalog(args.informix_catalog)
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
        source_table = config.get("qualified_source_table", table).split(".")[-1]
        if table not in keys:
            raise SystemExit(f"no key row for {table}")
        if not columns.get(table):
            raise SystemExit(f"no bronze columns for {table}")
        missing = [k for k in keys[table] if k not in dict(columns[table])]
        if missing:
            raise SystemExit(f"{table}: key columns {missing} are not in bronze")
        if source_table not in catalog and source_table.lower() not in catalog:
            raise SystemExit(f"{table}: Informix table {source_table} is not in the catalog")
        tables = catalog.get(source_table) or catalog[source_table.lower()]
        found = key_source(keys[table], tables)
        if found:
            blocks.append(
                keyed_table(table, columns[table], keys[table], found[0], tables["not_null"])
            )
            keyed += 1
        elif keys[table]:
            why = (
                f"key ({', '.join(keys[table])}) from its configuration, not a primary key "
                "or unique index; counts per distinct row"
            )
            blocks.append(keyless_table(table, why))
            keyless += 1
        else:
            blocks.append(keyless_table(table, "no key; counts per distinct row"))
            keyless += 1
    header = HEADER.format(count=len(spec["objects"]), keyed=keyed, keyless=keyless)
    sys.stdout.write(header + "\n" + "\n\n".join(blocks) + "\n")


if __name__ == "__main__":
    main()
