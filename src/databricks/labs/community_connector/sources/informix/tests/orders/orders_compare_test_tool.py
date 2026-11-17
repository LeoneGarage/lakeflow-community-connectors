"""Compare the orders test's silver order_items tables with the source.

The source side is the output of orders_fingerprint.sh, saved to a file: order_items summed
up in 25 buckets. The same sums are taken over each silver table, the materialized view
order_items and the foreachBatch sink's Delta table order_items_delta; every bucket must
match, which it only does if the table holds each source row the right number of times,
updated_at included (by its day and its time of day to the 10 microseconds).

    python orders_compare_test_tool.py <fingerprint file> --warehouse-id <id> \\
        [--profile DEFAULT] [--silver <catalog>.orders_silver] [--bronze <catalog>.orders_bronze] \\
        [--tables order_items,order_items_delta]

Exits 1 when a bucket of any table differs, or a table cannot be read (`ERROR`).
"""

from __future__ import annotations

import argparse
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import count_compare_test_tool as tool  # noqa: E402  pylint: disable=wrong-import-position


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("fingerprint", help="file holding the output of orders_fingerprint.sh")
    parser.add_argument("--warehouse-id", required=True, help="SQL warehouse to query with")
    parser.add_argument("--profile", default="DEFAULT", help="Databricks CLI profile")
    parser.add_argument("--silver", default="main.orders_silver", help="silver catalog.schema")
    parser.add_argument("--bronze", default="main.orders_bronze", help="bronze catalog.schema")
    parser.add_argument(
        "--tables",
        default="order_items,order_items_delta",
        help="comma-separated tables to compare, in the silver schema unless qualified",
    )
    args = parser.parse_args()

    def query(statement: str) -> list[list]:
        return tool.run_statement(args.profile, args.warehouse_id, statement)

    with open(args.fingerprint, encoding="utf-8") as lines:
        source = [
            [float(value) for value in line.strip().strip("|").split("|")]
            for line in lines
            if line.strip()
        ]
    matched = True
    for name in (name.strip() for name in args.tables.split(",")):
        if not name:
            continue
        table = name if "." in name else f"{args.silver}.{name}"
        try:
            silver = [
                [float(value) for value in row]
                for row in query(
                    "SELECT order_id % 25, count(*), sum(order_id), sum(qty), sum(price), "
                    "sum(order_id * qty * price), count(note), sum(day(updated_at)), "
                    "sum(CAST(date_format(updated_at, 'HHmmssSSSSS') AS BIGINT)) "
                    f"FROM {table} GROUP BY 1 ORDER BY 1"
                )
            ]
        except Exception as error:  # pylint: disable=broad-except
            # A table that is not there, say: the other tables are still compared. Not a
            # MISMATCH, which is a difference in the data, but the run still fails.
            matched = False
            print(f"ERROR {table}: cannot be read: {error}")
            continue
        differing = [(a, b) for a, b in zip(source, silver) if a != b]
        same = not differing and len(source) == len(silver)
        matched = matched and same
        print(
            "MATCH" if same else "MISMATCH",
            f"{table}: buckets {len(source)}/{len(silver)}, differing {len(differing)},",
            f"source rows {sum(row[1] for row in source):.0f},",
            f"silver rows {sum(row[1] for row in silver):.0f}",
        )
        for a, b in differing[:5]:
            print(" source", a)
            print(" silver", b)
        print(
            f"{table} duplicate groups / most copies / rows in them:",
            query(
                # Every column, as the silver views take them (orders_silver.sql).
                f"SELECT count(*), max(c), sum(c) FROM (SELECT *, count(*) c FROM {table} "
                "GROUP BY ALL HAVING count(*) > 1)"
            ),
        )
    print(
        "silver orders, count / highest id:",
        query(f"SELECT count(*), max(id) FROM {args.silver}.orders"),
    )
    # order_items_counts is private to the silver pipeline, so its counts are taken
    # again here from the bronze change log it reads.
    print(
        "counts per distinct row, groups / copies / fewest copies / unknown changes:",
        query(
            "SELECT count(*), sum(copies), min(copies), sum(other_changes) FROM (SELECT "
            "* EXCEPT (_informix_op, _informix_change_lsn, _informix_commit_lsn, "
            "_informix_tx_id, _informix_commit_time), "
            "sum(CASE WHEN _informix_op IN ('r', 'c', 'u') THEN 1 "
            "WHEN _informix_op = 'd' THEN -1 ELSE 0 END) AS copies, "
            "sum(CASE WHEN _informix_op IN ('r', 'c', 'u', 'd') THEN 0 ELSE 1 END) "
            f"AS other_changes FROM {args.bronze}.order_items_log "
            "GROUP BY ALL HAVING copies <> 0 OR other_changes <> 0)"
        ),
    )
    print(
        "bronze order_items rows by operation:",
        query(
            f"SELECT _informix_op, count(*) FROM {args.bronze}.order_items_log GROUP BY 1 ORDER BY 1"
        ),
    )
    return 0 if matched else 1


if __name__ == "__main__":
    sys.exit(main())
