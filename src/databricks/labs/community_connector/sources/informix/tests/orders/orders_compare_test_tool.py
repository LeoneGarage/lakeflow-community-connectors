"""Compare the orders test's silver order_items with the source.

The source side is the output of orders_fingerprint.sh, saved to a file: order_items summed
up in 25 buckets. The same sums are taken over the silver table; every bucket must match,
which it only does if silver holds each source row the right number of times.

    python orders_compare_test_tool.py <fingerprint file> --warehouse-id <id> \\
        [--profile DEFAULT] [--silver <catalog>.orders_silver] [--bronze <catalog>.orders_bronze]

Exits 1 when a bucket differs.
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
    args = parser.parse_args()

    def query(statement: str) -> list[list]:
        return tool.run_statement(args.profile, args.warehouse_id, statement)

    with open(args.fingerprint, encoding="utf-8") as lines:
        source = [
            [float(value) for value in line.strip().strip("|").split("|")]
            for line in lines
            if line.strip()
        ]
    silver = [
        [float(value) for value in row]
        for row in query(
            "SELECT order_id % 25, count(*), sum(order_id), sum(qty), sum(price), "
            f"sum(order_id * qty * price), count(note) FROM {args.silver}.order_items "
            "GROUP BY 1 ORDER BY 1"
        )
    ]
    differing = [(a, b) for a, b in zip(source, silver) if a != b]
    matched = not differing and len(source) == len(silver)
    print(
        "MATCH" if matched else "MISMATCH",
        f"buckets {len(source)}/{len(silver)}, differing {len(differing)},",
        f"source rows {sum(row[1] for row in source):.0f},",
        f"silver rows {sum(row[1] for row in silver):.0f}",
    )
    for a, b in differing[:5]:
        print(" source", a)
        print(" silver", b)
    print(
        "silver duplicate groups / most copies / rows in them:",
        query(
            f"SELECT count(*), max(c), sum(c) FROM (SELECT count(*) c FROM {args.silver}."
            "order_items GROUP BY order_id, sku, qty, price, note HAVING count(*) > 1)"
        ),
    )
    print(
        "silver orders, count / highest id:",
        query(f"SELECT count(*), max(id) FROM {args.silver}.orders"),
    )
    print(
        "counts view, groups / copies / fewest copies / unknown changes:",
        query(
            "SELECT count(*), sum(copies), min(copies), sum(other_changes) "
            f"FROM {args.silver}.order_items_counts"
        ),
    )
    print(
        "bronze order_items rows by operation:",
        query(
            f"SELECT _informix_op, count(*) FROM {args.bronze}.order_items GROUP BY 1 ORDER BY 1"
        ),
    )
    return 0 if matched else 1


if __name__ == "__main__":
    sys.exit(main())
