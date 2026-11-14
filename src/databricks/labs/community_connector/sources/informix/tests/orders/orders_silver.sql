-- Silver for the orders / order_items test. Reads the append-only bronze change logs.
-- ${bronze_schema} is the bronze pipeline's catalog.schema, set in this pipeline's
-- configuration (see orders_silver_pipeline.json).

-- orders has a key, so AUTO CDC rebuilds its current rows from the change log.
CREATE OR REFRESH STREAMING TABLE orders;

CREATE FLOW orders_cdc AS AUTO CDC INTO orders
FROM STREAM(${bronze_schema}.orders)
KEYS (id)
APPLY AS DELETE WHEN _informix_op = 'd'
SEQUENCE BY _informix_change_lsn
COLUMNS * EXCEPT (_informix_op, _informix_change_lsn, _informix_commit_lsn, _informix_tx_id, _informix_commit_time)
STORED AS SCD TYPE 1;

-- order_items has no key and can hold identical rows.
-- How many copies of each distinct row exist now: +1 per snapshot (r), insert (c) or
-- updated (u) row, -1 per delete (d) row. An UPDATE of a keyless table lands as a d row
-- with the old values followed by a u row with the new ones.
CREATE OR REFRESH MATERIALIZED VIEW order_items_counts (
  -- An operation this view does not know can't be netted, so the refresh fails instead.
  CONSTRAINT known_changes_only EXPECT (other_changes = 0) ON VIOLATION FAIL UPDATE,
  -- A delete with no matching insert means bronze is missing history.
  CONSTRAINT no_orphan_deletes EXPECT (copies >= 0) ON VIOLATION FAIL UPDATE
)
AS SELECT
  order_id, sku, qty, price, note,
  sum(CASE WHEN _informix_op IN ('r', 'c', 'u') THEN 1 WHEN _informix_op = 'd' THEN -1 ELSE 0 END) AS copies,
  sum(CASE WHEN _informix_op IN ('r', 'c', 'u', 'd') THEN 0 ELSE 1 END) AS other_changes
FROM ${bronze_schema}.order_items
GROUP BY order_id, sku, qty, price, note
HAVING copies <> 0 OR other_changes <> 0;

-- order_items as it is in the source: each row repeated once per copy.
CREATE OR REFRESH MATERIALIZED VIEW order_items
AS SELECT inline(array_repeat(struct(order_id, sku, qty, price, note), CAST(copies AS INT)))
FROM order_items_counts
WHERE copies > 0;
