# Silver for the orders test, second example: order_items kept in a standard Delta table
# by a foreachBatch sink, beside the order_items materialized view of orders_silver.sql.
# Unlike the view, the table has history and time travel. Named *_test* so the
# connector's merge and wheel leave it out.
#
# Pipeline configuration: bronze_schema, as for orders_silver.sql, and
# order_items_delta_table, the table to write (<catalog>.<schema>.<table>).
#
# The pipeline does not manage a sink's table, so this file keeps it right itself:
# - Each micro-batch rebuilds the orders it touches from the whole bronze change log and
#   swaps their rows in with replaceWhere writes, ORDERS_PER_WRITE orders to a write. Delta
#   cannot delete one of several identical rows, so an order is always rewritten whole, in
#   one write; rebuilding from bronze rather than from the batch makes a retried batch
#   write the same rows.
# - batch_id 0 starts a stream: the first update, and every full refresh, which resets the
#   flow's checkpoint and replays bronze from its first row. That batch replaces the whole
#   table, clearing what the earlier stream wrote, and marks the table as this stream's
#   (table property MARKER). A later batch that finds a table without the mark, such as an
#   existing table the configuration was pointed at, replaces it whole as well: the rows
#   of orders no batch touches would otherwise stay in it.
# - A stream over an empty table runs no batch, so when bronze is empty as the pipeline
#   starts the table is emptied here, while the pipeline's graph is being defined, if it
#   is this stream's. That also happens on a run that only validates. It is safe on any
#   run: the table is then what bronze nets to, no rows, and every order bronze receives
#   later is rebuilt by the batch that brings it.
# The writes carry no txnAppId/txnVersion: batch ids start again at 0 after a full
# refresh, and Delta would skip those writes as already done.
from pyspark import pipelines as dp

BRONZE = f"{spark.conf.get('bronze_schema')}.order_items"
TARGET = spark.conf.get("order_items_delta_table")
# How many orders one replaceWhere write lists, and how many such writes a batch may take
# before it replaces the whole table instead. Each write reads the bronze change log once,
# so a batch reads it this many times at most.
ORDERS_PER_WRITE = 10000
WRITES_PER_BATCH = 5
# Marks the table as written by this pipeline's stream over this bronze table.
MARKER = "informix.orders_test.stream"
# The pipeline's id is in spark.pipelines.pipelineId; pipelines.id is an older name for it.
PIPELINE = spark.conf.get("spark.pipelines.pipelineId", None) or spark.conf.get("pipelines.id", "")
STREAM = f"{PIPELINE}:{BRONZE}"

# The counts per distinct row of orders_silver.sql, each row repeated once per copy. A
# count that fails one of the view's expectations fails the batch. Keep the two in step:
# an operation or a column added to one belongs in the other. orders_compare_test_tool.py
# compares both tables with the source, so a difference between them shows there.
REBUILD = """
SELECT inline(array_repeat(struct(order_id, sku, qty, price, note), CAST(CASE
    WHEN other_changes <> 0 THEN raise_error(concat(
      'order_items_delta: an operation other than r, c, u or d for order ',
      CAST(order_id AS STRING)))
    WHEN copies < 0 THEN raise_error(concat(
      'order_items_delta: more deletes than inserts for a row of order ',
      CAST(order_id AS STRING)))
    ELSE copies END AS INT)))
FROM (
  SELECT order_id, sku, qty, price, note,
    sum(CASE WHEN _informix_op IN ('r', 'c', 'u') THEN 1
             WHEN _informix_op = 'd' THEN -1 ELSE 0 END) AS copies,
    sum(CASE WHEN _informix_op IN ('r', 'c', 'u', 'd') THEN 0 ELSE 1 END) AS other_changes
  FROM {bronze}
  WHERE {where}
  GROUP BY order_id, sku, qty, price, note)
"""


def write_orders(session, where=None):
    # The rows of the orders matching where (all orders when None) replace theirs.
    rows = session.sql(REBUILD.format(bronze=BRONZE, where=where or "true"))
    writer = rows.write.format("delta").mode("overwrite")
    if where is not None:
        writer = writer.option("replaceWhere", where)
    else:
        # A table taken over from something else may have other columns.
        writer = writer.option("overwriteSchema", "true")
    writer.saveAsTable(TARGET)


def marked(session):
    # Whether the table exists and is the one this stream wrote.
    if not session.catalog.tableExists(TARGET):
        return False
    properties = session.sql(f"SHOW TBLPROPERTIES {TARGET}").collect()
    return any(row.key == MARKER and row.value == STREAM for row in properties)


def write_all(session):
    write_orders(session)
    if not marked(session):
        session.sql(f"ALTER TABLE {TARGET} SET TBLPROPERTIES ('{MARKER}' = '{STREAM}')")


@dp.foreach_batch_sink(name="order_items_delta_sink")
def order_items_delta_sink(df, batch_id):
    session = df.sparkSession
    if batch_id == 0 or not marked(session):
        write_all(session)
        return
    limit = ORDERS_PER_WRITE * WRITES_PER_BATCH
    # order_id is an INT NOT NULL at the source.
    orders = sorted(
        int(row.order_id) for row in df.select("order_id").distinct().limit(limit + 1).collect()
    )
    if len(orders) > limit:
        write_all(session)
        return
    for at in range(0, len(orders), ORDERS_PER_WRITE):
        chunk = orders[at : at + ORDERS_PER_WRITE]
        write_orders(session, f"order_id IN ({', '.join(str(o) for o in chunk)})")


@dp.append_flow(target="order_items_delta_sink", name="order_items_delta_flow")
def order_items_delta_flow():
    return spark.readStream.table(BRONZE).select("order_id")


# A bronze table that is not there yet is left to the flow above to report, and a table
# this stream did not write is left alone: the first batch replaces it.
if (
    marked(spark)
    and spark.catalog.tableExists(BRONZE)
    and spark.read.table(BRONZE).isEmpty()
    and not spark.read.table(TARGET).isEmpty()
):
    spark.sql(f"TRUNCATE TABLE {TARGET}")
