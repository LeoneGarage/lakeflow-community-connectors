# Bronze pipeline source for the orders / order_items test: both tables land append-only,
# as change logs, named orders_log and order_items_log. Named *_test* so the connector's
# merge and wheel leave it out.
from databricks.labs.community_connector.pipeline import ingest
from databricks.labs.community_connector import register

spark.conf.set("spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true")

source_name = "informix"

pipeline_spec = {
    "connection_name": "informix",
    "objects": [
        # orders has a primary key, so it needs both settings to land as a change log. Its
        # copy is incremental, the default for a table with a key: in chunks beside the stream.
        {
            "table": {
                "source_table": "orders",
                "destination_table": "orders_log",
                "table_configuration": {
                    "qualified_source_table": "informix.orders",
                    "append.only.ingestion": "true",
                    "scd_type": "APPEND_ONLY",
                },
            }
        },
        # order_items has no key: it lands as a change log (r, c, d and u rows). Under
        # keyset the connector picks the index its snapshot pages by (here the one on
        # order_id), and the stream leaves out the changes the snapshot already holds.
        # With no index it could use, it would read without index order, with a warning.
        {
            "table": {
                "source_table": "order_items",
                "destination_table": "order_items_log",
                "table_configuration": {
                    "qualified_source_table": "informix.order_items",
                    # Small pages, so that the scan takes many and can be stopped part-way.
                    "keyless.snapshot.page.size": "200",
                },
            }
        },
    ],
}

register(spark, source_name)

ingest(spark, pipeline_spec)
