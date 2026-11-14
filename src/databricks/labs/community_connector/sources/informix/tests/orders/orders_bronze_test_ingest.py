# Bronze pipeline source for the orders / order_items test: both tables land append-only,
# as change logs. Named *_test* so the connector's merge and wheel leave it out.
from databricks.labs.community_connector.pipeline import ingest
from databricks.labs.community_connector import register

spark.conf.set("spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true")

source_name = "informix"

pipeline_spec = {
    "connection_name": "informix",
    "objects": [
        # orders has a primary key, so it needs both settings to land as a change log.
        {
            "table": {
                "source_table": "orders",
                "table_configuration": {
                    "qualified_source_table": "informix.orders",
                    "append.only.ingestion": "true",
                    "scd_type": "APPEND_ONLY",
                    "snapshot.mode": "initial",
                },
            }
        },
        # order_items has no key: it lands as a change log (r, c, d and u rows). Its snapshot
        # pages by order_id, named here so that it fails instead of falling back to the
        # unordered scan, and the stream leaves out the changes the snapshot already holds.
        {
            "table": {
                "source_table": "order_items",
                "table_configuration": {
                    "qualified_source_table": "informix.order_items",
                    "snapshot.mode": "initial",
                    "snapshot.scan": "keyset(order_id)",
                },
            }
        },
    ],
}

register(spark, source_name)

ingest(spark, pipeline_spec)
