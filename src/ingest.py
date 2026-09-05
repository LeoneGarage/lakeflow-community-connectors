from databricks.labs.community_connector.pipeline import ingest
from databricks.labs.community_connector import register

# Enable the injection of connection options from Unity Catalog connections into connectors
spark.conf.set("spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true")

source_name = "informix"

# =============================================================================
# INGESTION PIPELINE CONFIGURATION
# =============================================================================
# All 30 tables in the Informix `testdb` database are listed explicitly.
#
# `source_table` is the bare table name (no dots): the SDP pipeline uses it to
# build temporary view names (`source_<name>_upsert`) and the destination table
# name, and a dotted name is rejected as an illegal multipart identifier.
#
# The connector's real table identity (`owner.table`) is supplied per table via
# the `qualified_source_table` option, which the connector resolves to the
# actual Informix table. `qualified_source_table` is in the connection's
# externalOptionsAllowList, so it is passed through to the connector.
#
# Destination tables land at the pipeline default (members.connector_bronze).
# Each table uses the connector's own defaults for primary keys, cursor field,
# and ingestion type. Add more keys under `table_configuration` to customize
# (see the Informix connector README).
# =============================================================================

# The tables share one owner and one configuration shape, so they are listed by
# bare name here and expanded into the pipeline spec by the loop below rather
# than repeating the same block 30 times.
tables = [
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
]

# To customize a single table beyond the shared shape, drop it from `tables`
# above and append an explicit object to `objects` below, for example:
#

pipeline_spec = {
    "connection_name": "informix",
    "objects": [
        {
            "table": {
                "source_table": table,
                "table_configuration": {
                    "qualified_source_table": f"informix.{table}",
                },
            }
        }
        for table in tables
    ],

#     # Full config: customize destination and behavior
#     {
#         "table": {
#             "source_table": "<YOUR_TABLE_NAME>",
#             "destination_catalog": "<YOUR_CATALOG>",
#             "destination_schema": "<YOUR_SCHEMA>",
#             "destination_table": "<YOUR_TABLE>",
#             "table_configuration": {
#                 "scd_type": "<SCD_TYPE_1 | SCD_TYPE_2 | APPEND_ONLY>",
#                 "primary_keys": ["<PK_COL1>", ...],
#                 "<OTHER_OPTION_NAME>": "<VALUE>",  # e.g., for some connectors, additional options may be required (see connector's README).
#             },
#         }
#     },
#     # ... more tables to ingest...
}


# Dynamically import and register the LakeFlow source
register(spark, source_name)

# Ingest the tables specified in the pipeline spec
ingest(spark, pipeline_spec)
