# Pipeline source for informix-pipeline, the demo1 pipeline the cycles job
# (informix_cycles_job_test_tool.py) runs: the demo testdb's tables into the pipeline's
# catalog and schema. Named *_test* so the connector's merge and wheel leave it out.
from databricks.labs.community_connector.pipeline import ingest
from databricks.labs.community_connector import register

# Enable the injection of connection options from Unity Catalog connections into connectors
spark.conf.set("spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true")

source_name = "informix"

# =============================================================================
# INGESTION PIPELINE CONFIGURATION
# =============================================================================
#
# pipeline_spec
# ├── connection_name (required): The Unity Catalog connection name
# └── objects[]: List of tables to ingest
#     └── table
#         ├── source_table (required): The table name in the source system
#         ├── destination_catalog (optional): Target catalog (defaults to pipeline's default)
#         ├── destination_schema (optional): Target schema (defaults to pipeline's default)
#         ├── destination_table (optional): Target table name (defaults to source_table)
#         └── table_configuration (optional)
#             ├── scd_type: "SCD_TYPE_1" (default), "SCD_TYPE_2", or "APPEND_ONLY"
#             ├── primary_keys: List of columns to override connector's default keys
#             └── (other options): See source connector's README
# =============================================================================

# Every table lands in the pipeline's default catalog and schema
# (members.connector_bronze) under its Informix table name.
# qualified_source_table maps each name to its Informix owner.table.
pipeline_spec = {
    "connection_name": "informix",
    "objects": [
        {
            "table": {
                "source_table": "archivefulfillment",
                "table_configuration": {
                    "qualified_source_table": "informix.archivefulfillment",
                },
            }
        },
        {
            "table": {
                "source_table": "tw060",
                "table_configuration": {
                    "qualified_source_table": "informix.tw060",
                },
            }
        },
        {
            "table": {
                "source_table": "tw060_ovhc",
                "table_configuration": {
                    "qualified_source_table": "informix.tw060_ovhc",
                },
            }
        },
        {
            "table": {
                "source_table": "tw070",
                "table_configuration": {
                    "qualified_source_table": "informix.tw070",
                },
            }
        },
        {
            "table": {
                "source_table": "tw070d",
                "table_configuration": {
                    "qualified_source_table": "informix.tw070d",
                },
            }
        },
        {
            "table": {
                "source_table": "tw101",
                "table_configuration": {
                    "qualified_source_table": "informix.tw101",
                },
            }
        },
        {
            "table": {
                "source_table": "tw101_ovhc",
                "table_configuration": {
                    "qualified_source_table": "informix.tw101_ovhc",
                },
            }
        },
        {
            "table": {
                "source_table": "tw109",
                "table_configuration": {
                    "qualified_source_table": "informix.tw109",
                },
            }
        },
        {
            "table": {
                "source_table": "tw109_ovhc",
                "table_configuration": {
                    "qualified_source_table": "informix.tw109_ovhc",
                },
            }
        },
        {
            "table": {
                "source_table": "tw110",
                "table_configuration": {
                    "qualified_source_table": "informix.tw110",
                },
            }
        },
        {
            "table": {
                "source_table": "tw110_ovhc",
                "table_configuration": {
                    "qualified_source_table": "informix.tw110_ovhc",
                },
            }
        },
        {
            "table": {
                "source_table": "tw111",
                "table_configuration": {
                    "qualified_source_table": "informix.tw111",
                },
            }
        },
        {
            "table": {
                "source_table": "tw111d",
                "table_configuration": {
                    "qualified_source_table": "informix.tw111d",
                },
            }
        },
        {
            "table": {
                "source_table": "tw142_forcedmerge",
                "table_configuration": {
                    "qualified_source_table": "informix.tw142_forcedmerge",
                },
            }
        },
        {
            "table": {
                "source_table": "tw201",
                "table_configuration": {
                    "qualified_source_table": "informix.tw201",
                },
            }
        },
        {
            "table": {
                "source_table": "tw202",
                "table_configuration": {
                    "qualified_source_table": "informix.tw202",
                },
            }
        },
        {
            "table": {
                "source_table": "tw202d",
                "table_configuration": {
                    "qualified_source_table": "informix.tw202d",
                },
            }
        },
        {
            "table": {
                "source_table": "tw205",
                "table_configuration": {
                    "qualified_source_table": "informix.tw205",
                },
            }
        },
        {
            "table": {
                "source_table": "tw208",
                "table_configuration": {
                    "qualified_source_table": "informix.tw208",
                },
            }
        },
        {
            "table": {
                "source_table": "tw214",
                "table_configuration": {
                    "qualified_source_table": "informix.tw214",
                },
            }
        },
        {
            "table": {
                "source_table": "tw221",
                "table_configuration": {
                    "qualified_source_table": "informix.tw221",
                },
            }
        },
        {
            "table": {
                "source_table": "tw221d",
                "table_configuration": {
                    "qualified_source_table": "informix.tw221d",
                },
            }
        },
        {
            "table": {
                "source_table": "tw241",
                "table_configuration": {
                    "qualified_source_table": "informix.tw241",
                },
            }
        },
        {
            "table": {
                "source_table": "tw241_ovhc",
                "table_configuration": {
                    "qualified_source_table": "informix.tw241_ovhc",
                },
            }
        },
        {
            "table": {
                "source_table": "tw295",
                "table_configuration": {
                    "qualified_source_table": "informix.tw295",
                },
            }
        },
        {
            "table": {
                "source_table": "tw301",
                "table_configuration": {
                    "qualified_source_table": "informix.tw301",
                },
            }
        },
        {
            "table": {
                "source_table": "tw303",
                "table_configuration": {
                    "qualified_source_table": "informix.tw303",
                },
            }
        },
        {
            "table": {
                "source_table": "tw304",
                "table_configuration": {
                    "qualified_source_table": "informix.tw304",
                },
            }
        },
        {
            "table": {
                "source_table": "tw316",
                "table_configuration": {
                    "qualified_source_table": "informix.tw316",
                },
            }
        },
        {
            "table": {
                "source_table": "tw316_ovhc",
                "table_configuration": {
                    "qualified_source_table": "informix.tw316_ovhc",
                },
            }
        },
        # Keyless tables (no primary key, no unique index). tw900_member_tag is keyed
        # on the whole row, so it lands as current state; tw901_claim_note keeps the
        # default change log, copying its existing rows first.
        {
            "table": {
                "source_table": "tw900_member_tag",
                "table_configuration": {
                    "qualified_source_table": "informix.tw900_member_tag",
                    "primary.keys": "*",
                    "snapshot.scan": "cursor",
                },
            }
        },
        {
            "table": {
                "source_table": "tw901_claim_note",
                "table_configuration": {
                    "qualified_source_table": "informix.tw901_claim_note",
                    "snapshot.mode": "initial",
                },
            }
        },
    ],
}


# Dynamically import and register the LakeFlow source
register(spark, source_name)

# Ingest the tables specified in the pipeline spec
ingest(spark, pipeline_spec)
