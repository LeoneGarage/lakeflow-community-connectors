# Reads the Informix connector's _community_table_metadata (each table's key, cursor and
# ingestion type) for the tables of a pipeline source file, and prints them as one line,
# SILVER_KEYS <json>, for silver_generate_test_tool.py. The connector is registered as the
# pipeline registers it. Named *_test* so the connector's merge and wheel leave it out.
#
# Run as a one-time serverless spark_python_task on environment_version 5 (4 lacks
# pyspark.sql.streaming.datasource), with two parameters: the Git folder's src directory
# and the pipeline source file (tests/cycles/informix_pipeline_test_ingest.py). The read
# leaves append.only.ingestion out of each table's configuration: the connector reports
# no key for a table read append-only, and silver needs the table's own key.
import ast
import json
import sys

import databricks

ROOT, SPEC_FILE = sys.argv[1].rstrip("/"), sys.argv[2]
sys.path.insert(0, ROOT)
databricks.__path__.append(ROOT + "/databricks")

from pyspark.sql import SparkSession  # noqa: E402

from databricks.labs.community_connector import register  # noqa: E402

FRAMEWORK_KEYS = {"append.only.ingestion", "scd_type", "primary_keys", "sequence_by"}


def pipeline_spec(path):
    with open(path, encoding="utf-8") as handle:
        tree = ast.parse(handle.read())
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") == "pipeline_spec":
            return ast.literal_eval(node.value)
    raise SystemExit(f"no pipeline_spec in {path}")


spec = pipeline_spec(SPEC_FILE)
tables = [entry["table"]["source_table"] for entry in spec["objects"]]
configs = {
    entry["table"]["source_table"]: {
        k: v
        for k, v in entry["table"].get("table_configuration", {}).items()
        if k not in FRAMEWORK_KEYS
    }
    for entry in spec["objects"]
}
spark = SparkSession.builder.getOrCreate()
try:
    spark.conf.set("spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true")
except Exception as error:  # noqa: BLE001 - a job may not set it, and injects anyway
    print(f"conf not set: {type(error).__name__}")
register(spark, "informix")
rows = (
    spark.read.format("lakeflow_connect")
    .option("databricks.connection", spec["connection_name"])
    .option("tableName", "_community_table_metadata")
    .option("tableNameList", json.dumps(tables))
    .option("tableConfigs", json.dumps(configs))
    .load()
    .collect()
)
print("SILVER_KEYS " + json.dumps([row.asDict() for row in rows]))
