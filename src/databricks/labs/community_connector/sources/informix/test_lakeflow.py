"""Source-local Lakeflow contract regressions using an in-memory bridge."""

from __future__ import annotations

import pickle
import sys
import threading
import types
import unittest
from datetime import date, datetime

# The connector's production API uses PySpark type objects, but protocol/unit
# environments intentionally do not install the large PySpark distribution.
if "pyspark.sql.types" not in sys.modules:
    pyspark = types.ModuleType("pyspark")
    sql = types.ModuleType("pyspark.sql")
    spark_types = types.ModuleType("pyspark.sql.types")

    class _Type:
        pass

    class StructField:
        def __init__(self, name, data_type, nullable=True):
            self.name, self.dataType, self.nullable = name, data_type, nullable

    class StructType:
        def __init__(self, fields=()):
            self.fields = list(fields)

    for name in (
        "BinaryType",
        "BooleanType",
        "DateType",
        "DoubleType",
        "FloatType",
        "IntegerType",
        "LongType",
        "ShortType",
        "StringType",
        "TimestampType",
    ):
        setattr(spark_types, name, type(name, (_Type,), {}))

    class DecimalType(_Type):
        def __init__(self, precision=10, scale=0):
            self.precision, self.scale = precision, scale

    spark_types.DecimalType = DecimalType
    spark_types.StructField = StructField
    spark_types.StructType = StructType
    sys.modules.update(
        {"pyspark": pyspark, "pyspark.sql": sql, "pyspark.sql.types": spark_types}
    )

from databricks.labs.community_connector.sources.informix.informix import (  # noqa: E402
    _DEFAULT_MAX_RECORDS_PER_BATCH,
    _DEFAULT_SNAPSHOT_PAGE_SIZE,
    CURSOR,
    Column,
    InformixLakeflowConnect,
    LogRetentionError,
    UnsupportedChangeError,
    _bridge_config,
    _catalog_column,
    _framework_value,
    _spark_type,
)


def _table(owner="app", name="orders", cdc=True):
    return {
        "database": "demo",
        "owner": owner,
        "name": name,
        "columns": [
            {"name": "id", "type_name": "INTEGER", "nullable": False},
            {"name": "value", "type_name": "VARCHAR", "length": 20,
             "cdc_supported": cdc},
        ],
        "primary_keys": ["id"],
    }


class FakeBridge:
    def __init__(self):
        self.tables = [_table(), _table("sysadmin", "hidden"), _table(name="audit")]
        self.rows = [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}]
        self.changes = []
        self.now, self.minimum = 90, 1
        self.snapshot_calls = []

    def list_tables(self):
        return self.tables

    def get_table(self, identity):
        return next(t for t in self.tables if identity.endswith(f".{t['owner']}.{t['name']}"))

    def current_lsn(self):
        return self.now

    def minimum_lsn(self):
        return self.minimum

    def snapshot_page(self, identity, columns, primary_keys, after, limit):
        self.snapshot_calls.append((identity, tuple(columns), tuple(primary_keys), after, limit))
        rows = self.rows
        if after is not None:
            if len(after) != len(primary_keys):
                raise AssertionError("snapshot continuation arity changed")
            rows = [row for row in rows if row[primary_keys[0]] > after[0]]
        return rows[:limit]

    def read_changes(self, tables, start_lsn, timeout_seconds, max_records):
        return list(self.changes)


def _stream_offset(lsn=90):
    return {
        "commit_lsn": str(lsn), "change_lsn": str(lsn),
        "begin_lsn": str(lsn), "tx_id": None, "phase": "stream",
    }


class LakeflowContractTests(unittest.TestCase):
    def connector(self, bridge=None, **options):
        connector = InformixLakeflowConnect({"database": "demo", **options})
        connector._bridge_instance = bridge or FakeBridge()
        return connector

    def test_live_catalog_datetime_qualifier_is_normalized_for_cdc(self):
        column = _catalog_column(
            {"colname": "updated_at", "coltype": 10, "collength": 0x130F, "colno": 2}
        )
        self.assertEqual(column["length"], 0x000F)

    def test_spark_serialization_discards_live_bridge_state(self):
        connector = self.connector()
        connector._bridge_instance.unpicklable_lock = threading.Lock()
        restored = pickle.loads(pickle.dumps(connector))
        self.assertIsNone(restored._bridge_instance)
        self.assertEqual(restored.options, connector.options)

    def test_framework_temporal_values_are_iso_strings(self):
        self.assertEqual(_framework_value(date(2008, 6, 16)), "2008-06-16")
        self.assertEqual(
            _framework_value(datetime(2026, 7, 20, 1, 2, 3, 456000)),
            "2026-07-20T01:02:03.456000",
        )

    def test_cdc_max_records_matches_live_informix_boundary(self):
        self.connector(**{"cdc.max.records": "256"})
        with self.assertRaisesRegex(ValueError, "must be <= 256"):
            self.connector(**{"cdc.max.records": "257"})

    def test_locale_defaults(self):
        config = _bridge_config(
            {"hostname": "host", "database": "db", "user": "user", "password": "secret"}
        )
        self.assertEqual(config["db_locale"], "en_US.819")
        self.assertEqual(config["client_locale"], "en_US.utf8")

    def test_batch_size_defaults(self):
        self.assertEqual(_DEFAULT_SNAPSHOT_PAGE_SIZE, 10000)
        self.assertEqual(_DEFAULT_MAX_RECORDS_PER_BATCH, 10000)

    def test_smallint_schema_uses_framework_supported_integer_type(self):
        for type_name in ("SMALLINT", "INT2"):
            spark_type = _spark_type(Column(name="flag", type_name=type_name))
            self.assertEqual(type(spark_type).__name__, "IntegerType")

    def test_discovery_filter_schema_and_metadata(self):
        connector = self.connector(table_include_list="ignored")
        connector.options["table.include.list"] = "app.*"
        connector.options["table.exclude.list"] = "*.audit"
        self.assertEqual(connector.list_tables(), ["app.orders"])
        schema = connector.get_table_schema("app.orders", {})
        self.assertEqual([field.name for field in schema.fields][-4:],
                         [CURSOR, "_informix_commit_lsn", "_informix_tx_id", "_informix_op"])
        self.assertEqual(connector.read_table_metadata("app.orders", {}), {
            "primary_keys": ["id"], "cursor_field": CURSOR,
            "ingestion_type": "cdc_with_deletes",
        })

    def test_qualified_source_table_maps_logical_to_owner_qualified_name(self):
        connector = self.connector()
        schema = connector.get_table_schema(
            "orders", {"qualified_source_table": "app.orders"}
        )
        self.assertEqual(schema.fields[0].name, "id")
        with self.assertRaisesRegex(ValueError, "Unknown or excluded"):
            connector.get_table_schema("orders", {"source_table": "app.orders"})

    def test_snapshot_paging_and_independent_channel_high_water(self):
        bridge = FakeBridge()
        connector = self.connector(bridge, **{"snapshot.page.size": "1"})
        first, offset = connector.read_table("app.orders", {}, {})
        self.assertEqual([row["id"] for row in first], [1])
        self.assertEqual(offset["snapshot"]["last_pk"], [1])
        second, end = connector.read_table("app.orders", offset, {})
        self.assertEqual([row["id"] for row in second], [2])
        self.assertEqual(end["phase"], "stream")
        deletes, delete_offset = connector.read_table_deletes("app.orders", {}, {})
        self.assertEqual(list(deletes), [])
        self.assertEqual(delete_offset["commit_lsn"], "90")
        self.assertEqual(bridge.snapshot_calls[1][3], [1])

    def test_insert_update_delete_pk_change_rollback_discard_and_controls(self):
        bridge = FakeBridge()
        bridge.changes = [
            {"op": "METADATA"}, {"op": "TIMEOUT", "lsn": 89},
            {"op": "BEGIN", "tx_id": 1, "lsn": 100},
            {"op": "INSERT", "tx_id": 1, "lsn": 101, "row": {"id": 1, "value": "a"}},
            {"op": "BEFORE_UPDATE", "tx_id": 1, "lsn": 102,
             "row": {"id": 1, "value": "a"}},
            {"op": "AFTER_UPDATE", "tx_id": 1, "lsn": 103,
             "row": {"id": 2, "value": "b"}},
            {"op": "DELETE", "tx_id": 1, "lsn": 104,
             "row": {"id": 2, "value": "b"}},
            {"op": "COMMIT", "tx_id": 1, "lsn": 110},
            {"op": "BEGIN", "tx_id": 2, "lsn": 120},
            {"op": "INSERT", "tx_id": 2, "lsn": 121, "row": {"id": 9, "value": "x"}},
            {"op": "DISCARD", "tx_id": 2, "lsn": 121},
            {"op": "COMMIT", "tx_id": 2, "lsn": 122},
            {"op": "BEGIN", "tx_id": 3, "lsn": 130},
            {"op": "INSERT", "tx_id": 3, "lsn": 131, "row": {"id": 8, "value": "x"}},
            {"op": "ROLLBACK", "tx_id": 3, "lsn": 132},
        ]
        connector = self.connector(bridge)
        changes, _ = connector.read_table("app.orders", _stream_offset(), {})
        self.assertEqual(
            [(row["id"], row["_informix_op"]) for row in changes], [(1, "c"), (2, "u")]
        )
        deletes, _ = connector.read_table_deletes("app.orders", _stream_offset(), {})
        self.assertEqual([row["id"] for row in deletes], [1, 2])

    def test_retention_and_truncate_fail_explicitly(self):
        bridge = FakeBridge()
        bridge.minimum = 91
        connector = self.connector(bridge)
        with self.assertRaises(LogRetentionError):
            connector.read_table("app.orders", _stream_offset(), {})
        bridge.minimum = 1
        bridge.changes = [
            {"op": "BEGIN", "tx_id": 1, "lsn": 100},
            {"op": "TRUNCATE", "tx_id": 1, "lsn": 101, "table": "app.orders"},
            {"op": "COMMIT", "tx_id": 1, "lsn": 102},
        ]
        with self.assertRaises(UnsupportedChangeError):
            connector.read_table("app.orders", _stream_offset(), {})


if __name__ == "__main__":
    unittest.main()
