"""Source-local Lakeflow contract regressions using an in-memory bridge."""

from __future__ import annotations

import errno
import hashlib
import importlib
import json
import os
import pickle
import sys
import tempfile
import threading
import types
import unittest
from datetime import date, datetime
from unittest import mock

# The connector's production API uses PySpark type objects, but protocol/unit
# environments intentionally do not install the large PySpark distribution.
if "pyspark.sql.types" not in sys.modules:
    pyspark = types.ModuleType("pyspark")
    sql = types.ModuleType("pyspark.sql")
    datasource = types.ModuleType("pyspark.sql.datasource")
    streaming = types.ModuleType("pyspark.sql.streaming")
    streaming_datasource = types.ModuleType("pyspark.sql.streaming.datasource")
    spark_types = types.ModuleType("pyspark.sql.types")

    class _Type:
        pass

    class StructField:
        def __init__(self, name, data_type, nullable=True):
            self.name, self.dataType, self.nullable = name, data_type, nullable

    class StructType:
        def __init__(self, fields=()):
            self.fields = list(fields)

    class Row(dict):
        def __init__(self, **kwargs):
            super().__init__(kwargs)

    class _DataSource:
        pass

    class _SupportsTriggerAvailableNow:
        pass

    sql.Row = Row
    for name in (
        "DataSource",
        "DataSourceReader",
        "DataSourceStreamReader",
        "InputPartition",
        "SimpleDataSourceStreamReader",
    ):
        setattr(datasource, name, type(name, (_DataSource,), {}))
    streaming_datasource.ReadAllAvailable = type("ReadAllAvailable", (), {})
    streaming_datasource.SupportsTriggerAvailableNow = _SupportsTriggerAvailableNow

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
        "ArrayType",
        "DataType",
        "MapType",
        "VariantType",
        "VariantVal",
    ):
        setattr(spark_types, name, type(name, (_Type,), {}))

    class DecimalType(_Type):
        def __init__(self, precision=10, scale=0):
            self.precision, self.scale = precision, scale

    spark_types.DecimalType = DecimalType
    spark_types.StructField = StructField
    spark_types.StructType = StructType
    sys.modules.update(
        {
            "pyspark": pyspark,
            "pyspark.sql": sql,
            "pyspark.sql.datasource": datasource,
            "pyspark.sql.streaming": streaming,
            "pyspark.sql.streaming.datasource": streaming_datasource,
            "pyspark.sql.types": spark_types,
        }
    )

from databricks.labs.community_connector.sources.informix import (  # noqa: E402
    informix as informix_module,
)
from databricks.labs.community_connector.sources.informix.informix import (  # noqa: E402
    _DEFAULT_MAX_RECORDS_PER_BATCH,
    _DEFAULT_SNAPSHOT_PAGE_SIZE,
    _OFFSET_VERSION,
    CURSOR,
    Column,
    CommittedTransaction,
    InformixError,
    InformixLakeflowConnect,
    LogRetentionError,
    PurePythonInformixBridge,
    Table,
    TransactionBuffer,
    UnsupportedChangeError,
    _bridge_config,
    _capture_descriptor,
    _catalog_column,
    _committed_transactions,
    _framework_value,
    _informix_available_now_base,
    _recover,
    _schema_fingerprint,
    _schema_state,
    _sortable_lsn,
    _spark_type,
    _validate_shared_state_filesystem,
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
        self.prepared_identities = []
        self.validated_initial = []

    def list_tables(self):
        return self.tables

    def get_table(self, identity):
        return next(t for t in self.tables if identity.endswith(f".{t['owner']}.{t['name']}"))

    def current_lsn(self):
        return self.now

    def minimum_lsn(self):
        return self.minimum

    def prepare_initial_capture(self, identities):
        self.prepared_identities = list(identities)
        return self.now

    def validate_initial_lsn(self, capture, start_lsn):
        self.validated_initial.append((capture["identity"], start_lsn))

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


class FakeCdcTransport:
    def __init__(self, chunks):
        self.chunks = list(chunks)
        self.reads = 0

    def execute(self, sql, parameters=()):
        if "sysenv" in sql:
            return [{"env_value": "demo_server"}]
        if "cdc_opensess" in sql:
            return [{"session_id": 4}]
        return [{"status": 0}]

    def read_lodata(self, descriptor, requested):
        self.reads += 1
        return self.chunks.pop(0) if self.chunks else []


class RecordParser:
    def __init__(self, maximum):
        self.buffered_bytes = 0

    def feed(self, chunk):
        yield from chunk


def _stream_offset(lsn=90):
    return {
        "version": _OFFSET_VERSION,
        "commit_lsn": str(lsn), "change_lsn": str(lsn),
        "begin_lsn": str(lsn), "tx_id": None, "phase": "stream",
        "schema_fingerprint": _schema_fingerprint(Table.parse(_table(), "demo")),
        "schema_id": "1" * 32,
        "pipeline_scope": hashlib.sha256(b"test-pipeline").hexdigest()[:32],
    }


class LakeflowContractTests(unittest.TestCase):
    def setUp(self):
        self._shared_state = tempfile.TemporaryDirectory()

    def tearDown(self):
        self._shared_state.cleanup()

    def connector(self, bridge=None, **options):
        scope_label = str(options.pop("registration_scope", "test-pipeline"))
        connector = InformixLakeflowConnect(
            {
                "database": "demo",
                "cdc.shared.state.location": self._shared_state.name,
                **options,
            }
        )
        connector.set_registration_scope(
            hashlib.sha256(scope_label.encode()).hexdigest()[:32]
        )
        connector._bridge_instance = bridge or FakeBridge()
        return connector

    def test_shared_state_location_is_mandatory_and_absolute(self):
        with self.assertRaisesRegex(ValueError, "cdc.shared.state.location"):
            InformixLakeflowConnect({"database": "demo"})
        with self.assertRaisesRegex(ValueError, "absolute path"):
            InformixLakeflowConnect(
                {"database": "demo", "cdc.shared.state.location": "relative"}
            )
        with self.assertRaisesRegex(ValueError, "Unity Catalog Volume"):
            InformixLakeflowConnect(
                {
                    "database": "demo",
                    "hostname": "host",
                    "cdc.shared.state.location": self._shared_state.name,
                }
            )
        with self.assertRaisesRegex(ValueError, "traversal"):
            InformixLakeflowConnect(
                {
                    "database": "demo",
                    "hostname": "host",
                    "cdc.shared.state.location": "/Volumes/catalog/schema/volume/../other",
                }
            )

    def test_shared_state_connection_key_includes_port(self):
        table = Table.parse(_table(), "demo")
        first = self.connector(FakeBridge(), port="9088")
        second = self.connector(FakeBridge(), port="9089")

        self.assertNotEqual(
            first._shared_table_state_root(table),
            second._shared_table_state_root(table),
        )
        equivalent = self.connector(FakeBridge())
        equivalent.options.update(
            hostname="EXAMPLE.COM.", port="09088", server="demo", database="demo"
        )
        canonical = self.connector(FakeBridge())
        canonical.options.update(
            hostname="example.com", port="9088", server="demo", database="demo"
        )
        self.assertEqual(
            equivalent._shared_table_state_root(table),
            canonical._shared_table_state_root(table),
        )
        distinct_case = self.connector(FakeBridge())
        distinct_case.options.update(
            hostname="example.com", port="9088", server="DEMO", database="DEMO"
        )
        self.assertNotEqual(
            distinct_case._shared_table_state_root(table),
            canonical._shared_table_state_root(table),
        )
        with self.assertRaisesRegex(ValueError, "Unity Catalog Volume"):
            InformixLakeflowConnect(
                {
                    "database": "demo",
                    "hostname": "host",
                    "cdc.shared.state.location": "/Volumes/catalog-only",
                }
            )

    def test_live_catalog_datetime_qualifier_is_normalized_for_cdc(self):
        column = _catalog_column(
            {"colname": "updated_at", "coltype": 10, "collength": 0x130F, "colno": 2}
        )
        self.assertEqual(column["length"], 0x000F)
        self.assertTrue(column["cdc_supported"])

        unsupported = _catalog_column(
            {"colname": "invalid", "coltype": 10, "collength": 0x1314, "colno": 3}
        )
        self.assertFalse(unsupported["cdc_supported"])

    def test_catalog_enables_implemented_scalar_cdc_types(self):
        for type_id in (17, 18, 43):
            with self.subTest(type_id=type_id):
                column = _catalog_column(
                    {"colname": "value", "coltype": type_id, "collength": 100, "colno": 1}
                )
                self.assertTrue(column["cdc_supported"])

    def test_catalog_native_type_ids_are_not_confused_with_complex_types(self):
        expected = {
            19: "SET",
            20: "MULTISET",
            21: "LIST",
            22: "ROW",
            23: "COLLECTION",
            40: "UDT_VAR",
            41: "UDT_FIXED",
            43: "LVARCHAR",
            45: "BOOLEAN",
            52: "BIGINT",
            53: "BIGSERIAL",
            101: "BLOB",
            102: "CLOB",
        }
        for type_id, type_name in expected.items():
            with self.subTest(type_id=type_id):
                column = _catalog_column(
                    {"colname": "value", "coltype": type_id, "collength": 8, "colno": 1}
                )
                self.assertEqual(column["type_name"], type_name)

    def test_catalog_resolves_builtin_extended_scalar_types(self):
        lvarchar = _catalog_column(
            {
                "colname": "value",
                "coltype": 40,
                "collength": 16,
                "colno": 1,
                "tabid": 100,
                "extended_id": 1,
                "extended_name": "lvarchar",
                "extended_owner": "informix",
            }
        )
        boolean = _catalog_column(
            {
                "colname": "enabled",
                "coltype": 41,
                "collength": 1,
                "colno": 2,
                "tabid": 100,
                "extended_id": 5,
                "extended_name": "boolean",
                "extended_owner": "informix",
            }
        )

        self.assertEqual(lvarchar["type_name"], "LVARCHAR")
        self.assertTrue(lvarchar["cdc_supported"])
        self.assertEqual(boolean["type_name"], "BOOLEAN")
        self.assertTrue(boolean["cdc_supported"])

    def test_catalog_does_not_promote_user_defined_builtin_names(self):
        for coltype, extended_name, expected in (
            (40, "lvarchar", "UDT_VAR"),
            (41, "boolean", "UDT_FIXED"),
        ):
            with self.subTest(extended_name=extended_name):
                column = _catalog_column(
                    {
                        "colname": "value",
                        "coltype": coltype,
                        "collength": 16,
                        "extended_name": extended_name,
                        "extended_owner": "application",
                    }
                )

                self.assertEqual(column["type_name"], expected)
                self.assertFalse(column["cdc_supported"])

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

    def test_decimal_lsn_strings_preserve_numeric_order(self):
        self.assertLess(_sortable_lsn(99), _sortable_lsn(100))
        self.assertEqual(_sortable_lsn(100), "00000000000000000100")
        self.assertEqual(_sortable_lsn((1 << 64) - 1), "18446744073709551615")
        with self.assertRaisesRegex(InformixError, "unsigned 64-bit"):
            _sortable_lsn(1 << 64)

    def test_cdc_descriptors_use_client_locale_encoding(self):
        connector = self.connector(**{"CLIENT_LOCALE": "en_US.819"})
        table = connector._table("app.orders", {})
        descriptor = _capture_descriptor(
            table, informix_module._client_encoding(connector.options)
        )
        self.assertEqual(
            {column["encoding"] for column in descriptor["descriptors"]},
            {"iso8859-1"},
        )

    def test_cdc_max_records_matches_live_informix_boundary(self):
        self.connector(**{"cdc.max.records": "256"})
        with self.assertRaisesRegex(ValueError, "must be <= 256"):
            self.connector(**{"cdc.max.records": "257"})

    def test_connection_port_range_is_validated_before_connecting(self):
        self.connector(**{"port": "1"})
        self.connector(**{"port": "65535"})
        for value in ("0", "65536", "-1"):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "port"):
                self.connector(**{"port": value})

    def test_production_bridge_requires_server_name_with_connection_options(self):
        options = {
            "hostname": "db.example",
            "database": "demo",
            "user": "alice",
            "password": "secret",
        }
        with self.assertRaisesRegex(ValueError, "server"):
            PurePythonInformixBridge(options)

    def test_native_record_target_reads_an_open_transaction_through_commit(self):
        transport = FakeCdcTransport(
            [
                [
                    {"op": "BEGIN", "tx_id": 1, "lsn": 100},
                    {"op": "INSERT", "tx_id": 1, "lsn": 101},
                ],
                [
                    {"op": "INSERT", "tx_id": 1, "lsn": 102},
                    {"op": "COMMIT", "tx_id": 1, "lsn": 103},
                ],
            ]
        )
        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.transport = transport
        capture = {
            "identity": "demo:app.orders",
            "logical_identity": "demo.app.orders",
            "columns": ["id"],
            "descriptors": [{"name": "id", "type_name": "INTEGER"}],
        }

        with mock.patch.object(informix_module, "CdcFrameParser", RecordParser), mock.patch.object(
            informix_module, "decode_frame", side_effect=lambda frame, labels: dict(frame)
        ):
            records = bridge.read_changes([capture], 90, 1, 2)

        self.assertEqual(
            [record["op"] for record in records], ["BEGIN", "INSERT", "INSERT", "COMMIT"]
        )
        self.assertEqual(transport.reads, 2)

    def test_native_record_target_does_not_count_metadata(self):
        transport = FakeCdcTransport(
            [[
                {"op": "METADATA", "label": 1, "metadata": [{"name": "id"}]},
                {"op": "BEGIN", "tx_id": 1, "lsn": 100},
                {"op": "INSERT", "tx_id": 1, "lsn": 101},
                {"op": "COMMIT", "tx_id": 1, "lsn": 102},
            ]]
        )
        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.transport = transport
        capture = {
            "identity": "demo:app.orders",
            "logical_identity": "demo.app.orders",
            "columns": ["id"],
            "descriptors": [{"name": "id", "type_name": "INTEGER"}],
        }

        with mock.patch.object(informix_module, "CdcFrameParser", RecordParser), mock.patch.object(
            informix_module, "decode_frame", side_effect=lambda frame, labels: dict(frame)
        ), mock.patch.object(
            informix_module, "metadata_column_names", return_value=["id"]
        ), mock.patch.object(
            bridge, "_assert_capture_layout"
        ):
            records = bridge.read_changes([capture], 90, 1, 1)

        self.assertEqual(
            [record["op"] for record in records], ["METADATA", "BEGIN", "INSERT", "COMMIT"]
        )

    def test_native_metadata_rejects_catalog_layout_change_before_row_decode(self):
        bridge = object.__new__(PurePythonInformixBridge)
        changed = _table()
        changed["columns"][0] = {
            "name": "id",
            "type_name": "BIGINT",
            "nullable": False,
        }
        bridge._describe_table = mock.Mock(return_value=changed)
        original = _capture_descriptor(Table.parse(_table(), "demo"), "utf-8")

        with self.assertRaisesRegex(InformixError, "schema changed.*full refresh"):
            bridge._assert_capture_layout(original, "utf-8")

    def test_native_poll_rejects_second_metadata_layout(self):
        transport = FakeCdcTransport(
            [[
                {"op": "METADATA", "label": 1, "metadata": b"id integer"},
                {"op": "METADATA", "label": 1, "metadata": b"id bigint"},
            ]]
        )
        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.transport = transport
        capture = {
            "identity": "demo:app.orders",
            "logical_identity": "demo.app.orders",
            "columns": ["id"],
            "descriptors": [{"name": "id", "type_name": "INTEGER", "encoding": "utf-8"}],
        }

        with mock.patch.object(informix_module, "CdcFrameParser", RecordParser), mock.patch.object(
            informix_module, "decode_frame", side_effect=lambda frame, labels: dict(frame)
        ), mock.patch.object(
            informix_module, "metadata_column_names", return_value=["id"]
        ), mock.patch.object(
            bridge, "_assert_capture_layout"
        ), self.assertRaisesRegex(InformixError, "second CDC metadata layout"):
            bridge.read_changes([capture], 90, 1, 64)

    def test_native_poll_temporarily_extends_socket_timeout(self):
        class TimedTransport(FakeCdcTransport):
            def __init__(self):
                super().__init__([[{"op": "TIMEOUT", "lsn": 100}]])
                self.socket_timeout = 30.0
                self.timeouts = []

            def set_socket_timeout(self, timeout):
                self.socket_timeout = timeout
                self.timeouts.append(timeout)

        transport = TimedTransport()
        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.transport = transport
        capture = {
            "identity": "demo:app.orders",
            "logical_identity": "demo.app.orders",
            "columns": ["id"],
            "descriptors": [{"name": "id", "type_name": "INTEGER"}],
        }

        with mock.patch.object(informix_module, "CdcFrameParser", RecordParser), mock.patch.object(
            informix_module, "decode_frame", side_effect=lambda frame, labels: dict(frame)
        ):
            bridge.read_changes([capture], 90, 60, 2)

        self.assertEqual(transport.timeouts, [65.0, 30.0])

    def test_default_cdc_poll_byte_bound_skips_accounting(self):
        transport = FakeCdcTransport([[{"op": "TIMEOUT", "lsn": 100}]])
        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.transport = transport
        capture = {
            "identity": "demo:app.orders",
            "logical_identity": "demo.app.orders",
            "columns": ["id"],
            "descriptors": [{"name": "id", "type_name": "INTEGER"}],
        }

        with mock.patch.object(informix_module, "CdcFrameParser", RecordParser), mock.patch.object(
            informix_module, "decode_frame", side_effect=lambda frame, labels: dict(frame)
        ), mock.patch.object(
            informix_module, "_deep_size", side_effect=AssertionError("must not account")
        ):
            bridge.read_changes([capture], 90, 1, 2)

    def test_native_cdc_cleanup_rejects_nonzero_status(self):
        class FailedCleanupTransport(FakeCdcTransport):
            def execute(self, sql, parameters=()):
                if "cdc_endcapture" in sql:
                    return [{"status": -1}]
                return super().execute(sql, parameters)

        transport = FailedCleanupTransport([[{"op": "TIMEOUT", "lsn": 100}]])
        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.transport = transport
        capture = {
            "identity": "demo:app.orders",
            "logical_identity": "demo.app.orders",
            "columns": ["id"],
            "descriptors": [{"name": "id", "type_name": "INTEGER"}],
        }

        with mock.patch.object(informix_module, "CdcFrameParser", RecordParser), mock.patch.object(
            informix_module, "decode_frame", side_effect=lambda frame, labels: dict(frame)
        ), self.assertRaisesRegex(InformixError, "CDC session cleanup failed"):
            bridge.read_changes([capture], 90, 1, 2)

    def test_native_cdc_cleanup_is_attached_to_primary_error(self):
        class FailedCleanupTransport(FakeCdcTransport):
            def execute(self, sql, parameters=()):
                if "cdc_endcapture" in sql:
                    return [{"status": -1}]
                return super().execute(sql, parameters)

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.transport = FailedCleanupTransport([[{"op": "BROKEN"}]])
        capture = {
            "identity": "demo:app.orders",
            "logical_identity": "demo.app.orders",
            "columns": ["id"],
            "descriptors": [{"name": "id", "type_name": "INTEGER"}],
        }

        with mock.patch.object(informix_module, "CdcFrameParser", RecordParser), mock.patch.object(
            informix_module, "decode_frame", side_effect=ValueError("primary")
        ), self.assertRaisesRegex(ValueError, "primary") as caught:
            bridge.read_changes([capture], 90, 1, 2)

        self.assertIn("cleanup also failed", " ".join(caught.exception.__notes__))

    def test_timeout_discards_later_frames_in_the_same_native_chunk(self):
        transport = FakeCdcTransport(
            [[{"op": "TIMEOUT", "lsn": 100}, {"op": "BEGIN", "tx_id": 2, "lsn": 101}]]
        )
        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.transport = transport
        capture = {
            "identity": "demo:app.orders",
            "logical_identity": "demo.app.orders",
            "columns": ["id"],
            "descriptors": [{"name": "id", "type_name": "INTEGER"}],
        }

        with mock.patch.object(informix_module, "CdcFrameParser", RecordParser), mock.patch.object(
            informix_module, "decode_frame", side_effect=lambda frame, labels: dict(frame)
        ):
            records = bridge.read_changes([capture], 90, 1, 2)

        self.assertEqual([record["op"] for record in records], ["TIMEOUT"])

    def test_native_poll_has_a_total_record_safety_bound(self):
        transport = FakeCdcTransport(
            [[{"op": "METADATA"}, {"op": "METADATA"}, {"op": "METADATA"}]]
        )
        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {"cdc.max.poll.records": "2"}
        bridge.transport = transport
        capture = {
            "identity": "demo:app.orders",
            "logical_identity": "demo.app.orders",
            "columns": ["id"],
            "descriptors": [{"name": "id", "type_name": "INTEGER"}],
        }

        with mock.patch.object(informix_module, "CdcFrameParser", RecordParser), mock.patch.object(
            informix_module, "decode_frame", side_effect=lambda frame, labels: dict(frame)
        ), self.assertRaisesRegex(InformixError, "cdc.max.poll.records=2"):
            bridge.read_changes([capture], 90, 1, 2)

    def test_native_poll_has_a_total_decoded_byte_safety_bound(self):
        transport = FakeCdcTransport(
            [[{"op": "METADATA"}, {"op": "METADATA"}, {"op": "METADATA"}]]
        )
        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {"cdc.max.poll.bytes": "2"}
        bridge.transport = transport
        capture = {
            "identity": "demo:app.orders",
            "logical_identity": "demo.app.orders",
            "columns": ["id"],
            "descriptors": [{"name": "id", "type_name": "INTEGER"}],
        }

        with mock.patch.object(informix_module, "CdcFrameParser", RecordParser), mock.patch.object(
            informix_module, "decode_frame", side_effect=lambda frame, labels: dict(frame)
        ), self.assertRaisesRegex(InformixError, "cdc.max.poll.bytes=2"):
            bridge.read_changes([capture], 90, 1, 2)

    def test_locale_defaults(self):
        config = _bridge_config(
            {
                "hostname": "host",
                "database": "db",
                "user": "user",
                "password": "secret",
                "server": "srv",
            }
        )
        self.assertEqual(config["db_locale"], "en_US.819")
        self.assertEqual(config["client_locale"], "en_US.utf8")

    def test_partial_preparation_reports_tables_left_enabled(self):
        class PartialTransport:
            def execute(self, sql, parameters=()):
                if "cdc_set_fullrowlogging" in sql:
                    return [{"status": 0 if parameters[0].endswith(".orders") else -1}]
                raise AssertionError(sql)

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.transport = PartialTransport()
        bridge.options = {}

        with self.assertRaisesRegex(
            InformixError, r"partially applied.*demo:app.orders"
        ):
            bridge.prepare_initial_capture(
                ["demo:app.orders", "demo:app.customers"]
            )

    def test_initial_lsn_validation_activates_without_reading_lodata(self):
        class ActivationTransport:
            def __init__(self):
                self.sql = []

            def execute(self, sql, parameters=()):
                self.sql.append(sql)
                if "sysenv" in sql:
                    return [{"env_value": "demo_server"}]
                if "cdc_opensess" in sql:
                    return [{"session_id": 7}]
                return [{"status": 0}]

            def read_lodata(self, descriptor, requested):
                raise AssertionError("activation-only validation must not read LODATA")

        transport = ActivationTransport()
        bridge = object.__new__(PurePythonInformixBridge)
        bridge.transport = transport
        bridge.options = {}
        capture = {
            "identity": "demo:app.orders",
            "columns": ["id"],
        }

        bridge.validate_initial_lsn(capture, 80)

        self.assertTrue(any("cdc_activatesess" in sql for sql in transport.sql))
        self.assertTrue(
            any("cdc_opensess(?, 0, 1, 1, 1, 1)" in sql for sql in transport.sql)
        )

    def test_initial_lsn_validation_attaches_cleanup_failure_to_primary_error(self):
        class FailedValidationTransport:
            def execute(self, sql, parameters=()):
                if "sysenv" in sql:
                    return [{"env_value": "demo_server"}]
                if "cdc_opensess" in sql:
                    return [{"session_id": 7}]
                if "cdc_startcapture" in sql:
                    return [{"status": 0}]
                if "cdc_activatesess" in sql:
                    raise ValueError("primary")
                return [{"status": -1}]

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.transport = FailedValidationTransport()
        bridge.options = {}

        with self.assertRaisesRegex(ValueError, "primary") as caught:
            bridge.validate_initial_lsn(
                {"identity": "demo:app.orders", "columns": ["id"]}, 80
            )

        self.assertIn("validation cleanup also failed", " ".join(caught.exception.__notes__))

    def test_batch_size_defaults(self):
        self.assertEqual(_DEFAULT_SNAPSHOT_PAGE_SIZE, 10000)
        self.assertEqual(_DEFAULT_MAX_RECORDS_PER_BATCH, 10000)

    def test_all_bounded_options_are_validated_at_construction(self):
        invalid = {
            "snapshot.max.rows": "0",
            "cdc.timeout": "0",
            "cdc.max.frame.bytes": "15",
            "cdc.max.transaction.records": "0",
            "cdc.max.poll.records": "0",
            "cdc.max.poll.bytes": "-1",
            "cdc.read.bytes": "32768",
            "authentication.pam.max.rounds": "0",
            "authentication.login.timeout": "0",
            "redirect.max": "-1",
        }
        for name, value in invalid.items():
            with self.subTest(name=name), self.assertRaises(ValueError):
                InformixLakeflowConnect(
                    {
                        "database": "demo",
                        "cdc.shared.state.location": self._shared_state.name,
                        name: value,
                    }
                )
        for value in ("nan", "inf", "-inf"):
            with self.subTest(login_timeout=value), self.assertRaises(ValueError):
                InformixLakeflowConnect(
                    {
                        "database": "demo",
                        "cdc.shared.state.location": self._shared_state.name,
                        "authentication.login.timeout": value,
                    }
                )
        InformixLakeflowConnect(
            {
                "database": "demo",
                "cdc.shared.state.location": self._shared_state.name,
                "snapshot.max.bytes": "0",
                "metadata.max.bytes": "0",
                "cdc.max.poll.bytes": "0",
            }
        )

    def test_per_table_numeric_options_use_the_same_bounds(self):
        connector = self.connector()
        cases = (
            ("snapshot.page.size", "0", {}),
            ("cdc.timeout", "0", _stream_offset()),
            ("cdc.max.records", "257", _stream_offset()),
            ("max.records.per.batch", "0", _stream_offset()),
        )
        for name, value, offset in cases:
            with self.subTest(name=name), self.assertRaises(ValueError):
                connector.read_table("app.orders", offset, {name: value})

    def test_native_cdc_error_preserves_code_flags_and_payload(self):
        with self.assertRaisesRegex(InformixError, r"-12.*flags 5.*native detail"):
            TransactionBuffer().feed(
                {"op": "ERROR", "error": -12, "flags": 5, "payload": b"native detail"}
            )

    def test_native_cdc_rejects_negative_lsn_before_projection(self):
        with self.assertRaisesRegex(InformixError, "invalid LSN"):
            TransactionBuffer().feed({"op": "BEGIN", "tx_id": 1, "lsn": -1})

    def test_transaction_buffer_rejects_duplicate_begin(self):
        buffer = TransactionBuffer()
        buffer.feed({"op": "BEGIN", "tx_id": 1, "lsn": 10})
        with self.assertRaisesRegex(InformixError, "Duplicate CDC BEGIN"):
            buffer.feed({"op": "BEGIN", "tx_id": 1, "lsn": 11})

    def test_native_signed_transaction_id_is_normalized_to_uint32(self):
        transactions = _committed_transactions(
            [
                {"op": "BEGIN", "tx_id": -1, "lsn": 10},
                {"op": "INSERT", "tx_id": -1, "lsn": 11, "row": {"id": 1}},
                {"op": "COMMIT", "tx_id": -1, "lsn": 12},
                {"op": "TIMEOUT", "lsn": 12},
            ]
        )
        self.assertEqual(transactions[0].tx_id, (1 << 32) - 1)

    def test_timeout_unavailable_lsn_does_not_participate_in_ordering(self):
        transactions = _committed_transactions(
            [
                {"op": "TIMEOUT", "lsn": (1 << 64) - 1},
                {"op": "BEGIN", "tx_id": 1, "lsn": 10},
                {"op": "INSERT", "tx_id": 1, "lsn": 11, "row": {"id": 1}},
                {"op": "COMMIT", "tx_id": 1, "lsn": 12},
            ]
        )
        self.assertEqual(transactions[0].commit_lsn, 12)

    def test_transaction_buffer_rejects_lsn_regression(self):
        buffer = TransactionBuffer()
        buffer.feed({"op": "BEGIN", "tx_id": 1, "lsn": 10})
        buffer.feed({"op": "INSERT", "tx_id": 1, "lsn": 12, "row": {"id": 1}})
        with self.assertRaisesRegex(InformixError, "LSN regressed"):
            buffer.feed({"op": "COMMIT", "tx_id": 1, "lsn": 11})

    def test_cdc_stream_rejects_global_lsn_regression_between_transactions(self):
        records = [
            {"op": "BEGIN", "tx_id": 1, "lsn": 10},
            {"op": "BEGIN", "tx_id": 2, "lsn": 20},
            {"op": "COMMIT", "tx_id": 2, "lsn": 30},
            {"op": "COMMIT", "tx_id": 1, "lsn": 25},
        ]
        with self.assertRaisesRegex(InformixError, "regressed globally"):
            _committed_transactions(records)

    def test_discard_rollback_cutoff_may_precede_latest_data_lsn(self):
        transactions = _committed_transactions(
            [
                {"op": "BEGIN", "tx_id": 1, "lsn": 100},
                {"op": "INSERT", "tx_id": 1, "lsn": 105, "row": {"id": 1}},
                {"op": "INSERT", "tx_id": 1, "lsn": 120, "row": {"id": 2}},
                {"op": "DISCARD", "tx_id": 1, "lsn": 110},
                {"op": "COMMIT", "tx_id": 1, "lsn": 130},
            ]
        )

        self.assertEqual([record["row"]["id"] for record in transactions[0].records], [1])

    def test_table_metadata_rejects_unsafe_and_duplicate_columns(self):
        raw = _table()
        for columns, message in (
            ([*raw["columns"], {"name": "bad-name", "type_name": "INTEGER"}], "Unsafe"),
            ([raw["columns"][0], raw["columns"][0]], "Duplicate column"),
        ):
            with self.subTest(message=message), self.assertRaisesRegex(InformixError, message):
                Table.parse({**raw, "columns": columns}, "demo")

    def test_table_metadata_rejects_casefold_and_reserved_column_collisions(self):
        raw = _table()
        cases = (
            [
                {"name": "Value", "type_name": "INTEGER"},
                {"name": "value", "type_name": "INTEGER"},
            ],
            [
                *raw["columns"],
                {"name": "_INFORMIX_CHANGE_LSN", "type_name": "INTEGER"},
            ],
        )
        for columns in cases:
            with self.subTest(columns=columns), self.assertRaises(InformixError):
                Table.parse({**raw, "columns": columns, "primary_keys": []}, "demo")

    def test_table_metadata_rejects_duplicate_primary_key_columns(self):
        raw = _table()
        with self.assertRaisesRegex(InformixError, "Duplicate primary-key"):
            Table.parse({**raw, "primary_keys": ["id", "id"]}, "demo")

    def test_metadata_refresh_describes_only_the_requested_table(self):
        bridge = FakeBridge()
        counts = {"list": 0, "get": 0}
        original_list, original_get = bridge.list_tables, bridge.get_table

        def list_tables():
            counts["list"] += 1
            return original_list()

        def get_table(identity):
            counts["get"] += 1
            return original_get(identity)

        bridge.list_tables, bridge.get_table = list_tables, get_table
        connector = self.connector(bridge)
        connector.read_table_metadata("app.orders", {})
        connector.read_table_metadata("app.audit", {})

        self.assertEqual(counts, {"list": 1, "get": 2})

    def test_snapshot_bridge_passes_incremental_result_byte_bound(self):
        class SnapshotTransport:
            def __init__(self):
                self.maximum = None

            def execute(self, sql, parameters=(), max_result_bytes=None):
                self.maximum = max_result_bytes
                return [{"id": 1}]

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {"snapshot.max.bytes": "1234"}
        bridge.transport = SnapshotTransport()

        rows = bridge.snapshot_page("demo.app.orders", ["id"], ["id"], None, 2)

        self.assertEqual(rows, [{"id": 1}])
        self.assertEqual(bridge.transport.maximum, 1234)

    def test_production_bridge_reads_consistent_snapshot_in_one_transaction(self):
        class TransactionalTransport:
            def __init__(self):
                self.sql = []

            def execute(self, sql, parameters=(), max_result_bytes=None):
                self.sql.append(sql)
                if "sysmaster:sysdatabases" in sql:
                    return [{"is_ansi": 0}]
                if "sysmaster:syslogs" in sql:
                    return [{"uniqid": 2, "used": 3}]
                if sql.startswith("SELECT FIRST"):
                    return [{"id": 1}]
                return []

            def execute_command(self, sql):
                self.sql.append(f"COMMAND:{sql}")

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {"database": "demo"}
        bridge.config = {"database": "demo"}
        bridge.transport = TransactionalTransport()

        lsn, rows = bridge.consistent_snapshot(
            "demo.app.orders", ["id"], ["id"], 10, 100, 1 << 20
        )

        self.assertEqual(lsn, (2 << 32) + (3 << 12))
        self.assertEqual(rows, [{"id": 1}])
        self.assertEqual(
            [sql for sql in bridge.transport.sql if sql.startswith("COMMAND:")][:2],
            ["COMMAND:SET ISOLATION TO REPEATABLE READ", "COMMAND:BEGIN WORK"],
        )
        self.assertEqual(bridge.transport.sql[-1], "COMMAND:COMMIT WORK")

    def test_ansi_snapshot_uses_implicit_transaction(self):
        class AnsiTransport:
            def __init__(self):
                self.commands = []

            def execute(self, sql, parameters=(), max_result_bytes=None):
                if "sysmaster:sysdatabases" in sql:
                    return [{"is_ansi": 1}]
                if "sysmaster:syslogs" in sql:
                    return [{"uniqid": 2, "used": 3}]
                if sql.startswith("SELECT FIRST"):
                    return []
                return []

            def execute_command(self, sql):
                self.commands.append(sql)

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {"database": "demo"}
        bridge.config = {"database": "demo"}
        bridge.transport = AnsiTransport()

        bridge.consistent_snapshot("demo.app.orders", ["id"], ["id"], 10, 100, 0)

        self.assertEqual(
            bridge.transport.commands,
            ["COMMIT WORK", "SET ISOLATION TO REPEATABLE READ", "COMMIT WORK"],
        )

    def test_ansi_snapshot_rolls_back_after_query_failure(self):
        class FailingAnsiTransport:
            def __init__(self):
                self.commands = []

            def execute(self, sql, parameters=(), max_result_bytes=None):
                if "sysmaster:sysdatabases" in sql:
                    self.ansi_parameters = parameters
                    return [{"is_ansi": 1}]
                if "sysmaster:syslogs" in sql:
                    return [{"uniqid": 2, "used": 3}]
                if sql.startswith("SELECT FIRST"):
                    raise RuntimeError("snapshot failed")
                return []

            def execute_command(self, sql):
                self.commands.append(sql)

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {"database": "demo"}
        bridge.config = {"database": "demo"}
        bridge.transport = FailingAnsiTransport()

        with self.assertRaisesRegex(RuntimeError, "snapshot failed"):
            bridge.consistent_snapshot("demo.app.orders", ["id"], ["id"], 10, 100, 0)

        self.assertEqual(bridge.transport.ansi_parameters, ("demo",))
        self.assertEqual(
            bridge.transport.commands,
            ["COMMIT WORK", "SET ISOLATION TO REPEATABLE READ", "ROLLBACK WORK"],
        )

    def test_zero_snapshot_byte_bound_skips_accounting(self):
        class TransactionalTransport:
            def execute(self, sql, parameters=(), max_result_bytes=None):
                self.maximum = max_result_bytes
                if "sysmaster:sysdatabases" in sql:
                    return [{"is_ansi": 0}]
                if "sysmaster:syslogs" in sql:
                    return [{"uniqid": 2, "used": 3}]
                if sql.startswith("SELECT FIRST"):
                    return [{"id": 1}]
                return []

            def execute_command(self, sql):
                return None

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {"snapshot.max.bytes": "0", "database": "demo"}
        bridge.config = {"database": "demo"}
        bridge.transport = TransactionalTransport()

        with mock.patch.object(
            informix_module, "_deep_size", side_effect=AssertionError("must not account")
        ):
            _, rows = bridge.consistent_snapshot(
                "demo.app.orders", ["id"], ["id"], 10, 100, 0
            )

        self.assertEqual(rows, [{"id": 1}])
        self.assertIsNone(bridge.transport.maximum)

    def test_metadata_queries_use_decoded_result_byte_bound(self):
        class MetadataTransport:
            def __init__(self):
                self.maximum = None

            def execute(self, sql, parameters=(), max_result_bytes=None):
                self.maximum = max_result_bytes
                return []

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {"metadata.max.bytes": "4321"}
        bridge.transport = MetadataTransport()

        self.assertEqual(bridge.list_tables(), [])
        self.assertEqual(bridge.transport.maximum, 4321)

    def test_complete_metadata_discovery_uses_total_byte_bound(self):
        class MetadataTransport:
            def execute(self, sql, parameters=(), max_result_bytes=None):
                return [{"owner": "app", "tabname": "orders"}]

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {"metadata.max.bytes": "1"}
        bridge.transport = MetadataTransport()
        bridge._describe_table = lambda owner, name: {"owner": owner, "name": name}

        with self.assertRaisesRegex(InformixError, "metadata.max.bytes=1"):
            bridge.list_tables()

    def test_zero_metadata_byte_bound_skips_accounting(self):
        class MetadataTransport:
            def execute(self, sql, parameters=(), max_result_bytes=None):
                self.maximum = max_result_bytes
                return []

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {"metadata.max.bytes": "0"}
        bridge.transport = MetadataTransport()

        with mock.patch.object(
            informix_module, "_deep_size", side_effect=AssertionError("must not account")
        ):
            self.assertEqual(bridge.list_tables(), [])
        self.assertIsNone(bridge.transport.maximum)

    def test_primary_key_index_join_is_constrained_by_table_id(self):
        class CatalogTransport:
            def __init__(self):
                self.sql = []

            def execute(self, sql, parameters=(), max_result_bytes=None):
                self.sql.append(sql)
                if "syscolumns" in sql:
                    return [
                        {
                            "colname": "id",
                            "coltype": 2,
                            "collength": 4,
                            "colno": 1,
                            "tabid": 42,
                        }
                    ]
                return []

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.config = {"database": "demo"}
        bridge.transport = CatalogTransport()

        bridge._describe_table("app", "orders")

        self.assertIn("x.tabid=i.tabid", bridge.transport.sql[1])

    def test_live_catalog_requires_positive_table_incarnation(self):
        class CatalogTransport:
            def execute(self, sql, parameters=(), max_result_bytes=None):
                if "syscolumns" in sql:
                    return [
                        {"colname": "id", "coltype": 2, "collength": 4, "colno": 1}
                    ]
                return []

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.config = {"database": "demo"}
        bridge.transport = CatalogTransport()

        with self.assertRaisesRegex(InformixError, "missing tabid"):
            bridge._describe_table("app", "orders")

    def test_invalid_decimal_metadata_fails_before_ingestion(self):
        bridge = FakeBridge()
        bridge.tables = [
            {
                **_table(),
                "columns": [
                    {"name": "id", "type_name": "INTEGER", "nullable": False},
                    {
                        "name": "amount",
                        "type_name": "DECIMAL",
                        "precision": 39,
                        "scale": 2,
                    },
                ],
            }
        ]
        connector = self.connector(bridge)

        with self.assertRaisesRegex(InformixError, "invalid DECIMAL metadata"):
            connector.read_table_metadata("app.orders", {})

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
        connector = self.connector(
            bridge,
            **{"snapshot.page.size": "1"},
        )
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

    def test_consistent_snapshot_publishes_fresh_resume_lsn_to_both_readers(self):
        bridge = FakeBridge()

        def consistent_snapshot(*args, **kwargs):
            bridge.now = 150
            return 150, list(bridge.rows)

        bridge.consistent_snapshot = consistent_snapshot
        changes, upsert_offset = self.connector(bridge).read_table("app.orders", {}, {})
        delete_connector = self.connector(bridge)
        deletes, delete_offset = delete_connector.read_table_deletes(
            "app.orders", {}, {}
        )

        self.assertEqual([row["id"] for row in changes], [1, 2])
        self.assertEqual(upsert_offset["commit_lsn"], "150")
        self.assertEqual(list(deletes), [])
        self.assertEqual(delete_offset["commit_lsn"], "150")

    def test_concurrent_full_refreshes_use_pipeline_scoped_snapshot_boundaries(self):
        def snapshot_connector(pipeline_id, snapshot_lsn):
            bridge = FakeBridge()

            def consistent_snapshot(*args, **kwargs):
                bridge.now = snapshot_lsn
                return snapshot_lsn, list(bridge.rows)

            bridge.consistent_snapshot = consistent_snapshot
            connector = self.connector(bridge, registration_scope=pipeline_id)
            _, offset = connector.read_table("app.orders", {}, {})
            return bridge, offset

        bridge_a, offset_a = snapshot_connector("pipeline-a", 120)
        bridge_b, offset_b = snapshot_connector("pipeline-b", 150)
        _, delete_a = self.connector(
            bridge_a, registration_scope="pipeline-a"
        ).read_table_deletes("app.orders", {}, {})
        _, delete_b = self.connector(
            bridge_b, registration_scope="pipeline-b"
        ).read_table_deletes("app.orders", {}, {})

        self.assertEqual(offset_a["commit_lsn"], delete_a["commit_lsn"])
        self.assertEqual(offset_b["commit_lsn"], delete_b["commit_lsn"])
        self.assertNotEqual(delete_a["commit_lsn"], delete_b["commit_lsn"])

    def test_delete_reader_uses_boundary_published_by_upsert_reader(self):
        snapshot_bridge = FakeBridge()
        snapshot_connector = self.connector(snapshot_bridge)
        list(snapshot_connector.read_table("app.orders", {}, {})[0])

        delete_bridge = FakeBridge()
        delete_bridge.now = 120
        delete_connector = self.connector(delete_bridge)
        _, offset = delete_connector.read_table_deletes("app.orders", {}, {})

        self.assertEqual(offset["commit_lsn"], "90")
        self.assertEqual(snapshot_bridge.prepared_identities, ["demo:app.orders"])
        self.assertEqual(delete_bridge.prepared_identities, [])

    def test_delete_reader_waits_for_upsert_reader_to_publish_boundary(self):
        delete_connector = self.connector(FakeBridge())
        result = []

        def read_deletes():
            result.append(delete_connector.read_table_deletes("app.orders", {}, {})[1])

        thread = threading.Thread(target=read_deletes)
        thread.start()
        snapshot_bridge = FakeBridge()
        list(self.connector(snapshot_bridge).read_table("app.orders", {}, {})[0])
        thread.join(2)

        self.assertFalse(thread.is_alive())
        self.assertEqual(result[0]["commit_lsn"], "90")

    def test_upsert_reader_rotates_expired_shared_boundary(self):
        list(self.connector(FakeBridge()).read_table("app.orders", {}, {})[0])
        replacement = FakeBridge()
        replacement.minimum = 100
        replacement.now = 120

        list(
            self.connector(replacement, registration_scope="replacement").read_table(
                "app.orders", {}, {}
            )[0]
        )
        _, delete_offset = self.connector(
            replacement, registration_scope="replacement"
        ).read_table_deletes(
            "app.orders", {}, {}
        )

        self.assertEqual(replacement.prepared_identities, ["demo:app.orders"])
        self.assertEqual(delete_offset["commit_lsn"], "120")

    def test_connector_context_manager_closes_transport(self):
        bridge = FakeBridge()
        bridge.transport = mock.Mock()
        connector = self.connector(bridge)

        with connector:
            pass

        bridge.transport.close.assert_called_once_with()
        self.assertIsNone(connector._bridge_instance)

    def test_context_manager_preserves_primary_error_when_close_fails(self):
        bridge = FakeBridge()
        bridge.transport = mock.Mock()
        bridge.transport.close.side_effect = RuntimeError("close failed")
        connector = self.connector(bridge)

        with self.assertRaisesRegex(ValueError, "primary") as caught:
            with connector:
                raise ValueError("primary")

        if hasattr(caught.exception, "__notes__"):
            self.assertIn("close failed", " ".join(caught.exception.__notes__))

    def test_stream_offset_rejects_schema_changes_and_legacy_offsets(self):
        bridge = FakeBridge()
        connector = self.connector(bridge)
        legacy = _stream_offset()
        del legacy["schema_fingerprint"]
        with self.assertRaisesRegex(InformixError, "predates schema-safe offsets"):
            connector.read_table("app.orders", legacy, {})

        changed = _stream_offset()
        changed["schema_fingerprint"] = "0" * 64
        with self.assertRaisesRegex(InformixError, "Schema history.*missing"):
            connector.read_table("app.orders", changed, {})

    def test_restart_transitions_appended_nullable_column_without_snapshot(self):
        bridge = FakeBridge()
        connector = self.connector(bridge)
        _, checkpoint = connector.read_table("app.orders", {}, {})
        previous_fingerprint = checkpoint["schema_fingerprint"]
        bridge.tables[0]["columns"].append(
            {"name": "added", "type_name": "INTEGER", "nullable": True}
        )
        bridge.now = 120
        bridge.changes = [{"op": "TIMEOUT", "lsn": 120}]

        restarted = self.connector(bridge)
        rows, transitioned = restarted.read_table("app.orders", checkpoint, {})

        self.assertEqual(list(rows), [])
        self.assertEqual(transitioned["commit_lsn"], "120")
        self.assertNotEqual(transitioned["schema_fingerprint"], previous_fingerprint)

        bridge.changes = [
            {"op": "BEGIN", "tx_id": 7, "lsn": 121},
            {
                "op": "INSERT",
                "tx_id": 7,
                "lsn": 122,
                "row": {"id": 3, "value": "c", "added": 42},
            },
            {"op": "COMMIT", "tx_id": 7, "lsn": 123},
        ]
        rows, end = restarted.read_table("app.orders", transitioned, {})

        self.assertEqual(list(rows)[0]["added"], 42)
        self.assertEqual(end["commit_lsn"], "123")

    def test_pre_transition_batch_retains_previous_schema_fingerprint(self):
        bridge = FakeBridge()
        _, checkpoint = self.connector(bridge).read_table("app.orders", {}, {})
        previous_fingerprint = checkpoint["schema_fingerprint"]
        bridge.tables[0]["columns"].append(
            {"name": "added", "type_name": "INTEGER", "nullable": True}
        )
        bridge.now = 120
        bridge.changes = [
            {"op": "BEGIN", "tx_id": 7, "lsn": 100},
            {"op": "INSERT", "tx_id": 7, "lsn": 101, "row": {"id": 3, "value": "c"}},
            {"op": "COMMIT", "tx_id": 7, "lsn": 102},
        ]

        _, end = self.connector(bridge).read_table("app.orders", checkpoint, {})

        self.assertEqual(end["commit_lsn"], "102")
        self.assertEqual(end["schema_fingerprint"], previous_fingerprint)

    def test_post_transition_transaction_advances_boundary_without_stalling(self):
        bridge = FakeBridge()
        _, checkpoint = self.connector(bridge).read_table("app.orders", {}, {})
        bridge.tables[0]["columns"].append(
            {"name": "added", "type_name": "INTEGER", "nullable": True}
        )
        bridge.now = 120
        bridge.changes = [
            {"op": "BEGIN", "tx_id": 8, "lsn": 121},
            {
                "op": "INSERT",
                "tx_id": 8,
                "lsn": 122,
                "row": {"id": 4, "value": "d", "added": 9},
            },
            {"op": "COMMIT", "tx_id": 8, "lsn": 123},
        ]
        connector = self.connector(bridge)

        rows, transitioned = connector.read_table("app.orders", checkpoint, {})
        self.assertEqual(list(rows), [])
        self.assertEqual(transitioned["commit_lsn"], "120")

        rows, end = connector.read_table("app.orders", transitioned, {})
        self.assertEqual(list(rows)[0]["added"], 9)
        self.assertEqual(end["commit_lsn"], "123")

    def test_transaction_spanning_schema_transition_fails_closed(self):
        bridge = FakeBridge()
        _, checkpoint = self.connector(bridge).read_table("app.orders", {}, {})
        bridge.tables[0]["columns"].append(
            {"name": "added", "type_name": "INTEGER", "nullable": True}
        )
        bridge.now = 120
        bridge.changes = [
            {"op": "BEGIN", "tx_id": 9, "lsn": 119},
            {"op": "INSERT", "tx_id": 9, "lsn": 121, "row": {"id": 5, "value": "e"}},
            {"op": "COMMIT", "tx_id": 9, "lsn": 123},
        ]

        with self.assertRaisesRegex(InformixError, "spans schema transition"):
            self.connector(bridge).read_table("app.orders", checkpoint, {})

    def test_available_now_does_not_advance_past_frozen_transition_boundary(self):
        bridge = FakeBridge()
        _, checkpoint = self.connector(bridge).read_table("app.orders", {}, {})
        bridge.tables[0]["columns"].append(
            {"name": "added", "type_name": "INTEGER", "nullable": True}
        )
        bridge.now = 120
        bridge.changes = [{"op": "TIMEOUT", "lsn": 120}]
        connector = self.connector(bridge)
        connector._trigger_available_now = True
        connector._trigger_boundaries["demo.app.orders"] = (110, "a" * 32)

        _, end = connector.read_table("app.orders", checkpoint, {})

        self.assertEqual(end["commit_lsn"], checkpoint["commit_lsn"])


    def test_shared_state_probe_thread_start_failure_is_bounded(self):
        location = self._shared_state.name
        informix_module._VALIDATED_STATE_LOCATIONS.discard(location)
        with mock.patch.object(
            informix_module.threading.Thread,
            "start",
            side_effect=RuntimeError("thread unavailable"),
        ):
            with self.assertRaisesRegex(InformixError, "exclusive directory creation"):
                _validate_shared_state_filesystem(location)

    def test_stale_populated_probe_artifacts_are_recovered(self):
        root = os.path.join(self._shared_state.name, ".informix-probe-crashed")
        os.mkdir(root)
        for name in ("exclusive", "renamed", "occupied"):
            child = os.path.join(root, name)
            os.mkdir(child)
            if name != "exclusive":
                with open(
                    os.path.join(child, "record.json"), "w", encoding="utf-8"
                ) as handle:
                    handle.write(name)
        os.utime(root, (1, 1))

        informix_module._cleanup_probe_artifacts(self._shared_state.name)

        self.assertFalse(os.path.exists(root))

    def test_unsupported_volume_directory_open_does_not_fail_publication(self):
        connector = self.connector(FakeBridge())
        table = Table.parse(_table(), "demo")
        namespace = connector._immutable_namespace(table, "probe", "directory-open")
        real_open = informix_module.os.open

        def volume_open(path, flags, *args, **kwargs):
            if informix_module.os.path.isdir(path) and flags == informix_module.os.O_RDONLY:
                raise OSError(errno.EACCES, "directory handles unsupported")
            return real_open(path, flags, *args, **kwargs)

        with mock.patch.object(informix_module.os, "open", side_effect=volume_open):
            connector._publish_immutable_head(namespace, {"value": 90})

        self.assertEqual(connector._read_immutable_head(namespace)["value"], 90)


    def test_lagging_checkpoint_advances_one_schema_version_at_a_time(self):
        bridge = FakeBridge()
        _, checkpoint_a = self.connector(bridge).read_table("app.orders", {}, {})
        bridge.tables[0]["columns"].append(
            {"name": "added_b", "type_name": "INTEGER", "nullable": True}
        )
        bridge.now = 120
        bridge.changes = [{"op": "TIMEOUT", "lsn": 120}]
        _, checkpoint_b = self.connector(bridge).read_table(
            "app.orders", checkpoint_a, {}
        )
        bridge.tables[0]["columns"].append(
            {"name": "added_c", "type_name": "INTEGER", "nullable": True}
        )
        bridge.now = 140
        bridge.changes = [{"op": "TIMEOUT", "lsn": 140}]
        _, checkpoint_c = self.connector(bridge).read_table(
            "app.orders", checkpoint_b, {}
        )

        _, lagging_b = self.connector(bridge).read_table("app.orders", checkpoint_a, {})
        _, lagging_c = self.connector(bridge).read_table("app.orders", lagging_b, {})

        self.assertEqual(lagging_b["schema_fingerprint"], checkpoint_b["schema_fingerprint"])
        self.assertEqual(lagging_b["commit_lsn"], "120")
        self.assertEqual(lagging_c["schema_fingerprint"], checkpoint_c["schema_fingerprint"])
        self.assertEqual(lagging_c["commit_lsn"], "140")

    def test_future_immutable_schema_transition_fails_closed(self):
        bridge = FakeBridge()
        connector = self.connector(bridge)
        _, checkpoint = connector.read_table("app.orders", {}, {})
        bridge.tables[0]["columns"].append(
            {"name": "added", "type_name": "INTEGER", "nullable": True}
        )
        table = Table.parse(bridge.tables[0], "demo")
        future = _schema_state(
            table,
            999,
            predecessor=checkpoint["schema_id"],
        )
        connector._publish_immutable_head(
            connector._immutable_namespace(
                table, "schemas", checkpoint["schema_id"]
            ),
            {
                "created_at": informix_module.time.time(),
                "schema": future,
            },
            record_type="schema-transition",
        )

        with self.assertRaisesRegex(InformixError, "outside retained/current range"):
            connector.read_table("app.orders", checkpoint, {})
        self.assertIsNone(
            connector._read_immutable_head(
                connector._immutable_namespace(
                    table, "schema-nodes", str(future["id"])
                )
            )
        )

    def test_conflicting_additive_schema_transition_branch_fails_closed(self):
        bridge = FakeBridge()
        connector = self.connector(bridge)
        _, checkpoint = connector.read_table("app.orders", {}, {})
        conflicting_raw = json.loads(json.dumps(bridge.tables[0]))
        conflicting_raw["columns"].append(
            {"name": "branch_a", "type_name": "INTEGER", "nullable": True}
        )
        bridge.tables[0]["columns"].append(
            {"name": "branch_b", "type_name": "INTEGER", "nullable": True}
        )
        bridge.now = 120
        current = Table.parse(bridge.tables[0], "demo")
        conflicting = Table.parse(conflicting_raw, "demo")
        connector._publish_immutable_head(
            connector._immutable_namespace(
                current, "schemas", checkpoint["schema_id"]
            ),
            {
                "created_at": informix_module.time.time(),
                "schema": _schema_state(
                    conflicting, 120, predecessor=checkpoint["schema_id"]
                ),
            },
            record_type="schema-transition",
        )

        with self.assertRaisesRegex(InformixError, "not an additive column change"):
            connector.read_table("app.orders", checkpoint, {})

    def test_schema_transition_rejects_predecessor_after_checkpoint(self):
        bridge = FakeBridge()
        connector = self.connector(bridge)
        table = Table.parse(_table(), "demo")
        checkpoint = _stream_offset(100)
        connector._publish_immutable_head(
            connector._immutable_namespace(
                table, "schema-nodes", checkpoint["schema_id"]
            ),
            {
                "created_at": 1.0,
                "schema": _schema_state(
                    table, 110, schema_id=checkpoint["schema_id"]
                ),
                "scope": checkpoint["pipeline_scope"],
            },
            record_type="schema-node",
        )
        bridge.tables[0]["columns"].append(
            {"name": "added", "type_name": "INTEGER", "nullable": True}
        )
        bridge.now = 120

        with self.assertRaisesRegex(InformixError, "follows checkpoint"):
            connector.read_table("app.orders", checkpoint, {})

    def test_incompatible_full_refresh_creates_independent_schema_generation(self):
        bridge = FakeBridge()
        _, old_checkpoint = self.connector(bridge).read_table("app.orders", {}, {})
        bridge.tables[0]["columns"][1]["type_name"] = "INTEGER"
        bridge.now = 150

        refreshed = self.connector(bridge, registration_scope="new-layout")
        _, new_checkpoint = refreshed.read_table("app.orders", {}, {})
        bridge.changes = [{"op": "TIMEOUT", "lsn": 150}]
        refreshed.read_table("app.orders", new_checkpoint, {})
        refreshed.read_table("app.orders", new_checkpoint, {})

        self.assertEqual(new_checkpoint["commit_lsn"], "150")
        self.assertEqual(bridge.prepared_identities, ["demo:app.orders"])
        with self.assertRaisesRegex(InformixError, "not an additive.*full refresh"):
            self.connector(bridge).read_table("app.orders", old_checkpoint, {})

    def test_full_refresh_of_evolved_layout_uses_its_transition_lsn(self):
        bridge = FakeBridge()
        _, checkpoint_a = self.connector(bridge).read_table("app.orders", {}, {})
        bridge.tables[0]["columns"].append(
            {"name": "added", "type_name": "INTEGER", "nullable": True}
        )
        bridge.now = 120
        bridge.changes = [{"op": "TIMEOUT", "lsn": 120}]
        _, checkpoint_b = self.connector(bridge).read_table(
            "app.orders", checkpoint_a, {}
        )

        _, refreshed_b = self.connector(
            bridge, registration_scope="evolved-refresh"
        ).read_table("app.orders", {}, {})

        self.assertEqual(checkpoint_b["commit_lsn"], "120")
        self.assertEqual(refreshed_b["commit_lsn"], "120")
        self.assertEqual(
            refreshed_b["schema_fingerprint"], checkpoint_b["schema_fingerprint"]
        )
        self.assertNotEqual(refreshed_b["schema_id"], checkpoint_b["schema_id"])

    def test_repeated_layout_creates_a_distinct_full_refresh_generation(self):
        bridge = FakeBridge()
        original = json.loads(json.dumps(bridge.tables[0]))
        _, checkpoint_a1 = self.connector(bridge).read_table("app.orders", {}, {})
        bridge.tables[0]["columns"][1]["type_name"] = "INTEGER"
        bridge.now = 150
        _, checkpoint_d = self.connector(
            bridge, registration_scope="layout-d"
        ).read_table("app.orders", {}, {})
        bridge.tables[0] = original
        bridge.now = 200

        _, checkpoint_a2 = self.connector(
            bridge, registration_scope="layout-a2"
        ).read_table("app.orders", {}, {})

        self.assertEqual(
            checkpoint_a2["schema_fingerprint"], checkpoint_a1["schema_fingerprint"]
        )
        self.assertNotEqual(checkpoint_a2["schema_id"], checkpoint_a1["schema_id"])
        self.assertNotEqual(checkpoint_a2["schema_id"], checkpoint_d["schema_id"])
        self.assertEqual(checkpoint_a2["commit_lsn"], "200")

    def test_same_layout_new_table_incarnation_creates_new_generation(self):
        bridge = FakeBridge()
        bridge.tables[0]["incarnation"] = "101"
        _, first = self.connector(bridge).read_table("app.orders", {}, {})
        bridge.tables[0]["incarnation"] = "202"
        bridge.now = 200

        _, recreated = self.connector(
            bridge, registration_scope="incarnation-202"
        ).read_table("app.orders", {}, {})

        self.assertNotEqual(first["schema_fingerprint"], recreated["schema_fingerprint"])
        self.assertNotEqual(first["schema_id"], recreated["schema_id"])
        self.assertEqual(recreated["commit_lsn"], "200")




    def test_restart_rejects_non_additive_schema_change(self):
        bridge = FakeBridge()
        connector = self.connector(bridge)
        _, checkpoint = connector.read_table("app.orders", {}, {})
        bridge.tables[0]["columns"][1]["type_name"] = "INTEGER"

        with self.assertRaisesRegex(InformixError, "not an additive.*full refresh"):
            self.connector(bridge).read_table("app.orders", checkpoint, {})

    def test_stream_offset_rejects_previous_connector_format(self):
        connector = self.connector(FakeBridge())
        legacy = _stream_offset()
        del legacy["version"]

        with self.assertRaisesRegex(ValueError, "offset version.*full refresh"):
            connector.read_table("app.orders", legacy, {})

        version_three = _stream_offset()
        version_three["version"] = 3
        with self.assertRaisesRegex(ValueError, "offset version 3.*full refresh"):
            connector.read_table("app.orders", version_three, {})
        version_four = _stream_offset()
        version_four["version"] = 4
        with self.assertRaisesRegex(ValueError, "offset version 4.*full refresh"):
            connector.read_table("app.orders", version_four, {})

    def test_snapshot_continuation_rejects_schema_change_between_pages(self):
        bridge = FakeBridge()
        connector = self.connector(bridge, **{"snapshot.page.size": "1"})
        _, offset = connector.read_table("app.orders", {}, {})
        bridge.tables[0]["columns"].append(
            {"name": "added", "type_name": "INTEGER", "nullable": True}
        )

        with self.assertRaisesRegex(InformixError, "schema changed"):
            connector.read_table("app.orders", offset, {})

    def test_snapshot_and_delete_continuations_reject_legacy_fingerprint(self):
        bridge = FakeBridge()
        connector = self.connector(bridge, **{"snapshot.page.size": "1"})
        _, offset = connector.read_table("app.orders", {}, {})
        del offset["schema_fingerprint"]

        with self.assertRaisesRegex(InformixError, "predates schema-safe offsets"):
            connector.read_table("app.orders", offset, {})
        with self.assertRaisesRegex(InformixError, "predates schema-safe offsets"):
            connector.read_table_deletes("app.orders", offset, {})

    def test_snapshot_rechecks_schema_after_page_query(self):
        bridge = FakeBridge()
        original = bridge.snapshot_page

        def changing_snapshot(*args, **kwargs):
            rows = original(*args, **kwargs)
            bridge.tables[0]["columns"].append(
                {"name": "added", "type_name": "INTEGER", "nullable": True}
            )
            return rows

        bridge.snapshot_page = changing_snapshot
        connector = self.connector(bridge)

        with self.assertRaisesRegex(InformixError, "schema changed"):
            connector.read_table("app.orders", {}, {})

    def test_snapshot_revalidates_materializability_after_initial_refresh(self):
        bridge = FakeBridge()
        original = bridge.get_table

        def refreshed_with_unsupported_column(identity):
            raw = original(identity)
            return {
                **raw,
                "columns": [
                    *raw["columns"],
                    {"name": "payload", "type_name": "TEXT", "nullable": True},
                ],
            }

        bridge.get_table = refreshed_with_unsupported_column
        connector = self.connector(bridge)

        with self.assertRaisesRegex(InformixError, "cannot materialize.*payload"):
            connector.read_table("app.orders", {}, {})

    def test_snapshot_revalidates_cdc_capability_after_initial_refresh(self):
        bridge = FakeBridge()
        original = bridge.get_table

        def refreshed_without_primary_key(identity):
            return {**original(identity), "primary_keys": []}

        bridge.get_table = refreshed_without_primary_key
        connector = self.connector(bridge)

        with self.assertRaisesRegex(InformixError, "no longer CDC-capable"):
            connector.read_table("app.orders", {}, {})

    def test_stream_rechecks_schema_after_native_poll(self):
        bridge = FakeBridge()

        def changing_changes(*args, **kwargs):
            bridge.tables[0]["columns"].append(
                {"name": "added", "type_name": "INTEGER", "nullable": True}
            )
            return []

        bridge.read_changes = changing_changes
        connector = self.connector(bridge)

        with self.assertRaisesRegex(InformixError, "schema changed"):
            connector.read_table("app.orders", _stream_offset(), {})

    def test_snapshot_only_rechecks_schema_after_query(self):
        bridge = FakeBridge()
        bridge.tables = [_table(cdc=False)]
        original = bridge.snapshot_page

        def changing_snapshot(*args, **kwargs):
            rows = original(*args, **kwargs)
            bridge.tables[0]["columns"].append(
                {"name": "added", "type_name": "INTEGER", "nullable": True}
            )
            return rows

        bridge.snapshot_page = changing_snapshot
        connector = self.connector(bridge)

        with self.assertRaisesRegex(InformixError, "schema changed"):
            connector.read_table("app.orders", {}, {})

    def test_stream_offset_relationships_and_phase_are_validated(self):
        connector = self.connector()
        invalid_offsets = []
        reversed_offset = _stream_offset(100)
        reversed_offset["begin_lsn"] = "101"
        invalid_offsets.append(reversed_offset)
        invalid_phase = _stream_offset()
        invalid_phase["phase"] = "unknown"
        invalid_offsets.append(invalid_phase)
        invalid_fingerprint = _stream_offset()
        invalid_fingerprint["schema_fingerprint"] = "not-a-hash"
        invalid_offsets.append(invalid_fingerprint)
        oversized_lsn = _stream_offset()
        oversized_lsn.update(
            {
                "begin_lsn": str(1 << 64),
                "change_lsn": str(1 << 64),
                "commit_lsn": str(1 << 64),
            }
        )
        invalid_offsets.append(oversized_lsn)
        boolean_lsn = _stream_offset()
        boolean_lsn.update(begin_lsn=True, change_lsn=True, commit_lsn=True)
        invalid_offsets.append(boolean_lsn)
        boolean_tx = _stream_offset()
        boolean_tx["tx_id"] = True
        invalid_offsets.append(boolean_tx)
        oversized_tx = _stream_offset()
        oversized_tx["tx_id"] = 1 << 32
        invalid_offsets.append(oversized_tx)
        inconsistent_snapshot = _stream_offset()
        inconsistent_snapshot.update(
            {"phase": "snapshot", "snapshot_lsn": "91", "snapshot": {"last_pk": [1]}}
        )
        invalid_offsets.append(inconsistent_snapshot)

        for offset in invalid_offsets:
            with self.assertRaises(ValueError):
                connector.read_table("app.orders", offset, {})

    def test_historical_and_concurrent_deletes_sort_around_snapshot_rows(self):
        snapshot_bridge = FakeBridge()
        snapshot_connector = self.connector(snapshot_bridge)
        snapshot_rows, _ = snapshot_connector.read_table("app.orders", {}, {})
        snapshot_cursor = int(list(snapshot_rows)[0][CURSOR])

        delete_bridge = FakeBridge()
        delete_bridge.changes = [
            {"op": "BEGIN", "tx_id": 1, "lsn": 40},
            {"op": "DELETE", "tx_id": 1, "lsn": 50, "row": {"id": 1, "value": "old"}},
            {"op": "COMMIT", "tx_id": 1, "lsn": 60},
            {"op": "BEGIN", "tx_id": 2, "lsn": 91},
            {"op": "DELETE", "tx_id": 2, "lsn": 95, "row": {"id": 2, "value": "new"}},
            {"op": "COMMIT", "tx_id": 2, "lsn": 100},
        ]
        delete_connector = self.connector(delete_bridge)
        deletes, _ = delete_connector.read_table_deletes("app.orders", _stream_offset(1), {})
        delete_cursors = [int(row[CURSOR]) for row in deletes]

        self.assertLess(delete_cursors[0], snapshot_cursor)
        self.assertGreater(delete_cursors[1], snapshot_cursor)

    def test_unmaterializable_snapshot_type_fails_during_metadata(self):
        bridge = FakeBridge()
        table = _table(name="documents", cdc=False)
        table["columns"][1]["type_name"] = "TEXT"
        bridge.tables.append(table)
        connector = self.connector(bridge)

        with self.assertRaisesRegex(InformixError, r"value \(TEXT\)"):
            connector.read_table_metadata("app.documents", {})

        for operation in (
            lambda: connector.get_table_schema("app.documents", {}),
            lambda: connector.read_table("app.documents", {}, {}),
            lambda: connector.read_table_deletes("app.documents", {}, {}),
        ):
            with self.assertRaisesRegex(InformixError, r"value \(TEXT\)"):
                operation()

    def test_interleaved_transaction_recovery_is_atomic(self):
        early_record = {"op": "INSERT", "tx_id": 1, "lsn": 101, "row": {"id": 1}}
        transaction = CommittedTransaction(1, 100, 110, 110, (early_record,))
        checkpoint = _stream_offset(104)
        checkpoint["begin_lsn"] = "100"

        recovered = _recover([transaction], checkpoint)

        self.assertEqual(recovered, [transaction])
        self.assertEqual(recovered[0].records, (early_record,))

    def test_generated_available_now_base_installs_connector_callback(self):
        class TriggerBase:
            pass

        Wrapped = _informix_available_now_base(TriggerBase)

        class LakeflowStreamReader(Wrapped):
            def __init__(self):
                self.lakeflow_connect = mock.Mock()
                self.options = {"tableName": "members", "isDeleteFlow": "true"}

            def prepareForTriggerAvailableNow(self):
                raise AssertionError("shared no-op was not replaced")

        with self.assertLogs(informix_module.__name__, level="INFO") as captured:
            reader = LakeflowStreamReader()
            second = LakeflowStreamReader()
        reader.prepareForTriggerAvailableNow()
        reader.lakeflow_connect.prepare_for_trigger_available_now.assert_called_once_with()
        first_scope = reader.lakeflow_connect.set_registration_scope.call_args.args[0]
        second_scope = second.lakeflow_connect.set_registration_scope.call_args.args[0]
        self.assertRegex(first_scope, r"^[0-9a-f]{32}$")
        self.assertEqual(first_scope, second_scope)
        self.assertEqual(len(captured.output), 2)
        for message in captured.output:
            self.assertIn(f"scope={first_scope}", message)
            self.assertIn("table=members", message)
            self.assertIn("role=delete", message)

    def test_each_reader_registration_gets_a_fresh_scope(self):
        class TriggerBase:
            pass

        def reader_type(base):
            class LakeflowStreamReader(base):
                def __init__(self):
                    self.lakeflow_connect = mock.Mock()
                    self.options = {}

            return LakeflowStreamReader

        first_type = reader_type(_informix_available_now_base(TriggerBase))
        second_type = reader_type(_informix_available_now_base(first_type.__bases__[0]))
        first = first_type()
        second = second_type()

        first_scope = first.lakeflow_connect.set_registration_scope.call_args.args[0]
        second_scope = second.lakeflow_connect.set_registration_scope.call_args.args[0]
        self.assertRegex(first_scope, r"^[0-9a-f]{32}$")
        self.assertRegex(second_scope, r"^[0-9a-f]{32}$")
        self.assertNotEqual(first_scope, second_scope)

    def test_canonically_generated_reader_executes_available_now_callback(self):
        generated = importlib.import_module(
            "databricks.labs.community_connector.sources.informix."
            "_generated_informix_python_source"
        )

        class Registry:
            def register(self, source):
                self.source = source

        spark = types.SimpleNamespace(dataSource=Registry())
        generated.register_lakeflow_source(spark)
        # Registration can be invoked repeatedly in notebook/pipeline analysis.
        generated.register_lakeflow_source(spark)
        method = spark.dataSource.source.simpleStreamReader
        closure = dict(
            zip(method.__code__.co_freevars, (cell.cell_contents for cell in method.__closure__))
        )
        reader_type = closure["LakeflowStreamReader"]
        reader = reader_type.__new__(reader_type)
        reader.lakeflow_connect = mock.Mock()

        reader.prepareForTriggerAvailableNow()

        reader.lakeflow_connect.prepare_for_trigger_available_now.assert_called_once_with()

    def test_insert_update_delete_pk_change_rollback_discard_and_controls(self):
        bridge = FakeBridge()
        bridge.now = 200
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
        bridge.now = 200
        bridge.changes = [
            {"op": "BEGIN", "tx_id": 1, "lsn": 100},
            {"op": "TRUNCATE", "tx_id": 1, "lsn": 101, "table": "app.orders"},
            {"op": "COMMIT", "tx_id": 1, "lsn": 102},
        ]
        connector = self.connector(bridge)
        with self.assertRaises(UnsupportedChangeError):
            connector.read_table("app.orders", _stream_offset(), {})

    def test_incomplete_transaction_emits_nothing_and_does_not_advance(self):
        bridge = FakeBridge()
        bridge.changes = [
            {"op": "BEGIN", "tx_id": 7, "lsn": 100},
            {"op": "INSERT", "tx_id": 7, "lsn": 101,
             "row": {"id": 1, "value": "pending"}},
        ]
        start = _stream_offset()
        connector = self.connector(bridge)

        changes, end = connector.read_table("app.orders", start, {})

        self.assertEqual(list(changes), [])
        self.assertEqual(end, start)

    def test_triggered_stream_stops_at_initial_high_water(self):
        bridge = FakeBridge()
        bridge.now = 105
        bridge.changes = [
            {"op": "BEGIN", "tx_id": 8, "lsn": 106},
            {"op": "INSERT", "tx_id": 8, "lsn": 107,
             "row": {"id": 1, "value": "later"}},
            {"op": "COMMIT", "tx_id": 8, "lsn": 110},
        ]
        start = _stream_offset(100)
        connector = self.connector(bridge)
        connector.prepare_for_trigger_available_now()

        changes, end = connector.read_table("app.orders", start, {})

        self.assertEqual(list(changes), [])
        self.assertEqual(end["commit_lsn"], start["commit_lsn"])
        self.assertRegex(end["trigger_generation"], r"^[0-9a-f]{32}$")

    def test_triggered_readers_share_one_high_water(self):
        first_bridge = FakeBridge()
        first_bridge.now = 105
        second_bridge = FakeBridge()
        second_bridge.now = 110
        common = {"port": "9089", "user": "alice"}
        first = self.connector(
            first_bridge, **common, tableName="app.orders", isDeleteFlow="false"
        )
        second = self.connector(
            second_bridge, **common, tableName="app.orders", isDeleteFlow="true"
        )
        _, checkpoint = first.read_table("app.orders", {}, {})
        first.prepare_for_trigger_available_now()
        first.read_table("app.orders", checkpoint, {})
        self.assertEqual(first._trigger_boundaries["demo.app.orders"][0], 105)
        second.prepare_for_trigger_available_now()
        second.read_table_deletes("app.orders", checkpoint, {})

        self.assertEqual(second._trigger_boundaries["demo.app.orders"][0], 105)
        self.assertEqual(
            second._trigger_boundaries["demo.app.orders"][1],
            first._trigger_boundaries["demo.app.orders"][1],
        )
        self.assertEqual(second_bridge.validated_initial, [])

        second_bridge.now = 120
        second.prepare_for_trigger_available_now()
        self.assertNotIn("demo.app.orders", second._trigger_boundaries)

    def test_reused_reader_captures_new_boundary_for_next_available_now_update(self):
        bridge = FakeBridge()
        bridge.now = 105
        connector = self.connector(bridge)
        _, checkpoint = connector.read_table("app.orders", {}, {})
        connector.prepare_for_trigger_available_now()
        _, first_end = connector.read_table("app.orders", checkpoint, {})
        first_generation = connector._trigger_boundaries["demo.app.orders"][1]
        next_checkpoint = {
            **first_end,
            "begin_lsn": "105",
            "change_lsn": "105",
            "commit_lsn": "105",
            "trigger_generation": first_generation,
        }

        bridge.now = 120
        connector.prepare_for_trigger_available_now()
        connector.read_table("app.orders", next_checkpoint, {})

        self.assertEqual(connector._trigger_boundaries["demo.app.orders"][0], 120)
        self.assertNotEqual(
            connector._trigger_boundaries["demo.app.orders"][1], first_generation
        )

    def test_concurrent_pipelines_keep_trigger_boundaries_isolated(self):
        seed_bridge = FakeBridge()
        _, seed = self.connector(seed_bridge).read_table("app.orders", {}, {})
        checkpoint_a = {**seed, "trigger_generation": "a" * 32}
        checkpoint_b = {**seed, "trigger_generation": "b" * 32}

        upsert_a_bridge = FakeBridge()
        upsert_a_bridge.now = 105
        upsert_a = self.connector(upsert_a_bridge, registration_scope="pipeline-a")
        upsert_a.prepare_for_trigger_available_now()
        upsert_a.read_table("app.orders", checkpoint_a, {})

        upsert_b_bridge = FakeBridge()
        upsert_b_bridge.now = 110
        upsert_b = self.connector(upsert_b_bridge, registration_scope="pipeline-b")
        upsert_b.prepare_for_trigger_available_now()
        upsert_b.read_table("app.orders", checkpoint_b, {})

        delete_a = self.connector(FakeBridge(), registration_scope="pipeline-a")
        delete_a.prepare_for_trigger_available_now()
        delete_a.read_table_deletes("app.orders", checkpoint_a, {})
        delete_b = self.connector(FakeBridge(), registration_scope="pipeline-b")
        delete_b.prepare_for_trigger_available_now()
        delete_b.read_table_deletes("app.orders", checkpoint_b, {})

        self.assertEqual(delete_a._trigger_boundaries["demo.app.orders"][0], 105)
        self.assertEqual(delete_b._trigger_boundaries["demo.app.orders"][0], 110)
        self.assertNotEqual(
            delete_a._trigger_boundaries["demo.app.orders"][1],
            delete_b._trigger_boundaries["demo.app.orders"][1],
        )

    def test_divergent_trigger_predecessors_fail_closed(self):
        bridge = FakeBridge()
        _, seed = self.connector(bridge).read_table("app.orders", {}, {})
        upsert_checkpoint = {**seed, "trigger_generation": "a" * 32}
        delete_checkpoint = {**seed, "trigger_generation": "b" * 32}
        bridge.now = 125
        upsert = self.connector(bridge)
        upsert.prepare_for_trigger_available_now()
        upsert.read_table("app.orders", upsert_checkpoint, {})
        delete = self.connector(FakeBridge())
        delete.prepare_for_trigger_available_now()
        with self.assertRaisesRegex(InformixError, "Ambiguous immutable trigger"):
            delete.read_table_deletes("app.orders", delete_checkpoint, {})

    def test_trigger_boundary_allows_divergent_channel_lsns(self):
        bridge = FakeBridge()
        _, upsert_checkpoint = self.connector(bridge).read_table("app.orders", {}, {})
        delete_checkpoint = {
            **upsert_checkpoint,
            "begin_lsn": "80",
            "change_lsn": "80",
            "commit_lsn": "80",
        }
        bridge.now = 125
        upsert = self.connector(bridge)
        upsert.prepare_for_trigger_available_now()
        upsert.read_table("app.orders", upsert_checkpoint, {})
        delete = self.connector(FakeBridge())
        delete.prepare_for_trigger_available_now()
        delete.read_table_deletes("app.orders", delete_checkpoint, {})

        self.assertEqual(
            delete._trigger_boundaries["demo.app.orders"],
            upsert._trigger_boundaries["demo.app.orders"],
        )

    def test_atomic_coordination_does_not_require_runtime_pipeline_update_identity(self):
        bridge = FakeBridge()
        bridge.now = 105
        bridge.changes = [
            {"op": "BEGIN", "tx_id": 8, "lsn": 106},
            {"op": "INSERT", "tx_id": 8, "lsn": 107, "row": {"id": 1}},
            {"op": "COMMIT", "tx_id": 8, "lsn": 110},
        ]
        connector = self.connector(bridge)
        connector.prepare_for_trigger_available_now()

        _, checkpoint = connector.read_table("app.orders", _stream_offset(), {})

        self.assertRegex(checkpoint["trigger_generation"], r"^[0-9a-f]{32}$")

    def test_continuous_stream_does_not_freeze_high_water(self):
        bridge = FakeBridge()
        bridge.now = 105
        bridge.changes = [
            {"op": "BEGIN", "tx_id": 9, "lsn": 106},
            {"op": "INSERT", "tx_id": 9, "lsn": 107,
             "row": {"id": 1, "value": "later"}},
            {"op": "COMMIT", "tx_id": 9, "lsn": 110},
        ]
        connector = self.connector(bridge)

        changes, end = connector.read_table("app.orders", _stream_offset(100), {})

        self.assertEqual([row["id"] for row in changes], [1])
        self.assertEqual(end["commit_lsn"], "110")

    def test_continuous_stream_invokes_throttled_candidate_cleanup(self):
        bridge = FakeBridge()
        connector = self.connector(bridge)
        _, checkpoint = connector.read_table("app.orders", {}, {})
        bridge.changes = [{"op": "TIMEOUT", "lsn": bridge.now}]

        with mock.patch.object(
            informix_module, "_maybe_cleanup_immutable_candidates"
        ) as cleanup:
            connector.read_table("app.orders", checkpoint, {})

        cleanup.assert_called_with(self._shared_state.name)

    def test_immutable_head_elects_one_complete_winner(self):
        connector = self.connector(FakeBridge())
        table = Table.parse(_table(), "demo")
        namespace = connector._immutable_namespace(table, "probe", "one")
        barrier = threading.Barrier(2)
        results = []

        def publish(value):
            barrier.wait()
            results.append(connector._publish_immutable_head(namespace, {"value": value}))

        threads = [threading.Thread(target=publish, args=(value,)) for value in (1, 2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(results[0], results[1])
        self.assertIn(results[0]["value"], {1, 2})
        self.assertEqual(
            os.listdir(os.path.join(namespace, "head")), ["record.json"]
        )

    def test_immutable_head_never_replaces_winner(self):
        connector = self.connector(FakeBridge())
        table = Table.parse(_table(), "demo")
        namespace = connector._immutable_namespace(table, "probe", "stable")

        first = connector._publish_immutable_head(namespace, {"value": "first"})
        second = connector._publish_immutable_head(namespace, {"value": "second"})

        self.assertEqual(first["value"], "first")
        self.assertEqual(second, first)

    def test_malformed_initialization_winner_fails_before_use(self):
        connector = self.connector(FakeBridge())
        table = Table.parse(_table(), "demo")
        scope = connector._pipeline_scope()
        connector._publish_immutable_head(
            connector._immutable_namespace(table, "initialization", scope),
            {
                "created_at": 1.0,
                "initial_lsn": "90",
                "schema": {**_schema_state(table, 90), "id": "invalid"},
                "scope": scope,
                "table": table.native_identity,
            },
            record_type="initialization",
        )

        with self.assertRaisesRegex(InformixError, "initialization schema"):
            connector._shared_table_lsn(table, owner=True)

    def test_schema_node_namespace_rejects_wrong_embedded_id(self):
        connector = self.connector(FakeBridge())
        table = Table.parse(_table(), "demo")
        checkpoint = _stream_offset()
        connector._publish_immutable_head(
            connector._immutable_namespace(
                table, "schema-nodes", checkpoint["schema_id"]
            ),
            {
                "created_at": 1.0,
                "schema": _schema_state(table, 100, schema_id="2" * 32),
                "scope": checkpoint["pipeline_scope"],
            },
            record_type="schema-node",
        )

        with self.assertRaisesRegex(InformixError, "conflicts with immutable history"):
            connector.read_table("app.orders", checkpoint, {})

    def test_existing_schema_node_rejects_start_after_checkpoint(self):
        connector = self.connector(FakeBridge())
        table = Table.parse(_table(), "demo")
        checkpoint = _stream_offset(100)
        connector._publish_immutable_head(
            connector._immutable_namespace(
                table, "schema-nodes", checkpoint["schema_id"]
            ),
            {
                "created_at": 1.0,
                "schema": _schema_state(
                    table, 110, schema_id=checkpoint["schema_id"]
                ),
                "scope": checkpoint["pipeline_scope"],
            },
            record_type="schema-node",
        )

        with self.assertRaisesRegex(InformixError, "Invalid immutable schema-node"):
            connector.read_table("app.orders", checkpoint, {})

    def test_initialization_rejects_conflicting_schema_node_election_winner(self):
        connector = self.connector(FakeBridge())
        table = Table.parse(_table(), "demo")
        publish = connector._publish_immutable_head

        def conflicting_winner(namespace, record, *, record_type="generic"):
            winner = publish(namespace, record, record_type=record_type)
            if record_type == "schema-node":
                return {
                    **winner,
                    "schema": {**winner["schema"], "id": "f" * 32},
                }
            return winner

        with mock.patch.object(
            connector,
            "_publish_immutable_head",
            side_effect=conflicting_winner,
        ):
            with self.assertRaisesRegex(InformixError, "Conflicting immutable schema-node"):
                connector._shared_table_lsn(table, owner=True)

    def test_initialization_schema_node_is_pipeline_scope_independent(self):
        connector = self.connector(FakeBridge())
        table = Table.parse(_table(), "demo")
        _, schema_id = connector._shared_table_lsn(table, owner=True)
        node = connector._read_immutable_head(
            connector._immutable_namespace(table, "schema-nodes", schema_id)
        )
        self.assertNotIn("scope", node)

    def test_abandoned_candidate_is_not_committed_or_reclaimed_automatically(self):
        connector = self.connector(FakeBridge())
        table = Table.parse(_table(), "demo")
        namespace = connector._immutable_namespace(table, "probe", "abandoned")
        os.makedirs(namespace)
        candidate = os.path.join(namespace, "candidate-" + "a" * 32)
        os.mkdir(candidate)
        with open(os.path.join(candidate, "record.json"), "w", encoding="utf-8") as handle:
            handle.write("partial")
        os.utime(candidate, (1, 1))

        self.assertIsNone(connector._read_immutable_head(namespace))
        self.assertTrue(os.path.exists(candidate))

        with self.assertRaisesRegex(ValueError, "pipelines are stopped"):
            informix_module.cleanup_abandoned_immutable_candidates(
                self._shared_state.name,
                acknowledge_pipelines_stopped=False,
            )
        with self.assertRaisesRegex(ValueError, "/Volumes"):
            informix_module.cleanup_abandoned_immutable_candidates(
                self._shared_state.name,
                acknowledge_pipelines_stopped=True,
            )
        with mock.patch.object(
            informix_module,
            "_validated_volume_state_location",
            return_value=self._shared_state.name,
        ):
            removed = informix_module.cleanup_abandoned_immutable_candidates(
                "/Volumes/catalog/schema/volume/state",
                acknowledge_pipelines_stopped=True,
            )
        self.assertEqual(removed, 1)
        self.assertFalse(os.path.exists(candidate))

    def test_candidate_cleanup_ignores_symlinked_candidate_directory(self):
        target = os.path.join(self._shared_state.name, "target")
        os.mkdir(target)
        record = os.path.join(target, "record.json")
        with open(record, "w", encoding="utf-8") as handle:
            handle.write("keep")
        link = os.path.join(self._shared_state.name, "candidate-" + "b" * 32)
        os.symlink(target, link)

        removed = informix_module._cleanup_immutable_candidates(
            self._shared_state.name, headless_cutoff=float("inf")
        )

        self.assertEqual(removed, 0)
        self.assertTrue(os.path.exists(record))

    def test_candidate_cleanup_cannot_publish_an_empty_head_during_rename_race(self):
        namespace = os.path.join(self._shared_state.name, "namespace")
        candidate = os.path.join(namespace, "candidate-" + "e" * 32)
        os.makedirs(candidate)
        with open(os.path.join(candidate, "record.json"), "w", encoding="utf-8") as handle:
            handle.write("complete")
        os.utime(candidate, (1, 1))
        real_rename = informix_module.os.rename

        def publisher_wins(source, target):
            if source == candidate and ".candidate-gc-" in target:
                real_rename(candidate, os.path.join(namespace, "head"))
                raise FileNotFoundError(candidate)
            return real_rename(source, target)

        with mock.patch.object(informix_module.os, "rename", side_effect=publisher_wins):
            removed = informix_module._cleanup_immutable_candidates(
                self._shared_state.name, headless_cutoff=float("inf")
            )

        self.assertEqual(removed, 0)
        with open(
            os.path.join(namespace, "head", "record.json"), encoding="utf-8"
        ) as handle:
            self.assertEqual(handle.read(), "complete")

    def test_running_cleanup_collects_only_old_headless_candidates(self):
        old = os.path.join(self._shared_state.name, "candidate-" + "c" * 32)
        recent = os.path.join(self._shared_state.name, "candidate-" + "d" * 32)
        for path in (old, recent):
            os.mkdir(path)
            with open(os.path.join(path, "record.json"), "w", encoding="utf-8") as handle:
                handle.write("candidate")
        os.utime(old, (1, 1))
        informix_module._LAST_CANDIDATE_CLEANUP.pop(self._shared_state.name, None)

        informix_module._maybe_cleanup_immutable_candidates(self._shared_state.name)

        self.assertFalse(os.path.exists(old))
        self.assertTrue(os.path.exists(recent))

    def test_failed_running_cleanup_is_not_retried_within_daily_bucket(self):
        location = self._shared_state.name
        informix_module._LAST_CANDIDATE_CLEANUP.pop(location, None)
        with mock.patch.object(
            informix_module,
            "_cleanup_immutable_candidates",
            side_effect=InformixError("temporary"),
        ) as cleanup, mock.patch.object(
            informix_module, "_publish_candidate_cleanup_completion"
        ):
            informix_module._maybe_cleanup_immutable_candidates(location)
            informix_module._maybe_cleanup_immutable_candidates(location)

        self.assertEqual(cleanup.call_count, 1)

    def test_cleanup_completion_marker_is_versioned_and_validated(self):
        location = self._shared_state.name
        bucket = int(
            informix_module.time.time()
            // informix_module._CANDIDATE_CLEANUP_INTERVAL_SECONDS
        )
        informix_module._publish_candidate_cleanup_completion(location, bucket)
        marker = informix_module._read_cleanup_marker(
            informix_module._candidate_cleanup_completion_path(location, bucket),
            "candidate-cleanup-completion",
            bucket,
        )
        self.assertEqual(marker["format_version"], 1)
        self.assertEqual(marker["record_type"], "candidate-cleanup-completion")

    def test_offline_cleanup_rejects_nonexistent_volume_location(self):
        with self.assertRaisesRegex(ValueError, "does not exist"):
            informix_module.cleanup_abandoned_immutable_candidates(
                "/Volumes/missing_catalog/missing_schema/missing_volume/state",
                acknowledge_pipelines_stopped=True,
            )

    def test_malformed_cleanup_completion_is_quarantined_before_scan(self):
        location = self._shared_state.name
        bucket = int(
            informix_module.time.time()
            // informix_module._CANDIDATE_CLEANUP_INTERVAL_SECONDS
        )
        marker = informix_module._candidate_cleanup_completion_path(location, bucket)
        os.makedirs(os.path.dirname(marker))
        with open(marker, "w", encoding="utf-8") as handle:
            handle.write("not-json")
        informix_module._LAST_CANDIDATE_CLEANUP.pop(location, None)
        with mock.patch.object(informix_module, "_cleanup_immutable_candidates") as cleanup:
            informix_module._maybe_cleanup_immutable_candidates(location)
        cleanup.assert_called_once()
        self.assertEqual(
            informix_module._read_cleanup_marker(
                marker, "candidate-cleanup-completion", bucket
            )["bucket"],
            bucket,
        )

    def test_symlinked_cleanup_completion_is_quarantined_before_scan(self):
        location = self._shared_state.name
        bucket = int(
            informix_module.time.time()
            // informix_module._CANDIDATE_CLEANUP_INTERVAL_SECONDS
        )
        marker = informix_module._candidate_cleanup_completion_path(location, bucket)
        os.makedirs(os.path.dirname(marker))
        target = os.path.join(location, "marker-target")
        with open(target, "w", encoding="utf-8") as handle:
            handle.write("{}")
        os.symlink(target, marker)
        informix_module._LAST_CANDIDATE_CLEANUP.pop(location, None)
        with mock.patch.object(informix_module, "_cleanup_immutable_candidates") as cleanup:
            informix_module._maybe_cleanup_immutable_candidates(location)
        cleanup.assert_called_once()
        self.assertFalse(os.path.islink(marker))
        self.assertEqual(
            informix_module._read_cleanup_marker(
                marker, "candidate-cleanup-completion", bucket
            )["bucket"],
            bucket,
        )

    def test_cleanup_election_prevents_duplicate_scan(self):
        location = self._shared_state.name
        informix_module._LAST_CANDIDATE_CLEANUP.pop(location, None)
        with mock.patch.object(informix_module, "_cleanup_immutable_candidates") as cleanup:
            informix_module._maybe_cleanup_immutable_candidates(location)
            informix_module._LAST_CANDIDATE_CLEANUP.pop(location, None)
            informix_module._maybe_cleanup_immutable_candidates(location)
        self.assertEqual(cleanup.call_count, 1)

    def test_malformed_cleanup_election_fails_closed(self):
        location = self._shared_state.name
        bucket = int(
            informix_module.time.time()
            // informix_module._CANDIDATE_CLEANUP_INTERVAL_SECONDS
        )
        marker = informix_module._candidate_cleanup_election_path(location, bucket)
        os.makedirs(os.path.dirname(marker))
        with open(marker, "w", encoding="utf-8") as handle:
            handle.write("not-json")
        informix_module._LAST_CANDIDATE_CLEANUP.pop(location, None)
        with mock.patch.object(informix_module, "_cleanup_immutable_candidates") as cleanup:
            informix_module._maybe_cleanup_immutable_candidates(location)
        cleanup.assert_not_called()

    def test_cleanup_throttle_cache_is_bounded(self):
        informix_module._LAST_CANDIDATE_CLEANUP.clear()
        limit = informix_module._MAX_CANDIDATE_CLEANUP_THROTTLES
        for index in range(limit + 3):
            informix_module._remember_candidate_cleanup(f"location-{index}", float(index))
        self.assertEqual(len(informix_module._LAST_CANDIDATE_CLEANUP), limit)
        self.assertNotIn("location-0", informix_module._LAST_CANDIDATE_CLEANUP)

    def test_cleanup_prunes_old_marker_buckets(self):
        location = self._shared_state.name
        current = 100
        old = informix_module._candidate_cleanup_completion_path(location, 1)
        os.makedirs(os.path.dirname(old))
        with open(old, "w", encoding="utf-8") as handle:
            handle.write("old")
        informix_module._prune_candidate_cleanup_markers(location, current)
        self.assertFalse(os.path.exists(os.path.join(location, ".informix-candidate-cleanup", "1")))

    def test_cleanup_retains_exactly_seven_daily_buckets(self):
        location = self._shared_state.name
        current = 100
        root = os.path.join(location, ".informix-candidate-cleanup")
        for bucket in range(current - 7, current + 1):
            os.makedirs(os.path.join(root, str(bucket)))
        informix_module._prune_candidate_cleanup_markers(location, current)
        retained = sorted(int(name) for name in os.listdir(root))
        self.assertEqual(retained, list(range(current - 6, current + 1)))

    def test_immutable_head_rejects_symlinked_record(self):
        namespace = os.path.join(self._shared_state.name, "symlink-record")
        head = os.path.join(namespace, "head")
        os.makedirs(head)
        target = os.path.join(namespace, "target.json")
        with open(target, "w", encoding="utf-8") as handle:
            handle.write('{}')
        os.symlink(target, os.path.join(head, "record.json"))
        with self.assertRaisesRegex(InformixError, "Invalid Informix immutable record"):
            informix_module.InformixLakeflowConnect._read_immutable_head(namespace)

    def test_immutable_head_rejects_symlinked_head_directory(self):
        namespace = os.path.join(self._shared_state.name, "symlink-head")
        target = os.path.join(self._shared_state.name, "head-target")
        os.makedirs(target)
        with open(os.path.join(target, "record.json"), "w", encoding="utf-8") as handle:
            handle.write('{}')
        os.makedirs(namespace)
        os.symlink(target, os.path.join(namespace, "head"))
        with self.assertRaisesRegex(InformixError, "Invalid Informix immutable head"):
            informix_module.InformixLakeflowConnect._read_immutable_head(namespace)

    def test_oversized_immutable_record_does_not_leak_candidate(self):
        connector = self.connector(FakeBridge())
        table = Table.parse(_table(), "demo")
        namespace = connector._immutable_namespace(table, "probe", "oversized")

        with self.assertRaisesRegex(InformixError, "too large"):
            connector._publish_immutable_head(
                namespace, {"value": "x" * (informix_module._MAX_SHARED_STATE_BYTES + 1)}
            )

        self.assertEqual(os.listdir(namespace), [])

    def test_malformed_snapshot_lsn_raises_connector_error(self):
        connector = self.connector(FakeBridge())
        table = Table.parse(_table(), "demo")
        scope = connector._pipeline_scope()
        schema_id = "1" * 32
        connector._publish_immutable_head(
            connector._immutable_namespace(table, "snapshots", scope, schema_id),
            {
                "created_at": 1.0,
                "initial_lsn": "not-an-lsn",
                "scope": scope,
                "schema_id": schema_id,
                "snapshot_lsn": "90",
            },
            record_type="snapshot",
        )

        with self.assertRaisesRegex(InformixError, "Invalid initial_lsn"):
            connector._publish_snapshot_boundary(table, schema_id, 90, 90, scope)

    def test_publication_wraps_filesystem_creation_error(self):
        connector = self.connector(FakeBridge())
        table = Table.parse(_table(), "demo")
        namespace = connector._immutable_namespace(table, "probe", "mkdir-error")
        _validate_shared_state_filesystem(self._shared_state.name)
        with mock.patch.object(
            informix_module.os,
            "makedirs",
            side_effect=OSError(errno.EIO, "failed"),
        ):
            with self.assertRaisesRegex(InformixError, "Cannot create"):
                connector._publish_immutable_head(namespace, {"value": 1})

    def test_ambiguous_trigger_fallback_fails_closed(self):
        connector = self.connector(FakeBridge())
        table = Table.parse(_table(), "demo")
        checkpoint = _stream_offset()
        scope = checkpoint["pipeline_scope"]
        for index in (1, 2):
            connector._publish_immutable_head(
                connector._immutable_namespace(table, "triggers", f"candidate-{index}"),
                {
                    "created_at": float(index),
                    "checkpoint_lsn": checkpoint["commit_lsn"],
                    "generation": f"{index:032x}",
                    "high_water": "120",
                    "predecessor": "f" * 32,
                    "schema_id": checkpoint["schema_id"],
                    "scope": scope,
                },
                record_type="trigger",
            )

        with self.assertRaisesRegex(InformixError, "Ambiguous immutable trigger"):
            connector._shared_trigger_boundary(table, checkpoint, owner=False)

    def test_malformed_trigger_winner_fails_before_caching(self):
        connector = self.connector(FakeBridge())
        table = Table.parse(_table(), "demo")
        checkpoint = _stream_offset()
        scope = checkpoint["pipeline_scope"]
        predecessor = "initial"
        key = hashlib.sha256(
            "\0".join(
                (
                    scope,
                    checkpoint["schema_id"],
                    predecessor,
                )
            ).encode()
        ).hexdigest()
        connector._publish_immutable_head(
            connector._immutable_namespace(table, "triggers", key),
            {
                "checkpoint_lsn": checkpoint["commit_lsn"],
                "generation": "invalid",
                "high_water": "120",
                "predecessor": predecessor,
                "schema_id": checkpoint["schema_id"],
                "scope": scope,
            },
            record_type="trigger",
        )

        with self.assertRaisesRegex(InformixError, "trigger generation"):
            connector._shared_trigger_boundary(table, checkpoint, owner=True)
        self.assertNotIn(table.identity, connector._trigger_boundaries)

    def test_trigger_winner_rejects_wrong_predecessor_and_invalid_lsn(self):
        for predecessor, high_water, message in (
            ("f" * 32, "120", "trigger identity"),
            ("initial", "not-an-lsn", "trigger boundary"),
        ):
            with self.subTest(predecessor=predecessor, high_water=high_water):
                connector = self.connector(FakeBridge())
                table = Table.parse(_table(), "demo")
                checkpoint = _stream_offset()
                checkpoint["pipeline_scope"] = hashlib.sha256(
                    high_water.encode()
                ).hexdigest()[:32]
                scope = checkpoint["pipeline_scope"]
                key = hashlib.sha256(
                    "\0".join(
                        (scope, checkpoint["schema_id"], "initial")
                    ).encode()
                ).hexdigest()
                connector._publish_immutable_head(
                    connector._immutable_namespace(table, "triggers", key),
                    {
                        "checkpoint_lsn": checkpoint["commit_lsn"],
                        "generation": "a" * 32,
                        "high_water": high_water,
                        "predecessor": predecessor,
                        "schema_id": checkpoint["schema_id"],
                        "scope": scope,
                    },
                    record_type="trigger",
                )

                with self.assertRaisesRegex(InformixError, message):
                    connector._shared_trigger_boundary(table, checkpoint, owner=True)



    def test_trigger_cache_is_isolated_per_table(self):
        bridge = FakeBridge()
        connector = self.connector(bridge)
        first = Table.parse(_table(name="orders"), "demo")
        second = Table.parse(_table(name="members"), "demo")
        checkpoint = _stream_offset(90)

        bridge.now = 105
        first_boundary = connector._shared_trigger_boundary(
            first, checkpoint, owner=True
        )
        bridge.now = 120
        second_boundary = connector._shared_trigger_boundary(
            second, checkpoint, owner=True
        )

        self.assertEqual(first_boundary[0], 105)
        self.assertEqual(second_boundary[0], 120)
        self.assertEqual(len(connector._trigger_boundaries), 2)


if __name__ == "__main__":
    unittest.main()
