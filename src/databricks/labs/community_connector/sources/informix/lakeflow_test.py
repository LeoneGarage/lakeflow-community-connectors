"""Source-local Lakeflow contract regressions using an in-memory bridge."""

from __future__ import annotations

import base64
import errno
import hashlib
import importlib
import json
import logging
import os
import pickle
import sys
import tempfile
import threading
import time
import types
import unittest
from datetime import date, datetime
from decimal import Decimal
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
    ConnectionCapacityUnavailable,
    InformixError,
    InformixLakeflowConnect,
    LogRetentionError,
    PurePythonInformixBridge,
    SharedStateAccessUnavailable,
    Table,
    TransactionBuffer,
    TriggerBoundaryUnavailable,
    UnsupportedChangeError,
    _SharedCdcShard,
    _SharedCdcSnapshot,
    _SnapshotDrainPool,
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
    _validated_offset,
)
from databricks.labs.community_connector.sources.informix.sqli import (  # noqa: E402
    SqliProtocolError,
    SqliSessionRejected,
)


class _BlockImport:
    """Meta-path finder that makes importing a module (and its submodules) raise.

    Used to pin credential resolution onto its fail-open ``ImportError`` branch
    so a test never constructs a real ``databricks.sdk.WorkspaceClient`` and
    blocks on network host-metadata resolution. Only blocks imports not already
    in ``sys.modules``; remove it from ``sys.meta_path`` when done.
    """

    def __init__(self, prefix: str):
        self._prefix = prefix

    def find_spec(self, name, path, target=None):  # noqa: D401 - finder protocol
        if name == self._prefix or name.startswith(self._prefix + "."):
            raise ImportError(f"import of {name!r} blocked for test isolation")
        return None


def _table(owner="app", name="orders", cdc=True, primary_keys=("id",)):
    return {
        "database": "demo",
        "owner": owner,
        "name": name,
        "columns": [
            {"name": "id", "type_name": "INTEGER", "nullable": False},
            {"name": "value", "type_name": "VARCHAR", "length": 20, "cdc_supported": cdc},
        ],
        "primary_keys": list(primary_keys),
    }


def _publish_fence(root, slot_name, stage, *, marker="owner"):
    """Create a fence stage the way a real fencer does: with an author's marker.

    A fencer always plants an ``owner``/``pulse``/``mutating`` marker inside the
    stage it creates. An empty stage is a husk left by a reclaimer whose
    retirement rename failed, and it deliberately no longer fences (it would
    otherwise retire the slot permanently), so tests must plant a marker to
    express "this slot is fenced".
    """

    path = PurePythonInformixBridge._connection_slot_fence_path(root, slot_name, stage)
    os.makedirs(path, exist_ok=True)
    open(os.path.join(path, f"{marker}-" + "a" * 32), "wb").close()
    return path


class FakeBridge:
    def __init__(self):
        self.tables = [_table(), _table("sysadmin", "hidden"), _table(name="audit")]
        self.rows = [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}]
        self.changes = []
        self.now, self.minimum = 90, 1
        self.snapshot_calls = []
        self.snapshot_max_bytes = []
        self.snapshot_max_rows = []
        self.snapshot_filters = []
        self.snapshot_isolations = []
        self.prepared_identities = []
        self.validated_initial = []
        self.change_reads = []
        self.released_connections = 0
        # Renders the "__chunk_<key>" ordering string for keys chunked by
        # expression; identity by default, overridden to simulate a
        # DATETIME-to-string cast.
        self.chunk_key_fn = lambda key, value: value

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

    def snapshot_page(
        self,
        identity,
        columns,
        primary_keys,
        after,
        limit,
        max_bytes=None,
        skip=0,
        chunk_exprs=None,
        snapshot_filter=None,
    ):
        self.snapshot_max_bytes.append(max_bytes)
        self.snapshot_filters.append(snapshot_filter)
        self.snapshot_calls.append((identity, tuple(columns), tuple(primary_keys), after, limit))
        chunk_exprs = dict(chunk_exprs or {})

        def cursor_value(row, key):
            # Simulate the "__chunk_<key>" alias: chunk_key_fn renders the
            # order-preserving string form for keys chunked by expression.
            if key in chunk_exprs:
                return self.chunk_key_fn(key, row[key])
            return row[key]

        ordered = sorted(
            self.rows,
            key=lambda row: tuple(cursor_value(row, key) for key in primary_keys),
        )
        if after is not None:
            if len(after) != len(primary_keys):
                raise AssertionError("snapshot continuation arity changed")
            after_tuple = tuple(after)
            ordered = [
                row
                for row in ordered
                if tuple(cursor_value(row, key) for key in primary_keys) > after_tuple
            ]
        page = ordered[:limit]
        result = []
        for row in page:
            enriched = dict(row)
            for key in chunk_exprs:
                enriched[f"__chunk_{key}"] = cursor_value(row, key)
            result.append(enriched)
        return result

    def consistent_snapshot(
        self,
        identity,
        columns,
        primary_keys,
        page_size,
        max_rows,
        max_bytes,
        datetime_primary_key=False,
        page_consumer=None,
        snapshot_filter=None,
        isolation="REPEATABLE READ",
    ):
        del identity, columns, primary_keys, datetime_primary_key
        self.snapshot_max_bytes.append(max_bytes)
        self.snapshot_max_rows.append(max_rows)
        self.snapshot_filters.append(snapshot_filter)
        self.snapshot_isolations.append(isolation)
        if max_rows and len(self.rows) > max_rows:
            raise InformixError(f"Initial snapshot exceeds snapshot.max.rows={max_rows}")
        if page_consumer is not None:
            for page_index, start in enumerate(range(0, len(self.rows), page_size)):
                page_consumer(
                    self.now,
                    page_index,
                    list(self.rows[start : start + page_size]),
                )
            return self.now, []
        return self.now, list(self.rows)

    def snapshot_chunk(
        self,
        identity,
        columns,
        primary_keys,
        after,
        limit,
        max_bytes=None,
        chunk_exprs=None,
        snapshot_filter=None,
    ):
        rows = self.snapshot_page(
            identity,
            columns,
            primary_keys,
            after,
            limit,
            max_bytes,
            chunk_exprs=chunk_exprs,
            snapshot_filter=snapshot_filter,
        )
        return self.now, rows

    def max_primary_key(self, identity, primary_keys, chunk_exprs=None, snapshot_filter=None):
        self.snapshot_filters.append(snapshot_filter)
        if not self.rows:
            return None
        chunk_exprs = dict(chunk_exprs or {})

        def cursor_value(row, key):
            if key in chunk_exprs:
                return self.chunk_key_fn(key, row[key])
            return row[key]

        top = max(
            self.rows,
            key=lambda row: tuple(cursor_value(row, key) for key in primary_keys),
        )
        return [cursor_value(top, key) for key in primary_keys]

    def read_changes(self, tables, start_lsn, timeout_seconds, max_records):
        self.change_reads.append((tables, start_lsn))
        return list(self.changes)

    def release_connection(self):
        self.released_connections += 1


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
        "commit_lsn": str(lsn),
        "change_lsn": str(lsn),
        "begin_lsn": str(lsn),
        "tx_id": None,
        "phase": "stream",
        "schema_fingerprint": _schema_fingerprint(Table.parse(_table(), "demo")),
        "schema_id": "1" * 32,
        "pipeline_scope": hashlib.sha256(b"test-pipeline").hexdigest()[:32],
    }


def _incremental_reader_offset(lsn=90):
    """An in-progress incremental snapshot offset: phase==stream WITH an
    ``incremental`` block. This is the reader phase governed by
    ``snapshot.incremental.blocking`` (the block is dropped once the snapshot
    completes and the reader becomes pure stream)."""
    offset = _stream_offset(lsn)
    offset["incremental"] = {"started": True, "last_pk": None, "max_pk": None}
    return offset


def _volume_rename_semantics():
    """Patch os.rename to reject non-empty directories, as a UC Volume does.

    Connection-slot coordination runs on a Unity Catalog Volume, which refuses to
    rename a directory that still has entries. Local POSIX temporary directories
    allow it, so a rename that always fails in production succeeds in every test
    that does not install this shim. That gap hid a defect which permanently
    stranded connection slots, so it is worth simulating explicitly.
    """

    real_rename = os.rename

    def rename(src, dst, *args, **kwargs):
        try:
            if os.path.isdir(src) and os.listdir(src):
                raise PermissionError(errno.EPERM, "rename of non-empty directory")
        except FileNotFoundError:
            pass
        return real_rename(src, dst, *args, **kwargs)

    return mock.patch.object(informix_module.os, "rename", side_effect=rename)


def _sweep_connection_fences(root, passes=1):
    """Run cleanup, defeating the interval throttle between passes."""

    for _ in range(passes):
        informix_module._LAST_RETIRED_CONNECTION_CLEANUP.clear()
        informix_module._RETIRED_CONNECTION_CLEANUP_CURSOR.clear()
        PurePythonInformixBridge._cleanup_connection_slot_tombstones(root)


class _OfflineLakebase:
    """Stands in for a provisioned Lakebase endpoint.

    Connection slots and shared state live in Postgres, so without this every
    contract test would need credentials and a network round trip. Substituting at
    the ``LakebaseState`` boundary keeps the connector's own code paths -- the
    statements, the epoch guards, the election -- fully exercised, while the
    storage behind them is the in-process fake that ``lakebase_state_test``
    already mutation-tests.
    """

    def __init__(self, database=None):
        from databricks.labs.community_connector.sources.informix import (
            lakebase_state_test as fake,
        )

        self._fake = fake
        self.database = database if database is not None else fake._FakeDatabase()
        self.connections = []

    def install(self, test_case):
        """Patch LakebaseState so provisioning and connect never touch a network."""

        from databricks.labs.community_connector.sources.informix import lakebase_state

        facts = {
            "endpoint": "projects/offline/branches/production/endpoints/primary",
            "host": "offline.invalid",
            "state": "ACTIVE",
        }
        patch_provision = mock.patch.object(
            lakebase_state.LakebaseState, "provision", autospec=True, return_value=facts
        )
        patch_connect = mock.patch.object(
            lakebase_state.LakebaseState, "connect", autospec=True, side_effect=self._connect
        )
        test_case.addCleanup(patch_provision.stop)
        test_case.addCleanup(patch_connect.stop)
        patch_provision.start()
        patch_connect.start()
        # A fresh process cache per test: otherwise one test's state handle would
        # leak into the next and hide a provisioning bug.
        lakebase_state.LakebaseState._provisioned.clear()
        informix_module._LAKEBASE_WAITER_STATE.clear()
        informix_module._LAKEBASE_WAITER_CONNECTION.clear()
        return self

    def _connect(self, _state=None):
        connection = self._fake._FakeConnection(self.database)
        self.connections.append(connection)
        return connection

    def seed(self, namespace, slot_count):
        from databricks.labs.community_connector.sources.informix import lakebase_state

        lakebase_state.seed_slots(self._connect(), namespace, slot_count)


class LakeflowContractTests(unittest.TestCase):
    def setUp(self):
        self._shared_state = tempfile.TemporaryDirectory()
        self._lakebase = _OfflineLakebase().install(self)

    def tearDown(self):
        self._shared_state.cleanup()

    def connector(self, bridge=None, **options):
        scope_label = str(options.pop("registration_scope", "test-pipeline"))
        connector = InformixLakeflowConnect(
            {
                "database": "demo",
                "snapshot.staging.location": self._shared_state.name,
                # Required, and never reaches a real endpoint here: the offline
                # Lakebase stands in for provisioning and connecting.
                "lakebase.password": "test-state-password",
                # Most contract tests target the blocking consistent-snapshot
                # strategy. Incremental is the production default; tests that
                # exercise it override snapshot.mode explicitly.
                "snapshot.mode": "initial",
                # Sharded CDC and the shared snapshot-drain pool are the production
                # defaults, but these per-table contract tests inject a bridge directly,
                # so pin both off: they exercise the direct/inline read path and must not
                # spawn a background daemon (which builds its own bridge and would attempt
                # a real connection). Shared-mode tests opt in explicitly.
                "cdc.shared.session": "false",
                "snapshot.shared.session": "false",
                **options,
            }
        )
        connector.set_registration_scope(hashlib.sha256(scope_label.encode()).hexdigest()[:32])
        connector._bridge_instance = bridge or FakeBridge()
        return connector

    def test_close_releases_bridge_connection_and_capacity_slot(self):
        bridge = FakeBridge()
        connector = self.connector(bridge)

        connector.close()

        self.assertEqual(bridge.released_connections, 1)
        self.assertIsNone(connector._bridge_instance)

    def test_close_discards_bridge_when_release_fails(self):
        bridge = mock.Mock()
        bridge.release_connection.side_effect = RuntimeError("close failed")
        connector = self.connector(bridge)

        with self.assertRaisesRegex(RuntimeError, "close failed"):
            connector.close()

        self.assertIsNone(connector._bridge_instance)

    def test_sequential_metadata_reads_bypass_capacity_and_close_connections(self):
        bridge = FakeBridge()
        connector = self.connector(bridge)

        first = connector.read_table_metadata("app.orders", {})
        second = connector.read_table_metadata("app.orders", {})

        self.assertEqual(first, second)
        self.assertEqual(bridge.released_connections, 2)
        self.assertNotIn("_informix.bypass.connection.capacity", connector.options)

    def test_schema_discovery_restores_private_capacity_bypass(self):
        connector = self.connector()

        connector.get_table_schema("app.orders", {})

        self.assertNotIn("_informix.bypass.connection.capacity", connector.options)

    def test_registration_reads_only_the_requested_table(self):
        # Registration must resolve the one configured table with a targeted
        # lookup and never scan the whole catalog -- an unrelated unsupported
        # table elsewhere cannot then break a configured table's registration.
        bridge = FakeBridge()
        bridge.list_tables = mock.Mock(wraps=bridge.list_tables)
        bridge.get_table = mock.Mock(wraps=bridge.get_table)
        connector = self.connector(
            bridge,
            hostname="informix.example",
            server="ol_informix",
            registration_scope="targeted-read",
            **{"snapshot.staging.location": "/Volumes/test/default/informix_state"},
        )

        connector.read_table_metadata("app.orders", {})

        bridge.get_table.assert_called_once()
        bridge.list_tables.assert_not_called()

    def test_registration_table_is_shared_across_schema_instances(self):
        first_bridge = FakeBridge()
        second_bridge = FakeBridge()
        first_bridge.get_table = mock.Mock(wraps=first_bridge.get_table)
        second_bridge.get_table = mock.Mock(wraps=second_bridge.get_table)
        first_bridge.list_tables = mock.Mock(wraps=first_bridge.list_tables)
        options = {
            "hostname": "informix.example",
            "server": "ol_informix",
            "snapshot.staging.location": "/Volumes/test/default/informix_state",
            "registration_scope": "shared-schema-catalog",
        }
        first = self.connector(first_bridge, **options)
        second = self.connector(second_bridge, **options)

        first_schema = first.get_table_schema("app.orders", {})
        second_schema = second.get_table_schema("app.orders", {})

        self.assertEqual(
            [field.name for field in first_schema.fields],
            [field.name for field in second_schema.fields],
        )
        # One targeted lookup, shared across instances via the coordinator cache;
        # the catalog is never scanned wholesale.
        first_bridge.get_table.assert_called_once()
        second_bridge.get_table.assert_not_called()
        first_bridge.list_tables.assert_not_called()

    def test_new_registration_scope_refetches_table(self):
        first_bridge = FakeBridge()
        second_bridge = FakeBridge()
        first_bridge.get_table = mock.Mock(wraps=first_bridge.get_table)
        second_bridge.get_table = mock.Mock(wraps=second_bridge.get_table)
        options = {
            "hostname": "informix.example",
            "server": "ol_informix",
            "snapshot.staging.location": "/Volumes/test/default/informix_state",
        }
        first = self.connector(first_bridge, registration_scope="schema-generation-one", **options)
        second = self.connector(
            second_bridge, registration_scope="schema-generation-two", **options
        )

        first.get_table_schema("app.orders", {})
        second.get_table_schema("app.orders", {})

        first_bridge.get_table.assert_called_once()
        second_bridge.get_table.assert_called_once()

    def test_worker_release_failure_does_not_mask_primary_error(self):
        bridge = FakeBridge()
        bridge.list_tables = mock.Mock(side_effect=ValueError("primary failure"))
        bridge.release_connection = mock.Mock(side_effect=RuntimeError("release failure"))
        connector = self.connector(bridge)

        with self.assertRaisesRegex(ValueError, "primary failure") as caught:
            connector.list_tables()

        self.assertIn("release failure", " ".join(caught.exception.__notes__))

    def test_worker_release_failure_restores_private_capacity_mode(self):
        bridge = FakeBridge()
        bridge.release_connection = mock.Mock(side_effect=RuntimeError("release failure"))
        connector = self.connector(bridge)

        with self.assertRaisesRegex(RuntimeError, "release failure"):
            connector.read_table("app.orders", {}, {})

        self.assertNotIn("_informix.nonblocking.connection.capacity", connector.options)

    def test_worker_rejects_bridge_without_release_contract(self):
        class IncompleteBridge:
            @staticmethod
            def list_tables():
                return []

        connector = self.connector(IncompleteBridge())

        with self.assertRaisesRegex(InformixError, "release_connection"):
            connector.list_tables()

    def test_missing_release_contract_does_not_mask_primary_error(self):
        class FailingIncompleteBridge:
            @staticmethod
            def list_tables():
                raise ValueError("primary failure")

        connector = self.connector(FailingIncompleteBridge())

        with self.assertRaisesRegex(ValueError, "primary failure") as caught:
            connector.list_tables()

        self.assertIn("release_connection", " ".join(caught.exception.__notes__))

    def test_initial_snapshot_rejects_bridge_without_consistency_contract(self):
        bridge = FakeBridge()
        bridge.consistent_snapshot = None
        connector = self.connector(bridge)

        with self.assertRaisesRegex(InformixError, "consistent_snapshot"):
            connector.read_table("app.orders", {}, {})

        self.assertEqual(bridge.prepared_identities, [])

    def test_invalid_consistent_snapshot_results_are_not_published(self):
        cases = (
            ("invalid-lsn", lambda bridge: ("invalid", list(bridge.rows)), {}, "snapshot LSN"),
            ("old-lsn", lambda bridge: (bridge.now - 1, list(bridge.rows)), {}, "precedes"),
            (
                "future-lsn",
                lambda bridge: (bridge.now + 1, list(bridge.rows)),
                {},
                "exceeds current",
            ),
            (
                "too-many-rows",
                lambda bridge: (bridge.now, list(bridge.rows)),
                {"snapshot.max.rows": "1"},
                "snapshot.max.rows",
            ),
            (
                "wrong-shape",
                lambda bridge: (bridge.now, [{"id": 1}]),
                {},
                "exactly match",
            ),
            (
                "too-many-bytes",
                lambda bridge: (bridge.now, list(bridge.rows)),
                {"snapshot.max.bytes": "1"},
                "snapshot.max.bytes",
            ),
        )
        for label, result, options, message in cases:
            with self.subTest(label=label):
                bridge = FakeBridge()
                bridge.consistent_snapshot = lambda *args, _bridge=bridge, **kwargs: result(_bridge)
                connector = self.connector(bridge, registration_scope=label)

                with self.assertRaisesRegex(InformixError, message):
                    connector.read_table("app.orders", {}, options)

                table = Table.parse(bridge.tables[0], "demo")
                schema_id = connector._snapshot_schema_ids[
                    (connector._pipeline_scope(), table.identity)
                ]
                self.assertIsNone(
                    connector._read_immutable_head(
                        connector._immutable_namespace(
                            table,
                            "snapshots",
                            connector._pipeline_scope(),
                            schema_id,
                        )
                    )
                )

    def test_snapshot_materialization_failure_is_not_published(self):
        bridge = FakeBridge()
        bridge.tables[0]["columns"].append(
            {
                "name": "amount",
                "type_name": "DECIMAL",
                "nullable": True,
                "precision": 5,
                "scale": 0xFF,
            }
        )
        for row in bridge.rows:
            row["amount"] = object()
        connector = self.connector(bridge, registration_scope="bad-materialization")

        with self.assertRaisesRegex(InformixError, "is not a valid decimal"):
            connector.read_table("app.orders", {}, {})

        table = Table.parse(bridge.tables[0], "demo")
        schema_id = connector._snapshot_schema_ids[(connector._pipeline_scope(), table.identity)]
        self.assertIsNone(
            connector._read_immutable_head(
                connector._immutable_namespace(
                    table,
                    "snapshots",
                    connector._pipeline_scope(),
                    schema_id,
                )
            )
        )

    def test_snapshot_rejects_values_that_cannot_be_staged_safely(self):
        bridge = FakeBridge()
        bridge.rows[0]["id"] = object()
        connector = self.connector(bridge, registration_scope="bad-spark-type")

        with self.assertRaisesRegex(InformixError, "cannot be staged safely"):
            connector.read_table("app.orders", {}, {})

        table = Table.parse(bridge.tables[0], "demo")
        schema_id = connector._snapshot_schema_ids[(connector._pipeline_scope(), table.identity)]
        self.assertIsNone(
            connector._read_immutable_head(
                connector._immutable_namespace(
                    table,
                    "snapshots",
                    connector._pipeline_scope(),
                    schema_id,
                )
            )
        )

    def test_snapshot_defers_timestamp_validation_to_framework(self):
        for label, value in (
            ("invalid", "not-a-timestamp"),
            ("zoned", "2026-07-24T12:00:00+10:00"),
        ):
            with self.subTest(label=label):
                bridge = FakeBridge()
                bridge.tables[0]["columns"].append(
                    {
                        "name": "event_time",
                        "type_name": "DATETIME",
                        "nullable": True,
                        "length": 0x000A,
                    }
                )
                for row in bridge.rows:
                    row["event_time"] = value
                connector = self.connector(bridge, registration_scope=f"bad-timestamp-{label}")

                rows, _ = connector.read_table("app.orders", {}, {})
                self.assertEqual(list(rows)[0]["event_time"], value)

    def test_cdc_defers_spark_type_validation_to_framework(self):
        bridge = FakeBridge()
        connector = self.connector(bridge)
        _, checkpoint = connector.read_table("app.orders", {}, {})
        bridge.changes = [
            {"op": "BEGIN", "tx_id": 7, "lsn": 101},
            {
                "op": "INSERT",
                "tx_id": 7,
                "lsn": 102,
                "row": {"id": object(), "value": "invalid"},
            },
            {"op": "COMMIT", "tx_id": 7, "lsn": 103},
        ]

        rows, _ = connector.read_table("app.orders", checkpoint, {})
        self.assertIs(list(rows)[0]["id"], bridge.changes[1]["row"]["id"])

    def test_cdc_coerces_variable_scale_decimal_change_values(self):
        # Regression: a CDC change delivering a floating DECIMAL(p) value whose
        # magnitude needs more than p integer digits (114250 = 11425e1) used to
        # crash in Spark's convert_decimal via the decimal(38,38-p) mapping.
        bridge = FakeBridge()
        bridge.tables[0]["columns"].append(
            {
                "name": "agt_no",
                "type_name": "DECIMAL",
                "nullable": True,
                "precision": 5,
                "scale": 0xFF,
            }
        )
        for row in bridge.rows:
            row["agt_no"] = Decimal("1")
        connector = self.connector(bridge)
        _, checkpoint = connector.read_table("app.orders", {}, {})
        bridge.changes = [
            {"op": "BEGIN", "tx_id": 7, "lsn": 101},
            {
                "op": "INSERT",
                "tx_id": 7,
                "lsn": 102,
                "row": {"id": 99, "value": "x", "agt_no": Decimal("114250")},
            },
            {"op": "COMMIT", "tx_id": 7, "lsn": 103},
        ]

        rows, _ = connector.read_table("app.orders", checkpoint, {})
        changed = next(row for row in rows if row["id"] == 99)
        self.assertEqual(changed["agt_no"], Decimal("114250.000000000000000000"))

    def test_shaped_snapshot_memory_is_included_in_byte_limit(self):
        bridge = FakeBridge()
        connector = self.connector(bridge, registration_scope="shaped-byte-limit")
        raw_size = informix_module._deep_size(bridge.rows)

        with self.assertRaisesRegex(InformixError, "shaped snapshot exceeds"):
            connector.read_table("app.orders", {}, {"snapshot.max.bytes": str(raw_size)})

        table = Table.parse(bridge.tables[0], "demo")
        schema_id = connector._snapshot_schema_ids[(connector._pipeline_scope(), table.identity)]
        self.assertIsNone(
            connector._read_immutable_head(
                connector._immutable_namespace(
                    table,
                    "snapshots",
                    connector._pipeline_scope(),
                    schema_id,
                )
            )
        )

    def test_worker_release_does_not_require_python_311_sys_exception(self):
        bridge = FakeBridge()
        connector = self.connector(bridge)

        with mock.patch.object(
            informix_module.sys,
            "exception",
            side_effect=AssertionError("Python 3.11-only API used"),
            create=True,
        ):
            connector.list_tables()

        self.assertEqual(bridge.released_connections, 1)

    def test_shared_state_backoff_never_sleeps_past_deadline(self):
        with (
            mock.patch.object(informix_module.time, "monotonic", return_value=9.75),
            mock.patch.object(informix_module.random, "uniform", return_value=1.2),
            mock.patch.object(informix_module.time, "sleep") as sleep,
        ):
            next_delay = informix_module._sleep_with_backoff(10.0, 2.0)

        sleep.assert_called_once_with(0.25)
        self.assertEqual(next_delay, 2.0)

    def test_shared_state_validation_module_state_is_pickle_safe(self):
        coordinator = informix_module._STATE_VALIDATION_COORDINATOR
        restored = pickle.loads(pickle.dumps(coordinator))

        self.assertFalse(restored.validated)
        self.assertFalse(restored.claims)

    def test_generated_source_class_survives_cloudpickle_when_available(self):
        try:
            from pyspark import cloudpickle
        except ImportError:
            try:
                import cloudpickle
            except ImportError:
                self.skipTest("cloudpickle is not installed in the unit environment")
        generated = importlib.import_module(
            "databricks.labs.community_connector.sources.informix."
            "_generated_informix_python_source"
        )
        self.assertIsInstance(generated.datetime, type)

        registry = mock.Mock()
        spark = types.SimpleNamespace(dataSource=registry)
        generated.register_lakeflow_source(spark)
        source_class = registry.register.call_args.args[0]

        cloudpickle.loads(cloudpickle.dumps(source_class))

    def test_state_connection_key_includes_port(self):
        # The key pair identifies which Informix endpoint and table a state record
        # belongs to, so two ports must never collide while equivalent spellings
        # of one endpoint must agree.
        table = Table.parse(_table(), "demo")
        first = self.connector(FakeBridge(), port="9088")
        second = self.connector(FakeBridge(), port="9089")

        self.assertNotEqual(
            first._table_state_keys(table),
            second._table_state_keys(table),
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
            equivalent._table_state_keys(table),
            canonical._table_state_keys(table),
        )
        distinct_case = self.connector(FakeBridge())
        distinct_case.options.update(
            hostname="example.com", port="9088", server="DEMO", database="DEMO"
        )
        self.assertNotEqual(
            distinct_case._table_state_keys(table),
            canonical._table_state_keys(table),
        )
        with self.assertRaisesRegex(ValueError, "Unity Catalog Volume"):
            InformixLakeflowConnect(
                {
                    "database": "demo",
                    "hostname": "host",
                    "snapshot.staging.location": "/Volumes/catalog-only",
                    "lakebase.password": "test-state-password",
                }
            )

    def test_changing_registration_scope_clears_scope_bound_caches(self):
        connector = self.connector(FakeBridge())
        connector._snapshot_high_water["demo.app.orders"] = 90
        connector._snapshot_schema_ids["demo.app.orders"] = "a" * 32
        connector._trigger_boundaries["demo.app.orders"] = (
            100,
            "b" * 32,
            connector._registration_scope,
        )
        connector.prepare_for_trigger_available_now()
        self.assertTrue(connector._trigger_available_now)

        connector.set_registration_scope("f" * 32)

        self.assertEqual(connector._snapshot_high_water, {})
        self.assertEqual(connector._snapshot_schema_ids, {})
        self.assertEqual(connector._trigger_boundaries, {})
        self.assertFalse(connector._trigger_available_now)

    def test_new_registration_scope_does_not_retain_available_now_mode(self):
        bridge = FakeBridge()
        connector = self.connector(bridge)
        connector.prepare_for_trigger_available_now()
        connector.set_registration_scope("e" * 32)
        _, checkpoint = connector.read_table("app.orders", {}, {})
        bridge.changes = [
            {"op": "BEGIN", "tx_id": 8, "lsn": 101},
            {"op": "INSERT", "tx_id": 8, "lsn": 102, "row": {"id": 4, "value": "d"}},
            {"op": "COMMIT", "tx_id": 8, "lsn": 105},
        ]

        rows, end = connector.read_table("app.orders", checkpoint, {})

        self.assertEqual(len(list(rows)), 1)
        self.assertEqual(end["commit_lsn"], "105")
        self.assertIsNone(end["trigger_generation"])

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

    def test_catalog_preserves_decimal_with_variable_scale(self):
        column = _catalog_column(
            {"colname": "prefix", "coltype": 5, "collength": 0x05FF, "colno": 1}
        )

        self.assertEqual(column["precision"], 5)
        self.assertEqual(column["scale"], 0xFF)
        # Default maps variable-scale DECIMAL(p) to decimal(38,18); p is a
        # significant-digit count with a floating exponent, so the old
        # decimal(38,38-p) mapping under-sized the integer part and overflowed.
        spark_type = _spark_type(
            Column(
                name="prefix",
                type_name="DECIMAL",
                precision=column["precision"],
                scale=column["scale"],
            )
        )
        self.assertEqual((spark_type.precision, spark_type.scale), (38, 18))

    def test_variable_decimal_type_option_selects_target_spark_type(self):
        column = Column(name="amount", type_name="DECIMAL", precision=5, scale=0xFF)
        cases = {
            "string": "StringType",
            "double": "DoubleType",
            "integer": "LongType",
            "decimal(20,4)": "DecimalType",
        }
        for spec, expected in cases.items():
            with self.subTest(spec=spec):
                spark_type = _spark_type(column, {"decimal.variable.type": spec})
                self.assertEqual(type(spark_type).__name__, expected)
        decimal_type = _spark_type(column, {"decimal.variable.type": "decimal(20,4)"})
        self.assertEqual((decimal_type.precision, decimal_type.scale), (20, 4))

    def test_variable_decimal_column_type_overrides_global(self):
        agt = Column(name="agt_no", type_name="DECIMAL", precision=5, scale=0xFF)
        other = Column(name="rate", type_name="DECIMAL", precision=5, scale=0xFF)
        options = {
            "decimal.variable.type": "string",
            "decimal.variable.column.type": "agt_no:decimal(9,0)",
        }
        agt_type = _spark_type(agt, options)
        self.assertEqual(type(agt_type).__name__, "DecimalType")
        self.assertEqual((agt_type.precision, agt_type.scale), (9, 0))
        # The global option still governs columns without an override.
        self.assertEqual(type(_spark_type(other, options)).__name__, "StringType")

    def test_variable_decimal_type_rejects_invalid_spec(self):
        column = Column(name="amount", type_name="DECIMAL", precision=5, scale=0xFF)
        for spec in ("float", "decimal(40,2)", "decimal(10,20)", "decimal()"):
            with self.subTest(spec=spec):
                with self.assertRaises(ValueError):
                    _spark_type(column, {"decimal.variable.type": spec})

    def test_fixed_scale_decimal_ignores_variable_type_option(self):
        # Explicit DECIMAL(p,s) and ANSI-mode DECIMAL(p) (which the catalog
        # reports as scale 0, not the 255 sentinel) must keep DecimalType(p,s)
        # and ignore decimal.variable.type / its default.
        options = {"decimal.variable.type": "string"}
        for label, scale in (("explicit-scale", 2), ("ansi-decimal-p", 0)):
            with self.subTest(label=label):
                column = Column(name="amount", type_name="DECIMAL", precision=5, scale=scale)
                spark_type = _spark_type(column, options)
                self.assertEqual(type(spark_type).__name__, "DecimalType")
                self.assertEqual((spark_type.precision, spark_type.scale), (5, scale))

    def test_variable_decimal_default_converts_values_to_decimal(self):
        bridge = FakeBridge()
        bridge.tables[0]["columns"].append(
            {
                "name": "variable_amount",
                "type_name": "DECIMAL",
                "nullable": True,
                "precision": 5,
                "scale": 0xFF,
            }
        )
        # 114250 is 5 significant digits with a floating exponent (11425e1); the
        # old decimal(38,33) mapping crashed on exactly this shape.
        bridge.rows[0]["variable_amount"] = Decimal("114250")
        bridge.rows[1]["variable_amount"] = Decimal("0.00123")
        connector = self.connector(bridge)

        schema = connector.get_table_schema("app.orders", {})
        field = next(f for f in schema.fields if f.name == "variable_amount")
        self.assertEqual(type(field.dataType).__name__, "DecimalType")
        self.assertEqual((field.dataType.precision, field.dataType.scale), (38, 18))
        rows, _ = connector.read_table("app.orders", {}, {})
        self.assertEqual(
            [row["variable_amount"] for row in rows],
            [Decimal("114250.000000000000000000"), Decimal("0.001230000000000000")],
        )

    def test_variable_decimal_type_string_converts_values(self):
        bridge = FakeBridge()
        bridge.tables[0]["columns"].append(
            {
                "name": "variable_amount",
                "type_name": "DECIMAL",
                "nullable": True,
                "precision": 5,
                "scale": 0xFF,
            }
        )
        bridge.rows[0]["variable_amount"] = Decimal("123.45")
        bridge.rows[1]["variable_amount"] = Decimal("0.00123")
        connector = self.connector(bridge)
        options = {"decimal.variable.type": "string"}

        schema = connector.get_table_schema("app.orders", options)
        field = next(f for f in schema.fields if f.name == "variable_amount")
        self.assertEqual(type(field.dataType).__name__, "StringType")
        rows, _ = connector.read_table("app.orders", {}, options)
        self.assertEqual([row["variable_amount"] for row in rows], ["123.45", "0.00123"])

    def test_variable_decimal_type_integer_truncates_values(self):
        bridge = FakeBridge()
        bridge.tables[0]["columns"].append(
            {
                "name": "variable_amount",
                "type_name": "DECIMAL",
                "nullable": True,
                "precision": 5,
                "scale": 0xFF,
            }
        )
        bridge.rows[0]["variable_amount"] = Decimal("114250")
        bridge.rows[1]["variable_amount"] = Decimal("123.99")
        connector = self.connector(bridge)
        options = {"decimal.variable.type": "integer"}

        rows, _ = connector.read_table("app.orders", {}, options)
        self.assertEqual([row["variable_amount"] for row in rows], [114250, 123])

    def test_variable_decimal_value_overflowing_decimal_target_raises(self):
        bridge = FakeBridge()
        bridge.tables[0]["columns"].append(
            {
                "name": "variable_amount",
                "type_name": "DECIMAL",
                "nullable": True,
                "precision": 5,
                "scale": 0xFF,
            }
        )
        bridge.rows[0]["variable_amount"] = Decimal("114250")
        bridge.rows[1]["variable_amount"] = Decimal("1.00")
        connector = self.connector(bridge)
        options = {"decimal.variable.type": "decimal(5,2)"}

        with self.assertRaisesRegex(InformixError, "exceeds decimal"):
            connector.read_table("app.orders", {}, options)

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
        descriptor = _capture_descriptor(table, informix_module._client_encoding(connector.options))
        self.assertEqual(
            {column["encoding"] for column in descriptor["descriptors"]},
            {"iso8859-1"},
        )

    def test_cdc_max_records_matches_live_informix_boundary(self):
        self.connector(**{"cdc.max.records": "256"})
        with self.assertRaisesRegex(ValueError, "must be <= 256"):
            self.connector(**{"cdc.max.records": "257"})

    def test_cdc_max_records_defaults_to_the_largest_window_the_source_accepts(self):
        # Every poll pays a connection slot plus a full CDC session
        # open/activate/close, so requesting fewer records than Informix allows
        # amortises that fixed cost over less log progress. Pin the default to
        # the ceiling so it cannot regress to a smaller window unnoticed.
        self.assertEqual(informix_module._DEFAULT_CDC_MAX_RECORDS, 256)
        transport = FakeCdcTransport([[]])
        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.transport = transport
        requested: list[int] = []
        original = transport.execute

        def record(sql, parameters=()):
            if "cdc_opensess" in sql:
                requested.append(int(parameters[2]))
            return original(sql, parameters)

        transport.execute = record
        capture = {
            "identity": "demo:app.orders",
            "logical_identity": "demo.app.orders",
            "columns": ["id"],
            "descriptors": [{"name": "id", "type_name": "INTEGER"}],
        }
        connector = self.connector()
        default = connector._table_int_option({}, "cdc.max.records", 256, minimum=1, maximum=256)
        with (
            mock.patch.object(informix_module, "CdcFrameParser", RecordParser),
            mock.patch.object(
                informix_module, "decode_frame", side_effect=lambda frame, labels: dict(frame)
            ),
        ):
            bridge.read_changes([capture], 90, 1, default)

        # The window reaches Informix itself through cdc_opensess, not just the
        # connector's own loop bound.
        self.assertEqual(requested, [256])

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

        with (
            mock.patch.object(informix_module, "CdcFrameParser", RecordParser),
            mock.patch.object(
                informix_module, "decode_frame", side_effect=lambda frame, labels: dict(frame)
            ),
        ):
            records = bridge.read_changes([capture], 90, 1, 2)

        self.assertEqual(
            [record["op"] for record in records], ["BEGIN", "INSERT", "INSERT", "COMMIT"]
        )
        self.assertEqual(transport.reads, 2)

    def test_native_record_target_does_not_count_metadata(self):
        transport = FakeCdcTransport(
            [
                [
                    {"op": "METADATA", "label": 1, "metadata": [{"name": "id"}]},
                    {"op": "BEGIN", "tx_id": 1, "lsn": 100},
                    {"op": "INSERT", "tx_id": 1, "lsn": 101},
                    {"op": "COMMIT", "tx_id": 1, "lsn": 102},
                ]
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

        with (
            mock.patch.object(informix_module, "CdcFrameParser", RecordParser),
            mock.patch.object(
                informix_module, "decode_frame", side_effect=lambda frame, labels: dict(frame)
            ),
            mock.patch.object(informix_module, "metadata_column_names", return_value=["id"]),
            mock.patch.object(bridge, "_assert_capture_layout"),
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
            [
                [
                    {"op": "METADATA", "label": 1, "metadata": b"id integer"},
                    {"op": "METADATA", "label": 1, "metadata": b"id bigint"},
                ]
            ]
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

        with (
            mock.patch.object(informix_module, "CdcFrameParser", RecordParser),
            mock.patch.object(
                informix_module, "decode_frame", side_effect=lambda frame, labels: dict(frame)
            ),
            mock.patch.object(informix_module, "metadata_column_names", return_value=["id"]),
            mock.patch.object(bridge, "_assert_capture_layout"),
            self.assertRaisesRegex(InformixError, "second CDC metadata layout"),
        ):
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

        with (
            mock.patch.object(informix_module, "CdcFrameParser", RecordParser),
            mock.patch.object(
                informix_module, "decode_frame", side_effect=lambda frame, labels: dict(frame)
            ),
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

        with (
            mock.patch.object(informix_module, "CdcFrameParser", RecordParser),
            mock.patch.object(
                informix_module, "decode_frame", side_effect=lambda frame, labels: dict(frame)
            ),
            mock.patch.object(
                informix_module, "_deep_size", side_effect=AssertionError("must not account")
            ),
        ):
            bridge.read_changes([capture], 90, 1, 2)

    def test_a_stale_labelled_cdc_frame_fails_closed_naming_its_log_file(self):
        """A replayed foreign log record must fail loudly, not crash opaquely.

        Real frame captured from Informix 14.10: an INSERT tagged with this
        session's capture label whose payload is a syscolumns row from a reused
        logical log, so it cannot be decoded under the registered layout. It is
        neither skipped (it might be a real change) nor ingested (it is not this
        table's data) -- the poll fails with the log file named, because the fix
        is a full refresh onto a live restart position.
        """

        stale = base64.b64decode(
            "AAAAKAAAAHMAAABCAAAAKAAAAAcKnjegAAAAHwAAAAEAAAAAAAAAbwd0YWJuYW1lAAAAAQAB"
            "AA0AgIAAAACAAAAAAAAAAAAAAAAAAGxhc3MuYwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            "AAAAAAAAAAAAAAAAnQUAAAAAAABpHn7UAAAAAP//AAAAAAAAAAAAAAAAAAAAAAA="
        )

        class RawFrameTransport(FakeCdcTransport):
            def read_lodata(self, descriptor, requested):
                self.reads += 1
                return stale if self.reads == 1 else b""

        transport = RawFrameTransport([])
        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.transport = transport
        capture = {
            "identity": "demo:app.tw316_ovhc",
            "logical_identity": "demo.app.tw316_ovhc",
            "columns": ["source_rowid", "payload"],
            "descriptors": [
                {"name": "source_rowid", "type_name": "INTEGER", "length": 4},
                {"name": "payload", "type_name": "VARCHAR", "length": 160},
            ],
        }

        with self.assertRaises(InformixError) as caught:
            bridge.read_changes([capture], 30239870976, 1, 64)

        message = str(caught.exception)
        # Names the log file the offending record came from (7) and the restart
        # position's own log file (7), so the operator can see the checkpoint sits
        # in reused territory rather than guessing from a codec error.
        self.assertIn("logical log 7", message)
        self.assertIn("30239870976", message)
        self.assertIn("full refresh", message)
        # The decode failure is preserved rather than replaced.
        self.assertIsInstance(caught.exception.__cause__, informix_module.CdcProtocolError)

    def test_a_genuine_cdc_frame_still_decodes_through_read_changes(self):
        """The guard must not reject valid records: same label, live log file."""

        # The captured INSERT belongs to transaction 182; wrap it in the BEGIN and
        # COMMIT the poll requires so the record is a completed transaction.
        begin = base64.b64decode("AAAAKAAAAAAAAAAAAAAAAQAAAAwEpMG3AAAAtgAAAAAAAAAAAAAAAA==")
        good = base64.b64decode(
            "AAAAKAAAACEAAABCAAAAKAAAAAwEpMG4AAAAtgAAAAEAAAAAAAAAHQAAAAEcU3ludGhldGlj"
            "IE9WSEMgaGVhbHRoIGZ1bmQgMQ=="
        )
        commit = base64.b64decode("AAAAJAAAAAAAAAAAAAAAAgAAAAwEpMG5AAAAtgAAAAAAAAAA")

        class RawFrameTransport(FakeCdcTransport):
            def read_lodata(self, descriptor, requested):
                self.reads += 1
                return begin + good + commit if self.reads == 1 else b""

        transport = RawFrameTransport([])
        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.transport = transport
        capture = {
            "identity": "demo:app.tw316_ovhc",
            "logical_identity": "demo.app.tw316_ovhc",
            "columns": ["source_rowid", "payload"],
            "descriptors": [
                {"name": "source_rowid", "type_name": "INTEGER", "length": 4},
                {"name": "payload", "type_name": "VARCHAR", "length": 160},
            ],
        }

        records = bridge.read_changes([capture], 51539607552, 1, 64)

        rows = [record["row"] for record in records if record.get("op") == "INSERT"]
        self.assertEqual(rows, [{"source_rowid": 1, "payload": "Synthetic OVHC health fund 1"}])

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

        with (
            mock.patch.object(informix_module, "CdcFrameParser", RecordParser),
            mock.patch.object(
                informix_module, "decode_frame", side_effect=lambda frame, labels: dict(frame)
            ),
            self.assertRaisesRegex(InformixError, "CDC session cleanup failed"),
        ):
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

        with (
            mock.patch.object(informix_module, "CdcFrameParser", RecordParser),
            mock.patch.object(informix_module, "decode_frame", side_effect=ValueError("primary")),
            self.assertRaisesRegex(ValueError, "primary") as caught,
        ):
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

        with (
            mock.patch.object(informix_module, "CdcFrameParser", RecordParser),
            mock.patch.object(
                informix_module, "decode_frame", side_effect=lambda frame, labels: dict(frame)
            ),
        ):
            records = bridge.read_changes([capture], 90, 1, 2)

        self.assertEqual([record["op"] for record in records], ["TIMEOUT"])

    def test_native_poll_has_a_total_record_safety_bound(self):
        transport = FakeCdcTransport([[{"op": "METADATA"}, {"op": "METADATA"}, {"op": "METADATA"}]])
        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {"cdc.max.poll.records": "2"}
        bridge.transport = transport
        capture = {
            "identity": "demo:app.orders",
            "logical_identity": "demo.app.orders",
            "columns": ["id"],
            "descriptors": [{"name": "id", "type_name": "INTEGER"}],
        }

        with (
            mock.patch.object(informix_module, "CdcFrameParser", RecordParser),
            mock.patch.object(
                informix_module, "decode_frame", side_effect=lambda frame, labels: dict(frame)
            ),
            self.assertRaisesRegex(InformixError, "cdc.max.poll.records=2"),
        ):
            bridge.read_changes([capture], 90, 1, 2)

    def test_native_poll_has_a_total_decoded_byte_safety_bound(self):
        transport = FakeCdcTransport([[{"op": "METADATA"}, {"op": "METADATA"}, {"op": "METADATA"}]])
        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {"cdc.max.poll.bytes": "2"}
        bridge.transport = transport
        capture = {
            "identity": "demo:app.orders",
            "logical_identity": "demo.app.orders",
            "columns": ["id"],
            "descriptors": [{"name": "id", "type_name": "INTEGER"}],
        }

        with (
            mock.patch.object(informix_module, "CdcFrameParser", RecordParser),
            mock.patch.object(
                informix_module, "decode_frame", side_effect=lambda frame, labels: dict(frame)
            ),
            self.assertRaisesRegex(InformixError, "cdc.max.poll.bytes=2"),
        ):
            bridge.read_changes([capture], 90, 1, 2)

    def test_poll_record_bound_truncates_instead_of_failing_once_a_transaction_commits(self):
        # A long-running interleaved transaction keeps the poll loop reading
        # well past cdc.max.records, so under a bulk load the poll bound is
        # reached routinely. Failing there strands the flow: it retries the same
        # bound forever and its LSN never advances. Returning the transactions
        # that already committed lets the checkpoint move on instead.
        chunk = [
            {"op": "BEGIN", "tx_id": 1, "lsn": 100},
            {"op": "INSERT", "tx_id": 1, "lsn": 101},
            {"op": "COMMIT", "tx_id": 1, "lsn": 102},
            {"op": "BEGIN", "tx_id": 2, "lsn": 103},
            {"op": "INSERT", "tx_id": 2, "lsn": 104},
            {"op": "INSERT", "tx_id": 2, "lsn": 105},
        ]
        transport = FakeCdcTransport([chunk])
        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {"cdc.max.poll.records": "5"}
        bridge.transport = transport
        capture = {
            "identity": "demo:app.orders",
            "logical_identity": "demo.app.orders",
            "columns": ["id"],
            "descriptors": [{"name": "id", "type_name": "INTEGER"}],
        }

        with (
            mock.patch.object(informix_module, "CdcFrameParser", RecordParser),
            mock.patch.object(
                informix_module, "decode_frame", side_effect=lambda frame, labels: dict(frame)
            ),
        ):
            records = bridge.read_changes([capture], 90, 1, 2)

        self.assertEqual(len(records), 5)
        # The committed transaction survives so the caller can checkpoint it,
        # and the still-open one is left to be replayed by the next poll.
        committed = informix_module._committed_transactions(records)
        self.assertEqual([tx.tx_id for tx in committed], [1])

    def test_poll_byte_bound_truncates_instead_of_failing_once_a_transaction_commits(self):
        chunk = [
            {"op": "BEGIN", "tx_id": 1, "lsn": 100},
            {"op": "COMMIT", "tx_id": 1, "lsn": 101},
            {"op": "BEGIN", "tx_id": 2, "lsn": 102},
            {"op": "INSERT", "tx_id": 2, "lsn": 103},
        ]
        transport = FakeCdcTransport([chunk])
        bridge = object.__new__(PurePythonInformixBridge)
        # Sized to trip after the first transaction commits but before the
        # second one can finish, which is the interleaving that matters here.
        bridge.options = {"cdc.max.poll.bytes": "1000"}
        bridge.transport = transport
        capture = {
            "identity": "demo:app.orders",
            "logical_identity": "demo.app.orders",
            "columns": ["id"],
            "descriptors": [{"name": "id", "type_name": "INTEGER"}],
        }

        with (
            mock.patch.object(informix_module, "CdcFrameParser", RecordParser),
            mock.patch.object(
                informix_module, "decode_frame", side_effect=lambda frame, labels: dict(frame)
            ),
        ):
            records = bridge.read_changes([capture], 90, 1, 2)

        self.assertEqual([tx.tx_id for tx in informix_module._committed_transactions(records)], [1])

    def test_truncated_poll_tolerates_a_partial_trailing_frame(self):
        # Abandoning the rest of a chunk deliberately leaves the parser holding
        # a partial frame; that is expected, not the corruption the incomplete
        # frame guard exists to catch.
        class BufferingParser(RecordParser):
            def feed(self, chunk):
                yield from chunk
                self.buffered_bytes = 7

        chunk = [
            {"op": "BEGIN", "tx_id": 1, "lsn": 100},
            {"op": "COMMIT", "tx_id": 1, "lsn": 101},
            {"op": "BEGIN", "tx_id": 2, "lsn": 102},
            {"op": "INSERT", "tx_id": 2, "lsn": 103},
        ]
        transport = FakeCdcTransport([chunk])
        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {"cdc.max.poll.records": "3"}
        bridge.transport = transport
        capture = {
            "identity": "demo:app.orders",
            "logical_identity": "demo.app.orders",
            "columns": ["id"],
            "descriptors": [{"name": "id", "type_name": "INTEGER"}],
        }

        with (
            mock.patch.object(informix_module, "CdcFrameParser", BufferingParser),
            mock.patch.object(
                informix_module, "decode_frame", side_effect=lambda frame, labels: dict(frame)
            ),
        ):
            records = bridge.read_changes([capture], 90, 1, 2)

        self.assertEqual([tx.tx_id for tx in informix_module._committed_transactions(records)], [1])

    def test_poll_bound_still_fails_when_a_single_transaction_cannot_fit(self):
        # With nothing committed there is no progress to checkpoint, so silently
        # returning empty would stall the flow just as permanently. Surface it.
        chunk = [
            {"op": "BEGIN", "tx_id": 1, "lsn": 100},
            {"op": "INSERT", "tx_id": 1, "lsn": 101},
            {"op": "INSERT", "tx_id": 1, "lsn": 102},
            {"op": "INSERT", "tx_id": 1, "lsn": 103},
        ]
        transport = FakeCdcTransport([chunk])
        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {"cdc.max.poll.records": "2"}
        bridge.transport = transport
        capture = {
            "identity": "demo:app.orders",
            "logical_identity": "demo.app.orders",
            "columns": ["id"],
            "descriptors": [{"name": "id", "type_name": "INTEGER"}],
        }

        with (
            mock.patch.object(informix_module, "CdcFrameParser", RecordParser),
            mock.patch.object(
                informix_module, "decode_frame", side_effect=lambda frame, labels: dict(frame)
            ),
            self.assertRaisesRegex(
                InformixError, "cdc.max.poll.records=2 before any transaction completed"
            ),
        ):
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

        with self.assertRaisesRegex(InformixError, r"partially applied.*demo:app.orders"):
            bridge.prepare_initial_capture(["demo:app.orders", "demo:app.customers"])

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
        self.assertTrue(any("cdc_opensess(?, 0, 1, 1, 1, 1)" in sql for sql in transport.sql))

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
            bridge.validate_initial_lsn({"identity": "demo:app.orders", "columns": ["id"]}, 80)

        self.assertIn("validation cleanup also failed", " ".join(caught.exception.__notes__))

    def test_initial_lsn_validation_survives_teardown_timeout(self):
        # Primary (start+activate) succeeds; only cdc_endcapture teardown times out.
        # That is best-effort cleanup, so validation must not fail -- it drops the
        # transport (reset) and returns, rather than wedging the flow (tw101).
        class TeardownTimeoutTransport:
            def __init__(self):
                self.socket_timeout = 30.0

            def set_socket_timeout(self, timeout):
                self.socket_timeout = timeout

            def execute(self, sql, parameters=()):
                if "sysenv" in sql:
                    return [{"env_value": "demo_server"}]
                if "cdc_opensess" in sql:
                    return [{"session_id": 7}]
                if "cdc_startcapture" in sql or "cdc_activatesess" in sql:
                    return [{"status": 0}]
                if "cdc_endcapture" in sql:
                    raise TimeoutError("The read operation timed out")
                return [{"status": 0}]

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.transport = TeardownTimeoutTransport()
        bridge.options = {}
        reset_calls = []
        bridge.reset_transport = lambda: reset_calls.append(True)

        # Returns normally (no raise) despite the teardown timeout.
        bridge.validate_initial_lsn({"identity": "demo:app.orders", "columns": ["id"]}, 80)
        self.assertEqual(reset_calls, [True])

    def test_initial_lsn_validation_still_fails_on_non_timeout_cleanup_error(self):
        # A non-timeout cleanup failure with a successful primary is still fatal:
        # only a *timeout* teardown is treated as benign.
        class BadCleanupTransport:
            def execute(self, sql, parameters=()):
                if "sysenv" in sql:
                    return [{"env_value": "demo_server"}]
                if "cdc_opensess" in sql:
                    return [{"session_id": 7}]
                if "cdc_startcapture" in sql or "cdc_activatesess" in sql:
                    return [{"status": 0}]
                if "cdc_endcapture" in sql:
                    return [{"status": -1}]  # non-timeout failure
                return [{"status": 0}]

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.transport = BadCleanupTransport()
        bridge.options = {}
        bridge.reset_transport = lambda: None

        with self.assertRaisesRegex(InformixError, "cleanup failed"):
            bridge.validate_initial_lsn({"identity": "demo:app.orders", "columns": ["id"]}, 80)

    def test_initial_lsn_validation_extends_and_restores_socket_timeout(self):
        class TimedTransport:
            def __init__(self):
                self.socket_timeout = 30.0
                self.timeouts = []

            def set_socket_timeout(self, timeout):
                self.socket_timeout = timeout
                self.timeouts.append(timeout)

            def execute(self, sql, parameters=()):
                if "sysenv" in sql:
                    return [{"env_value": "demo_server"}]
                if "cdc_opensess" in sql:
                    return [{"session_id": 7}]
                return [{"status": 0}]

        transport = TimedTransport()
        bridge = object.__new__(PurePythonInformixBridge)
        bridge.transport = transport
        bridge.options = {}

        bridge.validate_initial_lsn({"identity": "demo:app.orders", "columns": ["id"]}, 80)

        # Raised to the 60s CDC default for the control-plane calls, then restored.
        self.assertEqual(transport.timeouts, [60.0, 30.0])

    def test_initial_lsn_validation_honors_cdc_read_timeout_option(self):
        class TimedTransport:
            def __init__(self):
                self.socket_timeout = 30.0
                self.timeouts = []

            def set_socket_timeout(self, timeout):
                self.socket_timeout = timeout
                self.timeouts.append(timeout)

            def execute(self, sql, parameters=()):
                if "sysenv" in sql:
                    return [{"env_value": "demo_server"}]
                if "cdc_opensess" in sql:
                    return [{"session_id": 7}]
                return [{"status": 0}]

        transport = TimedTransport()
        bridge = object.__new__(PurePythonInformixBridge)
        bridge.transport = transport
        bridge.options = {"cdc.read.timeout.seconds": "180"}

        bridge.validate_initial_lsn({"identity": "demo:app.orders", "columns": ["id"]}, 80)

        self.assertEqual(transport.timeouts, [180.0, 30.0])

    def test_batch_size_defaults(self):
        self.assertEqual(_DEFAULT_SNAPSHOT_PAGE_SIZE, 20000)
        self.assertEqual(informix_module._DEFAULT_SNAPSHOT_READ_TIMEOUT_SECONDS, 300)
        self.assertEqual(informix_module._DEFAULT_CDC_READ_TIMEOUT_SECONDS, 60)
        self.assertEqual(_DEFAULT_MAX_RECORDS_PER_BATCH, 10000)
        self.assertEqual(informix_module._DEFAULT_CONNECTION_WAIT_TIMEOUT_SECONDS, 600)

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

    def test_cdc_stream_accepts_interleaved_lsn_order_and_sorts_by_commit(self):
        records = [
            {"op": "BEGIN", "tx_id": 1, "lsn": 10},
            {"op": "BEGIN", "tx_id": 2, "lsn": 20},
            {"op": "INSERT", "tx_id": 2, "lsn": 40, "row": {"id": 2}},
            {"op": "INSERT", "tx_id": 1, "lsn": 30, "row": {"id": 1}},
            {"op": "COMMIT", "tx_id": 2, "lsn": 60},
            {"op": "COMMIT", "tx_id": 1, "lsn": 50},
        ]
        transactions = _committed_transactions(records)

        self.assertEqual([tx.tx_id for tx in transactions], [1, 2])
        self.assertEqual([tx.commit_lsn for tx in transactions], [50, 60])

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

    def test_table_metadata_accepts_locale_letter_identifiers(self):
        # Under the default en_US.819 (Latin-1) DB_LOCALE the identifier "letter"
        # class includes accented letters, so an accented owner/table/column name
        # is a valid undelimited identifier and must not be rejected.
        table = Table.parse(
            {
                "database": "demo",
                "owner": "café",
                "name": "señor",
                "columns": [
                    {"name": "größe", "type_name": "INTEGER", "nullable": False},
                    {"name": "value", "type_name": "VARCHAR", "length": 20},
                ],
                "primary_keys": ["größe"],
            },
            "demo",
        )
        self.assertEqual(table.owner, "café")
        self.assertEqual(table.name, "señor")
        self.assertEqual(table.columns[0].name, "größe")

    def test_table_metadata_still_rejects_sql_unsafe_identifiers(self):
        # Widening the identifier class must not admit injection-unsafe characters.
        for bad in ("a b", 'a"b', "a;b", "a-b"):
            with self.subTest(bad=bad), self.assertRaisesRegex(InformixError, "Unsafe"):
                Table.parse({**_table(), "name": bad}, "demo")

    def test_table_metadata_accepts_special_character_identifiers(self):
        # "." appears in "first.last" login owners and "+" in some names; both are
        # permitted and rendered as delimited identifiers in identity strings.
        table = Table.parse({**_table(), "owner": "jacqueline.clarke", "name": "orders"}, "demo")
        self.assertEqual(table.owner, "jacqueline.clarke")
        self.assertEqual(table.exposed_name, '"jacqueline.clarke".orders')
        self.assertEqual(table.identity, 'demo."jacqueline.clarke".orders')
        self.assertEqual(table.native_identity, 'demo:"jacqueline.clarke".orders')
        plus = Table.parse({**_table(), "owner": "HCF+gbc", "name": "orders"}, "demo")
        self.assertEqual(plus.exposed_name, '"HCF+gbc".orders')

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

        self.assertEqual(counts, {"list": 0, "get": 2})

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

    def test_snapshot_page_combines_filter_with_keyset_predicate(self):
        class SnapshotTransport:
            def __init__(self):
                self.sql = None

            def execute(self, sql, parameters=(), max_result_bytes=None):
                self.sql = sql
                self.parameters = parameters
                return []

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.transport = SnapshotTransport()

        bridge.snapshot_page(
            "demo.app.orders",
            ["id"],
            ["id"],
            [10],
            20,
            snapshot_filter="status = 'A'",
        )

        self.assertEqual(
            bridge.transport.sql,
            "SELECT FIRST 20 id FROM demo:app.orders "
            "WHERE (status = 'A') AND ((id > ?)) ORDER BY id",
        )
        self.assertEqual(bridge.transport.parameters, (10,))

    def test_snapshot_filter_rejects_statement_stacking_and_comments(self):
        for predicate in ("status = 'A'; DROP TABLE orders", "1=1 -- all", "/* all */ 1=1"):
            with (
                self.subTest(predicate=predicate),
                self.assertRaisesRegex(ValueError, "one SQL predicate"),
            ):
                informix_module._snapshot_filter({"snapshot.filter": predicate})

    def test_snapshot_bridge_supports_positional_pagination(self):
        class SnapshotTransport:
            def __init__(self):
                self.sql = None

            def execute(self, sql, parameters=(), max_result_bytes=None):
                self.sql = sql
                return [{"event_time": "13:03:36"}]

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.transport = SnapshotTransport()

        rows = bridge.snapshot_page(
            "demo.app.events", ["event_time"], ["event_time"], None, 2, skip=10000
        )

        self.assertEqual(rows, [{"event_time": "13:03:36"}])
        self.assertEqual(
            bridge.transport.sql,
            "SELECT SKIP 10000 FIRST 2 event_time FROM demo:app.events " "ORDER BY event_time",
        )

    def test_sql_identifier_delimits_only_when_needed(self):
        # Valid undelimited names (incl. accented under en_US.819) stay bare;
        # names with admitted-but-not-undelimited characters (e.g. "+") are
        # wrapped in double quotes, doubling any embedded quote.
        self.assertEqual(informix_module._sql_identifier("orders"), "orders")
        self.assertEqual(informix_module._sql_identifier("café"), "café")
        self.assertEqual(informix_module._sql_identifier("HCF+gbc"), '"HCF+gbc"')
        self.assertEqual(informix_module._sql_identifier('a"b'), '"a""b"')

    def test_identity_join_split_round_trips_special_characters(self):
        # A dotted "first.last" owner must survive the join/split round trip so
        # its "." is not mistaken for a component separator.
        for components in (
            ["demo", "app", "orders"],
            ["demo", "jacqueline.clarke", "orders"],
            ["demo", "a+b", "c.d"],
            ['weird"owner', "t"],
        ):
            joined = informix_module._join_identity(*components)
            self.assertEqual(informix_module._split_identity(joined), components)
        # Normal identifiers are unchanged (no gratuitous quoting).
        self.assertEqual(informix_module._join_identity("demo", "app", "orders"), "demo.app.orders")

    def test_snapshot_bridge_reads_dotted_owner_via_quoted_identity(self):
        class SnapshotTransport:
            def __init__(self):
                self.sql = None

            def execute(self, sql, parameters=(), max_result_bytes=None):
                self.sql = sql
                return []

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.transport = SnapshotTransport()

        bridge.snapshot_page('demo."jacqueline.clarke".orders', ["id"], ["id"], None, 5)

        self.assertEqual(
            bridge.transport.sql,
            'SELECT FIRST 5 id FROM demo:"jacqueline.clarke".orders ORDER BY id',
        )

    def test_snapshot_bridge_delimits_special_character_identifiers(self):
        class SnapshotTransport:
            def __init__(self):
                self.sql = None

            def execute(self, sql, parameters=(), max_result_bytes=None):
                self.sql = sql
                return []

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.transport = SnapshotTransport()

        bridge.snapshot_page("demo.HCF+gbc.a+b", ["event_time"], ["event_time"], None, 2)

        self.assertEqual(
            bridge.transport.sql,
            'SELECT FIRST 2 event_time FROM demo:"HCF+gbc"."a+b" ORDER BY event_time',
        )

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

        lsn, rows = bridge.consistent_snapshot("demo.app.orders", ["id"], ["id"], 10, 100, 1 << 20)

        self.assertEqual(lsn, (2 << 32) + (3 << 12))
        self.assertEqual(rows, [{"id": 1}])
        self.assertEqual(
            [sql for sql in bridge.transport.sql if sql.startswith("COMMAND:")][:2],
            ["COMMAND:SET ISOLATION TO REPEATABLE READ", "COMMAND:BEGIN WORK"],
        )
        self.assertEqual(bridge.transport.sql[-1], "COMMAND:COMMIT WORK")

    def test_incremental_chunk_captures_lsn_before_selecting_rows(self):
        # Incremental correctness rests on stamping snapshot rows with the LSN
        # captured *before* the chunk SELECT: a row read as-of that LSN is only
        # ever superseded by a concurrent or later change committing at a higher
        # LSN. If a refactor moved current_lsn() after the SELECT, the READ row
        # could win over a change it should have lost to, resurrecting stale or
        # deleted data. Pin the ordering so that regression can never land
        # silently -- both statements still execute, so only their order guards
        # the invariant.
        class OrderRecordingTransport:
            def __init__(self):
                self.sql = []

            def execute(self, sql, parameters=(), max_result_bytes=None):
                del parameters, max_result_bytes
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
        bridge.transport = OrderRecordingTransport()

        lsn, rows = bridge.snapshot_chunk("demo.app.orders", ["id"], ["id"], None, 10)

        self.assertEqual(lsn, (2 << 32) + (3 << 12))
        self.assertEqual(rows, [{"id": 1}])
        lsn_index = next(i for i, sql in enumerate(bridge.transport.sql) if "syslogs" in sql)
        select_index = next(
            i for i, sql in enumerate(bridge.transport.sql) if sql.startswith("SELECT FIRST")
        )
        self.assertLess(
            lsn_index,
            select_index,
            "chunk LSN must be captured before the chunk SELECT",
        )

    def test_production_bridge_streams_consistent_snapshot_pages_to_consumer(self):
        class TransactionalTransport:
            def __init__(self):
                self.queries = 0

            def execute(self, sql, parameters=(), max_result_bytes=None):
                del parameters, max_result_bytes
                if "sysmaster:sysdatabases" in sql:
                    return [{"is_ansi": 0}]
                if "sysmaster:syslogs" in sql:
                    return [{"uniqid": 2, "used": 3}]
                if sql.startswith("SELECT FIRST"):
                    self.queries += 1
                    return (
                        [{"id": 1}, {"id": 2}]
                        if self.queries == 1
                        else ([{"id": 2}] if self.queries == 2 else [])
                    )
                return []

            @staticmethod
            def execute_command(sql):
                del sql

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {"database": "demo"}
        bridge.config = {"database": "demo"}
        bridge.transport = TransactionalTransport()
        consumed = []

        lsn, rows = bridge.consistent_snapshot(
            "demo.app.orders",
            ["id"],
            ["id"],
            1,
            0,
            0,
            page_consumer=lambda boundary, index, page: consumed.append((boundary, index, page)),
        )

        self.assertEqual(rows, [])
        self.assertEqual(
            consumed,
            [
                (lsn, 0, [{"id": 1}]),
                (lsn, 1, [{"id": 2}]),
            ],
        )

    def test_keyless_consistent_snapshot_uses_one_unordered_streaming_cursor(self):
        class TransactionalTransport:
            def __init__(self):
                self.sql = []

            def execute(self, sql, parameters=(), max_result_bytes=None):
                del parameters, max_result_bytes
                self.sql.append(sql)
                if "sysmaster:sysdatabases" in sql:
                    return [{"is_ansi": 0}]
                if "sysmaster:syslogs" in sql:
                    return [{"uniqid": 2, "used": 3}]
                return []

            def execute_pages(self, sql, parameters, page_size, consumer):
                self.sql.append(sql)
                self.assertions = (parameters, page_size)
                consumer([{"id": 1}, {"id": 2}])
                consumer([{"id": 3}])

            def execute_command(self, sql):
                self.sql.append(f"COMMAND:{sql}")

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {"database": "demo"}
        bridge.config = {"database": "demo"}
        bridge.transport = TransactionalTransport()
        consumed = []

        lsn, rows = bridge.consistent_snapshot(
            "demo.app.orders",
            ["id"],
            [],
            2,
            100,
            1 << 20,
            page_consumer=lambda boundary, index, page: consumed.append((boundary, index, page)),
        )

        self.assertEqual(rows, [])
        self.assertEqual(bridge.transport.assertions, ((), 2))
        self.assertEqual(
            [sql for sql in bridge.transport.sql if sql.startswith("SELECT id FROM")],
            ["SELECT id FROM demo:app.orders"],
        )
        self.assertNotIn("ORDER BY", bridge.transport.sql[-2])
        self.assertEqual([page for _, _, page in consumed], [[{"id": 1}, {"id": 2}], [{"id": 3}]])

    def test_snapshot_stage_typed_values_round_trip(self):
        value = {
            "decimal": Decimal("12.3400"),
            "datetime": datetime(2026, 7, 27, 12, 34, 56, 123456),
            "date": date(2026, 7, 27),
            "binary": b"\x00\xff",
            "float": float("inf"),
            "tuple": (1, "value"),
        }

        decoded = informix_module._decode_snapshot_stage_value(
            informix_module._encode_snapshot_stage_value(value)
        )

        self.assertEqual(decoded, value)

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
            _, rows = bridge.consistent_snapshot("demo.app.orders", ["id"], ["id"], 10, 100, 0)

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
                if "syscolumns" in sql:
                    return [
                        {
                            "colname": "id",
                            "coltype": 2,
                            "collength": 4,
                            "colno": 1,
                            "tabid": 7,
                            "extended_id": 0,
                            "extended_name": None,
                            "extended_owner": None,
                            "owner": "app",
                            "tabname": "orders",
                        }
                    ]
                return []

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {"metadata.max.bytes": "1"}
        bridge.config = {"database": "demo"}
        bridge.transport = MetadataTransport()

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
        self.assertIn("t.tabtype = 'T'", bridge.transport.sql[0])
        self.assertIn("t.tabtype='T'", bridge.transport.sql[1])

    def test_live_catalog_requires_positive_table_incarnation(self):
        class CatalogTransport:
            def execute(self, sql, parameters=(), max_result_bytes=None):
                if "syscolumns" in sql:
                    return [{"colname": "id", "coltype": 2, "collength": 4, "colno": 1}]
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

    def test_money_schema_preserves_explicit_zero_scale(self):
        spark_type = _spark_type(Column(name="amount", type_name="MONEY", precision=12, scale=0))

        self.assertEqual(spark_type.precision, 12)
        self.assertEqual(spark_type.scale, 0)

    def test_discovery_filter_schema_and_metadata(self):
        connector = self.connector(table_include_list="ignored")
        connector.options["table.include.list"] = "app.*"
        connector.options["table.exclude.list"] = "*.audit"
        self.assertEqual(connector.list_tables(), ["app.orders"])
        schema = connector.get_table_schema("app.orders", {})
        self.assertEqual(
            [field.name for field in schema.fields][-4:],
            [CURSOR, "_informix_commit_lsn", "_informix_tx_id", "_informix_op"],
        )
        self.assertEqual(
            connector.read_table_metadata("app.orders", {}),
            {
                "primary_keys": ["id"],
                "cursor_field": CURSOR,
                "ingestion_type": "cdc_with_deletes",
            },
        )

    def _limit_name(self, capacity, reservation):
        width = informix_module._CONNECTION_LIMIT_WIDTH
        version = informix_module._CONNECTION_LIMIT_CONFIG_VERSION
        return f"limit-v{version}-c{capacity:0{width}d}-r{reservation:0{width}d}"

    def test_connection_slot_tree_cleanup_unlinks_root_symlink(self):
        with tempfile.TemporaryDirectory() as root:
            target = os.path.join(root, "target")
            link = os.path.join(root, "expired-" + "a" * 32)
            os.mkdir(target)
            protected = os.path.join(target, "protected")
            open(protected, "wb").close()
            os.symlink(target, link)

            PurePythonInformixBridge._remove_connection_slot_tree(link)

            self.assertFalse(os.path.lexists(link))
            self.assertTrue(os.path.isfile(protected))

    @staticmethod
    def _channel_bridge(**options):
        bridge = object.__new__(PurePythonInformixBridge)
        bridge._connection_slot = None
        bridge._connection_slot_token = None
        bridge._connection_slot_heartbeat_stop = None
        bridge._connection_slot_heartbeat = None
        bridge.options = dict(options)
        return bridge

    def test_delete_channel_is_declared_before_anything_can_connect(self):
        # The slot is taken by the first call reaching _ensure_connected, which
        # is the schema refresh rather than read_changes. Declaring the channel
        # later would let that first acquisition land outside the delete band.
        connector = self.connector()
        observed: list[str | None] = []

        def record(*args, **kwargs):
            observed.append(connector.options.get("_informix.connection.channel"))
            raise InformixError("stop after observing the channel")

        with mock.patch.object(connector, "_read_table_deletes", side_effect=record):
            with self.assertRaisesRegex(InformixError, "stop after observing"):
                connector.read_table_deletes("orders", _stream_offset(), {})

        self.assertEqual(observed, ["delete"])
        # And it must not leak past the read, or a later upsert read on the same
        # reader would inherit the delete channel's restricted band.
        self.assertIsNone(connector.options.get("_informix.connection.channel"))

    def _occupied_slot(self, root, name="slot-0000", renewed_at=None, markers=2):
        path = os.path.join(root, name)
        os.mkdir(path)
        written = []
        for index in range(markers):
            prefix = "owner" if index == 0 else "pulse"
            marker = os.path.join(path, f"{prefix}-" + f"{index:x}" * 32)
            open(marker, "wb").close()
            written.append(marker)
        if renewed_at is not None:
            for marker in written:
                os.utime(marker, (renewed_at, renewed_at))
            os.utime(path, (renewed_at, renewed_at))
        return path

    def test_continuous_reads_yield_and_count_consecutive_capacity_misses(self):
        bridge = FakeBridge()
        bridge.get_table = mock.Mock(
            side_effect=ConnectionCapacityUnavailable("capacity exhausted")
        )
        connector = self.connector(bridge)
        checkpoint = _stream_offset()

        with mock.patch.object(informix_module.time, "sleep"):
            rows, offset = connector.read_table("app.orders", checkpoint, {})
            self.assertEqual(list(rows), [])
            self.assertEqual(offset["capacity_retry_count"], 1)
            # A miss also raises the decaying acquisition-pressure signal.
            self.assertEqual(offset["capacity_pressure"], 1)
            # The offset must CHANGE or the framework treats the stream as
            # exhausted and stops calling the reader.
            self.assertNotEqual(offset, checkpoint)

            delete_rows, delete_offset = connector.read_table_deletes("app.orders", checkpoint, {})
            self.assertEqual(list(delete_rows), [])
            self.assertEqual(delete_offset["capacity_retry_count"], 1)

            # Misses accumulate across successive starved reads.
            _, second = connector.read_table("app.orders", offset, {})
            self.assertEqual(second["capacity_retry_count"], 2)
            self.assertEqual(second["capacity_pressure"], 2)

        self.assertEqual(bridge.released_connections, 3)

    def test_acquiring_a_slot_resets_the_consecutive_miss_count(self):
        # The count measures a CURRENT run of misses. A read that gets a slot must
        # clear it (it is the fail-loud guard), so the offset carries no
        # consecutive-miss count after a served read.
        connector = self.connector()
        starved = dict(_stream_offset())
        starved["capacity_retry_count"] = 7

        rows, offset = connector.read_table("app.orders", starved, {})
        list(rows)
        self.assertNotIn("capacity_retry_count", offset)

    def test_acquiring_a_slot_decays_pressure_rather_than_resetting_it(self):
        # Pressure feeds the acquisition budget and, unlike the consecutive-miss
        # guard, DECAYS (halves) on a served read rather than resetting -- so a
        # contended-but-served flow keeps some earned patience instead of
        # snapping back to the base budget and likely re-missing immediately.
        connector = self.connector()
        contended = dict(_stream_offset())
        contended["capacity_pressure"] = 8

        rows, offset = connector.read_table("app.orders", contended, {})
        list(rows)
        self.assertEqual(offset["capacity_pressure"], 4)

        # It decays toward zero over successive served reads, and the key is
        # dropped entirely once it reaches zero so a recovered flow carries no
        # residual state.
        low = dict(_stream_offset())
        low["capacity_pressure"] = 1
        _, recovered = connector.read_table("app.orders", low, {})
        self.assertNotIn("capacity_pressure", recovered)

    def _write_hint_generation(self, bridge, root, bucket, lsn=1):
        generation = bridge._connection_hint_generation(root, bucket)
        os.makedirs(generation, exist_ok=True)
        width = informix_module._LSN_DECIMAL_WIDTH
        name = f"{informix_module._CONNECTION_HINT_LSN_PREFIX}{lsn:0{width}d}"
        open(os.path.join(generation, name), "wb").close()
        return generation

    def test_backlog_rank_is_bounded_and_monotonic(self):
        # Ranks must stay bounded (cheap to compare, safe to persist) and never
        # decrease as backlog grows, or the bias would be arbitrary.
        ranks = [informix_module._backlog_rank(0)]
        for shift in range(0, 64, 4):
            ranks.append(informix_module._backlog_rank(1 << shift))
        self.assertEqual(ranks, sorted(ranks))
        self.assertEqual(ranks[0], 0)
        self.assertLessEqual(max(ranks), informix_module._CONNECTION_BACKLOG_RANK_LEVELS - 1)
        self.assertEqual(informix_module._backlog_rank(-5), 0)

    def test_acquisition_budget_grows_with_backlog_rank(self):
        # A flow further behind presses harder for the same pressure, which is
        # what skews scarce slots toward the readers with the most to catch up.
        connector = self.connector()
        base = dict(_stream_offset())
        base["capacity_pressure"] = 2
        behind = dict(base)
        behind["backlog_rank"] = informix_module._CONNECTION_BACKLOG_RANK_LEVELS - 1
        # Budgets are jittered, so compare the ceilings the jitter draws from.
        with mock.patch.object(informix_module.random, "uniform", lambda low, high: high):
            self.assertGreater(
                connector._capacity_attempt_budget(behind),
                connector._capacity_attempt_budget(base),
            )

    def test_a_blocking_reader_still_publishes_its_backlog_rank(self):
        # Regression, measured in production: ranks appeared ONLY on steady-state
        # CDC offsets and streaks ONLY on blocking bulk-copy offsets, so the two
        # never met and the readers with the most outstanding work were never
        # prioritised. A blocking reader has no attempt budget by design, so the
        # rank must not be gated on one -- and its rank must be derived from the
        # streak, since only the capacity-miss path ever writes backlog_rank.
        connector = self.connector()
        # A mid-incremental-copy checkpoint: blocking (no budget), streak present,
        # backlog_rank absent -- exactly the production shape.
        start = dict(_stream_offset())
        start["incremental"] = {"done": False}
        start["backlog_streak"] = informix_module._CONNECTION_BACKLOG_TRUNCATION_LEVELS
        self.assertIsNone(
            connector._capacity_attempt_budget(start),
            "expected a blocking reader (no per-attempt budget)",
        )

        with connector._capacity_attempt(start):
            published = connector.options.get(informix_module._CONNECTION_ATTEMPT_RANK_OPTION)
            self.assertIsNotNone(
                published, "a blocking reader published no rank, so it cannot be prioritised"
            )
            self.assertEqual(
                int(published),
                informix_module._CONNECTION_BACKLOG_RANK_LEVELS - 1,
                "a saturated streak must publish the top rank",
            )
        # Restored afterwards so it cannot leak into an unrelated later read.
        self.assertNotIn(informix_module._CONNECTION_ATTEMPT_RANK_OPTION, connector.options)

    def test_a_ranked_waiter_sweeps_for_slots_more_often(self):
        # The lever for a blocking waiter is cadence, not budget: it never gives up,
        # so what decides whether it wins a freed slot is how soon it looks again.
        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        self.assertEqual(bridge._connection_sweep_rank_scale(), 1.0)

        top = informix_module._CONNECTION_BACKLOG_RANK_LEVELS - 1
        scales = []
        for rank in range(0, top + 1):
            bridge.options = {informix_module._CONNECTION_ATTEMPT_RANK_OPTION: str(rank)}
            scales.append(bridge._connection_sweep_rank_scale())

        # Monotonically tighter with rank, and bounded so the metadata rate the wide
        # sweep interval exists to control cannot run away.
        self.assertEqual(scales, sorted(scales, reverse=True))
        self.assertEqual(scales[0], 1.0)
        self.assertEqual(scales[-1], informix_module._SLOT_SWEEP_RANK_FLOOR)
        self.assertGreaterEqual(min(scales), informix_module._SLOT_SWEEP_RANK_FLOOR)

    def test_a_corrupt_published_rank_leaves_the_sweep_cadence_unchanged(self):
        # Acquisition is the hot path: a malformed rank must degrade to the
        # pre-rank cadence rather than raise inside slot acquisition.
        bridge = object.__new__(PurePythonInformixBridge)
        for bad in ("", "abc", "-3", "1.5", None):
            with self.subTest(bad=bad):
                bridge.options = (
                    {} if bad is None else {informix_module._CONNECTION_ATTEMPT_RANK_OPTION: bad}
                )
                self.assertEqual(bridge._connection_sweep_rank_scale(), 1.0)

    def test_missing_backlog_rank_leaves_the_budget_unchanged(self):
        # With no usable hint the budget must match the pre-hint behaviour exactly,
        # so a deployment without hints is not silently re-tuned.
        connector = self.connector()
        offset = dict(_stream_offset())
        offset["capacity_pressure"] = 3
        with mock.patch.object(informix_module.random, "uniform", lambda low, high: high):
            without = connector._capacity_attempt_budget(offset)
            ranked = dict(offset)
            ranked["backlog_rank"] = 0
            self.assertEqual(connector._capacity_attempt_budget(ranked), without)

    def test_a_real_read_that_truncates_records_the_streak_in_its_checkpoint(self):
        # End-to-end through read_table, not just the helper: a read capped by
        # max.records.per.batch must leave the streak in the durable checkpoint, and
        # the next read that drains the log must clear it. Removing the recording
        # from _read_stream leaves every helper-level test passing, so this is the
        # test that actually pins the wiring.
        bridge = FakeBridge()
        connector = self.connector(bridge, **{"max.records.per.batch": "1"})
        _, checkpoint = connector.read_table("app.orders", {}, {})

        # Two committed transactions, one row each, against a budget of one row.
        bridge.changes = [
            {"op": "BEGIN", "tx_id": 1, "lsn": 101},
            {"op": "INSERT", "tx_id": 1, "lsn": 102, "row": {"id": 91, "value": "a"}},
            {"op": "COMMIT", "tx_id": 1, "lsn": 103},
            {"op": "BEGIN", "tx_id": 2, "lsn": 104},
            {"op": "INSERT", "tx_id": 2, "lsn": 105, "row": {"id": 92, "value": "b"}},
            {"op": "COMMIT", "tx_id": 2, "lsn": 106},
            {"op": "TIMEOUT", "lsn": 107},
        ]

        rows, truncated = connector.read_table("app.orders", checkpoint, {})
        self.assertEqual(len(list(rows)), 1, "the row budget did not cap this read")
        self.assertEqual(
            truncated.get("backlog_streak"),
            1,
            "a read capped by its row budget recorded no streak",
        )

        # Draining the remainder must clear the streak: priority may not outlive
        # the backlog that earned it.
        bridge.changes = [{"op": "TIMEOUT", "lsn": 200}]
        _, drained = connector.read_table("app.orders", truncated, {})
        self.assertNotIn("backlog_streak", drained, "a drained read kept a stale backlog streak")

    def test_a_read_that_fills_its_row_budget_records_a_backlog_streak(self):
        # First-hand proof of backlog: a read that stops because it filled its row
        # budget has established that more changes are waiting for THIS table. The
        # published hint cannot establish that -- it is a global log position, so it
        # rises for a quiet table whenever other tables are written.
        end = informix_module._with_backlog_streak({"commit_lsn": "5"}, {}, True)
        self.assertEqual(end["backlog_streak"], 1)

        # Consecutive truncations accumulate, so a sustained backlog outranks one
        # busy read.
        second = informix_module._with_backlog_streak({"commit_lsn": "6"}, end, True)
        self.assertEqual(second["backlog_streak"], 2)

        # Draining the log clears it outright: priority must not outlive the
        # backlog that earned it.
        drained = informix_module._with_backlog_streak({"commit_lsn": "7"}, second, False)
        self.assertNotIn("backlog_streak", drained)

    def test_backlog_streak_saturates(self):
        # A permanently backlogged flow persists this on every batch, so it must not
        # grow without bound inside the offset.
        offset: dict = {}
        for _ in range(50):
            offset = informix_module._with_backlog_streak({"commit_lsn": "1"}, offset, True)
        self.assertEqual(
            offset["backlog_streak"],
            informix_module._CONNECTION_BACKLOG_TRUNCATION_LEVELS,
        )

    def test_a_proven_backlog_outranks_a_merely_stale_reader(self):
        # The discrimination this exists to provide, measured in production: every
        # waiter ranked identically off the global hint because all their checkpoints
        # were recent. A flow that truncates every read must rank above one that is
        # only stale, even when the stale reader's hint-derived estimate is larger.
        levels = informix_module._CONNECTION_BACKLOG_TRUNCATION_LEVELS
        stale_only = informix_module._backlog_rank(1 << 20, 0)
        proven = informix_module._backlog_rank(0, levels)
        self.assertGreater(
            proven,
            stale_only,
            "a reader with proven outstanding work must outrank a merely stale one",
        )
        self.assertEqual(proven, informix_module._CONNECTION_BACKLOG_RANK_LEVELS - 1)
        # The bands must not overlap: ANY truncating flow outranks EVERY
        # non-truncating one, whatever the hint estimate says. A maximum-based
        # combination could not guarantee this, which is why the space is banded.
        worst_estimate = max(
            informix_module._backlog_rank(1 << shift, 0) for shift in range(0, 64, 4)
        )
        self.assertGreater(informix_module._backlog_rank(0, 1), worst_estimate)

    def test_backlog_streak_discriminates_where_the_global_hint_cannot(self):
        # Two readers with IDENTICAL checkpoints and one shared hint -- the exact
        # production case that produced a single rank for every waiter. The streak
        # must separate them.
        backlog = 1 << 20
        quiet = informix_module._backlog_rank(backlog, 0)
        behind = informix_module._backlog_rank(backlog, 3)
        self.assertGreater(
            behind, quiet, "identical checkpoints must still rank differently by proven backlog"
        )

    def test_a_capacity_miss_preserves_a_backlog_streak(self):
        # A miss reads nothing, so it cannot have drained the log: the streak
        # describes the source and must survive an attempt that never connected.
        # Dropping it would let contention itself erase the evidence of backlog.
        connector = self.connector()
        connector.options.update({"hostname": "h", "user": "u", "password": "p", "server": "srv"})
        start = dict(_stream_offset())
        start["backlog_streak"] = 2

        offset = connector._capacity_retry_offset(start)

        self.assertEqual(offset.get("backlog_streak"), 2)
        self.assertEqual(offset.get("backlog_rank"), informix_module._backlog_rank(0, 2))

    def test_a_served_read_keeps_its_freshly_measured_streak(self):
        # _reset_retry_counts clears the waiting-related keys, but the streak is a
        # measurement of the source that the just-completed read produced. Popping
        # it would make a permanently backlogged flow look caught up on every read
        # that happened to win a slot.
        connector = self.connector()
        end = {"commit_lsn": "9", "backlog_streak": 3}
        start = {"capacity_retry_count": 4, "capacity_pressure": 4, "backlog_rank": 5}

        _, reset = connector._reset_retry_counts((iter(()), end), start)

        self.assertEqual(reset.get("backlog_streak"), 3, "a served read discarded its measurement")
        self.assertNotIn("backlog_rank", reset)
        self.assertNotIn("capacity_retry_count", reset)

    def test_backlog_streak_survives_offset_validation(self):
        # The streak rides in the durable offset, so validation must accept it and
        # reject a corrupt value rather than trusting it into the rank arithmetic.
        offset = dict(_stream_offset())
        offset["backlog_streak"] = 2
        self.assertEqual(informix_module._validated_offset(offset)["backlog_streak"], 2)
        for bad in (-1, "2", True, 1.5):
            with self.subTest(bad=bad):
                corrupt = dict(_stream_offset())
                corrupt["backlog_streak"] = bad
                with self.assertRaises(ValueError):
                    informix_module._validated_offset(corrupt)

    def test_a_streak_only_offset_is_recognised_as_a_retry_offset(self):
        # A capacity-miss offset carries only bookkeeping keys. If backlog_streak
        # were not among the recognised set, an offset holding just it would be
        # mistaken for a real checkpoint and fail validation for want of a
        # commit_lsn.
        self.assertEqual(
            InformixLakeflowConnect._effective_start_offset({"backlog_streak": 2}),
            {},
        )

    def test_backlog_hint_is_readable_without_a_bridge(self):
        # The waiter path must not depend on bridge state of any kind, since the
        # reader that needs it is by definition the one without a connection.
        options = {
            "hostname": "h",
            "database": "d",
            "user": "u",
            "password": "p",
            "server": "srv",
        }
        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = options
        with mock.patch.object(PurePythonInformixBridge, "current_lsn", return_value=555):
            bridge._publish_connection_backlog_hint()

        # Read via the classmethod with options only -- no instance involved.
        self.assertEqual(
            PurePythonInformixBridge._read_connection_backlog_hint_at(options),
            555,
        )

    def test_backlog_hint_never_regresses(self):
        # GREATEST is what lets every holder publish without electing one per
        # quantum, so a later smaller position must not overwrite a larger one.
        options = {
            "hostname": "h",
            "database": "d",
            "user": "u",
            "password": "p",
            "server": "srv",
        }
        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = options
        for position in (900, 400):
            with mock.patch.object(PurePythonInformixBridge, "current_lsn", return_value=position):
                bridge._publish_connection_backlog_hint()

        self.assertEqual(
            PurePythonInformixBridge._read_connection_backlog_hint_at(options),
            900,
        )

    def test_backlog_rank_is_dropped_once_a_read_is_served(self):
        # The rank is a per-attempt observation, not accumulated state: a read that
        # got a slot has spent whatever bias it earned while waiting.
        connector = self.connector()
        waited = dict(_stream_offset())
        waited["backlog_rank"] = 5
        rows, offset = connector.read_table("app.orders", waited, {})
        list(rows)
        self.assertNotIn("backlog_rank", offset)

    def test_acquisition_budget_grows_with_pressure(self):
        # A reader under more acquisition pressure presses hardest, which is what
        # makes yielding fair rather than merely non-blocking. The budget scales
        # with capacity_pressure (which decays on success) rather than the
        # consecutive-miss failure guard.
        connector = self.connector()
        fresh = connector._capacity_attempt_budget(_stream_offset())
        pressured = dict(_stream_offset())
        pressured["capacity_pressure"] = 20
        aged = connector._capacity_attempt_budget(pressured)
        self.assertIsNotNone(fresh)
        self.assertIsNotNone(aged)
        self.assertGreater(aged, fresh)

        # The consecutive-miss counter must NOT drive the budget: only pressure
        # does, so a reader with a stale retry count but zero pressure stays at
        # the base budget (drawn from uniform(base/2, base), hence <= base).
        retry_only = dict(_stream_offset())
        retry_only["capacity_retry_count"] = 20
        self.assertLessEqual(
            connector._capacity_attempt_budget(retry_only),
            informix_module._CAPACITY_ATTEMPT_BASE_SECONDS,
        )

        # Bounded, so a long-lived pressured reader cannot block indefinitely.
        pinned = dict(_stream_offset())
        pinned["capacity_pressure"] = 10_000
        self.assertLessEqual(
            connector._capacity_attempt_budget(pinned),
            informix_module._CAPACITY_ATTEMPT_MAX_SECONDS,
        )
        # Randomised, so equally-aged readers do not converge on one give-up
        # instant and re-storm the Volume together.
        samples = {connector._capacity_attempt_budget(pressured) for _ in range(20)}
        self.assertGreater(len(samples), 1)

    def test_acquisition_budget_is_published_to_the_bridge_during_a_read(self):
        connector = self.connector()
        observed: list[str | None] = []

        def record(*args, **kwargs):
            observed.append(connector.options.get("_informix.connection.attempt.budget.seconds"))
            raise InformixError("stop after observing the budget")

        with mock.patch.object(connector, "_table", side_effect=record):
            with self.assertRaisesRegex(InformixError, "stop after observing"):
                connector.read_table("app.orders", _stream_offset(), {})

        self.assertEqual(len(observed), 1)
        self.assertIsNotNone(observed[0])
        self.assertGreater(float(observed[0]), 0)
        # Must not leak past the read.
        self.assertNotIn("_informix.connection.attempt.budget.seconds", connector.options)

    def test_zero_capacity_retry_delay_blocks_fully_but_still_yields_for_continuous(self):
        # A continuous flow with delay=0 opts into the full connection.wait.timeout
        # acquisition wait (budget None = block fully), giving each read the
        # maximum chance to claim a slot. It STILL yields rather than fails when
        # the wait exhausts -- only a triggered flow raises. And with delay=0 there
        # is no inter-read sleep before the next attempt.
        bridge = FakeBridge()
        bridge.get_table = mock.Mock(
            side_effect=ConnectionCapacityUnavailable("capacity exhausted")
        )
        connector = self.connector(bridge, **{"capacity.retry.max.delay.seconds": "0"})

        with mock.patch.object(informix_module.time, "sleep") as sleep:
            rows, offset = connector.read_table("app.orders", _stream_offset(), {})
            self.assertEqual(list(rows), [])
            self.assertEqual(offset["capacity_retry_count"], 1)
            delete_rows, delete_offset = connector.read_table_deletes(
                "app.orders", _stream_offset(), {}
            )
            self.assertEqual(list(delete_rows), [])
            self.assertEqual(delete_offset["capacity_retry_count"], 1)
        # No inter-read delay at 0.
        for call in sleep.call_args_list:
            self.assertEqual(call.args[0], 0)
        # Block-fully: no short acquisition budget is published, so the reader
        # waits out the full connection.wait.timeout.seconds before yielding.
        self.assertIsNone(connector._capacity_attempt_budget(_stream_offset()))

    def test_triggered_flow_blocks_fully_regardless_of_capacity_retry_delay(self):
        # A triggered flow drains and terminates, so it must block for the full
        # connection.wait.timeout.seconds to actually acquire a slot -- it never
        # uses the continuous yield budget. capacity.retry.max.delay.seconds
        # governs only the continuous yield/backoff and is irrelevant here, so a
        # triggered flow gets a None (block-fully) budget for EVERY value of it.
        # Regression: a positive delay (including the default 5) wrongly handed
        # triggered flows a sub-second budget, so they gave up in <1s under
        # contention and failed the update instead of waiting for a slot.
        for delay in ("0", "5", "60"):
            with self.subTest(delay=delay):
                connector = self.connector(**{"capacity.retry.max.delay.seconds": delay})
                connector.prepare_for_trigger_available_now()
                self.assertIsNone(connector._capacity_attempt_budget(_stream_offset()))

    def test_default_triggered_flow_blocks_fully(self):
        # The default capacity.retry.max.delay.seconds (5) must not leak the
        # continuous yield budget into a triggered flow.
        connector = self.connector()
        connector.prepare_for_trigger_available_now()
        self.assertIsNone(connector._capacity_attempt_budget(_stream_offset()))

    def test_capacity_delay_ignored_for_non_stream_reads(self):
        # Snapshot reads (and, by default, the in-progress incremental snapshot
        # reader phase) are bulk, latency-tolerant work, so with a positive
        # capacity.retry.max.delay.seconds they should still block the full wait
        # (budget None) rather than spend the short stream-phase budget. A first
        # read (no offset), the blocking snapshot phase, and a default incremental
        # reader offset are all non-stream for capacity.
        connector = self.connector(**{"capacity.retry.max.delay.seconds": "5"})
        snapshot = dict(_stream_offset())
        snapshot["phase"] = "snapshot"
        for label, off in (
            ("first-read", {}),
            ("snapshot", snapshot),
            ("incremental", _incremental_reader_offset()),
        ):
            with self.subTest(phase=label):
                self.assertIsNone(connector._capacity_attempt_budget(off))
                self.assertEqual(connector._effective_capacity_retry_delay(off), 0.0)

    def test_incremental_snapshot_blocking_defaults_to_true(self):
        # The incremental reader phase (phase==stream WITH an incremental block)
        # blocks for a slot by default, like the consistent snapshot -- distinct
        # from steady-state stream, which uses the yield budget.
        connector = self.connector(**{"capacity.retry.max.delay.seconds": "5"})
        incremental = _incremental_reader_offset()
        self.assertIsNone(connector._capacity_attempt_budget(incremental))
        self.assertEqual(connector._effective_capacity_retry_delay(incremental), 0.0)
        # A pure stream read (no incremental block) still uses the budget.
        self.assertIsNotNone(connector._capacity_attempt_budget(_stream_offset()))

    def test_incremental_snapshot_blocking_false_uses_yield_budget(self):
        # With snapshot.incremental.blocking=false the incremental reader phase is
        # treated as stream: a positive delay gives it the bounded yield budget
        # and the configured inter-read backoff instead of blocking fully.
        connector = self.connector(
            **{
                "capacity.retry.max.delay.seconds": "5",
                "snapshot.incremental.blocking": "false",
            }
        )
        incremental = _incremental_reader_offset()
        self.assertEqual(connector._effective_capacity_retry_delay(incremental), 5.0)
        self.assertIsNotNone(connector._capacity_attempt_budget(incremental))

    def test_incremental_snapshot_blocking_triggered_always_blocks(self):
        # A triggered flow blocks fully regardless of snapshot.incremental.blocking.
        for blocking in ("true", "false"):
            with self.subTest(blocking=blocking):
                connector = self.connector(
                    **{
                        "capacity.retry.max.delay.seconds": "5",
                        "snapshot.incremental.blocking": blocking,
                    }
                )
                connector.prepare_for_trigger_available_now()
                self.assertIsNone(connector._capacity_attempt_budget(_incremental_reader_offset()))

    def test_incremental_blocking_survives_capacity_retry_round_trip(self):
        # The blocking classification depends on the incremental block being
        # present in the start offset. A starved read yields via
        # _capacity_retry_offset, whose (bumped) offset becomes the next read's
        # start offset -- so the incremental block must survive that round trip,
        # keeping the read blocking (budget None) across consecutive misses rather
        # than silently degrading to the stream yield budget.
        connector = self.connector(**{"capacity.retry.max.delay.seconds": "5"})
        offset = _incremental_reader_offset()
        with mock.patch.object(time, "sleep"):
            for _ in range(3):
                self.assertIsNone(connector._capacity_attempt_budget(offset))
                offset = connector._capacity_retry_offset(offset)
        # The block (and thus the blocking classification) is still there.
        self.assertIn("incremental", offset)
        self.assertIsNone(connector._capacity_attempt_budget(offset))

    def test_capacity_delay_used_for_stream_reads(self):
        # The steady-state stream phase uses the configured delay: a short bounded
        # budget (not None) and the configured inter-read backoff.
        connector = self.connector(**{"capacity.retry.max.delay.seconds": "5"})
        stream = _stream_offset()
        self.assertEqual(connector._effective_capacity_retry_delay(stream), 5.0)
        self.assertIsNotNone(connector._capacity_attempt_budget(stream))

    def test_replay_blocks_for_the_full_connection_wait(self):
        connector = self.connector(**{"capacity.retry.max.delay.seconds": "5"})
        stream = _stream_offset()

        self.assertIsNotNone(connector._capacity_attempt_budget(stream))
        self.assertIsNone(connector._capacity_attempt_budget(stream, replaying=True))

        with connector._capacity_attempt(stream, replaying=True):
            self.assertNotIn(
                informix_module._CONNECTION_ATTEMPT_BUDGET_OPTION,
                connector.options,
            )

    def test_zero_capacity_delay_blocks_fully_in_every_phase(self):
        # delay=0 already means "block fully in every phase", so the non-stream
        # override changes nothing there: every phase stays None / 0.
        connector = self.connector(**{"capacity.retry.max.delay.seconds": "0"})
        snapshot = dict(_stream_offset())
        snapshot["phase"] = "snapshot"
        for off in ({}, snapshot, _stream_offset()):
            self.assertIsNone(connector._capacity_attempt_budget(off))
            self.assertEqual(connector._effective_capacity_retry_delay(off), 0.0)

    def test_continuous_capacity_retries_are_bounded_and_then_fail(self):
        bridge = FakeBridge()
        bridge.get_table = mock.Mock(
            side_effect=ConnectionCapacityUnavailable("capacity exhausted")
        )
        connector = self.connector(bridge)
        exhausted = dict(_stream_offset())
        exhausted["capacity_retry_count"] = informix_module._CAPACITY_RETRY_MAX_RETRIES

        with (
            mock.patch.object(informix_module.time, "sleep"),
            self.assertRaisesRegex(ConnectionCapacityUnavailable, "continuous retry limit"),
        ):
            connector.read_table("app.orders", exhausted, {})

    def test_capacity_retry_max_retries_defaults_to_the_module_constant(self):
        self.assertEqual(
            self.connector()._capacity_retry_max_retries(),
            informix_module._CAPACITY_RETRY_MAX_RETRIES,
        )

    def test_capacity_retry_max_retries_is_configurable(self):
        # The consecutive-miss cap that ends yielding and fails is tunable; a
        # continuous reader raises once it has missed this many times in a row.
        bridge = FakeBridge()
        bridge.get_table = mock.Mock(
            side_effect=ConnectionCapacityUnavailable("capacity exhausted")
        )
        connector = self.connector(bridge, **{"capacity.retry.max.retries": "3"})
        self.assertEqual(connector._capacity_retry_max_retries(), 3)

        # Below the cap: yields and bumps the count rather than raising.
        below = dict(_stream_offset())
        below["capacity_retry_count"] = 2
        with mock.patch.object(informix_module.time, "sleep"):
            rows, offset = connector.read_table("app.orders", below, {})
        self.assertEqual(list(rows), [])
        self.assertEqual(offset["capacity_retry_count"], 3)

        # At the cap: fails loudly.
        at_cap = dict(_stream_offset())
        at_cap["capacity_retry_count"] = 3
        with (
            mock.patch.object(informix_module.time, "sleep"),
            self.assertRaisesRegex(ConnectionCapacityUnavailable, "continuous retry limit"),
        ):
            connector.read_table("app.orders", at_cap, {})

    def test_capacity_retry_max_retries_is_validated(self):
        for value in ("0", "-1"):
            with (
                self.subTest(value=value),
                self.assertRaisesRegex(ValueError, "capacity.retry.max.retries"),
            ):
                self.connector(
                    **{"capacity.retry.max.retries": value}
                )._capacity_retry_max_retries()

    def test_capacity_retry_delay_defaults_to_five_seconds(self):
        # Briefly 8.0 to cut how often starved readers touched the shared Volume,
        # whose FUSE mount disconnects with ENOTCONN under sustained write
        # frequency (Databricks ES-1604921 / ES-1614902). Slots are Postgres rows
        # now, which absorb that sweep rate without noticing, so the narrower
        # range is restored: a freed slot no longer sits idle up to 8s before a
        # waiter re-sweeps for it.
        self.assertEqual(informix_module._DEFAULT_CAPACITY_RETRY_MAX_DELAY_SECONDS, 5.0)
        self.assertEqual(self.connector()._capacity_retry_max_delay(), 5.0)

    def test_default_capacity_retry_delay_is_jittered_across_its_whole_range(self):
        # The post-yield wait is what spaces a starved reader's next attempt, so at
        # the default it must spread across seconds rather than clustering at the
        # floor: dozens of readers returning in lockstep would re-storm the
        # endpoint together on the next round.
        connector = self.connector()
        slept: list[float] = []
        with mock.patch.object(informix_module.time, "sleep", side_effect=slept.append):
            for _ in range(200):
                connector._capacity_retry_offset(_stream_offset())
        floor = informix_module._CAPACITY_RETRY_MIN_DELAY_SECONDS
        ceiling = informix_module._DEFAULT_CAPACITY_RETRY_MAX_DELAY_SECONDS
        self.assertGreaterEqual(min(slept), floor)
        self.assertLessEqual(max(slept), ceiling)
        # Spread wide rather than bunched near the floor, which is what decorrelates
        # equally-aged readers instead of releasing them together.
        self.assertGreater(max(slept), ceiling * 0.6)

    def test_the_post_yield_wait_never_draws_below_its_floor(self):
        """The floor bounds the burst tail: a near-zero draw re-sweeps immediately.

        Briefly 0.5 so a reader repeatedly drawing low could not hammer the Volume
        in tight succession, which is the write-frequency pattern its FUSE daemon
        fails under. Back to 0.1 now that slots are Postgres rows: the floor still
        bounds the tail, but need not also throttle it.
        """

        connector = self.connector()
        slept: list[float] = []
        with mock.patch.object(informix_module.time, "sleep", side_effect=slept.append):
            for _ in range(300):
                connector._capacity_retry_offset(_stream_offset())

        self.assertEqual(informix_module._CAPACITY_RETRY_MIN_DELAY_SECONDS, 0.1)
        self.assertGreaterEqual(min(slept), 0.1)

    def test_a_zero_configured_delay_still_means_no_spacing(self):
        """`0` documents "block the full wait, no inter-read spacing".

        The floor must not invert the jitter bounds and reintroduce a pause, or
        `capacity.retry.max.delay.seconds=0` would silently stop meaning what the
        README says it means.
        """

        connector = self.connector(**{"capacity.retry.max.delay.seconds": "0"})
        slept: list[float] = []
        with mock.patch.object(informix_module.time, "sleep", side_effect=slept.append):
            connector._capacity_retry_offset(_stream_offset())

        self.assertEqual(slept, [0.0])

    def test_available_now_capacity_exhaustion_still_fails_rather_than_yielding(self):
        bridge = FakeBridge()
        bridge.get_table = mock.Mock(
            side_effect=ConnectionCapacityUnavailable("capacity exhausted")
        )
        connector = self.connector(bridge)
        connector.prepare_for_trigger_available_now()
        checkpoint = _stream_offset()

        with self.assertRaises(ConnectionCapacityUnavailable):
            connector.read_table("app.orders", checkpoint, {})

    def test_state_directory_open_retries_transient_permission_errors(self):
        for error_number in (errno.EPERM, errno.EACCES):
            with self.subTest(error_number=error_number), tempfile.TemporaryDirectory() as root:
                real_open = os.open
                attempts = 0

                def transient_open(path, flags, *args, **kwargs):
                    nonlocal attempts
                    attempts += 1
                    if attempts < 3:
                        raise PermissionError(error_number, os.strerror(error_number), path)
                    return real_open(path, flags, *args, **kwargs)

                with (
                    mock.patch.object(informix_module.os, "open", side_effect=transient_open),
                    mock.patch.object(informix_module.time, "sleep"),
                ):
                    descriptor = informix_module._open_state_directory(root, root)
                os.close(descriptor)
                self.assertEqual(attempts, 3)

    def test_a_dropped_mount_from_any_helper_yields_a_retry(self):
        """ENOTCONN must be tolerated wherever it surfaces, not only from open(2).

        Production regression: after ENOTCONN was made retryable inside
        _open_state_entry_with_retry, the very next update failed with the same
        errno raised from _validate_state_path's os.lstat -- a different call site
        with no retry. The Volume is touched from many helpers, so the read
        boundary classifies the failure instead of each site handling it.
        """

        bridge = FakeBridge()
        connector = self.connector(bridge)
        checkpoint = _stream_offset()

        # Now that state lives in Postgres, the Volume is reached only through
        # staged snapshot pages, so that is where a dropped mount surfaces. The
        # invariant under test is unchanged: ENOTCONN from *any* Volume call must
        # be classified at the read boundary rather than by each call site.
        with (
            mock.patch.object(
                bridge,
                "read_changes",
                side_effect=OSError(
                    errno.ENOTCONN, "Transport endpoint is not connected", "/Volumes/x"
                ),
            ),
            mock.patch.object(informix_module.time, "sleep"),
        ):
            rows, offset = connector.read_table("app.orders", checkpoint, {})

        self.assertEqual(list(rows), [])
        # A dropped mount carries its own counter, not the shared-state one: the
        # two retry at different cadences and so need different caps.
        self.assertEqual(offset["dropped_mount_retry_count"], 1)
        self.assertNotIn("shared_state_retry_count", offset)

    def test_a_dropped_mount_on_the_delete_channel_also_yields(self):
        bridge = FakeBridge()
        connector = self.connector(bridge)
        checkpoint = _stream_offset()

        with (
            mock.patch.object(
                bridge,
                "read_changes",
                side_effect=OSError(errno.ENOTCONN, "Transport endpoint is not connected"),
            ),
            mock.patch.object(informix_module.time, "sleep"),
        ):
            rows, offset = connector.read_table_deletes("app.orders", checkpoint, {})

        self.assertEqual(list(rows), [])
        self.assertEqual(offset["dropped_mount_retry_count"], 1)

    def test_a_dropped_mount_fails_the_flow_once_the_retry_cap_is_reached(self):
        """A mount that never returns must fail rather than yield forever.

        Production regression: all 60 flows stalled for 13 minutes with the
        pipeline reporting RUNNING, committing zero-row Delta versions every ~25s
        and logging no ERROR or WARN anywhere in the event log. The only evidence
        was the retry count inside the offset. The dropped-mount yield rides out a
        remount, which is right, but unbounded it makes an infrastructure outage
        indistinguishable from an idle source.
        """

        bridge = FakeBridge()
        connector = self.connector(bridge, **{"dropped.mount.max.retries": "3"})
        checkpoint = _stream_offset()

        with (
            mock.patch.object(
                bridge,
                "read_changes",
                side_effect=OSError(errno.ENOTCONN, "Transport endpoint is not connected"),
            ),
            mock.patch.object(informix_module.time, "sleep"),
        ):
            offset = dict(checkpoint)
            # Below the cap the reader keeps yielding, exactly as before.
            for expected in (1, 2, 3):
                rows, offset = connector.read_table("app.orders", offset, {})
                self.assertEqual(list(rows), [])
                self.assertEqual(offset["dropped_mount_retry_count"], expected)

            # The read that would take the count past the cap fails instead.
            with self.assertRaises(InformixError) as caught:
                connector.read_table("app.orders", offset, {})

        message = str(caught.exception)
        self.assertIn("3 consecutive reads", message)
        self.assertIn("Volume", message)
        # The originating errno must stay reachable for diagnosis.
        self.assertEqual(
            informix_module._dropped_mount_error(caught.exception).errno, errno.ENOTCONN
        )

    def test_a_returning_mount_resets_the_dropped_mount_failure_guard(self):
        """A flapping mount must not accumulate toward the cap across recoveries.

        The counter guards a *consecutive* run, so any read that reaches the
        Volume has to clear it. Without this a mount that drops briefly once an
        hour would eventually fail a healthy pipeline.
        """

        bridge = FakeBridge()
        connector = self.connector(bridge, **{"dropped.mount.max.retries": "3"})
        checkpoint = _stream_offset()

        with (
            mock.patch.object(
                bridge,
                "read_changes",
                side_effect=OSError(errno.ENOTCONN, "Transport endpoint is not connected"),
            ),
            mock.patch.object(informix_module.time, "sleep"),
        ):
            rows, offset = connector.read_table("app.orders", checkpoint, {})
            self.assertEqual(list(rows), [])
            rows, offset = connector.read_table("app.orders", offset, {})
            self.assertEqual(offset["dropped_mount_retry_count"], 2)

        # The mount is back: this read reaches the Volume and must clear the guard.
        rows, recovered = connector.read_table("app.orders", offset, {})
        list(rows)
        self.assertNotIn("dropped_mount_retry_count", recovered)

    def test_a_dropped_mount_retry_count_survives_offset_validation(self):
        """The new key must round-trip, or the framework rejects the checkpoint."""

        offset = dict(_stream_offset())
        offset["dropped_mount_retry_count"] = 4
        self.assertEqual(informix_module._validated_offset(offset)["dropped_mount_retry_count"], 4)

        for invalid in (-1, True, "4", None):
            with self.subTest(invalid=invalid):
                broken = dict(_stream_offset())
                broken["dropped_mount_retry_count"] = invalid
                with self.assertRaises(ValueError):
                    informix_module._validated_offset(broken)

    def test_a_retry_only_dropped_mount_offset_is_not_mistaken_for_checkpoint_state(self):
        """A yield-only offset must still resolve to "no checkpoint yet".

        The framework may hand back an offset carrying nothing but retry
        bookkeeping. If dropped_mount_retry_count were not recognised as a retry
        field, _effective_start_offset would treat it as a real stream checkpoint
        and the reader would resume from a position that does not exist.
        """

        connector = self.connector(FakeBridge())
        self.assertEqual(connector._effective_start_offset({"dropped_mount_retry_count": 2}), {})

    def test_dropped_mount_max_retries_rejects_a_useless_bound(self):
        connector = self.connector(FakeBridge(), **{"dropped.mount.max.retries": "0"})
        with self.assertRaises(ValueError):
            connector._dropped_mount_max_retries()

    def test_an_unrelated_os_error_still_fails_the_read(self):
        """The boundary must not swallow genuine filesystem faults."""

        bridge = FakeBridge()
        connector = self.connector(bridge)
        checkpoint = _stream_offset()

        with (
            mock.patch.object(
                bridge,
                "read_changes",
                side_effect=OSError(errno.EIO, "I/O error"),
            ),
            self.assertRaises(OSError) as caught,
        ):
            connector.read_table("app.orders", checkpoint, {})

        self.assertEqual(caught.exception.errno, errno.EIO)

    def test_a_dropped_mount_is_found_through_a_wrapped_cause(self):
        """The errno can arrive wrapped by an intermediate raise ... from error."""

        wrapped = InformixError("shared state unreachable")
        wrapped.__cause__ = OSError(errno.ENOTCONN, "Transport endpoint is not connected")

        self.assertIsNotNone(informix_module._dropped_mount_error(wrapped))
        self.assertIsNone(informix_module._dropped_mount_error(OSError(errno.EIO, "I/O error")))

    def test_dropped_mount_classification_survives_a_cyclic_cause(self):
        """A self-referential cause chain must not hang the classifier."""

        first = InformixError("a")
        second = InformixError("b")
        first.__cause__ = second
        second.__cause__ = first

        self.assertIsNone(informix_module._dropped_mount_error(first))

    def test_state_directory_open_retries_a_dropped_volume_mount(self):
        """ENOTCONN means the FUSE mount vanished, not that this entry is denied.

        Observed in production: the Volume's mount dropped and 23 flows failed
        simultaneously, every one on the shared-state root, while the mount was
        healthy again moments later. Retrying rides out the blip instead of
        failing every flow in the update.
        """

        with tempfile.TemporaryDirectory() as root:
            real_open = os.open
            attempts = 0

            def dropped_mount(path, flags, *args, **kwargs):
                nonlocal attempts
                attempts += 1
                if attempts < 3:
                    raise OSError(errno.ENOTCONN, "Transport endpoint is not connected", path)
                return real_open(path, flags, *args, **kwargs)

            with (
                mock.patch.object(informix_module.os, "open", side_effect=dropped_mount),
                mock.patch.object(informix_module.time, "sleep"),
            ):
                descriptor = informix_module._open_state_directory(root, root)
            os.close(descriptor)
            self.assertEqual(attempts, 3)

    def test_dropped_mount_retry_does_not_stat_the_absent_filesystem(self):
        """The symlink check disambiguates EPERM only; it cannot stat a gone mount.

        Running it under ENOTCONN would consult a filesystem that is not there,
        and a failure to stat must not be mistaken for evidence about the path.
        """

        with (
            mock.patch.object(
                informix_module.os,
                "open",
                side_effect=OSError(errno.ENOTCONN, "Transport endpoint is not connected"),
            ),
            mock.patch.object(informix_module.os, "stat") as stat_call,
            mock.patch.object(informix_module.time, "sleep"),
            self.assertRaisesRegex(SharedStateAccessUnavailable, "temporarily inaccessible"),
        ):
            informix_module._open_state_directory("/tmp/state", "/tmp/state")
        stat_call.assert_not_called()

    def test_dropped_mount_backs_off_further_than_a_permission_blip(self):
        """A remount settles in seconds; a denied entry in milliseconds."""

        delays = []
        with (
            mock.patch.object(
                informix_module.os,
                "open",
                side_effect=OSError(errno.ENOTCONN, "Transport endpoint is not connected"),
            ),
            mock.patch.object(
                informix_module.time, "sleep", side_effect=lambda value: delays.append(value)
            ),
            self.assertRaises(SharedStateAccessUnavailable),
        ):
            informix_module._open_state_directory("/tmp/state", "/tmp/state")

        self.assertEqual(len(delays), informix_module._SHARED_STATE_OPEN_ATTEMPTS - 1)
        # Bounded by the mount ceiling, and materially slower than the 0.25s
        # ceiling the permission path uses.
        self.assertLessEqual(max(delays), informix_module._SHARED_STATE_MOUNT_RETRY_MAX_SECONDS)
        self.assertGreater(max(delays), 0.25)

    def test_state_directory_open_does_not_retry_unrelated_error(self):
        with (
            mock.patch.object(
                informix_module.os,
                "open",
                side_effect=OSError(errno.EIO, "I/O failure"),
            ) as opened,
            self.assertRaises(OSError),
        ):
            informix_module._open_state_directory("/tmp/state", "/tmp/state")
        opened.assert_called_once()

    def test_state_directory_permission_error_on_symlink_is_immediate(self):
        with tempfile.TemporaryDirectory() as root:
            target = os.path.join(root, "target")
            link = os.path.join(root, "link")
            os.mkdir(target)
            os.symlink(target, link)
            with (
                mock.patch.object(
                    informix_module.os,
                    "open",
                    side_effect=PermissionError(errno.EPERM, "denied"),
                ) as opened,
                mock.patch.object(informix_module.time, "sleep") as sleep,
                self.assertRaisesRegex(InformixError, "traverses symlink"),
            ):
                informix_module._open_state_directory(link, link)
            opened.assert_called_once()
            sleep.assert_not_called()

    def test_state_directory_open_exhaustion_is_transient_error(self):
        with (
            mock.patch.object(
                informix_module.os,
                "open",
                side_effect=PermissionError(errno.EPERM, "denied"),
            ),
            mock.patch.object(informix_module.time, "sleep") as sleep,
            self.assertRaisesRegex(SharedStateAccessUnavailable, "temporarily inaccessible"),
        ):
            informix_module._open_state_directory("/tmp/state", "/tmp/state")
        self.assertEqual(sleep.call_count, informix_module._SHARED_STATE_OPEN_ATTEMPTS - 1)

    def test_reads_checkpoint_transient_shared_state_failures(self):
        bridge = FakeBridge()
        bridge.get_table = mock.Mock(side_effect=SharedStateAccessUnavailable("volume unavailable"))
        connector = self.connector(bridge)
        checkpoint = _stream_offset()

        with mock.patch.object(informix_module.time, "sleep"):
            rows, upsert = connector.read_table("app.orders", checkpoint, {})
            deletes, delete = connector.read_table_deletes("app.orders", checkpoint, {})

        self.assertEqual(list(rows), [])
        self.assertEqual(list(deletes), [])
        self.assertEqual(upsert["shared_state_retry_count"], 1)
        self.assertEqual(delete["shared_state_retry_count"], 1)
        self.assertEqual(
            {key: value for key, value in upsert.items() if key != "shared_state_retry_count"},
            checkpoint,
        )

    def test_initial_shared_state_retry_offset_is_accepted_and_reset(self):
        connector = self.connector(FakeBridge())

        rows, end = connector.read_table("app.orders", {"shared_state_retry_count": 3}, {})

        self.assertEqual(len(list(rows)), 2)
        self.assertNotIn("shared_state_retry_count", end)

    def test_shared_state_checkpoint_retries_are_bounded_and_validated(self):
        connector = self.connector(FakeBridge())
        with self.assertRaisesRegex(ValueError, "shared_state_retry_count"):
            connector._effective_start_offset({"shared_state_retry_count": -1})
        checkpoint = {
            **_stream_offset(),
            "shared_state_retry_count": informix_module._SHARED_STATE_ACCESS_MAX_RETRIES,
        }
        with self.assertRaisesRegex(SharedStateAccessUnavailable, "retry limit"):
            connector._shared_state_retry_offset(checkpoint)

    def test_available_now_server_session_rejection_is_fatal(self):
        bridge = FakeBridge()
        bridge.get_table = mock.Mock(side_effect=SqliSessionRejected("session rejected"))
        connector = self.connector(bridge)
        connector.prepare_for_trigger_available_now()
        checkpoint = _stream_offset()

        with self.assertRaisesRegex(SqliSessionRejected, "rejected"):
            connector.read_table("app.orders", checkpoint, {})

    def test_qualified_source_table_maps_logical_to_owner_qualified_name(self):
        connector = self.connector()
        schema = connector.get_table_schema("orders", {"qualified_source_table": "app.orders"})
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
        self.assertEqual(offset["phase"], "snapshot")
        self.assertEqual(offset["snapshot"]["page_index"], 1)
        second, end = connector.read_table("app.orders", offset, {})
        self.assertEqual([row["id"] for row in second], [2])
        self.assertEqual(end["phase"], "stream")
        deletes, delete_offset = connector.read_table_deletes("app.orders", offset, {})
        self.assertEqual(list(deletes), [])
        self.assertEqual(delete_offset["commit_lsn"], str(bridge.now))
        self.assertEqual(bridge.released_connections, 3)

    def test_a_staged_snapshot_with_pages_remaining_records_a_backlog_streak(self):
        # The staged (blocking) snapshot knows its remaining page count exactly, so
        # a reader mid-snapshot has certain outstanding work and must rank above an
        # idle one while competing for connection slots. Serving the final page
        # clears the streak, since the backlog that earned the priority is gone.
        bridge = FakeBridge()
        connector = self.connector(bridge, **{"snapshot.page.size": "1"})

        _, first = connector.read_table("app.orders", {}, {})
        self.assertEqual(first["phase"], "snapshot", "expected pages to remain")
        self.assertGreaterEqual(
            first.get("backlog_streak", 0),
            1,
            "a staged snapshot with pages remaining recorded no backlog streak",
        )

        _, final = connector.read_table("app.orders", first, {})
        self.assertEqual(final["phase"], "stream")
        self.assertNotIn(
            "backlog_streak", final, "the final snapshot page kept a stale backlog streak"
        )

    def test_snapshot_pages_replay_exactly_after_source_changes(self):
        bridge = FakeBridge()
        connector = self.connector(
            bridge,
            registration_scope="durable-page-replay",
            **{"snapshot.page.size": "1"},
        )

        planned_rows, first_end = connector.read_table("app.orders", {}, {})
        self.assertEqual([row["value"] for row in planned_rows], ["a"])
        bridge.rows[0]["value"] = "changed-after-planning"
        bridge.rows.insert(0, {"id": 0, "value": "inserted-after-planning"})

        replayed_rows, replayed_end = connector.read_table("app.orders", {}, {})
        second_rows, stream_end = connector.read_table("app.orders", first_end, {})

        self.assertEqual([row["value"] for row in replayed_rows], ["a"])
        self.assertEqual(replayed_end, first_end)
        self.assertEqual([row["value"] for row in second_rows], ["b"])
        self.assertEqual(stream_end["phase"], "stream")

    def test_advanced_snapshot_start_deletes_only_consumed_pages(self):
        bridge = FakeBridge()
        connector = self.connector(
            bridge,
            registration_scope="consumed-page-cleanup",
            **{"snapshot.page.size": "1"},
        )

        _, first_end = connector.read_table("app.orders", {}, {})
        table = Table.parse(bridge.tables[0], "demo")
        stage = connector._snapshot_stage_namespace(
            table,
            connector._pipeline_scope(first_end),
            str(first_end["schema_id"]),
        )
        run = os.path.join(stage, "runs", str(first_end["snapshot_lsn"]))
        first_page = os.path.join(run, "page-00000000")
        current_page = os.path.join(run, "page-00000001")
        self.assertTrue(os.path.isdir(first_page))
        self.assertTrue(os.path.isdir(current_page))

        rows, stream_end = connector.read_table("app.orders", first_end, {})

        self.assertEqual([row["id"] for row in rows], [2])
        self.assertEqual(stream_end["phase"], "stream")
        self.assertFalse(os.path.lexists(first_page))
        self.assertTrue(os.path.isdir(current_page))
        # The manifest is metadata, so it is a state record rather than a
        # directory beside the pages it describes.
        self.assertIsNotNone(
            connector._read_snapshot_stage_manifest(
                table,
                connector._pipeline_scope(first_end),
                str(first_end["schema_id"]),
            )
        )

        replayed, replayed_end = connector.read_table("app.orders", first_end, {})
        self.assertEqual([row["id"] for row in replayed], [2])
        self.assertEqual(replayed_end, stream_end)

    def test_snapshot_offset_primary_key_bounds_are_json_serializable(self):
        bridge = FakeBridge()
        bridge.tables[0]["columns"][0].update({"type_name": "DECIMAL", "precision": 5, "scale": 2})
        bridge.rows[0]["id"] = Decimal("1.25")
        bridge.rows[1]["id"] = Decimal("2.50")
        connector = self.connector(
            bridge,
            registration_scope="json-safe-snapshot-pk",
            **{"snapshot.page.size": "1"},
        )

        _, checkpoint = connector.read_table("app.orders", {}, {})

        json.dumps(checkpoint)
        self.assertEqual(
            checkpoint["snapshot"]["last_pk"],
            [{"$informix": "decimal", "value": "1.25"}],
        )

    def test_completed_snapshot_stage_is_removed_after_stream_checkpoint(self):
        bridge = FakeBridge()
        connector = self.connector(
            bridge,
            registration_scope="snapshot-stage-cleanup",
            **{"snapshot.page.size": "1"},
        )
        _, first_end = connector.read_table("app.orders", {}, {})
        _, stream_end = connector.read_table("app.orders", first_end, {})
        table = Table.parse(bridge.tables[0], "demo")
        stage = connector._snapshot_stage_namespace(
            table,
            connector._pipeline_scope(stream_end),
            str(stream_end["schema_id"]),
        )
        self.assertTrue(os.path.isdir(stage))

        connector.read_table("app.orders", stream_end, {})

        self.assertFalse(os.path.exists(stage))

    def test_snapshot_staging_retention_days_is_configurable(self):
        connector = self.connector(**{"snapshot.staging.retention.days": "7"})

        self.assertEqual(connector._snapshot_staging_retention_seconds, 7 * 86400)

    def test_snapshot_staging_retention_days_must_be_positive(self):
        with self.assertRaisesRegex(ValueError, "snapshot.staging.retention.days.*>= 1"):
            self.connector(**{"snapshot.staging.retention.days": "0"})

    def test_snapshot_staging_location_can_be_configured_separately(self):
        with tempfile.TemporaryDirectory() as staging:
            connector = self.connector(
                registration_scope="separate-snapshot-staging",
                **{"snapshot.staging.location": staging},
            )
            rows, _ = connector.read_table("app.orders", {}, {})

            self.assertEqual(len(list(rows)), 2)
            self.assertTrue(os.listdir(staging))

    def test_snapshot_staging_location_rejects_relative_path(self):
        with self.assertRaisesRegex(ValueError, "snapshot.staging.location.*absolute"):
            self.connector(**{"snapshot.staging.location": "relative/staging"})

    def test_missing_lakebase_password_fails_at_construction(self):
        # State access has no fallback credential, so this must fail while the
        # pipeline is being set up rather than on the first microbatch that needs
        # state -- by which point the update is already running.
        with self.assertRaisesRegex(ValueError, "Missing required Informix option: lakebase"):
            self.connector(**{"lakebase.password": ""})

    def test_blank_lakebase_password_is_not_accepted_as_a_password(self):
        with self.assertRaisesRegex(ValueError, "Missing required Informix option: lakebase"):
            self.connector(**{"lakebase.password": "   "})

    def test_state_role_is_not_configurable_from_connection_options(self):
        # The role name is fixed: every pipeline sharing an endpoint must agree on
        # which role owns the state tables, so an option could only misconfigure it.
        connector = self.connector(**{"lakebase.user_id": "someone-else"})

        state = informix_module.LakebaseState(connector.options, "identity")

        self.assertEqual(state._user_id, "informix_state_user")

    def test_snapshot_mode_initial_only_stops_after_snapshot(self):
        bridge = FakeBridge()
        connector = self.connector(bridge)

        first_rows, checkpoint = connector.read_table(
            "app.orders", {}, {"snapshot.mode": "initial_only"}
        )
        later_rows, unchanged = connector.read_table(
            "app.orders", checkpoint, {"snapshot.mode": "initial_only"}
        )
        delete_rows, delete_offset = connector.read_table_deletes(
            "app.orders", {}, {"snapshot.mode": "initial_only"}
        )

        self.assertEqual([row["id"] for row in first_rows], [1, 2])
        self.assertEqual(list(later_rows), [])
        self.assertEqual(unchanged, checkpoint)
        self.assertEqual(list(delete_rows), [])
        self.assertEqual(delete_offset, {})

    def test_snapshot_mode_cdc_only_starts_at_current_lsn(self):
        bridge = FakeBridge()
        bridge.now = 125
        connector = self.connector(bridge)

        rows, checkpoint = connector.read_table("app.orders", {}, {"snapshot.mode": "cdc_only"})
        delete_rows, delete_checkpoint = self.connector(bridge).read_table_deletes(
            "app.orders", {}, {"snapshot.mode": "cdc_only"}
        )

        self.assertEqual(list(rows), [])
        self.assertEqual(checkpoint["phase"], "stream")
        self.assertEqual(checkpoint["commit_lsn"], "125")
        self.assertEqual(bridge.snapshot_calls, [])
        self.assertEqual(list(delete_rows), [])
        self.assertEqual(delete_checkpoint["commit_lsn"], "125")

    def test_snapshot_mode_auto_snapshot_resnapshots_expired_checkpoint(self):
        bridge = FakeBridge()
        bridge.minimum = 91
        bridge.now = 150
        checkpoint = _stream_offset(90)
        checkpoint["pipeline_scope"] = "f" * 32

        rows, recovered = self.connector(bridge).read_table(
            "app.orders",
            checkpoint,
            {"snapshot.mode": "auto_snapshot", "snapshot.page.size": "1"},
        )
        delete_rows, delete_recovered = self.connector(bridge).read_table_deletes(
            "app.orders", checkpoint, {"snapshot.mode": "auto_snapshot"}
        )

        self.assertEqual([row["id"] for row in rows], [1])
        self.assertEqual(recovered["commit_lsn"], "150")
        self.assertIn("incremental", recovered)
        self.assertFalse(recovered["incremental"]["done"])
        self.assertNotEqual(recovered["pipeline_scope"], checkpoint["pipeline_scope"])
        self.assertEqual(list(delete_rows), [])
        self.assertEqual(delete_recovered["commit_lsn"], "150")
        self.assertEqual(delete_recovered["pipeline_scope"], recovered["pipeline_scope"])

    def test_snapshot_mode_auto_snapshot_starts_incrementally_without_checkpoint(self):
        bridge = FakeBridge()

        rows, checkpoint = self.connector(bridge).read_table(
            "app.orders",
            {},
            {"snapshot.mode": "auto_snapshot", "snapshot.page.size": "1"},
        )

        self.assertEqual([row["id"] for row in rows], [1])
        self.assertEqual(checkpoint["phase"], "stream")
        self.assertIn("incremental", checkpoint)
        self.assertFalse(checkpoint["incremental"]["done"])

    def test_snapshot_mode_recovery_rebuilds_matching_schema_history(self):
        bridge = FakeBridge()
        checkpoint = _stream_offset()
        connector = self.connector(bridge)

        rows, recovered = connector.read_table(
            "app.orders", checkpoint, {"snapshot.mode": "recovery"}
        )

        self.assertEqual(list(rows), [])
        self.assertEqual(recovered, checkpoint)
        self.assertEqual(bridge.snapshot_calls, [])
        schema_node = connector._read_immutable_head(
            connector._immutable_namespace(
                Table.parse(_table(), "demo"), "schema-nodes", checkpoint["schema_id"]
            )
        )
        self.assertIsNotNone(schema_node)

    def _publish_schema_node(self, connector, table, *, start_lsn):
        """Publish a schema-node record the way an owning upsert reader does.

        Keyed on the *derived* node id, which is what the delete channel's
        bootstrap looks up -- distinct from the synthetic schema_id a test
        checkpoint carries.
        """

        schema_id = informix_module._schema_node_id(table)
        connector._publish_immutable_head(
            connector._immutable_namespace(table, "schema-nodes", schema_id),
            {
                "created_at": time.time(),
                "schema": {
                    "id": schema_id,
                    "fingerprint": informix_module._schema_fingerprint(table),
                    "start_lsn": str(start_lsn),
                    "table": informix_module._schema_state(table, start_lsn)["table"],
                },
            },
            record_type="schema-node",
        )
        return schema_id

    def test_a_future_schema_node_boundary_recovers_from_the_current_lsn(self):
        # Observed in production: the source's logical log was reinitialized, so
        # every stored schema-node boundary sat AHEAD of the server's current LSN
        # (uniqid 6 and 12 against a log reset to 0-4). Declining left 16 delete
        # channels permanently dead while all of them reported RUNNING, and the
        # record cannot be repaired -- the node id derives from identity and
        # fingerprint alone, so a full refresh recomputes the same id and the
        # write-once head keeps the stale value forever.
        #
        # Recover from the server's CURRENT position. Safe precisely here: the
        # boundary belongs to a previous log incarnation, so nothing in the current
        # log has been consumed by this channel and starting anywhere in it cannot
        # skip a readable delete.
        #
        # Deliberately not minimum_lsn, which this originally used: that is byte 0 of
        # the oldest surviving log -- the next one Informix recycles -- so resuming
        # there decodes whatever table has since reused the space. Observed in
        # production as a UnicodeDecodeError on 0xf0 inside a foreign row.
        bridge = FakeBridge()
        connector = self.connector(bridge)
        table = Table.parse(_table(), "demo")
        self._publish_schema_node(connector, table, start_lsn=90)

        # Reinitialize the log underneath it: current LSN drops far below the
        # recorded boundary, which is exactly what a source restart produced.
        bridge.now = 5
        bridge.minimum = 1

        with self.assertLogs(informix_module.__name__, level="WARNING") as logs:
            boundary, schema_id = connector._schema_node_delete_boundary(table)

        self.assertEqual(boundary, 5, "the delete channel must recover at current_lsn")
        self.assertNotEqual(
            boundary, bridge.minimum, "byte 0 of the oldest retained log is not a safe restart"
        )
        self.assertEqual(schema_id, informix_module._schema_node_id(table))
        message = "\n".join(logs.output)
        self.assertIn("logical log was reinitialized", message)
        # The operator must learn that pre-reset deletes are gone for good, since
        # recovering the channel does not recover that window.
        self.assertIn("cannot be replicated", message)
        self.assertIn(table.exposed_name, message)

    def test_a_checkpoint_from_a_reinitialized_log_fails_instead_of_stalling(self):
        # Observed in production: the source reinitialized its logical log, so a
        # resuming reader's checkpoint named a log file the server no longer has.
        # _read_stream validated only the LOWER bound, so the reader activated CDC
        # at an unreachable LSN, returned nothing on every poll, and re-returned the
        # same checkpoint forever -- reporting RUNNING while replicating no rows.
        # This is the resume-path twin of the bootstrap case: distinct code, same
        # silent stall, and it must fail loudly instead.
        for deletes in (False, True):
            with self.subTest(deletes=deletes):
                bridge = FakeBridge()
                connector = self.connector(bridge)
                # Checkpoint in log file 12; the source is back at file 4.
                checkpoint = dict(_stream_offset())
                restart = (12 << 32) + 4096
                for key in ("commit_lsn", "change_lsn", "begin_lsn"):
                    checkpoint[key] = str(restart)
                bridge.now = (4 << 32) + 8192
                bridge.minimum = 0

                read = connector.read_table_deletes if deletes else connector.read_table
                with self.assertRaises(informix_module.LogRetentionError) as caught:
                    rows, _ = read("app.orders", checkpoint, {})
                    list(rows)

                message = str(caught.exception)
                self.assertIn("reinitialized its logical log", message)
                self.assertIn("full refresh", message)

    def test_a_checkpoint_marginally_past_current_lsn_still_resumes(self):
        # current_lsn takes its byte offset from syslogs.used, a sampled page
        # count, so a transaction committed between that sample and this read
        # legitimately sits slightly beyond it. That benign race must not be
        # mistaken for a log reinitialization, which is why the guard compares log
        # file numbers rather than raw LSNs.
        bridge = FakeBridge()
        connector = self.connector(bridge)
        checkpoint = dict(_stream_offset())
        # Same log file as the server, but a few pages past the sampled offset.
        current = (4 << 32) + 4096
        restart = (4 << 32) + 8192
        for key in ("commit_lsn", "change_lsn", "begin_lsn"):
            checkpoint[key] = str(restart)
        bridge.now = current
        bridge.minimum = 0
        bridge.changes = [{"op": "TIMEOUT", "lsn": restart}]

        rows, end = connector.read_table("app.orders", checkpoint, {})

        self.assertEqual(list(rows), [])
        self.assertEqual(end["commit_lsn"], str(restart), "a benign race was rejected")

    def test_a_stale_schema_node_boundary_still_declines(self):
        # The retention case must NOT adopt minimum_lsn. Its boundary was once
        # valid, so deletes committed between it and minimum were readable and
        # advancing would silently skip them. Only the log-reset case is safe to
        # recover, so the two branches must stay distinct.
        bridge = FakeBridge()
        connector = self.connector(bridge)
        table = Table.parse(_table(), "demo")
        self._publish_schema_node(connector, table, start_lsn=10)

        # The boundary has aged out: minimum advanced past it, current is beyond.
        bridge.minimum = 50
        bridge.now = 200

        with self.assertLogs(informix_module.__name__, level="WARNING") as logs:
            boundary, schema_id = connector._schema_node_delete_boundary(table)

        self.assertIsNone(boundary, "an aged-out boundary must not be advanced")
        self.assertIsNone(schema_id)
        self.assertIn("precedes the minimum retained", "\n".join(logs.output))

    def test_a_usable_schema_node_boundary_warns_about_nothing(self):
        # The warning must fire only for the unrecoverable case; a healthy
        # bootstrap must stay silent so the signal keeps its meaning.
        bridge = FakeBridge()
        connector = self.connector(bridge)
        table = Table.parse(_table(), "demo")
        self._publish_schema_node(connector, table, start_lsn=90)

        with mock.patch.object(logging.getLogger(informix_module.__name__), "warning") as warned:
            boundary, schema_id = connector._schema_node_delete_boundary(table)

        self.assertIsNotNone(boundary, "a usable boundary was declined")
        self.assertIsNotNone(schema_id)
        warned.assert_not_called()

    def test_snapshot_mode_recovery_rejects_new_or_changed_schema(self):
        bridge = FakeBridge()
        connector = self.connector(bridge)
        with self.assertRaisesRegex(InformixError, "existing stream checkpoint"):
            connector.read_table("app.orders", {}, {"snapshot.mode": "recovery"})

        changed = _table()
        changed["columns"] = [
            *changed["columns"],
            {
                "name": "added",
                "type_name": "VARCHAR",
                "nullable": True,
                "cdc_supported": True,
            },
        ]
        bridge.tables[0] = changed
        with self.assertRaisesRegex(InformixError, "source schema differs"):
            self.connector(bridge).read_table(
                "app.orders", _stream_offset(), {"snapshot.mode": "recovery"}
            )

    def test_unchanged_schema_microbatch_trusts_checkpoint_without_state_read(self):
        connector = self.connector(FakeBridge())
        checkpoint = _stream_offset()

        with mock.patch.object(
            connector,
            "_read_immutable_head",
            side_effect=AssertionError("unchanged schema must use its checkpoint"),
        ):
            rows, upsert = connector.read_table("app.orders", checkpoint, {})
            deletes, delete = connector.read_table_deletes("app.orders", checkpoint, {})

        self.assertEqual(list(rows), [])
        self.assertEqual(list(deletes), [])
        self.assertEqual(upsert, checkpoint)
        self.assertEqual(delete, checkpoint)

    def test_snapshot_mode_validation_and_snapshot_only_restrictions(self):
        connector = self.connector(FakeBridge())
        for mode in ("configuration_based", "custom"):
            with self.subTest(mode=mode), self.assertRaisesRegex(ValueError, "not supported"):
                connector.read_table("app.orders", {}, {"snapshot.mode": mode})
        with self.assertRaisesRegex(ValueError, "Unsupported snapshot.mode"):
            connector.read_table("app.orders", {}, {"snapshot.mode": "invalid"})

        bridge = FakeBridge()
        bridge.tables = [_table(cdc=False)]
        for mode in ("cdc_only", "recovery"):
            with self.subTest(mode=mode), self.assertRaisesRegex(ValueError, "CDC-capable"):
                self.connector(bridge).read_table("app.orders", {}, {"snapshot.mode": mode})

    def test_consistent_snapshot_publishes_fresh_resume_lsn_to_both_readers(self):
        bridge = FakeBridge()

        def consistent_snapshot(*args, **kwargs):
            bridge.now = 150
            return 150, list(bridge.rows)

        bridge.consistent_snapshot = consistent_snapshot
        changes, upsert_offset = self.connector(bridge).read_table("app.orders", {}, {})
        delete_connector = self.connector(bridge)
        deletes, delete_offset = delete_connector.read_table_deletes("app.orders", {}, {})

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
        _, delete_a = self.connector(bridge_a, registration_scope="pipeline-a").read_table_deletes(
            "app.orders", {}, {}
        )
        _, delete_b = self.connector(bridge_b, registration_scope="pipeline-b").read_table_deletes(
            "app.orders", {}, {}
        )

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

    def test_delete_reader_keeps_connection_maintenance_cleanup_enabled(self):
        connector = self.connector(FakeBridge())

        def read_deletes(*args, **kwargs):
            self.assertNotEqual(
                connector.options.get("_informix.connection.cleanup.enabled"),
                "false",
            )
            return iter(()), {}

        with mock.patch.object(connector, "_read_table_deletes", side_effect=read_deletes):
            rows, offset = connector.read_table_deletes("app.orders", {}, {})

        self.assertEqual(list(rows), [])
        # Carries only the advisory bootstrap retry counter -- no position, so it
        # cannot move a checkpoint.
        self.assertIsNone(offset.get("phase"))
        self.assertEqual(
            set(offset) - {"schema_node_fallback_retry_count"}, set(), f"unexpected keys: {offset}"
        )

    def test_delete_reader_retries_without_blocking_until_upsert_publishes_boundary(self):
        delete_connector = self.connector(FakeBridge())
        rows, pending = delete_connector.read_table_deletes("app.orders", {}, {})

        self.assertEqual(list(rows), [])
        self.assertIsNone(pending.get("phase"), "a declining bootstrap yields no position")

        snapshot_bridge = FakeBridge()
        list(self.connector(snapshot_bridge).read_table("app.orders", {}, {})[0])
        _, initialized = delete_connector.read_table_deletes("app.orders", pending, {})

        self.assertEqual(initialized["commit_lsn"], "90")

    def test_delete_reader_waits_for_upsert_boundary_without_opening_informix(self):
        class NoConnectionBridge(FakeBridge):
            def get_table(self, identity):
                raise AssertionError("coordination wait acquired an Informix connection")

        connector = self.connector(NoConnectionBridge())

        rows, pending = connector.read_table_deletes("app.orders", {}, {})

        self.assertEqual(list(rows), [])
        self.assertEqual(pending, {"schema_node_fallback_retry_count": 1})

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
        ).read_table_deletes("app.orders", {}, {})

        self.assertEqual(replacement.prepared_identities, ["demo:app.orders"])
        self.assertEqual(delete_offset["commit_lsn"], "120")

    def test_connector_context_manager_releases_connection(self):
        bridge = FakeBridge()
        connector = self.connector(bridge)

        with connector:
            pass

        self.assertEqual(bridge.released_connections, 1)
        self.assertIsNone(connector._bridge_instance)

    def test_context_manager_preserves_primary_error_when_close_fails(self):
        bridge = FakeBridge()
        bridge.release_connection = mock.Mock(side_effect=RuntimeError("close failed"))
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
        self.assertEqual(transitioned["commit_lsn"], checkpoint["commit_lsn"])
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

    def test_pre_ddl_batch_uses_successor_projection_with_null_added_column(self):
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

        rows, end = self.connector(bridge).read_table("app.orders", checkpoint, {})

        self.assertNotIn("added", list(rows)[0])
        self.assertEqual(end["commit_lsn"], "102")
        self.assertNotEqual(end["schema_fingerprint"], previous_fingerprint)

    def test_post_ddl_transaction_replays_from_retained_checkpoint(self):
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

        rows, end = connector.read_table("app.orders", checkpoint, {})
        self.assertEqual(list(rows)[0]["added"], 9)
        self.assertEqual(end["commit_lsn"], "123")
        capture, start_lsn = bridge.change_reads[-1]
        self.assertEqual(capture[0]["columns"], ["id", "value", "added"])
        self.assertEqual(start_lsn, int(checkpoint["begin_lsn"]))

    def test_transaction_spanning_observed_schema_change_uses_successor_projection(self):
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

        rows, end = self.connector(bridge).read_table("app.orders", checkpoint, {})

        self.assertNotIn("added", list(rows)[0])
        self.assertEqual(end["commit_lsn"], "123")

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
        connector._trigger_boundaries["demo.app.orders"] = (
            110,
            "a" * 32,
            checkpoint["pipeline_scope"],
        )

        _, end = connector.read_table("app.orders", checkpoint, {})

        self.assertEqual(end["commit_lsn"], checkpoint["commit_lsn"])

    def test_available_now_keeps_boundary_across_full_schema_transition(self):
        bridge = FakeBridge()
        _, old_checkpoint = self.connector(bridge).read_table("app.orders", {}, {})
        bridge.tables[0]["columns"].append(
            {"name": "added", "type_name": "INTEGER", "nullable": True}
        )
        bridge.now = 120
        bridge.changes = [{"op": "TIMEOUT", "lsn": 120}]
        self.connector(bridge).read_table("app.orders", old_checkpoint, {})

        bridge.now = 150
        bridge.changes = [
            {"op": "BEGIN", "tx_id": 8, "lsn": 125},
            {
                "op": "INSERT",
                "tx_id": 8,
                "lsn": 126,
                "row": {"id": 4, "value": "d", "added": 9},
            },
            {"op": "COMMIT", "tx_id": 8, "lsn": 130},
            {"op": "TIMEOUT", "lsn": 150},
        ]
        connector = self.connector(bridge)
        connector.prepare_for_trigger_available_now()

        rows, transitioned = connector.read_table("app.orders", old_checkpoint, {})
        frozen = connector._trigger_boundaries["demo.app.orders"]
        self.assertEqual(frozen[0], 150)
        self.assertNotEqual(transitioned["schema_id"], old_checkpoint["schema_id"])
        self.assertEqual(list(rows)[0]["added"], 9)
        self.assertEqual(transitioned["trigger_generation"], frozen[1])

        bridge.now = 170
        rows, completed = connector.read_table("app.orders", transitioned, {})

        self.assertEqual(list(rows), [])
        self.assertEqual(connector._trigger_boundaries["demo.app.orders"], frozen)
        self.assertEqual(completed["trigger_generation"], frozen[1])

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
        _, checkpoint_b = self.connector(bridge).read_table("app.orders", checkpoint_a, {})
        bridge.tables[0]["columns"].append(
            {"name": "added_c", "type_name": "INTEGER", "nullable": True}
        )
        bridge.now = 140
        bridge.changes = [{"op": "TIMEOUT", "lsn": 140}]
        _, checkpoint_c = self.connector(bridge).read_table("app.orders", checkpoint_b, {})

        _, lagging_b = self.connector(bridge).read_table("app.orders", checkpoint_a, {})
        _, lagging_c = self.connector(bridge).read_table("app.orders", lagging_b, {})

        self.assertEqual(lagging_b["schema_fingerprint"], checkpoint_b["schema_fingerprint"])
        self.assertEqual(lagging_b["commit_lsn"], checkpoint_b["commit_lsn"])
        self.assertEqual(lagging_c["schema_fingerprint"], checkpoint_c["schema_fingerprint"])
        self.assertEqual(lagging_c["commit_lsn"], checkpoint_c["commit_lsn"])

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
            connector._immutable_namespace(table, "schemas", checkpoint["schema_id"]),
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
                connector._immutable_namespace(table, "schema-nodes", str(future["id"]))
            )
        )

    def test_retained_schema_transition_before_checkpoint_is_metadata_only(self):
        bridge = FakeBridge()
        connector = self.connector(bridge)
        _, checkpoint = connector.read_table("app.orders", {}, {})
        checkpoint["commit_lsn"] = "110"
        checkpoint["change_lsn"] = "110"
        checkpoint["begin_lsn"] = "110"
        bridge.tables[0]["columns"].append(
            {"name": "added", "type_name": "INTEGER", "nullable": True}
        )
        bridge.now = 120
        table = Table.parse(bridge.tables[0], "demo")
        successor = _schema_state(
            table,
            100,
            predecessor=checkpoint["schema_id"],
        )
        connector._publish_immutable_head(
            connector._immutable_namespace(table, "schemas", checkpoint["schema_id"]),
            {
                "created_at": informix_module.time.time(),
                "schema": successor,
            },
            record_type="schema-transition",
        )
        bridge.changes = [{"op": "TIMEOUT", "lsn": 120}]

        rows, transitioned = connector.read_table("app.orders", checkpoint, {})

        self.assertEqual(list(rows), [])
        self.assertEqual(transitioned["commit_lsn"], "110")
        self.assertEqual(transitioned["schema_id"], successor["id"])

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
            connector._immutable_namespace(current, "schemas", checkpoint["schema_id"]),
            {
                "created_at": informix_module.time.time(),
                "schema": _schema_state(conflicting, 120, predecessor=checkpoint["schema_id"]),
            },
            record_type="schema-transition",
        )

        with self.assertRaisesRegex(InformixError, "not an additive column change"):
            connector.read_table("app.orders", checkpoint, {})

    def test_schema_transition_uses_checkpoint_not_schema_node_observation_lsn(self):
        bridge = FakeBridge()
        connector = self.connector(bridge)
        table = Table.parse(_table(), "demo")
        checkpoint = _stream_offset(100)
        connector._publish_immutable_head(
            connector._immutable_namespace(table, "schema-nodes", checkpoint["schema_id"]),
            {
                "created_at": 1.0,
                "schema": _schema_state(table, 110, schema_id=checkpoint["schema_id"]),
                "scope": checkpoint["pipeline_scope"],
            },
            record_type="schema-node",
        )
        bridge.tables[0]["columns"].append(
            {"name": "added", "type_name": "INTEGER", "nullable": True}
        )
        bridge.now = 120

        rows, transitioned = connector.read_table("app.orders", checkpoint, {})

        self.assertEqual(list(rows), [])
        self.assertNotEqual(transitioned["schema_id"], checkpoint["schema_id"])
        self.assertEqual(transitioned["commit_lsn"], checkpoint["commit_lsn"])

    def test_incompatible_full_refresh_creates_independent_schema_generation(self):
        bridge = FakeBridge()
        _, old_checkpoint = self.connector(bridge).read_table("app.orders", {}, {})
        bridge.tables[0]["columns"][1]["type_name"] = "INTEGER"
        for index, row in enumerate(bridge.rows, start=1):
            row["value"] = index
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

    def test_full_refresh_of_evolved_layout_reuses_its_schema_node(self):
        bridge = FakeBridge()
        _, checkpoint_a = self.connector(bridge).read_table("app.orders", {}, {})
        bridge.tables[0]["columns"].append(
            {"name": "added", "type_name": "INTEGER", "nullable": True}
        )
        for row in bridge.rows:
            row["added"] = None
        bridge.now = 120
        bridge.changes = [{"op": "TIMEOUT", "lsn": 120}]
        _, checkpoint_b = self.connector(bridge).read_table("app.orders", checkpoint_a, {})

        _, refreshed_b = self.connector(bridge, registration_scope="evolved-refresh").read_table(
            "app.orders", {}, {}
        )

        self.assertEqual(checkpoint_b["commit_lsn"], checkpoint_a["commit_lsn"])
        self.assertEqual(refreshed_b["commit_lsn"], "120")
        self.assertEqual(refreshed_b["schema_fingerprint"], checkpoint_b["schema_fingerprint"])
        self.assertEqual(refreshed_b["schema_id"], checkpoint_b["schema_id"])

    def test_repeated_layout_reuses_its_schema_node(self):
        bridge = FakeBridge()
        original = json.loads(json.dumps(bridge.tables[0]))
        _, checkpoint_a1 = self.connector(bridge).read_table("app.orders", {}, {})
        bridge.tables[0]["columns"][1]["type_name"] = "INTEGER"
        for index, row in enumerate(bridge.rows, start=1):
            row["value"] = index
        bridge.now = 150
        _, checkpoint_d = self.connector(bridge, registration_scope="layout-d").read_table(
            "app.orders", {}, {}
        )
        bridge.tables[0] = original
        for row, value in zip(bridge.rows, ("a", "b"), strict=True):
            row["value"] = value
        bridge.now = 200

        _, checkpoint_a2 = self.connector(bridge, registration_scope="layout-a2").read_table(
            "app.orders", {}, {}
        )

        self.assertEqual(checkpoint_a2["schema_fingerprint"], checkpoint_a1["schema_fingerprint"])
        self.assertEqual(checkpoint_a2["schema_id"], checkpoint_a1["schema_id"])
        self.assertNotEqual(checkpoint_a2["schema_id"], checkpoint_d["schema_id"])
        self.assertEqual(checkpoint_a2["commit_lsn"], "200")

    def test_same_layout_new_table_incarnation_creates_new_generation(self):
        bridge = FakeBridge()
        bridge.tables[0]["incarnation"] = "101"
        _, first = self.connector(bridge).read_table("app.orders", {}, {})
        bridge.tables[0]["incarnation"] = "202"
        bridge.now = 200

        _, recreated = self.connector(bridge, registration_scope="incarnation-202").read_table(
            "app.orders", {}, {}
        )

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
        offset = {
            **_stream_offset(),
            "phase": "snapshot",
            "snapshot_lsn": "90",
            "snapshot": {"last_pk": [1], "page_index": 1},
        }
        del offset["schema_fingerprint"]

        with self.assertRaisesRegex(InformixError, "predates schema-safe offsets"):
            connector.read_table("app.orders", offset, {})
        with self.assertRaisesRegex(InformixError, "predates schema-safe offsets"):
            connector.read_table_deletes("app.orders", offset, {})

    def test_snapshot_rechecks_schema_after_page_query(self):
        bridge = FakeBridge()
        original = bridge.consistent_snapshot

        def changing_snapshot(*args, **kwargs):
            rows = original(*args, **kwargs)
            bridge.tables[0]["columns"].append(
                {"name": "added", "type_name": "INTEGER", "nullable": True}
            )
            return rows

        bridge.consistent_snapshot = changing_snapshot
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

    def test_initial_refresh_reclassifies_a_keyless_table_as_append_cdc(self):
        bridge = FakeBridge()
        original = bridge.get_table

        def refreshed_without_primary_key(identity):
            return {**original(identity), "primary_keys": []}

        bridge.get_table = refreshed_without_primary_key
        connector = self.connector(bridge, **{"snapshot.mode": "cdc_only"})

        rows, offset = connector.read_table("app.orders", {}, {})
        self.assertEqual(list(rows), [])
        self.assertEqual(offset["phase"], "stream")
        self.assertIsNotNone(offset["commit_lsn"])

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

    def test_stream_retries_in_place_after_connection_drop(self):
        # A connection dropped mid-poll (SqliProtocolError, e.g. SQ_LODATA -1)
        # must be recovered by resetting the transport and reissuing the same
        # read under the held slot -- not surfaced as a stream failure. Verify
        # the poll eventually returns its records and the transport was reset
        # once per failed attempt.
        bridge = FakeBridge()
        bridge.reset_calls = 0
        bridge.reset_transport = lambda: setattr(bridge, "reset_calls", bridge.reset_calls + 1)
        attempts = {"n": 0}
        committed_tx = [
            {"op": "BEGIN", "tx_id": 8, "lsn": 101},
            {"op": "INSERT", "tx_id": 8, "lsn": 102, "row": {"id": 4, "value": "d"}},
            {"op": "COMMIT", "tx_id": 8, "lsn": 105},
        ]

        def flaky_changes(tables, start_lsn, timeout_seconds, max_records):
            attempts["n"] += 1
            if attempts["n"] <= 2:
                raise SqliProtocolError("SQ_LODATA server/ISAM error -1")
            return list(committed_tx)

        bridge.read_changes = flaky_changes
        connector = self.connector(bridge)

        with mock.patch.object(time, "sleep"):
            rows, offset = connector.read_table("app.orders", _stream_offset(), {})

        self.assertEqual(attempts["n"], 3)  # two drops, third succeeds
        self.assertEqual(bridge.reset_calls, 2)  # one reset per failed attempt
        self.assertGreater(len(list(rows)), 0)
        self.assertIsNotNone(offset)

    def test_stream_raises_after_connection_drop_retries_exhausted(self):
        # A persistent drop must still fail rather than loop forever. After the
        # bounded attempts are exhausted the SqliProtocolError propagates.
        bridge = FakeBridge()
        bridge.reset_transport = lambda: None

        def always_drops(*args, **kwargs):
            raise SqliProtocolError("SQ_LODATA server/ISAM error -1")

        bridge.read_changes = always_drops
        connector = self.connector(bridge)

        with mock.patch.object(time, "sleep"):
            with self.assertRaisesRegex(SqliProtocolError, "SQ_LODATA"):
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

    def test_snapshot_only_applies_table_byte_limit(self):
        bridge = FakeBridge()
        bridge.tables = [_table(cdc=False)]
        connector = self.connector(bridge)

        rows, offset = connector.read_table("app.orders", {}, {"snapshot.max.bytes": "12345"})

        self.assertEqual(len(list(rows)), 2)
        self.assertIsNone(offset)
        self.assertEqual(bridge.snapshot_max_bytes, [12345])

    APPEND = {"append.only.ingestion": "true"}

    def _keyless_connector(self, **options):
        bridge = FakeBridge()
        bridge.tables = [_table(primary_keys=())]
        options.setdefault("snapshot.mode", "cdc_only")
        return self.connector(bridge, **options), bridge

    def test_a_keyless_table_defaults_to_append_with_a_cursor(self):
        connector, _ = self._keyless_connector()

        metadata = connector.read_table_metadata("app.orders", {})

        self.assertEqual(metadata["ingestion_type"], "append")
        self.assertEqual(metadata["primary_keys"], [])
        self.assertEqual(metadata["cursor_field"], informix_module.CURSOR)

    def test_a_keyless_table_explicitly_opts_into_bounded_snapshot(self):
        connector, _ = self._keyless_connector()
        options = {"append.only.ingestion": "false", "snapshot.mode": "initial"}

        metadata = connector.read_table_metadata("app.orders", options)
        rows, offset = connector.read_table("app.orders", {}, options)

        self.assertEqual(metadata["ingestion_type"], "snapshot")
        self.assertIsNone(metadata["cursor_field"])
        self.assertEqual(len(list(rows)), 2)
        self.assertIsNone(offset)

    def test_an_opted_in_keyless_table_reports_append_with_a_cursor(self):
        connector, _ = self._keyless_connector()

        metadata = connector.read_table_metadata("app.orders", dict(self.APPEND))

        self.assertEqual(metadata["ingestion_type"], "append")
        self.assertEqual(metadata["primary_keys"], [])
        # The cursor is what makes the framework stream incrementally instead of
        # re-reading the whole table into an append target every microbatch.
        self.assertEqual(metadata["cursor_field"], informix_module.CURSOR)

    def test_append_ingestion_can_be_opted_in_connection_wide(self):
        connector, _ = self._keyless_connector(**{"append.only.ingestion": "true"})

        metadata = connector.read_table_metadata("app.orders", {})

        self.assertEqual(metadata["ingestion_type"], "append")

    def test_auto_appends_a_keyless_table(self):
        connector, _ = self._keyless_connector(**{"append.only.ingestion": "auto"})

        metadata = connector.read_table_metadata("app.orders", {})

        self.assertEqual(metadata["ingestion_type"], "append")
        self.assertEqual(metadata["cursor_field"], informix_module.CURSOR)

    def test_auto_leaves_a_keyed_table_on_cdc(self):
        bridge = FakeBridge()  # default app.orders has a primary key
        connector = self.connector(bridge)

        metadata = connector.read_table_metadata("app.orders", {"append.only.ingestion": "auto"})

        self.assertEqual(metadata["ingestion_type"], "cdc_with_deletes")
        self.assertTrue(metadata["primary_keys"])

    def test_primary_keys_override_promotes_keyless_to_cdc(self):
        # A declared key turns a physically keyless table into a keyed CDC table,
        # and is reported so the pipeline uses it as the destination merge key too.
        connector, _ = self._keyless_connector()

        metadata = connector.read_table_metadata("app.orders", {"primary.keys": "id"})

        self.assertEqual(metadata["ingestion_type"], "cdc_with_deletes")
        self.assertEqual(metadata["primary_keys"], ["id"])
        self.assertEqual(metadata["cursor_field"], informix_module.CURSOR)

    def test_primary_keys_override_accepts_multiple_columns(self):
        connector, _ = self._keyless_connector()

        metadata = connector.read_table_metadata("app.orders", {"primary.keys": "id,value"})

        self.assertEqual(metadata["ingestion_type"], "cdc_with_deletes")
        self.assertEqual(metadata["primary_keys"], ["id", "value"])

    def test_primary_keys_override_rejects_unknown_column(self):
        connector, _ = self._keyless_connector()

        with self.assertRaisesRegex(InformixError, "not present"):
            connector.read_table_metadata("app.orders", {"primary.keys": "nope"})

    def test_primary_keys_override_survives_schema_refresh(self):
        # A refresh re-reads the catalog (which reports no key); the override and
        # its fingerprint must persist rather than reverting to keyless.
        connector, _ = self._keyless_connector()
        table = connector._table("app.orders", {"primary.keys": "id"})
        self.assertEqual(table.primary_keys, ("id",))
        self.assertTrue(table.key_override)

        refreshed = connector._refresh_table_schema(table, None)

        self.assertEqual(refreshed.primary_keys, ("id",))
        self.assertTrue(refreshed.key_override)

    def test_a_table_with_uncapturable_columns_is_never_append(self):
        bridge = FakeBridge()
        bridge.tables = [_table(cdc=False, primary_keys=())]
        connector = self.connector(bridge)

        metadata = connector.read_table_metadata("app.orders", dict(self.APPEND))

        self.assertEqual(metadata["ingestion_type"], "snapshot")

    def test_append_defaults_to_streaming_from_now_without_history(self):
        connector, _ = self._keyless_connector()

        rows, offset = connector.read_table("app.orders", {}, {})

        self.assertEqual(list(rows), [], "stream backfill must not emit history")
        self.assertEqual(offset["phase"], "stream")
        # A checkpointable offset, unlike snapshot-only's None: this is what lets the
        # flow resume instead of re-reading.
        self.assertIsNotNone(offset["commit_lsn"])
        self.assertEqual(offset["commit_lsn"], offset["begin_lsn"])
        self.assertIsNone(offset["tx_id"])

    def test_append_snapshot_backfill_emits_history_then_streams(self):
        connector, bridge = self._keyless_connector()
        options = dict(self.APPEND, **{"snapshot.mode": "initial"})

        rows, offset = connector.read_table("app.orders", {}, options)

        self.assertEqual(len(list(rows)), 2, "snapshot backfill must emit existing rows")
        self.assertEqual(offset["phase"], "stream")
        self.assertEqual(
            bridge.snapshot_max_bytes,
            [0],
            "snapshot.max.bytes must default to unlimited",
        )
        self.assertEqual(
            bridge.snapshot_max_rows,
            [0],
            "snapshot.max.rows must default to unlimited",
        )

    def test_append_snapshot_backfill_fails_loudly_above_its_bound(self):
        connector, _ = self._keyless_connector()
        options = dict(
            self.APPEND,
            **{"snapshot.mode": "initial", "snapshot.max.rows": "1"},
        )

        with self.assertRaisesRegex(InformixError, "exceeds snapshot.max.rows=1"):
            connector.read_table("app.orders", {}, options)

    def test_a_second_append_read_streams_rather_than_rebackfilling(self):
        connector, bridge = self._keyless_connector()
        options = dict(self.APPEND, **{"snapshot.mode": "initial"})

        _, first = connector.read_table("app.orders", {}, options)
        before = len(bridge.snapshot_max_bytes)
        list(connector.read_table("app.orders", first, options)[0])

        self.assertEqual(
            len(bridge.snapshot_max_bytes),
            before,
            "a stream-phase append read must not query the snapshot again",
        )

    def test_append_only_has_no_delete_channel(self):
        connector, _ = self._keyless_connector()
        # Let the default append upsert path publish its channel start. Before
        # that, an uncheckpointed delete reader deliberately avoids opening an
        # Informix connection while it waits for upsert coordination.
        connector.read_table("app.orders", {}, {})

        # An empty batch here would read as "no deletes, ever" rather than a
        # misconfiguration, so this must raise.
        with self.assertRaisesRegex(ValueError, "no delete channel"):
            connector.read_table_deletes("app.orders", {}, {})

    def test_append_only_refuses_a_delete_channel_even_with_a_primary_key(self):
        bridge = FakeBridge()
        connector = self.connector(bridge)

        with self.assertRaisesRegex(ValueError, "no delete channel"):
            connector.read_table_deletes("app.orders", {}, dict(self.APPEND))

    def test_an_unsupported_append_snapshot_mode_is_rejected(self):
        connector, _ = self._keyless_connector()
        options = dict(self.APPEND, **{"snapshot.mode": "incremental"})

        with self.assertRaisesRegex(ValueError, "supports only snapshot.mode"):
            connector.read_table("app.orders", {}, options)

    def test_a_malformed_append_opt_in_is_rejected(self):
        connector, _ = self._keyless_connector()

        with self.assertRaisesRegex(ValueError, "append.only.ingestion"):
            connector.read_table_metadata("app.orders", {"append.only.ingestion": "maybe"})

    def test_a_keyed_table_may_opt_into_append(self):
        bridge = FakeBridge()
        connector = self.connector(bridge)

        metadata = connector.read_table_metadata("app.orders", dict(self.APPEND))

        self.assertEqual(metadata["ingestion_type"], "append")
        self.assertEqual(
            metadata["primary_keys"], [], "append reports no keys even when the table has one"
        )

    def test_cdc_streamable_does_not_require_a_primary_key(self):
        # The split that makes all of this possible: cdc_startcapture takes no key,
        # so capture capability and merge capability are different questions.
        columns = (
            informix_module.Column(name="id", type_name="INTEGER", nullable=False),
            informix_module.Column(name="value", type_name="VARCHAR", length=20),
        )
        keyless = informix_module.Table(
            database="demo", owner="app", name="orders", columns=columns, primary_keys=()
        )
        keyed = informix_module.Table(
            database="demo", owner="app", name="orders", columns=columns, primary_keys=("id",)
        )
        uncapturable = informix_module.Table(
            database="demo",
            owner="app",
            name="orders",
            columns=(informix_module.Column(name="id", type_name="INTEGER", cdc_supported=False),),
            primary_keys=(),
        )

        self.assertTrue(informix_module._cdc_streamable(keyless))
        self.assertFalse(informix_module._cdc_capable(keyless))
        self.assertTrue(informix_module._cdc_capable(keyed))
        self.assertFalse(informix_module._cdc_streamable(uncapturable))

    def test_resumed_snapshot_applies_table_byte_limit(self):
        bridge = FakeBridge()
        connector = self.connector(bridge)
        options = {"snapshot.page.size": "1", "snapshot.max.bytes": "12345"}
        _, checkpoint = connector.read_table("app.orders", {}, options)
        second_rows, _ = connector.read_table("app.orders", checkpoint, options)

        self.assertEqual(len(list(second_rows)), 1)
        self.assertEqual(bridge.snapshot_max_bytes, [12345])

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
            {
                "phase": "snapshot",
                "snapshot_lsn": "91",
                "snapshot": {"last_pk": [1], "page_index": 1},
            }
        )
        invalid_offsets.append(inconsistent_snapshot)
        generation_without_boundary = _stream_offset()
        generation_without_boundary["trigger_generation"] = "a" * 32
        invalid_offsets.append(generation_without_boundary)
        boundary_without_generation = _stream_offset()
        boundary_without_generation["trigger_high_water"] = "100"
        invalid_offsets.append(boundary_without_generation)
        for offset in invalid_offsets:
            with self.assertRaises(ValueError):
                connector.read_table("app.orders", offset, {})

    def test_completed_trigger_boundary_may_precede_current_checkpoint(self):
        checkpoint = _stream_offset(257743573232)
        checkpoint.update(
            {
                "trigger_generation": "a" * 32,
                "trigger_high_water": "257740537856",
            }
        )

        validated = informix_module._validated_offset(checkpoint)

        self.assertEqual(validated["commit_lsn"], "257743573232")
        self.assertEqual(validated["trigger_high_water"], "257740537856")

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

    def _replay_reader(self, connector, **options):
        """A real LakeflowStreamReader over ``connector``, with the installed patch."""

        class TriggerBase:
            pass

        Wrapped = _informix_available_now_base(TriggerBase)

        class LakeflowStreamReader(Wrapped):
            def __init__(inner):
                inner.lakeflow_connect = connector
                inner.options = {"tableName": "app.orders", **options}

            def read(inner, start):
                return connector.read_table(inner.options["tableName"], start, dict(inner.options))

        return LakeflowStreamReader()

    _REPLAY_CHANGES = (
        {"op": "BEGIN", "tx_id": 7, "lsn": 101},
        {"op": "INSERT", "tx_id": 7, "lsn": 102, "row": {"id": 1, "value": "a"}},
        {"op": "INSERT", "tx_id": 7, "lsn": 103, "row": {"id": 2, "value": "b"}},
        {"op": "COMMIT", "tx_id": 7, "lsn": 104},
        {"op": "BEGIN", "tx_id": 8, "lsn": 105},
        {"op": "INSERT", "tx_id": 8, "lsn": 106, "row": {"id": 3, "value": "c"}},
        {"op": "COMMIT", "tx_id": 8, "lsn": 107},
    )

    def test_an_unbounded_read_is_not_reproducible_across_row_budgets(self):
        """Why a replay must be bounded: the same range can end in two places.

        An unbounded read stops at whichever transaction boundary the row budget
        falls on, so replaying a committed range without a bound can legitimately
        end early -- and the framework commits its own `end` regardless, dropping
        everything in between.
        """

        ends = {}
        for budget in ("10", "2"):
            bridge = FakeBridge()
            # A fresh state store per iteration: the staged pages live in this
            # test's temp directory, so a manifest surviving from the previous
            # iteration would point at pages that no longer exist.
            self._lakebase.database.records.clear()
            connector = self.connector(bridge)
            _, checkpoint = connector.read_table("app.orders", {}, {})
            bridge.changes = list(self._REPLAY_CHANGES)
            _, end = connector.read_table(
                "app.orders", checkpoint, {"max.records.per.batch": budget}
            )
            ends[budget] = end["commit_lsn"]

        self.assertEqual(ends["10"], "107")
        self.assertEqual(ends["2"], "104")

    def test_a_bounded_replay_reproduces_the_committed_range(self):
        """The fix: the replay reads to `end` and stops, ignoring the row budget.

        The replaying reader carries a budget that would have stopped it at LSN 104,
        which is exactly the shortfall seen in production on 'tw241'. Bounded, it
        must still deliver the whole committed range.
        """

        bridge = FakeBridge()
        connector = self.connector(bridge)
        _, checkpoint = connector.read_table("app.orders", {}, {})
        bridge.changes = list(self._REPLAY_CHANGES)
        original, end = connector.read_table(
            "app.orders", checkpoint, {"max.records.per.batch": "10"}
        )
        original = list(original)

        replay_bridge = FakeBridge()
        replay_bridge.changes = list(self._REPLAY_CHANGES)
        replay_connector = self.connector(replay_bridge)
        reader = self._replay_reader(replay_connector, **{"max.records.per.batch": "2"})

        replayed = list(reader.readBetweenOffsets(checkpoint, end))

        self.assertEqual(len(replayed), len(original))
        self.assertEqual([row["id"] for row in replayed], [1, 2, 3])

    def test_the_replay_bound_does_not_leak_into_later_reads(self):
        """A normal read after a replay must be unbounded again."""

        bridge = FakeBridge()
        connector = self.connector(bridge)
        _, checkpoint = connector.read_table("app.orders", {}, {})
        bridge.changes = list(self._REPLAY_CHANGES)
        reader = self._replay_reader(connector, **{"max.records.per.batch": "10"})

        list(reader.readBetweenOffsets(checkpoint, {"commit_lsn": "104"}))

        self.assertNotIn(informix_module._REPLAY_STOP_LSN_OPTION, reader.options)
        _, end = reader.read(checkpoint)
        self.assertEqual(end["commit_lsn"], "107")

    def test_a_bounded_replay_fails_if_the_source_returns_before_its_end(self):
        bridge = FakeBridge()
        connector = self.connector(bridge)
        _, checkpoint = connector.read_table("app.orders", {}, {})
        bridge.changes = list(self._REPLAY_CHANGES[:4])
        reader = self._replay_reader(connector)

        with self.assertRaisesRegex(InformixError, "did not reach its committed end LSN"):
            list(reader.readBetweenOffsets(checkpoint, {**checkpoint, "commit_lsn": "107"}))

    def test_a_replay_never_turns_capacity_exhaustion_into_an_empty_batch(self):
        connector = self.connector(FakeBridge())
        _, checkpoint = connector.read_table("app.orders", {}, {})
        reader = self._replay_reader(connector)

        with (
            mock.patch.object(
                connector,
                "_read_stream",
                side_effect=ConnectionCapacityUnavailable("capacity exhausted"),
            ),
            self.assertRaises(ConnectionCapacityUnavailable),
        ):
            list(reader.readBetweenOffsets(checkpoint, dict(checkpoint)))

    def test_a_delete_replay_never_yields_on_capacity_exhaustion(self):
        connector = self.connector(FakeBridge())
        checkpoint = _stream_offset()
        options = {informix_module._REPLAY_STOP_LSN_OPTION: checkpoint["commit_lsn"]}

        with (
            mock.patch.object(
                connector,
                "_read_stream",
                side_effect=ConnectionCapacityUnavailable("capacity exhausted"),
            ),
            self.assertRaises(ConnectionCapacityUnavailable),
        ):
            connector.read_table_deletes("app.orders", checkpoint, options)

    def test_a_caught_up_contended_replay_does_not_fail(self):
        """The regression that took production down twice.

        A caught-up flow replays a range holding nothing new while carrying
        per-attempt bookkeeping. Two earlier attempts compared offsets here and
        raised, failing flows on 'tw070', 'tw101', 'tw303' and others. Bounding the
        read has no opinion about offset equality, so this must simply return empty.
        """

        connector = self.connector(FakeBridge())
        _, checkpoint = connector.read_table("app.orders", {}, {})
        reader = self._replay_reader(connector)
        contended = dict(checkpoint)
        contended["capacity_pressure"] = 3
        contended["backlog_rank"] = 2
        contended["capacity_retry_count"] = 1

        self.assertEqual(list(reader.readBetweenOffsets(contended, dict(checkpoint))), [])

    def test_a_replay_under_a_new_pipeline_scope_does_not_fail(self):
        """pipeline_scope is per-update, and a replay usually follows a restart."""

        connector = self.connector(FakeBridge())
        _, checkpoint = connector.read_table("app.orders", {}, {})
        reader = self._replay_reader(connector)
        restarted = dict(checkpoint)
        restarted["pipeline_scope"] = "f" * 32

        self.assertEqual(list(reader.readBetweenOffsets(restarted, dict(checkpoint))), [])

    def test_a_replay_without_an_end_position_fails_closed(self):
        """Spark must supply the position the replay is required to reach."""

        bridge = FakeBridge()
        connector = self.connector(bridge)
        _, checkpoint = connector.read_table("app.orders", {}, {})
        bridge.changes = list(self._REPLAY_CHANGES)
        reader = self._replay_reader(connector, **{"max.records.per.batch": "10"})

        with self.assertRaisesRegex(InformixError, "end has no commit_lsn"):
            list(reader.readBetweenOffsets(checkpoint, {}))

    def test_an_advisory_only_range_replays_as_empty(self):
        """Bootstrap bookkeeping advances no source position and has no rows."""

        class TriggerBase:
            pass

        Wrapped = _informix_available_now_base(TriggerBase)

        class LakeflowStreamReader(Wrapped):
            def __init__(inner):
                inner.lakeflow_connect = object()
                inner.options = {}

            def read(inner, start):
                raise AssertionError("an advisory-only range must not read Informix")

        reader = LakeflowStreamReader()

        self.assertEqual(
            list(reader.readBetweenOffsets({}, {"schema_node_fallback_retry_count": 1})),
            [],
        )

    def test_an_initial_replay_advances_advisory_offsets_in_process(self):
        class TriggerBase:
            pass

        Wrapped = _informix_available_now_base(TriggerBase)

        class LakeflowStreamReader(Wrapped):
            def __init__(inner):
                inner.lakeflow_connect = object()
                inner.options = {"connection.wait.timeout.seconds": "10"}
                inner.seen = []
                inner.wait_budgets = []

            def read(inner, start):
                inner.seen.append(dict(start))
                inner.wait_budgets.append(
                    float(inner.options[informix_module._REPLAY_CONNECTION_WAIT_BUDGET_OPTION])
                )
                count = start.get("schema_node_fallback_retry_count", 0)
                if count < informix_module._SCHEMA_NODE_FALLBACK_RETRIES:
                    return iter(()), {"schema_node_fallback_retry_count": count + 1}
                return iter(({"id": 1},)), {"commit_lsn": "110"}

        reader = LakeflowStreamReader()
        with mock.patch.object(
            informix_module.time, "monotonic", side_effect=[100, 100, 101, 102, 103, 104]
        ):
            rows = list(
                reader.readBetweenOffsets(
                    {"schema_node_fallback_retry_count": 1}, {"commit_lsn": "100"}
                )
            )

        self.assertEqual(rows, [{"id": 1}])
        self.assertEqual(
            [offset["schema_node_fallback_retry_count"] for offset in reader.seen],
            list(range(1, informix_module._SCHEMA_NODE_FALLBACK_RETRIES + 1)),
        )
        self.assertEqual(reader.wait_budgets, [10, 9, 8, 7, 6])
        self.assertNotIn(informix_module._REPLAY_CONNECTION_WAIT_BUDGET_OPTION, reader.options)

    def test_an_initial_replay_fails_if_advisory_progress_stalls(self):
        class TriggerBase:
            pass

        Wrapped = _informix_available_now_base(TriggerBase)

        class LakeflowStreamReader(Wrapped):
            def __init__(inner):
                inner.lakeflow_connect = object()
                inner.options = {"connection.wait.timeout.seconds": "1"}

            def read(inner, start):
                return iter(()), dict(start)

        with (
            mock.patch.object(informix_module.time, "monotonic", side_effect=[100, 100, 101]),
            mock.patch.object(informix_module.time, "sleep"),
            self.assertRaisesRegex(InformixError, "connection wait deadline"),
        ):
            list(
                LakeflowStreamReader().readBetweenOffsets(
                    {"schema_node_fallback_retry_count": 5}, {"commit_lsn": "100"}
                )
            )

    def test_max_schema_fallback_waits_for_upsert_publication(self):
        class TriggerBase:
            pass

        Wrapped = _informix_available_now_base(TriggerBase)

        class LakeflowStreamReader(Wrapped):
            def __init__(inner):
                inner.lakeflow_connect = object()
                inner.options = {"connection.wait.timeout.seconds": "10"}
                inner.calls = 0

            def read(inner, start):
                inner.calls += 1
                if inner.calls < 3:
                    return iter(()), dict(start)
                return iter(({"id": 1},)), {"commit_lsn": "110"}

        reader = LakeflowStreamReader()
        with (
            mock.patch.object(
                informix_module.time,
                "monotonic",
                side_effect=[100, 100, 100, 100, 100, 100],
            ),
            mock.patch.object(informix_module.time, "sleep") as sleep,
        ):
            rows = list(
                reader.readBetweenOffsets(
                    {"schema_node_fallback_retry_count": 5}, {"commit_lsn": "100"}
                )
            )

        self.assertEqual(rows, [{"id": 1}])
        self.assertEqual(reader.calls, 3)
        self.assertEqual(sleep.call_count, 2)

    def test_a_transient_mount_failure_is_retried_in_process(self):
        """A remount settles in seconds, so the read should ride it out in place.

        Yielding an empty batch is the cheaper answer for a normal read but is
        unavailable during a replay, so the retry removes the dilemma whenever the
        mount comes back at all.
        """

        bridge = FakeBridge()
        connector = self.connector(bridge)
        attempts = {"n": 0}
        real_read_changes = bridge.read_changes

        def flaky(*args, **kwargs):
            attempts["n"] += 1
            if attempts["n"] <= 3:
                raise OSError(errno.ENOTCONN, "Transport endpoint is not connected")
            return real_read_changes(*args, **kwargs)

        with (
            mock.patch.object(bridge, "read_changes", side_effect=flaky),
            mock.patch.object(informix_module.time, "sleep"),
        ):
            rows, offset = connector.read_table("app.orders", _stream_offset(), {})

        list(rows)
        # The read succeeded in place, so no retry bookkeeping is carried forward.
        self.assertNotIn("dropped_mount_retry_count", offset)
        self.assertGreater(attempts["n"], 3)

    def test_in_process_retries_are_bounded_then_yield_for_a_normal_read(self):
        """A mount that never returns must still degrade, not fail the flow."""

        bridge = FakeBridge()
        connector = self.connector(bridge)
        attempts = {"n": 0}

        def always_down(*args, **kwargs):
            attempts["n"] += 1
            raise OSError(errno.ENOTCONN, "Transport endpoint is not connected")

        with (
            mock.patch.object(bridge, "read_changes", side_effect=always_down),
            mock.patch.object(informix_module.time, "sleep"),
        ):
            rows, offset = connector.read_table("app.orders", _stream_offset(), {})

        self.assertEqual(list(rows), [])
        self.assertEqual(offset["dropped_mount_retry_count"], 1)
        self.assertGreater(attempts["n"], 1)

    def test_the_retry_count_still_climbs_across_successive_reads(self):
        """The in-process retry must not disable the fail-loud cap.

        Regression found while building this: a nested attempt that gave up and
        yielded had its offset passed through _reset_retry_counts, stripping the
        counter. Successive reads then reported 1, absent, 1, absent -- so the cap in
        _dropped_mount_retry_offset could never be reached and a permanent outage
        would have gone back to being silent.
        """

        bridge = FakeBridge()
        connector = self.connector(bridge)
        seen = []

        with (
            mock.patch.object(
                bridge,
                "read_changes",
                side_effect=OSError(errno.ENOTCONN, "Transport endpoint is not connected"),
            ),
            mock.patch.object(informix_module.time, "sleep"),
        ):
            offset = _stream_offset()
            for _ in range(4):
                rows, offset = connector.read_table("app.orders", offset, {})
                list(rows)
                seen.append(offset.get("dropped_mount_retry_count"))

        self.assertEqual(seen, [1, 2, 3, 4])

    def test_a_replay_raises_rather_than_yielding_when_the_mount_stays_down(self):
        """The hole fix 1 exists to close.

        During a replay the caller has already committed the range's end offset, so
        an empty batch is read as proof the range held nothing and the rows are
        dropped. Failing the batch instead lets Spark retry it.
        """

        bridge = FakeBridge()
        connector = self.connector(bridge)
        options = {informix_module._REPLAY_STOP_LSN_OPTION: "104"}

        with (
            mock.patch.object(
                bridge,
                "read_changes",
                side_effect=OSError(errno.ENOTCONN, "Transport endpoint is not connected"),
            ),
            mock.patch.object(informix_module.time, "sleep"),
            self.assertRaises(InformixError) as caught,
        ):
            connector.read_table("app.orders", _stream_offset(), options)

        self.assertIn("replaying a committed", str(caught.exception))

    def test_a_malformed_replay_bound_is_rejected(self):
        """A bad bound must not silently degrade to an unbounded read."""

        connector = self.connector(FakeBridge())
        for invalid in ("not-an-lsn", "-1"):
            with self.subTest(invalid=invalid):
                with self.assertRaises(InformixError):
                    connector._replay_stop_lsn({informix_module._REPLAY_STOP_LSN_OPTION: invalid})
        self.assertIsNone(connector._replay_stop_lsn({}))
        self.assertEqual(
            connector._replay_stop_lsn({informix_module._REPLAY_STOP_LSN_OPTION: "104"}), 104
        )

    def test_registration_scope_uses_driver_pipeline_and_update_ids(self):
        class TriggerBase:
            pass

        values = {
            "spark.pipelines.pipelineId": "94368468-bc2d-4797-868d-0cb9e19a5610",
            "spark.pipelines.updateId": "5fc90c10-d5d1-489b-afce-1bd9d36544c1",
        }
        spark = types.SimpleNamespace(
            conf=types.SimpleNamespace(get=lambda key, default=None: values.get(key, default))
        )
        Wrapped = _informix_available_now_base(TriggerBase, spark)

        class LakeflowStreamReader(Wrapped):
            def __init__(self):
                self.lakeflow_connect = mock.Mock()
                self.options = {}

        reader = LakeflowStreamReader()
        scope = reader.lakeflow_connect.set_registration_scope.call_args.args[0]
        self.assertEqual(
            scope,
            "94368468-bc2d-4797-868d-0cb9e19a5610" "_@_5fc90c10-d5d1-489b-afce-1bd9d36544c1",
        )

    def test_registration_scope_rejects_malformed_driver_ids(self):
        class TriggerBase:
            pass

        spark = types.SimpleNamespace(
            conf=types.SimpleNamespace(get=lambda key, default=None: "not-a-uuid")
        )
        with self.assertRaisesRegex(ValueError, "canonical UUID"):
            _informix_available_now_base(TriggerBase, spark)

    def test_pipeline_and_update_scope_is_valid_checkpoint_and_path_component(self):
        scope = "94368468-bc2d-4797-868d-0cb9e19a5610" "_@_5fc90c10-d5d1-489b-afce-1bd9d36544c1"
        connector = self.connector(FakeBridge())
        connector.set_registration_scope(scope)
        checkpoint = _stream_offset()
        checkpoint["pipeline_scope"] = scope

        self.assertEqual(_validated_offset(checkpoint)["pipeline_scope"], scope)
        namespace = connector._immutable_namespace(
            Table.parse(_table(), "demo"), "initialization", scope
        )
        self.assertEqual(os.path.basename(namespace), scope)

    def test_stream_checkpoint_migrates_to_current_update_scope(self):
        old_scope = "94368468-bc2d-4797-868d-0cb9e19a5610" "_@_11111111-1111-4111-8111-111111111111"
        current_scope = (
            "94368468-bc2d-4797-868d-0cb9e19a5610" "_@_22222222-2222-4222-8222-222222222222"
        )
        checkpoint = _stream_offset()
        checkpoint["pipeline_scope"] = old_scope
        connector = self.connector(FakeBridge())
        connector.set_registration_scope(current_scope)

        rows, migrated = connector.read_table("app.orders", checkpoint, {})

        self.assertEqual(list(rows), [])
        self.assertEqual(migrated["pipeline_scope"], current_scope)

    def test_delete_bootstrap_never_starts_ahead_of_the_data_channel(self):
        # The scope-independent fallback must resume at or behind the upsert
        # reader. Re-reading a delete is idempotent under apply_as_deletes;
        # starting ahead of the data channel would silently skip one.
        pipeline_id = "94368468-bc2d-4797-868d-0cb9e19a5610"
        first_scope = f"{pipeline_id}_@_11111111-1111-4111-8111-111111111111"
        second_scope = f"{pipeline_id}_@_22222222-2222-4222-8222-222222222222"

        upsert = self.connector(FakeBridge())
        upsert.set_registration_scope(first_scope)
        _, checkpoint = upsert.read_table("app.orders", {}, {})

        resumed = self.connector(FakeBridge())
        resumed.set_registration_scope(second_scope)
        carried = dict(checkpoint)
        carried["pipeline_scope"] = first_scope
        list(resumed.read_table("app.orders", carried, {})[0])

        delete_connector = self.connector(FakeBridge())
        delete_connector.set_registration_scope(second_scope)
        _, bootstrap = self._bootstrap_delete_reader(delete_connector)

        self.assertEqual(
            int(bootstrap["commit_lsn"]),
            int(checkpoint.get("begin_lsn") or checkpoint["commit_lsn"]),
            "delete bootstrap must use the resumed upsert's exact restart LSN",
        )

    def test_upsert_channel_start_converges_on_later_lower_restart_lsn(self):
        scope = "94368468-bc2d-4797-868d-0cb9e19a5610_@_22222222-2222-4222-8222-222222222222"
        table = Table.parse(_table(), "demo")
        connector = self.connector(FakeBridge())
        connector.set_registration_scope(scope)
        schema_id = "1" * 32
        fingerprint = _schema_fingerprint(table)

        connector._publish_upsert_channel_start(table, 120, schema_id, fingerprint, scope)
        connector._publish_upsert_channel_start(table, 90, schema_id, fingerprint, scope)
        connector._publish_upsert_channel_start(table, 150, schema_id, fingerprint, scope)

        self.assertEqual(
            connector._read_upsert_channel_start(table, scope),
            (90, schema_id, fingerprint),
        )

    def test_positional_checkpoint_publishes_channel_start_before_source_read(self):
        connector = self.connector(FakeBridge())
        checkpoint = _stream_offset(123)

        with (
            mock.patch.object(
                connector,
                "_read_table_attempt",
                side_effect=AssertionError("source read reached"),
            ),
            self.assertRaisesRegex(AssertionError, "source read reached"),
        ):
            connector.read_table("app.orders", checkpoint, {})

        self.assertTrue(connector._upsert_channel_start_exists("app.orders", {}))
        table = Table.parse(_table(), "demo")
        self.assertEqual(
            connector._read_upsert_channel_start(table, connector._pipeline_scope()),
            (123, checkpoint["schema_id"], checkpoint["schema_fingerprint"]),
        )

    def test_upsert_channel_start_still_rejects_schema_conflict(self):
        scope = "94368468-bc2d-4797-868d-0cb9e19a5610_@_33333333-3333-4333-8333-333333333333"
        table = Table.parse(_table(), "demo")
        connector = self.connector(FakeBridge())
        connector.set_registration_scope(scope)
        fingerprint = _schema_fingerprint(table)
        connector._publish_upsert_channel_start(table, 120, "1" * 32, fingerprint, scope)

        with self.assertRaisesRegex(InformixError, "Conflicting Informix upsert"):
            connector._publish_upsert_channel_start(table, 120, "2" * 32, fingerprint, scope)

    def test_delete_bootstrap_reads_shared_state_once_not_per_microbatch(self):
        # The fallback lives on the `not start_offset` bootstrap branch, so a
        # checkpointed delete microbatch must never touch the Volume for it.
        pipeline_id = "94368468-bc2d-4797-868d-0cb9e19a5610"
        first_scope = f"{pipeline_id}_@_11111111-1111-4111-8111-111111111111"
        second_scope = f"{pipeline_id}_@_22222222-2222-4222-8222-222222222222"

        upsert = self.connector(FakeBridge())
        upsert.set_registration_scope(first_scope)
        _, checkpoint = upsert.read_table("app.orders", {}, {})
        resumed = self.connector(FakeBridge())
        resumed.set_registration_scope(second_scope)
        carried = dict(checkpoint)
        carried["pipeline_scope"] = first_scope
        list(resumed.read_table("app.orders", carried, {})[0])

        delete_connector = self.connector(FakeBridge())
        delete_connector.set_registration_scope(second_scope)
        _, offset = self._bootstrap_delete_reader(delete_connector)
        self.assertEqual(offset.get("phase"), "stream")

        with mock.patch.object(
            delete_connector,
            "_read_immutable_head",
            side_effect=AssertionError("checkpointed delete microbatch must not read state"),
        ):
            for _ in range(3):
                _, offset = delete_connector.read_table_deletes("app.orders", offset, {})

    def _stale_schema_node_delete_reader(self):
        """Build a delete reader whose only boundary is a schema node from a
        *previous log incarnation* -- the production failure: a node recorded at
        uniqid 22 while the source's reinitialized log had reached only uniqid 10."""

        pipeline_id = "94368468-bc2d-4797-868d-0cb9e19a5610"
        first_scope = f"{pipeline_id}_@_11111111-1111-4111-8111-111111111111"
        second_scope = f"{pipeline_id}_@_22222222-2222-4222-8222-222222222222"
        bridge = FakeBridge()
        # Publish the schema node at uniqid 22 of the *pre-reset* log.
        bridge.now = 22 << 32
        bridge.minimum = 20 << 32
        upsert = self.connector(bridge)
        upsert.set_registration_scope(first_scope)
        _, checkpoint = upsert.read_table("app.orders", {}, {})
        resumed = self.connector(bridge)
        resumed.set_registration_scope(second_scope)
        carried = dict(checkpoint)
        carried["pipeline_scope"] = first_scope
        list(resumed.read_table("app.orders", carried, {})[0])

        # The source's log is reinitialized: it has reached only uniqid 10 while the
        # durable schema node still names uniqid 22, i.e. a boundary ahead of the
        # server's current position -- unreachable, and not merely aged out.
        reset = FakeBridge()
        reset.now = 10 << 32
        reset.minimum = 3 << 32
        delete_connector = self.connector(reset)
        delete_connector.set_registration_scope(second_scope)
        return delete_connector, reset

    def test_a_stale_current_scope_channel_start_is_not_used_after_log_reset(self):
        # Even a current-update rendezvous can become stale if Informix
        # reinitializes its logical log after publication.  It must be rejected,
        # and the reader must not fall back to an inferred position.
        delete_connector, _ = self._stale_schema_node_delete_reader()

        _, offset = delete_connector.read_table_deletes("app.orders", {}, {})

        self.assertIsNone(offset.get("phase"))

    def test_the_bootstrap_deferral_counter_accumulates_across_retries(self):
        # Regression: _reset_retry_counts stripped this counter on every read that
        # carried it, pinning it at 1 so the fallback was never reached and the delete
        # channel stalled forever.
        delete_connector, _ = self._stale_schema_node_delete_reader()

        offset, seen = {}, []
        for _ in range(informix_module._SCHEMA_NODE_FALLBACK_RETRIES):
            _, offset = delete_connector.read_table_deletes("app.orders", offset, {})
            seen.append(offset.get("schema_node_fallback_retry_count"))

        self.assertEqual(
            seen,
            list(range(1, informix_module._SCHEMA_NODE_FALLBACK_RETRIES + 1)),
            "the deferral counter must accumulate, not reset each read",
        )

    def test_delete_bootstrap_never_falls_back_from_a_stale_channel_start(self):
        # A stale boundary is not made safe by waiting.  Keep yielding until a
        # new snapshot generation publishes a usable current-update start.
        delete_connector, _ = self._stale_schema_node_delete_reader()

        _, offset = self._bootstrap_delete_reader(delete_connector)

        self.assertIsNone(offset.get("phase"))

    def test_a_corrupt_bootstrap_deferral_counter_is_rejected(self):
        for bad in (-1, "3", True, 1.5):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    informix_module._schema_node_fallback_retry_count(
                        {"schema_node_fallback_retry_count": bad}
                    )

    def _bootstrap_delete_reader(self, connector, table="app.orders", options=None):
        """Drive a delete reader's bootstrap until it yields a boundary or gives up.

        The scope-independent schema-node fallback is deliberately deferred: a reader
        with no offset prefers *this* update's scoped record for
        _SCHEMA_NODE_FALLBACK_RETRIES reads, because a bootstrap with no offset is
        exactly what a full refresh produces and the schema node may carry a boundary
        from a previous log incarnation. Tests that exercise the fallback therefore have
        to spend those retries first.

        Returns the final ``(rows, offset)``.
        """

        offset: dict = {}
        rows: object = iter(())
        # Reads 1..N spend the deferral (each returns count=1..N), and the read that
        # *observes* count==N is the first eligible to use the fallback -- so N+2 reads
        # are needed in the worst case, not N+1.
        for _ in range(informix_module._SCHEMA_NODE_FALLBACK_RETRIES + 2):
            rows, offset = connector.read_table_deletes(table, offset, options or {})
            if offset.get("phase") is not None:
                break
        return rows, offset

    def test_delete_bootstrap_declines_when_no_schema_node_exists(self):
        # With neither scoped records nor a schema node (genuine first run,
        # upsert still snapshotting) the reader must keep returning an empty
        # offset rather than invent a boundary.
        pipeline_id = "94368468-bc2d-4797-868d-0cb9e19a5610"
        scope = f"{pipeline_id}_@_33333333-3333-4333-8333-333333333333"
        delete_connector = self.connector(FakeBridge())
        delete_connector.set_registration_scope(scope)

        rows, offset = self._bootstrap_delete_reader(delete_connector)

        self.assertEqual(list(rows), [])
        self.assertIsNone(offset.get("phase"))

    def test_delete_bootstrap_declines_an_unretained_schema_node_boundary(self):
        # A node whose start_lsn has aged out of the logical log cannot be used:
        # advancing to the minimum retained LSN could skip deletes committed in
        # between, so the reader must decline instead.
        pipeline_id = "94368468-bc2d-4797-868d-0cb9e19a5610"
        first_scope = f"{pipeline_id}_@_11111111-1111-4111-8111-111111111111"
        second_scope = f"{pipeline_id}_@_22222222-2222-4222-8222-222222222222"

        upsert = self.connector(FakeBridge())
        upsert.set_registration_scope(first_scope)
        _, checkpoint = upsert.read_table("app.orders", {}, {})
        resumed = self.connector(FakeBridge())
        resumed.set_registration_scope(second_scope)
        carried = dict(checkpoint)
        carried["pipeline_scope"] = first_scope
        list(resumed.read_table("app.orders", carried, {})[0])

        aged = FakeBridge()
        aged.minimum = int(checkpoint["commit_lsn"]) + 1
        delete_connector = self.connector(aged)
        delete_connector.set_registration_scope(second_scope)

        rows, offset = self._bootstrap_delete_reader(delete_connector)

        self.assertEqual(list(rows), [])
        self.assertIsNone(offset.get("phase"))

    def test_registration_scope_exists_before_schema_discovery(self):
        class TriggerBase:
            pass

        previous = informix_module._INFORMIX_REGISTRATION_CONTEXT["scope"]
        try:
            _informix_available_now_base(TriggerBase)
            connector = InformixLakeflowConnect(
                {
                    "hostname": "informix.example",
                    "server": "ol_informix",
                    "database": "demo",
                    "snapshot.staging.location": "/Volumes/test/default/informix_state",
                    "lakebase.password": "test-state-password",
                }
            )

            self.assertRegex(connector._registration_scope, r"^[0-9a-f]{32}$")
            self.assertIsNotNone(connector._registration_table_cache_key())
        finally:
            informix_module._INFORMIX_REGISTRATION_CONTEXT["scope"] = previous

    def test_table_activity_is_rate_limited_and_runs_gc(self):
        pipeline_id = "94368468-bc2d-4797-868d-0cb9e19a5610"
        current = f"{pipeline_id}_@_33333333-3333-4333-8333-333333333333"
        connector = self.connector(FakeBridge())
        connector.set_registration_scope(current)
        table = connector._table("app.orders", {})
        connection = object()
        connector._lakebase_connection = lambda: connection

        with (
            mock.patch.object(informix_module, "touch_table_activity", return_value=True) as touch,
            mock.patch.object(informix_module, "collect_stale_table_state", return_value=0) as gc,
        ):
            connector._touch_table_state(table)
            connector._touch_table_state(table)

        self.assertEqual(touch.call_count, 1)
        self.assertEqual(gc.call_count, 1)
        self.assertEqual(gc.call_args.args[-2:], (30, 24.0))

    def test_scope_cleanup_retains_checkpoint_scope_regardless_of_phase(self):
        pipeline_id = "94368468-bc2d-4797-868d-0cb9e19a5610"
        current = f"{pipeline_id}_@_33333333-3333-4333-8333-333333333333"
        retained = f"{pipeline_id}_@_11111111-1111-4111-8111-111111111111"
        connector = self.connector(FakeBridge())
        connector.set_registration_scope(current)
        table = connector._table("app.orders", {})
        connection = object()
        connector._lakebase_connection = lambda: connection

        with mock.patch.object(
            informix_module, "delete_obsolete_scoped_state_records", return_value=1
        ) as cleanup:
            checkpoint = {"phase": "stream", "pipeline_scope": retained}
            connector._cleanup_previous_update_scopes(table, checkpoint)
            connector._cleanup_previous_update_scopes(table, checkpoint)

        cleanup.assert_called_once_with(
            connection,
            connector._lakebase_state_namespace(),
            connector._immutable_namespace(table),
            f"{pipeline_id}_@_",
            current,
            retained,
        )

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
        source = spark.dataSource.source(
            {
                "hostname": "informix.example",
                "server": "ol_informix",
                "database": "demo",
                "snapshot.staging.location": "/Volumes/test/default/informix_state",
                "lakebase.password": "test-state-password",
            }
        )
        self.assertRegex(source.lakeflow_connect._registration_scope, r"^[0-9a-f]{32}$")
        self.assertIsNotNone(source.lakeflow_connect._registration_table_cache_key())
        method = spark.dataSource.source.simpleStreamReader
        closure = dict(
            zip(method.__code__.co_freevars, (cell.cell_contents for cell in method.__closure__))
        )
        reader_type = closure["LakeflowStreamReader"]
        reader = reader_type.__new__(reader_type)
        reader.lakeflow_connect = mock.Mock()

        reader.prepareForTriggerAvailableNow()

        reader.lakeflow_connect.prepare_for_trigger_available_now.assert_called_once_with()

    def test_generated_source_resolves_module_globals_from_inside_the_wrapper(self):
        """The merged bundle must reach its module-level names at call time.

        ``merge_python_source`` nests this whole module inside
        ``register_lakeflow_source``, which demotes every module global to a local
        of that function. Two things broke in production because of it: a helper
        that rebound a global with ``global`` raised ``NameError`` (the ``global``
        declaration addresses real module globals, which the merge never assigns),
        and ``sys.modules[__name__]`` could not serve as a fallback namespace for
        the inlined Lakebase functions because they are locals too, not module
        attributes. Registering the source does not catch either -- both only fire
        when a method actually runs -- so this test calls through to them.
        """

        generated = importlib.import_module(
            "databricks.labs.community_connector.sources.informix."
            "_generated_informix_python_source"
        )

        # Recover the wrapper's namespace: the definitions live in its frame, so
        # there is no other way to assert on them as the merged code sees them.
        namespace = {}

        def capture(frame, event, _arg):
            if frame.f_code.co_name != "register_lakeflow_source":
                return None
            if event == "call":
                return capture
            if event == "return":
                namespace.update(frame.f_locals)
            return None

        spark = types.SimpleNamespace(dataSource=mock.Mock())
        sys.settrace(capture)
        try:
            generated.register_lakeflow_source(spark)
        finally:
            sys.settrace(None)
        self.assertTrue(namespace, "did not capture the wrapper's namespace")

        # Every Lakebase symbol the connector calls must be inlined and bound.
        for name in (
            "LakebaseState",
            "acquire_slot",
            "heartbeat_slot",
            "release_slot",
            "seed_slots",
            "publish_backlog_hint",
            "read_backlog_hints",
            "publish_connection_limit",
            "publish_state_record",
            "read_state_record",
        ):
            self.assertIn(name, namespace, f"{name} is unreachable in the merged bundle")

        # The lock helper is the exact call that raised NameError. It must return a
        # usable lock, and the same one every time, or the waiter caches it guards
        # would not actually be serialised.
        lock = namespace["_lakebase_waiter_lock"]()
        self.assertIs(lock, namespace["_lakebase_waiter_lock"]())
        with lock:
            pass

        # Drive the full waiter path that failed in the pipeline. It is advisory and
        # swallows everything, so assert on the swallowed exception: a NameError
        # here is the regression, while a credentials error means the code ran.
        swallowed = []

        class Collect(logging.Handler):
            def emit(self, record):
                if record.exc_info:
                    swallowed.append(record.exc_info[0])

        handler = Collect()
        logger = logging.getLogger("databricks.labs.community_connector.sources.informix.informix")
        root = logging.getLogger()
        for target in (logger, root):
            target.addHandler(handler)
            self.addCleanup(target.removeHandler, handler)
        previous = root.level
        root.setLevel(logging.DEBUG)
        self.addCleanup(root.setLevel, previous)

        bridge = namespace["PurePythonInformixBridge"]
        # Force the credential resolver down its fast fail-open branch. Without a
        # workspace host/token the connector falls back to constructing a real
        # ``databricks.sdk.WorkspaceClient``, which — when the SDK happens to be
        # installed — blocks for minutes on network host-metadata resolution with
        # retry/backoff before the advisory path finally swallows the error. The
        # SDK is absent on CI (it is not in ``requirements/sources.txt``), so there
        # the lazy import already raises ``ImportError`` instantly; block it here
        # too so the test is fast and deterministic in either environment. This is
        # exactly the path the assertions below verify: the code runs far enough to
        # resolve its module globals, then fails on credentials rather than a
        # ``NameError``.
        blocker = _BlockImport("databricks.sdk")
        # A finder is only consulted for modules not already in sys.modules, so
        # evict any cached databricks.sdk* first, then restore them afterward.
        cached_sdk = {
            name: module
            for name, module in list(sys.modules.items())
            if name == "databricks.sdk" or name.startswith("databricks.sdk.")
        }
        for name in cached_sdk:
            del sys.modules[name]
        sys.meta_path.insert(0, blocker)
        try:
            hint = bridge._read_connection_backlog_hint_at(
                {"hostname": "informix.example", "port": "9089", "server": "ol_informix"}
            )
        finally:
            sys.meta_path.remove(blocker)
            sys.modules.update(cached_sdk)

        self.assertIsNone(hint)
        self.assertNotIn(NameError, swallowed)

    def test_insert_update_delete_pk_change_rollback_discard_and_controls(self):
        bridge = FakeBridge()
        bridge.now = 200
        bridge.changes = [
            {"op": "METADATA"},
            {"op": "TIMEOUT", "lsn": 89},
            {"op": "BEGIN", "tx_id": 1, "lsn": 100},
            {"op": "INSERT", "tx_id": 1, "lsn": 101, "row": {"id": 1, "value": "a"}},
            {"op": "BEFORE_UPDATE", "tx_id": 1, "lsn": 102, "row": {"id": 1, "value": "a"}},
            {"op": "AFTER_UPDATE", "tx_id": 1, "lsn": 103, "row": {"id": 2, "value": "b"}},
            {"op": "DELETE", "tx_id": 1, "lsn": 104, "row": {"id": 2, "value": "b"}},
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
            {"op": "INSERT", "tx_id": 7, "lsn": 101, "row": {"id": 1, "value": "pending"}},
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
            {"op": "INSERT", "tx_id": 8, "lsn": 107, "row": {"id": 1, "value": "later"}},
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
        first = self.connector(first_bridge, **common, tableName="app.orders", isDeleteFlow="false")
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
        first_boundary = connector._trigger_boundaries["demo.app.orders"]
        first_generation = first_boundary[1]
        next_checkpoint = {
            **first_end,
            "begin_lsn": "105",
            "change_lsn": "105",
            "commit_lsn": "105",
            "trigger_generation": first_generation,
            "trigger_high_water": str(first_boundary[0]),
        }

        bridge.now = 120
        connector.set_registration_scope("e" * 32)
        connector.prepare_for_trigger_available_now()
        connector.read_table("app.orders", next_checkpoint, {})

        self.assertEqual(connector._trigger_boundaries["demo.app.orders"][0], 120)
        self.assertNotEqual(connector._trigger_boundaries["demo.app.orders"][1], first_generation)

    def test_trigger_cache_stays_frozen_for_checkpointed_generation(self):
        bridge = FakeBridge()
        connector = self.connector(bridge)
        _, checkpoint = connector.read_table("app.orders", {}, {})
        table = Table.parse(_table(), "demo")
        first = connector._shared_trigger_boundary(table, checkpoint, owner=True)

        bridge.now = first[0] + 10
        next_checkpoint = {
            **checkpoint,
            "commit_lsn": str(first[0]),
            "change_lsn": str(first[0]),
            "begin_lsn": str(first[0]),
            "trigger_generation": first[1],
            "trigger_high_water": str(first[0]),
        }
        second = connector._shared_trigger_boundary(table, next_checkpoint, owner=True)

        self.assertEqual(second, first)

    def test_trigger_cache_rejects_checkpoint_past_frozen_boundary(self):
        connector = self.connector(FakeBridge())
        _, checkpoint = connector.read_table("app.orders", {}, {})
        table = Table.parse(_table(), "demo")
        connector._trigger_boundaries[table.identity] = (
            100,
            "a" * 32,
            checkpoint["pipeline_scope"],
        )
        checkpoint = {
            **checkpoint,
            "begin_lsn": "101",
            "change_lsn": "101",
            "commit_lsn": "101",
        }

        with self.assertRaisesRegex(InformixError, "Cached trigger boundary"):
            connector._shared_trigger_boundary(table, checkpoint, owner=True)

    def test_trigger_boundary_survives_schema_transition_within_update(self):
        bridge = FakeBridge()
        bridge.now = 105
        owner = self.connector(bridge)
        _, checkpoint = owner.read_table("app.orders", {}, {})
        table = Table.parse(_table(), "demo")
        boundary = owner._shared_trigger_boundary(table, checkpoint, owner=True)

        transitioned = {**checkpoint, "schema_id": "f" * 32}
        delete_reader = self.connector(FakeBridge())
        recovered = delete_reader._shared_trigger_boundary(table, transitioned, owner=False)

        self.assertEqual(recovered, boundary)

    def test_available_now_reuses_frozen_boundary_after_generation_is_checkpointed(self):
        bridge = FakeBridge()
        _, checkpoint = self.connector(bridge).read_table("app.orders", {}, {})
        bridge.now = 105
        connector = self.connector(bridge)
        connector.prepare_for_trigger_available_now()
        table = Table.parse(_table(), "demo")

        first = connector._shared_trigger_boundary(table, checkpoint, owner=True)
        bridge.now = 125
        advanced = {
            **checkpoint,
            "trigger_generation": first[1],
            "trigger_high_water": str(first[0]),
        }
        second = connector._shared_trigger_boundary(table, advanced, owner=True)

        self.assertEqual(second, first)

    def test_concurrent_pipelines_keep_trigger_boundaries_isolated(self):
        seed_bridge = FakeBridge()
        _, seed = self.connector(seed_bridge).read_table("app.orders", {}, {})
        checkpoint_a = {
            **seed,
            "trigger_generation": "a" * 32,
            "trigger_high_water": seed["commit_lsn"],
        }
        checkpoint_b = {
            **seed,
            "trigger_generation": "b" * 32,
            "trigger_high_water": seed["commit_lsn"],
        }

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

    def test_divergent_trigger_predecessors_share_current_update_boundary(self):
        bridge = FakeBridge()
        _, seed = self.connector(bridge).read_table("app.orders", {}, {})
        upsert_checkpoint = {
            **seed,
            "trigger_generation": "a" * 32,
            "trigger_high_water": seed["commit_lsn"],
        }
        delete_checkpoint = {
            **seed,
            "trigger_generation": "b" * 32,
            "trigger_high_water": seed["commit_lsn"],
        }
        bridge.now = 125
        upsert = self.connector(bridge)
        upsert.prepare_for_trigger_available_now()
        upsert.read_table("app.orders", upsert_checkpoint, {})
        delete = self.connector(FakeBridge())
        table = Table.parse(_table(), "demo")
        recovered = delete._shared_trigger_boundary(table, delete_checkpoint, owner=False)

        self.assertEqual(recovered, upsert._trigger_boundaries[table.identity][:2])

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
            {"op": "INSERT", "tx_id": 9, "lsn": 107, "row": {"id": 1, "value": "later"}},
            {"op": "COMMIT", "tx_id": 9, "lsn": 110},
        ]
        connector = self.connector(bridge)

        changes, end = connector.read_table("app.orders", _stream_offset(100), {})

        self.assertEqual([row["id"] for row in changes], [1])
        self.assertEqual(end["commit_lsn"], "110")

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

    def test_schema_recovery_rejects_wrong_embedded_node_id(self):
        connector = self.connector(FakeBridge())
        table = Table.parse(_table(), "demo")
        checkpoint = _stream_offset()
        connector._publish_immutable_head(
            connector._immutable_namespace(table, "schema-nodes", checkpoint["schema_id"]),
            {
                "created_at": 1.0,
                "schema": _schema_state(table, 100, schema_id="2" * 32),
                "scope": checkpoint["pipeline_scope"],
            },
            record_type="schema-node",
        )

        with self.assertRaisesRegex(InformixError, "conflicts with immutable history"):
            connector.read_table("app.orders", checkpoint, {"snapshot.mode": "recovery"})

    def test_existing_same_layout_schema_node_may_start_after_checkpoint(self):
        connector = self.connector(FakeBridge())
        table = Table.parse(_table(), "demo")
        checkpoint = _stream_offset(100)
        connector._publish_immutable_head(
            connector._immutable_namespace(table, "schema-nodes", checkpoint["schema_id"]),
            {
                "created_at": 1.0,
                "schema": _schema_state(table, 110, schema_id=checkpoint["schema_id"]),
                "scope": checkpoint["pipeline_scope"],
            },
            record_type="schema-node",
        )

        rows, end = connector.read_table("app.orders", checkpoint, {})

        self.assertEqual(list(rows), [])
        self.assertEqual(end["schema_id"], checkpoint["schema_id"])

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

    def test_state_root_creation_fsyncs_existing_parent(self):
        parent = tempfile.TemporaryDirectory()
        self.addCleanup(parent.cleanup)
        root = os.path.join(parent.name, "new-state")
        path = os.path.join(root, "nested")
        with mock.patch.object(informix_module.os, "fsync") as fsync:
            informix_module._makedirs_durable(root, path)
        self.assertGreaterEqual(fsync.call_count, 3)

    def test_state_root_creation_supports_multiple_missing_components(self):
        parent = tempfile.TemporaryDirectory()
        self.addCleanup(parent.cleanup)
        root = os.path.join(parent.name, "team", "informix", "state")
        path = os.path.join(root, "nested", "namespace")

        informix_module._makedirs_durable(root, path)

        self.assertTrue(os.path.isdir(path))

    def test_immutable_header_rejects_float_version(self):
        with self.assertRaisesRegex(InformixError, "Unsupported or mismatched"):
            informix_module.InformixLakeflowConnect._validate_immutable_record_header(
                {"format_version": 1.0, "record_type": "trigger"},
                "trigger",
                "test",
            )

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

    def test_delete_trigger_missing_boundary_yields_immediately(self):
        connector = self.connector(FakeBridge())
        table = Table.parse(_table(), "demo")
        checkpoint = _stream_offset()
        with (
            mock.patch.object(
                informix_module.os,
                "scandir",
                side_effect=AssertionError("trigger history must not be scanned"),
            ),
            mock.patch.object(informix_module.time, "sleep") as sleep,
            self.assertRaisesRegex(TriggerBoundaryUnavailable, "has not published"),
        ):
            connector._shared_trigger_boundary(table, checkpoint, owner=False)
        sleep.assert_not_called()

    def test_delete_schema_transition_yields_without_holding_a_slot(self):
        """A delete reader must not sleep on a held slot waiting for the upsert reader.

        Reaching _schema_transition has already acquired a connection slot (the
        schema refresh in _read_stream connects first), and the upsert reader that
        must publish the transition needs a slot of its own. Because both channels
        can claim every slot at the default reservation of 0, waiting here while
        holding one is a genuine deadlock. The reader must yield instead.
        """

        class SlotTrackingBridge(FakeBridge):
            def __init__(self):
                super().__init__()
                self.slot_held = False
                self.slot_held_while_waiting = []

            def _connect(self):
                self.slot_held = True

            def get_table(self, identity):
                self._connect()
                return super().get_table(identity)

            def current_lsn(self):
                self._connect()
                return super().current_lsn()

            def minimum_lsn(self):
                self._connect()
                return super().minimum_lsn()

            def release_connection(self):
                self.slot_held = False
                return super().release_connection()

        bridge = SlotTrackingBridge()
        _, checkpoint = self.connector(bridge).read_table("app.orders", {}, {})
        # Evolve the schema so the delete reader takes the transition path.
        bridge.tables[0]["columns"].append(
            {"name": "added", "type_name": "INTEGER", "nullable": True}
        )
        bridge.now = 120
        delete = self.connector(bridge)

        def record_wait(deadline, delay):
            bridge.slot_held_while_waiting.append(bridge.slot_held)
            raise AssertionError("the delete channel must not wait for a schema transition")

        with mock.patch.object(informix_module, "_sleep_with_backoff", record_wait):
            with mock.patch.object(informix_module.time, "sleep"):
                rows, offset = delete.read_table_deletes("app.orders", checkpoint, {})

        self.assertEqual(list(rows), [])
        self.assertEqual(bridge.slot_held_while_waiting, [])
        self.assertFalse(bridge.slot_held)
        self.assertEqual(offset["schema_transition_retry_count"], 1)
        self.assertEqual(
            {key: value for key, value in offset.items() if key != "schema_transition_retry_count"},
            checkpoint,
        )

    def test_delete_schema_transition_resumes_once_the_owner_publishes(self):
        bridge = FakeBridge()
        _, checkpoint = self.connector(bridge).read_table("app.orders", {}, {})
        bridge.tables[0]["columns"].append(
            {"name": "added", "type_name": "INTEGER", "nullable": True}
        )
        bridge.now = 120
        bridge.changes = [{"op": "TIMEOUT", "lsn": 120}]

        with mock.patch.object(informix_module.time, "sleep"):
            _, yielded = self.connector(bridge).read_table_deletes("app.orders", checkpoint, {})
        self.assertEqual(yielded["schema_transition_retry_count"], 1)

        # The owning upsert reader publishes the transition.
        self.connector(bridge).read_table("app.orders", checkpoint, {})

        rows, resumed = self.connector(bridge).read_table_deletes("app.orders", yielded, {})

        self.assertEqual(list(rows), [])
        # The retry counter is cleared once the reader is served, and the offset
        # advances past the transition rather than repeating the yield.
        self.assertNotIn("schema_transition_retry_count", resumed)

    def test_upsert_schema_transition_still_waits_for_its_own_publication(self):
        """The owner keeps its retry loop: it is the writer, not a waiter."""

        bridge = FakeBridge()
        connector = self.connector(bridge)
        table = Table.parse(_table(), "demo")
        _, checkpoint = connector.read_table("app.orders", {}, {})
        waits = []

        original = informix_module._sleep_with_backoff

        def spy(deadline, delay):
            waits.append(delay)
            return original(deadline, delay)

        # An owner whose publication keeps losing to a concurrent writer must
        # still retry rather than yield; only the non-owner path fast-outs.
        with mock.patch.object(informix_module, "_sleep_with_backoff", spy):
            with mock.patch.object(connector, "_read_immutable_head", return_value=None):
                with self.assertRaisesRegex(InformixError, "Schema history .* is missing"):
                    connector._schema_transition(table, checkpoint["schema_id"], 100, owner=True)

    def test_delete_trigger_missing_boundary_advances_retry_checkpoint(self):
        connector = self.connector(FakeBridge())
        connector.prepare_for_trigger_available_now()
        checkpoint = _stream_offset()

        with mock.patch.object(informix_module.time, "sleep"):
            rows, first = connector.read_table_deletes("app.orders", checkpoint, {})
            rows_again, second = connector.read_table_deletes("app.orders", first, {})

        self.assertEqual(list(rows), [])
        self.assertEqual(list(rows_again), [])
        self.assertEqual(first["trigger_boundary_retry_count"], 1)
        self.assertEqual(second["trigger_boundary_retry_count"], 2)
        self.assertEqual(
            {key: value for key, value in second.items() if key != "trigger_boundary_retry_count"},
            checkpoint,
        )

    def test_delete_trigger_retry_counter_clears_after_owner_publishes(self):
        bridge = FakeBridge()
        owner = self.connector(bridge)
        delete = self.connector(bridge)
        owner.prepare_for_trigger_available_now()
        delete.prepare_for_trigger_available_now()
        checkpoint = _stream_offset()
        table = Table.parse(_table(), "demo")

        owner._shared_trigger_boundary(table, checkpoint, owner=True)
        rows, end = delete.read_table_deletes(
            "app.orders",
            {**checkpoint, "trigger_boundary_retry_count": 4},
            {},
        )

        self.assertEqual(list(rows), [])
        self.assertNotIn("trigger_boundary_retry_count", end)

    def test_boolean_options_reject_unknown_values(self):
        required = {
            "hostname": "localhost",
            "database": "demo",
            "user": "user",
            "password": "password",
            "server": "server",
        }
        for name in ("encrypt", "padVarchar", "redirect.enabled"):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, name):
                if name == "redirect.enabled":
                    informix_module._option_bool({name: "treu"}, name, False)
                else:
                    informix_module._bridge_config({**required, name: "treu"})

    def test_schema_history_recovery_never_scans_unrelated_transitions(self):
        # Recovery must address the one record it wants by key. Scanning would make
        # an unrelated transition able to break a lookup that has nothing to do
        # with it, so an unrelated record is planted and the scan path is poisoned.
        connector = self.connector(FakeBridge())
        table = Table.parse(_table(), "demo")
        connector._publish_immutable_head(
            connector._immutable_namespace(table, "schemas", "unrelated"),
            {"schema": {"start_lsn": "1"}},
            record_type="schema",
        )

        with mock.patch.object(
            informix_module.os, "scandir", side_effect=AssertionError("must not scan")
        ):
            self.assertIsNone(connector._find_immutable_schema_record(table, "a" * 32, "b" * 32))

    def test_initial_schema_node_recovery_accepts_progressed_checkpoint(self):
        bridge = FakeBridge()
        connector = self.connector(bridge)
        _, checkpoint = connector.read_table("app.orders", {}, {})
        table = Table.parse(_table(), "demo")
        node_path = connector._immutable_namespace(table, "schema-nodes", checkpoint["schema_id"])
        # Drop the elected record to simulate state lost before this reader ran.
        self._lakebase.database.records.clear()
        checkpoint_lsn = min(100, bridge.now)

        replacement = self.connector(bridge)
        replacement._record_current_schema(
            table,
            checkpoint_lsn,
            checkpoint["schema_id"],
            checkpoint["schema_fingerprint"],
            checkpoint["pipeline_scope"],
            owner=True,
        )

        restored = replacement._read_immutable_head(node_path)
        self.assertLessEqual(int(restored["schema"]["start_lsn"]), checkpoint_lsn)

    def test_malformed_trigger_winner_fails_before_caching(self):
        connector = self.connector(FakeBridge())
        table = Table.parse(_table(), "demo")
        checkpoint = _stream_offset()
        scope = checkpoint["pipeline_scope"]
        connector._publish_immutable_head(
            connector._immutable_namespace(table, "triggers", "scopes", scope),
            {
                "checkpoint_lsn": checkpoint["commit_lsn"],
                "generation": "invalid",
                "high_water": "120",
                "schema_id": checkpoint["schema_id"],
                "scope": scope,
            },
            record_type="trigger",
        )

        with self.assertRaisesRegex(InformixError, "trigger generation"):
            connector._shared_trigger_boundary(table, checkpoint, owner=True)
        self.assertNotIn(table.identity, connector._trigger_boundaries)

    def test_trigger_winner_rejects_wrong_scope_and_invalid_lsn(self):
        for wrong_scope, high_water, message in (
            (True, "120", "trigger identity"),
            (False, "not-an-lsn", "trigger boundary"),
        ):
            with self.subTest(wrong_scope=wrong_scope, high_water=high_water):
                connector = self.connector(FakeBridge())
                table = Table.parse(_table(), "demo")
                checkpoint = _stream_offset()
                checkpoint["pipeline_scope"] = hashlib.sha256(high_water.encode()).hexdigest()[:32]
                scope = checkpoint["pipeline_scope"]
                connector.set_registration_scope(scope)
                connector._publish_immutable_head(
                    connector._immutable_namespace(table, "triggers", "scopes", scope),
                    {
                        "checkpoint_lsn": checkpoint["commit_lsn"],
                        "generation": "a" * 32,
                        "high_water": high_water,
                        "schema_id": checkpoint["schema_id"],
                        "scope": "f" * 32 if wrong_scope else scope,
                    },
                    record_type="trigger",
                )

                with self.assertRaisesRegex(InformixError, message):
                    connector._shared_trigger_boundary(table, checkpoint, owner=True)


class IncrementalSnapshotTests(unittest.TestCase):
    """Contract regressions for the default incremental (chunked) snapshot."""

    def setUp(self):
        self._shared_state = tempfile.TemporaryDirectory()
        self._lakebase = _OfflineLakebase().install(self)

    def tearDown(self):
        self._shared_state.cleanup()

    def connector(self, bridge=None, **options):
        scope_label = str(options.pop("registration_scope", "incremental-pipeline"))
        connector = InformixLakeflowConnect(
            {
                "database": "demo",
                "snapshot.staging.location": self._shared_state.name,
                "lakebase.password": "test-state-password",
                # Pin the direct/inline path (see LakeflowContractTests.connector): these
                # tests inject a bridge directly and must not start a daemon reader.
                "cdc.shared.session": "false",
                "snapshot.shared.session": "false",
                **options,
            }
        )
        connector.set_registration_scope(hashlib.sha256(scope_label.encode()).hexdigest()[:32])
        connector._bridge_instance = bridge or FakeBridge()
        return connector

    def _drain(self, connector, options, start=None):
        """Read microbatches until the incremental block clears; return all rows."""

        offset = start or {}
        collected = []
        for _ in range(100):
            rows, offset = connector.read_table("app.orders", offset, options)
            collected.extend(rows)
            if "incremental" not in offset:
                break
        else:
            self.fail("incremental snapshot did not converge")
        return collected, offset

    def test_incremental_is_the_default_mode(self):
        connector = self.connector()
        self.assertEqual(connector._snapshot_mode({}), "incremental")

    def test_incremental_snapshot_persists_and_reuses_snapshot_filter(self):
        bridge = FakeBridge()
        connector = self.connector(bridge)

        _, offset = connector.read_table(
            "app.orders",
            {},
            {"snapshot.page.size": "1", "snapshot.filter": "status = 'A'"},
        )

        self.assertEqual(offset["incremental"]["snapshot_filter"], "status = 'A'")
        self.assertEqual(bridge.snapshot_filters, ["status = 'A'", "status = 'A'"])
        bridge.snapshot_filters.clear()
        connector.read_table("app.orders", offset, {"snapshot.page.size": "1"})
        self.assertEqual(
            bridge.snapshot_filters,
            ["status = 'A'"],
            "a resumed copy must use its checkpointed filter even if options change",
        )

    def test_an_unfinished_incremental_copy_records_a_backlog_streak(self):
        # Regression, observed in production: after a full refresh every reader sat
        # in the incremental phase and NONE recorded a backlog streak, because the
        # signal was only wired into the CDC row-budget break. The bulk-copy phase
        # is exactly when a flow has the most outstanding work, so a reader whose
        # chunk cursor has not reached the snapshot's upper bound must rank above an
        # idle one -- and here the evidence is exact, not an estimate.
        bridge = FakeBridge()
        connector = self.connector(bridge, **{"snapshot.mode": "incremental"})

        # One row per chunk, so the copy takes several microbatches to converge.
        options = {"snapshot.page.size": "1"}
        offset: dict = {}
        streaks = []
        for _ in range(100):
            _, offset = connector.read_table("app.orders", offset, options)
            if "incremental" not in offset:
                break
            streaks.append(offset.get("backlog_streak"))
        else:
            self.fail("incremental snapshot did not converge")

        self.assertTrue(streaks, "the copy completed in one batch; nothing to assert")
        self.assertTrue(
            all(s is not None and s >= 1 for s in streaks),
            f"an in-progress incremental copy recorded no backlog streak: {streaks}",
        )
        # Consecutive in-progress batches accumulate, up to the saturation cap.
        self.assertEqual(streaks, sorted(streaks), f"streak did not accumulate: {streaks}")
        # Finishing the copy clears it: priority must not outlive the backlog.
        self.assertNotIn(
            "backlog_streak",
            offset,
            "a completed incremental copy kept a stale backlog streak",
        )

    def test_a_mid_incremental_reader_outranks_an_idle_one(self):
        # The point of the signal: it must actually change the rank a waiter uses,
        # not merely appear in the offset.
        bridge = FakeBridge()
        connector = self.connector(bridge, **{"snapshot.mode": "incremental"})
        _, offset = connector.read_table("app.orders", {}, {"snapshot.page.size": "1"})
        self.assertIn("incremental", offset, "expected an in-progress copy")

        mid_copy = informix_module._backlog_rank(0, offset["backlog_streak"])
        idle = informix_module._backlog_rank(0, 0)
        self.assertGreater(mid_copy, idle)

    def test_incremental_chunks_stream_snapshot_rows_in_pk_order(self):
        bridge = FakeBridge()
        bridge.rows = [{"id": i, "value": str(i)} for i in range(1, 6)]
        connector = self.connector(bridge, **{"snapshot.page.size": "2"})

        rows, offset = self._drain(connector, {"snapshot.page.size": "2"})

        self.assertEqual([row["id"] for row in rows], [1, 2, 3, 4, 5])
        self.assertTrue(all(row["_informix_op"] == "r" for row in rows))
        self.assertEqual(offset["phase"], "stream")
        self.assertNotIn("incremental", offset)

    def test_incremental_snapshot_begins_streaming_immediately(self):
        bridge = FakeBridge()
        bridge.rows = [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}]
        connector = self.connector(bridge, **{"snapshot.page.size": "1"})

        _, offset = connector.read_table("app.orders", {}, {"snapshot.page.size": "1"})

        # CDC runs from the boundary during the snapshot: the very first
        # microbatch is already in the stream phase, unlike the blocking mode.
        self.assertEqual(offset["phase"], "stream")
        self.assertIn("incremental", offset)
        self.assertFalse(offset["incremental"]["done"])

    def test_incremental_interleaves_committed_changes_with_chunks(self):
        bridge = FakeBridge()
        bridge.rows = [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}]
        bridge.changes = [
            {"op": "BEGIN", "tx_id": 7, "lsn": 121},
            {"op": "INSERT", "tx_id": 7, "lsn": 122, "row": {"id": 9, "value": "new"}},
            {"op": "COMMIT", "tx_id": 7, "lsn": 123},
        ]
        connector = self.connector(bridge, **{"snapshot.page.size": "1"})

        rows, offset = connector.read_table("app.orders", {}, {"snapshot.page.size": "1"})
        rows = list(rows)

        ops = {(row["id"], row["_informix_op"]) for row in rows}
        # First chunk row (r) and the concurrently committed insert (c) both
        # appear in the same microbatch.
        self.assertIn((1, "r"), ops)
        self.assertIn((9, "c"), ops)
        self.assertEqual(offset["commit_lsn"], "123")

    def test_concurrent_change_supersedes_snapshot_row_by_lsn(self):
        bridge = FakeBridge()
        bridge.rows = [{"id": 1, "value": "stale"}]
        bridge.changes = [
            {"op": "BEGIN", "tx_id": 3, "lsn": 121},
            {"op": "INSERT", "tx_id": 3, "lsn": 122, "row": {"id": 1, "value": "fresh"}},
            {"op": "COMMIT", "tx_id": 3, "lsn": 123},
        ]
        connector = self.connector(bridge, **{"snapshot.page.size": "10"})

        rows, _ = connector.read_table("app.orders", {}, {"snapshot.page.size": "10"})
        rows = list(rows)

        snapshot_row = next(r for r in rows if r["_informix_op"] == "r" and r["id"] == 1)
        change_row = next(r for r in rows if r["_informix_op"] == "c" and r["id"] == 1)
        # The change carries a strictly higher change LSN, so apply_changes
        # sequence-merge keeps 'fresh' as the final value without a dedup window.
        self.assertLess(snapshot_row["_informix_change_lsn"], change_row["_informix_change_lsn"])

    def test_incremental_leaves_rows_beyond_max_key_to_the_stream(self):
        bridge = FakeBridge()
        bridge.rows = [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}]
        connector = self.connector(bridge, **{"snapshot.page.size": "10"})

        # A row inserted past the captured max key after the snapshot starts is
        # not copied by the chunk reader; the change stream carries it.
        rows, offset = connector.read_table("app.orders", {}, {"snapshot.page.size": "10"})
        snapshot_ids = [row["id"] for row in rows if row["_informix_op"] == "r"]

        self.assertEqual(snapshot_ids, [1, 2])
        self.assertNotIn("incremental", offset)

    def test_incremental_cursor_never_advances_past_an_unemitted_row(self):
        # Regression, observed in production: tw241 lost exactly 5 rows
        # (agt_no 59964..59968) at the boundary between two incremental chunks.
        # Chunk 3 persisted 20000 rows ending at 59963 and chunk 4 resumed at
        # 59969 -- so ``last_pk`` named a row the reader never emitted, and the
        # strictly-greater keyset predicate skipped everything between forever.
        #
        # The invariant that makes a chunked copy complete: ``last_pk`` is a
        # promise that every row up to and including it has been delivered. Any
        # future truncation between the fetch and the emitted list -- a byte cap,
        # a row cap, a filter -- breaks that promise silently, because a skipped
        # keyset range is indistinguishable from an empty one.
        #
        # Assert the promise directly against the bridge: whatever the reader
        # names as its cursor must be a row the caller actually received.
        bridge = FakeBridge()
        bridge.rows = [{"id": index, "value": str(index)} for index in range(1, 26)]
        connector = self.connector(bridge, **{"snapshot.page.size": "5"})
        options = {"snapshot.page.size": "5"}

        delivered: list[int] = []
        offset: dict = {}
        for _ in range(100):
            rows, offset = connector.read_table("app.orders", offset, options)
            delivered.extend(row["id"] for row in rows if row["_informix_op"] == "r")
            incremental = offset.get("incremental")
            if incremental is None:
                break
            cursor = incremental.get("last_pk")
            if cursor is None:
                continue
            # The cursor is the high-water mark of *delivered* rows, so every
            # source row at or below it must already be in hand. Comparing
            # against the source (not merely against the batch) is what catches
            # a cursor that jumped a gap.
            reached = informix_module._decode_snapshot_stage_value(cursor)[0]
            owed = [row["id"] for row in bridge.rows if row["id"] <= reached]
            self.assertEqual(
                sorted(delivered),
                sorted(owed),
                f"cursor advanced to id={reached} but these rows were never "
                f"emitted: {sorted(set(owed) - set(delivered))}",
            )
        else:
            self.fail("incremental snapshot did not converge")

        self.assertEqual(sorted(delivered), [row["id"] for row in bridge.rows])

    def test_incremental_chunk_emits_every_row_it_fetched(self):
        # The narrower mechanical guard behind the invariant above: the reader
        # must not drop rows between the bridge fetch and the emitted batch.
        # In production chunk 3 fetched a page whose last row was 59968 while
        # persisting only through 59963 -- five fetched rows discarded, and the
        # cursor left naming the discarded end.
        #
        # Count what the bridge handed over and what came back out. They may
        # differ only for rows above ``max_pk``, which the change stream owns.
        bridge = FakeBridge()
        bridge.rows = [{"id": index, "value": str(index)} for index in range(1, 26)]
        fetched: list[int] = []
        original = bridge.snapshot_chunk

        def recording_chunk(*args, **kwargs):
            lsn, rows = original(*args, **kwargs)
            fetched.extend(row["id"] for row in rows)
            return lsn, rows

        bridge.snapshot_chunk = recording_chunk
        connector = self.connector(bridge, **{"snapshot.page.size": "5"})

        delivered, _ = self._drain(connector, {"snapshot.page.size": "5"})
        emitted = [row["id"] for row in delivered if row["_informix_op"] == "r"]

        self.assertTrue(fetched, "the bridge was never asked for a chunk")
        self.assertEqual(
            sorted(set(fetched)),
            sorted(set(emitted)),
            "rows fetched from the source were dropped before being emitted",
        )

    def _replayed_chunk(self, connector, options, start, end):
        """Replay ``[start, end)`` the way readBetweenOffsets does: bounded by
        both the committed LSN and the committed chunk cursor."""

        replay = dict(options)
        replay[informix_module._REPLAY_STOP_LSN_OPTION] = str(end["commit_lsn"])
        stop_pk = (end.get("incremental") or {}).get("last_pk")
        replay[informix_module._REPLAY_STOP_PK_OPTION] = json.dumps(stop_pk)
        return connector.read_table("app.orders", start, replay)

    def test_replayed_range_covers_every_row_its_committed_cursor_claims(self):
        # Regression for the defect behind the tw241 loss. readBetweenOffsets
        # bounds the replayed CDC read by end.commit_lsn, but during the
        # incremental copy the same call ALSO emits a snapshot chunk -- and a
        # chunk is bounded by its keyset cursor, which no LSN constrains.
        #
        # Spark discards the offset a replay returns and commits ``end``
        # regardless. So if the replay's chunk covers less of the key range than
        # end.last_pk claims, the next read resumes past the shortfall and those
        # rows are skipped permanently. Measured on the original code: the
        # original batch emitted ids 5..8 and committed last_pk=8, while a replay
        # after ids 5 and 6 were deleted emitted 7..10 -- leaving 5 and 6 claimed
        # by the commit but never delivered.
        bridge = FakeBridge()
        bridge.rows = [{"id": index, "value": str(index)} for index in range(1, 21)]
        connector = self.connector(bridge, **{"snapshot.page.size": "4"})
        options = {"snapshot.page.size": "4"}

        _, first = connector.read_table("app.orders", {}, dict(options))
        second_rows, second = connector.read_table("app.orders", first, dict(options))
        original = sorted(row["id"] for row in second_rows if row["_informix_op"] == "r")
        self.assertEqual(original, [5, 6, 7, 8], "unexpected second batch")

        # The source moves on before the replay: the head of the replayed range
        # is gone, so an unbounded page slides forward past the committed cursor.
        bridge.rows = [row for row in bridge.rows if row["id"] not in (5, 6)]

        replayed, offset = self._replayed_chunk(connector, options, first, second)
        delivered = sorted(row["id"] for row in replayed if row["_informix_op"] == "r")

        # The replayed range is (first.last_pk, second.last_pk]: rows at or below
        # the start cursor belong to the batch before it and are not re-emitted.
        lower = first["incremental"]["last_pk"][0]
        committed = second["incremental"]["last_pk"][0]
        claimed = [row["id"] for row in bridge.rows if lower < row["id"] <= committed]
        self.assertEqual(
            [row for row in delivered if lower < row <= committed],
            claimed,
            "the replay left rows unclaimed below the cursor Spark commits: "
            f"{sorted(set(claimed) - set(delivered))}",
        )
        # The decisive assertion, and the one the shortfall above cannot make on
        # its own: an unbounded page slides forward and emits rows PAST the
        # committed cursor (measured: 7, 8, 9, 10 for a range committing at 8).
        # Spark commits ``end``, so the cursor it stores no longer describes what
        # was delivered -- and the rows past it are re-read by the next batch
        # while the shortfall behind it never is.
        self.assertLessEqual(
            max(delivered),
            committed,
            f"the replay emitted rows past the committed cursor {committed}: "
            f"{[row for row in delivered if row > committed]}",
        )
        self.assertEqual(
            offset["incremental"]["last_pk"],
            [committed],
            "the replay finished on a different cursor than Spark commits",
        )

    def test_replayed_range_pages_past_a_single_chunk_to_reach_the_bound(self):
        # A replay must not stop at snapshot.page.size. If rows were inserted
        # into the committed key range since the original read, that range no
        # longer fits in one page, and returning one page under-delivers exactly
        # as the unbounded read did. The LSN-bounded CDC path already lifts its
        # row budget for this reason (_REPLAY_UNBOUNDED_ROWS); the chunk path
        # must page until it reaches the committed cursor.
        bridge = FakeBridge()
        bridge.rows = [{"id": index * 10, "value": str(index)} for index in range(1, 21)]
        connector = self.connector(bridge, **{"snapshot.page.size": "4"})
        options = {"snapshot.page.size": "4"}

        _, first = connector.read_table("app.orders", {}, dict(options))
        _, second = connector.read_table("app.orders", first, dict(options))
        committed = second["incremental"]["last_pk"][0]

        # Fill the replayed range with far more rows than one page holds. The
        # seeded ids are multiples of ten, so the gaps between them take the
        # inserts without colliding with a row that already exists.
        lower = first["incremental"]["last_pk"][0]
        existing = {row["id"] for row in bridge.rows}
        for index in range(lower + 1, committed):
            if index not in existing:
                bridge.rows.append({"id": index, "value": f"late-{index}"})

        replayed, _ = self._replayed_chunk(connector, options, first, second)
        delivered = sorted(row["id"] for row in replayed if row["_informix_op"] == "r")
        claimed = sorted(row["id"] for row in bridge.rows if lower < row["id"] <= committed)

        self.assertGreater(len(claimed), 4, "the range must exceed one page")
        self.assertEqual(
            [row for row in delivered if lower < row <= committed],
            claimed,
            "the replay stopped at one page instead of reaching the committed cursor",
        )

    def test_chunk_probes_one_row_past_the_page_to_detect_remaining_rows(self):
        # Fix 2. A page filled to exactly snapshot.page.size is ambiguous: it may
        # be the last page, or there may be more. The blocking consistent_snapshot
        # loop resolves this by asking for page_capacity + 1 and keeping only
        # page_capacity. The chunk path must do the same, rather than inferring
        # completion from a coincidence of the max_pk comparison -- in this code
        # path a wrong answer is silent row loss, not an error.
        #
        # A table whose row count is an exact multiple of the page size is the
        # discriminating case: after the final full page the cursor has NOT
        # reached max_pk (it equals it), so an implementation without the probe
        # depends entirely on the >= max_pk clause to terminate.
        bridge = FakeBridge()
        bridge.rows = [{"id": index, "value": str(index)} for index in range(1, 9)]
        limits: list[int] = []
        original = bridge.snapshot_chunk

        def recording_chunk(identity, columns, primary_keys, after, limit, *args, **kwargs):
            limits.append(limit)
            return original(identity, columns, primary_keys, after, limit, *args, **kwargs)

        bridge.snapshot_chunk = recording_chunk
        connector = self.connector(bridge, **{"snapshot.page.size": "4"})

        delivered, offset = self._drain(connector, {"snapshot.page.size": "4"})
        emitted = sorted(row["id"] for row in delivered if row["_informix_op"] == "r")

        self.assertTrue(limits, "no chunk was fetched")
        self.assertEqual(
            set(limits),
            {5},
            f"the chunk fetch must request page_size + 1 to detect more rows; got {limits}",
        )
        # The probe row is a lookahead, never output: 8 rows in, 8 rows out.
        self.assertEqual(emitted, list(range(1, 9)))
        self.assertNotIn("incremental", offset)

    def test_chunk_cursor_of_the_wrong_arity_is_rejected(self):
        # Fix 3. The cursor and bound are compared against keys read from a table
        # refreshed between the CDC read and the chunk read. A cursor of the wrong
        # arity would compare mismatched tuples -- and because every value still
        # exists, rows would be skipped with no error at all. snapshot_page
        # validates this on the blocking path; the chunk path must too, so a
        # mismatch fails loudly instead of silently truncating the copy.
        bridge = FakeBridge()
        bridge.rows = [{"id": index, "value": str(index)} for index in range(1, 11)]
        connector = self.connector(bridge, **{"snapshot.page.size": "4"})
        options = {"snapshot.page.size": "4"}

        _, offset = connector.read_table("app.orders", {}, dict(options))
        self.assertIn("incremental", offset, "expected an in-progress copy")

        # A second key appears in the cursor while the table still has one.
        corrupt = dict(offset)
        corrupt["incremental"] = dict(offset["incremental"])
        corrupt["incremental"]["last_pk"] = [*offset["incremental"]["last_pk"], 0]

        with self.assertRaises((informix_module.CdcProtocolError, InformixError)):
            connector.read_table("app.orders", corrupt, dict(options))

    def test_replaying_the_range_that_finished_the_copy_drains_to_the_max_key(self):
        # The range that COMPLETED the copy has no incremental block on ``end``,
        # so there is no cursor to stop at. That must not be read as "no bound" --
        # the replay has to drain the rest of the snapshot, up to max_pk, or it
        # under-delivers the final range and the copy is left permanently short.
        # This is why the option distinguishes JSON ``null`` from absent.
        bridge = FakeBridge()
        bridge.rows = [{"id": index, "value": str(index)} for index in range(1, 11)]
        connector = self.connector(bridge, **{"snapshot.page.size": "4"})
        options = {"snapshot.page.size": "4"}

        _, first = connector.read_table("app.orders", {}, dict(options))
        _, second = connector.read_table("app.orders", first, dict(options))
        final_rows, final = connector.read_table("app.orders", second, dict(options))
        self.assertNotIn("incremental", final, "the copy did not finish here")
        original = sorted(row["id"] for row in final_rows if row["_informix_op"] == "r")

        replayed, _ = self._replayed_chunk(connector, options, second, final)
        delivered = sorted(row["id"] for row in replayed if row["_informix_op"] == "r")

        self.assertEqual(
            delivered,
            original,
            "replaying the copy's final range did not reproduce it",
        )
        # Nothing is left behind: the union of the three ranges is the table.
        self.assertEqual(max(delivered), max(row["id"] for row in bridge.rows))

    def test_copy_finishing_replay_pages_until_max_key(self):
        bridge = FakeBridge()
        bridge.rows = [{"id": index * 10, "value": str(index)} for index in range(1, 11)]
        connector = self.connector(bridge, **{"snapshot.page.size": "4"})
        options = {"snapshot.page.size": "4"}

        _, first = connector.read_table("app.orders", {}, dict(options))
        _, second = connector.read_table("app.orders", first, dict(options))
        _, final = connector.read_table("app.orders", second, dict(options))
        self.assertNotIn("incremental", final)

        # Grow the final committed key range beyond one page. JSON null must
        # remain distinguishable from an ordinary unbounded one-page read.
        bridge.rows.extend({"id": index, "value": f"late-{index}"} for index in range(81, 90))
        replayed, reached = self._replayed_chunk(connector, options, second, final)
        delivered = sorted(row["id"] for row in replayed if row["_informix_op"] == "r")

        self.assertEqual(delivered, list(range(81, 90)) + [90, 100])
        self.assertNotIn("incremental", reached)

    def test_first_incremental_range_replay_uses_the_end_cursor(self):
        bridge = FakeBridge()
        bridge.rows = [{"id": index * 10, "value": str(index)} for index in range(1, 11)]
        connector = self.connector(bridge, **{"snapshot.page.size": "4"})
        options = {"snapshot.page.size": "4"}
        _, end = connector.read_table("app.orders", {}, dict(options))
        committed = end["incremental"]["last_pk"][0]

        bridge.rows.extend({"id": index, "value": f"late-{index}"} for index in range(1, 10))
        # Replaying start={} after cancellation establishes a fresh boundary.
        # It may be later than the end Spark planned for the abandoned attempt.
        bridge.now += 10

        class TriggerBase:
            pass

        Wrapped = _informix_available_now_base(TriggerBase)

        class LakeflowStreamReader(Wrapped):
            def __init__(inner):
                inner.lakeflow_connect = connector
                inner.options = {"tableName": "app.orders", **options}

            def read(inner, start):
                return connector.read_table("app.orders", start, dict(inner.options))

        reader = LakeflowStreamReader()
        replayed = list(reader.readBetweenOffsets({}, end))
        delivered = sorted(row["id"] for row in replayed if row["_informix_op"] == "r")

        self.assertEqual(max(delivered), committed)

    def test_retry_metadata_does_not_make_an_initial_replay_checkpointed(self):
        """A schema fallback retry is bookkeeping, not a source position.

        Production delete flows restarted with only this key in their start
        offset. A fresh CDC session legitimately selected a later boundary, but
        replay validation mistook the non-empty dict for a checkpoint and failed
        because the reached LSN was greater than Spark's planned end LSN.
        """

        bridge = FakeBridge()
        connector = self.connector(bridge)
        _, end = connector.read_table("app.orders", {}, {})
        bridge.now += 10

        class TriggerBase:
            pass

        Wrapped = _informix_available_now_base(TriggerBase)

        class LakeflowStreamReader(Wrapped):
            def __init__(inner):
                inner.lakeflow_connect = connector
                inner.options = {"tableName": "app.orders"}

            def read(inner, start):
                return connector.read_table("app.orders", start, dict(inner.options))

        reader = LakeflowStreamReader()

        replayed = list(reader.readBetweenOffsets({"schema_node_fallback_retry_count": 1}, end))

        self.assertEqual([row["id"] for row in replayed], [1, 2])

    def test_incremental_empty_table_completes_without_chunks(self):
        bridge = FakeBridge()
        bridge.rows = []
        connector = self.connector(bridge)

        rows, offset = connector.read_table("app.orders", {}, {})

        self.assertEqual(list(rows), [])
        self.assertEqual(offset["phase"], "stream")
        self.assertNotIn("incremental", offset)

    def test_the_snapshot_bound_is_captured_after_the_stream_boundary(self):
        # The invariant the whole fix rests on. max_pk must be read at or AFTER
        # the CDC start LSN, so a row above max_pk necessarily commits above the
        # start LSN and the change stream that begins there carries it.
        #
        # Invert the order and a row inserted in between sits above the key bound
        # (snapshot disowns it) and below the stream start (CDC never reaches it).
        # That is precisely how tw214 lost four rows: max_pk came from 22:41:58
        # while the boundary was published at 22:46:29.
        #
        # Both calls still happen, so only their ORDER carries the guarantee --
        # assert it directly, the way the chunk-LSN ordering test does.
        order: list[str] = []
        bridge = FakeBridge()
        bridge.rows = [{"id": index, "value": str(index)} for index in range(1, 5)]
        original_max = bridge.max_primary_key

        def recording_max(*args, **kwargs):
            order.append("max_pk")
            return original_max(*args, **kwargs)

        bridge.max_primary_key = recording_max
        connector = self.connector(bridge)
        original_publish = connector._publish_snapshot_boundary

        def recording_publish(*args, **kwargs):
            order.append("boundary")
            return original_publish(*args, **kwargs)

        connector._publish_snapshot_boundary = recording_publish

        connector.read_table("app.orders", {}, {"snapshot.page.size": "2"})

        self.assertIn("boundary", order, "no snapshot boundary was published")
        self.assertIn("max_pk", order, "the snapshot bound was never captured")
        self.assertLess(
            order.index("boundary"),
            order.index("max_pk"),
            "the snapshot key bound must be captured after the stream boundary, "
            "or rows inserted in between are owned by neither side",
        )

    def test_recapturing_a_foreign_bound_keeps_the_progress_cursor(self):
        # Only the BOUND is stale when a block comes from another update; the
        # cursor still names rows this destination holds. The framework discards
        # the offset when it truncates the table -- and a discarded offset takes
        # the whole incremental block with it, so that case arrives as start=None
        # and re-seeds from scratch. Resetting last_pk here would therefore
        # re-read a prefix that is still present, protecting nothing: measured as
        # a 5x throughput drop across restarts (+1.47M rows in one 10-minute
        # cycle against +180K in the next).
        bridge = FakeBridge()
        bridge.rows = [{"id": index, "value": str(index)} for index in range(1, 13)]

        first = self.connector(bridge, registration_scope="update-A")
        emitted, offset = first.read_table("app.orders", {}, {"snapshot.page.size": "4"})
        self.assertEqual(
            sorted(row["id"] for row in emitted if row["_informix_op"] == "r"),
            [1, 2, 3, 4],
        )
        self.assertEqual(offset["incremental"]["last_pk"], [4])

        second = self.connector(bridge, registration_scope="update-B")
        resumed_rows, resumed = second.read_table("app.orders", offset, {"snapshot.page.size": "4"})
        delivered = sorted(row["id"] for row in resumed_rows if row["_informix_op"] == "r")

        # The copy continues past the cursor rather than re-reading the prefix.
        self.assertEqual(
            delivered,
            [5, 6, 7, 8],
            "recapturing the bound also reset the cursor and re-read the prefix",
        )
        self.assertEqual(resumed["incremental"]["last_pk"], [8])

    def test_a_foreign_scopes_snapshot_bound_is_not_reused(self):
        # Regression for the tw214 loss. A cancelled update published
        # max_pk=W0056332 at 22:41:58; the restart published a fresh, higher
        # initial_lsn at 22:46:29 and then ran the copy with the OLD key bound.
        # Rows inserted in between sat ABOVE the stale bound (so the snapshot
        # disowned them) and BELOW the new stream start (so CDC never reached
        # them) -- owned by neither, lost permanently.
        #
        # The invariant: max_pk must be captured at or after the CDC start LSN.
        # Then a row above max_pk necessarily commits above the start LSN and the
        # change stream carries it. Reusing another scope's bound inverts that.
        bridge = FakeBridge()
        bridge.rows = [{"id": index, "value": str(index)} for index in range(1, 9)]

        first = self.connector(bridge, registration_scope="update-A")
        _, offset = first.read_table("app.orders", {}, {"snapshot.page.size": "4"})
        stale = offset["incremental"]["max_pk"]
        self.assertEqual(stale, [8], "unexpected bound from the cancelled update")

        # The cancelled update's copy is discarded (a full refresh truncates the
        # destination) while its checkpoint survives. Rows arrive above the bound,
        # and the source LSN advances past the point that checkpoint recorded.
        for index in (9, 10, 11, 12):
            bridge.rows.append({"id": index, "value": f"late-{index}"})
        bridge.now += 50

        # A different update resumes that checkpoint. It must not adopt the bound.
        second = self.connector(bridge, registration_scope="update-B")
        rows, resumed = second.read_table("app.orders", offset, {"snapshot.page.size": "4"})
        list(rows)

        self.assertIn("incremental", resumed, "the copy should still be in progress")
        self.assertNotEqual(
            resumed["incremental"]["max_pk"],
            stale,
            "the restart reused the cancelled update's key bound",
        )
        self.assertEqual(
            resumed["incremental"]["max_pk"],
            [12],
            "the restart did not recapture the bound against its own boundary",
        )

    def test_provenance_survives_the_offsets_rewritten_scope_field(self):
        # The block carries its own ``scope`` stamp rather than relying on the
        # offset's ``pipeline_scope``, because _read_stream overwrites that field
        # with the *reading* scope on every batch (end["pipeline_scope"] =
        # pipeline_scope). An offset that has passed through one microbatch of a
        # new update therefore claims the new scope, so it could vouch for
        # itself. The stamp is written once, at capture, and never rewritten.
        bridge = FakeBridge()
        bridge.rows = [{"id": index, "value": str(index)} for index in range(1, 9)]

        first = self.connector(bridge, registration_scope="update-A")
        _, offset = first.read_table("app.orders", {}, {"snapshot.page.size": "4"})
        captured = offset["incremental"]["scope"]

        # Simulate the rewrite: the offset now advertises a different scope than
        # the one that captured the block, exactly as a resumed batch would.
        forged = dict(offset)
        forged["pipeline_scope"] = hashlib.sha256(b"update-B").hexdigest()[:32]

        self.assertEqual(
            forged["incremental"]["scope"],
            captured,
            "the capture stamp must not follow the offset's rewritten scope",
        )
        self.assertNotEqual(
            forged["incremental"]["scope"],
            forged["pipeline_scope"],
            "the forged offset should disagree with the capture stamp",
        )

        # Resuming under B must treat the bound as foreign, even though the
        # offset's own pipeline_scope field now says B.
        second = self.connector(bridge, registration_scope="update-B")
        self.assertTrue(
            second._incremental_bound_is_foreign(forged["incremental"]),
            "a bound whose offset was rewritten to the reading scope was trusted",
        )

    def test_an_ordinary_mid_copy_restart_still_resumes(self):
        # The guard must not turn every restart into a full re-copy: a microbatch
        # continuing within the SAME scope has valid provenance and must resume
        # from its cursor. Without this the fix trades data loss for re-reading
        # the whole table on each batch.
        bridge = FakeBridge()
        bridge.rows = [{"id": index, "value": str(index)} for index in range(1, 13)]
        connector = self.connector(bridge, registration_scope="steady")
        options = {"snapshot.page.size": "4"}

        first_rows, offset = connector.read_table("app.orders", {}, dict(options))
        self.assertEqual(
            sorted(row["id"] for row in first_rows if row["_informix_op"] == "r"),
            [1, 2, 3, 4],
        )

        second_rows, offset = connector.read_table("app.orders", offset, dict(options))
        resumed_ids = sorted(row["id"] for row in second_rows if row["_informix_op"] == "r")

        self.assertEqual(
            resumed_ids,
            [5, 6, 7, 8],
            "a same-scope restart re-read the prefix instead of resuming",
        )

    def test_incremental_boundary_is_consumed_by_delete_reader(self):
        upsert = self.connector(registration_scope="inc-shared")
        upsert.read_table("app.orders", {}, {})

        deletes = self.connector(registration_scope="inc-shared")
        rows, offset = deletes.read_table_deletes("app.orders", {}, {})

        self.assertEqual(list(rows), [])
        self.assertEqual(offset["phase"], "stream")
        self.assertEqual(offset["commit_lsn"], "90")

    def _datetime_bridge(self, qualifier):
        bridge = FakeBridge()
        # id becomes a DATETIME primary key with the given packed qualifier.
        bridge.tables[0]["columns"][0].update({"type_name": "DATETIME", "length": qualifier})
        from datetime import datetime as _dt

        bridge.rows = [
            {"id": _dt(2026, 1, 1, 0, 0, 0), "value": "a"},
            {"id": _dt(2026, 6, 15, 12, 30, 0), "value": "b"},
        ]
        # Simulate the order-preserving string cast Informix would apply.
        bridge.chunk_key_fn = lambda key, value: value.isoformat()
        return bridge

    def test_year_anchored_datetime_key_chunks_as_string(self):
        # 0x000f == YEAR TO FRACTION(5): order-preserving, so incremental
        # chunking proceeds via a string cast instead of falling back.
        bridge = self._datetime_bridge(0x000F)
        connector = self.connector(bridge, **{"snapshot.mode": "incremental"})

        rows, offset = self._drain(connector, {"snapshot.page.size": "1"})

        self.assertEqual([row["value"] for row in rows], ["a", "b"])
        # Emitted rows carry the real DATETIME value, never the helper alias.
        self.assertTrue(all("__chunk_id" not in row for row in rows))
        self.assertTrue(all(row["_informix_op"] == "r" for row in rows))
        self.assertNotIn("incremental", offset)

    def test_hour_anchored_datetime_key_chunks_as_string(self):
        # 0x060a == HOUR TO SECOND (TIME-of-day). Casting in its own qualifier
        # yields fixed-width HH:MM:SS text, a stable order, so it chunks
        # incrementally rather than falling back.
        from datetime import time as _t

        bridge = self._datetime_bridge(0x060A)
        bridge.rows = [
            {"id": _t(9, 29, 9), "value": "a"},
            {"id": _t(17, 5, 0), "value": "b"},
        ]
        bridge.chunk_key_fn = lambda key, value: value.isoformat()
        connector = self.connector(bridge, **{"snapshot.mode": "incremental"})

        rows, offset = self._drain(connector, {"snapshot.page.size": "1"})

        self.assertEqual([row["value"] for row in rows], ["a", "b"])
        self.assertTrue(all("__chunk_id" not in row for row in rows))
        self.assertNotIn("incremental", offset)

    def test_composite_key_with_datetime_and_scalar_chunks(self):
        # Composite key: a DATE-like DATETIME column followed by a string
        # column (the tw221 shape). Both are chunked lexicographically.
        from datetime import datetime as _dt

        bridge = FakeBridge()
        bridge.tables[0]["columns"][0].update(
            {"type_name": "DATETIME", "length": 0x060F}  # HOUR TO FRACTION(5)
        )
        bridge.tables[0]["primary_keys"] = ["id", "value"]
        bridge.rows = [
            {"id": _dt(2017, 5, 30, 9, 29, 9), "value": "C0174895"},
            {"id": _dt(2017, 5, 30, 9, 29, 9), "value": "C0174896"},
            {"id": _dt(2018, 1, 1, 10, 0, 0), "value": "A0000001"},
        ]
        # Only the DATETIME key column is cast; the scalar key compares natively.
        bridge.chunk_key_fn = lambda key, value: value.strftime("%H:%M:%S.00000")
        connector = self.connector(bridge, **{"snapshot.mode": "incremental"})

        rows, offset = self._drain(connector, {"snapshot.page.size": "1"})

        self.assertEqual(len(rows), 3)
        self.assertTrue(all("__chunk_id" not in row for row in rows))
        self.assertTrue(all("__chunk_value" not in row for row in rows))
        self.assertNotIn("incremental", offset)

    def test_malformed_datetime_qualifier_is_rejected_before_chunking(self):
        # A malformed DATETIME qualifier (0x0105, non-field start nibble) is not
        # materializable, so it fails discovery well before the chunk gate is
        # consulted -- it never reaches the string-cast fallback path.
        bridge = self._datetime_bridge(0x0105)
        connector = self.connector(bridge, **{"snapshot.mode": "incremental"})

        with self.assertRaisesRegex(InformixError, "cannot materialize"):
            connector.read_table("app.orders", {}, {})

    def test_datetime_as_string_option_off_falls_back(self):
        bridge = self._datetime_bridge(0x000F)
        connector = self.connector(
            bridge,
            **{
                "snapshot.mode": "incremental",
                "snapshot.incremental.datetime.as.string": "false",
            },
        )

        with self.assertLogs(
            "databricks.labs.community_connector.sources.informix.informix",
            level="INFO",
        ) as logs:
            _, offset = connector.read_table("app.orders", {}, {})

        self.assertNotIn("incremental", offset)
        self.assertTrue(
            any("falling back to the blocking" in line for line in logs.output),
            logs.output,
        )

    def test_offset_v10_incremental_block_round_trips(self):
        bridge = FakeBridge()
        connector = self.connector(bridge, **{"snapshot.page.size": "1"})

        _, offset = connector.read_table("app.orders", {}, {"snapshot.page.size": "1"})

        json.dumps(offset)  # must be JSON-serializable for the framework
        self.assertEqual(offset["version"], _OFFSET_VERSION)
        self.assertEqual(_validated_offset(offset), offset)

    def test_offset_incremental_requires_stream_phase(self):
        offset = _stream_offset(90)
        offset["phase"] = "snapshot"
        offset["snapshot_lsn"] = "90"
        offset["snapshot"] = {"last_pk": [1], "page_index": 0}
        offset["incremental"] = {"started": True, "last_pk": [1], "max_pk": [2], "done": False}

        with self.assertRaisesRegex(ValueError, "requires the stream phase"):
            _validated_offset(offset)


class ConnectionSlotLeaseLossTests(unittest.TestCase):
    """The heartbeat's lease-loss path.

    This path had no coverage, and a reference to a method the Volume removal
    deleted survived every test and the deployed bundle -- it only raises when a
    slot is actually acquired, which no offline test did. Every flow then failed
    with ``AttributeError: 'PurePythonInformixBridge' object has no attribute
    '_poison_connection_slot_lease'``.
    """

    def _bridge(self):
        bridge = object.__new__(informix_module.PurePythonInformixBridge)
        bridge.options = {}
        bridge._connected = True
        bridge._connect_lock = threading.RLock()
        bridge._operation_lock = threading.RLock()
        bridge._connection_lease_lost = threading.Event()
        bridge._connection_slot = "slot-0000"
        bridge._lakebase_slot = None
        bridge.transport = mock.Mock()
        return bridge

    def test_every_callback_the_heartbeat_is_handed_actually_exists(self):
        # The regression, stated directly: the acquisition path passes bound
        # methods to the heartbeat thread, and a missing one is invisible until a
        # slot is acquired at runtime.
        bridge = informix_module.PurePythonInformixBridge
        for name in (
            "_poison_connection_slot_lease",
            "_heartbeat_connection_slot",
            "_release_connection_slot",
        ):
            self.assertTrue(callable(getattr(bridge, name, None)), name)

    def test_losing_the_lease_closes_the_transport_and_releases_the_slot(self):
        bridge = self._bridge()
        released = []
        bridge._release_connection_slot = lambda: released.append(True)

        bridge._poison_connection_slot_lease()

        # The transport must close: the slot now belongs to another reader, so
        # continuing to use this connection would exceed the configured limit.
        bridge.transport.close.assert_called_once_with()
        self.assertFalse(bridge._connected)
        self.assertEqual(released, [True])

    def test_lease_loss_is_published_then_cleared(self):
        bridge = self._bridge()
        observed = []
        bridge._release_connection_slot = lambda: observed.append(
            bridge._connection_lease_lost.is_set()
        )

        bridge._poison_connection_slot_lease()

        # Set while the in-flight operation is still unwinding, so its wrapper
        # discards the result, and cleared afterwards so the bridge can reconnect.
        self.assertEqual(observed, [True])
        self.assertFalse(bridge._connection_lease_lost.is_set())

    def test_a_failing_transport_close_still_releases_the_slot(self):
        bridge = self._bridge()
        bridge.transport.close.side_effect = OSError("already gone")
        released = []
        bridge._release_connection_slot = lambda: released.append(True)

        bridge._poison_connection_slot_lease()

        # Otherwise a broken socket would strand capacity until the lease expired.
        self.assertEqual(released, [True])

    def test_poisoning_proceeds_when_the_operation_lock_is_held(self):
        bridge = self._bridge()
        released = []
        bridge._release_connection_slot = lambda: released.append(True)
        bridge._operation_lock = mock.Mock()
        # Simulate an operation that outlives the bounded wait.
        bridge._operation_lock.acquire.return_value = False

        bridge._poison_connection_slot_lease()

        # It must not wait forever: the releaser may be joining this very thread.
        bridge._operation_lock.acquire.assert_called_once()
        self.assertEqual(
            bridge._operation_lock.acquire.call_args.kwargs["timeout"],
            informix_module._POISON_OPERATION_LOCK_SECONDS,
        )
        self.assertEqual(released, [True])

    def test_heartbeat_reports_lease_loss_when_renewal_is_rejected(self):
        bridge = self._bridge()
        reported = []
        stop = threading.Event()
        state = mock.Mock()
        state.connect.return_value = mock.Mock(closed=False)
        bridge._lakebase = lambda: state
        with mock.patch.object(informix_module, "heartbeat_slot", return_value=False):
            with mock.patch.object(informix_module, "_CONNECTION_SLOT_HEARTBEAT_SECONDS", 0.01):
                bridge._heartbeat_connection_slot(object(), stop, lambda: reported.append(True))

        self.assertEqual(reported, [True])

    def test_releasing_a_slot_stops_its_heartbeat_thread(self):
        # The regression: release freed the slot but left the heartbeat running.
        # Another reader then claimed the slot and bumped the epoch, so the stale
        # thread's next renewal was rejected -- correctly reported as a lost lease,
        # poisoning a transport whose owner had already finished. 16 flows, exactly
        # max.concurrent.connections, failed this way.
        bridge = self._bridge()
        stop = threading.Event()
        finished = threading.Event()

        def heartbeat_body():
            stop.wait(5.0)
            finished.set()

        thread = threading.Thread(target=heartbeat_body, daemon=True)
        thread.start()
        bridge._connection_slot_heartbeat_stop = stop
        bridge._connection_slot_heartbeat = thread
        bridge._lakebase_slot = object()
        bridge._lakebase_connection = lambda: mock.Mock()

        with mock.patch.object(informix_module, "release_slot", return_value=True):
            bridge._release_connection_slot()

        self.assertTrue(stop.is_set(), "release did not signal the heartbeat to stop")
        # Asserted without waiting: release must not return until the thread is
        # gone, or the slot is freed while a renewal may still be in flight.
        self.assertFalse(thread.is_alive(), "release returned before the heartbeat exited")
        self.assertTrue(finished.is_set())
        # Handles are cleared so a second release cannot signal the same thread.
        self.assertIsNone(bridge._connection_slot_heartbeat)
        self.assertIsNone(bridge._connection_slot_heartbeat_stop)

    def test_release_stops_the_heartbeat_even_with_no_slot_held(self):
        # Ordering matters: the thread must be stopped before the early return for
        # an already-released slot, or a poisoned release leaks it.
        bridge = self._bridge()
        stop = threading.Event()
        bridge._connection_slot_heartbeat_stop = stop
        bridge._connection_slot_heartbeat = None
        bridge._lakebase_slot = None

        bridge._release_connection_slot()

        self.assertTrue(stop.is_set())

    def test_release_from_inside_the_heartbeat_does_not_join_itself(self):
        # The poison path runs on the heartbeat thread; joining itself would hang.
        bridge = self._bridge()
        stop = threading.Event()
        bridge._lakebase_slot = object()
        bridge._lakebase_connection = lambda: mock.Mock()
        outcome = []

        def release_from_heartbeat():
            bridge._connection_slot_heartbeat_stop = stop
            bridge._connection_slot_heartbeat = threading.current_thread()
            with mock.patch.object(informix_module, "release_slot", return_value=True):
                bridge._release_connection_slot()
            outcome.append("returned")

        thread = threading.Thread(target=release_from_heartbeat, daemon=True)
        thread.start()
        thread.join(timeout=5.0)

        self.assertEqual(outcome, ["returned"], "release deadlocked joining its own thread")

    def test_heartbeat_exits_immediately_once_release_has_begun(self):
        bridge = self._bridge()
        reported = []
        stop = threading.Event()
        stop.set()

        bridge._heartbeat_connection_slot(object(), stop, lambda: reported.append(True))

        self.assertEqual(reported, [])

    def test_release_racing_a_rejected_renewal_is_not_poisoned(self):
        # The ordering that matters: renewal is rejected, but by the time it
        # returns the owner has begun releasing. A releaser sets stop then joins
        # this thread, so poisoning here would have the two wait on each other.
        bridge = self._bridge()
        reported = []
        stop = threading.Event()
        state = mock.Mock()
        state.connect.return_value = mock.Mock(closed=False)
        bridge._lakebase = lambda: state

        def reject_and_release(_connection, _slot):
            stop.set()  # the releaser wins the race while we were renewing
            return False

        with mock.patch.object(informix_module, "heartbeat_slot", reject_and_release):
            with mock.patch.object(informix_module, "_CONNECTION_SLOT_HEARTBEAT_SECONDS", 0.01):
                bridge._heartbeat_connection_slot(object(), stop, lambda: reported.append(True))

        self.assertEqual(reported, [])


class SharedCdcShardTests(unittest.TestCase):
    """Fan-out unit tests for the sharded daemon-reader buffer (_SharedCdcShard)."""

    def setUp(self):
        self.table_a = Table.parse(_table(name="orders"), "demo")
        self.table_b = Table.parse(_table(name="audit"), "demo")
        self.id_a = self.table_a.native_identity
        self.id_b = self.table_b.native_identity

    def _shard_with_two_tables(self):
        shard = _SharedCdcShard()
        shard.subscribe(self.id_a, self.table_a, _capture_descriptor(self.table_a), 0, 0)
        shard.subscribe(self.id_b, self.table_b, _capture_descriptor(self.table_b), 0, 0)
        return shard

    def _two_table_stream(self):
        return [
            {"op": "BEGIN", "tx_id": 1, "lsn": 100},
            {
                "op": "INSERT",
                "tx_id": 1,
                "lsn": 101,
                "table": self.table_a.identity,
                "row": {"id": 1, "value": "a"},
            },
            {"op": "COMMIT", "tx_id": 1, "lsn": 102},
            {"op": "BEGIN", "tx_id": 2, "lsn": 103},
            {
                "op": "INSERT",
                "tx_id": 2,
                "lsn": 104,
                "table": self.table_b.identity,
                "row": {"id": 9, "value": "z"},
            },
            {"op": "COMMIT", "tx_id": 2, "lsn": 105},
            {"op": "TIMEOUT", "lsn": 106},
        ]

    def _schema_map(self):
        return {self.id_a: self.table_a, self.id_b: self.table_b}

    def test_multiplexes_two_tables_to_their_own_buffers(self):
        shard = self._shard_with_two_tables()
        caught_up = shard.ingest(self._two_table_stream(), self._schema_map(), 1, 106)
        self.assertTrue(caught_up)
        view_a = shard.snapshot(self.id_a, 0)
        view_b = shard.snapshot(self.id_b, 0)
        self.assertEqual([tx.tx_id for tx in view_a.committed], [1])
        self.assertEqual([tx.tx_id for tx in view_b.committed], [2])
        self.assertTrue(view_a.caught_up)
        self.assertEqual((view_a.min_lsn, view_a.current_lsn), (1, 106))

    def test_per_table_cursor_blocks_reread_duplicates(self):
        shard = self._shard_with_two_tables()
        stream = self._two_table_stream()
        shard.ingest(stream, self._schema_map(), 1, 106)
        shard.ingest(stream, self._schema_map(), 1, 106)  # daemon re-reads from the floor
        self.assertEqual(len(shard.buffers[self.id_a]), 1)
        self.assertEqual(len(shard.buffers[self.id_b]), 1)

    def test_snapshot_peeks_without_removing_until_checkpoint_advances(self):
        shard = self._shard_with_two_tables()
        shard.ingest(self._two_table_stream(), self._schema_map(), 1, 106)
        self.assertEqual([tx.tx_id for tx in shard.snapshot(self.id_a, 0).committed], [1])
        # A second poll at the same checkpoint still sees the un-consumed transaction.
        self.assertEqual([tx.tx_id for tx in shard.snapshot(self.id_a, 0).committed], [1])
        # Once the checkpoint passes its commit LSN it is trimmed.
        self.assertEqual(shard.snapshot(self.id_a, 102).committed, [])

    def test_late_subscriber_receives_history_without_duplicating_others(self):
        shard = _SharedCdcShard()
        shard.subscribe(self.id_a, self.table_a, _capture_descriptor(self.table_a), 0, 0)
        stream = self._two_table_stream()
        shard.ingest(stream, {self.id_a: self.table_a}, 1, 106)
        # B subscribes late; the daemon re-reads the log and B must get its history
        # while A is not re-delivered.
        shard.subscribe(self.id_b, self.table_b, _capture_descriptor(self.table_b), 0, 0)
        shard.ingest(stream, self._schema_map(), 1, 106)
        self.assertEqual([tx.tx_id for tx in shard.buffers[self.id_b]], [2])
        self.assertEqual(len(shard.buffers[self.id_a]), 1)

    def test_per_table_open_begin_isolates_tables(self):
        shard = self._shard_with_two_tables()
        stream = [
            {"op": "BEGIN", "tx_id": 3, "lsn": 200},
            {
                "op": "INSERT",
                "tx_id": 3,
                "lsn": 201,
                "table": self.table_a.identity,
                "row": {"id": 2, "value": "b"},
            },
        ]  # opened, not committed
        caught_up = shard.ingest(stream, self._schema_map(), 1, 201)
        self.assertFalse(caught_up)
        self.assertEqual(shard.snapshot(self.id_a, 0).open_begin, 200)
        self.assertIsNone(shard.snapshot(self.id_b, 0).open_begin)
        self.assertEqual(shard.shared_restart, 200)  # resume from the open BEGIN

    def test_subscribe_lowers_shared_restart_to_admit_history(self):
        shard = _SharedCdcShard()
        shard.subscribe(self.id_a, self.table_a, _capture_descriptor(self.table_a), 0, 50)
        self.assertEqual(shard.shared_restart, 50)
        shard.subscribe(self.id_b, self.table_b, _capture_descriptor(self.table_b), 0, 30)
        self.assertEqual(shard.shared_restart, 30)

    def test_snapshot_is_none_until_daemon_publishes(self):
        shard = self._shard_with_two_tables()
        self.assertIsNone(shard.snapshot(self.id_a, 0))


class SharedCdcOptionTests(unittest.TestCase):
    def setUp(self):
        self._shared_state = tempfile.TemporaryDirectory()
        self._lakebase = _OfflineLakebase().install(self)

    def tearDown(self):
        self._shared_state.cleanup()

    def _connector(self, **options):
        connector = InformixLakeflowConnect(
            {
                "database": "demo",
                "snapshot.staging.location": self._shared_state.name,
                "lakebase.password": "test-state-password",
                **options,
            }
        )
        connector._bridge_instance = FakeBridge()
        return connector

    def test_shared_session_defaults_on(self):
        self.assertTrue(self._connector()._shared_cdc_enabled())

    def test_shared_session_can_be_disabled(self):
        self.assertFalse(self._connector(**{"cdc.shared.session": "false"})._shared_cdc_enabled())

    def test_shared_session_enables(self):
        self.assertTrue(self._connector(**{"cdc.shared.session": "true"})._shared_cdc_enabled())

    def test_thread_count_defaults_to_connection_budget(self):
        self.assertEqual(self._connector()._shared_cdc_thread_count(), 16)

    def test_thread_count_override(self):
        self.assertEqual(
            self._connector(**{"cdc.shared.reader.threads": "4"})._shared_cdc_thread_count(), 4
        )

    def test_invalid_shared_session_value_rejected(self):
        with self.assertRaises(ValueError):
            self._connector(**{"cdc.shared.session": "maybe"})

    def test_zero_reader_threads_rejected(self):
        with self.assertRaises(ValueError):
            self._connector(**{"cdc.shared.reader.threads": "0"})

    def test_zero_buffer_rejected(self):
        with self.assertRaises(ValueError):
            self._connector(**{"cdc.shared.buffer.max.records": "0"})


class SharedCdcConsumerSeamTests(unittest.TestCase):
    """The streaming consumer reads from the shard and touches Informix for nothing."""

    def setUp(self):
        self._shared_state = tempfile.TemporaryDirectory()
        self._lakebase = _OfflineLakebase().install(self)

    def tearDown(self):
        self._shared_state.cleanup()

    def _connector(self, bridge):
        connector = InformixLakeflowConnect(
            {
                "database": "demo",
                "snapshot.staging.location": self._shared_state.name,
                "lakebase.password": "test-state-password",
                "cdc.shared.session": "true",
            }
        )
        connector.set_registration_scope(hashlib.sha256(b"shared").hexdigest()[:32])
        connector._bridge_instance = bridge
        return connector

    def test_shared_stream_read_uses_shard_and_skips_bridge(self):
        class _RecordingBridge(FakeBridge):
            def __init__(self):
                super().__init__()
                self.calls = []

            def get_table(self, identity):
                self.calls.append("get_table")
                return super().get_table(identity)

            def minimum_lsn(self):
                self.calls.append("minimum_lsn")
                return super().minimum_lsn()

            def current_lsn(self):
                self.calls.append("current_lsn")
                return super().current_lsn()

            def read_changes(self, *args, **kwargs):
                self.calls.append("read_changes")
                return super().read_changes(*args, **kwargs)

        bridge = _RecordingBridge()
        connector = self._connector(bridge)
        table = Table.parse(_table(), "demo")
        identity = table.native_identity

        shard = _SharedCdcShard()
        shard.subscribe(identity, table, _capture_descriptor(table), 0, 0)
        shard.ingest(
            [
                {"op": "BEGIN", "tx_id": 5, "lsn": 100},
                {
                    "op": "INSERT",
                    "tx_id": 5,
                    "lsn": 101,
                    "table": table.identity,
                    "row": {"id": 7, "value": "g"},
                },
                {"op": "COMMIT", "tx_id": 5, "lsn": 102},
                {"op": "TIMEOUT", "lsn": 103},
            ],
            {identity: table},
            1,
            103,
        )

        with mock.patch.object(informix_module, "_shared_cdc_shard", lambda *a, **k: shard):
            rows, _offset = connector._read_stream(table, _stream_offset(), {}, deletes=False)
        rows = list(rows)

        self.assertEqual([row["id"] for row in rows], [7])
        # Steady-state shared read: the consumer sourced schema, LSN bounds, and change
        # records from the shard, so it never touched the Informix bridge.
        self.assertEqual(bridge.calls, [])

    def test_fingerprint_mismatch_falls_back_to_direct_read(self):
        bridge = FakeBridge()
        connector = self._connector(bridge)
        table = Table.parse(_table(), "demo")
        identity = table.native_identity

        shard = _SharedCdcShard()
        shard.subscribe(identity, table, _capture_descriptor(table), 0, 0)
        shard.ingest([{"op": "TIMEOUT", "lsn": 103}], {identity: table}, 1, 103)

        checkpoint = _stream_offset()
        checkpoint["schema_fingerprint"] = "0" * 64  # simulate an in-flight transition

        with mock.patch.object(informix_module, "_shared_cdc_shard", lambda *a, **k: shard):
            view = connector._shared_cdc_snapshot(
                table, checkpoint, {}, False, connector._pipeline_scope()
            )
        self.assertIsNone(view)  # daemon view rejected -> direct-read fallback

    def test_primary_keys_override_table_shards_and_detects_pk_change(self):
        # A primary.keys override makes `value` the key. The daemon preserves that
        # override on its shard schema, so the table's fingerprint matches (no
        # fallback) and a change to `value` is detected as a key change.
        bridge = FakeBridge()
        connector = self._connector(bridge)
        base = Table.parse(_table(), "demo")
        override = informix_module.replace(base, primary_keys=("value",), key_override=True)
        identity = override.native_identity

        shard = _SharedCdcShard()
        shard.subscribe(identity, override, _capture_descriptor(override), 0, 0)
        shard.ingest(
            [
                {"op": "BEGIN", "tx_id": 6, "lsn": 100},
                {
                    "op": "BEFORE_UPDATE",
                    "tx_id": 6,
                    "lsn": 101,
                    "table": override.identity,
                    "before": {"id": 1, "value": "a"},
                },
                {
                    "op": "AFTER_UPDATE",
                    "tx_id": 6,
                    "lsn": 102,
                    "table": override.identity,
                    "after": {"id": 1, "value": "b"},
                },
                {"op": "COMMIT", "tx_id": 6, "lsn": 103},
                {"op": "TIMEOUT", "lsn": 104},
            ],
            {identity: override},  # daemon publishes schema WITH the override applied
            1,
            104,
        )

        checkpoint = _stream_offset()
        checkpoint["schema_fingerprint"] = _schema_fingerprint(override)

        with mock.patch.object(informix_module, "_shared_cdc_shard", lambda *a, **k: shard):
            snapshot = connector._shared_cdc_snapshot(
                override, checkpoint, {}, True, connector._pipeline_scope()
            )
            self.assertIsNotNone(snapshot)  # override fingerprint matches -> no fallback
            rows, _offset = connector._read_stream(override, checkpoint, {}, deletes=True)
        rows = list(rows)
        # `value` (the override key) changed a->b, so the delete channel emits the old key.
        self.assertEqual(len(rows), 1)


class SnapshotFairnessOptionTests(unittest.TestCase):
    """Options that select and tune the snapshot-drain fairness strategies."""

    def setUp(self):
        self._shared_state = tempfile.TemporaryDirectory()
        self._lakebase = _OfflineLakebase().install(self)

    def tearDown(self):
        self._shared_state.cleanup()

    def _connector(self, **options):
        connector = InformixLakeflowConnect(
            {
                "database": "demo",
                "snapshot.staging.location": self._shared_state.name,
                "lakebase.password": "test-state-password",
                **options,
            }
        )
        connector._bridge_instance = FakeBridge()
        return connector

    def test_shared_session_defaults_on(self):
        self.assertTrue(self._connector()._snapshot_shared_enabled())

    def test_shared_session_can_disable(self):
        self.assertFalse(
            self._connector(**{"snapshot.shared.session": "false"})._snapshot_shared_enabled()
        )

    def test_shared_session_can_enable(self):
        self.assertTrue(
            self._connector(**{"snapshot.shared.session": "true"})._snapshot_shared_enabled()
        )

    def test_reader_threads_default_one(self):
        self.assertEqual(self._connector()._snapshot_reader_thread_count(), 1)

    def test_reader_threads_override(self):
        self.assertEqual(
            self._connector(**{"snapshot.reader.threads": "4"})._snapshot_reader_thread_count(),
            4,
        )

    def test_zero_reader_threads_rejected(self):
        with self.assertRaises(ValueError):
            self._connector(**{"snapshot.reader.threads": "0"})

    def test_invalid_shared_session_rejected(self):
        with self.assertRaises(ValueError):
            self._connector(**{"snapshot.shared.session": "maybe"})

    def test_reservation_at_slot_count_rejected(self):
        with self.assertRaises(ValueError):
            self._connector(
                **{"snapshot.connection.reservation": "8", "max.concurrent.connections": "8"}
            )

    def test_reservation_below_slot_count_accepted(self):
        # Construction validates 0 <= reservation < slot_count; a valid value does not
        # raise. The floor it produces is asserted in SnapshotReservationFloorTests.
        self._connector(
            **{"snapshot.connection.reservation": "3", "max.concurrent.connections": "8"}
        )


class SnapshotReservationFloorTests(unittest.TestCase):
    """Model A: a snapshot-phase read claims its slot above the reservation, so the low
    slots stay reachable by the streaming/CDC readers.

    The slot floor is computed in the bridge's ``_acquire_connection_slot`` (the bridge
    shares the connector's ``options`` dict, so the drain marker the connector sets is
    visible there), so these exercise a real bridge with ``acquire_slot`` stubbed to
    capture the floor before any lease is taken.
    """

    def setUp(self):
        self._shared_state = tempfile.TemporaryDirectory()
        self._lakebase = _OfflineLakebase().install(self)

    def tearDown(self):
        self._shared_state.cleanup()

    def _bridge(self, **options):
        return informix_module.PurePythonInformixBridge(
            {
                "hostname": "localhost",
                "database": "demo",
                "user": "informix",
                "password": "secret",
                "server": "demo_on",
                "lakebase.password": "test-state-password",
                "max.concurrent.connections": "8",
                **options,
            }
        )

    def _captured_floor(self, bridge):
        seen = {}

        class _StopProbe(Exception):
            pass

        def probe(*args, floor, **kwargs):
            seen["floor"] = floor
            raise _StopProbe

        with mock.patch.object(informix_module, "acquire_slot", side_effect=probe):
            with self.assertRaises(_StopProbe):
                bridge._acquire_connection_slot()
        return seen["floor"]

    def test_marker_applies_reservation_floor(self):
        bridge = self._bridge(**{"snapshot.connection.reservation": "3"})
        bridge.options[informix_module._SNAPSHOT_DRAIN_MARKER_OPTION] = "true"
        self.assertEqual(self._captured_floor(bridge), 3)

    def test_no_marker_keeps_zero_floor(self):
        bridge = self._bridge(**{"snapshot.connection.reservation": "3"})
        self.assertEqual(self._captured_floor(bridge), 0)

    def test_marker_takes_max_of_delete_and_snapshot_floors(self):
        bridge = self._bridge(
            **{
                "snapshot.connection.reservation": "2",
                "upsert.connection.reservation": "5",
            }
        )
        bridge.options[informix_module._CONNECTION_CHANNEL_OPTION] = "delete"
        bridge.options[informix_module._SNAPSHOT_DRAIN_MARKER_OPTION] = "true"
        # Delete channel floors at the upsert reservation (5); the snapshot floor (2) is
        # lower, so the higher of the two wins.
        self.assertEqual(self._captured_floor(bridge), 5)

    def test_zero_reservation_defaults_to_a_third_in_model_a(self):
        # Model A (shared session off) with an unset reservation reserves a third of the
        # pool: floor(9 / 3) = 3.
        bridge = self._bridge(
            **{"snapshot.shared.session": "false", "max.concurrent.connections": "9"}
        )
        bridge.options[informix_module._SNAPSHOT_DRAIN_MARKER_OPTION] = "true"
        self.assertEqual(self._captured_floor(bridge), 3)

    def test_zero_reservation_not_derived_when_shared_session_on(self):
        # The third-of-the-pool default is Model A only; with shared session on, a zero
        # reservation stays zero (Model C bounds drains by its thread count instead).
        bridge = self._bridge(
            **{"snapshot.shared.session": "true", "max.concurrent.connections": "9"}
        )
        bridge.options[informix_module._SNAPSHOT_DRAIN_MARKER_OPTION] = "true"
        self.assertEqual(self._captured_floor(bridge), 0)

    def test_zero_reservation_rounds_up_to_one_on_small_pools(self):
        # floor(n / 3) is 0 for a 2-, 3-, or 4-slot pool, but at least one slot is
        # reserved whenever the pool can spare one.
        for slot_count in (2, 3, 4):
            bridge = self._bridge(
                **{
                    "snapshot.shared.session": "false",
                    "max.concurrent.connections": str(slot_count),
                }
            )
            bridge.options[informix_module._SNAPSHOT_DRAIN_MARKER_OPTION] = "true"
            self.assertEqual(self._captured_floor(bridge), 1, f"slot_count={slot_count}")

    def test_zero_reservation_stays_zero_on_single_slot_pool(self):
        # A single-slot pool cannot spare one: reserving its only slot would floor the
        # drain out of every slot and deadlock it, so the derived reservation is 0.
        bridge = self._bridge(
            **{"snapshot.shared.session": "false", "max.concurrent.connections": "1"}
        )
        bridge.options[informix_module._SNAPSHOT_DRAIN_MARKER_OPTION] = "true"
        self.assertEqual(self._captured_floor(bridge), 0)

    def test_explicit_reservation_overrides_the_third_default(self):
        # A positive value is honoured verbatim even in Model A.
        bridge = self._bridge(
            **{
                "snapshot.shared.session": "false",
                "snapshot.connection.reservation": "2",
                "max.concurrent.connections": "9",
            }
        )
        bridge.options[informix_module._SNAPSHOT_DRAIN_MARKER_OPTION] = "true"
        self.assertEqual(self._captured_floor(bridge), 2)


class DaemonReservationFloorTests(unittest.TestCase):
    """A background daemon (sharded CDC, snapshot drain pool) claims its slot above the
    daemon reservation, so the low slots stay reachable by consumer bootstrap reads that
    would otherwise be starved by a daemon pinning every slot."""

    def setUp(self):
        self._shared_state = tempfile.TemporaryDirectory()
        self._lakebase = _OfflineLakebase().install(self)

    def tearDown(self):
        self._shared_state.cleanup()

    def _bridge(self, **options):
        return informix_module.PurePythonInformixBridge(
            {
                "hostname": "localhost",
                "database": "demo",
                "user": "informix",
                "password": "secret",
                "server": "demo_on",
                "lakebase.password": "test-state-password",
                "max.concurrent.connections": "6",
                **options,
            }
        )

    def _captured_floor(self, bridge):
        seen = {}

        class _StopProbe(Exception):
            pass

        def probe(*args, floor, **kwargs):
            seen["floor"] = floor
            raise _StopProbe

        with mock.patch.object(informix_module, "acquire_slot", side_effect=probe):
            with self.assertRaises(_StopProbe):
                bridge._acquire_connection_slot()
        return seen["floor"]

    def test_daemon_marker_reserves_a_third_by_default(self):
        # Pool 6, unset reservation -> floor(6 / 3) = 2 reserved for consumer reads.
        bridge = self._bridge()
        bridge.options[informix_module._DAEMON_SLOT_MARKER_OPTION] = "true"
        self.assertEqual(self._captured_floor(bridge), 2)

    def test_no_daemon_marker_keeps_zero_floor(self):
        # A consumer bootstrap read (no daemon marker) may claim any slot.
        self.assertEqual(self._captured_floor(self._bridge()), 0)

    def test_daemon_reservation_explicit_override(self):
        bridge = self._bridge(**{"daemon.connection.reservation": "1"})
        bridge.options[informix_module._DAEMON_SLOT_MARKER_OPTION] = "true"
        self.assertEqual(self._captured_floor(bridge), 1)

    def test_daemon_floor_takes_max_with_delete_channel(self):
        bridge = self._bridge(
            **{"daemon.connection.reservation": "1", "upsert.connection.reservation": "3"}
        )
        bridge.options[informix_module._CONNECTION_CHANNEL_OPTION] = "delete"
        bridge.options[informix_module._DAEMON_SLOT_MARKER_OPTION] = "true"
        # Delete daemon floors at max(upsert reservation 3, daemon reservation 1) = 3.
        self.assertEqual(self._captured_floor(bridge), 3)

    def test_reservation_at_slot_count_rejected(self):
        # Eagerly validated in the connector constructor (0 <= reservation < slot_count).
        with self.assertRaises(ValueError):
            InformixLakeflowConnect(
                {
                    "database": "demo",
                    "snapshot.staging.location": self._shared_state.name,
                    "lakebase.password": "test-state-password",
                    "daemon.connection.reservation": "6",
                    "max.concurrent.connections": "6",
                }
            )

    def test_snapshot_daemon_floors_one_band_below_cdc_daemon(self):
        # Pool 6, daemon reservation 2, snapshot.reader.threads 1 -> the snapshot drain
        # floors at 2 - 1 = 1, one slot below the CDC daemon (which floors at 2). That
        # leaves slot [1, 2) reachable by the drain but NOT the CDC daemon, so a saturated
        # CDC daemon cannot starve the drain. Slot 0 stays for consumer bootstrap.
        bridge = self._bridge()
        bridge.options[informix_module._SNAPSHOT_DAEMON_SLOT_MARKER_OPTION] = "true"
        self.assertEqual(self._captured_floor(bridge), 1)

    def test_snapshot_daemon_floor_tracks_reader_threads(self):
        # More drain threads reserve a wider private band, pulling the floor further down.
        bridge = self._bridge(
            **{"daemon.connection.reservation": "3", "snapshot.reader.threads": "2"}
        )
        bridge.options[informix_module._SNAPSHOT_DAEMON_SLOT_MARKER_OPTION] = "true"
        self.assertEqual(self._captured_floor(bridge), 1)

    def test_snapshot_daemon_floor_collapses_to_zero_when_pool_cannot_spare_a_band(self):
        # When the drain threads meet or exceed the daemon reservation there is no private
        # band to carve; the floor collapses to 0, where the drain still reaches the
        # CDC-free low slots rather than being confined to the CDC-contended band.
        bridge = self._bridge(
            **{"daemon.connection.reservation": "1", "snapshot.reader.threads": "2"}
        )
        bridge.options[informix_module._SNAPSHOT_DAEMON_SLOT_MARKER_OPTION] = "true"
        self.assertEqual(self._captured_floor(bridge), 0)


class DaemonReaderFactoryMarkerTests(unittest.TestCase):
    """Both daemon reader factories tag their reader so its slot acquisition is floored."""

    def setUp(self):
        self._shared_state = tempfile.TemporaryDirectory()
        self._lakebase = _OfflineLakebase().install(self)

    def tearDown(self):
        self._shared_state.cleanup()

    def _connector(self, **options):
        connector = InformixLakeflowConnect(
            {
                "database": "demo",
                "snapshot.staging.location": self._shared_state.name,
                "lakebase.password": "test-state-password",
                **options,
            }
        )
        connector._bridge_instance = FakeBridge()
        return connector

    def test_cdc_reader_factory_sets_daemon_marker(self):
        reader = self._connector()._shared_cdc_reader_factory("upsert")()
        self.assertEqual(reader.options.get(informix_module._DAEMON_SLOT_MARKER_OPTION), "true")

    def test_snapshot_drain_reader_factory_sets_snapshot_daemon_marker(self):
        # The drain floors one band below the CDC daemon, so it carries the snapshot-daemon
        # marker -- not the CDC daemon marker, which would confine it to the CDC-contended
        # band and let a saturated CDC daemon starve it.
        reader = self._connector()._snapshot_drain_reader_factory()()
        self.assertEqual(
            reader.options.get(informix_module._SNAPSHOT_DAEMON_SLOT_MARKER_OPTION), "true"
        )
        self.assertNotEqual(reader.options.get(informix_module._DAEMON_SLOT_MARKER_OPTION), "true")


class SnapshotDrainMarkerTests(unittest.TestCase):
    """The connector sets the drain marker (Model A) around a snapshot-phase read and
    restores it afterward, so the bridge applies the reservation floor only then."""

    def setUp(self):
        self._shared_state = tempfile.TemporaryDirectory()
        self._lakebase = _OfflineLakebase().install(self)

    def tearDown(self):
        self._shared_state.cleanup()

    def _connector(self, **options):
        connector = InformixLakeflowConnect(
            {
                "database": "demo",
                "snapshot.staging.location": self._shared_state.name,
                "lakebase.password": "test-state-password",
                "snapshot.mode": "initial",
                "cdc.shared.session": "false",
                # Model A by default here; the shared-mode marker test overrides this.
                "snapshot.shared.session": "false",
                **options,
            }
        )
        connector.set_registration_scope(hashlib.sha256(b"marker").hexdigest()[:32])
        connector._bridge_instance = FakeBridge()
        return connector

    def test_snapshot_phase_read_sets_marker_during_read(self):
        connector = self._connector()
        seen = {}
        original = connector._refresh_table_schema

        def spy(table, fingerprint):
            seen.setdefault(
                "marker",
                connector.options.get(informix_module._SNAPSHOT_DRAIN_MARKER_OPTION),
            )
            return original(table, fingerprint)

        connector._refresh_table_schema = spy
        connector.read_table("app.orders", {}, {})
        self.assertEqual(seen["marker"], "true")

    def test_marker_restored_after_read(self):
        connector = self._connector()
        connector.read_table("app.orders", {}, {})
        self.assertNotIn(informix_module._SNAPSHOT_DRAIN_MARKER_OPTION, connector.options)

    def test_shared_mode_read_does_not_set_marker(self):
        # In shared mode the daemon bounds concurrency, so the consumer takes no floor.
        connector = self._connector(**{"snapshot.shared.session": "true"})
        connector._drain_via_snapshot_daemon = lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("should not reach the daemon in this marker check")
        )
        seen = {}
        original = connector._refresh_table_schema

        def spy(table, fingerprint):
            seen.setdefault(
                "marker",
                connector.options.get(informix_module._SNAPSHOT_DRAIN_MARKER_OPTION),
            )
            return original(table, fingerprint)

        connector._refresh_table_schema = spy
        try:
            connector.read_table("app.orders", {}, {})
        except AssertionError:
            pass  # the daemon stub fired after the schema refresh; the marker check ran
        self.assertIsNone(seen.get("marker"))

    def test_incremental_keyed_read_does_not_set_marker(self):
        # A keyed table's default incremental snapshot reads a bounded chunk per
        # microbatch and frees the slot between them, so it is never floored -- it
        # borrows the reserved slots freely. Only the monolithic initial drain is floored.
        connector = self._connector(**{"snapshot.mode": "incremental"})
        seen = {}
        original = connector._table

        def spy(name, opts):
            seen.setdefault(
                "marker",
                connector.options.get(informix_module._SNAPSHOT_DRAIN_MARKER_OPTION),
            )
            return original(name, opts)

        connector._table = spy
        connector.read_table("app.orders", {}, {})
        self.assertIsNone(seen.get("marker"))


class SnapshotDrainPoolTests(unittest.TestCase):
    """The bounded pool that runs Model C drains off the microbatch thread."""

    def test_submit_dedups_and_queues_once(self):
        pool = _SnapshotDrainPool()
        pool.submit(("k",), table=None, options={}, pipeline_scope="s", allow_keyless=False)
        pool.submit(("k",), table=None, options={}, pipeline_scope="s", allow_keyless=False)
        self.assertEqual(list(pool.queue), [("k",)])
        self.assertIn(("k",), pool.pending)

    def test_wait_returns_true_on_success(self):
        pool = _SnapshotDrainPool()
        pool.submit(("k",), table=None, options={}, pipeline_scope="s", allow_keyless=False)
        with pool.condition:
            pool.queue.clear()
            pool.pending.discard(("k",))
            pool.results[("k",)] = None
            pool.condition.notify_all()
        self.assertTrue(pool.wait(("k",), 1.0))

    def test_wait_reraises_failure(self):
        pool = _SnapshotDrainPool()
        pool.submit(("k",), table=None, options={}, pipeline_scope="s", allow_keyless=False)
        with pool.condition:
            pool.pending.discard(("k",))
            pool.results[("k",)] = InformixError("boom")
            pool.condition.notify_all()
        with self.assertRaisesRegex(InformixError, "boom"):
            pool.wait(("k",), 1.0)

    def test_wait_times_out_when_never_completed(self):
        pool = _SnapshotDrainPool()
        pool.submit(("k",), table=None, options={}, pipeline_scope="s", allow_keyless=False)
        self.assertFalse(pool.wait(("k",), 0.05))

    def test_worker_runs_job_and_closes_reader(self):
        pool = _SnapshotDrainPool()
        calls = {}

        class _FakeReader:
            def _read_snapshot(
                self, table, start, options, *, pipeline_scope_override, allow_keyless
            ):
                calls["read"] = (table, start, pipeline_scope_override, allow_keyless)

            def close(self):
                calls["closed"] = True

        pool.configure(reader_factory=_FakeReader, thread_count=1)
        worker = threading.Thread(
            target=informix_module._run_snapshot_drain_worker, args=(pool,), daemon=True
        )
        worker.start()
        pool.submit(
            ("k",), table="T", options={"a": "b"}, pipeline_scope="scope", allow_keyless=True
        )
        self.assertTrue(pool.wait(("k",), 2.0))
        self.assertEqual(calls["read"], ("T", None, "scope", True))
        self.assertTrue(calls["closed"])

    def test_worker_surfaces_failure_to_waiter(self):
        pool = _SnapshotDrainPool()

        class _BoomReader:
            def _read_snapshot(self, *args, **kwargs):
                raise InformixError("drain failed")

            def close(self):
                pass

        pool.configure(reader_factory=_BoomReader, thread_count=1)
        worker = threading.Thread(
            target=informix_module._run_snapshot_drain_worker, args=(pool,), daemon=True
        )
        worker.start()
        pool.submit(("k",), table="T", options={}, pipeline_scope="scope", allow_keyless=False)
        with self.assertRaisesRegex(InformixError, "drain failed"):
            pool.wait(("k",), 2.0)


class SnapshotDaemonSeamTests(unittest.TestCase):
    """Model C end to end: the consumer frees its slot, a daemon worker drains and stages
    the table, and the consumer serves the staged pages."""

    def setUp(self):
        self._shared_state = tempfile.TemporaryDirectory()
        self._lakebase = _OfflineLakebase().install(self)

    def tearDown(self):
        self._shared_state.cleanup()

    def _options(self, **extra):
        return {
            "database": "demo",
            "snapshot.staging.location": self._shared_state.name,
            "lakebase.password": "test-state-password",
            "snapshot.mode": "initial",
            "cdc.shared.session": "false",
            **extra,
        }

    def test_shared_read_drains_via_daemon_and_serves_pages(self):
        # A unique scope per invocation: the drain pool is a module-global keyed by scope,
        # and these classes are re-run under two file paths in one process, so a fixed
        # scope would make the second run reuse the first's pool + torn-down state.
        scope_hex = hashlib.sha256(os.urandom(16)).hexdigest()[:32]

        def fake_factory():
            reader = InformixLakeflowConnect(self._options(**{"snapshot.shared.session": "false"}))
            reader.set_registration_scope(scope_hex)
            reader._bridge_instance = FakeBridge()
            return reader

        consumer = InformixLakeflowConnect(
            self._options(**{"snapshot.shared.session": "true", "snapshot.reader.threads": "1"})
        )
        consumer.set_registration_scope(scope_hex)
        consumer._bridge_instance = FakeBridge()
        # Give the daemon a fake-bridge reader factory sharing this test's offline state
        # and stage, so no real connection is attempted.
        consumer._snapshot_drain_reader_factory = lambda: fake_factory

        released = []
        original_release = consumer._release_worker_connection

        def spy_release():
            released.append(True)
            return original_release()

        consumer._release_worker_connection = spy_release

        rows, offset = consumer.read_table("app.orders", {}, {})
        rows = list(rows)

        self.assertTrue(rows, "the consumer served the daemon-staged snapshot")
        self.assertTrue(released, "the consumer freed its slot before waiting on the daemon")
        # A single-page snapshot serves its only (final) page and hands straight to the
        # stream; a multi-page one stays in the snapshot phase. Either way it advanced.
        self.assertIn(offset.get("phase"), ("snapshot", "stream"))

    def test_shared_read_serves_existing_manifest_without_daemon(self):
        # When a prior drain already published the manifest, the shared-mode consumer
        # serves it directly and never enqueues a daemon job.
        scope_hex = hashlib.sha256(os.urandom(16)).hexdigest()[:32]
        # The primer drains inline (Model A) to pre-publish the manifest without a daemon.
        primer = InformixLakeflowConnect(self._options(**{"snapshot.shared.session": "false"}))
        primer.set_registration_scope(scope_hex)
        primer._bridge_instance = FakeBridge()
        primer.read_table("app.orders", {}, {})

        consumer = InformixLakeflowConnect(self._options(**{"snapshot.shared.session": "true"}))
        consumer.set_registration_scope(scope_hex)
        consumer._bridge_instance = FakeBridge()

        def _boom():
            raise AssertionError("daemon path must not run when a manifest already exists")

        consumer._drain_via_snapshot_daemon = lambda *a, **k: _boom()

        rows, _ = consumer.read_table("app.orders", {}, {})
        self.assertTrue(list(rows))

    def test_shared_read_drops_state_connection_before_waiting(self):
        # Regression: the drain consumer must not hold its Lakebase connection across the
        # wait. The manifest probe just before the wait leaves psycopg idle inside a read
        # transaction, which Lakebase reaps as an idle-in-transaction timeout over a
        # multi-minute drain; the post-wait manifest read then died with
        # IdleInTransactionSessionTimeout. The consumer now drops the cached connection
        # first, so the post-wait read reopens a fresh one.
        scope_hex = hashlib.sha256(os.urandom(16)).hexdigest()[:32]

        def fake_factory():
            reader = InformixLakeflowConnect(self._options(**{"snapshot.shared.session": "false"}))
            reader.set_registration_scope(scope_hex)
            reader._bridge_instance = FakeBridge()
            return reader

        consumer = InformixLakeflowConnect(
            self._options(**{"snapshot.shared.session": "true", "snapshot.reader.threads": "1"})
        )
        consumer.set_registration_scope(scope_hex)
        consumer._bridge_instance = FakeBridge()
        consumer._snapshot_drain_reader_factory = lambda: fake_factory

        seen = []
        original_reset = consumer._reset_lakebase_connection

        def spy_reset():
            # Capture the connection cached at reset time (the manifest probe opened one)
            # and confirm the reset actually closes and clears it.
            seen.append(getattr(consumer, "_lakebase_conn", None))
            return original_reset()

        consumer._reset_lakebase_connection = spy_reset

        rows, _ = consumer.read_table("app.orders", {}, {})

        self.assertTrue(list(rows), "the consumer served the daemon-staged snapshot")
        self.assertTrue(seen, "the consumer reset its Lakebase connection before the drain wait")
        pre_wait_conn = seen[0]
        self.assertIsNotNone(pre_wait_conn, "the manifest probe opened a state connection")
        self.assertTrue(pre_wait_conn.closed, "the pre-wait state connection was closed")
        self.assertIsNot(
            consumer._lakebase_conn,
            pre_wait_conn,
            "the post-wait manifest read reopened a fresh connection",
        )


class SnapshotIsolationOptionTests(unittest.TestCase):
    """The snapshot.isolation table option selects the SET ISOLATION level used by the
    monolithic initial snapshot, defaulting to the non-locking COMMITTED READ LAST
    COMMITTED."""

    def setUp(self):
        self._shared_state = tempfile.TemporaryDirectory()
        self._lakebase = _OfflineLakebase().install(self)

    def tearDown(self):
        self._shared_state.cleanup()

    def _connector(self, **options):
        connector = InformixLakeflowConnect(
            {
                "database": "demo",
                "snapshot.staging.location": self._shared_state.name,
                "lakebase.password": "test-state-password",
                "snapshot.mode": "initial",
                "snapshot.shared.session": "false",
                "cdc.shared.session": "false",
                **options,
            }
        )
        # Unique scope per connector: staged snapshot state is keyed by scope and these
        # classes run under two file paths in one process, so a fixed scope would collide.
        connector.set_registration_scope(hashlib.sha256(os.urandom(16)).hexdigest()[:32])
        connector._bridge_instance = FakeBridge()
        return connector

    def test_defaults_to_last_committed(self):
        self.assertEqual(self._connector()._snapshot_isolation({}), "COMMITTED READ LAST COMMITTED")

    def test_repeatable_read_selectable(self):
        self.assertEqual(
            self._connector()._snapshot_isolation({"snapshot.isolation": "repeatable_read"}),
            "REPEATABLE READ",
        )

    def test_token_is_whitespace_and_case_insensitive(self):
        self.assertEqual(
            self._connector()._snapshot_isolation(
                {"snapshot.isolation": "Committed Read Last Committed"}
            ),
            "COMMITTED READ LAST COMMITTED",
        )

    def test_invalid_value_raises(self):
        with self.assertRaises(ValueError):
            self._connector()._snapshot_isolation({"snapshot.isolation": "dirty_read"})

    def test_initial_snapshot_uses_last_committed_by_default(self):
        connector = self._connector()
        connector.read_table("app.orders", {}, {})
        self.assertEqual(
            connector._bridge_instance.snapshot_isolations, ["COMMITTED READ LAST COMMITTED"]
        )

    def test_initial_snapshot_honors_repeatable_read_table_option(self):
        connector = self._connector()
        connector.read_table("app.orders", {}, {"snapshot.isolation": "repeatable_read"})
        self.assertEqual(connector._bridge_instance.snapshot_isolations, ["REPEATABLE READ"])

    def test_bridge_emits_last_committed_set_isolation(self):
        # The transaction helper interpolates the isolation into the SET statement;
        # confirm the last-committed clause reaches Informix verbatim.
        class RecordingTransport:
            def __init__(self):
                self.sql = []

            def execute(self, sql, parameters=(), max_result_bytes=None):
                del parameters, max_result_bytes
                self.sql.append(sql)
                if "sysmaster:sysdatabases" in sql:
                    return [{"is_ansi": 0}]
                return []

            def execute_command(self, sql):
                self.sql.append(f"COMMAND:{sql}")

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {"database": "demo"}
        bridge.config = {"database": "demo"}
        bridge.transport = RecordingTransport()

        with bridge._repeatable_read_transaction("COMMITTED READ LAST COMMITTED"):
            pass
        self.assertIn(
            "COMMAND:SET ISOLATION TO COMMITTED READ LAST COMMITTED", bridge.transport.sql
        )

    def test_bridge_rejects_unknown_isolation(self):
        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {"database": "demo"}
        bridge.config = {"database": "demo"}
        with self.assertRaises(informix_module.InformixError):
            with bridge._repeatable_read_transaction("DROP TABLE users"):
                pass


if __name__ == "__main__":
    unittest.main()
