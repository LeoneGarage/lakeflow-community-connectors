"""Source-local Lakeflow contract regressions using an in-memory bridge."""

from __future__ import annotations

import base64
import dataclasses
import errno
import gzip
import hashlib
import hmac
import importlib
import json
import logging
import os
import pickle
import shutil
import socket
import stat
import sys
import tempfile
import threading
import time
import types
import unittest
from datetime import date, datetime, timezone
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
    SqliTransportError,
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


class FakeBridge:
    def __init__(self):
        self.tables = [_table(), _table("sysadmin", "hidden"), _table(name="audit")]
        self.rows = [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}]
        self.changes = []
        self.now, self.minimum = 90, 1
        self.utc_now = 1_700_000_000  # source UTC epoch seconds for snapshot commit-time
        self.snapshot_calls = []
        self.snapshot_max_bytes = []
        self.snapshot_max_rows = []
        self.snapshot_page_sizes = []
        self.snapshot_filters = []
        self.snapshot_isolations = []
        # The key columns each consistent_snapshot call was asked to page by; () is the
        # unordered cursor scan a keyless table, or snapshot.scan=cursor, takes.
        self.snapshot_primary_keys = []
        # Column names the fake catalog reports as having a non-NULL default.
        self.column_default_names = set()
        self.prepared_identities = []
        self.validated_initial = []
        self.change_reads = []
        self.released_connections = 0
        # Optional {(col, ...): [descending, ...]} for key_index_directions,
        # simulating an index whose columns match a primary.keys override.
        self.index_directions = {}
        # Renders the "__chunk_<key>" ordering string for keys chunked by
        # expression; identity by default, overridden to simulate a
        # DATETIME-to-string cast.
        self.chunk_key_fn = lambda key, value: value

    def list_tables(self):
        return self.tables

    def get_table(self, identity):
        return next(t for t in self.tables if identity.endswith(f".{t['owner']}.{t['name']}"))

    def column_defaults(self, identity, columns):
        del identity
        return sorted(column for column in columns if column in self.column_default_names)

    def key_index_directions(self, identity, columns):
        # Directions of an index matching the override columns; configured per
        # test via index_directions {(col, ...): [desc, ...]}. Default: no match.
        del identity
        return self.index_directions.get(tuple(columns))

    def current_lsn(self):
        return self.now

    def minimum_lsn(self):
        return self.minimum

    def current_utc_time(self):
        # Fixed source UTC epoch seconds (2023-11-14T22:13:20Z) for snapshot commit-time.
        return self.utc_now

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
        key_descending=None,
    ):
        self.snapshot_max_bytes.append(max_bytes)
        self.snapshot_filters.append(snapshot_filter)
        self.snapshot_calls.append((identity, tuple(columns), tuple(primary_keys), after, limit))
        chunk_exprs = dict(chunk_exprs or {})
        # Mirror the real bridge: page in the key index's own order, so a
        # mixed-direction key sorts DESC columns descending. Chunked keys and
        # all-ascending keys keep the plain lexicographic order.
        descending = () if chunk_exprs else tuple(key_descending or ())

        def cursor_value(row, key):
            # Simulate the "__chunk_<key>" alias: chunk_key_fn renders the
            # order-preserving string form for keys chunked by expression.
            if key in chunk_exprs:
                return self.chunk_key_fn(key, row[key])
            return row[key]

        def order_key(row):
            return informix_module._order_key(
                tuple(cursor_value(row, key) for key in primary_keys), descending
            )

        ordered = sorted(self.rows, key=order_key)
        if after is not None:
            if len(after) != len(primary_keys):
                raise AssertionError("snapshot continuation arity changed")
            after_order = informix_module._order_key(tuple(after), descending)
            ordered = [row for row in ordered if order_key(row) > after_order]
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
        del identity, columns, datetime_primary_key
        self.snapshot_primary_keys.append(tuple(primary_keys))
        self.snapshot_max_bytes.append(max_bytes)
        self.snapshot_max_rows.append(max_rows)
        self.snapshot_page_sizes.append(page_size)
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
        key_descending=None,
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
            key_descending=key_descending,
        )
        return self.now, rows

    def max_primary_key(
        self, identity, primary_keys, chunk_exprs=None, snapshot_filter=None, key_descending=None
    ):
        self.snapshot_filters.append(snapshot_filter)
        if not self.rows:
            return None
        chunk_exprs = dict(chunk_exprs or {})
        descending = () if chunk_exprs else tuple(key_descending or ())

        def cursor_value(row, key):
            if key in chunk_exprs:
                return self.chunk_key_fn(key, row[key])
            return row[key]

        # The "maximum" is the last tuple in the key's own paging order.
        top = max(
            self.rows,
            key=lambda row: informix_module._order_key(
                tuple(cursor_value(row, key) for key in primary_keys), descending
            ),
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
        # The state-validation coordinator memoizes resolved tables and leases by
        # (scope, identity) for the life of the process. Reset it per test too: without
        # this, a test asserting a cold-cache lookup (get_table called once) fails when
        # a prior test -- e.g. the same class re-collected via the tests/unit re-export
        # module -- already warmed the same hardcoded scope. Clearing it here is a
        # between-tests reset, so tests that rely on cross-instance sharing within a
        # single test are unaffected.
        coordinator = informix_module._STATE_VALIDATION_COORDINATOR
        coordinator.table_caches.clear()
        return self

    def _connect(self, _state=None):
        connection = self._fake._FakeConnection(self.database)
        self.connections.append(connection)
        return connection

    def seed(self, namespace, slot_count):
        from databricks.labs.community_connector.sources.informix import lakebase_state

        lakebase_state.seed_slots(self._connect(), namespace, slot_count)


def _committed_transactions(records):
    """The committed transactions of one poll's records."""
    return informix_module._transaction_batch(records)[0]


def _no_shared_cdc_view(*_args, **_kwargs):
    """Stand-in for _shared_cdc_snapshot: the shard has no view, so the poll reads directly."""
    return None


def _read_cdc_directly(connector):
    """Keep a connector's CDC polls off the shared shards, whose daemon would build its own
    bridge: each poll reads directly through the injected one, as a poll does while its
    shard has no view yet."""
    connector._shared_cdc_snapshot = _no_shared_cdc_view
    return connector


def _snapshot_drain_worker_stand_in():
    """Stand-in for _is_snapshot_drain_worker: this connector drains its own snapshots."""
    return True


def _drain_snapshots_inline(connector):
    """Drain a connector's snapshots on the connector itself, through its injected bridge,
    as a drain-pool worker does, instead of handing them to a pool worker that would build
    its own bridge."""
    connector._is_snapshot_drain_worker = _snapshot_drain_worker_stand_in
    return connector


class LakeflowContractTests(unittest.TestCase):
    def setUp(self):
        self._shared_state = tempfile.TemporaryDirectory()
        self._lakebase = _OfflineLakebase().install(self)

    def tearDown(self):
        self._shared_state.cleanup()

    def connector(self, bridge=None, *, shared_cdc=False, **options):
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
                # Sharded CDC and the snapshot-drain pool are always on, but these
                # per-table contract tests inject a bridge directly and must not spawn a
                # background daemon (which builds its own bridge and would attempt a real
                # connection): snapshots drain on this connector (see
                # _drain_snapshots_inline) and CDC polls read directly (see
                # _read_cdc_directly). Shared-CDC tests opt in explicitly.
                # Partitioned-reader tests opt in explicitly.
                "stream.partitioned": "false",
                **options,
            }
        )
        connector.set_registration_scope(hashlib.sha256(scope_label.encode()).hexdigest()[:32])
        connector._bridge_instance = bridge or FakeBridge()
        _drain_snapshots_inline(connector)
        return connector if shared_cdc else _read_cdc_directly(connector)

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

    def test_cdc_row_carries_commit_time_from_commit_record(self):
        # The COMMIT record's source timestamp (epoch seconds) is stamped onto every row
        # of the transaction as _informix_commit_time (a UTC datetime).
        bridge = FakeBridge()
        connector = self.connector(bridge)
        _, checkpoint = connector.read_table("app.orders", {}, {})
        bridge.changes = [
            {"op": "BEGIN", "tx_id": 7, "lsn": 101, "timestamp": 1_700_000_500},
            {"op": "INSERT", "tx_id": 7, "lsn": 102, "row": {"id": 99, "value": "x"}},
            {"op": "COMMIT", "tx_id": 7, "lsn": 103, "timestamp": 1_700_000_500},
        ]
        rows, _ = connector.read_table("app.orders", checkpoint, {})
        changed = next(row for row in rows if row["id"] == 99)
        self.assertEqual(changed["_informix_op"], "c")
        self.assertEqual(
            changed[informix_module.COMMIT_TIME],
            informix_module._commit_time_value(1_700_000_500),
        )

    def test_cdc_row_commit_time_is_null_when_commit_record_has_no_timestamp(self):
        bridge = FakeBridge()
        connector = self.connector(bridge)
        _, checkpoint = connector.read_table("app.orders", {}, {})
        bridge.changes = [
            {"op": "BEGIN", "tx_id": 7, "lsn": 101},
            {"op": "INSERT", "tx_id": 7, "lsn": 102, "row": {"id": 99, "value": "x"}},
            {"op": "COMMIT", "tx_id": 7, "lsn": 103},
        ]
        rows, _ = connector.read_table("app.orders", checkpoint, {})
        changed = next(row for row in rows if row["id"] == 99)
        self.assertIsNone(changed[informix_module.COMMIT_TIME])

    def test_snapshot_row_carries_source_utc_commit_time(self):
        # Snapshot ("r") rows are stamped with the source-server UTC time captured at the
        # snapshot boundary (FakeBridge.current_utc_time), so they carry an as-of time
        # rather than NULL.
        bridge = FakeBridge()
        connector = self.connector(bridge)
        rows, _ = connector.read_table("app.orders", {}, {})
        rows = list(rows)
        self.assertTrue(rows)
        for row in rows:
            self.assertEqual(row[informix_module.OP], "r")
            self.assertEqual(
                row[informix_module.COMMIT_TIME],
                informix_module._commit_time_value(bridge.utc_now),
            )

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

        self.assertFalse(restored.table_caches)
        self.assertFalse(restored.unsupported_column_warnings)

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

    def test_generated_source_class_pickles_under_the_broadcast_threshold(self):
        # Spark pickles the merged data source by value. Above 1 MiB PySpark ships
        # the pickle as a broadcast file, which serverless pipelines fail to load
        # (NoSuchFileException in the sandboxed Python worker). The connector keeps
        # its docstrings as "# Docstring:" comments, which are not pickled, to stay
        # under it; new docstrings or enough new code fail here instead of in a
        # pipeline.
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
        registry = mock.Mock()
        spark = types.SimpleNamespace(dataSource=registry)
        generated.register_lakeflow_source(spark)
        source_class = registry.register.call_args.args[0]

        self.assertLess(len(cloudpickle.dumps(source_class)), 1 << 20)

    def test_generated_state_op_reconnect_resolves_connection_lost_types(self):
        # Regression: connection_lost_error_types cached via a `global` rebind
        # crashed with NameError in the single-file merged deployment, where the
        # whole module is nested inside register_lakeflow_source() so its module
        # globals become that function's locals and `global` addresses a name
        # nothing ever assigns. The standalone-module unit tests import the real
        # module (where the global exists) and could not catch it -- this drives
        # the merged form the pipeline actually runs.
        generated = importlib.import_module(
            "databricks.labs.community_connector.sources.informix."
            "_generated_informix_python_source"
        )
        registry = mock.Mock()
        spark = types.SimpleNamespace(dataSource=registry)
        generated.register_lakeflow_source(spark)
        source_class = registry.register.call_args.args[0]

        with tempfile.TemporaryDirectory() as staging:
            source = source_class(
                {
                    "database": "demo",
                    "snapshot.staging.location": staging,
                    "lakebase.password": "test-state-password",
                    "stream.partitioned": "false",
                }
            )
            connector = source.lakeflow_connect
            sentinel = object()
            # Exercises the merge-nested connection_lost_error_types() closure that
            # raised NameError in production; the trivial thunk touches no Lakebase
            # connection, so this stays offline.
            result = connector._state_op_with_reconnect(lambda: sentinel, operation="probe")
        self.assertIs(result, sentinel)

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

    def test_catalog_accepts_interval_qualifiers(self):
        column = _catalog_column(
            {"colname": "elapsed", "coltype": 14, "collength": 0x0C4D, "colno": 2}
        )  # DAY(3) TO FRACTION(3), kept in the collength layout the decoder reads
        self.assertEqual((column["type_name"], column["length"]), ("INTERVAL", 0x0C4D))
        self.assertTrue(column["cdc_supported"])
        mixed = _catalog_column({"colname": "bad", "coltype": 14, "collength": 0x0624, "colno": 3})
        self.assertFalse(mixed["cdc_supported"])
        fraction = _catalog_column(
            {"colname": "jitter", "coltype": 14, "collength": 0x03CD, "colno": 4}
        )  # FRACTION TO FRACTION(3)
        self.assertTrue(fraction["cdc_supported"])

    def test_catalog_names_blob_and_clob_columns(self):
        # A live catalog reports both as fixed-length opaque types (coltype 41) named by
        # their extended type; any other opaque type keeps its generic name.
        for extended, expected in (("blob", "BLOB"), ("clob", "CLOB"), ("point", "UDT_FIXED")):
            with self.subTest(extended):
                column = _catalog_column(
                    {
                        "colname": "doc",
                        "coltype": 41,
                        "collength": 72,
                        "colno": 2,
                        "extended_name": extended,
                        "extended_owner": "informix",
                    }
                )
                self.assertEqual(column["type_name"], expected)
                self.assertFalse(column["cdc_supported"])

    def test_catalog_accepts_nchar(self):
        column = _catalog_column({"colname": "code", "coltype": 15, "collength": 10, "colno": 2})
        self.assertEqual((column["type_name"], column["length"]), ("NCHAR", 10))
        self.assertTrue(column["cdc_supported"])
        raw = _table()
        raw["columns"].append({"name": "code", "type_name": "NCHAR", "length": 10})
        table = Table.parse(raw, "demo")
        self.assertEqual(informix_module._snapshot_unsupported_columns(table), ())

    def test_an_interval_column_streams_and_lands_as_a_string(self):
        bridge = FakeBridge()
        raw = _table()
        raw["columns"].append(
            {"name": "elapsed", "type_name": "INTERVAL", "length": 0x084A, "nullable": True}
        )
        bridge.tables = [raw]
        connector = self.connector(bridge)

        metadata = connector.read_table_metadata("app.orders", {})
        fields = {f.name: f.dataType for f in connector.get_table_schema("app.orders", {}).fields}

        self.assertEqual(metadata["ingestion_type"], "cdc_with_deletes")
        self.assertEqual(type(fields["elapsed"]).__name__, "StringType")
        # UNLOAD right-aligns the text; the file path lands it as the decoders do.
        self.assertEqual(informix_module._unload_value("  -5 10:30:00", "INTERVAL"), "-5 10:30:00")
        self.assertEqual(informix_module._unload_value(" .016", "INTERVAL"), ".016")

    def test_an_interval_key_is_copied_by_the_blocking_snapshot(self):
        raw = _table()
        raw["columns"][0] = {"name": "id", "type_name": "INTERVAL", "length": 0x084A}
        table = Table.parse(raw, "demo")
        connector = self.connector(FakeBridge())
        self.assertEqual(connector._incremental_datetime_chunk_exprs(table, {}), ({}, False))
        self.assertIn("INTERVAL", informix_module._LITERAL_UNSAFE_KEY_TYPES)

    def test_a_float_key_is_copied_by_the_blocking_snapshot(self):
        # A keyset bound renders the last key into SQL, and a float has no exact literal
        # (the renderer raises TypeError on one), so a float key pages by position.
        for type_name in ("FLOAT", "SMALLFLOAT"):
            with self.subTest(type_name=type_name):
                raw = _table()
                raw["columns"][0] = {"name": "id", "type_name": type_name}
                table = Table.parse(raw, "demo")
                connector = self.connector(FakeBridge())
                self.assertEqual(
                    connector._incremental_datetime_chunk_exprs(table, {}), ({}, False)
                )
                self.assertIn(type_name, informix_module._LITERAL_UNSAFE_KEY_TYPES)

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
                informix_module, "decode_frame", side_effect=lambda frame, labels, **_: dict(frame)
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
                informix_module, "decode_frame", side_effect=lambda frame, labels, **_: dict(frame)
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
                informix_module, "decode_frame", side_effect=lambda frame, labels, **_: dict(frame)
            ),
            mock.patch.object(informix_module, "metadata_column_names", return_value=["id"]),
            mock.patch.object(bridge, "_describe_tables", return_value={}),
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
        original = _capture_descriptor(Table.parse(_table(), "demo"), "utf-8")

        bridge._assert_capture_layout(original, "utf-8", {("app", "orders"): _table()})
        for catalog in ({("app", "orders"): changed}, {}):
            with self.subTest(catalog=catalog):
                with self.assertRaisesRegex(InformixError, "schema changed.*full refresh"):
                    bridge._assert_capture_layout(original, "utf-8", catalog)

    def _read_with_metadata(self, captures, records):
        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.transport = FakeCdcTransport([records])
        bridge._describe_table = mock.Mock(side_effect=AssertionError("read one table"))
        bridge._describe_tables = mock.Mock(
            return_value={("app", "orders"): _table(), ("app", "lines"): _table(name="lines")}
        )
        with (
            mock.patch.object(informix_module, "CdcFrameParser", RecordParser),
            mock.patch.object(
                informix_module, "decode_frame", side_effect=lambda frame, labels, **_: dict(frame)
            ),
            mock.patch.object(
                informix_module, "metadata_column_names", return_value=["id", "value"]
            ),
        ):
            bridge.read_changes(captures, 90, 1, 64)
        return bridge

    def test_native_poll_reads_every_captured_tables_catalog_in_one_batch(self):
        captures = [
            _capture_descriptor(Table.parse(_table(name=name), "demo"), "utf-8")
            for name in ("orders", "lines")
        ]
        bridge = self._read_with_metadata(
            captures,
            [
                {"op": "METADATA", "label": 1, "metadata": b"id integer, value varchar(20)"},
                {"op": "METADATA", "label": 2, "metadata": b"id integer, value varchar(20)"},
                {"op": "TIMEOUT", "lsn": 100},
            ],
        )

        bridge._describe_tables.assert_called_once_with({("app", "orders"), ("app", "lines")})

    def test_native_poll_without_metadata_reads_no_catalog(self):
        capture = _capture_descriptor(Table.parse(_table(), "demo"), "utf-8")
        bridge = self._read_with_metadata([capture], [{"op": "TIMEOUT", "lsn": 100}])

        bridge._describe_tables.assert_not_called()

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
                informix_module, "decode_frame", side_effect=lambda frame, labels, **_: dict(frame)
            ),
            mock.patch.object(informix_module, "metadata_column_names", return_value=["id"]),
            mock.patch.object(bridge, "_describe_tables", return_value={}),
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
                informix_module, "decode_frame", side_effect=lambda frame, labels, **_: dict(frame)
            ),
        ):
            bridge.read_changes([capture], 90, 60, 2)

        self.assertEqual(transport.timeouts, [65.0, 30.0])

    def test_native_poll_restores_socket_timeout_after_a_lost_lease(self):
        # A lost lease skips the session teardown, but the raised timeout must still be
        # put back: the client keeps it for every later connection.
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
        bridge._connection_lease_lost = threading.Event()
        bridge._connection_lease_lost.set()
        capture = {
            "identity": "demo:app.orders",
            "logical_identity": "demo.app.orders",
            "columns": ["id"],
            "descriptors": [{"name": "id", "type_name": "INTEGER"}],
        }

        with (
            mock.patch.object(informix_module, "CdcFrameParser", RecordParser),
            mock.patch.object(
                informix_module, "decode_frame", side_effect=lambda frame, labels, **_: dict(frame)
            ),
        ):
            bridge.read_changes([capture], 90, 60, 2)

        self.assertEqual(transport.timeouts, [65.0, 30.0])

    @staticmethod
    def _stepwise_bridge(answers, options=None):
        # A plain-column key resolves stepwise inside a REPEATABLE READ view, so
        # the transport must answer the is_ansi probe and accept BEGIN/COMMIT.
        class StepwiseTransport:
            def __init__(self):
                self.socket_timeout = 30.0
                self.timeouts = []
                self.selects = []
                self._index = 0

            def set_socket_timeout(self, timeout):
                self.socket_timeout = timeout
                self.timeouts.append(timeout)

            def execute(self, sql, parameters=()):
                if "sysmaster:sysdatabases" in sql:
                    return [{"is_ansi": 0}]
                if sql.startswith("SELECT"):
                    # The read must run under the extended budget, not the default.
                    assert self.socket_timeout == options_dict_timeout
                    self.selects.append((sql, tuple(parameters)))
                    rows = answers[self._index]
                    self._index += 1
                    return rows
                return []

            def execute_command(self, sql):
                del sql

        options_dict = dict(options or {})
        options_dict_timeout = float(options_dict.get("snapshot.read.timeout.seconds", "300"))
        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = options_dict
        bridge.config = {"database": "demo"}
        bridge.transport = StepwiseTransport()
        return bridge

    def test_max_primary_key_composite_resolves_stepwise(self):
        # tw305's key: hpolicy (CHAR) + clm_no (DECIMAL). The max tuple is found
        # one column at a time so each read is an index sub-range scan, never a
        # whole-table sort -- and it holds even if the index mixes ASC/DESC.
        bridge = self._stepwise_bridge([[{"hpolicy": "H9"}], [{"clm_no": 5}]])

        result = bridge.max_primary_key("demo.app.tw305", ["hpolicy", "clm_no"])

        self.assertEqual(result, ["H9", 5])
        selects = bridge.transport.selects
        self.assertEqual(len(selects), 2)
        first_sql, first_params = selects[0]
        second_sql, second_params = selects[1]
        # First column: no equality prefix, ordered DESC, directive present.
        self.assertIn("{+FIRST_ROWS(1)}", first_sql)
        self.assertIn("FIRST 1 hpolicy", first_sql)
        self.assertNotIn("WHERE", first_sql)
        self.assertTrue(first_sql.rstrip().endswith("ORDER BY hpolicy DESC"))
        self.assertEqual(first_params, ())
        # Second column: pinned to the resolved first column via a bound param.
        self.assertIn("FIRST 1 clm_no", second_sql)
        self.assertIn("WHERE hpolicy = ?", second_sql)
        self.assertTrue(second_sql.rstrip().endswith("ORDER BY clm_no DESC"))
        self.assertEqual(second_params, ("H9",))

    def test_max_primary_key_stepwise_empty_table_returns_none(self):
        bridge = self._stepwise_bridge([[]])

        self.assertIsNone(bridge.max_primary_key("demo.app.tw305", ["hpolicy", "clm_no"]))
        # No second query once the first column has no rows.
        self.assertEqual(len(bridge.transport.selects), 1)

    def test_max_primary_key_stepwise_applies_and_restores_timeout(self):
        bridge = self._stepwise_bridge([[{"id": 42}]])

        result = bridge.max_primary_key("demo.app.orders", ["id"])

        self.assertEqual(result, [42])
        self.assertEqual(bridge.transport.timeouts, [300.0, 30.0])

    def test_max_primary_key_honors_snapshot_timeout_option(self):
        bridge = self._stepwise_bridge(
            [[{"id": 7}]], options={"snapshot.read.timeout.seconds": "900"}
        )

        bridge.max_primary_key("demo.app.orders", ["id"])

        self.assertEqual(bridge.transport.timeouts, [900.0, 30.0])

    def test_max_primary_key_mixed_direction_flips_the_fetch_order(self):
        # Key (hpolicy ASC, clm_no DESC). The last tuple in the index's own scan
        # order is (max hpolicy, then MIN clm_no within it), so the leading column
        # fetches DESC (its max) and the descending column fetches ASC (its min).
        bridge = self._stepwise_bridge([[{"hpolicy": "H9"}], [{"clm_no": 1}]])

        result = bridge.max_primary_key(
            "demo.app.tw305", ["hpolicy", "clm_no"], key_descending=[False, True]
        )

        self.assertEqual(result, ["H9", 1])
        first_sql, _ = bridge.transport.selects[0]
        second_sql, second_params = bridge.transport.selects[1]
        self.assertTrue(first_sql.rstrip().endswith("ORDER BY hpolicy DESC"))
        self.assertIn("WHERE hpolicy = ?", second_sql)
        self.assertTrue(second_sql.rstrip().endswith("ORDER BY clm_no ASC"))
        self.assertEqual(second_params, ("H9",))

    def test_max_primary_key_datetime_chunk_keeps_single_composite_read(self):
        # A DATETIME-chunked key cannot use the stepwise equality prefix (no index
        # serves an order-preserving cast expression), so it stays a single
        # composite read with the FIRST_ROWS directive and the extended budget.
        class TimedTransport:
            def __init__(self):
                self.socket_timeout = 30.0
                self.timeouts = []
                self.sql = None

            def set_socket_timeout(self, timeout):
                self.socket_timeout = timeout
                self.timeouts.append(timeout)

            def execute(self, sql, params):
                assert self.socket_timeout == 300.0
                assert params == ()
                self.sql = sql
                return [{"__chunk_ts": "2026-01-01"}]

        transport = TimedTransport()
        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.transport = transport

        result = bridge.max_primary_key(
            "demo.app.events", ["ts"], chunk_exprs={"ts": "TO_CHAR(ts)"}
        )

        self.assertEqual(result, ["2026-01-01"])
        self.assertEqual(transport.timeouts, [300.0, 30.0])
        self.assertIn("{+FIRST_ROWS(1)}", transport.sql)
        self.assertIn("TO_CHAR(ts) AS __chunk_ts", transport.sql)
        self.assertTrue(transport.sql.rstrip().endswith("ORDER BY TO_CHAR(ts) DESC"))

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
                informix_module, "decode_frame", side_effect=lambda frame, labels, **_: dict(frame)
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
                informix_module, "decode_frame", side_effect=lambda frame, labels, **_: dict(frame)
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
                informix_module, "decode_frame", side_effect=lambda frame, labels, **_: dict(frame)
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
                informix_module, "decode_frame", side_effect=lambda frame, labels, **_: dict(frame)
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
                informix_module, "decode_frame", side_effect=lambda frame, labels, **_: dict(frame)
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
                informix_module, "decode_frame", side_effect=lambda frame, labels, **_: dict(frame)
            ),
        ):
            records = bridge.read_changes([capture], 90, 1, 2)

        self.assertEqual(len(records), 5)
        # The committed transaction survives so the caller can checkpoint it,
        # and the still-open one is left to be replayed by the next poll.
        committed = _committed_transactions(records)
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
                informix_module, "decode_frame", side_effect=lambda frame, labels, **_: dict(frame)
            ),
        ):
            records = bridge.read_changes([capture], 90, 1, 2)

        self.assertEqual([tx.tx_id for tx in _committed_transactions(records)], [1])

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
                informix_module, "decode_frame", side_effect=lambda frame, labels, **_: dict(frame)
            ),
        ):
            records = bridge.read_changes([capture], 90, 1, 2)

        self.assertEqual([tx.tx_id for tx in _committed_transactions(records)], [1])

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
                informix_module, "decode_frame", side_effect=lambda frame, labels, **_: dict(frame)
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

    def test_locale_honors_spark_lowercased_keys(self):
        # Spark hands option keys to the Python Data Source lowercased, so a
        # user's ``CLIENT_LOCALE`` / ``DB_LOCALE`` arrive as ``client_locale`` /
        # ``db_locale``. The bridge config must pick those up, not fall back to
        # the utf8 default (which produced 0xa0 decode failures against a
        # Latin-1 source).
        config = _bridge_config(
            {
                "hostname": "host",
                "database": "db",
                "user": "user",
                "password": "secret",
                "server": "srv",
                "client_locale": "en_US.819",
                "db_locale": "en_US.819",
            }
        )
        self.assertEqual(config["client_locale"], "en_US.819")
        self.assertEqual(config["db_locale"], "en_US.819")
        self.assertEqual(
            informix_module._client_encoding({"client_locale": "en_US.819"}),
            "iso8859-1",
        )

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

    def test_initial_lsn_validation_retries_a_transient_primary_timeout(self):
        # A socket-read timeout during the primary open/start/activate calls is transient
        # syscdcv1 contention under a startup storm, not a real failure: validate_initial_lsn
        # resets the transport and retries with backoff, and a later attempt succeeds. The
        # dead-socket teardown is skipped on a timed-out attempt (only the succeeding attempt
        # tears its session down), so a retry does not pay a second timeout on cleanup.
        class FlakyActivateTransport:
            def __init__(self):
                self.socket_timeout = 30.0
                self.activate_attempts = 0
                self.endcapture_calls = 0

            def set_socket_timeout(self, timeout):
                self.socket_timeout = timeout

            def execute(self, sql, parameters=()):
                if "sysenv" in sql:
                    return [{"env_value": "demo_server"}]
                if "cdc_opensess" in sql:
                    return [{"session_id": 7}]
                if "cdc_activatesess" in sql:
                    self.activate_attempts += 1
                    if self.activate_attempts <= 2:
                        raise TimeoutError("The read operation timed out")
                    return [{"status": 0}]
                if "cdc_endcapture" in sql:
                    self.endcapture_calls += 1
                return [{"status": 0}]

        transport = FlakyActivateTransport()
        bridge = object.__new__(PurePythonInformixBridge)
        bridge.transport = transport
        bridge.options = {}
        reset_calls = []
        bridge.reset_transport = lambda: reset_calls.append(True)

        with mock.patch.object(informix_module.time, "sleep", lambda *_: None):
            bridge.validate_initial_lsn({"identity": "demo:app.orders", "columns": ["id"]}, 80)

        self.assertEqual(transport.activate_attempts, 3)  # two timeouts, then success
        self.assertEqual(reset_calls, [True, True])  # one transport reset per timed-out attempt
        self.assertEqual(transport.endcapture_calls, 1)  # teardown only on the succeeding attempt
        # A timed-out attempt still puts the timeout back; the client keeps it for every
        # later connection, so the raised one must not become the default.
        self.assertEqual(transport.socket_timeout, 30.0)

    def test_initial_lsn_validation_reraises_an_exhausted_primary_timeout(self):
        # If the primary calls keep timing out past the retry budget, validate_initial_lsn
        # stops retrying and re-raises so a genuinely-unreachable source fails honestly.
        class AlwaysTimeoutTransport:
            def __init__(self):
                self.socket_timeout = 30.0
                self.activate_attempts = 0

            def set_socket_timeout(self, timeout):
                self.socket_timeout = timeout

            def execute(self, sql, parameters=()):
                if "sysenv" in sql:
                    return [{"env_value": "demo_server"}]
                if "cdc_opensess" in sql:
                    return [{"session_id": 7}]
                if "cdc_activatesess" in sql:
                    self.activate_attempts += 1
                    raise TimeoutError("The read operation timed out")
                return [{"status": 0}]

        transport = AlwaysTimeoutTransport()
        bridge = object.__new__(PurePythonInformixBridge)
        bridge.transport = transport
        bridge.options = {}
        bridge.reset_transport = lambda: None

        with (
            mock.patch.object(informix_module.time, "sleep", lambda *_: None),
            self.assertRaises(TimeoutError),
        ):
            bridge.validate_initial_lsn({"identity": "demo:app.orders", "columns": ["id"]}, 80)

        # One initial attempt plus the full retry budget, then it gives up.
        self.assertEqual(
            transport.activate_attempts, informix_module._VALIDATE_LSN_RETRY_MAX_RETRIES + 1
        )

    def test_initial_lsn_validation_does_not_retry_a_non_timeout_primary_error(self):
        # A server-returned failure (nonzero cdc_startcapture) is a real error, not transient
        # contention, so it is raised on the first attempt without any retry.
        class BadStartTransport:
            def __init__(self):
                self.start_attempts = 0

            def set_socket_timeout(self, timeout):
                pass

            def execute(self, sql, parameters=()):
                if "sysenv" in sql:
                    return [{"env_value": "demo_server"}]
                if "cdc_opensess" in sql:
                    return [{"session_id": 7}]
                if "cdc_startcapture" in sql:
                    self.start_attempts += 1
                    return [{"status": -1}]  # non-timeout server failure
                return [{"status": 0}]

        transport = BadStartTransport()
        bridge = object.__new__(PurePythonInformixBridge)
        bridge.transport = transport
        bridge.options = {}
        reset_calls = []
        bridge.reset_transport = lambda: reset_calls.append(True)

        with self.assertRaisesRegex(InformixError, "cdc_startcapture"):
            bridge.validate_initial_lsn({"identity": "demo:app.orders", "columns": ["id"]}, 80)

        self.assertEqual(transport.start_attempts, 1)  # no retry
        self.assertEqual(reset_calls, [])

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
        self.assertEqual(informix_module._DEFAULT_KEYLESS_SNAPSHOT_PAGE_SIZE, 50000)
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

    def test_snapshot_page_composite_key_carries_first_rows_directive(self):
        # tw305's shape: a two-column key paged by the OR-form keyset. The
        # FIRST_ROWS directive must ride on it so Informix streams the page from
        # the (hpolicy, clm_no) index in order rather than sorting the table.
        class SnapshotTransport:
            def __init__(self):
                self.sql = None
                self.parameters = None

            def execute(self, sql, parameters=(), max_result_bytes=None):
                del max_result_bytes
                self.sql = sql
                self.parameters = parameters
                return []

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.transport = SnapshotTransport()

        # Numeric bound values keep the keyset shape without triggering the
        # DATETIME-key describe probe (orthogonal to the directive under test).
        bridge.snapshot_page(
            "demo.app.tw305",
            ["hpolicy", "clm_no"],
            ["hpolicy", "clm_no"],
            [100, 34],
            500,
        )

        # The redundant leading-column seek bound (hpolicy >= ?) precedes the
        # OR-form so the optimizer can seek to the cursor instead of scanning from
        # the index head; its bind leads the OR-form binds.
        self.assertEqual(
            bridge.transport.sql,
            "SELECT {+FIRST_ROWS(500)} FIRST 500 hpolicy,clm_no FROM demo:app.tw305 "
            "WHERE hpolicy >= ? AND ((hpolicy > ?) OR (hpolicy = ? AND clm_no > ?)) "
            "ORDER BY hpolicy,clm_no",
        )
        self.assertEqual(bridge.transport.parameters, (100, 100, 100, 34))

    def test_snapshot_page_mixed_direction_pages_in_index_order(self):
        # Key (hpolicy ASC, clm_no DESC): the ORDER BY matches the index so a
        # forward scan serves it without a sort, and the keyset comparison flips
        # to "<" for the DESC column ("next in scan order" is a smaller clm_no).
        class SnapshotTransport:
            def execute(self, sql, parameters=(), max_result_bytes=None):
                del max_result_bytes
                self.sql = sql
                self.parameters = parameters
                return []

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.transport = SnapshotTransport()

        bridge.snapshot_page(
            "demo.app.tw305",
            ["hpolicy", "clm_no"],
            ["hpolicy", "clm_no"],
            [100, 34],
            500,
            key_descending=[False, True],
        )

        # Leading hpolicy is ASC, so the seek bound stays ">=" while the DESC
        # trailing column keeps its flipped "<" in the OR-form -- the seek bound
        # reuses is_descending(0) and does not disturb the index-direction order.
        self.assertEqual(
            bridge.transport.sql,
            "SELECT {+FIRST_ROWS(500)} FIRST 500 hpolicy,clm_no FROM demo:app.tw305 "
            "WHERE hpolicy >= ? AND ((hpolicy > ?) OR (hpolicy = ? AND clm_no < ?)) "
            "ORDER BY hpolicy,clm_no DESC",
        )
        self.assertEqual(bridge.transport.parameters, (100, 100, 100, 34))

    def test_snapshot_page_seek_bound_flips_for_descending_leading_column(self):
        # A DESC leading column must seek with "<=" (next-in-scan-order is smaller),
        # matching its index-direction iteration -- not ">=", which would drop the
        # rest of the descending scan.
        class SnapshotTransport:
            def execute(self, sql, parameters=(), max_result_bytes=None):
                del max_result_bytes
                self.sql = sql
                self.parameters = parameters
                return []

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.transport = SnapshotTransport()

        bridge.snapshot_page(
            "demo.app.tw305",
            ["hpolicy", "clm_no"],
            ["hpolicy", "clm_no"],
            [100, 34],
            500,
            key_descending=[True, False],
        )

        self.assertEqual(
            bridge.transport.sql,
            "SELECT {+FIRST_ROWS(500)} FIRST 500 hpolicy,clm_no FROM demo:app.tw305 "
            "WHERE hpolicy <= ? AND ((hpolicy < ?) OR (hpolicy = ? AND clm_no > ?)) "
            "ORDER BY hpolicy DESC,clm_no",
        )
        self.assertEqual(bridge.transport.parameters, (100, 100, 100, 34))

    def test_snapshot_page_seek_bound_omitted_for_null_leading_cursor(self):
        # A NULL leading cursor value (possible under allow.nullable.index) must
        # NOT emit `k1 >= NULL` -- it is UNKNOWN for every row and would return an
        # empty page, silently dropping the rest of the table. The OR-form, which
        # already excludes NULL leading values, is kept as-is.
        class SnapshotTransport:
            def execute(self, sql, parameters=(), max_result_bytes=None):
                del max_result_bytes
                self.sql = sql
                self.parameters = parameters
                return []

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.transport = SnapshotTransport()

        bridge.snapshot_page("demo.app.orders", ["id"], ["id"], [None], 20)

        self.assertEqual(
            bridge.transport.sql,
            "SELECT {+FIRST_ROWS(20)} FIRST 20 id FROM demo:app.orders "
            "WHERE ((id > ?)) ORDER BY id",
        )
        self.assertNotIn(">=", bridge.transport.sql)
        self.assertEqual(bridge.transport.parameters, (None,))

    def test_snapshot_page_seek_bound_omitted_for_chunked_leading_key(self):
        # An order-preserving cast (chunk_exprs) leading key gets no seek bound:
        # no index serves the cast, so the bound would neither seek nor stay
        # provably redundant. (A numeric cursor here keeps the DATETIME describe
        # probe out of the way; the guard is chunk_exprs membership, not value type.)
        class SnapshotTransport:
            def execute(self, sql, parameters=(), max_result_bytes=None):
                del max_result_bytes
                self.sql = sql
                self.parameters = parameters
                return []

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.transport = SnapshotTransport()

        bridge.snapshot_page(
            "demo.app.events",
            ["ts"],
            ["ts"],
            [12345],
            20,
            chunk_exprs={"ts": "TO_CHAR(ts)"},
        )

        self.assertNotIn(">=", bridge.transport.sql)
        self.assertNotIn("<=", bridge.transport.sql)
        self.assertEqual(
            bridge.transport.sql,
            "SELECT {+FIRST_ROWS(20)} FIRST 20 ts,TO_CHAR(ts) AS __chunk_ts FROM demo:app.events "
            "WHERE ((TO_CHAR(ts) > ?)) ORDER BY TO_CHAR(ts)",
        )
        self.assertEqual(bridge.transport.parameters, (12345,))

    def test_snapshot_page_chunked_key_ignores_direction(self):
        # A chunked (DATETIME) key has no direction-aware index to match, so even
        # with key_descending set the page keeps the default ascending order.
        class SnapshotTransport:
            def execute(self, sql, parameters=(), max_result_bytes=None):
                del parameters, max_result_bytes
                self.sql = sql
                return []

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.transport = SnapshotTransport()

        bridge.snapshot_page(
            "demo.app.events",
            ["ts"],
            ["ts"],
            None,
            2,
            chunk_exprs={"ts": "TO_CHAR(ts)"},
            key_descending=[True],
        )

        self.assertTrue(bridge.transport.sql.rstrip().endswith("ORDER BY TO_CHAR(ts)"))

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
            "SELECT {+FIRST_ROWS(20)} FIRST 20 id FROM demo:app.orders "
            "WHERE (status = 'A') AND id >= ? AND ((id > ?)) ORDER BY id",
        )
        self.assertEqual(bridge.transport.parameters, (10, 10))

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
            "SELECT {+FIRST_ROWS(2)} SKIP 10000 FIRST 2 event_time FROM demo:app.events "
            "ORDER BY event_time",
        )

    def test_snapshot_bridge_delimits_column_and_key_names_that_need_it(self):
        # "+" and "." pass validation but are not undelimited identifiers: sent bare,
        # "a.b" would read column b of a table a.
        class SnapshotTransport:
            def __init__(self):
                self.sql = None

            def execute(self, sql, parameters=(), max_result_bytes=None):
                self.sql = sql
                return []

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.transport = SnapshotTransport()

        bridge.snapshot_page("demo.app.events", ["a.b", "c+d"], ["a.b"], [7], 2)

        self.assertEqual(
            bridge.transport.sql,
            'SELECT {+FIRST_ROWS(2)} FIRST 2 "a.b","c+d" FROM demo:app.events '
            'WHERE "a.b" >= ? AND (("a.b" > ?)) ORDER BY "a.b"',
        )

    def test_snapshot_pages_describe_the_table_once(self):
        # A string cursor value is checked against the key's type; describing the table
        # for it on every page cost catalog queries inside each page's transaction.
        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.transport = mock.Mock(spec=["execute"])
        bridge.transport.execute.return_value = []
        described = {"columns": [{"name": "code", "type_name": "CHAR", "length": 8}]}
        with mock.patch.object(bridge, "_describe_table", return_value=described) as describe:
            for after in (["a"], ["b"]):
                bridge.snapshot_page("demo.app.codes", ["code"], ["code"], after, 2)

        describe.assert_called_once_with("app", "codes")

    def test_a_non_ansi_database_is_probed_once(self):
        for is_ansi, probes in ((0, 1), (1, 2)):
            with self.subTest(is_ansi=is_ansi):
                bridge = object.__new__(PurePythonInformixBridge)
                bridge.options = {}
                bridge.config = {"database": "demo"}
                bridge.transport = mock.Mock(spec=["execute", "execute_command"])
                bridge.transport.execute.return_value = [{"is_ansi": is_ansi}]
                for _ in range(2):
                    with bridge._repeatable_read_transaction():
                        pass

                # An ANSI database keeps its probe: it opens the implicit transaction
                # that the following COMMIT WORK ends.
                self.assertEqual(bridge.transport.execute.call_count, probes)

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
            'SELECT {+FIRST_ROWS(5)} FIRST 5 id FROM demo:"jacqueline.clarke".orders ORDER BY id',
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
            'SELECT {+FIRST_ROWS(2)} FIRST 2 event_time FROM demo:"HCF+gbc"."a+b" ORDER BY event_time',
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
                if "FIRST_ROWS" in sql:
                    return [{"id": 1}]
                return []

            def execute_command(self, sql):
                self.sql.append(f"COMMAND:{sql}")

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {"database": "demo"}
        bridge.config = {"database": "demo"}
        bridge.transport = TransactionalTransport()

        lsn, rows = bridge.consistent_snapshot("demo.app.orders", ["id"], ["id"], 10, 100, 1 << 20)

        # Conservative boundary: start of the current log page (see boundary_lsn).
        self.assertEqual(lsn, (2 << 32) + (2 << 12))
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
                if "FIRST_ROWS" in sql:
                    return [{"id": 1}]
                return []

            def execute_command(self, sql):
                self.sql.append(f"COMMAND:{sql}")

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {"database": "demo"}
        bridge.config = {"database": "demo"}
        bridge.transport = OrderRecordingTransport()

        lsn, rows = bridge.snapshot_chunk("demo.app.orders", ["id"], ["id"], None, 10)

        # The stamp is the conservative boundary: the start of the current log page
        # (used=3 -> two full pages), never the page end that may exceed the true
        # write position (see boundary_lsn).
        self.assertEqual(lsn, (2 << 32) + (2 << 12))
        self.assertEqual(rows, [{"id": 1}])
        lsn_index = next(i for i, sql in enumerate(bridge.transport.sql) if "syslogs" in sql)
        select_index = next(i for i, sql in enumerate(bridge.transport.sql) if "FIRST_ROWS" in sql)
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
                if "FIRST_ROWS" in sql:
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

    def test_column_defaults_reports_only_non_null_defaults(self):
        # DEFAULT NULL (type N) leaves existing rows NULL, exactly like no default.
        class CatalogTransport:
            def __init__(self):
                self.calls = []

            def execute(self, sql, parameters=(), max_result_bytes=None):
                del max_result_bytes
                self.calls.append((sql, parameters))
                return [
                    {"colname": "added", "type": "L"},
                    {"colname": "nulled", "type": "N"},
                    {"colname": "unasked", "type": "C"},
                ]

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {"database": "demo"}
        bridge.config = {"database": "demo"}
        bridge.transport = CatalogTransport()

        found = bridge.column_defaults("demo.app.orders", ["added", "nulled"])

        self.assertEqual(found, ["added"])
        sql, parameters = bridge.transport.calls[-1]
        self.assertIn("JOIN sysdefaults d ON d.tabid = c.tabid AND d.colno = c.colno", sql)
        self.assertEqual(parameters, ("app", "orders"))

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
                if "FIRST_ROWS" in sql:
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
                if "FIRST_ROWS" in sql:
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
                if "FIRST_ROWS" in sql:
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
            [field.name for field in schema.fields][-5:],
            [
                CURSOR,
                "_informix_commit_lsn",
                "_informix_tx_id",
                "_informix_op",
                "_informix_commit_time",
            ],
        )
        self.assertEqual(
            connector.read_table_metadata("app.orders", {}),
            {
                "primary_keys": ["id"],
                "cursor_field": CURSOR,
                "ingestion_type": "cdc_with_deletes",
            },
        )

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

    def test_a_sys_table_is_refused_with_the_rule_named(self):
        # Discovery hides names and owners starting with "sys"; a user who configures
        # one by name should learn why rather than see a bare "unknown table".
        connector = self.connector()
        self.assertNotIn("sysadmin.hidden", connector.list_tables())
        with self.assertRaisesRegex(ValueError, "starts with 'sys'"):
            connector.get_table_schema("sysadmin.hidden", {})

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

    def test_staged_page_serves_typed_rows_in_order(self):
        # The serve path decodes a staged page's typed values row-by-row in place
        # (so the encoded and decoded copies never both exist in full -- the memory
        # fix for wide pages). That must preserve row order and decode typed values
        # exactly, identical to a whole-list decode.
        connector = self.connector()
        table = connector._table("app.orders", {})
        scope = connector._pipeline_scope()
        schema_id = "schema-typed-serve"
        snapshot_lsn = 100
        rows = [
            {"id": 1, "value": Decimal("1.50"), "when": date(2026, 1, 1)},
            {"id": 2, "value": Decimal("2.50"), "when": date(2026, 1, 2)},
            {"id": 3, "value": Decimal("3.50"), "when": date(2026, 1, 3)},
        ]
        connector._publish_snapshot_stage_page(table, scope, schema_id, snapshot_lsn, 0, rows, None)

        manifest = {"page_count": 1, "snapshot_lsn": str(snapshot_lsn)}
        served, end = connector._staged_snapshot_result(table, scope, schema_id, manifest, 0)

        self.assertEqual(list(served), rows)  # order + Decimal/date preserved
        self.assertEqual(end["phase"], "stream")

    def test_staged_page_rejects_a_row_count_mismatch(self):
        # The pre-decode row_count guard still fires after the in-place refactor.
        connector = self.connector()
        table = connector._table("app.orders", {})
        scope = connector._pipeline_scope()
        schema_id = "schema-typed-serve"
        snapshot_lsn = 100
        rows = [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}]
        connector._publish_snapshot_stage_page(table, scope, schema_id, snapshot_lsn, 0, rows, None)

        # A manifest is not consulted for row_count, but the page record's own
        # row_count/len(rows) agreement is validated inside the page reader; corrupt
        # the staged file to prove the serve path still refuses a bad page.
        page_path = os.path.join(
            connector._snapshot_stage_namespace(table, scope, schema_id),
            "runs",
            str(snapshot_lsn),
            "page-00000000",
            "data.json.gz",
        )
        full = os.path.join(connector._snapshot_staging_location, page_path)
        with gzip.open(full, "rb") as handle:
            record = json.loads(handle.read())
        record["rows"] = record["rows"][:1]  # drop a row without fixing row_count/sha256
        with gzip.open(full, "wb") as handle:
            handle.write(json.dumps(record).encode("utf-8"))

        manifest = {"page_count": 1, "snapshot_lsn": str(snapshot_lsn)}
        with self.assertRaisesRegex(informix_module.InformixError, "Invalid staged snapshot page"):
            connector._staged_snapshot_result(table, scope, schema_id, manifest, 0)

    def test_staged_page_streams_values_that_look_like_json_structure(self):
        # The streaming serve locates the rows array and the top-level sha256 field
        # *structurally* (string-aware), so column values full of JSON structural
        # characters -- brackets, quotes, commas, escapes, even a fake
        # "sha256":"..." and "rows":[ -- must neither confuse the scan nor the
        # integrity check, and must round-trip byte-for-byte.
        connector = self.connector()
        table = connector._table("app.orders", {})
        scope = connector._pipeline_scope()
        schema_id = "schema-adversarial"
        snapshot_lsn = 100
        rows = [
            {"id": 1, "value": "brackets ] [ } { comma , colon :"},
            {"id": 2, "value": 'quote " backslash \\ escaped \\" end'},
            {"id": 3, "value": '"sha256":"deadbeef","rows":[nope],"version":9'},
            {"id": 4, "value": "unicode ☃ and tab \t and newline \n"},
        ]
        connector._publish_snapshot_stage_page(table, scope, schema_id, snapshot_lsn, 0, rows, None)

        manifest = {"page_count": 1, "snapshot_lsn": str(snapshot_lsn)}
        served, _ = connector._staged_snapshot_result(table, scope, schema_id, manifest, 0)
        self.assertEqual(list(served), rows)

    def test_staged_page_streaming_detects_a_tampered_row_value(self):
        # Flipping a byte inside a row value changes the canonical body, so the
        # streamed integrity check must reject it even though row_count is intact.
        connector = self.connector()
        table = connector._table("app.orders", {})
        scope = connector._pipeline_scope()
        schema_id = "schema-tamper"
        snapshot_lsn = 100
        rows = [{"id": 1, "value": "alpha"}, {"id": 2, "value": "bravo"}]
        connector._publish_snapshot_stage_page(table, scope, schema_id, snapshot_lsn, 0, rows, None)

        page_path = os.path.join(
            connector._snapshot_stage_namespace(table, scope, schema_id),
            "runs",
            str(snapshot_lsn),
            "page-00000000",
            "data.json.gz",
        )
        full = os.path.join(connector._snapshot_staging_location, page_path)
        with gzip.open(full, "rb") as handle:
            text = handle.read().decode("ascii")
        tampered = text.replace("alpha", "alpXa")  # same length, sha256 not updated
        self.assertNotEqual(tampered, text)
        with gzip.open(full, "wb") as handle:
            handle.write(tampered.encode("ascii"))

        # Integrity is verified eagerly, before any row is yielded, so the serve
        # call itself refuses the tampered page.
        manifest = {"page_count": 1, "snapshot_lsn": str(snapshot_lsn)}
        with self.assertRaisesRegex(informix_module.InformixError, "Invalid staged snapshot page"):
            connector._staged_snapshot_result(table, scope, schema_id, manifest, 0)

    def test_staged_page_serves_rows_whose_columns_shadow_trailing_header_keys(self):
        # The serve path locates trailing header fields (upper_pk, sha256, version,
        # ...) from the tail with rfind, so a *row column* named exactly like one of
        # them must not be mistaken for the header field: the header field is always
        # the last occurrence (row columns live inside the earlier rows array). Stage
        # rows that shadow every trailing key and confirm they round-trip and the
        # resume cursor is the real header upper_pk (the last row's id), not a column.
        connector = self.connector()
        table = connector._table("app.orders", {})
        scope = connector._pipeline_scope()
        schema_id = "schema-shadow"
        snapshot_lsn = 100
        rows = [
            {
                "id": 1,
                "sha256": "not-a-digest",
                "upper_pk": [999],
                "version": 42,
                "table": "decoy",
                "snapshot_lsn": "0",
            },
            {
                "id": 2,
                "sha256": "still-not",
                "upper_pk": [888],
                "version": 7,
                "table": "decoy2",
                "snapshot_lsn": "0",
            },
        ]
        connector._publish_snapshot_stage_page(table, scope, schema_id, snapshot_lsn, 0, rows, None)

        # Two pages so the served page is non-final and carries a resume cursor.
        manifest = {"page_count": 2, "snapshot_lsn": str(snapshot_lsn)}
        served, end = connector._staged_snapshot_result(table, scope, schema_id, manifest, 0)
        self.assertEqual(list(served), rows)  # shadowing columns round-trip untouched
        # upper_pk is the real header field (last row's primary key), not row content.
        self.assertEqual(end["snapshot"]["last_pk"], [2])
        self.assertEqual(end["snapshot"]["page_index"], 1)

    def _flush_snapshot_cleanup(self, connector):
        # Force the background cleanup pool to finish, then drop it so the next
        # cleanup call lazily starts a fresh one -- makes async deletion deterministic
        # to assert in tests.
        executor = connector._snapshot_cleanup_executor_instance
        if executor is not None:
            executor.shutdown(wait=True)
            connector._snapshot_cleanup_executor_instance = None

    def test_consumed_snapshot_pages_are_cleaned_incrementally_off_the_serve_path(self):
        # Cleanup deletes only the pages advanced past since the last call (tracked by
        # a per-stream high-water mark), on a background pool -- never the old
        # O(page_index) rescan of every earlier page each microbatch. Verify the
        # observable outcome: the right pages are gone, the current page is kept, and
        # a second advance touches only the newly-consumed pages.
        connector = self.connector()
        table = connector._table("app.orders", {})
        scope = connector._pipeline_scope()
        schema_id = "cleanup"
        snapshot_lsn = 100
        for i in range(6):
            connector._publish_snapshot_stage_page(
                table, scope, schema_id, snapshot_lsn, i, [{"id": i}], None
            )
        run = os.path.join(
            connector._snapshot_stage_namespace(table, scope, schema_id), "runs", str(snapshot_lsn)
        )

        def page_dir(i):
            return os.path.join(run, f"page-{i:08d}")

        # Advancing to page 3 deletes 0,1,2 (async); 3,4,5 remain.
        connector._cleanup_consumed_snapshot_stage_pages(table, scope, schema_id, snapshot_lsn, 3)
        self._flush_snapshot_cleanup(connector)
        self.assertFalse(any(os.path.isdir(page_dir(i)) for i in (0, 1, 2)))
        self.assertTrue(all(os.path.isdir(page_dir(i)) for i in (3, 4, 5)))
        self.assertEqual(connector._snapshot_cleanup_watermark[(scope, schema_id, snapshot_lsn)], 3)

        # Advancing to page 5 deletes only 3,4; page 5 (the current page) is kept.
        connector._cleanup_consumed_snapshot_stage_pages(table, scope, schema_id, snapshot_lsn, 5)
        self._flush_snapshot_cleanup(connector)
        self.assertFalse(any(os.path.isdir(page_dir(i)) for i in range(5)))
        self.assertTrue(os.path.isdir(page_dir(5)))

    def test_cleanup_schedules_each_consumed_page_exactly_once(self):
        # The high-water mark makes cleanup incremental: re-serving the same page is a
        # no-op, and advancing schedules only the delta -- each page exactly once, the
        # current page never. This is what kills the old O(page_index)-per-microbatch
        # (O(N^2)-per-snapshot) FUSE rescan.
        connector = self.connector()
        table = connector._table("app.orders", {})
        scope = connector._pipeline_scope()
        schema_id = "cleanup2"
        snapshot_lsn = 200
        run = os.path.join(
            connector._snapshot_stage_namespace(table, scope, schema_id), "runs", str(snapshot_lsn)
        )
        recorded, lock = [], threading.Lock()

        def record(path):
            with lock:
                recorded.append(path)

        with mock.patch.object(
            informix_module.InformixLakeflowConnect,
            "_delete_snapshot_page",
            staticmethod(record),
        ):
            connector._cleanup_consumed_snapshot_stage_pages(
                table, scope, schema_id, snapshot_lsn, 3
            )
            self._flush_snapshot_cleanup(connector)
            connector._cleanup_consumed_snapshot_stage_pages(
                table, scope, schema_id, snapshot_lsn, 3
            )
            connector._cleanup_consumed_snapshot_stage_pages(
                table, scope, schema_id, snapshot_lsn, 5
            )
            self._flush_snapshot_cleanup(connector)

        expected = [os.path.join(run, f"page-{i:08d}") for i in range(5)]
        self.assertEqual(sorted(recorded), sorted(expected))
        self.assertEqual(len(recorded), 5)  # pages 0..4 once each; page 5 (current) never

    # ------------------------------------------------------------------
    # Partitioned snapshot-page reader (SupportsPartitionedStream)
    # ------------------------------------------------------------------

    def _partitioned_connector(self, bridge=None, **options):
        """A connector configured for the partitioned reader, with its CDC polls going
        through process-local shards, which leave no files behind (the base connector
        reads directly)."""
        opts = {"stream.partitioned": "true", "cdc.shared.session": "process", **options}
        return self.connector(bridge, shared_cdc=True, **opts)

    def _drive_partitioned_snapshot(self, connector, table_name="app.orders", options=None):
        """Drive the partitioned reader through the snapshot, mirroring Spark's cycle.

        Each iteration: latest_offset(start) -> partitions(start,end) -> read each
        partition, then advance. Stops when the stream phase is reached. Returns the
        (served_rows, offsets) collected, so a test can assert fan-out, order, and
        the snapshot->stream transition.
        """

        options = options or {}
        served: list[dict] = []
        offsets: list[dict] = []
        start: dict = {}
        for _ in range(1000):  # safety bound
            end = connector.latest_offset(table_name, options, start)
            for part in connector.get_partitions(table_name, options, start, end):
                served.extend(connector.read_partition(table_name, part, options))
            offsets.append(end)
            start = end
            if end.get("phase") == "stream":
                break
        self._flush_snapshot_cleanup(connector)
        return served, offsets

    def test_partitioned_reader_is_gated_by_the_option(self):
        # On for a keyed (CDC-capable) table when stream.partitioned is on; off
        # suppresses it so the framework falls back to simpleStreamReader.
        on = self._partitioned_connector()
        self.assertTrue(on.is_partitioned("app.orders"))
        off = self.connector(**{"stream.partitioned": "false"})
        self.assertFalse(off.is_partitioned("app.orders"))

    def test_partitioned_pages_per_batch_must_be_positive(self):
        with self.assertRaises(ValueError):
            self._partitioned_connector(**{"stream.partitioned.snapshot.pages.per.batch": "0"})

    def test_partitioned_snapshot_fans_pages_out_and_preserves_order(self):
        # A staged snapshot of several one-row pages fans out k pages per microbatch,
        # each served by read_partition, and the union equals the full snapshot in
        # page (id) order -- with no page served twice.
        bridge = FakeBridge()
        bridge.rows = [{"id": i, "value": f"v{i}"} for i in range(1, 6)]  # 5 rows -> 5 pages
        connector = self._partitioned_connector(
            bridge,
            **{"snapshot.page.size": "1", "stream.partitioned.snapshot.pages.per.batch": "3"},
        )

        served, offsets = self._drive_partitioned_snapshot(connector)

        self.assertEqual([row["id"] for row in served], [1, 2, 3, 4, 5])
        self.assertEqual(offsets[-1]["phase"], "stream", "snapshot must transition to stream")
        # A middle microbatch fanned out more than one page (proof of parallelism).
        self.assertTrue(
            any(o.get("phase") == "snapshot" for o in offsets),
            "expected at least one non-terminal snapshot batch",
        )

    def test_partitioned_snapshot_matches_the_serial_serve(self):
        # The partitioned drive yields exactly the rows a serial read_table drive
        # yields, in the same order -- the fan-out changes only how pages are read.
        def serial_rows():
            bridge = FakeBridge()
            bridge.rows = [{"id": i, "value": f"v{i}"} for i in range(1, 6)]
            connector = self.connector(
                bridge, registration_scope="serial", **{"snapshot.page.size": "1"}
            )
            rows: list[dict] = []
            start: dict = {}
            for _ in range(1000):
                page, end = connector.read_table("app.orders", start, {})
                rows.extend(page)
                start = end
                if end.get("phase") == "stream":
                    break
            return rows

        bridge = FakeBridge()
        bridge.rows = [{"id": i, "value": f"v{i}"} for i in range(1, 6)]
        connector = self._partitioned_connector(
            bridge,
            registration_scope="partitioned",
            **{"snapshot.page.size": "1", "stream.partitioned.snapshot.pages.per.batch": "2"},
        )

        served, _ = self._drive_partitioned_snapshot(connector)
        self.assertEqual(served, serial_rows())

    def test_partitioned_snapshot_offset_last_pk_matches_the_boundary_page(self):
        # A non-terminal fan-out offset carries the boundary page's upper_pk as
        # last_pk -- the same resume cursor the serial serve records at that
        # page_index. Validates the boundary-page-header read in latest_offset.
        bridge = FakeBridge()
        bridge.rows = [{"id": i, "value": f"v{i}"} for i in range(1, 6)]
        connector = self._partitioned_connector(
            bridge,
            **{"snapshot.page.size": "1", "stream.partitioned.snapshot.pages.per.batch": "2"},
        )

        # Bootstrap page 0 (embedded), then take one fan-out step from page 1.
        first = connector.latest_offset("app.orders", {}, {})
        second = connector.latest_offset("app.orders", {}, first)

        self.assertEqual(second["phase"], "snapshot")
        end_page = second["snapshot"]["page_index"]
        table = connector._table("app.orders", {})
        scope = connector._pipeline_scope(second)
        header, _ = connector._stream_staged_page(
            table, scope, str(second["schema_id"]), int(second["snapshot_lsn"]), end_page - 1
        )
        self.assertEqual(second["snapshot"]["last_pk"], header.get("upper_pk"))

    def test_partitioned_page_read_touches_no_bridge(self):
        # read_partition serves a staged page from identity strings only -- it must
        # make no Informix catalog call, so it is safe to run on an executor.
        bridge = FakeBridge()
        bridge.rows = [{"id": i, "value": f"v{i}"} for i in range(1, 6)]
        connector = self._partitioned_connector(
            bridge,
            **{"snapshot.page.size": "1", "stream.partitioned.snapshot.pages.per.batch": "3"},
        )
        first = connector.latest_offset("app.orders", {}, {})
        second = connector.latest_offset("app.orders", {}, first)
        partitions = connector.get_partitions("app.orders", {}, first, second)
        self.assertTrue(partitions and all(p["kind"] == "page" for p in partitions))

        bridge.get_table = mock.Mock(wraps=bridge.get_table)
        served = []
        for part in partitions:
            served.extend(connector.read_partition("app.orders", part, {}))

        bridge.get_table.assert_not_called()
        # Page 0 (id 1) was served via the embedded bootstrap; this fan-out covers
        # pages 1..3, which hold ids 2, 3, 4.
        self.assertEqual([row["id"] for row in served], [2, 3, 4])

    def test_partitioned_embedded_path_matches_read_table(self):
        # A non-page-serve microbatch (stream phase) yields a single embedded
        # partition whose rows and offset are exactly read_table's output.
        connector = self._partitioned_connector()
        stream_start = {"phase": "stream", "commit_lsn": "5"}
        sentinel_rows = [{"id": 7, "value": "x"}]
        sentinel_end = {"phase": "stream", "commit_lsn": "9"}
        with mock.patch.object(
            connector, "_read_upserts", return_value=(iter(sentinel_rows), sentinel_end)
        ) as read_table:
            end = connector.latest_offset("app.orders", {}, stream_start)
            partitions = connector.get_partitions("app.orders", {}, stream_start, end)
            rows = [
                row
                for part in partitions
                for row in connector.read_partition("app.orders", part, {})
            ]

        self.assertEqual(end, sentinel_end)
        self.assertEqual(partitions, [{"kind": "embedded", "rows": sentinel_rows}])
        self.assertEqual(rows, sentinel_rows)
        read_table.assert_called_once()

    def test_partitioned_embedded_delete_flow_routes_to_read_table_deletes(self):
        # A delete-channel reader (isDeleteFlow) never page-serves; its embedded
        # partition comes from read_table_deletes, not read_table.
        connector = self._partitioned_connector(**{"isDeleteFlow": "true"})
        start = {"phase": "stream", "commit_lsn": "5"}
        sentinel = ([{"id": 1}], {"phase": "stream", "commit_lsn": "6"})
        with (
            mock.patch.object(
                connector, "_read_deletes", return_value=(iter(sentinel[0]), sentinel[1])
            ) as deletes,
            mock.patch.object(connector, "_read_upserts") as upserts,
        ):
            end = connector.latest_offset("app.orders", {}, start)

        self.assertEqual(end, sentinel[1])
        deletes.assert_called_once()
        upserts.assert_not_called()

    def test_partitioned_embedded_recomputes_on_cache_miss(self):
        # get_partitions rebuilds the embedded rows via read_table if the driver
        # bridge cache was dropped between latest_offset and partitions (a re-plan),
        # so correctness never depends on the cache surviving.
        connector = self._partitioned_connector()
        start = {"phase": "stream", "commit_lsn": "5"}
        with mock.patch.object(
            connector,
            "_read_upserts",
            side_effect=lambda *a, **k: (iter([{"id": 3}]), {"phase": "stream", "commit_lsn": "6"}),
        ) as read_table:
            connector.latest_offset("app.orders", {}, start)
            connector._partition_embedded_rows.clear()  # simulate a re-plan
            partitions = connector.get_partitions(
                "app.orders", {}, start, {"phase": "stream", "commit_lsn": "6"}
            )

        self.assertEqual(partitions, [{"kind": "embedded", "rows": [{"id": 3}]}])
        self.assertEqual(read_table.call_count, 2)  # once in latest_offset, once on miss

    def test_partitioned_cache_miss_replay_is_bounded_by_the_committed_end(self):
        # Regression: on a driver restart Spark re-plans the uncommitted batch through
        # partitions(start, end) with the cache empty. The recompute ignored ``end``,
        # so a fresh read that stopped short (a direct poll capped by cdc.max.records
        # while the shared daemon was not ready) served a prefix while Spark committed
        # the original ``end`` -- dropping every row in between. The recompute must now
        # carry the replay bound the simple reader's readBetweenOffsets uses.
        connector = self._partitioned_connector()
        start = {"phase": "stream", "commit_lsn": "5"}
        end = {"phase": "stream", "commit_lsn": "9"}
        seen = []

        def read_table(name, start_offset, opts):
            seen.append(dict(opts))
            return iter([{"id": 1}, {"id": 2}, {"id": 3}]), dict(end)

        with mock.patch.object(connector, "_read_upserts", side_effect=read_table):
            connector.latest_offset("app.orders", {}, start)
            connector._partition_embedded_rows.clear()  # fresh driver process
            partitions = connector.get_partitions("app.orders", {}, start, end)

        self.assertEqual(
            partitions, [{"kind": "embedded", "rows": [{"id": 1}, {"id": 2}, {"id": 3}]}]
        )
        self.assertNotIn(informix_module._REPLAY_STOP_LSN_OPTION, seen[0])  # normal read
        self.assertEqual(seen[1][informix_module._REPLAY_STOP_LSN_OPTION], "9")  # bounded replay
        self.assertIn(informix_module._REPLAY_CONNECTION_WAIT_BUDGET_OPTION, seen[1])

    def test_partitioned_cache_miss_replay_fails_closed_on_a_short_read(self):
        # A recompute that reaches a lower LSN than the committed end must raise rather
        # than serve a prefix: Spark would commit ``end`` and the rest would be lost. It
        # keeps reading while it makes progress, so it fails once a read stalls short.
        connector = self._partitioned_connector()
        start = {"phase": "stream", "commit_lsn": "5"}
        end = {"phase": "stream", "commit_lsn": "9"}
        short = {"phase": "stream", "commit_lsn": "6"}
        reached = iter([dict(end), dict(short), dict(short)])

        with mock.patch.object(
            connector,
            "_read_upserts",
            side_effect=lambda *a, **k: (iter([{"id": 1}]), next(reached)),
        ):
            connector.latest_offset("app.orders", {}, start)
            connector._partition_embedded_rows.clear()
            with self.assertRaisesRegex(InformixError, "did not reach its committed end LSN"):
                connector.get_partitions("app.orders", {}, start, end)

    def test_partitioned_cache_miss_replay_continues_past_an_idle_poll(self):
        # The original batch may have crossed quiet log in several reads; a recompute
        # whose first read goes idle short of the committed end reads on to reach it.
        connector = self._partitioned_connector()
        start = {"phase": "stream", "commit_lsn": "5"}
        end = {"phase": "stream", "commit_lsn": "9"}
        reads = iter(
            [
                (iter([{"id": 0}]), dict(end)),
                (iter([{"id": 1}]), {"phase": "stream", "commit_lsn": "6"}),
                (iter([{"id": 2}]), dict(end)),
            ]
        )
        starts = []

        def read(name, start_offset, options):
            starts.append(start_offset.get("commit_lsn"))
            return next(reads)

        with mock.patch.object(connector, "_read_upserts", side_effect=read):
            connector.latest_offset("app.orders", {}, start)
            connector._partition_embedded_rows.clear()
            partitions = connector.get_partitions("app.orders", {}, start, end)

        self.assertEqual(starts, ["5", "5", "6"])
        self.assertEqual(partitions, [{"kind": "embedded", "rows": [{"id": 1}, {"id": 2}]}])

    def test_partitioned_cache_miss_replay_routes_the_delete_flow(self):
        connector = self._partitioned_connector(**{"isDeleteFlow": "true"})
        start = {"phase": "stream", "commit_lsn": "5"}
        end = {"phase": "stream", "commit_lsn": "6"}
        seen = []

        def read_table_deletes(name, start_offset, opts):
            seen.append(dict(opts))
            return iter([{"id": 1}]), dict(end)

        with (
            mock.patch.object(connector, "_read_deletes", side_effect=read_table_deletes),
            mock.patch.object(connector, "_read_upserts") as upserts,
        ):
            partitions = connector.get_partitions("app.orders", {}, start, end)
        self.assertEqual(partitions, [{"kind": "embedded", "rows": [{"id": 1}]}])
        self.assertEqual(seen[0][informix_module._REPLAY_STOP_LSN_OPTION], "6")
        upserts.assert_not_called()

    def test_partitioned_batch_get_partitions_falls_back(self):
        # The 2-arg (batch) form must raise so LakeflowBatchReader falls back to a
        # single non-partitioned read_table batch.
        connector = self._partitioned_connector()
        with self.assertRaises(NotImplementedError):
            connector.get_partitions("app.orders", {})

    def test_partitioned_embedded_rows_survive_json_and_round_trip(self):
        # F1: the framework JSON-serializes every partition descriptor
        # (InputPartition(json.dumps(descriptor))). read_table rows carry native
        # Decimal / bytes, which json cannot encode, so get_partitions must emit a
        # JSON-safe descriptor and read_partition must reconstruct the exact values.
        # The other embedded tests use int/str rows and so never crossed this boundary.
        connector = self._partitioned_connector()
        start = {"phase": "stream", "commit_lsn": "5"}
        row = {"id": 1, "amount": Decimal("10.50"), "blob": b"\x00\xffdata"}
        with mock.patch.object(
            connector,
            "_read_upserts",
            return_value=(iter([dict(row)]), {"phase": "stream", "commit_lsn": "6"}),
        ):
            connector.latest_offset("app.orders", {}, start)
            partitions = connector.get_partitions(
                "app.orders", {}, start, {"phase": "stream", "commit_lsn": "6"}
            )
        # The framework serializes the descriptor with json.dumps; it must not raise.
        wire = json.dumps(partitions[0])
        restored = json.loads(wire)
        served = list(connector.read_partition("app.orders", restored, {}))
        self.assertEqual(served, [row])
        self.assertIsInstance(served[0]["amount"], Decimal)
        self.assertEqual(served[0]["blob"], b"\x00\xffdata")

    def test_partitioned_malformed_snapshot_offset_falls_back_to_embedded(self):
        # A malformed snapshot-phase offset (here: missing the required `version`, so
        # `_validated_offset` rejects it) must degrade to the embedded path rather than
        # raise out of latest_offset / get_partitions.
        connector = self._partitioned_connector()
        bad = {"phase": "snapshot", "schema_id": "s1", "snapshot": {"page_index": 0}}
        with mock.patch.object(
            connector,
            "_read_upserts",
            return_value=(iter([{"id": 1}]), {"phase": "snapshot", "commit_lsn": "1"}),
        ) as read_table:
            end = connector.latest_offset("app.orders", {}, bad)
        self.assertEqual(end, {"phase": "snapshot", "commit_lsn": "1"})
        read_table.assert_called_once()

    def test_partitioned_embedded_cache_stays_bounded(self):
        # F6: repeated latest_offset calls that Spark never follows with get_partitions
        # (speculative discovery, empty batches) must not accumulate driver cache
        # entries -- only the latest microbatch is retained.
        connector = self._partitioned_connector()
        with mock.patch.object(
            connector,
            "_read_upserts",
            side_effect=lambda name, start, opts: (
                iter([{"id": 1}]),
                {"phase": "stream", "commit_lsn": str(int(start.get("commit_lsn", "0")) + 1)},
            ),
        ):
            for lsn in ("1", "2", "3"):
                connector.latest_offset("app.orders", {}, {"phase": "stream", "commit_lsn": lsn})
        self.assertLessEqual(len(connector._partition_embedded_rows), 1)

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
        # Consumed-page deletion is scheduled on a background pool during the read;
        # wait for it before asserting the page is gone.
        self._flush_snapshot_cleanup(connector)
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

    def test_staged_snapshot_resumes_across_a_new_update_scope(self):
        # A stop/start mints a new update id, so a resumed reader's registration scope
        # differs from the one that staged the pages. It must locate the manifest by the
        # scope the checkpoint records, not its own -- otherwise it misses the staged
        # pages and (before this fix) fails "run a full refresh" mid-load.
        bridge = FakeBridge()
        staging = self.connector(
            bridge, registration_scope="update-1", **{"snapshot.page.size": "1"}
        )
        _, mid = staging.read_table("app.orders", {}, {})
        self.assertEqual(mid["phase"], "snapshot")  # page 0 served, page 1 remains
        self.assertEqual(mid["snapshot"]["page_index"], 1)
        self.assertEqual(mid["pipeline_scope"], staging._pipeline_scope())

        # New update: same durable stores (staging Volume + offline Lakebase), a fresh
        # registration scope. Resuming from the prior checkpoint must reuse the pages.
        resumed = self.connector(
            bridge, registration_scope="update-2", **{"snapshot.page.size": "1"}
        )
        self.assertNotEqual(resumed._pipeline_scope(), staging._pipeline_scope())
        rows, end = resumed.read_table("app.orders", mid, {})

        self.assertEqual([row["id"] for row in rows], [2])  # resumed at the staged page 1
        self.assertEqual(end["phase"], "stream")

    def test_full_refresh_under_a_new_scope_re_snapshots(self):
        # A full refresh clears the Spark checkpoint, so the reader arrives with no
        # offset. Even though a prior update's staged pages persist in the durable
        # stores, an empty offset must establish a fresh boundary and re-snapshot from
        # page 0 -- the update-scoped initialization record guarantees this, so the
        # resume fix above must not turn a full refresh into a stale-data reuse.
        bridge = FakeBridge()
        first_update = self.connector(
            bridge, registration_scope="update-1", **{"snapshot.page.size": "1"}
        )
        first_update.read_table("app.orders", {}, {})

        refreshed = self.connector(
            bridge, registration_scope="update-2", **{"snapshot.page.size": "1"}
        )
        rows, end = refreshed.read_table("app.orders", {}, {})

        self.assertEqual([row["id"] for row in rows], [1])  # fresh snapshot, page 0
        self.assertEqual(end["phase"], "snapshot")
        self.assertEqual(end["snapshot"]["page_index"], 1)
        self.assertEqual(end["pipeline_scope"], refreshed._pipeline_scope())

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

    def test_completed_stage_cleanup_survives_a_concurrent_page_delete(self):
        # The serve path deletes consumed pages on a background pool, so one can vanish
        # mid-walk; the walk then stops with FileNotFoundError. The cleanup must walk
        # again rather than leave the rest of the stage behind marked as cleaned.
        bridge = FakeBridge()
        connector = self.connector(
            bridge,
            registration_scope="snapshot-stage-race",
            **{"snapshot.page.size": "1"},
        )
        _, first_end = connector.read_table("app.orders", {}, {})
        _, stream_end = connector.read_table("app.orders", first_end, {})
        table = Table.parse(bridge.tables[0], "demo")
        stage = connector._snapshot_stage_namespace(
            table, connector._pipeline_scope(stream_end), str(stream_end["schema_id"])
        )
        remove = PurePythonInformixBridge._remove_connection_slot_tree
        calls = []

        def cut_short(path):
            calls.append(path)
            if len(calls) == 1:
                os.makedirs(os.path.join(path, "late-page"), exist_ok=True)
                raise FileNotFoundError(path)
            return remove(path)

        with mock.patch.object(PurePythonInformixBridge, "_remove_connection_slot_tree", cut_short):
            connector._cleanup_completed_snapshot_stage(table, stream_end)

        self.assertGreaterEqual(len(calls), 2)
        self.assertFalse(os.path.exists(stage))
        self.assertIn(stage, connector._cleaned_snapshot_stages)

        os.makedirs(stage)
        connector._cleaned_snapshot_stages.discard(stage)
        always_cut = mock.Mock(side_effect=FileNotFoundError(stage))
        with mock.patch.object(
            PurePythonInformixBridge, "_remove_connection_slot_tree", always_cut
        ):
            connector._cleanup_completed_snapshot_stage(table, stream_end)

        self.assertTrue(os.path.exists(stage))
        self.assertNotIn(stage, connector._cleaned_snapshot_stages)  # a later read retries

    def test_snapshot_staging_retention_days_is_configurable(self):
        connector = self.connector(**{"snapshot.staging.retention.days": "7"})

        self.assertEqual(connector._snapshot_staging_retention_seconds, 7 * 86400)

    def test_snapshot_staging_retention_days_must_be_positive(self):
        with self.assertRaisesRegex(ValueError, "snapshot.staging.retention.days.*>= 1"):
            self.connector(**{"snapshot.staging.retention.days": "0"})

    def test_retention_sweep_removes_stale_runs_and_keeps_fresh(self):
        # Build the staging layout by hand with one old run and one fresh run, then run
        # the background sweep worker directly and assert only the stale run is reclaimed.
        with tempfile.TemporaryDirectory() as root:
            runs = os.path.join(root, "scope", "schema", "runs")
            stale = os.path.join(runs, "1000")
            fresh = os.path.join(runs, "2000")
            for run in (stale, fresh):
                os.makedirs(run)
            open(os.path.join(stale, "page-00000000"), "wb").close()
            open(os.path.join(fresh, "page-00000000"), "wb").close()
            now = time.time()
            os.utime(os.path.join(stale, "page-00000000"), (now - 10_000, now - 10_000))
            os.utime(os.path.join(fresh, "page-00000000"), (now, now))

            InformixLakeflowConnect._sweep_stale_snapshot_stages(root, retention_seconds=100.0)

            self.assertFalse(os.path.exists(stale))
            self.assertTrue(os.path.isdir(fresh))

    def test_retention_sweep_tolerates_missing_root(self):
        # A table that never staged has no root; the sweep must be a quiet no-op.
        InformixLakeflowConnect._sweep_stale_snapshot_stages(
            os.path.join(self._shared_state.name, "never-staged"), retention_seconds=100.0
        )

    def test_maybe_sweep_submits_once_per_table(self):
        connector = self.connector()
        table = Table.parse(FakeBridge().tables[0], "demo")

        class _RecordingExecutor:
            def __init__(self):
                self.submitted = []

            def submit(self, fn, *args):
                self.submitted.append((fn, args))

        rec = _RecordingExecutor()
        with mock.patch.object(connector, "_snapshot_cleanup_executor", return_value=rec):
            connector._maybe_sweep_stale_snapshot_stages(table)
            connector._maybe_sweep_stale_snapshot_stages(table)  # one-shot: no second submit

        self.assertEqual(len(rec.submitted), 1)
        fn, args = rec.submitted[0]
        self.assertEqual(fn, connector._sweep_stale_snapshot_stages)
        root = args[0]
        self.assertTrue(root.startswith(connector._snapshot_staging_location))
        self.assertTrue(root.endswith("snapshot-page-data"))

    def test_maybe_sweep_is_a_noop_without_a_staging_location(self):
        connector = self.connector()
        connector._snapshot_staging_location = ""
        table = Table.parse(FakeBridge().tables[0], "demo")
        with mock.patch.object(connector, "_snapshot_cleanup_executor") as executor:
            connector._maybe_sweep_stale_snapshot_stages(table)
        executor.assert_not_called()

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

    def test_handoff_captures_stream_offset_and_stops(self):
        bridge = FakeBridge()
        checkpoint = _stream_offset(125)

        rows, returned = self.connector(bridge).read_table(
            "app.orders", checkpoint, {"table.migration": "true"}
        )

        # No rows, same offset back -> the flow freezes at exactly this position.
        self.assertEqual(list(rows), [])
        self.assertEqual(returned, checkpoint)
        self.assertEqual(bridge.snapshot_calls, [])
        # The offset is parked in Lakebase for a later flow to resume from.
        self.assertEqual(len(self._lakebase.database.handoff), 1)
        (_, _, channel), token = next(iter(self._lakebase.database.handoff.items()))
        self.assertEqual(channel, 0)
        self.assertEqual(json.loads(token["offset_json"])["commit_lsn"], "125")

    def test_handoff_fails_closed_without_a_hand_off_able_position(self):
        bridge = FakeBridge()

        # No checkpoint and no parked token means the flow never reached a hand-off-able
        # position (streaming, an in-flight incremental copy, or a mid-serve snapshot).
        # Handoff never snapshots, so it fails closed rather than silently starting one.
        with self.assertRaisesRegex(InformixError, "no checkpoint to record and no parked token"):
            self.connector(bridge).read_table(
                "app.orders", {}, {"table.migration": "true", "snapshot.page.size": "1"}
            )
        self.assertEqual(self._lakebase.database.handoff, {})

    def test_handoff_captures_an_in_flight_incremental_copy(self):
        # phase=="stream" with the PK-chunked copy still in flight (an "incremental"
        # block present). This is a resumable position, so capture parks it and freezes
        # -- the cutover continues the copy from its cursor.
        bridge = FakeBridge()
        bridge.rows = [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}, {"id": 3, "value": "c"}]
        _, mid = self.connector(bridge).read_table(
            "app.orders", {}, {"snapshot.mode": "incremental", "snapshot.page.size": "1"}
        )
        self.assertIn("incremental", mid)
        self.assertFalse(mid["incremental"]["done"])

        rows, offset = self.connector(bridge).read_table(
            "app.orders", mid, {"table.migration": "true", "snapshot.page.size": "1"}
        )

        # Parked and frozen: no rows, same offset back, one token holding the mid-copy state.
        self.assertEqual(list(rows), [])
        self.assertEqual(offset, mid)
        self.assertEqual(len(self._lakebase.database.handoff), 1)
        (_, _, channel), token = next(iter(self._lakebase.database.handoff.items()))
        self.assertEqual(channel, 0)
        self.assertIn("incremental", json.loads(token["offset_json"]))

    def test_handoff_cutover_records_and_continues_the_copy_after_restore(self):
        # End to end: capture a table mid incremental copy, cut over to append (handoff
        # records the checkpoint and stops -- no rows, no re-snapshot), then Restore
        # (remove handoff) and confirm the copy *continues* from its cursor, losing no rows.
        bridge = FakeBridge()
        bridge.rows = [{"id": 1, "value": "a"}, {"id": 2, "value": "b"}, {"id": 3, "value": "c"}]
        opts = {"snapshot.mode": "incremental", "snapshot.page.size": "1"}
        first, mid = self.connector(bridge).read_table("app.orders", {}, opts)
        self.assertFalse(mid["incremental"]["done"])
        copied = {row["id"] for row in first}

        # Capture parks the mid-copy offset.
        self.connector(bridge).read_table("app.orders", mid, {"table.migration": "true"})

        # Cutover (handoff): seed from the token, then record-and-stop -- neither read emits
        # rows and no re-snapshot is taken.
        append_handoff = {"table.migration": "true", "append.only.ingestion": "true"}
        connector = self.connector(bridge)
        rows1, seeded = connector.read_table("app.orders", {}, append_handoff)
        self.assertEqual(list(rows1), [])  # seed emits no rows
        self.assertIn("incremental", seeded)  # seeded the in-flight copy, not a stream offset
        rows2, stopped = connector.read_table("app.orders", seeded, append_handoff)
        self.assertEqual(list(rows2), [])  # committed seed: record-and-stop, still no rows
        self.assertIn("incremental", stopped)  # copy state preserved for Restore to continue
        self.assertEqual(self._lakebase.database.handoff, {})  # token consumed

        # Restore: remove handoff and continue the copy from the recorded checkpoint.
        append = {"append.only.ingestion": "true"}
        offset = stopped
        for _ in range(6):  # drain remaining chunks to completion
            rows, offset = connector.read_table("app.orders", offset, append)
            copied |= {row["id"] for row in rows if row.get("_informix_op") == "r"}
            if "incremental" not in offset:
                break

        self.assertNotIn("incremental", offset)  # copy finished -> pure stream
        self.assertEqual(copied, {1, 2, 3})  # every existing row was emitted, none lost

    def test_handoff_relocates_a_blocking_snapshot_by_reference(self):
        # A blocking `initial` snapshot parked mid-serve carries phase=="snapshot" (unlike
        # an incremental copy, whose in-flight state is phase=="stream"). Capture parks
        # that offset as the token and stops -- no rows, no ride-out. The cutover under a
        # *new* scope records the capturing scope on stage_scope so the new flow reads the
        # staged pages in place, and after Restore finishes the snapshot from the parked
        # page_index before streaming under its own scope. The pages are referenced, never
        # copied.
        bridge = FakeBridge()

        # Old flow (scope A) serves page 0 of a 2-page blocking snapshot, parking at page 1.
        old = self.connector(
            bridge, registration_scope="old-location", **{"snapshot.page.size": "1"}
        )
        _, mid = old.read_table("app.orders", {}, {})
        self.assertEqual(mid["phase"], "snapshot")
        stage_scope = mid["pipeline_scope"]

        # Capture: park the snapshot-phase offset and stop -- no rows, same offset back.
        rows, parked = old.read_table("app.orders", mid, {"table.migration": "true"})
        self.assertEqual(list(rows), [])
        self.assertEqual(parked, mid)
        self.assertEqual(len(self._lakebase.database.handoff), 1)
        (_, _, channel), token = next(iter(self._lakebase.database.handoff.items()))
        self.assertEqual(channel, 0)
        self.assertEqual(json.loads(token["offset_json"])["phase"], "snapshot")

        # Cutover on a NEW scope: the seed records stage_scope=A / pipeline_scope=B, stops.
        new = self.connector(
            bridge, registration_scope="new-location", **{"snapshot.page.size": "1"}
        )
        rows1, seed = new.read_table("app.orders", {}, {"table.migration": "true"})
        self.assertEqual(list(rows1), [])
        self.assertEqual(seed["phase"], "snapshot")
        self.assertEqual(seed["stage_scope"], stage_scope)  # references the old pages
        self.assertNotEqual(seed["pipeline_scope"], stage_scope)  # owns a fresh scope

        # Seed committed: drop the token, still no rows, marker + stage_scope kept.
        rows2, stopped = new.read_table("app.orders", seed, {"table.migration": "true"})
        self.assertEqual(list(rows2), [])
        self.assertEqual(stopped["stage_scope"], stage_scope)
        self.assertEqual(self._lakebase.database.handoff, {})  # token consumed

        # Restore: finish the snapshot from the parked page against the old scope's pages,
        # then transition to stream under the NEW scope with stage_scope dropped.
        rows3, done = new.read_table("app.orders", stopped, {})
        self.assertEqual([row["id"] for row in rows3], [2])  # remaining page served
        self.assertEqual(done["phase"], "stream")
        self.assertNotIn("stage_scope", done)  # dropped at the transition
        self.assertEqual(done["pipeline_scope"], seed["pipeline_scope"])

    def test_handoff_reads_skip_obsolete_scope_cleanup(self):
        # In production, obsolete-scope cleanup runs real SQL. On a same-pipeline
        # relocation the capturing flow's scope (`{pipelineId}_@_{oldUpdate}`) looks like an
        # obsolete previous update, so an un-guarded cleanup on the cutover-*seed* read
        # (empty checkpoint -> no stage_scope yet to trigger the in-method guard) would
        # delete the snapshot manifest the Restore serve still needs. Guard it directly:
        # cleanup must not run at all while table.migration is set.
        #
        # (An end-to-end deletion test is not possible offline: the fake Postgres routes
        # `DELETE FROM state_records` to a no-op catch-all, so it never models the delete.)
        bridge = FakeBridge()
        capture_src = self.connector(bridge, **{"snapshot.page.size": "1"})
        _, mid = capture_src.read_table("app.orders", {}, {})
        capture_src.read_table("app.orders", mid, {"table.migration": "true"})

        connector = self.connector(bridge, **{"snapshot.page.size": "1"})
        with mock.patch.object(connector, "_cleanup_previous_update_scopes") as cleanup:
            # Cutover seed (empty checkpoint) then the committed seed -- both under handoff.
            _, seed = connector.read_table("app.orders", {}, {"table.migration": "true"})
            connector.read_table("app.orders", seed, {"table.migration": "true"})
        cleanup.assert_not_called()

    def test_handoff_capture_defers_to_embedded_in_partitioned_mode(self):
        # Partitioned mode intercepts snapshot-phase offsets in _snapshot_page_batch before
        # the embedded read. Under handoff that must defer to _read_handoff (park +
        # record-and-stop) instead of serving pages -- otherwise capture never parks its
        # token and the committed seed never drops it.
        bridge = FakeBridge()
        seed_connector = self._partitioned_connector(bridge, **{"snapshot.page.size": "1"})
        mid = seed_connector.latest_offset("app.orders", {}, {})
        self.assertEqual(mid["phase"], "snapshot")

        capture = self._partitioned_connector(bridge, **{"snapshot.page.size": "1"})
        parked = capture.latest_offset("app.orders", {"table.migration": "true"}, mid)

        # Record-and-stop: the page index did not advance (no serve), and the snapshot-phase
        # offset was parked as the token.
        self.assertEqual(parked["phase"], "snapshot")
        self.assertEqual(parked["snapshot"]["page_index"], mid["snapshot"]["page_index"])
        self.assertEqual(len(self._lakebase.database.handoff), 1)
        (_, _, _channel), token = next(iter(self._lakebase.database.handoff.items()))
        self.assertEqual(json.loads(token["offset_json"])["phase"], "snapshot")

    def test_handoff_relocation_serves_staged_pages_in_partitioned_mode(self):
        # After Restore, the partitioned reader must finish a relocated blocking snapshot
        # by reading the capturing scope's staged pages via stage_scope -- exercising the
        # partitioned path's own stage_scope threading (_snapshot_page_batch page range,
        # partition descriptors, and the flow-scope/stage-scope split in the end offset).
        bridge = FakeBridge()

        # Capture under scope A: serve page 0 of a 2-page blocking snapshot, park at page 1.
        old = self.connector(bridge, **{"snapshot.page.size": "1"})
        old.set_registration_scope("a" * 32)
        _, mid = old.read_table("app.orders", {}, {})
        self.assertEqual(mid["phase"], "snapshot")
        old.read_table("app.orders", mid, {"table.migration": "true"})

        # New partitioned flow under scope B: cutover seed + committed seed (both defer to
        # the embedded record-and-stop; Fix routes handoff snapshot offsets off the serve).
        new = self._partitioned_connector(bridge, **{"snapshot.page.size": "1"})
        new.set_registration_scope("b" * 32)
        seed = new.latest_offset("app.orders", {"table.migration": "true"}, {})
        self.assertEqual(seed["stage_scope"], "a" * 32)
        self.assertEqual(seed["pipeline_scope"], "b" * 32)
        stopped = new.latest_offset("app.orders", {"table.migration": "true"}, seed)
        self.assertEqual(stopped["snapshot"]["page_index"], 1)  # no serve under handoff

        # Restore: drive the partitioned serve from the seed with handoff removed.
        served, final = [], stopped
        for _ in range(10):
            end = new.latest_offset("app.orders", {}, final)
            for part in new.get_partitions("app.orders", {}, final, end):
                served.extend(new.read_partition("app.orders", part, {}))
            final = end
            if end.get("phase") == "stream":
                break

        self.assertEqual([row["id"] for row in served], [2])  # remaining page, from scope A
        self.assertEqual(final["phase"], "stream")
        self.assertEqual(final["pipeline_scope"], "b" * 32)  # streams under its own scope
        self.assertNotIn("stage_scope", final)  # dropped at the transition
        self._flush_snapshot_cleanup(new)  # drain background page cleanup this serve queued

    def test_handoff_capture_re_capture_is_idempotent(self):
        bridge = FakeBridge()
        checkpoint = _stream_offset(125)
        self.connector(bridge).read_table("app.orders", checkpoint, {"table.migration": "true"})
        self.connector(bridge).read_table("app.orders", checkpoint, {"table.migration": "true"})

        self.assertEqual(len(self._lakebase.database.handoff), 1)

    def test_restore_drops_a_handoff_token_the_cutover_left_behind(self):
        # Belt-and-suspenders: if a cutover committed the seed without a later same-mode
        # read to drop the token, the first post-Restore read (still carrying
        # handoff_seeded) must drop it -- otherwise it lingers (no TTL) and a future cutover
        # could adopt it. One-shot: the marker is not carried onto the output offset.
        bridge = FakeBridge()
        # Capture parks the upsert token.
        self.connector(bridge).read_table(
            "app.orders", _stream_offset(125), {"table.migration": "true"}
        )
        self.assertEqual(len(self._lakebase.database.handoff), 1)

        # Orphan scenario: a committed seed offset that still carries the marker, read
        # AFTER Restore (table.migration cleared).
        seed = dict(_stream_offset(125))
        seed["handoff_seeded"] = True
        _, offset = self.connector(bridge).read_table("app.orders", seed, {})

        self.assertEqual(self._lakebase.database.handoff, {})  # lingering token dropped
        self.assertNotIn("handoff_seeded", offset)  # one-shot: marker not carried forward

    def test_restore_drops_a_delete_channel_handoff_token_left_behind(self):
        # Same as above for the independently checkpointed delete channel (channel 1).
        bridge = FakeBridge()
        self.connector(bridge).read_table_deletes(
            "app.orders", _stream_offset(125), {"table.migration": "true"}
        )
        self.assertEqual(len(self._lakebase.database.handoff), 1)
        (_, _, channel), _token = next(iter(self._lakebase.database.handoff.items()))
        self.assertEqual(channel, 1)

        seed = dict(_stream_offset(125))
        seed["handoff_seeded"] = True
        self.connector(bridge).read_table_deletes("app.orders", seed, {})

        self.assertEqual(self._lakebase.database.handoff, {})  # delete-channel token dropped

    def test_restore_upsert_read_drops_an_orphaned_delete_token(self):
        # A keyed table's delete-channel token (channel 1) is normally dropped by the delete
        # reader -- but only when the delete offset carries handoff_seeded, which never
        # happens if the cutover delete flow arrived with a non-empty checkpoint and
        # re-parked instead of seeding. The token then lingers with no TTL. The first
        # post-Restore UPSERT read (guaranteed to fire, carrying the marker) must drop BOTH
        # channels so a keyed table's delete token cannot be orphaned. Regression: this read
        # used to add the delete channel only for append-only tables, stranding channel-1
        # tokens for keyed relocations whose delete flow never seeded from empty.
        bridge = FakeBridge()
        # Capture parks both the upsert (channel 0) and delete (channel 1) tokens.
        self.connector(bridge).read_table(
            "app.orders", _stream_offset(125), {"table.migration": "true"}
        )
        self.connector(bridge).read_table_deletes(
            "app.orders", _stream_offset(125), {"table.migration": "true"}
        )
        self.assertEqual(sorted(ch for (_, _, ch) in self._lakebase.database.handoff), [0, 1])

        # First post-Restore upsert read: the delete offset never got the marker, so the
        # delete reader would never drop channel 1 -- the upsert read must clear both.
        seed = dict(_stream_offset(125))
        seed["handoff_seeded"] = True
        _, offset = self.connector(bridge).read_table("app.orders", seed, {})

        self.assertEqual(self._lakebase.database.handoff, {})  # both channels dropped
        self.assertNotIn("handoff_seeded", offset)  # one-shot

    def test_handoff_requires_a_cdc_capable_table(self):
        bridge = FakeBridge()
        bridge.tables = [_table(cdc=False)]

        with self.assertRaisesRegex(ValueError, "CDC-capable"):
            self.connector(bridge).read_table(
                "app.orders", _stream_offset(), {"table.migration": "true"}
            )

    def test_snapshot_mode_handoff_is_rejected_in_favor_of_table_migration(self):
        # snapshot.mode=handoff is gone; it points the operator at table.migration.
        bridge = FakeBridge()
        with self.assertRaisesRegex(ValueError, "table.migration"):
            self.connector(bridge).read_table(
                "app.orders", _stream_offset(125), {"snapshot.mode": "handoff"}
            )

    def test_migration_capture_on_an_append_flow_parks_the_token(self):
        # Relocating an already-append table: the capture side is itself an append flow, so
        # it must PARK its stream offset -- not drop it, as the SCD1->append cutover-commit
        # does. Capture vs committed-seed is told apart by the handoff_seeded marker (absent
        # on capture). Regression: the append+stream branch used to drop unconditionally, so
        # capturing an append table parked nothing and the cutover then found no token.
        bridge = FakeBridge()
        append_migrate = {"table.migration": "true", "append.only.ingestion": "true"}

        rows, offset = self.connector(bridge).read_table(
            "app.orders", _stream_offset(125), append_migrate
        )
        # Record-and-stop: no rows, same offset, token parked on the upsert channel.
        self.assertEqual(list(rows), [])
        self.assertEqual(offset["commit_lsn"], "125")
        self.assertEqual(len(self._lakebase.database.handoff), 1)
        (_, _, channel), token = next(iter(self._lakebase.database.handoff.items()))
        self.assertEqual(channel, 0)
        self.assertEqual(json.loads(token["offset_json"])["commit_lsn"], "125")

        # Cutover: a fresh append flow (new destination, empty checkpoint) seeds from it.
        rows2, seed = self.connector(bridge).read_table("app.orders", {}, append_migrate)
        self.assertEqual(list(rows2), [])
        self.assertEqual(seed["phase"], "stream")
        self.assertEqual(seed["commit_lsn"], "125")
        self.assertTrue(seed.get("handoff_seeded"))

    def test_handoff_cutover_seeds_from_parked_offset_without_snapshot(self):
        bridge = FakeBridge()
        # Capture on the still-keyed flow.
        self.connector(bridge).read_table(
            "app.orders", _stream_offset(125), {"table.migration": "true"}
        )

        # Cutover: a fresh append flow (empty checkpoint) seeds from the token.
        rows, seeded = self.connector(bridge).read_table(
            "app.orders",
            {},
            {"table.migration": "true", "append.only.ingestion": "true"},
        )

        self.assertEqual(list(rows), [])
        self.assertEqual(seeded["phase"], "stream")
        self.assertEqual(seeded["commit_lsn"], "125")
        self.assertEqual(bridge.snapshot_calls, [])
        # Not deleted until the seed is committed (survives a crash before commit).
        self.assertEqual(len(self._lakebase.database.handoff), 1)

    def test_handoff_cutover_drops_token_once_the_seed_is_committed(self):
        bridge = FakeBridge()
        self.connector(bridge).read_table(
            "app.orders", _stream_offset(125), {"table.migration": "true"}
        )
        options = {"table.migration": "true", "append.only.ingestion": "true"}
        _, seeded = self.connector(bridge).read_table("app.orders", {}, options)

        # The next read arrives with the committed (non-empty) seed -> token is dropped.
        self.connector(bridge).read_table("app.orders", seeded, options)

        self.assertEqual(self._lakebase.database.handoff, {})

    def test_handoff_cutover_without_a_parked_offset_fails_closed(self):
        bridge = FakeBridge()

        with self.assertRaisesRegex(InformixError, "no parked checkpoint"):
            self.connector(bridge).read_table(
                "app.orders",
                {},
                {"table.migration": "true", "append.only.ingestion": "true"},
            )

    def test_handoff_keyed_relocation_cutover_seeds_without_snapshot(self):
        # Finding 1: a keyed table that keeps its scd_type (a destination relocation)
        # must resume from the parked position on cutover, NOT re-snapshot. The cutover
        # flow stays keyed, so it lands in _read_handoff (not _read_append_only).
        bridge = FakeBridge()
        # Capture on the source flow.
        self.connector(bridge, registration_scope="old-flow").read_table(
            "app.orders", _stream_offset(125), {"table.migration": "true"}
        )
        self.assertEqual(len(self._lakebase.database.handoff), 1)

        # Cutover: a fresh keyed flow (new scope, empty checkpoint) seeds from the token.
        connector = self.connector(bridge, registration_scope="new-flow")
        rows, seeded = connector.read_table("app.orders", {}, {"table.migration": "true"})

        self.assertEqual(list(rows), [])
        self.assertEqual(seeded["commit_lsn"], "125")
        self.assertEqual(seeded["phase"], "stream")
        self.assertTrue(seeded["handoff_seeded"])  # one-shot cutover marker
        self.assertEqual(bridge.snapshot_calls, [])  # crucially, NO re-snapshot
        self.assertEqual(len(self._lakebase.database.handoff), 1)  # kept until committed

    def test_handoff_keyed_relocation_cutover_records_and_stops(self):
        bridge = FakeBridge()
        self.connector(bridge, registration_scope="old-flow").read_table(
            "app.orders", _stream_offset(125), {"table.migration": "true"}
        )
        connector = self.connector(bridge, registration_scope="new-flow")
        rows1, seeded = connector.read_table("app.orders", {}, {"table.migration": "true"})
        self.assertEqual(list(rows1), [])  # seed emits no rows

        # The committed seed carries the marker -> record and stop: no rows, same offset
        # back, token consumed. Handoff never streams; the marker is kept so the offset is
        # not mistaken for a fresh capture and re-parked.
        rows2, resumed = connector.read_table("app.orders", seeded, {"table.migration": "true"})
        self.assertEqual(list(rows2), [])
        self.assertEqual(resumed["commit_lsn"], "125")
        self.assertTrue(resumed["handoff_seeded"])
        self.assertEqual(self._lakebase.database.handoff, {})  # token consumed

        # A later steady-state read stays stopped -- no rows, no orphan token re-parked --
        # even with table.migration still set (operator has not run Restore yet).
        rows3, steady = connector.read_table("app.orders", resumed, {"table.migration": "true"})
        self.assertEqual(list(rows3), [])
        self.assertEqual(steady["commit_lsn"], "125")
        self.assertEqual(self._lakebase.database.handoff, {})  # no orphan token re-parked

        # Restore: clear table.migration and the flow streams normally from 125.
        _, restored = connector.read_table("app.orders", steady, {})
        self.assertEqual(restored["phase"], "stream")

    def test_handoff_cutover_fails_closed_on_schema_change(self):
        # Finding 3: the parked schema_fp must be validated at cutover. If the source
        # schema drifted since capture, resuming would misread the stream -> fail closed.
        bridge = FakeBridge()
        self.connector(bridge).read_table(
            "app.orders", _stream_offset(125), {"table.migration": "true"}
        )
        bridge.tables[0]["columns"].append(
            {"name": "added", "type_name": "VARCHAR", "length": 10, "cdc_supported": True}
        )

        with self.assertRaisesRegex(InformixError, "schema changed"):
            self.connector(bridge).read_table(
                "app.orders",
                {},
                {"table.migration": "true", "append.only.ingestion": "true"},
            )

    def test_handoff_delete_channel_captures_and_freezes(self):
        # Finding 2: the delete channel is handed off on its own token (channel 1), so a
        # keyed relocation carries the delete position across the seam. Capture parks and
        # freezes, exactly like the upsert channel.
        bridge = FakeBridge()

        rows, offset = self.connector(bridge).read_table_deletes(
            "app.orders", _stream_offset(125), {"table.migration": "true"}
        )

        self.assertEqual(list(rows), [])
        self.assertEqual(dict(offset), _stream_offset(125))
        self.assertEqual(len(self._lakebase.database.handoff), 1)
        (_, _, channel), token = next(iter(self._lakebase.database.handoff.items()))
        self.assertEqual(channel, 1)  # the delete channel, distinct from the upsert token
        self.assertEqual(json.loads(token["offset_json"])["commit_lsn"], "125")

    def test_handoff_delete_channel_cutover_seeds_and_drops_token(self):
        bridge = FakeBridge()
        # Capture the delete position on the source flow.
        self.connector(bridge, registration_scope="old-flow").read_table_deletes(
            "app.orders", _stream_offset(125), {"table.migration": "true"}
        )
        connector = self.connector(bridge, registration_scope="new-flow")

        # Cutover: a fresh delete flow seeds from the channel-1 token at exactly 125.
        _, seeded = connector.read_table_deletes("app.orders", {}, {"table.migration": "true"})
        self.assertEqual(seeded["commit_lsn"], "125")
        self.assertTrue(seeded["handoff_seeded"])
        self.assertEqual(len(self._lakebase.database.handoff), 1)  # kept until committed

        # Committed seed: drop the channel-1 token and record-and-stop (no rows). The
        # marker is kept so the offset is not mistaken for a fresh capture and re-parked.
        rows, resumed = connector.read_table_deletes(
            "app.orders", seeded, {"table.migration": "true"}
        )
        self.assertEqual(list(rows), [])
        self.assertTrue(resumed["handoff_seeded"])
        self.assertEqual(self._lakebase.database.handoff, {})

        # A later steady-state delete read stays stopped -- no orphan token re-parked.
        connector.read_table_deletes("app.orders", resumed, {"table.migration": "true"})
        self.assertEqual(self._lakebase.database.handoff, {})

    def test_handoff_delete_channel_without_token_falls_back_to_upsert_boundary(self):
        # If only the upsert channel was captured, the delete cutover has no parked token;
        # it must not crash -- it falls back to the ordinary bootstrap that adopts the
        # upsert boundary published under the new scope.
        bridge = FakeBridge()
        list(self.connector(bridge).read_table("app.orders", {}, {})[0])

        rows, offset = self.connector(bridge).read_table_deletes(
            "app.orders", {}, {"table.migration": "true"}
        )

        self.assertEqual(list(rows), [])
        self.assertEqual(offset["commit_lsn"], "90")  # adopted the upsert boundary

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

    def test_delete_bootstrap_anchors_at_snapshot_floor_across_restart(self):
        # A restart (new update scope, same pipeline) publishes a fresh upsert
        # channel-start at the reader's *resume* position, which is ahead of the
        # snapshot boundary. A delete flow bootstrapping there would skip before-image
        # deletes -- e.g. a mutated-primary-key row's old-key delete -- committed between
        # the snapshot and the resume, orphaning the pre-update rows. The durable
        # per-pipeline snapshot floor must pull the bootstrap back to the snapshot
        # boundary the current destination rows were captured at.
        pid = "11111111-1111-1111-1111-111111111111"
        scope_snapshot = f"{pid}_@_22222222-2222-2222-2222-222222222222"
        scope_restart = f"{pid}_@_33333333-3333-3333-3333-333333333333"

        snapshot_connector = self.connector()
        snapshot_connector.set_registration_scope(scope_snapshot)
        list(snapshot_connector.read_table("app.orders", {}, {})[0])  # snapshot at now=90
        table = snapshot_connector._table("app.orders", {})
        schema_id = snapshot_connector._snapshot_schema_ids[(scope_snapshot, table.identity)]
        fingerprint = _schema_fingerprint(table)

        # A later update of the same pipeline resumes the upsert reader at 120 -- above
        # the 90 snapshot -- and publishes that as the new scope's channel-start.
        restart_connector = self.connector()
        restart_connector.set_registration_scope(scope_restart)
        restart_connector._publish_upsert_channel_start(
            restart_connector._table("app.orders", {}), 120, schema_id, fingerprint, scope_restart
        )

        _, offset = restart_connector.read_table_deletes("app.orders", {}, {})
        # Anchored at the snapshot floor (90), NOT the drifted upsert resume (120): the
        # deletes committed in (90, 120] stay within the delete channel's read range.
        self.assertEqual(offset["commit_lsn"], "90")

    _FLOOR_PIPELINE = "11111111-1111-1111-1111-111111111111"

    def _drifted_upsert(self, *, pipeline=_FLOOR_PIPELINE, restart_bridge=None):
        """Snapshot at LSN 90 in one update scope, then a later update of ``pipeline``
        that resumed its upsert reader at 120 and published that as its channel-start.
        Returns the restart-scope connector plus the snapshot's schema id/fingerprint."""

        scope_snapshot = f"{self._FLOOR_PIPELINE}_@_22222222-2222-2222-2222-222222222222"
        scope_restart = f"{pipeline}_@_33333333-3333-3333-3333-333333333333"
        snapshot_connector = self.connector()
        snapshot_connector.set_registration_scope(scope_snapshot)
        list(snapshot_connector.read_table("app.orders", {}, {})[0])  # snapshot at now=90
        table = snapshot_connector._table("app.orders", {})
        schema_id = snapshot_connector._snapshot_schema_ids[(scope_snapshot, table.identity)]
        fingerprint = _schema_fingerprint(table)
        restart_connector = self.connector(restart_bridge)
        restart_connector.set_registration_scope(scope_restart)
        restart_connector._publish_upsert_channel_start(
            restart_connector._table("app.orders", {}), 120, schema_id, fingerprint, scope_restart
        )
        return restart_connector, schema_id, fingerprint

    def test_delete_bootstrap_reports_when_the_floor_pulls_it_back(self):
        # The re-read can span days of log and lowers the delete shard's shared restart,
        # so the event log must show both positions.
        restart_connector, _, _ = self._drifted_upsert()
        with self.assertLogs(informix_module.__name__, level="INFO") as logs:
            _, offset = restart_connector.read_table_deletes("app.orders", {}, {})
        self.assertEqual(offset["commit_lsn"], "90")
        message = "\n".join(logs.output)
        self.assertIn("snapshot floor 90", message)
        self.assertIn("resume position 120", message)

    def test_delete_bootstrap_warns_when_the_floor_has_aged_below_retention(self):
        # The floor (90) has fallen out of the retained log (minimum 100): the deletes
        # between it and the resume position are unrecoverable, so the bootstrap uses
        # the resume position and says so instead of skipping them silently.
        bridge = FakeBridge()
        bridge.minimum, bridge.now = 100, 200
        restart_connector, _, _ = self._drifted_upsert(restart_bridge=bridge)
        with self.assertLogs(informix_module.__name__, level="WARNING") as logs:
            _, offset = restart_connector.read_table_deletes("app.orders", {}, {})
        self.assertEqual(offset["commit_lsn"], "120")
        message = "\n".join(logs.output)
        self.assertIn("floor 90", message)
        self.assertIn("precedes the minimum retained LSN 100", message)
        self.assertIn("resume position 120", message)

    def test_delete_bootstrap_ignores_another_pipelines_floor(self):
        # Concurrent full refreshes of one table land on different destinations at
        # different snapshot LSNs; a floor is per pipeline and never crosses over.
        other = "99999999-9999-9999-9999-999999999999"
        restart_connector, _, _ = self._drifted_upsert(pipeline=other)
        _, offset = restart_connector.read_table_deletes("app.orders", {}, {})
        self.assertEqual(offset["commit_lsn"], "120")

    def test_delete_bootstrap_warns_when_the_floor_belongs_to_an_earlier_schema(self):
        # A floor captured under a superseded schema generation cannot seed a bootstrap
        # offset that names the current layout; the resume position is used and the
        # skipped range is reported rather than silently dropped.
        restart_connector, schema_id, _ = self._drifted_upsert()
        table = restart_connector._table("app.orders", {})
        stale_generation = "f" * 32
        restart_connector._publish_delete_snapshot_floor(
            table, stale_generation, 95, restart_connector._pipeline_scope()
        )
        with self.assertLogs(informix_module.__name__, level="WARNING") as logs:
            _, offset = restart_connector.read_table_deletes("app.orders", {}, {})
        self.assertEqual(offset["commit_lsn"], "120")
        message = "\n".join(logs.output)
        self.assertIn(f"generation {stale_generation}", message)
        self.assertIn(f"now runs generation {schema_id}", message)

    def test_delete_snapshot_floor_only_moves_forward(self):
        # A later resnapshot advances the floor; an older or replayed publish cannot
        # lower it, so a stale writer can never pull a bootstrap below the rows the
        # destination actually holds.
        connector = self.connector()
        table = connector._table("app.orders", {})
        schema_id, scope = "a" * 32, connector._pipeline_scope()
        fingerprint = _schema_fingerprint(table)
        for lsn, expected in ((90, 90), (120, 120), (100, 120)):
            connector._publish_delete_snapshot_floor(table, schema_id, lsn, scope)
            self.assertEqual(
                connector._read_delete_snapshot_floor(table, schema_id, fingerprint), expected, lsn
            )

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
        # The published successor adds an INTEGER column the live table has as VARCHAR:
        # no widening reconciles the two. (A different added name would not conflict --
        # dropping one nullable column and adding another is an evolution in itself.)
        conflicting_raw = json.loads(json.dumps(bridge.tables[0]))
        conflicting_raw["columns"].append(
            {"name": "branch", "type_name": "INTEGER", "nullable": True}
        )
        bridge.tables[0]["columns"].append(
            {"name": "branch", "type_name": "VARCHAR", "nullable": True, "length": 20}
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

        with self.assertRaisesRegex(InformixError, "changed column branch other than by widening"):
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
        with self.assertRaisesRegex(
            InformixError, "changed column value other than by widening.*full refresh"
        ):
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

        with self.assertRaisesRegex(
            InformixError, "changed column value other than by widening.*full refresh"
        ):
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

    def test_staged_serve_skips_the_live_schema_query_within_the_refresh_interval(self):
        # A resumed snapshot re-queries Informix for drift only periodically, not per page.
        # The first resume microbatch refreshes; later ones within the interval serve
        # straight from the Volume with no get_table catalog query -- that per-page Informix
        # connection was the dominant serve cost.
        bridge = FakeBridge()
        bridge.rows.append({"id": 3, "value": "c"})  # three rows -> three single-row pages
        connector = self.connector(bridge, **{"snapshot.page.size": "1"})
        connector._snapshot_schema_refresh_interval = 3600  # no second refresh this stream
        _, o0 = connector.read_table("app.orders", {}, {})  # fresh -> page 0
        _, o1 = connector.read_table("app.orders", o0, {})  # first resume -> page 1 (refreshes)
        with mock.patch.object(bridge, "get_table", wraps=bridge.get_table) as get_table:
            _, o2 = connector.read_table("app.orders", o1, {})  # resume -> page 2, within interval
        self.assertEqual(o2["phase"], "stream", "expected the final page")
        self.assertEqual(
            get_table.call_count,
            0,
            "staged serve re-queried Informix within the refresh interval",
        )

    def test_staged_serve_detects_drift_when_the_refresh_interval_elapses(self):
        # With the interval elapsed (forced to 0 = every page), a resumed snapshot re-reads
        # the live schema and still aborts on drift on a non-first page: the guard is
        # preserved, just applied periodically rather than on every microbatch.
        bridge = FakeBridge()
        bridge.rows.append({"id": 3, "value": "c"})
        connector = self.connector(bridge, **{"snapshot.page.size": "1"})
        connector._snapshot_schema_refresh_interval = 0.0
        _, o0 = connector.read_table("app.orders", {}, {})  # fresh -> page 0
        _, o1 = connector.read_table("app.orders", o0, {})  # resume -> page 1 (refresh, no drift)
        bridge.tables[0]["columns"].append(
            {"name": "added", "type_name": "INTEGER", "nullable": True}
        )
        with self.assertRaisesRegex(InformixError, "schema changed"):
            connector.read_table("app.orders", o1, {})  # resume -> page 2, refresh detects drift

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

    def test_a_refresh_leaves_out_an_unsupported_column(self):
        informix_module._STATE_VALIDATION_COORDINATOR.unsupported_column_warnings.clear()
        bridge = FakeBridge()
        original = bridge.get_table

        def refreshed_with_unsupported_column(identity):
            raw = original(identity)
            return {
                **raw,
                "columns": [
                    *raw["columns"],
                    {
                        "name": "payload",
                        "type_name": "TEXT",
                        "nullable": True,
                        "cdc_supported": False,
                    },
                ],
            }

        bridge.get_table = refreshed_with_unsupported_column
        connector = self.connector(bridge)

        with self.assertLogs(informix_module.__name__, level="WARNING") as logs:
            rows, _ = connector.read_table("app.orders", {}, {})
            rows = list(rows)
        self.assertEqual(sorted(row["id"] for row in rows), [1, 2])
        self.assertTrue(all("payload" not in row for row in rows))
        self.assertTrue(any("payload (TEXT)" in line for line in logs.output))

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

    WILDCARD = {"primary.keys": "*"}

    def test_primary_keys_wildcard_keys_every_column_in_column_order(self):
        # `value` is nullable and joins the key anyway: like an explicit list, the
        # wildcard trusts the operator that key columns never actually hold NULL.
        for raw in ("*", " * ", '["*"]'):
            with self.subTest(raw=raw):
                connector, _ = self._keyless_connector()

                metadata = connector.read_table_metadata("app.orders", {"primary.keys": raw})

                self.assertEqual(metadata["ingestion_type"], "cdc_with_deletes")
                self.assertEqual(metadata["primary_keys"], ["id", "value"])
                table = connector._table("app.orders", {"primary.keys": raw})
                self.assertTrue(table.key_override and table.key_wildcard)

    def test_primary_keys_wildcard_must_stand_alone(self):
        connector, _ = self._keyless_connector()

        for raw in ("*,id", '["*", "id"]', "*,*"):
            with self.subTest(raw=raw), self.assertRaisesRegex(ValueError, "alone"):
                connector.read_table_metadata("app.orders", {"primary.keys": raw})

    def _project_deletes(self, connector, table_options, *records):
        """Project ``records`` onto app.orders' delete channel as one transaction."""
        table = connector._table("app.orders", dict(table_options))
        tx = informix_module.CommittedTransaction(1, 100, 110, 110, tuple(records))
        return informix_module._project_transaction(tx, table, deletes=True)

    def test_a_delete_carries_null_for_a_nullable_key_column(self):
        # The pipeline's AUTO CDC delete flow matches NULL key values, so a NULL in a
        # nullable key column is passed through rather than failing the flow. Under a
        # wildcard every update changes the key, so the update's old row is deleted the
        # same way.
        update = {
            "op": "UPDATE",
            "lsn": 103,
            "before_lsn": 102,
            "before": {"id": 1, "value": None},
            "after": {"id": 1, "value": "b"},
        }
        delete = {"op": "DELETE", "lsn": 104, "row": {"id": 1, "value": None}}
        for options in (dict(self.WILDCARD), {"primary.keys": "id,value"}):
            with self.subTest(options=options):
                connector, _ = self._keyless_connector()  # `value` is nullable

                rows = self._project_deletes(connector, options, update, delete)

                self.assertEqual(
                    [(row["id"], row["value"], row["_informix_op"]) for row in rows],
                    [(1, None, "d"), (1, None, "d")],
                )

    def test_a_null_in_a_not_null_key_column_still_fails_as_a_broken_record(self):
        # A NOT NULL key column -- every real catalog primary key -- never legitimately
        # holds NULL, so a NULL there is a corrupt or incomplete before image. So is a key
        # column missing from the image, since the decoder sets every captured column.
        cases = (
            ({"id": None, "value": "a"}, "NULL in NOT NULL key column id"),
            ({"value": "a"}, "no value for key column id"),
        )
        for options in ({}, dict(self.WILDCARD)):
            for row, message in cases:
                with self.subTest(options=options, row=row):
                    if options:
                        connector, _ = self._keyless_connector()  # `id` is NOT NULL
                    else:
                        connector = self.connector(FakeBridge())  # catalog key `id`
                    delete = {"op": "DELETE", "lsn": 101, "row": row}

                    with self.assertRaisesRegex(InformixError, message):
                        self._project_deletes(connector, options, delete)

    def test_a_schema_refresh_re_expands_a_wildcard_key(self):
        # An explicit list survives a refresh unchanged (see the test above); a wildcard
        # is every column, so it must pick up the added one.
        connector, bridge = self._keyless_connector()
        table = connector._table("app.orders", dict(self.WILDCARD))
        self.assertEqual(table.primary_keys, ("id", "value"))
        bridge.tables[0]["columns"].append(
            {"name": "added", "type_name": "INTEGER", "nullable": True}
        )

        refreshed = connector._refresh_table_schema(table, None)

        self.assertEqual(refreshed.primary_keys, ("id", "value", "added"))
        self.assertTrue(refreshed.key_override and refreshed.key_wildcard)

    def _read_after_adding_a_column(self, options, *, restart, bridge=None, then=None):
        """Checkpoint app.orders, append a nullable column at the source, then read on
        from that checkpoint with the same connector or a restarted one, under ``then``'s
        options when given."""
        rows, checkpoint, end, _reader, _bridge = self._add_column_and_read(
            options, restart=restart, bridge=bridge, then=then
        )
        return rows, checkpoint, end

    def _add_column_and_read(self, options, *, restart, bridge=None, then=None):
        bridge = bridge or FakeBridge()
        if bridge.tables == FakeBridge().tables:
            bridge.tables = [_table(primary_keys=())]
        connector = self.connector(bridge)
        _, checkpoint = connector.read_table("app.orders", {}, options)
        bridge.tables[0]["columns"].append(
            {"name": "added", "type_name": "INTEGER", "nullable": True}
        )
        bridge.now = 120
        bridge.changes = [{"op": "TIMEOUT", "lsn": 120}]
        reader = self.connector(bridge) if restart else connector
        rows, end = reader.read_table("app.orders", checkpoint, options if then is None else then)
        return list(rows), checkpoint, end, reader, bridge

    # A whole-row key grows by an appended nullable column without a full refresh: rows
    # written before the ALTER hold NULL in it at the source and in the target, so their
    # updates and deletes still match by the longer key.
    def test_an_added_column_extends_a_running_wildcard_key(self):
        rows, checkpoint, transitioned, reader, _ = self._add_column_and_read(
            dict(self.WILDCARD), restart=False
        )

        self.assertEqual(rows, [])
        self.assertNotEqual(transitioned["schema_fingerprint"], checkpoint["schema_fingerprint"])
        metadata = reader.read_table_metadata("app.orders", dict(self.WILDCARD))
        self.assertEqual(metadata["primary_keys"], ["id", "value", "added"])

    def test_an_added_column_extends_a_restarted_wildcard_key_for_later_changes(self):
        options = dict(self.WILDCARD)
        _, _, transitioned, reader, bridge = self._add_column_and_read(options, restart=True)
        bridge.changes = [
            {"op": "BEGIN", "tx_id": 7, "lsn": 121},
            # A row written before the ALTER: NULL in the new column on both sides.
            {"op": "DELETE", "tx_id": 7, "lsn": 122, "row": {"id": 1, "value": "a", "added": None}},
            # Setting the new column changes the key: the old row goes, the new one lands.
            {
                "op": "BEFORE_UPDATE",
                "tx_id": 7,
                "lsn": 123,
                "row": {"id": 2, "value": "b", "added": None},
            },
            {
                "op": "AFTER_UPDATE",
                "tx_id": 7,
                "lsn": 124,
                "row": {"id": 2, "value": "b", "added": 5},
            },
            {"op": "COMMIT", "tx_id": 7, "lsn": 125},
        ]

        upserts, _ = reader.read_table("app.orders", transitioned, options)
        deletes, _ = reader.read_table_deletes("app.orders", transitioned, options)

        self.assertEqual(
            [(row["id"], row["value"], row["added"], row["_informix_op"]) for row in upserts],
            [(2, "b", 5, "u")],
        )
        self.assertEqual(
            [(row["id"], row["value"], row["added"], row["_informix_op"]) for row in deletes],
            [(1, "a", None, "d"), (2, "b", None, "d")],
        )

    def test_a_column_added_with_a_default_still_needs_a_full_refresh(self):
        # Existing rows would carry the default at the source but NULL in the target.
        bridge = FakeBridge()
        bridge.tables = [_table(primary_keys=())]
        bridge.column_default_names = {"added"}

        with self.assertRaisesRegex(InformixError, "with a default.*run a full refresh"):
            self._read_after_adding_a_column(dict(self.WILDCARD), restart=True, bridge=bridge)

    def test_a_wildcard_key_does_not_grow_during_an_incremental_copy(self):
        # The copy's stored chunk position is laid out by the old key.
        options = {**self.WILDCARD, "snapshot.mode": "incremental", "snapshot.page.size": "1"}
        bridge = FakeBridge()
        bridge.tables = [_table(primary_keys=())]
        connector = self.connector(bridge)
        _, checkpoint = connector.read_table("app.orders", {}, options)
        self.assertIn("incremental", checkpoint, "the copy must still be in progress")
        bridge.tables[0]["columns"].append(
            {"name": "added", "type_name": "INTEGER", "nullable": True}
        )
        bridge.now = 120
        bridge.changes = [{"op": "TIMEOUT", "lsn": 120}]

        with self.assertRaisesRegex(InformixError, "primary keys; run a full refresh"):
            rows, _ = self.connector(bridge).read_table("app.orders", checkpoint, options)
            list(rows)

    def test_a_wildcard_key_may_grow_only_by_exactly_the_appended_columns(self):
        def table(columns, keys):
            raw = _table(primary_keys=keys)
            raw["columns"] = [
                {"name": name, "type_name": "INTEGER", "nullable": name != "id"} for name in columns
            ]
            return informix_module.Table.parse(raw, "demo")

        previous = table(("id", "value"), ("id", "value"))
        grown = table(("id", "value", "added"), ("id", "value", "added"))
        ensure = informix_module._ensure_compatible_schema_change

        ensure(previous, grown, extend_wildcard_key=True)
        with self.assertRaisesRegex(InformixError, "primary keys"):
            ensure(previous, grown)  # growth needs the wildcard
        for current in (
            table(("id", "value", "added"), ("id", "added", "value")),  # reordered
            table(("id", "value", "added"), ("id", "value")),  # unchanged is not growth
            table(("id", "value", "added", "more"), ("id", "value", "added")),  # partial
        ):
            with self.subTest(keys=current.primary_keys, columns=len(current.columns)):
                if current.primary_keys == previous.primary_keys:
                    continue  # an unchanged key is an ordinary additive change
                with self.assertRaisesRegex(InformixError, "primary keys"):
                    ensure(previous, current, extend_wildcard_key=True)
        explicit = table(("id", "value"), ("id",))
        with self.assertRaisesRegex(InformixError, "primary keys"):
            ensure(
                explicit, table(("id", "value", "added"), ("id", "added")), extend_wildcard_key=True
            )

    def test_a_wildcard_key_may_shrink_only_by_exactly_the_dropped_columns(self):
        def table(columns, keys, not_null=("id",)):
            raw = _table(primary_keys=keys)
            raw["columns"] = [
                {"name": name, "type_name": "INTEGER", "nullable": name not in not_null}
                for name in columns
            ]
            return informix_module.Table.parse(raw, "demo")

        previous = table(("id", "value", "extra"), ("id", "value", "extra"))
        shrunk = table(("id", "value"), ("id", "value"))
        ensure = informix_module._ensure_compatible_schema_change

        ensure(previous, shrunk, extend_wildcard_key=True, shrink_wildcard_key=True)
        # Mid-update the destination still merges on the longer key: restart, no refresh.
        with self.assertRaisesRegex(InformixError, "running update; stop and restart"):
            ensure(previous, shrunk, extend_wildcard_key=True)
        # No wildcard (or an incremental copy still running): the key change needs a refresh.
        with self.assertRaisesRegex(InformixError, "primary keys; run a full refresh"):
            ensure(previous, shrunk, shrink_wildcard_key=True)
        allowed = {"extend_wildcard_key": True, "shrink_wildcard_key": True}
        # A NOT NULL column leaves the key too: the target column is nullable regardless.
        ensure(
            table(("id", "value", "extra"), ("id", "value", "extra"), ("id", "extra")),
            shrunk,
            **allowed,
        )
        for current in (
            table(("id", "value", "added"), ("id", "value", "added")),  # drop and add at once
            table(("id", "value"), ("id",)),  # shrank by more than the dropped column
        ):
            with self.subTest(keys=current.primary_keys, columns=len(current.columns)):
                with self.assertRaisesRegex(InformixError, "primary keys"):
                    ensure(previous, current, **allowed)
        explicit = table(("id", "value", "extra"), ("id", "extra"))
        with self.assertRaisesRegex(InformixError, "primary keys"):
            ensure(explicit, table(("id", "value"), ("id",)), **allowed)

    def _checkpoint_then_drop_a_column(self, options):
        """Checkpoint app.orders with a nullable ``extra`` column, then drop it at the
        source: ``(bridge, checkpoint)``."""
        bridge = FakeBridge()
        bridge.tables = [_table(primary_keys=())]
        bridge.tables[0]["columns"].append(
            {"name": "extra", "type_name": "INTEGER", "nullable": True}
        )
        bridge.rows = [{**row, "extra": index} for index, row in enumerate(bridge.rows)]
        snapshot, checkpoint = self.connector(bridge).read_table("app.orders", {}, options)
        list(snapshot)  # Spark consumes a batch before it commits the offset
        bridge.tables[0]["columns"].pop()
        bridge.now = 120
        bridge.changes = [{"op": "TIMEOUT", "lsn": 120}]
        return bridge, checkpoint

    # A whole-row key shrinks with a dropped column on the first read of a new
    # update: the destination keeps the column, and changes match by the shorter key.
    def test_a_dropped_column_shrinks_a_restarted_wildcard_key(self):
        options = dict(self.WILDCARD)
        bridge, checkpoint = self._checkpoint_then_drop_a_column(options)
        reader = self.connector(bridge, registration_scope="next-update")
        rows, transitioned = reader.read_table("app.orders", checkpoint, options)

        self.assertEqual(list(rows), [])
        self.assertNotEqual(transitioned["schema_fingerprint"], checkpoint["schema_fingerprint"])
        metadata = reader.read_table_metadata("app.orders", options)
        self.assertEqual(metadata["primary_keys"], ["id", "value"])

        bridge.changes = [
            {"op": "BEGIN", "tx_id": 7, "lsn": 121},
            # Logged before the drop; Informix returns it without the column.
            {"op": "DELETE", "tx_id": 7, "lsn": 122, "row": {"id": 1, "value": "a"}},
            {"op": "BEFORE_UPDATE", "tx_id": 7, "lsn": 123, "row": {"id": 2, "value": "b"}},
            {"op": "AFTER_UPDATE", "tx_id": 7, "lsn": 124, "row": {"id": 2, "value": "c"}},
            {"op": "COMMIT", "tx_id": 7, "lsn": 125},
        ]
        upserts, _ = reader.read_table("app.orders", transitioned, options)
        deletes, _ = reader.read_table_deletes("app.orders", transitioned, options)

        upserts, deletes = list(upserts), list(deletes)
        self.assertEqual(
            [(row["id"], row["value"], row["_informix_op"]) for row in upserts], [(2, "c", "u")]
        )
        self.assertEqual(
            [(row["id"], row["value"], row["_informix_op"]) for row in deletes],
            [(1, "a", "d"), (2, "b", "d")],
        )
        self.assertFalse(any("extra" in row for row in upserts + deletes))

    def test_a_dropped_column_fails_a_running_wildcard_key_until_the_pipeline_restarts(self):
        options = dict(self.WILDCARD)
        bridge, checkpoint = self._checkpoint_then_drop_a_column(options)
        with self.assertRaisesRegex(InformixError, "stop and restart the pipeline"):
            self.connector(bridge).read_table("app.orders", checkpoint, options)
        # The same checkpoint read in the next update adopts the shorter key.
        reader = self.connector(bridge, registration_scope="next-update")
        rows, _ = reader.read_table("app.orders", checkpoint, options)
        self.assertEqual(list(rows), [])
        self.assertEqual(
            reader.read_table_metadata("app.orders", options)["primary_keys"], ["id", "value"]
        )

    # primary.keys "* except(...)": every column but the named ones, following the schema
    # as the bare wildcard does.
    EXCEPT = {"primary.keys": "* except(value)"}

    def test_primary_keys_wildcard_except_keys_every_other_column(self):
        for raw in (
            "* except(value)",
            "*EXCEPT(value)",
            " * Except ( value ) ",
            "* except(VALUE)",
            '["* except(value)"]',
        ):
            with self.subTest(raw=raw):
                connector, _ = self._keyless_connector()

                metadata = connector.read_table_metadata("app.orders", {"primary.keys": raw})

                self.assertEqual(metadata["ingestion_type"], "cdc_with_deletes")
                self.assertEqual(metadata["primary_keys"], ["id"])
                table = connector._table("app.orders", {"primary.keys": raw})
                self.assertTrue(table.key_override and table.key_wildcard)
                self.assertEqual([n.casefold() for n in table.key_wildcard_except], ["value"])

    def test_primary_keys_wildcard_except_must_name_columns_of_the_table(self):
        connector, _ = self._keyless_connector()
        for raw, missing in (("* except(later)", "later"), ("* except(Value, vaule)", "vaule")):
            with self.subTest(raw=raw):
                with self.assertRaisesRegex(InformixError, f"does not have: {missing}$"):
                    connector.read_table_metadata("app.orders", {"primary.keys": raw})

    def test_primary_keys_wildcard_except_rejects_malformed_values(self):
        connector, _ = self._keyless_connector()
        for raw, message in (
            ("* except()", "names no columns"),
            ("* except( , )", "names no columns"),
            ("* except(value, value)", "twice"),
            ("* except(value, VALUE)", "twice"),
            ("* except(id, value)", "leaving no key"),
            ("*, except(value)", "alone"),
        ):
            with self.subTest(raw=raw):
                with self.assertRaisesRegex((InformixError, ValueError), message):
                    connector.read_table_metadata("app.orders", {"primary.keys": raw})

    def test_a_schema_refresh_re_expands_a_wildcard_except_key(self):
        connector, bridge = self._keyless_connector()
        table = connector._table("app.orders", dict(self.EXCEPT))
        self.assertEqual(table.primary_keys, ("id",))
        bridge.tables[0]["columns"].append(
            {"name": "added", "type_name": "INTEGER", "nullable": True}
        )

        refreshed = connector._refresh_table_schema(table, None)

        self.assertEqual(refreshed.primary_keys, ("id", "added"))
        self.assertEqual(refreshed.key_wildcard_except, ("value",))

    def test_a_schema_refresh_fails_when_an_excepted_column_is_dropped(self):
        connector, bridge = self._keyless_connector()
        bridge.tables[0]["columns"].append(
            {"name": "extra", "type_name": "INTEGER", "nullable": True}
        )
        table = connector._table("app.orders", {"primary.keys": "* except(extra)"})
        bridge.tables[0]["columns"].pop()

        with self.assertRaisesRegex(InformixError, "does not have: extra"):
            connector._refresh_table_schema(table, None)

    def test_an_added_column_extends_a_running_wildcard_except_key(self):
        rows, checkpoint, transitioned, reader, _ = self._add_column_and_read(
            dict(self.EXCEPT), restart=False
        )

        self.assertEqual(rows, [])
        self.assertNotEqual(transitioned["schema_fingerprint"], checkpoint["schema_fingerprint"])
        metadata = reader.read_table_metadata("app.orders", dict(self.EXCEPT))
        self.assertEqual(metadata["primary_keys"], ["id", "added"])

    def test_an_added_column_named_in_except_stays_out_of_the_key(self):
        # The column can be named once it exists, before the pipeline restarts.
        options = {"primary.keys": "* except(value, added)"}
        rows, checkpoint, transitioned, reader, _ = self._add_column_and_read(
            dict(self.EXCEPT), restart=True, then=options
        )

        self.assertEqual(rows, [])
        # The column joins the table, so the schema moves on, but not the key.
        self.assertNotEqual(transitioned["schema_fingerprint"], checkpoint["schema_fingerprint"])
        self.assertEqual(reader.read_table_metadata("app.orders", options)["primary_keys"], ["id"])
        table = reader._table("app.orders", options)
        self.assertEqual([column.name for column in table.columns], ["id", "value", "added"])

    def _with_a_defaulted_added_column(self):
        bridge = FakeBridge()
        bridge.tables = [_table(primary_keys=())]
        bridge.column_default_names = {"added"}
        return bridge

    def test_an_excepted_column_added_with_a_default_needs_no_full_refresh(self):
        # Only a column joining the key needs existing rows to hold NULL in it.
        rows, _, _ = self._read_after_adding_a_column(
            dict(self.EXCEPT),
            restart=True,
            bridge=self._with_a_defaulted_added_column(),
            then={"primary.keys": "* except(value, added)"},
        )
        self.assertEqual(rows, [])

    def test_a_column_joining_a_wildcard_except_key_with_a_default_needs_a_full_refresh(self):
        with self.assertRaisesRegex(InformixError, "with a default.*full refresh"):
            self._read_after_adding_a_column(
                dict(self.EXCEPT), restart=True, bridge=self._with_a_defaulted_added_column()
            )

    def test_a_dropped_column_shrinks_a_restarted_wildcard_except_key(self):
        # ``extra`` is in the key and goes; ``value`` stays excepted.
        options = dict(self.EXCEPT)
        bridge, checkpoint = self._checkpoint_then_drop_a_column(options)
        with self.assertRaisesRegex(InformixError, "stop and restart the pipeline"):
            self.connector(bridge).read_table("app.orders", checkpoint, options)
        reader = self.connector(bridge, registration_scope="next-update")
        rows, transitioned = reader.read_table("app.orders", checkpoint, options)
        self.assertEqual(list(rows), [])
        self.assertNotEqual(transitioned["schema_fingerprint"], checkpoint["schema_fingerprint"])
        self.assertEqual(reader.read_table_metadata("app.orders", options)["primary_keys"], ["id"])

    def test_dropping_an_excepted_column_needs_only_its_name_taken_out(self):
        bridge, checkpoint = self._checkpoint_then_drop_a_column(
            {"primary.keys": "* except(extra)"}
        )
        with self.assertRaisesRegex(InformixError, "does not have: extra"):
            self.connector(bridge).read_table(
                "app.orders", checkpoint, {"primary.keys": "* except(extra)"}
            )
        # The key was every other column, which is the whole row now.
        options = dict(self.WILDCARD)
        reader = self.connector(bridge)
        rows, transitioned = reader.read_table("app.orders", checkpoint, options)
        self.assertEqual(list(rows), [])
        self.assertNotEqual(transitioned["schema_fingerprint"], checkpoint["schema_fingerprint"])
        self.assertEqual(
            reader.read_table_metadata("app.orders", options)["primary_keys"], ["id", "value"]
        )

    def test_changing_the_except_list_on_a_running_table_needs_a_full_refresh(self):
        bridge = _ProjectingBridge()
        bridge.tables[0]["primary_keys"] = []
        connector = self.connector(bridge)
        snapshot, checkpoint = connector.read_table(
            "app.orders", {}, {"primary.keys": "* except(extra)"}
        )
        list(snapshot)
        bridge.now = 120
        bridge.changes = [{"op": "TIMEOUT", "lsn": 120}]
        with self.assertRaisesRegex(InformixError, "primary keys; run a full refresh"):
            rows, _ = self.connector(bridge).read_table(
                "app.orders", checkpoint, {"primary.keys": "* except(value)"}
            )
            list(rows)

    def test_a_wildcard_except_key_grows_only_by_the_appended_columns_it_doesnt_except(self):
        def table(columns, keys):
            raw = _table(primary_keys=keys)
            raw["columns"] = [
                {"name": name, "type_name": "INTEGER", "nullable": name != "id"} for name in columns
            ]
            return informix_module.Table.parse(raw, "demo")

        ensure = informix_module._ensure_compatible_schema_change
        previous = table(("id", "note", "value"), ("id", "value"))
        grown = table(("id", "note", "value", "added", "memo"), ("id", "value", "added"))
        allowed = {"extend_wildcard_key": True, "wildcard_except": ("note", "memo")}

        ensure(previous, grown, **allowed)
        for current, why in (
            (grown, "the except list differs"),
            (
                table(("id", "note", "value", "added", "memo"), ("id", "value", "added", "memo")),
                "an excepted column joined",
            ),
        ):
            with self.subTest(why):
                options = (
                    {"extend_wildcard_key": True, "wildcard_except": ("note",)}
                    if (why == "the except list differs")
                    else allowed
                )
                with self.assertRaisesRegex(InformixError, "primary keys"):
                    ensure(previous, current, **options)

    def test_a_wildcard_except_key_covers_the_selected_readable_columns(self):
        for bridge, options, key in (
            (
                _ProjectingBridge(),
                {"exclude_columns": "value", "primary.keys": "* except(extra)"},
                ["id"],
            ),
            (_unsupported_bridge("TEXT"), {"primary.keys": "* except(value)"}, ["id"]),
            # A column left out is still a column of the table, so naming it is no typo.
            (
                _ProjectingBridge(),
                {"exclude_columns": "extra", "primary.keys": "* except(extra)"},
                ["id", "value"],
            ),
            (_unsupported_bridge("TEXT"), {"primary.keys": "* except(extra)"}, ["id", "value"]),
        ):
            with self.subTest(options=options):
                metadata = self.connector(bridge).read_table_metadata("app.orders", options)
                self.assertEqual(metadata["primary_keys"], key)

    def test_an_added_column_transitions_an_explicitly_keyed_flow(self):
        # The contrast: an explicit key list is not tied to the column set.
        rows, checkpoint, transitioned = self._read_after_adding_a_column(
            {"primary.keys": "id,value"}, restart=True
        )

        self.assertEqual(rows, [])
        self.assertNotEqual(transitioned["schema_fingerprint"], checkpoint["schema_fingerprint"])

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

    def test_keyless_initial_snapshot_uses_its_own_page_size_default(self):
        # A keyless initial drain has no seek cursor and stages whole pages, so it
        # defaults to a larger page than keyed keyset paging.
        connector, bridge = self._keyless_connector()
        options = dict(self.APPEND, **{"snapshot.mode": "initial"})

        connector.read_table("app.orders", {}, options)

        self.assertEqual(bridge.snapshot_page_sizes, [50000])

    def test_keyless_initial_snapshot_page_size_is_overridable(self):
        connector, bridge = self._keyless_connector()
        options = dict(
            self.APPEND,
            **{"snapshot.mode": "initial", "keyless.snapshot.page.size": "1234"},
        )

        connector.read_table("app.orders", {}, options)

        self.assertEqual(bridge.snapshot_page_sizes, [1234])

    def test_keyed_initial_snapshot_ignores_the_keyless_page_size(self):
        # The keyed blocking snapshot keeps using snapshot.page.size regardless of
        # the keyless knob, so the two options never interfere.
        bridge = FakeBridge()  # default app.orders is keyed
        connector = self.connector(bridge)

        connector.read_table(
            "app.orders",
            {},
            {"snapshot.mode": "initial", "keyless.snapshot.page.size": "1234"},
        )

        self.assertEqual(bridge.snapshot_page_sizes, [20000])

    def test_keyless_snapshot_page_size_below_minimum_is_rejected(self):
        connector, _ = self._keyless_connector()
        options = dict(
            self.APPEND,
            **{"snapshot.mode": "initial", "keyless.snapshot.page.size": "0"},
        )

        with self.assertRaises(ValueError):
            connector.read_table("app.orders", {}, options)

    def test_invalid_staging_transport_is_rejected(self):
        with self.assertRaises(ValueError):
            self.connector(FakeBridge(), **{"snapshot.staging.transport": "sftp"})

    def test_auto_transport_without_credentials_uses_fuse(self):
        # Offline (no ambient identity, no explicit workspace creds), auto resolves to
        # FUSE without probing for a token -- so existing pipelines are unaffected.
        connector, _ = self._keyless_connector(**{"snapshot.staging.transport": "auto"})
        options = dict(self.APPEND, **{"snapshot.mode": "initial"})

        rows, _ = connector.read_table("app.orders", {}, options)

        self.assertEqual(len(list(rows)), 2)
        self.assertTrue(connector._staging_transport_resolved)
        self.assertIsNone(connector._staging_files_api)

    def test_rest_transport_writes_pages_via_files_api(self):
        created: list = []

        class _FakeFilesApi:
            def __init__(self, host, provider):
                self.puts: list = []
                created.append(self)

            def put(self, path, data):
                # Persist to the staging path so the connector's FUSE read-back finds
                # it -- mirroring production, where a REST write and a FUSE read address
                # the same Volume file.
                self.puts.append((path, data))
                os.makedirs(os.path.dirname(path), mode=0o700, exist_ok=True)
                with open(path, "wb") as handle:
                    handle.write(data)

        def _fake_provider(*_a, **_k):
            provider = mock.Mock()
            provider.bearer.return_value = "tok"
            return provider

        with (
            mock.patch.object(informix_module, "WorkspaceFilesApi", _FakeFilesApi),
            mock.patch.object(informix_module, "StagingCredentialProvider", _fake_provider),
        ):
            connector, _ = self._keyless_connector(
                **{
                    "snapshot.staging.transport": "rest",
                    "lakebase.workspace.host": "https://ws",
                    "lakebase.workspace.token": "boot",
                }
            )
            options = dict(self.APPEND, **{"snapshot.mode": "initial"})
            connector.read_table("app.orders", {}, options)

        self.assertTrue(created, "the REST files client was never built")
        self.assertTrue(created[0].puts, "no snapshot page was written via the Files API")
        path, data = created[0].puts[0]
        self.assertTrue(path.endswith("/data.json.gz"), path)
        self.assertEqual(data[:2], b"\x1f\x8b", "staged page must be gzip")

    def test_auto_transport_falls_back_to_fuse_when_token_unavailable(self):
        def _unavailable_provider(*_a, **_k):
            provider = mock.Mock()
            provider.bearer.side_effect = informix_module.StagingTokenUnavailable("disabled")
            return provider

        with mock.patch.object(informix_module, "StagingCredentialProvider", _unavailable_provider):
            connector, _ = self._keyless_connector(
                **{
                    "snapshot.staging.transport": "auto",
                    "lakebase.workspace.host": "https://ws",
                    "lakebase.workspace.token": "boot",
                }
            )
            options = dict(self.APPEND, **{"snapshot.mode": "initial"})
            rows, _ = connector.read_table("app.orders", {}, options)

        # Fell back to FUSE and still staged the history successfully.
        self.assertEqual(len(list(rows)), 2)
        self.assertIsNone(connector._staging_files_api)

    def _rejecting_transport_read(self, transport):
        # A keyless initial read whose first page PUT gets a 401 nothing can recover.
        class _RejectedFilesApi:
            rejected = False

            def __init__(self, host, provider):
                pass

            def put(self, path, data):
                self.rejected = True  # revoked, and the starting credential expired
                raise informix_module.WorkspaceHttpError("PUT failed with HTTP 401", 401)

        def _fake_provider(*_a, **_k):
            provider = mock.Mock()
            provider.bearer.return_value = "tok"
            return provider

        with (
            mock.patch.object(informix_module, "WorkspaceFilesApi", _RejectedFilesApi),
            mock.patch.object(informix_module, "StagingCredentialProvider", _fake_provider),
        ):
            connector, _ = self._keyless_connector(
                **{
                    "snapshot.staging.transport": transport,
                    "lakebase.workspace.host": "https://ws",
                    "lakebase.workspace.token": "boot",
                }
            )
            options = dict(self.APPEND, **{"snapshot.mode": "initial"})
            rows, _ = connector.read_table("app.orders", {}, options)
            return connector, list(rows)

    def test_auto_transport_moves_to_fuse_after_an_unrecoverable_401(self):
        connector, rows = self._rejecting_transport_read("auto")

        # The rejected page and the rest were staged over FUSE.
        self.assertEqual(len(rows), 2)
        self.assertIsNone(connector._staging_files_api)

    def test_rest_transport_keeps_failing_on_an_unrecoverable_401(self):
        with self.assertRaises(informix_module.WorkspaceHttpError):
            self._rejecting_transport_read("rest")

    def test_rest_transport_hard_fails_when_token_unavailable(self):
        def _unavailable_provider(*_a, **_k):
            provider = mock.Mock()
            provider.bearer.side_effect = informix_module.StagingTokenUnavailable("disabled")
            return provider

        with mock.patch.object(informix_module, "StagingCredentialProvider", _unavailable_provider):
            connector, _ = self._keyless_connector(
                **{
                    "snapshot.staging.transport": "rest",
                    "lakebase.workspace.host": "https://ws",
                    "lakebase.workspace.token": "boot",
                }
            )
            options = dict(self.APPEND, **{"snapshot.mode": "initial"})
            with self.assertRaises(informix_module.StagingTokenUnavailable):
                connector.read_table("app.orders", {}, options)

    def test_pipelining_engages_only_for_keyless_rest_drains(self):
        # The gate is the drain's scan keys, not the table's key: () is a positional
        # drain (a keyless table, or a keyed one on snapshot.scan=cursor).
        keyless = ()
        keyed = ("id",)

        def _fake_provider(*_a, **_k):
            provider = mock.Mock()
            provider.bearer.return_value = "tok"
            return provider

        class _FakeFilesApi:
            def __init__(self, host, provider):
                pass

            def put(self, path, data):
                pass

        rest_opts = {
            "snapshot.staging.transport": "rest",
            "lakebase.workspace.host": "https://ws",
            "lakebase.workspace.token": "boot",
        }
        with (
            mock.patch.object(informix_module, "StagingCredentialProvider", _fake_provider),
            mock.patch.object(informix_module, "WorkspaceFilesApi", _FakeFilesApi),
        ):
            rest, _ = self._keyless_connector(**rest_opts)
            self.assertTrue(
                rest._should_pipeline_staging(keyless), "keyless REST drain should pipeline"
            )
            self.assertFalse(rest._should_pipeline_staging(keyed), "keyset drains are out of scope")

            off, _ = self._keyless_connector(
                **dict(rest_opts, **{"snapshot.staging.pipeline": "false"})
            )
            self.assertFalse(off._should_pipeline_staging(keyless), "the option must disable it")

        fuse, _ = self._keyless_connector(**{"snapshot.staging.transport": "fuse"})
        self.assertFalse(
            fuse._should_pipeline_staging(keyless),
            "pipelining FUSE writes would worsen the very issue REST avoids",
        )

    def test_rest_keyless_drain_pipelines_multiple_pages_without_loss(self):
        created: list = []

        class _FakeFilesApi:
            def __init__(self, host, provider):
                self.puts: list = []
                created.append(self)

            def put(self, path, data):
                self.puts.append(path)
                os.makedirs(os.path.dirname(path), mode=0o700, exist_ok=True)
                with open(path, "wb") as handle:
                    handle.write(data)

        def _fake_provider(*_a, **_k):
            provider = mock.Mock()
            provider.bearer.return_value = "tok"
            return provider

        with (
            mock.patch.object(informix_module, "WorkspaceFilesApi", _FakeFilesApi),
            mock.patch.object(informix_module, "StagingCredentialProvider", _fake_provider),
        ):
            bridge = FakeBridge()
            bridge.tables = [_table(primary_keys=())]
            bridge.rows = [{"id": i, "value": str(i)} for i in range(10)]
            connector = self.connector(
                bridge,
                **{
                    "snapshot.staging.transport": "rest",
                    "lakebase.workspace.host": "https://ws",
                    "lakebase.workspace.token": "boot",
                },
            )
            options = dict(
                self.APPEND, **{"snapshot.mode": "initial", "keyless.snapshot.page.size": "3"}
            )
            collected = []
            offset: dict = {}
            for _ in range(50):
                rows, offset = connector.read_table("app.orders", offset, options)
                collected.extend(rows)
                if offset.get("phase") == "stream":
                    break
            else:
                self.fail("append-only initial drain did not converge to the stream phase")

        self.assertEqual(len(collected), 10, "every row must survive the pipelined drain")
        self.assertGreater(len(created[0].puts), 1, "expected the drain to stage several pages")

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

    # One committed transaction: an insert followed by a delete of the same row. On the
    # append path the delete has no key to pair with, so it can only survive as a row.
    _APPEND_DELETE_CHANGES = [
        {"op": "BEGIN", "tx_id": 1, "lsn": 100},
        {"op": "INSERT", "tx_id": 1, "lsn": 101, "row": {"id": 1, "value": "a"}},
        {"op": "DELETE", "tx_id": 1, "lsn": 102, "row": {"id": 1, "value": "a"}},
        {"op": "COMMIT", "tx_id": 1, "lsn": 110},
    ]

    def _append_stream(self, table_options, changes=None, **connection_options):
        """Bootstrap a keyless append flow at its cdc_only boundary, then stream one
        batch of ``changes`` through its upsert channel."""
        connector, bridge = self._keyless_connector(**connection_options)
        _, offset = connector.read_table("app.orders", {}, table_options)
        bridge.changes = list(self._APPEND_DELETE_CHANGES if changes is None else changes)
        rows, _ = connector.read_table("app.orders", offset, table_options)
        return list(rows)

    @staticmethod
    def _ops(rows):
        return [(row["id"], row["value"], row["_informix_op"]) for row in rows]

    def test_an_append_delete_lands_as_a_full_before_image_tombstone(self):
        rows = self._append_stream({})

        self.assertEqual(self._ops(rows), [(1, "a", "c"), (1, "a", "d")])
        inserted, tombstone = rows
        # The tombstone is a complete change-log record -- every source column plus the
        # same metadata columns as the insert -- not a key-only delete.
        self.assertEqual(set(tombstone), set(inserted))
        self.assertEqual(tombstone[CURSOR], _sortable_lsn(102))
        self.assertEqual(
            tombstone[informix_module.COMMIT_LSN], inserted[informix_module.COMMIT_LSN]
        )
        self.assertEqual(tombstone[informix_module.TX_ID], 1)
        self.assertIn(informix_module.COMMIT_TIME, tombstone)

    def test_append_delete_tombstones_can_be_switched_off_per_table(self):
        rows = self._append_stream({"append.delete.tombstones": "false"})

        self.assertEqual(self._ops(rows), [(1, "a", "c")])

    def test_append_delete_tombstones_can_be_switched_off_connection_wide(self):
        rows = self._append_stream({}, **{"append.delete.tombstones": "false"})

        self.assertEqual(self._ops(rows), [(1, "a", "c")])

    def test_a_table_tombstone_option_overrides_the_connection_setting(self):
        rows = self._append_stream(
            {"append.delete.tombstones": "true"}, **{"append.delete.tombstones": "false"}
        )

        self.assertEqual(self._ops(rows), [(1, "a", "c"), (1, "a", "d")])

    def test_a_keyed_cdc_upsert_channel_never_emits_a_tombstone(self):
        # The pipeline applies a keyed table's deletes only through the delete flow; a
        # 'd' row on its upsert view would be merged as an upsert, so the option must
        # not leak onto the normal CDC path.
        for option in ({}, {"append.delete.tombstones": "true"}):
            with self.subTest(option=option):
                bridge = FakeBridge()  # default app.orders has a primary key
                bridge.now = 200
                bridge.changes = list(self._APPEND_DELETE_CHANGES)
                connector = self.connector(bridge)

                changes, _ = connector.read_table("app.orders", _stream_offset(), option)
                deletes, _ = connector.read_table_deletes("app.orders", _stream_offset(), option)

                self.assertEqual(self._ops(changes), [(1, "a", "c")])
                self.assertEqual([(row["id"], row["_informix_op"]) for row in deletes], [(1, "d")])

    def _keyed_append_stream(self, changes, table_options=None):
        """Bootstrap a keyed table forced to append at its cdc_only boundary, then stream
        one batch of ``changes`` through its upsert channel."""
        bridge = FakeBridge()  # default app.orders has a primary key
        connector = self.connector(bridge, **{"snapshot.mode": "cdc_only"})
        options = {**self.APPEND, **(table_options or {})}
        _, offset = connector.read_table("app.orders", {}, options)
        bridge.changes = list(changes)
        rows, _ = connector.read_table("app.orders", offset, options)
        return list(rows)

    # One committed transaction: insert id 1, then update it to id 2. The before- and
    # after-images are separate log records at their own positions, as Informix logs them.
    _KEY_CHANGE = [
        {"op": "BEGIN", "tx_id": 1, "lsn": 100},
        {"op": "INSERT", "tx_id": 1, "lsn": 101, "row": {"id": 1, "value": "a"}},
        {"op": "BEFORE_UPDATE", "tx_id": 1, "lsn": 102, "row": {"id": 1, "value": "a"}},
        {"op": "AFTER_UPDATE", "tx_id": 1, "lsn": 103, "row": {"id": 2, "value": "b"}},
        {"op": "COMMIT", "tx_id": 1, "lsn": 110},
    ]

    def test_a_keyed_table_forced_to_append_emits_a_tombstone_on_delete(self):
        # The gate is the append path, not the key: a keyed table opted into append
        # (append.only.ingestion=true alongside scd_type APPEND_ONLY) streams through the
        # same upsert channel, so its DELETE lands as a full-before-image 'd' row too --
        # key columns included, so a consumer can pair the tombstone with the row it
        # removes.
        rows = self._keyed_append_stream(self._APPEND_DELETE_CHANGES)

        self.assertEqual(self._ops(rows), [(1, "a", "c"), (1, "a", "d")])
        inserted, tombstone = rows
        self.assertEqual(set(tombstone), set(inserted))
        self.assertEqual(tombstone["id"], 1, "the key column survives on the tombstone")

    def test_a_key_change_on_a_keyed_append_table_tombstones_the_old_key(self):
        # Updating id 1 to id 2 removes id 1 at the source. Without a tombstone a
        # consumer taking the latest row per key would keep id 1 alive forever.
        rows = self._keyed_append_stream(self._KEY_CHANGE)

        self.assertEqual(self._ops(rows), [(1, "a", "c"), (1, "a", "d"), (2, "b", "u")])
        _, tombstone, updated = rows
        self.assertEqual(set(tombstone), set(updated), "the tombstone is a full row")
        # The tombstone is sequenced at the before-image's own log position, strictly
        # below the after-image's, so ordering by the cursor is unambiguous.
        self.assertEqual(
            [row[CURSOR] for row in rows],
            [_sortable_lsn(101), _sortable_lsn(102), _sortable_lsn(103)],
        )
        self.assertLess(tombstone[CURSOR], updated[CURSOR])
        for column in (informix_module.COMMIT_LSN, informix_module.TX_ID):
            self.assertEqual(tombstone[column], updated[column])

    def test_a_non_key_update_on_a_keyed_append_table_emits_no_tombstone(self):
        changes = [dict(record) for record in self._KEY_CHANGE]
        changes[3]["row"] = {"id": 1, "value": "b"}

        rows = self._keyed_append_stream(changes)

        self.assertEqual(self._ops(rows), [(1, "a", "c"), (1, "b", "u")])

    def test_a_wildcard_keyed_append_table_tombstones_every_update(self):
        # Every column is the key, so even an update to a non-id column changes the key:
        # the old row lands as a 'd' tombstone ahead of the new row's 'u'.
        changes = [dict(record) for record in self._KEY_CHANGE]
        changes[3]["row"] = {"id": 1, "value": "b"}

        rows = self._append_stream({**self.WILDCARD, **self.APPEND}, changes=changes)

        self.assertEqual(self._ops(rows), [(1, "a", "c"), (1, "a", "d"), (1, "b", "u")])

    def test_key_change_tombstones_follow_the_tombstone_option(self):
        rows = self._keyed_append_stream(self._KEY_CHANGE, {"append.delete.tombstones": "false"})

        self.assertEqual(self._ops(rows), [(1, "a", "c"), (2, "b", "u")])

    def test_a_keyless_append_update_never_emits_a_tombstone(self):
        # With no key there is nothing to change: an update is only its after-image.
        rows = self._append_stream({}, changes=self._KEY_CHANGE)

        self.assertEqual(self._ops(rows), [(1, "a", "c"), (2, "b", "u")])

    def test_a_keyed_cdc_upsert_channel_never_tombstones_a_key_change(self):
        # On the normal CDC path the old key is removed by the delete channel; a 'd' row
        # on the upsert view would be merged as an upsert.
        for option in ({}, {"append.delete.tombstones": "true"}):
            with self.subTest(option=option):
                bridge = FakeBridge()  # default app.orders has a primary key
                bridge.now = 200
                bridge.changes = list(self._KEY_CHANGE)
                connector = self.connector(bridge)

                changes, _ = connector.read_table("app.orders", _stream_offset(), option)
                deletes, _ = connector.read_table_deletes("app.orders", _stream_offset(), option)

                self.assertEqual(self._ops(changes), [(1, "a", "c"), (2, "b", "u")])
                self.assertEqual([(row["id"], row["_informix_op"]) for row in deletes], [(1, "d")])

    def test_an_append_delete_without_a_before_image_fails_clearly(self):
        changes = [
            {"op": "BEGIN", "tx_id": 1, "lsn": 100},
            {"op": "DELETE", "tx_id": 1, "lsn": 101},
            {"op": "COMMIT", "tx_id": 1, "lsn": 110},
        ]

        with self.assertRaisesRegex(InformixError, "no before image"):
            self._append_stream({}, changes=changes)

    def test_a_malformed_tombstone_option_is_rejected(self):
        connector, _ = self._keyless_connector()

        with self.assertRaisesRegex(ValueError, "append.delete.tombstones"):
            connector.read_table_metadata("app.orders", {"append.delete.tombstones": "maybe"})
        with self.assertRaisesRegex(ValueError, "append.delete.tombstones"):
            self._keyless_connector(**{"append.delete.tombstones": "maybe"})

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

    def test_an_unsupported_column_is_left_out_with_one_warning(self):
        # The rest of the table is ingested, and streamed, as if exclude_columns listed the
        # column; the warning names it once per process, not on every read.
        informix_module._STATE_VALIDATION_COORDINATOR.unsupported_column_warnings.clear()
        bridge = FakeBridge()
        table = _table(name="documents", cdc=False)
        table["columns"][1]["type_name"] = "TEXT"
        bridge.tables.append(table)
        connector = self.connector(bridge)

        with self.assertLogs(informix_module.__name__, level="WARNING") as logs:
            metadata = connector.read_table_metadata("app.documents", {})
            schema = connector.get_table_schema("app.documents", {})
            connector.read_table_metadata("app.documents", {})

        self.assertEqual(metadata["ingestion_type"], "cdc_with_deletes")
        self.assertEqual(metadata["primary_keys"], ["id"])
        self.assertIn("id", schema.fieldNames())
        self.assertNotIn("value", schema.fieldNames())
        warnings = [line for line in logs.output if "app.documents" in line]
        self.assertEqual(len(warnings), 1)
        self.assertIn("value (TEXT)", warnings[0])

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

    def test_generated_available_now_base_wraps_the_partitioned_reader(self):
        # The partitioned proxy is the default stream reader for CDC-capable tables.
        # It must get the same registration-scope setter and AvailableNow hook as the
        # simple reader; its replay goes through get_partitions, so it needs (and
        # gets) no readBetweenOffsets.
        class TriggerBase:
            pass

        Wrapped = _informix_available_now_base(TriggerBase)

        class LakeflowPartitionedStreamReader(Wrapped):
            def __init__(self):
                self.lakeflow_connect = mock.Mock()
                self.options = {"tableName": "members"}

            def prepareForTriggerAvailableNow(self):
                raise AssertionError("shared no-op was not replaced")

        reader = LakeflowPartitionedStreamReader()
        reader.prepareForTriggerAvailableNow()
        reader.lakeflow_connect.prepare_for_trigger_available_now.assert_called_once_with()
        scope = reader.lakeflow_connect.set_registration_scope.call_args.args[0]
        self.assertRegex(scope, r"^[0-9a-f]{32}$")
        self.assertFalse(hasattr(reader, "readBetweenOffsets"))

        class SomethingElse(Wrapped):
            def __init__(self):
                self.lakeflow_connect = mock.Mock()

        SomethingElse().lakeflow_connect.set_registration_scope.assert_not_called()

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

    def test_a_delete_replay_under_available_now_bypasses_the_trigger_boundary(self):
        """Regression surfaced by stop/restart cycling on tw070.

        A delete-flow replay after a driver restart must NOT consult the shared trigger
        boundary. That boundary is published per update scope by the upsert (owner)
        reader; a replay planned in a fresh scope finds none for the replayed generation,
        so _shared_trigger_boundary(owner=False) raises TriggerBoundaryUnavailable --
        fatal inside get_partitions (planPartitions has no latestOffset retry). The
        replay is bounded by its own committed end LSN, so it must complete without the
        live handshake. Patch the boundary to raise and assert the replay never calls it.
        """

        connector = self.connector(FakeBridge())
        connector.prepare_for_trigger_available_now()
        checkpoint = _stream_offset()
        # Replaying [checkpoint, checkpoint): the committed end equals the start, so the
        # bounded range is empty. The replay must still succeed and read no rows.
        options = {informix_module._REPLAY_STOP_LSN_OPTION: checkpoint["commit_lsn"]}
        with mock.patch.object(
            connector,
            "_shared_trigger_boundary",
            side_effect=TriggerBoundaryUnavailable("boundary not published in this scope"),
        ) as boundary:
            rows, _ = connector.read_table_deletes("app.orders", checkpoint, options)
            self.assertEqual(list(rows), [])
        boundary.assert_not_called()

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

    class _PollBridge(FakeBridge):
        """A FakeBridge whose CDC poll depends on where it starts, as Informix's log
        reader does on a busy log: a poll scans from its start and goes idle a fixed
        distance on, so re-polling from the same LSN goes idle at the same place.
        ``polls`` maps a start LSN to what a poll from there (or any later start below
        the next key) returns."""

        def __init__(self, polls):
            super().__init__()
            self.polls = dict(polls)

        def read_changes(self, tables, start_lsn, timeout_seconds, max_records):
            self.change_reads.append((tables, start_lsn))
            key = max(lsn for lsn in self.polls if lsn <= start_lsn)
            return list(self.polls[key])

    @staticmethod
    def _tx(tx_id, begin, row, commit, op="INSERT"):
        return [
            {"op": "BEGIN", "tx_id": tx_id, "lsn": begin},
            {"op": op, "tx_id": tx_id, "lsn": begin + 1, "row": row},
            {"op": "COMMIT", "tx_id": tx_id, "lsn": commit},
        ]

    def test_triggered_read_goes_on_past_an_idle_poll_short_of_its_high_water(self):
        # The log reader went idle at 120 while the update's boundary is 150: not caught
        # up. The offset moves to 120 -- so AvailableNow sees progress -- and the next
        # poll resumes there, finds the change at 130, and only a poll idle at 150
        # reaches the boundary.
        bridge = self._PollBridge(
            {
                100: [{"op": "TIMEOUT", "lsn": 120}],
                120: self._tx(9, 125, {"id": 3, "value": "c"}, 130)
                + [{"op": "TIMEOUT", "lsn": 150}],
                150: [{"op": "TIMEOUT", "lsn": 150}],
            }
        )
        bridge.now = 150
        connector = self.connector(bridge)
        connector.prepare_for_trigger_available_now()
        start = _stream_offset(100)

        rows, first = connector.read_table("app.orders", start, {})
        self.assertEqual(list(rows), [])
        self.assertEqual(
            (first["commit_lsn"], first["begin_lsn"], first["tx_id"]), ("120", "120", None)
        )
        self.assertNotIn("trigger_generation", {k for k, v in first.items() if v is not None})

        rows, second = connector.read_table("app.orders", first, {})
        self.assertEqual([row["id"] for row in list(rows)], [3])
        self.assertEqual(bridge.change_reads[-1][1], 120)
        self.assertEqual(second["commit_lsn"], "150")  # idle at the boundary: moved there
        self.assertEqual(second["trigger_high_water"], "150")

        rows, third = connector.read_table("app.orders", second, {})
        self.assertEqual(list(rows), [])
        self.assertEqual(third, second)  # unchanged: AvailableNow is drained

    def test_an_available_now_update_lands_changes_behind_an_idle_poll(self):
        # Spark ends an AvailableNow update at the first read that returns its start
        # offset. Before the fix the idle poll at 120 was taken as caught up and the
        # update ended there, leaving the change at 130 for the next update.
        bridge = self._PollBridge(
            {
                100: [{"op": "TIMEOUT", "lsn": 120}],
                120: self._tx(9, 125, {"id": 3, "value": "c"}, 130)
                + [{"op": "TIMEOUT", "lsn": 150}],
                150: [{"op": "TIMEOUT", "lsn": 150}],
            }
        )
        bridge.now = 150
        connector = self.connector(bridge)
        connector.prepare_for_trigger_available_now()
        start, landed = _stream_offset(100), []
        for _ in range(10):
            rows, end = connector.read_table("app.orders", start, {})
            landed += [row["id"] for row in rows]
            if end == start:
                break
            start = end
        self.assertEqual(landed, [3])

    def test_an_open_transaction_keeps_an_idle_triggered_read_at_its_begin(self):
        open_tx = [
            {"op": "BEGIN", "tx_id": 9, "lsn": 110},
            {"op": "INSERT", "tx_id": 9, "lsn": 111, "row": {"id": 3, "value": "c"}},
            {"op": "TIMEOUT", "lsn": 120},
        ]
        bridge = self._PollBridge({100: open_tx})
        bridge.now = 150
        connector = self.connector(bridge)
        connector.prepare_for_trigger_available_now()
        start = _stream_offset(100)
        rows, end = connector.read_table("app.orders", start, {})
        self.assertEqual(list(rows), [])
        self.assertEqual((end["commit_lsn"], end["begin_lsn"]), ("100", "100"))

    def test_a_continuous_read_keeps_its_checkpoint_on_an_idle_poll(self):
        bridge = self._PollBridge({100: [{"op": "TIMEOUT", "lsn": 120}]})
        bridge.now = 150
        connector = self.connector(bridge)
        start = _stream_offset(100)
        rows, end = connector.read_table("app.orders", start, {})
        self.assertEqual(list(rows), [])
        self.assertEqual(end["commit_lsn"], "100")

    def test_a_replay_lands_on_a_quiet_end_once_the_log_reader_passes_it(self):
        bridge = self._PollBridge({100: [{"op": "TIMEOUT", "lsn": 140}]})
        bridge.now = 150
        connector = self.connector(bridge)
        start = _stream_offset(100)
        quiet_end = {**start, "commit_lsn": "120", "change_lsn": "120", "begin_lsn": "120"}
        rows = informix_module._replay_between_offsets(
            start,
            quiet_end,
            lambda offset: connector.read_table("app.orders", offset, options),
            options := {},
            connector.options,
        )
        self.assertEqual(list(rows), [])

    def test_a_replay_lands_on_a_quiet_end_with_later_commits_in_the_log(self):
        # Regression (cycle 4, tw241): a triggered read went idle with nothing open and
        # moved its offset to where the log reader stopped, 120, past its last commit,
        # 105; the update was cancelled before Spark committed that batch. By the replay
        # the log held a later commit, 130, so the poll crossed the replay's bound
        # instead of going idle -- and the replay ended at 105 on every attempt, short of
        # 120. Crossing the bound proves every transaction up to it was delivered.
        bridge = self._PollBridge(
            {
                100: self._tx(8, 101, {"id": 3, "value": "c"}, 105)
                + self._tx(9, 125, {"id": 4, "value": "d"}, 130)
                + [{"op": "TIMEOUT", "lsn": 150}],
            }
        )
        bridge.now = 150
        connector = self.connector(bridge)
        start = _stream_offset(100)
        quiet_end = {**start, "commit_lsn": "120", "change_lsn": "120", "begin_lsn": "120"}
        rows = informix_module._replay_between_offsets(
            start,
            quiet_end,
            lambda offset: connector.read_table("app.orders", offset, options),
            options := {},
            connector.options,
        )
        self.assertEqual([row["id"] for row in rows], [3])

    def test_a_replay_still_fails_short_of_its_end_while_a_transaction_is_open(self):
        # The original read went idle at 120 with nothing open, so a transaction open
        # with its BEGIN below 120 is one the replay cannot account for: fail closed.
        bridge = self._PollBridge(
            {
                100: [
                    {"op": "BEGIN", "tx_id": 8, "lsn": 110},
                    {"op": "INSERT", "tx_id": 8, "lsn": 111, "row": {"id": 3, "value": "c"}},
                ]
                + self._tx(9, 125, {"id": 4, "value": "d"}, 130)
                + [{"op": "TIMEOUT", "lsn": 150}],
            }
        )
        bridge.now = 150
        connector = self.connector(bridge)
        start = _stream_offset(100)
        quiet_end = {**start, "commit_lsn": "120", "change_lsn": "120", "begin_lsn": "120"}
        with self.assertRaisesRegex(InformixError, "did not reach its committed end LSN 120"):
            list(
                informix_module._replay_between_offsets(
                    start,
                    quiet_end,
                    lambda offset: connector.read_table("app.orders", offset, options),
                    options := {},
                    connector.options,
                )
            )

    def test_a_replay_fails_when_its_end_transaction_never_arrives(self):
        # The committed end is transaction 7's commit at 120, but the log the replay
        # reads has no such transaction: landing on 120 would drop its rows.
        bridge = self._PollBridge(
            {
                100: self._tx(8, 101, {"id": 3, "value": "c"}, 105)
                + self._tx(9, 125, {"id": 4, "value": "d"}, 130)
                + [{"op": "TIMEOUT", "lsn": 150}],
            }
        )
        bridge.now = 150
        connector = self.connector(bridge)
        start = _stream_offset(100)
        tx_end = {**start, "commit_lsn": "120", "change_lsn": "120", "begin_lsn": "115", "tx_id": 7}
        with self.assertRaisesRegex(InformixError, "did not reach its committed end transaction 7"):
            list(
                informix_module._replay_between_offsets(
                    start,
                    tx_end,
                    lambda offset: connector.read_table("app.orders", offset, options),
                    options := {},
                    connector.options,
                )
            )

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

    def _checkpoint_then_add_a_column(self, bridge, options):
        _, checkpoint = self.connector(bridge).read_table("app.orders", {}, options)
        bridge.tables[0]["columns"].append(
            {"name": "added", "type_name": "INTEGER", "nullable": True}
        )
        bridge.now = 120
        bridge.changes = [{"op": "TIMEOUT", "lsn": 120}]
        return checkpoint

    def test_delete_reader_fails_with_the_upsert_error_for_a_defaulted_key_column(self):
        """The upsert reader rejects a column joining a wildcard key with a default before
        it publishes the transition; the delete reader must fail the same way rather
        than wait for a transition that never comes (it would hang a triggered update)."""

        options = {"primary.keys": "*"}
        bridge = FakeBridge()
        bridge.tables = [_table(primary_keys=())]
        bridge.column_default_names = {"added"}
        checkpoint = self._checkpoint_then_add_a_column(bridge, options)

        with mock.patch.object(informix_module.time, "sleep"):
            with self.assertRaisesRegex(InformixError, "with a default.*run a full refresh"):
                self.connector(bridge).read_table("app.orders", checkpoint, options)
            with self.assertRaisesRegex(
                InformixError, "upsert reader refused .*with a default.*run a full refresh"
            ):
                rows, _ = self.connector(bridge).read_table_deletes(
                    "app.orders", checkpoint, options
                )
                list(rows)

    def test_delete_reader_fails_with_any_refusal_the_upsert_reader_records(self):
        bridge = FakeBridge()
        checkpoint = self._checkpoint_then_add_a_column(bridge, {})
        refused = InformixError("cdc_startcapture failed with Informix error -83713")

        with mock.patch.object(informix_module.time, "sleep"):
            with mock.patch.object(bridge, "validate_initial_lsn", side_effect=refused):
                with self.assertRaisesRegex(InformixError, "-83713"):
                    self.connector(bridge).read_table("app.orders", checkpoint, {})
            with self.assertRaisesRegex(
                InformixError, "upsert reader refused the schema change .*-83713"
            ):
                self.connector(bridge).read_table_deletes("app.orders", checkpoint, {})

    def test_a_refusal_stays_with_the_update_that_recorded_it(self):
        bridge = FakeBridge()
        checkpoint = self._checkpoint_then_add_a_column(bridge, {})
        refused = InformixError("cdc_startcapture failed with Informix error -83713")
        with mock.patch.object(informix_module.time, "sleep"):
            with mock.patch.object(bridge, "validate_initial_lsn", side_effect=refused):
                with self.assertRaises(InformixError):
                    self.connector(bridge).read_table("app.orders", checkpoint, {})
            next_update = self.connector(bridge)
            next_update.set_registration_scope(hashlib.sha256(b"next-update").hexdigest()[:32])

            _, yielded = next_update.read_table_deletes("app.orders", checkpoint, {})

        self.assertEqual(yielded["schema_transition_retry_count"], 1)

    def test_a_published_transition_wins_over_an_earlier_refusal(self):
        # An upsert retry in the same update got past what refused it the first time.
        bridge = FakeBridge()
        checkpoint = self._checkpoint_then_add_a_column(bridge, {})
        refused = InformixError("cdc_startcapture failed with Informix error -83713")
        with mock.patch.object(informix_module.time, "sleep"):
            with mock.patch.object(bridge, "validate_initial_lsn", side_effect=refused):
                with self.assertRaises(InformixError):
                    self.connector(bridge).read_table("app.orders", checkpoint, {})
            self.connector(bridge).read_table("app.orders", checkpoint, {})

            rows, resumed = self.connector(bridge).read_table_deletes("app.orders", checkpoint, {})

        self.assertEqual(list(rows), [])
        self.assertNotIn("schema_transition_retry_count", resumed)

    def test_a_transient_upsert_failure_records_no_refusal(self):
        bridge = FakeBridge()
        checkpoint = self._checkpoint_then_add_a_column(bridge, {})
        with mock.patch.object(informix_module.time, "sleep"):
            with mock.patch.object(
                bridge, "validate_initial_lsn", side_effect=TimeoutError("read timed out")
            ):
                with self.assertRaises(TimeoutError):
                    self.connector(bridge).read_table("app.orders", checkpoint, {})

            _, yielded = self.connector(bridge).read_table_deletes("app.orders", checkpoint, {})

        self.assertEqual(yielded["schema_transition_retry_count"], 1)

    def test_triggered_delete_reader_stops_waiting_for_a_transition_that_never_comes(self):
        """A failed upsert flow does not end a triggered update, so the delete reader's
        wait for its transition is bounded by cdc.shared.state.wait.seconds."""

        bridge = FakeBridge()
        checkpoint = self._checkpoint_then_add_a_column(bridge, {})
        delete = self.connector(bridge)
        delete.prepare_for_trigger_available_now()

        with mock.patch.object(informix_module.time, "sleep"):
            _, yielded = delete.read_table_deletes("app.orders", checkpoint, {})
            self.assertEqual(yielded["schema_transition_retry_count"], 1)
            # The upsert reader never publishes; let the wait run out.
            for key in delete._schema_transition_waits:
                delete._schema_transition_waits[key] -= informix_module._SHARED_STATE_WAIT_SECONDS
            with self.assertRaisesRegex(
                InformixError, "has not published the schema transition .* in 300 seconds"
            ):
                delete.read_table_deletes("app.orders", yielded, {})

    def test_continuous_delete_reader_keeps_yielding_for_a_transition(self):
        """A continuous update fails as a whole once a flow fails, so there the delete
        reader keeps yielding with no time bound."""

        bridge = FakeBridge()
        checkpoint = self._checkpoint_then_add_a_column(bridge, {})
        delete = self.connector(bridge)

        with mock.patch.object(informix_module.time, "sleep"):
            _, first = delete.read_table_deletes("app.orders", checkpoint, {})
            _, second = delete.read_table_deletes("app.orders", first, {})

        self.assertEqual(delete._schema_transition_waits, {})
        self.assertEqual(second["schema_transition_retry_count"], 2)

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
                # Read directly and drain inline (see LakeflowContractTests.connector):
                # these tests inject a bridge directly and must not start a daemon reader.
                "stream.partitioned": "false",
                **options,
            }
        )
        connector.set_registration_scope(hashlib.sha256(scope_label.encode()).hexdigest()[:32])
        connector._bridge_instance = bridge or FakeBridge()
        return _drain_snapshots_inline(_read_cdc_directly(connector))

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

    def test_incremental_mixed_direction_key_pages_in_index_order(self):
        # Key (id ASC, seq DESC). The whole incremental copy -- max_pk, the paged
        # reads, and the cursor/bound comparisons -- must run in the index's own
        # order, not Python's all-ascending tuple order, or rows are emitted out
        # of order and the copy stops at the wrong bound.
        bridge = FakeBridge()
        bridge.tables = [
            {
                "database": "demo",
                "owner": "app",
                "name": "orders",
                "columns": [
                    {"name": "id", "type_name": "INTEGER", "nullable": False},
                    {"name": "seq", "type_name": "INTEGER", "nullable": False},
                    {"name": "value", "type_name": "VARCHAR", "length": 20},
                ],
                "primary_keys": ["id", "seq"],
                "key_descending": [False, True],
            }
        ]
        bridge.rows = [
            {"id": 1, "seq": 2, "value": "a"},
            {"id": 1, "seq": 1, "value": "b"},
            {"id": 2, "seq": 5, "value": "c"},
            {"id": 2, "seq": 3, "value": "d"},
        ]
        connector = self.connector(bridge, **{"snapshot.page.size": "1"})

        rows, offset = self._drain(connector, {"snapshot.page.size": "1"})

        # id ascending, seq descending within each id: a, b, c, d.
        self.assertEqual([row["value"] for row in rows], ["a", "b", "c", "d"])
        self.assertEqual({row["id"] for row in rows}, {1, 2})
        self.assertNotIn("incremental", offset)  # converged at the index-order max (2, 3)

    def test_incremental_resume_under_changed_key_order_fails_closed(self):
        # A checkpoint seeded under the old all-ascending order must not silently
        # resume once the key index is discovered as mixed-direction: the cursor is
        # not comparable across the change, so the copy has to restart.
        bridge = FakeBridge()
        bridge.tables = [
            {
                "database": "demo",
                "owner": "app",
                "name": "orders",
                "columns": [
                    {"name": "id", "type_name": "INTEGER", "nullable": False},
                    {"name": "seq", "type_name": "INTEGER", "nullable": False},
                    {"name": "value", "type_name": "VARCHAR", "length": 20},
                ],
                "primary_keys": ["id", "seq"],
                "key_descending": [False, True],  # table is now mixed-direction
            }
        ]
        bridge.rows = [{"id": 1, "seq": 1, "value": "a"}]
        connector = self.connector(bridge, **{"snapshot.page.size": "1"})

        # A block whose recorded ordering predates the mixed-direction discovery.
        stale = {
            "started": True,
            "last_pk": informix_module._encode_snapshot_stage_value([1, 1]),
            "max_pk": informix_module._encode_snapshot_stage_value([9, 9]),
            "done": False,
            "chunk_exprs": {},
            "snapshot_filter": None,
        }
        with self.assertRaisesRegex(informix_module.InformixError, "full refresh"):
            connector._next_incremental_chunk(
                informix_module.Table.parse(bridge.tables[0], "demo"),
                stale,
                {"snapshot.page.size": "1"},
            )

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
        # materializable, and as the key the column can't be left out, so it fails
        # discovery well before the chunk gate is consulted -- it never reaches the
        # string-cast fallback path.
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


class SnapshotReconnectTests(unittest.TestCase):
    """A connection dropped mid read during an incremental snapshot is reset and
    the identical read reissued in place, mirroring the CDC poll's reconnect
    path, rather than surfacing as a stream failure.

    Regression: production tw305 crash-looped on ``SqliTransportError: truncated
    SQLI stream: needed 2, got 0`` mid-chunk because the snapshot page and
    key-bound reads had no reconnect wrapper -- only the CDC poll did.
    """

    def setUp(self):
        self._shared_state = tempfile.TemporaryDirectory()
        self._lakebase = _OfflineLakebase().install(self)

    def tearDown(self):
        self._shared_state.cleanup()

    def connector(self, bridge=None, **options):
        connector = InformixLakeflowConnect(
            {
                "database": "demo",
                "snapshot.staging.location": self._shared_state.name,
                "lakebase.password": "test-state-password",
                # Inject the bridge directly; keep the daemon readers off.
                "stream.partitioned": "false",
                **options,
            }
        )
        connector.set_registration_scope(hashlib.sha256(b"reconnect-pipeline").hexdigest()[:32])
        connector._bridge_instance = bridge or FakeBridge()
        return _drain_snapshots_inline(_read_cdc_directly(connector))

    def _drain(self, connector, options):
        """Read microbatches until the incremental block clears; return all rows."""

        offset: dict = {}
        collected = []
        for _ in range(100):
            rows, offset = connector.read_table("app.orders", offset, options)
            collected.extend(rows)
            if "incremental" not in offset:
                break
        else:
            self.fail("incremental snapshot did not converge")
        return collected, offset

    def _counting_reset(self, bridge):
        bridge.reset_calls = 0
        bridge.reset_transport = lambda: setattr(bridge, "reset_calls", bridge.reset_calls + 1)

    def test_incremental_chunk_retries_in_place_after_connection_drop(self):
        bridge = FakeBridge()
        bridge.rows = [{"id": i, "value": str(i)} for i in range(1, 6)]
        self._counting_reset(bridge)
        real_chunk = bridge.snapshot_chunk
        drops = {"n": 0}

        def flaky_chunk(*args, **kwargs):
            # Drop the first two chunk reads, then serve the real page. The
            # cursor has not advanced, so the reissued read is identical.
            if drops["n"] < 2:
                drops["n"] += 1
                raise SqliTransportError("truncated SQLI stream: needed 2, got 0")
            return real_chunk(*args, **kwargs)

        bridge.snapshot_chunk = flaky_chunk
        connector = self.connector(bridge)

        with mock.patch.object(time, "sleep"):
            rows, offset = self._drain(connector, {"snapshot.page.size": "2"})

        self.assertEqual(drops["n"], 2)  # two drops absorbed in place
        self.assertEqual(bridge.reset_calls, 2)  # one transport reset per drop
        self.assertEqual([row["id"] for row in rows], [1, 2, 3, 4, 5])
        self.assertNotIn("incremental", offset, "the copy must still converge")

    def test_incremental_chunk_raises_after_reconnect_retries_exhausted(self):
        # A persistent drop must fail honestly rather than loop forever.
        bridge = FakeBridge()
        bridge.rows = [{"id": i, "value": str(i)} for i in range(1, 6)]
        self._counting_reset(bridge)

        def always_drops(*args, **kwargs):
            raise SqliTransportError("truncated SQLI stream: needed 2, got 0")

        bridge.snapshot_chunk = always_drops
        connector = self.connector(bridge)

        with mock.patch.object(time, "sleep"):
            with self.assertRaisesRegex(SqliTransportError, "truncated SQLI stream"):
                self._drain(connector, {"snapshot.page.size": "2"})
        # One reset per failed retry, bounded by the reconnect budget.
        self.assertEqual(bridge.reset_calls, informix_module._SNAPSHOT_RECONNECT_MAX_RETRIES)

    def test_key_bound_read_retries_in_place_after_connection_drop(self):
        # The seed's max_primary_key read (where the earlier tw305 timeout also
        # surfaced) recovers from a drop the same way.
        bridge = FakeBridge()
        bridge.rows = [{"id": i, "value": str(i)} for i in range(1, 4)]
        self._counting_reset(bridge)
        real_max = bridge.max_primary_key
        drops = {"n": 0}

        def flaky_max(*args, **kwargs):
            if drops["n"] < 1:
                drops["n"] += 1
                raise SqliTransportError("truncated SQLI stream: needed 2, got 0")
            return real_max(*args, **kwargs)

        bridge.max_primary_key = flaky_max
        connector = self.connector(bridge)

        with mock.patch.object(time, "sleep"):
            rows, offset = self._drain(connector, {"snapshot.page.size": "2"})

        self.assertEqual(drops["n"], 1)
        self.assertEqual(bridge.reset_calls, 1)
        self.assertEqual([row["id"] for row in rows], [1, 2, 3])
        self.assertNotIn("incremental", offset)

    def test_connection_drop_during_lease_loss_is_not_retried(self):
        # A deliberate lease-loss shutdown is not a blip: it must propagate at
        # once, never reset-and-retry, exactly as the CDC path treats it.
        bridge = FakeBridge()
        bridge.rows = [{"id": 1, "value": "a"}]
        bridge._connection_lease_lost = threading.Event()
        bridge._connection_lease_lost.set()
        self._counting_reset(bridge)

        def dropping_chunk(*args, **kwargs):
            raise SqliTransportError("truncated SQLI stream: needed 2, got 0")

        bridge.snapshot_chunk = dropping_chunk
        connector = self.connector(bridge)

        with mock.patch.object(time, "sleep"):
            with self.assertRaises(SqliTransportError):
                self._drain(connector, {"snapshot.page.size": "2"})
        self.assertEqual(bridge.reset_calls, 0, "a lease-loss drop must not reset/retry")


class LakebaseStateReconnectTests(unittest.TestCase):
    """A Lakebase (Postgres) state connection reaped while idle is dropped and the
    identical, idempotent state operation reissued on a fresh connection, rather
    than surfacing as a stream failure.

    Regression: production tw305 crash-looped on
    ``psycopg2.OperationalError: SSL SYSCALL error: EOF detected`` from
    ``publish_minimum_lsn_state_record`` -- the connector held its state
    connection idle across a long wait for capacity (``backlog_streak`` 4), the
    server reaped it, psycopg left ``.closed`` False so ``_lakebase_connection``
    handed the dead handle back, and the state path had no reconnect wrapper
    (only the Informix SQLI reads did).
    """

    class _FakeConnLost(Exception):
        """Stands in for psycopg's OperationalError/InterfaceError."""

    def setUp(self):
        self._shared_state = tempfile.TemporaryDirectory()
        self._lakebase = _OfflineLakebase().install(self)

    def tearDown(self):
        self._shared_state.cleanup()

    def _connector(self):
        connector = InformixLakeflowConnect(
            {
                "database": "demo",
                "snapshot.staging.location": self._shared_state.name,
                "lakebase.password": "test-state-password",
                "stream.partitioned": "false",
            }
        )
        connector.set_registration_scope(hashlib.sha256(b"lakebase-reconnect").hexdigest()[:32])
        return _drain_snapshots_inline(_read_cdc_directly(connector))

    def _instrument(self, connector, *, closed_after_drop=True):
        """Wire a counting reset that mimics the real one (drops the handle)."""

        resets = {"n": 0}

        def fake_reset():
            resets["n"] += 1
            connector._lakebase_conn = None

        connector._reset_lakebase_connection = fake_reset
        # The handle the loop inspects after a drop: closed => a genuine reap that
        # is safe to reconnect; open => a live-connection error left as-is.
        connector._lakebase_conn = types.SimpleNamespace(closed=closed_after_drop)
        return resets

    def test_state_op_reconnects_and_retries_after_dropped_connection(self):
        connector = self._connector()
        resets = self._instrument(connector)
        drops = {"n": 0}

        def flaky():
            if drops["n"] < 2:
                drops["n"] += 1
                # A drop leaves the cached handle closed, as psycopg does once a
                # query fails on a reaped socket.
                connector._lakebase_conn = types.SimpleNamespace(closed=True)
                raise self._FakeConnLost("SSL SYSCALL error: EOF detected")
            return "elected"

        with (
            mock.patch.object(
                informix_module,
                "connection_lost_error_types",
                return_value=(self._FakeConnLost,),
            ),
            mock.patch.object(time, "sleep"),
        ):
            result = connector._state_op_with_reconnect(flaky, operation="unit publish")

        self.assertEqual(result, "elected")
        self.assertEqual(drops["n"], 2)  # two reaps absorbed in place
        self.assertEqual(resets["n"], 2)  # one reconnect per reap

    def test_state_op_does_not_retry_a_live_connection_error(self):
        # A failure on a still-open connection (e.g. a serialization conflict,
        # also an OperationalError subclass) is not a drop: propagate at once,
        # never reconnect.
        connector = self._connector()
        resets = self._instrument(connector, closed_after_drop=False)

        def live_error():
            connector._lakebase_conn = types.SimpleNamespace(closed=False)
            raise self._FakeConnLost("could not serialize access")

        with (
            mock.patch.object(
                informix_module,
                "connection_lost_error_types",
                return_value=(self._FakeConnLost,),
            ),
            mock.patch.object(time, "sleep"),
        ):
            with self.assertRaisesRegex(self._FakeConnLost, "serialize"):
                connector._state_op_with_reconnect(live_error, operation="unit publish")
        self.assertEqual(resets["n"], 0, "a live-connection error must not reconnect")

    def test_state_op_raises_after_reconnect_retries_exhausted(self):
        connector = self._connector()
        resets = self._instrument(connector)

        def always_drops():
            connector._lakebase_conn = types.SimpleNamespace(closed=True)
            raise self._FakeConnLost("server closed the connection unexpectedly")

        with (
            mock.patch.object(
                informix_module,
                "connection_lost_error_types",
                return_value=(self._FakeConnLost,),
            ),
            mock.patch.object(time, "sleep"),
        ):
            with self.assertRaisesRegex(self._FakeConnLost, "server closed"):
                connector._state_op_with_reconnect(always_drops, operation="unit publish")
        self.assertEqual(resets["n"], informix_module._LAKEBASE_RECONNECT_MAX_RETRIES)

    def test_state_op_passes_through_when_driver_reports_no_error_types(self):
        # Neither psycopg2 nor psycopg importable => no types to catch => the
        # operation runs once and any error propagates unwrapped.
        connector = self._connector()
        resets = self._instrument(connector)

        with mock.patch.object(informix_module, "connection_lost_error_types", return_value=()):
            self.assertEqual(
                connector._state_op_with_reconnect(lambda: "ok", operation="unit"),
                "ok",
            )
            with self.assertRaises(self._FakeConnLost):
                connector._state_op_with_reconnect(self._raise_conn_lost, operation="unit")
        self.assertEqual(resets["n"], 0)

    def _raise_conn_lost(self):
        raise self._FakeConnLost("boom")

    def test_connection_lost_error_types_are_resolved_from_the_driver(self):
        from databricks.labs.community_connector.sources.informix import lakebase_state

        lakebase_state._CONNECTION_LOST_ERROR_TYPES.clear()
        try:
            types_tuple = lakebase_state.connection_lost_error_types()
        finally:
            lakebase_state._CONNECTION_LOST_ERROR_TYPES.clear()
        self.assertIsInstance(types_tuple, tuple)
        for error_type in types_tuple:
            self.assertTrue(issubclass(error_type, BaseException))
        try:
            import psycopg2  # noqa: PLC0415
        except ImportError:
            self.skipTest("psycopg2 not installed in this environment")
        self.assertIn(psycopg2.OperationalError, types_tuple)
        self.assertIn(psycopg2.InterfaceError, types_tuple)


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

    def test_release_forgets_the_slot(self):
        # release_connection publishes a backlog hint only while a slot is held, so a
        # released bridge must not still look like it holds one.
        bridge = self._bridge()
        bridge._connection_slot_heartbeat_stop = None
        bridge._connection_slot_heartbeat = None
        bridge._lakebase_slot = object()
        bridge._lakebase_connection = lambda: mock.Mock()

        with mock.patch.object(informix_module, "release_slot", return_value=True):
            bridge._release_connection_slot()

        self.assertIsNone(bridge._connection_slot)

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
        caught_up = shard.ingest(self._two_table_stream(), self._schema_map(), 1, 106, 0)
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
        shard.ingest(stream, self._schema_map(), 1, 106, 0)
        shard.ingest(stream, self._schema_map(), 1, 106, 0)  # daemon re-reads from the floor
        self.assertEqual(len(shard.buffers[self.id_a]), 1)
        self.assertEqual(len(shard.buffers[self.id_b]), 1)

    def test_snapshot_peeks_without_removing_until_checkpoint_advances(self):
        shard = self._shard_with_two_tables()
        shard.ingest(self._two_table_stream(), self._schema_map(), 1, 106, 0)
        self.assertEqual([tx.tx_id for tx in shard.snapshot(self.id_a, 0).committed], [1])
        # A second poll at the same checkpoint still sees the un-consumed transaction.
        self.assertEqual([tx.tx_id for tx in shard.snapshot(self.id_a, 0).committed], [1])
        # Once the checkpoint passes its commit LSN it is trimmed.
        self.assertEqual(shard.snapshot(self.id_a, 102).committed, [])

    def test_late_subscriber_receives_history_without_duplicating_others(self):
        shard = _SharedCdcShard()
        shard.subscribe(self.id_a, self.table_a, _capture_descriptor(self.table_a), 0, 0)
        stream = self._two_table_stream()
        shard.ingest(stream, {self.id_a: self.table_a}, 1, 106, 0)
        # B subscribes late; the daemon re-reads the log and B must get its history
        # while A is not re-delivered.
        shard.subscribe(self.id_b, self.table_b, _capture_descriptor(self.table_b), 0, 0)
        shard.ingest(stream, self._schema_map(), 1, 106, 0)
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
        caught_up = shard.ingest(stream, self._schema_map(), 1, 201, 0)
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

    def _tx(self, tx_id, begin, table, commit):
        return [
            {"op": "BEGIN", "tx_id": tx_id, "lsn": begin},
            {
                "op": "INSERT",
                "tx_id": tx_id,
                "lsn": begin + 1,
                "table": table.identity,
                "row": {"id": tx_id, "value": "x"},
            },
            {"op": "COMMIT", "tx_id": tx_id, "lsn": commit},
        ]

    def test_ingest_keeps_the_floor_of_a_subscriber_that_joined_mid_read(self):
        # Regression: A is subscribed at floor 800 and the daemon activates a read at
        # 800 capturing only A. While that read is in flight B subscribes with floor
        # 600 (its checkpoint), which lowers shared_restart to 600. ingest() used to
        # overwrite shared_restart with the read's own end (900), so the daemon's next
        # activation skipped B's log in (600, 900) forever -- and B's first view then
        # advanced its checkpoint straight past that gap.
        shard = _SharedCdcShard()
        shard.subscribe(self.id_a, self.table_a, _capture_descriptor(self.table_a), 0, 800)
        read_start = shard.shared_restart  # what the in-flight read was activated at
        shard.subscribe(self.id_b, self.table_b, _capture_descriptor(self.table_b), 600, 600)
        self.assertEqual(shard.shared_restart, 600)

        raw = self._tx(1, 810, self.table_a, 850) + self._tx(2, 860, self.table_a, 900)
        raw.append({"op": "TIMEOUT", "lsn": 901})
        shard.ingest(raw, {self.id_a: self.table_a}, 1, 901, read_start)

        # The floor B lowered survives the read that did not cover B.
        self.assertEqual(shard.shared_restart, 600)
        # A was covered by a read activated at its floor: its view is authoritative.
        self.assertEqual([tx.tx_id for tx in shard.snapshot(self.id_a, 0).committed], [1, 2])
        # B is not covered yet, so no view: the consumer stays on direct reads instead
        # of trusting a buffer that never saw (600, 900).
        self.assertIsNone(shard.snapshot(self.id_b, 600))

        # The next daemon read activates at the clamped floor and captures both tables;
        # B's history is delivered and only then does B get a view.
        raw = self._tx(3, 610, self.table_b, 700) + self._tx(1, 810, self.table_a, 850)
        raw += self._tx(2, 860, self.table_a, 900) + [{"op": "TIMEOUT", "lsn": 901}]
        shard.ingest(raw, self._schema_map(), 1, 901, shard.shared_restart)
        self.assertEqual([tx.tx_id for tx in shard.snapshot(self.id_b, 600).committed], [3])
        # A's already-delivered transactions were not re-delivered.
        self.assertEqual([tx.tx_id for tx in shard.snapshot(self.id_a, 0).committed], [1, 2])
        # Idle with nothing open, so the next read resumes where the log reader stopped.
        self.assertEqual(shard.shared_restart, 901)

    def test_an_idle_read_resumes_where_the_log_reader_stopped(self):
        shard = _SharedCdcShard()
        shard.subscribe(self.id_a, self.table_a, _capture_descriptor(self.table_a), 500, 500)
        shard.ingest([{"op": "TIMEOUT", "lsn": 700}], {self.id_a: self.table_a}, 1, 900, 500)
        # Nothing open: the next read starts at 700 rather than rescanning from 500, and
        # consumers learn the log reader is only at 700 of the current 900.
        self.assertEqual(shard.shared_restart, 700)
        self.assertEqual(shard.snapshot(self.id_a, 500).reached_lsn, 700)

        open_tx = [
            {"op": "BEGIN", "tx_id": 4, "lsn": 710, "table": self.table_a.identity},
            {"op": "TIMEOUT", "lsn": 800},
        ]
        shard.ingest(open_tx, {self.id_a: self.table_a}, 1, 900, 700)
        self.assertEqual(shard.shared_restart, 710)  # back to the open BEGIN
        self.assertIsNone(shard.snapshot(self.id_a, 500).reached_lsn)

    def test_snapshot_is_none_until_a_read_activated_at_or_below_the_floor_covers_it(self):
        shard = _SharedCdcShard()
        shard.subscribe(self.id_a, self.table_a, _capture_descriptor(self.table_a), 500, 500)
        # A read that captured A but was activated above A's floor does not cover it.
        shard.ingest([{"op": "TIMEOUT", "lsn": 700}], {self.id_a: self.table_a}, 1, 700, 650)
        self.assertIsNone(shard.snapshot(self.id_a, 500))
        self.assertEqual(shard.shared_restart, 500)  # clamped back to the uncovered floor
        shard.ingest([{"op": "TIMEOUT", "lsn": 701}], {self.id_a: self.table_a}, 1, 701, 500)
        self.assertIsNotNone(shard.snapshot(self.id_a, 500))

    def test_a_ttl_evicted_subscriber_must_be_covered_again(self):
        shard = _SharedCdcShard()
        shard.subscriber_ttl = 0.0
        shard.subscribe(self.id_a, self.table_a, _capture_descriptor(self.table_a), 0, 0)
        shard.ingest([{"op": "TIMEOUT", "lsn": 10}], {self.id_a: self.table_a}, 1, 10, 0)
        self.assertIsNotNone(shard.snapshot(self.id_a, 0))
        with shard.condition:
            shard.live_subscribers()  # TTL 0 -> evicted, coverage forgotten
        shard.subscribe(self.id_a, self.table_a, _capture_descriptor(self.table_a), 0, 0)
        shard.ingest([{"op": "TIMEOUT", "lsn": 11}], {self.id_a: self.table_a}, 1, 11, 5)
        self.assertIsNone(shard.snapshot(self.id_a, 0))  # read at 5 did not cover floor 0
        self.assertEqual(shard.shared_restart, 0)

    def test_a_table_whose_options_no_longer_fit_is_left_to_direct_reads(self):
        # One table's stale rename must not take every table in the shard off shared reads.
        class Bridge:
            def get_table(self, identity):
                return _wide_table()

        fit = Table.parse(_wide_table(), "demo")
        unfit = dataclasses.replace(fit, column_renames=(("gone", "label"),))
        with self.assertLogs(informix_module.__name__, level="WARNING") as logs:
            schema_map, dropped = informix_module._shard_schema_map(
                Bridge(), [("app.a", unfit), ("app.b", fit)]
            )
        self.assertEqual((list(schema_map), dropped), (["app.b"], ["app.a"]))
        self.assertIn("app.a", logs.output[0])
        shard = _SharedCdcShard()
        shard.subscribe("app.a", unfit, {"capture": 1}, 0, 10)
        shard.subscribe("app.b", fit, {"capture": 2}, 0, 20)
        shard.evict("app.a")
        self.assertEqual(list(shard.subscribers), ["app.b"])
        self.assertNotIn("app.a", shard.buffers)


class _CatalogTransport:
    """Answers the bridge's catalog queries from per-table catalog facts, as Informix would:
    ``tables`` maps (owner, name) to {tabid, version, columns: [(name, coltype, colno)],
    pk: [colno, ...], unique: [(idxname, [colno, ...])]}."""

    def __init__(self, tables):
        self.tables = tables
        self.statements = []

    def _matching(self, sql, parameters):
        values = list(parameters)
        if "t.owner = ?" in sql or "t.owner=?" in sql:
            return [(key, facts) for key, facts in self.tables.items() if key == tuple(values)]
        return [(key, facts) for key, facts in self.tables.items() if key[1] in values]

    @staticmethod
    def _parts(colnos):
        return {
            f"part{index + 1}": (colnos[index] if index < len(colnos) else 0) for index in range(16)
        }

    def execute(self, sql, parameters=(), max_result_bytes=None):
        self.statements.append(sql)
        if "SELECT tabid, owner, tabname, version" in sql:
            return [
                {
                    "tabid": facts["tabid"],
                    "owner": owner + "  ",
                    "tabname": name,
                    "version": facts["version"],
                }
                for (owner, name), facts in self.tables.items()
                if facts["tabid"] in parameters
            ]
        rows = []
        for (owner, name), facts in self._matching(sql, parameters):
            where = {"owner": owner, "tabname": name}
            if "syscolumns" in sql:
                # Without an ORDER BY the rows come in no particular order.
                columns = facts["columns"] if "ORDER BY" in sql else reversed(facts["columns"])
                rows += [
                    {
                        "colname": column,
                        "coltype": coltype,
                        "collength": 4,
                        "colno": colno,
                        "tabid": facts["tabid"],
                        "extended_id": 0,
                        "extended_name": None,
                        "extended_owner": None,
                        **where,
                    }
                    for column, coltype, colno in columns
                ]
            elif "constrtype" in sql and facts.get("pk"):
                rows.append({**self._parts(facts["pk"]), **where})
            elif "idxtype='U'" in sql:
                rows += [
                    {"idxname": index, **self._parts(colnos), **where}
                    for index, colnos in facts.get("unique", [])
                ]
        return rows


class _CatalogBridge:
    """A shard bridge with batched catalog reads that records each one. ``versions`` holds
    systables.version per table ID; ``tables`` the described tables by owner.name."""

    def __init__(self, *tables):
        self.tables = {f"{t['owner']}.{t['name']}": t for t in tables}
        self.versions = {int(t["incarnation"]): 1 for t in tables}
        self.calls = []

    def table_versions(self, tabids):
        self.calls.append(("versions", tuple(sorted(tabids))))
        return {
            int(t["incarnation"]): (t["owner"], t["name"], self.versions[int(t["incarnation"])])
            for t in self.tables.values()
            if int(t["incarnation"]) in tabids
        }

    def get_tables(self, identities):
        self.calls.append(("describe", tuple(identities)))
        return {
            identity: json.loads(json.dumps(self.tables[identity.split(".", 1)[1]]))
            for identity in identities
            if identity.split(".", 1)[1] in self.tables
        }


def _catalog_table(name, tabid):
    return {**_table(name=name), "incarnation": str(tabid)}


class ShardCatalogTests(unittest.TestCase):
    """A shard's daemon re-reads its tables' catalog only when systables.version says one
    changed, checked on a timer, and reads the changed tables in one batch."""

    def setUp(self):
        self.bridge = _CatalogBridge(_catalog_table("a", 11), _catalog_table("b", 12))
        self.subscribed = {
            name: Table.parse(_catalog_table(name, tabid), "demo")
            for name, tabid in (("a", 11), ("b", 12), ("c", 13))
        }
        self.catalog = informix_module._ShardCatalog()

    def tables(self, now, *names, check_seconds=30):
        names = names or ("a", "b")
        return self.catalog.tables(
            self.bridge, [(name, self.subscribed[name]) for name in names], now, check_seconds
        )

    def test_the_first_read_describes_every_table_in_one_batch(self):
        tables = self.tables(0)

        self.assertEqual(sorted(tables), ["a", "b"])
        self.assertEqual(
            self.bridge.calls,
            [("versions", (11, 12)), ("describe", ("demo.app.a", "demo.app.b"))],
        )

    def test_a_read_before_the_next_check_touches_no_catalog(self):
        first = self.tables(0)
        self.bridge.calls.clear()

        again = self.tables(29)

        self.assertEqual(self.bridge.calls, [])
        self.assertIs(again["a"], first["a"])

    def test_a_check_with_unchanged_versions_reads_nothing_else(self):
        self.tables(0)
        self.bridge.calls.clear()

        self.tables(30)

        self.assertEqual(self.bridge.calls, [("versions", (11, 12))])

    def test_a_check_re_reads_only_the_tables_whose_version_moved(self):
        self.tables(0)
        self.bridge.calls.clear()
        self.bridge.tables["app.b"]["columns"].append({"name": "added", "type_name": "INTEGER"})
        self.bridge.versions[12] = 2

        tables = self.tables(30)

        self.assertEqual(self.bridge.calls, [("versions", (11, 12)), ("describe", ("demo.app.b",))])
        self.assertEqual([column.name for column in tables["b"].columns][-1], "added")
        self.bridge.calls.clear()
        self.tables(60)
        self.assertEqual(self.bridge.calls, [("versions", (11, 12))])

    def test_a_new_subscriber_is_read_at_once_on_its_own(self):
        self.bridge.tables["app.c"] = _catalog_table("c", 13)
        self.bridge.versions[13] = 1
        self.tables(0)
        self.bridge.calls.clear()

        tables = self.tables(5, "a", "b", "c")

        self.assertEqual(sorted(tables), ["a", "b", "c"])
        self.assertEqual(self.bridge.calls, [("versions", (13,)), ("describe", ("demo.app.c",))])

    def test_a_zero_interval_checks_before_every_read(self):
        self.tables(0, check_seconds=0)
        self.bridge.calls.clear()

        self.tables(0, check_seconds=0)

        self.assertEqual(self.bridge.calls, [("versions", (11, 12))])

    def test_a_forced_check_runs_on_the_next_read(self):
        self.tables(0)
        self.bridge.calls.clear()

        self.catalog.force_check()
        self.tables(1)

        self.assertEqual(self.bridge.calls, [("versions", (11, 12))])

    def test_a_table_no_longer_in_the_catalog_is_left_to_direct_reads(self):
        self.tables(0)
        del self.bridge.tables["app.b"]
        del self.bridge.versions[12]

        with self.assertLogs(informix_module.__name__, level="WARNING") as logs:
            schema_map, unfit = informix_module._shard_schema_map(
                self.bridge,
                [(name, self.subscribed[name]) for name in ("a", "b")],
                self.catalog,
                30,
            )

        self.assertEqual((sorted(schema_map), unfit), (["a"], ["b"]))
        self.assertIn("no longer has it", logs.output[0])

    def test_a_renamed_table_is_read_again_under_its_name(self):
        # The table ID stays, but systables names another table: the name is read again.
        self.tables(0)
        self.bridge.calls.clear()
        self.bridge.tables["app.b"]["name"] = "renamed"

        self.tables(30)

        self.assertEqual(self.bridge.calls, [("versions", (11, 12)), ("describe", ("demo.app.b",))])

    def test_a_re_created_table_is_read_again_with_its_new_version(self):
        self.tables(0)
        self.bridge.calls.clear()
        self.bridge.tables["app.b"]["incarnation"] = "40"
        del self.bridge.versions[12]
        self.bridge.versions[40] = 1

        self.assertEqual(self.tables(30)["b"].incarnation, "40")
        # Its new table ID's version wasn't asked for, so the next check reads it again.
        self.tables(60)
        self.tables(90)

        self.assertEqual(
            self.bridge.calls,
            [
                ("versions", (11, 12)),
                ("describe", ("demo.app.b",)),
                ("versions", (11, 40)),
                ("describe", ("demo.app.b",)),
                ("versions", (11, 40)),
            ],
        )

    def test_a_bridge_without_batched_reads_re_reads_every_table_at_each_check(self):
        class Bridge:
            def __init__(self, tables):
                self.tables = tables
                self.read = []

            def get_table(self, identity):
                self.read.append(identity)
                return self.tables[identity.split(".", 1)[1]]

        self.bridge = Bridge(
            _CatalogBridge(_catalog_table("a", 11), _catalog_table("b", 12)).tables
        )
        self.tables(0)
        self.tables(10)
        self.tables(30)

        self.assertEqual(self.bridge.read, ["demo.app.a", "demo.app.b"] * 2)


class BatchedCatalogReadTests(unittest.TestCase):
    """table_versions and get_tables read many tables per catalog query, and get_tables
    describes each one exactly as get_table does."""

    TABLES = {
        ("app", "keyed"): {
            "tabid": 101,
            "version": 7,
            "columns": [("id", 2 + 256, 1), ("note", 13, 2)],
            "pk": [1],
        },
        ("app", "indexed"): {
            "tabid": 102,
            "version": 9,
            "columns": [("code", 2 + 256, 1), ("name", 13, 2)],
            "unique": [("ux_code", [1])],
        },
        ("other", "keyed"): {"tabid": 103, "version": 1, "columns": [("x", 2, 1)]},
    }

    def bridge(self):
        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.config = {"database": "demo"}
        bridge.transport = _CatalogTransport(self.TABLES)
        return bridge

    def test_get_tables_describes_each_table_as_get_table_does(self):
        bridge = self.bridge()
        identities = ["demo.app.keyed", "demo.app.indexed"]

        batched = bridge.get_tables(identities + ["demo.app.gone"])

        self.assertEqual(sorted(batched), sorted(identities))
        for identity in identities:
            with self.subTest(identity=identity):
                self.assertEqual(batched[identity], self.bridge().get_table(identity))
        self.assertEqual(batched["demo.app.indexed"]["primary_keys"], ["code"])
        # Columns, keys, and the keyless table's unique indexes: three queries in all.
        self.assertEqual(len(bridge.transport.statements), 3)

    def test_get_tables_keeps_only_the_owners_asked_for(self):
        batched = self.bridge().get_tables(["demo.app.keyed"])

        self.assertEqual(
            [column["name"] for column in batched["demo.app.keyed"]["columns"]], ["id", "note"]
        )

    def test_table_versions_reads_each_tables_version_by_table_id(self):
        bridge = self.bridge()

        self.assertEqual(
            bridge.table_versions([101, 102, 999]),
            {101: ("app", "keyed", 7), 102: ("app", "indexed", 9)},
        )
        self.assertEqual(len(bridge.transport.statements), 1)

    def test_batched_reads_split_into_batches(self):
        bridge = self.bridge()
        with mock.patch.object(informix_module, "_CATALOG_BATCH", 1):
            bridge.table_versions([101, 102])
            versions_queries = len(bridge.transport.statements)
            bridge.transport.statements.clear()
            bridge.get_tables(["demo.app.keyed", "demo.app.indexed"])

        self.assertEqual(versions_queries, 2)
        # Two names, two queries each, and one unique-index query for the keyless table.
        self.assertEqual(len(bridge.transport.statements), 5)


class HostShardEndpointPathTests(unittest.TestCase):
    """The shard's socket path must fit AF_UNIX's sun_path, even under a long TMPDIR."""

    def _paths(self, temp_dir):
        private = os.stat_result((stat.S_IFDIR | 0o700, 0, 0, 0, os.getuid(), 0, 0, 0, 0, 0))
        with (
            mock.patch.object(informix_module.tempfile, "gettempdir", return_value=temp_dir),
            mock.patch.object(informix_module.os, "makedirs"),
            mock.patch.object(informix_module.os, "lstat", return_value=private),
        ):
            return informix_module._HostShardEndpoints().paths(("ns", "scope", "upsert", 0))

    def test_a_short_temp_directory_is_used(self):
        _, socket_path, _ = self._paths("/tmp")
        self.assertTrue(socket_path.startswith(f"/tmp/informix-cdc-shards-{os.getuid()}/"))

    def test_a_long_temp_directory_falls_back_to_tmp(self):
        _, socket_path, _ = self._paths("/var/folders/" + "x" * 70 + "/T")
        self.assertTrue(socket_path.startswith(f"/tmp/informix-cdc-shards-{os.getuid()}/"))
        self.assertLessEqual(len(os.fsencode(socket_path)), 100)


class CurrentUtcTimeTests(unittest.TestCase):
    """A failed time probe poisons the SQLI client, so the bridge must reconnect after it."""

    def _bridge(self, error):
        bridge = informix_module.PurePythonInformixBridge(
            {
                "hostname": "localhost",
                "database": "demo",
                "user": "informix",
                "password": "secret",
                "server": "demo_on",
                "_informix.bypass.connection.capacity": "true",
            }
        )
        bridge.transport = mock.Mock()
        bridge.transport.execute.side_effect = error
        bridge._ensure_connected()
        return bridge

    def test_a_refused_probe_resets_the_transport_and_waits_before_asking_again(self):
        # The error may mean the source can't run the probe, or only a lock timeout, so
        # it is asked again after a while rather than never.
        bridge = self._bridge(SqliProtocolError("Informix SQL error -217/0"))
        now = {"t": 1000.0}

        with mock.patch.object(informix_module.time, "monotonic", lambda: now["t"]):
            self.assertIsNone(bridge.current_utc_time())
            self.assertFalse(bridge._connected)
            bridge.transport.close.assert_called_once()
            now["t"] += informix_module._UTC_TIME_REPROBE_SECONDS - 1
            self.assertIsNone(bridge.current_utc_time())
            bridge.transport.execute.assert_called_once()
            now["t"] += 1
            self.assertIsNone(bridge.current_utc_time())
            self.assertEqual(bridge.transport.execute.call_count, 2)

    def test_a_dropped_connection_is_retried_on_the_next_probe(self):
        bridge = self._bridge(informix_module.SqliTransportError("truncated SQLI stream"))

        self.assertIsNone(bridge.current_utc_time())
        self.assertFalse(bridge._connected)
        self.assertIsNone(bridge.current_utc_time())
        self.assertEqual(bridge.transport.execute.call_count, 2)


class KeyIndexDirectionsTests(unittest.TestCase):
    def test_a_lost_lease_refuses_the_read_like_every_other_bridge_operation(self):
        # Unserialized, the read raced the lease-loss shutdown and failed with a bare
        # protocol error instead of the capacity error the retry handlers expect.
        bridge = informix_module.PurePythonInformixBridge(
            {
                "hostname": "localhost",
                "database": "demo",
                "user": "informix",
                "password": "secret",
                "server": "demo_on",
                "_informix.bypass.connection.capacity": "true",
            }
        )
        bridge.transport = mock.Mock()
        bridge._connection_lease_lost.set()

        with self.assertRaises(informix_module.ConnectionCapacityUnavailable):
            bridge.key_index_directions("demo.app.orders", ["id"])
        bridge.transport.execute.assert_not_called()


class HostSharedCdcTests(unittest.TestCase):
    """cdc.shared.session=host (the default): the flow processes on one host share each
    shard through a leader process, reached over a Unix socket and elected by a lock."""

    KEY = ("namespace", "scope", "upsert", 0)
    CONFIG = {"timeout_seconds": 5, "max_records": 256, "buffer_cap": 10, "subscriber_ttl": 300.0}

    def setUp(self):
        self._shared_state = tempfile.TemporaryDirectory()
        self._lakebase = _OfflineLakebase().install(self)
        self.directory = tempfile.mkdtemp(dir="/tmp")
        self.addCleanup(shutil.rmtree, self.directory, True)
        self.endpoints = []
        self.addCleanup(self._close_endpoints)
        self.table = Table.parse(_table(), "demo")
        self.identity = self.table.native_identity
        self.capture = _capture_descriptor(self.table)
        self.shard = _SharedCdcShard()
        patcher = mock.patch.object(
            informix_module, "_shared_cdc_shard", lambda *a, **k: self.shard
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def tearDown(self):
        self._shared_state.cleanup()

    def _close_endpoints(self):
        for endpoints in self.endpoints:
            self._die(endpoints)

    def process(self):
        endpoints = informix_module._HostShardEndpoints(self.directory)
        self.endpoints.append(endpoints)
        return endpoints

    @staticmethod
    def _die(endpoints):
        """What a leader process's exit does: its lock and listener go away, leaving the
        socket file behind, as a killed process does; its serve threads hang up."""
        for key, leading in list(endpoints.leading.items()):
            del endpoints.leading[key]
            socket_path = endpoints.paths(key)[1]
            kept = socket_path + ".kept"
            if os.path.exists(socket_path):
                os.rename(socket_path, kept)
            try:
                leading["listener"].close()  # its finalizer would unlink the socket file
            except OSError:
                pass
            if os.path.exists(kept):
                os.rename(kept, socket_path)
            os.close(leading["lock_fd"])
        for connection in endpoints.clients.values():
            connection.close()
        endpoints.clients.clear()

    def poll(self, endpoints, commit=0, floor=0):
        return informix_module._host_shard_poll(
            self.KEY,
            lambda: None,
            dict(self.CONFIG),
            self.identity,
            self.table,
            self.capture,
            commit,
            floor,
            endpoints=endpoints,
        )

    def cover(self, read_start=0, tx_id=5, lsn=100):
        self.shard.ingest(
            [
                {"op": "BEGIN", "tx_id": tx_id, "lsn": lsn},
                {
                    "op": "INSERT",
                    "tx_id": tx_id,
                    "lsn": lsn + 1,
                    "table": self.table.identity,
                    "row": {"id": 7, "value": "g"},
                },
                {"op": "COMMIT", "tx_id": tx_id, "lsn": lsn + 2},
                {"op": "TIMEOUT", "lsn": lsn + 3},
            ],
            {self.identity: self.table},
            1,
            lsn + 3,
            read_start,
        )

    def requests(self):
        """Record each follower poll's request and the transactions its reply carried."""
        sent = []
        original = informix_module._host_shard_request

        def record(endpoints, key, request):
            status, payload = original(endpoints, key, request)
            carried = None if payload is None else [tx[0] for tx in payload["committed"]]
            sent.append((request["after"], carried))
            return status, payload

        patcher = mock.patch.object(informix_module, "_host_shard_request", side_effect=record)
        patcher.start()
        self.addCleanup(patcher.stop)
        return sent

    def test_the_option_is_host_by_default_or_process(self):
        mode = informix_module._shared_cdc_mode
        self.assertEqual(mode({}), "host")
        for value, expected in (
            ("host", "host"),
            (" HOST ", "host"),
            ("process", "process"),
            ("Process", "process"),
        ):
            with self.subTest(value=value):
                self.assertEqual(mode({"cdc.shared.session": value}), expected)
        for value in ("false", "true", "maybe"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "must be 'host' or 'process'"):
                    mode({"cdc.shared.session": value})

    def test_an_unknown_mode_fails_the_connector(self):
        options = {
            "database": "demo",
            "snapshot.staging.location": self._shared_state.name,
            "lakebase.password": "test-state-password",
        }
        InformixLakeflowConnect(dict(options))  # partitioned by default, shared by host
        with self.assertRaisesRegex(ValueError, "must be 'host' or 'process'"):
            InformixLakeflowConnect({**options, "cdc.shared.session": "false"})

    def test_a_view_crosses_processes_as_plain_data(self):
        table = informix_module.replace(
            self.table,
            key_override=True,
            key_wildcard=True,
            key_wildcard_except=("value",),
            column_selection=("exclude", ("extra",)),
            column_renames=(("value", "label"),),
            catalog_columns=self.table.columns,
            unsupported_columns=(Column("doc", "TEXT"),),
        )
        record = {
            "op": "INSERT",
            "tx_id": 1,
            "lsn": 11,
            "table": table.identity,
            "row": {
                "id": 1,
                "amount": Decimal("1.50"),
                "day": date(2026, 1, 2),
                "at": datetime(2026, 1, 2, 3, 4, 5),
                "raw": b"x",
                "none": None,
                "flag": True,
                "ratio": 0.5,
            },
        }
        view = informix_module._SharedCdcSnapshot(
            table=table,
            fingerprint="f" * 64,
            min_lsn=1,
            current_lsn=20,
            committed=[CommittedTransaction(1, 10, 12, 10, (record,), 1700000000)],
            caught_up=True,
            open_begin=None,
            reached_lsn=13,
        )
        wired = pickle.loads(pickle.dumps(informix_module._wire_view(view)))
        back = informix_module._unwire_view(wired)
        for item in dataclasses.fields(table):
            self.assertEqual(getattr(back.table, item.name), getattr(table, item.name), item.name)
        self.assertEqual(_schema_fingerprint(back.table), _schema_fingerprint(table))
        self.assertEqual(back.committed[0].records, (record,))
        self.assertEqual(
            (back.caught_up, back.reached_lsn, back.committed[0].commit_time),
            (True, 13, 1700000000),
        )

    def test_the_first_process_leads_and_the_next_one_polls_it(self):
        leader, follower = self.process(), self.process()

        self.assertIsNone(self.poll(leader))
        self.assertIn(self.KEY, leader.leading)
        self.assertIsNone(self.poll(follower))  # subscribed remotely; not covered yet
        self.assertNotIn(self.KEY, follower.leading)
        self.assertIn(self.identity, self.shard.subscribers)
        self.cover()

        view = self.poll(follower)

        self.assertEqual([tx.tx_id for tx in view.committed], [5])
        self.assertEqual(view.committed[0].records[0]["row"], {"id": 7, "value": "g"})
        self.assertEqual(view.fingerprint, _schema_fingerprint(self.table))
        self.assertEqual(_schema_fingerprint(view.table), _schema_fingerprint(self.table))
        self.assertTrue(view.caught_up)

    def test_a_follower_takes_over_when_the_leader_process_is_gone(self):
        leader, follower = self.process(), self.process()
        self.poll(leader)
        self.poll(follower)
        self.cover()
        self._die(leader)

        view = self.poll(follower)

        self.assertIn(self.KEY, follower.leading)
        self.assertEqual([tx.tx_id for tx in view.committed], [5])
        # And a third process now follows the new leader.
        self.assertEqual([tx.tx_id for tx in self.poll(self.process()).committed], [5])

    def test_an_unreachable_live_leader_leaves_the_follower_reading_directly(self):
        leader, follower = self.process(), self.process()
        self.poll(leader)
        self.cover()
        os.unlink(leader.paths(self.KEY)[1])

        self.assertIsNone(self.poll(follower))
        self.assertNotIn(self.KEY, follower.leading)

    def test_a_poll_the_leader_refuses_is_read_directly(self):
        leader, follower = self.process(), self.process()
        self.poll(leader)
        self.poll(follower)
        self.cover()
        with mock.patch.object(self.shard, "snapshot", side_effect=RuntimeError("boom")):
            with self.assertLogs(informix_module.__name__, level="WARNING") as logs:
                self.assertIsNone(self.poll(follower))
        self.assertIn("refused a poll", logs.output[0])
        self.assertIsNotNone(self.poll(follower))  # the connection survives a refusal

    def test_a_host_that_cannot_lock_shares_within_the_process(self):
        endpoints = self.process()
        with mock.patch("fcntl.flock", side_effect=PermissionError("flock not permitted")):
            with self.assertLogs(informix_module.__name__, level="WARNING") as logs:
                self.assertIsNone(self.poll(endpoints))
        self.assertIn("sharing within the process", logs.output[0])
        self.cover()
        self.assertEqual([tx.tx_id for tx in self.poll(endpoints).committed], [5])
        self.assertNotIn(self.KEY, endpoints.leading)

    def test_an_unusable_shard_directory_shares_within_the_process(self):
        permissive = tempfile.mkdtemp(dir="/tmp")
        self.addCleanup(shutil.rmtree, permissive, True)
        os.chmod(permissive, 0o755)
        for directory in ("/dev/null/informix-shards", permissive):
            with self.subTest(directory=directory):
                self.shard = _SharedCdcShard()
                endpoints = informix_module._HostShardEndpoints(directory)
                with self.assertLogs(informix_module.__name__, level="WARNING") as logs:
                    self.assertIsNone(self.poll(endpoints))
                self.assertIn("sharing within the process", logs.output[0])
                self.assertFalse(endpoints.leading)
                self.cover()
                self.assertEqual([tx.tx_id for tx in self.poll(endpoints).committed], [5])

    def test_a_leader_that_stops_serving_cannot_hang_a_follower(self):
        # A live process holds the lock and listens, but never accepts or handshakes.
        import fcntl
        import socket

        follower = self.process()
        lock_path, socket_path, key_path = follower.paths(self.KEY)
        lock = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        self.addCleanup(os.close, lock)
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with open(key_path, "wb") as out:
            out.write(b"k" * 32)
        silent = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.addCleanup(silent.close)
        silent.bind(socket_path)
        silent.listen(8)

        with mock.patch.object(informix_module, "_HOST_SHARD_REPLY_SECONDS", 0.3):
            started = time.monotonic()
            self.assertIsNone(self.poll(follower))
            self.assertLess(time.monotonic() - started, 5)
        self.assertFalse(follower.leading)
        self.assertFalse(follower.clients)

    def test_a_follower_is_sent_only_the_transactions_it_does_not_hold(self):
        leader, follower = self.process(), self.process()
        self.poll(leader)
        self.poll(follower)
        sent = self.requests()
        self.cover()
        self.assertEqual([tx.tx_id for tx in self.poll(follower).committed], [5])
        self.cover(tx_id=6, lsn=110)

        self.assertEqual([tx.tx_id for tx in self.poll(follower).committed], [5, 6])
        self.assertEqual([tx.tx_id for tx in self.poll(follower, commit=102).committed], [6])
        # A checkpoint moved back past what the follower holds starts over.
        self.assertEqual([tx.tx_id for tx in self.poll(follower, commit=0).committed], [6])

        self.assertEqual(sent, [(None, [5]), (102, [6]), (112, []), (None, [6])])

    def test_a_reply_that_cannot_be_sent_ends_the_serve_thread_quietly(self):
        self.shard.subscribe(self.identity, self.table, self.capture, 0, 0)
        self.cover()
        leader = self.process()
        leader.leading[self.KEY] = {"reader_factory": lambda: None, "remote": set()}
        self.addCleanup(leader.leading.pop, self.KEY, None)
        ends = socket.socketpair(socket.AF_UNIX)
        self.addCleanup(lambda: [end.close() for end in ends])
        connection = mock.Mock()
        connection.fileno.return_value = ends[0].fileno()
        request = {
            "config": dict(self.CONFIG),
            "identity": self.identity,
            "table": informix_module._wire_table(self.table),
            "capture": self.capture,
            "commit": 0,
            "floor": 0,
            "after": None,
        }
        connection.recv.side_effect = [request]
        connection.send.side_effect = [TypeError("cannot pickle"), BrokenPipeError()]
        with (
            mock.patch("multiprocessing.connection.deliver_challenge"),
            mock.patch("multiprocessing.connection.answer_challenge"),
        ):
            informix_module._host_shard_serve(leader, self.KEY, connection, b"k")
        self.assertEqual(connection.send.call_args.args[0][0], "error")
        connection.close.assert_called_once()

    def test_a_malformed_poll_gets_an_error_reply_and_the_connection_stays(self):
        leader = self.process()
        leader.leading[self.KEY] = {"reader_factory": lambda: None, "remote": set()}
        self.addCleanup(leader.leading.pop, self.KEY, None)
        ends = socket.socketpair(socket.AF_UNIX)
        self.addCleanup(lambda: [end.close() for end in ends])
        connection = mock.Mock()
        connection.fileno.return_value = ends[0].fileno()
        connection.recv.side_effect = [{}, EOFError()]
        with (
            mock.patch("multiprocessing.connection.deliver_challenge"),
            mock.patch("multiprocessing.connection.answer_challenge"),
        ):
            informix_module._host_shard_serve(leader, self.KEY, connection, b"k")
        self.assertEqual(connection.send.call_args.args[0][0], "error")
        self.assertEqual(connection.recv.call_count, 2)

    def test_a_follower_that_stalls_cannot_pin_a_serve_thread(self):
        import multiprocessing.connection

        def serve(peer_writes):
            ends = socket.socketpair(socket.AF_UNIX)
            self.addCleanup(lambda: [end.close() for end in ends])
            ends[1].sendall(peer_writes)
            connection = multiprocessing.connection.Connection(os.dup(ends[0].fileno()))
            thread = threading.Thread(
                target=informix_module._host_shard_serve,
                args=(leader, self.KEY, connection, b"k" * 32),
                daemon=True,
            )
            thread.start()
            thread.join(2)
            return thread

        leader = self.process()
        leader.leading[self.KEY] = {"reader_factory": lambda: None, "remote": set()}
        self.addCleanup(leader.leading.pop, self.KEY, None)
        with mock.patch.object(informix_module, "_HOST_SHARD_REPLY_SECONDS", 0.3):
            # Connected, then never answers the handshake.
            self.assertFalse(serve(b"").is_alive())
            with (
                mock.patch("multiprocessing.connection.deliver_challenge"),
                mock.patch("multiprocessing.connection.answer_challenge"),
            ):
                # Stalled half-way through sending a poll.
                self.assertFalse(serve(b"\x00\x00").is_alive())
                # Merely idle between polls: the connection is kept.
                self.assertTrue(serve(b"").is_alive())

    def test_a_leader_that_keeps_failing_to_accept_gives_the_shard_up(self):
        leader, follower = self.process(), self.process()
        self.poll(leader)
        followed = "demo:informix.followed"
        informix_module._host_shard_poll(
            self.KEY,
            lambda: None,
            dict(self.CONFIG),
            followed,
            self.table,
            self.capture,
            0,
            0,
            endpoints=follower,
        )
        self.assertEqual(set(self.shard.subscribers), {self.identity, followed})
        broken = mock.Mock()
        broken.accept.side_effect = OSError(errno.EMFILE, "Too many open files")
        with (
            mock.patch.dict(informix_module._SHARED_CDC_SHARDS, {self.KEY: self.shard}),
            mock.patch.object(informix_module.time, "sleep"),
            self.assertLogs(informix_module.__name__, level="WARNING") as logs,
        ):
            informix_module._host_shard_accept(leader, self.KEY, broken, b"k")
        self.assertIn("gave up", logs.output[-1])
        self.assertEqual(broken.accept.call_count, informix_module._HOST_SHARD_ACCEPT_FAILURES)
        self.assertNotIn(self.KEY, leader.leading)
        # Only the leader's own subscriber is left; the successor reads the follower's.
        self.assertEqual(set(self.shard.subscribers), {self.identity})
        successor = self.process()
        self.poll(successor)
        self.assertIn(self.KEY, successor.leading)

    def test_endpoints_never_serialize_their_locks_or_sockets(self):
        leader = self.process()
        self.poll(leader)
        copy = pickle.loads(pickle.dumps(leader))
        self.assertEqual((copy.leading, copy.clients, copy.unavailable), ({}, {}, None))

    def test_a_consumer_reads_through_another_processes_leader(self):
        bridge = FakeBridge()
        connector = InformixLakeflowConnect(
            {
                "database": "demo",
                "snapshot.staging.location": self._shared_state.name,
                "lakebase.password": "test-state-password",
                "stream.partitioned": "false",
            }
        )
        connector.set_registration_scope(hashlib.sha256(b"host").hexdigest()[:32])
        connector._bridge_instance = bridge
        leader, follower = self.process(), self.process()
        with mock.patch.object(informix_module, "_HOST_SHARD_ENDPOINTS", leader):
            self.assertIsNone(
                connector._shared_cdc_snapshot(
                    self.table, _stream_offset(), {}, False, connector._pipeline_scope()
                )
            )
        self.assertTrue(leader.leading)
        self.shard.subscribe(self.identity, self.table, self.capture, 0, 0)
        self.cover()
        before = list(bridge.changes)

        with mock.patch.object(informix_module, "_HOST_SHARD_ENDPOINTS", follower):
            rows, _offset = connector._read_stream(self.table, _stream_offset(), {}, deletes=False)
            rows = list(rows)

        self.assertFalse(follower.leading)
        self.assertEqual([row["id"] for row in rows], [7])
        self.assertEqual(bridge.change_reads, [])
        self.assertEqual(bridge.changes, before)


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

    def test_thread_count_defaults_to_quarter_of_pool(self):
        # Per-channel threads default to a quarter of the pool: with both channels sharded
        # independently that keeps total daemon slot demand (2K) near half the pool,
        # leaving room for consumer bootstrap reads and the snapshot drain.
        self.assertEqual(self._connector()._shared_cdc_thread_count(), 4)  # 16 // 4

    def test_thread_count_default_tracks_max_connections(self):
        self.assertEqual(
            self._connector(**{"max.concurrent.connections": "6"})._shared_cdc_thread_count(),
            1,  # 6 // 4 -> floored, then max(1, ...)
        )

    def test_thread_count_override(self):
        self.assertEqual(
            self._connector(**{"cdc.shared.reader.threads": "4"})._shared_cdc_thread_count(), 4
        )

    def test_thread_count_override_ignores_pool_size(self):
        self.assertEqual(
            self._connector(
                **{"cdc.shared.reader.threads": "8", "max.concurrent.connections": "6"}
            )._shared_cdc_thread_count(),
            8,
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

    def test_negative_startup_jitter_rejected(self):
        with self.assertRaises(ValueError):
            self._connector(**{"connection.startup.jitter.seconds": "-1"})

    def test_non_numeric_startup_jitter_rejected(self):
        with self.assertRaises(ValueError):
            self._connector(**{"connection.startup.jitter.seconds": "soon"})

    def test_zero_startup_jitter_accepted(self):
        # 0 is the disabled default and must construct.
        self._connector(**{"connection.startup.jitter.seconds": "0"})


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
                "cdc.shared.session": "process",
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
            0,
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
        shard.ingest([{"op": "TIMEOUT", "lsn": 103}], {identity: table}, 1, 103, 0)

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
            0,
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

    def test_reader_threads_default_is_one_below_the_daemon_reservation(self):
        # The default sits one below the CDC-daemon reservation so the snapshot-drain
        # floor never collapses to 0 -- a low slot always stays reachable by consumer
        # reads even while drains hold their slots for whole scans. Pool 6 -> reservation
        # 2 -> 1 worker; pool 16 -> reservation 5 -> 4 workers.
        self.assertEqual(
            self._connector(**{"max.concurrent.connections": "6"})._snapshot_reader_thread_count(),
            1,
        )
        self.assertEqual(
            self._connector(**{"max.concurrent.connections": "16"})._snapshot_reader_thread_count(),
            4,
        )

    def test_reader_threads_default_tracks_explicit_reservation(self):
        # An explicit daemon.connection.reservation moves the default with it (one below).
        self.assertEqual(
            self._connector(
                **{"max.concurrent.connections": "16", "daemon.connection.reservation": "8"}
            )._snapshot_reader_thread_count(),
            7,
        )

    def test_reader_threads_default_floors_at_one(self):
        # A tiny pool (reservation 1) still gets a worker rather than 0.
        self.assertEqual(
            self._connector(**{"max.concurrent.connections": "2"})._snapshot_reader_thread_count(),
            1,
        )

    def test_reader_threads_override(self):
        # An explicit value wins regardless of pool size.
        self.assertEqual(
            self._connector(
                **{"snapshot.reader.threads": "4", "max.concurrent.connections": "6"}
            )._snapshot_reader_thread_count(),
            4,
        )

    def test_zero_reader_threads_rejected(self):
        with self.assertRaises(ValueError):
            self._connector(**{"snapshot.reader.threads": "0"})


class SnapshotDrainWorkerFloorTests(unittest.TestCase):
    """A drain-pool worker claims its slot one band below the CDC daemon's floor, so a busy
    CDC daemon cannot starve it, and every other reader drains through the pool.

    The slot floor is computed in the bridge's ``_acquire_connection_slot``, so these
    exercise a real bridge with ``acquire_slot`` stubbed to capture the floor before any
    lease is taken.
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

    def test_a_drain_worker_floors_one_band_below_the_cdc_daemon(self):
        worker = {informix_module._SNAPSHOT_DAEMON_SLOT_MARKER_OPTION: "true"}
        daemon = {informix_module._DAEMON_SLOT_MARKER_OPTION: "true"}
        for pool, worker_floor, daemon_floor in (("16", 1, 5), ("8", 1, 2)):
            with self.subTest(pool=pool):
                size = {"max.concurrent.connections": pool}
                self.assertEqual(self._captured_floor(self._bridge(**size, **worker)), worker_floor)
                self.assertEqual(self._captured_floor(self._bridge(**size, **daemon)), daemon_floor)
                self.assertEqual(self._captured_floor(self._bridge(**size)), 0)

    def test_only_a_pool_worker_drains_inline(self):
        connector = InformixLakeflowConnect(
            {
                "database": "demo",
                "snapshot.staging.location": self._shared_state.name,
                "lakebase.password": "test-state-password",
            }
        )
        worker = connector._snapshot_drain_reader_factory()()
        self.assertFalse(connector._is_snapshot_drain_worker())
        self.assertTrue(worker._is_snapshot_drain_worker())


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
        # Pool 6, daemon reservation 2, snapshot.reader.threads pinned to 1 -> the snapshot
        # drain floors at 2 - 1 = 1, one slot below the CDC daemon (which floors at 2). That
        # leaves slot [1, 2) reachable by the drain but NOT the CDC daemon, so a saturated
        # CDC daemon cannot starve the drain. Slot 0 stays for consumer bootstrap. (Pinned
        # to 1 because the default now scales to a third of the pool.)
        bridge = self._bridge(**{"snapshot.reader.threads": "1"})
        bridge.options[informix_module._SNAPSHOT_DAEMON_SLOT_MARKER_OPTION] = "true"
        self.assertEqual(self._captured_floor(bridge), 1)

    def test_snapshot_daemon_floor_preserves_a_consumer_slot_by_default(self):
        # The default reader-thread count is one below the daemon reservation (pool 6 ->
        # reservation 2 -> 1 worker), so the snapshot-drain floor is 2 - 1 = 1: the drains
        # never collapse the floor to 0, and slot 0 stays reachable by consumer reads even
        # while the drains hold their slots for the whole scan.
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
        reader = self._connector()._shared_cdc_reader_factory()()
        self.assertEqual(reader.options.get(informix_module._DAEMON_SLOT_MARKER_OPTION), "true")

    def test_cdc_reader_factory_drops_inherited_attempt_budget(self):
        # The daemon waits on the turnover watchdog, so a short per-read consumer budget
        # inherited from the parent's options must not cap that wait and send it back to
        # the end of the queue.
        connector = self._connector()
        connector.options[informix_module._CONNECTION_ATTEMPT_BUDGET_OPTION] = "0.8"
        reader = connector._shared_cdc_reader_factory()()
        self.assertIsNone(reader.options.get(informix_module._CONNECTION_ATTEMPT_BUDGET_OPTION))

    def test_snapshot_drain_reader_factory_sets_snapshot_daemon_marker(self):
        # The drain floors one band below the CDC daemon, so it carries the snapshot-daemon
        # marker -- not the CDC daemon marker, which would confine it to the CDC-contended
        # band and let a saturated CDC daemon starve it.
        reader = self._connector()._snapshot_drain_reader_factory()()
        self.assertEqual(
            reader.options.get(informix_module._SNAPSHOT_DAEMON_SLOT_MARKER_OPTION), "true"
        )
        self.assertNotEqual(reader.options.get(informix_module._DAEMON_SLOT_MARKER_OPTION), "true")

    def test_snapshot_drain_reader_inherits_connection_wait_timeout(self):
        # The worker waits for its slot using the same connection.wait.timeout.seconds the
        # consumer uses as the drain's idle window -- one knob governs both. The factory no
        # longer overrides it (the removed snapshot.drain.wait.seconds), so it passes through.
        connector = self._connector(**{"connection.wait.timeout.seconds": "1800"})
        reader = connector._snapshot_drain_reader_factory()()
        self.assertEqual(reader.options.get("connection.wait.timeout.seconds"), "1800")

    def test_snapshot_drain_reader_does_not_inject_connection_wait_timeout(self):
        # With nothing set, the factory injects no connection.wait.timeout.seconds of its
        # own -- the worker falls back to the same default as any other reader.
        reader = self._connector()._snapshot_drain_reader_factory()()
        self.assertIsNone(reader.options.get("connection.wait.timeout.seconds"))

    def test_snapshot_drain_reader_drops_inherited_attempt_budget(self):
        # A per-read attempt budget from the consumer would cap the slot wait below the
        # idle window, so the factory clears it on the worker's copy.
        connector = self._connector()
        connector.options[informix_module._CONNECTION_ATTEMPT_BUDGET_OPTION] = "5"
        reader = connector._snapshot_drain_reader_factory()()
        self.assertIsNone(reader.options.get(informix_module._CONNECTION_ATTEMPT_BUDGET_OPTION))


class SnapshotDrainPoolTests(unittest.TestCase):
    """The bounded pool that runs snapshot drains off the microbatch thread."""

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

    def test_progress_resets_the_idle_window(self):
        # The wait is a liveness watchdog: reported progress resets the idle window, so a
        # job that keeps making progress does not trip even past several idle windows.
        pool = _SnapshotDrainPool()
        pool.submit(("k",), table=None, options={}, pipeline_scope="s", allow_keyless=False)

        def drive():
            for _ in range(6):  # 6 * 0.05s = 0.3s, well past the 0.1s idle window
                time.sleep(0.05)
                pool.report_progress(("k",))
            with pool.condition:
                pool.pending.discard(("k",))
                pool.progress.pop(("k",), None)
                pool.results[("k",)] = None
                pool.condition.notify_all()

        driver = threading.Thread(target=drive, daemon=True)
        driver.start()
        self.assertTrue(pool.wait(("k",), 0.1))
        driver.join(timeout=2)

    def test_wait_trips_after_progress_then_stalls(self):
        # Progress advances the window once, but once it stops the watchdog still trips.
        pool = _SnapshotDrainPool()
        pool.submit(("k",), table=None, options={}, pipeline_scope="s", allow_keyless=False)
        pool.report_progress(("k",))
        self.assertFalse(pool.wait(("k",), 0.05))

    def test_worker_progress_keeps_a_slow_drain_alive(self):
        # End to end: a drain that stages pages slower than the idle window still completes,
        # because the worker wires each staged page through report_progress.
        pool = _SnapshotDrainPool()

        class _SlowReader:
            _drain_progress = None

            def _read_snapshot(self, *args, **kwargs):
                for _ in range(6):  # 0.3s total, longer than the 0.1s idle window
                    time.sleep(0.05)
                    if self._drain_progress is not None:
                        self._drain_progress()

            def close(self):
                pass

        pool.configure(reader_factory=_SlowReader, thread_count=1)
        worker = threading.Thread(
            target=informix_module._run_snapshot_drain_worker, args=(pool,), daemon=True
        )
        worker.start()
        pool.submit(("k",), table="T", options={}, pipeline_scope="scope", allow_keyless=False)
        self.assertTrue(pool.wait(("k",), 0.1))

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

    def test_queued_job_waits_while_the_pool_makes_progress(self):
        # A queued job (no worker yet) must not time out while another drain is healthily
        # progressing ahead of it -- it waits its turn rather than failing behind a big drain.
        pool = _SnapshotDrainPool()
        pool.submit(("A",), table=None, options={}, pipeline_scope="s", allow_keyless=False)
        pool.mark_running(("A",))  # A is draining
        pool.submit(("B",), table=None, options={}, pipeline_scope="s", allow_keyless=False)

        def drive():
            for _ in range(6):  # 0.3s of pool progress, well past B's 0.1s idle window
                time.sleep(0.05)
                pool.report_progress(("A",))
            with pool.condition:  # A finishes, B is served
                pool.pending.discard(("B",))
                pool.running.discard(("B",))
                pool.progress.pop(("B",), None)
                pool.results[("B",)] = None
                pool.condition.notify_all()

        driver = threading.Thread(target=drive, daemon=True)
        driver.start()
        self.assertTrue(pool.wait(("B",), 0.1))  # B rode out A's drain, never tripped
        driver.join(timeout=2)

    def test_running_job_trips_on_its_own_stall_even_while_pool_progresses(self):
        # A running job is measured from its OWN progress: a hung fetch on its worker
        # trips even though another worker keeps the pool clock fresh.
        pool = _SnapshotDrainPool()
        pool.submit(("A",), table=None, options={}, pipeline_scope="s", allow_keyless=False)
        pool.mark_running(("A",))  # picked up, then stalls -- never reports again
        stop = threading.Event()

        def other_worker():
            while not stop.is_set():
                pool.report_progress(("B",))  # another worker advancing the pool clock
                time.sleep(0.02)

        noise = threading.Thread(target=other_worker, daemon=True)
        noise.start()
        try:
            self.assertFalse(pool.wait(("A",), 0.1))  # own stall trips despite pool activity
        finally:
            stop.set()
            noise.join(timeout=2)

    def test_queued_job_trips_when_the_whole_pool_is_quiet(self):
        # With nothing running and no progress anywhere, a queued job still times out.
        pool = _SnapshotDrainPool()
        pool.submit(("B",), table=None, options={}, pipeline_scope="s", allow_keyless=False)
        self.assertFalse(pool.wait(("B",), 0.05))


class SnapshotDaemonSeamTests(unittest.TestCase):
    """The drain pool end to end: the consumer frees its slot, a daemon worker drains and
    stages the table, and the consumer serves the staged pages."""

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
            "stream.partitioned": "false",
            **extra,
        }

    def test_shared_read_drains_via_daemon_and_serves_pages(self):
        # A unique scope per invocation: the drain pool is a module-global keyed by scope,
        # and these classes are re-run under two file paths in one process, so a fixed
        # scope would make the second run reuse the first's pool + torn-down state.
        scope_hex = hashlib.sha256(os.urandom(16)).hexdigest()[:32]

        def fake_factory():
            reader = InformixLakeflowConnect(self._options())
            reader.set_registration_scope(scope_hex)
            reader._bridge_instance = FakeBridge()
            return _drain_snapshots_inline(reader)

        consumer = InformixLakeflowConnect(self._options(**{"snapshot.reader.threads": "1"}))
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
        # The primer drains inline to pre-publish the manifest without a daemon.
        primer = _drain_snapshots_inline(InformixLakeflowConnect(self._options()))
        primer.set_registration_scope(scope_hex)
        primer._bridge_instance = FakeBridge()
        primer.read_table("app.orders", {}, {})

        consumer = InformixLakeflowConnect(self._options())
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
            reader = InformixLakeflowConnect(self._options())
            reader.set_registration_scope(scope_hex)
            reader._bridge_instance = FakeBridge()
            return _drain_snapshots_inline(reader)

        consumer = InformixLakeflowConnect(self._options(**{"snapshot.reader.threads": "1"}))
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
                "stream.partitioned": "false",
                **options,
            }
        )
        # Unique scope per connector: staged snapshot state is keyed by scope and these
        # classes run under two file paths in one process, so a fixed scope would collide.
        connector.set_registration_scope(hashlib.sha256(os.urandom(16)).hexdigest()[:32])
        connector._bridge_instance = FakeBridge()
        return _drain_snapshots_inline(_read_cdc_directly(connector))

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


class SnapshotScanOptionTests(unittest.TestCase):
    """snapshot.scan=cursor reads a keyed table's one-pass snapshot through the unordered
    keyless cursor instead of key-ordered keyset pages, while the key still drives the
    destination merge and the delete channel."""

    def setUp(self):
        self._shared_state = tempfile.TemporaryDirectory()
        self._lakebase = _OfflineLakebase().install(self)

    def tearDown(self):
        self._shared_state.cleanup()

    def _connector(self, bridge=None, **options):
        connector = InformixLakeflowConnect(
            {
                "database": "demo",
                "snapshot.staging.location": self._shared_state.name,
                "lakebase.password": "test-state-password",
                "snapshot.mode": "initial",
                "stream.partitioned": "false",
                **options,
            }
        )
        # Unique scope per connector so staged snapshot state never leaks between cases.
        connector.set_registration_scope(hashlib.sha256(os.urandom(16)).hexdigest()[:32])
        connector._bridge_instance = bridge or FakeBridge()
        return _drain_snapshots_inline(_read_cdc_directly(connector))

    @staticmethod
    def _keyless_bridge():
        bridge = FakeBridge()
        bridge.tables = [_table(primary_keys=())]
        return bridge

    def test_keyset_is_the_default(self):
        connector = self._connector()

        connector.read_table("app.orders", {}, {})

        self.assertEqual(connector._bridge_instance.snapshot_primary_keys, [("id",)])

    def test_cursor_drains_a_catalog_keyed_table_without_key_order(self):
        connector = self._connector()

        rows, _ = connector.read_table("app.orders", {}, {"snapshot.scan": "cursor"})

        bridge = connector._bridge_instance
        self.assertEqual(bridge.snapshot_primary_keys, [()])
        self.assertEqual(
            bridge.snapshot_page_sizes, [informix_module._DEFAULT_KEYLESS_SNAPSHOT_PAGE_SIZE]
        )
        self.assertEqual(sorted(row["id"] for row in rows), [1, 2])

    def test_cursor_keeps_an_overridden_key_for_the_merge(self):
        # The point of the option: a primary.keys override on a keyless table snapshots
        # without a per-page sort, yet still reports and merges on its key.
        for options in ({"primary.keys": "id"}, {"primary.keys": "*"}):
            with self.subTest(options=options):
                connector = self._connector(self._keyless_bridge())
                table_options = {**options, "snapshot.scan": "cursor"}

                metadata = connector.read_table_metadata("app.orders", table_options)
                connector.read_table("app.orders", {}, table_options)

                self.assertEqual(metadata["ingestion_type"], "cdc_with_deletes")
                self.assertTrue(metadata["primary_keys"])
                self.assertEqual(connector._bridge_instance.snapshot_primary_keys, [()])

    def test_connection_scope_applies_and_the_table_option_wins(self):
        connector = self._connector(**{"snapshot.scan": "cursor"})
        connector.read_table("app.orders", {}, {})
        self.assertEqual(connector._bridge_instance.snapshot_primary_keys, [()])

        connector = self._connector(**{"snapshot.scan": "cursor"})
        connector.read_table("app.orders", {}, {"snapshot.scan": "keyset"})
        self.assertEqual(connector._bridge_instance.snapshot_primary_keys, [("id",)])

    def test_incremental_mode_falls_back_to_the_cursor_consistent_snapshot(self):
        # A cursor scan has no key ranges to chunk by, so a fresh incremental snapshot
        # takes the one-pass consistent snapshot, as an unchunkable DATETIME key does.
        connector = self._connector()

        _, offset = connector.read_table(
            "app.orders", {}, {"snapshot.mode": "incremental", "snapshot.scan": "cursor"}
        )

        self.assertEqual(connector._bridge_instance.snapshot_primary_keys, [()])
        self.assertNotIn("incremental", offset)

        # Without the option the same mode chunks by key and never calls the drain.
        connector = self._connector()
        connector.read_table("app.orders", {}, {"snapshot.mode": "incremental"})
        self.assertEqual(connector._bridge_instance.snapshot_primary_keys, [])

    def test_a_cursor_scan_pipelines_its_rest_staging_like_a_keyless_drain(self):
        # The drain, not the key, decides the overlap: a keyed table read through the
        # cursor stages its pages through the same background uploader a keyless table
        # uses on the REST transport, while its keyset drain stays synchronous.
        built: list = []
        real_pipeline = informix_module._AsyncStagePipeline

        class _RecordingPipeline(real_pipeline):
            def __init__(self, *args, **kwargs):
                built.append(self)
                super().__init__(*args, **kwargs)

        class _FakeFilesApi:
            def __init__(self, host, provider):
                pass

            def put(self, path, data):
                os.makedirs(os.path.dirname(path), mode=0o700, exist_ok=True)
                with open(path, "wb") as handle:
                    handle.write(data)

        def _fake_provider(*_a, **_k):
            provider = mock.Mock()
            provider.bearer.return_value = "tok"
            return provider

        rest = {
            "snapshot.staging.transport": "rest",
            "lakebase.workspace.host": "https://ws",
            "lakebase.workspace.token": "boot",
        }
        with (
            mock.patch.object(informix_module, "_AsyncStagePipeline", _RecordingPipeline),
            mock.patch.object(informix_module, "WorkspaceFilesApi", _FakeFilesApi),
            mock.patch.object(informix_module, "StagingCredentialProvider", _fake_provider),
        ):
            for scan, pipelines in (("cursor", 1), ("keyset", 0)):
                with self.subTest(scan=scan):
                    del built[:]
                    bridge = FakeBridge()
                    bridge.rows = [{"id": i, "value": str(i)} for i in range(10)]
                    connector = self._connector(bridge, **rest)

                    connector.read_table(
                        "app.orders",
                        {},
                        {"snapshot.scan": scan, "keyless.snapshot.page.size": "3"},
                    )

                    self.assertEqual(len(built), pipelines)

    def test_a_malformed_scan_value_is_rejected_at_registration_and_construction(self):
        # Like append.delete.tombstones: a typo fails when the table is registered or
        # the connector is built, not on the first snapshot read.
        with self.assertRaisesRegex(ValueError, "snapshot.scan"):
            self._connector().read_table_metadata("app.orders", {"snapshot.scan": "sorted"})
        with self.assertRaisesRegex(ValueError, "snapshot.scan"):
            self._connector(**{"snapshot.scan": "sorted"})


class FairSlotQueueWiringTests(unittest.TestCase):
    """_acquire_connection_slot enqueues a fair-queue ticket, waits its turn, and
    always drops the ticket."""

    def setUp(self):
        self._lakebase = _OfflineLakebase().install(self)

    def _bridge(self, **options):
        return informix_module.PurePythonInformixBridge(
            {
                "hostname": "localhost",
                "database": "demo",
                "user": "informix",
                "password": "secret",
                "server": "demo_on",
                "lakebase.password": "test-state-password",
                "max.concurrent.connections": "1",
                **options,
            }
        )

    def _waiters(self, namespace):
        return sorted(ticket for (ns, ticket) in self._lakebase.database.waiters if ns == namespace)

    def _stop_heartbeat(self, bridge):
        stop = getattr(bridge, "_connection_slot_heartbeat_stop", None)
        if stop is not None:
            stop.set()

    def test_fair_queue_passes_a_ticket_to_acquire(self):
        bridge = self._bridge()
        captured = {}
        real = informix_module.acquire_slot

        def spy(*args, **kwargs):
            captured["ticket_id"] = kwargs.get("ticket_id")
            return real(*args, **kwargs)

        with mock.patch.object(informix_module, "acquire_slot", side_effect=spy):
            bridge._acquire_connection_slot()
        self.addCleanup(self._stop_heartbeat, bridge)
        self.assertIsNotNone(captured["ticket_id"])

    def test_reaped_ticket_is_re_enqueued(self):
        # A ticket reaped during a stall can never win, so the sweep takes a fresh one and
        # drops that one when it wins.
        bridge = self._bridge()
        real_enqueue = informix_module.enqueue_waiter
        real_heartbeat = informix_module.heartbeat_waiter
        real_dequeue = informix_module.dequeue_waiter
        tickets = []

        def enqueue(*args, **kwargs):
            tickets.append(real_enqueue(*args, **kwargs))
            return tickets[-1]

        def heartbeat(connection, namespace, ticket_id, **kwargs):
            if len(tickets) == 1:
                # Another waiter's reaper dropped this ticket during a stall.
                self._lakebase.database.waiters.pop((namespace, ticket_id))
            return real_heartbeat(connection, namespace, ticket_id, **kwargs)

        with (
            mock.patch.object(informix_module, "heartbeat_waiter", side_effect=heartbeat),
            mock.patch.object(informix_module, "enqueue_waiter", side_effect=enqueue),
            mock.patch.object(
                informix_module, "dequeue_waiter", side_effect=real_dequeue
            ) as dequeue,
        ):
            bridge._acquire_connection_slot()
        self.addCleanup(self._stop_heartbeat, bridge)

        self.assertEqual(len(tickets), 2)
        self.assertIsNotNone(bridge._lakebase_slot)
        dequeue.assert_called_once()
        self.assertEqual(dequeue.call_args.args[2], tickets[1])

    def test_reconnect_after_reset_keeps_the_held_slot(self):
        # reset_transport keeps the slot for an in-place retry. A reconnect that claimed
        # another would leave the first one's heartbeat renewing it until the process
        # exits, and every such retry would strand one more slot.
        bridge = self._bridge(**{"max.concurrent.connections": "2"})
        bridge.transport = mock.Mock()
        real = informix_module.acquire_slot
        with mock.patch.object(informix_module, "acquire_slot", side_effect=real) as acquire:
            bridge._ensure_connected()
            self.addCleanup(bridge._release_connection_slot)
            slot, heartbeat = bridge._lakebase_slot, bridge._connection_slot_heartbeat
            bridge.reset_transport()
            bridge._ensure_connected()

        self.assertEqual(acquire.call_count, 1)
        self.assertEqual(bridge.transport.connect.call_count, 2)
        self.assertIs(bridge._lakebase_slot, slot)
        self.assertIs(bridge._connection_slot_heartbeat, heartbeat)
        self.assertTrue(heartbeat.is_alive())

    def test_a_bridge_seeds_its_slots_once_per_limit(self):
        bridge = self._bridge()
        with (
            mock.patch.object(
                informix_module, "seed_slots", wraps=informix_module.seed_slots
            ) as seed,
            mock.patch.object(
                informix_module,
                "publish_connection_limit",
                wraps=informix_module.publish_connection_limit,
            ) as publish,
        ):
            bridge._acquire_connection_slot()
            bridge._release_connection_slot()
            bridge._acquire_connection_slot()
            self.addCleanup(bridge._release_connection_slot)

        seed.assert_called_once()
        publish.assert_called_once()

    def test_a_failed_claim_keeps_the_seeded_limit(self):
        # Both writes commit before the claim, so a starved reader that gives up and
        # comes straight back must not pay them again.
        bridge = self._bridge(**{"connection.wait.timeout.seconds": "0"})
        with mock.patch.object(
            informix_module, "seed_slots", wraps=informix_module.seed_slots
        ) as seed:
            with (
                mock.patch.object(informix_module, "acquire_slot", return_value=None),
                self.assertRaises(informix_module.ConnectionCapacityUnavailable),
            ):
                bridge._acquire_connection_slot()
            bridge._acquire_connection_slot()
            self.addCleanup(bridge._release_connection_slot)

        seed.assert_called_once()

    def test_successful_acquire_dequeues_its_ticket(self):
        bridge = self._bridge()
        bridge._acquire_connection_slot()
        self.addCleanup(self._stop_heartbeat, bridge)
        namespace = bridge._lakebase_namespace()
        self.assertIsNotNone(bridge._lakebase_slot)
        self.assertEqual(self._waiters(namespace), [])  # ticket dropped on success

    def test_acquire_waits_behind_an_older_ticket(self):
        from databricks.labs.community_connector.sources.informix import lakebase_state

        bridge = self._bridge()
        namespace = bridge._lakebase_namespace()
        connection = bridge._lakebase_connection()
        lakebase_state.seed_slots(connection, namespace, 1)
        # Another reader is already queued for the only slot; even though slot 0 is
        # free, this acquire must yield to the older ticket rather than jump ahead.
        older = lakebase_state.enqueue_waiter(connection, namespace, "other", floor=0, ceiling=1)

        with self.assertRaises(informix_module.ConnectionCapacityUnavailable):
            bridge._acquire_connection_slot(budget_seconds=0.01)

        # It cleaned up only its own ticket; the older waiter's is untouched.
        self.assertEqual(self._waiters(namespace), [older])
        self.assertIsNone(getattr(bridge, "_lakebase_slot", None))

    def _captured_acquire_kwargs(self, bridge):
        captured = {}
        real = informix_module.acquire_slot

        def spy(*args, **kwargs):
            captured.update(kwargs)
            return real(*args, **kwargs)

        with mock.patch.object(informix_module, "acquire_slot", side_effect=spy):
            bridge._acquire_connection_slot()
        self.addCleanup(self._stop_heartbeat, bridge)
        return captured

    def test_acquire_rotates_by_requester_class(self):
        captured = self._captured_acquire_kwargs(self._bridge())
        self.assertEqual(captured.get("waiter_class"), informix_module._SLOT_CLASS_CONSUMER)
        self.assertEqual(captured.get("num_classes"), informix_module._SLOT_CLASS_COUNT)

    def test_waiter_class_reflects_the_slot_markers(self):
        self.assertEqual(
            self._bridge()._connection_waiter_class(), informix_module._SLOT_CLASS_CONSUMER
        )
        self.assertEqual(
            self._bridge(
                **{informix_module._DAEMON_SLOT_MARKER_OPTION: "true"}
            )._connection_waiter_class(),
            informix_module._SLOT_CLASS_CDC_DAEMON,
        )
        self.assertEqual(
            self._bridge(
                **{informix_module._SNAPSHOT_DAEMON_SLOT_MARKER_OPTION: "true"}
            )._connection_waiter_class(),
            informix_module._SLOT_CLASS_SNAPSHOT_DAEMON,
        )


class CdcDaemonSlotLivenessTests(unittest.TestCase):
    """The CDC shard daemon uses the same turnover watchdog as the snapshot drain, so it
    holds its queue ticket while the pool is turning over (letting the round-robin rotation
    reach it) and only gives up -- degrading its consumers to direct reads -- when the pool
    is wedged. A consumer read (no daemon marker) keeps the plain deadline."""

    def setUp(self):
        self._lakebase = _OfflineLakebase().install(self)

    def _bridge(self, *, daemon=True, **options):
        bridge = informix_module.PurePythonInformixBridge(
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
        if daemon:
            bridge.options[informix_module._DAEMON_SLOT_MARKER_OPTION] = "true"
        return bridge

    def _stop_heartbeat(self, bridge):
        stop = getattr(bridge, "_connection_slot_heartbeat_stop", None)
        if stop is not None:
            stop.set()

    def test_turnover_resets_the_deadline_and_acquires(self):
        # Pool keeps claiming slots (token climbs) for three sweeps while every slot is
        # busy, then one frees and the daemon wins. The wait must not give up meanwhile.
        bridge = self._bridge()
        real_acquire = informix_module.acquire_slot
        calls = {"n": 0}

        def acquire(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] <= 3:
                return None
            return real_acquire(*args, **kwargs)

        token = {"v": 0}

        def climbing_token(*_args, **_kwargs):
            token["v"] += 1
            return token["v"]

        with (
            mock.patch.object(informix_module, "acquire_slot", side_effect=acquire),
            mock.patch.object(informix_module, "pool_progress_token", side_effect=climbing_token),
            mock.patch.object(informix_module.random, "uniform", return_value=0),
        ):
            bridge._acquire_connection_slot()
        self.addCleanup(self._stop_heartbeat, bridge)

        self.assertIsNotNone(bridge._lakebase_slot)

    def test_wedged_pool_raises_the_specific_capacity_error(self):
        bridge = self._bridge()

        with (
            mock.patch.object(informix_module, "acquire_slot", return_value=None),
            mock.patch.object(informix_module, "pool_progress_token", return_value=7),
            mock.patch.object(informix_module.random, "uniform", return_value=0),
        ):
            with self.assertRaises(informix_module.ConnectionCapacityUnavailable) as caught:
                bridge._acquire_connection_slot(budget_seconds=0.0)

        self.assertIn("wedged", str(caught.exception))


class ConnectionStartupJitterTests(unittest.TestCase):
    """A reader sleeps a one-shot jittered delay before its first connection so a pipeline
    starting dozens of flows does not open all their CDC sessions at once."""

    def _bridge(self, **options):
        return informix_module.PurePythonInformixBridge(
            {
                "hostname": "localhost",
                "database": "demo",
                "user": "informix",
                "password": "secret",
                "server": "demo_on",
                "lakebase.password": "test-state-password",
                **options,
            }
        )

    def test_jitter_sleeps_once_before_first_connect(self):
        bridge = self._bridge(**{"connection.startup.jitter.seconds": "5"})
        with (
            mock.patch.object(informix_module.random, "uniform", return_value=1.23) as unif,
            mock.patch.object(informix_module.time, "sleep") as slept,
        ):
            bridge._apply_startup_jitter()
            bridge._apply_startup_jitter()  # one-shot: the second call does nothing

        unif.assert_called_once_with(0, 5.0)
        slept.assert_called_once_with(1.23)
        self.assertTrue(bridge._startup_jitter_applied)

    def test_disabled_by_default(self):
        bridge = self._bridge()
        with mock.patch.object(informix_module.time, "sleep") as slept:
            bridge._apply_startup_jitter()
        slept.assert_not_called()
        self.assertTrue(bridge._startup_jitter_applied)  # flag still set, so no re-check

    def test_zero_does_not_sleep(self):
        bridge = self._bridge(**{"connection.startup.jitter.seconds": "0"})
        with mock.patch.object(informix_module.time, "sleep") as slept:
            bridge._apply_startup_jitter()
        slept.assert_not_called()

    def test_ensure_connected_applies_jitter_before_acquiring_a_slot(self):
        # The jitter runs inside _ensure_connected before slot acquisition, so a jittering
        # reader holds no slot while it waits. Assert ordering via a recording sequence.
        bridge = self._bridge(**{"connection.startup.jitter.seconds": "3"})
        bridge._connected = False
        order = []
        with (
            mock.patch.object(informix_module.random, "uniform", return_value=0.5),
            mock.patch.object(
                informix_module.time, "sleep", side_effect=lambda *_: order.append("jitter")
            ),
            mock.patch.object(
                bridge, "_acquire_connection_slot", side_effect=lambda *a, **k: order.append("slot")
            ),
            mock.patch.object(
                bridge, "_connect_transport", side_effect=lambda *a, **k: order.append("connect")
            ),
        ):
            bridge._ensure_connected()

        self.assertEqual(order, ["jitter", "slot", "connect"])


class SnapshotDrainSlotLivenessTests(unittest.TestCase):
    """The snapshot-drain worker's slot wait is a liveness watchdog: it resets while the
    pool is turning over (busy doing work) and only gives up when the pool is wedged.
    Every other caller keeps the plain ``connection.wait.timeout.seconds`` deadline."""

    def setUp(self):
        self._lakebase = _OfflineLakebase().install(self)

    def _bridge(self, *, drain=True, **options):
        bridge = informix_module.PurePythonInformixBridge(
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
        if drain:
            bridge.options[informix_module._SNAPSHOT_DAEMON_SLOT_MARKER_OPTION] = "true"
        return bridge

    def _stop_heartbeat(self, bridge):
        stop = getattr(bridge, "_connection_slot_heartbeat_stop", None)
        if stop is not None:
            stop.set()

    def test_turnover_resets_the_deadline_and_pings_then_acquires(self):
        # Pool keeps claiming slots (token climbs) for three sweeps while our slot is
        # busy, then a slot frees and we win. The wait must not give up, and each turnover
        # must ping the consumer's liveness callback.
        bridge = self._bridge()
        pinged = mock.Mock()
        bridge._slot_wait_liveness = pinged
        real_acquire = informix_module.acquire_slot
        calls = {"n": 0}

        def acquire(*args, **kwargs):
            calls["n"] += 1
            if calls["n"] <= 3:
                return None  # slot still busy
            return real_acquire(*args, **kwargs)  # now free -- really claim it

        token = {"v": 0}

        def climbing_token(*_args, **_kwargs):
            token["v"] += 1
            return token["v"]

        with (
            mock.patch.object(informix_module, "acquire_slot", side_effect=acquire),
            mock.patch.object(informix_module, "pool_progress_token", side_effect=climbing_token),
            mock.patch.object(informix_module.random, "uniform", return_value=0),
        ):
            bridge._acquire_connection_slot()
        self.addCleanup(self._stop_heartbeat, bridge)

        self.assertIsNotNone(bridge._lakebase_slot)  # eventually won a slot
        self.assertEqual(pinged.call_count, 3)  # one ping per turnover before the win

    def test_wedged_pool_raises_the_specific_capacity_error(self):
        # Token frozen (no slot ever claimed) -> the pool is wedged, not merely busy, so
        # the worker gives up with the precise message instead of waiting forever.
        bridge = self._bridge()
        pinged = mock.Mock()
        bridge._slot_wait_liveness = pinged

        with (
            mock.patch.object(informix_module, "acquire_slot", return_value=None),
            mock.patch.object(informix_module, "pool_progress_token", return_value=7),
            mock.patch.object(informix_module.random, "uniform", return_value=0),
        ):
            with self.assertRaises(informix_module.ConnectionCapacityUnavailable) as caught:
                bridge._acquire_connection_slot(budget_seconds=0.0)

        self.assertIn("wedged", str(caught.exception))
        pinged.assert_not_called()  # no turnover -> no liveness ping

    def test_non_drain_reader_never_polls_the_token(self):
        # A consumer read (no drain marker) keeps the plain hard deadline: it must fail its
        # microbatch and retry rather than wait out a backfill.
        bridge = self._bridge(drain=False)

        with (
            mock.patch.object(informix_module, "acquire_slot", return_value=None),
            mock.patch.object(informix_module, "pool_progress_token") as token,
            mock.patch.object(informix_module.random, "uniform", return_value=0),
        ):
            with self.assertRaises(informix_module.ConnectionCapacityUnavailable) as caught:
                bridge._acquire_connection_slot(budget_seconds=0.0)

        token.assert_not_called()
        self.assertNotIn("wedged", str(caught.exception))


class UniqueIndexPrimaryKeyPromotionTests(unittest.TestCase):
    """A physically key-less table with no primary key is promoted to keyed on a UNIQUE
    index, so it takes the resumable keyed path instead of the one-shot key-less drain.
    Selection is deterministic: fewest columns, ties broken by index name ascending, and
    only all-NOT-NULL indexes qualify (a primary key must be non-null)."""

    @staticmethod
    def _uidx(name, *columns):
        # (idxname, part1..part16) as a positional row, key columns then zero padding.
        return (name, *columns) + (0,) * (16 - len(columns))

    def test_selects_a_single_not_null_index(self):
        key = informix_module._select_unique_index_key(
            [("u_a", [1, 0])], {1: "a", 2: "b"}, {"a": False, "b": False}
        )
        self.assertEqual(key, [("a", False)])

    def test_prefers_the_index_with_fewest_columns(self):
        indexes = [("u_wide", [1, 2, 0]), ("u_narrow", [3, 0])]
        key = informix_module._select_unique_index_key(
            indexes, {1: "a", 2: "b", 3: "c"}, {"a": False, "b": False, "c": False}
        )
        self.assertEqual(key, [("c", False)])

    def test_breaks_column_count_ties_on_index_name_ascending(self):
        indexes = [("u_z", [1, 0]), ("u_a", [2, 0])]  # both one column
        key = informix_module._select_unique_index_key(
            indexes, {1: "a", 2: "b"}, {"a": False, "b": False}
        )
        self.assertEqual(key, [("b", False)])  # chosen from u_a, the lexically first name

    def test_skips_an_index_with_a_nullable_column(self):
        indexes = [("u_null", [3, 0]), ("u_ok", [1, 2, 0])]
        key = informix_module._select_unique_index_key(
            indexes, {1: "a", 2: "b", 3: "n"}, {"a": False, "b": False, "n": True}
        )
        # u_null is fewer columns but nullable, so the wider all-NOT-NULL index wins.
        self.assertEqual(key, [("a", False), ("b", False)])

    def test_descending_column_is_included_by_magnitude_in_key_order(self):
        # A descending index column is a negative part; it is still part of the key,
        # and its direction is reported so paging can match the index's own order.
        key = informix_module._select_unique_index_key(
            [("u", [-2, 1, 0])], {1: "a", 2: "b"}, {"a": False, "b": False}
        )
        self.assertEqual(key, [("b", True), ("a", False)])

    def test_unknown_column_disqualifies_the_index(self):
        key = informix_module._select_unique_index_key([("u", [9, 0])], {1: "a"}, {"a": False})
        self.assertEqual(key, [])

    def test_no_unique_index_stays_key_less(self):
        self.assertEqual(informix_module._select_unique_index_key([], {1: "a"}, {"a": False}), [])

    def test_describe_promotes_unique_index_when_no_primary_key(self):
        class CatalogTransport:
            def execute(self, sql, parameters=(), max_result_bytes=None):
                if "syscolumns" in sql:
                    return [
                        {"colname": "id", "coltype": 258, "collength": 4, "colno": 1, "tabid": 7},
                        {"colname": "code", "coltype": 258, "collength": 8, "colno": 2, "tabid": 7},
                    ]
                if "constrtype='P'" in sql:
                    return []  # no primary-key constraint
                if "idxtype='U'" in sql:
                    return [UniqueIndexPrimaryKeyPromotionTests._uidx("u_code", 2)]
                return []

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.config = {"database": "demo"}
        bridge.transport = CatalogTransport()

        table = bridge._describe_table("app", "orders")
        self.assertEqual(table["primary_keys"], ["code"])

    def test_describe_stays_key_less_when_promotion_is_disabled(self):
        seen = []

        class CatalogTransport:
            def execute(self, sql, parameters=(), max_result_bytes=None):
                seen.append(sql)
                if "syscolumns" in sql:
                    return [
                        {"colname": "id", "coltype": 258, "collength": 4, "colno": 1, "tabid": 7}
                    ]
                return []

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {"primary.key.from.unique.index": "false"}
        bridge.config = {"database": "demo"}
        bridge.transport = CatalogTransport()

        table = bridge._describe_table("app", "orders")
        self.assertEqual(table["primary_keys"], [])
        self.assertFalse(any("idxtype='U'" in sql for sql in seen))  # not even queried


class NullableIndexPromotionTests(unittest.TestCase):
    """A UNIQUE index with a nullable column is normally rejected as a key. The per-table
    allow.nullable.index option lets the operator opt in when no non-null key exists."""

    @staticmethod
    def _catalog_bridge():
        class CatalogTransport:
            def execute(self, sql, parameters=(), max_result_bytes=None):
                del parameters, max_result_bytes
                if "syscolumns" in sql:
                    # code is nullable; the only unique index (u_code) covers it.
                    return [
                        {
                            "colname": "id",
                            "coltype": 258,
                            "collength": 4,
                            "colno": 1,
                            "tabid": 7,
                        },
                        {
                            "colname": "code",
                            "coltype": 2,
                            "collength": 8,
                            "colno": 2,
                            "tabid": 7,
                        },
                    ]
                if "constrtype='P'" in sql:
                    return []
                if "idxtype='U'" in sql:
                    return [UniqueIndexPrimaryKeyPromotionTests._uidx("u_code", 2)]
                return []

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.config = {"database": "demo"}
        bridge.transport = CatalogTransport()
        return bridge

    def test_select_allows_nullable_columns_when_requested(self):
        indexes = [("u", [1, 0])]
        # Nullable column: rejected by default, accepted when allow_nullable is set.
        self.assertEqual(
            informix_module._select_unique_index_key(indexes, {1: "a"}, {"a": True}), []
        )
        self.assertEqual(
            informix_module._select_unique_index_key(
                indexes, {1: "a"}, {"a": True}, allow_nullable=True
            ),
            [("a", False)],
        )

    def test_describe_reports_nullable_candidate_but_not_a_key(self):
        # Discovery leaves the table key-less (the only unique index has a nullable
        # column) but records the candidate for the connector to promote.
        table = self._catalog_bridge()._describe_table("app", "orders")
        self.assertEqual(table["primary_keys"], [])
        self.assertEqual(table["nullable_index_key"], [["code", False]])

    def test_promotion_off_by_default(self):
        connector = object.__new__(InformixLakeflowConnect)
        table = informix_module.Table.parse(
            self._catalog_bridge()._describe_table("app", "orders"), "demo"
        )
        promoted = connector._apply_nullable_index_promotion(table, {})
        self.assertEqual(promoted.primary_keys, ())  # still key-less
        self.assertFalse(promoted.key_from_nullable_index)

    def test_promotion_on_when_opted_in(self):
        connector = object.__new__(InformixLakeflowConnect)
        table = informix_module.Table.parse(
            self._catalog_bridge()._describe_table("app", "orders"), "demo"
        )
        promoted = connector._apply_nullable_index_promotion(
            table, {"allow.nullable.index": "true"}
        )
        self.assertEqual(promoted.primary_keys, ("code",))
        self.assertTrue(promoted.key_from_nullable_index)

    def test_explicit_primary_keys_wins_over_promotion(self):
        connector = object.__new__(InformixLakeflowConnect)
        connector._override_direction_cache = {}
        connector._bridge_instance = self._catalog_bridge()
        table = informix_module.Table.parse(
            self._catalog_bridge()._describe_table("app", "orders"), "demo"
        )
        # Both options set: primary.keys is applied last and takes precedence.
        keyed = connector._apply_key_options(
            table, {"allow.nullable.index": "true", "primary.keys": "id"}
        )
        self.assertEqual(keyed.primary_keys, ("id",))
        self.assertTrue(keyed.key_override)
        self.assertFalse(keyed.key_from_nullable_index)

    def test_refresh_preserves_nullable_promotion(self):
        connector = object.__new__(InformixLakeflowConnect)
        connector._tables = None
        connector._bridge_instance = self._catalog_bridge()
        table = informix_module.Table.parse(
            self._catalog_bridge()._describe_table("app", "orders"), "demo"
        )
        promoted = connector._apply_nullable_index_promotion(
            table, {"allow.nullable.index": "true"}
        )

        refreshed = connector._refresh_table_schema(promoted, None)

        self.assertEqual(refreshed.primary_keys, ("code",))
        self.assertTrue(refreshed.key_from_nullable_index)


class MixedDirectionKeyTests(unittest.TestCase):
    """A key index with a DESC column is paged in the index's own order so a forward
    scan serves it without a sort. Directions flow from catalog discovery through the
    Table to the paging queries and the key-bound read; an all-ascending key is
    unaffected (carries no directions and behaves exactly as before)."""

    @staticmethod
    def _pk_row(*parts):
        # A sysconstraints/sysindexes key row: part1..part16, key columns then 0 pad.
        padded = list(parts) + [0] * (16 - len(parts))
        return {f"part{i + 1}": padded[i] for i in range(16)}

    def test_order_key_orders_descending_columns_in_reverse(self):
        order_key = informix_module._order_key
        desc = (False, True)  # (ASC, DESC)
        # Within one leading value, the DESC column ranks a larger value first.
        self.assertLess(order_key((1, 9), desc), order_key((1, 5), desc))
        # The ascending leading column still dominates.
        self.assertLess(order_key((1, 0), desc), order_key((2, 99), desc))
        # Equality and >= behave.
        self.assertTrue(order_key((1, 5), desc) >= order_key((1, 5), desc))
        self.assertFalse(order_key((1, 5), desc) > order_key((1, 5), desc))
        # Empty / all-ascending is the plain tuple order, untouched.
        self.assertEqual(order_key((1, 2), ()), (1, 2))
        self.assertLess(order_key((1, 5), (False, False)), order_key((1, 9), (False, False)))

    def test_primary_key_constraint_captures_column_directions(self):
        class CatalogTransport:
            def execute(self, sql, parameters=(), max_result_bytes=None):
                del parameters, max_result_bytes
                if "syscolumns" in sql:
                    return [
                        {"colname": "a", "coltype": 258, "collength": 4, "colno": 1, "tabid": 7},
                        {"colname": "b", "coltype": 258, "collength": 4, "colno": 2, "tabid": 7},
                    ]
                if "constrtype='P'" in sql:
                    return [MixedDirectionKeyTests._pk_row(1, -2)]  # a ASC, b DESC
                return []

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.config = {"database": "demo"}
        bridge.transport = CatalogTransport()

        table = bridge._describe_table("app", "t")
        self.assertEqual(table["primary_keys"], ["a", "b"])
        self.assertEqual(table["key_descending"], [False, True])

    def test_all_ascending_key_carries_no_directions(self):
        class CatalogTransport:
            def execute(self, sql, parameters=(), max_result_bytes=None):
                del parameters, max_result_bytes
                if "syscolumns" in sql:
                    return [
                        {"colname": "a", "coltype": 258, "collength": 4, "colno": 1, "tabid": 7},
                        {"colname": "b", "coltype": 258, "collength": 4, "colno": 2, "tabid": 7},
                    ]
                if "constrtype='P'" in sql:
                    return [MixedDirectionKeyTests._pk_row(1, 2)]
                return []

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.config = {"database": "demo"}
        bridge.transport = CatalogTransport()

        table = bridge._describe_table("app", "t")
        self.assertEqual(table["primary_keys"], ["a", "b"])
        self.assertEqual(table["key_descending"], [])  # normalized: nothing extra to carry

    def test_table_parse_round_trips_directions_and_checks_arity(self):
        raw = {
            "database": "demo",
            "owner": "app",
            "name": "t",
            "columns": [
                {"name": "a", "type_name": "INTEGER", "nullable": False},
                {"name": "b", "type_name": "INTEGER", "nullable": False},
            ],
            "primary_keys": ["a", "b"],
            "key_descending": [False, True],
        }
        table = informix_module.Table.parse(raw, "demo")
        self.assertEqual(table.key_descending, (False, True))
        # A directions list that does not match the key arity is rejected.
        with self.assertRaisesRegex(informix_module.InformixError, "Key direction metadata"):
            informix_module.Table.parse({**raw, "key_descending": [True]}, "demo")

    @staticmethod
    def _keyless_table(*columns):
        return informix_module.Table(
            "demo",
            "app",
            "t",
            tuple(
                informix_module.Column(name, "INTEGER", False, 4, None, None, True)
                for name in columns
            ),
            (),  # physically keyless; a primary.keys override supplies the key
        )

    @staticmethod
    def _override_connector(index_directions):
        connector = object.__new__(InformixLakeflowConnect)
        connector._override_direction_cache = {}

        class _StubBridge:
            def __init__(self):
                self.calls = 0

            def key_index_directions(self, identity, columns):
                del identity
                self.calls += 1
                return index_directions.get(tuple(columns))

        connector._bridge_instance = _StubBridge()
        return connector

    def test_override_adopts_a_matching_index_direction(self):
        # primary.keys names the columns of the (a ASC, b DESC) index; the override
        # picks up that index's directions so it pages in the index's own order.
        connector = self._override_connector({("a", "b"): [False, True]})
        table = self._keyless_table("a", "b")

        overridden = connector._apply_primary_key_override(table, {"primary.keys": "a,b"})

        self.assertEqual(overridden.primary_keys, ("a", "b"))
        self.assertEqual(overridden.key_descending, (False, True))

    def test_override_pages_ascending_when_no_index_matches(self):
        # No index matches the override columns, so paging stays all-ascending.
        connector = self._override_connector({})
        table = self._keyless_table("a", "b")

        overridden = connector._apply_primary_key_override(table, {"primary.keys": "a,b"})

        self.assertEqual(overridden.primary_keys, ("a", "b"))
        self.assertEqual(overridden.key_descending, ())

    def test_override_lookup_is_memoized_per_table_and_key(self):
        connector = self._override_connector({("a", "b"): [False, True]})
        table = self._keyless_table("a", "b")

        for _ in range(3):
            connector._apply_primary_key_override(table, {"primary.keys": "a,b"})

        self.assertEqual(connector._bridge_instance.calls, 1)  # one catalog lookup, then cached

    def test_override_restating_catalog_key_keeps_discovered_directions(self):
        # When primary.keys just restates the discovered key, its already-known
        # directions are kept and no catalog lookup happens.
        connector = self._override_connector({})  # would return None if consulted
        table = informix_module.Table(
            "demo",
            "app",
            "t",
            (
                informix_module.Column("a", "INTEGER", False, 4, None, None, True),
                informix_module.Column("b", "INTEGER", False, 4, None, None, True),
            ),
            ("a", "b"),
            key_descending=(False, True),
        )

        overridden = connector._apply_primary_key_override(table, {"primary.keys": "a,b"})

        self.assertTrue(overridden.key_override)
        self.assertEqual(overridden.key_descending, (False, True))
        self.assertEqual(connector._bridge_instance.calls, 0)  # discovered directions reused

    def test_refresh_preserves_override_directions(self):
        # A schema refresh of an overridden table must keep the adopted key-index
        # directions, or a mixed-direction override would revert to ascending
        # paging mid-copy (and trip the key-order guard).
        connector = object.__new__(InformixLakeflowConnect)
        connector._tables = None

        class _StubBridge:
            def get_table(self, identity):
                del identity
                return {
                    "database": "demo",
                    "owner": "app",
                    "name": "t",
                    "columns": [
                        {"name": "a", "type_name": "INTEGER", "nullable": False},
                        {"name": "b", "type_name": "INTEGER", "nullable": False},
                    ],
                    "primary_keys": [],  # physically keyless; the override supplies the key
                }

        connector._bridge_instance = _StubBridge()
        overridden = informix_module.Table(
            "demo",
            "app",
            "t",
            (
                informix_module.Column("a", "INTEGER", False, 4, None, None, True),
                informix_module.Column("b", "INTEGER", False, 4, None, None, True),
            ),
            ("a", "b"),
            key_override=True,
            key_descending=(False, True),
        )

        refreshed = connector._refresh_table_schema(overridden, None)

        self.assertEqual(refreshed.primary_keys, ("a", "b"))
        self.assertEqual(refreshed.key_descending, (False, True))

    def test_key_index_directions_matches_index_columns_in_order(self):
        class CatalogTransport:
            def execute(self, sql, parameters=(), max_result_bytes=None):
                del parameters, max_result_bytes
                if "syscolumns" in sql:
                    return [
                        {"colname": "a", "colno": 1},
                        {"colname": "b", "colno": 2},
                        {"colname": "c", "colno": 3},
                    ]
                if "sysindexes" in sql:
                    # One index on (a ASC, b DESC); b is a negative part.
                    return [MixedDirectionKeyTests._pk_row(1, -2)]
                return []

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.config = {"database": "demo"}
        bridge.transport = CatalogTransport()

        self.assertEqual(bridge.key_index_directions("demo.app.t", ["a", "b"]), [False, True])
        # A column set no index covers in that order gets no directions.
        self.assertIsNone(bridge.key_index_directions("demo.app.t", ["b", "a"]))
        self.assertIsNone(bridge.key_index_directions("demo.app.t", ["c"]))


class NullByteOptionTests(unittest.TestCase):
    """The string.null.byte option resolves to null (default), empty, or keep, and the
    bridge threads the resolved mode into both decode paths."""

    def test_default_is_null(self):
        self.assertEqual(informix_module._null_byte_mode({}), "null")

    def test_explicit_modes_and_alias(self):
        self.assertEqual(informix_module._null_byte_mode({"string.null.byte": "empty"}), "empty")
        self.assertEqual(informix_module._null_byte_mode({"string.null.byte": "KEEP"}), "keep")
        self.assertEqual(
            informix_module._null_byte_mode({"string.null.byte": "empty_string"}), "empty"
        )
        self.assertEqual(informix_module._null_byte_mode({"string.null.byte": ""}), "null")

    def test_invalid_mode_raises(self):
        with self.assertRaisesRegex(ValueError, "string.null.byte"):
            informix_module._null_byte_mode({"string.null.byte": "blank"})

    def test_bridge_threads_mode_to_the_sqli_client(self):
        bridge = object.__new__(informix_module.PurePythonInformixBridge)
        options = {
            "hostname": "localhost",
            "database": "demo",
            "user": "informix",
            "password": "secret",
            "server": "demo_on",
            "string.null.byte": "empty",
        }
        informix_module.PurePythonInformixBridge.__init__(bridge, options)
        self.assertEqual(bridge._null_byte, "empty")
        self.assertEqual(bridge.transport.null_byte, "empty")


class AsyncStagePipelineTests(unittest.TestCase):
    """The background uploader that overlaps page fetch with page upload."""

    def test_preserves_page_order(self):
        seen: list[int] = []
        pipeline = informix_module._AsyncStagePipeline(
            lambda _lsn, index, _rows: seen.append(index)
        )
        for index in range(6):
            pipeline.submit(10, index, [{"id": index}])
        pipeline.close()
        self.assertEqual(seen, [0, 1, 2, 3, 4, 5], "a single worker must keep page order")

    def test_close_reraises_a_worker_error(self):
        def boom(_lsn, _index, _rows):
            raise InformixError("upload failed")

        pipeline = informix_module._AsyncStagePipeline(boom)
        with self.assertRaisesRegex(InformixError, "upload failed"):
            for index in range(10):
                pipeline.submit(10, index, [{"id": index}])
            pipeline.close()

    def test_shutdown_swallows_the_worker_error(self):
        def boom(_lsn, _index, _rows):
            raise InformixError("upload failed")

        pipeline = informix_module._AsyncStagePipeline(boom)
        pipeline.submit(10, 0, [{"id": 0}])
        pipeline.shutdown()  # must join without raising, so a primary error can win

    def test_close_without_any_submit_is_a_noop(self):
        pipeline = informix_module._AsyncStagePipeline(lambda *_a: None)
        pipeline.close()  # worker never started; nothing to join or raise

    def test_backpressure_bounds_resident_pages(self):
        # With a depth-1 queue and a slow worker, submit must block rather than let
        # the producer race ahead and hold every page in memory at once.
        import threading as _threading

        release = _threading.Event()
        started = _threading.Event()

        def slow(_lsn, index, _rows):
            if index == 0:
                started.set()
                release.wait(2.0)

        pipeline = informix_module._AsyncStagePipeline(slow, queue_size=1)
        pipeline.submit(10, 0, [{"id": 0}])  # worker picks this up and blocks
        self.assertTrue(started.wait(2.0))
        pipeline.submit(10, 1, [{"id": 1}])  # fills the depth-1 queue

        blocked = _threading.Event()

        def third():
            pipeline.submit(10, 2, [{"id": 2}])  # must block until the worker drains
            blocked.set()

        producer = _threading.Thread(target=third)
        producer.start()
        self.assertFalse(blocked.wait(0.3), "submit should block while the queue is full")
        release.set()
        producer.join(2.0)
        self.assertTrue(blocked.is_set(), "submit should unblock once the worker drains")
        pipeline.close()


class UnloadHelperTests(unittest.TestCase):
    """Pure-function tests for the UNLOAD parser, typed converter, and splitter."""

    @staticmethod
    def _reader(data: bytes):
        return lambda offset, length: data[offset : offset + length]

    def test_parse_unload_fields_handles_delimiter_escapes_and_terminator(self):
        parse = informix_module._parse_unload_fields
        # Trailing terminator delimiter yields no spurious field; a line without one
        # still yields its last field.
        self.assertEqual(parse("a|b|"), ["a", "b"])
        self.assertEqual(parse("a|b"), ["a", "b"])
        # Escaped delimiter and newline stay inside the field.
        self.assertEqual(parse("a\\|b|c|"), ["a|b", "c"])
        self.assertEqual(parse("x\\ny|"), ["x\ny"])
        # An empty (NULL) field is preserved as "".
        self.assertEqual(parse("|a|"), ["", "a"])

    def test_parse_unload_fields_supports_hpl_custom_delimiter_and_escape(self):
        parse = informix_module._parse_unload_fields
        # HPL DELIMITED is the same shape with a job-configured delimiter/escape and no
        # trailing delimiter: a tab-delimited record with a `~` escape parses like UNLOAD.
        self.assertEqual(parse("a\tb\tc", delimiter="\t"), ["a", "b", "c"])
        self.assertEqual(parse("a~\tb\tc", delimiter="\t", escape="~"), ["a\tb", "c"])
        self.assertEqual(parse("x~ny", delimiter="\t", escape="~"), ["x\ny"])
        # `d1 d2` empty field is NULL for both writers, regardless of the delimiter.
        self.assertEqual(parse("a;;c", delimiter=";"), ["a", "", "c"])
        # An empty escape disables escape processing: a backslash is literal data.
        self.assertEqual(parse("a\\|b", escape=""), ["a\\", "b"])

    def test_unload_value_maps_types_and_empty_to_none(self):
        value = informix_module._unload_value
        self.assertIsNone(value("", "INTEGER"))
        self.assertEqual(value("42", "INTEGER"), 42)
        self.assertEqual(value("3.14", "DECIMAL(6,2)"), Decimal("3.14"))
        self.assertEqual(value("2.5", "FLOAT"), 2.5)
        self.assertEqual(value("2024-01-15", "DATE"), date(2024, 1, 15))
        self.assertEqual(
            value("2024-01-15 10:30:00", "DATETIME YEAR TO SECOND"),
            datetime(2024, 1, 15, 10, 30, 0),
        )
        self.assertTrue(value("t", "BOOLEAN"))
        self.assertFalse(value("f", "BOOLEAN"))
        self.assertEqual(value("hello", "VARCHAR"), "hello")

    def test_unload_value_renders_every_datetime_qualifier_as_the_live_decoders(self):
        # The qualifier is Column.length: start field in bits 8-11, end in bits 0-3
        # (YEAR=0, MONTH=2, DAY=4, HOUR=6, MINUTE=8, SECOND=10, FRACTION(n)=10+n).
        value = informix_module._unload_value
        self.assertEqual(
            value("2024-05-15 10:30:00.12345", "DATETIME", 0x00F),
            datetime(2024, 5, 15, 10, 30, 0, 123450),
        )
        self.assertEqual(value("2024-05-15", "DATETIME", 0x004), datetime(2024, 5, 15))
        self.assertEqual(value("10:30:00", "DATETIME", 0x60A), "10:30:00")
        self.assertEqual(value("10:30:00.123", "DATETIME", 0x60D), "10:30:00.123")
        self.assertEqual(value("2024-05", "DATETIME", 0x002), "YEAR=2024:MONTH=05")
        self.assertEqual(value("05-15", "DATETIME", 0x204), "MONTH=05:DAY=15")
        self.assertEqual(value("15 10", "DATETIME", 0x406), "DAY=15:HOUR=10")
        # Year 1 carries UTC, as the packed decoder's value does.
        self.assertEqual(
            value("0001-01-01 00:00:00", "DATETIME", 0x00A),
            datetime(1, 1, 1, tzinfo=timezone.utc),
        )
        with self.assertRaisesRegex(Exception, "does not match its 6 TO 10 qualifier"):
            value("10:30", "DATETIME", 0x60A)

    def test_unload_value_drops_char_padding_like_the_live_decoders(self):
        value = informix_module._unload_value
        self.assertEqual(value("ab   ", "CHAR"), "ab")
        self.assertEqual(value("ab   ", "NCHAR"), "ab")
        self.assertEqual(value("ab   ", "VARCHAR"), "ab   ")

    def test_a_unl_record_applies_the_null_byte_mode_before_the_row_filter(self):
        columns = (
            informix_module.Column(name="id", type_name="INTEGER"),
            informix_module.Column(name="note", type_name="VARCHAR"),
        )
        table = informix_module.Table(
            database="demo", owner="app", name="notes", columns=columns, primary_keys=("id",)
        )
        shape = informix_module._shape_unl_record
        record = b"1|a\x00b|"
        self.assertIsNone(shape(record, table, 100, {}, "utf-8")["note"])  # default: null
        self.assertEqual(shape(record, table, 100, {}, "utf-8", null_byte="empty")["note"], "ab")
        self.assertEqual(shape(record, table, 100, {}, "utf-8", null_byte="keep")["note"], "a\x00b")

    def test_unl_byte_splits_cover_every_record_exactly_once(self):
        # The correctness gate: splits must tile [0,size) with no gap/overlap, each split
        # must end right after a newline (records never span a boundary), and their union
        # must be every record exactly once -- across empty, single, and multi-record
        # sizes, with a tiny target/chunk so boundary-snapping crosses chunks.
        for count in (0, 1, 3, 6, 7):
            data = b"".join(b"row-%03d|\n" % i for i in range(count))
            reader = self._reader(data)
            splits = informix_module._unl_byte_splits(
                reader, len(data), target_bytes=10, chunk_bytes=3
            )
            if splits:
                self.assertEqual(splits[0][0], 0)
                self.assertEqual(splits[-1][1], len(data))
                for (_, end), (start, _) in zip(splits, splits[1:]):
                    self.assertEqual(end, start)  # contiguous, no gap/overlap
                    self.assertEqual(data[end - 1 : end], b"\n")  # boundary after a newline
            records = []
            for start, end in splits:
                records.extend(informix_module._iter_unl_records(reader, start, end, chunk_bytes=4))
            self.assertEqual(records, [b"row-%03d|" % i for i in range(count)])

    def test_unl_byte_splits_target_bounds_the_split_size(self):
        # A ~10-byte target over 8-byte records ("row-NNN|\n") cuts one record per split.
        data = b"".join(b"row-%03d|\n" % i for i in range(5))
        reader = self._reader(data)
        splits = informix_module._unl_byte_splits(reader, len(data), target_bytes=1, chunk_bytes=64)
        # target=1 snaps to the very next newline, so every record is its own split.
        self.assertEqual(len(splits), 5)
        self.assertTrue(all(end - start == 9 for start, end in splits))

    def test_unl_byte_splits_include_a_trailing_unterminated_record(self):
        data = b"a|\nb|"  # second record has no trailing newline
        reader = self._reader(data)
        splits = informix_module._unl_byte_splits(reader, len(data), target_bytes=1, chunk_bytes=3)
        records = []
        for start, end in splits:
            records.extend(list(informix_module._iter_unl_records(reader, start, end)))
        self.assertEqual(records, [b"a|", b"b|"])

    # UNLOAD writes an embedded newline as backslash + newline and a literal backslash
    # as a doubled backslash. Record 2 below holds both, plus a value ending in an
    # escaped backslash right before its terminator (an even run: still a terminator).
    _ESCAPED = b"1|plain|\n2|line one\\\nline two|back\\\\slash|\n3|end\\\\|\n4|x|\n"
    _ESCAPED_RECORDS = [
        b"1|plain|",
        b"2|line one\\\nline two|back\\\\slash|",
        b"3|end\\\\|",
        b"4|x|",
    ]

    def test_iter_unl_records_keeps_an_escaped_newline_inside_the_record(self):
        # Regression: the reader split on every physical newline, so a record whose
        # value holds an escaped newline came out as two fragments with the wrong
        # field count (and the serve failed closed on it).
        for chunk_bytes in (1, 2, 3, 4, 7, 64):
            records = list(
                informix_module._iter_unl_records(
                    self._reader(self._ESCAPED), 0, len(self._ESCAPED), chunk_bytes=chunk_bytes
                )
            )
            self.assertEqual(records, self._ESCAPED_RECORDS, chunk_bytes)
        # The record parser then decodes the escaped newline back into the value.
        fields = informix_module._parse_unload_fields(self._ESCAPED_RECORDS[1].decode())
        self.assertEqual(fields, ["2", "line one\nline two", "back\\slash"])

    def test_unl_byte_splits_never_cut_at_an_escaped_newline(self):
        # Every cut point must land after an *unescaped* newline, including when the
        # target lands exactly on the escaped newline (or its escape byte), so the
        # boundary finder has to look before the target to classify it.
        data = self._ESCAPED
        reader = self._reader(data)
        for target in range(1, len(data) + 1):
            for chunk_bytes in (1, 3, 64):
                splits = informix_module._unl_byte_splits(
                    reader, len(data), target_bytes=target, chunk_bytes=chunk_bytes
                )
                self.assertEqual(splits[0][0], 0)
                self.assertEqual(splits[-1][1], len(data))
                records = []
                for start, end in splits:
                    if end < len(data):
                        self.assertEqual(data[end - 1 : end], b"\n")
                        self.assertNotEqual(data[end - 2 : end], b"\\\n")
                    records.extend(
                        informix_module._iter_unl_records(
                            reader, start, end, chunk_bytes=chunk_bytes
                        )
                    )
                self.assertEqual(records, self._ESCAPED_RECORDS, (target, chunk_bytes))

    def test_unl_escape_disabled_treats_every_newline_as_a_terminator(self):
        data = b"a\\\nb|\nc|\n"
        reader = self._reader(data)
        plain = list(informix_module._iter_unl_records(reader, 0, len(data), escape=b""))
        self.assertEqual(plain, [b"a\\", b"b|", b"c|"])
        splits = informix_module._unl_byte_splits(
            reader, len(data), target_bytes=1, chunk_bytes=2, escape=b""
        )
        self.assertEqual(len(splits), 3)
        # An HPL job's own escape character is honoured the same way.
        data = b"a~\nb|\nc|\n"
        reader = self._reader(data)
        self.assertEqual(
            list(informix_module._iter_unl_records(reader, 0, len(data), escape=b"~")),
            [b"a~\nb|", b"c|"],
        )

    def test_iter_gzip_unl_records_honours_escaped_newlines(self):
        compressed = gzip.compress(self._ESCAPED)
        for chunk_bytes in (1, 4, 64):
            records = list(
                informix_module._iter_gzip_unl_records(
                    self._reader(compressed), len(compressed), chunk_bytes=chunk_bytes
                )
            )
            self.assertEqual(records, self._ESCAPED_RECORDS, chunk_bytes)

    def test_unl_escape_byte_requires_a_single_byte_in_the_extract_encoding(self):
        self.assertEqual(informix_module._unl_escape_byte("\\", "utf-8"), b"\\")
        self.assertEqual(informix_module._unl_escape_byte("", "utf-8"), b"")
        with self.assertRaisesRegex(InformixError, "single byte"):
            informix_module._unl_escape_byte("\u00e9", "utf-8")

    def test_unl_file_is_gzip_detects_magic_bytes(self):
        is_gzip = informix_module._unl_file_is_gzip
        self.assertTrue(is_gzip(self._reader(gzip.compress(b"1|a|\n"))))
        self.assertFalse(is_gzip(self._reader(b"1|a|\n")))
        self.assertFalse(is_gzip(self._reader(b"")))  # empty file is not gzip

    def test_iter_gzip_unl_records_matches_the_plain_iterator(self):
        # Round-trip: the records decompressed from a gzipped .unl equal the records the
        # plain iterator yields on the same bytes -- across a trailing unterminated record
        # and a chunk size far smaller than the payload so decompression crosses chunks.
        for data in (
            b"",
            b"1|a|\n",
            b"1|a|\n2|b|\n3|c|",
            b"".join(b"r-%03d|\n" % i for i in range(20)),
        ):
            compressed = gzip.compress(data)
            gz = list(
                informix_module._iter_gzip_unl_records(
                    self._reader(compressed), len(compressed), chunk_bytes=4
                )
            )
            plain = list(informix_module._iter_unl_records(self._reader(data), 0, len(data)))
            self.assertEqual(gz, plain, data)

    def test_iter_gzip_unl_records_handles_concatenated_members(self):
        # `cat a.gz b.gz` -> two gzip members; the reader must decode both, in order.
        compressed = gzip.compress(b"1|a|\n2|b|\n") + gzip.compress(b"3|c|\n4|d|\n")
        records = list(
            informix_module._iter_gzip_unl_records(
                self._reader(compressed), len(compressed), chunk_bytes=5
            )
        )
        self.assertEqual(records, [b"1|a|", b"2|b|", b"3|c|", b"4|d|"])


class UnloadSnapshotSourceTests(LakeflowContractTests):
    """End-to-end serial serve of a `.unl` snapshot source (offline, FakeBridge)."""

    def _source(self, *lines: str) -> str:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        for index, content in enumerate(lines):
            with open(os.path.join(directory.name, f"part-{index:03d}.unl"), "w") as handle:
                handle.write(content)
        return directory.name

    def _gzip_source(self, *files: str) -> str:
        """A source dir whose `.unl.gz` files hold the given (already-joined) contents."""
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        for index, content in enumerate(files):
            path = os.path.join(directory.name, f"part-{index:03d}.unl.gz")
            with open(path, "wb") as handle:
                handle.write(gzip.compress(content.encode()))
        return directory.name

    def _write_manifest(self, source: str, **fields) -> None:
        with open(os.path.join(source, "_manifest.json"), "w") as handle:
            json.dump(fields, handle)

    def test_unl_row_commit_time_from_manifest_boundary_time(self):
        # A file-source snapshot has no live boundary to query, so `r` rows take their
        # commit time from the extract's _manifest.json boundary_time (epoch seconds).
        source = self._source("1|alpha|\n2|beta|\n")
        self._write_manifest(source, boundary_time=1_700_000_000)
        options = {"snapshot.source": source}
        rows, _ = self.connector(FakeBridge()).read_table("app.orders", {}, options)
        rows = list(rows)
        self.assertTrue(rows)
        for row in rows:
            self.assertEqual(
                row[informix_module.COMMIT_TIME],
                informix_module._commit_time_value(1_700_000_000),
            )

    def test_unl_row_commit_time_null_without_manifest_boundary_time(self):
        # No manifest boundary_time -> NULL commit time (no fabricated value).
        source = self._source("1|alpha|\n2|beta|\n")
        rows, _ = self.connector(FakeBridge()).read_table(
            "app.orders", {}, {"snapshot.source": source}
        )
        self.assertTrue(all(row[informix_module.COMMIT_TIME] is None for row in rows))

    def test_unl_manifest_boundary_time_rides_partitioned_descriptors(self):
        # The manifest boundary_time reaches the executor path via the unl_split descriptor,
        # so the partitioned fan-out stamps the same commit time as the serial serve.
        source = self._source("1|a|\n2|b|\n3|c|\n")
        self._write_manifest(source, boundary_lsn="42", boundary_time=1_700_000_000)
        options = {"snapshot.source": source}
        connector = self.connector(FakeBridge())
        offset = connector.latest_offset("app.orders", options, {})
        partitions = connector.get_partitions("app.orders", options, {}, offset)
        self.assertTrue(all(p.get("commit_time") == 1_700_000_000 for p in partitions))
        fanned = [
            row
            for partition in partitions
            for row in connector.read_partition("app.orders", partition, options)
        ]
        self.assertTrue(fanned)
        for row in fanned:
            self.assertEqual(
                row[informix_module.COMMIT_TIME],
                informix_module._commit_time_value(1_700_000_000),
            )

    def test_unl_partitions_skip_a_field_of_an_unsupported_type(self):
        # The extract holds the TEXT column's field; the descriptor names the columns
        # ingested so the executor drops it, as under exclude_columns.
        bridge = FakeBridge()
        bridge.tables[0]["columns"].insert(
            1, {"name": "note", "type_name": "TEXT", "nullable": True, "cdc_supported": False}
        )
        source = self._source("1|first note|a|\n2|second|b|\n")
        options = {"snapshot.source": source}
        connector = self.connector(bridge)
        offset = connector.latest_offset("app.orders", options, {})
        partitions = connector.get_partitions("app.orders", options, {}, offset)
        splits = [partition for partition in partitions if partition.get("kind") == "unl_split"]
        self.assertTrue(splits)
        self.assertTrue(all(split["selected"] == ["id", "value"] for split in splits))
        fanned = [
            row
            for partition in partitions
            for row in connector.read_partition("app.orders", partition, options)
        ]
        self.assertEqual(
            sorted((row["id"], row["value"], "note" in row) for row in fanned),
            [(1, "a", False), (2, "b", False)],
        )
        rows, _ = self.connector(bridge).read_table("app.orders", {}, options)
        self.assertEqual(
            sorted((row["id"], row["value"], "note" in row) for row in rows),
            [(1, "a", False), (2, "b", False)],
        )

    def test_unl_manifest_rejects_a_non_integer_boundary_time(self):
        source = self._source("1|a|\n")
        self._write_manifest(source, boundary_time="not-a-number")
        with self.assertRaisesRegex(InformixError, "boundary_time"):
            rows, _ = self.connector(FakeBridge()).read_table(
                "app.orders", {}, {"snapshot.source": source}
            )
            list(rows)

    def test_unl_source_serves_shaped_rows_and_transitions_to_stream(self):
        source = self._source("1|alpha|\n2|beta|\n")
        rows, offset = self.connector(FakeBridge()).read_table(
            "app.orders", {}, {"snapshot.source": source}
        )
        rows = list(rows)
        # Straight to the stream phase at the connector-pinned boundary (FakeBridge
        # current_lsn = 90), exactly like a completed live snapshot.
        self.assertEqual(offset["phase"], "stream")
        self.assertEqual(offset["commit_lsn"], "90")
        self.assertEqual([(row["id"], row["value"]) for row in rows], [(1, "alpha"), (2, "beta")])
        for row in rows:
            self.assertEqual(row[informix_module.OP], "r")
            self.assertIsNone(row[informix_module.TX_ID])
            self.assertEqual(row[informix_module.COMMIT_LSN], informix_module._sortable_lsn(90))
            self.assertEqual(row[informix_module.CURSOR], informix_module._sortable_lsn(90))

    def test_unl_source_reads_files_in_name_order(self):
        source = self._source("1|alpha|\n", "2|beta|\n")
        rows, _ = self.connector(FakeBridge()).read_table(
            "app.orders", {}, {"snapshot.source": source}
        )
        self.assertEqual([row["id"] for row in rows], [1, 2])

    def test_unl_gzip_source_serves_shaped_rows(self):
        # A gzip-compressed .unl.gz extract shapes to the same rows as a plain one and
        # transitions to the stream phase at the pinned boundary.
        source = self._gzip_source("1|alpha|\n2|beta|\n")
        rows, offset = self.connector(FakeBridge()).read_table(
            "app.orders", {}, {"snapshot.source": source}
        )
        rows = list(rows)
        self.assertEqual(offset["phase"], "stream")
        self.assertEqual([(r["id"], r["value"]) for r in rows], [(1, "alpha"), (2, "beta")])
        for row in rows:
            self.assertEqual(row[informix_module.OP], "r")

    def test_unl_all_splits_flags_gzip_whole_file_and_byte_splits_plain(self):
        # A mixed directory: the gzip file is one non-splittable whole-file split
        # (gzip=True, spanning [0, size)); the plain file byte-splits (gzip=False).
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        with open(os.path.join(directory.name, "a.unl"), "wb") as handle:
            handle.write(b"1|a|\n2|b|\n3|c|\n")
        gz_path = os.path.join(directory.name, "b.unl.gz")
        with open(gz_path, "wb") as handle:
            handle.write(gzip.compress(b"4|d|\n5|e|\n"))

        connector = self.connector(FakeBridge())
        table = connector._table("app.orders", {})
        options = {"snapshot.source": directory.name, "snapshot.split.bytes": "1"}
        splits = connector._unl_all_splits(table, options)
        gzip_splits = [s for s in splits if s[0] == gz_path]
        plain_splits = [s for s in splits if s[0] != gz_path]
        self.assertEqual(len(gzip_splits), 1)
        self.assertTrue(gzip_splits[0][3])  # gzip flag
        self.assertEqual(gzip_splits[0][1], 0)
        self.assertEqual(gzip_splits[0][2], os.path.getsize(gz_path))
        self.assertEqual(len(plain_splits), 3)  # target=1 -> one split per record
        self.assertTrue(all(not s[3] for s in plain_splits))

    def test_unl_gzip_partitioned_fan_out_matches_serial(self):
        # A gzip file fans out as a single unl_split (gzip=True) whose read_partition
        # output equals the serial serve, row-for-row.
        source = self._gzip_source("1|a|\n2|b|\n3|c|\n")
        options = {"snapshot.source": source}
        serial = list(self.connector(FakeBridge()).read_table("app.orders", {}, options)[0])

        connector = self.connector(FakeBridge())
        offset = connector.latest_offset("app.orders", options, {})
        partitions = connector.get_partitions("app.orders", options, {}, offset)
        self.assertEqual([p["kind"] for p in partitions], ["unl_split"])
        self.assertTrue(partitions[0]["gzip"])
        fanned = [
            row
            for partition in partitions
            for row in connector.read_partition("app.orders", partition, options)
        ]
        self.assertEqual(fanned, serial)

    # --- gzip record windows (snapshot.gzip.records.per.batch) -----------------------

    _FIVE_RECORDS = "1|a|\n2|b|\n3|c|\n4|d|\n5|e|\n"

    def _serve(self, connector, options, start=None, max_batches=20):
        """Drive the partitioned file serve batch by batch until the stream phase.

        Returns every served row, each batch's (partition kinds, end offset), and the
        final offset."""
        start = start or {}
        rows, batches = [], []
        for _ in range(max_batches):
            end = connector.latest_offset("app.orders", options, start)
            partitions = connector.get_partitions("app.orders", options, start, end)
            batches.append(([p["kind"] for p in partitions], end))
            for partition in partitions:
                rows.extend(connector.read_partition("app.orders", partition, options))
            start = end
            if end["phase"] == "stream":
                return rows, batches, end
        self.fail("file serve did not reach the stream phase")

    @staticmethod
    def _position(offset):
        snapshot = offset.get("snapshot") or {}
        return offset["phase"], snapshot.get("page_index"), snapshot.get("record_index", 0)

    def test_gzip_record_windows_serve_the_file_across_batches(self):
        source = self._gzip_source(self._FIVE_RECORDS)
        options = {"snapshot.source": source, "snapshot.gzip.records.per.batch": "2"}
        serial = list(self.connector(FakeBridge()).read_table("app.orders", {}, options)[0])

        rows, batches, _ = self._serve(self.connector(FakeBridge()), options)

        self.assertEqual(rows, serial)
        self.assertEqual([kinds for kinds, _ in batches], [["embedded"]] * 3)
        self.assertEqual(
            [self._position(end) for _, end in batches],
            [("snapshot", 0, 2), ("snapshot", 0, 4), ("stream", None, 0)],
        )

    def test_gzip_record_windows_keep_one_decompressor_across_batches(self):
        source = self._gzip_source(self._FIVE_RECORDS)
        options = {"snapshot.source": source, "snapshot.gzip.records.per.batch": "2"}
        connector = self.connector(FakeBridge())
        with mock.patch.object(
            connector, "_open_unl_gzip_records", wraps=connector._open_unl_gzip_records
        ) as opened:
            self._serve(connector, options)

        self.assertEqual(opened.call_count, 1, "normal progress is one pass over the file")

    def test_a_restart_resumes_mid_file_from_the_recorded_record(self):
        # A fresh connector stands in for a restarted driver: it has no open
        # decompressor, so it reopens the file and skips the records already served.
        source = self._gzip_source(self._FIVE_RECORDS)
        options = {"snapshot.source": source, "snapshot.gzip.records.per.batch": "2"}
        serial = list(self.connector(FakeBridge()).read_table("app.orders", {}, options)[0])
        first = self.connector(FakeBridge())
        end = first.latest_offset("app.orders", options, {})
        partitions = first.get_partitions("app.orders", options, {}, end)
        served = [row for p in partitions for row in first.read_partition("app.orders", p, options)]

        restarted = self.connector(FakeBridge())
        rest, _, _ = self._serve(restarted, options, start=end)

        self.assertEqual([row["id"] for row in served], [1, 2])
        self.assertEqual(served + rest, serial, "no record re-ingested or lost")

    def test_a_lost_window_replays_exactly_its_logged_records(self):
        # Re-planning an uncommitted batch on a fresh driver: get_partitions without the
        # latest_offset that cached its rows must replay exactly [start, end).
        source = self._gzip_source(self._FIVE_RECORDS)
        options = {"snapshot.source": source, "snapshot.gzip.records.per.batch": "2"}
        planner = self.connector(FakeBridge())
        start = planner.latest_offset("app.orders", options, {})
        end = planner.latest_offset("app.orders", options, start)
        last = planner.latest_offset("app.orders", options, end)

        for window_start, window_end, ids in ((start, end, [3, 4]), (end, last, [5])):
            with self.subTest(ids=ids):
                replayer = self.connector(FakeBridge())
                partitions = replayer.get_partitions(
                    "app.orders", options, window_start, window_end
                )
                rows = [
                    row
                    for p in partitions
                    for row in replayer.read_partition("app.orders", p, options)
                ]
                self.assertEqual([row["id"] for row in rows], ids)

    def test_a_repeated_latest_offset_does_not_advance_the_window(self):
        source = self._gzip_source(self._FIVE_RECORDS)
        options = {"snapshot.source": source, "snapshot.gzip.records.per.batch": "2"}
        connector = self.connector(FakeBridge())

        first = connector.latest_offset("app.orders", options, {})
        again = connector.latest_offset("app.orders", options, {})
        partitions = connector.get_partitions("app.orders", options, {}, again)
        rows = [
            row for p in partitions for row in connector.read_partition("app.orders", p, options)
        ]

        self.assertEqual(first, again)
        self.assertEqual([row["id"] for row in rows], [1, 2])

    def test_executor_windows_stop_before_a_gzip_file_served_in_record_windows(self):
        # Plain files still fan out to executors; a gzip file gets its own driver windows.
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        with open(os.path.join(directory.name, "a.unl"), "w") as handle:
            handle.write("1|a|\n2|b|\n")
        with open(os.path.join(directory.name, "b.unl.gz"), "wb") as handle:
            handle.write(gzip.compress(b"3|c|\n4|d|\n5|e|\n"))
        with open(os.path.join(directory.name, "c.unl"), "w") as handle:
            handle.write("6|f|\n")
        options = {
            "snapshot.source": directory.name,
            "snapshot.gzip.records.per.batch": "2",
            "snapshot.split.bytes": "1",
        }
        serial = list(self.connector(FakeBridge()).read_table("app.orders", {}, options)[0])

        rows, batches, _ = self._serve(self.connector(FakeBridge()), options)

        self.assertEqual(rows, serial)
        self.assertEqual(
            [kinds for kinds, _ in batches],
            [["unl_split", "unl_split"], ["embedded"], ["embedded"], ["unl_split"]],
        )

    def test_clearing_the_option_mid_file_finishes_it_without_re_reading(self):
        source = self._gzip_source(self._FIVE_RECORDS)
        windowed = {"snapshot.source": source, "snapshot.gzip.records.per.batch": "2"}
        connector = self.connector(FakeBridge())
        end = connector.latest_offset("app.orders", windowed, {})
        connector.get_partitions("app.orders", windowed, {}, end)

        plain = {"snapshot.source": source}
        rows, batches, _ = self._serve(self.connector(FakeBridge()), plain, start=end)

        self.assertEqual([row["id"] for row in rows], [3, 4, 5])
        self.assertEqual([kinds for kinds, _ in batches], [["embedded"]])

    def test_a_shrunken_gzip_file_fails_closed_on_resume(self):
        source = self._gzip_source(self._FIVE_RECORDS)
        options = {"snapshot.source": source, "snapshot.gzip.records.per.batch": "2"}
        connector = self.connector(FakeBridge())
        offset = connector.latest_offset("app.orders", options, {})
        offset = connector.latest_offset("app.orders", options, offset)  # record_index 4
        with open(os.path.join(source, "part-000.unl.gz"), "wb") as handle:
            handle.write(gzip.compress(b"1|a|\n2|b|\n"))

        with self.assertRaisesRegex(InformixError, "part-000.unl.gz changed size"):
            self.connector(FakeBridge()).latest_offset("app.orders", options, offset)

    def test_a_gzip_skip_past_the_end_of_the_file_fails_closed(self):
        # Backstop beneath the manifest: the reopen-and-skip never serves past a file
        # that holds fewer records than the checkpoint has served.
        source = self._gzip_source(self._FIVE_RECORDS)
        options = {"snapshot.source": source}
        connector = self.connector(FakeBridge())
        table = connector._table("app.orders", options)
        split = connector._unl_all_splits(table, options)[0]

        with self.assertRaisesRegex(InformixError, "fewer than the 10"):
            connector._read_unl_gzip_window(table, options, 90, split, 10, 2)

    # --- extract manifest: a file changed after the snapshot started ------------------

    _THREE_RECORDS = "1|a|\n2|b|\n3|c|\n"

    def _first_batch(self, options):
        """Serve one single-split batch of a three-record plain file; return its end."""
        connector = self.connector(
            FakeBridge(), **{"stream.partitioned.snapshot.pages.per.batch": "1"}
        )
        end = connector.latest_offset("app.orders", options, {})
        connector.get_partitions("app.orders", options, {}, end)
        self.assertEqual(end["phase"], "snapshot")
        return end

    def _resume(self, options, offset):
        return self.connector(
            FakeBridge(), **{"stream.partitioned.snapshot.pages.per.batch": "1"}
        ).latest_offset("app.orders", options, offset)

    @staticmethod
    def _rewrite_keeping_mtime(path, content):
        info = os.stat(path)
        with open(path, "w") as handle:
            handle.write(content)
        os.utime(path, ns=(info.st_atime_ns, info.st_mtime_ns))

    def test_a_same_size_content_change_is_caught_on_resume(self):
        source = self._source(self._THREE_RECORDS)
        options = {"snapshot.source": source, "snapshot.split.bytes": "1"}
        end = self._first_batch(options)
        self._rewrite_keeping_mtime(os.path.join(source, "part-000.unl"), "1|a|\n2|z|\n3|c|\n")

        with self.assertRaisesRegex(InformixError, "part-000.unl has different content"):
            self._resume(options, end)

    def test_a_touched_file_is_caught_on_resume(self):
        source = self._source(self._THREE_RECORDS)
        options = {"snapshot.source": source, "snapshot.split.bytes": "1"}
        end = self._first_batch(options)
        path = os.path.join(source, "part-000.unl")
        info = os.stat(path)
        os.utime(path, ns=(info.st_atime_ns, info.st_mtime_ns + 3600 * 10**9))

        with self.assertRaisesRegex(InformixError, "different modification time"):
            self._resume(options, end)

    def test_an_mtime_shift_within_tolerance_is_accepted(self):
        # A Volume reports an unsettled mtime for a few seconds after a write, so a small
        # shift with unchanged content must not fail the resume.
        source = self._source(self._THREE_RECORDS)
        options = {"snapshot.source": source, "snapshot.split.bytes": "1"}
        end = self._first_batch(options)
        path = os.path.join(source, "part-000.unl")
        info = os.stat(path)
        os.utime(path, ns=(info.st_atime_ns, info.st_mtime_ns + 2 * 10**9))

        resumed = self._resume(options, end)

        self.assertEqual(resumed["snapshot"]["page_index"], 2)

    # Added and removed files are separate tests: each needs its own pinned manifest, and
    # cases within one test share the pipeline scope the manifest is keyed by.
    def test_an_added_file_is_caught_on_resume(self):
        source = self._source(self._THREE_RECORDS)
        options = {"snapshot.source": source, "snapshot.split.bytes": "1"}
        end = self._first_batch(options)
        with open(os.path.join(source, "part-001.unl"), "w") as handle:
            handle.write("4|d|\n")

        with self.assertRaisesRegex(InformixError, "added part-001.unl"):
            self._resume(options, end)

    def test_a_removed_file_is_caught_on_resume(self):
        source = self._source(self._THREE_RECORDS, "4|d|\n")
        options = {"snapshot.source": source, "snapshot.split.bytes": "1"}
        end = self._first_batch(options)
        os.remove(os.path.join(source, "part-001.unl"))

        with self.assertRaisesRegex(InformixError, "removed part-001.unl"):
            self._resume(options, end)

    def test_a_replaced_file_is_caught_on_a_replanned_batch(self):
        # A batch re-planned after a restart reaches get_partitions without its
        # latest_offset, so the check must run there too.
        source = self._gzip_source(self._FIVE_RECORDS)
        options = {"snapshot.source": source, "snapshot.gzip.records.per.batch": "2"}
        planner = self.connector(FakeBridge())
        start = planner.latest_offset("app.orders", options, {})
        end = planner.latest_offset("app.orders", options, start)
        with open(os.path.join(source, "part-000.unl.gz"), "wb") as handle:
            handle.write(gzip.compress(b"9|x|\n8|y|\n7|z|\n6|w|\n5|v|\n"))

        with self.assertRaisesRegex(InformixError, "changed after its snapshot started"):
            self.connector(FakeBridge()).get_partitions("app.orders", options, start, end)

    def test_a_snapshot_without_a_manifest_pins_one_on_its_next_read(self):
        # A snapshot that began before manifests existed is pinned on its next read, which
        # then guards the rest of its serve.
        source = self._source(self._THREE_RECORDS)
        options = {"snapshot.source": source}
        connector = self.connector(FakeBridge())
        table = connector._table("app.orders", options)
        legacy_schema_id = "f" * 32

        connector._verify_unl_extract(table, options, "legacy-scope", legacy_schema_id, 90, set())
        self._rewrite_keeping_mtime(os.path.join(source, "part-000.unl"), "1|a|\n2|z|\n3|c|\n")
        path = os.path.join(source, "part-000.unl")

        with self.assertRaisesRegex(InformixError, "different content"):
            self.connector(FakeBridge())._verify_unl_extract(
                table, options, "legacy-scope", legacy_schema_id, 90, {path}
            )

    def test_the_fingerprint_samples_the_head_and_tail_of_a_large_file(self):
        # With a 4-byte sample, a 15-byte file is fingerprinted by its first and last 4
        # bytes: a same-size edit there is caught, one confined to the middle is not.
        source = self._source(self._THREE_RECORDS)
        options = {"snapshot.source": source}
        path = os.path.join(source, "part-000.unl")
        with mock.patch.object(informix_module, "_UNL_FINGERPRINT_SAMPLE_BYTES", 4):
            connector = self.connector(FakeBridge())
            table = connector._table("app.orders", options)
            connector._verify_unl_extract(table, options, "sample-scope", "e" * 32, 90, set())

            self._rewrite_keeping_mtime(path, "1|a|\n2|z|\n3|c|\n")
            self.connector(FakeBridge())._verify_unl_extract(
                table, options, "sample-scope", "e" * 32, 90, {path}
            )

            self._rewrite_keeping_mtime(path, "9|a|\n2|z|\n3|c|\n")
            with self.assertRaisesRegex(InformixError, "different content"):
                self.connector(FakeBridge())._verify_unl_extract(
                    table, options, "sample-scope", "e" * 32, 90, {path}
                )

    def test_an_invalid_record_index_is_rejected(self):
        source = self._gzip_source(self._FIVE_RECORDS)
        options = {"snapshot.source": source, "snapshot.gzip.records.per.batch": "2"}
        offset = self.connector(FakeBridge()).latest_offset("app.orders", options, {})
        for bad in (-1, True, "2"):
            with self.subTest(record_index=bad):
                broken = json.loads(json.dumps(offset))
                broken["snapshot"]["record_index"] = bad
                with self.assertRaisesRegex(ValueError, "record_index"):
                    informix_module._validated_offset(broken)

    def test_unl_hpl_custom_delimiter_serves_shaped_rows(self):
        # An HPL DELIMITED extract: tab-delimited, no trailing delimiter. With the
        # matching snapshot.unl.delimiter it shapes to the same rows as an UNLOAD extract
        # and transitions to the stream phase at the pinned boundary.
        source = self._source("1\talpha\n2\tbeta\n")
        options = {"snapshot.source": source, "snapshot.unl.delimiter": "\t"}
        rows, offset = self.connector(FakeBridge()).read_table("app.orders", {}, options)
        rows = list(rows)
        self.assertEqual(offset["phase"], "stream")
        self.assertEqual([(r["id"], r["value"]) for r in rows], [(1, "alpha"), (2, "beta")])
        for row in rows:
            self.assertEqual(row[informix_module.OP], "r")

    def test_unl_hpl_custom_delimiter_partitioned_fan_out_matches_serial(self):
        # The delimiter rides each unl_split descriptor, so the executor path parses an
        # HPL extract identically to the serial driver path -- row-for-row, no dup/loss.
        source = self._source("1\ta\n2\tb\n3\tc\n")
        options = {"snapshot.source": source, "snapshot.unl.delimiter": "\t"}
        serial = list(self.connector(FakeBridge()).read_table("app.orders", {}, options)[0])

        connector = self.connector(FakeBridge())
        with mock.patch.object(informix_module, "_UNL_SPLIT_TARGET_BYTES", 1):
            offset = connector.latest_offset("app.orders", options, {})
            partitions = connector.get_partitions("app.orders", options, {}, offset)
            fanned = [
                row
                for partition in partitions
                for row in connector.read_partition("app.orders", partition, options)
            ]
        self.assertTrue(partitions)
        self.assertTrue(all(p.get("delimiter") == "\t" for p in partitions))
        self.assertEqual(fanned, serial)

    def test_unl_hpl_escape_disabled_treats_backslash_as_literal_data(self):
        # snapshot.unl.escape="" disables escape processing (an HPL job that writes no
        # escapes): a backslash in a VARCHAR is literal rather than starting an escape.
        source = self._source("1|a\\b|\n")
        options = {"snapshot.source": source, "snapshot.unl.escape": ""}
        rows, _ = self.connector(FakeBridge()).read_table("app.orders", {}, options)
        rows = list(rows)
        self.assertEqual(rows[0]["value"], "a\\b")

    def test_unl_delimiter_option_rejects_a_multi_character_value(self):
        with self.assertRaises(ValueError):
            self.connector(FakeBridge(), **{"snapshot.unl.delimiter": "||"})

    def test_unl_escape_equal_to_delimiter_is_rejected(self):
        with self.assertRaises(ValueError):
            self.connector(
                FakeBridge(),
                **{"snapshot.unl.delimiter": ",", "snapshot.unl.escape": ","},
            )

    def test_unl_source_null_field_becomes_none(self):
        source = self._source("1||\n")
        rows, _ = self.connector(FakeBridge()).read_table(
            "app.orders", {}, {"snapshot.source": source}
        )
        rows = list(rows)
        self.assertEqual(rows[0]["id"], 1)
        self.assertIsNone(rows[0]["value"])

    def test_unl_source_column_count_mismatch_fails_closed(self):
        source = self._source("1|a|extra|\n")
        rows, _ = self.connector(FakeBridge()).read_table(
            "app.orders", {}, {"snapshot.source": source}
        )
        with self.assertRaisesRegex(InformixError, "fields but the table has"):
            list(rows)

    def test_unl_source_missing_path_fails_closed(self):
        with self.assertRaisesRegex(InformixError, "not readable"):
            rows, _ = self.connector(FakeBridge()).read_table(
                "app.orders", {}, {"snapshot.source": "/no/such/unl/dir"}
            )
            list(rows)

    def test_unl_source_empty_directory_fails_closed(self):
        source = self._source()  # no files
        with self.assertRaisesRegex(InformixError, "no .unl files"):
            rows, _ = self.connector(FakeBridge()).read_table(
                "app.orders", {}, {"snapshot.source": source}
            )
            list(rows)

    def test_unl_partitioned_fan_out_matches_serial_and_covers_all_rows(self):
        # The fan-out gate: latest_offset pins the boundary, get_partitions emits one
        # unl_split per ~target-byte split (target forced to 8B here to split a single
        # 5-record file into 3 pieces), and the UNION of read_partition over every
        # descriptor equals the serial serve output row-for-row -- no dup, no loss,
        # identical shaping.
        source = self._source("1|a|\n2|b|\n3|c|\n4|d|\n5|e|\n")
        options = {"snapshot.source": source}
        serial = list(self.connector(FakeBridge()).read_table("app.orders", {}, options)[0])

        connector = self.connector(FakeBridge())
        with mock.patch.object(informix_module, "_UNL_SPLIT_TARGET_BYTES", 8):
            offset = connector.latest_offset("app.orders", options, {})
            partitions = connector.get_partitions("app.orders", options, {}, offset)
            fanned = [
                row
                for partition in partitions
                for row in connector.read_partition("app.orders", partition, options)
            ]
        # Boundary offset transitions straight to the stream phase at the pinned LSN.
        self.assertEqual(offset["phase"], "stream")
        self.assertEqual(offset["commit_lsn"], "90")
        # One 25B file at an 8B target -> 3 splits, all unl_split at the boundary LSN.
        self.assertEqual([p["kind"] for p in partitions], ["unl_split"] * 3)
        self.assertEqual({p["snapshot_lsn"] for p in partitions}, {90})
        # Splits tile the file contiguously (no gap/overlap).
        self.assertEqual(partitions[0]["start_byte"], 0)
        for earlier, later in zip(partitions, partitions[1:]):
            self.assertEqual(earlier["end_byte"], later["start_byte"])
        self.assertEqual(fanned, serial)
        self.assertEqual(
            [(r["id"], r["value"]) for r in fanned],
            [(i, c) for i, c in [(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")]],
        )

    def test_unl_split_bytes_option_controls_the_split_count(self):
        # The snapshot.split.bytes table option flows through to the byte target: a huge
        # target yields one split, a tiny one splits per record -- both serve every row.
        source = self._source("1|a|\n2|b|\n3|c|\n4|d|\n5|e|\n")
        base = {"snapshot.source": source}
        expected = [(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")]

        for target, want_splits in (("1000000", 1), ("1", 5)):
            options = {**base, "snapshot.split.bytes": target}
            connector = self.connector(FakeBridge())
            offset = connector.latest_offset("app.orders", options, {})
            partitions = connector.get_partitions("app.orders", options, {}, offset)
            self.assertEqual(len(partitions), want_splits, target)
            fanned = [
                row
                for partition in partitions
                for row in connector.read_partition("app.orders", partition, options)
            ]
            self.assertEqual([(r["id"], r["value"]) for r in fanned], expected, target)

    def test_unl_split_bytes_option_rejects_a_non_positive_value(self):
        with self.assertRaises(ValueError):
            self.connector(FakeBridge(), **{"snapshot.split.bytes": "0"})

    def _drain_unl_windows(self, connector, options):
        """Drive the windowed partitioned serve to completion; return (rows, offsets)."""
        start: dict = {}
        rows: list[dict] = []
        offsets: list[dict] = []
        for _ in range(50):
            end = connector.latest_offset("app.orders", options, start)
            partitions = connector.get_partitions("app.orders", options, start, end)
            for partition in partitions:
                rows.extend(connector.read_partition("app.orders", partition, options))
            offsets.append(end)
            if end["phase"] == "stream":
                break
            start = end
        else:
            self.fail("windowed .unl serve did not reach the stream phase")
        return rows, offsets

    def test_unl_snapshot_windows_across_microbatches(self):
        # The windowing gate: with pages.per.batch=2 and a byte target that makes each
        # record its own split (5 splits), the serve runs 3 microbatches (2+2+1). Their
        # union equals the serial serve row-for-row -- no dup, no loss -- the split index
        # advances 2 -> 4, and only the final microbatch transitions to the stream phase.
        source = self._source("1|a|\n2|b|\n3|c|\n4|d|\n5|e|\n")
        options = {"snapshot.source": source, "snapshot.split.bytes": "1"}
        serial = list(self.connector(FakeBridge()).read_table("app.orders", {}, options)[0])

        connector = self.connector(
            FakeBridge(), **{"stream.partitioned.snapshot.pages.per.batch": "2"}
        )
        rows, offsets = self._drain_unl_windows(connector, options)

        self.assertEqual(len(offsets), 3)
        self.assertEqual([o["phase"] for o in offsets], ["snapshot", "snapshot", "stream"])
        # Non-terminal offsets advance the split index; all pin the same boundary LSN.
        self.assertEqual(offsets[0]["snapshot"]["page_index"], 2)
        self.assertEqual(offsets[1]["snapshot"]["page_index"], 4)
        self.assertEqual({o["commit_lsn"] for o in offsets}, {"90"})
        self.assertEqual(rows, serial)
        self.assertEqual(
            [(r["id"], r["value"]) for r in rows],
            [(i, c) for i, c in [(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")]],
        )

    def test_unl_snapshot_resumes_from_a_mid_snapshot_offset(self):
        # Resume: a snapshot-phase offset at split index 2 serves exactly splits [2,4) and
        # advances the index to 4 -- so a restart mid-snapshot neither repeats nor skips.
        source = self._source("1|a|\n2|b|\n3|c|\n4|d|\n5|e|\n")
        options = {"snapshot.source": source, "snapshot.split.bytes": "1"}
        connector = self.connector(
            FakeBridge(), **{"stream.partitioned.snapshot.pages.per.batch": "2"}
        )
        first = connector.latest_offset("app.orders", options, {})
        self.assertEqual(first["snapshot"]["page_index"], 2)

        resumed = connector.latest_offset("app.orders", options, first)
        self.assertEqual(resumed["snapshot"]["page_index"], 4)
        partitions = connector.get_partitions("app.orders", options, first, resumed)
        rows = [
            row
            for partition in partitions
            for row in connector.read_partition("app.orders", partition, options)
        ]
        self.assertEqual([(r["id"], r["value"]) for r in rows], [(3, "c"), (4, "d")])

    def test_unl_read_partition_shapes_without_a_live_table(self):
        # A descriptor carries the column metadata, so the executor shapes rows with no
        # Table and no connection; local fallback reads (no token/host).
        source = self._source("7|hi|\n")
        path = os.path.join(source, "part-000.unl")
        partition = {
            "kind": "unl_split",
            "path": path,
            "start_byte": 0,
            "end_byte": os.path.getsize(path),
            "columns": [
                {"name": "id", "type_name": "INTEGER"},
                {"name": "value", "type_name": "VARCHAR"},
            ],
            "snapshot_lsn": 90,
            "exposed_name": "app.orders",
            "encoding": "iso8859-1",
        }
        rows = list(self.connector(FakeBridge()).read_partition("app.orders", partition, {}))
        self.assertEqual(len(rows), 1)
        self.assertEqual((rows[0]["id"], rows[0]["value"]), (7, "hi"))
        self.assertEqual(rows[0][informix_module.OP], "r")
        self.assertIsNone(rows[0][informix_module.TX_ID])
        self.assertEqual(rows[0][informix_module.COMMIT_LSN], informix_module._sortable_lsn(90))

    def test_unl_read_partition_ranged_rest_read(self):
        # A descriptor with token+host reads via a Files-API ranged GET; the fake HTTP
        # layer honours the inclusive Range header, so the executor pulls exactly its
        # split's bytes without a local file.
        payload = b"3|x|\n4|y|\n"

        class _RangeResponse:
            def __init__(self, body):
                self._body = body

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                return self._body

        def fake_urlopen(request, timeout=None):
            header = request.headers.get("Range")
            span = header.split("=", 1)[1]
            start, end = (int(part) for part in span.split("-"))
            return _RangeResponse(payload[start : end + 1])

        partition = {
            "kind": "unl_split",
            "path": "/Volumes/c/s/v/extract.unl",
            "start_byte": 0,
            "end_byte": len(payload),
            "columns": [
                {"name": "id", "type_name": "INTEGER"},
                {"name": "value", "type_name": "VARCHAR"},
            ],
            "snapshot_lsn": 90,
            "exposed_name": "app.orders",
            "encoding": "iso8859-1",
            "token": "dapi-batch-token",
            "host": "https://ws",
        }
        from databricks.labs.community_connector.sources.informix import lakebase_state

        with mock.patch.object(lakebase_state.urllib.request, "urlopen", fake_urlopen):
            rows = list(self.connector(FakeBridge()).read_partition("app.orders", partition, {}))
        self.assertEqual([(r["id"], r["value"]) for r in rows], [(3, "x"), (4, "y")])

    def test_unl_snapshot_serve_gate_excludes_delete_flow(self):
        options = {"snapshot.source": "/vol/x"}
        upsert = self.connector(FakeBridge())
        self.assertTrue(upsert._unl_snapshot_serve(options, {}))
        # A snapshot-phase offset is a continuation window -> still the file serve.
        self.assertTrue(upsert._unl_snapshot_serve(options, {"phase": "snapshot"}))
        # A committed (stream) offset means the snapshot finished -> ordinary CDC path.
        self.assertFalse(upsert._unl_snapshot_serve(options, _stream_offset()))
        # The delete channel never serves the file snapshot.
        deletes = self.connector(FakeBridge(), **{"isDeleteFlow": "true"})
        self.assertFalse(deletes._unl_snapshot_serve(options, {}))

    def _manifest(self, source: str, boundary) -> None:
        with open(os.path.join(source, "_manifest.json"), "w") as handle:
            json.dump({"boundary_lsn": boundary, "note": "ignored key"}, handle)

    def test_unl_boundary_lsn_option_sets_the_boundary(self):
        # FakeBridge: minimum_lsn=1, current_lsn=90. An operator LSN of 50 is used as
        # the authoritative boundary instead of the connector self-pinning at 90.
        source = self._source("1|a|\n2|b|\n")
        rows, offset = self.connector(FakeBridge()).read_table(
            "app.orders", {}, {"snapshot.source": source, "snapshot.boundary.lsn": "50"}
        )
        rows = list(rows)
        self.assertEqual(offset["phase"], "stream")
        self.assertEqual(offset["commit_lsn"], "50")
        for row in rows:
            self.assertEqual(row[informix_module.OP], "r")
            self.assertEqual(row[informix_module.COMMIT_LSN], informix_module._sortable_lsn(50))

    def test_unl_boundary_lsn_from_manifest(self):
        source = self._source("1|a|\n")
        self._manifest(source, "55")
        _, offset = self.connector(FakeBridge()).read_table(
            "app.orders", {}, {"snapshot.source": source}
        )
        self.assertEqual(offset["commit_lsn"], "55")

    def test_unl_boundary_lsn_option_wins_over_manifest(self):
        source = self._source("1|a|\n")
        self._manifest(source, "55")
        _, offset = self.connector(FakeBridge()).read_table(
            "app.orders", {}, {"snapshot.source": source, "snapshot.boundary.lsn": "50"}
        )
        self.assertEqual(offset["commit_lsn"], "50")

    def test_unl_boundary_lsn_absent_self_pins(self):
        source = self._source("1|a|\n")
        _, offset = self.connector(FakeBridge()).read_table(
            "app.orders", {}, {"snapshot.source": source}
        )
        # No option, no manifest -> self-pin at the bridge high_water (90).
        self.assertEqual(offset["commit_lsn"], "90")

    def test_unl_boundary_lsn_below_minimum_fails_closed(self):
        bridge = FakeBridge()
        bridge.minimum = 60  # log recycled past 50 -> gap
        source = self._source("1|a|\n")
        with self.assertRaisesRegex(InformixError, "retained/current log range"):
            rows, _ = self.connector(bridge).read_table(
                "app.orders", {}, {"snapshot.source": source, "snapshot.boundary.lsn": "50"}
            )
            list(rows)

    def test_unl_boundary_lsn_above_current_fails_closed(self):
        source = self._source("1|a|\n")
        with self.assertRaisesRegex(InformixError, "retained/current log range"):
            rows, _ = self.connector(FakeBridge()).read_table(
                "app.orders", {}, {"snapshot.source": source, "snapshot.boundary.lsn": "95"}
            )
            list(rows)

    def test_unl_boundary_lsn_non_integer_option_fails_closed(self):
        source = self._source("1|a|\n")
        with self.assertRaises(InformixError):
            rows, _ = self.connector(FakeBridge()).read_table(
                "app.orders", {}, {"snapshot.source": source, "snapshot.boundary.lsn": "abc"}
            )
            list(rows)

    def test_unl_boundary_lsn_non_integer_manifest_fails_closed(self):
        source = self._source("1|a|\n")
        self._manifest(source, "not-a-number")
        with self.assertRaises(InformixError):
            rows, _ = self.connector(FakeBridge()).read_table(
                "app.orders", {}, {"snapshot.source": source}
            )
            list(rows)

    def test_unl_boundary_lsn_connection_option_validated_at_construction(self):
        with self.assertRaisesRegex(ValueError, "snapshot.boundary.lsn"):
            self.connector(FakeBridge(), **{"snapshot.boundary.lsn": "-3"})

    def test_unl_boundary_lsn_idempotent_across_latest_offset_calls(self):
        source = self._source("1|a|\n")
        options = {"snapshot.source": source, "snapshot.boundary.lsn": "50"}
        connector = self.connector(FakeBridge())
        first = connector.latest_offset("app.orders", options, {})
        second = connector.latest_offset("app.orders", options, {})
        self.assertEqual(first["commit_lsn"], "50")
        self.assertEqual(first, second)


class BoundaryLsnTests(unittest.TestCase):
    """The capture boundary never runs ahead of the log's true write position."""

    def _bridge(self, uniqid, used):
        class Transport:
            def execute(self, sql, parameters=()):
                if "syslogs" in sql:
                    return [{"uniqid": uniqid, "used": used}]
                return [{"status": 0}]

        bridge = object.__new__(PurePythonInformixBridge)
        bridge.options = {}
        bridge.config = {"database": "demo"}
        bridge.transport = Transport()
        return bridge

    def test_log_lsn_page_start_anchors_one_page_below_the_page_end(self):
        self.assertEqual(informix_module._log_lsn(7, 3), (7 << 32) + (3 << 12))
        self.assertEqual(informix_module._log_lsn(7, 3, page_start=True), (7 << 32) + (2 << 12))
        # A log with no used page yet anchors at its start either way.
        self.assertEqual(informix_module._log_lsn(7, 0, page_start=True), 7 << 32)
        self.assertEqual(informix_module._log_lsn(7, 0), 7 << 32)

    def test_boundary_lsn_is_never_ahead_of_current_lsn(self):
        bridge = self._bridge(7, 3)
        self.assertEqual(bridge.current_lsn(), (7 << 32) + (3 << 12))
        self.assertEqual(bridge.boundary_lsn(), (7 << 32) + (2 << 12))
        self.assertLess(bridge.boundary_lsn(), bridge.current_lsn())

    def test_capture_boundaries_use_the_conservative_position(self):
        # Every position a snapshot is sequenced against -- the shared initial
        # boundary, a consistent snapshot's LSN, and each incremental chunk's stamp --
        # is the conservative one; only stop bounds keep current_lsn.
        bridge = self._bridge(7, 3)
        conservative = (7 << 32) + (2 << 12)
        self.assertEqual(bridge.prepare_initial_capture(["demo:app.orders"]), conservative)
        with (
            mock.patch.object(bridge, "_repeatable_read_transaction") as tx,
            mock.patch.object(bridge, "snapshot_page", return_value=[]),
        ):
            tx.return_value.__enter__ = lambda *a: None
            tx.return_value.__exit__ = lambda *a: False
            chunk_lsn, _rows = bridge.snapshot_chunk("demo.app.orders", ["id"], ["id"], None, 10)
            snapshot_lsn, _rows = bridge.consistent_snapshot(
                "demo.app.orders", ["id"], ["id"], 10, 0, 0
            )
        self.assertEqual(chunk_lsn, conservative)
        self.assertEqual(snapshot_lsn, conservative)


def _wide_table(primary_keys=("id",)):
    raw = _table(primary_keys=primary_keys)
    raw["columns"].append({"name": "extra", "type_name": "INTEGER", "nullable": True})
    return raw


def _project(row, columns):
    return {name: row[name] for name in columns if name in row}


class _ProjectingBridge(FakeBridge):
    """A FakeBridge whose snapshots and CDC images carry only the requested columns, as
    Informix's do, so a column selection shows in what the connector emits."""

    def __init__(self):
        super().__init__()
        self.tables = [_wide_table()]
        self.rows = [{"id": 1, "value": "a", "extra": 7}, {"id": 2, "value": "b", "extra": 8}]
        self.snapshot_columns = []

    def consistent_snapshot(self, identity, columns, *args, **kwargs):
        self.snapshot_columns.append(tuple(columns))
        consumer = kwargs.get("page_consumer")
        if consumer is not None:
            kwargs["page_consumer"] = lambda lsn, index, rows: consumer(
                lsn, index, [_project(row, columns) for row in rows]
            )
        lsn, rows = super().consistent_snapshot(identity, columns, *args, **kwargs)
        return lsn, [_project(row, columns) for row in rows]

    def read_changes(self, tables, start_lsn, timeout_seconds, max_records):
        records = super().read_changes(tables, start_lsn, timeout_seconds, max_records)
        captured = tables[0]["columns"] if tables else []
        return [
            dict(record, row=_project(record["row"], captured))
            if isinstance(record.get("row"), dict)
            else record
            for record in records
        ]


class _TableOptionTestCase(unittest.TestCase):
    def setUp(self):
        self._shared_state = tempfile.TemporaryDirectory()
        self._lakebase = _OfflineLakebase().install(self)

    def tearDown(self):
        self._shared_state.cleanup()

    def connector(self, bridge, **extra):
        connector = InformixLakeflowConnect(
            {
                "database": "demo",
                "snapshot.staging.location": self._shared_state.name,
                "lakebase.password": "test-state-password",
                "snapshot.mode": "initial",
                "stream.partitioned": "false",
                **extra,
            }
        )
        connector.set_registration_scope(hashlib.sha256(b"test-pipeline").hexdigest()[:32])
        connector._bridge_instance = bridge
        return _drain_snapshots_inline(_read_cdc_directly(connector))

    def stream(self, bridge, options):
        """Snapshot app.orders and return the connector with its stream checkpoint."""
        connector = self.connector(bridge)
        rows, checkpoint = connector.read_table("app.orders", {}, dict(options))
        list(rows)
        return connector, checkpoint

    @staticmethod
    def committed(*records, tx_id=7, lsn=121):
        """``(op, row)`` records as one committed transaction, LSNs numbered from ``lsn``."""
        out = [{"op": "BEGIN", "tx_id": tx_id, "lsn": lsn}]
        for offset, (op, row) in enumerate(records, start=1):
            out.append({"op": op, "tx_id": tx_id, "lsn": lsn + offset, "row": row})
        out.append({"op": "COMMIT", "tx_id": tx_id, "lsn": lsn + len(records) + 1})
        return out

    def changes(self, connector, bridge, checkpoint, options, *records):
        """Feed one transaction and return ``(upserts, deletes)`` as (id, op) pairs."""
        bridge.now = 200
        bridge.changes = self.committed(*records)
        upserts, _ = connector.read_table("app.orders", checkpoint, dict(options))
        deletes, _ = connector.read_table_deletes("app.orders", checkpoint, dict(options))
        return (
            [(row["id"], row["_informix_op"]) for row in upserts],
            [(row["id"], row["_informix_op"]) for row in deletes],
        )


class ColumnSelectionTests(_TableOptionTestCase):
    """include_columns / exclude_columns narrow what is read and landed: the reported
    schema, the snapshot SELECT, and the CDC capture all see only the selected columns."""

    INCLUDE = {"include_columns": "id,value"}

    def test_include_columns_narrows_schema_snapshot_and_capture(self):
        bridge = _ProjectingBridge()
        connector, checkpoint = self.stream(bridge, self.INCLUDE)

        schema = connector.get_table_schema("app.orders", dict(self.INCLUDE))
        self.assertNotIn("extra", [field.name for field in schema.fields])
        self.assertEqual(bridge.snapshot_columns, [("id", "value")])
        bridge.now = 200
        bridge.changes = self.committed(("INSERT", {"id": 3, "value": "c", "extra": 9}))
        rows = list(connector.read_table("app.orders", checkpoint, dict(self.INCLUDE))[0])
        self.assertEqual(bridge.change_reads[-1][0][0]["columns"], ["id", "value"])
        self.assertEqual(
            [(row["id"], row["value"], "extra" in row) for row in rows], [(3, "c", False)]
        )

    def test_exclude_columns_accepts_a_json_array_and_matches_include(self):
        connector = self.connector(_ProjectingBridge())
        excluded = connector.get_table_schema("app.orders", {"exclude_columns": '["extra"]'})
        included = connector.get_table_schema("app.orders", dict(self.INCLUDE))
        self.assertEqual(
            [field.name for field in excluded.fields], [field.name for field in included.fields]
        )

    def test_excluding_an_uncapturable_column_makes_the_table_streamable(self):
        bridge = FakeBridge()
        bridge.tables = [_table(cdc=False)]
        connector = self.connector(bridge)

        self.assertEqual(
            connector.read_table_metadata("app.orders", {})["ingestion_type"], "snapshot"
        )
        metadata = connector.read_table_metadata("app.orders", {"exclude_columns": "value"})
        self.assertEqual(metadata["ingestion_type"], "cdc_with_deletes")

    def test_the_key_must_stay_selected_unless_primary_keys_replaces_it(self):
        connector = self.connector(_ProjectingBridge())
        with self.assertRaisesRegex(InformixError, "Key column.*id.*must be selected"):
            connector.read_table_metadata("app.orders", {"exclude_columns": "id"})
        metadata = connector.read_table_metadata(
            "app.orders", {"exclude_columns": "id", "primary.keys": "value"}
        )
        self.assertEqual(metadata["primary_keys"], ["value"])

    def test_invalid_selections_fail(self):
        connector = self.connector(_ProjectingBridge())
        for options, message in (
            ({"include_columns": "id,missing"}, "does not have: missing"),
            ({"exclude_columns": "nope"}, "does not have: nope"),
            ({"include_columns": "id", "exclude_columns": "extra"}, "not both"),
            ({"include_columns": "id,id"}, "duplicate"),
            ({"include_columns": " "}, "lists no columns"),
            ({"exclude_columns": "id,value,extra"}, "no columns"),
        ):
            with self.subTest(options=options):
                with self.assertRaisesRegex((InformixError, ValueError), message):
                    connector.read_table_metadata("app.orders", options)

    def test_a_wildcard_key_covers_only_the_selected_columns(self):
        connector = self.connector(_ProjectingBridge())
        metadata = connector.read_table_metadata(
            "app.orders", {"exclude_columns": "extra", "primary.keys": "*"}
        )
        self.assertEqual(metadata["primary_keys"], ["id", "value"])

    def test_changing_the_selection_on_a_running_stream_fails_closed(self):
        bridge = _ProjectingBridge()
        connector, checkpoint = self.stream(bridge, self.INCLUDE)
        bridge.now = 200
        bridge.changes = [{"op": "TIMEOUT", "lsn": 200}]
        widened = {"include_columns": "id,value,extra"}
        with self.assertRaisesRegex(InformixError, "include_columns.*changed.*full refresh"):
            connector.read_table("app.orders", checkpoint, widened)

    def test_a_table_without_a_selection_keeps_its_fingerprint(self):
        # The fingerprint every existing checkpoint was written with: no selection or
        # row_filter part is appended when neither option is set.
        table = Table.parse(_wide_table(), "demo")
        legacy = repr(
            (
                table.database,
                table.owner,
                table.name,
                table.incarnation,
                table.primary_keys,
                tuple(
                    (
                        c.name,
                        c.type_name,
                        c.nullable,
                        c.length,
                        c.precision,
                        c.scale,
                        c.cdc_supported,
                    )
                    for c in table.columns
                ),
            )
        )
        self.assertEqual(_schema_fingerprint(table), hashlib.sha256(legacy.encode()).hexdigest())

    def test_selection_and_filter_survive_schema_history(self):
        connector = self.connector(_ProjectingBridge())
        table = connector._table(
            "app.orders", {"include_columns": "id,value", "row_filter": "id > 1"}
        )
        restored = informix_module._table_from_schema_state(_schema_state(table, 100), "demo")
        self.assertEqual(restored.column_selection, ("include", ("id", "value")))
        self.assertEqual(restored.row_filter, "id > 1")
        self.assertEqual(_schema_fingerprint(restored), _schema_fingerprint(table))

    def test_a_unl_record_is_matched_by_catalog_position_then_narrowed(self):
        connector = self.connector(_ProjectingBridge())
        table = connector._table(
            "app.orders", {"exclude_columns": "value", "row_filter": "extra >= 8"}
        )
        predicate = informix_module._row_filter_predicate(table, "latin-1")

        def shape(raw):
            return informix_module._shape_unl_record(
                raw, table, 100, {}, "utf-8", predicate=predicate
            )

        kept = shape(b"2|b|8|")
        self.assertEqual((kept["id"], kept["extra"], "value" in kept), (2, 8, False))
        self.assertIsNone(shape(b"1|a|7|"))
        with self.assertRaisesRegex(InformixError, "has 2 fields but the table has 3 columns"):
            shape(b"1|7|")

    def test_an_executor_unl_split_rebuilds_the_selection_and_filter(self):
        connector = self.connector(_ProjectingBridge())
        path = os.path.join(self._shared_state.name, "orders.unl")
        with open(path, "wb") as handle:
            handle.write(b"1|a|7|\n2|b|8|\n")
        partition = {
            "kind": "unl_split",
            "path": path,
            "start_byte": 0,
            "end_byte": os.path.getsize(path),
            "gzip": False,
            "columns": [
                {"name": "id", "type_name": "INTEGER"},
                {"name": "value", "type_name": "VARCHAR", "length": 20},
                {"name": "extra", "type_name": "INTEGER"},
            ],
            "selected": ["id", "extra"],
            "row_filter": "extra >= 8",
            "snapshot_lsn": 100,
            "exposed_name": "app.orders",
        }
        rows = list(connector._read_unl_split(partition, {}))
        self.assertEqual(
            [(row["id"], row["extra"], "value" in row) for row in rows], [(2, 8, False)]
        )


def _unsupported_bridge(type_name="BLOB", **column):
    """A _ProjectingBridge whose ``extra`` column has a type the connector can't read."""
    bridge = _ProjectingBridge()
    bridge.tables[0]["columns"][2] = {
        "name": "extra",
        "type_name": type_name,
        "nullable": True,
        "cdc_supported": False,
        **column,
    }
    return bridge


class UnsupportedColumnTests(_TableOptionTestCase):
    """A column of a type the connector can't read is left out, as if exclude_columns
    listed it, and the rest of the table is snapshotted and streamed with a warning."""

    def setUp(self):
        super().setUp()
        informix_module._STATE_VALIDATION_COORDINATOR.unsupported_column_warnings.clear()

    def test_it_is_left_out_of_schema_snapshot_and_capture(self):
        bridge = _unsupported_bridge()
        with self.assertLogs(informix_module.__name__, level="WARNING") as logs:
            connector, checkpoint = self.stream(bridge, {})
        self.assertTrue(
            any("app.orders" in line and "extra (BLOB)" in line for line in logs.output)
        )

        schema = connector.get_table_schema("app.orders", {})
        self.assertNotIn("extra", schema.fieldNames())
        self.assertEqual(bridge.snapshot_columns, [("id", "value")])
        self.assertEqual(
            connector.read_table_metadata("app.orders", {})["ingestion_type"], "cdc_with_deletes"
        )
        bridge.now = 200
        bridge.changes = self.committed(("INSERT", {"id": 3, "value": "c", "extra": b"\x00"}))
        rows = list(connector.read_table("app.orders", checkpoint, {})[0])
        self.assertEqual(bridge.change_reads[-1][0][0]["columns"], ["id", "value"])
        self.assertEqual([(row["id"], "extra" in row) for row in rows], [(3, False)])

    def test_every_unsupported_type_is_left_out(self):
        for type_name, length in (
            ("BYTE", None),
            ("TEXT", None),
            ("BLOB", None),
            ("CLOB", None),
            ("SET", None),
            ("MULTISET", None),
            ("LIST", None),
            ("ROW", None),
            ("UDT_VAR", None),
            ("UDT_FIXED", None),
            ("COLLECTION", None),
            ("DATETIME", 0x0105),
            ("INTERVAL", 0x0624),
        ):
            with self.subTest(type_name):
                bridge = _unsupported_bridge(type_name, length=length)
                schema = self.connector(bridge).get_table_schema("app.orders", {})
                self.assertNotIn("extra", schema.fieldNames())

    def test_a_fresh_delete_reader_checks_only_the_columns_it_reads(self):
        # Registration caches the bare catalog table, and a delete reader without a
        # checkpoint validates that cache before waiting for the upsert reader. It must
        # narrow it as the read path does, whether the connector or exclude_columns left
        # the column out, or the column fails the delete flow.
        for options in ({}, {"exclude_columns": "extra"}):
            with self.subTest(options=options):
                connector = self.connector(_unsupported_bridge("TEXT"))
                connector.get_table_schema("app.orders", dict(options))
                rows, _ = connector.read_table_deletes("app.orders", {}, dict(options))
                self.assertEqual(list(rows), [])

    def test_it_warns_once_per_table(self):
        connector = self.connector(_unsupported_bridge("CLOB"))
        with self.assertLogs(informix_module.__name__, level="WARNING") as logs:
            for _ in range(3):
                connector.read_table_metadata("app.orders", {})
                connector.get_table_schema("app.orders", {})
        self.assertEqual(len([line for line in logs.output if "extra (CLOB)" in line]), 1)

    def test_listing_it_in_exclude_columns_draws_no_warning(self):
        connector = self.connector(_unsupported_bridge())
        with self.assertNoLogs(informix_module.__name__, level="WARNING"):
            schema = connector.get_table_schema("app.orders", {"exclude_columns": "extra"})
        self.assertNotIn("extra", schema.fieldNames())

    def test_an_exclude_columns_entry_for_it_comes_and_goes_without_a_refresh(self):
        # It is left out either way, so listing it (to silence the warning) or taking the
        # entry out again leaves the ingested columns as they were.
        bridge = _unsupported_bridge("TEXT")
        connector, checkpoint = self.stream(bridge, {})
        for options in ({"exclude_columns": "extra"}, {}):
            with self.subTest(options=options):
                bridge.now += 50
                bridge.changes = self.committed(
                    ("INSERT", {"id": bridge.now, "value": "c", "extra": "x"}), lsn=bridge.now - 10
                )
                rows, checkpoint = connector.read_table("app.orders", checkpoint, dict(options))
                self.assertEqual(
                    [(row["id"], "extra" in row) for row in rows], [(bridge.now, False)]
                )
        with self.assertRaisesRegex(InformixError, "exclude_columns.*changed.*full refresh"):
            connector.read_table("app.orders", checkpoint, {"exclude_columns": "value"})

    def test_naming_it_in_include_columns_fails(self):
        connector = self.connector(_unsupported_bridge())
        with self.assertRaisesRegex(InformixError, r"include_columns.*can't read: extra \(BLOB\)"):
            connector.read_table_metadata("app.orders", {"include_columns": "id,extra"})
        metadata = connector.read_table_metadata("app.orders", {"include_columns": "id,value"})
        self.assertEqual(metadata["primary_keys"], ["id"])

    def test_a_key_column_of_an_unsupported_type_fails_the_table(self):
        # Rows are merged and deleted by the key, so its column can't be left out: the
        # table fails as it always did. A primary.keys list naming other columns keys the
        # table on those instead, and "*" keys it on the columns ingested.
        bridge = _unsupported_bridge("DATETIME", length=0x0105, nullable=False)
        bridge.tables[0]["primary_keys"] = ["extra"]
        connector = self.connector(bridge)
        failure = r"cannot materialize: extra \(DATETIME with an unsupported qualifier\)"
        for options in ({}, {"primary.keys": "id,extra"}):
            with self.subTest(options=options):
                with self.assertRaisesRegex(InformixError, failure):
                    connector.read_table_metadata("app.orders", dict(options))
                with self.assertRaisesRegex(InformixError, failure):
                    connector.get_table_schema("app.orders", dict(options))
        for options, key in (
            ({"primary.keys": "id"}, ["id"]),
            ({"primary.keys": "*"}, ["id", "value"]),
        ):
            with self.subTest(options=options):
                metadata = connector.read_table_metadata("app.orders", dict(options))
                self.assertEqual(metadata["primary_keys"], key)

    def test_a_malformed_primary_keys_fails_before_a_column_is_left_out(self):
        connector = self.connector(_unsupported_bridge())
        for raw, message in (
            (" ", "lists no columns"),
            ("*,id", "alone"),
            ("id,id", "duplicate"),
        ):
            with self.subTest(raw=raw):
                with self.assertNoLogs(informix_module.__name__, level="WARNING"):
                    with self.assertRaisesRegex(ValueError, message):
                        connector.read_table_metadata("app.orders", {"primary.keys": raw})

    def test_primary_keys_naming_an_unsupported_column_fails_the_table(self):
        connector = self.connector(_unsupported_bridge())
        with self.assertRaisesRegex(InformixError, r"cannot materialize: extra \(BLOB\)"):
            connector.read_table_metadata("app.orders", {"primary.keys": "extra"})

    def test_a_promoted_nullable_index_with_an_unsupported_column_fails_the_table(self):
        bridge = _unsupported_bridge("DATETIME", length=0x0105)
        bridge.tables[0]["primary_keys"] = []
        bridge.tables[0]["nullable_index_key"] = [["value", False], ["extra", False]]
        connector = self.connector(bridge)
        with self.assertRaisesRegex(InformixError, r"cannot materialize: extra"):
            connector.read_table_metadata("app.orders", {"allow.nullable.index": "true"})
        # Not promoted, the index is no key: the column is left out of a keyless table.
        metadata = connector.read_table_metadata("app.orders", {})
        self.assertEqual(metadata["ingestion_type"], "append")

    def test_a_wildcard_key_covers_only_the_readable_columns(self):
        connector = self.connector(_unsupported_bridge())
        metadata = connector.read_table_metadata("app.orders", {"primary.keys": "*"})
        self.assertEqual(metadata["primary_keys"], ["id", "value"])

    def test_a_table_of_only_unsupported_columns_fails(self):
        bridge = FakeBridge()
        bridge.tables = [
            {
                "database": "demo",
                "owner": "app",
                "name": "orders",
                "columns": [{"name": "doc", "type_name": "BLOB", "cdc_supported": False}],
                "primary_keys": [],
            }
        ]
        with self.assertRaisesRegex(InformixError, r"no column the connector can read: doc"):
            self.connector(bridge).get_table_schema("app.orders", {})

    def test_the_fingerprint_matches_the_table_without_the_column(self):
        # So a column of an unsupported type added at the source later is left out without
        # a schema transition.
        with_column = self.connector(_unsupported_bridge())._table("app.orders", {})
        bridge = _ProjectingBridge()
        del bridge.tables[0]["columns"][2]
        without = self.connector(bridge)._table("app.orders", {})
        self.assertEqual(_schema_fingerprint(with_column), _schema_fingerprint(without))

    def test_a_unl_record_keeps_the_unsupported_field_position(self):
        connector = self.connector(_unsupported_bridge("TEXT"))
        table = connector._table("app.orders", {})
        kept = informix_module._shape_unl_record(b"2|b|some text|", table, 100, {}, "utf-8")
        self.assertEqual((kept["id"], kept["value"], "extra" in kept), (2, "b", False))
        with self.assertRaisesRegex(InformixError, "has 2 fields but the table has 3 columns"):
            informix_module._shape_unl_record(b"2|b|", table, 100, {}, "utf-8")


class SchemaEvolutionTests(_TableOptionTestCase):
    """A running stream adopts widened columns and dropped non-key columns without a full
    refresh; every other change still fails closed."""

    @staticmethod
    def table(columns, keys=("id",), selection=None):
        raw = _table(primary_keys=keys)
        raw["columns"] = [dict(column) for column in columns]
        if selection is not None:
            raw["column_selection"] = selection
        return Table.parse(raw, "demo")

    ID = {"name": "id", "type_name": "INTEGER", "nullable": False}

    def column(self, name, type_name="VARCHAR", length=20, nullable=True):
        return {"name": name, "type_name": type_name, "length": length, "nullable": nullable}

    def test_compatible_changes_are_accepted(self):
        ensure = informix_module._ensure_compatible_schema_change
        base = self.table([self.ID, self.column("v"), self.column("w", "SMALLINT", 2)])
        for name, current in (
            (
                "longer VARCHAR",
                [self.ID, self.column("v", length=50), self.column("w", "SMALLINT", 2)],
            ),
            ("SMALLINT to INTEGER", [self.ID, self.column("v"), self.column("w", "INTEGER", 4)]),
            ("drop nullable", [self.ID, self.column("v")]),
            ("drop then append", [self.ID, self.column("w", "SMALLINT", 2), self.column("x")]),
            (
                "VARCHAR min length only",
                [self.ID, self.column("v", length=20 + 5 * 256), self.column("w", "SMALLINT", 2)],
            ),
            (
                "shorter VARCHAR",
                [self.ID, self.column("v", length=10), self.column("w", "SMALLINT", 2)],
            ),
        ):
            with self.subTest(name):
                ensure(base, self.table(current))
        ensure(
            self.table([self.ID, self.column("c", "NCHAR", 10)]),
            self.table([self.ID, self.column("c", "NCHAR", 20)]),
        )
        # AUTO CDC creates every target column nullable, even from a NOT NULL source, so
        # a dropped NOT NULL column just holds NULL in later rows, like a nullable one.
        ensure(
            self.table([self.ID, self.column("v"), self.column("n", nullable=False)]),
            self.table([self.ID, self.column("v")]),
        )

    def test_incompatible_changes_fail_closed(self):
        ensure = informix_module._ensure_compatible_schema_change
        base = self.table([self.ID, self.column("v"), self.column("n", nullable=False)])
        for name, current, message in (
            (
                "string retyped",
                [self.ID, self.column("v", "CHAR", 20), self.column("n", nullable=False)],
                "other than by widening it or shortening a string",
            ),
            (
                "nullability",
                [self.ID, self.column("v", nullable=False), self.column("n", nullable=False)],
                "other than by widening",
            ),
            (
                "type",
                [self.ID, self.column("v", "INTEGER", 4), self.column("n", nullable=False)],
                "other than by widening",
            ),
            ("reorder", [self.ID, self.column("n", nullable=False), self.column("v")], "reordered"),
            (
                "insert before",
                [self.ID, self.column("x"), self.column("v"), self.column("n", nullable=False)],
                "reordered",
            ),
            (
                "append NOT NULL",
                [
                    self.ID,
                    self.column("v"),
                    self.column("n", nullable=False),
                    self.column("x", nullable=False),
                ],
                "non-nullable",
            ),
        ):
            with self.subTest(name):
                with self.assertRaisesRegex(InformixError, message):
                    ensure(base, self.table(current))

    def test_numeric_widening_follows_delta_type_widening(self):
        ensure = informix_module._ensure_compatible_schema_change

        def column(type_name, precision=None, scale=None):
            return {
                "name": "n",
                "type_name": type_name,
                "nullable": True,
                "precision": precision,
                "scale": scale,
            }

        widenings = (
            (column("SMALLINT"), column("BIGINT")),
            (column("INTEGER"), column("INT8")),
            (column("SERIAL"), column("BIGINT")),
            (column("SMALLFLOAT"), column("FLOAT")),
            (column("DECIMAL", 5, 2), column("DECIMAL", 10, 4)),
            (column("DECIMAL", 5, 2), column("DECIMAL", 7, 2)),
            (column("MONEY", 8, 2), column("MONEY", 12, 2)),
            # Live Informix 14.10 returned pre-ALTER records in the new type for INTEGER
            # (key included) to DECIMAL(10,0)/(12,2) and FLOAT, SMALLINT to DECIMAL(10,0)
            # and FLOAT, INT8 to DECIMAL(20,0), and BIGINT to DECIMAL(22,2); AUTO CDC on
            # Databricks widened int to decimal(10,0)/(12,2) and double, and bigint to
            # decimal(20,0), in place.
            (column("INTEGER"), column("DECIMAL", 10, 0)),
            (column("INTEGER"), column("DECIMAL", 12, 2)),
            (column("SMALLINT"), column("DECIMAL", 10, 0)),
            (column("SERIAL"), column("MONEY", 14, 2)),
            (column("INT8"), column("DECIMAL", 20, 0)),
            (column("BIGINT"), column("DECIMAL", 22, 2)),
            (column("INTEGER"), column("FLOAT")),
            (column("SMALLINT"), column("FLOAT")),
        )
        for old, new in widenings:
            with self.subTest(f"{old['type_name']} to {new['type_name']}"):
                ensure(self.table([self.ID, old]), self.table([self.ID, new]))
        narrowings = (
            (column("BIGINT"), column("INTEGER")),
            (column("INT8"), column("BIGINT")),  # same width, different encoding
            (column("FLOAT"), column("SMALLFLOAT")),
            (column("INTEGER"), column("SMALLFLOAT")),
            (column("INT8"), column("FLOAT")),  # Delta doesn't widen LongType to DoubleType
            (column("SMALLINT"), column("DECIMAL", 5, 0)),  # Delta needs decimal(10,0)+
            (column("INTEGER"), column("DECIMAL", 11, 2)),  # nine integer digits
            (column("BIGINT"), column("DECIMAL", 19, 0)),
            (column("INTEGER"), column("DECIMAL", 16, 0xFF)),  # variable scale
            (column("DECIMAL", 10, 0), column("INTEGER")),
            (column("DECIMAL", 5, 2), column("DECIMAL", 6, 4)),  # loses an integer digit
            (column("DECIMAL", 10, 2), column("DECIMAL", 8, 2)),
            (column("DECIMAL", 30, 2), column("DECIMAL", 40, 2)),  # past DecimalType's 38
            (column("DECIMAL", 10, 0xFF), column("DECIMAL", 16, 0xFF)),  # variable scale
            (column("DECIMAL", 5, 2), column("MONEY", 8, 2)),
        )
        for old, new in narrowings:
            with self.subTest(f"{old} to {new}"):
                with self.assertRaisesRegex(InformixError, "other than by widening"):
                    ensure(self.table([self.ID, old]), self.table([self.ID, new]))

    def test_a_numeric_widening_transitions_the_stream_and_its_reported_type(self):
        bridge = _ProjectingBridge()
        connector, checkpoint = self.stream(bridge, {})
        before = connector.get_table_schema("app.orders", {})
        bridge.tables[0]["columns"][2]["type_name"] = "BIGINT"  # extra: INTEGER -> BIGINT
        upserts, _ = self.changes(
            connector, bridge, checkpoint, {}, ("INSERT", {"id": 3, "value": "c", "extra": 5 << 32})
        )
        after = connector.get_table_schema("app.orders", {})

        self.assertEqual(upserts, [(3, "c")])
        types = [
            {field.name: type(field.dataType).__name__ for field in schema.fields}["extra"]
            for schema in (before, after)
        ]
        self.assertEqual(types, ["IntegerType", "LongType"])

    def test_a_selection_may_only_lose_names_of_columns_gone_from_the_source(self):
        ensure = informix_module._ensure_compatible_schema_change
        before = self.table(
            [self.ID, self.column("v"), self.column("w")], selection=["include", ["id", "v", "w"]]
        )
        after_drop = self.table([self.ID, self.column("v")], selection=["include", ["id", "v"]])
        ensure(before, after_drop)  # w was dropped at the source, then taken out of the list
        still_there = self.table([self.ID, self.column("v")], selection=["include", ["id", "v"]])
        still_there = dataclasses.replace(
            still_there, catalog_columns=still_there.columns + (Column.parse(self.column("w")),)
        )
        with self.assertRaisesRegex(InformixError, "include_columns.*changed"):
            ensure(before, still_there)

    def test_shorter_strings_are_accepted(self):
        def column(type_name, length, nullable=True):
            return Column(name="s", type_name=type_name, length=length, nullable=nullable)

        shortened = informix_module._shortened
        # Informix 14.10 shortens these in place, keeping the table's tabid.
        for type_name in ("CHAR", "NCHAR", "VARCHAR", "NVARCHAR"):
            with self.subTest(type_name):
                self.assertTrue(shortened(column(type_name, 30), column(type_name, 10)))
        for old, new in (
            # It rebuilds the table under a new tabid for an LVARCHAR change.
            (column("LVARCHAR", 100), column("LVARCHAR", 20)),
            (column("VARCHAR", 10), column("VARCHAR", 30)),  # a widening
            (column("VARCHAR", 30), column("CHAR", 10)),  # another type
            (column("VARCHAR", 30), column("VARCHAR", 10, nullable=False)),
            (Column(name="s", type_name="BIGINT"), Column(name="s", type_name="INTEGER")),
        ):
            with self.subTest(f"not {old.type_name} to {new.type_name}"):
                self.assertFalse(shortened(old, new))

    def test_a_shortened_string_transitions_a_running_stream(self):
        bridge = _ProjectingBridge()
        connector, checkpoint = self.stream(bridge, {})
        bridge.tables[0]["columns"][1]["length"] = 5  # ALTER ... MODIFY (value VARCHAR(5))
        upserts, _ = self.changes(
            connector, bridge, checkpoint, {}, ("INSERT", {"id": 3, "value": "abcde", "extra": 1})
        )
        schema = connector.get_table_schema("app.orders", {})

        self.assertEqual(upserts, [(3, "c")])
        self.assertEqual(
            type({f.name: f.dataType for f in schema.fields}["value"]).__name__, "StringType"
        )

    def test_a_shortened_key_column_needs_a_full_refresh(self):
        # Informix truncates the existing keys without logging a change, so the rows
        # already landed would no longer match by key.
        ensure = informix_module._ensure_compatible_schema_change

        def code(length):
            return self.column("code", "CHAR", length, nullable=False)

        keyed = self.table([code(10), self.column("v")], keys=("code",))
        with self.assertRaisesRegex(
            InformixError, "shortened key column code: .*no longer match.*run a full refresh"
        ):
            ensure(keyed, self.table([code(5), self.column("v")], keys=("code",)))
        # A shorter non-key column of the same table carries on.
        ensure(keyed, self.table([code(10), self.column("v", length=5)], keys=("code",)))

        def whole_row(length):
            table = self.table([self.ID, self.column("v", length=length)], keys=("id", "v"))
            return dataclasses.replace(table, key_override=True, key_wildcard=True)

        with self.assertRaisesRegex(
            InformixError, "shortened key column v \\(every column is in its primary.keys='\\*'"
        ):
            ensure(whole_row(20), whole_row(5))

    def test_a_shortened_column_of_a_whole_row_key_fails_the_read(self):
        options = {"primary.keys": "*"}
        bridge = FakeBridge()
        bridge.tables = [_table(primary_keys=())]
        _, checkpoint = self.stream(bridge, options)
        bridge.tables[0]["columns"][1]["length"] = 5  # ALTER ... MODIFY (value VARCHAR(5))
        bridge.now = 120
        bridge.changes = [{"op": "TIMEOUT", "lsn": 120}]

        with self.assertRaisesRegex(InformixError, "shortened key column value"):
            rows, _ = self.connector(bridge).read_table("app.orders", checkpoint, options)
            list(rows)

    def test_a_shortened_string_carries_on_during_an_incremental_copy(self):
        # The copy pages by the key, which the shortened column isn't part of.
        options = {"snapshot.mode": "incremental", "snapshot.page.size": "1"}
        bridge = FakeBridge()
        connector = self.connector(bridge)
        _, checkpoint = connector.read_table("app.orders", {}, options)
        self.assertIn("incremental", checkpoint, "the copy must still be in progress")
        bridge.tables[0]["columns"][1]["length"] = 5
        bridge.now = 120
        bridge.changes = [{"op": "TIMEOUT", "lsn": 120}]

        rows, resumed = self.connector(bridge).read_table("app.orders", checkpoint, options)
        list(rows)

        self.assertNotEqual(resumed["schema_id"], checkpoint["schema_id"])

    def test_a_widened_column_transitions_a_running_stream(self):
        bridge = _ProjectingBridge()
        connector, checkpoint = self.stream(bridge, {})
        bridge.tables[0]["columns"][1]["length"] = 50
        upserts, _ = self.changes(
            connector, bridge, checkpoint, {}, ("INSERT", {"id": 3, "value": "x" * 40, "extra": 1})
        )
        self.assertEqual(upserts, [(3, "c")])

    def test_a_dropped_column_transitions_a_running_stream(self):
        bridge = _ProjectingBridge()
        connector, checkpoint = self.stream(bridge, {})
        del bridge.tables[0]["columns"][2]  # ALTER TABLE ... DROP (extra)
        bridge.now = 200
        bridge.changes = self.committed(("INSERT", {"id": 3, "value": "c"}))
        rows = list(connector.read_table("app.orders", checkpoint, {})[0])
        self.assertEqual([(row["id"], "extra" in row) for row in rows], [(3, False)])
        self.assertEqual(bridge.change_reads[-1][0][0]["columns"], ["id", "value"])

    def test_a_dropped_not_null_column_transitions_a_running_stream(self):
        # The destination's copy of the column is nullable whatever the source said, so
        # later rows hold NULL there as they would for a nullable column.
        bridge = _ProjectingBridge()
        bridge.tables[0]["columns"][2]["nullable"] = False
        connector, checkpoint = self.stream(bridge, {})
        del bridge.tables[0]["columns"][2]  # ALTER TABLE ... DROP (extra)
        bridge.now = 200
        bridge.changes = self.committed(("INSERT", {"id": 3, "value": "c"}))
        rows = list(connector.read_table("app.orders", checkpoint, {})[0])
        self.assertEqual([(row["id"], "extra" in row) for row in rows], [(3, False)])

    def test_dropping_a_selected_column_asks_for_the_list_to_be_edited(self):
        options = {"include_columns": "id,value,extra"}
        bridge = _ProjectingBridge()
        connector, checkpoint = self.stream(bridge, options)
        del bridge.tables[0]["columns"][2]
        bridge.now = 200
        bridge.changes = [{"op": "TIMEOUT", "lsn": 200}]
        with self.assertRaisesRegex(InformixError, "does not have: extra"):
            connector.read_table("app.orders", checkpoint, options)
        rows, end = connector.read_table("app.orders", checkpoint, {"include_columns": "id,value"})
        self.assertEqual(list(rows), [])
        self.assertNotEqual(end["schema_fingerprint"], checkpoint["schema_fingerprint"])

    def test_an_unsupported_column_added_to_a_running_stream_is_left_out(self):
        informix_module._STATE_VALIDATION_COORDINATOR.unsupported_column_warnings.clear()
        bridge = _ProjectingBridge()
        connector, checkpoint = self.stream(bridge, {})
        bridge.tables[0]["columns"].append(  # ALTER TABLE ... ADD (doc TEXT), in place
            {"name": "doc", "type_name": "TEXT", "nullable": True, "cdc_supported": False}
        )
        bridge.now = 200
        bridge.changes = self.committed(("INSERT", {"id": 3, "value": "c", "extra": 9}))
        with self.assertLogs(informix_module.__name__, level="WARNING") as logs:
            rows, end = connector.read_table("app.orders", checkpoint, {})
            rows = list(rows)
        self.assertTrue(any("doc (TEXT)" in line for line in logs.output))
        self.assertEqual([(row["id"], "doc" in row) for row in rows], [(3, False)])
        self.assertEqual(bridge.change_reads[-1][0][0]["columns"], ["id", "value", "extra"])
        self.assertEqual(end["schema_fingerprint"], checkpoint["schema_fingerprint"])

    def test_a_blob_column_added_with_a_rebuild_needs_a_full_refresh(self):
        # Informix 14.10 rebuilds the table under a new tabid to add a BLOB or CLOB column.
        bridge = _ProjectingBridge()
        connector, checkpoint = self.stream(bridge, {})
        bridge.tables[0]["incarnation"] = "144"  # ALTER TABLE ... ADD (doc BLOB): rebuilt
        bridge.tables[0]["columns"].append(
            {"name": "doc", "type_name": "BLOB", "nullable": True, "cdc_supported": False}
        )
        bridge.now = 200
        bridge.changes = [{"op": "TIMEOUT", "lsn": 200}]
        with self.assertRaisesRegex(InformixError, "changed table identity"):
            connector.read_table("app.orders", checkpoint, {})

    def added_column_is_left_out_under(self, options, raw):
        """Stream app.orders keyed by ``options``, add a TEXT column, and check it is left out
        while the key stays ``id``."""
        informix_module._STATE_VALIDATION_COORDINATOR.unsupported_column_warnings.clear()
        bridge = _ProjectingBridge()
        bridge.tables[0].update(raw)
        connector, checkpoint = self.stream(bridge, options)
        bridge.tables[0]["columns"].append(  # ALTER TABLE ... ADD (doc TEXT)
            {"name": "doc", "type_name": "TEXT", "nullable": True, "cdc_supported": False}
        )
        bridge.now = 200
        bridge.changes = self.committed(("INSERT", {"id": 3, "value": "c", "extra": 9}))
        with self.assertLogs(informix_module.__name__, level="WARNING") as logs:
            rows, end = connector.read_table("app.orders", checkpoint, dict(options))
            rows = list(rows)
        self.assertTrue(any("doc (TEXT)" in line for line in logs.output))
        self.assertEqual([(row["id"], "doc" in row) for row in rows], [(3, False)])
        self.assertEqual(end["schema_fingerprint"], checkpoint["schema_fingerprint"])
        metadata = connector.read_table_metadata("app.orders", dict(options))
        self.assertEqual(metadata["primary_keys"], ["id"])

    def test_a_primary_keys_override_leaves_out_an_unsupported_column_added(self):
        # The carry path's key for an override, as for the catalog key and the wildcard.
        self.added_column_is_left_out_under({"primary.keys": "id"}, {"primary_keys": []})

    def test_a_promoted_nullable_index_leaves_out_an_unsupported_column_added(self):
        # The carry path's key for a nullable-index promotion.
        self.added_column_is_left_out_under(
            {"allow.nullable.index": "true"},
            {"primary_keys": [], "nullable_index_key": [["id", False]]},
        )

    def test_a_wildcard_key_reports_a_retyped_column_before_the_key_shrinks(self):
        # Left out, the column would otherwise pass for one dropped from the whole-row key,
        # and a running update would ask for a restart before the full refresh it needs.
        ensure = informix_module._ensure_compatible_schema_change
        retyped = {
            "name": "w",
            "type_name": "DATETIME",
            "length": 0x0105,
            "nullable": True,
            "cdc_supported": False,
        }
        base = dataclasses.replace(
            self.table([self.ID, self.column("v"), self.column("w")], keys=("id", "v", "w")),
            key_override=True,
            key_wildcard=True,
            incarnation="137",
        )
        for incarnation, message in (
            ("144", "changed table identity"),
            ("137", r"type the connector can't read: w \(DATETIME with an unsupported"),
        ):
            with self.subTest(incarnation=incarnation):
                fresh = dataclasses.replace(
                    self.table([self.ID, self.column("v"), retyped], keys=()),
                    incarnation=incarnation,
                )
                current = informix_module._carry_table_options(fresh, base)
                self.assertEqual(current.primary_keys, ("id", "v"))
                for shrink in (False, True):
                    with self.assertRaisesRegex(InformixError, message):
                        ensure(base, current, extend_wildcard_key=True, shrink_wildcard_key=shrink)

    def test_a_wildcard_key_leaves_out_an_unsupported_column_added_while_streaming(self):
        informix_module._STATE_VALIDATION_COORDINATOR.unsupported_column_warnings.clear()
        options = {"primary.keys": "*"}
        bridge = _ProjectingBridge()
        connector, checkpoint = self.stream(bridge, options)
        bridge.tables[0]["columns"].append(  # ALTER TABLE ... ADD (doc TEXT)
            {"name": "doc", "type_name": "TEXT", "nullable": True, "cdc_supported": False}
        )
        bridge.now = 200
        bridge.changes = self.committed(("INSERT", {"id": 3, "value": "c", "extra": 9}))
        rows, end = connector.read_table("app.orders", checkpoint, dict(options))
        self.assertEqual([(row["id"], "doc" in row) for row in rows], [(3, False)])
        self.assertEqual(end["schema_fingerprint"], checkpoint["schema_fingerprint"])
        metadata = connector.read_table_metadata("app.orders", dict(options))
        self.assertEqual(metadata["primary_keys"], ["id", "value", "extra"])

    def test_a_key_column_changed_to_an_unsupported_type_fails_the_read(self):
        bridge = _ProjectingBridge()
        connector, checkpoint = self.stream(bridge, {})
        bridge.tables[0]["columns"][0] = {  # ALTER TABLE ... MODIFY (id DATETIME ...)
            "name": "id",
            "type_name": "DATETIME",
            "length": 0x0105,
            "nullable": False,
            "cdc_supported": False,
        }
        bridge.now = 200
        bridge.changes = [{"op": "TIMEOUT", "lsn": 200}]
        with self.assertRaisesRegex(InformixError, "cannot materialize: id"):
            connector.read_table("app.orders", checkpoint, {})

    def test_a_recreated_table_reports_its_new_identity_before_a_retyped_column(self):
        # Informix can't convert a column to TEXT or BYTE in place (-608), so a column that
        # comes back as TEXT came with a re-created table under a new tabid: that is the
        # change to report, not the column.
        ensure = informix_module._ensure_compatible_schema_change
        text = {"name": "w", "type_name": "TEXT", "nullable": True, "cdc_supported": False}
        base = dataclasses.replace(
            self.table([self.ID, self.column("v"), self.column("w")], keys=()), incarnation="137"
        )
        fresh = dataclasses.replace(
            self.table([self.ID, self.column("v"), text], keys=()), incarnation="144"
        )
        current = informix_module._leave_out_unsupported_columns(fresh, fresh.primary_keys)
        with self.assertRaisesRegex(InformixError, "changed table identity"):
            ensure(base, current)
        with self.assertRaisesRegex(InformixError, r"type the connector can't read: w \(TEXT\)"):
            ensure(base, dataclasses.replace(current, incarnation="137"))

    def test_a_column_recorded_with_an_unsupported_type_is_dropped_without_a_refresh(self):
        # A build that kept a TEXT column in the schema recorded it in the history. It
        # didn't change type, so leaving it out now is an ordinary drop: no full refresh,
        # and the destination keeps the column.
        ensure = informix_module._ensure_compatible_schema_change
        text = {"name": "w", "type_name": "TEXT", "nullable": True, "cdc_supported": False}
        for keys in ((), ("id",)):
            with self.subTest(keys=keys):
                base = self.table([self.ID, self.column("v"), text], keys=keys)
                current = informix_module._leave_out_unsupported_columns(
                    self.table([self.ID, self.column("v"), text], keys=keys), keys
                )
                self.assertEqual([column.name for column in current.columns], ["id", "v"])
                ensure(base, current)
        # In the key it isn't left out at all, so the table fails as it always did.
        keyed = self.table([self.ID, self.column("v"), text], keys=("id", "w"))
        kept = informix_module._leave_out_unsupported_columns(keyed, keyed.primary_keys)
        self.assertEqual([column.name for column in kept.columns], ["id", "v", "w"])
        with self.assertRaisesRegex(InformixError, "cannot materialize: w"):
            informix_module._ensure_materializable(kept)

    def test_a_column_changed_to_an_unsupported_type_needs_a_full_refresh(self):
        # Left out, it would otherwise pass as a dropped column and stop updating silently.
        bridge = _ProjectingBridge()
        connector, checkpoint = self.stream(bridge, {})
        bridge.tables[0]["columns"][2] = {  # ALTER TABLE ... MODIFY (extra TEXT)
            "name": "extra",
            "type_name": "TEXT",
            "nullable": True,
            "cdc_supported": False,
        }
        bridge.now = 200
        bridge.changes = [{"op": "TIMEOUT", "lsn": 200}]
        with self.assertRaisesRegex(
            InformixError,
            r"changed column\(s\) to a type the connector can't read: extra "
            r"\(TEXT\); run a full refresh",
        ):
            connector.read_table("app.orders", checkpoint, {})

    def test_a_dropped_key_column_fails_closed_whatever_its_nullability(self):
        # A primary.keys override or a nullable-index promotion may key on a nullable
        # column; dropping it must not pass as a dropped non-key column, or every row
        # would key on NULL.
        base = self.table([self.ID, self.column("k", "INTEGER", 4), self.column("v")], keys=("k",))
        fresh = self.table([self.ID, self.column("v")], keys=())
        for name, previous in (
            ("primary.keys override", dataclasses.replace(base, key_override=True)),
            (
                "nullable-index promotion",
                dataclasses.replace(base, key_from_nullable_index=True, nullable_index_key=("k",)),
            ),
        ):
            with self.subTest(name):
                with self.assertRaisesRegex(
                    InformixError, "Key column\\(s\\) k .* no longer exist"
                ):
                    informix_module._carry_table_options(fresh, previous)
                carried = dataclasses.replace(fresh, primary_keys=("k",), key_override=True)
                with self.assertRaisesRegex(InformixError, "dropped key column k"):
                    informix_module._ensure_compatible_schema_change(previous, carried)


class RowFilterTests(_TableOptionTestCase):
    """row_filter keeps only matching rows -- in snapshot SQL and in CDC, where an update
    moving a row into the filter upserts it and one moving it out deletes it."""

    FILTER = {"row_filter": "extra >= 8"}

    def filtered_table(self, text, raw=None):
        bridge = FakeBridge()
        bridge.tables = [raw or _wide_table()]
        return self.connector(bridge)._table("app.orders", {"row_filter": text})

    def test_the_filter_is_stored_in_canonical_form(self):
        table = self.filtered_table(
            "extra>=8 and (value = 'it''s' or value is not null) AND NOT id != 1.50"
        )
        self.assertEqual(
            table.row_filter,
            "(extra >= 8) AND ((value = 'it''s') OR (value IS NOT NULL)) AND (NOT (id <> 1.5))",
        )
        self.assertEqual(self.filtered_table(table.row_filter).row_filter, table.row_filter)

    def test_invalid_filters_fail(self):
        float_table = _wide_table()
        float_table["columns"][2]["type_name"] = "FLOAT"
        nvarchar_table = _wide_table()
        nvarchar_table["columns"][1]["type_name"] = "NVARCHAR"
        nchar_table = _wide_table()
        nchar_table["columns"][1].update(type_name="NCHAR", length=10)
        for text, raw, message in (
            ("extra = NULL", None, "IS NULL"),
            ("extra >", None, "expected a value"),
            ("missing = 1", None, "does not have or does not select"),
            ("extra = 'x'", None, "with a string value"),
            ("extra = 1 extra", None, "unexpected"),
            ("extra ~ 1", None, "unexpected character"),
            ("extra = 1.5", float_table, "cannot filter on FLOAT"),
            ("value > 'a'", nvarchar_table, "only test NVARCHAR column value for equality"),
            ("value BETWEEN 'a' AND 'b'", nchar_table, "only test NCHAR column value for"),
            ("id = DATE '2024-02-30'", None, "valid calendar"),
        ):
            with self.subTest(text):
                with self.assertRaisesRegex((InformixError, ValueError), message):
                    self.filtered_table(text, raw)

    def test_the_filter_renders_as_informix_sql(self):
        raw = _wide_table()
        raw["columns"] += [
            {"name": "born", "type_name": "DATE"},
            {
                "name": "seen",
                "type_name": "DATETIME",
                "length": (0 << 8) | 13,
            },  # YEAR TO FRACTION(3)
            {"name": "flag", "type_name": "BOOLEAN"},
        ]
        table = self.filtered_table(
            "value IN ('a', 'b''c') AND extra NOT BETWEEN -1 AND 2.50 AND born >= DATE '2024-01-31'"
            " AND seen < TIMESTAMP '2024-01-31 10:00:00.123' AND flag = TRUE AND id IS NULL",
            raw,
        )
        self.assertEqual(
            informix_module._row_filter_sql(table),
            "(value IN ('a', 'b''c')) AND (extra NOT BETWEEN -1 AND 2.5) AND "
            "(born >= MDY(1, 31, 2024)) AND "
            "(seen < DATETIME(2024-01-31 10:00:00.123) YEAR TO FRACTION(3)) AND "
            "(flag = 't') AND (id IS NULL)",
        )
        with self.assertRaisesRegex(InformixError, "more precise than DATETIME column seen"):
            self.filtered_table("seen = TIMESTAMP '2024-01-31 10:00:00.1234'", raw)

    def test_python_evaluation_follows_sql_three_valued_logic(self):
        table = self.filtered_table("value = 'a' OR extra > 5")
        predicate = informix_module._row_filter_predicate(table)
        matches = informix_module._row_filter_matches
        self.assertTrue(matches(predicate, {"value": "a", "extra": None}))  # TRUE OR NULL
        self.assertFalse(matches(predicate, {"value": "b", "extra": None}))  # FALSE OR NULL
        negated = informix_module._row_filter_predicate(self.filtered_table("NOT extra IN (1, 2)"))
        self.assertFalse(matches(negated, {"extra": None}))  # NOT NULL is NULL
        self.assertTrue(matches(negated, {"extra": 3}))
        padded = informix_module._row_filter_predicate(self.filtered_table("value = 'a'"))
        self.assertTrue(matches(padded, {"value": "a  "}))  # trailing spaces ignored

    def test_snapshots_apply_the_filter_with_snapshot_filter(self):
        bridge = _ProjectingBridge()
        options = {"row_filter": "extra >= 8", "snapshot.filter": "id > 0"}
        self.stream(bridge, options)
        self.assertIn("(id > 0) AND (extra >= 8)", bridge.snapshot_filters)

    def test_cdc_membership_follows_the_images(self):
        bridge = _ProjectingBridge()
        connector, checkpoint = self.stream(bridge, self.FILTER)
        cases = (
            ("insert outside", [("INSERT", {"id": 3, "value": "c", "extra": 1})], [], []),
            ("insert inside", [("INSERT", {"id": 3, "value": "c", "extra": 9})], [(3, "c")], []),
            (
                "update stays inside",
                [
                    ("BEFORE_UPDATE", {"id": 2, "value": "b", "extra": 8}),
                    ("AFTER_UPDATE", {"id": 2, "value": "x", "extra": 9}),
                ],
                [(2, "u")],
                [],
            ),
            (
                "update moves in under a new key",
                [
                    ("BEFORE_UPDATE", {"id": 1, "value": "a", "extra": 7}),
                    ("AFTER_UPDATE", {"id": 5, "value": "a", "extra": 9}),
                ],
                [(5, "u")],
                [],
            ),
            (
                "update moves out",
                [
                    ("BEFORE_UPDATE", {"id": 2, "value": "b", "extra": 8}),
                    ("AFTER_UPDATE", {"id": 2, "value": "b", "extra": 1}),
                ],
                [],
                [(2, "d")],
            ),
            (
                "update stays outside",
                [
                    ("BEFORE_UPDATE", {"id": 1, "value": "a", "extra": 7}),
                    ("AFTER_UPDATE", {"id": 1, "value": "z", "extra": 6}),
                ],
                [],
                [],
            ),
            ("delete inside", [("DELETE", {"id": 2, "value": "b", "extra": 8})], [], [(2, "d")]),
            ("delete outside", [("DELETE", {"id": 1, "value": "a", "extra": 7})], [], []),
        )
        for name, records, upserts, deletes in cases:
            with self.subTest(name):
                self.assertEqual(
                    self.changes(connector, bridge, checkpoint, self.FILTER, *records),
                    (upserts, deletes),
                )

    def test_append_path_tombstones_a_row_leaving_the_filter(self):
        bridge = _ProjectingBridge()
        bridge.tables = [_wide_table(primary_keys=())]
        options = {"append.only.ingestion": "true", **self.FILTER}
        connector, checkpoint = self.stream(bridge, options)
        bridge.now = 200
        bridge.changes = self.committed(
            ("BEFORE_UPDATE", {"id": 2, "value": "b", "extra": 8}),
            ("AFTER_UPDATE", {"id": 2, "value": "b", "extra": 1}),
        )
        rows = list(connector.read_table("app.orders", checkpoint, options)[0])
        self.assertEqual(
            [(row["id"], row["extra"], row["_informix_op"]) for row in rows], [(2, 8, "d")]
        )

    def test_changing_the_filter_on_a_running_stream_fails_closed(self):
        bridge = _ProjectingBridge()
        connector, checkpoint = self.stream(bridge, self.FILTER)
        bridge.now = 200
        bridge.changes = [{"op": "TIMEOUT", "lsn": 200}]
        list(
            connector.read_table("app.orders", checkpoint, {"row_filter": "extra>=8"})[0]
        )  # same filter
        with self.assertRaisesRegex(InformixError, "row_filter.*changed.*full refresh"):
            connector.read_table("app.orders", checkpoint, {"row_filter": "extra >= 9"})

    def test_a_filter_column_must_be_selected(self):
        with self.assertRaisesRegex(InformixError, "does not have or does not select"):
            self.connector(_ProjectingBridge())._table(
                "app.orders", {"exclude_columns": "extra", "row_filter": "extra > 1"}
            )

    def test_like_is_stored_and_rendered_with_its_escape_spelled_out(self):
        table = self.filtered_table("value like 'ab%  ' and value not like 'a!_c' escape '!'")
        canonical = "(value LIKE 'ab%' ESCAPE '\\') AND (value NOT LIKE 'a!_c' ESCAPE '!')"
        self.assertEqual(table.row_filter, canonical)
        self.assertEqual(self.filtered_table(table.row_filter).row_filter, canonical)
        self.assertEqual(informix_module._row_filter_sql(table), canonical)

    def test_like_matches_as_informix_does(self):
        # Each case was measured on Informix 14.10 against CHAR, VARCHAR, NVARCHAR, and
        # LVARCHAR columns alike (CHAR values arrive with their padding stripped).
        cases = (
            ("value LIKE 'abc'", "abc  ", True),  # the value's trailing blanks are ignored
            ("value LIKE 'abc_'", "abc  ", False),
            ("value LIKE 'abc  '", "abc", True),  # and so are the pattern's
            ("value LIKE '% '", "", True),
            ("value LIKE '%abc'", " abc", True),  # leading blanks count
            ("value LIKE 'ABC'", "abc", False),  # case-sensitive
            ("value LIKE ''", "", True),
            ("value LIKE '%_%'", "", False),
            ("value LIKE 'a_c'", "a%c", True),
            ("value LIKE 'a\\_c'", "a_c", True),  # backslash escapes by default
            ("value LIKE 'a\\_c'", "abc", False),
            ("value LIKE 'a\\bc'", "abc", True),  # an escaped ordinary character is literal
            ("value LIKE 'a\\c'", "a\\c", False),
            ("value LIKE 'a\\\\c'", "a\\c", True),
            ("value LIKE 'a!_c' ESCAPE '!'", "a_c", True),
            ("value LIKE 'a\\c' ESCAPE '!'", "a\\c", True),  # another escape frees backslash
            ("value LIKE 'a!!c' ESCAPE '!'", "a!c", True),
            ("value LIKE 'a%c'", "a\nc", True),
            ("value NOT LIKE 'a%'", "bc", True),
            ("value NOT LIKE 'a%'", None, False),  # NULL is unknown either way
            ("value LIKE '%'", None, False),
        )
        for text, value, expected in cases:
            with self.subTest(text=text, value=value):
                predicate = informix_module._row_filter_predicate(self.filtered_table(text))
                self.assertIs(
                    informix_module._row_filter_matches(predicate, {"value": value}), expected
                )

    def test_invalid_like_filters_fail(self):
        nvarchar_table = _wide_table()
        nvarchar_table["columns"][1]["type_name"] = "NVARCHAR"
        self.filtered_table("value LIKE 'a%'", nvarchar_table)  # NVARCHAR takes LIKE
        nchar_table = _wide_table()
        nchar_table["columns"][1].update(type_name="NCHAR", length=10)
        self.filtered_table("value LIKE 'a%' OR value IN ('b', 'c')", nchar_table)
        for text, message in (
            ("extra LIKE '1%'", "only apply LIKE to a string column"),
            ("value LIKE 1", "needs a quoted pattern"),
            ("value LIKE 'ab\\'", "ends with its escape character"),
            ("value LIKE 'ab\\ '", "ends with its escape character"),
            ("value LIKE 'ab!' ESCAPE '!'", "ends with its escape character"),
            ("value LIKE 'a' ESCAPE '!!'", "must be one character"),
            ("value LIKE 'a' ESCAPE '%'", "must be one character"),
            ("value NOT = 'a'", "IN, BETWEEN, or LIKE after NOT"),
        ):
            with self.subTest(text):
                with self.assertRaisesRegex((InformixError, ValueError), message):
                    self.filtered_table(text)

    def _function_table(self):
        raw = _wide_table()
        raw["columns"] += [
            {"name": "code", "type_name": "CHAR", "length": 10},
            {
                "name": "amount",
                "type_name": "DECIMAL",
                "length": 0x0A03,
                "precision": 10,
                "scale": 3,
            },
            {"name": "born", "type_name": "DATE"},
        ]
        return raw

    def test_function_filters_render_and_reparse_canonically(self):
        table = self.filtered_table(
            "upper(trim(code)) = 'AB' and year(born) >= 2020 and coalesce(value, 'x') <> 'y'"
            " and mod(extra, 2) = 1 and round(amount, -1) between 10 and 20",
            self._function_table(),
        )
        canonical = (
            "(UPPER(TRIM(code)) = 'AB') AND (YEAR(born) >= 2020) AND "
            "(COALESCE(value, 'x') <> 'y') AND (MOD(extra, 2) = 1) AND "
            "(ROUND(amount, -1) BETWEEN 10 AND 20)"
        )
        self.assertEqual(table.row_filter, canonical)
        self.assertEqual(
            self.filtered_table(canonical, self._function_table()).row_filter, canonical
        )
        # COALESCE goes to Informix as NVL, which keeps an LVARCHAR result unpadded.
        self.assertEqual(
            informix_module._row_filter_sql(table), canonical.replace("COALESCE(", "NVL(")
        )

    def test_functions_evaluate_as_informix_was_measured_to(self):
        # Each case was measured on Informix 14.10 (2,352 function cases in all), with
        # CHAR values blank-padded to their width before a function sees them.
        raw = self._function_table()
        cases = (
            ("upper(code) = 'ABC'", {"code": "abc"}, True),
            ("upper(value) = '\u00e0B'", {"value": "\u00e0b"}, True),  # ASCII letters only
            ("char_length(code) = 10", {"code": "abc"}, True),  # padded CHAR
            ("char_length(rtrim(code)) = 3", {"code": "abc"}, True),
            ("length(value) = 3", {"value": "abc  "}, True),  # trailing blanks not counted
            ("substr(value, 4) IS NULL", {"value": "abc"}, True),  # start past the end
            ("substr(value, 1, 0) = ''", {"value": "abc"}, True),
            ("substr(code, 11) IS NULL", {"code": "abc"}, True),
            ("substr(code, 2, 3) = 'bc'", {"code": "abc"}, True),
            ("char_length(substr(code, 2, 3)) = 3", {"code": "abc"}, True),
            ("char_length(nvl(code, 'none')) = 10", {"code": None}, True),  # fallback padded
            ("lpad(value, 2, '*') = 'ab'", {"value": "abc"}, True),  # truncates
            ("rpad(value, 5, '.-') = 'ab.-.'", {"value": "ab"}, True),
            ("mod(amount, 3) = -2", {"amount": Decimal("-2.500")}, True),  # truncates first
            ("round(amount, 0) = -3", {"amount": Decimal("-2.500")}, True),  # half away from 0
            ("trunc(amount, -1) = 120", {"amount": Decimal("123.456")}, True),
            ("weekday(born) = 0", {"born": date(2023, 1, 1)}, True),  # Sunday
            ("upper(value) = 'X'", {"value": None}, False),
        )
        for text, row, expected in cases:
            with self.subTest(text):
                predicate = informix_module._row_filter_predicate(self.filtered_table(text, raw))
                self.assertIs(informix_module._row_filter_matches(predicate, row), expected)

    def test_invalid_function_filters_fail(self):
        raw = self._function_table()
        for text, message in (
            ("hash(value) = 'x'", "only transform_columns applies"),
            ("upper(extra) = 'x'", "cannot apply UPPER to a number"),
            ("year(value) = 1", "cannot apply YEAR to a string"),
            ("substr(value, 0) = 'x'", "SUBSTR takes a start from 1"),
            ("mod(extra, 0) = 1", "MOD takes a whole, non-zero divisor"),
            ("round(amount, 0.5) = 1", "takes whole digits"),
            ("upper(value, 'x') = 'x'", "gives UPPER 2 argument"),
            ("nvl(extra, 'x') = 1", "gives NVL a string where it takes a number"),
            ("upper(value) > 1", "a string, with a number value"),
            ("today() = 1", "TODAY\\(\\) is not a supported function"),
        ):
            with self.subTest(text):
                with self.assertRaisesRegex((InformixError, ValueError), message):
                    self.filtered_table(text, raw)

    def test_cdc_membership_follows_a_function_filter(self):
        options = {"row_filter": "upper(value) LIKE 'B%'"}
        bridge = _ProjectingBridge()
        connector, checkpoint = self.stream(bridge, options)
        moved_out = (
            ("BEFORE_UPDATE", {"id": 2, "value": "b", "extra": 8}),
            ("AFTER_UPDATE", {"id": 2, "value": "x", "extra": 8}),
        )
        self.assertEqual(
            self.changes(connector, bridge, checkpoint, options, *moved_out), ([], [(2, "d")])
        )

    def test_cdc_membership_follows_a_like_filter(self):
        options = {"row_filter": "value LIKE 'b%'"}
        bridge = _ProjectingBridge()
        connector, checkpoint = self.stream(bridge, options)
        moved_out = (
            ("BEFORE_UPDATE", {"id": 2, "value": "b", "extra": 8}),
            ("AFTER_UPDATE", {"id": 2, "value": "x", "extra": 8}),
        )
        self.assertEqual(
            self.changes(connector, bridge, checkpoint, options, *moved_out), ([], [(2, "d")])
        )
        inserted = ("INSERT", {"id": 3, "value": "bob", "extra": 1})
        self.assertEqual(
            self.changes(connector, bridge, checkpoint, options, inserted), ([(3, "c")], [])
        )

    def test_a_char_operand_is_padded_to_its_byte_width(self):
        # Informix pads a CHAR(4) to four bytes: '\u00e9' is two bytes in UTF-8, so the padded
        # value is three characters, and one byte in ISO 8859-1, so four. NCHAR pads alike.
        matches = informix_module._row_filter_matches
        predicate = informix_module._row_filter_predicate
        for type_name in ("CHAR", "NCHAR"):
            with self.subTest(type_name):
                raw = _table()
                raw["columns"].append({"name": "code", "type_name": type_name, "length": 4})
                table = informix_module._apply_row_filter(
                    Table.parse(raw, "demo"), "CHAR_LENGTH(code) = 3"
                )
                self.assertTrue(matches(predicate(table, "utf-8"), {"code": "\u00e9"}))
                self.assertFalse(matches(predicate(table, "latin-1"), {"code": "\u00e9"}))

    def test_file_records_count_length_in_the_connection_database_locale(self):
        # DB_LOCALE is a connection option: an executor's filter must read it from there
        # rather than default to ISO 8859-1 while the CDC path counts UTF-8 bytes.
        path = os.path.join(self._shared_state.name, "orders.unl")
        with open(path, "wb") as handle:
            handle.write("3|caf\u00e9|9|\n4|cafe|9|\n".encode("utf-8"))
        partition = {
            "kind": "unl_split",
            "path": path,
            "start_byte": 0,
            "end_byte": os.path.getsize(path),
            "gzip": False,
            "columns": [
                {"name": "id", "type_name": "INTEGER"},
                {"name": "value", "type_name": "VARCHAR", "length": 20},
                {"name": "extra", "type_name": "INTEGER"},
            ],
            "row_filter": "LENGTH(value) = 5",
            "snapshot_lsn": 100,
            "exposed_name": "app.orders",
            "encoding": "utf-8",
        }
        utf8 = self.connector(_ProjectingBridge(), DB_LOCALE="en_US.utf8")
        self.assertEqual([row["id"] for row in utf8._read_unl_split(partition, {})], [3])
        latin1 = self.connector(_ProjectingBridge())
        self.assertEqual([row["id"] for row in latin1._read_unl_split(partition, {})], [])


class ColumnRenameTests(_TableOptionTestCase):
    """rename_columns changes only the names the connector reports -- schema, rows, and
    key -- while every other option keeps naming source columns."""

    RENAME = {"rename_columns": '{"value": "label", "extra": "amount"}'}

    def test_renames_parse_from_json_or_pairs(self):
        parse = informix_module._column_renames
        expected = (("extra", "amount"), ("value", "label"))
        self.assertEqual(parse(self.RENAME), expected)
        self.assertEqual(parse({"rename_columns": " value : label, extra:amount "}), expected)
        self.assertIsNone(parse({"rename_columns": "value:value"}))  # no-op entries drop out
        self.assertIsNone(parse({}))
        for raw, message in (
            ('{"value": "a", "value": "b"}', "renames a column twice"),
            ("value:a, value:b", "renames a column twice"),
            ('{"value": 1}', "map column names to column names"),
            ("{not json", "not a valid JSON object"),
            ("value=label", "JSON object or 'source:destination' pairs"),
            ("value:", "empty column name"),
            ("value:my label", "can't hold"),
            ("value:a(b)", "can't hold"),
        ):
            with self.subTest(raw):
                with self.assertRaisesRegex(ValueError, message):
                    parse({"rename_columns": raw})

    def test_schema_rows_and_key_use_the_destination_names(self):
        options = {"rename_columns": "id:order_id, value:label"}
        bridge = _ProjectingBridge()
        connector, checkpoint = self.stream(bridge, options)

        schema = connector.get_table_schema("app.orders", dict(options))
        metadata = connector.read_table_metadata("app.orders", dict(options))
        bridge.now = 200
        bridge.changes = self.committed(
            ("INSERT", {"id": 3, "value": "c", "extra": 9}),
            ("DELETE", {"id": 1, "value": "a", "extra": 7}),
        )
        upserts = list(connector.read_table("app.orders", checkpoint, dict(options))[0])
        deletes = list(connector.read_table_deletes("app.orders", checkpoint, dict(options))[0])

        names = [field.name for field in schema.fields]
        self.assertEqual(names[:3], ["order_id", "label", "extra"])
        self.assertEqual(metadata["primary_keys"], ["order_id"])
        self.assertEqual(
            [(row["order_id"], row["label"], row["extra"]) for row in upserts], [(3, "c", 9)]
        )
        self.assertEqual([(row["order_id"], row["_informix_op"]) for row in deletes], [(1, "d")])
        self.assertFalse({"id", "value"} & (set(upserts[0]) | set(deletes[0])))
        # Internally the source names stand: the capture still asks Informix for them.
        self.assertEqual(bridge.change_reads[-1][0][0]["columns"], ["id", "value", "extra"])

    def test_snapshot_rows_and_partitions_are_renamed(self):
        bridge = _ProjectingBridge()
        connector = self.connector(bridge)
        rows, _ = connector.read_table("app.orders", {}, dict(self.RENAME))
        self.assertEqual(
            [(row["id"], row["label"], row["amount"]) for row in rows], [(1, "a", 7), (2, "b", 8)]
        )
        encoded = connector._encode_embedded_rows([{"id": 5, "value": "e", "extra": 1}])
        partition_rows = list(
            connector.read_partition(
                "app.orders", {"kind": "embedded", "rows": encoded}, dict(self.RENAME)
            )
        )
        self.assertEqual(partition_rows, [{"id": 5, "label": "e", "amount": 1}])

    def test_columns_may_swap_names_but_not_clash(self):
        connector = self.connector(_ProjectingBridge())
        swapped = connector._table("app.orders", {"rename_columns": "value:extra, extra:value"})
        self.assertEqual(swapped.column_renames, (("extra", "value"), ("value", "extra")))
        for raw, message in (
            ("value:extra", "more than one column named extra"),
            ("value:EXTRA", "more than one column named"),  # Delta ignores case
            ("value:_informix_op", "more than one column named _informix_op"),
            ("missing:x", "does not have or does not select: missing"),
        ):
            with self.subTest(raw):
                with self.assertRaisesRegex(InformixError, message):
                    connector._table("app.orders", {"rename_columns": raw})
        with self.assertRaisesRegex(InformixError, "does not select: extra"):
            connector._table(
                "app.orders", {"exclude_columns": "extra", "rename_columns": "extra:amount"}
            )

    def test_other_options_keep_naming_source_columns(self):
        options = {"rename_columns": "extra:amount", "row_filter": "extra >= 8"}
        bridge = _ProjectingBridge()
        connector, checkpoint = self.stream(bridge, options)
        inserted = ("INSERT", {"id": 3, "value": "c", "extra": 9})
        self.assertEqual(
            self.changes(connector, bridge, checkpoint, options, inserted), ([(3, "c")], [])
        )

    def test_changing_the_renames_on_a_running_stream_fails_closed(self):
        bridge = _ProjectingBridge()
        connector, checkpoint = self.stream(bridge, self.RENAME)
        bridge.now = 200
        bridge.changes = [{"op": "TIMEOUT", "lsn": 200}]
        reordered = {"rename_columns": "extra:amount,value:label"}
        list(connector.read_table("app.orders", checkpoint, reordered)[0])  # same renames
        with self.assertRaisesRegex(InformixError, "rename_columns.*changed.*full refresh"):
            connector.read_table("app.orders", checkpoint, {"rename_columns": "value:label"})

    def test_a_renamed_column_dropped_at_the_source_asks_for_its_entry_to_go(self):
        bridge = _ProjectingBridge()
        connector, checkpoint = self.stream(bridge, self.RENAME)
        del bridge.tables[0]["columns"][2]  # ALTER TABLE ... DROP (extra)
        bridge.now = 200
        bridge.changes = [{"op": "TIMEOUT", "lsn": 200}]
        with self.assertRaisesRegex(InformixError, "does not select: extra"):
            connector.read_table("app.orders", checkpoint, dict(self.RENAME))
        edited = {"rename_columns": "value:label"}
        rows, end = connector.read_table("app.orders", checkpoint, edited)
        self.assertEqual(list(rows), [])
        self.assertNotEqual(end["schema_fingerprint"], checkpoint["schema_fingerprint"])

    def test_tables_without_renames_keep_their_fingerprint(self):
        connector = self.connector(_ProjectingBridge())
        plain = connector._table("app.orders", {})
        renamed = connector._table("app.orders", dict(self.RENAME))
        self.assertIsNone(plain.column_renames)
        self.assertNotIn("column_renames", _schema_state(plain, 1)["table"])
        self.assertNotEqual(_schema_fingerprint(plain), _schema_fingerprint(renamed))
        restored = Table.parse(_schema_state(renamed, 1)["table"], "demo")
        self.assertEqual(restored.column_renames, renamed.column_renames)
        self.assertEqual(_schema_fingerprint(restored), _schema_fingerprint(renamed))


class ColumnTransformTests(_TableOptionTestCase):
    """transform_columns replaces values as rows leave the connector, on every path, before
    renames; row_filter and the key options keep seeing source values."""

    KEY = "0123456789abcdef-test-key"
    MASKED = {"transform_columns": '{"value": "MASK(value, 1)", "extra": "NULLIFY(extra)"}'}

    def keyed(self, bridge):
        return self.connector(bridge, **{"transform.hash.key": self.KEY})

    def stream(self, bridge, options, connector=None):
        connector = connector or self.connector(bridge)
        rows, checkpoint = connector.read_table("app.orders", {}, dict(options))
        return connector, checkpoint, list(rows)

    def test_transforms_parse_from_a_json_object(self):
        parse = informix_module._column_transforms
        parsed = parse({"transform_columns": '{"value": "mask( value ,1)", "id": "hash(id)"}'})
        self.assertEqual(
            [(name, text) for name, text, _ in parsed],
            [("id", "HASH(id)"), ("value", "MASK(value, 1)")],
        )
        self.assertIsNone(parse({}))
        for raw, message in (
            ("{not json", "not a valid JSON object"),
            ('{"value": 1}', "map column names to expressions"),
            ('{"value": "MASK(value)", "value": "HASH(value)"}', "transforms a column twice"),
            ('{"value": "value"}', "applies no function"),
            ('{"value": "SHA2(value)"}', "SHA2\\(\\) is not a supported function"),
            ('{"value": "MASK(value) = 1"}', "unexpected"),
        ):
            with self.subTest(raw):
                with self.assertRaisesRegex(ValueError, message):
                    parse({"transform_columns": raw})

    def test_snapshot_changes_and_schema_carry_the_transformed_values(self):
        bridge = _ProjectingBridge()
        connector, checkpoint, snapshot = self.stream(bridge, self.MASKED)
        schema = connector.get_table_schema("app.orders", dict(self.MASKED))
        bridge.now = 200
        bridge.changes = self.committed(("INSERT", {"id": 3, "value": "secret", "extra": 9}))
        upserts = list(connector.read_table("app.orders", checkpoint, dict(self.MASKED))[0])

        # A value no longer than the characters MASK keeps is masked whole.
        self.assertEqual(
            [(row["id"], row["value"], row["extra"]) for row in snapshot],
            [(1, "*", None), (2, "*", None)],
        )
        self.assertEqual(
            [(row["id"], row["value"], row["extra"]) for row in upserts], [(3, "*****t", None)]
        )
        fields = {field.name: field for field in schema.fields}
        self.assertEqual(type(fields["value"].dataType).__name__, "StringType")
        self.assertEqual(type(fields["extra"].dataType).__name__, "IntegerType")
        self.assertTrue(fields["extra"].nullable)

    def test_a_hashed_key_matches_across_snapshot_changes_and_deletes(self):
        options = {"transform_columns": '{"id": "HASH(id)", "value": "UPPER(value)"}'}
        bridge = _ProjectingBridge()
        connector, checkpoint, snapshot = self.stream(bridge, options, self.keyed(bridge))
        bridge.now = 200
        bridge.changes = self.committed(
            ("BEFORE_UPDATE", {"id": 2, "value": "b", "extra": 8}),
            ("AFTER_UPDATE", {"id": 2, "value": "z", "extra": 8}),
            ("DELETE", {"id": 1, "value": "a", "extra": 7}),
        )
        upserts = list(connector.read_table("app.orders", checkpoint, dict(options))[0])
        deletes = list(connector.read_table_deletes("app.orders", checkpoint, dict(options))[0])
        expected = {
            n: hmac.new(self.KEY.encode(), str(n).encode(), hashlib.sha256).hexdigest()
            for n in (1, 2)
        }

        self.assertEqual([row["id"] for row in snapshot], [expected[1], expected[2]])
        self.assertEqual([(row["id"], row["value"]) for row in upserts], [(expected[2], "Z")])
        self.assertEqual([row["id"] for row in deletes], [expected[1]])  # matches its row
        schema = connector.get_table_schema("app.orders", dict(options))
        self.assertEqual(type(schema.fields[0].dataType).__name__, "StringType")
        metadata = connector.read_table_metadata("app.orders", dict(options))
        self.assertEqual(metadata["primary_keys"], ["id"])

    def test_hash_needs_a_key_and_keys_only_take_hash(self):
        bridge = _ProjectingBridge()
        unkeyed = self.connector(bridge)
        with self.assertRaisesRegex(InformixError, "transform.hash.key"):
            unkeyed._table("app.orders", {"transform_columns": '{"value": "HASH(value)"}'})
        short = self.connector(bridge, **{"transform.hash.key": "short"})
        with self.assertRaisesRegex(InformixError, "at least 16 characters"):
            short._table("app.orders", {"transform_columns": '{"value": "HASH(value)"}'})
        keyed = self.keyed(bridge)
        for raw, message in (
            ('{"id": "ABS(id)"}', "may only HASH key column id"),
            ('{"id": "HASH(ABS(id))"}', "may only HASH key column id"),
            ('{"value": "UPPER(extra)"}', "may only read the column it replaces"),
            ('{"missing": "MASK(missing)"}', "does not have or does not select"),
            ('{"extra": "UPPER(extra)"}', "cannot apply UPPER to a number"),
        ):
            with self.subTest(raw):
                with self.assertRaisesRegex(InformixError, message):
                    keyed._table("app.orders", {"transform_columns": raw})

    def test_hash_digests_numbers_in_one_form(self):
        # A hashed INTEGER key widened to DECIMAL or FLOAT keeps its digest, and a
        # variable-scale DECIMAL hashes the same from a file ("1.5") and CDC ("1.50").
        text = informix_module._hash_text
        self.assertEqual({text(5), text(5.0), text(Decimal("5.00")), text(Decimal("5E+0"))}, {"5"})
        self.assertEqual({text(1.5), text(Decimal("1.5")), text(Decimal("1.50"))}, {"1.5"})
        self.assertEqual((text(Decimal("0.00")), text(Decimal("-12.340"))), ("0", "-12.34"))
        self.assertEqual((text(True), text(b"\x01")), ("true", "01"))

    def test_hash_is_detected_in_the_expression_not_its_literals(self):
        unkeyed = self.connector(_ProjectingBridge())
        table = unkeyed._table(
            "app.orders", {"transform_columns": "{\"value\": \"REPLACE(value, 'HASH(', 'x')\"}"}
        )
        self.assertIsNone(table.transform_key_id)

    def test_integer_result_functions_land_as_integers(self):
        options = {"transform_columns": '{"value": "LENGTH(value)"}'}
        connector, _, snapshot = self.stream(_ProjectingBridge(), options)
        schema = connector.get_table_schema("app.orders", dict(options))
        fields = {field.name: field for field in schema.fields}
        self.assertEqual(type(fields["value"].dataType).__name__, "IntegerType")
        self.assertEqual([row["value"] for row in snapshot], [1, 1])

    def test_nvl_falls_back_in_the_column_type(self):
        options = {"transform_columns": '{"extra": "NVL(extra, 0)"}'}
        bridge = _ProjectingBridge()
        connector, checkpoint, _ = self.stream(bridge, options)
        bridge.now = 200
        bridge.changes = self.committed(("INSERT", {"id": 3, "value": "c", "extra": None}))
        upserts = list(connector.read_table("app.orders", checkpoint, dict(options))[0])
        self.assertEqual([(row["id"], row["extra"]) for row in upserts], [(3, 0)])
        self.assertIs(type(upserts[0]["extra"]), int)  # not the Decimal the literal parsed to
        with self.assertRaisesRegex(InformixError, "whole numbers only"):
            connector._table("app.orders", {"transform_columns": '{"extra": "NVL(extra, 1.5)"}'})
        number = informix_module._number_for_column
        column = informix_module.Column.parse
        decimal = column({"name": "p", "type_name": "DECIMAL", "precision": 10, "scale": 2})
        money = column({"name": "m", "type_name": "MONEY", "precision": 16, "scale": 2})
        self.assertEqual(number(Decimal("7"), decimal, {}), Decimal("7.00"))
        self.assertEqual(str(number(Decimal("7"), money, {})), "7.00")
        bigint = column({"name": "n", "type_name": "BIGINT"})
        self.assertIs(type(number(Decimal("7"), bigint, {})), int)

    def test_an_executor_types_the_fallback_from_the_descriptor(self):
        options = {"transform_columns": '{"extra": "NVL(extra, 0)"}'}
        connector = self.connector(_ProjectingBridge())
        table = connector._table("app.orders", options)
        fields = connector._transform_descriptor_fields(table)
        self.assertEqual([column["name"] for column in fields["transform_columns"]], ["extra"])
        path = os.path.join(self._shared_state.name, "orders.unl")
        with open(path, "wb") as handle:
            handle.write(b"3|c||\n")
        partition = {
            "kind": "unl_split",
            "path": path,
            "start_byte": 0,
            "end_byte": os.path.getsize(path),
            "gzip": False,
            "columns": [
                {"name": "id", "type_name": "INTEGER"},
                {"name": "value", "type_name": "VARCHAR", "length": 20},
                {"name": "extra", "type_name": "INTEGER"},
            ],
            "snapshot_lsn": 100,
            "exposed_name": "app.orders",
        }
        rows = list(connector.read_partition("app.orders", partition, dict(options)))
        self.assertEqual([(row["id"], row["extra"]) for row in rows], [(3, 0)])
        self.assertIs(type(rows[0]["extra"]), int)

    def test_a_transform_keeps_a_not_null_column_not_null_unless_it_can_null(self):
        connector = self.keyed(_ProjectingBridge())
        options = {"transform_columns": '{"id": "HASH(id)", "value": "SUBSTR(value, 2)"}'}
        fields = {f.name: f for f in connector.get_table_schema("app.orders", options).fields}
        self.assertFalse(fields["id"].nullable)
        self.assertTrue(fields["value"].nullable)
        parsed = informix_module._column_transforms(
            {
                "transform_columns": '{"a": "UPPER(MASK(a))", "b": "LOWER(SUBSTR(b, 3))", '
                '"c": "NULLIFY(c)", "d": "ABS(NVL(d, 1))"}'
            }
        )
        operands = {name: operand for name, _, operand in parsed}
        may_null = informix_module._transform_may_null
        self.assertEqual([may_null(operands[name]) for name in "abcd"], [False, True, True, False])

    def test_renames_follow_and_filters_see_source_values(self):
        options = {
            "transform_columns": '{"value": "MASK(value)"}',
            "rename_columns": "value:label",
            "row_filter": "value = 'b'",
        }
        bridge = _ProjectingBridge()
        _, _, snapshot = self.stream(bridge, options)
        self.assertEqual(bridge.snapshot_filters[-1], "value = 'b'")
        connector = self.connector(bridge)
        rows, checkpoint = connector.read_table("app.orders", {}, dict(options))
        bridge.now = 200
        bridge.changes = self.committed(
            ("INSERT", {"id": 3, "value": "b", "extra": 1}),
            ("INSERT", {"id": 4, "value": "c", "extra": 1}),
        )
        upserts = list(connector.read_table("app.orders", checkpoint, dict(options))[0])
        self.assertEqual([(row["id"], row["label"]) for row in upserts], [(3, "*")])  # 4 filtered

    def test_append_tombstones_are_transformed_too(self):
        bridge = _ProjectingBridge()
        bridge.tables = [_wide_table(primary_keys=())]
        options = {"append.only.ingestion": "true", **self.MASKED}
        connector, checkpoint, _ = self.stream(bridge, options)
        bridge.now = 200
        bridge.changes = self.committed(("DELETE", {"id": 2, "value": "bb", "extra": 8}))
        rows = list(connector.read_table("app.orders", checkpoint, options)[0])
        self.assertEqual(
            [(row["value"], row["extra"], row["_informix_op"]) for row in rows], [("*b", None, "d")]
        )

    def test_changing_transforms_or_the_key_on_a_running_stream_fails_closed(self):
        options = {"transform_columns": '{"value": "HASH(value)"}'}
        bridge = _ProjectingBridge()
        connector, checkpoint, _ = self.stream(bridge, options, self.keyed(bridge))
        bridge.now = 200
        bridge.changes = [{"op": "TIMEOUT", "lsn": 200}]
        reformatted = {"transform_columns": '{"value":"hash(value)"}'}
        list(connector.read_table("app.orders", checkpoint, reformatted)[0])  # same transform
        masked = {"transform_columns": '{"value": "MASK(value)"}'}
        with self.assertRaisesRegex(InformixError, "transform_columns.*changed.*full refresh"):
            connector.read_table("app.orders", checkpoint, masked)
        rekeyed = self.connector(bridge, **{"transform.hash.key": "another-key-of-16+chars"})
        with self.assertRaisesRegex(InformixError, "transform.hash.key.*changed"):
            rekeyed.read_table("app.orders", checkpoint, dict(options))

    def test_tables_without_transforms_keep_their_fingerprint(self):
        connector = self.keyed(_ProjectingBridge())
        plain = connector._table("app.orders", {})
        hashed = {"transform_columns": '{"value": "HASH(value)"}'}
        transformed = connector._table("app.orders", hashed)
        self.assertIsNone(plain.column_transforms)
        self.assertNotIn("column_transforms", _schema_state(plain, 1)["table"])
        self.assertNotEqual(_schema_fingerprint(plain), _schema_fingerprint(transformed))
        restored = Table.parse(_schema_state(transformed, 1)["table"], "demo")
        self.assertEqual(restored.column_transforms, (("value", "HASH(value)"),))
        self.assertEqual(_schema_fingerprint(restored), _schema_fingerprint(transformed))
        # The key id is an HMAC under the key, not the key or a plain digest of it.
        self.assertNotIn(self.KEY, json.dumps(_schema_state(transformed, 1)))


if __name__ == "__main__":
    unittest.main()
