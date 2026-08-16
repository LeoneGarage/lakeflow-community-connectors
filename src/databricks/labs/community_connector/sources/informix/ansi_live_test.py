"""Opt-in live regression for ANSI-mode Informix snapshot transactions."""

from __future__ import annotations

import json
import os
import unittest
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path


def _live_config() -> dict[str, str]:
    inline = os.environ.get("CONNECTOR_TEST_CONFIG_JSON")
    path = os.environ.get("CONNECTOR_TEST_CONFIG_PATH")
    if inline:
        value = json.loads(inline)
    elif path:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    else:
        return {}
    return {str(key): str(item) for key, item in value.items()}


class AnsiSnapshotLiveTest(unittest.TestCase):
    """Run only when the supplied configuration explicitly opts into ANSI validation."""

    def test_repeatable_read_snapshot_on_live_ansi_database(self):
        options = _live_config()
        if options.get("ansi.live.validation", "false").lower() != "true":
            self.skipTest("set ansi.live.validation=true for an ANSI-mode live regression")
        # Import only after explicit live opt-in. This requires the real
        # Databricks/PySpark environment and never installs unit-test stubs.
        from databricks.labs.community_connector.sources.informix.informix import (
            PurePythonInformixBridge,
            _field,
        )

        table_name = options.get("ansi.test.table")
        if not table_name or table_name.count(".") != 1:
            self.fail("ansi.test.table must be an owner.table name")

        bridge = PurePythonInformixBridge(options)
        try:
            ansi_rows = bridge.transport.execute(
                "SELECT is_ansi FROM sysmaster:sysdatabases WHERE name = ?",
                (options["database"],),
            )
            self.assertEqual(len(ansi_rows), 1)
            self.assertEqual(int(_field(ansi_rows[0], "is_ansi", 0)), 1)
            table = bridge.get_table(f"{options['database']}.{table_name}")
            columns = [column["name"] for column in table["columns"]]
            primary_keys = list(table["primary_keys"])
            self.assertTrue(primary_keys, "ANSI live test table must have a primary key")
            _, rows = bridge.consistent_snapshot(
                f"{options['database']}.{table_name}",
                columns,
                primary_keys,
                100,
                10000,
                64 << 20,
            )
            self.assertIsInstance(rows, list)
        finally:
            bridge.transport.close()


class ScalarTypesLiveTest(unittest.TestCase):
    """Opt-in snapshot and CDC regression for newly enabled scalar layouts."""

    def test_int8_serial8_datetime_and_lvarchar_snapshot_and_cdc(self):
        options = _live_config()
        if options.get("scalar.types.live.validation", "false").lower() != "true":
            self.skipTest(
                "set scalar.types.live.validation=true for scalar snapshot/CDC validation"
            )
        from databricks.labs.community_connector.sources.informix.informix import (
            PurePythonInformixBridge,
            Table,
            _capture_descriptor,
            _client_encoding,
        )

        table_name = options.get("scalar.types.test.table")
        if not table_name or table_name.count(".") != 1:
            self.fail("scalar.types.test.table must be an owner.table name")
        bridge = PurePythonInformixBridge({**options, "padVarchar": "false"})
        padded_bridge = None
        writer = None
        primary_error = None
        try:
            identity = f"{options['database']}.{table_name}"
            raw_table = bridge.get_table(identity)
            types = {str(column["type_name"]) for column in raw_table["columns"]}
            required = {"INT8", "SERIAL8", "DATETIME", "LVARCHAR", "BOOLEAN"}
            self.assertTrue(required <= types, f"missing scalar test columns: {required - types}")
            columns = [str(column["name"]) for column in raw_table["columns"]]
            rows = bridge.snapshot_page(identity, columns, (), None, 100)
            self.assertTrue(rows, "scalar live test table must contain at least one row")
            expected_snapshot = self._json_object_option(options, "scalar.types.expected.snapshot")
            expected_null_snapshot = self._json_object_option(
                options, "scalar.types.expected.null.snapshot"
            )
            scalar_columns = self._scalar_columns(raw_table, required)
            for option_name, expected, require_null in (
                ("scalar.types.expected.snapshot", expected_snapshot, False),
                ("scalar.types.expected.null.snapshot", expected_null_snapshot, True),
            ):
                self._require_expected_scalar_columns(expected, scalar_columns, option_name)
                if require_null:
                    self._require_null_scalar(expected, scalar_columns, option_name)
                else:
                    self._require_boundary_values(
                        expected,
                        scalar_columns,
                        option_name,
                        _client_encoding(options),
                    )
                snapshot_row = self._matching_row(rows, expected)
                self.assertIsNotNone(
                    snapshot_row,
                    f"no snapshot row matched {expected!r}: {rows!r}",
                )
                self._assert_scalar_types(snapshot_row, scalar_columns, expected)

            padded_bridge = PurePythonInformixBridge(
                {**options, "padVarchar": "true"}
            )
            padded_rows = padded_bridge.snapshot_page(identity, columns, (), None, 100)
            for expected in (expected_snapshot, expected_null_snapshot):
                padded_row = self._matching_row(padded_rows, expected)
                self.assertIsNotNone(
                    padded_row,
                    f"no padded snapshot row matched {expected!r}: {padded_rows!r}",
                )
                self._assert_scalar_types(padded_row, scalar_columns, expected)
            if self._database_is_ansi(padded_bridge):
                padded_bridge.transport.execute_command("COMMIT WORK")
            padded_bridge.transport.close()
            padded_bridge = None
            if self._database_is_ansi(bridge):
                bridge.transport.execute_command("COMMIT WORK")

            mutation_sql = self._required_option(options, "scalar.types.mutation.sql")
            null_mutation_sql = self._required_option(
                options, "scalar.types.null.mutation.sql"
            )
            cleanup_sql = self._required_option(options, "scalar.types.cleanup.sql")
            expected_cdc = self._json_object_option(options, "scalar.types.expected.cdc")
            expected_null_cdc = self._json_object_option(
                options, "scalar.types.expected.null.cdc"
            )
            table = Table.parse(raw_table, options["database"])
            writer = PurePythonInformixBridge(options)
            self._run_insert_smoke(writer, options)
            for option_name, sql, expected, require_null in (
                ("scalar.types.expected.cdc", mutation_sql, expected_cdc, False),
                (
                    "scalar.types.expected.null.cdc",
                    null_mutation_sql,
                    expected_null_cdc,
                    True,
                ),
            ):
                self._require_expected_scalar_columns(expected, scalar_columns, option_name)
                if require_null:
                    self._require_null_scalar(expected, scalar_columns, option_name)
                else:
                    self._require_boundary_values(
                        expected,
                        scalar_columns,
                        option_name,
                        _client_encoding(options),
                    )
                start_lsn = bridge.prepare_initial_capture([table.native_identity])
                self._execute_committed(writer, sql)
                records = bridge.read_changes(
                    [_capture_descriptor(table, _client_encoding(options))],
                    start_lsn,
                    int(options.get("cdc.timeout", "5")),
                    256,
                )
                changes = []
                for record in records:
                    if record.get("op") not in {"INSERT", "UPDATE", "AFTER_UPDATE"}:
                        continue
                    row = record.get("after", record.get("row"))
                    if isinstance(row, dict):
                        changes.append(row)
                cdc_row = self._matching_row(changes, expected)
                self.assertIsNotNone(
                    cdc_row,
                    f"no CDC row matched {expected!r}: {changes!r}",
                )
                self._assert_scalar_types(cdc_row, scalar_columns, expected)
        except BaseException as error:
            primary_error = error
            raise
        finally:
            cleanup_errors = []
            if writer is not None:
                try:
                    self._execute_committed(writer, cleanup_sql)
                except Exception as error:
                    cleanup_errors.append(error)
                try:
                    writer.transport.close()
                except Exception as error:
                    cleanup_errors.append(error)
            try:
                bridge.transport.close()
            except Exception as error:
                cleanup_errors.append(error)
            if padded_bridge is not None:
                try:
                    padded_bridge.transport.close()
                except Exception as error:
                    cleanup_errors.append(error)
            if cleanup_errors and primary_error is None:
                primary_cleanup_error = cleanup_errors[0]
                if hasattr(primary_cleanup_error, "add_note"):
                    for error in cleanup_errors[1:]:
                        primary_cleanup_error.add_note(
                            f"Additional live scalar cleanup failure: {error}"
                        )
                raise primary_cleanup_error
            if cleanup_errors and primary_error is not None and hasattr(primary_error, "add_note"):
                for error in cleanup_errors:
                    primary_error.add_note(f"Live scalar cleanup also failed: {error}")

    def test_mutations_are_committed_in_ansi_and_non_ansi_databases(self):
        class Transport:
            def __init__(self, ansi: bool):
                self.ansi = ansi
                self.calls = []

            def execute(self, sql, parameters=()):
                self.calls.append(("execute", sql, parameters))
                if sql.startswith("SELECT is_ansi"):
                    return [{"is_ansi": int(self.ansi)}]
                return []

            def execute_command(self, sql):
                self.calls.append(("command", sql))

        for ansi, first_command in ((True, "COMMIT WORK"), (False, "BEGIN WORK")):
            with self.subTest(ansi=ansi):
                transport = Transport(ansi)
                bridge = type(
                    "Bridge",
                    (),
                    {"transport": transport, "config": {"database": "demo"}},
                )()

                self._execute_committed(bridge, "INSERT INTO scalar_types VALUES (1)")

                commands = [call[1] for call in transport.calls if call[0] == "command"]
                self.assertEqual(
                    commands,
                    [
                        first_command,
                        "INSERT INTO scalar_types VALUES (1)",
                        "COMMIT WORK",
                    ],
                )

    def test_insert_smoke_requires_cleanup_and_runs_both_commands(self):
        class Transport:
            def __init__(self):
                self.calls = []

            def execute(self, sql, parameters=()):
                self.calls.append(("execute", sql, parameters))
                return [{"is_ansi": 1}]

            def execute_command(self, sql):
                self.calls.append(("command", sql))

        bridge = type(
            "Bridge",
            (),
            {"transport": Transport(), "config": {"database": "demo"}},
        )()
        with self.assertRaisesRegex(AssertionError, "configured together"):
            self._run_insert_smoke(
                bridge, {"scalar.types.insert.smoke.sql": "INSERT INTO t VALUES (1)"}
            )

        self._run_insert_smoke(
            bridge,
            {
                "scalar.types.insert.smoke.sql": "INSERT INTO t VALUES (1)",
                "scalar.types.insert.smoke.cleanup.sql": "DELETE FROM t WHERE id=1",
            },
        )

        commands = [call[1] for call in bridge.transport.calls if call[0] == "command"]
        self.assertEqual(
            commands,
            [
                "COMMIT WORK",
                "INSERT INTO t VALUES (1)",
                "COMMIT WORK",
                "COMMIT WORK",
                "DELETE FROM t WHERE id=1",
                "COMMIT WORK",
            ],
        )

    def test_insert_smoke_does_not_cleanup_when_insert_fails(self):
        class Transport:
            def __init__(self):
                self.commands = []

            def execute(self, _sql, parameters=()):
                return [{"is_ansi": 1}]

            def execute_command(self, sql):
                self.commands.append(sql)
                if sql == "INSERT INTO t VALUES (1)":
                    raise RuntimeError("duplicate fixture key")

        transport = Transport()
        bridge = type(
            "Bridge", (), {"transport": transport, "config": {"database": "demo"}}
        )()
        with self.assertRaisesRegex(RuntimeError, "duplicate fixture key"):
            self._run_insert_smoke(
                bridge,
                {
                    "scalar.types.insert.smoke.sql": "INSERT INTO t VALUES (1)",
                    "scalar.types.insert.smoke.cleanup.sql": "DELETE FROM t WHERE id=1",
                },
            )
        self.assertNotIn("DELETE FROM t WHERE id=1", transport.commands)

    def test_scalar_fixture_requires_real_boundaries_and_all_nullable_nulls(self):
        columns = {
            "INT8": {"name": "int8_value", "type_name": "INT8", "nullable": True},
            "SERIAL8": {
                "name": "serial8_value",
                "type_name": "SERIAL8",
                "nullable": False,
            },
            "DATETIME": {
                "name": "datetime_value",
                "type_name": "DATETIME",
                "nullable": True,
                "length": 0x000F,
            },
            "LVARCHAR": {
                "name": "lvarchar_value",
                "type_name": "LVARCHAR",
                "nullable": True,
                "length": 3,
            },
            "BOOLEAN": {
                "name": "boolean_value",
                "type_name": "BOOLEAN",
                "nullable": True,
            },
        }
        boundary = {
            "int8_value": -(1 << 63),
            "serial8_value": 42,
            "datetime_value": "2026-07-21T12:34:56.12345",
            "lvarchar_value": "abc",
            "boolean_value": True,
        }
        nulls = {
            "int8_value": None,
            "serial8_value": 1,
            "datetime_value": None,
            "lvarchar_value": None,
            "boolean_value": None,
        }

        self._require_boundary_values(boundary, columns, "boundary", "utf-8")
        self._require_null_scalar(nulls, columns, "nulls")
        with self.assertRaisesRegex(AssertionError, "signed INT8 extreme"):
            self._require_boundary_values(
                {**boundary, "int8_value": 1}, columns, "bad", "utf-8"
            )
        with self.assertRaisesRegex(AssertionError, "every nullable scalar"):
            self._require_null_scalar({**nulls, "lvarchar_value": "x"}, columns, "bad")
        latin_columns = {**columns, "LVARCHAR": {**columns["LVARCHAR"], "length": 1}}
        self._require_boundary_values(
            {**boundary, "serial8_value": 42, "lvarchar_value": "é"},
            latin_columns,
            "latin1",
            "iso8859-1",
        )

    def test_matching_row_compares_declared_datetime_precision_semantically(self):
        row = {"value": datetime(2026, 7, 21, 12, 34, 56, 123450)}

        self.assertIs(
            self._matching_row([row], {"value": "2026-07-21T12:34:56.12345"}), row
        )

    @staticmethod
    def _required_option(options: dict[str, str], name: str) -> str:
        value = options.get(name, "").strip()
        if not value:
            raise AssertionError(f"{name} is required for scalar live validation")
        return value

    @classmethod
    def _json_object_option(cls, options: dict[str, str], name: str) -> dict[str, object]:
        value = json.loads(cls._required_option(options, name))
        if not isinstance(value, dict) or not value:
            raise AssertionError(f"{name} must be a non-empty JSON object")
        return value

    @classmethod
    def _contains_values(cls, row: dict[str, object], expected: dict[str, object]) -> bool:
        return all(
            key in row and cls._values_equal(row[key], value)
            for key, value in expected.items()
        )

    @classmethod
    def _values_equal(cls, actual: object, expected: object) -> bool:
        if isinstance(actual, datetime) and isinstance(expected, str):
            try:
                return actual == datetime.fromisoformat(expected)
            except ValueError:
                return False
        if isinstance(actual, date) and not isinstance(actual, datetime) and isinstance(
            expected, str
        ):
            try:
                return actual == date.fromisoformat(expected)
            except ValueError:
                return False
        return cls._normalized(actual) == cls._normalized(expected)

    @classmethod
    def _matching_row(
        cls, rows: list[dict[str, object]], expected: dict[str, object]
    ) -> dict[str, object] | None:
        return next((row for row in rows if cls._contains_values(row, expected)), None)

    @staticmethod
    def _scalar_columns(
        table: dict[str, object], required: set[str]
    ) -> dict[str, dict[str, object]]:
        columns = table.get("columns")
        if not isinstance(columns, list):
            raise AssertionError("live scalar table metadata has no columns")
        result = {
            str(column["type_name"]): column
            for column in columns
            if isinstance(column, dict) and column.get("type_name") in required
        }
        if set(result) != required:
            raise AssertionError(f"missing scalar column metadata: {required - set(result)}")
        return result

    @staticmethod
    def _require_expected_scalar_columns(
        expected: dict[str, object],
        scalar_columns: dict[str, dict[str, object]],
        option_name: str,
    ) -> None:
        missing = {
            str(column["name"])
            for column in scalar_columns.values()
            if str(column["name"]) not in expected
        }
        if missing:
            raise AssertionError(f"{option_name} is missing scalar columns: {sorted(missing)}")

    @staticmethod
    def _require_null_scalar(
        expected: dict[str, object],
        scalar_columns: dict[str, dict[str, object]],
        option_name: str,
    ) -> None:
        nullable = {
            str(column["name"])
            for column in scalar_columns.values()
            if bool(column.get("nullable", True))
        }
        if not nullable:
            raise AssertionError("live scalar fixture has no nullable scalar columns")
        missing = {name for name in nullable if expected.get(name, object()) is not None}
        if missing:
            raise AssertionError(
                f"{option_name} must set every nullable scalar to null: {sorted(missing)}"
            )

    @staticmethod
    def _require_boundary_values(
        expected: dict[str, object],
        scalar_columns: dict[str, dict[str, object]],
        option_name: str,
        encoding: str,
    ) -> None:
        for source_type, column in scalar_columns.items():
            name = str(column["name"])
            value = expected[name]
            if source_type == "INT8" and value not in {-(1 << 63), (1 << 63) - 1}:
                raise AssertionError(f"{option_name}.{name} must be a signed INT8 extreme")
            if source_type == "SERIAL8" and (
                not isinstance(value, int)
                or isinstance(value, bool)
                or not 1 <= value < (1 << 63)
            ):
                raise AssertionError(
                    f"{option_name}.{name} must be a positive generated SERIAL8 value"
                )
            if source_type == "LVARCHAR":
                maximum = int(column.get("length") or 0)
                if (
                    not isinstance(value, str)
                    or maximum < 1
                    or len(value.encode(encoding)) != maximum
                ):
                    raise AssertionError(
                        f"{option_name}.{name} must contain exactly {maximum} {encoding} bytes"
                    )
            if source_type == "BOOLEAN" and not isinstance(value, bool):
                raise AssertionError(f"{option_name}.{name} must be a boolean")
            if source_type == "DATETIME":
                qualifier = int(column.get("length") or 0)
                fraction_digits = max(0, (qualifier & 0xF) - 10)
                if not isinstance(value, str):
                    raise AssertionError(f"{option_name}.{name} must be an ISO string")
                actual_fraction = value.partition(".")[2]
                if len(actual_fraction) != fraction_digits:
                    raise AssertionError(
                        f"{option_name}.{name} must preserve {fraction_digits} fraction digits"
                    )

    @staticmethod
    def _assert_scalar_types(
        row: dict[str, object],
        scalar_columns: dict[str, dict[str, object]],
        expected: dict[str, object],
    ) -> None:
        for source_type, column in scalar_columns.items():
            name = str(column["name"])
            value = row.get(name)
            if expected[name] is None:
                if value is not None:
                    raise AssertionError(
                        f"live scalar value {name} ({source_type}) should be null"
                    )
                continue
            if value is None:
                raise AssertionError(f"live scalar value {name} ({source_type}) is null")
            if source_type in {"INT8", "SERIAL8"}:
                valid = isinstance(value, int) and not isinstance(value, bool)
            elif source_type == "LVARCHAR":
                valid = isinstance(value, str)
            elif source_type == "BOOLEAN":
                valid = isinstance(value, bool)
            else:
                qualifier = int(column.get("length") or 0)
                start, end = (qualifier >> 8) & 0xF, qualifier & 0xF
                valid = isinstance(value, datetime if start == 0 and end >= 4 else str)
            if not valid:
                raise AssertionError(
                    f"live scalar value {name} ({source_type}) has type "
                    f"{type(value).__name__}"
                )

    @staticmethod
    def _database_is_ansi(bridge) -> bool:
        rows = bridge.transport.execute(
            "SELECT is_ansi FROM sysmaster:sysdatabases WHERE name = ?",
            (bridge.config["database"],),
        )
        if len(rows) != 1:
            raise AssertionError("could not determine live scalar database transaction mode")
        row = rows[0]
        value = next(iter(row.values())) if isinstance(row, dict) else row[0]
        return bool(int(value))

    @classmethod
    def _execute_committed(cls, bridge, sql: str) -> None:
        execute_command = getattr(bridge.transport, "execute_command", bridge.transport.execute)
        is_ansi = cls._database_is_ansi(bridge)
        if is_ansi:
            execute_command("COMMIT WORK")
        else:
            execute_command("BEGIN WORK")
        try:
            execute_command(sql)
            execute_command("COMMIT WORK")
        except BaseException as primary_error:
            try:
                execute_command("ROLLBACK WORK")
            except Exception as rollback_error:
                if hasattr(primary_error, "add_note"):
                    primary_error.add_note(f"Live scalar rollback also failed: {rollback_error}")
            raise

    @classmethod
    def _run_insert_smoke(cls, bridge, options: dict[str, str]) -> None:
        insert_sql = options.get("scalar.types.insert.smoke.sql", "").strip()
        cleanup_sql = options.get("scalar.types.insert.smoke.cleanup.sql", "").strip()
        if bool(insert_sql) != bool(cleanup_sql):
            raise AssertionError(
                "scalar.types.insert.smoke.sql and "
                "scalar.types.insert.smoke.cleanup.sql must be configured together"
            )
        if not insert_sql:
            return
        cls._execute_committed(bridge, insert_sql)
        cls._execute_committed(bridge, cleanup_sql)

    @staticmethod
    def _normalized(value: object) -> object:
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, bytes):
            return value.hex()
        return value


if __name__ == "__main__":
    unittest.main()
