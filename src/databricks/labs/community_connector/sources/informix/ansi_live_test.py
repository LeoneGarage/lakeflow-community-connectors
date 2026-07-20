"""Opt-in live regression for ANSI-mode Informix snapshot transactions."""

from __future__ import annotations

import json
import os
import unittest
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


if __name__ == "__main__":
    unittest.main()
