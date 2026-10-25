#!/usr/bin/env python3
"""Live probe: keyless Informix tables keyed with ``primary.keys: "*"``, end to end.

Creates fresh keyless tables in the demo Informix database, ingests them with a
throwaway pipeline running this checkout's connector, and drives them through
inserts, updates, deletes, and schema changes, checking after every step that each
destination holds what the README says it should:

- ``<run>_s1``: SCD type 1 -- the current rows;
- ``<run>_s2``: SCD type 2 -- the current versions (``__END_AT IS NULL``);
- ``<run>_log``: a change log with the key (``append.only.ingestion: "true"``) --
  the README's current-state query over it;
- ``<run>_hb``: a keyed heartbeat, updated before every pipeline run, whose value
  shows the run read to the end of the log.

Phases, in order:

 1. initial snapshot with duplicate rows and NULLs;
 2. inserts, key-changing updates (value to value, NULL to value, value to NULL), a
    no-op update, deletes (a NULL-bearing row, one copy of a duplicate), and a
    multi-statement transaction;
 3. a nullable column added without a default: the key grows;
 4. an INTEGER column widened to BIGINT (needs pipelines.enableTypeWidening);
 5. a nullable column dropped: the key shrinks, and two rows that differed only in
    it collide;
 6. a NOT NULL column dropped;
 7. a column dropped and another added in one step: must fail and ask for a full
    refresh, which then recovers;
 8. a column added with a default: must fail and ask for a full refresh, which
    then recovers;
 9. a column dropped while a continuous update runs: must fail and ask for a
    restart, after which the next update adopts the shorter key;
10. exact copies: two inserted collapse to one row; updating one copy removes the
    shared row; a row updated into a copy of another, then one copy deleted;
11. widenings: VARCHAR(20) to VARCHAR(40), INTEGER to DECIMAL(12,2), DECIMAL(12,2)
    to DECIMAL(14,2), each with a key change on a row from before it;
12. a narrowing, DECIMAL(14,2) to DECIMAL(10,2): must fail, then a full refresh
    recovers.

``--mode continuous`` (the default) runs one continuous update and has each step wait
for its changes to arrive; DDL steps stop it first, since Informix refuses DDL on a
captured table, and the next step starts a new one. ``--mode triggered`` runs a
triggered update per step instead.

The expected destinations come from a model of the documented semantics (exact
duplicates collapse; deleting one copy removes the row; after a drop, rows that
differed only in the dropped column share a key, which a later delete removes from
every SCD1 row; under SCD2 it closes the newest version -- every open version tied for
newest, so both when one snapshot loaded them).

Informix tables are never dropped: on the demo server, freeing partitions (DROP
TABLE, key-dropping MODIFY, TEMP tables) has stalled CDC server-wide. Each run makes
new run-suffixed tables instead. The pipeline, its schema, and the workspace folder
are deleted at the end unless ``--keep`` is given.

Informix CDC can hold changes back for minutes, so a triggered update often ends
before reading them: each step keeps running updates until the heartbeat arrives,
for up to HEARTBEAT_BUDGET_SECONDS, and reports how many it needed. A heartbeat that
never arrives means CDC stopped delivering -- on the demo server that has happened
server-wide, and only restarting Informix clears it. The probe then records the stall
with the server's CDC state and restarts the Informix engine inside the container,
which keeps testdb and the probe tables, resets the tables to the rows the stalled
phase found, checks a full refresh of them, and reruns that phase; at most
MAX_RESTARTS times. A phase that had already changed the tables' DDL can't be reset in
place, so then the container is restarted instead: it rebuilds testdb, dropping every
table, and the probe recreates its tables as the phase found them.

Usage (from the repo root, with the repo virtualenv):

    .venv/bin/python src/databricks/labs/community_connector/sources/informix/tests/\
keyless_star_probe_test_tool.py --warehouse-id <id> [--start-phase N] [--end-phase N] \
[--keep] [--report out.json]

``--start-phase N`` replays the earlier phases on the model alone, builds the tables
in that state, checks a full refresh of them, and starts at phase N.

The ``_test_`` filename keeps this developer tool out of the generated connector.
"""

from __future__ import annotations

import argparse
import base64
import collections
import copy
import datetime
import decimal
import json
import os
import pathlib
import random
import re
import shutil
import subprocess
import sys
import tempfile
import time
from typing import Any, Callable

REPO_SRC = pathlib.Path(__file__).resolve().parents[6]
sys.path.insert(0, str(REPO_SRC))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import count_compare_test_tool as CC  # noqa: E402

C = CC.C

UPDATE_TIMEOUT_SECONDS = 20 * 60
TERMINAL = {"COMPLETED", "FAILED", "CANCELED"}
# Measured on the demo server: triggered updates missed changes committed 1-7 minutes
# before them.
HEARTBEAT_BUDGET_SECONDS = 15 * 60
# An update expected to fail has done so within about two minutes when every flow fails.
EXPECTED_FAILURE_SECONDS = 10 * 60
MAX_RESTARTS = 3
STALL_DIAGNOSTICS = r"""
onstat -l | awk '$3 ~ /^U/ && $3 ~ /C/'
onstat -g cdc config 2>&1 | sed -n '4,40p'
grep -E 'Checkpoint Completed|CDC:' /opt/ibm/data/logs/online.log | tail -20
"""


# What informix_stop.sh and informix_online.sh run, waiting for On-Line (onstat - says so).
ENGINE_RESTART = r"""
export APP_VENDOR_DIR=ibm
. /opt/ibm/scripts/informix_inf.env
timeout 60 onmode -kuy; echo "__ONMODE=$?"
oninit -w 300 </dev/null; echo "__ONINIT=$?"
onstat - | sed -n 2p
"""


class Stalled(RuntimeError):
    """The heartbeat never arrived: Informix CDC stopped delivering changes."""


class DdlRefused(RuntimeError):
    """An ALTER failed on one table; ``done`` lists the tables it already changed."""

    def __init__(self, message: str, done: list[str]) -> None:
        super().__init__(message)
        self.done = done


def log(message: str) -> None:
    print(f"{datetime.datetime.now(datetime.timezone.utc):%H:%M:%S} {message}", flush=True)


# ---- Informix ---------------------------------------------------------------------


class Informix:
    """Runs SQL on the demo Informix through SSM and dbaccess, as the compare tools do."""

    ENV = (
        "-e INFORMIXDIR=/opt/ibm/informix -e INFORMIXSERVER=informix_tcp "
        "-e DBACCNOIGN=1 "
        "-e LD_LIBRARY_PATH=/opt/ibm/informix/lib:/opt/ibm/informix/lib/esql "
        "-e PATH=/opt/ibm/informix/bin:/usr/bin:/bin"
    )

    def __init__(self) -> None:
        # Ambient AWS credentials reach the host, as in count_compare_test_tool.
        C.aws = lambda args, *command: C.run(["aws", *command, "--region", args.aws_region])
        instance_id = subprocess.run(
            [
                "aws",
                "ec2",
                "describe-instances",
                "--region",
                "us-east-1",
                "--filters",
                "Name=tag:Name,Values=informix-cdc",
                "Name=instance-state-name,Values=running",
                "--query",
                "Reservations[].Instances[].InstanceId",
                "--output",
                "text",
            ],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.split()[0]
        self.args = CC.make_args(instance_id)

    def sql(self, statements: str) -> str:
        """Run ``statements`` in one dbaccess session; fail with its output on an error."""
        encoded = base64.b64encode(statements.encode()).decode()
        command = "; ".join(
            [
                f"printf %s {encoded} | base64 -d > /tmp/kstar.sql",
                "docker cp /tmp/kstar.sql informix-cdc:/tmp/kstar.sql",
                f"docker exec {self.ENV} informix-cdc sh -c "
                "'dbaccess testdb /tmp/kstar.sql > /tmp/kstar.out 2>&1; echo __RC=$?; "
                'cat /tmp/kstar.out; for f in /tmp/kstar_*.unl; do [ -f "$f" ] && '
                '{ echo "__FILE=$f"; cat "$f"; echo __END; rm -f "$f"; }; done; true\'',
            ]
        )
        out = C.wait_ssm(self.args, C.send_ssm(self.args, command), 900)
        rc = next((line[5:] for line in out.splitlines() if line.startswith("__RC=")), None)
        if rc != "0":
            raise RuntimeError(f"dbaccess failed (rc={rc}):\n{out[-3000:]}\nSQL:\n{statements}")
        return out

    def shell(self, script: str) -> str:
        """Run a shell script inside the container (onstat and the like)."""
        encoded = base64.b64encode(script.encode()).decode()
        command = "; ".join(
            [
                f"printf %s {encoded} | base64 -d > /tmp/kstar.sh",
                "docker cp /tmp/kstar.sh informix-cdc:/tmp/kstar.sh",
                f"docker exec {self.ENV} informix-cdc sh /tmp/kstar.sh 2>&1; true",
            ]
        )
        return C.wait_ssm(self.args, C.send_ssm(self.args, command), 600)

    def restart_engine(self) -> None:
        """Restart the Informix engine inside the container; testdb and every table stay.

        This clears a CDC stall -- the shared log reader lives in engine memory -- without
        the container restart's testdb rebuild. Every connection drops, so stop the
        pipelines first. ``informix_entry.sh`` runs the rebuild only at container start
        and then just tails the message log, so the container keeps running meanwhile.
        """
        out = self.shell(ENGINE_RESTART)
        if "On-Line" not in out.split("__ONINIT=")[-1]:
            raise RuntimeError(f"the Informix engine did not come back On-Line:\n{out[-2000:]}")
        log("restarted the Informix engine")

    def restart(self) -> None:
        """Restart the container and wait for it to rebuild testdb.

        The rebuild drops every table it did not create, the probe's included, and ends
        by starting the demo workload, whose process marks it finished.
        """
        C.wait_ssm(self.args, C.send_ssm(self.args, "docker restart informix-cdc"), 600)
        probe = (
            "docker exec informix-cdc sh -c "
            "'ps -ef | grep -v grep | grep -c newhealth_cdc_workload; true'; true"
        )
        deadline = time.monotonic() + 25 * 60
        while time.monotonic() < deadline:
            lines = C.wait_ssm(self.args, C.send_ssm(self.args, probe), 120).strip().splitlines()
            if lines and lines[-1].strip().isdigit() and int(lines[-1]) > 0:
                return
            time.sleep(30)
        raise RuntimeError("the Informix container did not finish rebuilding in 25 minutes")

    def rows(self, table: str, columns: list[str]) -> list[tuple]:
        """Every row of ``table`` as a tuple of strings (None for NULL), via UNLOAD."""
        out = self.sql(
            f"UNLOAD TO '/tmp/kstar_rows.unl' SELECT {', '.join(columns)} FROM informix.{table};\n"
        )
        return [tuple(_unload_value(v) for v in fields) for fields in _unload_records(out)]

    def rowids(self, table: str, where: str) -> list[int]:
        out = self.sql(
            f"UNLOAD TO '/tmp/kstar_rowid.unl' SELECT ROWID FROM informix.{table} WHERE {where};\n"
        )
        return [int(fields[0]) for fields in _unload_records(out)]


def _unload_records(out: str) -> list[list[str]]:
    start = out.find("__FILE=")
    if start < 0:
        return []
    body = out[out.index("\n", start) + 1 : out.index("__END", start)]
    return [C.parse_unload_line(record) for record in C.iter_unload_records(body)]


def _unload_value(value: str) -> str | None:
    return None if value == "" else value


class _ModelOnlyInformix:
    """Stands in for Informix while earlier phases replay on the model alone."""

    def sql(self, statements: str) -> str:
        return "".join("(expression)\n0\n" for _ in range(statements.count("EXECUTE FUNCTION")))

    def rowids(self, _table: str, _where: str) -> list[int]:
        return [1, 2]


def full_row_logging(table: str, enabled: bool) -> str:
    return (
        "EXECUTE FUNCTION syscdcv1:cdc_set_fullrowlogging("
        f"'testdb:informix.{table}', {1 if enabled else 0});\n"
    )


def _function_results(out: str) -> list[str]:
    """The values dbaccess printed under ``(expression)`` for EXECUTE FUNCTION calls."""
    lines = out.splitlines()
    results = []
    for index, line in enumerate(lines):
        if line.strip() == "(expression)":
            value = next((text.strip() for text in lines[index + 1 :] if text.strip()), "")
            results.append(value)
    return results


# ---- Databricks -------------------------------------------------------------------


class Databricks:
    def __init__(self, profile: str, warehouse_id: str) -> None:
        self.profile = profile
        self.warehouse_id = warehouse_id

    def cli(self, *args: str, input_json: dict | None = None) -> Any:
        command = ["databricks", *args, "-p", self.profile, "-o", "json"]
        path = None
        if input_json is not None:
            with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
                json.dump(input_json, handle)
            path = handle.name
            command += ["--json", f"@{path}"]
        try:
            done = subprocess.run(command, capture_output=True, text=True, check=False)
        finally:
            if path is not None:
                os.unlink(path)
        if done.returncode != 0:
            raise RuntimeError(f"databricks {' '.join(args[:2])} failed: {done.stderr.strip()}")
        return json.loads(done.stdout) if done.stdout.strip() else None

    def sql(self, statement: str) -> list[list[Any]]:
        body = {"warehouse_id": self.warehouse_id, "wait_timeout": "50s", "statement": statement}
        result = self.cli("api", "post", "/api/2.0/sql/statements", input_json=body)
        while result["status"]["state"] in ("PENDING", "RUNNING"):
            time.sleep(3)
            result = self.cli("api", "get", f"/api/2.0/sql/statements/{result['statement_id']}")
        if result["status"]["state"] != "SUCCEEDED":
            raise RuntimeError(f"SQL failed: {result['status']}\n{statement}")
        return (result.get("result") or {}).get("data_array") or []


class Pipeline:
    def __init__(self, dbx: Databricks, pipeline_id: str) -> None:
        self.dbx = dbx
        self.id = pipeline_id

    def update_state(self, update_id: str) -> dict:
        return self.dbx.cli("pipelines", "get-update", self.id, update_id)["update"]

    def start(self, *, full_refresh: bool = False) -> str:
        """Start an update, retrying while the pipeline still settles a finished one."""
        args = ["pipelines", "start-update", self.id] + (["--full-refresh"] if full_refresh else [])
        deadline = time.monotonic() + 5 * 60
        while True:
            try:
                return self.dbx.cli(*args)["update_id"]
            except RuntimeError as error:
                if time.monotonic() >= deadline:
                    raise
                log(f"  could not start an update yet ({str(error)[:300]}); retrying")
                time.sleep(20)

    def wait(self, update_id: str, timeout: int = UPDATE_TIMEOUT_SECONDS) -> str:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            state = self.update_state(update_id)["state"]
            if state in TERMINAL:
                return state
            time.sleep(15)
        raise RuntimeError(f"update {update_id} did not finish in {timeout}s")

    def run(self, *, full_refresh: bool = False) -> tuple[str, str]:
        update_id = self.start(full_refresh=full_refresh)
        log(f"  update {update_id} started (full_refresh={full_refresh})")
        state = self.wait(update_id)
        log(f"  update {update_id} {state}")
        return update_id, state

    def stop(self) -> None:
        subprocess.run(
            [
                "databricks",
                "pipelines",
                "stop",
                self.id,
                "-p",
                self.dbx.profile,
                "--timeout",
                "20m",
            ],
            capture_output=True,
            text=True,
            check=False,
        )

    def set_continuous(self, continuous: bool) -> None:
        spec = self.dbx.cli("pipelines", "get", self.id)["spec"]
        spec["continuous"] = continuous
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
            json.dump(spec, handle)
        try:
            subprocess.run(
                [
                    "databricks",
                    "pipelines",
                    "update",
                    self.id,
                    "-p",
                    self.dbx.profile,
                    "--json",
                    f"@{handle.name}",
                ],
                capture_output=True,
                text=True,
                check=True,
            )
        finally:
            os.unlink(handle.name)

    def latest_update(self) -> str | None:
        updates = self.dbx.cli("pipelines", "get", self.id).get("latest_updates") or []
        return updates[0]["update_id"] if updates else None

    def ensure_running(self, *, full_refresh: bool = False) -> str:
        """The running continuous update, or a new one; a full refresh always starts anew."""
        latest = self.latest_update()
        if latest and not full_refresh and self.update_state(latest)["state"] not in TERMINAL:
            return latest
        if full_refresh:
            self.stop()
        update_id = self.start(full_refresh=full_refresh)
        log(f"  continuous update {update_id} started (full_refresh={full_refresh})")
        return update_id

    def errors(self, update_id: str) -> list[str]:
        """The update's ERROR and WARN events with their exceptions.

        A flow's exception sits in the event's ``error`` column, on each failed attempt
        (a WARN while the flow is retried); the final ERROR event carries none.
        """
        rows = self.dbx.sql(
            "SELECT message, CAST(error AS STRING) "
            f"FROM event_log('{self.id}') WHERE origin.update_id = '{update_id}' "
            "AND level IN ('ERROR', 'WARN') ORDER BY timestamp"
        )
        return [f"{message} {error or ''}" for message, error in rows]


# ---- Expected destinations --------------------------------------------------------


class Model:
    """The rows Informix holds and what each destination should show for them.

    ``scd1`` counts physical destination rows per current-column tuple: normally 0 or
    1, but after a drop collides rows, one key can cover several. ``scd2`` lists the
    open versions under each key by the change that opened them; one snapshot opens
    them all at the same position. A delete of a key removes every SCD1 row under it
    and closes the newest SCD2 versions: all of those tied for newest (measured live).
    """

    def __init__(self, columns: list[str]) -> None:
        self.columns = list(columns)
        self.source: list[tuple] = []
        self.scd1: collections.Counter = collections.Counter()
        self.scd2: dict[tuple, list[int]] = {}
        self.ops: collections.Counter = collections.Counter()
        self.clock = 0

    def load(self, rows: list[tuple]) -> None:
        """An initial snapshot (or a full refresh) of the current source rows."""
        self.source += [tuple(row) for row in rows]
        self.full_refresh()

    def full_refresh(self) -> None:
        self.clock += 1
        self.scd1 = collections.Counter(set(self.source))
        self.scd2 = {row: [self.clock] for row in set(self.source)}
        self.ops = collections.Counter(r=len(self.source))

    def _upsert(self, row: tuple) -> None:
        self.clock += 1
        if not self.scd1[row]:
            self.scd1[row] = 1
        if not self.scd2.get(row):
            self.scd2[row] = [self.clock]

    def _remove(self, row: tuple) -> None:
        self.clock += 1
        self.scd1[row] = 0
        opened = self.scd2.get(row, [])
        if opened:
            self.scd2[row] = [start for start in opened if start != max(opened)]

    def insert(self, row: tuple) -> None:
        self.source.append(tuple(row))
        self._upsert(tuple(row))
        self.ops["c"] += 1

    def matching(self, predicate: Callable[[dict], bool]) -> list[int]:
        return [i for i, row in enumerate(self.source) if predicate(dict(zip(self.columns, row)))]

    def delete(self, predicate: Callable[[dict], bool], *, one: bool = False) -> None:
        indexes = self.matching(predicate)
        if not indexes:
            raise AssertionError("model delete matched nothing")
        for index in sorted(indexes[:1] if one else indexes, reverse=True):
            self._remove(self.source.pop(index))
            self.ops["d"] += 1

    def update(
        self, predicate: Callable[[dict], bool], changes: dict, *, one: bool = False
    ) -> None:
        indexes = self.matching(predicate)
        if not indexes:
            raise AssertionError("model update matched nothing")
        for index in indexes[:1] if one else indexes:
            old = self.source[index]
            new = tuple(changes.get(name, value) for name, value in zip(self.columns, old))
            self.source[index] = new
            if old != new:
                self._remove(old)
                self.ops["d"] += 1
            self._upsert(new)
            self.ops["u"] += 1

    def retype(self, name: str, convert: Callable[[str], str]) -> None:
        """Re-render a widened column's values as both sides now print them."""
        index = self.columns.index(name)

        def fix(row: tuple) -> tuple:
            value = row[index]
            return row[:index] + (None if value is None else convert(value),) + row[index + 1 :]

        self.source = [fix(row) for row in self.source]
        scd1: collections.Counter = collections.Counter()
        for row, n in self.scd1.items():
            scd1[fix(row)] += n
        scd2: dict[tuple, list[int]] = {}
        for row, opened in self.scd2.items():
            scd2.setdefault(fix(row), []).extend(opened)
        self.scd1, self.scd2 = scd1, scd2

    def add_column(self, name: str) -> None:
        self.columns.append(name)
        self.source = [row + (None,) for row in self.source]
        self.scd1 = collections.Counter({row + (None,): n for row, n in self.scd1.items()})
        self.scd2 = {row + (None,): opened for row, opened in self.scd2.items()}

    def drop_column(self, name: str) -> None:
        index = self.columns.index(name)
        project = lambda row: row[:index] + row[index + 1 :]  # noqa: E731
        self.columns.pop(index)
        self.source = [project(row) for row in self.source]
        scd1: collections.Counter = collections.Counter()
        for row, n in self.scd1.items():
            scd1[project(row)] += n
        scd2: dict[tuple, list[int]] = {}
        for row, opened in self.scd2.items():
            scd2.setdefault(project(row), []).extend(opened)
        self.scd1, self.scd2 = scd1, scd2

    def expected(self, attr: str) -> collections.Counter:
        if attr == "scd2":
            return +collections.Counter({row: len(opened) for row, opened in self.scd2.items()})
        return +getattr(self, attr)

    def present(self) -> collections.Counter:
        return collections.Counter({row: 1 for row, n in self.scd1.items() if n})


# ---- The probe --------------------------------------------------------------------

BASE_COLUMNS = ["id", "name", "qty", "code", "note"]
BASE_DDL = "id INT NOT NULL, name VARCHAR(20), qty INT, code CHAR(3) NOT NULL, note VARCHAR(20)"


def v(*values: Any) -> tuple:
    """A row as the model and both sides' readers see it: strings, None for NULL."""
    return tuple(None if value is None else str(value) for value in values)


def lit(value: str | None) -> str:
    if value is None:
        return "NULL"
    return value if value.lstrip("-").isdigit() else "'" + value.replace("'", "''") + "'"


class Probe:
    def __init__(self, cli: argparse.Namespace) -> None:
        self.cli = cli
        self.run_id = datetime.datetime.now(datetime.timezone.utc).strftime("%m%d%H%M%S")
        self.prefix = f"kstar_{self.run_id}"
        self.variants = {
            "s1": f"{self.prefix}_s1",
            "s2": f"{self.prefix}_s2",
            "log": f"{self.prefix}_log",
        }
        self.heartbeat = f"{self.prefix}_hb"
        self.beat = 0
        self.ifx = Informix()
        self.dbx = Databricks(cli.profile, cli.warehouse_id)
        self.model = Model(BASE_COLUMNS)
        # Column name -> its DDL, in table order: what a rebuild after a restart creates.
        self.ddl = dict(part.split(" ", 1) for part in BASE_DDL.split(", "))
        self.results: list[dict] = []
        self.lagged: list[str] = []
        self.stalls: list[dict] = []
        self.skipped: list[str] = []  # checks the server made unreachable
        self.refresh_next = False
        self.replaying = False  # fast_forward: phases change only the model
        self.pipeline: Pipeline | None = None
        user = self.dbx.cli("current-user", "me")["userName"]
        self.workspace_dir = f"/Users/{user}/keyless_star_probe_{self.run_id}"
        self.schema = f"{cli.catalog}.{cli.schema}"

    # -- setup / teardown

    def deploy(self) -> None:
        """Upload this checkout's connector and an ingest file naming the probe tables."""
        src = REPO_SRC / "databricks"
        with tempfile.TemporaryDirectory() as stage_dir:
            stage = pathlib.Path(stage_dir) / "src" / "databricks"
            ignore = shutil.ignore_patterns(
                "__pycache__", "*.pyc", "*_test.py", "tests", "build", "*.egg-info"
            )
            for name in (
                "__init__.py",
                "labs/__init__.py",
                "labs/community_connector/__init__.py",
                "labs/community_connector/sources/__init__.py",
            ):
                (stage / name).parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(src / name, stage / name)
            for package in ("interface", "libs", "pipeline", "sparkpds"):
                shutil.copytree(
                    src / "labs/community_connector" / package,
                    stage / "labs/community_connector" / package,
                    ignore=ignore,
                )
            informix = src / "labs/community_connector/sources/informix"
            target = stage / "labs/community_connector/sources/informix"
            target.mkdir(parents=True)
            for name in (
                "__init__.py",
                "_generated_informix_python_source.py",
                "cdc_protocol.py",
                "informix.py",
                "lakebase_state.py",
                "sqli.py",
                "connector_spec.yaml",
            ):
                shutil.copy(informix / name, target / name)
            ingest = pathlib.Path(stage_dir) / "ingest.py"
            ingest.write_text(self.ingest_source())
            subprocess.run(
                ["databricks", "workspace", "mkdirs", self.workspace_dir, "-p", self.cli.profile],
                check=True,
                capture_output=True,
            )
            subprocess.run(
                [
                    "databricks",
                    "workspace",
                    "import-dir",
                    str(pathlib.Path(stage_dir) / "src"),
                    f"{self.workspace_dir}/src",
                    "--overwrite",
                    "-p",
                    self.cli.profile,
                ],
                check=True,
                capture_output=True,
            )
            subprocess.run(
                [
                    "databricks",
                    "workspace",
                    "import",
                    f"{self.workspace_dir}/ingest.py",
                    "--file",
                    str(ingest),
                    "--format",
                    "AUTO",
                    "--language",
                    "PYTHON",
                    "--overwrite",
                    "-p",
                    self.cli.profile,
                ],
                check=True,
                capture_output=True,
            )
        log(f"deployed connector and ingest file to {self.workspace_dir}")

    def ingest_source(self) -> str:
        star = {"primary.keys": "*", "snapshot.mode": "initial", "snapshot.scan": "cursor"}
        configs = {
            self.variants["s1"]: {**star, "scd_type": "SCD_TYPE_1"},
            self.variants["s2"]: {**star, "scd_type": "SCD_TYPE_2"},
            self.variants["log"]: {
                **star,
                "append.only.ingestion": "true",
                "scd_type": "APPEND_ONLY",
            },
            self.heartbeat: {},
        }
        objects = [
            {
                "table": {
                    "source_table": table,
                    "table_configuration": {
                        "qualified_source_table": f"informix.{table}",
                        **options,
                    },
                }
            }
            for table, options in configs.items()
        ]
        spec = {"connection_name": self.cli.connection, "objects": objects}
        return (
            "from databricks.labs.community_connector.pipeline import ingest\n"
            "from databricks.labs.community_connector import register\n\n"
            "spark.conf.set(\n"
            '    "spark.databricks.unityCatalog.connectionDfOptionInjection.enabled", "true"\n'
            ")\n\n"
            f"pipeline_spec = {json.dumps(spec, indent=4)}\n\n"
            'register(spark, "informix")\n'
            "ingest(spark, pipeline_spec)\n"
        )

    def create_pipeline(self) -> None:
        self.dbx.sql(
            f"CREATE SCHEMA IF NOT EXISTS {self.schema} COMMENT 'keyless_star_probe_test_tool'"
        )
        created = self.dbx.cli(
            "pipelines",
            "create",
            input_json={
                "name": f"keyless_star_probe_{self.run_id}",
                "catalog": self.cli.catalog,
                "schema": self.cli.schema,
                "serverless": True,
                "channel": "CURRENT",
                "continuous": self.cli.mode == "continuous",
                "development": False,
                "root_path": f"{self.workspace_dir}/src",
                "libraries": [{"file": {"path": f"{self.workspace_dir}/ingest.py"}}],
                "configuration": {"pipelines.enableTypeWidening": "true"},
            },
        )
        self.pipeline = Pipeline(self.dbx, created["pipeline_id"])
        log(f"created pipeline {self.pipeline.id}")

    def teardown(self) -> None:
        if self.cli.keep:
            log("kept the pipeline, schema and workspace folder (--keep)")
            return
        # Setup may have failed part way: clean up whatever it got to.
        if self.pipeline is not None:
            self.pipeline.stop()
            subprocess.run(
                ["databricks", "pipelines", "delete", self.pipeline.id, "-p", self.cli.profile],
                capture_output=True,
                check=False,
            )
        # Deleting the pipeline can leave its __materialization_* storage tables behind.
        leftovers = self.dbx.sql(
            f"SELECT table_name FROM {self.cli.catalog}.information_schema.tables "
            f"WHERE table_schema = '{self.cli.schema}' AND table_name LIKE '%{self.prefix}%'"
        )
        for table in {*self.variants.values(), self.heartbeat, *(row[0] for row in leftovers)}:
            try:
                self.dbx.sql(f"DROP TABLE IF EXISTS {self.schema}.`{table}`")
            except RuntimeError as error:
                log(f"could not drop {table}: {error}")
        try:  # only when empty: --schema may name a schema holding other tables
            self.dbx.sql(f"DROP SCHEMA IF EXISTS {self.schema}")
        except RuntimeError:
            pass
        subprocess.run(
            [
                "databricks",
                "workspace",
                "delete",
                self.workspace_dir,
                "--recursive",
                "-p",
                self.cli.profile,
            ],
            capture_output=True,
            check=False,
        )
        log(
            "deleted the pipeline, its destination tables and the workspace folder, as far as "
            f"they were created; Informix tables {self.prefix}_* are left in place"
        )

    # -- Informix changes, applied to every variant table and to the model

    def each(self, template: str) -> str:
        return "".join(template.format(t=table) + "\n" for table in self.variants.values())

    def create_tables(self, rows: list[tuple] = ()) -> None:
        """Create the variant tables with the current columns, holding ``rows``."""
        columns = ", ".join(f"{name} {spec}" for name, spec in self.ddl.items())
        statements = self.each(f"CREATE TABLE informix.{{t}} ({columns});")
        for row in rows:
            values = ", ".join(lit(value) for value in row)
            statements += self.each(
                f"INSERT INTO informix.{{t}} ({', '.join(self.ddl)}) VALUES ({values});"
            )
        statements += (
            f"CREATE TABLE informix.{self.heartbeat} (id INT NOT NULL PRIMARY KEY, n INT);\n"
        )
        statements += f"INSERT INTO informix.{self.heartbeat} VALUES (1, {self.beat});\n"
        self.ifx.sql(statements)
        log(f"created Informix tables {self.prefix}_s1/_s2/_log/_hb holding {len(rows)} rows")

    def reset_tables(self, rows: list[tuple] = ()) -> None:
        """Put the variant tables back to holding ``rows``, in place: no table is dropped."""
        statements = self.each("DELETE FROM informix.{t};")
        for row in rows:
            values = ", ".join(lit(value) for value in row)
            statements += self.each(
                f"INSERT INTO informix.{{t}} ({', '.join(self.ddl)}) VALUES ({values});"
            )
        statements += f"UPDATE informix.{self.heartbeat} SET n = {self.beat} WHERE id = 1;\n"
        self.ifx.sql(statements)
        log(f"reset Informix tables {self.prefix}_s1/_s2/_log to {len(rows)} rows")

    def insert(self, *rows: tuple) -> str:
        sql = ""
        for row in rows:
            values = ", ".join(lit(value) for value in row)
            sql += self.each(
                f"INSERT INTO informix.{{t}} ({', '.join(self.model.columns)}) VALUES ({values});"
            )
            self.model.insert(row)
        return sql

    def update(self, where: dict, changes: dict) -> str:
        condition = " AND ".join(f"{k} = {lit(v_)}" for k, v_ in where.items())
        assignment = ", ".join(
            f"{k} = {lit(v_)}" if v_ is not ... else f"{k} = {k}" for k, v_ in changes.items()
        )
        self.model.update(
            lambda row: all(row[k] == v_ for k, v_ in where.items()),
            {k: v_ for k, v_ in changes.items() if v_ is not ...},
        )
        return self.each(f"UPDATE informix.{{t}} SET {assignment} WHERE {condition};")

    def delete(self, where: dict) -> str:
        condition = " AND ".join(
            f"{k} IS NULL" if v_ is None else f"{k} = {lit(v_)}" for k, v_ in where.items()
        )
        self.model.delete(lambda row: all(row[k] == v_ for k, v_ in where.items()))
        return self.each(f"DELETE FROM informix.{{t}} WHERE {condition};")

    def delete_one_copy(self, where: dict) -> None:
        """Delete one physical copy of a duplicated row, by ROWID, in each table."""
        condition = " AND ".join(f"{k} = {lit(v_)}" for k, v_ in where.items())
        statements = ""
        for table in self.variants.values():
            rowids = self.ifx.rowids(table, condition)
            if len(rowids) < 2:
                raise AssertionError(
                    f"{table}: expected duplicates for {where}, found {len(rowids)}"
                )
            statements += f"DELETE FROM informix.{table} WHERE ROWID = {max(rowids)};\n"
        self.ifx.sql(statements)
        self.model.delete(lambda row: all(row[k] == v_ for k, v_ in where.items()), one=True)

    def update_one_copy(self, where: dict, changes: dict) -> None:
        """Update one physical copy of a duplicated row, by ROWID, in each table."""
        condition = " AND ".join(f"{k} = {lit(v_)}" for k, v_ in where.items())
        assignment = ", ".join(f"{k} = {lit(v_)}" for k, v_ in changes.items())
        statements = ""
        for table in self.variants.values():
            rowids = self.ifx.rowids(table, condition)
            if len(rowids) < 2:
                raise AssertionError(
                    f"{table}: expected duplicates for {where}, found {len(rowids)}"
                )
            statements += f"UPDATE informix.{table} SET {assignment} WHERE ROWID = {max(rowids)};\n"
        self.ifx.sql(statements)
        self.model.update(
            lambda row: all(row[k] == v_ for k, v_ in where.items()), changes, one=True
        )

    def column_type(self, column: str) -> dict[str, str | None]:
        """Each variant destination's type for ``column``."""
        types = {}
        for table in self.variants.values():
            rows = self.dbx.sql(
                f"SELECT full_data_type FROM {self.cli.catalog}.information_schema.columns "
                f"WHERE table_schema = '{self.cli.schema}' AND table_name = '{table}' "
                f"AND column_name = '{column}'"
            )
            types[table] = rows[0][0] if rows else None
        return types

    def alter(
        self,
        *clauses: str,
        attempts: int = 1,
        while_running: bool = False,
        skip: tuple[str, ...] = (),
    ) -> None:
        """ALTER every variant table as the README's DDL steps say: logging off, ALTER, on.

        One table at a time, so a refusal leaves the others untouched, and full-row
        logging is switched back on whatever happens. Informix refuses both while a CDC
        session captures the table, so a running continuous update is stopped first (the
        next step starts a new one) unless ``while_running``; with ``attempts`` > 1 a
        refusal is retried, which lands the DDL in a gap between a running pipeline's polls.
        A failure raises DdlRefused naming the tables already changed; ``skip`` leaves
        those out of a second attempt.
        """
        if self.cli.mode == "continuous" and not while_running and not self.replaying:
            self.pipeline.stop()
            # The pipeline is idle, but Informix takes a few seconds more to drop its
            # CDC sessions, and refuses the DDL until it has.
            attempts = max(attempts, 40)
        done: list[str] = []
        for table in self.variants.values():
            if table in skip:
                continue
            statements = full_row_logging(table, False)
            statements += "".join(f"ALTER TABLE informix.{table} {clause};\n" for clause in clauses)
            statements += full_row_logging(table, True)
            for attempt in range(1, attempts + 1):
                try:
                    out = self.ifx.sql(statements)
                    break
                except RuntimeError as error:
                    self.ifx.sql(full_row_logging(table, True))
                    if attempt == attempts or not _captured_refusal(str(error)):
                        raise DdlRefused(str(error), done) from error
                    time.sleep(random.uniform(0.2, 2.0))
            if attempts > 1:
                log(f"  {table}: the DDL went through on attempt {attempt}")
            results = _function_results(out)
            if results != ["0", "0"]:
                self.ifx.sql(full_row_logging(table, True))
                raise RuntimeError(
                    f"cdc_set_fullrowlogging returned {results} for {table}:\n{out[-1500:]}"
                )
            done.append(table)
        for clause in clauses:
            action, name, spec = re.fullmatch(r"(ADD|MODIFY|DROP) \((\w+) ?(.*)\)", clause).groups()
            if action == "DROP":
                del self.ddl[name]
            else:
                self.ddl[name] = spec

    def nudge(self) -> None:
        self.beat += 1
        self.ifx.sql(f"UPDATE informix.{self.heartbeat} SET n = {self.beat} WHERE id = 1;\n")

    # -- checks

    def target_rows(self, variant: str) -> collections.Counter:
        table = f"{self.schema}.{self.variants[variant]}"
        cols = ", ".join(f"CAST({c} AS STRING)" for c in self.model.columns)
        if variant == "s1":
            rows = self.dbx.sql(f"SELECT {cols} FROM {table}")
        elif variant == "s2":
            rows = self.dbx.sql(f"SELECT {cols} FROM {table} WHERE __END_AT IS NULL")
        else:
            partition = ", ".join(self.model.columns)
            rows = self.dbx.sql(
                f"SELECT {cols} FROM (SELECT *, row_number() OVER (PARTITION BY {partition} "
                f"ORDER BY _informix_change_lsn DESC) AS rn FROM {table}) "
                "WHERE rn = 1 AND _informix_op <> 'd'"
            )
        return collections.Counter(tuple(row) for row in rows)

    def check(self, phase: str, *, ops: bool = True) -> bool:
        """Compare every destination with the model and record the result.

        Each table's upsert and delete flows checkpoint on their own, so the heartbeat
        arriving shows only that its own flow read to the end. A destination still
        short gets more updates, within HEARTBEAT_BUDGET_SECONDS, before it fails:
        wrong rows stay wrong across updates, late ones settle (and are recorded).
        """
        if self.replaying:
            return True
        source = _canonical(
            collections.Counter(self.ifx.rows(self.variants["s1"], self.model.columns))
        )
        modelled = _canonical(collections.Counter(self.model.source))
        if source != modelled:
            problems = ["source differs from the model: " + _diff(modelled, source)]
            self.record(phase, False, problems)
            return False
        deadline = time.monotonic() + HEARTBEAT_BUDGET_SECONDS
        problems = self._readable_problems(ops)
        extra = 0
        while problems and time.monotonic() < deadline:
            extra += 1
            if self.cli.mode == "continuous":
                time.sleep(30)
            elif self.pipeline.run()[1] != "COMPLETED":
                break
            problems = self._readable_problems(ops)
        if extra and not problems:
            what = "re-checks 30 s apart" if self.cli.mode == "continuous" else "more updates"
            self.lagged.append(f"{phase}: matched only after {extra} {what}")
            log(f"  {phase}: the destinations matched only after {extra} {what}")
        self.record(phase, not problems, problems)
        return not problems

    def _readable_problems(self, ops: bool) -> list[str]:
        try:
            return self.destination_problems(ops)
        except RuntimeError as error:
            if not _refreshing(error):
                raise
            return [f"a destination is mid-refresh: {str(error)[:200]}"]

    def destination_problems(self, ops: bool) -> list[str]:
        problems: list[str] = []
        expectations = {
            "s1": self.model.expected("scd1"),
            "s2": self.model.expected("scd2"),
            "log": self.model.present(),
        }
        for variant, expected in expectations.items():
            expected, actual = _canonical(expected), _canonical(self.target_rows(variant))
            if actual != expected:
                problems.append(f"{variant}: {_diff(expected, actual)}")
        if ops:
            counted = dict(
                self.dbx.sql(
                    "SELECT _informix_op, CAST(count(*) AS STRING) "
                    f"FROM {self.schema}.{self.variants['log']} "
                    "GROUP BY _informix_op"
                )
            )
            actual_ops = {op: int(n) for op, n in counted.items()}
            if actual_ops != dict(+self.model.ops):
                problems.append(f"log events {actual_ops} != expected {dict(+self.model.ops)}")
        beat = self.dbx.sql(
            f"SELECT CAST(n AS STRING) FROM {self.schema}.{self.heartbeat} WHERE id = 1"
        )
        if [row[0] for row in beat] != [str(self.beat)]:
            problems.append(
                f"heartbeat {beat} != {self.beat}: the run did not reach the end of the log"
            )
        return problems

    def record(self, phase: str, passed: bool, details: list[str] | str = ()) -> None:
        if self.replaying:
            return
        details = [details] if isinstance(details, str) else list(details)
        self.results.append({"phase": phase, "passed": passed, "details": details})
        log(f"{'PASS' if passed else 'FAIL'} {phase}" + "".join(f"\n    {d}" for d in details))

    def run_update(self, phase: str, *, full_refresh: bool = False) -> tuple[str, str]:
        """Read until the heartbeat written just before arrives.

        Continuous mode waits on one running update; triggered mode runs updates until
        it arrives. Late arrivals are recorded; a heartbeat still missing after
        HEARTBEAT_BUDGET_SECONDS raises Stalled.
        """
        if self.refresh_next:
            full_refresh, self.refresh_next = True, False
        self.nudge()
        if self.replaying:
            return "", "COMPLETED"
        deadline = time.monotonic() + HEARTBEAT_BUDGET_SECONDS
        if self.cli.mode == "continuous":
            return self._wait_continuous(phase, full_refresh, deadline)
        attempt = 0
        while True:
            attempt += 1
            refresh = full_refresh and attempt == 1
            update_id, state = self.pipeline.run(full_refresh=refresh)
            if state != "COMPLETED":
                errors = self.pipeline.errors(update_id)
                raise RuntimeError(f"{phase}: update {update_id} {state}: {errors[:3]}")
            flagged = self.pipeline.update_state(update_id).get("full_refresh")
            if bool(flagged) != refresh:
                raise RuntimeError(f"{phase}: update {update_id} full_refresh={flagged}")
            beat = self.dbx.sql(
                f"SELECT CAST(n AS STRING) FROM {self.schema}.{self.heartbeat} WHERE id = 1"
            )
            if [row[0] for row in beat] == [str(self.beat)]:
                if attempt > 1:
                    self.lagged.append(f"{phase}: needed {attempt} updates")
                    log(f"  {phase}: the heartbeat arrived only in update {attempt}")
                return update_id, state
            if time.monotonic() >= deadline:
                raise Stalled(
                    f"{phase}: heartbeat {self.beat} missing after {attempt} updates "
                    f"({HEARTBEAT_BUDGET_SECONDS // 60} minutes)"
                )

    def _wait_continuous(self, phase: str, full_refresh: bool, deadline: float) -> tuple[str, str]:
        started = time.monotonic()
        update_id = self.pipeline.ensure_running(full_refresh=full_refresh)
        while True:
            state = self.pipeline.update_state(update_id)["state"]
            if state in TERMINAL:
                errors = self.pipeline.errors(update_id)
                raise RuntimeError(f"{phase}: continuous update {update_id} {state}: {errors[:3]}")
            if self._heartbeat_arrived():
                minutes = (time.monotonic() - started) / 60
                if minutes > 3:
                    self.lagged.append(f"{phase}: heartbeat after {minutes:.1f} minutes")
                log(f"  {phase}: the heartbeat arrived after {minutes:.1f} minutes")
                return update_id, state
            if time.monotonic() >= deadline:
                raise Stalled(
                    f"{phase}: heartbeat {self.beat} missing after "
                    f"{HEARTBEAT_BUDGET_SECONDS // 60} minutes of continuous update {update_id}"
                )
            time.sleep(15)

    def _heartbeat_arrived(self) -> bool:
        try:
            beat = self.dbx.sql(
                f"SELECT CAST(n AS STRING) FROM {self.schema}.{self.heartbeat} WHERE id = 1"
            )
        except RuntimeError as error:
            if _refreshing(error):
                return False
            raise
        return [row[0] for row in beat] == [str(self.beat)]

    def recover(self, phase: str, stall: Stalled, saved: tuple) -> None:
        """Clear a stall and put the tables back as ``phase`` found them.

        An engine restart clears the stall and keeps testdb, so the tables are reset in
        place. Only a phase that had already changed the tables' DDL needs the container
        restart, which rebuilds testdb, so the tables can be created afresh. The phase's
        own results are discarded with it; the stall is reported separately, with the
        server's CDC state, since it is not a connector check.
        """
        model, ddl, results = saved
        self.stalls.append(
            {
                "phase": phase,
                "stall": str(stall),
                "discarded": self.results[results:],
                "diagnostics": self.ifx.shell(STALL_DIAGNOSTICS)[-4000:],
            }
        )
        if len(self.stalls) > MAX_RESTARTS:
            raise RuntimeError(f"CDC stalled {len(self.stalls)} times; giving up") from stall
        self.pipeline.stop()
        if self.cli.mode == "triggered":
            self.pipeline.set_continuous(False)
        ddl_changed = self.ddl != ddl
        self.model, self.ddl = model, ddl
        del self.results[results:]
        if list(self.ddl) != self.model.columns:
            raise RuntimeError(f"saved columns {list(self.ddl)} != model {self.model.columns}")
        if ddl_changed:
            log(f"STALL {stall}; the phase changed the tables' DDL, so restarting the "
                "Informix container (it rebuilds testdb)")
            self.ifx.restart()
            self.create_tables(self.model.source)
        else:
            log(f"STALL {stall}; restarting the Informix engine (testdb and the tables stay)")
            self.ifx.restart_engine()
            self.reset_tables(self.model.source)
        if not self.model.source:  # before the initial snapshot: the phase reloads itself
            self.refresh_next = True
            return
        self.run_update(f"{phase} resume", full_refresh=True)
        self.model.full_refresh()
        self.check(f"{phase} resume: a full refresh reloads the rebuilt tables")

    def expect_failure(self, phase: str, *needles: str) -> None:
        """Start an update that must fail with one of ``needles`` and end as FAILED."""
        self.nudge()
        if self.replaying:
            return
        update_id = self.pipeline.start()
        log(f"  update {update_id} started, expected to fail")
        deadline = time.monotonic() + EXPECTED_FAILURE_SECONDS
        state = self.pipeline.update_state(update_id)["state"]
        while state not in TERMINAL and time.monotonic() < deadline:
            time.sleep(15)
            state = self.pipeline.update_state(update_id)["state"]
        log(f"  update {update_id} {state}")
        errors = self.pipeline.errors(update_id)
        found = next((n for n in needles if any(n in error for error in errors)), needles[0])
        self.record(
            f"{phase}: the update fails with {' or '.join(repr(n) for n in needles)}",
            any(found in error for error in errors),
            _evidence(errors, found),
        )
        self.record(
            f"{phase}: the update ends as FAILED",
            state == "FAILED",
            f"state {state} after up to {EXPECTED_FAILURE_SECONDS // 60} minutes",
        )
        # Stops an update still running, and the retry a production-mode pipeline
        # starts on its own after a failure, so the recovery update can start.
        self.pipeline.stop()

    # -- phases

    def phase_1_initial(self) -> None:
        rows = [
            v(1, "one", 10, "AAA", "n1"),
            v(2, "two", 20, "BBB", "n2"),
            v(3, None, 30, "CCC", None),
            v(4, "four", None, "DDD", "n4"),
            v(5, "dup", 50, "EEE", "nd"),
            v(5, "dup", 50, "EEE", "nd"),
            v(6, "six", 60, "FFF", "x6a"),
            v(6, "six", 60, "FFF", "x6b"),
            v(7, "seven", 70, "GGG", "n7"),
        ]
        statements = ""
        for row in rows:
            values = ", ".join(lit(value) for value in row)
            statements += self.each(
                f"INSERT INTO informix.{{t}} ({', '.join(BASE_COLUMNS)}) VALUES ({values});"
            )
        self.ifx.sql(statements)
        self.model.load(rows)
        self.run_update("1 initial snapshot")
        self.check("1 initial snapshot with duplicates and NULLs")

    def phase_2_dml(self) -> None:
        sql = self.insert(
            v(9, "nine", 90, "III", "n9"),
            v(9, "nine", 90, "III", "n9"),
            v(10, None, None, "JJJ", None),
        )
        sql += self.update({"id": "1"}, {"name": "uno"})  # value to value
        sql += self.update({"id": "3"}, {"name": "three"})  # NULL to value
        sql += self.update({"id": "4"}, {"name": None})  # value to NULL
        sql += self.update({"id": "7"}, {"name": ...})  # no-op: same values
        sql += self.delete({"id": "2"})
        sql += self.delete({"id": "10", "name": None})  # a row holding NULLs
        self.ifx.sql(sql)
        self.delete_one_copy({"id": "5"})  # one of two duplicates
        txn = "BEGIN WORK;\n"
        txn += self.insert(v(11, "eleven", 110, "KKK", "n11"))
        txn += self.update({"id": "11"}, {"qty": "111"})
        txn += self.delete({"id": "9"})  # both copies
        txn += "COMMIT WORK;\n"
        self.ifx.sql(txn)
        self.run_update("2 DML")
        self.check(
            "2 inserts, key-changing and no-op updates, deletes, one duplicate copy, a transaction"
        )

    def phase_3_add_column(self) -> None:
        self.alter("ADD (extra INT)")
        self.model.add_column("extra")
        sql = self.update({"id": "7"}, {"extra": "1"})  # an old row: NULL to value
        sql += self.delete({"id": "4"})  # an old row holding NULL
        sql += self.insert(v(12, "twelve", 120, "LLL", "n12", 5))
        self.ifx.sql(sql)
        self.run_update("3 add column")
        self.check("3 nullable column added: the key grows, old rows still match")

    def phase_4_widen(self) -> None:
        self.alter("MODIFY (qty BIGINT)")
        sql = self.insert(v(13, "big", 5000000000, "MMM", "n13", None))
        sql += self.update({"id": "12"}, {"qty": "6000000000"})
        self.ifx.sql(sql)
        self.run_update("4 widen")
        if not self.replaying:
            types = {
                table: self.dbx.sql(
                    f"SELECT full_data_type FROM {self.cli.catalog}.information_schema.columns "
                    f"WHERE table_schema = '{self.cli.schema}' AND table_name = '{table}' "
                    "AND column_name = 'qty'"
                )
                for table in self.variants.values()
            }
            widened = all(rows and rows[0][0].upper() == "BIGINT" for rows in types.values())
            self.record("4 qty widened to BIGINT in every destination", widened, json.dumps(types))
        self.check("4 INTEGER widened to BIGINT, values above the INTEGER range land")

    def phase_5_drop_nullable(self) -> None:
        self.alter("DROP (note)")
        self.model.drop_column("note")
        sql = self.update({"id": "13"}, {"name": "bigger"})
        sql += self.insert(v(14, "fourteen", 140, "NNN", None))
        self.ifx.sql(sql)
        self.run_update("5 drop nullable")
        self.check("5 nullable column dropped: the key shrinks; id 6 rows now share a key")
        self.delete_one_copy({"id": "6"})
        self.run_update("5 collision delete")
        self.check(
            "5 deleting one collided copy: SCD1 removes both rows, SCD2 closes both "
            "(one snapshot loaded them, so both are the newest)"
        )

    def phase_6_drop_not_null(self) -> None:
        self.alter("DROP (code)")
        self.model.drop_column("code")
        sql = self.update({"id": "14"}, {"name": "fourteen2"})
        sql += self.delete({"id": "1"})
        sql += self.insert(v(15, "fifteen", 150, None))
        self.ifx.sql(sql)
        self.run_update("6 drop NOT NULL")
        self.check("6 NOT NULL column dropped without a full refresh")

    def phase_7_drop_and_add(self) -> None:
        self.alter("DROP (extra)", "ADD (tag VARCHAR(10))")
        self.model.drop_column("extra")
        self.model.add_column("tag")
        self.expect_failure("7 drop and add in one step", "primary keys; run a full refresh")
        self.run_update("7 recovery", full_refresh=True)
        self.model.full_refresh()
        self.check("7 full refresh recovers after a drop and an add in one step")

    def phase_8_add_with_default(self) -> None:
        self.alter("ADD (withdef INT DEFAULT 7)")
        self.model.columns.append("withdef")
        self.model.source = [row + ("7",) for row in self.model.source]
        self.expect_failure("8 column added with a default", "with a default")
        self.run_update("8 recovery", full_refresh=True)
        self.model.full_refresh()
        self.check("8 full refresh recovers after a column added with a default")

    def phase_9_drop_mid_update(self) -> None:
        if self.replaying:  # the running update is live-only; keep the model's changes
            self.alter("DROP (tag)")
            self.model.drop_column("tag")
            self.ifx.sql(self._phase_9_changes())
            return
        pipeline = self.pipeline
        triggered = self.cli.mode == "triggered"
        if triggered:
            before = pipeline.latest_update()
            pipeline.set_continuous(True)
            update_id = None
            for _ in range(24):
                latest = pipeline.latest_update()
                if latest and latest != before:
                    update_id = latest
                    break
                time.sleep(5)
            if update_id is None:
                update_id = pipeline.start()
            log(f"  continuous update {update_id} started")
        else:
            update_id = pipeline.ensure_running()
        time.sleep(150)  # let every flow start streaming
        try:
            self.alter("DROP (tag)", attempts=40, while_running=True)
        except DdlRefused as error:
            if not _captured_refusal(str(error)):
                raise
            # A running pipeline's CDC sessions left no gap for the DDL (continuous mode
            # keeps them open almost all the time), so the guard can't be reached. Drop
            # the column with the pipeline stopped and go on to the restart check.
            pipeline.stop()
            if triggered:
                pipeline.set_continuous(False)
            note = "9 drop during a running update: Informix refused the DDL on every try"
            self.skipped.append(note)
            log(f"  {note}; dropping it with the pipeline stopped instead")
            self.alter("DROP (tag)", skip=tuple(error.done))
            self.model.drop_column("tag")
            self.ifx.sql(self._phase_9_changes())
            self.run_update("9 restart")
            self.check("9 the restarted update adopts the shorter key without a full refresh")
            return
        self.model.drop_column("tag")
        needle = "during a running update; stop and restart"
        deadline = time.monotonic() + 300
        errors: list[str] = []
        while time.monotonic() < deadline:
            errors = pipeline.errors(update_id)
            if any(needle in error for error in errors):
                break
            time.sleep(20)
        self.record(
            "9 a running update fails asking for a restart",
            any(needle in error for error in errors),
            _evidence(errors, needle),
        )
        pipeline.stop()
        if triggered:
            pipeline.set_continuous(False)
        self.ifx.sql(self._phase_9_changes())
        self.run_update("9 restart")
        self.check("9 the restarted update adopts the shorter key without a full refresh")

    def _phase_9_changes(self) -> str:
        sql = self.update({"id": "3"}, {"name": "three2"})
        return sql + self.insert(v(16, "sixteen", 160, "7"))

    def phase_10_duplicates(self) -> None:
        sql = self.insert(
            v(20, "twenty", 200, 7),
            v(20, "twenty", 200, 7),
            v(21, "a21", 210, 7),
            v(22, "b22", 220, 7),
        )
        self.ifx.sql(sql)
        self.run_update("10 copies")
        self.check("10 two exact copies inserted collapse to one row")
        # One copy changes: the shared row leaves the target though a copy stays at the
        # source; a distinct row becomes a copy of another.
        self.update_one_copy({"id": "20"}, {"name": "twenty2"})
        self.ifx.sql(self.update({"id": "22"}, {"id": "21", "name": "a21", "qty": "210"}))
        self.run_update("10 update copies")
        self.check(
            "10 updating one copy removes the shared row; a row updated into a copy of "
            "another keeps one row"
        )
        self.delete_one_copy({"id": "21"})
        self.run_update("10 delete a made copy")
        self.check("10 deleting one copy of a row that became a duplicate removes the row")

    def phase_11_widen_more(self) -> None:
        self.alter("MODIFY (name VARCHAR(40))")
        sql = self.update({"id": "16"}, {"name": "sixteen with a much longer name"})
        sql += self.insert(v(23, "a name longer than twenty chars", 230, 7))
        self.ifx.sql(sql)
        self.run_update("11 longer VARCHAR")
        self.check("11 VARCHAR(20) lengthened to VARCHAR(40); old rows still match")
        for spec, label, qty, new_id in (
            ("DECIMAL(12,2)", "INTEGER to DECIMAL(12,2)", "71", 24),
            ("DECIMAL(14,2)", "DECIMAL(12,2) to DECIMAL(14,2)", "72", 25),
        ):
            self.alter(f"MODIFY (withdef {spec} DEFAULT 7)")
            self.model.retype("withdef", lambda value: f"{float(value):.2f}")
            # An old row's key change: its delete must match the widened target value.
            sql = self.update({"id": "7"}, {"qty": qty})
            sql += self.insert(v(new_id, "decimal", 240, "12.50"))
            self.ifx.sql(sql)
            self.run_update(f"11 {label}")
            if not self.replaying:
                types = self.column_type("withdef")
                expected = spec.lower()
                self.record(
                    f"11 withdef is {expected} in every destination",
                    all(found == expected for found in types.values()),
                    json.dumps(types),
                )
            self.check(f"11 {label}: old rows still match")

    def phase_12_narrow(self) -> None:
        self.alter("MODIFY (withdef DECIMAL(10,2) DEFAULT 7)")
        # The pipeline merges the narrower type the connector now reports into the existing
        # destination before any flow reads, so its schema-merge error usually comes first.
        self.expect_failure(
            "12 DECIMAL(14,2) narrowed to DECIMAL(10,2)",
            "other than by widening",
            "CANNOT_UPDATE_TABLE_SCHEMA",
        )
        self.run_update("12 recovery", full_refresh=True)
        self.model.full_refresh()
        self.check("12 full refresh recovers after a narrowing")

    PHASES = [
        "phase_1_initial",
        "phase_2_dml",
        "phase_3_add_column",
        "phase_4_widen",
        "phase_5_drop_nullable",
        "phase_6_drop_not_null",
        "phase_7_drop_and_add",
        "phase_8_add_with_default",
        "phase_9_drop_mid_update",
        "phase_10_duplicates",
        "phase_11_widen_more",
        "phase_12_narrow",
    ]

    def fast_forward(self, phases: int) -> None:
        """Replay the first ``phases`` phases on the model alone, touching nothing live.

        Leaves the model, column DDL and heartbeat as those phases end, so the run can
        build that state for real and start at the next phase.
        """
        live, self.ifx, self.replaying = self.ifx, _ModelOnlyInformix(), True
        try:
            for name in self.PHASES[:phases]:
                getattr(self, name)()
        finally:
            self.ifx, self.replaying = live, False

    def run(self) -> int:
        start = self.cli.start_phase
        if start > 1:
            self.fast_forward(start - 1)
            log(f"replayed phases 1-{start - 1} on the model; starting at phase {start}")
        try:
            self.create_tables(self.model.source)
            self.deploy()
            self.create_pipeline()
            if start > 1:
                self.run_update(f"{start} start", full_refresh=True)
                self.model.full_refresh()
                self.check(
                    f"{start} start: a full refresh loads the tables as phase {start} finds them"
                )
            index = start - 1
            while index < self.cli.end_phase:
                name = self.PHASES[index]
                saved = (copy.deepcopy(self.model), dict(self.ddl), len(self.results))
                log(f"== {name}")
                try:
                    getattr(self, name)()
                except Stalled as stall:
                    self.recover(name, stall, saved)
                    continue  # rerun the phase on the rebuilt tables
                index += 1
        # Report and stop: later phases depend on this one.
        except Exception as error:  # noqa: BLE001  # pylint: disable=broad-exception-caught
            self.record("aborted", False, f"{type(error).__name__}: {error}")
        finally:
            self.teardown()
        passed = sum(result["passed"] for result in self.results)
        log(f"{passed}/{len(self.results)} checks passed")
        if self.lagged:
            log("updates that ended before reading every change: " + "; ".join(self.lagged))
        for stall in self.stalls:
            log(f"CDC stalled in {stall['phase']} ({stall['stall']}); Informix was restarted")
        for note in self.skipped:
            log(f"not reached: {note}")
        if self.cli.report:
            pathlib.Path(self.cli.report).write_text(
                json.dumps(
                    {
                        "run": self.run_id,
                        "tables": [*self.variants.values(), self.heartbeat],
                        "pipeline": self.pipeline.id if self.pipeline else None,
                        "results": self.results,
                        "lagged": self.lagged,
                        "stalls": self.stalls,
                        "skipped": self.skipped,
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
        return 0 if passed == len(self.results) else 1


def _canonical(rows: collections.Counter) -> collections.Counter:
    """Rows with every decimal value written one way.

    Informix UNLOAD prints DECIMAL(12,2) 7 as ``7.0``, Databricks as ``7.00``; the column
    types are checked separately.
    """

    def value(text: str | None) -> str | None:
        if text is None or not re.fullmatch(r"-?\d+\.\d+", text):
            return text
        return format(decimal.Decimal(text).normalize(), "f")

    canonical: collections.Counter = collections.Counter()
    for row, n in rows.items():
        canonical[tuple(value(item) for item in row)] += n
    return canonical


def _refreshing(error: Exception) -> bool:
    """Whether a destination read failed only because an update is (re)creating it."""
    return any(code in str(error) for code in ("TABLE_OR_VIEW_NOT_FOUND", "UNRESOLVED_COLUMN"))


def _captured_refusal(output: str) -> bool:
    """Whether Informix refused the DDL because a CDC session captures the table."""
    return "-83710" in output or "19816:" in output


def _evidence(errors: list[str], needle: str) -> list[str]:
    """The text around ``needle`` in the first event holding it, else the first events."""
    for error in errors:
        at = error.find(needle)
        if at >= 0:
            return [error[max(at - 250, 0) : at + len(needle) + 50]]
    return [error[:400] for error in errors[:2]]


def _diff(expected: collections.Counter, actual: collections.Counter) -> str:
    missing = expected - actual
    extra = actual - expected
    return f"missing {dict(missing)} extra {dict(extra)}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--warehouse-id", required=True, help="SQL warehouse for destination reads")
    parser.add_argument("--profile", default="demo1", help="Databricks CLI profile")
    parser.add_argument("--connection", default="informix", help="Unity Catalog connection")
    parser.add_argument("--catalog", default="main")
    parser.add_argument("--schema", default="keyless_star_probe")
    parser.add_argument("--keep", action="store_true", help="keep the pipeline, schema and files")
    parser.add_argument("--report", help="write the results to this JSON file")
    parser.add_argument(
        "--start-phase",
        type=int,
        default=1,
        choices=range(1, len(Probe.PHASES) + 1),
        help="start at this phase, on tables built as the earlier phases leave them",
    )
    parser.add_argument(
        "--mode",
        choices=("continuous", "triggered"),
        default="continuous",
        help="continuous: each step waits on one running update; triggered: runs updates",
    )
    parser.add_argument(
        "--end-phase",
        type=int,
        default=len(Probe.PHASES),
        choices=range(1, len(Probe.PHASES) + 1),
        help="stop after this phase",
    )
    cli = parser.parse_args()
    if cli.end_phase < cli.start_phase:
        parser.error("--end-phase is before --start-phase")
    return Probe(cli).run()


if __name__ == "__main__":
    raise SystemExit(main())
