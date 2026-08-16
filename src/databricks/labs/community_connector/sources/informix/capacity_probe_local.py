"""Single-process, no-Spark capacity contention probe for the Informix connector.

The Spark-based probe cannot import the connector by its ``databricks.labs...``
package name on a Databricks cluster: the runtime already binds the installed
``databricks`` SDK package, so the connector namespace never resolves. This
variant sidesteps that entirely by loading the connector module *by file path*
and driving the real connection-slot protocol with plain threads on one node.

It measures the exact behavior behind the ``ConnectionCapacityUnavailable``
timeouts: how long ``PurePythonInformixBridge._acquire_connection_slot`` takes,
and how many time out, when more workers than slots contend for capacity on the
target Volume. It exercises the genuine fencing/lease/heartbeat code — only the
distribution (threads vs. Spark tasks) differs, so it captures single-node
contention, not cross-executor behavior.

Safe to run: it uses a disposable ``.informix-capacity-<token>`` endpoint under
the given location and never touches the pipeline's live ``limit-config`` or
``slot-*`` state, and never connects to Informix.

Usage (in any Python cell on a cluster, or a plain Python REPL with the
connector source available):

    # 1. point this at the connector SOURCE module (informix.py, which defines
    #    PurePythonInformixBridge at module scope). NOT the generated file --
    #    that wraps every class inside register_lakeflow_source() and exposes
    #    nothing importable.
    INFORMIX_PY = ("/Workspace/Users/leon.eller@databricks.com/informix-pipeline/"
                   "src/databricks/labs/community_connector/sources/informix/"
                   "informix.py")
    LOCATION = "/Volumes/members/connector_bronze/informix_cdc_state"

    # 2. paste this file's body (or %run it), then:
    import json
    print(json.dumps(run_capacity_probe(INFORMIX_PY, LOCATION,
                                        slot_count=16, workers=60,
                                        hold_seconds=3.0), indent=2))
"""

from __future__ import annotations

import importlib.util
import os
import secrets
import threading
import time
from typing import Any


def _ensure_stub_module(name: str):
    """Create an empty stub module (and its parent chain) in sys.modules."""

    import sys
    import types

    parts = name.split(".")
    for i in range(1, len(parts) + 1):
        sub = ".".join(parts[:i])
        if sub not in sys.modules:
            module = types.ModuleType(sub)
            module.__path__ = []  # mark as package so submodule imports resolve
            sys.modules[sub] = module
            if i > 1:
                setattr(sys.modules[".".join(parts[: i - 1])], parts[i - 1], module)
    return sys.modules[name]


def _extend_databricks_namespace(src_root: str) -> None:
    """Make ``databricks.labs.community_connector`` importable from ``src_root``.

    The Databricks runtime already binds the installed ``databricks`` SDK, whose
    ``__path__`` does not include the connector's source tree, so
    ``import databricks.labs...`` fails with ``No module named 'databricks.labs'``.
    Extending each existing package's ``__path__`` to also include ``src_root``
    lets the connector's real ``interface``/``sqli``/``cdc_protocol`` modules
    resolve normally, without shadowing the SDK.
    """

    import importlib
    import sys

    if src_root not in sys.path:
        sys.path.insert(0, src_root)
    # Walk the package chain that already exists (databricks, databricks.labs, ...)
    # and graft the connector src directory onto each __path__ so submodule
    # lookups find the connector files.
    chain = [
        ("databricks", os.path.join(src_root, "databricks")),
        ("databricks.labs", os.path.join(src_root, "databricks", "labs")),
        (
            "databricks.labs.community_connector",
            os.path.join(src_root, "databricks", "labs", "community_connector"),
        ),
    ]
    for name, path in chain:
        if not os.path.isdir(path):
            continue
        try:
            module = importlib.import_module(name)
        except ModuleNotFoundError:
            # Parent not yet importable even after grafting a grandparent; the
            # next iterations extend paths that make it resolvable.
            continue
        existing = list(getattr(module, "__path__", []))
        if path not in existing:
            try:
                module.__path__.append(path)  # type: ignore[attr-defined]
            except AttributeError:
                module.__path__ = [*existing, path]


def _load_connector(informix_py_path: str):
    """Import the connector's ``informix`` module and return it.

    Resolves the ``databricks`` namespace collision by grafting the connector
    source root onto the installed ``databricks`` package path, then imports the
    module by its real dotted name so its ``interface``/``sqli``/``cdc_protocol``
    dependencies load normally. On a real cluster PySpark is present; when it is
    not (e.g. a bare REPL), missing PySpark data-source symbols — used only by
    the Spark integration, never by the slot protocol — are stubbed on demand so
    the module can import.
    """

    import re
    import sys

    # informix_py_path = <src_root>/databricks/labs/community_connector/sources/informix/<file>.py
    informix_dir = os.path.dirname(os.path.abspath(informix_py_path))
    src_root = os.path.abspath(os.path.join(informix_dir, *([os.pardir] * 5)))
    _extend_databricks_namespace(src_root)

    dotted = "databricks.labs.community_connector.sources.informix.informix"
    missing_name = re.compile(r"cannot import name '(?P<attr>\w+)' from '(?P<mod>[\w.]+)'")
    for _ in range(64):
        try:
            return importlib.import_module(dotted)
        except ModuleNotFoundError as error:
            if not error.name or not error.name.startswith("pyspark"):
                raise
            _ensure_stub_module(error.name)
        except ImportError as error:
            match = missing_name.search(str(error))
            if match is None or not match.group("mod").startswith("pyspark"):
                raise
            stub = _ensure_stub_module(match.group("mod"))
            setattr(stub, match.group("attr"), type(match.group("attr"), (), {}))
    raise RuntimeError("could not satisfy connector PySpark imports after stubbing")


def run_capacity_probe(
    informix_py_path: str,
    location: str,
    *,
    slot_count: int = 16,
    workers: int = 60,
    hold_seconds: float = 3.0,
) -> dict[str, Any]:
    """Drive the real slot protocol with ``workers`` threads over ``slot_count`` slots.

    Returns a report dict. Each thread acquires a slot, holds it ``hold_seconds``
    to emulate a snapshot/CDC read, then releases it.
    """

    connector = _load_connector(informix_py_path)
    bridge_cls = connector.PurePythonInformixBridge
    capacity_error = connector.ConnectionCapacityUnavailable

    endpoint = os.path.join(location.rstrip("/"), f".informix-capacity-{secrets.token_hex(8)}")
    wait_timeout = max(5.0, hold_seconds * 4)

    results: list[dict[str, Any]] = []
    results_lock = threading.Lock()
    start_barrier = threading.Barrier(workers)

    def worker(task_id: int) -> None:
        bridge = object.__new__(bridge_cls)
        bridge._connection_slot = None
        bridge._connection_slot_token = None
        bridge._connection_slot_heartbeat_stop = None
        bridge._connection_slot_heartbeat = None
        bridge._connection_lease_lost = threading.Event()
        bridge.options = {
            "max.concurrent.connections": str(slot_count),
            "connection.wait.timeout.seconds": str(wait_timeout),
            "_informix.connection.cleanup.enabled": "false",
        }
        bridge._connection_slot_root = lambda: endpoint
        acquired = False
        timed_out = False
        error = None
        wait_elapsed = 0.0
        try:
            start_barrier.wait(timeout=60)
            started = time.monotonic()
            try:
                bridge._acquire_connection_slot()
                acquired = True
                wait_elapsed = time.monotonic() - started
                time.sleep(hold_seconds)
            except capacity_error:
                timed_out = True
                wait_elapsed = time.monotonic() - started
            except BaseException as exc:
                error = f"{type(exc).__name__}:{exc}"[:200]
                wait_elapsed = time.monotonic() - started
            finally:
                if acquired:
                    try:
                        bridge._release_connection_slot()
                    except BaseException as exc:
                        error = error or f"release:{type(exc).__name__}"
        except BaseException as exc:  # barrier/setup failure
            error = error or f"setup:{type(exc).__name__}:{exc}"[:200]
        with results_lock:
            results.append(
                {
                    "task_id": task_id,
                    "acquired": acquired,
                    "timed_out": timed_out,
                    "wait_seconds": round(wait_elapsed, 3),
                    "error": error,
                }
            )

    os.makedirs(endpoint, mode=0o700, exist_ok=True)
    threads = [threading.Thread(target=worker, args=(i,)) for i in range(workers)]
    wall_start = time.monotonic()
    try:
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=wait_timeout + hold_seconds + 60)
    finally:
        _cleanup(endpoint)
    wall_elapsed = time.monotonic() - wall_start

    waits = sorted(r["wait_seconds"] for r in results if r["acquired"])
    return {
        "location": location,
        "slot_count": slot_count,
        "workers": workers,
        "hold_seconds": hold_seconds,
        "wait_timeout_seconds": wait_timeout,
        "wall_seconds": round(wall_elapsed, 3),
        "acquired": sum(1 for r in results if r["acquired"]),
        "timed_out": sum(1 for r in results if r["timed_out"]),
        "errored": sum(1 for r in results if r["error"]),
        "acquire_wait_min": waits[0] if waits else None,
        "acquire_wait_median": waits[len(waits) // 2] if waits else None,
        "acquire_wait_max": waits[-1] if waits else None,
        "still_alive_threads": sum(1 for t in threads if t.is_alive()),
        "errors": sorted({r["error"] for r in results if r["error"]}),
    }


def _cleanup(root: str) -> None:
    for current, dirs, files in os.walk(root, topdown=False):
        for name in files:
            try:
                os.unlink(os.path.join(current, name))
            except OSError:
                pass
        for name in dirs:
            try:
                os.rmdir(os.path.join(current, name))
            except OSError:
                pass
    try:
        os.rmdir(root)
    except OSError:
        pass


__all__ = ["run_capacity_probe"]
