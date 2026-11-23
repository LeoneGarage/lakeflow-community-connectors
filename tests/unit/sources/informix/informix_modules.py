"""The connector's modules that were split into several, each as one namespace for the suites.

``informix.py``, ``lakebase_state.py`` and ``sqli.py`` were each split into modules of at
most 1500 lines. The suites still import the original module and patch names on it
(``mock.patch.object(informix_module, "time", ...)``), as when every name lived in it.
install() makes each original module stand for all of its parts: a name it doesn't hold
is looked up in them, and setting or deleting a name does so in every part that holds
it, so the code reading it sees the change, and deleting restores each part's own value.
"""

import contextlib
import importlib
import logging
import pathlib
import types
from unittest import mock

PACKAGE = "databricks.labs.community_connector.sources.informix"
# What the suites' log checks listen to: the package's logger, every module's parent.
LOGGER = PACKAGE

PARTS = {
    "informix": (
        "informix_options",
        "informix_registration",
        "informix_keys",
        "informix_filters",
        "informix_transactions",
        "informix_shards",
        "informix_host_sharing",
        "informix_unload",
        "informix_offsets",
        "informix_bridge_session",
        "informix_bridge_reads",
        "informix_bridge",
        "informix_reader",
        "informix_partitions",
        "informix_deletes",
        "informix_staging",
        "informix_scan",
        "informix_index_scan",
        "informix_unl",
        "informix_stream",
        "informix_schema",
        "informix",
    ),
    "lakebase_state": ("lakebase_workspace", "lakebase_state_store", "lakebase_state"),
    "sqli": ("sqli_codec", "sqli"),
}

_SAVED: dict[tuple[str, str], list[dict[types.ModuleType, object]]] = {}
_MODULES: dict[str, tuple[types.ModuleType, ...]] = {}


def parts(name: str) -> tuple[types.ModuleType, ...]:
    if name not in _MODULES:
        _MODULES[name] = tuple(importlib.import_module(f"{PACKAGE}.{part}") for part in PARTS[name])
    return _MODULES[name]


def part_files(name: str) -> list[pathlib.Path]:
    return [pathlib.Path(module.__file__) for module in parts(name)]


class _Namespace(types.ModuleType):
    def _parts(self):
        return parts(self.__name__.rsplit(".", 1)[1])

    def __getattr__(self, name):
        if not name.startswith("__"):
            for part in self._parts():
                if name in vars(part):
                    return vars(part)[name]
        raise AttributeError(f"module {self.__name__!r} has no attribute {name!r}")

    def __setattr__(self, name, value):
        holders = [] if name.startswith("__") else [p for p in self._parts() if name in vars(p)]
        if not holders:
            types.ModuleType.__setattr__(self, name, value)
            return
        _SAVED.setdefault((self.__name__, name), []).append({p: vars(p)[name] for p in holders})
        for part in holders:
            types.ModuleType.__setattr__(part, name, value)

    def __delattr__(self, name):
        stack = _SAVED.get((self.__name__, name))
        if not stack:
            types.ModuleType.__delattr__(self, name)
            return
        for part, value in stack.pop().items():
            types.ModuleType.__setattr__(part, name, value)


def install() -> None:
    for name in PARTS:
        module = importlib.import_module(f"{PACKAGE}.{name}")
        parts(name)
        module.__class__ = _Namespace


@contextlib.contextmanager
def patched_loggers(method: str, name: str = "informix"):
    # One mock in place of ``method`` on the logger of every part of ``name``: the code
    # logs through logging.getLogger(__name__), the logger of its own part.
    shared = mock.Mock()
    with contextlib.ExitStack() as stack:
        for part in parts(name):
            stack.enter_context(
                mock.patch.object(logging.getLogger(part.__name__), method, shared)
            )
        yield shared
