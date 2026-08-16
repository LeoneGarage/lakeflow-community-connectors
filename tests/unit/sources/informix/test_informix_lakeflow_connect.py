"""CI entry point for the Informix connector test suite.

Informix is a database CDC connector, not an HTTP source, so it has no
in-process HTTP simulator and does not subclass ``LakeflowConnectTests``.
Its coverage lives in ``unittest.TestCase`` suites next to the connector:

    - ``sources/informix/lakeflow_test.py``      (Lakeflow contract, snapshots)
    - ``sources/informix/protocol_test.py``      (SQLI wire protocol, CDC)
    - ``sources/informix/lakebase_state.py``     (Lakebase connection-slot state)

CI only collects ``tests/unit/sources/<source>/``, so this module discovers
every ``unittest.TestCase`` in those source-local modules and re-exports it
here. pytest collects ``unittest.TestCase`` subclasses found in a module's
namespace regardless of the ``python_classes`` name filter, so importing the
classes is enough to run them under the CI-visible path.
"""

import contextlib
import unittest

# Import real PySpark first, when it is installed, so that ``lakeflow_test``'s
# module-level guard (``if "pyspark.sql.types" not in sys.modules``) sees it and
# skips installing its lightweight ``pyspark`` stub. That stub replaces
# ``pyspark.sql`` with a non-package module; if it were installed during a
# whole-tree ``pytest tests/`` run it would break ``import pyspark.sql.datasource``
# for every connector test collected afterward. On CI PySpark is always present
# (it is in requirements/sources.txt and root.txt); in a minimal environment
# without it, the import fails here and the stub installs as before.
with contextlib.suppress(ImportError):
    import pyspark.sql.datasource  # noqa: F401
    import pyspark.sql.streaming.datasource  # noqa: F401
    import pyspark.sql.types  # noqa: F401

from databricks.labs.community_connector.sources.informix import (  # noqa: E402
    lakebase_state_test,
    lakeflow_test,
    protocol_test,
)

_SOURCE_LOCAL_TEST_MODULES = (
    lakeflow_test,
    protocol_test,
    lakebase_state_test,
)

for _module in _SOURCE_LOCAL_TEST_MODULES:
    for _name in dir(_module):
        _obj = getattr(_module, _name)
        if (
            isinstance(_obj, type)
            and issubclass(_obj, unittest.TestCase)
            and _obj is not unittest.TestCase
        ):
            globals()[_name] = _obj

del _module, _name, _obj, _SOURCE_LOCAL_TEST_MODULES
