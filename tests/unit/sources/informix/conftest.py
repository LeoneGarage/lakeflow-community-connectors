"""Setup for the Informix connector's suites.

Real PySpark is imported first, when it is installed, so that the contract suite's
module-level guard (``if "pyspark.sql.types" not in sys.modules``) sees it and leaves
its lightweight ``pyspark`` stub out. That stub replaces ``pyspark.sql`` with a
non-package module; installed during a whole-tree ``pytest tests/`` run, it would break
``import pyspark.sql.datasource`` for every connector test collected afterward. On CI
PySpark is always present; in a minimal environment the import fails here and the stub
installs as before.

The live tools directory goes on ``sys.path``, so the suites import the tools by name,
as the tools import each other. And the connector's split modules each get one
namespace for the suites (see informix_modules).
"""

import contextlib
import pathlib
import sys

with contextlib.suppress(ImportError):
    import pyspark.sql.datasource  # noqa: F401
    import pyspark.sql.streaming.datasource  # noqa: F401
    import pyspark.sql.types  # noqa: F401

_HERE = pathlib.Path(__file__).resolve().parent
for _path in (_HERE, _HERE / "tools"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

import informix_modules  # noqa: E402

informix_modules.install()
