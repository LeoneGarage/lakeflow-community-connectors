"""Build step that keeps the connector's test modules out of its wheel.

``pyproject.toml`` maps the package to the connector directory, so setuptools packages
every ``.py`` there, the test modules beside the runtime code included, and has no
setting that leaves a module out. ``[tool.setuptools.cmdclass]`` points ``build_py``
here. It drops the modules named ``*_test*``, the pattern
``tools/scripts/merge_python_source.py`` excludes from the generated source, so the
wheel and the merged file carry the same modules. This file sits in ``tests/``, which
is not a package, so it never reaches the wheel itself.
"""

import fnmatch

from setuptools.command.build_py import build_py

_TEST_MODULES = "*_test*"


class BuildPy(build_py):
    def find_package_modules(self, package, package_dir):
        return [
            entry
            for entry in super().find_package_modules(package, package_dir)
            if not fnmatch.fnmatch(entry[1], _TEST_MODULES)
        ]
