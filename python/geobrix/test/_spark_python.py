"""Pin the Python that local Spark workers start to the interpreter running the tests.

WHY: a local Spark session starts its Python workers with ``PYSPARK_PYTHON``, or ``python3`` from ``PATH`` when that is
unset. On a host where ``python3`` is not the interpreter the tests run under (the repo venv), the workers and the
driver differ and the first Python UDF fails with ``PYTHON_VERSION_MISMATCH``. Setting both variables to
``sys.executable`` makes every local session use one interpreter. A value the caller already set stays: Docker, CI and
Databricks keep what they export.
"""

import os
import sys

_VARS = ("PYSPARK_PYTHON", "PYSPARK_DRIVER_PYTHON")


def pin(env=None, executable=None) -> None:
    """Default both Spark interpreter variables to the running interpreter; keep any value already set."""
    env = os.environ if env is None else env
    executable = sys.executable if executable is None else executable
    if not executable:
        return
    for name in _VARS:
        env.setdefault(name, executable)
