"""register_proj_grids reaches the heavy tier's JVM registry: through a stand-in JVM (the call it makes), and through a
real PySpark JVM with the staged JAR (the call works: ProjGridRegistry.set takes a Scala Seq, not the java.util.List
py4j makes of a Python list)."""

from pathlib import Path

import pytest

from databricks.labs.gbx import crs_grids
from databricks.labs.gbx.core import proj_grids

JARS = sorted(
    (Path(__file__).resolve().parents[2] / "lib").glob(
        "geobrix-*-jar-with-dependencies.jar"
    )
)


@pytest.fixture(autouse=True)
def _restore_registry():
    """The registry is process-wide: leave it as this file found it (a leaked dir changes later level choices)."""
    saved = proj_grids.get_registered_dirs()
    yield
    proj_grids.set_registered_dirs(saved, replace=True)


def test_heavy_apply_sets_jvm_registry(monkeypatch):
    calls = {}

    class _Reg:
        def set(self, jlist, replace):
            calls["dirs"] = list(jlist)
            calls["replace"] = replace

    class _JVM:
        class com:
            class databricks:
                class labs:
                    class gbx:
                        class operations:
                            ProjGridRegistry = _Reg()

        class org:  # Spark's list-to-Seq conversion
            class apache:
                class spark:
                    class api:
                        class python:
                            class PythonUtils:
                                toSeq = staticmethod(lambda xs: list(xs))

    class _Spark:
        _jvm = _JVM()

    crs_grids.register_proj_grids(spark=_Spark(), dirs=["/Volumes/a"], replace=True)
    assert calls["dirs"] == ["/Volumes/a"]
    assert calls["replace"] is True


# Run in its own Python process: a JVM started in the test process stays up after spark.stop() and fixes the driver
# options of every later session there (the P9 file's library path, for one).
_REAL_JVM = """
import sys
from pyspark.sql import SparkSession
from databricks.labs.gbx import crs_grids
spark = SparkSession.builder.master("local[1]").config("spark.jars", sys.argv[1]).getOrCreate()
crs_grids.register_proj_grids(spark, sys.argv[2], replace=True)
print("REGISTRY " + spark._jvm.com.databricks.labs.gbx.operations.ProjGridRegistry.get().mkString("|"))
spark.stop()
"""


@pytest.mark.integration
def test_register_proj_grids_reaches_the_real_jvm_registry(tmp_path):
    import os
    import subprocess
    import sys

    if not JARS:
        pytest.skip(
            "PRECONDITION MISSING: the staged JAR (gbx-test-parity.sh stages it); this row not run"
        )
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(p for p in sys.path if p)}
    res = subprocess.run(
        [sys.executable, "-c", _REAL_JVM, str(JARS[-1]), str(tmp_path)],
        capture_output=True,
        text=True,
        timeout=600,
        env=env,
    )
    assert f"REGISTRY {tmp_path}" in res.stdout.splitlines(), (
        res.stdout + res.stderr[-3000:]
    )
