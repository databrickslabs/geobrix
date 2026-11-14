"""Local Spark workers run the interpreter the tests run under (test/_spark_python.py, applied by test/conftest.py)."""

import os
import sys
from test import _provided_spark
from test import _spark_python as sp

import pytest


def test_pin_defaults_both_variables_to_the_given_interpreter():
    env = {}
    sp.pin(env, "/venv/bin/python")
    assert env == {
        "PYSPARK_PYTHON": "/venv/bin/python",
        "PYSPARK_DRIVER_PYTHON": "/venv/bin/python",
    }


def test_pin_keeps_a_value_the_caller_set():
    env = {"PYSPARK_PYTHON": "/usr/bin/python3"}
    sp.pin(env, "/venv/bin/python")
    assert env["PYSPARK_PYTHON"] == "/usr/bin/python3"
    assert env["PYSPARK_DRIVER_PYTHON"] == "/venv/bin/python"


def test_the_test_session_has_both_variables_set():
    assert os.environ.get("PYSPARK_PYTHON")
    assert os.environ.get("PYSPARK_DRIVER_PYTHON")


@pytest.mark.skipif(
    _provided_spark.wanted(), reason="the platform's session runs its own workers"
)
def test_a_local_session_worker_runs_the_driver_interpreter():
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("spark-python-pin")
        .getOrCreate()
    )

    @F.udf("string")
    def worker_python():
        import os as _os
        import sys as _sys

        return _os.path.realpath(_sys.executable)

    got = spark.range(1).select(worker_python().alias("p")).first()["p"]
    assert got == os.path.realpath(sys.executable)
