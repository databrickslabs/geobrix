"""Shared fixtures for bench tests that require a local Spark session."""

import logging
import os
import sys

import pytest

# Ensure PySpark workers use the same Python interpreter as the driver so that
# local Spark tests don't fail with PYTHON_VERSION_MISMATCH.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="module")
def spark():
    logging.getLogger("py4j").setLevel(logging.ERROR)
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("bench-tests")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    yield session


@pytest.fixture(scope="module")
def spark_session_local(spark):
    """The local classic session, under the name the levels leg's tests use."""
    return spark
