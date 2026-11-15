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


@pytest.fixture
def fuse_tier(monkeypatch):
    """The FUSE floor of the FILE probe: a session without `read_files`/`list_files`, as a local[2] session is.

    The FILE legs' tests build their inputs on the local disk and expect the FUSE listing (and `na_by_design` for the
    FILE-only modes). A Databricks cluster probes as a FILE tier, which lists a local path through `read_files` and
    fails: the tests pin the tier they are about.
    """
    for module in (
        "databricks.labs.gbx.ds.file_gbx",
        "databricks.labs.gbx.vectorx.file_read",
    ):
        monkeypatch.setattr(f"{module}.file_access_tier", lambda spark=None: "fuse")
