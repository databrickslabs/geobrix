"""Fixtures for the extended driver tests: GDAL's own bindings (the env wheel's ``osgeo``) and a local Spark session.

The suite needs the env wheel's ``osgeo`` on rasterio's libgdal, as the engine tests do. It skips where that is not
the case, and runs in the container and in the light CI shards, which carry the payload.
"""

import logging
import os
import sys

import pytest

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(autouse=True)
def _isolate_gdal_env():
    """Snapshot and restore GDAL/PROJ env vars around every test."""
    keys = ("GDAL_DATA", "PROJ_DATA", "PROJ_LIB")
    saved = {k: os.environ.get(k) for k in keys}
    yield
    for k, v in saved.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


@pytest.fixture(scope="session")
def gb():
    """GDAL's Python bindings (``gdal``, ``ogr``, ``osr``) after the driver policy is on; skips without the payload."""
    from databricks.labs.gbx import _gdal_policy, _osgeo
    from databricks.labs.gbx.engine import run

    why = run.unavailable_reason()
    if why:
        pytest.skip(why)
    b = _osgeo.bindings("test_drivers_extended")
    _gdal_policy.activate()
    return b


@pytest.fixture(scope="module")
def spark(gb):
    """A plain local session with every GeoBrix source and function registered (the light tier: no JAR)."""
    logging.getLogger("py4j").setLevel(logging.ERROR)
    from pyspark.sql import SparkSession

    from databricks.labs import gbx

    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-drivers-extended")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    gbx.register(session)
    yield session
