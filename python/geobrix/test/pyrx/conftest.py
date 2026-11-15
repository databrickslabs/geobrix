"""Shared fixtures for pyrx tests.

The Spark fixture deliberately creates a plain session with NO spark.jars —
pyrx is the lightweight, JAR-free API. If this fixture ever needs the JAR,
something is wrong with the layering.
"""

import logging
import os
import sys
import tempfile

import numpy as np
import pytest
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

# Ensure PySpark workers use the same interpreter as the driver.  Must be set
# before any SparkContext is created (local-mode workers are separate Python
# processes spawned per-task; without this they default to system Python and
# fail with PYTHON_VERSION_MISMATCH when the test venv uses a different minor).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(autouse=True)
def _clear_file_support_cache():
    """Clear _FILE_SUPPORT_CACHE before and after every test.

    file_supported() caches its result in _FILE_SUPPORT_CACHE keyed by
    id(spark).  Tests that monkeypatch spark.sql to force True/False must
    not pollute later tests that share the same Spark session (same id(spark)).
    Without this fixture, test_file_supported_returns_true_when_probe_succeeds
    caches True and causes ~55 order-dependent failures in the full suite.
    """
    from databricks.labs.gbx.pyrx._file_ref import _FILE_SUPPORT_CACHE

    _FILE_SUPPORT_CACHE.clear()
    yield
    _FILE_SUPPORT_CACHE.clear()


@pytest.fixture(autouse=True)
def _isolate_gdal_env():
    """Snapshot and restore GDAL/PROJ env vars around every test.

    test_env.py intentionally mutates GDAL_DATA; without isolation a bogus
    value could bleed into later tests when the whole suite runs in one
    pytest process.
    """
    keys = ("GDAL_DATA", "PROJ_DATA", "PROJ_LIB")
    saved = {k: os.environ.get(k) for k in keys}
    yield
    for k, v in saved.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


def make_geotiff_bytes(width=4, height=3, count=1, epsg=4326, nodata=-9999.0):
    """Return in-memory single/multi-band GTiff bytes with a known georeference.

    Origin (ulx, uly) = (10.0, 50.0); pixel size 0.5 x 0.5 (north-up).
    So extent = (10.0, 50.0 - 0.5*height) .. (10.0 + 0.5*width, 50.0).
    """
    transform = from_origin(10.0, 50.0, 0.5, 0.5)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=count,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=transform,
        nodata=nodata,
    )
    data = np.arange(width * height, dtype="float32").reshape(height, width)
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            for b in range(1, count + 1):
                ds.write(data + (b - 1) * 100, b)
        return mf.read()


def cog_errors(data, strict=True):
    """GDAL's COG validator (``osgeo_utils``, through ``databricks.labs.gbx._osgeo``) on COG bytes
    or a path: its errors, and its warnings too when ``strict``. Skips with the helper's reason
    where GDAL's Python bindings do not import (the macOS host, the PyPI-stack CI shard).

    Bytes are written to a file first, so the validator only opens a path and shares nothing
    with rasterio: it may run on another GDAL than rasterio's (``same_libgdal=False``; the dev
    container's system osgeo), where a rasterio ``MemoryFile`` would not be visible.

    GDAL's validator is looser than rio-cogeo's on one check: an untiled file 513 to 1024 pixels
    wide is no error here (GDAL only errors on stripes wider than 1024). The rows feed COG-driver
    output, which is tiled; a row on other large output must not rely on that check.
    """
    from databricks.labs.gbx import _osgeo

    try:
        validator = _osgeo.utils(
            "samples.validate_cloud_optimized_geotiff",
            "the COG check",
            same_libgdal=False,
        )
    except _osgeo.OsgeoUnavailable as exc:
        pytest.skip(str(exc))
    with tempfile.TemporaryDirectory() as d:
        path = data
        if isinstance(data, (bytes, bytearray)):
            path = os.path.join(d, "cog.tif")
            with open(path, "wb") as fh:
                fh.write(data)
        warnings, errors, _details = validator.validate(str(path), full_check=True)
    return errors + (warnings if strict else [])


@pytest.fixture(scope="session")
def gtiff_bytes():
    return make_geotiff_bytes()


@pytest.fixture(scope="module")
def spark():
    import sys

    logging.getLogger("py4j").setLevel(logging.ERROR)
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("pyrx-tests")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )
    yield session
