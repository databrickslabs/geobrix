"""Raster reader parity / correctness (Docker / integration).

``test_raster_gbx_pixels_match_source`` (always runs in Docker) — the light
``raster_gbx`` reader's decoded tile pixels equal rasterio reading the source
directly, and every row carries ``cellid == -1`` + the 11-key metadata set.
This is a deterministic ground-truth correctness gate for the light reader.

Runs in Docker only (sample data mounted at ``/Volumes``). Marked ``integration``; execute with::

    bash scripts/commands/gbx-test-python.sh \
        --path python/geobrix/test/pyrx/ds/test_reader_parity.py \
        --with-integration --log reader-parity.log
"""

import logging
import os
from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.io import MemoryFile

pytestmark = pytest.mark.integration

# Real raster mounted under /Volumes in the dev container. Override with
# GBX_PARITY_SAMPLE (e.g. a cluster Volume path).
SAMPLE = os.environ.get(
    "GBX_PARITY_SAMPLE",
    "/Volumes/main/default/test-data/geobrix-examples/london/sentinel2/london_sentinel2_red.tif",
)

# The 11 metadata keys of a materialized tile.
EXPECTED_METADATA_KEYS = {
    "path",
    "sourcePath",
    "driver",
    "format",
    "last_command",
    "last_error",
    "all_parents",
    "size",
    "compression",
    "isZipped",
    "isSubset",
}

# Tolerances mirror bench/compare.py.
REL_TOL = 1e-3
ABS_TOL = 1e-3

# JAR discovery mirrors python/geobrix/test/rasterx/test_pixel_ops.py.
_HERE = Path(__file__).resolve()
_LIBDIR = (_HERE.parents[3] / "lib").resolve()
_JAR_CANDIDATES = sorted(_LIBDIR.glob("geobrix-*-jar-with-dependencies.jar"))


@pytest.fixture(scope="module")
def spark_with_jar():
    """A SparkSession with the geobrix JAR on the classpath.

    The JAR makes the heavy ``gdal`` DataSource resolvable (auto-discovered via
    META-INF/services); the light ``raster_gbx`` DataSource is pure Python and
    is registered explicitly below.
    """
    from databricks.labs.gbx import manager as gbx_manager

    if not _JAR_CANDIDATES:
        pytest.skip(f"no geobrix JAR in {_LIBDIR} (build/stage it first)")
    if not os.path.exists(SAMPLE):
        pytest.skip(f"sample raster not mounted at {SAMPLE}")

    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)
    jar = str(_JAR_CANDIDATES[-1])
    session = (
        SparkSession.builder.master("local[2]")
        .appName("pyrx-ds-parity")
        .config(
            "spark.driver.extraJavaOptions",
            "-Djava.library.path=/usr/local/lib:/usr/lib:/usr/java/packages/lib:"
            "/usr/lib64:/lib64:/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", jar)
        .getOrCreate()
    )

    gbx_manager.register_sources(session)
    yield session


def _decode(raster_bytes):
    with MemoryFile(bytes(raster_bytes)) as mf, mf.open() as ds:
        return ds.read()  # (bands, h, w)


def test_raster_gbx_pixels_match_source(spark_with_jar):
    """Light reader's decoded tile equals rasterio reading the source directly."""
    light = (
        spark_with_jar.read.format("raster_gbx")
        .option("virtualTiles", "false")
        .load(SAMPLE)
        .orderBy("source")
        .collect()
    )
    assert len(light) >= 1

    for row in light:
        assert row["tile"]["cellid"] == -1
        assert set(row["tile"]["metadata"].keys()) == EXPECTED_METADATA_KEYS
        assert row["source"] == SAMPLE

    with rasterio.open(SAMPLE) as src:
        truth = src.read()  # (bands, H, W)

    # The sample is < sizeInMB (16) so it is a single tile covering the whole
    # raster; compare that tile's pixels to the full source array.
    assert len(light) == 1, f"expected 1 tile for {SAMPLE}, got {len(light)}"
    arr = _decode(light[0]["tile"]["raster"])
    assert (
        arr.shape == truth.shape
    ), f"shape differs: light={arr.shape} source={truth.shape}"
    np.testing.assert_allclose(arr, truth, rtol=REL_TOL, atol=ABS_TOL)
