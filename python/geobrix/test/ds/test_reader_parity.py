"""Raster reader parity / correctness (Docker / integration).

Two checks:

1. ``test_raster_gbx_pixels_match_source`` (always runs in Docker) — the light
   ``raster_gbx`` reader's decoded tile pixels equal rasterio reading the source
   directly, and every row carries ``cellid == -1`` + the 11-key metadata set.
   This is a deterministic ground-truth correctness gate for the light reader.

2. ``test_raster_gbx_matches_gdal`` — the swap-out compare against the heavy
   Scala ``gdal`` reader: equal tile count + ``cellid`` + metadata KEY-SET +
   decoded-pixel equality within tolerance (NOT byte-for-byte; both tiers
   independently re-encode to GTiff). **Skips** when the heavy ``gdal`` reader
   yields 0 rows in the current environment — it does not produce tiles in the
   local dev container (a heavy-tier local GDAL-init quirk, unrelated to the
   light reader), so the live light-vs-heavy comparison is exercised on a
   cluster (see the cluster bench), where the heavy tier runs.

Runs in Docker only (needs the geobrix JAR for ``gdal`` + sample data mounted at
``/Volumes``). Marked ``integration``; execute with::

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

# The 11 keys the heavy reader emits (WindowedExtract.scala:108-119).
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

# Tolerances mirror bench/compare.py (light vs heavy never byte-equal).
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
    from databricks.labs.gbx.ds.register import register

    register(session)
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


def test_raster_gbx_matches_gdal(spark_with_jar):
    """Swap-out parity vs the heavy ``gdal`` reader (skips if heavy is unavailable here)."""
    # The heavy gdal reader is not exercisable in the local dev container: it
    # yields 0 tiles, and when an earlier (JAR-free) pyrx Spark session already
    # owns the JVM context, ``format("gdal")`` raises outright. Either way, run
    # the live light-vs-heavy comparison on a cluster, not here.
    try:
        heavy = (
            spark_with_jar.read.format("gdal").load(SAMPLE).orderBy("source").collect()
        )
    except Exception as exc:  # noqa: BLE001 - environment-dependent heavy reader
        pytest.skip(
            f"heavy 'gdal' reader unavailable in this environment: {str(exc)[:120]}"
        )
    if len(heavy) == 0:
        pytest.skip(
            "heavy 'gdal' reader produced 0 rows in this environment "
            "(yields no tiles in local Docker); run the live light-vs-heavy "
            "comparison on a cluster."
        )

    light = (
        spark_with_jar.read.format("raster_gbx")
        .option("virtualTiles", "false")
        .load(SAMPLE)
        .orderBy("source")
        .collect()
    )
    assert len(light) == len(
        heavy
    ), f"tile/row count differs: light={len(light)} heavy={len(heavy)}"

    for h, l in zip(heavy, light):
        assert l["tile"]["cellid"] == -1
        assert h["tile"]["cellid"] == l["tile"]["cellid"]
        assert set(l["tile"]["metadata"].keys()) == set(h["tile"]["metadata"].keys())
        ha, la = _decode(h["tile"]["raster"]), _decode(l["tile"]["raster"])
        assert (
            ha.shape == la.shape
        ), f"tile pixel dims differ: light={la.shape} heavy={ha.shape}"
        np.testing.assert_allclose(la, ha, rtol=REL_TOL, atol=ABS_TOL)
