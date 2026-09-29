"""Tests for rst_land_cover — v2-tile -> class-mask-tile columnar function."""

import numpy as np
import pytest
from pyspark.sql import functions as F
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx import functions as prx


def _two_region_rgb_bytes(h=32, w=32):
    """3-band uint8 GTiff: left half green (-> vegetation), right half
    bright/gray (-> bare). Mirrors test_land_cover.py's ``_rgb()`` fixture."""
    a = np.zeros((3, h, w), np.uint8)
    a[0, :, : w // 2] = 30  # left half: green
    a[1, :, : w // 2] = 200
    a[2, :, : w // 2] = 40
    a[0, :, w // 2 :] = 210  # right half: bright/gray
    a[1, :, w // 2 :] = 205
    a[2, :, w // 2 :] = 200
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=3,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(0, h, 1, 1),
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(a)
        return mf.read()


@pytest.fixture
def two_region_rgb_tile_df(spark):
    raster = _two_region_rgb_bytes()
    df = spark.createDataFrame([(raster,)], ["raster"])
    return df.select(prx.rst_fromcontent("raster", F.lit("GTiff")).alias("tile"))


def test_rst_land_cover_emits_classmask_tile(spark, two_region_rgb_tile_df):
    out = two_region_rgb_tile_df.select(
        prx.rst_land_cover(F.col("tile"), method="spectral", smooth=0).alias("lc")
    )
    row = out.select(
        prx.rst_numbands("lc").alias("n"), prx.rst_type("lc").alias("ty")
    ).first()
    assert row["n"] == 1
    assert row["ty"][0] == "Int32"

    tile_row = out.select("lc").first()["lc"]
    arr = prx.tile_to_numpy(tile_row)
    distinct = set(np.unique(arr)) - {-1}
    assert len(distinct) >= 2


def test_rst_land_cover_then_polygonize(spark, two_region_rgb_tile_df):
    df = two_region_rgb_tile_df.select(
        prx.rst_land_cover(F.col("tile"), method="spectral", smooth=0).alias("lc")
    )
    df.createOrReplaceTempView("lc_tiles")
    prx.register(spark)
    rows = spark.sql(
        "SELECT t.geom_wkb AS g, t.value AS v FROM lc_tiles, "
        "LATERAL gbx_rst_polygonize(lc, 1, 4) t"
    ).collect()
    vals = {r["v"] for r in rows}
    assert len(rows) >= 2
    assert len(vals) >= 2
