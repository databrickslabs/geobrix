"""cog_gbx default file names for the retile family, end to end.

``gbx_rst_retile`` windows and ``gbx_rst_separatebands`` bands share their parent's
cellid and source. Written with ``cog_gbx`` and no ``nameCol`` they must each land in
their own file, not overwrite one another.
"""

import glob
import os

import numpy as np
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.ds.cog import CogGbxDataSource
from databricks.labs.gbx.ds.raster import RasterGbxDataSource
from databricks.labs.gbx.pyrx import functions as prx


def _write_source(path, *, width, height, count):
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=count,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(0, 60, 0.01, 0.01),
    )
    with rasterio.open(path, "w", **profile) as ds:
        for band in range(1, count + 1):
            ds.write(np.full((height, width), band * 40, dtype="uint8"), band)


def _write_parts(spark, src, out, fn):
    """Read ``src`` (source, tile), cut it with the LATERAL function ``fn``, and write
    the (source, tile) parts with cog_gbx and no nameCol."""
    spark.dataSource.register(RasterGbxDataSource)
    spark.dataSource.register(CogGbxDataSource)
    prx.register(spark)
    spark.read.format("raster_gbx").load(str(src)).createOrReplaceTempView("_cog_src")
    parts = spark.sql(
        f"SELECT source, struct(t.*) AS tile FROM _cog_src, LATERAL {fn} t"
    )
    parts.write.format("cog_gbx").mode("overwrite").option("cogBlockSize", "128").save(
        str(out)
    )
    return sorted(glob.glob(os.path.join(str(out), "*.tif")))


def test_retile_windows_write_to_distinct_files(spark, tmp_path):
    src = tmp_path / "in" / "scene.tif"
    src.parent.mkdir()
    _write_source(str(src), width=512, height=512, count=1)

    files = _write_parts(spark, src, tmp_path / "out", "gbx_rst_retile(tile, 256, 256)")

    assert len(files) == 4, f"expected one file per window, got {files}"
    for f in files:
        with rasterio.open(f) as ds:
            assert (ds.width, ds.height) == (256, 256)


def test_separatebands_write_to_distinct_files(spark, tmp_path):
    src = tmp_path / "in" / "scene.tif"
    src.parent.mkdir()
    _write_source(str(src), width=256, height=256, count=3)

    files = _write_parts(spark, src, tmp_path / "out", "gbx_rst_separatebands(tile)")

    assert len(files) == 3, f"expected one file per band, got {files}"
    values = []
    for f in files:
        with rasterio.open(f) as ds:
            assert ds.count == 1
            values.append(int(ds.read(1)[0, 0]))
    assert sorted(values) == [40, 80, 120]
