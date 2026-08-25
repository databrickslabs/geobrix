"""tileSize grid emission: materialized per-cell bytes + virtual round-trip."""

import numpy as np
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.ds.raster import RasterGbxDataSource
from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.virtual_tile import VirtualTile


def _write(tmp_path, width=512, height=512):
    p = str(tmp_path / "r.tif")
    prof = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(10.0, 50.0, 0.001, 0.001),
        nodata=-9999.0,
    )
    with rasterio.open(p, "w", **prof) as ds:
        ds.write(np.arange(width * height, dtype="float32").reshape(height, width), 1)
    return p


def test_materialized_tilesize_grid(spark, tmp_path):
    spark.dataSource.register(RasterGbxDataSource)
    p = _write(tmp_path, 512, 512)
    rows = (
        spark.read.format("raster_gbx")
        .option("tileSize", "256,256")
        .option("virtualTiles", "false")
        .load(p)
    ).collect()
    assert len(rows) == 4
    for r in rows:
        assert r["tile"]["raster"] is not None
        assert r["tile"]["clip_polygon"] is None
        assert r["tile"]["window"]["width"] == 256


def test_materialized_tilesize_pixels_match_source(spark, tmp_path):
    spark.dataSource.register(RasterGbxDataSource)
    p = _write(tmp_path, 512, 512)
    rows = (
        spark.read.format("raster_gbx")
        .option("tileSize", "256,256")
        .option("virtualTiles", "false")
        .load(p)
    ).collect()
    with rasterio.open(p) as ds:
        full = ds.read(1)
    from rasterio.io import MemoryFile

    for r in rows:
        w = r["tile"]["window"]
        with MemoryFile(bytes(r["tile"]["raster"])) as mf, mf.open() as t:
            got = t.read(1)
        exp = full[
            w["row_off"] : w["row_off"] + w["height"],
            w["col_off"] : w["col_off"] + w["width"],
        ]
        assert np.array_equal(got, exp)


def test_virtual_tilesize_round_trips(spark, tmp_path):
    spark.dataSource.register(RasterGbxDataSource)
    p = _write(tmp_path, 512, 512)
    rows = (
        spark.read.format("raster_gbx")
        .option("virtualTiles", "true")
        .option("tileSize", "256,256")
        .load(p)
    ).collect()
    assert len(rows) == 4
    with rasterio.open(p) as ds:
        full = ds.read(1)
    for r in rows:
        assert r["tile"]["raster"] is None
        tile = VirtualTile.from_row(r["tile"])
        with ot.open_tile(tile) as t:
            got = t.read(1)
        w = r["tile"]["window"]
        exp = full[
            w["row_off"] : w["row_off"] + w["height"],
            w["col_off"] : w["col_off"] + w["width"],
        ]
        assert np.array_equal(got, exp)


def test_overlap_grid_count(spark, tmp_path):
    spark.dataSource.register(RasterGbxDataSource)
    p = _write(tmp_path, 512, 512)
    rows = (
        spark.read.format("raster_gbx")
        .option("tileSize", "256,256")
        .option("overlapPercent", "25")
        .load(p)
    ).collect()
    assert len(rows) == 9
