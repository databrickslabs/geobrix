"""clipPolygons emission: materialized pre-clip (Choice 2) + virtual instructions."""

import numpy as np
import rasterio
from rasterio.io import MemoryFile
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


def _tri_wkt():
    # a triangle inside cols 50..250, rows 50..250 (non-rectangular -> real mask).
    # WKT string: survives Spark's string-typed .option() (raw WKB bytes would be
    # stringified to a Python repr and fail to parse). Planning converts to WKB.
    from shapely.geometry import Polygon

    pts = [
        (10.0 + 50 * 0.001, 50.0 - 250 * 0.001),
        (10.0 + 250 * 0.001, 50.0 - 250 * 0.001),
        (10.0 + 150 * 0.001, 50.0 - 50 * 0.001),
    ]
    return Polygon(pts).wkt


def test_materialized_clip_is_preclipped_with_nodata(spark, tmp_path):
    spark.dataSource.register(RasterGbxDataSource)
    p = _write(tmp_path)
    df = (
        spark.read.format("raster_gbx")
        .option("clipPolygons", _tri_wkt())
        .option("clipCrs", "EPSG:4326")
        .load(p)
    )
    rows = df.collect()
    assert len(rows) == 1
    t = rows[0]["tile"]
    assert t["raster"] is not None  # materialized
    assert t["clip_polygon"] is not None  # reference to applied clip
    with MemoryFile(bytes(t["raster"])) as mf, mf.open() as ds:
        arr = ds.read(1)
        nod = ds.nodata
    # triangle mask -> some interior real pixels AND some nodata corners present
    assert np.any(arr == nod) and np.any(arr != nod)


def test_virtual_clip_carries_instructions_and_round_trips(spark, tmp_path):
    spark.dataSource.register(RasterGbxDataSource)
    p = _write(tmp_path)
    wkt = _tri_wkt()
    df = (
        spark.read.format("raster_gbx")
        .option("virtualTiles", "true")
        .option("clipPolygons", wkt)
        .option("clipCrs", "EPSG:4326")
        .load(p)
    )
    rows = df.collect()
    assert len(rows) == 1
    t = rows[0]["tile"]
    assert t["raster"] is None  # virtual: instructions, not applied
    assert t["clip_polygon"] is not None and t["clip_crs"] == "EPSG:4326"
    # round-trip: open_tile applies the clip -> same as a direct mask of the window
    tile = VirtualTile.from_row(t)
    with ot.open_tile(tile) as ds:
        got = ds.read(1)
        gnod = ds.nodata
    assert np.any(got == gnod) and np.any(got != gnod)  # triangle masked
