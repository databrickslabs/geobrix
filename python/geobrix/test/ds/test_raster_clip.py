"""clipPolygons emission: materialized pre-clip (Choice 2) + virtual instructions.

Also covers box-as-clipPolygon cases migrated from the removed bbox option:
  - windows to AOI
  - north-overhang clips to dataset extent
  - non-overlapping file is skipped
  - reader emits plain GTiff (not COG)
"""

import numpy as np
import rasterio
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.ds.gtiff import GTiffGbxDataSource
from databricks.labs.gbx.ds.raster import RasterGbxDataSource
from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.cog import GBX_FORMAT, sniff_header
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


# ---------------------------------------------------------------------------
# Small raster helpers (4x3, 0.5 deg/px) for box-window tests
# extent: origin (10.0, 50.0), 0.5 px → x[10,12], y[48.5,50]
# ---------------------------------------------------------------------------


def _write_small(tmp_path, width=4, height=3, epsg=4326):
    p = str(tmp_path / "s.tif")
    data = np.arange(width * height, dtype="float32").reshape(height, width)
    with rasterio.open(
        p,
        "w",
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=from_origin(10.0, 50.0, 0.5, 0.5),
        nodata=-9999.0,
    ) as ds:
        ds.write(data, 1)
    return p


def _tile_bounds(row):
    with MemoryFile(bytes(row["tile"]["raster"])) as mf, mf.open() as out:
        b = out.bounds
        return (b.left, b.bottom, b.right, b.top), (out.width, out.height)


def _box_wkt(minx, miny, maxx, maxy):
    """Return a closed WKT POLYGON box.  Spark .option() is string-typed so we
    pass WKT; raw WKB bytes would be stringified to a Python repr and fail to
    parse inside the reader."""
    return (
        f"POLYGON(({minx} {miny}, {maxx} {miny}, {maxx} {maxy}, {minx} {maxy}, "
        f"{minx} {miny}))"
    )


# ---------------------------------------------------------------------------
# Box-as-clipPolygon window selection (migrated from removed bbox option)
# A rectangle is its own envelope → window equals the old bbox window.
# A box masks nothing → materialized pre-clip tile has no extra nodata.
# ---------------------------------------------------------------------------


def test_box_clip_windows_to_aoi(spark, tmp_path):
    """clipPolygons box selects a 2×2 window within the 4×3 small raster."""
    p = _write_small(tmp_path)
    spark.dataSource.register(RasterGbxDataSource)
    df = (
        spark.read.format("raster_gbx")
        .option("clipPolygons", _box_wkt(10.5, 49.0, 11.5, 50.0))
        .option("clipCrs", "EPSG:4326")
        .load(p)
    )
    rows = df.collect()
    assert len(rows) == 1
    bounds, (w, h) = _tile_bounds(rows[0])
    assert bounds == (10.5, 49.0, 11.5, 50.0)
    assert (w, h) == (2, 2)  # 1.0 deg / 0.5 px


def test_box_clip_north_overhang_clips_to_dataset(spark, tmp_path):
    """A box that extends north past the dataset is clipped to the dataset top."""
    p = _write_small(tmp_path)
    spark.dataSource.register(RasterGbxDataSource)
    df = (
        spark.read.format("raster_gbx")
        .option("clipPolygons", _box_wkt(10.5, 49.0, 11.5, 51.0))
        .option("clipCrs", "EPSG:4326")
        .load(p)
    )
    rows = df.collect()
    bounds, _ = _tile_bounds(rows[0])
    assert bounds[3] == 50.0  # top clipped to dataset top, not 51.0


def test_non_overlapping_box_skips_file(spark, tmp_path):
    """A box that does not intersect the raster produces zero rows."""
    p = _write_small(tmp_path)
    spark.dataSource.register(RasterGbxDataSource)
    df = (
        spark.read.format("raster_gbx")
        .option("clipPolygons", _box_wkt(20, 20, 21, 21))
        .option("clipCrs", "EPSG:4326")
        .load(p)
    )
    assert df.collect() == []


def test_box_clip_emits_plain_gtiff(spark, tmp_path):
    """Reader always emits plain GTiff tiles; COG creation is a writer concern."""
    p = str(tmp_path / "large.tif")
    width, height = 512, 512
    data = np.arange(width * height, dtype="float32").reshape(height, width)
    with rasterio.open(
        p,
        "w",
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(0.0, 1.0, 1.0 / width, 1.0 / height),
        nodata=-9999.0,
    ) as ds:
        ds.write(data, 1)
    spark.dataSource.register(RasterGbxDataSource)
    df = (
        spark.read.format("raster_gbx")
        .option("clipPolygons", _box_wkt(0.0, 0.0, 0.5, 0.5))
        .option("clipCrs", "EPSG:4326")
        .load(p)
    )
    rows = df.collect()
    assert len(rows) == 1, "expected exactly one tile for the AOI"
    tile = rows[0]["tile"]
    raster_bytes = bytes(tile["raster"])
    metadata = tile["metadata"]
    assert metadata.get(GBX_FORMAT) == "gtiff", (
        f"metadata gbx_format was '{metadata.get(GBX_FORMAT)}', expected 'gtiff'; "
        "clip read must emit plain GTiff (use cog_gbx writer for COG creation)"
    )
    info = sniff_header(raster_bytes)
    assert (
        not info.is_cog
    ), f"bytes sniff says is_cog={info.is_cog}; expected plain GTiff, not COG"


def test_gtiff_gbx_parity_clip(spark, tmp_path):
    """gtiff_gbx and raster_gbx produce geometrically identical tiles for the
    same GTiff + same clipPolygons box selection.

    This is a regression tripwire: a format-registration test only proves the
    format string resolves to a class; this test proves both sources emit tiles
    with the same spatial geometry (bounds + pixel dimensions).
    """
    p = _write_small(tmp_path)
    spark.dataSource.register(RasterGbxDataSource)
    spark.dataSource.register(GTiffGbxDataSource)
    opt = ("clipPolygons", _box_wkt(10.5, 49.0, 11.5, 50.0))
    r1 = (
        spark.read.format("raster_gbx")
        .option(*opt)
        .option("clipCrs", "EPSG:4326")
        .load(p)
        .collect()[0]
    )
    r2 = (
        spark.read.format("gtiff_gbx")
        .option(*opt)
        .option("clipCrs", "EPSG:4326")
        .load(p)
        .collect()[0]
    )
    assert _tile_bounds(r1) == _tile_bounds(r2)


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
