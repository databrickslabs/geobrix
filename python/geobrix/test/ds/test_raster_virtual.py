"""virtualTiles emit mode: bytes-free (path, whole-file window) tiles that
round-trip through open_tile to the correct pixels.
"""

import numpy as np
import rasterio
from rasterio.transform import from_origin

from databricks.labs.gbx.ds.raster import RasterGbxDataSource
from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA, VirtualTile

# ---------------------------------------------------------------------------
# Inline layout helpers (sourced from test/pyrx/_layouts.py — inlined here
# because test/ds/ cannot do a relative import from the sibling test/pyrx/
# package and the installed namespace has no test.pyrx path).
# ---------------------------------------------------------------------------
_PX = 0.001


def _PIXELS(width, height):
    return np.arange(width * height, dtype="float32").reshape(height, width)


def _base_profile(width, height, epsg):
    return dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=from_origin(10.0, 50.0, _PX, _PX),
        nodata=-9999.0,
    )


def write_striped_gtiff(dst_path, width=512, height=512, epsg=4326):
    prof = _base_profile(width, height, epsg)
    prof.update(tiled=False)
    with rasterio.open(dst_path, "w", **prof) as ds:
        ds.write(_PIXELS(width, height), 1)
    return dst_path


def write_tiled_gtiff(dst_path, width=512, height=512, blocksize=256, epsg=4326):
    prof = _base_profile(width, height, epsg)
    prof.update(tiled=True, blockxsize=blocksize, blockysize=blocksize)
    with rasterio.open(dst_path, "w", **prof) as ds:
        ds.write(_PIXELS(width, height), 1)
    return dst_path


def write_cog(dst_path, width=512, height=512, blocksize=256, epsg=4326):
    prof = _base_profile(width, height, epsg)
    prof.update(driver="COG", blocksize=blocksize, overview_resampling="nearest")
    prof.pop("tiled", None)
    with rasterio.open(dst_path, "w", **prof) as ds:
        ds.write(_PIXELS(width, height), 1)
    return dst_path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write3(tmp_path):
    return {
        "cog": write_cog(str(tmp_path / "a.cog.tif"), 512, 512, 256),
        "tiled": write_tiled_gtiff(str(tmp_path / "a.tiled.tif"), 512, 512, 256),
        "striped": write_striped_gtiff(str(tmp_path / "a.striped.tif"), 512, 512),
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_virtual_emits_bytes_free_rows(spark, tmp_path):
    spark.dataSource.register(RasterGbxDataSource)
    _write3(tmp_path)
    df = (
        spark.read.format("raster_gbx")
        .option("virtualTiles", "true")
        .load(str(tmp_path))
    )
    assert df.schema["tile"].dataType == V2_TILE_SCHEMA
    rows = df.collect()
    assert len(rows) == 3  # one per file
    for r in rows:
        t = r["tile"]
        assert t["raster"] is None  # bytes-free
        assert t["path"] is not None
        assert t["window"]["col_off"] == 0 and t["window"]["row_off"] == 0
        assert t["window"]["width"] == 512 and t["window"]["height"] == 512
        assert t["metadata"] and "width" in t["metadata"]


def test_virtual_row_round_trips_through_open_tile(spark, tmp_path):
    spark.dataSource.register(RasterGbxDataSource)
    _write3(tmp_path)
    tiled_path = str(tmp_path / "a.tiled.tif")
    rows = (
        spark.read.format("raster_gbx").option("virtualTiles", "true").load(tiled_path)
    ).collect()
    assert len(rows) == 1
    t = rows[0]["tile"]
    tile = VirtualTile.from_row(t)  # reader row -> VirtualTile
    with ot.open_tile(tile) as ds:
        got = ds.read(1)
    with rasterio.open(tiled_path) as ds:
        exp = ds.read(1)
    assert np.array_equal(got, exp)  # whole-file window == full read
