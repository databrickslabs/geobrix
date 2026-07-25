"""Pure-function tests for pyrx raster->BNG-grid aggregation.

Spark-free: open GTiff bytes with rasterio and call gridagg.raster_to_grid
directly with grid="bng", mirroring the heavyweight RST_BNG_RasterToGrid
semantics (reproject to EPSG:27700, per-pixel BNG cell math via pygx._bng,
out-of-GB pixels dropped via is_valid, String cell ids at the output boundary).
"""

import re

import numpy as np
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pygx import _bng
from databricks.labs.gbx.pyrx.core import gridagg

# BNG rendered cell id: two grid letters then optional easting/northing digits
# (e.g. "TQ300800"), optionally a trailing quadrant suffix.
_BNG_ID_RE = re.compile(r"^[A-Z]{2}\d*(SW|NW|NE|SE)?$")


def _open(raster_bytes):
    return MemoryFile(raster_bytes).open()


def _raster(data, *, epsg, origin, px, nodata=-9999.0):
    """Single-band GTiff from a 2-D numpy array at the given CRS/georeference.

    ``origin`` is the (ulx, uly) top-left corner; ``px`` the pixel size (both
    axes). North-up transform via ``from_origin``.
    """
    h, w = data.shape
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=from_origin(origin[0], origin[1], px, px),
        nodata=nodata,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data.astype("float32"), 1)
        return mf.read()


# --- (a) EPSG:27700 London raster, all valid -> String ids + correct mean ----
def test_bng_avg_london_string_ids_and_mean():
    # 2x2 raster in central London (EPSG:27700), 200 m pixels wholly inside a
    # single 1 km (res 3) cell -> one cell, mean of [2,4,6,8] == 5.0.
    data = np.array([[2.0, 4.0], [6.0, 8.0]], dtype="float32")
    raster = _raster(data, epsg=27700, origin=(530000.0, 180400.0), px=200.0)
    with _open(raster) as ds:
        result = gridagg.raster_to_grid(ds, 3, "bng", "avg")
    assert len(result) == 1
    band = result[0]
    assert len(band) == 1
    cell = band[0]
    assert isinstance(cell["cellID"], str)
    assert _BNG_ID_RE.match(cell["cellID"]), cell["cellID"]
    assert cell["measure"] == 5.0


def test_bng_count_measure_is_int():
    data = np.array([[2.0, 4.0], [6.0, 8.0]], dtype="float32")
    raster = _raster(data, epsg=27700, origin=(530000.0, 180400.0), px=200.0)
    with _open(raster) as ds:
        result = gridagg.raster_to_grid(ds, 3, "bng", "count")
    band = result[0]
    assert sum(c["measure"] for c in band) == 4
    assert all(isinstance(c["measure"], int) for c in band)
    assert all(isinstance(c["cellID"], str) for c in band)


def test_bng_resolution_string_key():
    # "1km" is the string alias for res 3; must normalize via get_resolution.
    data = np.array([[2.0, 4.0], [6.0, 8.0]], dtype="float32")
    raster = _raster(data, epsg=27700, origin=(530000.0, 180400.0), px=200.0)
    with _open(raster) as ds:
        by_int = gridagg.raster_to_grid(ds, 3, "bng", "avg")
    with _open(raster) as ds:
        by_str = gridagg.raster_to_grid(ds, "1km", "bng", "avg")
    assert by_int == by_str


# --- (b) all-nodata band -> [] (no zero-valid-pixel cell, sec 2.6) -----------
def test_bng_all_nodata_yields_empty():
    data = np.full((3, 3), -9999.0, dtype="float32")
    raster = _raster(data, epsg=27700, origin=(530000.0, 180000.0), px=100.0)
    with _open(raster) as ds:
        result = gridagg.raster_to_grid(ds, 3, "bng", "avg")
    assert result == [[]]


# --- (c) 4326 input auto-warped to 27700 -> valid BNG cells ------------------
def test_bng_4326_input_autowarped():
    # Small WGS84 raster over central London; must be reprojected to 27700
    # internally and yield valid BNG cells.
    data = np.arange(9, dtype="float32").reshape(3, 3)
    raster = _raster(data, epsg=4326, origin=(-0.13, 51.52), px=0.01)
    with _open(raster) as ds:
        result = gridagg.raster_to_grid(ds, 2, "bng", "count")
    band = result[0]
    assert len(band) >= 1
    for c in band:
        assert isinstance(c["cellID"], str)
        assert _BNG_ID_RE.match(c["cellID"]), c["cellID"]
        assert _bng.is_valid(_bng.parse(c["cellID"]))


# --- (d) out-of-GB pixels dropped via is_valid -------------------------------
def test_bng_out_of_gb_pixels_dropped():
    # 27700 raster straddling the eastern GB boundary (easting 700000). Columns
    # east of the boundary produce out-of-GB cells that is_valid must drop.
    data = np.arange(6, dtype="float32").reshape(1, 6)
    origin = (690000.0, 200000.0)
    px = 5000.0
    raster = _raster(data, epsg=27700, origin=origin, px=px)

    # Expected: bin every valid pixel exactly as the engine should, keeping only
    # cells that pass is_valid, and compare the emitted cell set.
    gt = from_origin(origin[0], origin[1], px, px).to_gdal()
    expected = set()
    dropped_any = False
    for col in range(6):
        e = gt[0] + (col + 0.5) * gt[1] + 0.5 * gt[2]
        n = gt[3] + (col + 0.5) * gt[4] + 0.5 * gt[5]
        cid = _bng.point_to_cell_id(e, n, 2)
        if _bng.is_valid(cid):
            expected.add(_bng.format(cid))
        else:
            dropped_any = True

    assert dropped_any, "test fixture must contain at least one out-of-GB pixel"

    with _open(raster) as ds:
        result = gridagg.raster_to_grid(ds, 2, "bng", "count")
    emitted = {c["cellID"] for c in result[0]}
    assert emitted == expected
    for c in result[0]:
        assert _bng.is_valid(_bng.parse(c["cellID"]))
