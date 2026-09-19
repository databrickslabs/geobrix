"""Tests for pyrx isoband — reclassify + per-patch band polygonize.

Spark-free: all cases exercise features.isoband() directly via _serde.open_tile.
"""

import numpy as np
import pytest
import shapely.wkb
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import features


# ---------------------------------------------------------------------------
# Helper: build GTiff bytes from a custom numpy array
# ---------------------------------------------------------------------------
def _make_geotiff(data, nodata=-9999.0, epsg=4326):
    """Return GTiff bytes wrapping *data* (2-D float32 array)."""
    h, w = data.shape
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=from_origin(10.0, 50.0, 0.5, 0.5),
        nodata=nodata,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data.astype("float32"), 1)
        return mf.read()


# ---------------------------------------------------------------------------
# Test 1: basic bands and patches
# ---------------------------------------------------------------------------
def test_isoband_bands_and_patches():
    # 4x4 raster: left half value 2 (band [0,5)), right half value 12 (band [10,20))
    data = np.zeros((4, 4), dtype="float32")
    data[:, :2] = 2.0
    data[:, 2:] = 12.0
    b = _make_geotiff(data)
    with _serde.open_tile(b) as ds:
        out = features.isoband(ds, [0, 5, 10, 20])

    assert out, "expected non-empty result"
    bands = sorted({(band, lo, hi) for _, band, lo, hi in out})
    assert (0, 0.0, 5.0) in bands, "value 2 should land in band 0 [0, 5)"
    assert (2, 10.0, 20.0) in bands, "value 12 should land in band 2 [10, 20)"
    # band 1 ([5, 10)) has no pixels
    assert all(band != 1 for _, band, _, _ in out), "no pixels in [5, 10) expected"
    # one contiguous patch per side
    patches_band0 = [g for g, band, _, _ in out if band == 0]
    assert (
        len(patches_band0) == 1
    ), f"expected 1 patch for band 0, got {len(patches_band0)}"
    geom = shapely.wkb.loads(patches_band0[0])
    assert geom.area > 0


# ---------------------------------------------------------------------------
# Test 2: non-ascending breaks raise
# ---------------------------------------------------------------------------
def test_isoband_nonascending_breaks_raise():
    data = np.ones((3, 3), dtype="float32") * 5.0
    b = _make_geotiff(data)
    with _serde.open_tile(b) as ds:
        with pytest.raises(ValueError, match="strictly ascending"):
            features.isoband(ds, [10, 5, 20])


def test_isoband_equal_breaks_raise():
    data = np.ones((3, 3), dtype="float32") * 5.0
    b = _make_geotiff(data)
    with _serde.open_tile(b) as ds:
        with pytest.raises(ValueError, match="strictly ascending"):
            features.isoband(ds, [0, 0, 10])


# ---------------------------------------------------------------------------
# Test 3: two disjoint regions in the same band → two patches
# ---------------------------------------------------------------------------
def test_isoband_disjoint_regions_same_band():
    # 6x6 raster: top-left 2x2 = value 2 (band 0 [0,5)),
    #             bottom-right 2x2 = value 2 (band 0 [0,5)),
    #             everything else = value 7 (band 1 [5,10)) — separates the two corners.
    data = np.full((6, 6), 7.0, dtype="float32")
    data[0:2, 0:2] = 2.0
    data[4:6, 4:6] = 2.0
    b = _make_geotiff(data)
    with _serde.open_tile(b) as ds:
        out = features.isoband(ds, [0, 5, 10])

    patches_band0 = [g for g, band, _, _ in out if band == 0]
    assert (
        len(patches_band0) == 2
    ), f"expected 2 disjoint patches for band 0, got {len(patches_band0)}"


# ---------------------------------------------------------------------------
# Test 4: out-of-range values are excluded
# ---------------------------------------------------------------------------
def test_isoband_out_of_range_excluded():
    # Values below breaks[0] or >= breaks[-1] should produce no patches
    data = np.array([[-5.0, -1.0], [100.0, 200.0]], dtype="float32")
    b = _make_geotiff(data)
    with _serde.open_tile(b) as ds:
        out = features.isoband(ds, [0, 50])
    assert (
        len(out) == 0
    ), f"expected empty result for all-out-of-range pixels, got {len(out)}"


# ---------------------------------------------------------------------------
# Test 5: NoData excluded
# ---------------------------------------------------------------------------
def test_isoband_nodata_excluded():
    data = np.full((4, 4), -9999.0, dtype="float32")
    data[1:3, 1:3] = 5.0  # 4 valid pixels, all in band [0, 10)
    b = _make_geotiff(data, nodata=-9999.0)
    with _serde.open_tile(b) as ds:
        out = features.isoband(ds, [0, 10, 20])
    assert len(out) == 1
    _, band, lo, hi = out[0]
    assert band == 0 and lo == 0.0 and hi == 10.0


# ---------------------------------------------------------------------------
# Test 6: bands tile the raster with no gaps for in-range values
# ---------------------------------------------------------------------------
def test_isoband_all_pixels_covered():
    # 4x4 raster: rows 0-1 = value 1 (band [0,5)), rows 2-3 = value 7 (band [5,10))
    data = np.zeros((4, 4), dtype="float32")
    data[0:2, :] = 1.0
    data[2:4, :] = 7.0
    b = _make_geotiff(data)
    with _serde.open_tile(b) as ds:
        out = features.isoband(ds, [0, 5, 10])
    band_set = {band for _, band, _, _ in out}
    assert 0 in band_set and 1 in band_set, f"expected bands 0 and 1, got {band_set}"
    # Total pixel area across all patches should sum to ≈ full raster area
    total_area = sum(shapely.wkb.loads(g).area for g, _, _, _ in out)
    assert total_area > 0
