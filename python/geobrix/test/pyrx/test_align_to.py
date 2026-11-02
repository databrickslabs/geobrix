"""Tests for rst_align_to (light tier).

Verifies that a tile warped to a reference grid:
  1. Exactly matches the reference's CRS, extent, and pixel dimensions.
  2. Works for CRS reprojection (4326 -> 27700 and vice-versa).
  3. Propagates NoData through the warp.
  4. Produces a tile that passes the combine family alignment check when
     combined with the reference tile (round-trip identity).
  5. Column wrapper + SQL registration works.
"""

import numpy as np
import pytest
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import agg


def _make_tile(
    width,
    height,
    crs,
    ulx,
    uly,
    pixel_size,
    values=None,
    nodata=-9999.0,
    dtype="float64",
):
    """Create in-memory single-band GTiff bytes."""
    if values is None:
        values = np.arange(width * height, dtype=dtype).reshape(1, height, width)
    else:
        values = np.asarray(values, dtype=dtype).reshape(1, height, width)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype=dtype,
        crs=crs,
        transform=from_origin(ulx, uly, pixel_size, pixel_size),
        nodata=nodata,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(values)
        return mf.read()


def _grid_matches_ref(tile_bytes, ref_bytes, tol=1e-6):
    """True when tile's grid (dims + transform + CRS) matches the reference."""
    with _serde.open_tile(tile_bytes) as t, _serde.open_tile(ref_bytes) as r:
        if t.width != r.width or t.height != r.height:
            return False
        # Compare all 6 affine transform coefficients within relative tolerance
        tc = (
            t.transform.c,
            t.transform.a,
            t.transform.b,
            t.transform.f,
            t.transform.d,
            t.transform.e,
        )
        rc = (
            r.transform.c,
            r.transform.a,
            r.transform.b,
            r.transform.f,
            r.transform.d,
            r.transform.e,
        )
        for ti, ri in zip(tc, rc):
            if abs(ti - ri) > tol * (1.0 + abs(ri)):
                return False
        # CRS equality
        if t.crs != r.crs:
            try:
                from pyproj import CRS as _ProjCRS

                if not _ProjCRS.from_user_input(t.crs).equals(
                    _ProjCRS.from_user_input(r.crs)
                ):
                    return False
            except Exception:
                return str(t.crs) == str(r.crs)
        return True


# ---------------------------------------------------------------------------
# Core warp behaviour
# ---------------------------------------------------------------------------


def test_align_to_same_crs_matches_reference_grid():
    """Warping to a same-CRS reference must produce exactly the reference grid."""
    src = _make_tile(10, 10, "EPSG:4326", -180.0, 90.0, 0.5)
    ref = _make_tile(4, 4, "EPSG:4326", 0.0, 2.0, 1.0)
    out = agg.align_to_tiles(src, ref)
    assert _grid_matches_ref(out, ref)


def test_align_to_reprojection_4326_to_27700():
    """Warping a 4326 tile to a 27700 reference must match the reference grid."""
    # Reference tile in EPSG:27700 (British National Grid), London area
    ref = _make_tile(8, 8, "EPSG:27700", 520000.0, 185000.0, 1000.0)
    # Source tile covering a similar extent in WGS84
    src = _make_tile(10, 10, "EPSG:4326", -0.5, 51.7, 0.1)
    out = agg.align_to_tiles(src, ref)
    assert _grid_matches_ref(out, ref)


def test_align_to_output_dims_match_reference():
    """The output tile has exactly the same width/height as the reference."""
    ref = _make_tile(6, 5, "EPSG:4326", 0.0, 2.0, 1.0)
    src = _make_tile(10, 10, "EPSG:4326", -5.0, 10.0, 0.5)
    out = agg.align_to_tiles(src, ref)
    with _serde.open_tile(out) as ds:
        assert ds.width == 6
        assert ds.height == 5


def test_align_to_crs_matches_reference():
    """Output CRS equals reference CRS (not source CRS)."""
    src = _make_tile(8, 8, "EPSG:4326", -0.5, 51.7, 0.1)
    ref = _make_tile(4, 4, "EPSG:27700", 520000.0, 185000.0, 1000.0)
    out = agg.align_to_tiles(src, ref)
    with _serde.open_tile(out) as ds, _serde.open_tile(ref) as r:
        try:
            from pyproj import CRS as _ProjCRS

            assert _ProjCRS.from_user_input(ds.crs).equals(
                _ProjCRS.from_user_input(r.crs)
            )
        except Exception:
            assert str(ds.crs) == str(r.crs)


def test_align_to_transform_matches_reference():
    """Output geotransform coefficients match reference within tolerance."""
    src = _make_tile(12, 12, "EPSG:4326", -1.0, 52.0, 0.05)
    ref = _make_tile(4, 4, "EPSG:4326", 0.0, 2.0, 1.0)
    out = agg.align_to_tiles(src, ref)
    with _serde.open_tile(out) as ds, _serde.open_tile(ref) as r:
        tol = 1e-6
        for t_c, r_c in zip(
            [
                ds.transform.c,
                ds.transform.a,
                ds.transform.b,
                ds.transform.f,
                ds.transform.d,
                ds.transform.e,
            ],
            [
                r.transform.c,
                r.transform.a,
                r.transform.b,
                r.transform.f,
                r.transform.d,
                r.transform.e,
            ],
        ):
            assert abs(t_c - r_c) <= tol * (1.0 + abs(r_c))


def test_align_to_result_passes_combine_alignment_check():
    """Two tiles aligned to the same reference pass the combine alignment check."""
    ref = _make_tile(4, 4, "EPSG:27700", 520000.0, 185000.0, 1000.0, values=[1.0] * 16)
    src_a = _make_tile(8, 8, "EPSG:4326", -0.3, 51.6, 0.05, values=[2.0] * 64)
    src_b = _make_tile(6, 6, "EPSG:4326", -0.4, 51.7, 0.07, values=[3.0] * 36)
    aligned_a = agg.align_to_tiles(src_a, ref)
    aligned_b = agg.align_to_tiles(src_b, ref)
    # Must not raise (alignment check satisfied)
    out = agg.combine_sum_tiles([aligned_a, aligned_b])
    assert out is not None


# ---------------------------------------------------------------------------
# NoData propagation
# ---------------------------------------------------------------------------


def test_align_to_preserves_nodata_value():
    """The output tile carries the source's NoData value."""
    src = _make_tile(4, 4, "EPSG:4326", 0.0, 2.0, 0.5, nodata=-9999.0)
    ref = _make_tile(4, 4, "EPSG:4326", 0.0, 2.0, 1.0)
    out = agg.align_to_tiles(src, ref)
    with _serde.open_tile(out) as ds:
        assert ds.nodata == pytest.approx(-9999.0)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_align_to_identity_same_grid():
    """If source already matches the reference grid, output matches reference grid."""
    same = _make_tile(4, 4, "EPSG:4326", 0.0, 2.0, 1.0, values=[5.0] * 16)
    ref = _make_tile(4, 4, "EPSG:4326", 0.0, 2.0, 1.0)
    out = agg.align_to_tiles(same, ref)
    assert _grid_matches_ref(out, ref)


def test_align_to_none_input_returns_none():
    assert agg.align_to_tiles(None, b"anything") is None
    assert agg.align_to_tiles(b"anything", None) is None


# ---------------------------------------------------------------------------
# Column wrapper + SQL registration
# ---------------------------------------------------------------------------
