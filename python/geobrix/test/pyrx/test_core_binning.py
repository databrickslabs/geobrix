"""TDD tests for pyrx core binning (DSM engine).

Spark-free: exercises bin_points directly with numpy arrays, validates GTiff
bytes with rasterio. All assertions are hand-checkable.
"""

import numpy as np
import pytest
from rasterio.io import MemoryFile
from rasterio.transform import from_bounds
from rasterio.crs import CRS


def _read_band(gtiff_bytes):
    with MemoryFile(gtiff_bytes) as mf, mf.open() as ds:
        return ds.read(1), ds.nodata, ds.transform, ds.crs


# ---------------------------------------------------------------------------
# Phase 1 – RED tests (max, count, empty NoData)
# ---------------------------------------------------------------------------


def test_binpoints_max_hand_computed():
    from databricks.labs.gbx.pyrx.core.binning import bin_points

    # 2x2 grid over [0,2]x[0,2]; two points in the top-left cell, one in
    # the bottom-right. Raster row 0 is the TOP (high y).
    x = [0.5, 0.5, 1.5]
    y = [1.5, 1.5, 0.5]  # top-left = y>1, bottom-right = y<1
    z = [10.0, 25.0, 7.0]
    b = bin_points(x, y, z, 0, 0, 2, 2, 2, 2, 2227, "max")
    arr, nodata, _, _ = _read_band(b)
    assert arr.shape == (2, 2)
    assert arr[0, 0] == pytest.approx(25.0)  # top-left cell = max(10,25)
    assert arr[1, 1] == pytest.approx(7.0)   # bottom-right
    assert arr[0, 1] == pytest.approx(nodata)  # empty cell -> NoData
    assert arr[1, 0] == pytest.approx(nodata)


def test_binpoints_count_and_empty():
    from databricks.labs.gbx.pyrx.core.binning import bin_points

    x, y, z = [0.5, 0.6], [1.5, 1.4], [1.0, 1.0]
    b = bin_points(x, y, z, 0, 0, 2, 2, 2, 2, 2227, "count")
    arr, nodata, _, _ = _read_band(b)
    assert arr[0, 0] == pytest.approx(2.0)
    # cells without points get NoData
    assert arr[0, 1] == pytest.approx(nodata)
    assert arr[1, 0] == pytest.approx(nodata)
    assert arr[1, 1] == pytest.approx(nodata)


# ---------------------------------------------------------------------------
# Phase 2 – mean / min / median / percentile
# ---------------------------------------------------------------------------


def test_binpoints_min():
    from databricks.labs.gbx.pyrx.core.binning import bin_points

    x = [0.5, 0.5]
    y = [1.5, 1.5]
    z = [3.0, 9.0]
    b = bin_points(x, y, z, 0, 0, 2, 2, 2, 2, 4326, "min")
    arr, nodata, _, _ = _read_band(b)
    assert arr[0, 0] == pytest.approx(3.0)


def test_binpoints_mean():
    from databricks.labs.gbx.pyrx.core.binning import bin_points

    x = [0.25, 0.75, 1.5]
    y = [1.5, 1.5, 1.5]
    z = [4.0, 8.0, 100.0]  # top-row: cell(0,0)=mean(4,8)=6; cell(0,1)=100
    b = bin_points(x, y, z, 0, 0, 2, 2, 2, 2, 4326, "mean")
    arr, nodata, _, _ = _read_band(b)
    assert arr[0, 0] == pytest.approx(6.0)
    assert arr[0, 1] == pytest.approx(100.0)


def test_binpoints_median():
    from databricks.labs.gbx.pyrx.core.binning import bin_points

    # Three points in top-left: median of [1,3,5] = 3
    x = [0.5, 0.5, 0.5]
    y = [1.5, 1.5, 1.5]
    z = [1.0, 5.0, 3.0]
    b = bin_points(x, y, z, 0, 0, 2, 2, 2, 2, 4326, "median")
    arr, nodata, _, _ = _read_band(b)
    assert arr[0, 0] == pytest.approx(3.0)


def test_binpoints_percentile():
    from databricks.labs.gbx.pyrx.core.binning import bin_points

    # Four values [0,1,2,3] in one cell; 75th percentile = 2.25
    x = [0.5] * 4
    y = [1.5] * 4
    z = [0.0, 1.0, 2.0, 3.0]
    b = bin_points(x, y, z, 0, 0, 2, 2, 2, 2, 4326, "percentile:75")
    arr, nodata, _, _ = _read_band(b)
    assert arr[0, 0] == pytest.approx(2.25, abs=1e-4)


# ---------------------------------------------------------------------------
# Phase 3 – georeference-alignment (spec §5.2)
# ---------------------------------------------------------------------------


def test_binpoints_georef_alignment():
    """Output transform/crs/shape must match a reference DEM's exact georeference."""
    from databricks.labs.gbx.pyrx.core.binning import bin_points

    ref_xmin, ref_ymin, ref_xmax, ref_ymax = 530000.0, 179000.0, 531000.0, 180000.0
    ref_w, ref_h, ref_srid = 10, 10, 27700

    ref_transform = from_bounds(ref_xmin, ref_ymin, ref_xmax, ref_ymax, ref_w, ref_h)
    ref_crs = CRS.from_epsg(ref_srid)

    # Scatter a few points inside the extent
    rng = np.random.default_rng(42)
    px = rng.uniform(ref_xmin, ref_xmax, 20)
    py = rng.uniform(ref_ymin, ref_ymax, 20)
    pz = rng.uniform(10.0, 100.0, 20)

    b = bin_points(px, py, pz, ref_xmin, ref_ymin, ref_xmax, ref_ymax,
                   ref_w, ref_h, ref_srid, "max")
    arr, nodata, out_transform, out_crs = _read_band(b)

    assert arr.shape == (ref_h, ref_w)
    assert out_crs == ref_crs
    # All 6 affine coefficients must match exactly (from_bounds is deterministic)
    for i in range(6):
        assert out_transform[i] == pytest.approx(ref_transform[i]), (
            f"transform[{i}] mismatch: {out_transform[i]} != {ref_transform[i]}"
        )


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_binpoints_nodata_sentinel():
    """NoData value must be exactly -9999.0."""
    from databricks.labs.gbx.pyrx.core.binning import bin_points

    b = bin_points([0.5], [1.5], [5.0], 0, 0, 2, 2, 2, 2, 4326, "max")
    _, nodata, _, _ = _read_band(b)
    assert nodata == -9999.0


def test_binpoints_dtype_float32():
    """Band dtype must be float32."""
    from databricks.labs.gbx.pyrx.core.binning import bin_points

    b = bin_points([0.5], [1.5], [5.0], 0, 0, 2, 2, 2, 2, 4326, "max")
    with MemoryFile(b) as mf, mf.open() as ds:
        assert ds.dtypes[0] == "float32"


def test_binpoints_out_of_bounds_ignored():
    """Points outside [xmin,xmax]x[ymin,ymax] must be silently dropped."""
    from databricks.labs.gbx.pyrx.core.binning import bin_points

    # Only the in-bounds point should register
    x = [0.5, 99.0]
    y = [1.5, 99.0]
    z = [42.0, 1000.0]
    b = bin_points(x, y, z, 0, 0, 2, 2, 2, 2, 4326, "max")
    arr, nodata, _, _ = _read_band(b)
    assert arr[0, 0] == pytest.approx(42.0)
    # cell that could be poisoned by out-of-bounds point is NoData
    assert arr[1, 1] == pytest.approx(nodata)


def test_binpoints_unknown_statistic_raises():
    from databricks.labs.gbx.pyrx.core.binning import bin_points

    with pytest.raises(ValueError, match="unknown statistic"):
        bin_points([0.5], [1.5], [1.0], 0, 0, 2, 2, 2, 2, 4326, "bogus")


def test_binpoints_exact_boundary_dropped():
    """Half-open interval: points exactly on xmax or ymax are DROPPED (not
    clamped into the last cell). A point just inside lands in the last col/row.
    """
    from databricks.labs.gbx.pyrx.core.binning import bin_points

    # Grid: [0,2]x[0,2], 2x2.
    # x==xmax (x=2) -> col==2 -> dropped (last col = col 1).
    # y==ymax (y=2) -> row==h -> dropped (last row = row 1 in 0-indexed, but
    #   row=floor((2-2)/2*2)=0... wait: row=floor((ymax-y)/(ymax-ymin)*h).
    #   y=ymax=2 -> row=floor(0/2*2)=floor(0)=0, which IS in [0,h) = [0,2).
    #   So y==ymax lands in row 0 (top row) — that's correct behaviour (it IS
    #   the upper boundary of that pixel). Only x==xmax / col==w is the
    #   problematic upper edge in x direction.
    #   Similarly y==ymin -> row=floor(2/2*2)=floor(2)=2 which is >= h, dropped.
    # Test the x-boundary case:
    # - point A at x=2.0 (==xmax), y=0.5, z=999  -> col=2 -> DROPPED
    # - point B at x=1.99, y=0.5, z=42            -> col=1 (last col) -> lands
    # - point C at y=0.0 (==ymin), x=0.5, z=888   -> row=2 -> DROPPED
    # - point D at y=0.01, x=0.5, z=55            -> row=1 (bottom row) -> lands
    x = [2.0,  1.99, 0.5,  0.5]
    y = [0.5,  0.5,  0.0,  0.01]
    z = [999.0, 42.0, 888.0, 55.0]
    b = bin_points(x, y, z, 0, 0, 2, 2, 2, 2, 4326, "max")
    arr, nodata, _, _ = _read_band(b)
    # A dropped: arr[1,1] should be 42 (from B), not 999.
    assert arr[1, 1] == pytest.approx(42.0), (
        f"expected 42.0 (B lands), got {arr[1,1]} — "
        "x==xmax point was not dropped"
    )
    # C dropped: arr[1,0] should be 55 (from D), not 888.
    assert arr[1, 0] == pytest.approx(55.0), (
        f"expected 55.0 (D lands), got {arr[1,0]} — "
        "y==ymin point was not dropped"
    )
