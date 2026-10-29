"""TDD: coverage/assignment params in gridagg.raster_to_grid.

Four required checks (brief):
  (a) sparse+centroid == pre-edit golden (backward compat; cell set + measures)
  (b) complete is superset of sparse; covered-but-empty -> None; count -> 0.0
  (c) covering conserves mass for sum  (Sigma(cell sums) approx pixel total)
  (d) count measure is float in all modes

Additional checks:
  - covering+avg == centroid+avg when all pixels in one cell (weights all 1.0)
  - covering count is Sigma-w (fractional, may be < n_valid due to border pixels)
  - weighted variance/stddev: single coarse-cell sanity
  - BNG: sparse+centroid backward compat (string ids, same measure)
"""

import numpy as np
import pytest
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx.core.gridagg import raster_to_grid


def _open(b):
    return MemoryFile(b).open()


def _raster(data, *, epsg=4326, origin=(10.0, 50.0), px=0.5, nodata=-9999.0):
    """Single-band GTiff from a 2-D numpy array."""
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


# ---------------------------------------------------------------------------
# (a) sparse+centroid == pre-edit golden (backward compat)
# ---------------------------------------------------------------------------


def test_sparse_centroid_avg_matches_golden_h3():
    """sparse+centroid explicit path must reproduce the pre-edit default output.

    Golden: 2x2 raster [0,1,2,3] at H3 res=0 -> all 4 pixels in 1 cell,
    avg = 1.5.  This is what the pre-edit default (implicit sparse+centroid)
    returned; after the edit, coverage="sparse" must give the same.
    """
    data = np.array([[0.0, 1.0], [2.0, 3.0]], dtype="float32")
    b = _raster(data)
    with _open(b) as ds:
        result = raster_to_grid(
            ds, 0, "h3", "avg", coverage="sparse", assignment="centroid"
        )
    band = result[0]
    assert len(band) == 1
    assert band[0]["measure"] == pytest.approx(1.5)
    assert all(r["measure"] is not None for r in band)


def test_sparse_centroid_sum_matches_golden_quadbin():
    """sparse+centroid sum: 2x2 [2,4,6,8] at res=0 -> single cell, sum=20."""
    data = np.array([[2.0, 4.0], [6.0, 8.0]], dtype="float32")
    b = _raster(data)
    with _open(b) as ds:
        result = raster_to_grid(
            ds, 0, "quadbin", "sum", coverage="sparse", assignment="centroid"
        )
    band = result[0]
    assert len(band) == 1
    assert band[0]["measure"] == pytest.approx(20.0)


def test_sparse_centroid_bng_backward_compat():
    """BNG sparse+centroid: same string ids and measures as pre-edit default.

    London 2x2 raster (EPSG:27700) at 1km (res=3): all pixels in 1 cell,
    avg = 5.0.
    """
    data = np.array([[2.0, 4.0], [6.0, 8.0]], dtype="float32")
    b = _raster(data, epsg=27700, origin=(530000.0, 180400.0), px=200.0)
    with _open(b) as ds:
        result = raster_to_grid(
            ds, 3, "bng", "avg", coverage="sparse", assignment="centroid"
        )
    band = result[0]
    assert len(band) == 1
    assert isinstance(band[0]["cellID"], str)
    assert band[0]["measure"] == pytest.approx(5.0)


# ---------------------------------------------------------------------------
# (a) count magnitude preserved (count is now float, but value == old int)
# ---------------------------------------------------------------------------


def test_sparse_centroid_count_magnitude_equal():
    """count with sparse+centroid: same magnitude as pre-edit (12 pixels -> 12.0)."""
    from .conftest import make_geotiff_bytes

    b = make_geotiff_bytes(width=4, height=3, count=1)  # all 12 pixels valid
    with _open(b) as ds:
        result = raster_to_grid(
            ds, 6, "h3", "count", coverage="sparse", assignment="centroid"
        )
    total = sum(r["measure"] for band in result for r in band)
    assert total == pytest.approx(12.0)


# ---------------------------------------------------------------------------
# (b) complete is superset of sparse; None for avg, 0.0 for count
# ---------------------------------------------------------------------------


def test_complete_is_superset_of_sparse_h3():
    """complete coverage includes every cell that sparse does, plus possibly more."""
    data = np.array([[1.0, 2.0], [3.0, 4.0]], dtype="float32")
    b = _raster(data, px=0.1)
    with _open(b) as ds:
        sparse = raster_to_grid(
            ds, 4, "h3", "avg", coverage="sparse", assignment="centroid"
        )
    with _open(b) as ds:
        complete = raster_to_grid(
            ds, 4, "h3", "avg", coverage="complete", assignment="centroid"
        )
    sparse_ids = {r["cellID"] for band in sparse for r in band}
    complete_ids = {r["cellID"] for band in complete for r in band}
    assert sparse_ids <= complete_ids, "sparse cell set must be subset of complete"
    assert len(complete[0]) >= len(sparse[0])


def test_complete_empty_cells_have_none_for_avg():
    """Covered-but-empty cells in complete mode have measure=None for avg."""
    data = np.array([[1.0, 2.0]], dtype="float32")
    b = _raster(data, px=0.1)
    with _open(b) as ds:
        sparse = raster_to_grid(
            ds, 4, "h3", "avg", coverage="sparse", assignment="centroid"
        )
    with _open(b) as ds:
        complete = raster_to_grid(
            ds, 4, "h3", "avg", coverage="complete", assignment="centroid"
        )
    sparse_ids = {r["cellID"] for r in sparse[0]}
    extra = [r for r in complete[0] if r["cellID"] not in sparse_ids]
    for r in extra:
        assert (
            r["measure"] is None
        ), f"extra cell in complete should have None measure, got {r['measure']}"


def test_complete_count_empty_emits_zero():
    """Covered-but-empty cells in complete mode emit 0.0 for count (not None)."""
    data = np.array([[1.0, 2.0]], dtype="float32")
    b = _raster(data, px=0.1)
    with _open(b) as ds:
        sparse = raster_to_grid(
            ds, 4, "h3", "count", coverage="sparse", assignment="centroid"
        )
    with _open(b) as ds:
        complete = raster_to_grid(
            ds, 4, "h3", "count", coverage="complete", assignment="centroid"
        )
    # ALL cells in complete must have non-None float measure
    for r in complete[0]:
        assert r["measure"] is not None, "count cells must never be None"
        assert isinstance(r["measure"], float)
    # Extra cells (beyond sparse) must have 0.0
    sparse_ids = {r["cellID"] for r in sparse[0]}
    for r in complete[0]:
        if r["cellID"] not in sparse_ids:
            assert r["measure"] == pytest.approx(
                0.0
            ), f"empty count cell must be 0.0, got {r['measure']}"


def test_complete_quadbin():
    """complete mode works for quadbin too (superset, None for avg empties)."""
    data = np.array([[5.0, 6.0], [7.0, 8.0]], dtype="float32")
    b = _raster(data, px=0.1)
    with _open(b) as ds:
        sparse = raster_to_grid(
            ds, 6, "quadbin", "avg", coverage="sparse", assignment="centroid"
        )
    with _open(b) as ds:
        complete = raster_to_grid(
            ds, 6, "quadbin", "avg", coverage="complete", assignment="centroid"
        )
    sparse_ids = {r["cellID"] for band in sparse for r in band}
    complete_ids = {r["cellID"] for band in complete for r in band}
    assert sparse_ids <= complete_ids


# ---------------------------------------------------------------------------
# (c) covering conserves mass for sum
# ---------------------------------------------------------------------------


def test_covering_sum_conserves_mass_h3():
    """Covering sum: Sigma(cell sums) approx Sigma(pixel values).

    At coarse resolution (res=1, edge ~2.6 Mkm), a small raster fits entirely
    within 1-2 cells, so every pixel's area fractions sum to exactly 1.0 and
    mass is conserved precisely.
    """
    data = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]], dtype="float32")
    expected_total = float(data.sum())  # 21.0
    b = _raster(data, px=0.5)
    with _open(b) as ds:
        result = raster_to_grid(
            ds, 1, "h3", "sum", coverage="sparse", assignment="covering"
        )
    total = sum(r["measure"] for band in result for r in band)
    assert (
        abs(total - expected_total) < 1e-6
    ), f"covering sum {total} != expected {expected_total}"


def test_covering_sum_conserves_mass_quadbin():
    """Covering sum (quadbin): same mass conservation at coarse resolution."""
    data = np.array([[10.0, 20.0], [30.0, 40.0]], dtype="float32")
    expected_total = float(data.sum())  # 100.0
    b = _raster(data, px=0.5)
    with _open(b) as ds:
        result = raster_to_grid(
            ds, 1, "quadbin", "sum", coverage="sparse", assignment="covering"
        )
    total = sum(r["measure"] for band in result for r in band)
    assert (
        abs(total - expected_total) < 1e-6
    ), f"covering sum {total} != expected {expected_total}"


# ---------------------------------------------------------------------------
# (d) count is float in all modes
# ---------------------------------------------------------------------------


def test_count_is_float_sparse_centroid():
    """count yields float for sparse+centroid."""
    data = np.ones((3, 4), dtype="float32")
    b = _raster(data)
    with _open(b) as ds:
        result = raster_to_grid(
            ds, 6, "h3", "count", coverage="sparse", assignment="centroid"
        )
    for r in result[0]:
        assert isinstance(
            r["measure"], float
        ), f"count measure is {type(r['measure'])}, expected float"


def test_count_is_float_complete():
    """count yields float for complete (including 0.0 empty cells)."""
    data = np.ones((2, 2), dtype="float32")
    b = _raster(data, px=0.1)
    with _open(b) as ds:
        result = raster_to_grid(
            ds, 4, "h3", "count", coverage="complete", assignment="centroid"
        )
    for r in result[0]:
        assert isinstance(r["measure"], float)


def test_count_is_float_covering():
    """covering count (Sigma-w) is float."""
    data = np.array([[1.0, 2.0]], dtype="float32")
    b = _raster(data, px=0.5)
    with _open(b) as ds:
        result = raster_to_grid(
            ds, 2, "h3", "count", coverage="sparse", assignment="covering"
        )
    for r in result[0]:
        assert isinstance(r["measure"], float)


def test_count_is_float_bng():
    """BNG count is float."""
    data = np.array([[2.0, 4.0], [6.0, 8.0]], dtype="float32")
    b = _raster(data, epsg=27700, origin=(530000.0, 180400.0), px=200.0)
    with _open(b) as ds:
        result = raster_to_grid(
            ds, 3, "bng", "count", coverage="sparse", assignment="centroid"
        )
    band = result[0]
    assert sum(r["measure"] for r in band) == pytest.approx(4.0)
    assert all(isinstance(r["measure"], float) for r in band)


# ---------------------------------------------------------------------------
# Additional: weighted reducer correctness
# ---------------------------------------------------------------------------


def test_covering_avg_equals_centroid_avg_single_cell():
    """When all pixels fall in one cell, covering avg == centroid avg.

    All area-fractions are 1.0 (pixel fully inside the cell), so
    Sigma(v*1.0)/Sigma(1.0) = Sigma(v)/n = centroid avg.
    """
    data = np.array([[2.0, 4.0], [6.0, 8.0]], dtype="float32")
    b = _raster(data, px=0.5)
    with _open(b) as ds:
        c_avg = raster_to_grid(
            ds, 0, "h3", "avg", coverage="sparse", assignment="centroid"
        )
    with _open(b) as ds:
        w_avg = raster_to_grid(
            ds, 0, "h3", "avg", coverage="sparse", assignment="covering"
        )
    assert len(c_avg[0]) == 1
    assert len(w_avg[0]) == 1
    assert w_avg[0][0]["measure"] == pytest.approx(c_avg[0][0]["measure"], rel=1e-6)


def test_covering_min_max_weight_agnostic():
    """covering min/max are weight-agnostic (same as centroid min/max in single cell)."""
    data = np.array([[2.0, 4.0], [6.0, 8.0]], dtype="float32")
    b = _raster(data, px=0.5)
    with _open(b) as ds:
        c_min = raster_to_grid(
            ds, 0, "h3", "min", coverage="sparse", assignment="centroid"
        )
    with _open(b) as ds:
        w_min = raster_to_grid(
            ds, 0, "h3", "min", coverage="sparse", assignment="covering"
        )
    assert c_min[0][0]["measure"] == pytest.approx(w_min[0][0]["measure"])
    # max
    with _open(b) as ds:
        c_max = raster_to_grid(
            ds, 0, "h3", "max", coverage="sparse", assignment="centroid"
        )
    with _open(b) as ds:
        w_max = raster_to_grid(
            ds, 0, "h3", "max", coverage="sparse", assignment="covering"
        )
    assert c_max[0][0]["measure"] == pytest.approx(w_max[0][0]["measure"])


def test_covering_variance_single_cell():
    """Covering variance in single cell: Sigma(w*(v-mean)^2)/Sigma(w).

    When all weights are 1.0 (pixel fully in cell), this reduces to population
    variance == centroid variance.
    """
    data = np.array([[2.0, 4.0], [6.0, 8.0]], dtype="float32")
    b = _raster(data, px=0.5)
    with _open(b) as ds:
        c_var = raster_to_grid(
            ds, 0, "h3", "variance", coverage="sparse", assignment="centroid"
        )
    with _open(b) as ds:
        w_var = raster_to_grid(
            ds, 0, "h3", "variance", coverage="sparse", assignment="covering"
        )
    assert c_var[0][0]["measure"] == pytest.approx(w_var[0][0]["measure"], rel=1e-6)


def test_covering_median_single_cell():
    """Covering median in single cell with equal weights == centroid median."""
    data = np.array([[1.0, 2.0], [3.0, 4.0]], dtype="float32")
    b = _raster(data, px=0.5)
    with _open(b) as ds:
        c_med = raster_to_grid(
            ds, 0, "h3", "median", coverage="sparse", assignment="centroid"
        )
    with _open(b) as ds:
        w_med = raster_to_grid(
            ds, 0, "h3", "median", coverage="sparse", assignment="covering"
        )
    assert c_med[0][0]["measure"] == pytest.approx(w_med[0][0]["measure"], rel=1e-6)


def test_covering_count_total_weight():
    """Covering count total across cells approx n_valid pixels (mass-conserving)."""
    data = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]], dtype="float32")
    b = _raster(data, px=0.5)
    with _open(b) as ds:
        result = raster_to_grid(
            ds, 1, "h3", "count", coverage="sparse", assignment="covering"
        )
    total_w = sum(r["measure"] for band in result for r in band)
    n_valid = float(data.size)
    # At coarse res=1, pixels fully inside cells -> Sigma(w)=1 per pixel -> total=n_valid
    assert (
        abs(total_w - n_valid) < 1e-6
    ), f"total covering weight {total_w} vs n_valid {n_valid}"


# ---------------------------------------------------------------------------
# Validation: bad params raise ValueError
# ---------------------------------------------------------------------------


def test_invalid_coverage_raises():
    data = np.ones((2, 2), dtype="float32")
    b = _raster(data)
    with _open(b) as ds:
        with pytest.raises(ValueError, match="coverage"):
            raster_to_grid(ds, 0, "h3", "avg", coverage="bad")


def test_invalid_assignment_raises():
    data = np.ones((2, 2), dtype="float32")
    b = _raster(data)
    with _open(b) as ds:
        with pytest.raises(ValueError, match="assignment"):
            raster_to_grid(ds, 0, "h3", "avg", assignment="bad")
