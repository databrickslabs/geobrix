"""Custom-grid raster_to_grid + iter_tessellate (light tier).

Failing state before implementation:
  - raster_to_grid(grid="custom") raises ValueError("unknown grid")
  - iter_tessellate(grid="custom") raises ValueError("grid must be one of")

After implementation:
  - custom centroid+sparse: 4 valid pixels -> 4 cells with correct avg values
  - custom centroid+complete: partial raster yields 1 null cell + 3 measured cells
  - custom covering+sparse: single-pixel cells match centroid measures
  - custom covering+complete: partial raster yields 1 null cell
  - custom tessellate centroid+sparse: 4 chips (one per pixel)
  - custom tessellate covering: >= 4 chips
  - custom cell IDs are plain ints (Long-compatible)
"""

import numpy as np
import pytest
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pygx._custom import CustomGridConf
from databricks.labs.gbx.pyrx.core.gridagg import raster_to_grid

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


def _make_conf():
    """2x2-cell custom grid at resolution 1: extent (0,0)-(200,200), 100x100 cells.

    - root_cell_count_x = ceil(200/200) = 1, same for y
    - cell_splits = 2, so at res=1: total_cells_x = 1*2 = 2, same y -> 4 cells
    - cell_width/height at res=1 = 200/2 = 100
    """
    return CustomGridConf(
        bound_x_min=0,
        bound_x_max=200,
        bound_y_min=0,
        bound_y_max=200,
        cell_splits=2,
        root_cell_size_x=200,
        root_cell_size_y=200,
        srid=-1,
    )


def _conf_row(conf):
    """Plain dict accepted by _custom.conf_from_row (mimics a Spark Row)."""
    return {
        "bound_x_min": conf.bound_x_min,
        "bound_x_max": conf.bound_x_max,
        "bound_y_min": conf.bound_y_min,
        "bound_y_max": conf.bound_y_max,
        "cell_splits": conf.cell_splits,
        "root_cell_size_x": conf.root_cell_size_x,
        "root_cell_size_y": conf.root_cell_size_y,
        "srid": conf.srid,
    }


def _make_raster_no_crs(data, *, origin_x=0.0, origin_y=200.0, px=100.0):
    """Small single-band raster in grid-native coords with NO CRS (crs=None).

    Rasterio north-up convention: origin is NW corner (west, north).
    With origin_x=0, origin_y=200, px=100:
      pixel (col=j, row=i) centroid x = j*100 + 50, y = 200 - i*100 - 50
    So:
      (0,0)->( 50,150), (1,0)->(150,150)
      (0,1)->( 50, 50), (1,1)->(150, 50)
    At custom grid res=1 (cell size 100x100):
      cell_pos_x = trunc(x/100), cell_pos_y = trunc(y/100)
      (0,0)->(0,1), (1,0)->(1,1), (0,1)->(0,0), (1,1)->(1,0)
    """
    h, w = data.shape
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="float32",
        crs=None,
        transform=from_origin(origin_x, origin_y, px, px),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data.astype("float32"), 1)
        return mf.read()


def _open(b):
    return MemoryFile(b).open()


_CONF = _make_conf()
_CONF_ROW = _conf_row(_CONF)
_RES = 1  # resolution 1 -> 4 cells of 100x100 each


def _make_full_raster():
    """2x2 raster, all valid: [[10., 20.], [30., 40.]]."""
    return _make_raster_no_crs(np.array([[10.0, 20.0], [30.0, 40.0]], dtype="float32"))


def _make_partial_raster():
    """2x2 raster, bottom-left NoData: [[10., 20.], [-9999., 40.]]."""
    return _make_raster_no_crs(
        np.array([[10.0, 20.0], [-9999.0, 40.0]], dtype="float32")
    )


# ---------------------------------------------------------------------------
# Centroid tests
# ---------------------------------------------------------------------------


def test_custom_rastertogridavg_centroid_sparse_cell_set():
    """centroid+sparse: 4 valid pixels -> 4 cells, avg equals pixel value each."""
    b = _make_full_raster()
    with _open(b) as ds:
        result = raster_to_grid(
            ds,
            _RES,
            "custom",
            "avg",
            coverage="sparse",
            assignment="centroid",
            grid_conf=_CONF_ROW,
        )
    assert len(result) == 1, "expected one band"
    band = result[0]
    assert len(band) == 4, f"expected 4 cells, got {len(band)}"
    measures = sorted(r["measure"] for r in band)
    assert measures == pytest.approx([10.0, 20.0, 30.0, 40.0])
    assert all(isinstance(r["cellID"], int) for r in band)


def test_custom_rastertogridavg_centroid_complete_null_cells():
    """centroid+complete: NoData pixel -> its cell gets measure=None (avg)."""
    b = _make_partial_raster()
    with _open(b) as ds:
        result = raster_to_grid(
            ds,
            _RES,
            "custom",
            "avg",
            coverage="complete",
            assignment="centroid",
            grid_conf=_CONF_ROW,
        )
    assert len(result) == 1
    band = result[0]
    assert len(band) == 4, f"expected 4 cells (3 valid + 1 null), got {len(band)}"
    null_cells = [r for r in band if r["measure"] is None]
    assert len(null_cells) == 1, f"expected 1 null cell, got {null_cells}"
    valid_measures = sorted(r["measure"] for r in band if r["measure"] is not None)
    assert valid_measures == pytest.approx([10.0, 20.0, 40.0])


def test_custom_rastertogridcount_centroid_complete_zero():
    """count+complete: covered-but-empty cell gets measure=0.0, not None."""
    b = _make_partial_raster()
    with _open(b) as ds:
        result = raster_to_grid(
            ds,
            _RES,
            "custom",
            "count",
            coverage="complete",
            assignment="centroid",
            grid_conf=_CONF_ROW,
        )
    band = result[0]
    assert len(band) == 4
    zero_cells = [r for r in band if r["measure"] == 0.0]
    assert len(zero_cells) == 1, f"expected 1 zero-count cell, got {zero_cells}"


def test_custom_rastertogridavg_centroid_cellids_unique():
    """Each pixel maps to a distinct cell; cell IDs must be unique."""
    b = _make_full_raster()
    with _open(b) as ds:
        result = raster_to_grid(
            ds,
            _RES,
            "custom",
            "avg",
            coverage="sparse",
            assignment="centroid",
            grid_conf=_CONF_ROW,
        )
    band = result[0]
    ids = [r["cellID"] for r in band]
    assert len(ids) == len(set(ids)), f"duplicate cell IDs: {ids}"


# ---------------------------------------------------------------------------
# Covering tests
# ---------------------------------------------------------------------------


def test_custom_rastertogridavg_covering_sparse_matches_centroid():
    """covering+sparse with non-overlapping cells must produce same measures as centroid."""
    b = _make_full_raster()
    with _open(b) as ds:
        r_cov = raster_to_grid(
            ds,
            _RES,
            "custom",
            "avg",
            coverage="sparse",
            assignment="covering",
            grid_conf=_CONF_ROW,
        )
        r_cen = raster_to_grid(
            ds,
            _RES,
            "custom",
            "avg",
            coverage="sparse",
            assignment="centroid",
            grid_conf=_CONF_ROW,
        )
    assert len(r_cov[0]) == len(r_cen[0])
    cov_measures = sorted(r["measure"] for r in r_cov[0])
    cen_measures = sorted(r["measure"] for r in r_cen[0])
    assert cov_measures == pytest.approx(cen_measures, abs=1e-3)


def test_custom_rastertogridavg_covering_complete_null_cells():
    """covering+complete: NoData pixel -> its cell gets measure=None."""
    b = _make_partial_raster()
    with _open(b) as ds:
        result = raster_to_grid(
            ds,
            _RES,
            "custom",
            "avg",
            coverage="complete",
            assignment="covering",
            grid_conf=_CONF_ROW,
        )
    band = result[0]
    assert len(band) == 4
    null_cells = [r for r in band if r["measure"] is None]
    assert len(null_cells) == 1


# ---------------------------------------------------------------------------
# Custom tessellate tests
# ---------------------------------------------------------------------------


def test_custom_tessellate_centroid_sparse_chip_count():
    """iter_tessellate centroid+sparse: one chip per valid pixel (4 for 2x2)."""
    from databricks.labs.gbx.pyrx.core.tessellate import iter_tessellate

    b = _make_full_raster()
    with _open(b) as ds:
        results = list(
            iter_tessellate(
                ds, _RES, "custom", mode="centroid", coverage="sparse", conf=_CONF
            )
        )
    assert len(results) == 4


def test_custom_tessellate_covering_produces_chips():
    """iter_tessellate covering mode: all 4 cells overlap the 2x2 raster."""
    from databricks.labs.gbx.pyrx.core.tessellate import iter_tessellate

    b = _make_full_raster()
    with _open(b) as ds:
        results = list(
            iter_tessellate(
                ds, _RES, "custom", mode="covering", coverage="sparse", conf=_CONF
            )
        )
    assert len(results) >= 4


def test_custom_tessellate_cellids_are_ints():
    """Custom grid cell IDs emitted by iter_tessellate must be plain ints."""
    from databricks.labs.gbx.pyrx.core.tessellate import iter_tessellate

    b = _make_full_raster()
    with _open(b) as ds:
        results = list(
            iter_tessellate(
                ds, _RES, "custom", mode="centroid", coverage="sparse", conf=_CONF
            )
        )
    for cellid, _ in results:
        assert isinstance(cellid, int), f"expected int cellID, got {type(cellid)}"


def test_custom_tessellate_centroid_complete_includes_empty_chip():
    """centroid+complete on partial raster emits empty chip for the NoData cell."""
    from databricks.labs.gbx.pyrx.core.tessellate import iter_tessellate

    b = _make_partial_raster()
    with _open(b) as ds:
        results = list(
            iter_tessellate(
                ds, _RES, "custom", mode="centroid", coverage="complete", conf=_CONF
            )
        )
    # 3 valid pixel chips + 1 empty chip for NoData cell
    assert len(results) == 4


# ---------------------------------------------------------------------------
# Resolution-0 regression: cell id 0 must NOT be filtered by `or -1` sentinel
# ---------------------------------------------------------------------------


def test_custom_rastertogridavg_centroid_res0_cellid_zero_present():
    """REGRESSION: cell id 0 at resolution 0 must survive the out-of-bounds filter.

    At resolution 0 all pixels map to the single cell with id 0 (``get_cell_id(0, 0)``).
    An ``or -1`` sentinel incorrectly treats 0 as falsy and maps it to -1, then
    ``keep = cids_raw != -1`` drops every pixel, producing an empty result.
    The ``is not None`` fix preserves id 0.
    """
    # Resolution 0: one cell covering the whole extent, cell_id = 0
    b = _make_full_raster()
    with _open(b) as ds:
        result = raster_to_grid(
            ds,
            0,
            "custom",
            "avg",
            coverage="sparse",
            assignment="centroid",
            grid_conf=_CONF_ROW,
        )
    band = result[0]
    assert len(band) == 1, f"expected 1 cell at res=0, got {len(band)}"
    assert band[0]["cellID"] == 0, f"expected cell id 0, got {band[0]['cellID']}"
    # avg of all 4 pixels: (10 + 20 + 30 + 40) / 4 = 25.0
    assert band[0]["measure"] == pytest.approx(25.0)


def test_custom_tessellate_centroid_res0_agrees_with_rastertogrid():
    """Centroid tessellate at res=0 also yields cell id 0, matching rastertogrid."""
    from databricks.labs.gbx.pyrx.core.tessellate import iter_tessellate

    b = _make_full_raster()
    with _open(b) as ds:
        results = list(
            iter_tessellate(
                ds, 0, "custom", mode="centroid", coverage="sparse", conf=_CONF
            )
        )
    assert len(results) == 1, f"expected 1 chip at res=0, got {len(results)}"
    cellid, _ = results[0]
    assert cellid == 0, f"expected cell id 0, got {cellid}"
