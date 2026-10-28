"""O(boundary) covering interior fast-path for the light tier.

TDD guards (per brief):
1. Result-identity (direct old-path vs fast-path comparison): _covering_band output
   for custom/quadbin/bng is within 1e-9 (per-cell) between old path (fast-path
   disabled via monkeypatch of _COVERING_FAST_PATH_EXACT) and fast-path enabled.
   This is the primary result-neutrality proof for all three exact-geometry grids.
2. H3 unchanged: H3 uses the old path; output is trivially identical.
3. Polyfill-skip proof: interior pixels of exact-geometry grids (custom, quadbin,
   bng) SKIP _polyfill_cells; only boundary pixels enumerate candidates.
4. Fast-path NOT taken for H3.

The quadbin fixture uses a NON-DEGENERATE res=5 fixture (2 distinct cells, 1
boundary pixel straddling tile x=15/16, 1 interior pixel fully inside tile x=15).
The res=0 world-covering fixture cannot distinguish the fast-path from the old path.
"""

from unittest.mock import patch

import numpy as np
import pytest
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pygx._custom import CustomGridConf
from databricks.labs.gbx.pyrx.core.gridagg import raster_to_grid

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _open(b):
    return MemoryFile(b).open()


def _to_measure_map(band_result):
    """Convert a band result list to {cellID: measure} dict."""
    return {r["cellID"]: r["measure"] for r in band_result}


# ---------------------------------------------------------------------------
# Custom-grid fixtures
# ---------------------------------------------------------------------------


def _make_custom_conf():
    """1000x1000-unit grid, cell_splits=2, so res=1 → 4 cells each 500x500.

    Cell layout at resolution 1 (cell_width = cell_height = 500):
      cell_pos_x = trunc((x - 0) / 500),  cell_pos_y = trunc((y - 0) / 500)

      (0,0): x in [0, 500), y in [0, 500)   — lower-left
      (1,0): x in [500,1000), y in [0, 500)  — lower-right
      (0,1): x in [0, 500), y in [500,1000)  — upper-left
      (1,1): x in [500,1000), y in [500,1000) — upper-right
    """
    return CustomGridConf(
        bound_x_min=0,
        bound_x_max=1000,
        bound_y_min=0,
        bound_y_max=1000,
        cell_splits=2,
        root_cell_size_x=1000,
        root_cell_size_y=1000,
        srid=-1,
    )


def _conf_row(conf):
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


def _make_custom_raster_mixed():
    """3×1 raster — 1 boundary pixel + 2 interior pixels.

    Raster: west=300, north=400, pixel_size=200 (no CRS → grid-native).
    Grid at res=1: 4 cells, each 500×500.

    Corner coordinates per pixel (col, row=0):
      col=0: corners at x ∈ {300,500}, y ∈ {200,400}
             trunc(300/500)=0  →  left corners in cell_pos_x=0
             trunc(500/500)=1  →  right corners in cell_pos_x=1
             → corners span two cells → BOUNDARY pixel (fast-path skipped)
      col=1: corners at x ∈ {500,700}, y ∈ {200,400}
             trunc(500/500)=1, trunc(700/500)=1  →  cell_pos_x=1 for all
             trunc(200/500)=0, trunc(400/500)=0  →  cell_pos_y=0 for all
             → all four corners in cell(pos_x=1,pos_y=0) → INTERIOR
      col=2: corners at x ∈ {700,900}, y ∈ {200,400}
             trunc(700/500)=1, trunc(900/500)=1  →  cell_pos_x=1 for all
             → all four corners in cell(pos_x=1,pos_y=0) → INTERIOR

    Values: [10.0, 20.0, 30.0]
    Expected sum (sparse, covering):
      cell(0,0): pixel-0 rectangle ∩ box(0,0,500,500) = box(300,200,500,400),
                 area=200×200=40000 = pixel area → weight=1.0 → sum=10.0
      cell(1,0): pixel-1 weight=1.0 (interior) + pixel-2 weight=1.0 (interior)
                 → sum = 20.0 + 30.0 = 50.0
    """
    data = np.array([[10.0, 20.0, 30.0]], dtype="float32")
    profile = dict(
        driver="GTiff",
        width=3,
        height=1,
        count=1,
        dtype="float32",
        transform=from_origin(300.0, 400.0, 200.0, 200.0),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        return mf.read()


def _make_custom_raster_interior_only():
    """2×1 raster — 2 interior pixels (both in cell(1,0)).

    Raster: west=510, north=390, pixel_size=100 (no CRS → grid-native).
    Corners for both pixels:
      col=0: x ∈ {510, 610}, y ∈ {290, 390} → cell_pos_x=1, cell_pos_y=0 → INTERIOR
      col=1: x ∈ {610, 710}, y ∈ {290, 390} → cell_pos_x=1, cell_pos_y=0 → INTERIOR

    Values: [5.0, 7.0]
    Expected sum: cell(1,0) → 12.0
    """
    data = np.array([[5.0, 7.0]], dtype="float32")
    profile = dict(
        driver="GTiff",
        width=2,
        height=1,
        count=1,
        dtype="float32",
        transform=from_origin(510.0, 390.0, 100.0, 100.0),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        return mf.read()


# ---------------------------------------------------------------------------
# BNG fixtures
# ---------------------------------------------------------------------------


def _make_bng_raster_mixed():
    """2×1 raster in EPSG:27700 — 1 interior pixel + 1 boundary pixel.

    BNG resolution 3 (1 km cells): each cell spans 1000 m.
    Raster: west=530100, north=180900, pixel_size=800.

    col=0: e ∈ {530100, 530900}, n ∈ {180100, 180900}
           point_to_cell_id corners: all within TQ30 (e=[530000,531000), n=[180000,181000))
           → all four corners → same BNG cell → INTERIOR
           Shapely intersection of pixel with TQ30 box = pixel itself (exact integers)
           → weight = 640000/640000 = 1.0 exactly (same as fast-path direct 1.0)

    col=1: e ∈ {530900, 531700}, n ∈ {180100, 180900}
           530900 → TQ30 (e<531000); 531700 → TQ31 (e≥531000)
           → corners span TQ30 and TQ31 → BOUNDARY pixel (fallback used)

    Values: [3.0, 7.0]
    """
    data = np.array([[3.0, 7.0]], dtype="float32")
    profile = dict(
        driver="GTiff",
        width=2,
        height=1,
        count=1,
        dtype="float32",
        crs="EPSG:27700",
        transform=from_origin(530100.0, 180900.0, 800.0, 800.0),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        return mf.read()


# ---------------------------------------------------------------------------
# Quadbin fixtures (NON-DEGENERATE: res=5, 2 cells, 1 boundary + 1 interior)
# ---------------------------------------------------------------------------


def _make_quadbin_raster_nondeg():
    """2×1 raster at EPSG:4326 — 1 interior pixel + 1 boundary pixel.

    At quadbin res=5 (z=5, 32×32 tiles), longitude tile boundaries are at
    lon = k×11.25 - 180 for k=0..32. The boundary between tile x=15 and
    tile x=16 is at lon=0.

    Tile y=15 at z=5 spans approximately lat ∈ [0°, ~11.2°] (above equator).

    Raster: west=-6, north=3, xsize=4 (lon), ysize=2 (lat).

    col=0: lon ∈ [-6, -2], lat ∈ [1, 3]
           All 4 corners → tile (x=15, y=15) (lon<0, lat∈[1,3] is above equator
           and below ~11.2°) → INTERIOR pixel
           Shapely intersection = pixel itself → weight=1.0 exactly (same as fast-path)

    col=1: lon ∈ [-2, +2], lat ∈ [1, 3]
           Left corners at lon=-2 → tile x=15; right at lon=+2 → tile x=16
           → BOUNDARY pixel; each half-pixel goes to one tile (area fraction=0.5)

    Values: [10.0, 20.0]
    Expected sum (sparse):
      tile(15,15): 10.0×1.0 (interior) + 20.0×0.5 (left half of boundary) = 20.0
      tile(16,15): 20.0×0.5 (right half of boundary) = 10.0
      total = 30.0 (mass conserved)
    """
    data = np.array([[10.0, 20.0]], dtype="float32")
    profile = dict(
        driver="GTiff",
        width=2,
        height=1,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(-6.0, 3.0, 4.0, 2.0),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        return mf.read()


# ---------------------------------------------------------------------------
# 1. Old-path vs fast-path neutrality harness: custom, quadbin, bng (≤1e-9)
# ---------------------------------------------------------------------------


def test_custom_fast_path_neutral_vs_old_path():
    """Direct old-path vs fast-path comparison for custom: per-cell max delta ≤1e-9.

    Forces the old path by patching _COVERING_FAST_PATH_EXACT to frozenset(),
    then compares per-cell sum measures with the fast-path-enabled default.
    The fast-path contributes weight=1.0 directly; the old path computes shapely
    intersection of a pixel fully inside a square cell → area/area = 1.0 exactly.
    Expected max delta: 0.0 (both paths produce exactly 1.0 for interior pixels).
    """
    from databricks.labs.gbx.pyrx.core import gridagg

    conf = _make_custom_conf()
    b = _make_custom_raster_mixed()
    kwargs = dict(
        coverage="sparse",
        assignment="covering",
        grid_conf=_conf_row(conf),
    )

    with patch.object(gridagg, "_COVERING_FAST_PATH_EXACT", frozenset()):
        with _open(b) as ds:
            result_old = raster_to_grid(ds, 1, "custom", "sum", **kwargs)

    with _open(b) as ds:
        result_new = raster_to_grid(ds, 1, "custom", "sum", **kwargs)

    old_map = _to_measure_map(result_old[0])
    new_map = _to_measure_map(result_new[0])
    assert set(old_map) == set(
        new_map
    ), f"Cell sets differ: old={set(old_map)} new={set(new_map)}"
    max_diff = max(abs(new_map[cid] - old_map[cid]) for cid in old_map)
    assert max_diff < 1e-9, (
        f"Custom fast-path vs old-path max per-cell delta: {max_diff:.3e} "
        f"(bar 1e-9; expected 0.0 — both paths give weight=1.0 exactly for "
        f"interior square-cell pixels)"
    )


def test_bng_fast_path_neutral_vs_old_path():
    """Direct old-path vs fast-path comparison for BNG: per-cell max delta ≤1e-9.

    Interior pixel (col=0) all-integer corners → shapely intersection area is
    exact (640000/640000 = 1.0); fast-path gives 1.0 directly.
    Boundary pixel (col=1) uses the old path in both runs (corners differ).
    Expected max delta: 0.0.
    """
    from databricks.labs.gbx.pyrx.core import gridagg

    b = _make_bng_raster_mixed()
    kwargs = dict(coverage="sparse", assignment="covering")

    with patch.object(gridagg, "_COVERING_FAST_PATH_EXACT", frozenset()):
        with _open(b) as ds:
            result_old = raster_to_grid(ds, 3, "bng", "sum", **kwargs)

    with _open(b) as ds:
        result_new = raster_to_grid(ds, 3, "bng", "sum", **kwargs)

    old_map = _to_measure_map(result_old[0])
    new_map = _to_measure_map(result_new[0])
    assert set(old_map) == set(
        new_map
    ), f"Cell sets differ: old={set(old_map)} new={set(new_map)}"
    max_diff = max(abs(new_map[cid] - old_map[cid]) for cid in old_map)
    assert max_diff < 1e-9, (
        f"BNG fast-path vs old-path max per-cell delta: {max_diff:.3e} "
        f"(bar 1e-9; expected 0.0 — interior pixel uses exact-integer coords)"
    )


def test_quadbin_fast_path_neutral_vs_old_path():
    """Direct old-path vs fast-path comparison for quadbin: per-cell max delta ≤1e-9.

    Non-degenerate res=5 fixture: col=0 is interior (all 4 corners tile x=15),
    col=1 is boundary (straddles tile x=15/x=16 boundary at lon=0).
    Interior pixel: fast-path weight=1.0; old-path shapely intersection = pixel
    itself (fully inside cell bbox) → area/area = 1.0 exactly.
    Boundary pixel: both runs use old path (corners differ); same result.
    Expected max delta: 0.0.
    """
    from databricks.labs.gbx.pyrx.core import gridagg

    b = _make_quadbin_raster_nondeg()
    kwargs = dict(coverage="sparse", assignment="covering")

    with patch.object(gridagg, "_COVERING_FAST_PATH_EXACT", frozenset()):
        with _open(b) as ds:
            result_old = raster_to_grid(ds, 5, "quadbin", "sum", **kwargs)

    with _open(b) as ds:
        result_new = raster_to_grid(ds, 5, "quadbin", "sum", **kwargs)

    old_map = _to_measure_map(result_old[0])
    new_map = _to_measure_map(result_new[0])
    assert set(old_map) == set(
        new_map
    ), f"Cell sets differ: old={set(old_map)} new={set(new_map)}"
    max_diff = max(abs(new_map[cid] - old_map[cid]) for cid in old_map)
    assert max_diff < 1e-9, (
        f"Quadbin fast-path vs old-path max per-cell delta: {max_diff:.3e} "
        f"(bar 1e-9; expected 0.0 — interior pixel fully inside square tile)"
    )


# ---------------------------------------------------------------------------
# 2. Analytic result-identity guards: custom, BNG, quadbin
# ---------------------------------------------------------------------------


def test_custom_covering_sum_matches_expected_analytics():
    """Custom covering sum matches analytic expectation.

    The boundary pixel (col=0, fast-path skipped) still yields correct
    intersection result via shapely.  Interior pixels 1 and 2 yield weight=1.0
    (fast-path or fallback: identical).
    """
    conf = _make_custom_conf()
    b = _make_custom_raster_mixed()

    with _open(b) as ds:
        result = raster_to_grid(
            ds,
            1,
            "custom",
            "sum",
            coverage="sparse",
            assignment="covering",
            grid_conf=_conf_row(conf),
        )

    band = result[0]
    m = _to_measure_map(band)
    assert len(band) == 2, f"expected 2 cells, got {len(band)}: {band}"
    cell_sum = sorted(m.values())
    assert cell_sum[0] == pytest.approx(10.0, abs=1e-6)
    assert cell_sum[1] == pytest.approx(50.0, abs=1e-6)


def test_custom_covering_avg_matches_expected_analytics():
    """Custom covering avg = weighted avg (all-interior fixture, weights=1.0)."""
    conf = _make_custom_conf()
    b = _make_custom_raster_interior_only()

    with _open(b) as ds:
        result = raster_to_grid(
            ds,
            1,
            "custom",
            "avg",
            coverage="sparse",
            assignment="covering",
            grid_conf=_conf_row(conf),
        )

    band = result[0]
    assert len(band) == 1
    # 2 interior pixels with values [5.0, 7.0], each weight=1.0 → avg=6.0
    assert band[0]["measure"] == pytest.approx(6.0, abs=1e-9)


def test_custom_covering_count_matches_expected():
    """Custom covering count = Σw = n_pixels (all interior, all weight=1.0)."""
    conf = _make_custom_conf()
    b = _make_custom_raster_interior_only()

    with _open(b) as ds:
        result = raster_to_grid(
            ds,
            1,
            "custom",
            "count",
            coverage="sparse",
            assignment="covering",
            grid_conf=_conf_row(conf),
        )

    band = result[0]
    assert len(band) == 1
    assert band[0]["measure"] == pytest.approx(2.0, abs=1e-9)
    assert isinstance(band[0]["measure"], float)


def test_bng_covering_sum_mass_conserved():
    """BNG covering sum: Σ(cell sums) == Σ(pixel values) to within 1e-6.

    The interior pixel (col=0) contributes its full value to one cell.
    The boundary pixel (col=1) is split between TQ30 and TQ31 by shapely.
    Total mass must equal 3.0 + 7.0 = 10.0.
    """
    b = _make_bng_raster_mixed()

    with _open(b) as ds:
        result = raster_to_grid(
            ds,
            3,
            "bng",
            "sum",
            coverage="sparse",
            assignment="covering",
        )

    band = result[0]
    m = _to_measure_map(band)
    total = sum(m.values())
    assert total == pytest.approx(10.0, abs=1e-6)


def test_quadbin_covering_sum_nondeg_matches_analytics():
    """Quadbin covering sum: non-degenerate res=5 fixture matches analytic values.

    Interior pixel (col=0, tile x=15): contributes value=10.0 with weight=1.0.
    Boundary pixel (col=1, straddles lon=0): split evenly between tile x=15 and
    tile x=16 (each half is 2° wide out of 4° total → fraction=0.5).

    Expected: tile(15,15)=20.0, tile(16,15)=10.0, total=30.0.
    """
    b = _make_quadbin_raster_nondeg()

    with _open(b) as ds:
        result = raster_to_grid(
            ds,
            5,
            "quadbin",
            "sum",
            coverage="sparse",
            assignment="covering",
        )

    band = result[0]
    m = _to_measure_map(band)
    assert len(band) == 2, f"expected 2 cells, got {len(band)}: {band}"
    total = sum(m.values())
    assert total == pytest.approx(30.0, abs=1e-6), f"mass not conserved: {total}"
    cell_sums = sorted(m.values())
    assert cell_sums[0] == pytest.approx(10.0, abs=1e-6)
    assert cell_sums[1] == pytest.approx(20.0, abs=1e-6)


# ---------------------------------------------------------------------------
# 3. H3 output unchanged (old path used)
# ---------------------------------------------------------------------------


def test_h3_covering_sum_unchanged_by_fast_path():
    """H3 covering sum is correct (old path, fast-path never fires for H3).

    Uses the same all-in-one-cell fixture from test_gridagg_coverage_assignment.py
    (2×1 raster at res=1, px=0.5 → all pixels in 1-2 coarse cells).
    Mass must be conserved: Σ(cell sums) ≈ Σ(pixel values).
    """
    data = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]], dtype="float32")
    expected_total = float(data.sum())  # 21.0

    profile = dict(
        driver="GTiff",
        width=3,
        height=2,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(0.0, 1.0, 0.5, 0.5),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        b = mf.read()

    with _open(b) as ds:
        result = raster_to_grid(
            ds,
            1,
            "h3",
            "sum",
            coverage="sparse",
            assignment="covering",
        )

    total = sum(r["measure"] for band in result for r in band)
    assert (
        abs(total - expected_total) < 1e-4
    ), f"H3 covering sum {total} != expected {expected_total}"


def test_h3_covering_polyfill_still_called():
    """H3 uses the old path — _polyfill_cells must be called for every valid pixel."""
    data = np.array([[1.0, 2.0]], dtype="float32")
    profile = dict(
        driver="GTiff",
        width=2,
        height=1,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=from_origin(0.0, 1.0, 0.5, 0.5),
        nodata=-9999.0,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        b = mf.read()

    from databricks.labs.gbx.pyrx.core import tessellate as _tess

    original = _tess._polyfill_cells
    call_count = []

    def _counting_polyfill(*args, **kwargs):
        call_count.append(1)
        return original(*args, **kwargs)

    with patch.object(_tess, "_polyfill_cells", side_effect=_counting_polyfill):
        with _open(b) as ds:
            raster_to_grid(ds, 1, "h3", "sum", coverage="sparse", assignment="covering")

    # H3 has 2 valid pixels → 2 polyfill calls (no fast-path)
    assert (
        len(call_count) == 2
    ), f"H3 should call _polyfill_cells for every pixel; got {len(call_count)}"


# ---------------------------------------------------------------------------
# 4. Polyfill-skip proof: interior pixels skip _polyfill_cells
# ---------------------------------------------------------------------------


def test_custom_interior_pixels_skip_polyfill():
    """Interior custom pixels must NOT call _polyfill_cells; only boundary pixels do.

    Fixture: 3×1 raster with 1 boundary pixel (col=0) and 2 interior pixels (col=1,2).
    After fast-path: _polyfill_cells called exactly once (for the boundary pixel only).
    """
    conf = _make_custom_conf()
    b = _make_custom_raster_mixed()

    from databricks.labs.gbx.pyrx.core import tessellate as _tess

    original = _tess._polyfill_cells
    call_count = []

    def _counting_polyfill(*args, **kwargs):
        call_count.append(1)
        return original(*args, **kwargs)

    with patch.object(_tess, "_polyfill_cells", side_effect=_counting_polyfill):
        with _open(b) as ds:
            raster_to_grid(
                ds,
                1,
                "custom",
                "sum",
                coverage="sparse",
                assignment="covering",
                grid_conf=_conf_row(conf),
            )

    assert len(call_count) == 1, (
        f"Expected exactly 1 _polyfill_cells call (boundary pixel only); "
        f"got {len(call_count)}.  "
        f"Interior pixels must take the fast-path and skip polyfill."
    )


def test_custom_all_interior_skips_polyfill_entirely():
    """All-interior fixture: _polyfill_cells must never be called (count=0)."""
    conf = _make_custom_conf()
    b = _make_custom_raster_interior_only()

    from databricks.labs.gbx.pyrx.core import tessellate as _tess

    original = _tess._polyfill_cells
    call_count = []

    def _counting_polyfill(*args, **kwargs):
        call_count.append(1)
        return original(*args, **kwargs)

    with patch.object(_tess, "_polyfill_cells", side_effect=_counting_polyfill):
        with _open(b) as ds:
            raster_to_grid(
                ds,
                1,
                "custom",
                "avg",
                coverage="sparse",
                assignment="covering",
                grid_conf=_conf_row(conf),
            )

    assert len(call_count) == 0, (
        f"All-interior fixture: expected 0 _polyfill_cells calls; "
        f"got {len(call_count)}.  All pixels must take the fast-path."
    )


def test_quadbin_nondeg_interior_skips_polyfill():
    """Non-degenerate quadbin (res=5): interior pixel skips polyfill, boundary uses it.

    Fixture: 2×1 raster — col=0 interior (tile x=15), col=1 boundary (straddles x=15/16).
    Expected: exactly 1 _polyfill_cells call (boundary pixel col=1 only).
    """
    b = _make_quadbin_raster_nondeg()

    from databricks.labs.gbx.pyrx.core import tessellate as _tess

    original = _tess._polyfill_cells
    call_count = []

    def _counting_polyfill(*args, **kwargs):
        call_count.append(1)
        return original(*args, **kwargs)

    with patch.object(_tess, "_polyfill_cells", side_effect=_counting_polyfill):
        with _open(b) as ds:
            raster_to_grid(
                ds,
                5,
                "quadbin",
                "sum",
                coverage="sparse",
                assignment="covering",
            )

    assert len(call_count) == 1, (
        f"Non-degenerate quadbin: expected 1 _polyfill_cells call (boundary pixel); "
        f"got {len(call_count)}."
    )


def test_bng_interior_pixel_skips_polyfill():
    """BNG interior pixel (col=0) must skip _polyfill_cells.

    2×1 raster: col=0 is interior (all corners in TQ30), col=1 is boundary.
    Expected: exactly 1 _polyfill_cells call (col=1 boundary pixel only).
    """
    b = _make_bng_raster_mixed()

    from databricks.labs.gbx.pyrx.core import tessellate as _tess

    original = _tess._polyfill_cells
    call_count = []

    def _counting_polyfill(*args, **kwargs):
        call_count.append(1)
        return original(*args, **kwargs)

    with patch.object(_tess, "_polyfill_cells", side_effect=_counting_polyfill):
        with _open(b) as ds:
            raster_to_grid(
                ds,
                3,
                "bng",
                "sum",
                coverage="sparse",
                assignment="covering",
            )

    assert len(call_count) == 1, (
        f"BNG: expected exactly 1 _polyfill_cells call (boundary pixel); "
        f"got {len(call_count)}."
    )
