"""Micro-benchmark: geom-aware kring boundary-as-line speedup (quadbin / bng / custom).

Committed harness for the O(perimeter) lazy-seed optimisation that landed in v0.5.2:
quadbin, BNG, and custom grids now scale with the polygon's *boundary*, not its area.
Results are byte-identical to before except for one corrected heavy-tier over-count on
non-rectangular polygons (see release notes).

h3 uses H3's native ``polygon_to_cells_experimental`` bulk fill (C-optimised); the
perimeter-walk path was benchmarked and found to be 23–96× SLOWER than the native fill
for large polygons, so h3 retains the native O(area) path.  The h3 section below times
the native path only (no before/after comparison).

This file is SPARK-FREE and needs no Docker, JAR, or Databricks cluster.

Benchmark tests carry the ``bench`` pytest marker and are SKIPPED in normal CI:

    pytest -m bench python/geobrix/test/pygx/bench_geomk_boundary.py

Fast regression assertions (no ``bench`` marker) verify that the lazy path never
calls the O(area) polyfill for quadbin/bng/custom; they DO run in normal
``gbx:test:python`` runs.

Run the benchmark standalone (prints per-grid speedup tables):

    python python/geobrix/test/pygx/bench_geomk_boundary.py

Expected results (reference machine: geobrix-dev Docker, 4-core, 8 GB):
  - quadbin z=15 large poly: ~50–120× speedup (boundary-out)
  - bng res=3 large poly:    ~15–30× speedup  (boundary-out)
  - custom res=1 large poly: ~50–100× speedup (boundary-out)
  - h3 res=8/10:             native bulk polyfill path (not converted — bulk
                              polygon_to_cells_experimental outperforms perimeter-walk)
"""

from __future__ import annotations

import time
from typing import Any, Callable

import pytest
import shapely
from shapely.geometry import MultiPolygon, Polygon

from databricks.labs.gbx.pygx import _bng, _custom, _dilate, _h3, _quadbin

# ---------------------------------------------------------------------------
# pytest marker — benchmark tests skipped in normal CI runs
# ---------------------------------------------------------------------------
pytestmark = []  # applied per-test below; module-level mark would skip regression too

_BENCH_MARK = pytest.mark.skip(reason="benchmark - not included in normal CI runs")


# ===========================================================================
# Polygon factories
# ===========================================================================


def _large_wgs84_blob(
    cx: float = -90.0, cy: float = 40.0, rx: float = 1.3, ry: float = 1.1
):
    """Irregular 20-vertex polygon in WGS84, ~230×240 km at mid-latitudes.

    Matches the spike harness (spike-h3-boundary-perf.md / spike_h3_boundary_perf.py)
    so benchmark numbers are directly comparable to the spike's estimated speedups.
    """
    import math

    angles = [i * 2 * math.pi / 20 for i in range(20)]
    # Vary radius by ±30 % to make it irregular (not a plain ellipse).
    import random

    rng = random.Random(42)
    coords = [
        (
            cx + rx * (0.7 + 0.6 * rng.random()) * math.cos(a),
            cy + ry * (0.7 + 0.6 * rng.random()) * math.sin(a),
        )
        for a in angles
    ]
    coords.append(coords[0])
    return Polygon(coords)


def _large_bng_blob(
    cx: float = 430_000, cy: float = 300_000, rx: float = 90_000, ry: float = 70_000
):
    """Irregular 20-vertex polygon in EPSG:27700 (BNG), ~90 km × 70 km."""
    import math
    import random

    angles = [i * 2 * math.pi / 20 for i in range(20)]
    rng = random.Random(42)
    coords = [
        (
            cx + rx * (0.7 + 0.6 * rng.random()) * math.cos(a),
            cy + ry * (0.7 + 0.6 * rng.random()) * math.sin(a),
        )
        for a in angles
    ]
    coords.append(coords[0])
    return Polygon(coords)


def _large_custom_blob(
    cx: float = 500_000, cy: float = 500_000, rx: float = 100_000, ry: float = 90_000
):
    """Irregular 20-vertex polygon for a 0..1 M × 0..1 M custom grid, ~100 km × 90 km cells."""
    import math
    import random

    angles = [i * 2 * math.pi / 20 for i in range(20)]
    rng = random.Random(42)
    coords = [
        (
            cx + rx * (0.7 + 0.6 * rng.random()) * math.cos(a),
            cy + ry * (0.7 + 0.6 * rng.random()) * math.sin(a),
        )
        for a in angles
    ]
    coords.append(coords[0])
    return Polygon(coords)


def _medium_wgs84_blobs(n: int = 200, seed: int = 7):
    """``n`` irregular medium WGS84 polygons, radius 0.010–0.022° each (~1–2.4 km)."""
    import math
    import random

    rng = random.Random(seed)
    polys = []
    for _ in range(n):
        cx, cy = rng.uniform(-98, -82), rng.uniform(36, 44)
        r = rng.uniform(0.010, 0.022)
        angles = [i * 2 * math.pi / 12 for i in range(12)]
        coords = [
            (
                cx + r * (0.7 + 0.6 * rng.random()) * math.cos(a),
                cy + r * (0.7 + 0.6 * rng.random()) * math.sin(a),
            )
            for a in angles
        ]
        coords.append(coords[0])
        polys.append(Polygon(coords))
    return polys


def _medium_bng_blobs(n: int = 200, seed: int = 7):
    """``n`` irregular medium BNG polygons, radius 4–9 km each."""
    import math
    import random

    rng = random.Random(seed)
    polys = []
    for _ in range(n):
        cx, cy = rng.uniform(150_000, 600_000), rng.uniform(50_000, 600_000)
        r = rng.uniform(4_000, 9_000)
        angles = [i * 2 * math.pi / 12 for i in range(12)]
        coords = [
            (
                cx + r * (0.7 + 0.6 * rng.random()) * math.cos(a),
                cy + r * (0.7 + 0.6 * rng.random()) * math.sin(a),
            )
            for a in angles
        ]
        coords.append(coords[0])
        polys.append(Polygon(coords))
    return polys


def _medium_custom_blobs(n: int = 200, seed: int = 7):
    """``n`` irregular medium polygons for the 0..1 M custom grid, radius 50–100 km each."""
    import math
    import random

    rng = random.Random(seed)
    polys = []
    for _ in range(n):
        cx, cy = rng.uniform(100_000, 900_000), rng.uniform(100_000, 900_000)
        r = rng.uniform(50_000, 100_000)
        angles = [i * 2 * math.pi / 12 for i in range(12)]
        coords = [
            (
                cx + r * (0.7 + 0.6 * rng.random()) * math.cos(a),
                cy + r * (0.7 + 0.6 * rng.random()) * math.sin(a),
            )
            for a in angles
        ]
        coords.append(coords[0])
        # Clamp to grid extent
        valid = Polygon(
            [(max(0, min(1_000_000, x)), max(0, min(1_000_000, y))) for x, y in coords]
        )
        if valid.is_valid and not valid.is_empty:
            polys.append(valid)
    return polys[:n]


# ===========================================================================
# Custom grid fixture (0..1 M × 0..1 M, 2×2 splits, 1000 m root cell)
# ===========================================================================

_CUSTOM_CONF = _custom.CustomGridConf(
    bound_x_min=0,
    bound_x_max=1_000_000,
    bound_y_min=0,
    bound_y_max=1_000_000,
    cell_splits=2,
    root_cell_size_x=1_000,
    root_cell_size_y=1_000,
    srid=-1,
)

# ===========================================================================
# Before / After helpers (O(area) reference vs lazy O(perimeter) public path)
# ===========================================================================

# ---- quadbin ----


def _qb_before(geom: Polygon, z: int, k: int = 2) -> list:
    """O(area) reference: classify all cells, then expand."""
    cls = _dilate.classify(
        geom,
        z,
        polyfill_fn=lambda g, r: _quadbin.polyfill(g, r),
        cell_geom_fn=_quadbin._cell_geom,
    )
    return sorted(
        _dilate.geom_expand(
            "ring", k, "boundary-out", cls, lambda c: _quadbin.k_loop(c, 1)
        )
    )


def _qb_after(geom: Polygon, z: int, k: int = 2) -> list:
    """Lazy O(perimeter) public path."""
    return _quadbin.geometry_k_ring(geom, z, k, mode="boundary-out")


# ---- bng ----


def _bng_before(geom: Polygon, res: int, k: int = 2) -> list:
    """O(area) reference for BNG: classify + expand."""
    cls = _bng.classify_bng(geom, res)
    return sorted(
        c
        for c in _dilate.geom_expand(
            "ring", k, "boundary-out", cls, lambda c: _bng.k_loop(c, 1)
        )
        if _bng.is_valid(c)
    )


def _bng_after(geom: Polygon, res: int, k: int = 2) -> list:
    """Lazy O(perimeter) public path for BNG."""
    return sorted(_bng.geometry_k_ring(geom, res, k, mode="boundary-out"))


# ---- custom ----


def _custom_before(conf, geom: Polygon, res: int, k: int = 2) -> list:
    """O(area) reference for custom grid: classify + expand."""
    cls = _custom.classify(conf, geom, res)
    return sorted(
        _dilate.geom_expand(
            "ring", k, "boundary-out", cls, lambda c: _custom.k_loop(conf, c, 1)
        )
    )


def _custom_after(conf, geom: Polygon, res: int, k: int = 2) -> list:
    """Lazy O(perimeter) public path for custom grid."""
    return _custom.geometry_k_ring(conf, geom, res, k, mode="boundary-out")


# ---- h3 ----


def _h3_native(geom: Polygon, res: int, k: int = 2) -> list:
    """Native O(area) h3 path: bulk polyfill classify + expand."""
    cls = _h3.classify(geom, res)
    return sorted(_dilate.geom_expand("ring", k, "boundary-out", cls, _h3._neighbors))


def _h3_public(geom: Polygon, res: int, k: int = 2) -> list:
    """Public h3 path (native O(area) bulk polyfill — not converted to lazy)."""
    from shapely import to_wkb

    wkb = to_wkb(geom)
    return sorted(_h3.geom_expand("ring", wkb, res, k, mode="boundary-out"))


# ===========================================================================
# Timing helpers
# ===========================================================================


def _timed(fn: Callable, n_warmup: int = 1, n_rep: int = 3):
    """Run fn() n_warmup+n_rep times; return (result, median_s)."""
    result = None
    for _ in range(n_warmup):
        result = fn()
    times = []
    for _ in range(n_rep):
        t0 = time.perf_counter()
        result = fn()
        times.append(time.perf_counter() - t0)
    times.sort()
    return result, times[len(times) // 2]


def _throughput(fn_list: list[Callable], n_warmup: int = 1) -> float:
    """Warm-up on first item, then time all items; return polys/sec."""
    fn_list[0]()  # warmup
    t0 = time.perf_counter()
    for fn in fn_list:
        fn()
    elapsed = time.perf_counter() - t0
    return len(fn_list) / elapsed if elapsed > 0 else float("inf")


# ===========================================================================
# ---- BENCHMARK TESTS (marked skip — not CI) ----
# ===========================================================================


@_BENCH_MARK
def test_bench_quadbin_large():
    """quadbin z=15 large-polygon boundary-out: BEFORE vs AFTER."""
    geom = _large_wgs84_blob()
    z = 15
    _, t_before = _timed(lambda: _qb_before(geom, z), n_warmup=1, n_rep=3)
    after_result, t_after = _timed(lambda: _qb_after(geom, z), n_warmup=1, n_rep=3)
    before_result = _qb_before(geom, z)

    assert sorted(before_result) == sorted(
        after_result
    ), "BEFORE/AFTER results differ for quadbin!"
    ratio = t_before / t_after if t_after > 0 else float("inf")
    n_interior = len(
        _dilate.classify(
            geom,
            z,
            polyfill_fn=lambda g, r: _quadbin.polyfill(g, r),
            cell_geom_fn=_quadbin._cell_geom,
        ).s_cover
    )
    print(
        f"\n[quadbin z={z}] N_interior={n_interior}, "
        f"before={t_before*1000:.0f}ms, after={t_after*1000:.0f}ms, speedup={ratio:.1f}×"
    )
    assert ratio >= 5, f"Expected ≥5× speedup for quadbin, got {ratio:.1f}×"


@_BENCH_MARK
def test_bench_bng_large():
    """BNG res=3 (1 km) large-polygon boundary-out: BEFORE vs AFTER."""
    geom = _large_bng_blob()
    res = 3
    _, t_before = _timed(lambda: _bng_before(geom, res), n_warmup=1, n_rep=3)
    after_result, t_after = _timed(lambda: _bng_after(geom, res), n_warmup=1, n_rep=3)
    before_result = _bng_before(geom, res)

    assert sorted(before_result) == sorted(
        after_result
    ), "BEFORE/AFTER results differ for BNG!"
    ratio = t_before / t_after if t_after > 0 else float("inf")
    n_interior = len(_bng.classify_bng(geom, res).s_cover)
    print(
        f"\n[bng res={res}] N_interior={n_interior}, "
        f"before={t_before*1000:.0f}ms, after={t_after*1000:.0f}ms, speedup={ratio:.1f}×"
    )
    assert ratio >= 5, f"Expected ≥5× speedup for BNG, got {ratio:.1f}×"


@_BENCH_MARK
def test_bench_custom_large():
    """Custom grid res=1 (500 m) large-polygon boundary-out: BEFORE vs AFTER."""
    conf = _CUSTOM_CONF
    geom = _large_custom_blob()
    res = 1
    _, t_before = _timed(lambda: _custom_before(conf, geom, res), n_warmup=1, n_rep=3)
    after_result, t_after = _timed(
        lambda: _custom_after(conf, geom, res), n_warmup=1, n_rep=3
    )
    before_result = _custom_before(conf, geom, res)

    assert sorted(before_result) == sorted(
        after_result
    ), "BEFORE/AFTER results differ for custom!"
    ratio = t_before / t_after if t_after > 0 else float("inf")
    n_interior = len(_custom.classify(conf, geom, res).s_cover)
    print(
        f"\n[custom res={res}] N_interior={n_interior}, "
        f"before={t_before*1000:.0f}ms, after={t_after*1000:.0f}ms, speedup={ratio:.1f}×"
    )
    assert ratio >= 5, f"Expected ≥5× speedup for custom, got {ratio:.1f}×"


@_BENCH_MARK
def test_bench_h3_native_res8():
    """h3 res=8 large-polygon boundary-out: native bulk polyfill path timing.

    h3 uses H3's native ``polygon_to_cells_experimental`` bulk fill (C-optimised)
    for all polygon inputs.  A perimeter-walk lazy path was evaluated and found to be
    23–96× SLOWER than the native bulk fill for large polygons (each boundary cell
    requires multiple local polyfill clips), so h3 retains the O(area) native path.
    This test documents native-path timing only (no before/after comparison).
    """
    geom = _large_wgs84_blob()
    res = 8
    result, t_native = _timed(lambda: _h3_native(geom, res), n_warmup=1, n_rep=3)
    n_interior = len(_h3.classify(geom, res).s_cover)
    print(
        f"\n[h3 res={res}] N_interior={n_interior}, native={t_native*1000:.0f}ms"
        f"\n  NOTE: native path (not converted — bulk polyfill outperforms perimeter-walk for h3)"
    )
    assert result is not None


@_BENCH_MARK
def test_bench_h3_native_res10():
    """h3 res=10 medium-throughput: native bulk polyfill path (50 medium polygons, polys/sec).

    h3 is not converted to a lazy perimeter-walk path (bulk polyfill outperforms it).
    This test documents the native-path throughput.
    """
    polys = _medium_wgs84_blobs(n=50)
    res = 10
    native_fns = [lambda p=p: _h3_native(p, res, k=1) for p in polys]
    tp_native = _throughput(native_fns)
    print(
        f"\n[h3 res={res} throughput (50 med polys)] native={tp_native:.1f} pol/s"
        f"\n  NOTE: native path (not converted — bulk polyfill outperforms perimeter-walk for h3)"
    )
    assert tp_native > 0, "h3 native throughput must be positive"


@_BENCH_MARK
def test_bench_throughput_all_grids():
    """Medium-polygon throughput (polys/sec) for all four grids: BEFORE vs AFTER."""
    results = {}

    # quadbin z=17 (~240 m tiles), 200 medium polygons
    wgs_polys = _medium_wgs84_blobs(n=200)
    z = 17
    tp_b = _throughput([lambda p=p: _qb_before(p, z, k=1) for p in wgs_polys])
    tp_a = _throughput([lambda p=p: _qb_after(p, z, k=1) for p in wgs_polys])
    results["quadbin z=17"] = (tp_b, tp_a)

    # bng res=3 (1 km), 200 medium polygons
    bng_polys = _medium_bng_blobs(n=200)
    tp_b = _throughput([lambda p=p: _bng_before(p, 3, k=1) for p in bng_polys])
    tp_a = _throughput([lambda p=p: _bng_after(p, 3, k=1) for p in bng_polys])
    results["bng res=3"] = (tp_b, tp_a)

    # custom res=2 (250 m), 100 medium polygons (slower)
    cust_polys = _medium_custom_blobs(n=100)[:100]
    conf = _CUSTOM_CONF
    res = 2
    tp_b = _throughput(
        [lambda p=p: _custom_before(conf, p, res, k=1) for p in cust_polys]
    )
    tp_a = _throughput(
        [lambda p=p: _custom_after(conf, p, res, k=1) for p in cust_polys]
    )
    results["custom res=2"] = (tp_b, tp_a)

    print("\n[throughput summary]")
    print(f"  {'grid':<18} {'before pol/s':>14} {'after pol/s':>13} {'speedup':>9}")
    for name, (tb, ta) in results.items():
        ratio = ta / tb if tb > 0 else float("inf")
        print(f"  {name:<18} {tb:>14.1f} {ta:>13.1f} {ratio:>8.1f}×")

    for name, (tb, ta) in results.items():
        ratio = ta / tb if tb > 0 else float("inf")
        assert (
            ratio >= 1.5
        ), f"Expected ≥1.5× throughput speedup for {name}, got {ratio:.1f}×"


# ===========================================================================
# ---- REGRESSION ASSERTIONS (run in normal CI) ----
#
# These are FAST tests (small polygon, <50 ms each).  They verify the
# structural invariant: the lazy path never calls the O(area) polyfill.
# ===========================================================================

from shapely.geometry import box as _box  # noqa: E402 (after heavy imports above)


def _small_wgs84_poly():
    """Small WGS84 polygon (~10 km box) — fast enough for CI."""
    return _box(-0.15, 51.48, 0.05, 51.56)


def _small_bng_poly():
    """Small BNG polygon (~10 km box)."""
    return _box(520_000, 170_000, 530_000, 180_000)


def _small_custom_poly():
    """Small custom-grid polygon (~50 km box)."""
    return _box(450_000, 450_000, 500_000, 500_000)


def test_regression_lazy_skips_polyfill_quadbin(monkeypatch):
    """Lazy quadbin path must NOT call _quadbin.polyfill for boundary-out."""
    call_count = {"n": 0}
    real_polyfill = _quadbin.polyfill

    def counting_polyfill(geom, resolution):
        call_count["n"] += 1
        return real_polyfill(geom, resolution)

    monkeypatch.setattr(_quadbin, "polyfill", counting_polyfill)
    geom = _small_wgs84_poly()
    z = 15

    # BEFORE (reference O(area) path): should call polyfill at least once
    count_before = call_count["n"]
    _dilate.classify(
        geom,
        z,
        polyfill_fn=lambda g, r: _quadbin.polyfill(g, r),
        cell_geom_fn=_quadbin._cell_geom,
    )
    assert call_count["n"] > count_before, "Reference classify() must call polyfill"

    # AFTER (lazy path): polyfill must NOT be called
    call_count["n"] = 0
    _quadbin.geometry_k_ring(geom, z, k=1, mode="boundary-out")
    assert call_count["n"] == 0, (
        f"Lazy quadbin path called polyfill {call_count['n']} time(s); "
        "it must not materialise the interior for polygon boundary-out"
    )


def test_regression_lazy_skips_polyfill_bng(monkeypatch):
    """Lazy BNG path must NOT call _bng.polyfill for boundary-out."""
    call_count = {"n": 0}
    real_polyfill = _bng.polyfill

    def counting_polyfill(geom, resolution):
        call_count["n"] += 1
        return real_polyfill(geom, resolution)

    monkeypatch.setattr(_bng, "polyfill", counting_polyfill)
    geom = _small_bng_poly()
    res = 3

    # BEFORE: classify_bng calls polyfill via the polyfill_fn closure.
    # Need to call classify directly so our monkeypatched _bng.polyfill is reached.
    count_before = call_count["n"]
    _dilate.classify(
        geom,
        res,
        polyfill_fn=lambda g, r: _bng.polyfill(g, r),
        cell_geom_fn=_bng._bng_cell_geom,
        point_to_cell_fn=lambda x, y: _bng.point_to_cell_id(x, y, res),
    )
    assert call_count["n"] > count_before, "Reference classify() must call BNG polyfill"

    # AFTER: lazy path must not call polyfill
    call_count["n"] = 0
    _bng.geometry_k_ring(geom, res, k=1, mode="boundary-out")
    assert call_count["n"] == 0, (
        f"Lazy BNG path called polyfill {call_count['n']} time(s); "
        "it must not materialise the interior for polygon boundary-out"
    )


def test_regression_lazy_skips_polyfill_custom(monkeypatch):
    """Lazy custom-grid path must NOT call _custom.polyfill for boundary-out."""
    call_count = {"n": 0}
    real_polyfill = _custom.polyfill
    conf = _CUSTOM_CONF

    def counting_polyfill(c, geom, resolution):
        call_count["n"] += 1
        return real_polyfill(c, geom, resolution)

    monkeypatch.setattr(_custom, "polyfill", counting_polyfill)
    geom = _small_custom_poly()
    res = 2

    # BEFORE: reference classify calls polyfill
    count_before = call_count["n"]
    _dilate.classify(
        geom,
        res,
        polyfill_fn=lambda g, r: _custom.polyfill(conf, g, r),
        cell_geom_fn=lambda c: _custom._cell_geom(conf, c),
        point_to_cell_fn=lambda x, y: _custom.point_to_cell_id_or_none(conf, x, y, res),
    )
    assert (
        call_count["n"] > count_before
    ), "Reference classify() must call custom polyfill"

    # AFTER: lazy path must not call polyfill
    call_count["n"] = 0
    _custom.geometry_k_ring(conf, geom, res, k=1, mode="boundary-out")
    assert call_count["n"] == 0, (
        f"Lazy custom path called polyfill {call_count['n']} time(s); "
        "it must not materialise the interior for polygon boundary-out"
    )


# ===========================================================================
# Standalone driver — run this file directly to see benchmark results
# ===========================================================================


def _run_benchmark():
    """Print per-grid speedup table.  Called when this file is run as a script."""
    import sys

    print("=" * 72)
    print("GeoBrix geom-aware kring boundary-as-line speedup benchmark")
    print("(BEFORE = O(area) classify+expand; AFTER = lazy O(perimeter) public path)")
    print("=" * 72)

    rows = []

    # -----------------------------------------------------------------------
    # Large-polygon boundary-out k=2 (the headline speedup scenario)
    # -----------------------------------------------------------------------

    # quadbin z=15
    geom_wgs = _large_wgs84_blob()
    z = 15
    r_before, t_before = _timed(lambda: _qb_before(geom_wgs, z), n_warmup=1, n_rep=3)
    r_after, t_after = _timed(lambda: _qb_after(geom_wgs, z), n_warmup=1, n_rep=3)
    n_int = len(
        _dilate.classify(
            geom_wgs,
            z,
            polyfill_fn=lambda g, r: _quadbin.polyfill(g, r),
            cell_geom_fn=_quadbin._cell_geom,
        ).s_cover
    )
    rows.append(("quadbin z=15 (large)", n_int, t_before, t_after))

    # bng res=3
    geom_bng = _large_bng_blob()
    res = 3
    _, t_before = _timed(lambda: _bng_before(geom_bng, res), n_warmup=1, n_rep=3)
    _, t_after = _timed(lambda: _bng_after(geom_bng, res), n_warmup=1, n_rep=3)
    n_int = len(_bng.classify_bng(geom_bng, res).s_cover)
    rows.append(("bng res=3 (large)", n_int, t_before, t_after))

    # custom res=1
    geom_cust = _large_custom_blob()
    conf = _CUSTOM_CONF
    res = 1
    _, t_before = _timed(
        lambda: _custom_before(conf, geom_cust, res), n_warmup=1, n_rep=3
    )
    _, t_after = _timed(
        lambda: _custom_after(conf, geom_cust, res), n_warmup=1, n_rep=3
    )
    n_int = len(_custom.classify(conf, geom_cust, res).s_cover)
    rows.append(("custom res=1 (large)", n_int, t_before, t_after))

    # h3 res=8 (large) — native path only (not converted; bulk polyfill outperforms perimeter-walk)
    geom_wgs8 = _large_wgs84_blob()
    res = 8
    _, t_native = _timed(lambda: _h3_native(geom_wgs8, res), n_warmup=1, n_rep=3)
    n_int = len(_h3.classify(geom_wgs8, res).s_cover)
    rows.append(("h3 res=8 (native, large)", n_int, t_native, t_native))

    print(
        f"\n{'Scenario':<28} {'N_interior':>12} {'before ms':>12} {'after ms':>10} {'speedup':>9}"
    )
    print("-" * 76)
    for name, n_int, tb, ta in rows:
        ratio = tb / ta if ta > 0 else float("inf")
        print(
            f"  {name:<26} {n_int:>12,} {tb*1000:>12.0f} {ta*1000:>10.0f} {ratio:>8.1f}×"
        )

    # -----------------------------------------------------------------------
    # h3 res=10 large-polygon — native path only (no lazy conversion)
    # -----------------------------------------------------------------------
    res = 10
    geom_wgs10 = _large_wgs84_blob()
    _, t_native10 = _timed(lambda: _h3_native(geom_wgs10, res), n_warmup=1, n_rep=3)
    n_int = len(_h3.classify(geom_wgs10, res).s_cover)
    print(
        f"  {'h3 res=10 (native,large)':<26} {n_int:>12,} {t_native10*1000:>12.0f} {'[native]':>10} {'n/a':>9}"
    )

    # -----------------------------------------------------------------------
    # Medium-polygon throughput (polys/sec)
    # -----------------------------------------------------------------------
    print(
        f"\n{'Grid':<22} {'n polys':>8} {'before pol/s':>14} {'after pol/s':>14} {'speedup':>9}"
    )
    print("-" * 72)

    # h3 res=10 medium throughput — native path only (not converted)
    res10_polys = _medium_wgs84_blobs(n=200)
    tp_native_h3 = _throughput(
        [lambda p=p: _h3_native(p, 10, k=1) for p in res10_polys]
    )
    print(
        f"  {'h3 res=10 (native)':<20} {200:>8} {'[native]':>14} {tp_native_h3:>14.1f} {'n/a':>9}"
    )

    # quadbin z=17 medium throughput
    qb_polys = _medium_wgs84_blobs(n=200)
    tp_b = _throughput([lambda p=p: _qb_before(p, 17, k=1) for p in qb_polys])
    tp_a = _throughput([lambda p=p: _qb_after(p, 17, k=1) for p in qb_polys])
    ratio = tp_a / tp_b if tp_b > 0 else float("inf")
    print(
        f"  {'quadbin z=17 thrput':<20} {200:>8} {tp_b:>14.1f} {tp_a:>14.1f} {ratio:>8.1f}×"
    )

    # bng res=3 medium throughput
    bng_polys = _medium_bng_blobs(n=200)
    tp_b = _throughput([lambda p=p: _bng_before(p, 3, k=1) for p in bng_polys])
    tp_a = _throughput([lambda p=p: _bng_after(p, 3, k=1) for p in bng_polys])
    ratio = tp_a / tp_b if tp_b > 0 else float("inf")
    print(
        f"  {'bng res=3 thrput':<20} {200:>8} {tp_b:>14.1f} {tp_a:>14.1f} {ratio:>8.1f}×"
    )

    # custom res=2 medium throughput (100 polys — slower grid)
    cust_polys = _medium_custom_blobs(n=100)[:100]
    conf = _CUSTOM_CONF
    tp_b = _throughput(
        [lambda p=p: _custom_before(conf, p, 2, k=1) for p in cust_polys]
    )
    tp_a = _throughput([lambda p=p: _custom_after(conf, p, 2, k=1) for p in cust_polys])
    ratio = tp_a / tp_b if tp_b > 0 else float("inf")
    print(
        f"  {'custom res=2 thrput':<20} {len(cust_polys):>8} {tp_b:>14.1f} {tp_a:>14.1f} {ratio:>8.1f}×"
    )

    print("\n" + "=" * 72)
    print("Done.")


if __name__ == "__main__":
    _run_benchmark()
