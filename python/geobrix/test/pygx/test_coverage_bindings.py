"""Coverage-param binding tests (0.5.1 re-cut).

Part A: on a holed polygon (where the three bases are genuinely distinct), explicit
coverage="core" changes the hole-in seeds vs the default "coveras" on every grid.
(That each public geomkring / geomkloop accepts the `coverage` argument is rows of the
grid-array oracle table.)

Part B: hole-out disjoint-loop invariant (visited0 fix, _dilate.py change).
For hole-out (and hole-out-ignore-geom) on a holed polygon,
``geomkloop(k=1)`` must be DISJOINT from ``geomkloop(k=0)`` (the seed).
"""

# isort: skip_file  — imports below are intentionally ordered around the
# pytest.importorskip("quadbin") guard; let isort leave this file alone.

import pytest
from shapely import to_wkb
from shapely.geometry.polygon import Polygon

pytest.importorskip("quadbin")

from databricks.labs.gbx.pygx import _bng, _custom  # noqa: E402
from databricks.labs.gbx.pygx import _h3 as _h3mod  # noqa: E402
from databricks.labs.gbx.pygx import _quadbin  # noqa: E402
from databricks.labs.gbx.pygx._custom import CustomGridConf  # noqa: E402

# ---------------------------------------------------------------------------
# Shared holed polygon fixtures
# ---------------------------------------------------------------------------

# Quadbin — WGS84 box with interior hole; res chosen so ~6x9 cells span hole.
_HOLED_OUTER_QB = [(-76.0, 38.0), (-72.0, 38.0), (-72.0, 43.0), (-76.0, 43.0)]
_HOLED_HOLE_QB = [(-75.0, 39.0), (-73.0, 39.0), (-73.0, 42.0), (-75.0, 42.0)]
_HOLED_POLY_QB = Polygon(_HOLED_OUTER_QB, [_HOLED_HOLE_QB])
_HOLED_WKB_QB = bytes(to_wkb(_HOLED_POLY_QB))
_RES_QB = 10  # quadbin resolution (h3 uses _H3_COARSE_RES_DONUT for hole tests)

# H3 donut for hole-out disjoint-loop and coverage-difference tests.
# Outer 8°×8°, hole 4°×4° centred around NYC; at res-5 (edge ~61 km ≈ 0.55°)
# the 4°×4° hole spans ~7 cells wide × ~7 cells tall — h_core is non-empty and
# h_cover ≠ h_core (hexagonal cells don't align with degree boundaries).
_H3_OUTER_DONUT = Polygon([(-78, 36), (-70, 36), (-70, 44), (-78, 44), (-78, 36)])
_H3_HOLE_DONUT = Polygon([(-76, 38), (-72, 38), (-72, 42), (-76, 42), (-76, 38)])
_H3_DONUT = _H3_OUTER_DONUT.difference(_H3_HOLE_DONUT)
_H3_DONUT_WKB = bytes(to_wkb(_H3_DONUT))
_H3_COARSE_RES_DONUT = 5  # edge ~61 km; hole diameter ~7 cells → h_core non-empty

# BNG — EPSG:27700 box with interior hole; res=3 → 1 km cells
_HOLED_OUTER_BNG = [
    (530000, 180000),
    (540000, 180000),
    (540000, 190000),
    (530000, 190000),
]
_HOLED_HOLE_BNG = [
    (533000, 183000),
    (537000, 183000),
    (537000, 187000),
    (533000, 187000),
]
_HOLED_POLY_BNG = Polygon(_HOLED_OUTER_BNG, [_HOLED_HOLE_BNG])
_HOLED_WKB_BNG = bytes(to_wkb(_HOLED_POLY_BNG))
_RES_BNG = 3  # 1 km

# Custom — same as the coverage_param grid tests
_CUSTOM_CONF = CustomGridConf(
    bound_x_min=0,
    bound_x_max=1_000_000,
    bound_y_min=0,
    bound_y_max=1_000_000,
    cell_splits=2,
    root_cell_size_x=1000,
    root_cell_size_y=1000,
    srid=-1,
)
_HOLED_OUTER_CU = [
    (530000, 180000),
    (540000, 180000),
    (540000, 190000),
    (530000, 190000),
]
_HOLED_HOLE_CU = [
    (533000, 183000),
    (537000, 183000),
    (537000, 187000),
    (533000, 187000),
]
_HOLED_POLY_CU = Polygon(_HOLED_OUTER_CU, [_HOLED_HOLE_CU])
_HOLED_WKB_CU = bytes(to_wkb(_HOLED_POLY_CU))
_RES_CU = 0  # coarsest custom resolution

# ---------------------------------------------------------------------------
# Part B: hole-out / hole-out-ignore-geom disjoint-loop invariant
# (engine-level; does not require Spark)
# ---------------------------------------------------------------------------


def _neighbors_quadbin(c):
    """8-neighbor quadbin ring via k_loop."""
    return _quadbin.k_loop(c, 1)


def _neighbors_custom(c):
    return _custom.k_loop(_CUSTOM_CONF, c, 1)


@pytest.mark.parametrize("mode", ["hole-out", "hole-out-ignore-geom"])
def test_hole_out_loop_k1_disjoint_from_k0_quadbin(mode):
    """geomkloop(k=1) must be disjoint from geomkloop(k=0) for hole-out modes (quadbin)."""
    k0 = set(_quadbin.geometry_k_loop(_HOLED_WKB_QB, _RES_QB, 0, mode=mode))
    k1 = set(_quadbin.geometry_k_loop(_HOLED_WKB_QB, _RES_QB, 1, mode=mode))
    if not k0:
        pytest.skip(
            f"quadbin hole-out k0 empty at res {_RES_QB} — no hole cells to test"
        )
    assert k0.isdisjoint(k1), (
        f"quadbin {mode}: geomkloop(k=1) re-emits seed cells "
        f"(overlap={sorted(k0 & k1)[:3]}). visited0 fix not applied."
    )


@pytest.mark.parametrize("mode", ["hole-out", "hole-out-ignore-geom"])
def test_hole_out_loop_k1_disjoint_from_k0_custom(mode):
    """geomkloop(k=1) must be disjoint from geomkloop(k=0) for hole-out modes (custom)."""
    k0 = set(
        _custom.geometry_k_loop(_CUSTOM_CONF, _HOLED_WKB_CU, _RES_CU, 0, mode=mode)
    )
    k1 = set(
        _custom.geometry_k_loop(_CUSTOM_CONF, _HOLED_WKB_CU, _RES_CU, 1, mode=mode)
    )
    if not k0:
        pytest.skip(
            f"custom hole-out k0 empty at res {_RES_CU} — no hole cells to test"
        )
    assert k0.isdisjoint(k1), (
        f"custom {mode}: geomkloop(k=1) re-emits seed cells "
        f"(overlap={sorted(k0 & k1)[:3]}). visited0 fix not applied."
    )


@pytest.mark.parametrize("mode", ["hole-out", "hole-out-ignore-geom"])
def test_hole_out_loop_k1_disjoint_from_k0_bng(mode):
    """geomkloop(k=1) must be disjoint from geomkloop(k=0) for hole-out modes (BNG)."""
    k0 = set(_bng.geometry_k_loop(_HOLED_POLY_BNG, _RES_BNG, 0, mode=mode))
    k1 = set(_bng.geometry_k_loop(_HOLED_POLY_BNG, _RES_BNG, 1, mode=mode))
    if not k0:
        pytest.skip(f"bng hole-out k0 empty at res {_RES_BNG} — no hole cells to test")
    assert k0.isdisjoint(k1), (
        f"bng {mode}: geomkloop(k=1) re-emits seed cells "
        f"(overlap={sorted(k0 & k1)[:3]}). visited0 fix not applied."
    )


@pytest.mark.parametrize("mode", ["hole-out", "hole-out-ignore-geom"])
def test_hole_out_loop_k1_disjoint_from_k0_h3(mode):
    """geomkloop(k=1) must be disjoint from geomkloop(k=0) for hole-out modes (H3).

    Uses the 8°×8° / 4°×4° donut at res-5 (edge ~61 km); visited0 fix applies
    to H3 via the shared _dilate engine — this is the regression guard.
    """
    h3 = pytest.importorskip("h3")  # noqa: F841
    k0 = _h3mod.geom_expand("loop", _H3_DONUT_WKB, _H3_COARSE_RES_DONUT, 0, mode)
    k1 = _h3mod.geom_expand("loop", _H3_DONUT_WKB, _H3_COARSE_RES_DONUT, 1, mode)
    if not k0:
        pytest.skip(
            f"h3 hole-out k0 empty at res {_H3_COARSE_RES_DONUT} — no hole cells to test"
        )
    assert k0.isdisjoint(k1), (
        f"h3 {mode}: geomkloop(k=1) re-emits seed cells "
        f"(overlap={sorted(k0 & k1)[:3]}). visited0 fix not applied."
    )


# ---------------------------------------------------------------------------
# Part A: coverage param reaches engine through UDF impls
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Coverage-difference asserts (engine-level): coveras vs core produce distinct
# hole-in seeds on a holed polygon where the three bases are strictly nested.
# Uses non-grid-aligned fixtures for BNG and custom (500m offset from 1 km /
# 1 000-unit cell boundaries) so that h_cover != h_core.
# ---------------------------------------------------------------------------


def test_quadbin_geomkring_coveras_vs_core_differ():
    """coveras and core produce distinct hole-in seeds on the quadbin holed polygon.

    Quadbin cells are not aligned with degree boundaries → h_cover != h_core →
    hole-in k=0 (= outer_perimeter(h_{basis})) differs between the two coverages.
    """
    k0_coveras = set(
        _quadbin.geometry_k_ring(
            _HOLED_WKB_QB, _RES_QB, 0, mode="hole-in", coverage="coveras"
        )
    )
    k0_core = set(
        _quadbin.geometry_k_ring(
            _HOLED_WKB_QB, _RES_QB, 0, mode="hole-in", coverage="core"
        )
    )
    assert (
        k0_coveras
    ), "quadbin coveras hole-in k=0 must be non-empty on a holed polygon"
    assert (
        not k0_core
    ), "core hole-in k=0 is empty — a straddling rim cell is never fully-in-hole (core fills at k>=1)"
    assert k0_coveras != k0_core, (
        "quadbin coveras and core must produce distinct hole-in seeds "
        "(h_cover != h_core for non-grid-aligned hole)"
    )


def test_bng_geomkring_coveras_vs_core_differ():
    """coveras and core produce distinct hole-in seeds for a non-grid-aligned BNG holed polygon.

    Outer (530 500,180 500)→(539 500,189 500) with hole (533 500,183 500)→(536 500,186 500)
    at res=3 (1 km cells).  The 500 m inset from the 1 km cell grid ensures cells
    straddle both the outer and hole boundaries → h_cover ≠ h_core.
    """
    outer = [(530500, 180500), (539500, 180500), (539500, 189500), (530500, 189500)]
    hole = [(533500, 183500), (536500, 183500), (536500, 186500), (533500, 186500)]
    poly = Polygon(outer, [hole])
    wkb = bytes(to_wkb(poly))
    res = 3  # 1 km cells
    k0_coveras = set(
        _bng.geometry_k_ring_str(wkb, res, 0, mode="hole-in", coverage="coveras")
    )
    k0_core = set(
        _bng.geometry_k_ring_str(wkb, res, 0, mode="hole-in", coverage="core")
    )
    assert k0_coveras, "bng coveras hole-in k=0 must be non-empty"
    assert (
        not k0_core
    ), "core hole-in k=0 is empty — a straddling rim cell is never fully-in-hole (core fills at k>=1)"
    assert k0_coveras != k0_core, (
        "bng coveras and core must produce distinct hole-in seeds "
        "for a non-grid-aligned holed polygon"
    )


def test_custom_geomkring_coveras_vs_core_differ():
    """coveras and core produce distinct hole-in seeds for a non-grid-aligned custom holed polygon.

    Outer (530 500,180 500)→(539 500,189 500) with hole (533 500,183 500)→(536 500,186 500)
    at res=0 (1 000-unit cells).  The 500-unit inset ensures cells straddle boundaries
    → h_cover ≠ h_core.
    """
    outer = [(530500, 180500), (539500, 180500), (539500, 189500), (530500, 189500)]
    hole = [(533500, 183500), (536500, 183500), (536500, 186500), (533500, 186500)]
    poly = Polygon(outer, [hole])
    wkb = bytes(to_wkb(poly))
    k0_coveras = set(
        _custom.geometry_k_ring(
            _CUSTOM_CONF, wkb, _RES_CU, 0, mode="hole-in", coverage="coveras"
        )
    )
    k0_core = set(
        _custom.geometry_k_ring(
            _CUSTOM_CONF, wkb, _RES_CU, 0, mode="hole-in", coverage="core"
        )
    )
    assert k0_coveras, "custom coveras hole-in k=0 must be non-empty"
    assert (
        not k0_core
    ), "core hole-in k=0 is empty — a straddling rim cell is never fully-in-hole (core fills at k>=1)"
    assert k0_coveras != k0_core, (
        "custom coveras and core must produce distinct hole-in seeds "
        "for a non-grid-aligned holed polygon"
    )


def test_h3_geomkring_coveras_vs_core_differ():
    """coveras and core produce distinct hole-in seeds on the H3 donut polygon (engine-level).

    H3 hexagonal cells never align with degree boundaries → h_cover != h_core for the
    4°×4° hole at res-5 → hole-in k=0 (= outer_perimeter(h_{basis})) differs.
    """
    h3 = pytest.importorskip("h3")  # noqa: F841
    k0_coveras = _h3mod.geom_expand(
        "ring", _H3_DONUT_WKB, _H3_COARSE_RES_DONUT, 0, "hole-in", "coveras"
    )
    k0_core = _h3mod.geom_expand(
        "ring", _H3_DONUT_WKB, _H3_COARSE_RES_DONUT, 0, "hole-in", "core"
    )
    assert k0_coveras, "h3 coveras hole-in k=0 must be non-empty on the donut polygon"
    assert (
        not k0_core
    ), "core hole-in k=0 is empty — a straddling rim cell is never fully-in-hole (core fills at k>=1)"
    assert k0_coveras != k0_core, (
        "h3 coveras and core must produce distinct hole-in seeds "
        "(hexagonal cells don't align with degree boundaries → h_cover != h_core)"
    )
