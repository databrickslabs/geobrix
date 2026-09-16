"""Unit tests for pygx quadbin geometry-aware kring/kloop.

Tests mirror the brief Step 1 fixture exactly, plus a fixture-correctness gate
for the holed polygon used in parity tests (verifying hCore is non-empty).
"""

import pytest
from shapely import to_wkb
from shapely.geometry import box
from shapely.geometry.polygon import Polygon

from databricks.labs.gbx.pygx import _quadbin


def _wkb():
    return to_wkb(box(-73.99, 40.71, -73.95, 40.75))  # NYC lon/lat


# Holed fixture mirroring the Scala and parity tests:
# east-US box (-76,38)→(-72,43) with 2°×3° interior hole (-75,39)→(-73,42) at res 10.
# At res 10 (cells ≈0.35°) the hole spans ~6×9 cells → hCore non-empty.
_HOLED_OUTER = [(-76.0, 38.0), (-72.0, 38.0), (-72.0, 43.0), (-76.0, 43.0)]
_HOLED_HOLE = [(-75.0, 39.0), (-73.0, 39.0), (-73.0, 42.0), (-75.0, 42.0)]
_HOLED_POLY = Polygon(_HOLED_OUTER, [_HOLED_HOLE])
_HOLED_RES = 10


def test_quadbin_geomkring_boundary_out_k0_is_boundary_ring():
    """boundary-out k=0 == the boundary covering ring (== boundary-in k0)."""
    g, res = _wkb(), 12  # default mode = boundary-out
    k0_out = set(_quadbin.geometry_k_ring(g, res, 0))
    k0_in = set(_quadbin.geometry_k_ring(g, res, 0, mode="boundary-in"))
    assert k0_out == k0_in, "boundary-out k0 must equal boundary-in k0 (boundary ring)"
    assert k0_out, "boundary-out k0 (boundary ring) must be non-empty"


def test_quadbin_geomkring_boundary_out_excludes_interior():
    """boundary-out k=1 = boundary ring (k0) + outward band; INTERIOR excluded."""
    g, res = _wkb(), 12
    fill = set(_quadbin.polyfill(g, res))
    k0 = set(_quadbin.geometry_k_ring(g, res, 0))
    k1 = set(_quadbin.geometry_k_ring(g, res, 1))
    assert k1, "boundary-out k=1 must be non-empty"
    # any covering-set cell in the result is on the boundary ring (interior excluded)
    assert (k1 & fill) <= k0, "boundary-out excludes the covering-set interior"
    assert k1 - fill, "boundary-out k=1 must add an outward band beyond the cover"


def test_quadbin_geomkloop_is_ring_diff():
    g, res = _wkb(), 12
    r2 = set(_quadbin.geometry_k_ring(g, res, 2))
    r1 = set(_quadbin.geometry_k_ring(g, res, 1))
    assert set(_quadbin.geometry_k_loop(g, res, 2)) == (r2 - r1)


def test_quadbin_geomkring_returns_bigint():
    assert all(isinstance(c, int) for c in _quadbin.geometry_k_ring(_wkb(), 12, 1))


def test_quadbin_geomkring_k0_boundary_out_equals_boundary_in():
    """boundary-out k=0 == boundary-in k=0 (both = the boundary covering ring)."""
    g, res = _wkb(), 12
    k0_out = set(_quadbin.geometry_k_ring(g, res, 0, mode="boundary-out"))
    k0_in = set(_quadbin.geometry_k_ring(g, res, 0, mode="boundary-in"))
    assert k0_out == k0_in and k0_out, "boundary-out k0 = boundary-in k0 (non-empty)"


def test_quadbin_geomkring_k0_boundary_in_is_outer_perimeter():
    """boundary-in/ignore-holes k=0 == outer_perimeter(s_cover) under the perimeter fix.

    Layer-2 fix: boundary-in and boundary-in-ignore-holes seed from the covering-set
    perimeter (outer ring of s_cover), not the full polyfill.  This makes them
    alignment-robust: a grid-aligned polygon has zero straddling cells but always has
    a non-empty outer perimeter.
    """
    g, res = _wkb(), 12
    fill = set(_quadbin.polyfill(g, res))
    for mode in ("boundary-in", "boundary-in-ignore-holes"):
        k0 = set(_quadbin.geometry_k_ring(g, res, 0, mode=mode))
        # k0 is the outer perimeter — a non-empty subset of the covering set.
        assert k0, f"mode={mode}: k0 must be non-empty"
        assert k0 <= fill, f"mode={mode}: k0 must be a subset of polyfill"


def test_quadbin_geomkloop_boundary_out_k0_is_boundary_ring():
    """boundary-out k=0 loop == the boundary ring (== ring k0; == boundary-in loop k0)."""
    g, res = _wkb(), 12
    loop0 = set(_quadbin.geometry_k_loop(g, res, 0))
    ring0 = set(_quadbin.geometry_k_ring(g, res, 0))
    assert loop0 == ring0 and loop0, "boundary-out loop k0 = ring k0 (boundary ring)"


def test_quadbin_geomkring_all_modes_return_bigints():
    """All 6 modes return int (bigint) cell ids."""
    from databricks.labs.gbx.pygx._dilate import MODES

    g, res = _wkb(), 12
    for mode in MODES:
        cells = _quadbin.geometry_k_ring(g, res, 1, mode=mode)
        assert all(isinstance(c, int) for c in cells), f"mode={mode}: non-int cell"


def test_quadbin_geomkring_invalid_mode_raises():
    """Unknown mode raises ValueError."""
    with pytest.raises(ValueError, match="unknown mode"):
        _quadbin.geometry_k_ring(_wkb(), 12, 1, mode="BOGUS")


def test_holed_fixture_h_core_nonempty():
    """Fixture correctness gate: large hole at res 10 must populate hCore.

    The 2°×3° hole (-75,39)→(-73,42) at res 10 spans ~6×9 cells; cells
    well inside the hole are fully contained by the hole polygon → hCore
    must be non-empty so hole-in/hole-out exercise genuine inward fill.
    """
    from shapely.geometry import shape

    cls = _quadbin.classify(shape(_HOLED_POLY), _HOLED_RES)
    assert len(cls.h_core) > 0, (
        f"hCore is empty — hole is too small for res {_HOLED_RES}; "
        f"hCover={len(cls.h_cover)}, hBorder={len(cls.h_border)}"
    )


def test_hole_in_mode_reaches_hcore():
    """hole-in k=3 expansion reaches hCore cells and stays within hCover."""
    from shapely.geometry import shape

    cls = _quadbin.classify(shape(_HOLED_POLY), _HOLED_RES)
    expanded = set(
        _quadbin.geometry_k_ring(to_wkb(_HOLED_POLY), _HOLED_RES, 3, mode="hole-in")
    )
    # All cells must be within the hole region.
    assert expanded <= cls.h_cover, "hole-in result contains cells outside hCover"
    # After 3 steps inward, must reach some hCore cells.
    assert expanded & cls.h_core, "hole-in k=3 did not reach any hCore cells"
    # Must not contain cells that are only in the solid interior (pCore \ hCover).
    solid_only = cls.p_core - cls.h_cover
    assert not (expanded & solid_only), "hole-in result leaked into solid pCore"


# High-latitude gap fixture: a tall, narrow box spanning 60–80°N at zoom 5.
#
# Why this exercises the latitude-aware step gap:
#   cell_step_lon = 360 / 2^5 = 11.25° (longitude width per tile at zoom 5)
#   At 80°N: tile lat-height ≈ cos(80°) * 11.25° ≈ 0.174 * 11.25° ≈ 1.96°
#
# Pre-fix: the VERTICAL west/east edges (dy = 20°, dx ≈ 0) are sampled with
#   n = max(1, int(20 / 11.25) + 1) = 2  →  3 sample points: 60°N (y=9), 70°N (y=7), 80°N (y=3)
#   (tile indices at zoom 5; computed from the web-mercator projection)
#   After 1-ring dilation C covers {y=10,9}, {y=8,7,6}, {y=4,3,2} — but y=5
#   (approximately 73–76°N, also x=16) is NOT in C:
#     y=6 ∈ C (neighbor of y=7), y=4 ∈ C (neighbor of y=3), but y=5 ∉ C because
#     _local_perimeter expands ONE ring only — y=5 is 2 steps from y=3 and 2 from y=7,
#     and not a neighbor of any band cell.
#   Since y=5 overlaps the polygon it is in s_cover, but the lazy seed misses it.
#
# Post-fix: lat_step = 11.25° * cos(80°) ≈ 1.96°
#   n = max(1, int(20 / 1.96) + 1) = 11  →  12 sample points, ≈1.67° apart.
#   Every tile row (≈2–3° tall at these latitudes) gets at least one sample → y=5 captured.
_QB_HIGH_LAT_GEOM = box(0.0, 60.0, 2.0, 80.0)
_RES_HIGH_LAT = 5


def test_quadbin_high_lat_lazy_matches_oracle():
    """Lazy boundary-out at ~75°N equals the O(area) oracle after the lat-step fix.

    Pre-fix: the longitude-only step (11.25° at zoom 5) samples the 20°-tall
    vertical edge with only 3 points (60°N, 70°N, 80°N); after 1-ring dilation
    the band C misses tile y=5 (~73–76°N) → lazy seed ≠ oracle seed → lazy ≠ oracle.

    Post-fix: lat_step ≈ 1.96° gives 12 points → y=5 is captured → lazy == oracle.
    """
    from databricks.labs.gbx.pygx import _dilate

    geom = _QB_HIGH_LAT_GEOM
    res = _RES_HIGH_LAT
    k = 1

    # Oracle: full O(area) classify + geom_expand (never affected by lat step).
    cls = _quadbin.classify(geom, res)
    oracle = sorted(
        _dilate.geom_expand(
            "ring", k, "boundary-out", cls, lambda c: _quadbin.k_loop(c, 1)
        )
    )

    # Lazy path via the public API (uses geom_expand_lazy with the lat-aware step post-fix).
    lazy = sorted(_quadbin.geometry_k_ring(to_wkb(geom), res, k, "boundary-out"))

    # Verify the oracle is non-trivial (sanity-check the fixture).
    assert len(oracle) > 0, "oracle must be non-empty for this fixture"

    assert lazy == oracle, (
        f"High-lat lazy boundary-out mismatch at zoom {res}, 60–80°N: "
        f"oracle has {len(oracle)} cells, lazy has {len(lazy)} cells; "
        f"cells in oracle only: {sorted(set(oracle) - set(lazy))[:10]}; "
        f"cells in lazy only: {sorted(set(lazy) - set(oracle))[:10]}"
    )
