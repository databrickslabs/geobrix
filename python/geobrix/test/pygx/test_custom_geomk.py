"""Unit tests for pygx custom geometry-aware kring/kloop.

Tests mirror test_quadbin_geomk.py exactly, adapted for the custom grid
conf-leading calling convention. Includes fixture-correctness gate asserting
hCore is non-empty so hole-in/hole-out exercises genuine inward fill.
"""

import pytest

shapely = pytest.importorskip("shapely")  # _custom imports shapely at module load

from shapely import to_wkb  # noqa: E402
from shapely.geometry import box  # noqa: E402
from shapely.geometry.polygon import Polygon  # noqa: E402

from databricks.labs.gbx.pygx import _custom  # noqa: E402
from databricks.labs.gbx.pygx._custom import CustomGridConf  # noqa: E402


def _conf():
    """A 0..1,000,000 grid with 1 000-unit root cells (splits=2).

    Mirrors the doc SQL example grid and the existing test_custom_core.py fixture.
    At resolution 0 each cell is 1 000 × 1 000 units.
    """
    return CustomGridConf(
        bound_x_min=0,
        bound_x_max=1_000_000,
        bound_y_min=0,
        bound_y_max=1_000_000,
        cell_splits=2,
        root_cell_size_x=1000,
        root_cell_size_y=1000,
        srid=-1,
    )


# Simple fixture: a 5 000 × 5 000 box → 25 cells at res 0 (1 000-unit cells).
_SIMPLE_GEOM = box(530000, 180000, 535000, 185000)
_SIMPLE_RES = 0


# Holed fixture: 60 km × 60 km outer box with an interior hole offset 500 units
# from the cell-grid boundaries.  The 500-unit inset ensures cells STRADDLE the
# hole boundary (h_border non-empty) while cells deep inside the hole are FULLY
# contained (h_core non-empty).
#
# Outer:   500000..560000 × 100000..160000 (60×60 cells at res 0)
# Hole:    510500..549500 × 110500..149500 (inset 500 units from the grid lines
#          at x=510 000/550 000, y=110 000/150 000)
#
# At res 0 (1 000-unit cells):
#  - h_border: ~152 cells straddling the hole boundary (positive area intersection
#    but NOT fully contained, e.g. cell (510,110)=[510 000..511 000]^2 overlaps at
#    [510 500..511 000]^2 area 250 000 > 0, but x_min=510 000 < 510 500 → outside)
#  - h_core:   38 × 38 = 1 444 cells for x∈[511..548], y∈[111..148] — fully inside
#    the hole ([511 000..549 000] ⊂ [510 500..549 500])
_OUTER_COORDS = [
    (500000, 100000),
    (560000, 100000),
    (560000, 160000),
    (500000, 160000),
]
_HOLE_COORDS = [
    (510500, 110500),
    (549500, 110500),
    (549500, 149500),
    (510500, 149500),
]
_HOLED_POLY = Polygon(_OUTER_COORDS, [_HOLE_COORDS])
_HOLED_RES = 0


def _wkb(geom) -> bytes:
    return bytes(to_wkb(geom))


# ── basic properties ──────────────────────────────────────────────────────────


def test_custom_geomkring_boundary_out_k0_is_boundary_ring():
    """boundary-out k=0 == the boundary covering ring (== boundary-in k0)."""
    conf = _conf()
    g = _wkb(_SIMPLE_GEOM)
    k0_out = set(_custom.geometry_k_ring(conf, g, _SIMPLE_RES, 0))
    k0_in = set(_custom.geometry_k_ring(conf, g, _SIMPLE_RES, 0, mode="boundary-in"))
    assert (
        k0_out == k0_in and k0_out
    ), "boundary-out k0 = boundary-in k0 (boundary ring)"


def test_custom_geomkring_boundary_out_excludes_interior():
    """boundary-out k=1 = boundary ring (k0) + outward band; INTERIOR excluded."""
    conf = _conf()
    g = _wkb(_SIMPLE_GEOM)
    fill = set(_custom.polyfill(conf, _SIMPLE_GEOM, _SIMPLE_RES))
    k0 = set(_custom.geometry_k_ring(conf, g, _SIMPLE_RES, 0))
    k1 = set(_custom.geometry_k_ring(conf, g, _SIMPLE_RES, 1))
    assert k1, "boundary-out k=1 must be non-empty"
    assert (k1 & fill) <= k0, "boundary-out excludes the covering-set interior"
    assert k1 - fill, "boundary-out k=1 must add an outward band beyond the cover"


def test_custom_geomkloop_is_ring_diff():
    """loop(k) == ring(k) – ring(k–1)."""
    conf = _conf()
    g = _wkb(_SIMPLE_GEOM)
    r2 = set(_custom.geometry_k_ring(conf, g, _SIMPLE_RES, 2))
    r1 = set(_custom.geometry_k_ring(conf, g, _SIMPLE_RES, 1))
    loop2 = set(_custom.geometry_k_loop(conf, g, _SIMPLE_RES, 2))
    assert loop2 == (r2 - r1)


def test_custom_geomkring_returns_bigint():
    """All returned cell ids are int (BIGINT-safe)."""
    conf = _conf()
    g = _wkb(_SIMPLE_GEOM)
    cells = _custom.geometry_k_ring(conf, g, _SIMPLE_RES, 1)
    assert cells  # non-empty
    assert all(isinstance(c, int) for c in cells)


def test_custom_geomkloop_boundary_out_k0_is_boundary_ring():
    """boundary-out k=0 loop == the boundary ring (same as k=0 ring)."""
    conf = _conf()
    g = _wkb(_SIMPLE_GEOM)
    loop0 = set(_custom.geometry_k_loop(conf, g, _SIMPLE_RES, 0))
    ring0 = set(_custom.geometry_k_ring(conf, g, _SIMPLE_RES, 0))
    assert loop0 == ring0 and loop0, "boundary-out loop k0 = ring k0 (boundary ring)"


def test_custom_geomkring_all_modes_return_bigints():
    """All 6 modes return int (bigint) cell ids."""
    from databricks.labs.gbx.pygx._dilate import MODES

    conf = _conf()
    g = _wkb(_SIMPLE_GEOM)
    for mode in MODES:
        cells = _custom.geometry_k_ring(conf, g, _SIMPLE_RES, 1, mode=mode)
        assert all(isinstance(c, int) for c in cells), f"mode={mode}: non-int cell"


def test_custom_geomkring_invalid_mode_raises():
    """Unknown mode raises ValueError (parameter error, not data error)."""
    conf = _conf()
    g = _wkb(_SIMPLE_GEOM)
    with pytest.raises(ValueError, match="unknown mode"):
        _custom.geometry_k_ring(conf, g, _SIMPLE_RES, 1, mode="BOGUS")


# ── holed-polygon fixture gate ────────────────────────────────────────────────


def test_holed_fixture_h_core_nonempty():
    """Fixture correctness gate: the 40 km × 40 km hole at res 0 must yield non-empty hCore.

    Cells fully contained by the hole polygon must exist so that hole-in/hole-out modes
    genuinely exercise inward fill (not just h_border traversal).
    """
    conf = _conf()
    cls = _custom.classify(conf, _HOLED_POLY, _HOLED_RES)
    assert len(cls.h_core) > 0, (
        f"hCore is empty — hole may be too small for res {_HOLED_RES}; "
        f"hCover={len(cls.h_cover)}, hBorder={len(cls.h_border)}"
    )


def test_hole_in_mode_reaches_hcore():
    """hole-in k=3 expansion reaches hCore cells and stays within hCover."""
    conf = _conf()
    cls = _custom.classify(conf, _HOLED_POLY, _HOLED_RES)
    g = _wkb(_HOLED_POLY)
    expanded = set(_custom.geometry_k_ring(conf, g, _HOLED_RES, 3, mode="hole-in"))
    # All result cells must be within the hole region.
    assert expanded <= cls.h_cover, "hole-in result contains cells outside hCover"
    # After 3 steps inward, must reach some hCore cells.
    assert expanded & cls.h_core, "hole-in k=3 did not reach any hCore cells"
    # Must not contain cells only in the solid interior (pCore \ hCover).
    solid_only = cls.p_core - cls.h_cover
    assert not (expanded & solid_only), "hole-in result leaked into solid pCore"


def test_custom_polyfill_and_kring_geom_straddling_upper_boundary():
    """Regression: a geometry straddling the grid's ceil-overshoot upper boundary
    must not raise.

    A custom grid whose extent is not an exact multiple of the root cell size has a
    physical over-scan of up to one cell past bound_x/y_max (ceil rounding); the
    over-scan candidate cell's center can lie just outside bound_max. Such a center
    is not a real grid cell and must be skipped, not raise a data-context ValueError
    (which previously surfaced as a Serverless task failure). Found by the geomk
    stress benchmark.
    """
    conf = CustomGridConf(
        bound_x_min=0,
        bound_x_max=20000,  # 20000 / 65 = 307.7 -> ceil 308 -> 308*65 = 20020 overshoot
        bound_y_min=0,
        bound_y_max=20000,
        cell_splits=2,
        root_cell_size_x=65,
        root_cell_size_y=65,
        srid=27700,
    )
    g = box(19950, 19950, 20010, 20010)  # extends to 20010 > bound_max 20000
    fill = _custom.polyfill(conf, g, 1)  # must not raise
    ring = _custom.geometry_k_ring(conf, _wkb(g), 1, 1)  # must not raise
    k0 = _custom.geometry_k_ring(conf, _wkb(g), 1, 0)  # boundary ring (k0)
    assert isinstance(fill, list) and isinstance(ring, list)
    # boundary-out excludes the interior: any covering cell in the ring is on the
    # boundary ring (k0); k>=1 adds the outward band.
    assert (set(ring) & set(fill)) <= set(k0)
