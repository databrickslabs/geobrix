"""GeometryCollection support for geom-aware kring/kloop (light tier).

A GEOMETRYCOLLECTION is processed as the union of its members: polygon members
contribute area-covered cells, line members length-crossed cells, point members
their containing cell (under coverage=coveras). Members are no longer dropped.
Tested via the quadbin public API (bbox polyfill needs no per-grid config).
h3 GC support is tested via the _h3.geom_expand API (h3 is light-only for
geom-aware kring/kloop).
"""

from shapely import to_wkb
from shapely.geometry import (
    GeometryCollection,
    LineString,
    MultiPolygon,
    Point,
    Polygon,
    box,
)

from databricks.labs.gbx.pygx import _h3, _quadbin

RES = 18


def _ring(geom, k=1, mode="boundary-in", coverage="coveras"):
    return set(_quadbin.geometry_k_ring(to_wkb(geom), RES, k, mode, coverage))


def _poly_a():
    return box(-122.44, 37.75, -122.43, 37.76)


def _poly_b():
    return box(-122.42, 37.75, -122.41, 37.76)


def test_gc_polygon_plus_point_includes_point_member():
    # A GC of one polygon + a point far from the polygon. Under coveras the point's
    # containing cell must appear — i.e. the point member is NOT dropped.
    pt = Point(-122.40, 37.80)
    poly = _poly_a()
    gc = GeometryCollection([poly, pt])
    pt_cells = _ring(pt, k=0, mode="boundary-in")
    gc_cells = _ring(gc, k=0, mode="boundary-in")
    poly_cells = _ring(poly, k=0, mode="boundary-in")
    assert pt_cells, "point alone should cover its containing cell"
    assert pt_cells <= gc_cells, "GC dropped the point member"
    assert poly_cells <= gc_cells, "GC dropped the polygon member"


def test_gc_polygon_plus_line_includes_line_member():
    line = LineString([(-122.405, 37.80), (-122.395, 37.80)])
    poly = _poly_a()
    gc = GeometryCollection([poly, line])
    line_cells = _ring(line, k=0, mode="boundary-in")
    gc_cells = _ring(gc, k=0, mode="boundary-in")
    assert line_cells, "line alone should cover crossed cells"
    assert line_cells <= gc_cells, "GC dropped the line member"


def test_gc_of_polygons_equals_multipolygon():
    # Consistency guarantee: a GC of only polygons == the MultiPolygon of them,
    # for every mode and coverage, ring and loop.
    polys = [_poly_a(), _poly_b()]
    gc = GeometryCollection(polys)
    mp = MultiPolygon(polys)
    for mode in (
        "boundary-out",
        "boundary-in",
        "boundary-in-ignore-holes",
        "hole-in",
        "hole-out",
        "hole-out-ignore-geom",
    ):
        for coverage in ("coveras", "polyfill", "core"):
            gc_r = set(_quadbin.geometry_k_ring(to_wkb(gc), RES, 2, mode, coverage))
            mp_r = set(_quadbin.geometry_k_ring(to_wkb(mp), RES, 2, mode, coverage))
            assert gc_r == mp_r, f"ring mismatch mode={mode} cov={coverage}"
            gc_l = set(_quadbin.geometry_k_loop(to_wkb(gc), RES, 2, mode, coverage))
            mp_l = set(_quadbin.geometry_k_loop(to_wkb(mp), RES, 2, mode, coverage))
            assert gc_l == mp_l, f"loop mismatch mode={mode} cov={coverage}"


def test_gc_point_line_empty_under_core():
    # Under coverage=core, 0/1-dim members contribute nothing (they have no
    # fully-contained cell) — so GC(point, line) is empty under core.
    gc = GeometryCollection(
        [Point(-122.40, 37.80), LineString([(-122.41, 37.79), (-122.40, 37.79)])]
    )
    assert _ring(gc, k=0, mode="boundary-in", coverage="core") == set()


def test_nested_gc_flattens():
    poly, pt = _poly_a(), Point(-122.40, 37.80)
    nested = GeometryCollection([GeometryCollection([poly]), pt])
    flat = GeometryCollection([poly, pt])
    assert _ring(nested, k=1) == _ring(flat, k=1)


def test_empty_gc_returns_empty():
    assert _ring(GeometryCollection([]), k=1) == set()


def test_gc_with_empty_member_skips_empty_includes_live():
    # A GC whose first member is an empty geometry: the empty member is skipped and
    # the live member's cells are present.
    live = Point(-122.40, 37.80)
    gc = GeometryCollection([Polygon(), live])  # Polygon() is empty
    live_cells = _ring(live, k=0, mode="boundary-in")
    gc_cells = _ring(gc, k=0, mode="boundary-in")
    assert live_cells, "live point should cover its containing cell"
    assert live_cells <= gc_cells, "GC dropped the live member beside the empty one"


# ---------------------------------------------------------------------------
# h3 GC tests — _h3.geom_expand is the light-only geom-aware API for h3
# (no heavy h3 geomkring/loop; these tests need no Databricks session).
# ---------------------------------------------------------------------------

# Use a slightly larger box so h3 res-10 cells are definitely non-empty.
# _poly_a/b reuse the same SF coordinates as the quadbin tests above.
_H3_RES = 10  # edge ~0.065 km; 0.01° box ≈ 0.7–1.1 km → several cells
_H3_POLY_A = box(-122.44, 37.75, -122.43, 37.76)  # 0.01° SF box
_H3_POLY_B = box(-122.42, 37.75, -122.41, 37.76)  # adjacent box
_H3_PT = Point(-122.40, 37.80)  # point well away from the boxes
_H3_LINE = LineString([(-122.405, 37.80), (-122.395, 37.80)])  # line near the point


def _h3_ring(geom, k=0, mode="boundary-in", coverage="coveras"):
    return _h3.geom_expand("ring", to_wkb(geom), _H3_RES, k, mode, coverage)


def _h3_loop(geom, k=1, mode="boundary-out", coverage="coveras"):
    return _h3.geom_expand("loop", to_wkb(geom), _H3_RES, k, mode, coverage)


def test_h3_gc_polygon_plus_point_includes_point_member():
    """h3: a GC of polygon + far point — point member's cell is NOT dropped."""
    pt_cells = _h3_ring(_H3_PT)
    assert pt_cells, "h3 point alone should cover its containing cell (precondition)"
    gc = GeometryCollection([_H3_POLY_A, _H3_PT])
    gc_cells = _h3_ring(gc)
    poly_cells = _h3_ring(_H3_POLY_A)
    assert poly_cells, "h3 poly alone must be non-empty (precondition)"
    assert pt_cells <= gc_cells, "h3 GC dropped the point member"
    assert poly_cells <= gc_cells, "h3 GC dropped the polygon member"


def test_h3_gc_polygon_plus_line_includes_line_member():
    """h3: a GC of polygon + line — line member's cells are NOT dropped."""
    line_cells = _h3_ring(_H3_LINE)
    assert line_cells, "h3 line alone should cover crossed cells (precondition)"
    gc = GeometryCollection([_H3_POLY_A, _H3_LINE])
    gc_cells = _h3_ring(gc)
    assert line_cells <= gc_cells, "h3 GC dropped the line member"


def test_h3_gc_of_polygons_equals_multipolygon():
    """h3: GC of only polygons == the MultiPolygon of them, all modes × coverages × ring+loop."""
    polys = [_H3_POLY_A, _H3_POLY_B]
    gc = GeometryCollection(polys)
    mp = MultiPolygon(polys)
    gc_wkb = to_wkb(gc)
    mp_wkb = to_wkb(mp)
    for mode in (
        "boundary-out",
        "boundary-in",
        "boundary-in-ignore-holes",
        "hole-in",
        "hole-out",
        "hole-out-ignore-geom",
    ):
        for coverage in ("coveras", "polyfill", "core"):
            gc_r = _h3.geom_expand("ring", gc_wkb, _H3_RES, 2, mode, coverage)
            mp_r = _h3.geom_expand("ring", mp_wkb, _H3_RES, 2, mode, coverage)
            assert gc_r == mp_r, f"h3 ring mismatch mode={mode} cov={coverage}"
            gc_l = _h3.geom_expand("loop", gc_wkb, _H3_RES, 2, mode, coverage)
            mp_l = _h3.geom_expand("loop", mp_wkb, _H3_RES, 2, mode, coverage)
            assert gc_l == mp_l, f"h3 loop mismatch mode={mode} cov={coverage}"


def test_h3_gc_point_line_empty_under_core():
    """h3: GC(point, line) is empty under coverage=core (0/1-dim → no fully-contained cell)."""
    gc = GeometryCollection([_H3_PT, _H3_LINE])
    assert _h3_ring(gc, k=0, mode="boundary-in", coverage="core") == set()


def test_h3_empty_gc_returns_empty():
    """h3: empty GC returns empty set."""
    assert (
        _h3.geom_expand(
            "ring", to_wkb(GeometryCollection([])), _H3_RES, 1, "boundary-out"
        )
        == set()
    )


def test_h3_nested_gc_flattens():
    """h3: nested GC == flat GC (nested collections are recursively flattened)."""
    nested = GeometryCollection([GeometryCollection([_H3_POLY_A]), _H3_PT])
    flat = GeometryCollection([_H3_POLY_A, _H3_PT])
    assert _h3_ring(nested, k=1) == _h3_ring(flat, k=1)


def test_h3_gc_with_empty_member_skips_empty_includes_live():
    """h3: a GC with an empty member beside a live member — empty is skipped."""
    live = _H3_PT
    gc = GeometryCollection([Polygon(), live])  # Polygon() is empty
    live_cells = _h3_ring(live)
    gc_cells = _h3_ring(gc)
    assert live_cells, "h3 live point should cover its containing cell (precondition)"
    assert live_cells <= gc_cells, "h3 GC dropped the live member beside the empty one"
