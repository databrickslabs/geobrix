"""GeometryCollection support for geom-aware kring/kloop (light tier).

A GEOMETRYCOLLECTION is processed as the union of its members: polygon members
contribute area-covered cells, line members length-crossed cells, point members
their containing cell (under coverage=coveras). Members are no longer dropped.
Tested via the quadbin public API (bbox polyfill needs no per-grid config).
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

from databricks.labs.gbx.pygx import _quadbin

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
        "boundary-out", "boundary-in", "boundary-in-ignore-holes",
        "hole-in", "hole-out", "hole-out-ignore-geom",
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
