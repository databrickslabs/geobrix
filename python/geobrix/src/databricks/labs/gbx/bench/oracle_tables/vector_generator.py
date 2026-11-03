"""Oracle table for the vector fan-outs (triangulation, elevation grids, MVT pyramid): inputs -> expected, run at every
level by test_family_cross_tier.py.

Expected values come from the fixtures' definitions or an independent library, never from the function under test.
The surface is the plane z = 2x + y + 1 sampled at the four corners of a 10 m square and its centre, so the
triangulation is the four triangles that meet in the centre (a Delaunay triangulation of a square with its centre is
unique) and every interpolated Z is the plane's. The MVT tiles come from `mercantile`, the layer from
`mapbox_vector_tile`.
"""

import mercantile
from shapely import wkb as _wkb
from shapely.geometry import Point

from databricks.labs.gbx.bench.oracles import Case

TRI, EBB, EGM, PYR = (
    "gbx_st_triangulate",
    "gbx_st_interpolateelevationbbox",
    "gbx_st_interpolateelevationgeom",
    "gbx_st_asmvt_pyramid",
)


def _z(x: float, y: float) -> float:
    """The plane the surface lies on."""
    return 2.0 * x + y + 1.0


_XY = [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (5.0, 5.0)]
SQUARE_WKT = [f"POINT Z ({x} {y} {_z(x, y)})" for x, y in _XY]
SQUARE_WKB = [_wkb.dumps(Point(x, y, _z(x, y))) for x, y in _XY]
# The four corners go round the centre: each triangle is two neighbouring corners and the centre.
A, B, C, D, M = _XY
SQUARE_TRIANGLES = [(A, B, M), (B, C, M), (C, D, M), (D, A, M)]

# A kite: P3 and P4 are 2 m apart across the line P1-P2 (10 m), so the Delaunay edge is P3-P4. A break line along
# P1-P2 forces the other diagonal.
KITE = ["POINT Z (0 0 0)", "POINT Z (10 0 0)", "POINT Z (5 1 0)", "POINT Z (5 -1 0)"]
P1, P2, P3, P4 = (0.0, 0.0), (10.0, 0.0), (5.0, 1.0), (5.0, -1.0)
KITE_FREE = [(P1, P3, P4), (P2, P3, P4)]
KITE_FORCED = [(P1, P2, P3), (P1, P2, P4)]
LINE_P1P2 = ["LINESTRING (0 0, 10 0)"]
F = "NONENCROACHING"

# Cell centres of a 2 x 2 grid over (0, 0)-(10, 10): 2.5 and 7.5 on both axes.
CENTRES = [(x, y, _z(x, y)) for x in (2.5, 7.5) for y in (2.5, 7.5)]

# A feature at 100 E, 45 N is far from every tile seam of zoom 0 and 1 (the tiles keep a 64 px overhang), so it is in
# one tile of each.
PYR_POINT = "POINT (100 45)"
PYR_TILES = [(z, *tuple(mercantile.tile(100.0, 45.0, z))[:2]) for z in (0, 1)]
ATTRS = {"name": "a", "id": 7}

VECTOR_GENERATOR = (
    # ---------------------------------------------------------------------------------------------- triangulate
    Case(
        "tri_square_centre",
        TRI,
        (SQUARE_WKT, [], 0.0, 0.0, F),
        "triangles",
        SQUARE_TRIANGLES,
    ),
    Case(
        "tri_square_centre_bin",
        TRI,
        (SQUARE_WKB, [], 0.0, 0.0, F),
        "triangles",
        SQUARE_TRIANGLES,
    ),
    Case("tri_kite_free", TRI, (KITE, [], 0.0, 0.0, F), "triangles", KITE_FREE),
    Case(
        "tri_kite_forced", TRI, (KITE, LINE_P1P2, 0.0, 0.0, F), "triangles", KITE_FORCED
    ),
    Case(
        "tri_mode_constrained",
        TRI,
        (KITE, [], 0.0, 0.0, F, "constrained"),
        "triangles",
        KITE_FREE,
    ),
    Case("tri_null_points", TRI, (None, [], 0.0, 0.0, F), "no_rows"),
    Case("tri_empty_points", TRI, ([], [], 0.0, 0.0, F), "no_rows"),
    Case("tri_null_tolerance", TRI, (SQUARE_WKT, [], None, 0.0, F), "no_rows"),
    Case(
        "tri_bad_mode", TRI, (KITE, [], 0.0, 0.0, F, "sideways"), "raises", "sideways"
    ),
    # ---------------------------------------------------------------------------------------------- bbox grid
    Case(
        "elevbbox_plane",
        EBB,
        (SQUARE_WKT, [], 0.0, 0.0, F, 0.0, 0.0, 10.0, 10.0, 2, 2, 0),
        "elev_points",
        CENTRES,
    ),
    Case(
        "elevbbox_plane_bin",
        EBB,
        (SQUARE_WKB, [], 0.0, 0.0, F, 0.0, 0.0, 10.0, 10.0, 2, 2, 27700),
        "elev_points",
        CENTRES,
    ),
    # Half of the grid is outside the hull of the points: those cells give no row.
    Case(
        "elevbbox_outside_hull",
        EBB,
        (SQUARE_WKT, [], 0.0, 0.0, F, 0.0, 0.0, 20.0, 10.0, 4, 2, 0),
        "elev_points",
        CENTRES,
    ),
    Case(
        "elevbbox_null_points",
        EBB,
        (None, [], 0.0, 0.0, F, 0.0, 0.0, 10.0, 10.0, 2, 2, 0),
        "no_rows",
    ),
    Case(
        "elevbbox_null_extent",
        EBB,
        (SQUARE_WKT, [], 0.0, 0.0, F, None, 0.0, 10.0, 10.0, 2, 2, 0),
        "no_rows",
    ),
    Case(
        "elevbbox_bad_mode",
        EBB,
        (SQUARE_WKT, [], 0.0, 0.0, F, 0.0, 0.0, 10.0, 10.0, 2, 2, 0, "sideways"),
        "raises",
        "sideways",
    ),
    # ---------------------------------------------------------------------------------------------- origin grid
    # The origin is the north-west corner; a negative cell height steps south.
    Case(
        "elevgeom_plane",
        EGM,
        (SQUARE_WKT, [], 0.0, 0.0, F, "POINT (0 10)", 2, 2, 5.0, -5.0),
        "elev_points",
        CENTRES,
    ),
    Case(
        "elevgeom_origin_bin",
        EGM,
        (SQUARE_WKT, [], 0.0, 0.0, F, _wkb.dumps(Point(0.0, 10.0)), 2, 2, 5.0, -5.0),
        "elev_points",
        CENTRES,
    ),
    Case(
        "elevgeom_origin_ewkt",
        EGM,
        (SQUARE_WKT, [], 0.0, 0.0, F, "SRID=27700;POINT (0 10)", 2, 2, 5.0, -5.0),
        "elev_points",
        CENTRES,
    ),
    Case(
        "elevgeom_null_origin",
        EGM,
        (SQUARE_WKT, [], 0.0, 0.0, F, None, 2, 2, 5.0, -5.0),
        "no_rows",
    ),
    Case(
        "elevgeom_null_points",
        EGM,
        (None, [], 0.0, 0.0, F, "POINT (0 10)", 2, 2, 5.0, -5.0),
        "no_rows",
    ),
    # ---------------------------------------------------------------------------------------------- MVT pyramid
    Case(
        "pyramid_point_z0_1",
        PYR,
        (PYR_POINT, ATTRS, 0, 1),
        "mvt_tiles",
        {"tiles": PYR_TILES, "layer": "layer", "props": {"name": "a", "id": 7}},
    ),
    Case(
        "pyramid_layer_named",
        PYR,
        (PYR_POINT, ATTRS, 0, 0, "roads", 4096),
        "mvt_tiles",
        {"tiles": PYR_TILES[:1], "layer": "roads", "props": {"name": "a", "id": 7}},
    ),
    Case(
        "pyramid_point_bin",
        PYR,
        (_wkb.dumps(Point(100.0, 45.0)), ATTRS, 0, 1),
        "mvt_tiles",
        {"tiles": PYR_TILES, "layer": "layer", "props": {"name": "a", "id": 7}},
    ),
    Case("pyramid_null_geom", PYR, (None, ATTRS, 0, 1), "no_rows"),
    Case("pyramid_null_zoom", PYR, (PYR_POINT, ATTRS, None, 1), "no_rows"),
    Case("pyramid_zoom_order", PYR, (PYR_POINT, ATTRS, 2, 1), "raises", "max_z"),
    Case("pyramid_zoom_over_max", PYR, (PYR_POINT, ATTRS, 0, 21), "raises", "21"),
)
