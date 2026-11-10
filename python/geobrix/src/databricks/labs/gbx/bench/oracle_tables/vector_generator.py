"""Oracle table for the vector fan-outs (triangulation, elevation grids, MVT pyramid): inputs -> expected, run at every
level by test_family_cross_tier.py.

Expected values come from the fixtures' definitions or an independent library, never from the function under test.
The surface is the plane z = 2x + y + 1 sampled at the four corners of a 10 m square and its centre, so the
triangulation is the four triangles that meet in the centre (a Delaunay triangulation of a square with its centre is
unique) and every interpolated Z is the plane's. The conforming and snap rows take their triangles from JTS 1.20, the
reference algorithm (see `_tin_rows`); their elevation rows keep every point and break line on the plane, so the Z is
the plane's whatever the triangles. The MVT tiles come from `mercantile`, the layer from `mapbox_vector_tile`.
"""

import mercantile
from shapely import wkb as _wkb
from shapely.geometry import Point

from databricks.labs.gbx.bench.oracle_tables import _tin_rows as R
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
# ... and of a 4 x 4 grid: 1.25, 3.75, 6.25 and 8.75.
CENTRES_4 = [
    (x, y, _z(x, y)) for x in (1.25, 3.75, 6.25, 8.75) for y in (1.25, 3.75, 6.25, 8.75)
]
# Two break lines, as one multi-line, from the square's east corners to x = 20, on the plane.
MULTILINE_EAST = (
    f"MULTILINESTRING Z ((10 0 {_z(10, 0)}, 20 0 {_z(20, 0)}), (10 10 {_z(10, 10)}, 20 10 {_z(20, 10)}))"
)
# Cell centres of a 4 x 2 grid over (0, 0)-(20, 10).
CENTRES_EAST = [(x, y, _z(x, y)) for x in (2.5, 7.5, 12.5, 17.5) for y in (2.5, 7.5)]
M = "MIDPOINT"
SQUARE_BL = ([R.line(R.SQUARE_LINE)], R.points(R.SQUARE))

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
    # A MULTIPOINT as one element of the array is flattened into its member points.
    Case(
        "tri_multipoint_element",
        TRI,
        (
            [
                "MULTIPOINT Z ("
                + ", ".join(f"({x} {y} {_z(x, y)})" for x, y in _XY)
                + ")"
            ],
            [],
            0.0,
            0.0,
            F,
        ),
        "triangles",
        SQUARE_TRIANGLES,
        ("session",),
    ),
    # ---------------------------------------------------------------------------------------------- conforming mode
    # No break line: the conforming triangulation is the Delaunay one.
    Case(
        "tri_conforming_no_lines",
        TRI,
        (KITE, [], 0.0, 0.0, M, "conforming"),
        "triangles",
        KITE_FREE,
    ),
    # The 2D break line P1-P2 has both kite points inside its circle: one extra point at (5, 0) splits it.
    Case(
        "tri_kite_conforming",
        TRI,
        (KITE, LINE_P1P2, 0.0, 0.0, F, "conforming"),
        "triangles_exact",
        R.KITE_CONFORMING,
    ),
    # The same input, both split rules: NONENCROACHING projects the centre onto the line, MIDPOINT halves the segment
    # until no half's circle holds the centre.
    Case(
        "tri_square_bl_conforming",
        TRI,
        (SQUARE_BL[1], SQUARE_BL[0], 0.0, 0.0, F, "conforming"),
        "triangles_exact",
        R.SQUARE_BL_NON,
    ),
    Case(
        "tri_square_bl_conforming_midpoint",
        TRI,
        (SQUARE_BL[1], SQUARE_BL[0], 0.0, 0.0, M, "conforming"),
        "triangles_exact",
        R.SQUARE_BL_MID,
    ),
    # A grid: four points on every cell's circle, so the triangles are JTS's choice among the ties.
    Case(
        "tri_grid_conforming",
        TRI,
        (R.points(R.GRID), [R.line(R.GRID_LINE)], 0.0, 0.0, F, "conforming"),
        "triangles_exact",
        R.GRID_CONFORMING,
    ),
    Case(
        "tri_crossing_lines_conforming_raises",
        TRI,
        (R.points(R.BOX), [R.line(c) for c in R.CROSSING], 0.0, 0.0, F, "conforming"),
        "raises",
        "split crossing break lines where they cross",
    ),
    Case(
        "elevbbox_conforming_no_lines",
        EBB,
        (SQUARE_WKT, [], 0.0, 0.0, F, 0.0, 0.0, 10.0, 10.0, 2, 2, 0, "conforming"),
        "elev_points",
        CENTRES,
    ),
    # Every extra point takes the Z of its break line, so the surface is the plane at every cell.
    Case(
        "elevbbox_conforming_midpoint_plane",
        EBB,
        (
            SQUARE_BL[1],
            SQUARE_BL[0],
            0.0,
            0.0,
            M,
            0.0,
            0.0,
            10.0,
            10.0,
            4,
            4,
            0,
            "conforming",
        ),
        "elev_points",
        CENTRES_4,
    ),
    Case(
        "elevgeom_conforming_plane",
        EGM,
        (
            SQUARE_BL[1],
            SQUARE_BL[0],
            0.0,
            0.0,
            F,
            "POINT (0 10)",
            4,
            4,
            2.5,
            -2.5,
            "conforming",
        ),
        "elev_points",
        CENTRES_4,
    ),
    # ---------------------------------------------------------------------------------------------- snap_tolerance
    # A point within snap_tolerance of a break line moves onto the nearest one BEFORE either mode triangulates.
    *(
        Case(
            f"tri_snap_no_zero_area_{mode}",
            TRI,
            (R.points(R.SLIVER), [R.line(R.SLIVER_LINE)], 0.0, 0.05, F, mode),
            "triangles_exact",
            want,
        )
        for mode, want in (
            ("constrained", R.SLIVER_CONSTRAINED),
            ("conforming", R.SLIVER_CONFORMING),
        )
    ),
    *(
        Case(
            f"tri_snap_horizontal_vertical_{mode}",
            TRI,
            (R.points(R.HV), [R.line(ln) for ln in R.HV_LINES], 0.0, 0.05, F, mode),
            "triangles_exact",
            want,
        )
        for mode, want in (
            ("constrained", R.HV_CONSTRAINED),
            ("conforming", R.HV_CONFORMING),
        )
    ),
    Case(
        "tri_snap_nearest_line_and_one_spot",
        TRI,
        (
            R.points(R.NEAR),
            [R.line(ln) for ln in R.NEAR_LINES],
            0.001,
            0.05,
            F,
            "conforming",
        ),
        "triangles_exact",
        R.NEAR_CONFORMING,
    ),
    Case(
        "tri_snap_exact_tie_first_line",
        TRI,
        (
            R.points(R.TIE),
            [R.line(ln) for ln in R.TIE_LINES],
            0.0,
            0.05,
            F,
            "conforming",
        ),
        "triangles_exact",
        R.TIE_CONFORMING,
    ),
    Case(
        "tri_snap_merges_into_line_end",
        TRI,
        (R.points(R.END), [R.line(R.END_LINE)], 0.001, 0.05, F, "constrained"),
        "triangles_exact",
        R.END_CONSTRAINED,
    ),
    *(
        Case(
            f"elevbbox_snap_plane_{mode}",
            EBB,
            (
                R.points(R.SNAP_SQUARE),
                SQUARE_BL[0],
                0.0,
                0.05,
                M,
                0.0,
                0.0,
                10.0,
                10.0,
                4,
                4,
                0,
                mode,
            ),
            "elev_points",
            CENTRES_4,
        )
        for mode in ("constrained", "conforming")
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
    # A multi-line break line adds each of its lines. Its two lines carry the hull from the square out to x = 20, so
    # every cell of the 4 x 2 grid has a row (without them half of it is outside, as in the row below).
    *(
        Case(
            f"elevbbox_multiline_breakline_{mode}",
            EBB,
            (
                SQUARE_WKT,
                [MULTILINE_EAST],
                0.0,
                0.0,
                F,
                0.0,
                0.0,
                20.0,
                10.0,
                4,
                2,
                0,
                mode,
            ),
            "elev_points",
            CENTRES_EAST,
        )
        for mode in ("constrained", "conforming")
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
        ("session",),  # UC takes a GEOMETRY origin, not WKB bytes
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
    # ---------------------------------------------------------------------------------------------- Unity Catalog only
    # A grid whose rows could not fit the array cap is refused from its cell count before the surface is built (the
    # price: a grid with cells outside the hull is refused too, as the estimate counts every cell).
    Case(
        "uc_elevbbox_grid_over_cap_refused",
        EBB,
        (SQUARE_WKT, [], 0.0, 0.0, F, 0.0, 0.0, 10.0, 10.0, 2000, 1000, 0),
        "raises",
        "upper estimate",
        ("uc",),
    ),
    Case(
        "uc_elevgeom_grid_over_cap_refused",
        EGM,
        (SQUARE_WKT, [], 0.0, 0.0, F, "POINT (0 10)", 2000, 1000, 0.005, -0.01),
        "raises",
        "upper estimate",
        ("uc",),
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
        ("session",),  # UC takes a GEOMETRY, not WKB bytes
    ),
    # NULL attributes are a feature with no properties; in Python the NULL is the STRUCT cast, CAST(NULL AS struct<>).
    Case(
        "pyramid_null_attrs",
        PYR,
        (PYR_POINT, None, 0, 1),
        "mvt_tiles",
        {"tiles": PYR_TILES, "layer": "layer", "props": {}},
    ),
    Case("pyramid_null_geom", PYR, (None, ATTRS, 0, 1), "no_rows"),
    Case("pyramid_null_zoom", PYR, (PYR_POINT, ATTRS, None, 1), "no_rows"),
    Case("pyramid_zoom_order", PYR, (PYR_POINT, ATTRS, 2, 1), "raises", "max_z"),
    Case("pyramid_zoom_over_max", PYR, (PYR_POINT, ATTRS, 0, 21), "raises", "21"),
)
