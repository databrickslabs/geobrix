"""Oracle table for the vector scalars (validity, cleaning, antimeridian, CRS, legacy decode): inputs -> expected,
run at every level by test_family_cross_tier.py.

Expected values come from the geometry's definition or an independent library, never from the function under
test: the noded pieces of a bowtie line, a point snapped to a grid, the longitude arithmetic of the antimeridian,
and the UTM zone 33 centre (the central meridian 15 E maps to the false easting 500000; the northing is the
`pyproj` value for (15 E, 50 N)). A geometry argument is a WKT, an EWKT (`SRID=4326;`), WKB or EWKB bytes
(the `wkb_*` / `ewkb_*` fixtures below): a column holds one type, so each encoding family is a row of its own and
the output carries the SRID of the first argument unless the row fixes one.
"""

from databricks.labs.gbx.bench.oracles import Build, Case, builder, uc_view

SQUARE = "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"
BOWTIE = "POLYGON((0 0, 1 1, 1 0, 0 1, 0 0))"
SHAPES = {
    "bowtie": BOWTIE,
    "line_dupes": "LINESTRING(0 0, 0 0, 1 1, 1 1, 2 2)",
    "line_cross": "LINESTRING(0 0, 10 10, 0 10, 10 0)",
    "point_grid": "POINT(1.234 5.678)",
    "line_west": "LINESTRING(-10 0, 10 0)",
    "bump": "POLYGON((0 0, 0 5, 0.001 8, 0 10, 10 10, 10 0, 0 0))",
    "line_near": "LINESTRING(0 0.4, 10 0.4)",
    "dateline": "POLYGON((170 -10, 190 -10, 190 10, 170 10, 170 -10))",
    "point_east": "POINT(190 10)",
    "point_utm": "POINT(15 50)",
}
LINE_REF = "LINESTRING(0 0, 10 0)"
BLADE = "LINESTRING(180 -90, 180 90)"
TEN = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"
EW = lambda w: f"SRID=4326;{w}"  # noqa: E731
NOT_A_GEOMETRY = "not a geometry"


def _wkb(wkt, srid=0):
    import shapely

    g = shapely.from_wkt(wkt)
    return shapely.to_wkb(
        shapely.set_srid(g, srid) if srid else g, include_srid=bool(srid)
    )


for _k, _w in SHAPES.items():
    builder(f"wkb_{_k}")(lambda tmpdir, w=_w: _wkb(w))
    builder(f"ewkb_{_k}")(lambda tmpdir, w=_w: _wkb(w, 4326))
builder("ewkb_point_utm_z")(lambda tmpdir: _wkb("POINT Z(15 50 100)", 4326))
builder("ewkb_point_54008")(lambda tmpdir: _wkb("POINT(1 2)", 54008))


def w(k):
    return Build(f"wkb_{k}")


def e(k):
    return Build(f"ewkb_{k}")


EV, NODE, RP, RRP, SHL = (
    "gbx_st_explainvalidity",
    "gbx_st_node",
    "gbx_st_reduceprecision",
    "gbx_st_removerepeatedpoints",
    "gbx_st_shiftlongitude",
)
SPT, SNAP, SPLIT, WRAPX = (
    "gbx_st_simplifypreservetopology",
    "gbx_st_snap",
    "gbx_st_split",
    "gbx_st_wrapx",
)
CRS, SETCRS, TCRS, LEG = (
    "gbx_st_crs",
    "gbx_st_setcrs",
    "gbx_st_transformcrs",
    "gbx_st_legacyaswkb",
)

BOWTIE_JSON = '{"valid": false, "reason": "Self-intersection[0.5 0.5]", "code": 10, "location": "POINT(0.5 0.5)"}'
VALID_JSON = '{"valid": true, "reason": "Valid Geometry", "code": 0, "location": null}'
NODED = "MULTILINESTRING((0 0, 5 5), (5 5, 10 10, 0 10, 5 5), (5 5, 10 0))"
DATELINE_PIECES = (
    "GEOMETRYCOLLECTION(POLYGON((170 -10, 180 -10, 180 10, 170 10, 170 -10)), "
    "POLYGON((180 -10, 190 -10, 190 10, 180 10, 180 -10)))"
)
# UTM zone 33N (EPSG:32633) of POINT(15 50): the central meridian gives the false easting 500000.
UTM = "POINT(500000 5538630.702867474)"
UTM_Z = "POINT Z(500000 5538630.702867474 100)"
PROJ4_UTM33 = "+proj=utm +zone=33 +datum=WGS84 +units=m +no_defs"
SESSION = ("session",)

VECTOR_SCALAR = (
    # --- gbx_st_explainvalidity: JSON {valid, reason, code, location}; the geometry argument is any of the four forms
    Case("ev_bowtie", EV, (BOWTIE,), "exact", BOWTIE_JSON),
    Case("ev_valid", EV, (SQUARE,), "exact", VALID_JSON, variants=((EW(SQUARE),),)),
    Case(
        "ev_bin",
        EV,
        (w("bowtie"),),
        "exact",
        BOWTIE_JSON,
        SESSION,
        variants=((e("bowtie"),),),
    ),
    Case("ev_null", EV, (None,), "null"),
    # --- gbx_st_node
    Case("node_cross", NODE, (SHAPES["line_cross"],), "geom_srid", NODED),
    Case(
        "node_text",
        NODE,
        (SHAPES["line_cross"],),
        "geom_srid",
        NODED,
        SESSION,
        variants=((EW(SHAPES["line_cross"]),),),
    ),
    Case(
        "node_bin",
        NODE,
        (w("line_cross"),),
        "geom_srid",
        NODED,
        SESSION,
        variants=((e("line_cross"),),),
    ),
    Case("node_null", NODE, (None,), "null"),
    # --- gbx_st_reduceprecision: snap to a grid (the nearest multiple of the grid size)
    Case("rp_point", RP, (SHAPES["point_grid"], 1.0), "geom_srid", "POINT(1 6)"),
    Case(
        "rp_text",
        RP,
        (SHAPES["point_grid"], 1.0),
        "geom_srid",
        "POINT(1 6)",
        SESSION,
        variants=((EW(SHAPES["point_grid"]), 1.0),),
    ),
    Case(
        "rp_bin",
        RP,
        (w("point_grid"), 1.0),
        "geom_srid",
        "POINT(1 6)",
        SESSION,
        variants=((e("point_grid"), 1.0),),
    ),
    Case("rp_null", RP, (SHAPES["point_grid"], None), "null"),
    Case("rp_negative", RP, (SHAPES["point_grid"], -1.0), "raises", "-1.0"),
    Case("rp_inf", RP, (SHAPES["point_grid"], float("inf")), "raises", "inf"),
    # --- gbx_st_removerepeatedpoints: tolerance defaults to 0 (exact duplicates only)
    Case(
        "rrp_dupes",
        RRP,
        (SHAPES["line_dupes"],),
        "geom_srid",
        "LINESTRING(0 0, 1 1, 2 2)",
    ),
    Case(
        "rrp_text",
        RRP,
        (SHAPES["line_dupes"],),
        "geom_srid",
        "LINESTRING(0 0, 1 1, 2 2)",
        SESSION,
        variants=((EW(SHAPES["line_dupes"]),),),
    ),
    Case(
        "rrp_bin",
        RRP,
        (w("line_dupes"),),
        "geom_srid",
        "LINESTRING(0 0, 1 1, 2 2)",
        SESSION,
        variants=((e("line_dupes"),),),
    ),
    Case(
        "rrp_tolerance",
        RRP,
        ("LINESTRING(0 0, 0.001 0, 1 1)", 0.01),
        "geom_srid",
        "LINESTRING(0 0, 1 1)",
    ),
    Case(
        "rrp_null_tolerance",
        RRP,
        (SHAPES["line_dupes"], None),
        "geom_srid",
        "LINESTRING(0 0, 1 1, 2 2)",
    ),
    Case("rrp_negative", RRP, (SHAPES["line_dupes"], -0.5), "raises", "-0.5"),
    Case("rrp_null", RRP, (None,), "null"),
    # --- gbx_st_shiftlongitude: negative longitudes move into [0, 360]
    Case(
        "shl_polygon",
        SHL,
        ("POLYGON((-170 -10, -150 -10, -150 10, -170 10, -170 -10))",),
        "geom_srid",
        "POLYGON((190 -10, 210 -10, 210 10, 190 10, 190 -10))",
    ),
    Case(
        "shl_text",
        SHL,
        (SHAPES["line_west"],),
        "geom_srid",
        "LINESTRING(350 0, 10 0)",
        SESSION,
        variants=((EW(SHAPES["line_west"]),),),
    ),
    Case(
        "shl_bin",
        SHL,
        (w("line_west"),),
        "geom_srid",
        "LINESTRING(350 0, 10 0)",
        SESSION,
        variants=((e("line_west"),),),
    ),
    Case("shl_null", SHL, (None,), "null"),
    # --- gbx_st_simplifypreservetopology: Douglas-Peucker; the bump 0.001 off the line x = 0 goes
    Case(
        "spt_bump",
        SPT,
        (SHAPES["bump"], 1.0),
        "geom_srid",
        "POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))",
    ),
    Case(
        "spt_text",
        SPT,
        (SHAPES["bump"], 1.0),
        "geom_srid",
        "POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))",
        SESSION,
        variants=((EW(SHAPES["bump"]), 1.0),),
    ),
    Case(
        "spt_bin",
        SPT,
        (w("bump"), 1.0),
        "geom_srid",
        "POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))",
        SESSION,
        variants=((e("bump"), 1.0),),
    ),
    Case("spt_null_tolerance", SPT, (SHAPES["bump"], None), "null"),
    Case("spt_negative", SPT, (SHAPES["bump"], -1.0), "raises", "-1.0"),
    # --- gbx_st_snap: vertices within the tolerance move onto the reference
    Case(
        "snap_close",
        SNAP,
        (SHAPES["line_near"], LINE_REF, 0.5),
        "geom_srid",
        "LINESTRING(0 0, 10 0)",
    ),
    Case(
        "snap_far",
        SNAP,
        (SHAPES["line_near"], LINE_REF, 0.1),
        "geom_srid",
        "LINESTRING(0 0.4, 10 0.4)",
    ),
    Case(
        "snap_text",
        SNAP,
        (SHAPES["line_near"], LINE_REF, 0.5),
        "geom_srid",
        "LINESTRING(0 0, 10 0)",
        SESSION,
        variants=((EW(SHAPES["line_near"]), EW(LINE_REF), 0.5),),
    ),
    Case(
        "snap_bin",
        SNAP,
        (w("line_near"), LINE_REF, 0.5),
        "geom_srid",
        "LINESTRING(0 0, 10 0)",
        SESSION,
        variants=((e("line_near"), LINE_REF, 0.5),),
    ),
    Case("snap_null_reference", SNAP, (SHAPES["line_near"], None, 0.5), "null"),
    Case(
        "snap_negative", SNAP, (SHAPES["line_near"], LINE_REF, -0.5), "raises", "-0.5"
    ),
    Case(
        "snap_nan", SNAP, (SHAPES["line_near"], LINE_REF, float("nan")), "raises", "nan"
    ),
    # --- gbx_st_split: the pieces of the geometry on either side of the blade, as a collection
    Case(
        "split_dateline",
        SPLIT,
        (SHAPES["dateline"], BLADE),
        "geom_srid",
        DATELINE_PIECES,
    ),
    Case(
        "split_text",
        SPLIT,
        (SHAPES["dateline"], BLADE),
        "geom_srid",
        DATELINE_PIECES,
        SESSION,
        variants=((EW(SHAPES["dateline"]), EW(BLADE)),),
    ),
    Case(
        "split_bin",
        SPLIT,
        (w("dateline"), BLADE),
        "geom_srid",
        DATELINE_PIECES,
        SESSION,
        variants=((e("dateline"), BLADE),),
    ),
    # A MULTI input is split part by part; a blade that misses leaves the input as the one piece.
    Case(
        "split_multi",
        SPLIT,
        (
            "MULTIPOLYGON(((170 -10, 190 -10, 190 10, 170 10, 170 -10)), ((170 20, 190 20, 190 40, 170 40, 170 20)))",
            BLADE,
        ),
        "geom_srid",
        "GEOMETRYCOLLECTION(POLYGON((170 -10, 180 -10, 180 10, 170 10, 170 -10)), "
        "POLYGON((180 -10, 190 -10, 190 10, 180 10, 180 -10)), "
        "POLYGON((170 20, 180 20, 180 40, 170 40, 170 20)), "
        "POLYGON((180 20, 190 20, 190 40, 180 40, 180 20)))",
    ),
    Case(
        "split_no_cut",
        SPLIT,
        ("POLYGON((10 0, 20 0, 20 10, 10 10, 10 0))", BLADE),
        "geom_srid",
        "GEOMETRYCOLLECTION(POLYGON((10 0, 20 0, 20 10, 10 10, 10 0)))",
    ),
    Case("split_null_blade", SPLIT, (SHAPES["dateline"], None), "null"),
    # --- gbx_st_wrapx: a negative direction moves the x at or past the origin, a positive one x at or before it
    Case(
        "wrapx_east",
        WRAPX,
        (SHAPES["point_east"], 180.0, -360.0),
        "geom_srid",
        "POINT(-170 10)",
    ),
    Case(
        "wrapx_west",
        WRAPX,
        ("POINT(-190 10)", -180.0, 360.0),
        "geom_srid",
        "POINT(170 10)",
    ),
    Case(
        "wrapx_text",
        WRAPX,
        (SHAPES["point_east"], 180.0, -360.0),
        "geom_srid",
        "POINT(-170 10)",
        SESSION,
        variants=((EW(SHAPES["point_east"]), 180.0, -360.0),),
    ),
    Case(
        "wrapx_bin",
        WRAPX,
        (w("point_east"), 180.0, -360.0),
        "geom_srid",
        "POINT(-170 10)",
        SESSION,
        variants=((e("point_east"), 180.0, -360.0),),
    ),
    # The origin itself moves with its side, so a split edge stays attached to its piece.
    Case(
        "wrapx_at_origin_west",
        WRAPX,
        ("POINT(180 10)", 180.0, -360.0),
        "geom_srid",
        "POINT(-180 10)",
    ),
    Case(
        "wrapx_at_origin_east",
        WRAPX,
        ("POINT(0 10)", 0.0, 360.0),
        "geom_srid",
        "POINT(360 10)",
    ),
    Case("wrapx_null_origin", WRAPX, (SHAPES["point_east"], None, -360.0), "null"),
    # --- gbx_st_crs: the canonical CRS of the SRID a geometry carries; NULL when it carries none
    Case("crs_ewkt", CRS, (EW("POINT(1 2)"),), "exact", "EPSG:4326"),
    Case("crs_esri", CRS, ("SRID=54008;POINT(1 2)",), "exact", "ESRI:54008"),
    Case("crs_ewkb", CRS, (Build("ewkb_point_54008"),), "exact", "ESRI:54008", SESSION),
    Case(
        "crs_plain",
        CRS,
        ("POINT(1 2)",),
        "null",
        variants=(("SRID=999999;POINT(1 2)",),),
    ),
    Case("crs_plain_wkb", CRS, (Build("wkb_point_east"),), "null", SESSION),
    Case("crs_null", CRS, (None,), "null"),
    # --- gbx_st_setcrs: stamp, never reproject; the CRS is a code, an integer or an integer string
    Case(
        "setcrs_epsg",
        SETCRS,
        ("POINT(1 2)", "EPSG:32633"),
        "geom_srid",
        ("POINT(1 2)", 32633),
    ),
    Case(
        "setcrs_forms",
        SETCRS,
        ("POINT(1 2)", "32633"),
        "geom_srid",
        ("POINT(1 2)", 32633),
        SESSION,
        variants=((EW("POINT(1 2)"), "EPSG:32633"),),
    ),
    Case(
        "setcrs_esri",
        SETCRS,
        ("POINT(1 2)", "ESRI:54008"),
        "geom_srid",
        ("POINT(1 2)", 54008),
    ),
    Case(
        "setcrs_int", SETCRS, ("POINT(1 2)", 32633), "geom_srid", ("POINT(1 2)", 32633)
    ),
    Case(
        "setcrs_bin",
        SETCRS,
        (Build("wkb_point_east"), "EPSG:3857"),
        "geom_srid",
        ("POINT(190 10)", 3857),
        SESSION,
        variants=((Build("ewkb_point_east"), "EPSG:3857"),),
    ),
    Case("setcrs_null_geom", SETCRS, (None, "EPSG:4326"), "null"),
    Case("setcrs_null_crs", SETCRS, ("POINT(1 2)", None), "null"),
    # A SRID is an integer: a CRS with no integer authority code cannot be stamped.
    Case(
        "setcrs_authority_less",
        SETCRS,
        ("POINT(1 2)", PROJ4_UTM33),
        "raises",
        "authority-less",
    ),
    Case(
        "setcrs_non_integer_code",
        SETCRS,
        ("POINT(1 2)", "OGC:CRS84"),
        "raises",
        "CRS84",
    ),
    Case(
        "setcrs_unknown", SETCRS, ("POINT(1 2)", "EPSG:99999"), "raises", "EPSG:99999"
    ),
    # --- gbx_st_transformcrs: the same place in the same CRS, however the CRS is spelled. A code (or a code as
    # an integer or an integer string) leaves its SRID on the geometry; a definition with no authority leaves none.
    Case(
        "tcrs_utm",
        TCRS,
        (EW(SHAPES["point_utm"]), "EPSG:32633"),
        "geom_srid",
        (UTM, 32633),
    ),
    Case(
        "tcrs_forms",
        TCRS,
        (EW(SHAPES["point_utm"]), "32633"),
        "geom_srid",
        (UTM, 32633),
        SESSION,
        variants=((EW(SHAPES["point_utm"]), "urn:ogc:def:crs:EPSG::32633"),),
    ),
    Case(
        "tcrs_int_target",
        TCRS,
        (EW(SHAPES["point_utm"]), 32633),
        "geom_srid",
        (UTM, 32633),
    ),
    Case(
        "tcrs_proj4_target",
        TCRS,
        (EW(SHAPES["point_utm"]), PROJ4_UTM33),
        "geom_srid",
        (UTM, 0),
    ),
    Case(
        "tcrs_ewkb",
        TCRS,
        (e("point_utm"), "EPSG:32633"),
        "geom_srid",
        (UTM, 32633),
        SESSION,
    ),
    # A geometry with no SRID takes the source CRS argument; the embedded SRID wins when there is one.
    Case(
        "tcrs_source_arg",
        TCRS,
        (SHAPES["point_utm"], "EPSG:32633", "EPSG:4326"),
        "geom_srid",
        (UTM, 32633),
        variants=((SHAPES["point_utm"], "EPSG:32633", "4326"),),
    ),
    Case(
        "tcrs_wkb_source_arg",
        TCRS,
        (w("point_utm"), "EPSG:32633", "EPSG:4326"),
        "geom_srid",
        (UTM, 32633),
        SESSION,
    ),
    Case(
        "tcrs_z_kept",
        TCRS,
        (Build("ewkb_point_utm_z"), "EPSG:32633"),
        "geom_srid",
        (UTM_Z, 32633),
        SESSION,
    ),
    # Bad data is NULL: no source CRS at all, a point outside the target's area of use, text that is no geometry.
    Case("tcrs_no_source", TCRS, (SHAPES["point_utm"], "EPSG:32633"), "null"),
    Case("tcrs_outside_area", TCRS, (EW("POINT(-100 10)"), "EPSG:32633"), "null"),
    Case("tcrs_unreadable", TCRS, (NOT_A_GEOMETRY, "EPSG:32633"), "null"),
    Case("tcrs_null_geom", TCRS, (None, "EPSG:32633"), "null"),
    Case("tcrs_null_target", TCRS, (EW(SHAPES["point_utm"]), None), "null"),
    # A bad CRS argument is an error naming it.
    Case(
        "tcrs_bad_target",
        TCRS,
        (EW(SHAPES["point_utm"]), "EPSG:99999"),
        "raises",
        "EPSG:99999",
    ),
    Case(
        "tcrs_bad_source",
        TCRS,
        (SHAPES["point_utm"], "EPSG:32633", "NOT_A_CRS_XYZ"),
        "raises",
        "NOT_A_CRS_XYZ",
    ),
    # --- gbx_st_legacyaswkb: the Mosaic legacy struct {typeId, srid, boundaries, holes} decoded to ISO WKB
    Case(
        "leg_polygon_hole",
        LEG,
        (
            {
                "typeId": 5,
                "srid": 0,
                "boundaries": [
                    [[0.0, 0.0], [10.0, 0.0], [10.0, 10.0], [0.0, 10.0], [0.0, 0.0]]
                ],
                "holes": [
                    [[[2.0, 2.0], [4.0, 2.0], [4.0, 4.0], [2.0, 4.0], [2.0, 2.0]]]
                ],
            },
        ),
        "geom_srid",
        ("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 4 2, 4 4, 2 4, 2 2))", 0),
    ),
    Case(
        "leg_point_z",
        LEG,
        ({"typeId": 1, "srid": 0, "boundaries": [[[1.0, 2.0, 3.0]]], "holes": []},),
        "geom_srid",
        ("POINT Z(1 2 3)", 0),
    ),
    Case(
        "leg_multipoint",
        LEG,
        (
            {
                "typeId": 2,
                "srid": 0,
                "boundaries": [[[30.0, 10.0], [40.0, 20.0]]],
                "holes": [],
            },
        ),
        "geom_srid",
        ("MULTIPOINT((30 10), (40 20))", 0),
    ),
    Case(
        "leg_multipolygon_holes",
        LEG,
        (
            {
                "typeId": 6,
                "srid": 0,
                "boundaries": [
                    [[0.0, 0.0], [10.0, 0.0], [10.0, 10.0], [0.0, 10.0], [0.0, 0.0]],
                    [
                        [20.0, 20.0],
                        [25.0, 20.0],
                        [25.0, 25.0],
                        [20.0, 25.0],
                        [20.0, 20.0],
                    ],
                ],
                "holes": [
                    [[[2.0, 2.0], [4.0, 2.0], [4.0, 4.0], [2.0, 4.0], [2.0, 2.0]]],
                    [],
                ],
            },
        ),
        "geom_srid",
        (
            "MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 4 2, 4 4, 2 4, 2 2)), "
            "((20 20, 25 20, 25 25, 20 25, 20 20)))",
            0,
        ),
    ),
    Case(
        "leg_collection_refused",
        LEG,
        ({"typeId": 8, "srid": 0, "boundaries": [[[1.0, 2.0]]], "holes": []},),
        "raises",
        "GeometryCollection",
    ),
    Case("leg_null", LEG, (None,), "null"),
)

# --- Unity Catalog rows: a GEOMETRY result carries an integer SRID, and the SRID of the argument is kept.
VECTOR_SCALAR += (
    Case(
        "uc_wrapx_east",
        WRAPX,
        (SHAPES["point_east"], 180.0, -360.0),
        "geom_srid",
        ("POINT(-170 10)", 4326),
        ("uc",),
    ),
    Case(
        "uc_wrapx_west",
        WRAPX,
        ("POINT(-190 10)", -180.0, 360.0),
        "geom_srid",
        ("POINT(170 10)", 4326),
        ("uc",),
    ),
    Case(
        "uc_wrapx_at_origin_west",
        WRAPX,
        ("POINT(180 10)", 180.0, -360.0),
        "geom_srid",
        ("POINT(-180 10)", 4326),
        ("uc",),
    ),
    Case(
        "uc_wrapx_at_origin_east",
        WRAPX,
        ("POINT(0 10)", 0.0, 360.0),
        "geom_srid",
        ("POINT(360 10)", 4326),
        ("uc",),
    ),
    Case(
        "uc_wrapx_keeps_srid",
        WRAPX,
        ("POINT(190 10)", 180.0, -360.0),
        "geom_srid",
        ("POINT(-170 10)", 3857),
        ("uc",),
        geom_srid=3857,
    ),
)
VECTOR_SCALAR = uc_view(
    VECTOR_SCALAR,
    session_only=(
        "wrapx_east",
        "wrapx_west",
        "wrapx_at_origin_west",
        "wrapx_at_origin_east",
    ),
)
