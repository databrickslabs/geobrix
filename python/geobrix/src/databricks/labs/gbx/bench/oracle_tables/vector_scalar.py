"""Oracle table for the vector scalars (validity, cleaning, antimeridian, CRS, legacy decode): inputs -> expected,
run at every level by test_family_cross_tier.py.

Expected values come from the geometry's definition or an independent library, never from the function under
test: the noded pieces of a bowtie line, a point snapped to a grid, the longitude arithmetic of the antimeridian,
and the UTM zone 33 centre (the central meridian 15 E maps to the false easting 500000; the northing is the
`pyproj` value for (15 E, 50 N)). A geometry argument is a WKT, an EWKT (`SRID=4326;`), WKB or EWKB bytes
(the `wkb_*` / `ewkb_*` fixtures below): a column holds one type, so each encoding family is a row of its own and
the output carries the SRID of the first argument unless the row fixes one.
"""

from dataclasses import replace

from databricks.labs.gbx.bench.oracles import Build, Case, builder, uc_view

# The session rows of these scalars also run on the plain Python function (test_family_oracle_l3_callables): every
# scalar of this module's table, and `gbx_st_makevalid`, whose rows sit in the shared MVP_S table.
L3_CALLABLE = True
L3_CALLABLE_FNS = ("gbx_st_makevalid",)

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


def _ewkb_dim(wkt, dim, srid=4326):
    """EWKB of a WKT with `dim` ordinates per vertex (4 keeps the M: shapely's default would drop it at the writer)."""
    import shapely

    return shapely.to_wkb(
        shapely.set_srid(shapely.from_wkt(wkt), srid),
        output_dimension=dim,
        flavor="extended",
        include_srid=True,
    )


builder("ewkb_partial_z_line")(
    lambda tmpdir: _ewkb_dim("LINESTRING Z (15 51 5, 16 52 NaN)", 3)
)
builder("ewkb_zm_point")(lambda tmpdir: _ewkb_dim("POINT ZM (15 51 5 99)", 4))
builder("ewkb_zm_line")(
    lambda tmpdir: _ewkb_dim("LINESTRING ZM (15 51 5 99, 16 52 6 98)", 4)
)
builder("ewkb_m_point")(lambda tmpdir: _ewkb_dim("POINT M (15 51 99)", 4))
builder("ewkb_point_11_42")(lambda tmpdir: _wkb("POINT(11 42)", 4326))
builder("ewkb_mislabelled")(lambda tmpdir: _wkb("POINT(500000 5600000)", 4326))


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
    # The reasons GEOS gives, with the stable code each gets (a location it names is the point it embeds).
    Case(
        "ev_ring_self_intersection",
        EV,
        ("POLYGON((0 0,2 2,4 0,4 4,2 2,0 4,0 0))",),
        "json_subset",
        {"valid": False, "code": 11, "location": "POINT(2 2)"},
    ),
    Case(
        "ev_hole_outside_shell",
        EV,
        ("POLYGON((0 0,0 3,3 3,3 0,0 0),(5 5,5 6,6 5,5 5))",),
        "json_subset",
        {"valid": False, "code": 20, "location": "POINT(5 5)"},
    ),
    Case(
        "ev_holes_nested",
        EV,
        (
            "POLYGON((0 0,0 10,10 10,10 0,0 0),(1 1,1 9,9 9,9 1,1 1),(2 2,2 8,8 8,8 2,2 2))",
        ),
        "json_subset",
        {"valid": False, "code": 21, "location": "POINT(2 2)"},
    ),
    Case(
        "ev_interior_disconnected",
        EV,
        ("POLYGON((0 0,0 10,10 10,10 0,0 0),(0 5,5 10,10 5,5 0,0 5))",),
        "json_subset",
        {"valid": False, "code": 22},
    ),
    Case(
        "ev_nested_shells",
        EV,
        ("MULTIPOLYGON(((0 0,0 10,10 10,10 0,0 0)),((2 2,2 8,8 8,8 2,2 2)))",),
        "json_subset",
        {"valid": False, "code": 23},
    ),
    Case(
        "ev_too_few_points",
        EV,
        ("POLYGON((0 0,1 1,0 0))",),
        "json_subset",
        {"valid": False, "code": 1},
    ),
    # two identical holes overlap along their edges, which GEOS finds before it counts rings
    Case(
        "ev_duplicate_holes",
        EV,
        (
            "POLYGON((0 0,0 10,10 10,10 0,0 0),(2 2,2 8,8 8,8 2,2 2),(2 2,2 8,8 8,8 2,2 2))",
        ),
        "json_subset",
        {"valid": False, "code": 10},
    ),
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
    Case(
        "rrp_negative",
        RRP,
        (SHAPES["line_dupes"], -0.5),
        "raises",
        "tolerance must be a finite number, 0 or more; got -0.5",
    ),
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

# --- The CRS functions on the geometry shapes that once broke them (session rows). Coordinates (15 51) and (16 52) are
# inside the area of use of EPSG:32633 (UTM 33N); their UTM values are the `pyproj` values of the two points. The
# absent Z of a vertex is NaN, never 0: a Z that was not measured must not read as an elevation of zero.
U51 = (500000.0000000011, 5649824.888255088)
U52 = (568649.7048951713, 5761510.316555307)
MIXED = "GEOMETRYCOLLECTION Z (POINT Z (15 51 5), POINT (16 52))"  # GEOS refuses this text as it stands
MIXED_Z = "GEOMETRYCOLLECTION Z (POINT Z (15 51 5), POINT Z (16 52 NaN))"
MIXED_2D = f"GEOMETRYCOLLECTION (POINT ({U51[0]} {U51[1]}), POINT ({U52[0]} {U52[1]}))"
MIXED_EMPTY = "GEOMETRYCOLLECTION Z (POINT Z (15 51 5), POINT EMPTY)"
MIXED_EMPTY_Z = "GEOMETRYCOLLECTION Z (POINT Z (15 51 5), POINT Z EMPTY)"
MIXED_ZM = "GEOMETRYCOLLECTION ZM (POINT ZM (15 51 5 99), POINT (16 52))"
MIXED_NESTED = (
    "GEOMETRYCOLLECTION Z (POINT Z (15 51 5), GEOMETRYCOLLECTION (POINT (16 52)))"
)
MIXED_NESTED_Z = "GEOMETRYCOLLECTION Z (POINT Z (15 51 5), GEOMETRYCOLLECTION Z (POINT Z (16 52 NaN)))"
ZM_GC = "GEOMETRYCOLLECTION ZM (POINT ZM (15 51 5 99), LINESTRING ZM (15 51 5 99, 16 52 6 98))"
ZM_GC_Z = "GEOMETRYCOLLECTION Z (POINT Z (15 51 5), LINESTRING Z (15 51 5, 16 52 6))"
ZM_POLY = "POLYGON ZM ((15 51 5 99, 16 51 5 98, 16 52 5 97, 15 51 5 99))"
ZM_POLY_Z = "POLYGON Z ((15 51 5, 16 51 5, 16 52 5, 15 51 5))"
ZM_POLY_UTM_Z = f"POLYGON Z (({U51[0]} {U51[1]} 5, 570168.8615103093 5650300.786644997 5, {U52[0]} {U52[1]} 5, {U51[0]} {U51[1]} 5))"
CUSTOM_TM = (
    'PROJCS["Custom_TM",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],'
    'PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]],PROJECTION["Transverse_Mercator"],'
    'PARAMETER["central_meridian",13.7],PARAMETER["scale_factor",0.9996],UNIT["metre",1]]'
)
BAD_SRID = "SRID=999999;POINT(15 51)"
P51 = "POINT(15 51)"
UP51 = f"POINT({U51[0]} {U51[1]})"

VECTOR_SCALAR += (
    # --- gbx_st_setcrs never touches a coordinate, so a shape stays what it was, less the measure
    Case(
        "setcrs_mixed_dim",
        SETCRS,
        (EW(MIXED), "EPSG:32633"),
        "geom_srid",
        (MIXED_Z, 32633),
        SESSION,
    ),
    Case(
        "setcrs_mixed_empty",
        SETCRS,
        (EW(MIXED_EMPTY), "EPSG:32633"),
        "geom_srid",
        (MIXED_EMPTY_Z, 32633),
        SESSION,
    ),
    Case(
        "setcrs_mixed_zm",
        SETCRS,
        (EW(MIXED_ZM), "EPSG:32633"),
        "geom_srid",
        (MIXED_Z, 32633),
        SESSION,
    ),
    Case(
        "setcrs_mixed_nested",
        SETCRS,
        (EW(MIXED_NESTED), "EPSG:32633"),
        "geom_srid",
        (MIXED_NESTED_Z, 32633),
        SESSION,
    ),
    Case(
        "setcrs_zm_gc",
        SETCRS,
        (EW(ZM_GC), "EPSG:4326"),
        "geom_srid",
        (ZM_GC_Z, 4326),
        SESSION,
    ),
    Case(
        "setcrs_zm_polygon",
        SETCRS,
        (EW(ZM_POLY), "EPSG:4326"),
        "geom_srid",
        (ZM_POLY_Z, 4326),
        SESSION,
    ),
    Case(
        "setcrs_zm_bin",
        SETCRS,
        (Build("ewkb_zm_line"), "EPSG:4326"),
        "geom_srid",
        ("LINESTRING Z (15 51 5, 16 52 6)", 4326),
        SESSION,
    ),
    Case(
        "setcrs_m_only",
        SETCRS,
        (EW("POINT M (15 51 99)"), "EPSG:4326"),
        "geom_srid",
        (P51, 4326),
        SESSION,
    ),
    Case(
        "setcrs_m_only_bin",
        SETCRS,
        (Build("ewkb_m_point"), "EPSG:4326"),
        "geom_srid",
        (P51, 4326),
        SESSION,
    ),
    Case(
        "setcrs_partial_z_bin",
        SETCRS,
        (Build("ewkb_partial_z_line"), "EPSG:32633"),
        "geom_srid",
        ("LINESTRING Z (15 51 5, 16 52 NaN)", 32633),
        SESSION,
    ),
    Case("setcrs_empty_text", SETCRS, ("", "EPSG:4326"), "null", None, SESSION),
    Case(
        "setcrs_ignf_code",
        SETCRS,
        (P51, "IGNF:LAMB93"),
        "raises",
        "authority-less",
        SESSION,
    ),
    # --- gbx_st_transformcrs: X and Y move, a Z that every vertex has rides along, and the measure is dropped
    Case(
        "tcrs_esri",
        TCRS,
        (EW(P51), "ESRI:54008"),
        "geom_srid",
        ("POINT(1052965.165046599 5652085.722544106)", 54008),
        SESSION,
    ),
    Case(
        "tcrs_custom_tm",
        TCRS,
        (EW("POINT(11 42)"), CUSTOM_TM),
        "geom_srid",
        ("POINT(-223616.3728811576 4653303.147689764)", 0),
        SESSION,
    ),
    Case(
        "tcrs_custom_tm_bin",
        TCRS,
        (Build("ewkb_point_11_42"), CUSTOM_TM),
        "geom_srid",
        ("POINT(-223616.3728811576 4653303.147689764)", 0),
        SESSION,
    ),
    Case(
        "tcrs_proj4_target_bin",
        TCRS,
        (e("point_utm"), PROJ4_UTM33),
        "geom_srid",
        (UTM, 0),
        SESSION,
    ),
    Case(
        "tcrs_ogc_crs84", TCRS, (EW(P51), "OGC:CRS84"), "geom_srid", (P51, 0), SESSION
    ),
    Case(
        "tcrs_ignf",
        TCRS,
        (EW("POINT(2 48)"), "IGNF:LAMB93"),
        "geom_srid",
        ("POINT(625422.4046271729 6767095.930771304)", 0),
        SESSION,
    ),
    Case(
        "tcrs_z_text",
        TCRS,
        (EW("POINT Z (15 51 500)"), "EPSG:32633"),
        "geom_srid",
        (f"POINT Z({U51[0]} {U51[1]} 500)", 32633),
        SESSION,
    ),
    Case(
        "tcrs_partial_z_text",
        TCRS,
        (EW("LINESTRING Z (15 51 5, 16 52 NaN)"), "EPSG:32633"),
        "geom_srid",
        (f"LINESTRING({U51[0]} {U51[1]}, {U52[0]} {U52[1]})", 32633),
        SESSION,
    ),
    Case(
        "tcrs_partial_z_bin",
        TCRS,
        (Build("ewkb_partial_z_line"), "EPSG:32633"),
        "geom_srid",
        (f"LINESTRING({U51[0]} {U51[1]}, {U52[0]} {U52[1]})", 32633),
        SESSION,
    ),
    Case(
        "tcrs_mixed_dim",
        TCRS,
        (EW(MIXED), "EPSG:32633"),
        "geom_srid",
        (MIXED_2D, 32633),
        SESSION,
    ),
    Case(
        "tcrs_mixed_empty",
        TCRS,
        (EW(MIXED_EMPTY), "EPSG:32633"),
        "geom_srid",
        (f"GEOMETRYCOLLECTION Z (POINT Z ({U51[0]} {U51[1]} 5), POINT Z EMPTY)", 32633),
        SESSION,
    ),
    Case(
        "tcrs_mixed_zm",
        TCRS,
        (EW(MIXED_ZM), "EPSG:32633"),
        "geom_srid",
        (MIXED_2D, 32633),
        SESSION,
    ),
    Case(
        "tcrs_mixed_nested",
        TCRS,
        (EW(MIXED_NESTED), "EPSG:32633"),
        "geom_srid",
        (
            f"GEOMETRYCOLLECTION (POINT ({U51[0]} {U51[1]}), GEOMETRYCOLLECTION (POINT ({U52[0]} {U52[1]})))",
            32633,
        ),
        SESSION,
    ),
    Case(
        "tcrs_zm_point",
        TCRS,
        (EW("POINT ZM (15 51 5 99)"), "EPSG:32633"),
        "geom_srid",
        (f"POINT Z({U51[0]} {U51[1]} 5)", 32633),
        SESSION,
    ),
    Case(
        "tcrs_zm_polygon",
        TCRS,
        (EW(ZM_POLY), "EPSG:32633"),
        "geom_srid",
        (ZM_POLY_UTM_Z, 32633),
        SESSION,
    ),
    Case(
        "tcrs_zm_bin",
        TCRS,
        (Build("ewkb_zm_point"), "EPSG:32633"),
        "geom_srid",
        (f"POINT Z({U51[0]} {U51[1]} 5)", 32633),
        SESSION,
    ),
    Case(
        "tcrs_m_only",
        TCRS,
        (EW("POINT M (15 51 99)"), "EPSG:32633"),
        "geom_srid",
        (UP51, 32633),
        SESSION,
    ),
    Case(
        "tcrs_m_only_bin",
        TCRS,
        (Build("ewkb_m_point"), "EPSG:32633"),
        "geom_srid",
        (UP51, 32633),
        SESSION,
    ),
    # Bad data is NULL: an SRID nothing knows, a point PROJ sends to infinity, a plain geometry with no source CRS.
    Case("tcrs_unknown_srid", TCRS, (BAD_SRID, "EPSG:32633"), "null", None, SESSION),
    Case(
        "tcrs_unknown_srid_mixed",
        TCRS,
        (f"SRID=999999;{MIXED}", "EPSG:32633"),
        "null",
        None,
        SESSION,
    ),
    Case("tcrs_mixed_no_source", TCRS, (MIXED, "EPSG:32633"), "null", None, SESSION),
    Case(
        "tcrs_mislabelled",
        TCRS,
        (EW("POINT(500000 5600000)"), "EPSG:32633"),
        "null",
        None,
        SESSION,
    ),
    Case(
        "tcrs_mislabelled_bin",
        TCRS,
        (Build("ewkb_mislabelled"), "EPSG:32633"),
        "null",
        None,
        SESSION,
    ),
    Case(
        "tcrs_invalid_latitude",
        TCRS,
        (EW("POINT(10 100)"), "EPSG:32633"),
        "null",
        None,
        SESSION,
    ),
    Case(
        "tcrs_zm_no_source",
        TCRS,
        ("POINT ZM (15 51 5 99)", "EPSG:32633"),
        "null",
        None,
        SESSION,
    ),
    Case(
        "tcrs_garbage_target",
        TCRS,
        (EW(P51), "GARBAGE_CRS_STRING_XXXX"),
        "raises",
        "GARBAGE_CRS_STRING_XXXX",
        SESSION,
    ),
    Case(
        "tcrs_mixed_bad_source",
        TCRS,
        (MIXED, "EPSG:32633", "NOT_A_CRS_XYZ"),
        "raises",
        "NOT_A_CRS_XYZ",
        SESSION,
    ),
    # --- gbx_st_crs reads the SRID a shape plainly carries
    Case("crs_mixed_dim", CRS, (EW(MIXED),), "exact", "EPSG:4326", SESSION),
    Case("crs_zm", CRS, (Build("ewkb_zm_point"),), "exact", "EPSG:4326", SESSION),
    Case("crs_unknown_srid", CRS, (BAD_SRID,), "null", None, SESSION),
)

# --- Reprojection on the CRS shapes the checks above do not reach (session rows; the values are `pyproj` values).
# A point in the target's area of use comes out where the library puts it; a target of the same CRS leaves it.
RD_PROJ4 = (
    "+proj=sterea +lat_0=52.15616055555555 +lon_0=5.38763888888889 +k=0.9999079 +x_0=155000 +y_0=463000 "
    "+ellps=bessel +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
)
MERCATOR_PROJ4 = "+proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
VECTOR_SCALAR += (
    Case(
        "tcrs_in_domain_bng",
        TCRS,
        (EW("POINT(-0.13 51.5)"), "EPSG:27700"),
        "geom_srid",
        ("POINT(529895.3736306638 179553.1035017661)", 27700),
        SESSION,
    ),
    # and back: the UTM point of (15 E, 50 N) is that longitude and latitude again
    Case(
        "tcrs_utm_to_geographic",
        TCRS,
        ("SRID=32633;POINT(500000 5538630.702867474)", "EPSG:4326"),
        "geom_srid",
        ("POINT(15 50)", 4326),
        SESSION,
    ),
    Case(
        "tcrs_same_crs",
        TCRS,
        (EW(P51), "EPSG:4326"),
        "geom_srid",
        (P51, 4326),
        SESSION,
    ),
    # a target with no area of use is not read as out of domain
    Case(
        "tcrs_bounds_absent",
        TCRS,
        (EW("POINT(0 0)"), MERCATOR_PROJ4),
        "geom_srid",
        ("POINT(0 0)", 0),
        SESSION,
    ),
    # A PROJ4 string that PROJ's fuzzy matcher pairs with EPSG:28992 (its null datum shift puts the point ~177 m away)
    # must be projected by its own definition: the two targets are asked in turn, and each must keep its own answer.
    Case(
        "tcrs_rd_authority",
        TCRS,
        (EW("POINT(5.39 52.16)"), "EPSG:28992"),
        "geom_srid",
        ("POINT(155191.35381243424 463537.13627329096)", 28992),
        SESSION,
    ),
    Case(
        "tcrs_rd_proj4_null_shift",
        TCRS,
        (EW("POINT(5.39 52.16)"), RD_PROJ4),
        "geom_srid",
        ("POINT(155161.54513868762 463362.66380410263)", 0),
        SESSION,
    ),
    Case(
        "tcrs_no_source_bin",
        TCRS,
        (Build("wkb_point_utm"), "EPSG:32633"),
        "null",
        None,
        SESSION,
    ),
)

# --- Mixed dimensionality that is no collection: `LINESTRING Z (15 51 5, 16 52)` has a Z on one vertex only. The
# lightweight level normalizes it (the absent Z is NaN), the JVM reads the text as no geometry and answers NULL
# (HEAVY_PENDING).
MIXED_LINE = "LINESTRING Z (15 51 5, 16 52)"
MIXED_MULTIPOINT = "MULTIPOINT Z ((15 51 5), (16 52))"
for _name, _wkt, _z, _utm in (
    (
        "line",
        MIXED_LINE,
        "LINESTRING Z (15 51 5, 16 52 NaN)",
        f"LINESTRING ({U51[0]} {U51[1]}, {U52[0]} {U52[1]})",
    ),
    (
        "multipoint",
        MIXED_MULTIPOINT,
        "MULTIPOINT Z ((15 51 5), (16 52 NaN))",
        f"MULTIPOINT (({U51[0]} {U51[1]}), ({U52[0]} {U52[1]}))",
    ),
):
    VECTOR_SCALAR += (
        Case(f"crs_mixed_{_name}", CRS, (EW(_wkt),), "exact", "EPSG:4326", SESSION),
        Case(
            f"setcrs_mixed_{_name}",
            SETCRS,
            (EW(_wkt), "EPSG:32633"),
            "geom_srid",
            (_z, 32633),
            SESSION,
        ),
        Case(
            f"tcrs_mixed_{_name}",
            TCRS,
            (EW(_wkt), "EPSG:32633"),
            "geom_srid",
            (_utm, 32633),
            SESSION,
        ),
    )

# --- Unity Catalog rows: a GEOMETRY result carries an integer SRID, and the SRID of the argument is kept.
VECTOR_SCALAR += (
    Case("uc_ev_valid", EV, (SQUARE,), "exact", VALID_JSON, ("uc",)),
    # the struct's own srid becomes the GEOMETRY's SRID (the other legacy rows carry 0)
    Case(
        "uc_leg_point_srid",
        LEG,
        ({"typeId": 1, "srid": 27700, "boundaries": [[[1.0, 2.0]]], "holes": []},),
        "geom_srid",
        ("POINT(1 2)", 27700),
        ("uc",),
    ),
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
# --- the seven shapely scalars on UC (gbx_st_node, _reduceprecision, _removerepeatedpoints, _shiftlongitude,
# _simplifypreservetopology, _snap, _split). A shared row built from plain WKT carries no SRID on the session, while a UC
# GEOMETRY always carries one, so each such row stays a session row and its UC twin is derived from it: the same
# arguments and the same expected geometry, now with the SRID the UC argument was built with. One twin per function
# repeats it with SRID 3857, which is the row that fails when a handler drops the SRID of its result.
_SEVEN = frozenset({NODE, RP, RRP, SHL, SPT, SNAP, SPLIT})


def _uc_twins(cases):
    shared = [
        c
        for c in cases
        if c.fn in _SEVEN
        and c.kind == "geom_srid"
        and "uc" in c.surfaces
        and isinstance(c.expect, str)
        and isinstance(c.args[0], str)
    ]
    twins, seen = [], set()
    for c in shared:
        twins.append(
            replace(
                c,
                cid=f"uc_{c.cid}",
                expect=(c.expect, c.geom_srid),
                surfaces=("uc",),
                variants=(),
            )
        )
        if c.fn not in seen:  # the first row of each function, again in another SRID
            seen.add(c.fn)
            twins.append(
                replace(
                    c,
                    cid=f"uc_{c.cid}_keeps_srid",
                    expect=(c.expect, 3857),
                    surfaces=("uc",),
                    variants=(),
                    geom_srid=3857,
                )
            )
    return tuple(twins), tuple(c.cid for c in shared)


_TWINS, _SHARED_PLAIN = _uc_twins(VECTOR_SCALAR)
VECTOR_SCALAR += _TWINS + (
    # a GEOS failure on a geometry it cannot process (a bowtie has no valid output) is NULL on UC, an error in a session
    Case("uc_rp_invalid_input_null", RP, (BOWTIE, 0.5), "null", None, ("uc",)),
    # a form the function does not take raises with a Fix (a point has nothing to cut)
    Case(
        "uc_split_point_refused",
        SPLIT,
        ("POINT(1 1)", LINE_REF),
        "uc_refused",
        "Splitting Point",
        ("uc",),
    ),
)
VECTOR_SCALAR = uc_view(
    VECTOR_SCALAR,
    session_only=_SHARED_PLAIN
    + (
        "ev_valid",  # its EWKT variant: UC st_geomfromwkt takes no EWKT prefix (uc_ev_valid is the plain-WKT row)
        "wrapx_east",
        "wrapx_west",
        "wrapx_at_origin_west",
        "wrapx_at_origin_east",
    ),
)
