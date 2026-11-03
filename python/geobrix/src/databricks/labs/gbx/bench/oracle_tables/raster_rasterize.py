"""Oracle table for the functions that build a raster from vectors or points: `gbx_rst_rasterize`,
`gbx_rst_gridfrompoints`, `gbx_rst_binpoints` and `gbx_rst_dtmfromgeoms`. Inputs -> expected, run at every level by
test_family_cross_tier.py.

Expected values come from the definitions, never from the function under test: a polygon is burned where the pixel
centre is inside it (shapely's `contains_xy`), a point set is interpolated by inverse-distance weighting written out
over every pixel centre, points are binned by a numpy statistic, and the height of a terrain model of points on a
plane is that plane. The grids are 10 x 10 map units in EPSG:3857, so a pixel centre is never on an edge of the
geometries. The longitude/latitude row projects with the closed-form sphere formula, no CRS library.
"""

from databricks.labs.gbx.bench.oracles import Build, Case, builder

RZ, GP, BP, DT = (
    "gbx_rst_rasterize",
    "gbx_rst_gridfrompoints",
    "gbx_rst_binpoints",
    "gbx_rst_dtmfromgeoms",
)
ONLY = ("session",)
SQUARE_WKT = "POLYGON((2 2, 6 2, 6 6, 2 6, 2 2))"
BOX = (0.0, 0.0, 10.0, 10.0)
GRID_META = {"count": 1, "dtype": "float64", "nodata": -9999.0, "crs": "EPSG:3857"}
# the four points of the IDW rows, and a value for each (no two pixel centres are equally far from two of them)
PTS = [(1.0, 1.0), (8.0, 2.0), (2.0, 9.0), (9.0, 8.0)]
VALS = [10.0, 20.0, 30.0, 40.0]
PTS_WKT = [f"POINT({x} {y})" for x, y in PTS]
# one more point, exactly on the centre of the top-left pixel of a 4 x 4 grid over BOX (no pixel has two nearest points)
CENTRE_PTS = [*PTS, (1.25, 8.75)]
CENTRE_VALS = [*VALS, 50.0]
# the points of the binning rows: two share a pixel (column 1, row 1), one on the east edge and one outside are left out
BX = [1.5, 1.5, 3.5, 8.5, 10.0, -1.0]
BY = [8.5, 8.5, 6.5, 0.5, 5.0, 5.0]
BZ = [10.0, 20.0, 5.0, 7.0, 99.0, 99.0]
# the points of the terrain rows lie on the plane z = x + 2 y and fill the square 0..10
PLANE_PTS = [(0.0, 0.0), (10.0, 0.0), (0.0, 10.0), (10.0, 10.0), (5.0, 5.0)]
DTM_WKT = [f"POINT Z({x} {y} {x + 2 * y})" for x, y in PLANE_PTS]


def _bin_args(stat=None):
    base = (BX, BY, BZ, 0.0, 0.0, 10.0, 10.0, 10, 10, 3857)
    return base if stat is None else (*base, stat)


@builder("rz_square_wkb")
def _rz_square_wkb(tmpdir):
    import shapely

    return shapely.to_wkb(shapely.from_wkt(SQUARE_WKT))


@builder("rz_square_ewkb")
def _rz_square_ewkb(tmpdir):
    import shapely

    g = shapely.set_srid(shapely.from_wkt(SQUARE_WKT), 3857)
    return shapely.to_wkb(g, include_srid=True)


@builder("rz_lonlat_ewkb")
def _rz_lonlat_ewkb(tmpdir):
    import shapely

    g = shapely.set_srid(
        shapely.from_wkt("POLYGON((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, 0.5 0.5))"), 4326
    )
    return shapely.to_wkb(g, include_srid=True)


@builder("gp_points_wkb")
def _gp_points_wkb(tmpdir):
    import shapely

    return [shapely.to_wkb(shapely.Point(x, y)) for x, y in PTS]


@builder("gp_centre_points_wkb")
def _gp_centre_points_wkb(tmpdir):
    import shapely

    return [shapely.to_wkb(shapely.Point(x, y)) for x, y in CENTRE_PTS]


@builder("dtm_points_wkb")
def _dtm_points_wkb(tmpdir):
    import shapely

    return [shapely.to_wkb(shapely.Point(x, y, x + 2 * y)) for x, y in PLANE_PTS]


SQ_WKB, SQ_EWKB, LONLAT = (
    Build("rz_square_wkb"),
    Build("rz_square_ewkb"),
    Build("rz_lonlat_ewkb"),
)
GP_WKB, DTM_WKB = Build("gp_points_wkb"), Build("dtm_points_wkb")
GP_CENTRE_WKB = Build("gp_centre_points_wkb")
LONLAT_EWKT = "SRID=4326;POLYGON((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, 0.5 0.5))"
# 2 degrees east and north in Web Mercator metres: x = R * radians(lon); y = R * ln(tan(pi / 4 + radians(lat) / 2))
MERC_X2, MERC_Y2 = 222638.98158654713, 222684.20850554318


def _rz(geom, *rest, value=5.0, extent=BOX, size=(10, 10)):
    return (geom, value, *extent, *size, *rest)


RASTER_RASTERIZE = (
    # --- rasterize: the pixels whose centre the geometry covers hold the value; the four geometry formats agree
    Case("rasterize_wkt", RZ, _rz(SQUARE_WKT, 3857), "ref", "rasterize", ONLY),
    Case("rasterize_wkb", RZ, _rz(SQ_WKB, 3857), "ref", "rasterize", ONLY),
    Case(
        "rasterize_ewkt",
        RZ,
        _rz("SRID=3857;" + SQUARE_WKT, 3857),
        "ref",
        "rasterize",
        ONLY,
    ),
    Case("rasterize_ewkb", RZ, _rz(SQ_EWKB, 3857), "ref", "rasterize", ONLY),
    Case(
        "rasterize_props",
        RZ,
        _rz(SQUARE_WKT, 3857),
        "tile_meta",
        {**GRID_META, "width": 10, "height": 10, "materialized": True},
        ONLY,
    ),
    # a geometry in longitude/latitude (its SRID says so) is reprojected into the output CRS before it is burned
    Case(
        "rasterize_reprojects",
        RZ,
        (LONLAT_EWKT, 1.0, 0.0, 0.0, MERC_X2, MERC_Y2, 8, 8, 3857),
        "ref",
        "rasterize",
        ONLY,
    ),
    Case(
        "rasterize_reprojects_ewkb",
        RZ,
        (LONLAT, 1.0, 0.0, 0.0, MERC_X2, MERC_Y2, 8, 8, 3857),
        "ref",
        "rasterize",
        ONLY,
    ),
    Case(
        "rasterize_out_crs",
        RZ,
        _rz(SQUARE_WKT, None, "EPSG:3857"),
        "tile_meta",
        {**GRID_META, "crs": "EPSG:3857"},
        ONLY,
    ),
    Case(
        "rasterize_no_crs",
        RZ,
        _rz(SQUARE_WKT),
        "tile_meta",
        {"no_crs": True, "width": 10},
        ONLY,
    ),
    Case(
        "rasterize_both_crs",
        RZ,
        _rz(SQUARE_WKT, 3857, "EPSG:3857"),
        "raises",
        "not both",
        ONLY,
    ),
    Case(
        "rasterize_unknown_srid", RZ, _rz(SQUARE_WKT, 999999), "raises", "999999", ONLY
    ),
    Case(
        "rasterize_zero_width",
        RZ,
        _rz(SQUARE_WKT, 3857, size=(0, 10)),
        "raises",
        "0 x 10",
        ONLY,
    ),
    Case(
        "rasterize_inverted_extent",
        RZ,
        _rz(SQUARE_WKT, 3857, extent=(10.0, 0.0, 0.0, 10.0)),
        "raises",
        "xmax above xmin",
        ONLY,
    ),
    Case(
        "rasterize_bad_geometry",
        RZ,
        _rz("not a geometry", 3857),
        "raises",
        "not a geometry",
        ONLY,
    ),
    Case("rasterize_null_geom", RZ, _rz(None, 3857), "null", None, ONLY),
    Case(
        "rasterize_null_value",
        RZ,
        _rz(SQUARE_WKT, 3857, value=None),
        "null",
        None,
        ONLY,
    ),
    Case(
        "rasterize_null_extent",
        RZ,
        _rz(SQUARE_WKT, 3857, extent=(None, 0.0, 10.0, 10.0)),
        "null",
        None,
        ONLY,
    ),
    # --- gridfrompoints: inverse-distance weighting over every pixel centre
    Case(
        "gridfrompoints_wkb",
        GP,
        (GP_WKB, VALS, *BOX, 4, 4, 3857),
        "ref",
        "gridfrompoints",
        ONLY,
    ),
    Case(
        "gridfrompoints_wkt",
        GP,
        (PTS_WKT, VALS, *BOX, 4, 4, 3857),
        "ref",
        "gridfrompoints",
        ONLY,
    ),
    Case(
        "gridfrompoints_power",
        GP,
        (GP_WKB, VALS, *BOX, 4, 4, 3857, 1.0),
        "ref",
        "gridfrompoints",
        ONLY,
    ),
    # the nearest point only (a k-nearest search, not the all-points sum)
    Case(
        "gridfrompoints_nearest",
        GP,
        (GP_WKB, VALS, *BOX, 4, 4, 3857, 2.0, 1),
        "ref",
        "gridfrompoints",
        ONLY,
    ),
    # a point exactly on a pixel centre gives that pixel its value (distance 0), also in the nearest-point search
    Case(
        "gridfrompoints_nearest_on_centre",
        GP,
        (GP_CENTRE_WKB, CENTRE_VALS, *BOX, 4, 4, 3857, 2.0, 1),
        "ref",
        "gridfrompoints",
        ONLY,
    ),
    Case(
        "gridfrompoints_no_points",
        GP,
        ([], [], *BOX, 4, 4, 3857),
        "ref",
        "gridfrompoints",
        ONLY,
    ),
    Case(
        "gridfrompoints_props",
        GP,
        (GP_WKB, VALS, *BOX, 4, 4, 3857),
        "tile_meta",
        {**GRID_META, "width": 4, "height": 4, "materialized": True},
        ONLY,
    ),
    Case(
        "gridfrompoints_out_crs",
        GP,
        (GP_WKB, VALS, *BOX, 4, 4, None, 2.0, 12, "EPSG:3857"),
        "tile_meta",
        {"crs": "EPSG:3857"},
        ONLY,
    ),
    # the 8-argument form: no output CRS at all
    Case(
        "gridfrompoints_no_crs",
        GP,
        (GP_WKB, VALS, *BOX, 4, 4),
        "tile_meta",
        {"no_crs": True, "width": 4},
        ONLY,
    ),
    Case(
        "gridfrompoints_both_crs",
        GP,
        (GP_WKB, VALS, *BOX, 4, 4, 3857, 2.0, 12, "EPSG:3857"),
        "raises",
        "not both",
        ONLY,
    ),
    Case(
        "gridfrompoints_length_mismatch",
        GP,
        (GP_WKB, VALS[:3], *BOX, 4, 4, 3857),
        "raises",
        "length mismatch",
        ONLY,
    ),
    Case(
        "gridfrompoints_zero_power",
        GP,
        (GP_WKB, VALS, *BOX, 4, 4, 3857, 0.0),
        "raises",
        "power",
        ONLY,
    ),
    Case(
        "gridfrompoints_zero_max_pts",
        GP,
        (GP_WKB, VALS, *BOX, 4, 4, 3857, 2.0, 0),
        "raises",
        "max_pts",
        ONLY,
    ),
    Case(
        "gridfrompoints_zero_height",
        GP,
        (GP_WKB, VALS, *BOX, 4, 0, 3857),
        "raises",
        "4 x 0",
        ONLY,
    ),
    Case(
        "gridfrompoints_null_points",
        GP,
        (None, VALS, *BOX, 4, 4, 3857),
        "null",
        None,
        ONLY,
    ),
    Case(
        "gridfrompoints_null_extent",
        GP,
        (GP_WKB, VALS, None, 0.0, 10.0, 10.0, 4, 4, 3857),
        "null",
        None,
        ONLY,
    ),
    # --- binpoints: the points of a pixel reduced by a statistic; an edge point and an outside point are left out
    *(
        Case(f"binpoints_{w}", BP, _bin_args(w), "ref", "binpoints", ONLY)
        for w in ("max", "min", "mean", "median", "count", "percentile:90")
    ),
    Case("binpoints_default_statistic", BP, _bin_args(), "ref", "binpoints", ONLY),
    Case("binpoints_upper_case", BP, _bin_args("MAX"), "ref", "binpoints", ONLY),
    Case(
        "binpoints_props",
        BP,
        _bin_args(),
        "tile_meta",
        {
            **GRID_META,
            "dtype": "float32",
            "width": 10,
            "height": 10,
            "materialized": True,
        },
        ONLY,
    ),
    Case("binpoints_bad_statistic", BP, _bin_args("modal"), "raises", "modal", ONLY),
    Case(
        "binpoints_length_mismatch",
        BP,
        ([1.5, 2.5], [1.5], [1.0, 2.0], *BOX, 10, 10, 3857),
        "raises",
        "same length",
        ONLY,
    ),
    Case(
        "binpoints_zero_width",
        BP,
        (BX, BY, BZ, *BOX, 0, 10, 3857),
        "raises",
        "0 x 10",
        ONLY,
    ),
    Case("binpoints_empty", BP, ([], [], [], *BOX, 10, 10, 3857), "null", None, ONLY),
    Case(
        "binpoints_null_x", BP, (None, BY, BZ, *BOX, 10, 10, 3857), "null", None, ONLY
    ),
    Case(
        "binpoints_null_srid", BP, (BX, BY, BZ, *BOX, 10, 10, None), "null", None, ONLY
    ),
    # --- dtmfromgeoms: the height interpolated in the triangle of the pixel centre; NoData outside the hull
    Case(
        "dtmfromgeoms_wkb",
        DT,
        (DTM_WKB, [], 0.0, 0.0, *BOX, 5, 5, 3857),
        "ref",
        "dtm_plane",
        ONLY,
    ),
    Case(
        "dtmfromgeoms_wkt",
        DT,
        (DTM_WKT, [], 0.0, 0.0, *BOX, 5, 5, 3857),
        "ref",
        "dtm_plane",
        ONLY,
    ),
    Case(
        "dtmfromgeoms_outside_hull",
        DT,
        (DTM_WKB, [], 0.0, 0.0, -5.0, -5.0, 15.0, 15.0, 8, 8, 3857),
        "ref",
        "dtm_plane",
        ONLY,
    ),
    Case(
        "dtmfromgeoms_no_data",
        DT,
        (DTM_WKB, [], 0.0, 0.0, -5.0, -5.0, 15.0, 15.0, 8, 8, 3857, -1.0),
        "ref",
        "dtm_plane",
        ONLY,
    ),
    Case(
        "dtmfromgeoms_props",
        DT,
        (DTM_WKB, [], 0.0, 0.0, *BOX, 5, 5, 3857),
        "tile_meta",
        {**GRID_META, "width": 5, "height": 5, "materialized": True},
        ONLY,
    ),
    Case(
        "dtmfromgeoms_both_crs",
        DT,
        (DTM_WKB, [], 0.0, 0.0, *BOX, 5, 5, 3857, -9999.0, "EPSG:3857"),
        "raises",
        "not both",
        ONLY,
    ),
    Case(
        "dtmfromgeoms_no_z",
        DT,
        (["POINT(0 0)", "POINT(1 0)", "POINT(0 1)"], [], 0.0, 0.0, *BOX, 5, 5, 3857),
        "raises",
        "Z coordinate",
        ONLY,
    ),
    Case(
        "dtmfromgeoms_no_points",
        DT,
        ([], [], 0.0, 0.0, *BOX, 5, 5, 3857),
        "raises",
        "at least one point",
        ONLY,
    ),
    Case(
        "dtmfromgeoms_zero_width",
        DT,
        (DTM_WKB, [], 0.0, 0.0, *BOX, 0, 5, 3857),
        "raises",
        "0 x 5",
        ONLY,
    ),
    Case(
        "dtmfromgeoms_null_points",
        DT,
        (None, [], 0.0, 0.0, *BOX, 5, 5, 3857),
        "null",
        None,
        ONLY,
    ),
    Case(
        "dtmfromgeoms_null_extent",
        DT,
        (DTM_WKB, [], 0.0, 0.0, None, 0.0, 10.0, 10.0, 5, 5, 3857),
        "null",
        None,
        ONLY,
    ),
)
