"""Oracle table for the grid scalars (BNG, quadbin, custom grid, H3): inputs -> expected, run at every level by
test_family_cross_tier.py.

Expected values come from the fixtures' definitions or an independent library, never from the function
under test. A BNG cell is a square of EPSG:27700 metres: `TQ3080` is the 1 km square whose south-west corner
is (530000, 180000), since the 100 km square `TQ` starts at (500000, 100000). A quadbin cell is a web-mercator
tile (x, y, zoom); its corners come from the tile arithmetic below, and its id from the `quadbin` library. A
custom-grid cell id is `position | resolution << 56` with `position = row * columns + column`. H3 values come
from the `h3` library.
"""

import math
import struct

from databricks.labs.gbx.bench.oracles import Build, Case, builder, uc_view

TQ3080 = "POLYGON((530000 180000, 531000 180000, 531000 181000, 530000 181000, 530000 180000))"
# A point whose coordinates are infinite: there is no cell for it (WKB, since WKT cannot spell infinity).
INF_POINT = struct.pack("<BIdd", 1, 1, float("inf"), float("inf"))
POINT_WKB = struct.pack("<BIdd", 1, 1, 530500.0, 180500.0)


def _chip(cellid, core, wkt):
    import shapely

    return {
        "cellid": cellid,
        "core": core,
        "chip": shapely.to_wkb(shapely.from_wkt(wkt)),
    }


# Two chips of TQ3080: its west half and its south half (EPSG:27700 metres).
WEST = "POLYGON((530000 180000, 530500 180000, 530500 181000, 530000 181000, 530000 180000))"
SOUTH = "POLYGON((530000 180000, 531000 180000, 531000 180500, 530000 180500, 530000 180000))"


@builder("chip_west")
def _chip_west(tmpdir):
    return _chip("TQ3080", False, WEST)


@builder("chip_south")
def _chip_south(tmpdir):
    return _chip("TQ3080", False, SOUTH)


@builder("chip_no_bytes")
def _chip_no_bytes(tmpdir):
    """A non-core chip with no geometry: what an empty intersection leaves in a chip table."""
    return {"cellid": "TQ3080", "core": False, "chip": None}


@builder("chip_west_core")
def _chip_west_core(tmpdir):
    return _chip("TQ3080", True, WEST)


@builder("chip_other_cell")
def _chip_other_cell(tmpdir):
    return _chip(
        "TQ3180",
        False,
        "POLYGON((531000 180000, 531500 180000, 531500 181000, 531000 181000, 531000 180000))",
    )


AWKB, AWKT, AREA, CENT = (
    "gbx_bng_aswkb",
    "gbx_bng_aswkt",
    "gbx_bng_cellarea",
    "gbx_bng_centroid",
)
DIST, EDIST, PAC = (
    "gbx_bng_distance",
    "gbx_bng_euclideandistance",
    "gbx_bng_pointascell",
)
CINT, CUNI = "gbx_bng_cellintersection", "gbx_bng_cellunion"

# The bytes of TQ3080's centre, laid out by hand: little-endian (NDR) like shapely, GDAL and st_asbinary, no SRID. The
# same call must give these bytes at the lightweight and the JAR level. (A point has no ring to start anywhere, so the
# bytes can only differ by byte order or flags.)
TQ3080_CENTRE_WKB = struct.pack("<BIdd", 1, 1, 530500.0, 180500.0)


def _square_wkb(x0, y0, x1, y1):
    """A square's plain WKB, laid out by hand in the JVM's ring: little-endian polygon, one ring of five points that
    starts at the south-west corner and runs counter-clockwise."""
    ring = ((x0, y0), (x1, y0), (x1, y1), (x0, y1), (x0, y0))
    return struct.pack("<BIII", 1, 3, 1, 5) + b"".join(
        struct.pack("<dd", x, y) for x, y in ring
    )


TQ3080_WKB = _square_wkb(530000.0, 180000.0, 531000.0, 181000.0)

GRID_SCALAR = (
    Case("aswkb_tq3080", AWKB, ("TQ3080",), "geom_equals", TQ3080),
    # The exact bytes (session surface only: the UC handler returns EWKB with the SRID).
    Case(
        "aswkb_tq3080_bytes",
        AWKB,
        ("TQ3080",),
        "wkb_bytes",
        TQ3080_WKB,
        surfaces=("session",),
    ),
    Case("aswkb_null", AWKB, (None,), "null"),
    # A malformed cell id is a data error: NULL, not a failed query.
    Case("aswkb_bad_cell", AWKB, ("XX9999",), "null"),
    Case("aswkt_tq3080", AWKT, ("TQ3080",), "geom_equals", TQ3080),
    Case(
        "aswkt_100km",
        AWKT,
        ("TQ",),
        "geom_equals",
        "POLYGON((500000 100000, 600000 100000, 600000 200000, 500000 200000, 500000 100000))",
    ),
    Case("aswkt_null", AWKT, (None,), "null"),
    Case("cellarea_1km", AREA, ("TQ3080",), "exact", 1.0),
    Case("cellarea_100km", AREA, ("TQ",), "exact", 10000.0),
    # TQ38SW is the south-west 5 km quadrant of the 10 km square TQ38.
    Case("cellarea_quadrant", AREA, ("TQ38SW",), "exact", 25.0),
    Case("cellarea_null", AREA, (None,), "null", variants=(("XX9999",),)),
    Case("centroid_tq3080", CENT, ("TQ3080",), "geom_equals", "POINT(530500 180500)"),
    # The exact bytes (session surface only: the UC handler returns EWKB with the SRID).
    Case(
        "centroid_tq3080_bytes",
        CENT,
        ("TQ3080",),
        "wkb_bytes",
        TQ3080_CENTRE_WKB,
        surfaces=("session",),
    ),
    Case("centroid_null", CENT, (None,), "null"),
    # TQ3282 is 2 cells east and 2 north of TQ3080.
    Case("distance_diag", DIST, ("TQ3080", "TQ3282"), "exact", 4),
    Case("distance_same", DIST, ("TQ3080", "TQ3080"), "exact", 0),
    Case("distance_null", DIST, (None, "TQ3080"), "null", variants=(("TQ3080", None),)),
    Case("edistance_diag", EDIST, ("TQ3080", "TQ3282"), "exact", 2),
    Case("edistance_east", EDIST, ("TQ3080", "TQ3580"), "exact", 5),
    Case("edistance_null", EDIST, (None, "TQ3080"), "null"),
    Case("pac_point_1km", PAC, ("POINT(530500 180500)", 3), "exact", "TQ3080"),
    Case("pac_point_wkb", PAC, (POINT_WKB, 3), "exact", "TQ3080"),
    # The centroid of the 2 km square (530000..532000, 180000..182000) is (531000, 181000).
    Case(
        "pac_polygon_centroid",
        PAC,
        (
            "POLYGON((530000 180000, 532000 180000, 532000 182000, 530000 182000, 530000 180000))",
            3,
        ),
        "exact",
        "TQ3181",
    ),
    Case("pac_quadrant", PAC, ("POINT(530500 180500)", -3), "exact", "TQ38SW"),
    Case("pac_null_geom", PAC, (None, 3), "null"),
    Case("pac_empty", PAC, ("POINT EMPTY", 3), "null"),
    # The one resolution contract: a whole number, its digit string or a named size; NULL is NULL; anything
    # else raises naming the value.
    Case("bng_res_null", PAC, ("POINT(530500 180500)", None), "null"),
    Case("bng_res_3_5_raises", PAC, ("POINT(530500 180500)", 3.5), "raises", "3.5"),
    Case("bng_res_digit_string", PAC, ("POINT(530500 180500)", "3"), "exact", "TQ3080"),
    Case("bng_res_named", PAC, ("POINT(530500 180500)", "1km"), "exact", "TQ3080"),
    Case("bng_res_whole_double", PAC, ("POINT(530500 180500)", 3.0), "exact", "TQ3080"),
    Case("bng_res_bad_int", PAC, ("POINT(530500 180500)", 1001), "raises", "1001"),
    # 2**32 + 3 narrowed to INT wraps to the valid index 3: a BIGINT outside INT raises naming the value instead.
    Case(
        "bng_res_huge_bigint",
        PAC,
        ("POINT(530500 180500)", 4294967299),
        "raises",
        "4294967299",
    ),
    Case("bng_res_unknown", PAC, ("POINT(530500 180500)", "3km"), "raises", "3km"),
    # A NULL in an INT column hands a pandas batch over as floats: the valid rows around it still answer.
    # (Session only: the batch is a pandas UDF artifact.)
    Case(
        "pac_mixed_null_res",
        PAC,
        (
            ("POINT(530500 180500)", 3),
            ("POINT(530500 180500)", None),
            ("POINT(325500 673500)", 3),
        ),
        "exact_rows",
        ("TQ3080", None, "NT2573"),
        ("session",),
    ),
    # A non-finite point has no cell: NULL on every level.
    Case("bng_inf_point", PAC, (INF_POINT, 3), "null"),
    Case(
        "cellint_halves",
        CINT,
        (Build("chip_west"), Build("chip_south")),
        "cells",
        {"cells": {"TQ3080": 250000.0}, "core": 0},
    ),
    # A core chip is the whole cell: the left one comes back unchanged.
    Case(
        "cellint_left_core",
        CINT,
        (Build("chip_west_core"), Build("chip_south")),
        "cells",
        {"cells": {"TQ3080": 500000.0}, "core": 1},
    ),
    # Chips of two different cells have nothing in common: an empty chip of the left cell.
    Case(
        "cellint_other_cell",
        CINT,
        (Build("chip_west"), Build("chip_other_cell")),
        "cells",
        {"cells": {}, "count": 1, "core": 0},
    ),
    Case("cellint_null", CINT, (None, Build("chip_south")), "null"),
    Case(
        "cellunion_halves",
        CUNI,
        (Build("chip_west"), Build("chip_south")),
        "cells",
        {"cells": {"TQ3080": 750000.0}, "core": 0},
    ),
    Case(
        "cellunion_right_core",
        CUNI,
        (Build("chip_south"), Build("chip_west_core")),
        "cells",
        {"cells": {"TQ3080": 500000.0}, "core": 1},
    ),
    Case("cellunion_null", CUNI, (Build("chip_west"), None), "null"),
    # A chip with NULL bytes (the empty result of an intersection) is an empty geometry: the union is the other chip.
    Case(
        "cellunion_empty_chip",
        CUNI,
        (Build("chip_no_bytes"), Build("chip_south")),
        "cells",
        {"cells": {"TQ3080": 500000.0}, "core": 0},
    ),
)


# ---------------------------------------------------------------------------------------------- quadbin
def qb_lon(x, z):
    """The longitude of the west edge of tile column x at zoom z."""
    return x / 2**z * 360.0 - 180.0


def qb_lat(y, z):
    """The latitude of the north edge of tile row y at zoom z."""
    return math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / 2**z))))


def qb_tiles(x0, y0, x1, y1, z):
    """The WKT of the rectangle that tiles x0..x1, y0..y1 (inclusive) cover."""
    w, e, n, s = qb_lon(x0, z), qb_lon(x1 + 1, z), qb_lat(y0, z), qb_lat(y1 + 1, z)
    return f"POLYGON(({w} {s}, {e} {s}, {e} {n}, {w} {n}, {w} {s}))"


# Tile (163, 395) at zoom 10 holds San Francisco; ids are from quadbin.tile_to_cell. (The neighbours
# and rings are in grid_array.)
QB_C = 5233961839712272383
QB_E = 5233961886956912639  # the tile to the east, (164, 395)
QB_C5 = 5211447621146640383  # the zoom-5 tile that holds the same point, (5, 12)
QB_LON, QB_LAT = -122.4194, 37.7749
QB_PT = f"POINT({QB_LON} {QB_LAT})"
QB_D2 = 5233961788172664831  # two steps from QB_C

QPAC, QAWKB, QCENT, QRES = (
    "gbx_quadbin_pointascell",
    "gbx_quadbin_aswkb",
    "gbx_quadbin_centroid",
    "gbx_quadbin_resolution",
)
QDIST, QUNI = "gbx_quadbin_distance", "gbx_quadbin_cellunion"
INF = float("inf")

# ---------------------------------------------------------------------------------------------- custom grid
CUSTOM_GRID = {
    "bound_x_min": 0,
    "bound_x_max": 1000000,
    "bound_y_min": 0,
    "bound_y_max": 1000000,
    "cell_splits": 2,
    "root_cell_size_x": 1000,
    "root_cell_size_y": 1000,
    "srid": 27700,
}


@builder("custom_grid_27700")
def _custom_grid_27700(tmpdir):
    return dict(CUSTOM_GRID)


def cg_id(col, row, res):
    """The id of the cell at (col, row) of resolution res on CUSTOM_GRID: 1000 root columns, each split in two
    per resolution."""
    return row * 1000 * 2**res + col | res << 56


# The cell at column 1061, row 361 of resolution 1 (500 m cells) is the square (530500, 180500)..(531000, 181000).
CG_CELL = cg_id(1061, 361, 1)
CG_BOX = "POLYGON((530500 180500, 531000 180500, 531000 181000, 530500 181000, 530500 180500))"
CG_PT = "POINT(530750 180750)"
CGRID, CPAC, CAWKB, CAWKT, CCENT, CDIST = (
    "gbx_custom_grid",
    "gbx_custom_pointascell",
    "gbx_custom_cellaswkb",
    "gbx_custom_cellaswkt",
    "gbx_custom_centroid",
    "gbx_custom_distance",
)
G = Build("custom_grid_27700")

# ---------------------------------------------------------------------------------------------- H3
H3_CELL = 613196570331971583  # h3.latlng_to_cell(37.7749, -122.4194, 8)
H3_LNG, H3_LAT = -122.41827103692466, 37.773515097238146  # h3.cell_to_latlng
HBBOX = "gbx_h3_cell_bbox"

GRID_SCALAR += (
    # quadbin
    Case("qb_pac_z10", QPAC, (QB_LON, QB_LAT, 10), "exact", QB_C),
    Case("qb_pac_z5", QPAC, (QB_LON, QB_LAT, 5), "exact", QB_C5),
    Case("qb_pac_null_lon", QPAC, (None, QB_LAT, 10), "null"),
    # The one resolution contract, as for BNG.
    Case("quadbin_res_null", QPAC, (QB_LON, QB_LAT, None), "null"),
    Case("quadbin_res_3_5_raises", QPAC, (QB_LON, QB_LAT, 3.5), "raises", "3.5"),
    Case("quadbin_res_digit_string", QPAC, (QB_LON, QB_LAT, "10"), "exact", QB_C),
    Case(
        "quadbin_mixed_null_res",
        QPAC,
        (
            (QB_LON, QB_LAT, 10),
            (QB_LON, QB_LAT, None),
            (QB_LON, QB_LAT, 5),
        ),
        "exact_rows",
        (QB_C, None, QB_C5),
        ("session",),
    ),
    # A coordinate that is not finite has no cell: NULL on every level.
    Case(
        "quadbin_inf_point",
        QPAC,
        (INF, QB_LAT, 10),
        "null",
        variants=((QB_LON, -INF, 10), (-INF, INF, 10)),
    ),
    Case("qb_aswkb", QAWKB, (QB_C,), "geom_equals", qb_tiles(163, 395, 163, 395, 10)),
    Case("qb_aswkb_null", QAWKB, (None,), "null"),
    Case(
        "qb_centroid",
        QCENT,
        (QB_C,),
        "geom_equals",
        f"POINT({(qb_lon(163, 10) + qb_lon(164, 10)) / 2} {(qb_lat(395, 10) + qb_lat(396, 10)) / 2})",
    ),
    Case("qb_centroid_null", QCENT, (None,), "null"),
    Case("qb_resolution", QRES, (QB_C,), "exact", 10),
    Case("qb_resolution_z5", QRES, (QB_C5,), "exact", 5),
    Case("qb_resolution_null", QRES, (None,), "null"),
    Case("qb_distance_1", QDIST, (QB_C, QB_E), "exact", 1),
    Case("qb_distance_2", QDIST, (QB_C, QB_D2), "exact", 2),
    Case("qb_distance_same", QDIST, (QB_C, QB_C), "exact", 0),
    Case("qb_distance_null", QDIST, (None, QB_C), "null", variants=((QB_C, None),)),
    Case("qb_distance_mixed_zoom", QDIST, (QB_C, QB_C5), "raises", "resolution"),
    # A NULL beside real ids makes pandas hand the BIGINT column over as floats: an id past 2**53 (these are
    # ~5.2e18) must still be the same cell, not the nearest float's. (Session only: the batch is a pandas artifact.)
    Case(
        "qb_distance_big_ids_beside_null",
        QDIST,
        ((QB_C, QB_E), (None, QB_C), (QB_C, QB_D2)),
        "exact_rows",
        (1, None, 2),
        ("session",),
    ),
    Case(
        "qb_cellunion",
        QUNI,
        ([QB_C, QB_E],),
        "geom_equals",
        qb_tiles(163, 395, 164, 395, 10),
    ),
    Case("qb_cellunion_null", QUNI, (None,), "null"),
    # custom grid
    Case(
        "cg_grid",
        CGRID,
        (0, 1000000, 0, 1000000, 2, 1000, 1000, 27700),
        "exact",
        (0, 1000000, 0, 1000000, 2, 1000, 1000, 27700),
    ),
    # Seven arguments: no CRS, srid -1.
    Case(
        "cg_grid_no_srid",
        CGRID,
        (0, 1000000, 0, 1000000, 2, 1000, 1000),
        "exact",
        (0, 1000000, 0, 1000000, 2, 1000, 1000, -1),
    ),
    Case(
        "cg_grid_bad_bounds",
        CGRID,
        (1000000, 0, 0, 1000000, 2, 1000, 1000, 27700),
        "raises",
        "bound_x_max",
    ),
    Case("cg_pac_res1", CPAC, (CG_PT, G, 1), "exact", CG_CELL),
    Case("cg_pac_res0", CPAC, (CG_PT, G, 0), "exact", cg_id(530, 180, 0)),
    Case(
        "cg_pac_wkb",
        CPAC,
        (struct.pack("<BIdd", 1, 1, 530750.0, 180750.0), G, 1),
        "exact",
        CG_CELL,
    ),
    Case("cg_pac_null_geom", CPAC, (None, G, 1), "null"),
    Case("cg_pac_empty", CPAC, ("POINT EMPTY", G, 1), "null"),
    # A point beyond the grid's bounds has no cell: NULL.
    Case("cg_pac_outside", CPAC, ("POINT(2000000 180750)", G, 1), "null"),
    Case("custom_res_null", CPAC, (CG_PT, G, None), "null"),
    Case("custom_res_3_5_raises", CPAC, (CG_PT, G, 3.5), "raises", "3.5"),
    Case(
        "custom_res_huge_bigint", CPAC, (CG_PT, G, 4294967297), "raises", "4294967297"
    ),
    Case("custom_res_digit_string", CPAC, (CG_PT, G, "1"), "exact", CG_CELL),
    Case(
        "custom_mixed_null_res",
        CPAC,
        ((CG_PT, G, 1), (CG_PT, G, None), (CG_PT, G, 0)),
        "exact_rows",
        (CG_CELL, None, cg_id(530, 180, 0)),
        ("session",),
    ),
    Case("custom_inf_point", CPAC, (INF_POINT, G, 1), "null"),
    Case("cg_aswkb", CAWKB, (CG_CELL, G), "geom_equals", CG_BOX),
    # The exact bytes: the same ring start and direction as the JVM's.
    Case(
        "cg_aswkb_bytes",
        CAWKB,
        (CG_CELL, G),
        "wkb_bytes",
        _square_wkb(530500.0, 180500.0, 531000.0, 181000.0),
        surfaces=("session",),
    ),
    Case("cg_aswkb_null", CAWKB, (None, G), "null", variants=((CG_CELL, None),)),
    Case("cg_aswkt", CAWKT, (CG_CELL, G), "geom_equals", CG_BOX),
    Case("cg_aswkt_null", CAWKT, (None, G), "null"),
    Case("cg_centroid", CCENT, (CG_CELL, G), "geom_equals", CG_PT),
    Case("cg_centroid_null", CCENT, (None, G), "null"),
    Case("cg_distance", CDIST, (CG_CELL, G, cg_id(1064, 363, 1)), "exact", 3),
    Case("cg_distance_same", CDIST, (CG_CELL, G, CG_CELL), "exact", 0),
    Case("cg_distance_null", CDIST, (None, G, CG_CELL), "null"),
    # Same as qb_distance_big_ids_beside_null: custom ids are ~7.2e16, past 2**53.
    Case(
        "cg_distance_big_ids_beside_null",
        CDIST,
        (
            (CG_CELL, G, cg_id(1064, 363, 1)),
            (None, G, CG_CELL),
            (CG_CELL, G, cg_id(1062, 361, 1)),
        ),
        "exact_rows",
        (3, None, 1),
        ("session",),
    ),
    # H3
    Case(
        "h3bbox_default", HBBOX, (H3_CELL,), "approx", (H3_LNG, H3_LAT, H3_LNG, H3_LAT)
    ),
    # spatial_envelope wraps the hexagon: min and max of h3.cell_to_boundary.
    Case(
        "h3bbox_envelope",
        HBBOX,
        (H3_CELL, 4326, "spatial_envelope"),
        "approx",
        (
            -122.42419231791122,
            37.76882308826447,
            -122.41234965984782,
            37.77820687262237,
        ),
    ),
    Case("h3bbox_null", HBBOX, (None,), "null"),
)

# --- Unity Catalog rows: the GEOMETRY argument arrives as a value with an integer SRID, never as bytes or text.

GRID_SCALAR = uc_view(
    GRID_SCALAR,
    {PAC: 27700},
    session_only=("pac_point_wkb", "bng_inf_point"),
)

GRID_SCALAR += (
    # A WKB BINARY is not a GEOMETRY: the engine refuses it for a GEOMETRY parameter (never a silent NULL).
    Case(
        "uc_wkb_into_geometry_refused",
        PAC,
        (POINT_WKB, 3),
        "raises",
        "BINARY",  # the handler's message, and the engine's (a BINARY where GEOMETRY(ANY) is required)
        ("uc",),
        geom_srid=27700,
    ),
    # BNG cells are EPSG:27700: a geometry in another SRID is refused with a Fix:, not read as metres.
    Case(
        "uc_pac_wrong_srid",
        PAC,
        ("POINT(-0.1 51.5)", 3),
        "uc_refused",
        None,
        ("uc",),
        geom_srid=4326,
    ),
    # A UC BINARY result carries the integer SRID: EWKB with 27700 (the session's WKB carries none).
    Case("uc_bng_aswkb_srid", AWKB, ("TQ3080",), "geom_srid", (TQ3080, 27700), ("uc",)),
    Case(
        "uc_bng_centroid_srid",
        CENT,
        ("TQ3080",),
        "geom_srid",
        ("POINT(530500 180500)", 27700),
        ("uc",),
    ),
)
