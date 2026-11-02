"""Oracle table for the grid scalars (BNG): inputs -> expected, run at every level by test_family_cross_tier.py.

Expected values come from the fixtures' definitions or an independent library, never from the function
under test. A BNG cell is a square of EPSG:27700 metres: `TQ3080` is the 1 km square whose south-west corner
is (530000, 180000), since the 100 km square `TQ` starts at (500000, 100000).
"""

import struct

from databricks.labs.gbx.bench.oracles import Build, Case, builder

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

GRID_SCALAR = (
    Case("aswkb_tq3080", AWKB, ("TQ3080",), "geom_equals", TQ3080),
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
)
