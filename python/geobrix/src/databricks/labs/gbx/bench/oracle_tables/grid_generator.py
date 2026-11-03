"""Oracle table for the grid explodes (BNG, quadbin, custom grid, H3): inputs -> expected, run at every level by
test_family_cross_tier.py.

Expected values come from the fixtures' definitions or an independent library, never from the function under
test. Each explode returns the cells of the array function of the same name, one row per cell, so the expected
cell sets are the ones `grid_array.py` counts out by hand (BNG), takes from `quadbin.tile_to_cell` and
`h3.grid_disk` / `h3.grid_ring`, or builds from the custom grid's cell numbering. The quadbin and H3 ids are
past 2**53: a row that went through a float would round them.
"""

from databricks.labs.gbx.bench.oracle_tables.grid_array import (
    CG0_PT,
    CG_LOOP1,
    CG_RING1,
    H3_LOOP1,
    H3_RING1,
    INF_POINT,
    POINT,
    QB_LOOP1,
    QB_RING1,
    _square,
    _tq,
)
from databricks.labs.gbx.bench.oracle_tables.grid_scalar import QB_PT, G
from databricks.labs.gbx.bench.oracles import Big, Case, uc_view

BKR, BKL = "gbx_bng_kringexplode", "gbx_bng_kloopexplode"
BGKR, BGKL = "gbx_bng_geomkringexplode", "gbx_bng_geomkloopexplode"
QGKR, QGKL = "gbx_quadbin_geomkringexplode", "gbx_quadbin_geomkloopexplode"
CGKR, CGKL = "gbx_custom_geomkringexplode", "gbx_custom_geomkloopexplode"
HGKR, HGKL = "gbx_h3_geomkringexplode", "gbx_h3_geomkloopexplode"

# A 3 km x 3 km polygon on the cell edges covers the 3 x 3 cells (529..531, 179..181) of 1 km; one ring of dilation
# makes the 5 x 5 block around them. The band (boundary-out) leaves the middle cell out, and the loop at distance
# 1 is the 16 cells of the block's edge.
POLY3 = "POLYGON((529000 179000, 529000 182000, 532000 182000, 532000 179000, 529000 179000))"
POLY3_BAND = [c for c in _square(530, 180, 2, False) if c != _tq(530, 180)]
POLY3_LOOP = _square(530, 180, 2, True)

GRID_GENERATOR = (
    # ---------------------------------------------------------------------------------------------- BNG cells
    Case("kringx_k1", BKR, ("TQ3080", 1), "cell_rows", _square(530, 180, 1, False)),
    Case("kringx_k0", BKR, ("TQ3080", 0), "cell_rows", ["TQ3080"]),
    Case("kringx_null", BKR, (None, 1), "no_rows"),
    Case("kringx_null_k", BKR, ("TQ3080", None), "no_rows"),
    # A malformed cell id is a data error: no rows, not a failed query.
    Case("kringx_bad_cell", BKR, ("XX9999", 1), "no_rows"),
    Case("kloopx_k1", BKL, ("TQ3080", 1), "cell_rows", _square(530, 180, 1, True)),
    Case("kloopx_k2", BKL, ("TQ3080", 2), "cell_rows", _square(530, 180, 2, True)),
    Case("kloopx_null", BKL, (None, 1), "no_rows"),
    # ---------------------------------------------------------------------------------------------- BNG geometry
    # A point inside TQ3080 covers that cell; one ring of dilation adds its eight neighbours.
    Case(
        "geomkringx_point_k1",
        BGKR,
        (POINT, 3, 1),
        "cell_rows",
        _square(530, 180, 1, False),
    ),
    Case(
        "geomkringx_mode_coverage",
        BGKR,
        (POINT, 3, 1, "boundary-out", "coveras"),
        "cell_rows",
        _square(530, 180, 1, False),
    ),
    Case(
        "geomkringx_digit_res",
        BGKR,
        (POINT, "3", 1),
        "cell_rows",
        _square(530, 180, 1, False),
    ),
    # A PySpark integer column is a BIGINT: the JVM level reads k as a whole number of any width.
    Case(
        "geomkringx_bigint_k",
        BGKR,
        (POINT, 3, Big(1)),
        "cell_rows",
        _square(530, 180, 1, False),
    ),
    Case("geomkringx_bad_mode", BGKR, (POINT, 3, 1, "sideways"), "raises", "sideways"),
    Case(
        "geomkringx_bad_coverage",
        BGKR,
        (POINT, 3, 1, "boundary-out", "sideways"),
        "raises",
        "sideways",
    ),
    Case("geomkringx_res_3_5", BGKR, (POINT, 3.5, 1), "raises", "3.5"),
    Case("geomkringx_null", BGKR, (None, 3, 1), "no_rows"),
    Case("geomkringx_null_res", BGKR, (POINT, None, 1), "no_rows"),
    # A non-finite point has no cell, and a geometry that cannot be read has none: no rows on every level.
    Case("geomkringx_inf_point", BGKR, (INF_POINT, 3, 1), "no_rows"),
    Case("geomkringx_bad_geom", BGKR, ("not a geometry", 3, 1), "no_rows"),
    Case(
        "geomkloopx_point_k1",
        BGKL,
        (POINT, 3, 1),
        "cell_rows",
        _square(530, 180, 1, True),
    ),
    Case("geomkringx_polygon_k1", BGKR, (POLY3, 3, 1), "cell_rows", POLY3_BAND),
    Case(
        "geomkringx_polygon_boundary_out",
        BGKR,
        (POLY3, 3, 1, "boundary-out"),
        "cell_rows",
        POLY3_BAND,
    ),
    Case("geomkloopx_polygon_k1", BGKL, (POLY3, 3, 1), "cell_rows", POLY3_LOOP),
    Case(
        "geomkloopx_polygon_boundary_out",
        BGKL,
        (POLY3, 3, 1, "boundary-out"),
        "cell_rows",
        POLY3_LOOP,
    ),
    Case("geomkloopx_null_k", BGKL, (POINT, 3, None), "no_rows"),
    Case("geomkloopx_bad_mode", BGKL, (POINT, 3, 1, "sideways"), "raises", "sideways"),
    Case("geomkloopx_res_3_5", BGKL, (POINT, 3.5, 1), "raises", "3.5"),
    # ---------------------------------------------------------------------------------------------- quadbin
    Case("qb_geomkringx_point_k1", QGKR, (QB_PT, 10, 1), "cell_rows", QB_RING1),
    Case(
        "qb_geomkringx_mode_coverage",
        QGKR,
        (QB_PT, 10, 1, "boundary-out", "coveras"),
        "cell_rows",
        QB_RING1,
    ),
    Case("qb_geomkringx_digit_res", QGKR, (QB_PT, "10", 1), "cell_rows", QB_RING1),
    Case(
        "qb_geomkringx_bad_mode",
        QGKR,
        (QB_PT, 10, 1, "sideways"),
        "raises",
        "sideways",
    ),
    Case("qb_geomkringx_res_3_5", QGKR, (QB_PT, 3.5, 1), "raises", "3.5"),
    Case("qb_geomkringx_null", QGKR, (None, 10, 1), "no_rows"),
    Case("qb_geomkringx_inf_point", QGKR, (INF_POINT, 10, 1), "no_rows"),
    Case("qb_geomkloopx_point_k1", QGKL, (QB_PT, 10, 1), "cell_rows", QB_LOOP1),
    Case("qb_geomkloopx_null_k", QGKL, (QB_PT, 10, None), "no_rows"),
    Case(
        "qb_geomkloopx_bad_mode",
        QGKL,
        (QB_PT, 10, 1, "sideways"),
        "raises",
        "sideways",
    ),
    # ---------------------------------------------------------------------------------------------- custom grid
    Case("cg_geomkringx_point_k1", CGKR, (CG0_PT, G, 0, 1), "cell_rows", CG_RING1),
    Case("cg_geomkringx_digit_res", CGKR, (CG0_PT, G, "0", 1), "cell_rows", CG_RING1),
    Case(
        "cg_geomkringx_bad_mode",
        CGKR,
        (CG0_PT, G, 0, 1, "sideways"),
        "raises",
        "sideways",
    ),
    Case(
        "cg_geomkringx_bad_coverage",
        CGKR,
        (CG0_PT, G, 0, 1, "boundary-out", "sideways"),
        "raises",
        "sideways",
    ),
    Case("cg_geomkringx_res_3_5", CGKR, (CG0_PT, G, 3.5, 1), "raises", "3.5"),
    Case("cg_geomkringx_null", CGKR, (None, G, 0, 1), "no_rows"),
    Case("cg_geomkringx_null_grid", CGKR, (CG0_PT, None, 0, 1), "no_rows"),
    Case("cg_geomkringx_inf_point", CGKR, (INF_POINT, G, 0, 1), "no_rows"),
    Case("cg_geomkloopx_point_k1", CGKL, (CG0_PT, G, 0, 1), "cell_rows", CG_LOOP1),
    Case("cg_geomkloopx_null_k", CGKL, (CG0_PT, G, 0, None), "no_rows"),
    Case(
        "cg_geomkloopx_bad_mode",
        CGKL,
        (CG0_PT, G, 0, 1, "sideways"),
        "raises",
        "sideways",
    ),
    # ---------------------------------------------------------------------------------------------- H3
    Case("h3_geomkringx_point_k1", HGKR, (QB_PT, 8, 1), "cell_rows", H3_RING1),
    Case("h3_geomkringx_digit_res", HGKR, (QB_PT, "8", 1), "cell_rows", H3_RING1),
    Case(
        "h3_geomkringx_bad_mode", HGKR, (QB_PT, 8, 1, "sideways"), "raises", "sideways"
    ),
    Case("h3_geomkringx_res_3_5", HGKR, (QB_PT, 3.5, 1), "raises", "3.5"),
    Case("h3_geomkringx_null", HGKR, (None, 8, 1), "no_rows"),
    Case("h3_geomkringx_inf_point", HGKR, (INF_POINT, 8, 1), "no_rows"),
    Case("h3_geomkloopx_point_k1", HGKL, (QB_PT, 8, 1), "cell_rows", H3_LOOP1),
    Case("h3_geomkloopx_null_k", HGKL, (QB_PT, 8, None), "no_rows"),
    Case(
        "h3_geomkloopx_bad_mode", HGKL, (QB_PT, 8, 1, "sideways"), "raises", "sideways"
    ),
)

GRID_GENERATOR = uc_view(
    GRID_GENERATOR,
    {BGKR: 27700},
    session_only=(
        "geomkringx_bigint_k",  # a BIGINT k: the UC table function declares k INT
        "geomkringx_inf_point",
        "geomkringx_bad_geom",
        "h3_geomkringx_inf_point",
    ),
)

# --- Unity Catalog rows: a grid is defined in one SRID, and a geometry in another is refused with a Fix:.
GRID_GENERATOR += (
    Case(
        "uc_geomkringx_wrong_srid",
        BGKR,
        (POINT, 3, 1),
        "uc_refused",
        None,
        ("uc",),
        geom_srid=4326,
    ),
    Case(
        "uc_h3_geomkringx_wrong_srid",
        HGKR,
        (QB_PT, 8, 1),
        "uc_refused",
        None,
        ("uc",),
        geom_srid=27700,
    ),
)
