"""Oracle table for the grid arrays (BNG, quadbin, custom grid, H3): inputs -> expected, run at every level by
test_family_cross_tier.py.

Expected values come from the fixtures' definitions or an independent library, never from the function
under test. A 1 km BNG cell inside the 100 km square `TQ` (south-west corner (500000, 100000)) is named
`TQ<ee><nn>` from its south-west corner in kilometres, so its neighbours are counted out by hand below.
"""

import struct

from databricks.labs.gbx.bench.oracle_tables.grid_scalar import (
    QB_C,
    QB_E,
    QB_PT,
    G,
    cg_id,
)
from databricks.labs.gbx.bench.oracles import Case


def _tq(e_km: int, n_km: int) -> str:
    """The 1 km cell whose south-west corner is (e_km, n_km) kilometres, inside TQ."""
    return f"TQ{e_km - 500:02d}{n_km - 100:02d}"


def _square(e_km: int, n_km: int, k: int, hollow: bool):
    """The cells within ring distance k of the cell at (e_km, n_km) (only those at exactly k when hollow)."""
    return [
        _tq(e_km + dx, n_km + dy)
        for dx in range(-k, k + 1)
        for dy in range(-k, k + 1)
        if not hollow or max(abs(dx), abs(dy)) == k
    ]


SQ2 = "POLYGON((530000 180000, 532000 180000, 532000 182000, 530000 182000, 530000 180000))"
SQ2_CELLS = [_tq(530, 180), _tq(531, 180), _tq(530, 181), _tq(531, 181)]
POINT = "POINT(530500 180500)"
INF_POINT = struct.pack("<BIdd", 1, 1, float("inf"), float("inf"))

KR, KL, PF = "gbx_bng_kring", "gbx_bng_kloop", "gbx_bng_polyfill"
GKR, GKL = "gbx_bng_geomkring", "gbx_bng_geomkloop"

GRID_ARRAY = (
    Case("kring_k1", KR, ("TQ3080", 1), "cell_set", _square(530, 180, 1, False)),
    Case("kring_k0", KR, ("TQ3080", 0), "cell_set", ["TQ3080"]),
    Case("kring_k2", KR, ("TQ3080", 2), "cell_set", _square(530, 180, 2, False)),
    Case("kring_null", KR, (None, 1), "null", variants=(("TQ3080", None),)),
    # A malformed cell id is a data error: NULL, not a failed query.
    Case("kring_bad_cell", KR, ("XX9999", 1), "null"),
    Case("kloop_k1", KL, ("TQ3080", 1), "cell_set", _square(530, 180, 1, True)),
    Case("kloop_k2", KL, ("TQ3080", 2), "cell_set", _square(530, 180, 2, True)),
    Case("kloop_null", KL, (None, 1), "null"),
    # The 2 km square holds the centres of four 1 km cells.
    Case("polyfill_2x2", PF, (SQ2, 3), "cell_set", SQ2_CELLS),
    Case("polyfill_named_res", PF, (SQ2, "1km"), "cell_set", SQ2_CELLS),
    Case("polyfill_digit_res", PF, (SQ2, "3"), "cell_set", SQ2_CELLS),
    Case("polyfill_null", PF, (None, 3), "null"),
    Case("polyfill_bad_geom", PF, ("not a geometry", 3), "null"),
    # The one resolution contract, on an array function.
    Case("bng_res_3_5_raises_array", PF, (SQ2, 3.5), "raises", "3.5"),
    Case("polyfill_res_null", PF, (SQ2, None), "null"),
    # A point inside TQ3080 covers that cell; one ring of dilation adds its eight neighbours (mode boundary-out).
    Case(
        "geomkring_point_k1",
        GKR,
        (POINT, 3, 1),
        "cell_set",
        _square(530, 180, 1, False),
    ),
    Case(
        "geomkring_mode_coverage",
        GKR,
        (POINT, 3, 1, "boundary-out", "coveras"),
        "cell_set",
        _square(530, 180, 1, False),
    ),
    Case(
        "geomkring_digit_res",
        GKR,
        (POINT, "3", 1),
        "cell_set",
        _square(530, 180, 1, False),
    ),
    Case("geomkring_bad_mode", GKR, (POINT, 3, 1, "sideways"), "raises", "sideways"),
    Case("geomkring_null", GKR, (None, 3, 1), "null"),
    Case(
        "geomkloop_point_k1", GKL, (POINT, 3, 1), "cell_set", _square(530, 180, 1, True)
    ),
    Case("geomkloop_null", GKL, (POINT, 3, None), "null"),
    Case("geomkloop_bad_mode", GKL, (POINT, 3, 1, "sideways"), "raises", "sideways"),
    Case("geomkloop_res_3_5", GKL, (POINT, 3.5, 1), "raises", "3.5"),
    # A non-finite point has no cell: NULL on every level.
    Case("bng_inf_point_array", GKR, (INF_POINT, 3, 1), "null"),
)


# ---------------------------------------------------------------------------------------------- quadbin
# The 3x3 tiles around tile (163, 395) at zoom 10, ids from quadbin.tile_to_cell.
QB_RING1 = [
    5233961826827370495,
    5233961835417305087,
    5233961929906585599,
    5233961831122337791,
    QB_C,
    5233961934201552895,
    5233961878366978047,
    5233961886956912639,
    5233961981446193151,
]
QB_LOOP1 = [c for c in QB_RING1 if c != QB_C]
QB_LOOP2 = [  # exactly two tiles away: the 5x5 block's outer ring
    5233961788172664831,
    5233961813942468607,
    5233961822532403199,
    5233961917021683711,
    5233961925611618303,
    5233961801057566719,
    5233961938496520191,
    5233961805352534015,
    5233961942791487487,
    5233961852597174271,
    5233961990036127743,
    5233961856892141567,
    5233961882661945343,
    5233961891251879935,
    5233961985741160447,
    5233961994331095039,
]
QB_SOUTH = 5233961934201552895  # tile (163, 396)
# A rectangle inside tile (163, 395); one across tiles 163 and 164 of row 395; and a triangle whose long side
# (from (-122.2, 37.9) to (-122.5, 37.6)) passes north-west of tile (164, 396), so it touches three of the four
# tiles its bounding box covers.
QB_IN = "POLYGON((-122.6 37.8, -122.5 37.8, -122.5 37.9, -122.6 37.9, -122.6 37.8))"
QB_SPAN = "POLYGON((-122.5 37.8, -122.3 37.8, -122.3 37.9, -122.5 37.9, -122.5 37.8))"
QB_TRIANGLE = "POLYGON((-122.5 37.9, -122.2 37.9, -122.5 37.6, -122.5 37.9))"

QKR, QKL, QPF, QTS = (
    "gbx_quadbin_kring",
    "gbx_quadbin_kloop",
    "gbx_quadbin_polyfill",
    "gbx_quadbin_tessellate",
)
QGKR, QGKL = "gbx_quadbin_geomkring", "gbx_quadbin_geomkloop"

GRID_ARRAY += (
    Case("qb_kring_k1", QKR, (QB_C, 1), "cell_set", QB_RING1),
    Case("qb_kring_k0", QKR, (QB_C, 0), "cell_set", [QB_C]),
    Case("qb_kring_null", QKR, (None, 1), "null", variants=((QB_C, None),)),
    Case("qb_kring_negative_k", QKR, (QB_C, -1), "raises", "-1"),
    Case("qb_kloop_k1", QKL, (QB_C, 1), "cell_set", QB_LOOP1),
    Case("qb_kloop_k2", QKL, (QB_C, 2), "cell_set", QB_LOOP2),
    Case("qb_kloop_k0", QKL, (QB_C, 0), "cell_set", [QB_C]),
    Case("qb_kloop_null", QKL, (None, 1), "null"),
    Case("qb_polyfill_inside", QPF, (QB_IN, 10), "cell_set", [QB_C]),
    Case("qb_polyfill_span", QPF, (QB_SPAN, 10), "cell_set", [QB_C, QB_E]),
    Case("qb_polyfill_null", QPF, (None, 10), "null"),
    Case("qb_polyfill_bad_geom", QPF, ("not a geometry", 10), "null"),
    Case("quadbin_res_null_array", QPF, (QB_IN, None), "null"),
    Case("quadbin_res_3_5_raises_array", QPF, (QB_IN, 3.5), "raises", "3.5"),
    Case("quadbin_res_digit_string_array", QPF, (QB_IN, "10"), "cell_set", [QB_C]),
    # The chips are the cells the geometry really touches: the bounding box covers four tiles, the triangle three.
    Case(
        "qb_tessellate_triangle",
        QTS,
        (QB_TRIANGLE, 10),
        "cell_set",
        [QB_C, QB_E, QB_SOUTH],
    ),
    Case("qb_tessellate_null", QTS, (None, 10), "null"),
    Case("qb_geomkring_point_k1", QGKR, (QB_PT, 10, 1), "cell_set", QB_RING1),
    Case(
        "qb_geomkring_mode_coverage",
        QGKR,
        (QB_PT, 10, 1, "boundary-out", "coveras"),
        "cell_set",
        QB_RING1,
    ),
    Case(
        "qb_geomkring_bad_mode", QGKR, (QB_PT, 10, 1, "sideways"), "raises", "sideways"
    ),
    Case("qb_geomkring_null", QGKR, (None, 10, 1), "null"),
    Case("qb_geomkloop_point_k1", QGKL, (QB_PT, 10, 1), "cell_set", QB_LOOP1),
    Case("qb_geomkloop_null", QGKL, (QB_PT, 10, None), "null"),
    Case(
        "qb_geomkloop_bad_mode", QGKL, (QB_PT, 10, 1, "sideways"), "raises", "sideways"
    ),
    Case("qb_geomkloop_res_3_5", QGKL, (QB_PT, 3.5, 1), "raises", "3.5"),
    Case("quadbin_res_null_geomk", QGKR, (QB_PT, None, 1), "null"),
    Case("quadbin_res_3_5_raises_geomk", QGKR, (QB_PT, 3.5, 1), "raises", "3.5"),
    Case(
        "quadbin_res_digit_string_geomk", QGKR, (QB_PT, "10", 1), "cell_set", QB_RING1
    ),
    Case("quadbin_inf_point_geomk", QGKR, (INF_POINT, 10, 1), "null"),
)

# ---------------------------------------------------------------------------------------------- custom grid
# Cell (530, 180) of resolution 0 (1 km cells) holds POINT(530500 180500); (529..531, 179..181) is its 3x3 block.
CG0 = cg_id(530, 180, 0)
CG0_PT = "POINT(530500 180500)"
CG_RING1 = [cg_id(c, r, 0) for c in (529, 530, 531) for r in (179, 180, 181)]
CG_LOOP1 = [c for c in CG_RING1 if c != CG0]
# The 2 km square (530000..532000, 180000..182000) holds the centres of four 1 km cells.
CG_SQ2 = "POLYGON((530000 180000, 532000 180000, 532000 182000, 530000 182000, 530000 180000))"
CG_SQ2_CELLS = [cg_id(c, r, 0) for c in (530, 531) for r in (180, 181)]

CKR, CKL, CPF = "gbx_custom_kring", "gbx_custom_kloop", "gbx_custom_polyfill"
CGKR, CGKL = "gbx_custom_geomkring", "gbx_custom_geomkloop"

GRID_ARRAY += (
    Case("cg_kring_k1", CKR, (CG0, G, 1), "cell_set", CG_RING1),
    Case("cg_kring_k0", CKR, (CG0, G, 0), "cell_set", [CG0]),
    Case("cg_kring_null", CKR, (None, G, 1), "null", variants=((CG0, G, None),)),
    Case("cg_kloop_k1", CKL, (CG0, G, 1), "cell_set", CG_LOOP1),
    Case("cg_kloop_k0", CKL, (CG0, G, 0), "cell_set", [CG0]),
    Case("cg_kloop_null", CKL, (None, G, 1), "null", variants=((CG0, G, None),)),
    Case("cg_polyfill_2x2", CPF, (CG_SQ2, G, 0), "cell_set", CG_SQ2_CELLS),
    Case("cg_polyfill_null", CPF, (None, G, 0), "null"),
    Case("cg_polyfill_bad_geom", CPF, ("not a geometry", G, 0), "null"),
    Case("custom_res_null_array", CPF, (CG_SQ2, G, None), "null"),
    Case("custom_res_3_5_raises_array", CPF, (CG_SQ2, G, 3.5), "raises", "3.5"),
    Case(
        "custom_res_digit_string_array", CPF, (CG_SQ2, G, "0"), "cell_set", CG_SQ2_CELLS
    ),
    Case("cg_geomkring_point_k1", CGKR, (CG0_PT, G, 0, 1), "cell_set", CG_RING1),
    Case(
        "cg_geomkring_bad_mode",
        CGKR,
        (CG0_PT, G, 0, 1, "sideways"),
        "raises",
        "sideways",
    ),
    Case("cg_geomkring_null", CGKR, (None, G, 0, 1), "null"),
    Case("cg_geomkloop_point_k1", CGKL, (CG0_PT, G, 0, 1), "cell_set", CG_LOOP1),
    Case("cg_geomkloop_null", CGKL, (CG0_PT, G, 0, None), "null"),
    Case(
        "cg_geomkloop_bad_mode",
        CGKL,
        (CG0_PT, G, 0, 1, "sideways"),
        "raises",
        "sideways",
    ),
    Case("cg_geomkloop_res_3_5", CGKL, (CG0_PT, G, 3.5, 1), "raises", "3.5"),
    Case("custom_res_null_geomk", CGKR, (CG0_PT, G, None, 1), "null"),
    Case("custom_res_3_5_raises_geomk", CGKR, (CG0_PT, G, 3.5, 1), "raises", "3.5"),
    Case(
        "custom_res_digit_string_geomk", CGKR, (CG0_PT, G, "0", 1), "cell_set", CG_RING1
    ),
    Case("custom_inf_point", CGKR, (INF_POINT, G, 0, 1), "null"),
)

# ---------------------------------------------------------------------------------------------- H3
# h3.latlng_to_cell(37.7749, -122.4194, 8), its h3.grid_disk(.., 1) and h3.grid_ring(.., 1), as integers.
H3_RING1 = [
    613196570331971583,
    613196570334068735,
    613196570336165887,
    613196570338263039,
    613196570340360191,
    613196570342457343,
    613196570344554495,
]
H3_LOOP1 = H3_RING1[1:]
HGKR, HGKL = "gbx_h3_geomkring", "gbx_h3_geomkloop"

GRID_ARRAY += (
    Case("h3_geomkring_point_k1", HGKR, (QB_PT, 8, 1), "cell_set", H3_RING1),
    Case(
        "h3_geomkring_bad_mode", HGKR, (QB_PT, 8, 1, "sideways"), "raises", "sideways"
    ),
    Case("h3_geomkring_null", HGKR, (None, 8, 1), "null"),
    Case("h3_geomkloop_point_k1", HGKL, (QB_PT, 8, 1), "cell_set", H3_LOOP1),
    Case("h3_geomkloop_null", HGKL, (QB_PT, 8, None), "null"),
    Case(
        "h3_geomkloop_bad_mode", HGKL, (QB_PT, 8, 1, "sideways"), "raises", "sideways"
    ),
    Case("h3_geomkloop_res_3_5", HGKL, (QB_PT, 3.5, 1), "raises", "3.5"),
    Case("h3_res_null", HGKR, (QB_PT, None, 1), "null"),
    Case("h3_res_3_5_raises", HGKR, (QB_PT, 3.5, 1), "raises", "3.5"),
    Case("h3_res_digit_string", HGKR, (QB_PT, "8", 1), "cell_set", H3_RING1),
    Case("h3_inf_point", HGKR, (INF_POINT, 8, 1), "null"),
)
