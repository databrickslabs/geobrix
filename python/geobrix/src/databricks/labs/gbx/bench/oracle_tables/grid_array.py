"""Oracle table for the grid arrays (BNG): inputs -> expected, run at every level by test_family_cross_tier.py.

Expected values come from the fixtures' definitions or an independent library, never from the function
under test. A 1 km BNG cell inside the 100 km square `TQ` (south-west corner (500000, 100000)) is named
`TQ<ee><nn>` from its south-west corner in kilometres, so its neighbours are counted out by hand below.
"""

import struct

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
    # A non-finite point has no cell: NULL on every level.
    Case("bng_inf_point_array", GKR, (INF_POINT, 3, 1), "null"),
)
