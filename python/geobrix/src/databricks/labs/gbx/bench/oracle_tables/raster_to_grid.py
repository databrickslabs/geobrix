"""Oracle table for the raster-to-grid reducers (`gbx_rst_<grid>_rastertogrid<stat>`, 32 functions): inputs ->
expected, run at every level by test_family_cross_tier.py.

Expected values come from the fixtures' definitions or an independent library, never from the function under
test. A pixel belongs to the cell its centre is in: the h3 and quadbin libraries, the BNG reference squares and the
custom grid's numbering say which, and numpy reduces the valid pixels of each band over each cell (see
`oracle_checks.check_grid_stats`). Fixtures: `rtg_ll` is a two-band 80 x 60 Float32 raster on a 0.01 degree grid at
(-1, 52) in EPSG:4326; `rtg_bng` a two-band 40 x 30 one on a 100 m grid at (530000, 183000) in EPSG:27700. Both
carry NoData holes (a different set in each band), so every band is judged on its own valid pixels.

The grids differ in the cell id: H3, quadbin and custom ids are BIGINT, far past 2**53 (they never meet a float),
BNG ids are strings. One function takes the error-contract rows of its grid, the others one value row per
statistic. The functions are table functions (rows of band, cellID, measure) at every level; the JVM level
(an ARRAY scalar) is not part of the contract.
"""

from databricks.labs.gbx.bench.oracle_tables.grid_scalar import G
from databricks.labs.gbx.bench.oracles import Build, Case, _gtiff, _tile, builder

STATS = ("avg", "count", "max", "median", "min", "stddev", "sum", "variance")
ONLY = ("session",)
LL, BNG = Build("rtg_ll"), Build("rtg_bng")
ERR, CORRUPT = Build("tile_error"), Build("tile_corrupt")


def _bands(h, w):
    """Two bands: a modular ramp with NoData where x + y is a multiple of 11, and a scaled one with NoData where
    2 x + y is a multiple of 13."""
    import numpy as np

    y, x = np.mgrid[0:h, 0:w].astype("float64")
    one = (x * 7 + y * 13) % 17 + 1
    two = (x * 3 + y * 5) % 23 * 1.5 + 0.25
    one[(x + y) % 11 == 0] = -9999.0
    two[(2 * x + y) % 13 == 0] = -9999.0
    return np.stack([one, two])


@builder("rtg_ll")
def _rtg_ll(tmpdir):
    return _tile(_gtiff(_bands(60, 80), "EPSG:4326", origin=(-1.0, 52.0), px=0.01))


@builder("rtg_bng")
def _rtg_bng(tmpdir):
    return _tile(
        _gtiff(_bands(30, 40), "EPSG:27700", origin=(530000.0, 183000.0), px=100.0)
    )


# (grid, SQL prefix, the tile, the arguments between tile and coverage, the resolution as an index)
_GRIDS = (
    ("h3", "gbx_rst_h3_rastertogrid", LL, (5,), 5),
    ("quadbin", "gbx_rst_quadbin_rastertogrid", LL, (11,), 11),
    ("bng", "gbx_rst_bng_rastertogrid", BNG, (3,), 3),
    ("custom", "gbx_rst_custom_rastertogrid", BNG, (G, 1), 1),
)
_SHORT = {"h3": "h3", "quadbin": "qb", "bng": "bng", "custom": "cg"}
_PREFIX = {g: p for g, p, *_ in _GRIDS}
_ARGS = {g: (t, *a) for g, _, t, a, _ in _GRIDS}
_RES = {g: r for g, *_, r in _GRIDS}


def _stat_rows():
    """One value row per (grid, statistic): the default coverage and assignment, so the example is the plain call."""
    return tuple(
        Case(
            f"{_SHORT[grid]}_rtg_{stat}",
            f"{prefix}{stat}",
            _ARGS[grid],
            "grid_stats",
            {"grid": grid, "res": _RES[grid], "stat": stat},
            ONLY,
        )
        for grid, prefix, *_ in _GRIDS
        for stat in STATS
    )


def _fn(grid, stat):
    return f"{_PREFIX[grid]}{stat}"


RASTER_TO_GRID = _stat_rows() + (
    # ---------------------------------------------------------------------------------------------- coverage
    # sparse: exactly the cells that hold a valid pixel. quadbin ids are far past 2**53: they must come out exact
    Case(
        "qb_rtg_cellid_past_2_53",
        _fn("quadbin", "sum"),
        (LL, 11, "sparse"),
        "grid_stats",
        {
            "grid": "quadbin",
            "res": 11,
            "stat": "sum",
            "coverage": "sparse",
            "past_2_53": True,
        },
        ONLY,
    ),
    Case(
        "cg_rtg_cellid_past_2_53",
        _fn("custom", "count"),
        (BNG, G, 1, "sparse"),
        "grid_stats",
        {
            "grid": "custom",
            "res": 1,
            "stat": "count",
            "coverage": "sparse",
            "past_2_53": True,
        },
        ONLY,
    ),
    Case(
        "h3_rtg_sparse",
        _fn("h3", "avg"),
        (LL, 5, "sparse"),
        "grid_stats",
        {"grid": "h3", "res": 5, "stat": "avg", "coverage": "sparse"},
        ONLY,
    ),
    # a BNG size names its index
    Case(
        "bng_rtg_named_res",
        _fn("bng", "count"),
        (BNG, "1km"),
        "grid_stats",
        {"grid": "bng", "res": 3, "stat": "count"},
        ONLY,
    ),
    # covering: every cell a pixel overlaps gets its share, so the counts add up to the valid pixels
    Case(
        "qb_rtg_count_covering",
        _fn("quadbin", "count"),
        (LL, 11, "complete", "covering"),
        "grid_stats",
        {"grid": "quadbin", "res": 11, "stat": "count", "assignment": "covering"},
        ONLY,
    ),
    # ---------------------------------------------------------------------------------------------- error contract
    Case("h3_rtg_null_tile", _fn("h3", "avg"), (None, 5), "no_rows", None, ONLY),
    Case("h3_rtg_null_res", _fn("h3", "avg"), (LL, None), "no_rows", None, ONLY),
    Case("h3_rtg_error_tile", _fn("h3", "avg"), (ERR, 5), "no_rows", None, ONLY),
    Case("h3_rtg_corrupt", _fn("h3", "avg"), (CORRUPT, 5), "no_rows", None, ONLY),
    Case(
        "h3_rtg_bad_coverage",
        _fn("h3", "avg"),
        (LL, 5, "sideways"),
        "raises",
        "sideways",
        ONLY,
    ),
    Case(
        "h3_rtg_bad_assignment",
        _fn("h3", "avg"),
        (LL, 5, "complete", "sideways"),
        "raises",
        "sideways",
        ONLY,
    ),
    # a bad resolution raises naming it, also for a tile that is an error tile
    Case("h3_rtg_bad_res", _fn("h3", "avg"), (LL, 16), "raises", "16", ONLY),
    Case(
        "h3_rtg_bad_res_error_tile", _fn("h3", "min"), (ERR, 16), "raises", "16", ONLY
    ),
    Case("qb_rtg_bad_res", _fn("quadbin", "avg"), (LL, 21), "raises", "21", ONLY),
    Case("bng_rtg_bad_res", _fn("bng", "avg"), (BNG, "3km"), "raises", "3km", ONLY),
    Case("cg_rtg_bad_res", _fn("custom", "avg"), (BNG, G, 99), "raises", "99", ONLY),
    Case(
        "cg_rtg_null_grid", _fn("custom", "avg"), (BNG, None, 1), "no_rows", None, ONLY
    ),
    Case(
        "cg_rtg_negative_res", _fn("custom", "avg"), (BNG, G, -1), "raises", "-1", ONLY
    ),
    # the custom-grid function has its own eval(tile, grid, resolution, ...): an error tile and unreadable bytes
    # give no rows there too
    Case("cg_rtg_error_tile", _fn("custom", "avg"), (ERR, G, 1), "no_rows", None, ONLY),
    Case(
        "cg_rtg_corrupt", _fn("custom", "avg"), (CORRUPT, G, 1), "no_rows", None, ONLY
    ),
)
