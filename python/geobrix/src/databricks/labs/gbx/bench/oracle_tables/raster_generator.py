"""Oracle table for the raster generators that split or cut a tile (maketiles, separatebands, tooverlappingtiles and
the four grid tessellations): inputs -> expected, run at every level by test_family_cross_tier.py.

Expected values come from the fixtures' definitions or an independent library, never from the function under test.
A window is counted out from the tile size, the overlap and the raster edge; a band is the band the fixture wrote;
a tessellation cell is the cell the h3 or quadbin library (or the grid's own numbering) puts a pixel centre in, so
the tile of a cell must hold exactly the valid pixels there. Fixtures: `rg_big` is a 1200 x 1200 Float32 raster
without compression (5.76 MB); `rg_ramp_4326` an 80 x 60 ramp on a 0.01 degree grid at (-1, 52); `rg_bng` a 40 x 30
ramp on a 100 m grid in EPSG:27700 at (530000, 183000), so it covers twelve 1 km cells.
"""

from databricks.labs.gbx.bench.oracle_tables.grid_scalar import G
from databricks.labs.gbx.bench.oracle_tables.raster_tile_op import _ramp
from databricks.labs.gbx.bench.oracles import (
    UPSTREAM_ERROR,
    Build,
    Case,
    _tile,
    builder,
)

MK, SB, OV = "gbx_rst_maketiles", "gbx_rst_separatebands", "gbx_rst_tooverlappingtiles"
H3T, QBT, BNGT, CGT = (
    "gbx_rst_h3_tessellate",
    "gbx_rst_quadbin_tessellate",
    "gbx_rst_bng_tessellate",
    "gbx_rst_custom_tessellate",
)
T4326, ERR, CORRUPT = Build("tile_4326"), Build("tile_error"), Build("tile_corrupt")
BANDS3, BIG = Build("rto_bands3"), Build("rg_big")
RAMP4326, RAMPBNG = Build("rg_ramp_4326"), Build("rg_bng")
ONLY = ("session",)


def _plane(h, w):
    import numpy as np

    y, x = np.mgrid[0:h, 0:w].astype("float64")
    return (x * 7 + y * 13) % 17 + 1


@builder("rg_big")
def _rg_big(tmpdir):
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    from databricks.labs.gbx.pyrx.core.compression import creation_opts
    from databricks.labs.gbx.pyrx.core.crs import resolve_crs

    z = _plane(1200, 1200).astype("float32")
    profile = dict(
        driver="GTiff",
        width=1200,
        height=1200,
        count=1,
        dtype="float32",
        crs=resolve_crs("EPSG:3857"),
        transform=from_origin(400000.0, 5760000.0, 10.0, 10.0),
        **creation_opts("float32", compress="none"),
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(z[np.newaxis])
        return _tile(mf.read())


@builder("rg_ramp_4326")
def _rg_ramp_4326(tmpdir):
    from databricks.labs.gbx.bench.oracles import _gtiff

    return _tile(
        _gtiff(_plane(60, 80)[None], "EPSG:4326", origin=(-1.0, 52.0), px=0.01)
    )


@builder("rg_bng")
def _rg_bng(tmpdir):
    from databricks.labs.gbx.bench.oracles import _gtiff

    return _tile(
        _gtiff(
            _plane(30, 40)[None], "EPSG:27700", origin=(530000.0, 183000.0), px=100.0
        )
    )


# The windows of a 64 x 48 raster cut in 32 x 32 pieces, without overlap and with half of each edge overlapping
# (a step of 16): every window is a row, the last ones clamped to the raster edge.
QUARTERS = [(0, 0, 32, 32), (32, 0, 32, 32), (0, 32, 32, 16), (32, 32, 32, 16)]
HALF_OVERLAP = [
    (c, r, min(32, 64 - c), min(32, 48 - r))
    for r in (0, 16, 32)
    for c in (0, 16, 32, 48)
]
# 5.76 MB in 1 MB pieces: the quad-split goes on until a part fits, 2 x 2 is 1.44 MB, 4 x 4 is 0.36 MB.
SIXTEEN = [(c, r, 300, 300) for r in range(0, 1200, 300) for c in range(0, 1200, 300)]

# The ramp tile as three tiles: band k of rto_bands3 is the ramp plus 100 k.
_BANDS = [(_ramp(False) + 100.0 * k).tolist() for k in (1, 2, 3)]
_CRS3857 = {"crs": "EPSG:3857", "count": 1, "width": 8, "height": 6}

RASTER_GENERATOR = (
    # ---------------------------------------------------------------------------------------------- maketiles
    Case("maketiles_5mb", MK, (BIG, 1), "windows", SIXTEEN, ONLY),
    Case("maketiles_fits", MK, (BIG, 8), "windows", [(0, 0, 1200, 1200)], ONLY),
    Case("maketiles_no_split", MK, (T4326, 0), "windows", [(0, 0, 64, 48)], ONLY),
    Case("maketiles_null", MK, (None, 1), "no_rows", None, ONLY),
    Case("maketiles_null_size", MK, (T4326, None), "error_tile", None, ONLY),
    Case("maketiles_error_tile", MK, (ERR, 1), "error_tile", UPSTREAM_ERROR, ONLY),
    Case("maketiles_corrupt", MK, (CORRUPT, 1), "error_tile", None, ONLY),
    # ---------------------------------------------------------------------------------------------- separatebands
    Case(
        "separatebands_three",
        SB,
        (BANDS3,),
        "tile_rows",
        {"pixels": _BANDS, "meta": _CRS3857},
        ONLY,
    ),
    Case(
        "separatebands_single",
        SB,
        (Build("rto_plain"),),
        "tile_rows",
        {"pixels": [_ramp(False).tolist()], "meta": _CRS3857},
        ONLY,
    ),
    Case("separatebands_null", SB, (None,), "no_rows", None, ONLY),
    Case("separatebands_error_tile", SB, (ERR,), "error_tile", UPSTREAM_ERROR, ONLY),
    Case("separatebands_corrupt", SB, (CORRUPT,), "error_tile", None, ONLY),
    # ---------------------------------------------------------------------------------------------- tooverlappingtiles
    Case("overlap_half", OV, (T4326, 32, 32, 50), "windows", HALF_OVERLAP, ONLY),
    Case("overlap_zero", OV, (T4326, 32, 32, 0), "windows", QUARTERS, ONLY),
    Case("overlap_all", OV, (T4326, 32, 32, 100), "raises", "overlap", ONLY),
    Case("overlap_zero_window", OV, (T4326, 0, 32, 10), "raises", "tile_width", ONLY),
    Case("overlap_null", OV, (None, 32, 32, 50), "no_rows", None, ONLY),
    Case("overlap_null_size", OV, (T4326, None, 32, 50), "error_tile", None, ONLY),
    Case(
        "overlap_error_tile", OV, (ERR, 32, 32, 50), "error_tile", UPSTREAM_ERROR, ONLY
    ),
    Case("overlap_corrupt", OV, (CORRUPT, 32, 32, 50), "error_tile", None, ONLY),
    # ---------------------------------------------------------------------------------------------- H3
    Case(
        "h3_tess_res4",
        H3T,
        (RAMP4326, 4),
        "tessellation",
        {"grid": "h3", "res": 4, "complete": True},
        ONLY,
    ),
    Case(
        "h3_tess_sparse",
        H3T,
        (RAMP4326, 4, "centroid", "sparse"),
        "tessellation",
        {"grid": "h3", "res": 4},
        ONLY,
    ),
    Case(
        "h3_tess_bad_assignment",
        H3T,
        (RAMP4326, 4, "sideways"),
        "raises",
        "sideways",
        ONLY,
    ),
    Case(
        "h3_tess_bad_coverage",
        H3T,
        (RAMP4326, 4, "centroid", "sideways"),
        "raises",
        "sideways",
        ONLY,
    ),
    Case("h3_tess_null", H3T, (None, 4), "no_rows", None, ONLY),
    Case("h3_tess_null_res", H3T, (RAMP4326, None), "error_tile", None, ONLY),
    Case("h3_tess_error_tile", H3T, (ERR, 4), "error_tile", UPSTREAM_ERROR, ONLY),
    Case("h3_tess_corrupt", H3T, (CORRUPT, 4), "error_tile", None, ONLY),
    # ---------------------------------------------------------------------------------------------- quadbin
    Case(
        "qb_tess_z10",
        QBT,
        (RAMP4326, 10),
        "tessellation",
        {"grid": "quadbin", "res": 10, "complete": True},
        ONLY,
    ),
    Case(
        "qb_tess_bad_assignment",
        QBT,
        (RAMP4326, 10, "sideways"),
        "raises",
        "sideways",
        ONLY,
    ),
    Case("qb_tess_null", QBT, (None, 10), "no_rows", None, ONLY),
    Case("qb_tess_null_res", QBT, (RAMP4326, None), "error_tile", None, ONLY),
    # ---------------------------------------------------------------------------------------------- BNG
    Case(
        "bng_tess_1km",
        BNGT,
        (RAMPBNG, 3),
        "tessellation",
        {"grid": "bng", "res": 3},
        ONLY,
    ),
    Case(
        "bng_tess_named_res",
        BNGT,
        (RAMPBNG, "1km"),
        "tessellation",
        {"grid": "bng", "res": 3},
        ONLY,
    ),
    Case("bng_tess_bad_res", BNGT, (RAMPBNG, "3km"), "raises", "3km", ONLY),
    Case(
        "bng_tess_bad_assignment",
        BNGT,
        (RAMPBNG, 3, "sideways"),
        "raises",
        "sideways",
        ONLY,
    ),
    Case("bng_tess_null", BNGT, (None, 3), "no_rows", None, ONLY),
    Case("bng_tess_null_res", BNGT, (RAMPBNG, None), "error_tile", None, ONLY),
    # ---------------------------------------------------------------------------------------------- custom grid
    Case(
        "cg_tess_res0",
        CGT,
        (RAMPBNG, G, 0),
        "tessellation",
        {"grid": "custom", "res": 0},
        ONLY,
    ),
    Case(
        "cg_tess_bad_assignment",
        CGT,
        (RAMPBNG, G, 0, "sideways"),
        "raises",
        "sideways",
        ONLY,
    ),
    Case("cg_tess_null", CGT, (None, G, 0), "no_rows", None, ONLY),
    Case("cg_tess_null_res", CGT, (RAMPBNG, G, None), "error_tile", None, ONLY),
    Case("cg_tess_null_grid", CGT, (RAMPBNG, None, 0), "error_tile", None, ONLY),
)
