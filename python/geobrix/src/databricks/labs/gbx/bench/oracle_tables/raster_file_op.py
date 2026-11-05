"""Oracle table for the file-writing raster functions (`gbx_rst_buildoverviews`, `gbx_rst_cog_convert`): inputs ->
expected, run at every level by test_family_cross_tier.py.

Expected values come from the fixtures' definitions. `tile_4326` is a 64 x 48 float32 GeoTIFF (EPSG:4326, NoData -9999):
the result keeps its pixels, size, CRS and NoData, and adds what the function is for: overviews at the decimation
factors asked for, or the Cloud Optimized GeoTIFF layout with the internal block size asked for. The colour-relief and
viewshed functions of the same family keep their rows in the GDAL spec engine tables (spec_dem.py, spec_analysis.py).
"""

from databricks.labs.gbx.bench.oracle_tables._uc_tile import edges, share
from databricks.labs.gbx.bench.oracles import UPSTREAM_ERROR, Build, Case

BO, CG = "gbx_rst_buildoverviews", "gbx_rst_cog_convert"
TILE, VIRT, ERR, CORRUPT = (
    Build("tile_4326"),
    Build("tile_virtual"),
    Build("tile_error"),
    Build("tile_corrupt"),
)
ONLY = ("session",)
SAME = {
    "width": 64,
    "height": 48,
    "count": 1,
    "dtype": "float32",
    "nodata": -9999.0,
    "crs": "EPSG:4326",
    "driver": "GTiff",
    "meta_driver": "GTiff",
    "pixels_of_arg0": True,
}
_SESSION_ROWS = (
    # --- buildoverviews: one overview per decimation factor, the pixels unchanged
    Case(
        "buildoverviews_levels",
        BO,
        (TILE, [2, 4]),
        "tile_meta",
        {**SAME, "overviews": [2, 4]},
        ONLY,
    ),
    Case(
        "buildoverviews_resampling",
        BO,
        (TILE, [2], "nearest"),
        "tile_meta",
        {**SAME, "overviews": [2]},
        ONLY,
    ),
    Case(
        "buildoverviews_virtual",
        BO,
        (VIRT, [2]),
        "tile_meta",
        {**SAME, "overviews": [2]},
        ONLY,
    ),
    Case(
        "buildoverviews_bad_resampling",
        BO,
        (TILE, [2], "bogus"),
        "raises",
        "bogus",
        ONLY,
    ),
    Case("buildoverviews_bad_levels", BO, (TILE, [1]), "raises", "levels", ONLY),
    Case("buildoverviews_null_tile", BO, (None, [2]), "null", None, ONLY),
    Case("buildoverviews_null_levels", BO, (TILE, None), "null", None, ONLY),
    Case(
        "buildoverviews_error_tile", BO, (ERR, [2]), "error_tile", UPSTREAM_ERROR, ONLY
    ),
    Case("buildoverviews_corrupt", BO, (CORRUPT, [2]), "error_tile", None, ONLY),
    # --- cog_convert: the COG layout with the block size asked for, the pixels unchanged
    Case(
        "cog_convert_layout", CG, (TILE,), "tile_meta", {**SAME, "layout": "COG"}, ONLY
    ),
    # the default codec, AUTO, is ZSTD in the COG, at every level
    Case(
        "cog_convert_auto",
        CG,
        (TILE, "AUTO"),
        "tile_meta",
        {**SAME, "layout": "COG", "compression": "ZSTD"},
        ONLY,
    ),
    Case(
        "cog_convert_blocksize",
        CG,
        (TILE, "AUTO", 16),
        "tile_meta",
        {**SAME, "layout": "COG", "blocksize": (16, 16)},
        ONLY,
    ),
    Case(
        "cog_convert_virtual", CG, (VIRT,), "tile_meta", {**SAME, "layout": "COG"}, ONLY
    ),
    Case("cog_convert_bad_blocksize", CG, (TILE, "AUTO", 100), "raises", "100", ONLY),
    Case("cog_convert_bad_compression", CG, (TILE, "SQUASH"), "raises", "SQUASH", ONLY),
    Case("cog_convert_null_tile", CG, (None,), "null", None, ONLY),
    Case("cog_convert_error_tile", CG, (ERR,), "error_tile", UPSTREAM_ERROR, ONLY),
    Case("cog_convert_corrupt", CG, (CORRUPT,), "error_tile", None, ONLY),
)

# Unity Catalog: both functions give the session's tile for a materialized one. A virtual tile is refused with a Fix and a
# corrupt one is NULL (a UC function has no error tile); an error tile goes through unchanged, whatever the arguments are.
RASTER_FILE_OP = share(
    _SESSION_ROWS,
    "buildoverviews_levels",
    "buildoverviews_resampling",
    "buildoverviews_bad_resampling",
    "buildoverviews_bad_levels",
    "buildoverviews_null_tile",
    "buildoverviews_null_levels",
    "buildoverviews_error_tile",
    "cog_convert_layout",
    "cog_convert_auto",
    "cog_convert_blocksize",
    "cog_convert_bad_blocksize",
    "cog_convert_bad_compression",
    "cog_convert_null_tile",
    "cog_convert_error_tile",
) + edges(buildoverviews=([2],), cog_convert=())
