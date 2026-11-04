"""Oracle table for the band-math functions of the GDAL spec engine family (spectral indices, threshold, the
canopy height model): inputs -> expected, run at every level by test_family_cross_tier.py.

Expected values come from the formulas' definitions (oracles._REFS), compared on the same NoData mask: a pixel is
NoData where an input band is NoData or the result is not finite. The fixture `calc_bands` is a 6 x 8 tile of three
float32 bands (1 red, 2 near-infrared, 3 blue) on a 10 m grid, with one NoData pixel in band 1 and one pixel where
bands 1 and 2 are both 0 (a division by zero).
"""

from databricks.labs.gbx.bench.oracle_tables._uc_tile import edges, share
from databricks.labs.gbx.bench.oracles import (
    UPSTREAM_ERROR,
    Build,
    Case,
    _gtiff,
    _tile,
    builder,
)

NDVI, NDWI, NBR = "gbx_rst_ndvi", "gbx_rst_ndwi", "gbx_rst_nbr"
SAVI, EVI, IDX = "gbx_rst_savi", "gbx_rst_evi", "gbx_rst_index"
THR, CHM = "gbx_rst_threshold", "gbx_rst_chm"
BANDS, ERR, CORRUPT = Build("calc_bands"), Build("tile_error"), Build("tile_corrupt")
ONLY = ("session",)


def _bands(shift=0.0):
    import numpy as np

    y, x = np.mgrid[0:6, 0:8].astype("float64")
    red = 10.0 + (x * 3 + y) % 7 + shift
    nir = 30.0 + (x * 5 + y * 2) % 11 + shift
    blue = 2.0 + (x + y) % 4 + shift
    red[4, 5] = -9999.0
    red[2, 2], nir[2, 2] = 0.0, 0.0
    return np.stack([red, nir, blue])


@builder("calc_bands")
def _calc_bands(tmpdir):
    return _tile(_gtiff(_bands(), "EPSG:3857"))


@builder("calc_dsm")
def _calc_dsm(tmpdir):
    """A surface 3 m above `calc_dem` (2 m below it at one pixel, which clamps to 0), with one NoData pixel."""
    import numpy as np

    z = _calc_dem_array() + 3.0
    z[1, 1] -= 5.0
    z[4, 4] = -9999.0
    return _tile(_gtiff(z[np.newaxis], "EPSG:3857"))


def _calc_dem_array():
    import numpy as np

    y, x = np.mgrid[0:6, 0:8].astype("float64")
    return 100.0 + x + 2 * y


@builder("calc_dem")
def _calc_dem(tmpdir):
    import numpy as np

    return _tile(_gtiff(_calc_dem_array()[np.newaxis], "EPSG:3857"))


_session_calc_rows = (
    Case("ndvi", NDVI, (BANDS, 1, 2), "ref", "ndvi", ONLY),
    Case("ndvi_null_band", NDVI, (BANDS, None, 2), "null", None, ONLY),
    Case("ndvi_null_tile", NDVI, (None, 1, 2), "null", None, ONLY),
    Case("ndvi_error_tile", NDVI, (ERR, 1, 2), "error_tile", UPSTREAM_ERROR, ONLY),
    Case("ndvi_corrupt", NDVI, (CORRUPT, 1, 2), "error_tile", None, ONLY),
    Case("ndvi_band_out_of_range", NDVI, (BANDS, 1, 7), "raises", "7", ONLY),
    Case("ndwi", NDWI, (BANDS, 3, 2), "ref", "ndwi", ONLY),
    Case("nbr", NBR, (BANDS, 2, 3), "ref", "nbr", ONLY),
    Case("savi", SAVI, (BANDS, 1, 2, 0.25), "ref", "savi", ONLY),
    Case("savi_sql_default", SAVI, (BANDS, 1, 2), "ref", "savi", ONLY),
    Case("evi", EVI, (BANDS, 1, 2, 3, 2.0, 6.0, 7.5, 2.5), "ref", "evi", ONLY),
    Case("evi_sql_defaults", EVI, (BANDS, 1, 2, 3), "ref", "evi", ONLY),
    Case(
        "index_ndvi",
        IDX,
        (BANDS, "ndvi", {"red": 1, "nir": 2}),
        "ref",
        "index_ndvi",
        ONLY,
    ),
    Case(
        "index_msavi",
        IDX,
        (BANDS, "MSAVI", {"Red": 1, "NIR": 2}),
        "ref",
        "index_msavi",
        ONLY,
    ),
    Case(
        "index_unknown_formula",
        IDX,
        (BANDS, "nope", {"red": 1, "nir": 2}),
        "raises",
        "nope",
        ONLY,
    ),
    Case("threshold", THR, (BANDS, ">", 12.0), "ref", "threshold", ONLY),
    Case("threshold_le", THR, (BANDS, "<=", 33.0), "ref", "threshold", ONLY),
    Case("threshold_bad_operator", THR, (BANDS, "~", 1.0), "raises", "~", ONLY),
    Case("chm", CHM, (Build("calc_dsm"), Build("calc_dem")), "ref", "chm", ONLY),
    Case("chm_null_dsm", CHM, (None, Build("calc_dem")), "null", None, ONLY),
    # a DSM that opens but cannot be read or aligned is an error tile, not a failed task
    Case("chm_corrupt", CHM, (CORRUPT, Build("calc_dem")), "error_tile", None, ONLY),
    Case(
        "chm_error_tile",
        CHM,
        (ERR, Build("calc_dem")),
        "error_tile",
        UPSTREAM_ERROR,
        ONLY,
    ),
)


# Unity Catalog: the band math of a materialized tile is the session's; a band the tile does not have is a wrong argument
SPEC_CALC = share(
    _session_calc_rows,
    "threshold", "threshold_le", "threshold_bad_operator",
    "ndvi", "ndvi_null_band", "ndvi_null_tile", "ndvi_error_tile", "ndvi_band_out_of_range", "ndwi", "nbr", "savi",
    "savi_sql_default", "evi", "evi_sql_defaults", "index_ndvi", "index_msavi", "index_unknown_formula",
    "chm", "chm_null_dsm", "chm_error_tile",
) + edges(
    threshold=(">", 12.0), ndvi=(1, 2), ndwi=(3, 2), nbr=(2, 3), savi=(1, 2, 0.25), evi=(1, 2, 3, 2.0, 6.0, 7.5, 2.5),
    index=("ndvi", {"red": 1, "nir": 2}), chm=(Build("calc_dem"),),
) + (
    Case("uc_threshold_null_tile", THR, (None, ">", 1.0), "null", None, ("uc",)),
    Case("uc_threshold_error_tile", THR, (ERR, "~", 1.0), "error_tile", UPSTREAM_ERROR, ("uc",)),
    Case("uc_index_band_out_of_range", IDX, (BANDS, "ndvi", {"red": 1, "nir": 7}), "raises", "7", ("uc",)),
    Case("uc_index_null_band_map", IDX, (BANDS, "ndvi", None), "null", None, ("uc",)),
    Case("uc_evi_band_out_of_range", EVI, (BANDS, 1, 2, 9), "raises", "9", ("uc",)),
    # chm reads two tiles: a virtual or an unreadable DEM is as a virtual or an unreadable DSM
    Case("uc_chm_virtual_dem_refused", CHM, (Build("calc_dsm"), Build("tile_virtual")), "uc_refused", None, ("uc",)),
    Case("uc_chm_corrupt_dem_null", CHM, (Build("calc_dsm"), CORRUPT), "null", None, ("uc",)),
    Case("uc_chm_null_dem", CHM, (Build("calc_dsm"), None), "null", None, ("uc",)),
)  # fmt: skip
