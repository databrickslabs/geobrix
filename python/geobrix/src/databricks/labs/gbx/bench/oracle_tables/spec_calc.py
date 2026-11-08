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


def _holes():
    """Four float32 bands (1 red, 2 near-infrared, 3 green, 4 shortwave infrared) of 6 x 8 with a NoData pixel in every
    band (a different one each), NaN pixels, a zero denominator for each pair a row below uses, and a pixel whose
    MSAVI radicand is negative: each must come out NoData, never 0, NaN or a stand-in.
    """
    import numpy as np

    y, x = np.mgrid[0:6, 0:8].astype("float64")
    red = 10.0 + (x * 3 + y) % 7
    nir = 30.0 + (x * 5 + y * 2) % 11
    green = 4.0 + (x + y * 3) % 5
    swir = 20.0 + (x * 2 + y) % 9
    red[4, 5], nir[1, 6], green[0, 3], swir[5, 1] = -9999.0, -9999.0, -9999.0, -9999.0
    nir[3, 0], green[2, 6] = np.nan, np.nan
    red[2, 2], nir[2, 2] = 0.0, 0.0  # red + nir == 0
    green[5, 5], nir[5, 5] = 0.0, 0.0  # green + nir == 0
    nir[0, 0], swir[0, 0] = 0.0, 0.0  # nir + swir == 0
    red[1, 1], nir[1, 1] = -1.0, 0.5  # (2 nir + 1)^2 - 8 (nir - red) < 0
    return np.stack([red, nir, green, swir])


@builder("calc_holes")
def _calc_holes(tmpdir):
    return _tile(_gtiff(_holes(), "EPSG:3857"))


def _typed(dtype, nodata, shift):
    """Two bands of 6 x 8 in `dtype`, a NoData pixel in each when `nodata` is declared."""
    import numpy as np

    y, x = np.mgrid[0:6, 0:8]
    bands = np.stack([(x * 7 + y * 3) % 40, (x * 5 + y) % 60]) + shift
    if nodata is not None:
        bands[0, 4, 5] = nodata
        bands[1, 1, 6] = nodata
    return bands.astype(dtype)


@builder("calc_byte")
def _calc_byte(tmpdir):
    """Byte bands that declare NoData 255."""
    return _tile(
        _gtiff(_typed("uint8", 255, 0), "EPSG:3857", nodata=255, dtype="uint8")
    )


@builder("calc_int16")
def _calc_int16(tmpdir):
    """Int16 bands, some negative, that declare no NoData (the type's fallback, -32768, applies)."""
    return _tile(
        _gtiff(_typed("int16", None, -20), "EPSG:3857", nodata=None, dtype="int16")
    )


def _calc_file(tmpdir):
    import os

    path = os.path.join(tmpdir, "calc_bands.tif")
    with open(path, "wb") as fh:
        fh.write(_gtiff(_bands(), "EPSG:3857"))
    return path


@builder("calc_bands_file")
def _calc_bands_file(tmpdir):
    """The bands as a file the tile points at (a virtual tile)."""
    return _tile(None, _calc_file(tmpdir), None)


@builder("calc_bands_vrt")
def _calc_bands_vrt(tmpdir):
    """A VRT over that file: a virtual tile of another driver."""
    import os

    from osgeo import gdal

    vrt, tif = os.path.join(tmpdir, "calc_bands.vrt"), _calc_file(tmpdir)
    gdal.BuildVRT(vrt, [tif]).FlushCache()
    with open(
        vrt
    ) as fh:  # the source by absolute name, so the reference can read the VRT's bytes too
        text = fh.read().replace(
            'relativeToVRT="1">calc_bands.tif', f'relativeToVRT="0">{tif}'
        )
    with open(vrt, "w") as fh:
        fh.write(text)
    return _tile(None, vrt, None)


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


HOLES, FILE, VRT = (
    Build("calc_holes"),
    Build("calc_bands_file"),
    Build("calc_bands_vrt"),
)
# The engine's rows: NoData, NaN and zero denominators in every band, virtual tiles, and the arguments it refuses.
_engine_rows = (
    Case("ndvi_holes", NDVI, (HOLES, 1, 2), "ref", "ndvi", ONLY),
    Case("ndwi_holes", NDWI, (HOLES, 3, 2), "ref", "ndwi", ONLY),
    Case("nbr_holes", NBR, (HOLES, 2, 4), "ref", "nbr", ONLY),
    Case("savi_holes", SAVI, (HOLES, 1, 2, 0.5), "ref", "savi", ONLY),
    Case("evi_holes", EVI, (HOLES, 1, 2, 3, 1.0, 6.0, 7.5, 2.5), "ref", "evi", ONLY),
    Case("index_holes_ndvi", IDX, (HOLES, "ndvi", {"red": 1, "nir": 2}), "ref", "index_ndvi", ONLY),
    Case("index_holes_gndvi", IDX, (HOLES, "gndvi", {"green": 3, "nir": 2}), "ref", "index_gndvi", ONLY),
    Case("index_holes_msavi", IDX, (HOLES, "msavi", {"red": 1, "nir": 2}), "ref", "index_msavi", ONLY),
    Case("index_holes_ndvi_re", IDX, (HOLES, "ndvi_re", {"red_edge": 3, "nir": 2}), "ref", "index_ndvi_re", ONLY),
    Case("index_holes_ndmi", IDX, (HOLES, "ndmi", {"nir": 2, "swir": 4}), "ref", "index_ndmi", ONLY),
    Case("index_holes_ndsi", IDX, (HOLES, "ndsi", {"green": 3, "swir": 4}), "ref", "index_ndsi", ONLY),
    Case("threshold_holes", THR, (HOLES, ">=", 12.0), "ref", "threshold", ONLY),
    Case("threshold_holes_eq", THR, (HOLES, "==", 0.0), "ref", "threshold", ONLY),
    # the result keeps the source's type and NoData, or the type's fallback when it declares none
    Case("threshold_byte", THR, (Build("calc_byte"), "<", 10.0), "ref", "threshold", ONLY),
    Case("threshold_byte_type", THR, (Build("calc_byte"), "<", 10.0), "tile_meta",
         {"dtype": "uint8", "nodata": 255.0, "count": 2}, ONLY),
    Case("threshold_int16", THR, (Build("calc_int16"), "!=", 0.0), "ref", "threshold", ONLY),
    Case("threshold_int16_type", THR, (Build("calc_int16"), "!=", 0.0), "tile_meta",
         {"dtype": "int16", "nodata": -32768.0, "count": 2}, ONLY),
    Case("ndvi_virtual_file", NDVI, (FILE, 1, 2), "ref", "ndvi", ONLY),
    Case("ndvi_virtual_vrt", NDVI, (VRT, 1, 2), "ref", "ndvi", ONLY),
    Case("threshold_virtual_vrt", THR, (VRT, "<=", 33.0), "ref", "threshold", ONLY),
    Case("ndvi_band_zero", NDVI, (BANDS, 0, 2), "raises", "red_idx", ONLY),
    Case("ndvi_band_beyond", NDVI, (BANDS, 1, 4), "raises", "nir_idx", ONLY),
    Case("evi_band_beyond", EVI, (BANDS, 1, 2, 4), "raises", "blue_idx", ONLY),
    Case("index_missing_band", IDX, (BANDS, "ndvi", {"red": 1}), "raises", "nir", ONLY),
    Case("index_band_beyond", IDX, (BANDS, "ndvi", {"red": 1, "nir": 4}), "raises", "nir", ONLY),
    Case("savi_null_l", SAVI, (BANDS, 1, 2, None), "null", None, ONLY),
    Case("threshold_null_op", THR, (BANDS, None, 1.0), "null", None, ONLY),
    Case("index_null_name", IDX, (BANDS, None, {"red": 1, "nir": 2}), "null", None, ONLY),
    Case("threshold_error_tile", THR, (ERR, ">", 1.0), "error_tile", UPSTREAM_ERROR, ONLY),
)  # fmt: skip


# Unity Catalog: the band math of a materialized tile is the session's; a band the tile does not have is a wrong argument
SPEC_CALC = _engine_rows + share(
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
