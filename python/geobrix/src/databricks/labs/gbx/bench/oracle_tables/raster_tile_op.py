"""Oracle table for the raster tile-to-tile functions that edit, re-encode, filter or realign one tile: inputs ->
expected, run at every level by test_family_cross_tier.py.

Expected values come from the fixtures' definitions or an independent calculation, never from the function under
test: a focal filter and a convolution are written out pixel by pixel (oracle_refs), a percentile stretch from
numpy's percentile, a realignment from rasterio.warp, a subdataset from rasterio's own NetCDF driver, a CRS or
format from what the result's raster reports. The fixtures are small: `rto_ramp` is a 8 x 6 float32 ramp on a 10 m
grid in EPSG:3857 with one NoData pixel (row 2, column 3); `rto_plain` the same without NoData; `rto_bands3` three
bands of the ramp (band k adds 100 k); `rto_byte` a 16 x 12 UInt8 ramp and `rto_u16` a UInt16 one, both without
NoData; `rto_rgb` four colour blocks (green, light, grey, dark) and one black pixel.
"""

from databricks.labs.gbx.bench.oracles import (
    UPSTREAM_ERROR,
    Build,
    Case,
    _encode,
    _gtiff,
    _tile,
    builder,
)

AL, AF, BD, CV = (
    "gbx_rst_align_to",
    "gbx_rst_asformat",
    "gbx_rst_band",
    "gbx_rst_convolve",
)
DB, FI, SD, IN = (
    "gbx_rst_derivedband",
    "gbx_rst_filter",
    "gbx_rst_getsubdataset",
    "gbx_rst_initnodata",
)
LC, PS, SC, SS, UT = (
    "gbx_rst_land_cover",
    "gbx_rst_percentile_stretch",
    "gbx_rst_setcrs",
    "gbx_rst_setsrid",
    "gbx_rst_updatetype",
)
T4326, T3857, VIRT = Build("tile_4326"), Build("tile_3857"), Build("tile_virtual")
ERR, CORRUPT = Build("tile_error"), Build("tile_corrupt")
RAMP, PLAIN, BANDS3 = Build("rto_ramp"), Build("rto_plain"), Build("rto_bands3")
BYTE, U16, CAST, RGB = (
    Build("rto_byte"),
    Build("rto_u16"),
    Build("rto_cast"),
    Build("rto_rgb"),
)
ONLY = ("session",)

# the pixel function of the derivedband rows: GDAL's VRT pixel-function signature, filling the output in place
DOUBLE_PLUS_ONE = (
    "def double_plus_one(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, "
    "**kwargs): out_ar[:] = in_ar[0] * 2 + 1"
)
ASYMMETRIC = [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]]
IDENTITY = [[0.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 0.0]]


def _ramp(nodata_pixel=True):
    import numpy as np

    y, x = np.mgrid[0:6, 0:8].astype("float64")
    z = (x * 7 + y * 13) % 17 + 1
    if nodata_pixel:
        z[2, 3] = -9999.0
    return z


@builder("rto_ramp")
def _rto_ramp(tmpdir):
    import numpy as np

    return _tile(_gtiff(_ramp()[np.newaxis], "EPSG:3857"))


@builder("rto_plain")
def _rto_plain(tmpdir):
    import numpy as np

    return _tile(_gtiff(_ramp(False)[np.newaxis], "EPSG:3857", nodata=None))


@builder("rto_bands3")
def _rto_bands3(tmpdir):
    import numpy as np

    z = _ramp(False)
    return _tile(
        _gtiff(np.stack([z + 100.0 * k for k in (1, 2, 3)]), "EPSG:3857", nodata=None)
    )


def _grid(dtype, w, h, count=1):
    """A UInt8 or UInt16 ramp tile of `count` bands in EPSG:3857 on a 10 m grid, no NoData."""
    import numpy as np
    from rasterio.transform import from_origin

    from databricks.labs.gbx.pyrx.core.crs import resolve_crs

    y, x = np.mgrid[0:h, 0:w]
    bands = np.stack(
        [((x * 5 + y * 3 + 10 * k) % 200).astype(dtype) for k in range(count)]
    )
    return _tile(
        _encode(
            bands,
            "GTiff",
            crs=resolve_crs("EPSG:3857"),
            transform=from_origin(400000.0, 5760000.0, 10.0, 10.0),
        )
    )


def _rto_bands3_4326():
    """The bands of `rto_bands3` on a 0.01 degree grid from (-1, 52) in EPSG:4326: GDAL's NetCDF writer keeps a
    geographic grid as latitude and longitude variables, so the file reads back georeferenced.
    """
    import numpy as np
    from rasterio.transform import from_origin

    from databricks.labs.gbx.pyrx.core.crs import resolve_crs

    z = _ramp(False)
    return _tile(
        _encode(
            np.stack([z + 100.0 * k for k in (1, 2, 3)]).astype("float32"),
            "GTiff",
            crs=resolve_crs("EPSG:4326"),
            transform=from_origin(-1.0, 52.0, 0.01, 0.01),
        )
    )


@builder("rto_byte")
def _rto_byte(tmpdir):
    return _grid("uint8", 16, 12)


@builder("rto_u16")
def _rto_u16(tmpdir):
    return _grid("uint16", 16, 12)


@builder("rto_cast")
def _rto_cast(tmpdir):
    """Eight float32 values, some fractional, some outside UInt8's range and one NaN (it casts to 0), no NoData."""
    import numpy as np

    z = np.array([[[-5.0, 0.4, 1.6, 254.4], [255.6, 300.0, 100.2, np.nan]]])
    return _tile(_gtiff(z, "EPSG:3857", nodata=None))


@builder("rto_nan_nodata")
def _rto_nan_nodata(tmpdir):
    """Two Float32 bands that declare NaN as their NoData value, with NaN pixels in both."""
    import numpy as np
    from rasterio.transform import from_origin

    from databricks.labs.gbx.pyrx.core.crs import resolve_crs

    z = np.array(
        [[[1.0, np.nan], [3.0, 4.0]], [[np.nan, 6.0], [7.0, np.nan]]], "float32"
    )
    return _tile(
        _encode(
            z,
            "GTiff",
            crs=resolve_crs("EPSG:3857"),
            transform=from_origin(400000.0, 5760000.0, 10.0, 10.0),
            nodata=np.nan,
        )
    )


@builder("rto_u16_wide")
def _rto_u16_wide(tmpdir):
    """Six UInt16 values from 0 to 65535, three of them past Int16's range (an integer-to-integer cast)."""
    import numpy as np
    from rasterio.transform import from_origin

    from databricks.labs.gbx.pyrx.core.crs import resolve_crs

    z = np.array([[[0, 100, 32767], [32768, 40000, 65535]]], dtype="uint16")
    return _tile(
        _encode(
            z,
            "GTiff",
            crs=resolve_crs("EPSG:3857"),
            transform=from_origin(400000.0, 5760000.0, 10.0, 10.0),
        )
    )


@builder("rto_rgb")
def _rto_rgb(tmpdir):
    """12 x 12 UInt8, red-green-blue: the top left quarter green (30, 200, 40), the top right light (210, 205, 200),
    the bottom left mid grey (100, 100, 100), the bottom right dark (30, 30, 30); one black pixel (0, 0, 0) at row 6, col 6.
    """
    import numpy as np
    from rasterio.transform import from_origin

    from databricks.labs.gbx.pyrx.core.crs import resolve_crs

    a = np.zeros((3, 12, 12), "uint8")
    for (rs, cs), rgb in {
        ((0, 6), (0, 6)): (30, 200, 40),
        ((0, 6), (6, 12)): (210, 205, 200),
        ((6, 12), (0, 6)): (100, 100, 100),
        ((6, 12), (6, 12)): (30, 30, 30),
    }.items():
        for band, v in enumerate(rgb):
            a[band, rs[0] : rs[1], cs[0] : cs[1]] = v
    a[:, 6, 6] = 0
    return _tile(
        _encode(
            a,
            "GTiff",
            crs=resolve_crs("EPSG:3857"),
            transform=from_origin(400000.0, 5760000.0, 10.0, 10.0),
        )
    )


@builder("rto_two_bands")
def _rto_two_bands(tmpdir):
    return _grid("uint8", 8, 6, count=2)


@builder("rto_coarse_ref")
def _rto_coarse_ref(tmpdir):
    """A 31 x 23 grid of 0.02 degree pixels in EPSG:4326 whose pixel centres are the centres of every second pixel of
    `tile_4326` (a pixel 0.01 degrees, from (-1, 52)): the first centre is at (-0.985, 51.985), and a pixel of the
    tile has its centre at (-0.995 + 0.01 k, 51.995 - 0.01 m)."""
    import numpy as np
    from rasterio.transform import from_origin

    from databricks.labs.gbx.pyrx.core.crs import resolve_crs

    return _tile(
        _encode(
            np.zeros((1, 23, 31), "float32"),
            "GTiff",
            crs=resolve_crs("EPSG:4326"),
            transform=from_origin(-0.995, 51.995, 0.02, 0.02),
        )
    )


@builder("rto_ref_3857")
def _rto_ref_3857(tmpdir):
    """A 20 x 16 grid of 1000 m pixels in EPSG:3857 over the west of `tile_4326`'s area (from lon -0.9, lat 51.9)."""
    import numpy as np
    from rasterio.transform import from_origin

    from databricks.labs.gbx.pyrx.core.crs import resolve_crs

    return _tile(
        _encode(
            np.zeros((1, 16, 20), "float32"),
            "GTiff",
            crs=resolve_crs("EPSG:3857"),
            transform=from_origin(-100189.0, 6807000.0, 1000.0, 1000.0),
        )
    )


@builder("rto_nc")
def _rto_nc(tmpdir):
    """A virtual tile of a NetCDF file whose three 2-D variables, `Band1` to `Band3`, are the bands of `rto_bands3`:
    GDAL's NetCDF writer turns each band of a raster into a variable, listed as a subdataset.
    """
    import os
    import warnings

    from rasterio.errors import NotGeoreferencedWarning

    from databricks.labs.gbx._gdal_policy import open_data
    from databricks.labs.gbx.bench.oracles import _raster_of, to_netcdf

    tif, nc = (os.path.join(tmpdir, f"rto_bands.{e}") for e in ("tif", "nc"))
    with open(tif, "wb") as fh:
        fh.write(_raster_of(_rto_bands3_4326()))
    to_netcdf(tif, nc)
    with warnings.catch_warnings():  # a container has no georeferencing of its own
        warnings.simplefilter("ignore", NotGeoreferencedWarning)
        with open_data(nc) as container:
            assert (
                len(container.subdatasets) == 3
            ), "the three bands are three variables"
    return _tile(None, nc, None)


@builder("rto_nc_bytes")
def _rto_nc_bytes(tmpdir):
    """The NetCDF file of `rto_nc` as a materialized tile's bytes (what reading it with a reader gives)."""
    with open(_rto_nc(tmpdir)["path"], "rb") as fh:
        return _tile(fh.read())


@builder("rto_nc_windowed")
def _rto_nc_windowed(tmpdir):
    """The tile of `rto_nc` cut to a window: a container has no pixel grid for the window to apply to."""
    tile = _rto_nc(tmpdir)
    tile["window"] = {"col_off": 0, "row_off": 0, "width": 4, "height": 4}
    return tile


RASTER_TILE_OP = (
    # --- align_to: nearest-neighbour onto the grid of a reference tile (rasterio.warp), the same CRS
    Case(
        "align_to_coarse", AL, (T4326, Build("rto_coarse_ref")), "ref", "align_to", ONLY
    ),
    # onto a grid in another CRS: the result has that grid
    Case(
        "align_to_other_crs",
        AL,
        (T4326, Build("rto_ref_3857")),
        "tile_meta",
        {"grid_of_arg1": True, "count": 1},
        ONLY,
    ),
    Case(
        "align_to_virtual",
        AL,
        (VIRT, Build("rto_coarse_ref")),
        "tile_meta",
        {"grid_of_arg1": True},
        ONLY,
    ),
    Case("align_to_null_tile", AL, (None, Build("rto_coarse_ref")), "null", None, ONLY),
    Case("align_to_null_reference", AL, (T4326, None), "null", None, ONLY),
    Case(
        "align_to_error_in",
        AL,
        (ERR, Build("rto_coarse_ref")),
        "error_tile",
        UPSTREAM_ERROR,
        ONLY,
    ),
    Case(
        "align_to_error_reference", AL, (T4326, ERR), "error_tile", UPSTREAM_ERROR, ONLY
    ),
    Case(
        "align_to_corrupt",
        AL,
        (CORRUPT, Build("rto_coarse_ref")),
        "error_tile",
        None,
        ONLY,
    ),
    Case("align_to_corrupt_reference", AL, (T4326, CORRUPT), "error_tile", None, ONLY),
    # only the reference's CRS, size and transform are read: a reference over the read cap (READ_CAP_ROWS) is no error
    Case(
        "align_to_big_reference",
        AL,
        (BYTE, T4326),
        "tile_meta",
        {"grid_of_arg1": True},
        ONLY,
    ),
    # --- asformat: the bytes open as the driver asked for (in any case), the pixels unchanged
    Case(
        "asformat_png",
        AF,
        (BYTE, "PNG"),
        "tile_meta",
        {"driver": "PNG", "meta_driver": "PNG", "pixels_of_arg0": True},
        ONLY,
    ),
    Case(
        "asformat_lower_case",
        AF,
        (BYTE, "png"),
        "tile_meta",
        {"driver": "PNG", "pixels_of_arg0": True},
        ONLY,
    ),
    Case(
        "asformat_gtiff",
        AF,
        (T4326, "GTiff"),
        "tile_meta",
        {"driver": "GTiff", "pixels_of_arg0": True, "crs": "EPSG:4326"},
        ONLY,
    ),
    # COG is a layout of a GeoTIFF: it opens as GTiff, with the COG layout
    Case(
        "asformat_cog",
        AF,
        (T4326, "COG"),
        "tile_meta",
        {
            "driver": "GTiff",
            "meta_driver": "GTiff",
            "layout": "COG",
            "pixels_of_arg0": True,
        },
        ONLY,
    ),
    # the driver name is resolved to GDAL's spelling: `cog` writes what `COG` does
    Case(
        "asformat_lower_cog",
        AF,
        (T4326, "cog"),
        "tile_meta",
        {
            "driver": "GTiff",
            "meta_driver": "GTiff",
            "layout": "COG",
            "pixels_of_arg0": True,
        },
        ONLY,
    ),
    Case(
        "asformat_virtual", AF, (VIRT, "GTiff"), "tile_meta", {"driver": "GTiff"}, ONLY
    ),
    Case(
        "asformat_bad_format",
        AF,
        (BYTE, "NoSuchDriver"),
        "raises",
        "NoSuchDriver",
        ONLY,
    ),
    Case("asformat_null_tile", AF, (None, "PNG"), "null", None, ONLY),
    Case("asformat_null_format", AF, (BYTE, None), "null", None, ONLY),
    Case("asformat_error_in", AF, (ERR, "PNG"), "error_tile", UPSTREAM_ERROR, ONLY),
    Case(
        "asformat_error_tile_passes_bad_format",
        AF,
        (ERR, "NoSuchDriver"),
        "error_tile",
        UPSTREAM_ERROR,
        ONLY,
    ),
    Case("asformat_corrupt", AF, (CORRUPT, "PNG"), "error_tile", None, ONLY),
    # --- band: one band of the tile as it is
    Case("band_2", BD, (BANDS3, 2), "ref", "band", ONLY),
    Case(
        "band_count",
        BD,
        (BANDS3, 3),
        "tile_meta",
        {"count": 1, "dtype": "float32", "crs": "EPSG:3857"},
        ONLY,
    ),
    Case("band_virtual", BD, (VIRT, 1), "tile_meta", {"count": 1}, ONLY),
    Case("band_zero", BD, (BANDS3, 0), "raises", "0", ONLY),
    Case("band_negative", BD, (BANDS3, -1), "raises", "-1", ONLY),
    Case("band_too_high", BD, (BANDS3, 4), "raises", "4", ONLY),
    Case("band_null_tile", BD, (None, 1), "null", None, ONLY),
    Case("band_null_index", BD, (BANDS3, None), "null", None, ONLY),
    Case("band_error_in", BD, (ERR, 1), "error_tile", UPSTREAM_ERROR, ONLY),
    Case(
        "band_error_tile_passes_bad_index",
        BD,
        (ERR, 0),
        "error_tile",
        UPSTREAM_ERROR,
        ONLY,
    ),
    Case("band_corrupt", BD, (CORRUPT, 1), "error_tile", None, ONLY),
    # --- convolve: the kernel laid over each pixel, not flipped, zero-padded, NoData adding 0, never NoData itself
    Case("convolve_asymmetric", CV, (RAMP, ASYMMETRIC), "ref", "convolve", ONLY),
    Case("convolve_identity", CV, (RAMP, IDENTITY), "ref", "convolve", ONLY),
    Case(
        "convolve_float64",
        CV,
        (RAMP, IDENTITY),
        "tile_meta",
        {"dtype": "float64", "crs": "EPSG:3857"},
        ONLY,
    ),
    Case(
        "convolve_virtual",
        CV,
        (VIRT, IDENTITY),
        "tile_meta",
        {"dtype": "float64", "width": 64, "height": 48},
        ONLY,
    ),
    Case(
        "convolve_even_kernel",
        CV,
        (RAMP, [[1.0, 2.0], [3.0, 4.0]]),
        "raises",
        "2x2",
        ONLY,
    ),
    Case(
        "convolve_ragged_kernel",
        CV,
        (RAMP, [[1.0, 2.0, 3.0], [4.0, 5.0]]),
        "raises",
        "kernel must be a rectangular",
        ONLY,
    ),
    Case("convolve_null_tile", CV, (None, IDENTITY), "null", None, ONLY),
    Case("convolve_null_kernel", CV, (RAMP, None), "null", None, ONLY),
    Case("convolve_error_in", CV, (ERR, IDENTITY), "error_tile", UPSTREAM_ERROR, ONLY),
    Case("convolve_corrupt", CV, (CORRUPT, IDENTITY), "error_tile", None, ONLY),
    # --- derivedband: the user's pixel function over the bands, a Float64 band out
    Case(
        "derivedband_double_plus_one",
        DB,
        (PLAIN, DOUBLE_PLUS_ONE, "double_plus_one"),
        "ref",
        "derived_plus_one",
        ONLY,
    ),
    Case(
        "derivedband_float64",
        DB,
        (PLAIN, DOUBLE_PLUS_ONE, "double_plus_one"),
        "tile_meta",
        {"dtype": "float64", "count": 1, "crs": "EPSG:3857"},
        ONLY,
    ),
    Case(
        "derivedband_virtual",
        DB,
        (VIRT, DOUBLE_PLUS_ONE, "double_plus_one"),
        "tile_meta",
        {"dtype": "float64", "width": 64},
        ONLY,
    ),
    Case(
        "derivedband_unknown_function",
        DB,
        (PLAIN, DOUBLE_PLUS_ONE, "nope"),
        "raises",
        "nope",
        ONLY,
    ),
    Case(
        "derivedband_syntax_error",
        DB,
        (PLAIN, "def broken(:\n", "broken"),
        "raises",
        "python_func",
        ONLY,
    ),
    Case(
        "derivedband_null_tile",
        DB,
        (None, DOUBLE_PLUS_ONE, "double_plus_one"),
        "null",
        None,
        ONLY,
    ),
    Case(
        "derivedband_null_function",
        DB,
        (PLAIN, DOUBLE_PLUS_ONE, None),
        "null",
        None,
        ONLY,
    ),
    Case(
        "derivedband_error_in",
        DB,
        (ERR, DOUBLE_PLUS_ONE, "double_plus_one"),
        "error_tile",
        UPSTREAM_ERROR,
        ONLY,
    ),
    Case(
        "derivedband_corrupt",
        DB,
        (CORRUPT, DOUBLE_PLUS_ONE, "double_plus_one"),
        "error_tile",
        None,
        ONLY,
    ),
    # --- filter: a focal minimum, maximum, mean or median over the valid pixels of the window, cut at the edges
    Case("filter_min", FI, (RAMP, 3, "min"), "ref", "filter", ONLY),
    Case("filter_max", FI, (RAMP, 3, "max"), "ref", "filter", ONLY),
    Case("filter_mean", FI, (RAMP, 3, "mean"), "ref", "filter", ONLY),
    Case("filter_median", FI, (RAMP, 3, "median"), "ref", "filter", ONLY),
    Case("filter_median_5", FI, (RAMP, 5, "median"), "ref", "filter", ONLY),
    Case("filter_upper_case", FI, (RAMP, 3, "MAX"), "ref", "filter", ONLY),
    Case(
        "filter_mean_float32",
        FI,
        (RAMP, 3, "mean"),
        "tile_meta",
        {"dtype": "float32"},
        ONLY,
    ),
    Case(
        "filter_keeps_type",
        FI,
        (BYTE, 3, "max"),
        "tile_meta",
        {"dtype": "uint8", "crs": "EPSG:3857"},
        ONLY,
    ),
    Case(
        "filter_virtual",
        FI,
        (VIRT, 3, "min"),
        "tile_meta",
        {"width": 64, "height": 48},
        ONLY,
    ),
    Case("filter_even_size", FI, (RAMP, 4, "min"), "raises", "4", ONLY),
    Case("filter_zero_size", FI, (RAMP, 0, "min"), "raises", "0", ONLY),
    Case("filter_fractional_size", FI, (RAMP, 3.9, "min"), "raises", "3.9", ONLY),
    Case("filter_bad_operation", FI, (RAMP, 3, "mode"), "raises", "mode", ONLY),
    Case("filter_null_tile", FI, (None, 3, "min"), "null", None, ONLY),
    Case("filter_null_size", FI, (RAMP, None, "min"), "null", None, ONLY),
    Case("filter_null_operation", FI, (RAMP, 3, None), "null", None, ONLY),
    Case("filter_error_in", FI, (ERR, 3, "min"), "error_tile", UPSTREAM_ERROR, ONLY),
    Case(
        "filter_error_tile_passes_bad_operation",
        FI,
        (ERR, 3, "mode"),
        "error_tile",
        UPSTREAM_ERROR,
        ONLY,
    ),
    Case("filter_corrupt", FI, (CORRUPT, 3, "min"), "error_tile", None, ONLY),
    # --- getsubdataset: a variable of a container file as a tile
    Case(
        "getsubdataset_band2", SD, (Build("rto_nc"), "Band2"), "ref", "subdataset", ONLY
    ),
    Case(
        "getsubdataset_bytes",
        SD,
        (Build("rto_nc_bytes"), "Band2"),
        "ref",
        "subdataset",
        ONLY,
    ),
    Case(
        "getsubdataset_missing", SD, (Build("rto_nc"), "nope"), "raises", "nope", ONLY
    ),
    Case(
        "getsubdataset_windowed",
        SD,
        (Build("rto_nc_windowed"), "Band2"),
        "error_fix",
        None,
        ONLY,
    ),
    Case("getsubdataset_plain_tile", SD, (T4326, "Band1"), "raises", "Band1", ONLY),
    Case("getsubdataset_null_tile", SD, (None, "Band1"), "null", None, ONLY),
    Case("getsubdataset_null_name", SD, (Build("rto_nc"), None), "null", None, ONLY),
    Case(
        "getsubdataset_error_in", SD, (ERR, "Band1"), "error_tile", UPSTREAM_ERROR, ONLY
    ),
    # --- initnodata: a NoData value is set; one the tile declares is kept
    Case(
        "initnodata_unset",
        IN,
        (U16,),
        "tile_meta",
        {"nodata": 65535.0, "dtype": "uint16", "pixels_of_arg0": True},
        ONLY,
    ),
    Case(
        "initnodata_kept",
        IN,
        (RAMP,),
        "tile_meta",
        {"nodata": -9999.0, "pixels_of_arg0": True},
        ONLY,
    ),
    # a NaN NoData value is a value like any other: kept, not replaced by the type's constant
    Case(
        "initnodata_nan_kept",
        IN,
        (Build("rto_nan_nodata"),),
        "tile_meta",
        {"nodata": float("nan"), "count": 2, "pixels_of_arg0": True},
        ONLY,
    ),
    Case(
        "initnodata_virtual",
        IN,
        (VIRT,),
        "tile_meta",
        {"nodata": -9999.0, "width": 64},
        ONLY,
    ),
    Case("initnodata_null_tile", IN, (None,), "null", None, ONLY),
    Case("initnodata_error_in", IN, (ERR,), "error_tile", UPSTREAM_ERROR, ONLY),
    Case("initnodata_corrupt", IN, (CORRUPT,), "error_tile", None, ONLY),
    # --- land_cover: class ids from the colours (the spectral thresholds), an Int32 band with -1 for NoData
    Case(
        "land_cover_spectral",
        LC,
        (RGB, "spectral", 6, 0),
        "ref",
        "land_cover_spectral",
        ONLY,
    ),
    Case(
        "land_cover_kmeans",
        LC,
        (RGB, "kmeans", 4, 0),
        "ref",
        "land_cover_spectral",
        ONLY,
    ),
    Case(
        "land_cover_sql_defaults",
        LC,
        (RGB,),
        "tile_meta",
        {"dtype": "int32", "count": 1, "nodata": -1.0, "crs": "EPSG:3857"},
        ONLY,
    ),
    Case(
        "land_cover_two_bands",
        LC,
        (Build("rto_two_bands"), "spectral", 6, 0),
        "error_fix",
        None,
        ONLY,
    ),
    Case("land_cover_bad_method", LC, (RGB, "sinc", 6, 0), "raises", "sinc", ONLY),
    Case("land_cover_zero_clusters", LC, (RGB, "kmeans", 0, 0), "raises", "0", ONLY),
    Case("land_cover_null_tile", LC, (None, "spectral", 6, 0), "null", None, ONLY),
    Case("land_cover_null_method", LC, (RGB, None, 6, 0), "null", None, ONLY),
    Case(
        "land_cover_error_in",
        LC,
        (ERR, "spectral", 6, 0),
        "error_tile",
        UPSTREAM_ERROR,
        ONLY,
    ),
    Case(
        "land_cover_corrupt", LC, (CORRUPT, "spectral", 6, 0), "error_tile", None, ONLY
    ),
    # --- percentile_stretch: each band scaled between two percentiles of its valid pixels to 8 bits, 0 for NoData
    Case(
        "percentile_stretch_2_98",
        PS,
        (RAMP, 2.0, 98.0),
        "ref",
        "percentile_stretch",
        ONLY,
    ),
    Case(
        "percentile_stretch_uint8",
        PS,
        (RAMP, 2.0, 98.0),
        "tile_meta",
        {"dtype": "uint8", "nodata": 0.0, "crs": "EPSG:3857"},
        ONLY,
    ),
    Case(
        "percentile_stretch_virtual",
        PS,
        (VIRT, 2.0, 98.0),
        "tile_meta",
        {"dtype": "uint8", "width": 64},
        ONLY,
    ),
    Case(
        "percentile_stretch_low_not_below_high",
        PS,
        (RAMP, 50.0, 50.0),
        "raises",
        "lo_pct",
        ONLY,
    ),
    Case(
        "percentile_stretch_out_of_range", PS, (RAMP, 2.0, 101.0), "raises", "101", ONLY
    ),
    Case("percentile_stretch_null_tile", PS, (None, 2.0, 98.0), "null", None, ONLY),
    Case(
        "percentile_stretch_null_percentile", PS, (RAMP, None, 98.0), "null", None, ONLY
    ),
    Case(
        "percentile_stretch_error_in",
        PS,
        (ERR, 2.0, 98.0),
        "error_tile",
        UPSTREAM_ERROR,
        ONLY,
    ),
    Case(
        "percentile_stretch_error_tile_passes_bad_range",
        PS,
        (ERR, 50.0, 50.0),
        "error_tile",
        UPSTREAM_ERROR,
        ONLY,
    ),
    Case(
        "percentile_stretch_corrupt", PS, (CORRUPT, 2.0, 98.0), "error_tile", None, ONLY
    ),
    # --- setcrs: the CRS is stamped from a string, the pixels and the geotransform stay
    Case(
        "setcrs_epsg",
        SC,
        (T4326, "EPSG:3857"),
        "tile_meta",
        {"crs": "EPSG:3857", "pixels_of_arg0": True, "transform_of_arg0": True},
        ONLY,
    ),
    Case(
        "setcrs_esri",
        SC,
        (T4326, "ESRI:54008"),
        "tile_meta",
        {"crs": "ESRI:54008", "pixels_of_arg0": True},
        ONLY,
    ),
    Case(
        "setcrs_digit_string",
        SC,
        (T4326, "3857"),
        "tile_meta",
        {"crs": "EPSG:3857", "srid": 3857},
        ONLY,
    ),
    Case(
        "setcrs_wkt",
        SC,
        (T4326, Build("wkt_3857")),
        "tile_meta",
        {"crs": "EPSG:3857"},
        ONLY,
    ),
    Case(
        "setcrs_proj4",
        SC,
        (
            T4326,
            "+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +k=1 +units=m +nadgrids=@null +wktext +no_defs",
        ),
        "tile_meta",
        {"crs": "EPSG:3857"},
        ONLY,
    ),
    Case(
        "setcrs_virtual",
        SC,
        (VIRT, "EPSG:3857"),
        "tile_meta",
        {"crs": "EPSG:3857", "width": 64},
        ONLY,
    ),
    Case("setcrs_not_a_crs", SC, (T4326, "not-a-crs"), "raises", "not-a-crs", ONLY),
    Case("setcrs_null_tile", SC, (None, "EPSG:3857"), "null", None, ONLY),
    Case("setcrs_null_crs", SC, (T4326, None), "null", None, ONLY),
    Case("setcrs_error_in", SC, (ERR, "EPSG:3857"), "error_tile", UPSTREAM_ERROR, ONLY),
    Case(
        "setcrs_error_tile_passes_bad_crs",
        SC,
        (ERR, "not-a-crs"),
        "error_tile",
        UPSTREAM_ERROR,
        ONLY,
    ),
    Case("setcrs_corrupt", SC, (CORRUPT, "EPSG:3857"), "error_tile", None, ONLY),
    # --- setsrid: the CRS is stamped from a code (0 clears it), the pixels and the geotransform stay
    Case(
        "setsrid_3857",
        SS,
        (T4326, 3857),
        "tile_meta",
        {
            "crs": "EPSG:3857",
            "srid": 3857,
            "pixels_of_arg0": True,
            "transform_of_arg0": True,
        },
        ONLY,
    ),
    Case(
        "setsrid_esri_code",
        SS,
        (T4326, 54008),
        "tile_meta",
        {"crs": "ESRI:54008"},
        ONLY,
    ),
    Case(
        "setsrid_zero_clears",
        SS,
        (T4326, 0),
        "tile_meta",
        {"no_crs": True, "pixels_of_arg0": True},
        ONLY,
    ),
    Case(
        "setsrid_virtual",
        SS,
        (VIRT, 3857),
        "tile_meta",
        {"crs": "EPSG:3857", "width": 64},
        ONLY,
    ),
    Case(
        "setsrid_virtual_zero",
        SS,
        (VIRT, 0),
        "tile_meta",
        {"no_crs": True, "width": 64},
        ONLY,
    ),
    Case("setsrid_negative", SS, (T4326, -1), "raises", "-1", ONLY),
    Case("setsrid_unknown_code", SS, (T4326, 999999), "raises", "999999", ONLY),
    Case("setsrid_null_tile", SS, (None, 3857), "null", None, ONLY),
    Case("setsrid_null_srid", SS, (T4326, None), "null", None, ONLY),
    Case("setsrid_error_in", SS, (ERR, 3857), "error_tile", UPSTREAM_ERROR, ONLY),
    Case(
        "setsrid_error_tile_passes_bad_srid",
        SS,
        (ERR, -1),
        "error_tile",
        UPSTREAM_ERROR,
        ONLY,
    ),
    Case("setsrid_corrupt", SS, (CORRUPT, 3857), "error_tile", None, ONLY),
    # --- updatetype: each band cast to the type; floats round to the nearest integer and clamp to the type's range
    Case("updatetype_int32", UT, (RAMP, "Int32"), "ref", "cast", ONLY),
    Case(
        "updatetype_int32_props",
        UT,
        (RAMP, "Int32"),
        "tile_meta",
        {"dtype": "int32", "nodata": -9999.0, "crs": "EPSG:3857"},
        ONLY,
    ),
    # clamps the out-of-range values and casts the NaN pixel to 0
    Case("updatetype_byte_clamps", UT, (CAST, "Byte"), "ref", "cast", ONLY),
    # integer to integer: past Int16's range is clamped, not wrapped (and not an overflow error under NumPy 2)
    Case(
        "updatetype_uint16_clamps",
        UT,
        (Build("rto_u16_wide"), "Int16"),
        "ref",
        "cast",
        ONLY,
    ),
    Case(
        "updatetype_byte_drops_nodata",
        UT,
        (RAMP, "Byte"),
        "tile_meta",
        {"dtype": "uint8", "nodata": None},
        ONLY,
    ),
    Case(
        "updatetype_lower_case",
        UT,
        (RAMP, "float64"),
        "tile_meta",
        {"dtype": "float64"},
        ONLY,
    ),
    Case(
        "updatetype_virtual",
        UT,
        (VIRT, "Int16"),
        "tile_meta",
        {"dtype": "int16", "width": 64},
        ONLY,
    ),
    Case("updatetype_bad_type", UT, (RAMP, "Float128"), "raises", "Float128", ONLY),
    Case("updatetype_null_tile", UT, (None, "Int32"), "null", None, ONLY),
    Case("updatetype_null_type", UT, (RAMP, None), "null", None, ONLY),
    Case("updatetype_error_in", UT, (ERR, "Int32"), "error_tile", UPSTREAM_ERROR, ONLY),
    Case(
        "updatetype_error_tile_passes_bad_type",
        UT,
        (ERR, "Float128"),
        "error_tile",
        UPSTREAM_ERROR,
        ONLY,
    ),
    Case("updatetype_corrupt", UT, (CORRUPT, "Int32"), "error_tile", None, ONLY),
    # a tile decoded over the read cap is an error tile with a Fix, never a failed task (READ_CAP_ROWS lowers the cap)
    Case(
        "tile_op_decoded_over_cap",
        UT,
        (T4326, "Int32"),
        "error_tile_has",
        "the tile is 12 KiB decoded, over this compute's read cap (4 KiB). Fix: retile it first (gbx_rst_retile)",
        ONLY,
    ),
)
