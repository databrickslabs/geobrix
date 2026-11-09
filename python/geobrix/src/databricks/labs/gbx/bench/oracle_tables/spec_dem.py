"""Oracle table for the DEM functions of the GDAL spec engine family: inputs -> expected, run at every level by
test_family_cross_tier.py.

Expected values come from independent numpy references written from each algorithm's definition (oracle_refs.py:
Horn's gradients, Riley's ruggedness index, the standard hillshade formula), compared on the same NoData mask: a
pixel is NoData when its 3 x 3 window holds NoData or crosses the border. The fixtures are small exact surfaces:
`dem_3857` (a smooth surface with a 3 x 3 NoData hole, on a 10 m grid), a plane that rises to the east (aspect
270 degrees compass) and a flat surface (no aspect).
"""

import os
from dataclasses import replace

from databricks.labs.gbx.bench.oracle_tables._uc_tile import edges, share
from databricks.labs.gbx.bench.oracles import (
    UPSTREAM_ERROR,
    Build,
    Case,
    _dem_surface,
    _gtiff,
    _tile,
    builder,
)

SL, AS, HS = "gbx_rst_slope", "gbx_rst_aspect", "gbx_rst_hillshade"
TRI, TPI, RGH = "gbx_rst_tri", "gbx_rst_tpi", "gbx_rst_roughness"
CR = "gbx_rst_color_relief"
DEM, PLANE, FLAT = Build("dem_3857"), Build("dem_plane_east"), Build("dem_flat")
ERR, CORRUPT, VIRT = Build("tile_error"), Build("tile_corrupt"), Build("dem_file")
ONLY = ("session",)
MULTI, INT16, NOCRS, GEO = (
    Build("dem_multiband"),
    Build("dem_int16"),
    Build("dem_nocrs"),
    Build("dem_4326"),
)


@builder("dem_plane_east")
def _plane_east(tmpdir):
    import numpy as np

    z = np.tile(
        2.0 * np.arange(16), (16, 1)
    )  # rises 2 m per pixel (0.2 per metre) to the east
    return _tile(_gtiff(z[np.newaxis], "EPSG:3857"))


@builder("dem_flat")
def _flat(tmpdir):
    import numpy as np

    return _tile(_gtiff(np.full((1, 16, 16), 5.0), "EPSG:3857"))


@builder("dem_multiband")
def _multiband(tmpdir):
    """Three bands, band 1 the DEM surface: a DEM function reads band 1 only."""
    import numpy as np

    z = _dem_surface()
    return _tile(_gtiff(np.stack([z, z * 2.0, z + 5.0]), "EPSG:3857"))


@builder("dem_int16")
def _int16(tmpdir):
    """The surface rounded to Int16 with its hole at NoData -32768."""
    import numpy as np

    z = _dem_surface()
    z = np.where(z == -9999.0, -32768.0, np.rint(z))
    return _tile(_gtiff(z[np.newaxis], "EPSG:3857", nodata=-32768, dtype="int16"))


@builder("dem_nocrs")
def _nocrs(tmpdir):
    """The surface on a 10 unit grid with no CRS: the horizontal and vertical units are taken as equal."""
    return _tile(_gtiff(_dem_surface()[None], None))


@builder("dem_4326")
def _dem_4326(tmpdir):
    """The surface on a 0.001 degree grid at 51.5 degrees north (EPSG:4326): GDAL derives the metres per degree."""
    return _tile(
        _gtiff(_dem_surface()[None], "EPSG:4326", origin=(-0.032, 51.532), px=0.001)
    )


@builder("dem_file")
def _file(tmpdir):
    """The DEM as a virtual tile: a path to its GeoTIFF, no bytes."""
    from databricks.labs.gbx.bench.oracles import _build

    path = os.path.join(tmpdir, "oracle_dem.tif")
    with open(path, "wb") as fh:
        fh.write(bytes(_build("dem_3857", tmpdir)["raster"]))
    return _tile(None, path, {"col_off": 0, "row_off": 0, "width": 64, "height": 64})


# The color tables of the color_relief rows (the function takes the table's text). `dem_3857` runs from about 90 to 160.
_RGB = "90 255 0 0\n120 0 255 0\n160 0 0 255\nnv 10 20 30"
_RGBA = "90 255 0 0 255\n120 0 255 0 128\n160 0 0 255 0\nnv 10 20 30 77"
_RGB_NO_NV = "90 255 0 0\n120 0 255 0\n160 0 0 255"
_RGBA_NO_NV = "90 255 0 0 255\n120 0 255 0 128\n160 0 0 255 0"
# Stops that stop inside the DEM's range, so both ends clamp.
_NARROW = "100 255 0 0\n140 0 0 255\nnv 1 2 3"
# A share of the valid pixels' range, entries out of order, a name for each color, and a loose layout.
_PERCENT = "0% 0 0 255\n50% 0 255 0\n100% 255 0 0\nnv 0 0 0"
_UNSORTED = "160 0 0 255\n90 255 0 0\n120 0 255 0\nnv 0 0 0"
_NAMED = "90 red\n120 Green\n160 blue\nnv white"
_LOOSE = "# a ramp\n90,255,0,0\n\n  160\t0 0 255  \r\n"
_session_dem_rows = (
    # slope: the engine's own rows (SPEC_SE) hold its edge cases; this one is the docs example.
    Case("slope_example", SL, (DEM, "degrees"), "ref", "slope", ONLY),
    Case(
        "slope_bad_unit",
        SL,
        (DEM, "radians"),
        "raises",
        "unit must be one of 'degrees', 'percent'",
        ONLY,
    ),
    Case("aspect_compass", AS, (DEM, False, False), "ref", "aspect", ONLY),
    Case("aspect_trig", AS, (DEM, True, False), "ref", "aspect", ONLY),
    Case("aspect_plane_east", AS, (PLANE, False, False), "ref", "aspect", ONLY),
    Case("aspect_flat_nodata", AS, (FLAT, False, False), "ref", "aspect", ONLY),
    Case("aspect_flat_zero", AS, (FLAT, False, True), "ref", "aspect", ONLY),
    # GDAL drops the output's NoData value with zero_for_flat; the engine puts it back at the border and around the hole
    Case("aspect_zero_for_flat_hole", AS, (DEM, False, True), "ref", "aspect", ONLY),
    Case("aspect_virtual", AS, (VIRT, False, False), "ref", "aspect", ONLY),
    Case("aspect_null_param", AS, (DEM, None, False), "null", None, ONLY),
    Case("aspect_null_tile", AS, (None, False, False), "null", None, ONLY),
    Case(
        "aspect_error_tile",
        AS,
        (ERR, False, False),
        "error_tile",
        UPSTREAM_ERROR,
        ONLY,
    ),
    Case("aspect_corrupt", AS, (CORRUPT, False, False), "error_tile", None, ONLY),
    # a tile decoded over the read cap is an error tile with a Fix, never a failed task (READ_CAP_ROWS lowers the cap)
    Case(
        "dem_decoded_over_cap",
        AS,
        (Build("tile_4326"), False, False),
        "error_tile_has",
        "the tile is 12 KiB decoded, over this compute's read cap (4 KiB). Fix: retile it first (gbx_rst_retile)",
        ONLY,
    ),
    Case(
        "hillshade_default",
        HS,
        (DEM, 315.0, 45.0, 1.0, None, None),
        "ref",
        "hillshade",
        ONLY,
    ),
    Case(
        "hillshade_params",
        HS,
        (DEM, 270.0, 30.0, 2.0, None, None),
        "ref",
        "hillshade",
        ONLY,
    ),
    Case(
        "hillshade_scales",
        HS,
        (DEM, 315.0, 45.0, 1.0, 2.0, 0.5),
        "ref",
        "hillshade",
        ONLY,
    ),
    Case("hillshade_sql_defaults", HS, (DEM,), "ref", "hillshade", ONLY),
    Case(
        "hillshade_half_scale",
        HS,
        (DEM, 315.0, 45.0, 1.0, 2.0, None),
        "raises",
        "xscale and yscale must be supplied together",
        ONLY,
    ),
    Case("hillshade_null_param", HS, (DEM, None, 45.0, 1.0), "null", None, ONLY),
    Case("tri", TRI, (DEM,), "ref", "tri", ONLY),
    Case("tri_virtual", TRI, (VIRT,), "ref", "tri", ONLY),
    Case("tri_null_tile", TRI, (None,), "null", None, ONLY),
    Case("tri_error_tile", TRI, (ERR,), "error_tile", UPSTREAM_ERROR, ONLY),
    Case("tpi", TPI, (DEM,), "ref", "tpi", ONLY),
    Case("roughness", RGH, (DEM,), "ref", "roughness", ONLY),
    # color_relief: the table is text. Stops, clamping outside them and the nv color against numpy; a table with an
    # alpha column is RGBA; a NoData pixel takes the nv color, and without one it is transparent (RGBA) or black
    Case("color_relief_rgb", CR, (DEM, _RGB), "ref", "color_relief", ONLY),
    Case("color_relief_rgba", CR, (DEM, _RGBA), "ref", "color_relief", ONLY),
    Case("color_relief_clamped", CR, (DEM, _NARROW), "ref", "color_relief", ONLY),
    Case("color_relief_named", CR, (DEM, _NAMED), "ref", "color_relief", ONLY),
    Case("color_relief_percent", CR, (DEM, _PERCENT), "ref", "color_relief", ONLY),
    Case("color_relief_unsorted", CR, (DEM, _UNSORTED), "ref", "color_relief", ONLY),
    Case("color_relief_loose_layout", CR, (DEM, _LOOSE), "ref", "color_relief", ONLY),
    Case("color_relief_hole_black", CR, (DEM, _RGB_NO_NV), "ref", "color_relief", ONLY),
    Case(
        "color_relief_hole_transparent",
        CR,
        (DEM, _RGBA_NO_NV),
        "ref",
        "color_relief",
        ONLY,
    ),
    Case("color_relief_int16", CR, (INT16, _RGB), "ref", "color_relief", ONLY),
    Case("color_relief_multiband", CR, (MULTI, _RGB), "ref", "color_relief", ONLY),
    Case("color_relief_virtual", CR, (VIRT, _RGB), "ref", "color_relief", ONLY),
    Case("color_relief_null_tile", CR, (None, _RGB), "null", None, ONLY),
    Case("color_relief_null_table", CR, (DEM, None), "null", None, ONLY),
    Case(
        "color_relief_error_tile", CR, (ERR, _RGB), "error_tile", UPSTREAM_ERROR, ONLY
    ),
    Case("color_relief_corrupt", CR, (CORRUPT, _RGB), "error_tile", None, ONLY),
    # a table that is not a color table raises, naming the bad line (or the file path that was passed in its place)
    Case(
        "color_relief_path",
        CR,
        (DEM, "/Volumes/c/s/v/colors.txt"),
        "raises",
        "file path",
        ONLY,
    ),
    Case("color_relief_empty", CR, (DEM, ""), "raises", "is empty", ONLY),
    Case(
        "color_relief_only_nv",
        CR,
        (DEM, "nv 0 0 0"),
        "raises",
        "no color entries",
        ONLY,
    ),
    Case(
        "color_relief_bad_line",
        CR,
        (DEM, "90 255 0 0\n120 0 255"),
        "raises",
        "line 2",
        ONLY,
    ),
    Case(
        "color_relief_unknown_name",
        CR,
        (DEM, "90 red\n120 pink"),
        "raises",
        "pink",
        ONLY,
    ),
    Case(
        "color_relief_channel_over_255",
        CR,
        (DEM, "90 255 0 256"),
        "raises",
        "line 1",
        ONLY,
    ),
)

# One DEM input path for every algorithm, as `gdal raster slope` reads it: band 1 of a multi-band tile, an Int16 DEM
# whose NoData is -32768 (a Float32 output with the same mask), pixel units for a tile with no CRS, and GDAL's own
# metres per degree for a tile in EPSG:4326. Slope's rows carry the four; the other functions repeat the two that do not
# depend on a scale, and the scale-aware hillshade the CRS-less one.
_DEM_ALGORITHMS = (
    ("aspect", AS, (False, False)),
    ("hillshade", HS, (315.0, 45.0, 1.0, None, None)),
    ("tri", TRI, ()),
    ("tpi", TPI, ()),
    ("roughness", RGH, ()),
)
_session_dem_rows += (
    Case("dem_multiband", SL, (MULTI, "degrees"), "ref", "slope", ONLY),
    Case("dem_int16", SL, (INT16, "degrees"), "ref", "slope", ONLY),
    Case("dem_nocrs", SL, (NOCRS, "degrees"), "ref", "slope", ONLY),
    Case("dem_4326", SL, (GEO, "degrees"), "ref", "gdaldem_slope", ONLY),
    Case("dem_4326_aspect", AS, (GEO, False, False), "ref", "aspect", ONLY),
    Case(
        "dem_nocrs_hillshade",
        HS,
        (NOCRS, 315.0, 45.0, 1.0, None, None),
        "ref",
        "hillshade",
        ONLY,
    ),
) + tuple(
    Case(f"dem_{kind}_{short}", fn, (inp, *extra), "ref", short, ONLY)
    for kind, inp in (("multiband", MULTI), ("int16", INT16))
    for short, fn, extra in _DEM_ALGORITHMS
)

# The contour lines: `vz_columns` (raster_vectorize.py) rises by one per pixel to the east, so the line of value v is
# the vertical line half way between the centres of columns v - 0.5 and v + 0.5, at x = 400000 + (v + 0.5) * 10.
CT = "gbx_rst_contour"
COLUMNS = Build("vz_columns")


def _ramp(nodata, hole):
    """A 10 x 10 surface that rises by one per pixel to the east (as `vz_columns` does), with a block of `hole` in the
    columns 4 and 5 (the lines of 2.5 and 6.5 stay clear of it)."""
    import numpy as np

    z = np.tile(np.arange(10, dtype="float64"), (10, 1))
    z[3:6, 4:6] = hole
    return _tile(_gtiff(z[np.newaxis], "EPSG:3857", nodata=nodata))


@builder("contour_ramp_sentinel")
def _ramp_sentinel(tmpdir):
    """The ramp with a block of -9999 in a tile that declares no NoData: the sentinel is not data."""
    return _ramp(None, -9999.0)


@builder("contour_ramp_declared")
def _ramp_declared(tmpdir):
    """The ramp with a block of -1 declared as NoData (-1 lies below every pixel that is data)."""
    return _ramp(-1.0, -1.0)


@builder("contour_all_nodata")
def _all_nodata(tmpdir):
    """Every pixel is NoData: there is nothing to trace."""
    import numpy as np

    return _tile(_gtiff(np.full((1, 10, 10), -9999.0), "EPSG:3857"))


SENTINEL, DECLARED = Build("contour_ramp_sentinel"), Build("contour_ramp_declared")
_X = {1.5: 400020.0, 3.5: 400040.0}
_REF_ARGS = {  # the features_ref rows: their arguments after the tile are shared by the session and UC rows
    "contour_dem_levels": (DEM, [100.0, 110.0, 120.0, 130.0]),
    "contour_dem_interval": (DEM, [], 5.0, 2.0),
    "contour_dem_hole": (DEM, [90.0, 150.0]),
    "contour_dem_multiband": (MULTI, [100.0, 120.0]),
    "contour_dem_int16": (INT16, [100.5, 120.5]),
    "contour_dem_virtual": (VIRT, [100.0, 120.0]),
}

_session_dem_rows += (
    Case("contour_levels", CT, (COLUMNS, [1.5, 3.5]), "contours", _X, ONLY),
    Case(
        "contour_interval",
        CT,
        (COLUMNS, [], 2.0, 0.5),
        "contours",
        {0.5: 400010.0, 2.5: 400030.0, 4.5: 400050.0},
        ONLY,
    ),
    Case(
        "contour_virtual",
        CT,
        (Build("vz_columns_virtual"), [1.5, 3.5]),
        "contours",
        _X,
        ONLY,
    ),
    # the value of a line is the level asked for, to the last digit (GDAL's layer holds it rounded to six decimals)
    Case(
        "contour_level_exact",
        CT,
        (COLUMNS, [1.1234567891, 2.718281828459045]),
        "contours",
        {1.1234567891: 400016.234567891, 2.718281828459045: 400032.18281828459},
        ONLY,
    ),
    Case(
        "contour_interval_exact",
        CT,
        (COLUMNS, [], 0.7, 0.15),
        "contours",
        {0.15 + k * 0.7: 400000.0 + (0.15 + k * 0.7 + 0.5) * 10 for k in range(7)},
        ONLY,
    ),
    # levels win over an interval; the list is sorted, without repeats and NULLs before GDAL reads it
    Case(
        "contour_levels_win",
        CT,
        (COLUMNS, [1.5], 2.0, 0.5),
        "contours",
        {1.5: 400020.0},
        ONLY,
    ),
    Case("contour_levels_unsorted", CT, (COLUMNS, [3.5, 1.5]), "contours", _X, ONLY),
    Case(
        "contour_levels_duplicate", CT, (COLUMNS, [1.5, 3.5, 1.5]), "contours", _X, ONLY
    ),
    Case(
        "contour_levels_null_element",
        CT,
        (COLUMNS, [1.5, None, 3.5]),
        "contours",
        _X,
        ONLY,
    ),
    Case("contour_levels_none_cross", CT, (COLUMNS, [99.5]), "contours", {}, ONLY),
    # a NoData region has no line: neither the declared value nor the -9999 sentinel of a tile that declares none
    Case(
        "contour_nodata_sentinel",
        CT,
        (SENTINEL, [-5000.0, 2.5, 6.5]),
        "contours",
        {2.5: 400030.0, 6.5: 400070.0},
        ONLY,
    ),
    Case(
        "contour_nodata_declared",
        CT,
        (DECLARED, [-0.5, 2.5, 6.5]),
        "contours",
        {2.5: 400030.0, 6.5: 400070.0},
        ONLY,
    ),
    Case(
        "contour_all_nodata",
        CT,
        (Build("contour_all_nodata"), [], 1.0),
        "contours",
        {},
        ONLY,
    ),
    # an argument mistake raises, naming the value; a NULL, error or unreadable tile is NULL
    Case(
        "contour_no_interval",
        CT,
        (COLUMNS, [], 0.0, 0.0),
        "raises",
        "interval must be > 0",
        ONLY,
    ),
    Case(
        "contour_null_interval",
        CT,
        (COLUMNS, [], None),
        "raises",
        "interval must be > 0",
        ONLY,
    ),
    Case(
        "contour_nonfinite_interval",
        CT,
        (COLUMNS, [], float("inf")),
        "raises",
        ("got inf", "got Infinity"),
        ONLY,
    ),
    Case(
        "contour_nonfinite_level",
        CT,
        (COLUMNS, [float("inf")]),
        "raises",
        "levels must be finite",
        ONLY,
    ),
    Case(
        "contour_blank_attr",
        CT,
        (COLUMNS, [1.5], 0.0, 0.0, " "),
        "raises",
        "attr_field must be non-empty",
        ONLY,
    ),
    Case("contour_null_tile", CT, (None, [1.5]), "null", None, ONLY),
    Case("contour_error_tile", CT, (ERR, [1.5]), "null", None, ONLY),
    Case("contour_corrupt", CT, (CORRUPT, [1.5]), "null", None, ONLY),
    # a virtual tile whose file is not there is data, not a mistake: NULL, the reason logged once
    Case("contour_missing_path", CT, (Build("vz_missing"), [1.5]), "null", None, ONLY),
    # The lines against GDAL's older `gdal.ContourGenerateEx` (an entry point apart from `raster contour`), which draws
    # the same lines: exact. The earlier scikit-image lines, still the Unity Catalog kernel's, are equivalent to them:
    # the same levels and line counts, within half a pixel, each up to a pixel shorter (see check_features_ref).
    *(
        Case(cid, CT, args, "features_ref", "exact", ONLY)
        for cid, args in _REF_ARGS.items()
    ),
)


# Unity Catalog: the surface of a materialized DEM tile is the session's; a virtual tile is refused with a Fix, a
# corrupt one is NULL, and a unit that is not degrees or percent is a wrong argument (the session raises too).
SPEC_DEM = share(
    _session_dem_rows,
    "slope_example", "aspect_compass", "aspect_trig", "aspect_plane_east", "aspect_flat_nodata", "aspect_flat_zero",
    "aspect_null_param", "aspect_null_tile", "aspect_error_tile",
    "hillshade_default", "hillshade_params", "hillshade_scales", "hillshade_sql_defaults", "hillshade_half_scale",
    "hillshade_null_param", "tri", "tri_null_tile", "tri_error_tile", "tpi", "roughness",
    "contour_levels", "contour_interval", "contour_null_tile", "contour_error_tile", "contour_corrupt",
    "contour_levels_win", "contour_levels_unsorted", "contour_levels_duplicate", "contour_levels_null_element",
    "contour_level_exact", "contour_interval_exact",
    "contour_levels_none_cross", "contour_nodata_sentinel", "contour_nodata_declared", "contour_all_nodata",
    "contour_no_interval", "contour_null_interval", "contour_nonfinite_interval", "contour_nonfinite_level",
    "contour_blank_attr",
) + tuple(
    replace(c, cid=f"uc_{c.cid}", expect="equivalent", surfaces=("uc",))
    for c in _session_dem_rows
    if c.kind == "features_ref" and c.args[0] is not VIRT
) + edges(
    slope=("degrees",), aspect=(False, False), hillshade=(315.0, 45.0, 1.0), tpi=(), tri=(), roughness=(),
    contour=([1.5],),
) + (
    Case("uc_slope_percent", SL, (DEM, "percent"), "ref", "slope", ("uc",)),
    Case("uc_slope_percent_cased", SL, (DEM, "Percent"), "ref", "slope", ("uc",)),
    Case("uc_slope_scales", SL, (DEM, "degrees", 2.0, 0.5), "ref", "slope", ("uc",)),
    Case("uc_slope_half_scale", SL, (DEM, "degrees", 2.0, None), "raises", "xscale and yscale must be supplied together", ("uc",)),
    Case("uc_slope_bad_unit", SL, (DEM, "radians"), "uc_refused", None, ("uc",)),
    Case("uc_slope_null_unit", SL, (DEM, None), "null", None, ("uc",)),
    Case("uc_slope_null_tile", SL, (None, "degrees"), "null", None, ("uc",)),
    Case("uc_slope_error_tile", SL, (ERR, "radians"), "error_tile", UPSTREAM_ERROR, ("uc",)),
)  # fmt: skip
