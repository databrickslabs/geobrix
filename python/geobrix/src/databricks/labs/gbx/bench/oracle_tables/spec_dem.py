"""Oracle table for the DEM functions of the GDAL spec engine family: inputs -> expected, run at every level by
test_family_cross_tier.py.

Expected values come from independent numpy references written from each algorithm's definition (oracle_refs.py:
Horn's gradients, Riley's ruggedness index, the standard hillshade formula), compared on the same NoData mask: a
pixel is NoData when its 3 x 3 window holds NoData or crosses the border. The fixtures are small exact surfaces:
`dem_3857` (a smooth surface with a 3 x 3 NoData hole, on a 10 m grid), a plane that rises to the east (aspect
270 degrees compass) and a flat surface (no aspect).
"""

import os

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


def _colour_table(tmpdir, name, alpha):
    """A gdaldem colour file of oracle_refs.COLOR_STOPS (and its `nv` line), RGB or RGBA."""
    from databricks.labs.gbx.bench import oracle_refs

    n = 4 if alpha else 3
    lines = [
        f"{e:g} " + " ".join(str(c) for c in col[:n])
        for e, col in oracle_refs.COLOR_STOPS
    ]
    lines.append("nv " + " ".join(str(c) for c in oracle_refs.COLOR_NV[:n]))
    path = os.path.join(tmpdir, name)
    with open(path, "w") as fh:
        fh.write("\n".join(lines) + "\n")
    return path


@builder("cr_table")
def _cr_table(tmpdir):
    return _colour_table(tmpdir, "oracle_colors.txt", False)


@builder("cr_table_alpha")
def _cr_table_alpha(tmpdir):
    return _colour_table(tmpdir, "oracle_colors_alpha.txt", True)


@builder("cr_table_missing")
def _cr_table_missing(tmpdir):
    return os.path.join(tmpdir, "no_such_colors.txt")


TABLE, TABLE_A = Build("cr_table"), Build("cr_table_alpha")
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
    Case("tri_null_tile", TRI, (None,), "null", None, ONLY),
    Case("tri_error_tile", TRI, (ERR,), "error_tile", UPSTREAM_ERROR, ONLY),
    Case("tpi", TPI, (DEM,), "ref", "tpi", ONLY),
    Case("roughness", RGH, (DEM,), "ref", "roughness", ONLY),
    # color_relief: stops, clamping outside them and the nv colour against numpy; a table with an alpha column is RGBA
    Case("color_relief_rgb", CR, (DEM, TABLE), "ref", "color_relief_rgb", ONLY),
    Case("color_relief_rgba", CR, (DEM, TABLE_A), "ref", "color_relief_rgba", ONLY),
    Case("color_relief_virtual", CR, (VIRT, TABLE), "ref", "color_relief_rgb", ONLY),
    Case("color_relief_null_tile", CR, (None, TABLE), "null", None, ONLY),
    Case("color_relief_null_table", CR, (DEM, None), "null", None, ONLY),
    Case(
        "color_relief_error_tile", CR, (ERR, TABLE), "error_tile", UPSTREAM_ERROR, ONLY
    ),
    Case("color_relief_corrupt", CR, (CORRUPT, TABLE), "error_tile", None, ONLY),
    # a colour file that does not exist raises and names the path
    Case(
        "color_relief_missing_table",
        CR,
        (DEM, Build("cr_table_missing")),
        "raises",
        "no_such_colors",
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

_session_dem_rows += (
    Case(
        "contour_levels",
        CT,
        (COLUMNS, [1.5, 3.5]),
        "contours",
        {1.5: 400020.0, 3.5: 400040.0},
        ONLY,
    ),
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
        {1.5: 400020.0, 3.5: 400040.0},
        ONLY,
    ),
    Case("contour_null_tile", CT, (None, [1.5]), "null", None, ONLY),
    Case("contour_error_tile", CT, (ERR, [1.5]), "null", None, ONLY),
    Case("contour_corrupt", CT, (CORRUPT, [1.5]), "null", None, ONLY),
    Case(
        "contour_missing_path",
        CT,
        (Build("vz_missing"), [1.5]),
        "raises",
        "gbx_missing_tile",
        ONLY,
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
    # no levels and no interval makes no line; an empty value field is refused too (the session raises as well)
    Case("uc_contour_no_interval", CT, (COLUMNS, [], 0.0, 0.0), "raises", "interval must be > 0", ("uc",)),
    Case("uc_contour_empty_attr", CT, (COLUMNS, [1.5], 0.0, 0.0, " "), "raises", "attr_field must be non-empty", ("uc",)),
)  # fmt: skip
