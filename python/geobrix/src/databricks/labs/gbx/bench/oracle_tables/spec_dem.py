"""Oracle table for the DEM functions of the GDAL spec engine family: inputs -> expected, run at every level by
test_family_cross_tier.py.

Expected values come from independent numpy references written from each algorithm's definition (oracle_refs.py:
Horn's gradients, Riley's ruggedness index, the standard hillshade formula), compared on the same NoData mask: a
pixel is NoData when its 3 x 3 window holds NoData or crosses the border. The fixtures are small exact surfaces:
`dem_3857` (a smooth surface with a 3 x 3 NoData hole, on a 10 m grid), a plane that rises to the east (aspect
270 degrees compass) and a flat surface (no aspect).
"""

import os

from databricks.labs.gbx.bench.oracles import (
    UPSTREAM_ERROR,
    Build,
    Case,
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
SPEC_DEM = (
    # slope: the engine's own rows (SPEC_SE) hold its edge cases; this one is the docs example.
    Case("slope_example", SL, (DEM, "degrees"), "ref", "slope", ONLY),
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

# The contour lines: `vz_columns` (raster_vectorize.py) rises by one per pixel to the east, so the line of value v is
# the vertical line half way between the centres of columns v - 0.5 and v + 0.5, at x = 400000 + (v + 0.5) * 10.
CT = "gbx_rst_contour"
COLUMNS = Build("vz_columns")

SPEC_DEM += (
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
    Case("contour_null_tile", CT, (None, [1.5]), "null", None, ONLY),
    Case("contour_error_tile", CT, (ERR, [1.5]), "null", None, ONLY),
    Case("contour_corrupt", CT, (CORRUPT, [1.5]), "null", None, ONLY),
)
