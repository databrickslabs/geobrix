"""Oracle table for the warp functions of the GDAL spec engine family (reproject, resample, clip): inputs ->
expected, run at every level by test_family_cross_tier.py.

Expected values come from the fixtures' definitions or an independent calculation, never from the function under
test: a reprojection is judged against rasterio.warp (oracles._reference_warp), a resample by the output size its
arguments define and, for `average`, the mean of each source block (oracle_refs.block_mean), a clip by the columns
the cutline covers. `tile_4326` is 64 x 48 pixels of 0.01 degrees from (-1, 52); `tile_3857` the same size on a
100 m grid; the cutlines are given in degrees, and their web-mercator twin by the spherical mercator formulas.
"""

import math

from databricks.labs.gbx.bench.oracles import (
    UPSTREAM_ERROR,
    Build,
    Case,
    _gtiff,
    _tile,
    builder,
)

TR, WM, RS = "gbx_rst_transform", "gbx_rst_to_webmercator", "gbx_rst_resample"
RSZ, RSR, CL = "gbx_rst_resample_to_size", "gbx_rst_resample_to_res", "gbx_rst_clip"
T4326, T3857 = Build("tile_4326"), Build("tile_3857")
ERR, CORRUPT, VIRT = Build("tile_error"), Build("tile_corrupt"), Build("tile_virtual")
ONLY = ("session",)


def _mercator(lon, lat):
    """Spherical web mercator of a longitude/latitude in degrees (EPSG:3857)."""
    r = 6378137.0
    return (
        r * math.radians(lon),
        r * math.log(math.tan(math.pi / 4.0 + math.radians(lat) / 2.0)),
    )


def _box(x0, y0, x1, y1):
    return f"POLYGON(({x0} {y0}, {x1} {y0}, {x1} {y1}, {x0} {y1}, {x0} {y0}))"


# the left 32 columns of tile_4326 and every row; the cutline sticks out of the tile on three sides
LEFT_HALF = _box(-1.05, 51.4, -0.68, 52.1)
LEFT_HALF_3857 = _box(
    *_mercator(-1.05, 51.4), *_mercator(-0.68, 52.1)
)  # the same cutline in EPSG:3857
# a box inside the pixel at column 10, row 10 (x -0.9..-0.89, y 51.9..51.89) that does not hold the pixel's centre
IN_ONE_PIXEL = _box(-0.899, 51.898, -0.897, 51.896)
FAR_AWAY = _box(10.0, 10.0, 11.0, 11.0)


@builder("clip_left_half_wkb")
def _left_half_wkb(tmpdir):
    import shapely

    return shapely.to_wkb(shapely.from_wkt(LEFT_HALF))


@builder("clip_left_half_ewkb")
def _left_half_ewkb(tmpdir):
    import shapely

    return shapely.to_wkb(
        shapely.set_srid(shapely.from_wkt(LEFT_HALF), 4326), include_srid=True
    )


@builder("warp_ramp")
def _ramp(tmpdir):
    """An 8 x 6 ramp, 0..47, in EPSG:3857 with no NoData: every 2 x 2 block has an exact mean."""
    import numpy as np

    z = np.arange(48, dtype="float64").reshape(1, 6, 8)
    return _tile(_gtiff(z, "EPSG:3857", nodata=None))


SPEC_WARP = (
    # transform: a reprojection judged against rasterio.warp on the same grid (nearest neighbour)
    Case("transform_3857", TR, (T4326, 3857), "tile_props", "EPSG:3857", ONLY),
    Case("transform_4326", TR, (T3857, 4326), "tile_props", ("EPSG:4326", 4326), ONLY),
    Case("transform_virtual", TR, (VIRT, 3857), "tile_props", "EPSG:3857", ONLY),
    # a tile already in the target CRS comes out as it is
    Case("transform_identity", TR, (T3857, 3857), "tile_props", "EPSG:3857", ONLY),
    # a tile with no CRS cannot be reprojected: an error tile that says how to set one
    Case("transform_no_crs", TR, (Build("tile_nocrs"), 3857), "error_fix", None, ONLY),
    # an SRID that names no CRS is data: an error tile
    Case("transform_bad_srid", TR, (T4326, 999999), "error_tile", None, ONLY),
    Case("transform_null_tile", TR, (None, 3857), "null", None, ONLY),
    Case("transform_null_srid", TR, (T4326, None), "null", None, ONLY),
    Case("transform_error_in", TR, (ERR, 3857), "error_tile", UPSTREAM_ERROR, ONLY),
    Case("transform_corrupt", TR, (CORRUPT, 3857), "error_tile", None, ONLY),
    # to_webmercator: the same reprojection to EPSG:3857, with a resampling word
    Case(
        "webmercator_nearest", WM, (T4326, "nearest"), "tile_props", "EPSG:3857", ONLY
    ),
    # the default resampling is bilinear; the grid is the one rasterio's calculate_default_transform picks (51 x 62)
    Case("webmercator_default", WM, (T4326,), "tile_size", (51, 62), ONLY),
    Case("webmercator_virtual", WM, (VIRT, "near"), "tile_props", "EPSG:3857", ONLY),
    Case("webmercator_bad_word", WM, (T4326, "sinc"), "raises", "sinc", ONLY),
    # the word is checked also for a tile that needs no reprojection
    Case("webmercator_bad_word_identity", WM, (T3857, "sinc"), "raises", "sinc", ONLY),
    Case("webmercator_no_crs", WM, (Build("tile_nocrs"),), "error_fix", None, ONLY),
    Case("webmercator_null_tile", WM, (None,), "null", None, ONLY),
    Case("webmercator_error_in", WM, (ERR,), "error_tile", UPSTREAM_ERROR, ONLY),
    # resample by a factor: the output size is the factor times the source size
    Case("resample_half", RS, (T4326, 0.5), "tile_size", (32, 24), ONLY),
    Case("resample_double", RS, (T4326, 2.0), "tile_size", (128, 96), ONLY),
    Case("resample_virtual", RS, (VIRT, 0.5), "tile_size", (32, 24), ONLY),
    Case(
        "resample_virtual_vrt", RS, (Build("dem_vrt"), 0.5), "tile_size", (32, 32), ONLY
    ),
    # `average` by one half is the mean of every 2 x 2 block (GDAL's word `near` and a capital are accepted)
    Case(
        "resample_average_half",
        RS,
        (Build("warp_ramp"), 0.5, "average"),
        "ref",
        "resample_average_half",
        ONLY,
    ),
    Case(
        "resample_word_case",
        RS,
        (Build("warp_ramp"), 0.5, "Average"),
        "ref",
        "resample_average_half",
        ONLY,
    ),
    Case("resample_zero", RS, (T4326, 0.0), "raises", "factor", ONLY),
    Case("resample_negative", RS, (T4326, -2.0), "raises", "-2.0", ONLY),
    Case("resample_bad_algorithm", RS, (T4326, 0.5, "sinc"), "raises", "sinc", ONLY),
    Case("resample_null_tile", RS, (None, 0.5), "null", None, ONLY),
    Case("resample_null_factor", RS, (T4326, None), "null", None, ONLY),
    Case("resample_error_in", RS, (ERR, 0.5), "error_tile", UPSTREAM_ERROR, ONLY),
    Case("resample_corrupt", RS, (CORRUPT, 0.5), "error_tile", None, ONLY),
    # a tile decoded over the read cap is an error tile with a Fix, never a failed task (READ_CAP_ROWS lowers the cap)
    Case(
        "resample_decoded_over_cap",
        RS,
        (T4326, 0.5),
        "error_tile_has",
        "the tile is 12 KiB decoded, over this compute's read cap (4 KiB). Fix: retile it first (gbx_rst_retile)",
        ONLY,
    ),
    # resample to a size and to a ground resolution
    Case("resample_to_size", RSZ, (T4326, 10, 20), "tile_size", (10, 20), ONLY),
    Case("resample_to_size_zero", RSZ, (T4326, 0, 20), "raises", "width_px", ONLY),
    Case("resample_to_size_null", RSZ, (T4326, 10, None), "null", None, ONLY),
    Case("resample_to_res", RSR, (T4326, 0.02, 0.02), "tile_size", (32, 24), ONLY),
    Case("resample_to_res_zero", RSR, (T4326, 0.0, 0.02), "raises", "x_res", ONLY),
    Case("resample_to_res_null", RSR, (T4326, 0.02, None), "null", None, ONLY),
    # clip: the output is the cutline's envelope on the tile's grid, outside the cutline NoData
    Case("clip_half", CL, (T4326, LEFT_HALF, False), "tile_size", (32, 48), ONLY),
    Case(
        "clip_half_pixels", CL, (T4326, LEFT_HALF, False), "ref", "clip_left_half", ONLY
    ),
    Case(
        "clip_half_wkb",
        CL,
        (T4326, Build("clip_left_half_wkb"), False),
        "tile_size",
        (32, 48),
        ONLY,
    ),
    # the cutline as EWKB (carries SRID 4326) and as EWKT give the same result as WKB and WKT
    Case(
        "clip_half_ewkb",
        CL,
        (T4326, Build("clip_left_half_ewkb"), False),
        "tile_size",
        (32, 48),
        ONLY,
    ),
    Case("clip_virtual", CL, (VIRT, LEFT_HALF, False), "tile_size", (32, 48), ONLY),
    # the cutline's CRS: declared by clip_crs, or carried by an EWKT SRID, which wins
    Case(
        "clip_crs_declared",
        CL,
        (T4326, LEFT_HALF_3857, False, "EPSG:3857"),
        "tile_size",
        (32, 48),
        ONLY,
    ),
    Case(
        "clip_ewkt_srid_wins",
        CL,
        (T4326, f"SRID=3857;{LEFT_HALF_3857}", False, "EPSG:4326"),
        "tile_size",
        (32, 48),
        ONLY,
    ),
    # all_touched keeps the pixels the boundary touches: a box inside one pixel, not holding its centre
    Case(
        "clip_all_touched", CL, (T4326, IN_ONE_PIXEL, True), "tile_size", (1, 1), ONLY
    ),
    # a cutline that misses the tile entirely gives NULL
    Case("clip_no_overlap", CL, (T4326, FAR_AWAY, False), "null", None, ONLY),
    Case("clip_null_tile", CL, (None, LEFT_HALF, False), "null", None, ONLY),
    Case("clip_null_geom", CL, (T4326, None, False), "null", None, ONLY),
    Case("clip_null_flag", CL, (T4326, LEFT_HALF, None), "null", None, ONLY),
    Case(
        "clip_error_in", CL, (ERR, LEFT_HALF, False), "error_tile", UPSTREAM_ERROR, ONLY
    ),
    Case("clip_corrupt", CL, (CORRUPT, LEFT_HALF, False), "error_tile", None, ONLY),
)
