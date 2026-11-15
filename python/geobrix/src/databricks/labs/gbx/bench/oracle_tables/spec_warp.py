"""Oracle table for the warp functions of the GDAL spec engine family (reproject, resample, clip): inputs ->
expected, run at every level by test_family_cross_tier.py.

Expected values come from the fixtures' definitions or an independent calculation, never from the function under
test: a reprojection is judged against rasterio.warp (oracles._reference_warp), a resample by the output size its
arguments define and, for `average`, the mean of each source block (oracle_refs.block_mean), a clip by the columns
the cutline covers. `tile_4326` is 64 x 48 pixels of 0.01 degrees from (-1, 52); `tile_3857` the same size on a
100 m grid; the cutlines are given in degrees, and their web-mercator twin by the spherical mercator formulas.
"""

import math
import os

from databricks.labs.gbx._gdal_policy import open_bytes
from databricks.labs.gbx.bench.oracle_tables._uc_tile import edges, mirror, share
from databricks.labs.gbx.bench.oracles import (
    UPSTREAM_ERROR,
    Build,
    Case,
    _build,
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


# a right triangle on whole pixel edges of the `clip_ramp_*` tiles (16 x 12 pixels of 10 m from (400000, 5760000)), whose
# envelope is the columns 2 to 13 and the rows 2 to 11
TRIANGLE = "POLYGON((400020 5759980, 400140 5759980, 400140 5759880, 400020 5759980))"


@builder("clip_ramp_u16")
def _clip_ramp_u16(tmpdir):
    """A UInt16 ramp with no NoData declared, EPSG:3857: its fallback NoData is 65535."""
    import numpy as np

    return _tile(
        _gtiff(
            np.arange(16 * 12, dtype="uint16").reshape(1, 12, 16),
            "EPSG:3857",
            nodata=None,
            dtype="uint16",
        )
    )


@builder("clip_ramp_nodata")
def _clip_ramp_nodata(tmpdir):
    """A Float32 ramp that declares NoData -9999 (on a corner pixel outside the clip), EPSG:3857."""
    import numpy as np

    z = np.arange(16 * 12, dtype="float32").reshape(1, 12, 16)
    z[0, 0, 0] = -9999.0
    return _tile(_gtiff(z, "EPSG:3857", nodata=-9999.0))


@builder("warp_ramp")
def _ramp(tmpdir):
    """An 8 x 6 ramp, 0..47, in EPSG:3857 with no NoData: every 2 x 2 block has an exact mean."""
    import numpy as np

    z = np.arange(48, dtype="float64").reshape(1, 6, 8)
    return _tile(_gtiff(z, "EPSG:3857", nodata=None))


def _ramp_tile(crs, origin, px):
    """A 20 x 15 float32 ramp, 1..300, with no NoData declared: a 0 in a result is a fill, never a source value."""
    import numpy as np

    z = np.arange(1, 301, dtype="float32").reshape(1, 15, 20)
    return _tile(_gtiff(z, crs, nodata=None, origin=origin, px=px))


@builder("warp_ramp_geo")
def _ramp_geo(tmpdir):
    """The ramp in EPSG:4326, one degree a pixel from (0, 60): a reprojection to EPSG:3035 turns it, and its corners
    fall outside the output's bounding box."""
    return _ramp_tile("EPSG:4326", (0.0, 60.0), 1.0)


@builder("warp_ramp_laea")
def _ramp_laea(tmpdir):
    """The ramp in EPSG:3035, 20 km a pixel from (2000000, 2000000): a reprojection to web mercator turns it."""
    return _ramp_tile("EPSG:3035", (2000000.0, 2000000.0), 20000.0)


@builder("tile_gcp")
def _gcp_tile(tmpdir):
    """A 16 x 12 float32 ramp georeferenced by four ground control points in EPSG:4326 (0.01 degrees a pixel from
    (-1, 52)), so its projection is the GCP projection: it has a CRS but no geotransform.
    """
    import numpy as np
    from rasterio.control import GroundControlPoint as Gcp
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.pyrx.core.compression import creation_opts
    from databricks.labs.gbx.pyrx.core.crs import resolve_crs

    z = np.arange(192, dtype="float32").reshape(1, 12, 16)
    z[0, 4:6, 4:6] = -9999.0
    corners = [(0, 0), (0, 16), (12, 0), (12, 16)]  # (row, column)
    points = [
        Gcp(row, col, -1.0 + 0.01 * col, 52.0 - 0.01 * row) for row, col in corners
    ]
    with MemoryFile() as mf:
        with mf.open(
            driver="GTiff",
            width=16,
            height=12,
            count=1,
            dtype="float32",
            nodata=-9999.0,
            **creation_opts("float32", compress="zstd"),
        ) as ds:
            ds.write(z)
            ds.gcps = (points, resolve_crs("EPSG:4326"))
        return _tile(mf.read())


@builder("dem_cog_overviews")
def _dem_cog(tmpdir):
    """The 64 x 64 DEM as a tiled GeoTIFF with one 2x overview, as a virtual tile: a read at half the size is served by
    the overview."""
    import numpy as np
    import rasterio
    from rasterio.enums import Resampling

    path = os.path.join(tmpdir, "warp_dem_overviews.tif")
    if not os.path.exists(path):
        raw = _build("dem_3857", tmpdir)["raster"]
        with open_bytes(bytes(raw)) as src:
            profile = dict(
                driver="GTiff",
                width=src.width,
                height=src.height,
                count=src.count,
                dtype=src.dtypes[0],
                crs=src.crs,
                transform=src.transform,
                nodata=src.nodata,
                tiled=True,
                blockxsize=32,
                blockysize=32,
            )
            data = np.asarray(src.read())
        # built on local disk, then copied whole: tmpdir may be a UC Volume (Serverless), whose FUSE mount takes no
        # random-access write, and a GTiff written in place and reopened r+ for its overviews is no TIFF there
        import shutil
        import tempfile

        from databricks.labs.gbx.pyrx.core.local_temp import local_temp_root

        with tempfile.TemporaryDirectory(dir=local_temp_root()) as work:
            local = os.path.join(work, "warp_dem_overviews.tif")
            with rasterio.open(local, "w", **profile) as dst:
                dst.write(data)
            with rasterio.open(local, "r+") as dst:
                dst.build_overviews([2], Resampling.average)
            shutil.copyfile(local, path)
    return _tile(None, path, None)


_session_warp_rows = (
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
    # a tile georeferenced by ground control points has a CRS, its GCP projection: it is warped, not refused
    Case(
        "transform_gcp",
        TR,
        (Build("tile_gcp"), 3857),
        "ref",
        "transform_gcp",
        ONLY,
    ),
    # a source with no NoData declared: the pixels outside the reprojected footprint are masked, not a valid 0
    Case(
        "transform_undeclared_masked",
        TR,
        (Build("warp_ramp_geo"), 3035),
        "warp_masked",
        "EPSG:3035",
        ONLY,
    ),
    # a source with a declared NoData is unchanged: the outside is the NoData value, and there is no mask band
    Case(
        "transform_declared_unchanged",
        TR,
        (T4326, 3035),
        "tile_meta",
        {"nodata": -9999.0, "count": 1, "mask_band": False},
        ONLY,
    ),
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
    Case(
        "webmercator_undeclared_masked",
        WM,
        (Build("warp_ramp_laea"), "near"),
        "warp_masked",
        "EPSG:3857",
        ONLY,
    ),
    Case("webmercator_virtual", WM, (VIRT, "near"), "tile_props", "EPSG:3857", ONLY),
    Case("webmercator_bad_word", WM, (T4326, "sinc"), "raises", "sinc", ONLY),
    # the word is checked also for a tile that needs no reprojection
    Case("webmercator_bad_word_identity", WM, (T3857, "sinc"), "raises", "sinc", ONLY),
    Case("webmercator_no_crs", WM, (Build("tile_nocrs"),), "error_fix", None, ONLY),
    Case("webmercator_null_tile", WM, (None,), "null", None, ONLY),
    # a NULL tile is NULL before the word is checked
    Case("webmercator_null_tile_bad_word", WM, (None, "sinc"), "null", None, ONLY),
    Case("webmercator_error_in", WM, (ERR,), "error_tile", UPSTREAM_ERROR, ONLY),
    # resample by a factor: the output size is the factor times the source size
    Case("resample_half", RS, (T4326, 0.5), "tile_size", (32, 24), ONLY),
    Case("resample_double", RS, (T4326, 2.0), "tile_size", (128, 96), ONLY),
    Case("resample_virtual", RS, (VIRT, 0.5), "tile_size", (32, 24), ONLY),
    Case(
        "resample_virtual_vrt", RS, (Build("dem_vrt"), 0.5), "tile_size", (32, 32), ONLY
    ),
    # a virtual tile of a file with an overview is read at the target size, so the overview serves it
    Case(
        "resample_virtual_cog",
        RS,
        (Build("dem_cog_overviews"), 0.25),
        "tile_size",
        (16, 16),
        ONLY,
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
    # NoData stays NoData: the block of the first 4 x 4 NoData pixels is still NoData after the mean of every 2 x 2 block
    Case(
        "resample_average_nodata_stays",
        RS,
        (T4326, 0.5, "average"),
        "ref",
        "resample_average_nodata",
        ONLY,
    ),
    # a resample whose pixels all land in the source has no mask band, with no NoData declared too
    Case(
        "resample_undeclared_no_mask",
        RS,
        (Build("warp_ramp"), 0.5),
        "tile_meta",
        {"nodata": None, "count": 1, "mask_band": False},
        ONLY,
    ),
    Case("resample_zero", RS, (T4326, 0.0), "raises", "factor", ONLY),
    Case("resample_negative", RS, (T4326, -2.0), "raises", "-2.0", ONLY),
    Case("resample_bad_algorithm", RS, (T4326, 0.5, "sinc"), "raises", "sinc", ONLY),
    # the words are the ones gdalwarp takes, at every level: GDAL's cubicspline is accepted, rasterio's gauss is not
    Case(
        "resample_cubicspline",
        RS,
        (T4326, 0.5, "cubicspline"),
        "tile_size",
        (32, 24),
        ONLY,
    ),
    Case("resample_gauss", RS, (T4326, 0.5, "gauss"), "raises", "gauss", ONLY),
    Case("resample_null_tile", RS, (None, 0.5), "null", None, ONLY),
    Case("resample_null_factor", RS, (T4326, None), "null", None, ONLY),
    Case("resample_error_in", RS, (ERR, 0.5), "error_tile", UPSTREAM_ERROR, ONLY),
    # an error tile passes through before its arguments are checked
    Case(
        "resample_error_tile_passes_bad_word",
        RS,
        (ERR, 0.5, "sinc"),
        "error_tile",
        UPSTREAM_ERROR,
        ONLY,
    ),
    # an error tile passes through before the word is judged, for a web mercator reprojection too (`error_tile_has`, not
    # `error_tile`: the JVM web mercator called by its private name still judges the word first)
    Case(
        "webmercator_error_tile_passes_bad_word",
        WM,
        (ERR, "sinc"),
        "error_tile_has",
        UPSTREAM_ERROR,
        ONLY,
    ),
    Case("resample_corrupt", RS, (CORRUPT, 0.5), "error_tile", None, ONLY),
    # an upsampling whose output would not fit the decoded budget is refused before it runs: the input and the output
    # (400 x the 64 x 48 tile is about 1.9 GiB decoded) are judged together
    Case("resample_upsample_over_budget", RS, (T4326, 400.0), "error_fix", None, ONLY),
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
    # a resolution that does not divide the extent gives the whole number of pixels nearest to it
    Case(
        "resample_to_res_rounds", RSR, (T4326, 0.03, 0.03), "tile_size", (21, 16), ONLY
    ),
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
    # a cutline smaller than a pixel keeps the pixel it touches (column 10, row 10: the value 68), not a NoData pixel
    Case(
        "clip_tiny_keeps_a_pixel",
        CL,
        (T4326, IN_ONE_PIXEL, False),
        "ref",
        "clip_pixel_10_10",
        ONLY,
    ),
    # the pixels a clip leaves out are NoData: the source's own, else the dtype's fallback (65535 for UInt16), never valid zeros
    Case(
        "clip_outside_is_the_fallback_nodata",
        CL,
        (Build("clip_ramp_u16"), TRIANGLE, False),
        "ref",
        "clip_triangle",
        ONLY,
    ),
    Case(
        "clip_outside_is_the_source_nodata",
        CL,
        (Build("clip_ramp_nodata"), TRIANGLE, False),
        "ref",
        "clip_triangle",
        ONLY,
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
    # a cutline that cannot be read is the tile's error tile, not a failed task
    Case(
        "clip_bad_cutline",
        CL,
        (T4326, "POLYGON((0 0, 1", False),
        "error_tile",
        None,
        ONLY,
    ),
)


def _uc_clip_row(cid, session_cid, **changes):
    """A clip row of the session as a UC row: the cutline is a GEOMETRY that carries its own SRID (`geom_srid`)."""
    from dataclasses import replace

    row = next(c for c in _session_warp_rows if c.cid == session_cid)
    return replace(row, cid=cid, surfaces=("uc",), **changes)


# Unity Catalog: the reprojections and resamples run the session's engine call on UC too (engine.uc), so each session
# row runs there with the session's value; clip (no engine row) shares its rows by name. A virtual tile is refused with a
# Fix, a corrupt one or one with no CRS to reproject from is NULL (an error tile in the session), a CRS form UC does not
# take is refused, and so is a result decoded over the UC cap, before it is made.
SPEC_WARP = mirror(
    share(
        _session_warp_rows,
        "clip_half", "clip_half_pixels", "clip_all_touched", "clip_tiny_keeps_a_pixel",
        "clip_outside_is_the_fallback_nodata", "clip_outside_is_the_source_nodata", "clip_no_overlap", "clip_null_tile",
        "clip_null_geom", "clip_null_flag", "clip_error_in",
    ),
    null=("transform_no_crs", "transform_corrupt", "webmercator_no_crs", "resample_corrupt"),
    refused=("transform_bad_srid", "resample_upsample_over_budget"),
) + edges(
    transform=(3857,), to_webmercator=(), resample=(0.5,), resample_to_size=(10, 20), resample_to_res=(0.02, 0.02),
    clip=(LEFT_HALF, False),
) + (
    # a result decoded over the cap is refused before its pixels are made: 20000 x 20000 float32 is 1.5 GiB
    Case("uc_resample_to_size_result_over_cap", RSZ, (T4326, 20000, 20000), "uc_refused", None, ("uc",)),
    Case("uc_resample_result_over_cap", RS, (T4326, 1000.0), "uc_refused", None, ("uc",)),
    Case("uc_resample_to_res_result_over_cap", RSR, (T4326, 0.00001, 0.00001), "uc_refused", None, ("uc",)),
    # the cutline is a GEOMETRY: a BINARY is not one (the warehouse's own cast error, which has no Fix: line, or the
    # handler's text); a plain one takes `clip_crs`, an EWKT one carries its SRID
    Case(
        "uc_clip_wkb_into_geometry_refused", CL, (T4326, Build("clip_left_half_wkb"), False), "raises",
        ("DATATYPE_MISMATCH", "a BINARY or STRING is not a GEOMETRY"), ("uc",),
    ),
    _uc_clip_row("uc_clip_crs_declared", "clip_crs_declared", geom_srid=0),
    _uc_clip_row("uc_clip_ewkt_srid_wins", "clip_ewkt_srid_wins", geom_srid=3857),
    Case(
        "uc_clip_wkt_crs_refused", CL, (T4326, LEFT_HALF, False, "+proj=longlat +datum=WGS84"), "uc_refused", None,
        ("uc",), geom_srid=0,
    ),
)  # fmt: skip
