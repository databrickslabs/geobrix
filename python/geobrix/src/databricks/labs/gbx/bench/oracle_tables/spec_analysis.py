"""Oracle table for the analysis functions of the GDAL spec engine family that read a tile (today: the summary): inputs ->
expected, run at every level by test_family_cross_tier.py.

The expected values come from the fixtures' definitions (see raster_accessor.py): the 4 x 3 grid of 1..12 with its
first pixel NoData has the valid values 2..12, so its minimum is 2, its maximum 12 and its mean 7.
"""

from databricks.labs.gbx.bench.oracle_tables._uc_tile import edges, share
from databricks.labs.gbx.bench.oracles import (
    UPSTREAM_ERROR,
    Build,
    Case,
    _tile,
    builder,
)

S, PRX, FND = "gbx_rst_summary", "gbx_rst_proximity", "gbx_rst_fillnodata"
VS = "gbx_rst_viewshed"
# The observer of the viewshed rows: pixel (6, 10) of the 21 x 21 `vs_wall` DEM (10 m pixels from (400000, 5760000)).
OBSERVER = "POINT(400065 5759895)"
OBSERVER_WKB = bytes.fromhex("010100000000000000046b1841000000c0e5f85541")
STATS, VIRT, ERR = Build("acc_stats"), Build("tile_virtual"), Build("tile_error")
CLASSES, DEM, CORRUPT = (
    Build("analysis_classes"),
    Build("dem_3857"),
    Build("tile_corrupt"),
)
ONLY = ("session",)


@builder("analysis_classes")
def _classes(tmpdir):
    """A 64 x 48 Byte raster of three classes in blocks (1, 2, 3; 0 elsewhere), NoData 0, EPSG:3857 on a 10 m grid."""
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    z = np.zeros((1, 48, 64), dtype="uint8")
    z[0, 5:16, 5:16] = 1
    z[0, 30:41, 40:56] = 2
    z[0, 20:26, 25:31] = 3
    profile = dict(
        driver="GTiff",
        width=64,
        height=48,
        count=1,
        dtype="uint8",
        nodata=0,
        crs="EPSG:3857",
        transform=from_origin(400000.0, 5760000.0, 10.0, 10.0),
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(z)
        return _tile(mf.read())


@builder("vs_wall")
def _vs_wall(tmpdir):
    """A flat 21 x 21 DEM (100 m, EPSG:3857, 10 m pixels) with a 1000 m wall in column 14: from the observer in
    column 6 every cell east of the wall is hidden and every other cell is visible."""
    import numpy as np

    from databricks.labs.gbx.bench.oracles import _gtiff

    z = np.full((1, 21, 21), 100.0)
    z[0, :, 14] = 1000.0
    return _tile(_gtiff(z, "EPSG:3857"))


@builder("vs_wall_file")
def _vs_wall_file(tmpdir):
    import os

    from databricks.labs.gbx.bench.oracles import _build

    path = os.path.join(tmpdir, "oracle_wall.tif")
    with open(path, "wb") as fh:
        fh.write(bytes(_build("vs_wall", tmpdir)["raster"]))
    return _tile(None, path, {"col_off": 0, "row_off": 0, "width": 21, "height": 21})


@builder("fill_two_bands")
def _fill_two_bands(tmpdir):
    """Two 9 x 9 Float32 bands of a gradient, NoData -9999, with a hole in the middle of each (band 2's is wider)."""
    import numpy as np

    from databricks.labs.gbx.bench.oracles import _gtiff

    yy, xx = np.mgrid[0:9, 0:9].astype("float64")
    z = np.stack([10.0 + xx + yy, 100.0 + 2.0 * xx - yy])
    z[0, 4, 4] = -9999.0
    z[1, 3:6, 3:6] = -9999.0
    return _tile(_gtiff(z, "EPSG:3857"))


@builder("fill_undeclared")
def _fill_undeclared(tmpdir):
    """A 9 x 9 Float32 gradient with a -9999 pixel and no NoData declared."""
    import numpy as np

    from databricks.labs.gbx.bench.oracles import _gtiff

    yy, xx = np.mgrid[0:9, 0:9].astype("float64")
    z = (10.0 + xx + yy)[np.newaxis]
    z[0, 4, 4] = -9999.0
    return _tile(_gtiff(z, "EPSG:3857", nodata=None))


@builder("fill_masked")
def _fill_masked(tmpdir):
    """A Float32 tile whose validity is its own mask band and which declares no NoData: a 64 x 64 gradient with no NoData,
    reprojected by the engine's warp from EPSG:3857 to UTM 32N (the grid turns, and the corners outside the source's footprint
    are masked)."""
    import numpy as np

    from databricks.labs.gbx import manifest
    from databricks.labs.gbx.bench.oracles import _gtiff
    from databricks.labs.gbx.engine import run

    yy, xx = np.mgrid[0:64, 0:64].astype("float64")
    src = _gtiff((10.0 + xx + yy)[np.newaxis], "EPSG:3857", nodata=None)
    res = run.run(
        manifest.load().functions["gbx_rst_transform"], [src], {"srid": 32632}
    )
    return _tile(res.raster)


@builder("fill_all_nodata")
def _fill_all_nodata(tmpdir):
    import numpy as np

    from databricks.labs.gbx.bench.oracles import _gtiff

    return _tile(_gtiff(np.full((1, 4, 4), -9999.0), "EPSG:3857"))


@builder("fill_no_crs")
def _fill_no_crs(tmpdir):
    import numpy as np

    from databricks.labs.gbx.bench.oracles import _gtiff

    return _tile(_gtiff(np.arange(16.0).reshape(1, 4, 4), None, nodata=None))


WALL = Build("vs_wall")
SPEC_ANALYSIS = (
    Case(
        "summary",
        S,
        (STATS,),
        "json_subset",
        {
            "driverShortName": "GTiff",
            "size": [4, 3],
            "bands": [
                {
                    "band": 1,
                    "type": "Float32",
                    "noDataValue": -9999.0,
                    "minimum": 2.0,
                    "maximum": 12.0,
                    "mean": 7.0,
                }
            ],
        },
        ("session",),
    ),
    Case(
        "summary_virtual", S, (VIRT,), "json_subset", {"size": [64, 48]}, ("session",)
    ),
    # GDAL gives the mean of single-precision pixels accumulated in double, and rounds it to three decimals
    Case(
        "summary_float32_precision",
        S,
        (Build("tile_4326"),),
        "json_subset",
        {"bands": [{"mean": 47.718}]},
        ("session",),
    ),
    # the whole JSON against GDAL's own `gdal.Info(format="json", stats=True)`, through osgeo
    Case("summary_json", S, (Build("tile_4326"),), "json_ref", None, ("session",)),
    Case(
        "summary_two_bands",
        S,
        (Build("fill_two_bands"),),
        "json_ref",
        None,
        ("session",),
    ),
    # a band with no valid pixel has no statistics; the JSON is still GDAL's
    Case(
        "summary_all_nodata",
        S,
        (Build("fill_all_nodata"),),
        "json_ref",
        None,
        ("session",),
    ),
    Case("summary_no_crs", S, (Build("fill_no_crs"),), "json_ref", None, ("session",)),
    Case("summary_null_tile", S, (None,), "null", None, ("session",)),
    Case("summary_error_tile", S, (ERR,), "null", None, ("session",)),
    # proximity: the distance to the nearest source pixel, judged against scipy's exact Euclidean distance
    Case("proximity_targets", PRX, (CLASSES, "1,2"), "ref", "proximity_geo", ONLY),
    # no target values: every pixel that is not 0 is a source
    Case("proximity_default_sources", PRX, (CLASSES,), "ref", "proximity_geo", ONLY),
    Case(
        "proximity_pixel_capped",
        PRX,
        (CLASSES, "1", "PIXEL", 5.0),
        "ref",
        "proximity_pixel",
        ONLY,
    ),
    Case("proximity_bad_units", PRX, (CLASSES, "1", "feet"), "raises", "feet", ONLY),
    Case("proximity_bad_cap", PRX, (CLASSES, "1", "GEO", -1.0), "raises", "-1.0", ONLY),
    Case("proximity_null_tile", PRX, (None, "1"), "null", None, ONLY),
    # a NULL unit is the default, GEO, as a NULL list of target values and a NULL cap mean none
    Case(
        "proximity_null_units",
        PRX,
        (CLASSES, "1", None),
        "ref",
        "proximity_geo",
        ONLY,
    ),
    # an explicit NULL cap is no cap: the SQL call sends all four arguments to every level
    Case(
        "proximity_null_cap",
        PRX,
        (CLASSES, "1", "GEO", None),
        "ref",
        "proximity_geo",
        ONLY,
    ),
    # NaN is the heavy tier's absent sentinel: no cap, at every level
    Case(
        "proximity_nan_cap",
        PRX,
        (CLASSES, "1", "GEO", float("nan")),
        "ref",
        "proximity_geo",
        ONLY,
    ),
    Case("proximity_error_in", PRX, (ERR, "1"), "error_tile", UPSTREAM_ERROR, ONLY),
    Case("proximity_corrupt", PRX, (CORRUPT, "1"), "error_tile", None, ONLY),
    # fillnodata: rasterio's own GDAL FillNodata call is the reference (the 3 x 3 hole of dem_3857)
    Case("fillnodata_default", FND, (DEM,), "ref", "fillnodata", ONLY),
    # a search distance of one pixel cannot reach the centre of the hole: it stays NoData
    Case("fillnodata_virtual", FND, (Build("dem_file"),), "ref", "fillnodata", ONLY),
    Case("fillnodata_short_search", FND, (DEM, 1.0, 0), "ref", "fillnodata", ONLY),
    Case("fillnodata_smoothed", FND, (DEM, 100.0, 2), "ref", "fillnodata", ONLY),
    Case("fillnodata_null_tile", FND, (None,), "null", None, ONLY),
    Case("fillnodata_null_distance", FND, (DEM, None, 0), "null", None, ONLY),
    # a tile whose validity is its own mask band: a pixel GDAL fills becomes valid, one the search does not reach stays masked
    Case(
        "fillnodata_masked_filled_valid",
        FND,
        (Build("fill_masked"), 100.0, 0),
        "fill_mask",
        None,
        ONLY,
    ),
    Case(
        "fillnodata_masked_short_search",
        FND,
        (Build("fill_masked"), 1.0, 0),
        "fill_mask",
        "partial",
        ONLY,
    ),
    Case("fillnodata_null_smoothing", FND, (DEM, 100.0, None), "null", None, ONLY),
    # every band is filled, each by its own mask
    Case(
        "fillnodata_two_bands",
        FND,
        (Build("fill_two_bands"),),
        "ref",
        "fillnodata",
        ONLY,
    ),
    # a tile that declares no NoData has no pixel to fill: it comes back as it is
    Case(
        "fillnodata_undeclared",
        FND,
        (Build("fill_undeclared"),),
        "ref",
        "fillnodata",
        ONLY,
    ),
    # the search distance and the passes are whole numbers of pixels and passes
    Case("fillnodata_fractional_distance", FND, (DEM, 2.5, 0), "raises", "2.5", ONLY),
    Case("fillnodata_negative_distance", FND, (DEM, -1.0, 0), "raises", "-1", ONLY),
    Case("fillnodata_negative_smoothing", FND, (DEM, 100.0, -1), "raises", "-1", ONLY),
    Case("fillnodata_error_in", FND, (ERR,), "error_tile", UPSTREAM_ERROR, ONLY),
    Case("fillnodata_corrupt", FND, (CORRUPT,), "error_tile", None, ONLY),
    # viewshed: 255 up to and including the wall (column 14), 0 behind it
    Case("viewshed_wall", VS, (WALL, OBSERVER, 2.0), "ref", "viewshed_wall", ONLY),
    Case("viewshed_wkb", VS, (WALL, OBSERVER_WKB, 2.0), "ref", "viewshed_wall", ONLY),
    Case(
        "viewshed_virtual",
        VS,
        (Build("vs_wall_file"), OBSERVER, 2.0),
        "ref",
        "viewshed_wall",
        ONLY,
    ),
    # a distance limit gives the window around the observer: 3 pixels either side of pixel (6, 10)
    Case(
        "viewshed_max_distance",
        VS,
        (WALL, OBSERVER, 2.0, 1.6, 30.0),
        "tile_meta",
        {"width": 7, "height": 7, "dtype": "uint8"},
        ONLY,
    ),
    Case(
        "viewshed_not_a_point",
        VS,
        (WALL, "LINESTRING(400065 5759895, 400075 5759895)", 2.0),
        "raises",
        "POINT",
        ONLY,
    ),
    Case("viewshed_negative_height", VS, (WALL, OBSERVER, -1.0), "raises", "-1", ONLY),
    Case("viewshed_null_tile", VS, (None, OBSERVER, 2.0), "null", None, ONLY),
    Case("viewshed_null_observer", VS, (WALL, None, 2.0), "null", None, ONLY),
    Case(
        "viewshed_error_tile",
        VS,
        (ERR, OBSERVER, 2.0),
        "error_tile",
        UPSTREAM_ERROR,
        ONLY,
    ),
    Case("viewshed_corrupt", VS, (CORRUPT, OBSERVER, 2.0), "error_tile", None, ONLY),
)

# polygonize: `pg_halves` is a 6 x 6 raster of 10 m pixels, 1 in the left three columns and 2 in the right three;
# `pg_checker` a 2 x 2 raster [[1, 2], [2, 1]]; `pg_hole` a 3 x 3 raster of 1 with a NoData pixel in the middle.
PG = "gbx_rst_polygonize"


def _pg_tile(z, nodata=None, dtype="float32"):
    """A tile of `z` (a 2-D list, or a list of them for a raster of several bands) as `dtype`, 10 m pixels, EPSG:3857."""
    import numpy as np

    from databricks.labs.gbx.bench.oracles import _gtiff

    a = np.array(z, dtype="float64")
    return _tile(
        _gtiff(
            a if a.ndim == 3 else a[np.newaxis],
            "EPSG:3857",
            nodata=nodata,
            dtype=dtype,
        )
    )


@builder("pg_halves")
def _pg_halves(tmpdir):
    return _pg_tile([[1, 1, 1, 2, 2, 2]] * 6)


@builder("pg_checker")
def _pg_checker(tmpdir):
    return _pg_tile([[1, 2], [2, 1]])


@builder("pg_hole")
def _pg_hole(tmpdir):
    return _pg_tile([[1, 1, 1], [1, -9999, 1], [1, 1, 1]], nodata=-9999.0)


@builder("pg_float")
def _pg_float(tmpdir):
    """A 2 x 2 Float64 raster [[1.2, 1.4], [1.4, 1.2]]: four regions, because a float is not rounded to a whole number first."""
    return _pg_tile([[1.2, 1.4], [1.4, 1.2]], dtype="float64")


@builder("pg_int16")
def _pg_int16(tmpdir):
    return _pg_tile([[1, 2], [2, 1]], dtype="int16")


@builder("pg_bands")
def _pg_bands(tmpdir):
    """Two bands: band 1 is 7 everywhere, band 2 is 1 in the left three columns and 2 in the right three."""
    return _pg_tile([[[7] * 6] * 6, [[1, 1, 1, 2, 2, 2]] * 6])


@builder("pg_nan")
def _pg_nan(tmpdir):
    """A Float32 raster of 1 whose middle pixel is NaN, declared as the NoData value."""
    return _pg_tile([[1, 1, 1], [1, float("nan"), 1], [1, 1, 1]], nodata=float("nan"))


@builder("pg_noise")
def _pg_noise(tmpdir):
    """40 x 40 pixels of five classes, in clumps, so regions touch at corners and have holes: Int16."""
    import numpy as np

    rng = np.random.RandomState(7)
    z = rng.randint(0, 5, size=(10, 10)).repeat(4, axis=0).repeat(4, axis=1)
    z = np.where(rng.rand(40, 40) < 0.15, rng.randint(0, 5, size=(40, 40)), z)
    return _pg_tile(z, dtype="int16")


@builder("pg_noise_float")
def _pg_noise_float(tmpdir):
    """The same clumps as Float64 with halves added, and a NoData value on some pixels."""
    import numpy as np

    rng = np.random.RandomState(11)
    z = rng.randint(0, 4, size=(10, 10)).repeat(4, axis=0).repeat(4, axis=1) + 0.5
    z = np.where(rng.rand(40, 40) < 0.1, -9999.0, z)
    return _pg_tile(z, nodata=-9999.0, dtype="float64")


def _pg_layout(count, alpha_last=True, mask=False):
    """A `count`-band Byte raster, 6 x 6: every band 5 over rows 0-1 and 4-5 and 99 over rows 2-3. With an alpha band (the
    last, labelled so) it is 255 over the opaque rows and 0 over rows 2-3; with `mask`, rows 2-3 are masked instead.
    """
    import numpy as np
    from rasterio.enums import ColorInterp as CI
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    data = np.full((count, 6, 6), 5, dtype="uint8")
    data[:, 2:4, :] = 99
    if alpha_last:
        data[-1] = 255
        data[-1, 2:4, :] = 0
    interp = [CI.gray] + [CI.undefined] * (count - 1)
    if count == 4:
        interp = [CI.red, CI.green, CI.blue, CI.undefined]
    if alpha_last:
        interp[-1] = CI.alpha
    profile = dict(
        driver="GTiff",
        width=6,
        height=6,
        count=count,
        dtype="uint8",
        crs="EPSG:3857",
        transform=from_origin(400000.0, 5760000.0, 10.0, 10.0),
        photometric="RGB" if count == 4 else "MINISBLACK",
    )
    import rasterio

    with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True), MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.colorinterp = (
                interp  # before the write: GDAL ignores a label set after the pixels
            )
            dst.write(data)
            if mask:
                m = np.full((6, 6), 255, dtype="uint8")
                m[2:4, :] = 0
                dst.write_mask(m)
        return _tile(mf.read())


@builder("pg_gray_alpha")
def _pg_gray_alpha(tmpdir):
    """Gray and alpha: GDAL itself reads the alpha band as the mask. Rows 2-3 are transparent."""
    return _pg_layout(2)


@builder("pg_gray_x_alpha")
def _pg_gray_x_alpha(tmpdir):
    """Three bands, the last an alpha band: GDAL derives no mask from it, so the engine makes the transparent rows
    (2-3) the tile's mask."""
    return _pg_layout(3)


@builder("pg_rgba")
def _pg_rgba(tmpdir):
    return _pg_layout(4)


@builder("pg_own_mask")
def _pg_own_mask(tmpdir):
    """Three bands with no alpha band and an own mask band that hides rows 2-3."""
    return _pg_layout(3, alpha_last=False, mask=True)


HALVES, CHECKER, HOLE = Build("pg_halves"), Build("pg_checker"), Build("pg_hole")

SPEC_ANALYSIS += (
    # 18 pixels of 100 m2 each side.
    Case(
        "polygonize_two_values",
        PG,
        (HALVES,),
        "polygons",
        [(1.0, 1800.0), (2.0, 1800.0)],
        ONLY,
    ),
    Case(
        "polygonize_band_arg",
        PG,
        (HALVES, 1, 4),
        "polygons",
        [(1.0, 1800.0), (2.0, 1800.0)],
        ONLY,
    ),
    # Pixels that touch at a corner are one region with 8-connectedness and four regions with 4.
    Case(
        "polygonize_checker_4",
        PG,
        (CHECKER, 1, 4),
        "polygons",
        [(1.0, 100.0), (1.0, 100.0), (2.0, 100.0), (2.0, 100.0)],
        ONLY,
    ),
    Case(
        "polygonize_checker_8",
        PG,
        (CHECKER, 1, 8),
        "polygons",
        [(1.0, 200.0), (2.0, 200.0)],
        ONLY,
    ),
    # The NoData pixel is no polygon and no part of the ring around it: 8 pixels.
    Case("polygonize_nodata_hole", PG, (HOLE,), "polygons", [(1.0, 800.0)], ONLY),
    Case("polygonize_bad_connectedness", PG, (HALVES, 1, 5), "raises", "5", ONLY),
    Case("polygonize_bad_band", PG, (HALVES, 2), "raises", "2", ONLY),
    Case("polygonize_null", PG, (None,), "no_rows", None, ONLY),
    Case("polygonize_error_tile", PG, (ERR,), "no_rows", None, ONLY),
    Case("polygonize_corrupt", PG, (CORRUPT,), "no_rows", None, ONLY),
    # a NULL optional argument is the default (band 1, 4-connectedness)
    Case(
        "polygonize_null_optionals",
        PG,
        (HALVES, None, None),
        "polygons",
        [(1.0, 1800.0), (2.0, 1800.0)],
        ONLY,
    ),
    Case("polygonize_band_zero", PG, (HALVES, 0), "raises", "0", ONLY),
    # a float is grouped as it is, not rounded to a whole number first (which would join all four pixels into one region
    # of value 1), and a region's value is the pixel value
    Case(
        "polygonize_float_exact",
        PG,
        (Build("pg_float"),),
        "polygons",
        [(1.2, 100.0), (1.2, 100.0), (1.4, 100.0), (1.4, 100.0)],
        ONLY,
    ),
    Case(
        "polygonize_int16",
        PG,
        (Build("pg_int16"), 1, 8),
        "polygons",
        [(1.0, 200.0), (2.0, 200.0)],
        ONLY,
    ),
    Case(
        "polygonize_second_band",
        PG,
        (Build("pg_bands"), 2),
        "polygons",
        [(1.0, 1800.0), (2.0, 1800.0)],
        ONLY,
    ),
    Case(
        "polygonize_first_band_of_two",
        PG,
        (Build("pg_bands"),),
        "polygons",
        [(7.0, 3600.0)],
        ONLY,
    ),
    # a NaN NoData pixel is no polygon, as any NoData pixel
    Case(
        "polygonize_nan_nodata",
        PG,
        (Build("pg_nan"),),
        "polygons",
        [(1.0, 800.0)],
        ONLY,
    ),
    # transparent pixels and pixels a tile's own mask hides are no data: 24 of 36 pixels, 100 m2 each, in two regions
    *(
        Case(
            f"polygonize_{name}",
            PG,
            (Build(f"pg_{name}"),),
            "polygons",
            [(5.0, 1200.0), (5.0, 1200.0)],
            ONLY,
        )
        for name in ("gray_alpha", "gray_x_alpha", "rgba", "own_mask")
    ),
    # the polygons against GDAL's older Polygonize / FPolygonize: clumps that touch at corners and have holes, whole
    # numbers and floats with NoData, 4- and 8-connected
    Case("polygonize_ref_4", PG, (Build("pg_noise"), 1, 4), "polygons_ref", None, ONLY),
    Case("polygonize_ref_8", PG, (Build("pg_noise"), 1, 8), "polygons_ref", None, ONLY),
    Case(
        "polygonize_ref_float_4",
        PG,
        (Build("pg_noise_float"), 1, 4),
        "polygons_ref",
        None,
        ONLY,
    ),
    Case(
        "polygonize_ref_float_8",
        PG,
        (Build("pg_noise_float"), 1, 8),
        "polygons_ref",
        None,
        ONLY,
    ),
    # a virtual tile is read from its file; a file that is not there is retried, then raises naming the path
    Case(
        "polygonize_virtual",
        PG,
        (Build("vz_columns_virtual"),),
        "polygons",
        [(float(v), 600.0) for v in range(6)],
        ONLY,
    ),
    Case(
        "polygonize_missing_path",
        PG,
        (Build("vz_missing"),),
        "raises",
        "gbx_missing_tile",
        ONLY,
    ),
)

# Unity Catalog: the fill and the proximity of a materialized tile are the session's; the summary's NULL rows are too. The
# summary's JSON stays the earlier numpy shape on Unity Catalog until its handler runs the engine, so its JSON rows are the
# session's alone
SPEC_ANALYSIS = (
    share(
        SPEC_ANALYSIS, "summary_null_tile", "summary_error_tile",
        "fillnodata_default", "fillnodata_short_search", "fillnodata_smoothed", "fillnodata_null_tile",
        "fillnodata_null_distance", "fillnodata_error_in",
        "proximity_targets", "proximity_default_sources", "proximity_pixel_capped", "proximity_bad_units",
        "proximity_bad_cap", "proximity_null_tile", "proximity_null_units", "proximity_null_cap", "proximity_nan_cap",
        "proximity_error_in",
    )
    + edges(summary=(), fillnodata=(), proximity=("1",))
)  # fmt: skip
