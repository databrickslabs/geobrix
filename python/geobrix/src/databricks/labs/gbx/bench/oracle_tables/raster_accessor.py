"""Oracle tables for the raster accessors (header, geotransform, coordinates) and the pixel statistics: inputs ->
expected, run at every level by test_family_cross_tier.py.

Expected values come from the fixtures' definitions or an independent library, never from the function under
test. The fixtures are small and exact: `acc_stats` is a 4 x 3 grid of 1..12 with its first pixel NoData (so the
valid values are 2..12: count 11, minimum 2, maximum 12, mean and median 7), on a 10 m grid whose upper-left
corner is (400000, 5760000); `tile_4326` is the 64 x 48 oracle tile (0.01 degree pixels from (-1, 52)).
"""

import math

from databricks.labs.gbx.bench.oracles import Build, Case, _gtiff, _tile, builder

NODATA = -9999.0
GRID = [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]]
BOX_4326 = "POLYGON((-1 51.52, -0.36 51.52, -0.36 52, -1 52, -1 51.52))"
# the rotated fixture's geotransform (gt0..gt5): upper-left (-1, 52), scale 0.01 / -0.01, skew 0.002 / 0.003
ROT = dict(c=-1.0, a=0.01, b=0.002, f=52.0, d=0.003, e=-0.01)


def _grid_bands(nodata_first=True, bands=1):
    import numpy as np

    grid = np.array([GRID], dtype="float32")
    stack = np.concatenate([grid * (i + 1) for i in range(bands)], axis=0)
    if nodata_first:
        stack[:, 0, 0] = NODATA
    return stack


def _raster(
    bands,
    crs,
    transform,
    dtype="float32",
    nodata=None,
    tags=None,
    band_tags=None,
    driver="GTiff",
):
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.pyrx.core.crs import resolve_crs

    count, h, w = bands.shape
    prof = dict(
        driver=driver, width=w, height=h, count=count, dtype=dtype, transform=transform
    )
    if crs:
        prof["crs"] = resolve_crs(crs)
    if nodata is not None:
        prof["nodata"] = nodata
    with MemoryFile() as mf:
        with mf.open(**prof) as ds:
            ds.write(bands.astype(dtype))
            if tags:
                ds.update_tags(**tags)
            for band, bt in (band_tags or {}).items():
                ds.update_tags(band, **bt)
        return mf.read()


@builder("acc_stats")
def _acc_stats(tmpdir):
    return _tile(_gtiff(_grid_bands(), "EPSG:32633"))


@builder("acc_esri")
def _acc_esri(tmpdir):
    """A tile in ESRI:54008 (Sinusoidal), a CRS with an ESRI authority code and no EPSG one."""
    return _tile(_gtiff(_grid_bands(), "ESRI:54008"))


@builder("acc_nocrs")
def _acc_nocrs(tmpdir):
    return _tile(_gtiff(_grid_bands(), None))


@builder("acc_all_nodata")
def _acc_all_nodata(tmpdir):
    import numpy as np

    return _tile(_gtiff(np.full((1, 3, 4), NODATA, dtype="float32"), "EPSG:32633"))


@builder("acc_two_band")
def _acc_two_band(tmpdir):
    """Two Int16 bands (1..12 and twice that), no NoData, on the oracle grid in EPSG:4326, with a dataset tag and a tag
    on band 1."""
    from rasterio.transform import from_origin

    return _tile(
        _raster(
            _grid_bands(nodata_first=False, bands=2).astype("int16"),
            "EPSG:4326",
            from_origin(-1.0, 52.0, 0.01, 0.01),
            dtype="int16",
            tags={"AUTHOR": "gbx"},
            band_tags={1: {"UNITS": "m"}},
        )
    )


@builder("acc_rotated")
def _acc_rotated(tmpdir):
    from rasterio.transform import Affine

    return _tile(
        _raster(
            _grid_bands(nodata_first=False),
            "EPSG:4326",
            Affine(ROT["a"], ROT["b"], ROT["c"], ROT["d"], ROT["e"], ROT["f"]),
        )
    )


R = "gbx_rst_"
# the centre of pixel (column 1, row 0) of `acc_stats`, which holds the value 2, in its own CRS (UTM 33N)
PIXEL = (400015.0, 5759995.0)


# The same pixel centre in WGS 84 (longitude, latitude), from pyproj's EPSG:32633 -> EPSG:4326 transform: the
# independent side of the reprojection rows.
LONLAT = (13.544124253, 51.981621846)


def _point_wkb(srid=0):
    import shapely

    pt = shapely.Point(*PIXEL)
    return shapely.to_wkb(
        shapely.set_srid(pt, srid) if srid else pt, include_srid=bool(srid)
    )


builder("acc_point_wkb")(lambda tmpdir: _point_wkb())
builder("acc_point_ewkb")(lambda tmpdir: _point_wkb(32633))


def _c(cid, short, args, kind, expect=None, surfaces=("session",), **kw):
    return Case(cid, R + short, args, kind, expect, surfaces, **kw)


T4326, T3857, VIRT, STATS = (
    Build("tile_4326"),
    Build("tile_3857"),
    Build("tile_virtual"),
    Build("acc_stats"),
)
TWO, ROTD, NOCRS, EMPTY = (
    Build("acc_two_band"),
    Build("acc_rotated"),
    Build("acc_nocrs"),
    Build("acc_all_nodata"),
)
ERR, CORRUPT = Build("tile_error"), Build("tile_corrupt")


def _oracle_tile_len() -> int:
    from databricks.labs.gbx.bench.oracles import _tile_bytes

    return len(_tile_bytes("EPSG:4326"))


RASTER_ACCESSOR = (
    # size, band count, CRS
    _c("width", "width", (T4326,), "exact", 64),
    _c("height", "height", (T4326,), "exact", 48),
    _c("numbands", "numbands", (T4326,), "exact", 1),
    _c("numbands_two", "numbands", (TWO,), "exact", 2),
    _c("srid", "srid", (T4326,), "exact", 4326),
    _c("srid_3857", "srid", (T3857,), "exact", 3857),
    _c("srid_none", "srid", (NOCRS,), "null"),
    _c("crs", "crs", (T4326,), "exact", "EPSG:4326"),
    _c("crs_3857", "crs", (T3857,), "exact", "EPSG:3857"),
    _c("crs_none", "crs", (NOCRS,), "null"),
    # an ESRI authority code is kept: the CRS string names it and the SRID is its code
    _c("crs_esri", "crs", (Build("acc_esri"),), "exact", "ESRI:54008"),
    _c("srid_esri", "srid", (Build("acc_esri"),), "exact", 54008),
    # a virtual tile answers from its file's header, window-correct
    _c("width_virtual", "width", (VIRT,), "exact", 64),
    # geotransform
    _c("pixelwidth", "pixelwidth", (T4326,), "approx", 0.01),
    _c("pixelheight", "pixelheight", (T4326,), "approx", 0.01),
    _c("scalex", "scalex", (T4326,), "approx", 0.01),
    _c("scaley", "scaley", (T4326,), "approx", -0.01),
    _c("upperleftx", "upperleftx", (T4326,), "approx", -1.0),
    _c("upperlefty", "upperlefty", (T4326,), "approx", 52.0),
    _c("skewx", "skewx", (T4326,), "approx", 0.0),
    _c("skewy", "skewy", (T4326,), "approx", 0.0),
    _c("rotation", "rotation", (T4326,), "approx", 0.0),
    # a rotated grid: skews are gt2 / gt4, the pixel sizes include the skew, the rotation is atan(gt4 / gt1)
    _c("skewx_rotated", "skewx", (ROTD,), "approx", ROT["b"]),
    _c("skewy_rotated", "skewy", (ROTD,), "approx", ROT["d"]),
    _c(
        "pixelwidth_rotated",
        "pixelwidth",
        (ROTD,),
        "approx",
        math.hypot(ROT["a"], ROT["d"]),
    ),
    _c(
        "pixelheight_rotated",
        "pixelheight",
        (ROTD,),
        "approx",
        math.hypot(ROT["e"], ROT["b"]),
    ),
    _c(
        "rotation_rotated",
        "rotation",
        (ROTD,),
        "approx",
        math.atan(ROT["d"] / ROT["a"]),
    ),
    _c(
        "georeference",
        "georeference",
        (T4326,),
        "exact",
        {
            "upperLeftX": -1.0,
            "upperLeftY": 52.0,
            "scaleX": 0.01,
            "scaleY": -0.01,
            "skewX": 0.0,
            "skewY": 0.0,
        },
    ),
    _c("boundingbox", "boundingbox", (T4326,), "geom_equals", BOX_4326),
    # driver, band types, NoData
    _c("format", "format", (T4326,), "exact", "GTiff"),
    _c("format_hfa", "format", (Build("tile_4326_hfa"),), "exact", "HFA"),
    _c("type", "type", (T4326,), "exact", ["Float32"]),
    _c("type_two_band", "type", (TWO,), "exact", ["Int16", "Int16"]),
    _c("getnodata", "getnodata", (T4326,), "exact", [NODATA]),
    _c("getnodata_none", "getnodata", (TWO,), "null"),
    # sizes: the raster buffer of a materialized tile; the decoded window (1 band x 64 x 48 x 4 bytes) of a virtual one
    _c("memsize", "memsize", (T4326,), "exact", _oracle_tile_len),
    _c("memsize_virtual", "memsize", (VIRT,), "exact", 64 * 48 * 4),
    # metadata maps
    _c(
        "metadata",
        "metadata",
        (TWO,),
        "exact",
        {
            "driver": "GTiff",
            "width": "4",
            "height": "3",
            "count": "2",
            "dtype": "int16",
            "crs": "EPSG:4326",
            "nodata": "",
            "tag.AUTHOR": "gbx",
            "tag.AREA_OR_POINT": "Area",
        },
    ),
    _c("bandmetadata", "bandmetadata", (TWO, 1), "exact", {"UNITS": "m"}),
    _c("bandmetadata_none", "bandmetadata", (TWO, 2), "exact", {}),
    _c("subdatasets", "subdatasets", (T4326,), "exact", {}),
    # pixel <-> world: pixel centres on the 10 m grid from (400000, 5760000); a point outside gives a pixel outside
    _c(
        "rastertoworldcoord",
        "rastertoworldcoord",
        (STATS, 1, 2),
        "approx",
        (400015.0, 5759975.0),
    ),
    _c("rastertoworldcoordx", "rastertoworldcoordx", (STATS, 1, 2), "approx", 400015.0),
    _c(
        "rastertoworldcoordy", "rastertoworldcoordy", (STATS, 1, 2), "approx", 5759975.0
    ),
    _c(
        "worldtorastercoord",
        "worldtorastercoord",
        (STATS, 400015.0, 5759975.0),
        "exact",
        (1, 2),
    ),
    _c(
        "worldtorastercoordx",
        "worldtorastercoordx",
        (STATS, 400015.0, 5759975.0),
        "exact",
        1,
    ),
    _c(
        "worldtorastercoordy",
        "worldtorastercoordy",
        (STATS, 400015.0, 5759975.0),
        "exact",
        2,
    ),
    _c(
        "worldtorastercoord_edge",
        "worldtorastercoordx",
        (STATS, 400010.0, 5759990.0),
        "exact",
        1,
    ),
    _c(
        "worldtorastercoord_outside",
        "worldtorastercoord",
        (STATS, 399990.0, 5760010.0),
        "exact",
        (-1, -1),
    ),
    # the error contract: a NULL tile, an error tile and unreadable bytes give NULL, never an error
    _c("width_null_tile", "width", (None,), "null"),
    _c("width_error_tile", "width", (ERR,), "null"),
    _c("width_corrupt_tile", "width", (CORRUPT,), "null"),
    _c("bandmetadata_null_band", "bandmetadata", (TWO, None), "null"),
)

RASTER_STAT = (
    # one band: valid values are 2..12 (the first pixel is NoData)
    _c("avg", "avg", (STATS,), "approx", [7.0]),
    _c("min", "min", (STATS,), "exact", [2.0]),
    _c("max", "max", (STATS,), "exact", [12.0]),
    _c("median", "median", (STATS,), "exact", [7.0]),
    _c("pixelcount", "pixelcount", (STATS,), "exact", [11]),
    # two bands, no NoData: 1..12 and 2..24
    _c("avg_two_band", "avg", (TWO,), "approx", [6.5, 13.0]),
    _c("min_two_band", "min", (TWO,), "exact", [1.0, 2.0]),
    _c("max_two_band", "max", (TWO,), "exact", [12.0, 24.0]),
    _c("median_two_band", "median", (TWO,), "exact", [6.5, 13.0]),
    _c("pixelcount_two_band", "pixelcount", (TWO,), "exact", [12, 12]),
    # the mean of single-precision pixels is accumulated in double: the 64 x 48 tile's valid values are
    # (index mod 97) of a 3072-pixel ramp minus its first 4 x 4 block
    _c("avg_float32_precision", "avg", (T4326,), "approx", [47.71793193717277]),
    # a band with no valid pixel: NULL for each statistic and 0 for the count; the raster is empty
    _c("avg_all_nodata", "avg", (EMPTY,), "exact", [None]),
    _c("min_all_nodata", "min", (EMPTY,), "exact", [None]),
    _c("max_all_nodata", "max", (EMPTY,), "exact", [None]),
    _c("median_all_nodata", "median", (EMPTY,), "exact", [None]),
    _c("pixelcount_all_nodata", "pixelcount", (EMPTY,), "exact", [0]),
    _c("isempty", "isempty", (T4326,), "exact", False),
    _c("isempty_all_nodata", "isempty", (EMPTY,), "exact", True),
    _c("avg_virtual", "avg", (VIRT,), "approx", [47.71793193717277]),
    # histogram: n buckets across [min, max], the top edge in the last bucket, values outside the range dropped
    _c("histogram", "histogram", (STATS, 2), "exact", {"band_1": [5, 6]}),
    _c(
        "histogram_range",
        "histogram",
        (STATS, 3, 0.0, 6.0),
        "exact",
        {"band_1": [0, 2, 3]},
    ),
    _c(
        "histogram_top_edge",
        "histogram",
        (STATS, 2, 2.0, 12.0),
        "exact",
        {"band_1": [5, 6]},
    ),
    _c(
        "histogram_null_min",
        "histogram",
        (STATS, 2, None, 8.0),
        "exact",
        {"band_1": [3, 4]},
    ),
    _c(
        "histogram_null_max",
        "histogram",
        (STATS, 2, 7.0, None),
        "exact",
        {"band_1": [3, 3]},
    ),
    _c(
        "histogram_two_band",
        "histogram",
        (TWO, 2),
        "exact",
        {"band_1": [6, 6], "band_2": [6, 6]},
    ),
    _c(
        "histogram_include_nodata",
        "histogram",
        (STATS, 2, -9999.0, 12.0, True),
        "exact",
        {"band_1": [1, 11]},
    ),
    _c("histogram_zero_buckets", "histogram", (STATS, 0), "raises", "n_buckets"),
    # sample: the value of each band at a point in the raster CRS; outside the raster NULL
    _c("sample", "sample", (STATS, "POINT(400015 5759995)"), "exact", [2.0]),
    # the four geometry encodings give the same value; a point in another CRS is moved into the raster CRS
    _c(
        "sample_wkb",
        "sample",
        (STATS, Build("acc_point_wkb")),
        "exact",
        [2.0],
        variants=((STATS, Build("acc_point_ewkb")),),
    ),
    _c(
        "sample_wkt",
        "sample",
        (STATS, "POINT(400015 5759995)"),
        "exact",
        [2.0],
        variants=((STATS, "SRID=32633;POINT(400015 5759995)"),),
    ),
    _c(
        "sample_ewkt_reprojected",
        "sample",
        (STATS, "SRID=4326;POINT(%.9f %.9f)" % LONLAT),
        "exact",
        [2.0],
    ),
    _c(
        "sample_crs_argument",
        "sample",
        (STATS, "POINT(%.9f %.9f)" % LONLAT, "EPSG:4326"),
        "exact",
        [2.0],
    ),
    _c("sample_two_band", "sample", (TWO, "POINT(-0.995 51.995)"), "exact", [1.0, 2.0]),
    _c("sample_outside", "sample", (STATS, "POINT(500000 5760000)"), "null"),
    _c(
        "sample_not_a_point",
        "sample",
        (STATS, "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))"),
        "raises",
        "POINT",
    ),
    # tryopen: true for a tile that opens; false for bytes that do not and for a tile with nothing in it
    _c("tryopen", "tryopen", (T4326,), "exact", True),
    _c("tryopen_corrupt", "tryopen", (CORRUPT,), "exact", False),
    _c("tryopen_error_tile", "tryopen", (ERR,), "exact", False),
    # the error contract: a NULL tile and an error tile give NULL (an empty raster is true for isempty)
    _c("avg_null_tile", "avg", (None,), "null"),
    _c("avg_error_tile", "avg", (ERR,), "null"),
    _c("avg_corrupt_tile", "avg", (CORRUPT,), "null"),
    _c("isempty_error_tile", "isempty", (ERR,), "exact", True),
)
