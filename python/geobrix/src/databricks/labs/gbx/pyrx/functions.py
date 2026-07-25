"""pyrx public API — Arrow-UDF Column wrappers (signatures mirror rasterx).

Swap-compatible with ``databricks.labs.gbx.rasterx.functions``:
    from databricks.labs.gbx.pyrx import functions as prx
    df.select(prx.rst_width("tile"))
"""

from typing import List, Optional

import pandas as pd
from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import pandas_udf, udtf
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from databricks.labs.gbx import _register
from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx._udf import (
    ColLike,
    _col,
    _raster_field,
    sql_scalar_udf,
    sql_scalar_udf2,
    tile_scalar_udf,
    tile_scalar_udf2,
)
from databricks.labs.gbx.pyrx.core import accessors
from databricks.labs.gbx.pyrx.core import agg as agg_core
from databricks.labs.gbx.pyrx.core import analysis as analysis_core
from databricks.labs.gbx.pyrx.core import cellraster as cellraster_core
from databricks.labs.gbx.pyrx.core import coords
from databricks.labs.gbx.pyrx.core import derivedband as derivedband_core
from databricks.labs.gbx.pyrx.core import edit, features, focal, gridagg, indices
from databricks.labs.gbx.pyrx.core import mapalgebra as mapalgebra_core
from databricks.labs.gbx.pyrx.core import ops as ops_core
from databricks.labs.gbx.pyrx.core import resample, terrain
from databricks.labs.gbx.pyrx.core import tessellate as tessellate_core
from databricks.labs.gbx.pyrx.core import tiling
from databricks.labs.gbx.pyrx.core import tin as tin_core
from databricks.labs.gbx.pyrx.core import warp, xyz
from databricks.labs.gbx.pyrx.core.escape import rst_apply, tile_to_numpy  # noqa: F401


def _registrar_groups() -> List[_register.Group]:
    """One group for pyrx (rasterio guard): scalar/agg UDFs (from SQL_REGISTRY),
    UDTFs, and the format-agnostic pmtiles aggregate. Insertion order matches the
    pre-only register() ordering so only=None is behavior-identical."""
    entries = {}
    for name, udf_obj in SQL_REGISTRY.items():
        entries[name] = lambda s, n=name, u=udf_obj: s.udf.register(n, u)

    udtfs = [
        ("gbx_rst_polygonize", _RstPolygonizeUDTF),
        ("gbx_rst_h3_rastertogridavg", _RstH3RasterToGridAvgUDTF),
        ("gbx_rst_h3_rastertogridcount", _RstH3RasterToGridCountUDTF),
        ("gbx_rst_h3_rastertogridmax", _RstH3RasterToGridMaxUDTF),
        ("gbx_rst_h3_rastertogridmin", _RstH3RasterToGridMinUDTF),
        ("gbx_rst_h3_rastertogridmedian", _RstH3RasterToGridMedianUDTF),
        ("gbx_rst_quadbin_rastertogridavg", _RstQuadbinRasterToGridAvgUDTF),
        ("gbx_rst_quadbin_rastertogridcount", _RstQuadbinRasterToGridCountUDTF),
        ("gbx_rst_quadbin_rastertogridmax", _RstQuadbinRasterToGridMaxUDTF),
        ("gbx_rst_quadbin_rastertogridmin", _RstQuadbinRasterToGridMinUDTF),
        ("gbx_rst_quadbin_rastertogridmedian", _RstQuadbinRasterToGridMedianUDTF),
        ("gbx_rst_separatebands", _RstSeparateBandsUDTF),
        ("gbx_rst_retile", _RstRetileUDTF),
        ("gbx_rst_tooverlappingtiles", _RstToOverlappingTilesUDTF),
        ("gbx_rst_maketiles", _RstMakeTilesUDTF),
        ("gbx_rst_h3_tessellate", _RstH3TessellateUDTF),
        ("gbx_rst_quadbin_tessellate", _RstQuadbinTessellateUDTF),
        ("gbx_rst_xyzpyramid", _RstXyzPyramidUDTF),
    ]
    for name, cls in udtfs:
        entries[name] = lambda s, n=name, c=cls: s.udtf.register(n, c)

    def _reg_pmtiles(s):
        from databricks.labs.gbx.pmtiles import register_pmtiles_agg

        register_pmtiles_agg(s)

    entries["gbx_pmtiles_agg"] = _reg_pmtiles

    def _guard():
        from databricks.labs.gbx.pyrx import _env

        _env.assert_rasterio_available()

    return [(_guard, entries)]


def register(spark: SparkSession = None, only: Optional[List[str]] = None) -> None:
    """Explicitly register the pyrx functions as Spark SQL functions.

    Installs the same ``gbx_rst_*`` SQL names the heavyweight rasterx package
    uses, but powered by the pyspark/rasterio implementation (no JAR). Call this
    once when you want the functions from SQL. The Python Column API
    (``prx.rst_width(col)``) works WITHOUT this call.

    You register the lightweight OR the heavyweight package in a given session;
    they share the ``gbx_rst_*`` names, so the last registration wins.

    Args:
        spark: Spark session (uses the active session if not provided).
        only: Optional list of function names to register (instead of all).
            Accepts SQL names (``gbx_rst_slope``) or short names (``rst_slope``),
            case-insensitively. ``None`` registers everything; ``[]`` registers
            nothing. An unrecognized name raises ``ValueError``.
    """
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    _register.run_groups(_registrar_groups(), spark, only)


# --- Module-level UDF singletons (built once at import) ---------------------
_u_width = tile_scalar_udf(accessors.width, IntegerType())
_u_height = tile_scalar_udf(accessors.height, IntegerType())
_u_numbands = tile_scalar_udf(accessors.numbands, IntegerType())
_u_srid = tile_scalar_udf(accessors.srid, IntegerType())
_u_pixelwidth = tile_scalar_udf(accessors.pixelwidth, DoubleType())
_u_pixelheight = tile_scalar_udf(accessors.pixelheight, DoubleType())
_u_upperleftx = tile_scalar_udf(accessors.upperleftx, DoubleType())
_u_upperlefty = tile_scalar_udf(accessors.upperlefty, DoubleType())
_u_boundingbox = tile_scalar_udf(accessors.boundingbox, BinaryType())
_u_scalex = tile_scalar_udf(accessors.scalex, DoubleType())
_u_scaley = tile_scalar_udf(accessors.scaley, DoubleType())
_u_isempty = tile_scalar_udf(accessors.isempty, BooleanType())
_u_type = tile_scalar_udf(accessors.type, ArrayType(StringType()))
_u_getnodata = tile_scalar_udf(accessors.getnodata, ArrayType(DoubleType()))


# metadata: pandas_udf rejects MapType in some Arrow builds; fall back to
# a regular Python UDF for this one function only.
@f.udf(MapType(StringType(), StringType()))
def _metadata_udf(raster):
    if raster is None:
        return None
    from databricks.labs.gbx.pyrx import _env, _serde

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(raster)) as ds:
        return accessors.metadata(ds)


_u_r2w_x = tile_scalar_udf2(coords.raster_to_world_x, DoubleType())
_u_r2w_y = tile_scalar_udf2(coords.raster_to_world_y, DoubleType())
_u_w2r_x = tile_scalar_udf2(coords.world_to_raster_x, IntegerType())
_u_w2r_y = tile_scalar_udf2(coords.world_to_raster_y, IntegerType())


# --- Group 1: per-band statistics & accessor UDFs ---------------------------
_u_avg = tile_scalar_udf(accessors.avg, ArrayType(DoubleType()))
_u_min = tile_scalar_udf(accessors.minimum, ArrayType(DoubleType()))
_u_max = tile_scalar_udf(accessors.maximum, ArrayType(DoubleType()))
_u_median = tile_scalar_udf(accessors.median, ArrayType(DoubleType()))
_u_pixelcount = tile_scalar_udf(accessors.pixelcount, ArrayType(LongType()))
_u_rotation = tile_scalar_udf(accessors.rotation, DoubleType())
_u_skewx = tile_scalar_udf(accessors.skewx, DoubleType())
_u_skewy = tile_scalar_udf(accessors.skewy, DoubleType())
_u_format = tile_scalar_udf(accessors.format, StringType())


# memsize: works off the raw raster bytes (no rasterio open needed) — mirror
# heavyweight which returns the in-memory buffer length.
@f.udf(LongType())
def _memsize_udf(raster):
    if raster is None:
        return None
    return int(len(bytes(raster)))


# Struct-accepting memsize for SQL registration: reads the raster byte length
# from the tile struct directly (no rasterio open).
@f.udf(LongType())
def _memsize_struct_udf(tile):
    if tile is None or tile["raster"] is None:
        return None
    return int(len(bytes(tile["raster"])))


# MapType return paths use plain @f.udf (pandas_udf rejects MapType on some
# Arrow builds), matching the existing _metadata_udf fallback.
@f.udf(MapType(StringType(), DoubleType()))
def _georeference_udf(tile):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        return accessors.georeference(ds)


@f.udf(MapType(StringType(), StringType()))
def _bandmetadata_udf(tile, band):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        return accessors.bandmetadata(ds, int(band))


@f.udf(MapType(StringType(), StringType()))
def _subdatasets_udf(tile):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        return accessors.subdatasets(ds)


@f.udf(StringType())
def _summary_udf(tile):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        return accessors.summary(ds)


@f.udf(MapType(StringType(), ArrayType(LongType())))
def _histogram_udf(tile, n_buckets, min_val, max_val, include_nodata):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    nb = 256 if n_buckets is None else int(n_buckets)
    lo = None if min_val is None else float(min_val)
    hi = None if max_val is None else float(max_val)
    inc = bool(include_nodata) if include_nodata is not None else False
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        return accessors.histogram(ds, nb, lo, hi, inc)


@f.udf(_serde.TILE_SCHEMA)
def _getsubdataset_udf(tile, name):
    if tile is None or tile["raster"] is None or name is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = accessors.getsubdataset(ds, str(name))
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


# --- Group 2: struct coordinate UDFs ----------------------------------------
_R2W_COORD_SCHEMA = StructType(
    [
        StructField("x", DoubleType(), True),
        StructField("y", DoubleType(), True),
    ]
)
_W2R_COORD_SCHEMA = StructType(
    [
        StructField("x", IntegerType(), True),
        StructField("y", IntegerType(), True),
    ]
)

_GRID_SCHEMA = StructType(
    [
        StructField("xmin", DoubleType()),
        StructField("ymin", DoubleType()),
        StructField("xmax", DoubleType()),
        StructField("ymax", DoubleType()),
        StructField("pixel_size", DoubleType()),
        StructField("width", IntegerType()),
        StructField("height", IntegerType()),
        StructField("srid", IntegerType()),
    ]
)

_BBOX_SCHEMA = StructType(
    [
        StructField("xmin", DoubleType()),
        StructField("ymin", DoubleType()),
        StructField("xmax", DoubleType()),
        StructField("ymax", DoubleType()),
    ]
)


@f.udf(_R2W_COORD_SCHEMA)
def _rastertoworldcoord_udf(tile, x, y):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        return coords.raster_to_world_coord(ds, int(x), int(y))


@f.udf(_W2R_COORD_SCHEMA)
def _worldtorastercoord_udf(tile, x, y):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        return coords.world_to_raster_coord(ds, float(x), float(y))


# --- Constructor ------------------------------------------------------------
@f.udf(_serde.TILE_SCHEMA)
def _fromcontent_udf(raster, drv):
    if raster is None:
        return None
    return _serde.build_tile(bytes(raster), drv or "GTiff")


def rst_fromcontent(content: ColLike, driver: ColLike) -> Column:
    """Build a tile struct from raster BINARY content and GDAL driver name."""
    return _fromcontent_udf(_col(content), _col(driver))


# rst_fromfile: read raster bytes from a (FUSE) path and wrap as a tile.
# Mirrors the heavyweight gbx_rst_fromfile(path, driver): 2 string args; the
# driver is a format hint carried into metadata (rasterio auto-detects on open).
# The heavyweight wraps eval in Option(...).orNull, so a bad/missing path
# yields NULL rather than raising — match that by returning None on failure.
@f.udf(_serde.TILE_SCHEMA)
def _fromfile_udf(path, driver):
    if path is None:
        return None
    from rasterio.io import MemoryFile

    from databricks.labs.gbx.ds._listing import to_local_path
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    drv = "GTiff" if driver is None else str(driver)
    try:
        # Read the source bytes SEQUENTIALLY (FUSE-safe) and open from an in-memory
        # MemoryFile, rather than rasterio.open() on the path: a tiled/COG GTiff over a
        # UC Volume seeks to block offsets on read, which Volume FUSE can't serve. Columns
        # carry dbfs:-qualified paths (to_spark_uri); strip the scheme for the open().
        with open(to_local_path(str(path)), "rb") as _fh:
            _src_bytes = _fh.read()
        with MemoryFile(_src_bytes) as _src_mf, _src_mf.open() as src:
            data = src.read()
            profile = src.profile.copy()
            profile.update(driver="GTiff")
            with MemoryFile() as mf:
                with mf.open(**profile) as dst:
                    dst.write(data)
                new_bytes = mf.read()
    except Exception:
        # Heavyweight returns NULL on read failure (Option(...).orNull).
        return None
    return _serde.build_tile(new_bytes, drv, 0)


def rst_fromfile(path: ColLike, driver: ColLike = "GTiff") -> Column:
    """Build a tile struct by reading the raster at ``path`` into its bytes.

    Mirrors the heavyweight ``gbx_rst_fromfile(path, driver)``: ``path`` is a
    filesystem path (FUSE ``/Volumes/...`` paths work on Databricks) and
    ``driver`` is a GDAL driver short-name hint carried into the tile metadata
    (rasterio auto-detects the actual format on open). A path that cannot be
    read returns NULL (matching the heavyweight's null-on-error behaviour).

    Args:
        path:   Raster file path (string column).
        driver: GDAL driver short name hint. Defaults to "GTiff".

    Returns:
        Tile struct, or NULL if the path cannot be read.
    """
    drv = f.lit(driver) if isinstance(driver, str) else _col(driver)
    return _fromfile_udf(_col(path), drv)


# rst_merge: single ARRAY<tile struct> arg in one row -> mosaic tile.
# Mirrors gbx_rst_merge: extract each element's raster bytes and mosaic by
# extent (reuses core.agg.merge_tiles, the same union-extent reducer the
# heavyweight RST_MergeAgg uses). cellid = 0 (no aggregate group key here).
@f.udf(_serde.TILE_SCHEMA)
def _merge_udf(tiles):
    if not tiles:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    rasters = [
        bytes(t["raster"]) for t in tiles if t is not None and t["raster"] is not None
    ]
    if not rasters:
        return None
    new_bytes = agg_core.merge_tiles(rasters)
    return _serde.build_tile(new_bytes, "GTiff", 0)


def rst_merge(tiles: ColLike) -> Column:
    """Mosaic an ARRAY of tiles into one tile spanning their union extent.

    Mirrors the heavyweight ``gbx_rst_merge``: ``tiles`` is a single column of
    ARRAY<tile struct> (e.g. ``f.array("ta", "tb")``); each element's raster is
    placed by its own georeference and the output spans the union extent. On
    overlap the merge is first-tile-wins in array order. Output ``cellid`` is 0.

    Args:
        tiles: Column of ARRAY<tile struct>.

    Returns:
        Tile struct spanning the union extent, or NULL on an empty array.
    """
    return _merge_udf(_col(tiles))


# rst_combineavg: single ARRAY<tile struct> arg -> per-pixel mean tile.
# Mirrors gbx_rst_combineavg: NoData-aware per-pixel mean across the stack
# (reuses core.agg.combineavg_tiles). cellid follows the heavyweight rule —
# head's cellid if every element shares it, else -1.
@f.udf(_serde.TILE_SCHEMA)
def _combineavg_udf(tiles):
    if not tiles:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    elems = [t for t in tiles if t is not None and t["raster"] is not None]
    if not elems:
        return None
    rasters = [bytes(t["raster"]) for t in elems]
    cellids = {int(t["cellid"]) for t in elems}
    cellid = int(elems[0]["cellid"]) if len(cellids) == 1 else -1
    new_bytes = agg_core.combineavg_tiles(rasters)
    return _serde.build_tile(new_bytes, "GTiff", cellid)


def rst_combineavg(tiles: ColLike) -> Column:
    """NoData-aware per-pixel mean across an ARRAY of aligned tiles.

    Mirrors the heavyweight ``gbx_rst_combineavg``: ``tiles`` is a single column
    of ARRAY<tile struct>; each declared NoData is excluded from both the sum
    and the divisor (a valid 0 counts). Output ``cellid`` is the shared input
    cellid when every element matches, else -1 (matching the heavyweight).

    PARITY DIVERGENCE: assumes the tiles are ALREADY aligned (same
    shape/extent/CRS) and raises ``ValueError`` on mismatched shapes rather than
    resampling (inherited from ``core.agg.combineavg_tiles``).

    Args:
        tiles: Column of ARRAY<tile struct> (same-grid).

    Returns:
        Tile struct of per-pixel means, or NULL on an empty array.
    """
    return _combineavg_udf(_col(tiles))


# rst_frombands: single ARRAY<single-band tile> arg -> multi-band tile.
# Mirrors gbx_rst_frombands: array ORDER is band order (element 0 -> band 1).
# Reuses core.agg.frombands_tiles by pairing each element with its 0-based
# position as the band_index, so the reducer's ascending sort preserves order.
@f.udf(_serde.TILE_SCHEMA)
def _frombands_udf(bands):
    if not bands:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    indexed = [
        (i, bytes(t["raster"]))
        for i, t in enumerate(bands)
        if t is not None and t["raster"] is not None
    ]
    if not indexed:
        return None
    cellid = int(bands[0]["cellid"]) if bands[0] is not None else 0
    new_bytes = agg_core.frombands_tiles(indexed)
    return _serde.build_tile(new_bytes, "GTiff", cellid)


def rst_frombands(bands: ColLike) -> Column:
    """Assemble an ARRAY of single-band tiles into one multi-band tile.

    Mirrors the heavyweight ``gbx_rst_frombands``: ``bands`` is a single column
    of ARRAY<tile struct> and the ARRAY ORDER is the band order (element 0 ->
    band 1, element 1 -> band 2, ...). Georef/CRS/dtype/nodata are taken from
    the first element. Output ``cellid`` carries from the first element.

    Args:
        bands: Column of ARRAY<single-band tile struct>, in band order.

    Returns:
        Multi-band tile struct, or NULL on an empty array.
    """
    return _frombands_udf(_col(bands))


# --- Tier 1b: tile-returning warp UDFs -------------------------------------
@f.udf(_serde.TILE_SCHEMA)
def _transform_udf(tile, target_srid):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = warp.reproject_to_srid(ds, int(target_srid))
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _to_webmercator_udf(tile, resampling):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = warp.reproject_to_srid(ds, 3857, resampling=str(resampling))
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


def rst_transform(tile: ColLike, target_srid: ColLike) -> Column:
    """Reproject the raster to the target SRID (EPSG code)."""
    return _transform_udf(_col(tile), _col(target_srid))


def rst_to_webmercator(tile: ColLike, resampling: ColLike = "bilinear") -> Column:
    """Reproject the tile to EPSG:3857 (web mercator). resampling defaults to 'bilinear'."""
    resampling_col = (
        f.lit(resampling) if isinstance(resampling, str) else _col(resampling)
    )
    return _to_webmercator_udf(_col(tile), resampling_col)


# --- Tier 1c: tile-returning resample UDFs ----------------------------------
@f.udf(_serde.TILE_SCHEMA)
def _resample_udf(tile, factor, algorithm):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = resample.resample_by_factor(ds, float(factor), str(algorithm))
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _resample_to_size_udf(tile, width_px, height_px, algorithm):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = resample.resample_to_size(
            ds, int(width_px), int(height_px), str(algorithm)
        )
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _resample_to_res_udf(tile, x_res, y_res, algorithm):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = resample.resample_to_res(
            ds, float(x_res), float(y_res), str(algorithm)
        )
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


def rst_resample(
    tile: ColLike, factor: ColLike, algorithm: ColLike = "bilinear"
) -> Column:
    """Resample a raster tile by a multiplicative factor (>1 upsamples, 0<factor<1 downsamples).

    CRS and geographic extent are preserved; only the pixel grid changes.
    """
    alg = f.lit(algorithm) if isinstance(algorithm, str) else _col(algorithm)
    return _resample_udf(_col(tile), _col(factor), alg)


def rst_resample_to_size(
    tile: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    algorithm: ColLike = "bilinear",
) -> Column:
    """Resample a raster tile to exact pixel dimensions (width_px x height_px).

    CRS and geographic extent are preserved; only the pixel grid changes.
    """
    alg = f.lit(algorithm) if isinstance(algorithm, str) else _col(algorithm)
    return _resample_to_size_udf(_col(tile), _col(width_px), _col(height_px), alg)


def rst_resample_to_res(
    tile: ColLike,
    x_res: ColLike,
    y_res: ColLike,
    algorithm: ColLike = "bilinear",
) -> Column:
    """Resample a raster tile to a target ground resolution in CRS units.

    CRS and geographic extent are preserved; pixel count is derived from extent / resolution.
    """
    alg = f.lit(algorithm) if isinstance(algorithm, str) else _col(algorithm)
    return _resample_to_res_udf(_col(tile), _col(x_res), _col(y_res), alg)


# --- Tier 1d: tile-returning edit UDFs -------------------------------------
@f.udf(_serde.TILE_SCHEMA)
def _clip_udf(tile, geom_wkb, all_touched):
    if tile is None or tile["raster"] is None or geom_wkb is None:
        return None
    from databricks.labs.gbx._geom import parse_geom
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    # parse_geom keeps the SRID (EWKT/EWKB carry it) so clip_to_geom can
    # reproject the cutline to the raster CRS, mirroring heavy RST_Clip.
    geom = parse_geom(geom_wkb)
    if geom is None:
        return None
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = edit.clip_to_geom(ds, geom, bool(all_touched))
    if new_bytes is None:
        # Cutline does not overlap the raster -> null tile (no crash), mirroring
        # heavy GDAL Warp -cutline producing an empty result.
        return None
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _update_type_udf(tile, new_type):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = edit.update_type(ds, str(new_type))
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _init_nodata_udf(tile):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = edit.init_nodata(ds)
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


def rst_clip(tile: ColLike, clip: ColLike, cutline_all_touched: ColLike) -> Column:
    """Clip the raster to a geometry (WKB, EWKB, WKT, or EWKT). cutline_all_touched includes pixels touched by the boundary."""
    return _clip_udf(_col(tile), _col(clip), _col(cutline_all_touched))


def rst_updatetype(tile: ColLike, new_type: ColLike) -> Column:
    """Cast all raster bands to a new GDAL data type (e.g. 'Int32', 'Float64')."""
    return _update_type_udf(_col(tile), _col(new_type))


def rst_initnodata(tile: ColLike) -> Column:
    """Ensure a NoData value is set on the raster tile; uses -9999.0 if not already set."""
    return _init_nodata_udf(_col(tile))


# --- Tier 1d6: operations UDFs (tryopen, setsrid, band, asformat, ----------
# buildoverviews, sample) ----------------------------------------------------
@f.udf(BooleanType())
def _tryopen_udf(tile):
    if tile is None or tile["raster"] is None:
        return False
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    return ops_core.try_open(bytes(tile["raster"]))


@f.udf(_serde.TILE_SCHEMA)
def _setsrid_udf(tile, srid):
    if tile is None or tile["raster"] is None or srid is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = edit.set_srid(ds, int(srid))
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _band_udf(tile, band_index):
    if tile is None or tile["raster"] is None or band_index is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = edit.band(ds, int(band_index))
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _asformat_udf(tile, new_format):
    if tile is None or tile["raster"] is None or new_format is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = ops_core.as_format(ds, str(new_format))
    # metadata.driver must reflect the requested output format.
    return _serde.build_tile(new_bytes, str(new_format), tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _buildoverviews_udf(tile, levels, resampling):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    resamp = "average" if resampling is None else str(resampling)
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = ops_core.build_overviews(ds, list(levels), resamp)
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(ArrayType(DoubleType()))
def _sample_udf(tile, geom_wkb):
    if tile is None or tile["raster"] is None or geom_wkb is None:
        return None
    from databricks.labs.gbx._geom import parse_geom
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    # parse_geom keeps the SRID (EWKT/EWKB carry it) so ops_core.sample can
    # reproject the point to the raster CRS, mirroring heavy intent.
    geom = parse_geom(geom_wkb)
    if geom is None:
        return None
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        return ops_core.sample(ds, geom)


@f.udf(_serde.TILE_SCHEMA)
def _proximity_udf(tile, target_values, distunits, max_distance):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    units = "GEO" if distunits is None else str(distunits)
    tv = None if target_values is None else str(target_values)
    md = None if max_distance is None else float(max_distance)
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = analysis_core.proximity(ds, tv, units, md)
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


# rst_contour: tile + levels (ARRAY<DOUBLE>) + interval/base/attr_field ->
# ARRAY<struct(geom_wkb BINARY, value DOUBLE)> (mirrors _polygonize_udf shape).
_CONTOUR_SCHEMA = ArrayType(
    StructType(
        [
            StructField("geom_wkb", BinaryType(), nullable=False),
            StructField("value", DoubleType(), nullable=False),
        ]
    )
)


@f.udf(_CONTOUR_SCHEMA)
def _contour_udf(tile, levels, interval, base, attr_field):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    lvls = [] if levels is None else [float(v) for v in levels if v is not None]
    iv = 0.0 if interval is None else float(interval)
    bs = 0.0 if base is None else float(base)
    attr = "elev" if attr_field is None else str(attr_field)
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        return analysis_core.contour(ds, lvls, iv, bs, attr)


@f.udf(_serde.TILE_SCHEMA)
def _viewshed_udf(tile, observer_geom, observer_height, target_height, max_distance):
    if tile is None or tile["raster"] is None or observer_geom is None:
        return None
    from databricks.labs.gbx._geom import parse_geom
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    # observer_geom may be WKB/EWKB (binary) or WKT/EWKT (string); require a POINT.
    geom = parse_geom(observer_geom)
    if geom is None:
        return None
    if geom.geom_type != "Point":
        raise ValueError(
            f"rst_viewshed requires a POINT observer_geom; got {geom.geom_type}"
        )
    oh = 0.0 if observer_height is None else float(observer_height)
    th = 0.0 if target_height is None else float(target_height)
    md = None if max_distance is None else float(max_distance)
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        # CRS-align the observer: if the point carries a positive SRID and the
        # raster has a CRS, reproject EPSG:srid -> raster CRS so a 4326 observer
        # over a UTM DEM lands correctly. Heavy RST_Viewshed assumes a pre-aligned
        # observer; the light tier reprojects so a differently-projected point does
        # not silently miss. Unknown EPSG / transform failure -> use as-is.
        import shapely as _shapely

        ox, oy = geom.x, geom.y
        srid = _shapely.get_srid(geom)
        if srid > 0 and ds.crs is not None:
            try:
                import rasterio as _rio
                from rasterio.warp import transform as _transform

                src_crs = _rio.crs.CRS.from_epsg(srid)
                if src_crs != ds.crs:
                    xs, ys = _transform(src_crs, ds.crs, [ox], [oy])
                    ox, oy = xs[0], ys[0]
            except Exception:
                ox, oy = geom.x, geom.y
        new_bytes = analysis_core.viewshed(ds, ox, oy, oh, th, md)
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _cog_convert_udf(tile, compression, blocksize, overview_resampling):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    comp = "DEFLATE" if compression is None else str(compression)
    bs = 512 if blocksize is None else int(blocksize)
    resamp = "AVERAGE" if overview_resampling is None else str(overview_resampling)
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = analysis_core.cog_convert(ds, comp, bs, resamp)
    # COG is a GTiff variant on disk; downstream readers see driver "GTiff".
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


def rst_tryopen(tile: ColLike) -> Column:
    """Return BOOLEAN: True if the raster bytes open as a valid dataset.

    Mirrors the heavyweight ``gbx_rst_tryopen`` — any failure to open (corrupt
    bytes, unknown format) yields False rather than raising.
    """
    return _tryopen_udf(_col(tile))


def rst_setsrid(tile: ColLike, srid: ColLike) -> Column:
    """Stamp the CRS as ``EPSG:<srid>`` WITHOUT reprojecting the pixels.

    Equivalent to ``gdal_edit.py -a_srs``: pixel values and the GeoTransform
    are unchanged; only the CRS metadata is rewritten. Use ``rst_transform``
    for an actual reprojecting warp.

    Args:
        tile: Tile struct column.
        srid: Positive EPSG code to stamp.

    Returns:
        Tile with the same pixels/transform but CRS = EPSG:srid.
    """
    return _setsrid_udf(_col(tile), _col(srid))


def rst_band(tile: ColLike, band_index: ColLike) -> Column:
    """Extract a single 1-based band as a new single-band tile.

    Equivalent to ``gdal_translate -b <band_index>``: the extracted tile
    preserves the source CRS, GeoTransform, nodata, and dtype; only the band
    count is reduced to 1. ``band_index`` is 1-based and must be in range.

    Args:
        tile:       Tile struct column.
        band_index: 1-based band index in ``[1 .. numbands]``.

    Returns:
        Single-band tile struct.
    """
    return _band_udf(_col(tile), _col(band_index))


def rst_asformat(tile: ColLike, new_format: ColLike) -> Column:
    """Re-encode the raster to another GDAL driver (e.g. 'PNG', 'GTiff').

    Mirrors the heavyweight ``gbx_rst_asformat``: the output tile's raster
    bytes are encoded in ``new_format`` and the tile metadata ``driver``
    reflects it. Raises if the requested driver is unavailable in this GDAL
    build.

    Args:
        tile:       Tile struct column.
        new_format: GDAL driver short name (e.g. 'GTiff', 'PNG').

    Returns:
        Tile struct whose raster bytes are encoded in ``new_format``.
    """
    fmt = f.lit(new_format) if isinstance(new_format, str) else _col(new_format)
    return _asformat_udf(_col(tile), fmt)


def rst_buildoverviews(
    tile: ColLike, levels: ColLike, resampling: ColLike = "average"
) -> Column:
    """Build internal pyramid overviews at the given decimation ``levels``.

    Mirrors the heavyweight ``gbx_rst_buildoverviews``: ``levels`` is a
    non-empty array of integer decimation factors, each >= 2; ``resampling``
    defaults to "average" (one of near, average, rms, gauss, cubic,
    cubicspline, lanczos, bilinear, mode). Overviews are embedded internally
    in the output GTiff (no .ovr sidecar).

    Args:
        tile:       Tile struct column.
        levels:     ARRAY<INT> of decimation factors (e.g. ``f.array(...)``).
        resampling: Overview resampling algorithm. Defaults to "average".

    Returns:
        Tile struct with internal overviews embedded.
    """
    resamp = f.lit(resampling) if isinstance(resampling, str) else _col(resampling)
    return _buildoverviews_udf(_col(tile), _col(levels), resamp)


def rst_proximity(
    tile: ColLike,
    target_values: ColLike = None,
    distunits: ColLike = "GEO",
    max_distance: ColLike = None,
) -> Column:
    """Compute a proximity raster: each pixel's distance to the nearest source.

    Mirrors the heavyweight ``gbx_rst_proximity`` (GDAL ComputeProximity),
    implemented with ``scipy.ndimage.distance_transform_edt``.

    Args:
        tile:          Tile struct column.
        target_values: Optional comma-separated string of source pixel values,
                       matched in GDAL's integer domain (each pixel is rounded to
                       the nearest integer before the comparison). When given,
                       source pixels are those whose rounded value is in the set.
                       When None, the GDAL default applies: source = pixels whose
                       rounded value is != 0.
        distunits:     ``"GEO"`` (default; CRS ground units, scaled by pixel
                       size) or ``"PIXEL"`` (pixel counts).
        max_distance:  Optional positive distance cap; pixels beyond it become
                       NoData.

    Returns:
        Single-band Float32 tile (nodata = -1.0); source pixels get distance 0.
    """
    tv_col = (
        f.lit(None).cast(StringType())
        if target_values is None
        else (
            f.lit(target_values)
            if isinstance(target_values, str)
            else _col(target_values)
        )
    )
    units_col = (
        f.lit("GEO")
        if distunits is None
        else (f.lit(distunits) if isinstance(distunits, str) else _col(distunits))
    )
    md_col = (
        f.lit(None).cast(DoubleType())
        if max_distance is None
        else (
            f.lit(float(max_distance))
            if isinstance(max_distance, (int, float))
            else _col(max_distance)
        )
    )
    return _proximity_udf(_col(tile), tv_col, units_col, md_col)


def rst_cog_convert(
    tile: ColLike,
    compression: ColLike = "DEFLATE",
    blocksize: ColLike = 512,
    overview_resampling: ColLike = "AVERAGE",
) -> Column:
    """Convert a raster tile to a Cloud Optimized GeoTIFF (COG) layout.

    Mirrors the heavyweight ``gbx_rst_cog_convert`` (``gdal.Translate -of COG``),
    implemented with rio-cogeo's ``cog_translate``. The output tile's raster
    bytes are COG-layout GTiff; downstream readers see ``metadata.driver =
    "GTiff"`` (COG is a GTiff variant).

    Args:
        tile:                Tile struct column.
        compression:         COG compression / rio-cogeo profile (default
                             "DEFLATE"; e.g. NONE, LZW, ZSTD, WEBP, JPEG, LERC).
        blocksize:           Internal tile size in pixels, square (default 512).
        overview_resampling: Overview-pyramid resampling (default "AVERAGE").

    Returns:
        Tile struct whose raster bytes are a COG.
    """
    comp_col = (
        f.lit("DEFLATE")
        if compression is None
        else (f.lit(compression) if isinstance(compression, str) else _col(compression))
    )
    bs_col = (
        f.lit(512)
        if blocksize is None
        else (f.lit(int(blocksize)) if isinstance(blocksize, int) else _col(blocksize))
    )
    resamp_col = (
        f.lit("AVERAGE")
        if overview_resampling is None
        else (
            f.lit(overview_resampling)
            if isinstance(overview_resampling, str)
            else _col(overview_resampling)
        )
    )
    return _cog_convert_udf(_col(tile), comp_col, bs_col, resamp_col)


def rst_contour(
    tile: ColLike,
    levels: ColLike,
    interval: ColLike = 0.0,
    base: ColLike = 0.0,
    attr_field: ColLike = "elev",
) -> Column:
    """Generate contour lines from a raster as ``(geom_wkb, value)`` features.

    Mirrors the heavyweight ``gbx_rst_contour`` (GDAL ContourGenerateEx),
    implemented with ``skimage.measure.find_contours``.

    Args:
        tile:       Tile struct column.
        levels:     ARRAY<DOUBLE> of explicit contour values (e.g.
                    ``f.array(f.lit(10.0), f.lit(20.0))``). Pass an empty array
                    (``f.array().cast("array<double>")``) to use ``interval``.
        interval:   Equal-interval step; used only when ``levels`` is empty
                    (must then be > 0). Defaults to 0.0.
        base:       Contour base value for the interval mode. Defaults to 0.0.
        attr_field: Value-field label (parity-only; the struct field is always
                    ``value``). Defaults to "elev".

    Returns:
        ARRAY<struct(geom_wkb BINARY, value DOUBLE)> — one LineString per
        contour, in the raster's CRS.
    """
    iv = (
        f.lit(float(interval)) if isinstance(interval, (int, float)) else _col(interval)
    )
    bs = f.lit(float(base)) if isinstance(base, (int, float)) else _col(base)
    attr = f.lit(attr_field) if isinstance(attr_field, str) else _col(attr_field)
    return _contour_udf(_col(tile), _col(levels), iv, bs, attr)


def rst_viewshed(
    tile: ColLike,
    observer_geom: ColLike,
    observer_height: ColLike = 0.0,
    target_height: ColLike = 1.6,
    max_distance: ColLike = None,
) -> Column:
    """Compute a binary viewshed (255 visible / 0 invisible) from a DEM tile.

    Mirrors the heavyweight ``gbx_rst_viewshed`` (GDAL ViewshedGenerate),
    implemented with ``xrspatial.viewshed``.

    Args:
        tile:            Tile struct column (the DEM).
        observer_geom:   POINT observer location in the raster's CRS, as WKB
                         (BINARY) or WKT (STRING). Non-POINT geometries raise.
        observer_height: Observer height above the DEM (>= 0). Defaults to 0.0.
        target_height:   Target height above the DEM at each tested cell (>= 0).
                         Defaults to 0.0.
        max_distance:    Optional analysis-distance cap in CRS ground units
                         (> 0). ``None`` = unlimited.

    Returns:
        Single-band uint8 (Byte) tile struct: 255 = visible, 0 = invisible.

    PARITY DIVERGENCE: the visibility front-end is xarray-spatial's CPU
    line-of-sight scan (vertical-angle grid thresholded to a binary mask),
    not GDAL's GVM_Edge sweep with earth-curvature correction — the binary
    visible/invisible classification matches but exact edge cells near grazing
    angles or with curvature can differ.
    """
    oh = (
        f.lit(float(observer_height))
        if isinstance(observer_height, (int, float))
        else _col(observer_height)
    )
    th = (
        f.lit(float(target_height))
        if isinstance(target_height, (int, float))
        else _col(target_height)
    )
    md = (
        f.lit(None).cast(DoubleType())
        if max_distance is None
        else (
            f.lit(float(max_distance))
            if isinstance(max_distance, (int, float))
            else _col(max_distance)
        )
    )
    return _viewshed_udf(_col(tile), _col(observer_geom), oh, th, md)


def rst_sample(tile: ColLike, geom_wkb: ColLike) -> Column:
    """Sample per-band raster values at a POINT geometry (WKB, EWKB, WKT, or EWKT).

    Mirrors the heavyweight ``gbx_rst_sample``: requires a POINT geometry
    (raises otherwise), uses (geom.x, geom.y) as a world coordinate already
    aligned to the raster CRS, and returns ARRAY<DOUBLE> with one value per
    band in band order. Points outside the raster extent return null.

    Args:
        tile:     Tile struct column.
        geom_wkb: POINT geometry as WKB, EWKB, WKT, or EWKT.

    Returns:
        ARRAY<DOUBLE>: one value per band, or null if the point is out of extent.
    """
    return _sample_udf(_col(tile), _col(geom_wkb))


# --- Tier 1d3: band-math / focal UDFs --------------------------------------
@f.udf(_serde.TILE_SCHEMA)
def _threshold_udf(tile, op, value):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = edit.threshold(ds, op, value)
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _filter_udf(tile, kernel_size, operation):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = focal.filt(ds, int(kernel_size), str(operation))
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _convolve_udf(tile, kernel):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = focal.convolve(ds, kernel)
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


def rst_threshold(tile: ColLike, op: ColLike = None, value: ColLike = None) -> Column:
    """Keep pixels satisfying the comparison; others become NoData.

    Args:
        tile:  Tile struct column.
        op:    Comparison operator: ">", "<", ">=", "<=", "==", "!=".
               Defaults to ">".
        value: Threshold scalar.  Defaults to 0.0.

    Returns:
        Tile with the same dtype and band count; failing pixels set to NoData.
    """
    op_col = (
        f.lit(">") if op is None else (f.lit(op) if isinstance(op, str) else _col(op))
    )
    val_col = (
        f.lit(0.0)
        if value is None
        else (f.lit(value) if isinstance(value, (int, float)) else _col(value))
    )
    return _threshold_udf(_col(tile), op_col, val_col)


def rst_filter(tile: ColLike, kernel_size: ColLike, operation: ColLike) -> Column:
    """Apply a focal filter over a square window per band.

    Args:
        tile:        Tile struct column.
        kernel_size: Side length of the square neighbourhood (odd integer).
        operation:   One of "min", "max", "mean", "median".

    Returns:
        Filtered tile; same band count.  "mean" returns Float32; others
        preserve the input dtype.
    """
    return _filter_udf(_col(tile), _col(kernel_size), _col(operation))


def rst_convolve(tile: ColLike, kernel: ColLike) -> Column:
    """Convolve each band with a 2-D kernel (ARRAY<ARRAY<DOUBLE>>).

    Args:
        tile:   Tile struct column.
        kernel: 2-D array column of floats (e.g. built with ``f.array``).

    Returns:
        Convolved tile with dtype Float64.
    """
    return _convolve_udf(_col(tile), _col(kernel))


# --- Tier 1d4: map algebra UDF ----------------------------------------------
@f.udf(_serde.TILE_SCHEMA)
def _mapalgebra_udf(tiles, expression):
    if tiles is None or expression is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    rasters = [
        bytes(t["raster"]) for t in tiles if t is not None and t["raster"] is not None
    ]
    if not rasters:
        return None
    new_bytes = mapalgebra_core.mapalgebra(rasters, str(expression))
    return _serde.build_tile(new_bytes, "GTiff", tiles[0]["cellid"])


def rst_mapalgebra(tiles: ColLike, expression: ColLike) -> Column:
    """Apply a map-algebra expression across an array of tiles.

    Band 1 of each tile (in array order) binds to A, B, C, …; the expression is
    evaluated with numexpr (safe math only — no arbitrary code execution).
    Output is a single-band Float32 tile on the first input's georeference.

    Args:
        tiles:      Column of ARRAY<tile struct> (e.g. ``f.array("ta", "tb")``).
        expression: Math expression string, e.g. ``"(A - B) / (A + B)"``.

    Returns:
        Single-band Float32 tile struct.
    """
    expr_col = f.lit(expression) if isinstance(expression, str) else _col(expression)
    return _mapalgebra_udf(_col(tiles), expr_col)


# --- Tier 1d3: generic named-index dispatcher (rst_index) -------------------
@f.udf(_serde.TILE_SCHEMA)
def _index_udf(tile, formula_name, band_map):
    if tile is None or tile["raster"] is None or formula_name is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = indices.index(ds, str(formula_name), dict(band_map or {}))
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


def rst_index(tile: ColLike, formula_name: ColLike, band_map: ColLike) -> Column:
    """Compute a named spectral index via a band-map (mirrors ``gbx_rst_index``).

    ``formula_name`` (case-insensitive) selects a built-in formula; ``band_map``
    is a MAP<STRING, INT> wiring the formula's named bands to 1-based band
    indices in the tile. Returns a single-band Float32 tile.

    Built-in formulae: ``ndvi``, ``gndvi``, ``msavi``, ``ndvi_re``, ``ndmi``,
    ``ndsi``.

    Args:
        tile:         Tile struct column.
        formula_name: Built-in index name (string literal or column).
        band_map:     MAP<STRING, INT> column (e.g. ``map('red', 1, 'nir', 2)``).

    Returns:
        Single-band Float32 tile struct.
    """
    name_col = (
        f.lit(formula_name) if isinstance(formula_name, str) else _col(formula_name)
    )
    return _index_udf(_col(tile), name_col, _col(band_map))


# --- Tier 1d2: spectral index UDFs -----------------------------------------
@f.udf(_serde.TILE_SCHEMA)
def _ndvi_udf(tile, red_band, nir_band):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = indices.ndvi(ds, int(red_band), int(nir_band))
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _ndwi_udf(tile, green_idx, nir_idx):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = indices.ndwi(ds, int(green_idx), int(nir_idx))
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _nbr_udf(tile, nir_idx, swir_idx):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = indices.nbr(ds, int(nir_idx), int(swir_idx))
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _savi_udf(tile, red_idx, nir_idx, l_val):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = indices.savi(ds, int(red_idx), int(nir_idx), l=float(l_val))
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _evi_udf(tile, red_idx, nir_idx, blue_idx, l_val, c1, c2, g):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = indices.evi(
            ds,
            int(red_idx),
            int(nir_idx),
            int(blue_idx),
            l=float(l_val),
            c1=float(c1),
            c2=float(c2),
            g=float(g),
        )
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


def rst_ndvi(tile: ColLike, red_band: ColLike, nir_band: ColLike) -> Column:
    """Compute NDVI = (NIR - Red) / (NIR + Red); single-band Float32 tile."""
    return _ndvi_udf(_col(tile), _col(red_band), _col(nir_band))


def rst_ndwi(tile: ColLike, green_idx: ColLike, nir_idx: ColLike) -> Column:
    """Compute NDWI = (Green - NIR) / (Green + NIR); single-band Float32 tile."""
    return _ndwi_udf(_col(tile), _col(green_idx), _col(nir_idx))


def rst_nbr(tile: ColLike, nir_idx: ColLike, swir_idx: ColLike) -> Column:
    """Compute NBR = (NIR - SWIR) / (NIR + SWIR); single-band Float32 tile."""
    return _nbr_udf(_col(tile), _col(nir_idx), _col(swir_idx))


def rst_savi(
    tile: ColLike, red_idx: ColLike, nir_idx: ColLike, l: ColLike = 0.5  # noqa: E741
) -> Column:
    """Compute SAVI = (NIR - Red) / (NIR + Red + L) * (1 + L); single-band Float32 tile."""
    l_col = f.lit(l) if isinstance(l, (int, float)) else _col(l)
    return _savi_udf(_col(tile), _col(red_idx), _col(nir_idx), l_col)


def rst_evi(  # noqa: E741
    tile: ColLike,
    red_idx: ColLike,
    nir_idx: ColLike,
    blue_idx: ColLike,
    l: ColLike = 1.0,
    c1: ColLike = 6.0,
    c2: ColLike = 7.5,
    g: ColLike = 2.5,
) -> Column:
    """Compute EVI = G * (NIR - Red) / (NIR + C1*Red - C2*Blue + L); single-band Float32 tile."""
    l_col = f.lit(l) if isinstance(l, (int, float)) else _col(l)
    c1_col = f.lit(c1) if isinstance(c1, (int, float)) else _col(c1)
    c2_col = f.lit(c2) if isinstance(c2, (int, float)) else _col(c2)
    g_col = f.lit(g) if isinstance(g, (int, float)) else _col(g)
    return _evi_udf(
        _col(tile),
        _col(red_idx),
        _col(nir_idx),
        _col(blue_idx),
        l_col,
        c1_col,
        c2_col,
        g_col,
    )


# --- Tier 1e: constructor + fill UDFs (vector bridge) -----------------------
@f.udf(_serde.TILE_SCHEMA)
def _rasterize_udf(geom_wkb, value, xmin, ymin, xmax, ymax, width_px, height_px, srid):
    if geom_wkb is None:
        return None
    from databricks.labs.gbx._geom import geom_to_wkb
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    new_bytes = features.rasterize_geom(
        geom_to_wkb(geom_wkb), value, xmin, ymin, xmax, ymax, width_px, height_px, srid
    )
    return _serde.build_tile(new_bytes, "GTiff", 0)


def rst_rasterize(
    geom_wkb: ColLike,
    value: ColLike,
    xmin: ColLike,
    ymin: ColLike,
    xmax: ColLike,
    ymax: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    srid: ColLike,
) -> Column:
    """Burn a geometry (WKB, EWKB, WKT, or EWKT) into a new raster tile at the given extent/size/SRID."""
    return _rasterize_udf(
        _col(geom_wkb),
        _col(value),
        _col(xmin),
        _col(ymin),
        _col(xmax),
        _col(ymax),
        _col(width_px),
        _col(height_px),
        _col(srid),
    )


@f.udf(_serde.TILE_SCHEMA)
def _fillnodata_udf(tile, max_search_dist, smoothing_iter):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = features.fill_nodata(ds, max_search_dist, smoothing_iter)
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


def rst_fillnodata(
    tile: ColLike,
    max_search_dist: ColLike = None,
    smoothing_iter: ColLike = None,
) -> Column:
    """Interpolate across NoData gaps in the raster."""
    msd = f.lit(None) if max_search_dist is None else _col(max_search_dist)
    smi = f.lit(None) if smoothing_iter is None else _col(smoothing_iter)
    return _fillnodata_udf(_col(tile), msd, smi)


# --- Tier 1e3: TIN / IDW constructors (point-array -> tile) -----------------
@f.udf(_serde.TILE_SCHEMA)
def _gridfrompoints_udf(
    points,
    values,
    xmin,
    ymin,
    xmax,
    ymax,
    width_px,
    height_px,
    srid,
    power=None,
    max_pts=None,
):
    if points is None or values is None:
        return None
    from databricks.labs.gbx.pyrx import _env
    from databricks.labs.gbx.pyrx.core.tin import _parse_geom_elem

    _env.configure_gdal_env()
    # Decode points and values together so a null/empty point drops its paired
    # value too, keeping the two arrays parallel for the IDW solver. Each point
    # may be WKB/EWKB/WKT/EWKT (routed through the shared decoder).
    xy = []
    vals = []
    for raw, v in zip(points, values):
        if v is None:
            continue
        g = _parse_geom_elem(raw)
        if g is None or g.is_empty:
            continue
        xy.append((g.x, g.y))
        vals.append(float(v))
    new_bytes = tin_core.idw_grid(
        xy,
        vals,
        xmin,
        ymin,
        xmax,
        ymax,
        int(width_px),
        int(height_px),
        int(srid),
        power=2.0 if power is None else float(power),
        max_pts=12 if max_pts is None else int(max_pts),
    )
    return _serde.build_tile(new_bytes, "GTiff", 0)


def rst_gridfrompoints(
    points: ColLike,
    values: ColLike,
    xmin: ColLike,
    ymin: ColLike,
    xmax: ColLike,
    ymax: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    srid: ColLike,
    power: ColLike = 2.0,
    max_pts: ColLike = 12,
) -> Column:
    """Inverse-distance-weighted (IDW) grid from an ARRAY of POINT WKB + values.

    ``points`` is ARRAY<BINARY> (WKB points), ``values`` is the parallel
    ARRAY<DOUBLE>. For each output cell center the value is the inverse-distance
    weighted mean of the nearest ``max_pts`` points (weight = 1/distance**power);
    a point coincident with a cell center returns that value. Output is a
    single-band Float64 tile over ``[xmin,ymin,xmax,ymax]`` at
    ``width_px x height_px`` in EPSG:``srid``; NoData = -9999.0.

    Args:
        points:    ARRAY<BINARY> of WKB POINT geometries.
        values:    ARRAY<DOUBLE> parallel to ``points``.
        xmin..ymax: Output extent in CRS units.
        width_px, height_px: Output raster size in pixels.
        srid:      EPSG code for the output CRS.
        power:     IDW exponent (default 2.0).
        max_pts:   Max neighbours per cell (default 12).

    Returns:
        Single-band Float64 tile struct.
    """
    p = f.lit(power) if isinstance(power, (int, float)) else _col(power)
    m = f.lit(max_pts) if isinstance(max_pts, (int, float)) else _col(max_pts)
    return _gridfrompoints_udf(
        _col(points),
        _col(values),
        _col(xmin),
        _col(ymin),
        _col(xmax),
        _col(ymax),
        _col(width_px),
        _col(height_px),
        _col(srid),
        p,
        m,
    )


@f.udf(_serde.TILE_SCHEMA)
def _dtmfromgeoms_udf(
    points,
    breaklines,
    merge_tolerance,
    snap_tolerance,
    xmin,
    ymin,
    xmax,
    ymax,
    width_px,
    height_px,
    srid,
    no_data=None,
):
    if points is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    pts_xyz = tin_core.points_xyz_from_wkb(points)
    # breaklines may be WKB/EWKB/WKT/EWKT; pass through untouched so
    # delaunay_dtm decodes each element via the shared geom parser.
    bl = [b for b in breaklines if b is not None] if breaklines else None
    new_bytes = tin_core.delaunay_dtm(
        pts_xyz,
        bl,
        xmin,
        ymin,
        xmax,
        ymax,
        int(width_px),
        int(height_px),
        int(srid),
        no_data=-9999.0 if no_data is None else float(no_data),
    )
    return _serde.build_tile(new_bytes, "GTiff", 0)


def rst_dtmfromgeoms(
    points: ColLike,
    breaklines: ColLike,
    merge_tolerance: ColLike,
    snap_tolerance: ColLike,
    xmin: ColLike,
    ymin: ColLike,
    xmax: ColLike,
    ymax: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    srid: ColLike,
    no_data: ColLike = -9999.0,
) -> Column:
    """Delaunay-TIN DTM from Z-valued POINT WKB (+ optional breaklines).

    ``points`` is ARRAY<BINARY> of WKB POINTs WITH Z; ``breaklines`` is
    ARRAY<BINARY> of WKB linestrings (may be null/empty). A Delaunay
    triangulation of the points' (x, y) is built and Z is barycentrically
    interpolated at each output cell center. Cells outside the convex hull
    become ``no_data``. Output is a single-band Float64 tile over the extent at
    ``width_px x height_px`` in EPSG:``srid``.

    PARITY DIVERGENCE: the lightweight tier performs an UNCONSTRAINED Delaunay
    interpolation. ``breaklines`` are accepted but NOT enforced as hard edges
    (their vertices are folded in as extra triangulation points only), and
    ``merge_tolerance`` / ``snap_tolerance`` are accepted for signature parity
    but have no effect.

    Args:
        points:          ARRAY<BINARY> of WKB POINT-with-Z geometries.
        breaklines:      ARRAY<BINARY> of WKB linestrings (accepted, not enforced).
        merge_tolerance: Accepted for parity; not applied.
        snap_tolerance:  Accepted for parity; not applied.
        xmin..ymax:      Output extent in CRS units.
        width_px, height_px: Output raster size in pixels.
        srid:            EPSG code for the output CRS.
        no_data:         NoData sentinel (default -9999.0).

    Returns:
        Single-band Float64 tile struct.
    """
    nd = f.lit(no_data) if isinstance(no_data, (int, float)) else _col(no_data)
    return _dtmfromgeoms_udf(
        _col(points),
        _col(breaklines),
        _col(merge_tolerance),
        _col(snap_tolerance),
        _col(xmin),
        _col(ymin),
        _col(xmax),
        _col(ymax),
        _col(width_px),
        _col(height_px),
        _col(srid),
        nd,
    )


_POLYGONIZE_ROW_SCHEMA = StructType(
    [
        StructField("geom_wkb", BinaryType(), nullable=False),
        StructField("value", DoubleType(), nullable=False),
    ]
)


@udtf(returnType=_POLYGONIZE_ROW_SCHEMA)
class _RstPolygonizeUDTF:
    """Streaming UDTF: yield one (geom_wkb, value) row per contiguous value region.

    Uses rasterio.features.shapes as a lazy generator — never buffers the full
    polygon list (unbounded fan-out OOM guard).
    """

    def eval(self, tile, band, connectedness):
        if tile is None or tile["raster"] is None:
            return
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        with _serde.open_tile(bytes(tile["raster"])) as ds:
            for g, v in features.polygonize(ds, int(band), int(connectedness)):
                yield (g, v)


def rst_polygonize(
    tile: ColLike, band: ColLike = 1, connectedness: ColLike = 4
) -> None:
    """Extract vector polygons from a raster's contiguous equal-value regions.

    Light tier is a Python UDTF — invoke as a SQL LATERAL table function::

        SELECT t.geom_wkb, t.value
        FROM <df>, LATERAL gbx_rst_polygonize(tile, band, connectedness) t

    Returns one row per contiguous equal-value region; NoData pixels excluded.
    Each row: geom_wkb BINARY (WKB geometry), value DOUBLE (pixel value).
    """
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, LATERAL gbx_rst_polygonize(tile, band, connectedness) t"
    )


# --- Tier 1e2: tiling UDTFs (separatebands, retile, tooverlappingtiles) -----
# All four fan out to ARRAY<tile> in the heavyweight tier; the light tier
# streams one tile struct per row via UDTFs (eval yields each tile dict
# incrementally from the iter_* cores — never buffers the full list).
# Each UDTF row IS the tile struct (TILE_SCHEMA: cellid, raster, metadata).


@udtf(returnType=_serde.TILE_SCHEMA)
class _RstSeparateBandsUDTF:
    """Streaming UDTF: yield one single-band tile struct per band."""

    def eval(self, tile):
        if tile is None or tile["raster"] is None:
            return
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        with _serde.open_tile(bytes(tile["raster"])) as ds:
            for i, b in enumerate(tiling.iter_separate_bands(ds)):
                yield _serde.build_tile(b, "GTiff", i)


@udtf(returnType=_serde.TILE_SCHEMA)
class _RstRetileUDTF:
    """Streaming UDTF: yield one sub-tile struct per non-overlapping window."""

    def eval(self, tile, tile_width, tile_height):
        if tile is None or tile["raster"] is None:
            return
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        with _serde.open_tile(bytes(tile["raster"])) as ds:
            for i, b in enumerate(
                tiling.iter_retile(ds, int(tile_width), int(tile_height))
            ):
                yield _serde.build_tile(b, "GTiff", i)


@udtf(returnType=_serde.TILE_SCHEMA)
class _RstToOverlappingTilesUDTF:
    """Streaming UDTF: yield one sub-tile struct per overlapping window."""

    def eval(self, tile, tile_width, tile_height, overlap):
        if tile is None or tile["raster"] is None:
            return
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        with _serde.open_tile(bytes(tile["raster"])) as ds:
            for i, b in enumerate(
                tiling.iter_to_overlapping_tiles(
                    ds, int(tile_width), int(tile_height), int(overlap)
                )
            ):
                yield _serde.build_tile(b, "GTiff", i)


@udtf(returnType=_serde.TILE_SCHEMA)
class _RstMakeTilesUDTF:
    """Streaming UDTF: yield one sub-tile struct per power-of-4 split tile."""

    def eval(self, tile, size_in_mb):
        if tile is None or tile["raster"] is None:
            return
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        raster = bytes(tile["raster"])
        with _serde.open_tile(raster) as ds:
            # Pass the encoded byte length so the power-of-4 split count matches
            # heavy BalancedSubdivision (which keys on GDAL's in-memory file size).
            for i, b in enumerate(
                tiling.iter_make_tiles(ds, float(size_in_mb), size_bytes=len(raster))
            ):
                yield _serde.build_tile(b, "GTiff", i)


@udtf(returnType=_serde.TILE_SCHEMA)
class _RstH3TessellateUDTF:
    """Streaming UDTF: yield one clipped tile struct per overlapping H3 cell."""

    def eval(self, tile, resolution, mode=None):
        if tile is None or tile["raster"] is None or resolution is None:
            return
        effective_mode = mode if mode is not None else "covering"
        if effective_mode not in {"covering", "centroid"}:
            raise ValueError(
                f"rst_h3_tessellate: mode must be one of covering, centroid; "
                f"got '{effective_mode}'"
            )
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        with _serde.open_tile(bytes(tile["raster"])) as ds:
            for cellid, raster in tessellate_core.iter_tessellate_h3(
                ds, int(resolution), mode=effective_mode
            ):
                if raster is None:  # defensive: never emit a null-raster tile row
                    continue
                yield _serde.build_tile(raster, "GTiff", cellid)


def rst_separatebands(tile: ColLike) -> None:
    """Split a multi-band tile into single-band tiles (one row per band).

    Light tier is a Python UDTF — invoke as a SQL LATERAL table function::

        SELECT t.* FROM <df>, LATERAL gbx_rst_separatebands(tile) t

    Each output row is a tile struct carrying the same georeferencing and CRS
    as the input; one row per band.
    """
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, LATERAL gbx_rst_separatebands(tile) t"
    )


def rst_retile(tile: ColLike, tile_width: ColLike, tile_height: ColLike) -> None:
    """Partition a tile into non-overlapping sub-tiles of the given pixel size.

    Edge tiles are narrower/shorter when the raster dimensions are not exact
    multiples of tile_width/tile_height. Each output tile carries the correct
    windowed transform and CRS.

    Light tier is a Python UDTF — invoke as a SQL LATERAL table function::

        SELECT t.* FROM <df>, LATERAL gbx_rst_retile(tile, tile_width, tile_height) t

    Each output row is a tile struct; one row per sub-tile.
    """
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, LATERAL gbx_rst_retile(tile, tile_width, tile_height) t"
    )


def rst_tooverlappingtiles(
    tile: ColLike,
    tile_width: ColLike,
    tile_height: ColLike,
    overlap: ColLike,
) -> None:
    """Partition a tile into overlapping sub-tiles.

    Each tile is tile_width x tile_height pixels. *overlap* is a **percentage**
    of the tile size: the per-edge overlap is ``ceil(tile_width * overlap / 100)``
    pixels and the stride is ``tile_width - overlap_px`` (likewise for height).
    Edge tiles are clamped to the raster boundary.

    Light tier is a Python UDTF — invoke as a SQL LATERAL table function::

        SELECT t.* FROM <df>,
        LATERAL gbx_rst_tooverlappingtiles(tile, tile_width, tile_height, overlap) t

    Each output row is a tile struct; one row per sub-tile.
    """
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, "
        "LATERAL gbx_rst_tooverlappingtiles(tile, tile_width, tile_height, overlap) t"
    )


def rst_h3_tessellate(
    tile: ColLike, resolution: ColLike, mode: ColLike = "covering"
) -> None:
    """Tessellate a raster into H3 cells (mirrors ``gbx_rst_h3_tessellate``).

    For every H3 cell overlapping the raster's extent at *resolution*, the
    raster is clipped to that cell's hexagon and one tile is produced, carrying
    the H3 cell id as its ``cellid``. A cell is skipped only when its hexagon
    does not geometrically overlap the raster; a cell that overlaps but clips to
    entirely NoData is still emitted, and its value reducers
    (``gbx_rst_max``/``min``/``avg``/``median``) return SQL ``NULL`` for it.

    Light tier is a Python UDTF — invoke as a SQL LATERAL table function::

        SELECT t.* FROM <df>, LATERAL gbx_rst_h3_tessellate(tile, resolution) t
        SELECT t.* FROM <df>, LATERAL gbx_rst_h3_tessellate(tile, resolution, 'centroid') t

    Each output row is a tile struct; one row per overlapping H3 cell.

    Args:
        tile:       Tile struct column.
        resolution: H3 resolution in ``[0, 15]``.
        mode:       Tessellation mode: ``"covering"`` (default) — each H3 cell
                    that overlaps the raster extent is clipped to its hexagon
                    boundary; ``"centroid"`` — each valid pixel is assigned to
                    exactly one cell by its centroid (strict partition, no
                    overlap).
    """
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, LATERAL gbx_rst_h3_tessellate(tile, resolution) t"
    )


@udtf(returnType=_serde.TILE_SCHEMA)
class _RstQuadbinTessellateUDTF:
    """Streaming UDTF: yield one clipped tile struct per overlapping quadbin cell."""

    def eval(self, tile, resolution, mode=None):
        if tile is None or tile["raster"] is None or resolution is None:
            return
        effective_mode = mode if mode is not None else "covering"
        if effective_mode not in {"covering", "centroid"}:
            raise ValueError(
                f"rst_quadbin_tessellate: mode must be one of covering, centroid; "
                f"got '{effective_mode}'"
            )
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        with _serde.open_tile(bytes(tile["raster"])) as ds:
            for cellid, raster in tessellate_core.iter_tessellate_quadbin(
                ds, int(resolution), mode=effective_mode
            ):
                if raster is None:  # defensive: never emit a null-raster tile row
                    continue
                yield _serde.build_tile(raster, "GTiff", cellid)


def rst_quadbin_tessellate(
    tile: ColLike, resolution: ColLike, mode: ColLike = "covering"
) -> None:
    """Tessellate a raster into quadbin cells (mirrors ``gbx_rst_quadbin_tessellate``).

    For every quadbin cell overlapping the raster's extent at *resolution*, the
    raster is clipped to that cell's bounding-box polygon and one tile is
    produced, carrying the quadbin cell id as its ``cellid``. A cell is skipped
    only when its bbox does not geometrically overlap the raster; a cell that
    overlaps but clips to entirely NoData is still emitted, and its value
    reducers return SQL ``NULL`` for it.

    Light tier is a Python UDTF — invoke as a SQL LATERAL table function::

        SELECT t.* FROM <df>, LATERAL gbx_rst_quadbin_tessellate(tile, resolution) t
        SELECT t.* FROM <df>, LATERAL gbx_rst_quadbin_tessellate(tile, resolution, 'centroid') t

    Each output row is a tile struct; one row per overlapping quadbin cell.

    Args:
        tile:       Tile struct column.
        resolution: Quadbin resolution in ``[0, 20]`` (polyfill limit).
        mode:       Tessellation mode: ``"covering"`` (default) — each quadbin
                    cell overlapping the raster extent is clipped to its bbox;
                    ``"centroid"`` — each valid pixel is assigned to exactly one
                    cell by its centroid (strict partition, no overlap).
    """
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, LATERAL gbx_rst_quadbin_tessellate(tile, resolution) t"
    )


def rst_maketiles(tile: ColLike, size_in_mb: ColLike) -> None:
    """Split a raster into tiles of approximately size_in_mb each (one row per tile).

    Quad-splits the raster into a power-of-4 grid (1, 4, 16, ... tiles) until
    each tile's encoded size fits within the target MB budget, then partitions
    it into non-overlapping sub-tiles. Each output tile carries the correct
    windowed transform and CRS.

    Light tier is a Python UDTF — invoke as a SQL LATERAL table function::

        SELECT t.* FROM <df>, LATERAL gbx_rst_maketiles(tile, size_in_mb) t

    Each output row is a tile struct; one row per sub-tile.
    """
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, LATERAL gbx_rst_maketiles(tile, size_in_mb) t"
    )


# --- Tier 1f: terrain UDFs (slope, aspect, hillshade) ----------------------
@f.udf(_serde.TILE_SCHEMA)
def _slope_udf(tile, unit, xscale, yscale):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    xs = None if xscale is None else float(xscale)
    ys = None if yscale is None else float(yscale)
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = terrain.slope(ds, unit=str(unit), xscale=xs, yscale=ys)
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _aspect_udf(tile, trigonometric, zero_for_flat):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = terrain.aspect(
            ds,
            trigonometric=bool(trigonometric),
            zero_for_flat=bool(zero_for_flat),
        )
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _hillshade_udf(tile, azimuth, altitude, z_factor, xscale, yscale):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    xs = None if xscale is None else float(xscale)
    ys = None if yscale is None else float(yscale)
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = terrain.hillshade(
            ds,
            azimuth=float(azimuth),
            altitude=float(altitude),
            z_factor=float(z_factor),
            xscale=xs,
            yscale=ys,
        )
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


def rst_slope(
    tile: ColLike,
    unit: ColLike = "degrees",
    xscale: ColLike = None,
    yscale: ColLike = None,
) -> Column:
    """Compute terrain slope from a single-band DEM tile (Horn's 3x3 method).

    By default the horizontal scale is auto-derived from the raster CRS
    (geographic grids use a latitude-based degree->metre ratio, projected
    grids use linear units). Pass both ``xscale`` and ``yscale`` (vertical
    units per horizontal unit) to override the auto scale.

    Args:
        tile:    Tile struct column containing a single-band DEM raster.
        unit:    ``"degrees"`` (default) or ``"percent"``.
        xscale:  Optional explicit horizontal scale override (with ``yscale``).
        yscale:  Optional explicit vertical scale override (with ``xscale``).

    Returns:
        Single-band Float32 tile; nodata = -9999.
    """
    unit_col = f.lit(unit) if isinstance(unit, str) else _col(unit)
    xs_col = f.lit(None) if xscale is None else _col(xscale)
    ys_col = f.lit(None) if yscale is None else _col(yscale)
    return _slope_udf(_col(tile), unit_col, xs_col, ys_col)


def rst_aspect(
    tile: ColLike,
    trigonometric: ColLike = False,
    zero_for_flat: ColLike = False,
) -> Column:
    """Compute terrain aspect from a single-band DEM tile (Horn's 3x3 method).

    Default output is compass degrees: 0 = North, increasing clockwise.
    Flat cells are -9999 unless zero_for_flat is True.

    Aspect is a pure direction, so it is scale-invariant: ``gdaldem aspect``
    does not apply the horizontal CRS scale, and neither does this function (no
    ``xscale`` / ``yscale``). Only slope and hillshade, whose magnitude depends
    on the gradient, are CRS-scale-aware.

    Args:
        tile:           Tile struct column containing a single-band DEM raster.
        trigonometric:  Return math-convention (CCW from east) instead of compass.
        zero_for_flat:  Return 0 for flat cells instead of -9999.

    Returns:
        Single-band Float32 tile; nodata = -9999.
    """
    trig_col = (
        f.lit(trigonometric) if isinstance(trigonometric, bool) else _col(trigonometric)
    )
    zff_col = (
        f.lit(zero_for_flat) if isinstance(zero_for_flat, bool) else _col(zero_for_flat)
    )
    return _aspect_udf(_col(tile), trig_col, zff_col)


def rst_hillshade(
    tile: ColLike,
    azimuth: ColLike = 315.0,
    altitude: ColLike = 45.0,
    z_factor: ColLike = 1.0,
    xscale: ColLike = None,
    yscale: ColLike = None,
) -> Column:
    """Compute hillshade from a single-band DEM tile (Horn's 3x3 method).

    Horizontal scale is auto-derived from the CRS by default; pass both
    ``xscale`` and ``yscale`` to override.

    Args:
        tile:      Tile struct column containing a single-band DEM raster.
        azimuth:   Sun azimuth in degrees (default 315 = NW).
        altitude:  Sun elevation above horizon in degrees (default 45).
        z_factor:  Vertical exaggeration applied to gradients (default 1.0).
        xscale:    Optional explicit horizontal scale override (with ``yscale``).
        yscale:    Optional explicit vertical scale override (with ``xscale``).

    Returns:
        Single-band Byte (uint8) tile; values 0..255.
    """
    az_col = f.lit(azimuth) if isinstance(azimuth, (int, float)) else _col(azimuth)
    alt_col = f.lit(altitude) if isinstance(altitude, (int, float)) else _col(altitude)
    zf_col = f.lit(z_factor) if isinstance(z_factor, (int, float)) else _col(z_factor)
    xs_col = f.lit(None) if xscale is None else _col(xscale)
    ys_col = f.lit(None) if yscale is None else _col(yscale)
    return _hillshade_udf(_col(tile), az_col, alt_col, zf_col, xs_col, ys_col)


# --- Tier 1g: terrain ruggedness UDFs (tri, tpi, roughness) -----------------
@f.udf(_serde.TILE_SCHEMA)
def _tri_udf(tile):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = terrain.tri(ds)
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _tpi_udf(tile):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = terrain.tpi(ds)
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


@f.udf(_serde.TILE_SCHEMA)
def _roughness_udf(tile):
    if tile is None or tile["raster"] is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = terrain.roughness(ds)
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


def rst_tri(tile: ColLike) -> Column:
    """Compute Terrain Ruggedness Index (TRI) from a single-band DEM tile.

    TRI = mean of the absolute differences between the center cell and each of
    its 8 neighbours (Wilson 2007).  Flat terrain yields 0.

    Returns:
        Single-band Float32 tile; nodata = -9999.
    """
    return _tri_udf(_col(tile))


def rst_tpi(tile: ColLike) -> Column:
    """Compute Topographic Position Index (TPI) from a single-band DEM tile.

    TPI = center - mean(8 neighbours).  Positive = local high; negative = local
    low; flat terrain yields 0.

    Returns:
        Single-band Float32 tile; nodata = -9999.
    """
    return _tpi_udf(_col(tile))


def rst_roughness(tile: ColLike) -> Column:
    """Compute terrain roughness from a single-band DEM tile.

    Roughness = max(3x3 window) - min(3x3 window).  Flat terrain yields 0.

    Returns:
        Single-band Float32 tile; nodata = -9999.
    """
    return _roughness_udf(_col(tile))


@f.udf(_serde.TILE_SCHEMA)
def _color_relief_udf(tile, color_table_path):
    if tile is None or tile["raster"] is None or color_table_path is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = terrain.color_relief(ds, str(color_table_path))
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


def rst_color_relief(tile: ColLike, color_table_path: ColLike) -> Column:
    """Map a single-band DEM through a gdaldem color table to an RGB(A) Byte tile.

    Reads a gdaldem-style color file (elevation R G B [A] per line; ``nv`` for
    NoData pixels; ``<n>%`` for percentage of the band range).  Applies linear
    interpolation per channel.

    Args:
        tile:             Tile struct column containing a single-band DEM raster.
        color_table_path: Column or string path to a gdaldem color file.

    Returns:
        3-band (RGB) or 4-band (RGBA) Byte tile.
    """
    ctp = (
        f.lit(color_table_path)
        if isinstance(color_table_path, str)
        else _col(color_table_path)
    )
    return _color_relief_udf(_col(tile), ctp)


# --- Tier 0: accessors ------------------------------------------------------
def rst_width(tile: ColLike) -> Column:
    return _u_width(_raster_field(_col(tile)))


def rst_height(tile: ColLike) -> Column:
    return _u_height(_raster_field(_col(tile)))


def rst_numbands(tile: ColLike) -> Column:
    return _u_numbands(_raster_field(_col(tile)))


def rst_srid(tile: ColLike) -> Column:
    return _u_srid(_raster_field(_col(tile)))


def rst_pixelwidth(tile: ColLike) -> Column:
    return _u_pixelwidth(_raster_field(_col(tile)))


def rst_pixelheight(tile: ColLike) -> Column:
    return _u_pixelheight(_raster_field(_col(tile)))


def rst_upperleftx(tile: ColLike) -> Column:
    return _u_upperleftx(_raster_field(_col(tile)))


def rst_upperlefty(tile: ColLike) -> Column:
    return _u_upperlefty(_raster_field(_col(tile)))


def rst_boundingbox(tile: ColLike) -> Column:
    return _u_boundingbox(_raster_field(_col(tile)))


def rst_metadata(tile: ColLike) -> Column:
    return _metadata_udf(_raster_field(_col(tile)))


def rst_scalex(tile: ColLike) -> Column:
    return _u_scalex(_raster_field(_col(tile)))


def rst_scaley(tile: ColLike) -> Column:
    return _u_scaley(_raster_field(_col(tile)))


def rst_isempty(tile: ColLike) -> Column:
    """True if the raster has no size or every band is entirely NoData; BOOLEAN."""
    return _u_isempty(_raster_field(_col(tile)))


def rst_type(tile: ColLike) -> Column:
    """Return the GDAL data-type name per band (e.g. ['Float32', 'Float32'])."""
    return _u_type(_raster_field(_col(tile)))


def rst_getnodata(tile: ColLike) -> Column:
    """Return the NoData value per band as an array of doubles, or null if not set."""
    return _u_getnodata(_raster_field(_col(tile)))


# --- Tier 1: coordinate transforms -----------------------------------------
def rst_rastertoworldcoordx(
    tile: ColLike, pixel_x: ColLike, pixel_y: ColLike
) -> Column:
    return _u_r2w_x(_raster_field(_col(tile)), _col(pixel_x), _col(pixel_y))


def rst_rastertoworldcoordy(
    tile: ColLike, pixel_x: ColLike, pixel_y: ColLike
) -> Column:
    return _u_r2w_y(_raster_field(_col(tile)), _col(pixel_x), _col(pixel_y))


def rst_worldtorastercoordx(
    tile: ColLike, world_x: ColLike, world_y: ColLike
) -> Column:
    return _u_w2r_x(_raster_field(_col(tile)), _col(world_x), _col(world_y))


def rst_worldtorastercoordy(
    tile: ColLike, world_x: ColLike, world_y: ColLike
) -> Column:
    return _u_w2r_y(_raster_field(_col(tile)), _col(world_x), _col(world_y))


def rst_rastertoworldcoord(tile: ColLike, x: ColLike, y: ColLike) -> Column:
    """World coordinate of pixel (x=col, y=row) as STRUCT<x: DOUBLE, y: DOUBLE>."""
    return _rastertoworldcoord_udf(_col(tile), _col(x), _col(y))


def rst_worldtorastercoord(tile: ColLike, x: ColLike, y: ColLike) -> Column:
    """Pixel (col, row) containing world (x, y) as STRUCT<x: INT, y: INT>."""
    return _worldtorastercoord_udf(_col(tile), _col(x), _col(y))


# --- Group 1: per-band statistics & accessors -------------------------------
def rst_avg(tile: ColLike) -> Column:
    """Per-band mean of valid (non-NoData) pixels; ARRAY<DOUBLE>.

    Empty / all-invalid bands return NULL.
    """
    return _u_avg(_raster_field(_col(tile)))


def rst_min(tile: ColLike) -> Column:
    """Per-band minimum of valid (non-NoData) pixels; ARRAY<DOUBLE>.

    Empty / all-invalid bands return NULL.
    """
    return _u_min(_raster_field(_col(tile)))


def rst_max(tile: ColLike) -> Column:
    """Per-band maximum of valid (non-NoData) pixels; ARRAY<DOUBLE>.

    Empty / all-invalid bands return NULL.
    """
    return _u_max(_raster_field(_col(tile)))


def rst_median(tile: ColLike) -> Column:
    """Per-band median of valid (non-NoData) pixels; ARRAY<DOUBLE>.

    Empty / all-invalid bands return NULL.
    """
    return _u_median(_raster_field(_col(tile)))


def rst_pixelcount(tile: ColLike) -> Column:
    """Per-band count of valid (non-NoData) pixels; ARRAY<LONG>.

    Empty / all-invalid bands return 0.
    """
    return _u_pixelcount(_raster_field(_col(tile)))


def rst_memsize(tile: ColLike) -> Column:
    """Serialized size of the raster in bytes (length of the raster buffer); LONG."""
    return _memsize_udf(_raster_field(_col(tile)))


def rst_rotation(tile: ColLike) -> Column:
    """Rotation angle = atan(skewY / scaleX) in radians; DOUBLE."""
    return _u_rotation(_raster_field(_col(tile)))


def rst_skewx(tile: ColLike) -> Column:
    """X skew of the geotransform (gt2); DOUBLE."""
    return _u_skewx(_raster_field(_col(tile)))


def rst_skewy(tile: ColLike) -> Column:
    """Y skew of the geotransform (gt4); DOUBLE."""
    return _u_skewy(_raster_field(_col(tile)))


def rst_format(tile: ColLike) -> Column:
    """GDAL driver short name of the raster (e.g. 'GTiff'); STRING."""
    return _u_format(_raster_field(_col(tile)))


def rst_georeference(tile: ColLike) -> Column:
    """Geotransform as MAP<STRING,DOUBLE>.

    Keys: upperLeftX, upperLeftY, scaleX, scaleY, skewX, skewY.
    """
    return _georeference_udf(_col(tile))


def rst_bandmetadata(tile: ColLike, band: ColLike) -> Column:
    """Metadata tags of the given 1-based band as MAP<STRING,STRING>."""
    return _bandmetadata_udf(_col(tile), _col(band))


def rst_subdatasets(tile: ColLike) -> Column:
    """Subdataset map as MAP<STRING,STRING>; empty for plain single-dataset rasters."""
    return _subdatasets_udf(_col(tile))


def rst_getsubdataset(tile: ColLike, name: ColLike) -> Column:
    """Extract the named subdataset as a new raster tile struct.

    Raises if no subdataset matches ``name`` (mirrors heavyweight).
    """
    nm = f.lit(name) if isinstance(name, str) else _col(name)
    return _getsubdataset_udf(_col(tile), nm)


def rst_summary(tile: ColLike) -> Column:
    """gdalinfo-style JSON summary string with per-band statistics; STRING.

    The JSON shape is GeoBrix-specific (driver, size, crs, geoTransform, bands
    with min/max/mean/stdDev), not a byte-for-byte ``gdalinfo -json`` match.
    """
    return _summary_udf(_col(tile))


def rst_histogram(
    tile: ColLike,
    n_buckets: ColLike = 256,
    min: ColLike = None,  # noqa: A002 - mirrors heavyweight arg name
    max: ColLike = None,  # noqa: A002
    include_nodata: ColLike = False,
) -> Column:
    """Per-band histogram as MAP<STRING, ARRAY<LONG>> keyed by ``band_<i>``.

    Args:
        tile:           Tile struct column.
        n_buckets:      Number of equal-width buckets across [min, max] (default 256).
        min:            Lower bound; defaults to the band's valid-pixel minimum.
        max:            Upper bound; defaults to the band's valid-pixel maximum.
        include_nodata: Include masked pixels in the binning (default False).

    Values outside [min, max] are dropped (no out-of-range bucket).
    """
    nb = f.lit(n_buckets) if isinstance(n_buckets, int) else _col(n_buckets)
    lo = f.lit(None) if min is None else _col(min)
    hi = f.lit(None) if max is None else _col(max)
    inc = (
        f.lit(include_nodata)
        if isinstance(include_nodata, bool)
        else _col(include_nodata)
    )
    return _histogram_udf(_col(tile), nb, lo, hi, inc)


# --- Tier 1d5: derived-band UDF ---------------------------------------------
@f.udf(_serde.TILE_SCHEMA)
def _derivedband_udf(tile, pyfunc, func_name):
    if tile is None or tile["raster"] is None or pyfunc is None or func_name is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        new_bytes = derivedband_core.derivedband(ds, str(pyfunc), str(func_name))
    return _serde.build_tile(new_bytes, "GTiff", tile["cellid"])


def rst_derivedband(tile_expr: ColLike, pyfunc: ColLike, func_name: ColLike) -> Column:
    """Apply a user-provided Python function to the raster's bands.

    pyfunc follows GDAL's VRT pixel-function signature::

        func(in_ar, out_ar, xoff, yoff, xsize, ysize,
             raster_xsize, raster_ysize, buf_radius, gt, **kwargs)

    where ``in_ar`` is a list of 2-D NumPy arrays (one per input band) and
    ``out_ar`` is a preallocated 2-D output array the function fills in-place
    (``out_ar[:] = ...``). This matches GDAL's Python pixel-function contract,
    so a pyfunc authored for the heavyweight ``rst_derivedband`` works here.

    SECURITY: pyfunc is executed in-process without sandboxing — pass only
    trusted (your own) code, the same trust model as any Spark UDF.

    Args:
        tile_expr: Tile struct column.
        pyfunc:    Python source code (string) defining the function.
        func_name: Name of the callable within ``pyfunc``.

    Returns:
        Single-band Float64 tile struct.
    """
    pf = f.lit(pyfunc) if isinstance(pyfunc, str) else _col(pyfunc)
    fn = f.lit(func_name) if isinstance(func_name, str) else _col(func_name)
    return _derivedband_udf(_col(tile_expr), pf, fn)


# --- Tier 1h: web-mercator XYZ tiling UDFs ---------------------------------
@f.udf(BinaryType())
def _tilexyz_udf(tile, z, x, y, format, size, resampling, rescale=None):
    # Mirror heavyweight: rst_tilexyz NEVER returns null — a null/empty tile or
    # any hard failure yields a transparent PNG (slippy-map servers need a 200).
    sz = int(size) if size is not None else 256
    if tile is None or tile["raster"] is None:
        return xyz.transparent_png(sz)
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    fmt = str(format) if format is not None else "PNG"
    resamp = str(resampling) if resampling is not None else "bilinear"
    rsc = rescale if rescale is not None else "auto"
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        return xyz.render_tile(ds, int(z), int(x), int(y), fmt, sz, resamp, rescale=rsc)


_XYZPYRAMID_ROW_SCHEMA = StructType(
    [
        StructField("z", IntegerType(), False),
        StructField("x", IntegerType(), False),
        StructField("y", IntegerType(), False),
        StructField("bytes", BinaryType(), True),
    ]
)


@udtf(returnType=_XYZPYRAMID_ROW_SCHEMA)
class _RstXyzPyramidUDTF:
    """Streaming UDTF: yield one (z, x, y, bytes) row per intersecting XYZ tile.

    Uses ``xyz.iter_pyramid`` (a lazy generator over the zoom range) — never
    buffers the full pyramid (large-fan-out OOM guard). The zoom / render-arg /
    tile-count guards fire up front before any tile is rendered or yielded.
    """

    def eval(
        self, tile, min_z, max_z, format=None, size=None, resampling=None, rescale=None
    ):
        # Defaults make format/size/resampling/rescale optional in the SQL UDTF call
        # (gbx_rst_xyzpyramid(tile, min_z, max_z)). None maps to PNG/256/bilinear/auto.
        if tile is None or tile["raster"] is None:
            return
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        fmt = str(format) if format is not None else "PNG"
        sz = int(size) if size is not None else 256
        resamp = str(resampling) if resampling is not None else "bilinear"
        rsc = rescale if rescale is not None else "auto"
        with _serde.open_tile(bytes(tile["raster"])) as ds:
            for z, x, y, b in xyz.iter_pyramid(
                ds, int(min_z), int(max_z), fmt, sz, resamp, rsc
            ):
                yield (z, x, y, b)


def rst_tilexyz(
    tile: ColLike,
    z: ColLike,
    x: ColLike,
    y: ColLike,
    format: ColLike = "PNG",
    size: ColLike = 256,
    resampling: ColLike = "bilinear",
    rescale: ColLike = "auto",
) -> Column:
    """Render a single web-mercator XYZ slippy-map tile from the raster.

    Warps the source into a ``size`` x ``size`` raster covering exactly the
    EPSG:3857 bbox of (z, x, y) and encodes it to the requested image format.

    Args:
        tile:       Tile struct column.
        z, x, y:    Web-mercator tile coordinates (Y north-down slippy-map).
        format:     "PNG" (default), "JPEG", or "WEBP" (case-insensitive).
        size:       Output tile side in pixels, in (0, 4096]. Default 256.
        resampling: GDAL warp resampling name (near, bilinear (default), cubic,
                    cubicspline, lanczos, average, mode, max, min, med, q1, q3).
        rescale:    8-bit encoding contrast: "auto" (default) rescales non-8-bit
                    rasters by whole-dataset per-band min/max and passes uint8
                    through unchanged; "none" keeps the raw full-dtype-range
                    mapping. (An explicit (min, max) pair is supported by the
                    direct core/UDF API; see the Note below.)

    Note:
        The ``rescale`` Column wrapper supports the ``"auto"``/``"none"`` string
        modes and a Column expression; a numeric ``(min, max)`` tuple is
        supported only by the direct/core/UDF API, not the Column wrapper.

    Returns:
        BINARY image bytes. Out-of-extent / empty tiles (and any hard failure)
        return a transparent RGBA PNG of ``size`` x ``size`` — NEVER null — so
        slippy-map servers always get a 200-status body.
    """
    if isinstance(rescale, (tuple, list)):
        raise ValueError(
            "rst_tilexyz: an explicit (min, max) rescale tuple is not supported "
            "through the Column API; pass rescale='auto' or 'none' (or a Column), "
            "or use the core/UDF path for explicit bounds."
        )
    fmt = f.lit(format) if isinstance(format, str) else _col(format)
    sz = f.lit(size) if isinstance(size, int) else _col(size)
    resamp = f.lit(resampling) if isinstance(resampling, str) else _col(resampling)
    if isinstance(rescale, str):
        rsc = f.lit(rescale)
    else:
        rsc = _col(rescale)
    return _tilexyz_udf(_col(tile), _col(z), _col(x), _col(y), fmt, sz, resamp, rsc)


def rst_xyzpyramid(
    tile: ColLike,
    min_z: ColLike,
    max_z: ColLike,
    format: ColLike = "PNG",
    size: ColLike = 256,
    resampling: ColLike = "bilinear",
) -> None:
    """Render every web-mercator XYZ tile intersecting the raster across a zoom range.

    Computes the source extent in WGS84, enumerates intersecting (z, x, y) tiles
    for each zoom in [min_z, max_z] (WebMercatorQuad TMS, Y north-down), and
    renders each via the same path as :func:`rst_tilexyz`.

    Light tier is a Python UDTF — invoke as a SQL LATERAL table function::

        SELECT t.z, t.x, t.y, t.bytes
        FROM <df>, LATERAL gbx_rst_xyzpyramid(tile, min_z, max_z, format, size, resampling) t

    Each output row is ``struct(z INT, x INT, y INT, bytes BINARY)``, one per
    intersecting tile. Raises if the candidate tile-count across the range
    exceeds 1,000,000.

    Args:
        tile:       Tile struct column.
        min_z:      Minimum zoom (>= 0).
        max_z:      Maximum zoom (>= min_z, <= 20).
        format:     "PNG" (default), "JPEG", or "WEBP".
        size:       Output tile side in pixels, in (0, 4096]. Default 256.
        resampling: GDAL warp resampling name (default "bilinear").
        rescale:    8-bit encoding contrast: "auto" (default), "none", or a
                    (min, max) pair. See rst_tilexyz.
    """
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, "
        "LATERAL gbx_rst_xyzpyramid(tile, min_z, max_z, format, size, resampling) t"
    )


# --- Tier 1i: raster->grid aggregation UDTFs (h3 + quadbin) -----------------
# Streaming UDTFs: yield flat (band INT, cellID LONG, measure <T>) rows.
# band is 1-based (rasterio convention).  measure type matches Scala heavy:
#   h3 count  -> IntegerType  (INT)
#   quadbin count -> LongType (LONG)
#   avg/max/min/median -> DoubleType (DOUBLE)


def _grid_flat_schema(measure_type):
    return StructType(
        [
            StructField("band", IntegerType(), False),
            StructField("cellID", LongType(), True),
            StructField("measure", measure_type, True),
        ]
    )


_GRID_FLAT_DOUBLE_SCHEMA = _grid_flat_schema(DoubleType())
_GRID_FLAT_INT_SCHEMA = _grid_flat_schema(IntegerType())  # h3 count
_GRID_FLAT_LONG_SCHEMA = _grid_flat_schema(LongType())  # quadbin count


def _make_rastertogrid_udtf(grid, agg, flat_schema):
    @udtf(returnType=flat_schema)
    class _RasterToGridUDTF:
        def eval(self, tile, resolution):
            if tile is None or tile["raster"] is None:
                return
            from databricks.labs.gbx.pyrx import _env

            _env.configure_gdal_env()
            with _serde.open_tile(bytes(tile["raster"])) as ds:
                bands_data = gridagg.raster_to_grid(ds, int(resolution), grid, agg)
            # Yield flat rows (band, cellID, measure) — never buffer full nested list.
            for band_idx, cells in enumerate(bands_data, start=1):
                for cell in cells:
                    yield (band_idx, int(cell["cellID"]), cell["measure"])

    return _RasterToGridUDTF


_RstH3RasterToGridAvgUDTF = _make_rastertogrid_udtf(
    "h3", "avg", _GRID_FLAT_DOUBLE_SCHEMA
)
_RstH3RasterToGridCountUDTF = _make_rastertogrid_udtf(
    "h3", "count", _GRID_FLAT_INT_SCHEMA
)
_RstH3RasterToGridMaxUDTF = _make_rastertogrid_udtf(
    "h3", "max", _GRID_FLAT_DOUBLE_SCHEMA
)
_RstH3RasterToGridMinUDTF = _make_rastertogrid_udtf(
    "h3", "min", _GRID_FLAT_DOUBLE_SCHEMA
)
_RstH3RasterToGridMedianUDTF = _make_rastertogrid_udtf(
    "h3", "median", _GRID_FLAT_DOUBLE_SCHEMA
)
_RstQuadbinRasterToGridAvgUDTF = _make_rastertogrid_udtf(
    "quadbin", "avg", _GRID_FLAT_DOUBLE_SCHEMA
)
_RstQuadbinRasterToGridCountUDTF = _make_rastertogrid_udtf(
    "quadbin", "count", _GRID_FLAT_LONG_SCHEMA
)
_RstQuadbinRasterToGridMaxUDTF = _make_rastertogrid_udtf(
    "quadbin", "max", _GRID_FLAT_DOUBLE_SCHEMA
)
_RstQuadbinRasterToGridMinUDTF = _make_rastertogrid_udtf(
    "quadbin", "min", _GRID_FLAT_DOUBLE_SCHEMA
)
_RstQuadbinRasterToGridMedianUDTF = _make_rastertogrid_udtf(
    "quadbin", "median", _GRID_FLAT_DOUBLE_SCHEMA
)

_RASTERTOGRID_DOC = """{summary}

    Per band, every valid (non-NoData) pixel is mapped to a {grid} cell at the
    given ``resolution`` via its pixel-centroid world coordinate; the pixel
    values falling in each cell are reduced by {agg_desc}. The raster is
    interpreted as EPSG:4326 lon/lat (no reprojection -- reproject upstream with
    ``rst_transform`` if your source CRS differs).

    Light tier is a Python UDTF -- invoke as a SQL LATERAL table function::

        SELECT t.band, t.cellID, t.measure
        FROM <df>, LATERAL gbx_rst_{sql_name}(tile, resolution) t

    Each row: band INT (1-based), cellID LONG ({grid} cell id),
    measure {measure}.

    Args:
        tile:       Tile struct column.
        resolution: {grid} resolution ({res_range}).
    """


def rst_h3_rastertogridavg(tile: ColLike, resolution: ColLike) -> None:
    """Aggregate raster pixel values into H3 cells by mean, per band."""
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, LATERAL gbx_rst_h3_rastertogridavg(tile, resolution) t"
    )


def rst_h3_rastertogridcount(tile: ColLike, resolution: ColLike) -> None:
    """Count raster pixels falling in each H3 cell, per band."""
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, LATERAL gbx_rst_h3_rastertogridcount(tile, resolution) t"
    )


def rst_h3_rastertogridmax(tile: ColLike, resolution: ColLike) -> None:
    """Aggregate raster pixel values into H3 cells by maximum, per band."""
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, LATERAL gbx_rst_h3_rastertogridmax(tile, resolution) t"
    )


def rst_h3_rastertogridmin(tile: ColLike, resolution: ColLike) -> None:
    """Aggregate raster pixel values into H3 cells by minimum, per band."""
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, LATERAL gbx_rst_h3_rastertogridmin(tile, resolution) t"
    )


def rst_h3_rastertogridmedian(tile: ColLike, resolution: ColLike) -> None:
    """Aggregate raster pixel values into H3 cells by median, per band."""
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, LATERAL gbx_rst_h3_rastertogridmedian(tile, resolution) t"
    )


def rst_quadbin_rastertogridavg(tile: ColLike, resolution: ColLike) -> None:
    """Aggregate raster pixel values into quadbin cells by mean, per band."""
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, LATERAL gbx_rst_quadbin_rastertogridavg(tile, resolution) t"
    )


def rst_quadbin_rastertogridcount(tile: ColLike, resolution: ColLike) -> None:
    """Count raster pixels falling in each quadbin cell, per band."""
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, LATERAL gbx_rst_quadbin_rastertogridcount(tile, resolution) t"
    )


def rst_quadbin_rastertogridmax(tile: ColLike, resolution: ColLike) -> None:
    """Aggregate raster pixel values into quadbin cells by maximum, per band."""
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, LATERAL gbx_rst_quadbin_rastertogridmax(tile, resolution) t"
    )


def rst_quadbin_rastertogridmin(tile: ColLike, resolution: ColLike) -> None:
    """Aggregate raster pixel values into quadbin cells by minimum, per band."""
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, LATERAL gbx_rst_quadbin_rastertogridmin(tile, resolution) t"
    )


def rst_quadbin_rastertogridmedian(tile: ColLike, resolution: ColLike) -> None:
    """Aggregate raster pixel values into quadbin cells by median, per band."""
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, LATERAL gbx_rst_quadbin_rastertogridmedian(tile, resolution) t"
    )


rst_h3_rastertogridavg.__doc__ = _RASTERTOGRID_DOC.format(
    summary="Aggregate raster pixel values into H3 cells by mean, per band.",
    grid="H3",
    agg_desc="their mean (DOUBLE)",
    res_range="0..15",
    measure="DOUBLE",
    sql_name="h3_rastertogridavg",
)
rst_h3_rastertogridcount.__doc__ = _RASTERTOGRID_DOC.format(
    summary="Count raster pixels falling in each H3 cell, per band.",
    grid="H3",
    agg_desc="a pixel count (INTEGER)",
    res_range="0..15",
    measure="INTEGER",
    sql_name="h3_rastertogridcount",
)
rst_h3_rastertogridmax.__doc__ = _RASTERTOGRID_DOC.format(
    summary="Aggregate raster pixel values into H3 cells by maximum, per band.",
    grid="H3",
    agg_desc="their maximum (DOUBLE)",
    res_range="0..15",
    measure="DOUBLE",
    sql_name="h3_rastertogridmax",
)
rst_h3_rastertogridmin.__doc__ = _RASTERTOGRID_DOC.format(
    summary="Aggregate raster pixel values into H3 cells by minimum, per band.",
    grid="H3",
    agg_desc="their minimum (DOUBLE)",
    res_range="0..15",
    measure="DOUBLE",
    sql_name="h3_rastertogridmin",
)
rst_h3_rastertogridmedian.__doc__ = _RASTERTOGRID_DOC.format(
    summary="Aggregate raster pixel values into H3 cells by median, per band.",
    grid="H3",
    agg_desc="their median (DOUBLE; even counts average the two middle values)",
    res_range="0..15",
    measure="DOUBLE",
    sql_name="h3_rastertogridmedian",
)
rst_quadbin_rastertogridavg.__doc__ = _RASTERTOGRID_DOC.format(
    summary="Aggregate raster pixel values into quadbin cells by mean, per band.",
    grid="quadbin",
    agg_desc="their mean (DOUBLE)",
    res_range="0..20",
    measure="DOUBLE",
    sql_name="quadbin_rastertogridavg",
)
rst_quadbin_rastertogridcount.__doc__ = _RASTERTOGRID_DOC.format(
    summary="Count raster pixels falling in each quadbin cell, per band.",
    grid="quadbin",
    agg_desc="a pixel count (LONG)",
    res_range="0..20",
    measure="LONG",
    sql_name="quadbin_rastertogridcount",
)
rst_quadbin_rastertogridmax.__doc__ = _RASTERTOGRID_DOC.format(
    summary="Aggregate raster pixel values into quadbin cells by maximum, per band.",
    grid="quadbin",
    agg_desc="their maximum (DOUBLE)",
    res_range="0..20",
    measure="DOUBLE",
    sql_name="quadbin_rastertogridmax",
)
rst_quadbin_rastertogridmin.__doc__ = _RASTERTOGRID_DOC.format(
    summary="Aggregate raster pixel values into quadbin cells by minimum, per band.",
    grid="quadbin",
    agg_desc="their minimum (DOUBLE)",
    res_range="0..20",
    measure="DOUBLE",
    sql_name="quadbin_rastertogridmin",
)
rst_quadbin_rastertogridmedian.__doc__ = _RASTERTOGRID_DOC.format(
    summary="Aggregate raster pixel values into quadbin cells by median, per band.",
    grid="quadbin",
    agg_desc="their median (DOUBLE; even counts average the two middle values)",
    res_range="0..20",
    measure="DOUBLE",
    sql_name="quadbin_rastertogridmedian",
)


# ---------------------------------------------------------------------------
# Tier 2: grouped aggregators (rst_*_agg)
# ---------------------------------------------------------------------------
# Spark 4.0 forbids a Python aggregate from returning a StructType, but allows
# a grouped-agg pandas_udf to return BINARY and a scalar UDF to wrap that result
# inside .agg(). So each public aggregator COMPOSES a BINARY grouped-agg UDF
# with a scalar ``as_tile`` UDF, yielding a tile struct transparently while
# preserving the heavyweight call pattern: df.groupBy(k).agg(rx.rst_*_agg(...)).
#
# The grouped-agg UDFs accept the tile STRUCT column directly (Arrow delivers a
# struct column to the pandas_udf as a Series of dict-like rows; we extract each
# member's ``raster`` bytes via row["raster"]). VERIFIED to work for both the
# Python .agg() path and SQL GROUP BY — no raster-bytes fallback needed.


def _tile_raster_bytes(row):
    """Extract raster bytes from a tile-struct row delivered by Arrow.

    Arrow hands a StructType column to a pandas_udf as a Series whose elements
    are dict-like (mapping field name -> value). Returns None for null rows.
    """
    if row is None:
        return None
    raster = row["raster"]
    return None if raster is None else bytes(raster)


# --- scalar as_tile UDFs: wrap an aggregated BINARY result into a tile struct
@f.udf(_serde.TILE_SCHEMA)
def _as_tile_udf(raster_bytes):
    if raster_bytes is None:
        return None
    rb = bytes(raster_bytes)
    if len(rb) == 0:
        return None
    return _serde.build_tile(rb, "GTiff", 0)


# combineavg must carry the group's first cellid through to the output tile.
# Spark forbids mixing a grouped-agg pandas_udf with another aggregate (e.g.
# f.first) in the same .agg(), so we cannot pass the cellid as a sibling
# aggregate. Instead the grouped-agg UDF prepends an 8-byte big-endian cellid
# envelope onto the raster bytes; this scalar UDF strips it back off.
def _as_tile_cellid_envelope_udf_fn(raster_bytes):
    if raster_bytes is None:
        return None
    rb = bytes(raster_bytes)
    if len(rb) < 8:
        return None
    cellid = int.from_bytes(rb[:8], "big", signed=True)
    return _serde.build_tile(rb[8:], "GTiff", cellid)


_as_tile_cellid_envelope_udf = f.udf(_serde.TILE_SCHEMA)(
    _as_tile_cellid_envelope_udf_fn
)


# --- grouped-agg pandas_udf(BinaryType()) reducers --------------------------
@pandas_udf(BinaryType())
def _merge_agg_udf(tile: pd.Series) -> bytes:
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    rasters = [b for b in (_tile_raster_bytes(r) for r in tile) if b is not None]
    return agg_core.merge_tiles(rasters)


@pandas_udf(BinaryType())
def _combineavg_agg_udf(tile: pd.Series) -> bytes:
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    rasters = []
    cellid = 0
    first = True
    for r in tile:
        rb = _tile_raster_bytes(r)
        if rb is None:
            continue
        if first:
            cid = r["cellid"]
            cellid = int(cid) if cid is not None else 0
            first = False
        rasters.append(rb)
    out = agg_core.combineavg_tiles(rasters)
    if out is None:
        return None
    # Prepend an 8-byte big-endian cellid envelope (stripped by the scalar UDF).
    return cellid.to_bytes(8, "big", signed=True) + bytes(out)


@pandas_udf(BinaryType())
def _combineavg_agg_sql_udf(tile: pd.Series) -> bytes:
    # SQL registration variant: returns raw GTiff bytes (no cellid envelope), so
    # SQL callers can wrap it directly with gbx_rst_fromcontent(<agg>, 'GTiff').
    # SQL has no tile cellid concept beyond the struct, and the envelope would
    # corrupt fromcontent — so the SQL aggregate drops the cellid (always 0).
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    rasters = [b for b in (_tile_raster_bytes(r) for r in tile) if b is not None]
    return agg_core.combineavg_tiles(rasters)


@pandas_udf(BinaryType())
def _frombands_agg_udf(tile: pd.Series, band_index: pd.Series) -> bytes:
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    indexed = []
    for r, idx in zip(tile, band_index):
        rb = _tile_raster_bytes(r)
        if rb is not None and idx is not None:
            indexed.append((int(idx), rb))
    return agg_core.frombands_tiles(indexed)


@pandas_udf(BinaryType())
def _rasterize_agg_udf(
    geom_wkb: pd.Series,
    value: pd.Series,
    xmin: pd.Series,
    ymin: pd.Series,
    xmax: pd.Series,
    ymax: pd.Series,
    width_px: pd.Series,
    height_px: pd.Series,
    srid: pd.Series,
) -> bytes:
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    features_list = [
        (bytes(g), float(v))
        for g, v in zip(geom_wkb, value)
        if g is not None and v is not None
    ]
    if not features_list:
        return None
    # Extent/size/srid are per-group constants; read them from the first row.
    return agg_core.rasterize_features(
        features_list,
        xmin.iloc[0],
        ymin.iloc[0],
        xmax.iloc[0],
        ymax.iloc[0],
        width_px.iloc[0],
        height_px.iloc[0],
        srid.iloc[0],
    )


@pandas_udf(BinaryType())
def _derivedband_agg_udf(
    tile: pd.Series, python_func: pd.Series, func_name: pd.Series
) -> bytes:
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    rasters = [b for b in (_tile_raster_bytes(r) for r in tile) if b is not None]
    if not rasters:
        return None
    # pyfunc/func_name are per-group constants; read them from the first row.
    return agg_core.derivedband_tiles(
        rasters, str(python_func.iloc[0]), str(func_name.iloc[0])
    )


@pandas_udf(BinaryType())
def _gridfrompoints_agg_udf(
    point: pd.Series,
    value: pd.Series,
    xmin: pd.Series,
    ymin: pd.Series,
    xmax: pd.Series,
    ymax: pd.Series,
    width_px: pd.Series,
    height_px: pd.Series,
    srid: pd.Series,
    power: pd.Series = None,
    max_pts: pd.Series = None,
) -> bytes:
    from databricks.labs.gbx.pyrx import _env
    from databricks.labs.gbx.pyrx.core.tin import _parse_geom_elem

    _env.configure_gdal_env()
    xy = []
    vals = []
    for g, v in zip(point, value):
        if v is None:
            continue
        geom = _parse_geom_elem(g)
        if geom is None or geom.is_empty:
            continue
        xy.append((geom.x, geom.y))
        vals.append(float(v))
    if not xy:
        return None
    # Extent/size/srid/power/max_pts are per-group constants; read from row 0.
    return tin_core.idw_grid(
        xy,
        vals,
        xmin.iloc[0],
        ymin.iloc[0],
        xmax.iloc[0],
        ymax.iloc[0],
        int(width_px.iloc[0]),
        int(height_px.iloc[0]),
        int(srid.iloc[0]),
        power=2.0 if power is None else float(power.iloc[0]),
        max_pts=12 if max_pts is None else int(max_pts.iloc[0]),
    )


@pandas_udf(BinaryType())
def _dtmfromgeoms_agg_udf(
    point: pd.Series,
    breaklines: pd.Series,
    merge_tolerance: pd.Series,
    snap_tolerance: pd.Series,
    xmin: pd.Series,
    ymin: pd.Series,
    xmax: pd.Series,
    ymax: pd.Series,
    width_px: pd.Series,
    height_px: pd.Series,
    srid: pd.Series,
    no_data: pd.Series = None,
) -> bytes:
    from databricks.labs.gbx.pyrx import _env
    from databricks.labs.gbx.pyrx.core.tin import _parse_geom_elem

    _env.configure_gdal_env()
    pts = []
    for g in point:
        geom = _parse_geom_elem(g)
        if geom is None or geom.is_empty:
            continue
        if not geom.has_z:
            raise ValueError(
                "rst_dtmfromgeoms_agg: point has no Z coordinate — supply 3D WKB "
                "(e.g. 'POINT Z (x y z)')"
            )
        c = geom.coords[0]
        pts.append((c[0], c[1], c[2]))
    if not pts:
        return None
    # breaklines is a per-group constant ARRAY of geoms (WKB/EWKB/WKT/EWKT);
    # read from row 0 and let delaunay_dtm decode each element.
    bl_arr = breaklines.iloc[0]
    bl = [b for b in bl_arr if b is not None] if bl_arr is not None else None
    return tin_core.delaunay_dtm(
        pts,
        bl,
        xmin.iloc[0],
        ymin.iloc[0],
        xmax.iloc[0],
        ymax.iloc[0],
        int(width_px.iloc[0]),
        int(height_px.iloc[0]),
        int(srid.iloc[0]),
        no_data=-9999.0 if no_data is None else float(no_data.iloc[0]),
    )


@pandas_udf(BinaryType())
def _rst_h3_rasterize_agg_udf(
    cellid: pd.Series,
    value: pd.Series,
    srid: pd.Series,
    pixel_size: pd.Series,
    xmin: pd.Series,
    ymin: pd.Series,
    xmax: pd.Series,
    ymax: pd.Series,
    width: pd.Series,
    height: pd.Series,
    mode: pd.Series,
    kring_pad: pd.Series,
) -> bytes:
    from databricks.labs.gbx.pyrx import _env
    from databricks.labs.gbx.pyrx.core import cellraster as cr

    _env.configure_gdal_env()
    cells = [int(c) for c in cellid if c is not None]
    if not cells:
        return None
    # Null value -> presence mask (1.0). A null in a typed (Double) value column
    # arrives as np.nan, not None, so guard with pd.isna (np.nan is not None).
    vals = [1.0 if v is None or pd.isna(v) else float(v) for v in value]
    cell_values = {}
    for c, v in zip(cells, vals):
        cell_values[c] = v  # last-wins (cells of one res don't overlap)

    res = cr._resolution([cr._h3_str(c) for c in cells])
    _srid = int(srid.iloc[0]) if srid is not None and srid.iloc[0] is not None else 4326
    _mode = (
        mode.iloc[0] if mode is not None and mode.iloc[0] is not None else "centroids"
    )
    _kp = (
        int(kring_pad.iloc[0])
        if kring_pad is not None and kring_pad.iloc[0] is not None
        else 1
    )

    def _has(s):
        return s is not None and s.iloc[0] is not None

    if _has(xmin) and _has(width):
        grid = (
            float(xmin.iloc[0]),
            float(ymin.iloc[0]),
            float(xmax.iloc[0]),
            float(ymax.iloc[0]),
            (float(xmax.iloc[0]) - float(xmin.iloc[0])) / int(width.iloc[0]),
            int(width.iloc[0]),
            int(height.iloc[0]),
            _srid,
        )
    else:
        _ps = float(pixel_size.iloc[0]) if _has(pixel_size) else None
        grid = cr.compute_gridspec(
            cells, srid=_srid, pixel_size=_ps, mode=_mode, kring_pad=_kp
        )
    return cr.cells_to_raster(cell_values, *grid, resolution=res)


# --- public Column wrappers (compose grouped-agg BINARY + scalar as_tile) ----
def rst_merge_agg(tile: ColLike) -> Column:
    """Merge a group's tile rasters into one spatial mosaic tile.

    Use inside ``.agg()``::

        df.groupBy(k).agg(prx.rst_merge_agg("tile").alias("merged"))

    Each tile carries its own georef/CRS, so the merge is spatial and the output
    spans the union extent. Returns a tile struct (cellid 0).
    """
    return _as_tile_udf(_merge_agg_udf(_col(tile)))


def rst_combineavg_agg(tile: ColLike) -> Column:
    """Per-pixel mean across a group's aligned tiles, ignoring NoData.

    Use inside ``.agg()``::

        df.groupBy(k).agg(prx.rst_combineavg_agg("tile").alias("avg"))

    Assumes the group's tiles are aligned (same shape/extent/CRS); raises if
    shapes differ. The output cellid is the group's first tile cellid. Returns a
    tile struct.
    """
    return _as_tile_cellid_envelope_udf(_combineavg_agg_udf(_col(tile)))


def rst_frombands_agg(tile: ColLike, band_index: ColLike) -> Column:
    """Stack a group's single-band tiles into one multi-band tile.

    Bands are ordered by ``band_index`` ASCENDING (the ordering guarantee).
    Use inside ``.agg()``::

        df.groupBy(k).agg(prx.rst_frombands_agg("tile", "band_index").alias("stacked"))

    Returns a tile struct (cellid 0).
    """
    return _as_tile_udf(_frombands_agg_udf(_col(tile), _col(band_index)))


def rst_rasterize_agg(
    geom_wkb: ColLike,
    value: ColLike,
    xmin: ColLike,
    ymin: ColLike,
    xmax: ColLike,
    ymax: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    srid: ColLike,
) -> Column:
    """Burn a group's ``(geom_wkb, value)`` features into ONE tile.

    The extent/size/srid args are per-group constants. Overlap is last-wins.
    Use inside ``.agg()``::

        df.groupBy(k).agg(
            prx.rst_rasterize_agg("g", "v", 0, 0, 4, 4, 256, 256, 4326).alias("burned")
        )

    Returns a tile struct (cellid 0).
    """
    return _as_tile_udf(
        _rasterize_agg_udf(
            _col(geom_wkb),
            _col(value),
            _col(xmin),
            _col(ymin),
            _col(xmax),
            _col(ymax),
            _col(width_px),
            _col(height_px),
            _col(srid),
        )
    )


def rst_derivedband_agg(
    tile: ColLike, python_func: ColLike, func_name: ColLike
) -> Column:
    """Apply a user GDAL VRT pixel function across a group's tiles.

    Each tile contributes one input band; the pyfunc (``func_name`` entry point)
    runs across the N bands to produce a single-band output tile.
    ``python_func``/``func_name`` are per-group constants. Use inside ``.agg()``::

        df.groupBy(k).agg(prx.rst_derivedband_agg("tile", code, "fn").alias("out"))

    SECURITY: ``python_func`` is exec'd in-process without sandboxing — pass only
    trusted code. Returns a tile struct (cellid 0).
    """
    pf = f.lit(python_func) if isinstance(python_func, str) else _col(python_func)
    fn = f.lit(func_name) if isinstance(func_name, str) else _col(func_name)
    return _as_tile_udf(_derivedband_agg_udf(_col(tile), pf, fn))


def rst_gridfrompoints_agg(
    point: ColLike,
    value: ColLike,
    xmin: ColLike,
    ymin: ColLike,
    xmax: ColLike,
    ymax: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    srid: ColLike,
    power: ColLike = 2.0,
    max_pts: ColLike = 12,
) -> Column:
    """Streaming IDW grid per group: one ``(point, value)`` per row -> one tile.

    The extent/size/srid/power/max_pts args are per-group constants. Equal to
    ``rst_gridfrompoints`` over the same points. Use inside ``.agg()``::

        df.groupBy(k).agg(
            prx.rst_gridfrompoints_agg("pt", "v", 0, 0, 10, 10, 8, 8, 32633).alias("t")
        )

    Returns a tile struct (cellid 0).
    """
    p = f.lit(power) if isinstance(power, (int, float)) else _col(power)
    m = f.lit(max_pts) if isinstance(max_pts, (int, float)) else _col(max_pts)
    return _as_tile_udf(
        _gridfrompoints_agg_udf(
            _col(point),
            _col(value),
            _col(xmin),
            _col(ymin),
            _col(xmax),
            _col(ymax),
            _col(width_px),
            _col(height_px),
            _col(srid),
            p,
            m,
        )
    )


def rst_dtmfromgeoms_agg(
    point: ColLike,
    breaklines: ColLike,
    merge_tolerance: ColLike,
    snap_tolerance: ColLike,
    xmin: ColLike,
    ymin: ColLike,
    xmax: ColLike,
    ymax: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    srid: ColLike,
    no_data: ColLike = -9999.0,
) -> Column:
    """Streaming Delaunay-TIN DTM per group: one Z-point per row -> one tile.

    ``breaklines`` is a per-group constant ARRAY<BINARY>; every other non-point
    arg is a per-group constant. Equal to ``rst_dtmfromgeoms`` over the same
    points. Use inside ``.agg()``::

        df.groupBy(k).agg(
            prx.rst_dtmfromgeoms_agg(
                "pt", f.lit(None), 0.0, 0.0, 0, 0, 10, 10, 10, 10, 32633
            ).alias("t")
        )

    PARITY DIVERGENCE: unconstrained Delaunay — ``breaklines`` accepted but not
    enforced; ``merge_tolerance`` / ``snap_tolerance`` accepted but not applied.
    Returns a tile struct (cellid 0).
    """
    nd = f.lit(no_data) if isinstance(no_data, (int, float)) else _col(no_data)
    return _as_tile_udf(
        _dtmfromgeoms_agg_udf(
            _col(point),
            _col(breaklines),
            _col(merge_tolerance),
            _col(snap_tolerance),
            _col(xmin),
            _col(ymin),
            _col(xmax),
            _col(ymax),
            _col(width_px),
            _col(height_px),
            _col(srid),
            nd,
        )
    )


def rst_h3_rasterize_agg(
    cellid: ColLike,
    value: ColLike = None,
    srid: ColLike = None,
    pixel_size: ColLike = None,
    xmin: ColLike = None,
    ymin: ColLike = None,
    xmax: ColLike = None,
    ymax: ColLike = None,
    width: ColLike = None,
    height: ColLike = None,
    mode: ColLike = None,
    kring_pad: ColLike = None,
) -> Column:
    """Rasterize a group's H3 cells into one tile (pixel-centroid burn).

    ``value`` omitted -> presence mask (1.0/NoData). Supply an explicit extent
    (xmin..height, e.g. from ``rst_h3_gridspec``) for aligned band stacking;
    else the grid is auto-derived per ``mode``/``kring_pad``. Use inside
    ``.agg()``::

        df.groupBy(k).agg(prx.rst_h3_rasterize_agg("cellid").alias("tile"))

    SQL returns BINARY (the raw grouped-agg UDF); Python returns a tile struct
    (wrapped by ``_as_tile_udf``).
    """

    def _c(x, default):
        return _col(x) if x is not None else f.lit(default)

    return _as_tile_udf(
        _rst_h3_rasterize_agg_udf(
            _col(cellid),
            _c(value, None),
            _c(srid, 4326),
            _c(pixel_size, None),
            _c(xmin, None),
            _c(ymin, None),
            _c(xmax, None),
            _c(ymax, None),
            _c(width, None),
            _c(height, None),
            _c(mode, "centroids"),
            _c(kring_pad, 1),
        )
    )


# ---------------------------------------------------------------------------
# H3 cell bbox + gridspec helpers
# ---------------------------------------------------------------------------


@f.udf(_BBOX_SCHEMA)
def _h3_cell_bbox_udf(cellid, srid, mode, kring_pad):
    """Return STRUCT<xmin,ymin,xmax,ymax> for one H3 cell in *srid*.

    When *kring_pad* > 0 the cell is expanded to its k-ring neighbourhood
    before computing the bounding box, so the returned bbox covers the full
    padded neighbourhood of that cell.
    """
    if cellid is None:
        return None
    import h3 as _h3

    from databricks.labs.gbx.pyrx.core import cellraster as _cr

    cstr = _cr._h3_str(int(cellid))
    pad = int(kring_pad) if kring_pad is not None else 0
    if pad > 0:
        cells = list(_h3.grid_disk(cstr, pad))
    else:
        cells = [cstr]

    _srid = int(srid) if srid is not None else 4326
    _mode = mode or "centroids"

    lons, lats = [], []
    for c in cells:
        if _mode == "centroids":
            la, lo = _h3.cell_to_latlng(c)
            lons.append(lo)
            lats.append(la)
        else:
            for la, lo in _h3.cell_to_boundary(c):
                lons.append(lo)
                lats.append(la)

    xs, ys = _cr._reproject(lons, lats, 4326, _srid)
    return (float(xs.min()), float(ys.min()), float(xs.max()), float(ys.max()))


def gbx_h3_cell_bbox(
    cellid: ColLike, srid: ColLike = None, mode: ColLike = None
) -> Column:
    """Bounding box of one H3 cell in *srid* (centroid point or hexagon envelope).

    Returns a ``STRUCT<xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE>``.

    Args:
        cellid: Column holding the H3 cell id (integer).
        srid:   Output CRS EPSG code. Defaults to 4326.
        mode:   ``"centroids"`` (default) or ``"spatial_envelope"``.
    """
    return _h3_cell_bbox_udf(
        _col(cellid),
        _col(srid) if srid is not None else f.lit(4326),
        _col(mode) if mode is not None else f.lit("centroids"),
        f.lit(0),
    )


def rst_h3_gridspec(
    df,
    cell_col="cellid",
    *group_cols,
    srid=4326,
    pixel_size=None,
    mode="centroids",
    kring_pad=1,
):
    """Snapped shared-canvas grid spec per group of H3 cells.

    Returns the grouped DataFrame with a ``grid`` column of type
    ``STRUCT<xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE,
    pixel_size DOUBLE, width INT, height INT, srid INT>``.

    Implemented as per-cell bbox expansion (via a scalar UDF) + native Spark
    min/max aggregation + snap arithmetic from ``cellraster.snap_bounds``,
    so it is Serverless-safe (no ``spark.conf.set``, no JVM access).

    Args:
        df:          Input DataFrame.
        cell_col:    Column name holding H3 cell IDs (integer).
        *group_cols: Additional grouping columns (e.g. a tile/region key).
        srid:        Output CRS EPSG code. Defaults to 4326.
        pixel_size:  Ground resolution in CRS units. ``None`` = auto from H3
                     resolution (edge-length heuristic, same as
                     ``cellraster.compute_gridspec``).
        mode:        ``"centroids"`` (default) or ``"spatial_envelope"``.
        kring_pad:   k-ring padding applied per cell before computing its bbox.
                     Defaults to 1 (matches ``compute_gridspec`` default).

    Returns:
        DataFrame grouped by *group_cols* with a ``grid`` struct column added.
    """
    # Sample one cell on the driver to obtain the H3 resolution for auto pixel_size.
    # An empty input is always an error: there is nothing to rasterize onto.
    _res = None
    _pixel_size = pixel_size
    if _pixel_size is None:
        first_row = df.select(cell_col).first()
        if first_row is None or first_row[0] is None:
            raise ValueError("empty cell set")
        sample_str = cellraster_core._h3_str(int(first_row[0]))
        import h3 as _h3_driver

        _res = _h3_driver.get_resolution(sample_str)

    # Per-cell expanded bbox (kring_pad applied inside the scalar UDF).
    b = _h3_cell_bbox_udf(_col(cell_col), f.lit(srid), f.lit(mode), f.lit(kring_pad))
    gcols = list(group_cols)
    enriched = df.withColumn("_bb", b)
    agg_expr = enriched.groupBy(*gcols) if gcols else enriched.groupBy()
    bounds = agg_expr.agg(
        f.min("_bb.xmin").alias("_xmin"),
        f.min("_bb.ymin").alias("_ymin"),
        f.max("_bb.xmax").alias("_xmax"),
        f.max("_bb.ymax").alias("_ymax"),
    )

    # Capture closure values for the snap UDF (driver-side constants).
    _srid_val = srid

    @f.udf(_GRID_SCHEMA)
    def _snap_to_grid(bxmin, bymin, bxmax, bymax):
        if bxmin is None:
            return None
        import math as _math

        import h3 as _h3

        ps = _pixel_size
        if ps is None:
            mid_lat = (float(bymin) + float(bymax)) / 2.0
            edge_m = (
                _h3.average_hexagon_edge_length(_res, unit="m")
                if _res is not None
                else 1.0
            )
            if _srid_val == 4326:
                ps = edge_m / (111320.0 * max(_math.cos(_math.radians(mid_lat)), 1e-6))
            else:
                ps = edge_m

        bxmin_f = float(bxmin)
        bymin_f = float(bymin)
        bxmax_f = float(bxmax)
        bymax_f = float(bymax)

        xmin = _math.floor(bxmin_f / ps) * ps
        ymax = _math.ceil(bymax_f / ps) * ps
        width = max(1, int(_math.ceil((bxmax_f - xmin) / ps)))
        height = max(1, int(_math.ceil((ymax - bymin_f) / ps)))
        xmax = xmin + width * ps
        ymin = ymax - height * ps
        return (xmin, ymin, xmax, ymax, ps, width, height, _srid_val)

    return bounds.withColumn(
        "grid",
        _snap_to_grid(f.col("_xmin"), f.col("_ymin"), f.col("_xmax"), f.col("_ymax")),
    ).drop("_xmin", "_ymin", "_xmax", "_ymax")


# ---------------------------------------------------------------------------
# SQL registration registry
# ---------------------------------------------------------------------------
# Struct-accepting scalar UDFs for SQL registration.  The Python Column API
# still goes through the pandas_udf path above (tile_scalar_udf/2); these are
# separate objects that accept the full tile struct (so SQL can pass the struct
# column directly without callers needing to extract the raster subfield).

_sql_accessors = {
    "gbx_rst_width": sql_scalar_udf(accessors.width, IntegerType()),
    "gbx_rst_height": sql_scalar_udf(accessors.height, IntegerType()),
    "gbx_rst_numbands": sql_scalar_udf(accessors.numbands, IntegerType()),
    "gbx_rst_srid": sql_scalar_udf(accessors.srid, IntegerType()),
    "gbx_rst_pixelwidth": sql_scalar_udf(accessors.pixelwidth, DoubleType()),
    "gbx_rst_pixelheight": sql_scalar_udf(accessors.pixelheight, DoubleType()),
    "gbx_rst_upperleftx": sql_scalar_udf(accessors.upperleftx, DoubleType()),
    "gbx_rst_upperlefty": sql_scalar_udf(accessors.upperlefty, DoubleType()),
    "gbx_rst_scalex": sql_scalar_udf(accessors.scalex, DoubleType()),
    "gbx_rst_scaley": sql_scalar_udf(accessors.scaley, DoubleType()),
    "gbx_rst_isempty": sql_scalar_udf(accessors.isempty, BooleanType()),
    "gbx_rst_boundingbox": sql_scalar_udf(accessors.boundingbox, BinaryType()),
    "gbx_rst_metadata": sql_scalar_udf(
        accessors.metadata, MapType(StringType(), StringType())
    ),
    "gbx_rst_type": sql_scalar_udf(accessors.type, ArrayType(StringType())),
    "gbx_rst_getnodata": sql_scalar_udf(accessors.getnodata, ArrayType(DoubleType())),
    "gbx_rst_rastertoworldcoordx": sql_scalar_udf2(
        coords.raster_to_world_x, DoubleType()
    ),
    "gbx_rst_rastertoworldcoordy": sql_scalar_udf2(
        coords.raster_to_world_y, DoubleType()
    ),
    "gbx_rst_worldtorastercoordx": sql_scalar_udf2(
        coords.world_to_raster_x, IntegerType()
    ),
    "gbx_rst_worldtorastercoordy": sql_scalar_udf2(
        coords.world_to_raster_y, IntegerType()
    ),
    # Group 1 per-band statistics & scalar accessors (struct-accepting).
    "gbx_rst_avg": sql_scalar_udf(accessors.avg, ArrayType(DoubleType())),
    "gbx_rst_min": sql_scalar_udf(accessors.minimum, ArrayType(DoubleType())),
    "gbx_rst_max": sql_scalar_udf(accessors.maximum, ArrayType(DoubleType())),
    "gbx_rst_median": sql_scalar_udf(accessors.median, ArrayType(DoubleType())),
    "gbx_rst_pixelcount": sql_scalar_udf(accessors.pixelcount, ArrayType(LongType())),
    "gbx_rst_rotation": sql_scalar_udf(accessors.rotation, DoubleType()),
    "gbx_rst_skewx": sql_scalar_udf(accessors.skewx, DoubleType()),
    "gbx_rst_skewy": sql_scalar_udf(accessors.skewy, DoubleType()),
    "gbx_rst_format": sql_scalar_udf(accessors.format, StringType()),
    # memsize reads the raster byte length straight off the tile struct.
    "gbx_rst_memsize": _memsize_struct_udf,
    "gbx_rst_georeference": _georeference_udf,
    "gbx_rst_bandmetadata": _bandmetadata_udf,
    "gbx_rst_subdatasets": _subdatasets_udf,
    "gbx_rst_getsubdataset": _getsubdataset_udf,
    "gbx_rst_summary": _summary_udf,
    "gbx_rst_histogram": _histogram_udf,
    "gbx_rst_rastertoworldcoord": _rastertoworldcoord_udf,
    "gbx_rst_worldtorastercoord": _worldtorastercoord_udf,
    "gbx_h3_cell_bbox": _h3_cell_bbox_udf,
}

# Tile-returning / constructor / array UDFs already accept the tile struct
# (or raw constructor inputs for fromcontent/rasterize); register the existing
# objects directly — no wrapper needed.
_sql_tile_ops = {
    "gbx_rst_fromcontent": _fromcontent_udf,
    "gbx_rst_fromfile": _fromfile_udf,
    "gbx_rst_merge": _merge_udf,
    "gbx_rst_combineavg": _combineavg_udf,
    "gbx_rst_frombands": _frombands_udf,
    "gbx_rst_transform": _transform_udf,
    "gbx_rst_to_webmercator": _to_webmercator_udf,
    "gbx_rst_resample": _resample_udf,
    "gbx_rst_resample_to_size": _resample_to_size_udf,
    "gbx_rst_resample_to_res": _resample_to_res_udf,
    "gbx_rst_clip": _clip_udf,
    "gbx_rst_updatetype": _update_type_udf,
    "gbx_rst_initnodata": _init_nodata_udf,
    "gbx_rst_tryopen": _tryopen_udf,
    "gbx_rst_setsrid": _setsrid_udf,
    "gbx_rst_band": _band_udf,
    "gbx_rst_asformat": _asformat_udf,
    "gbx_rst_buildoverviews": _buildoverviews_udf,
    "gbx_rst_sample": _sample_udf,
    "gbx_rst_proximity": _proximity_udf,
    "gbx_rst_contour": _contour_udf,
    "gbx_rst_viewshed": _viewshed_udf,
    "gbx_rst_cog_convert": _cog_convert_udf,
    "gbx_rst_fillnodata": _fillnodata_udf,
    "gbx_rst_rasterize": _rasterize_udf,
    "gbx_rst_gridfrompoints": _gridfrompoints_udf,
    "gbx_rst_dtmfromgeoms": _dtmfromgeoms_udf,
    # gbx_rst_polygonize is a UDTF registered separately in register() via
    # spark.udtf.register — UDTFs cannot go through spark.udf.register.
    # gbx_rst_{h3,quadbin}_rastertogrid* are UDTFs registered separately in
    # register() via spark.udtf.register — UDTFs cannot go through spark.udf.register.
    "gbx_rst_ndvi": _ndvi_udf,
    "gbx_rst_ndwi": _ndwi_udf,
    "gbx_rst_nbr": _nbr_udf,
    "gbx_rst_savi": _savi_udf,
    "gbx_rst_evi": _evi_udf,
    "gbx_rst_slope": _slope_udf,
    "gbx_rst_aspect": _aspect_udf,
    "gbx_rst_hillshade": _hillshade_udf,
    "gbx_rst_tri": _tri_udf,
    "gbx_rst_tpi": _tpi_udf,
    "gbx_rst_roughness": _roughness_udf,
    "gbx_rst_color_relief": _color_relief_udf,
    "gbx_rst_threshold": _threshold_udf,
    "gbx_rst_filter": _filter_udf,
    "gbx_rst_convolve": _convolve_udf,
    "gbx_rst_mapalgebra": _mapalgebra_udf,
    "gbx_rst_index": _index_udf,
    "gbx_rst_derivedband": _derivedband_udf,
    # gbx_rst_separatebands / retile / tooverlappingtiles / maketiles /
    # h3_tessellate / xyzpyramid are fan-out UDTFs registered separately in
    # register() via spark.udtf.register — UDTFs cannot go through
    # spark.udf.register.
    "gbx_rst_tilexyz": _tilexyz_udf,
}

# Grouped aggregators register the BINARY grouped-agg pandas_udf directly; SQL
# callers use them in GROUP BY and wrap the BINARY result with
# gbx_rst_fromcontent(<agg>, 'GTiff') to recover a tile struct. The grouped-agg
# UDFs accept the tile STRUCT column directly in SQL as well (verified).
_sql_aggregators = {
    "gbx_rst_merge_agg": _merge_agg_udf,
    "gbx_rst_combineavg_agg": _combineavg_agg_sql_udf,
    "gbx_rst_frombands_agg": _frombands_agg_udf,
    "gbx_rst_rasterize_agg": _rasterize_agg_udf,
    "gbx_rst_derivedband_agg": _derivedband_agg_udf,
    "gbx_rst_gridfrompoints_agg": _gridfrompoints_agg_udf,
    "gbx_rst_dtmfromgeoms_agg": _dtmfromgeoms_agg_udf,
    "gbx_rst_h3_rasterize_agg": _rst_h3_rasterize_agg_udf,
}

SQL_REGISTRY = {**_sql_accessors, **_sql_tile_ops, **_sql_aggregators}
