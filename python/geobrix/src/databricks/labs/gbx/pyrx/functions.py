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
from databricks.labs.gbx.pyrx._udf import ColLike, _col, _crs_col
from databricks.labs.gbx.pyrx.core import accessors
from databricks.labs.gbx.pyrx.core import agg as agg_core
from databricks.labs.gbx.pyrx.core import analysis as analysis_core
from databricks.labs.gbx.pyrx.core import cellraster as cellraster_core
from databricks.labs.gbx.pyrx.core import coords
from databricks.labs.gbx.pyrx.core import derivedband as derivedband_core
from databricks.labs.gbx.pyrx.core import edit, features, focal, gridagg, indices
from databricks.labs.gbx.pyrx.core import mapalgebra as mapalgebra_core
from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core import ops as ops_core
from databricks.labs.gbx.pyrx.core import resample, terrain
from databricks.labs.gbx.pyrx.core import tessellate as tessellate_core
from databricks.labs.gbx.pyrx.core import tiling
from databricks.labs.gbx.pyrx.core import tin as tin_core
from databricks.labs.gbx.pyrx.core import warp, xyz
from databricks.labs.gbx.pyrx.core.escape import rst_apply, tile_to_numpy  # noqa: F401
from databricks.labs.gbx.pyrx.core.virtual_tile import V2_TILE_SCHEMA, VirtualTile


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
        ("gbx_rst_h3_rastertogridsum", _RstH3RasterToGridSumUDTF),
        ("gbx_rst_h3_rastertogridvariance", _RstH3RasterToGridVarianceUDTF),
        ("gbx_rst_h3_rastertogridstddev", _RstH3RasterToGridStddevUDTF),
        ("gbx_rst_quadbin_rastertogridavg", _RstQuadbinRasterToGridAvgUDTF),
        ("gbx_rst_quadbin_rastertogridcount", _RstQuadbinRasterToGridCountUDTF),
        ("gbx_rst_quadbin_rastertogridmax", _RstQuadbinRasterToGridMaxUDTF),
        ("gbx_rst_quadbin_rastertogridmin", _RstQuadbinRasterToGridMinUDTF),
        ("gbx_rst_quadbin_rastertogridmedian", _RstQuadbinRasterToGridMedianUDTF),
        ("gbx_rst_quadbin_rastertogridsum", _RstQuadbinRasterToGridSumUDTF),
        ("gbx_rst_quadbin_rastertogridvariance", _RstQuadbinRasterToGridVarianceUDTF),
        ("gbx_rst_quadbin_rastertogridstddev", _RstQuadbinRasterToGridStddevUDTF),
        ("gbx_rst_bng_rastertogridavg", _RstBngRasterToGridAvgUDTF),
        ("gbx_rst_bng_rastertogridcount", _RstBngRasterToGridCountUDTF),
        ("gbx_rst_bng_rastertogridmax", _RstBngRasterToGridMaxUDTF),
        ("gbx_rst_bng_rastertogridmin", _RstBngRasterToGridMinUDTF),
        ("gbx_rst_bng_rastertogridmedian", _RstBngRasterToGridMedianUDTF),
        ("gbx_rst_bng_rastertogridsum", _RstBngRasterToGridSumUDTF),
        ("gbx_rst_bng_rastertogridvariance", _RstBngRasterToGridVarianceUDTF),
        ("gbx_rst_bng_rastertogridstddev", _RstBngRasterToGridStddevUDTF),
        ("gbx_rst_separatebands", _RstSeparateBandsUDTF),
        ("gbx_rst_retile", _RstRetileUDTF),
        ("gbx_rst_tooverlappingtiles", _RstToOverlappingTilesUDTF),
        ("gbx_rst_maketiles", _RstMakeTilesUDTF),
        ("gbx_rst_h3_tessellate", _RstH3TessellateUDTF),
        ("gbx_rst_quadbin_tessellate", _RstQuadbinTessellateUDTF),
        ("gbx_rst_bng_tessellate", _RstBngTessellateUDTF),
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


# --- Virtual-aware tile helpers (Increment 4) -------------------------------
# These let a UDF body consume a v1 tile struct, a v2 tile struct, a virtual
# (bytes-free) tile, or raw bytes uniformly, and shape a tile-returning result
# via the shared ``open_tile.shape_output`` force-output helper. The operation
# math is never touched — only how the input dataset is opened and how the
# result is shaped.
def _tile_is_empty(tile) -> bool:
    """True when a tile carries neither raster bytes nor a virtual ``path``.

    A v1/v2 materialized tile has ``raster``; a virtual tile has ``path`` (+
    ``window``). Either is openable, so only a tile missing both is "empty"
    (the null-guard the UDF bodies branch on). Mirrors the old
    ``tile is None or tile["raster"] is None`` guard for materialized tiles
    while additionally admitting virtual tiles.
    """
    if tile is None:
        return True
    if isinstance(tile, (bytes, bytearray)):
        return len(bytes(tile)) == 0
    if isinstance(tile, VirtualTile):
        return tile.raster is None and tile.path is None
    d = tile.asDict() if hasattr(tile, "asDict") else dict(tile)
    return d.get("raster") is None and d.get("path") is None


def _tile_cellid(tile) -> int:
    """Extract ``cellid`` from any tile shape (defaults to 0)."""
    if isinstance(tile, (bytes, bytearray)):
        return 0
    if isinstance(tile, VirtualTile):
        return int(tile.cellid)
    d = tile.asDict() if hasattr(tile, "asDict") else dict(tile)
    return int(d.get("cellid") or 0)


def _dataset_to_gtiff_bytes(ds) -> bytes:
    """Encode an open rasterio dataset to standalone GTiff bytes (full extent).

    Used by multi-input ops (``rst_merge``) that consume their inputs through
    the ``_open_all`` front-door but need raster bytes for the core reducer.
    """
    from rasterio.io import MemoryFile

    data = ds.read()
    profile = ds.profile.copy()
    profile.update(driver="GTiff")
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data)
        return mf.read()


def _force_output_requested(virtualize_dir, virtualize_prefix, materialize) -> bool:
    """True when any force-output param was supplied (dispatch to the v2 UDF)."""
    return (
        virtualize_dir is not None
        or virtualize_prefix is not None
        or materialize is not None
    )


def _validate_force_output(virtualize_dir, materialize) -> None:
    """Eager Python-side guard for the mutually-exclusive force-output combo."""
    if virtualize_dir is not None and materialize:
        raise ValueError("virtualize_dir and materialize=True are mutually exclusive")


def _force_output_lits(virtualize_dir, virtualize_prefix, materialize):
    """The 3 trailing force-output literal Columns for a v2 UDF invocation."""
    return (
        f.lit(virtualize_dir),
        f.lit(virtualize_prefix),
        f.lit(materialize),
    )


def _shaped_result_row(
    new_bytes, cellid, virtualize_dir, virtualize_prefix, materialize
):
    """Wrap produced bytes as a VirtualTile, apply shape_output, return its Row.

    Runs on the worker inside a force-output (v2) UDF. ``virtualize_dir`` writes
    the bytes to a durable path (FUSE-safe) and returns a light virtual row;
    ``materialize=True`` keeps the bytes; neither returns the natural (bytes)
    shape. Returns a dict matching ``V2_TILE_SCHEMA``.
    """
    if new_bytes is None:
        return None
    # Fresh VirtualTile with empty metadata — no source metadata copied, so the
    # produced tile carries no pending_* keys (they were consumed into the bytes).
    vt = VirtualTile(cellid=int(cellid), raster=bytes(new_bytes))
    shaped = ot.shape_output(
        vt,
        virtualize_dir=virtualize_dir,
        virtualize_prefix=virtualize_prefix,
        materialize=materialize,
    )
    return shaped.to_row()


# --- Virtual-aware accessor UDF factories (Increment 4) ---------------------
# Accessors take the FULL tile struct (not just the raster subfield) so a
# virtual tile's ``path`` is reachable. HEADER-ONLY accessors answer from the
# file header via ``open_header`` (no pixel read; window-correct for windowed
# virtual tiles); PIXEL accessors materialise the window via ``_open``. The
# core accessor math is never touched.
def _header_accessor_udf(core_fn, return_type):
    """Struct-accepting header-only accessor UDF (open_header, no pixel read)."""

    @f.udf(return_type)
    def _udf(tile):
        if _tile_is_empty(tile):
            return None
        try:
            from databricks.labs.gbx.pyrx import _env

            _env.configure_gdal_env()
            with ot.open_header(tile) as ds:
                return core_fn(ds)
        except Exception:  # noqa: BLE001
            return None

    return _udf


def _pixel_accessor_udf(core_fn, return_type):
    """Struct-accepting pixel accessor UDF (_open, materialises the window)."""

    @f.udf(return_type)
    def _udf(tile):
        if _tile_is_empty(tile):
            return None
        try:
            from databricks.labs.gbx.pyrx import _env

            _env.configure_gdal_env()
            with ot._open(tile) as ds:
                return core_fn(ds)
        except Exception:  # noqa: BLE001
            return None

    return _udf


# --- FILE-aware 2-arg UDF factories (Increment 5 / Task 4) ------------------
# These produce UDFs with signature (tile: Struct, file_ref: FileRef|null).
# The SQL registry keeps pointing at the single-arg ``_u_*`` UDFs (fallback path
# per spec §4.3); only the public Python Column bindings use ``_uf_*``.
def _header_accessor_udf_file(core_fn, return_type):
    """Struct + FileRef accepting header-only accessor UDF (2-arg).

    Signature: (tile: Struct, file_ref: FileRef|null) → return_type
    Calls open_header(tile, file_ref=file_ref) internally.  When file_ref is
    None the open_header front-door falls back to the plain-path read path.
    """

    @f.udf(return_type)
    def _udf(tile, file_ref):
        if _tile_is_empty(tile):
            return None
        try:
            from databricks.labs.gbx.pyrx import _env

            _env.configure_gdal_env()
            with ot.open_header(tile, file_ref=file_ref) as ds:
                return core_fn(ds)
        except Exception:  # noqa: BLE001
            return None

    return _udf


def _pixel_accessor_udf_file(core_fn, return_type):
    """Struct + FileRef accepting pixel accessor UDF (2-arg).

    Signature: (tile: Struct, file_ref: FileRef|null) → return_type
    Calls _open(tile, file_ref=file_ref) internally.  When file_ref is None
    the _open front-door falls back to the plain-path read path.
    """

    @f.udf(return_type)
    def _udf(tile, file_ref):
        if _tile_is_empty(tile):
            return None
        try:
            from databricks.labs.gbx.pyrx import _env

            _env.configure_gdal_env()
            with ot._open(tile, file_ref=file_ref) as ds:
                return core_fn(ds)
        except Exception:  # noqa: BLE001
            return None

    return _udf


# --- Module-level UDF singletons (built once at import) ---------------------
# Header-only accessors (open_header — answer from the header, no pixel read;
# a virtual tile is resolved from its ``path``).
_u_height = _header_accessor_udf(accessors.height, IntegerType())
_u_numbands = _header_accessor_udf(accessors.numbands, IntegerType())
_u_srid = _header_accessor_udf(accessors.srid, IntegerType())
_u_crs = _header_accessor_udf(accessors.crs, StringType())
_u_pixelwidth = _header_accessor_udf(accessors.pixelwidth, DoubleType())
_u_pixelheight = _header_accessor_udf(accessors.pixelheight, DoubleType())
_u_upperleftx = _header_accessor_udf(accessors.upperleftx, DoubleType())
_u_upperlefty = _header_accessor_udf(accessors.upperlefty, DoubleType())
_u_boundingbox = _header_accessor_udf(accessors.boundingbox, BinaryType())
_u_scalex = _header_accessor_udf(accessors.scalex, DoubleType())
_u_scaley = _header_accessor_udf(accessors.scaley, DoubleType())
_u_type = _header_accessor_udf(accessors.type, ArrayType(StringType()))
_u_getnodata = _header_accessor_udf(accessors.getnodata, ArrayType(DoubleType()))
# Pixel accessors (_open — materialise the window).
_u_isempty = _pixel_accessor_udf(accessors.isempty, BooleanType())


# metadata: HEADER-ONLY accessor. Struct-accepting (so a virtual tile's ``path``
# is reachable) and MapType-returning (plain @f.udf; pandas_udf rejects MapType
# on some Arrow builds).
@f.udf(MapType(StringType(), StringType()))
def _metadata_udf(tile):
    if _tile_is_empty(tile):
        return None
    try:
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        with ot.open_header(tile) as ds:
            return accessors.metadata(ds)
    except Exception:  # noqa: BLE001
        return None


# --- FILE-aware 2-arg UDF singletons (Task 4 / Increment 5) ----------------
# Used by the public Python Column bindings; SQL registry still uses ``_u_*``.
# Header-only (open_header — no pixel read):
_uf_height = _header_accessor_udf_file(accessors.height, IntegerType())
_uf_numbands = _header_accessor_udf_file(accessors.numbands, IntegerType())
_uf_srid = _header_accessor_udf_file(accessors.srid, IntegerType())
_uf_crs = _header_accessor_udf_file(accessors.crs, StringType())
_uf_width = _header_accessor_udf_file(accessors.width, IntegerType())
_uf_boundingbox = _header_accessor_udf_file(accessors.boundingbox, BinaryType())
# Pixel-reading (_open — materialises the window):
_uf_isempty = _pixel_accessor_udf_file(accessors.isempty, BooleanType())

# Group 1 pixel accessors — FILE-aware 2-arg singletons.
_uf_avg = _pixel_accessor_udf_file(accessors.avg, ArrayType(DoubleType()))
_uf_min = _pixel_accessor_udf_file(accessors.minimum, ArrayType(DoubleType()))
_uf_max = _pixel_accessor_udf_file(accessors.maximum, ArrayType(DoubleType()))
_uf_median = _pixel_accessor_udf_file(accessors.median, ArrayType(DoubleType()))
_uf_pixelcount = _pixel_accessor_udf_file(accessors.pixelcount, ArrayType(LongType()))

# Group 1 header accessors — FILE-aware 2-arg singletons.
_uf_type = _header_accessor_udf_file(accessors.type, ArrayType(StringType()))
_uf_getnodata = _header_accessor_udf_file(accessors.getnodata, ArrayType(DoubleType()))


# metadata: HEADER-ONLY, MapType return (plain @f.udf; pandas_udf rejects
# MapType on some Arrow builds).  2-arg FILE-aware variant for Python bindings.
@f.udf(MapType(StringType(), StringType()))
def _uf_metadata_udf(tile, file_ref):
    if _tile_is_empty(tile):
        return None
    try:
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        with ot.open_header(tile, file_ref=file_ref) as ds:
            return accessors.metadata(ds)
    except Exception:  # noqa: BLE001
        return None


# summary: PIXEL accessor, StringType return.  2-arg FILE-aware variant.
@f.udf(StringType())
def _uf_summary_udf(tile, file_ref):
    if _tile_is_empty(tile):
        return None
    try:
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        with ot._open(tile, file_ref=file_ref) as ds:
            return accessors.summary(ds)
    except Exception:  # noqa: BLE001
        return None


# histogram: PIXEL accessor, MapType return.  2-arg FILE-aware variant.
@f.udf(MapType(StringType(), ArrayType(LongType())))
def _uf_histogram_udf(tile, file_ref, n_buckets, min_val, max_val, include_nodata):
    if _tile_is_empty(tile):
        return None
    try:
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        nb = 256 if n_buckets is None else int(n_buckets)
        lo = None if min_val is None else float(min_val)
        hi = None if max_val is None else float(max_val)
        inc = bool(include_nodata) if include_nodata is not None else False
        with ot._open(tile, file_ref=file_ref) as ds:
            return accessors.histogram(ds, nb, lo, hi, inc)
    except Exception:  # noqa: BLE001
        return None


# Coordinate transforms are HEADER-ONLY (ds.xy / ds.index — no pixel read), so
# build them like the other header accessors: struct-accepting (a virtual tile's
# ``path`` is reachable) + open_header. The pre-sweep tile_scalar_udf2 builders
# took only the raster subfield, which is None for a virtual tile -> silent NULL.
def _header_accessor_udf2(core_fn, return_type):
    """Struct-accepting header-only 2-arg accessor UDF (open_header, no pixel read)."""

    @f.udf(return_type)
    def _udf(tile, a, b):
        if _tile_is_empty(tile):
            return None
        try:
            from databricks.labs.gbx.pyrx import _env

            _env.configure_gdal_env()
            with ot.open_header(tile) as ds:
                return core_fn(ds, a, b)
        except Exception:  # noqa: BLE001
            return None

    return _udf


def _header_accessor_udf3_file(core_fn, return_type):
    """Struct + FileRef + 2 scalar args header-only accessor UDF (4 args total)."""

    @f.udf(return_type)
    def _udf(tile, file_ref, a, b):
        if _tile_is_empty(tile):
            return None
        try:
            from databricks.labs.gbx.pyrx import _env

            _env.configure_gdal_env()
            with ot.open_header(tile, file_ref=file_ref) as ds:
                return core_fn(ds, a, b)
        except Exception:  # noqa: BLE001
            return None

    return _udf


_u_r2w_x = _header_accessor_udf2(coords.raster_to_world_x, DoubleType())
_u_r2w_y = _header_accessor_udf2(coords.raster_to_world_y, DoubleType())
_u_w2r_x = _header_accessor_udf2(coords.world_to_raster_x, IntegerType())
_u_w2r_y = _header_accessor_udf2(coords.world_to_raster_y, IntegerType())

_uf_r2w_x = _header_accessor_udf3_file(coords.raster_to_world_x, DoubleType())
_uf_r2w_y = _header_accessor_udf3_file(coords.raster_to_world_y, DoubleType())
_uf_w2r_x = _header_accessor_udf3_file(coords.world_to_raster_x, IntegerType())
_uf_w2r_y = _header_accessor_udf3_file(coords.world_to_raster_y, IntegerType())


# --- Group 1: per-band statistics & accessor UDFs ---------------------------
# PIXEL accessors (_open — need the window pixels).
_u_min = _pixel_accessor_udf(accessors.minimum, ArrayType(DoubleType()))
_u_max = _pixel_accessor_udf(accessors.maximum, ArrayType(DoubleType()))
_u_median = _pixel_accessor_udf(accessors.median, ArrayType(DoubleType()))
_u_pixelcount = _pixel_accessor_udf(accessors.pixelcount, ArrayType(LongType()))
_u_avg = _pixel_accessor_udf(accessors.avg, ArrayType(DoubleType()))
# HEADER-only accessors (open_header — answer from the header, no pixel read).
_u_rotation = _header_accessor_udf(accessors.rotation, DoubleType())
_u_skewx = _header_accessor_udf(accessors.skewx, DoubleType())
_u_skewy = _header_accessor_udf(accessors.skewy, DoubleType())
_u_format = _header_accessor_udf(accessors.format, StringType())
_u_width = _header_accessor_udf(accessors.width, IntegerType())

# Back-compat aliases: rst_width/rst_avg were wired to explicit UDF singletons
# in Task 6; keep the names pointing at the factory-built virtual-aware UDFs.
_width_header_udf = _u_width
_avg_pixel_udf = _u_avg


# memsize: works off the raw raster bytes (no rasterio open needed) — mirror
# heavyweight which returns the in-memory buffer length.
@f.udf(LongType())
def _memsize_udf(raster):
    if raster is None:
        return None
    return int(len(bytes(raster)))


# Struct-accepting memsize for SQL registration: returns byte length for a
# materialized tile; estimated decoded window footprint for a virtual tile
# (count * width * height * itemsize via open_header — no pixel read).
@f.udf(LongType())
def _memsize_struct_udf(tile):
    if _tile_is_empty(tile):
        return None
    vt = ot._to_virtual_tile(tile)
    if not vt.is_virtual():
        return int(len(bytes(vt.raster)))
    # virtual: estimate decoded window footprint from the header (no pixel read)
    import numpy as np

    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot.open_header(tile) as ds:
        itemsize = np.dtype(ds.dtypes[0]).itemsize
        return int(ds.count * ds.width * ds.height * itemsize)


# MapType return paths use plain @f.udf (pandas_udf rejects MapType on some
# Arrow builds), matching the existing _metadata_udf fallback.
# georeference / bandmetadata / subdatasets: HEADER-ONLY accessors (open_header,
# no pixel read). MapType returns use plain @f.udf (pandas_udf rejects MapType
# on some Arrow builds).
@f.udf(MapType(StringType(), DoubleType()))
def _georeference_udf(tile):
    if _tile_is_empty(tile):
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot.open_header(tile) as ds:
        return accessors.georeference(ds)


@f.udf(MapType(StringType(), StringType()))
def _bandmetadata_udf(tile, band):
    if _tile_is_empty(tile):
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot.open_header(tile) as ds:
        return accessors.bandmetadata(ds, int(band))


@f.udf(MapType(StringType(), StringType()))
def _subdatasets_udf(tile):
    if _tile_is_empty(tile):
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot.open_header(tile) as ds:
        return accessors.subdatasets(ds)


# summary / histogram / getsubdataset: PIXEL accessors (_open — need the window).
@f.udf(StringType())
def _summary_udf(tile):
    if _tile_is_empty(tile):
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile) as ds:
        return accessors.summary(ds)


@f.udf(MapType(StringType(), ArrayType(LongType())))
def _histogram_udf(tile, n_buckets, min_val, max_val, include_nodata):
    if _tile_is_empty(tile):
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    nb = 256 if n_buckets is None else int(n_buckets)
    lo = None if min_val is None else float(min_val)
    hi = None if max_val is None else float(max_val)
    inc = bool(include_nodata) if include_nodata is not None else False
    with ot._open(tile) as ds:
        return accessors.histogram(ds, nb, lo, hi, inc)


@f.udf(V2_TILE_SCHEMA)
def _getsubdataset_udf(tile, name):
    if _tile_is_empty(tile) or name is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile) as ds:
        new_bytes = accessors.getsubdataset(ds, str(name))
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


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
    if _tile_is_empty(tile):
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile) as ds:
        return coords.raster_to_world_coord(ds, int(x), int(y))


@f.udf(_W2R_COORD_SCHEMA)
def _worldtorastercoord_udf(tile, x, y):
    if _tile_is_empty(tile):
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile) as ds:
        return coords.world_to_raster_coord(ds, float(x), float(y))


# --- Constructor ------------------------------------------------------------
@f.udf(V2_TILE_SCHEMA)
def _fromcontent_udf(raster, drv):
    if raster is None:
        return None
    return _serde.build_tile(bytes(raster), drv or "GTiff")


def rst_fromcontent(content: ColLike, driver: ColLike) -> Column:
    """Build a tile struct from raster BINARY content and GDAL driver name."""
    return _fromcontent_udf(_col(content), _col(driver))


# rst_fromfile: reference a raster on disk as a tile. LIGHT-TIER DEFAULT is a
# VIRTUAL tile — the source file IS the durable backing store, so the tile just
# points at the path (plus the whole-file window + metadata read from the header,
# no pixel I/O). ``materialize=True`` reads the pixels now and returns a
# materialized (bytes) tile. Because the path is already durable there is no
# ``virtualize_dir``/``virtualize_prefix`` (those exist only for functions that
# COMPUTE new pixels and need somewhere to write them).
# A bad/missing path yields NULL (matching the heavyweight's Option(...).orNull).
def _fromfile_impl(path, driver, materialize):
    """Shared body for both the Python-binding UDF (3-arg) and the SQL UDF (2-arg,
    virtual). Returns a v2 tile row (virtual by default, materialized when asked),
    or None on a bad/missing path."""
    if path is None:
        return None
    from databricks.labs.gbx.ds._listing import to_local_path
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    drv = "GTiff" if driver is None else str(driver)
    local = to_local_path(str(path))

    if materialize:
        # Eager path: read the source bytes SEQUENTIALLY (FUSE-safe) and re-encode
        # via an in-memory MemoryFile — a tiled/COG GTiff over a UC Volume seeks to
        # block offsets on read, which Volume FUSE can't serve, so we read the whole
        # file then open from memory rather than rasterio.open() on the path.
        from rasterio.io import MemoryFile

        try:
            with open(local, "rb") as _fh:
                _src_bytes = _fh.read()
            with MemoryFile(_src_bytes) as _src_mf, _src_mf.open() as src:
                data = src.read()
                profile = src.profile.copy()
                profile.update(driver="GTiff")
                with MemoryFile() as mf:
                    with mf.open(**profile) as dst:
                        dst.write(data)
                    new_bytes = mf.read()
        except Exception:  # noqa: BLE001 — null-on-error, matching heavyweight
            return None
        return _serde.build_tile(new_bytes, drv, 0)

    # Default (virtual): open the HEADER only (no pixel read; header reads don't
    # seek block offsets, so FUSE is fine) to record width/height/count, and
    # return a bytes-free tile that points at the path over its whole-file window.
    # Like the raster reader's passthrough tile, ``crs`` is intentionally left
    # NULL: that field doubles as open_tile's warp-target, so setting it to the
    # source CRS would make a later rst_setsrid see pending_srid != crs and warp
    # spuriously. The source CRS stays implicit in the path.
    import rasterio

    try:
        with rasterio.open(local) as src:
            width, height, count = src.width, src.height, src.count
    except Exception:  # noqa: BLE001 — null-on-error, matching heavyweight
        return None
    meta = {
        "sourcePath": str(path),
        "driver": drv,
        "width": str(width),
        "height": str(height),
        "count": str(count),
    }
    # Store the BARE (scheme-free) path, exactly like the raster reader's virtual
    # passthrough tile (raster.py stores partition.file_path). Downstream staging
    # (_stage_local_if_needed) triggers on a bare "/Volumes"/"/dbfs" prefix and
    # does NOT strip a "dbfs:" scheme, so a to_spark_uri()-qualified path would
    # never stage and rasterio.open("dbfs:/Volumes/...") would fail on Databricks.
    return VirtualTile(
        cellid=0,
        raster=None,
        path=local,
        window=(0, 0, width, height),
        metadata=meta,
    ).to_row()


@f.udf(V2_TILE_SCHEMA)
def _fromfile_udf(path, driver, materialize):
    """Python-binding UDF: 3-arg (path, driver, materialize)."""
    return _fromfile_impl(path, driver, bool(materialize))


@f.udf(V2_TILE_SCHEMA)
def _fromfile_sql_udf(path, driver):
    """SQL UDF registered by the LIGHT (pyrx) tier: 2-arg (path, driver) → a
    VIRTUAL tile (materialize=False, the light-tier default). Keeps the historical
    2-arg SQL surface; the tier that registers decides virtual vs materialized."""
    return _fromfile_impl(path, driver, False)


@f.udf(V2_TILE_SCHEMA)
def _fromfile_sql_materialized_udf(path, driver):
    """SQL UDF registered by the HEAVY (rasterx) tier: 2-arg (path, driver) → a
    MATERIALIZED tile (bytes present). Same call text as the light registration;
    the heavyweight tier reads the pixels because JVM/heavy expressions cannot use
    a virtual path-only tile. Whichever tier's register() ran last wins."""
    return _fromfile_impl(path, driver, True)


def rst_fromfile(
    path: ColLike, driver: ColLike = "GTiff", materialize: Optional[bool] = None
) -> Column:
    """Reference the raster at ``path`` as a tile — a **virtual** tile by default.

    ``path`` is a filesystem path (FUSE ``/Volumes/...`` paths work on Databricks)
    and ``driver`` is a GDAL driver short-name hint carried into the tile metadata
    (rasterio auto-detects the actual format on open).

    Light-tier default is a **virtual tile**: bytes-free, pointing at ``path`` over
    its whole-file window — no pixels are read until a downstream op needs them.
    This is the lazy entry point for loading rasters by path. Pass
    ``materialize=True`` to read the pixels now and return a materialized (bytes)
    tile instead. There is no ``virtualize_dir``/``virtualize_prefix`` here: the
    source file already is the durable backing store, so nothing needs writing.

    A path that cannot be opened returns NULL (matching the heavyweight's
    null-on-error behaviour), whether virtual (header open fails) or materialized.

    Args:
        path:        Raster file path (string column).
        driver:      GDAL driver short name hint. Defaults to "GTiff".
        materialize: ``True`` reads pixels now and returns a materialized tile;
                     default (``None``/``False``) returns a virtual tile.

    Returns:
        Tile struct (virtual by default, materialized when ``materialize=True``),
        or NULL if the path cannot be opened.
    """
    drv = f.lit(driver) if isinstance(driver, str) else _col(driver)
    return _fromfile_udf(_col(path), drv, f.lit(bool(materialize)))


# rst_merge: single ARRAY<tile struct> arg in one row -> mosaic tile.
# Mirrors gbx_rst_merge: extract each element's raster bytes and mosaic by
# extent (reuses core.agg.merge_tiles, the same union-extent reducer the
# heavyweight RST_MergeAgg uses). cellid = 0 (no aggregate group key here).
def _merge_bytes(tiles):
    """Shared merge body: collect each input tile's GTiff bytes, mosaic by extent.
    Returns ``(merged_bytes, dropped)`` where ``dropped`` is the count of corrupt
    members skipped, or ``None`` on an empty/all-corrupt array.

    CRITICAL — materialized inputs pass their ORIGINAL bytes verbatim.
    ``agg_core.merge_tiles`` sorts inputs on their RAW GTiff bytes to pick a
    deterministic, tier-agreeing last-wins overlap winner (bitwise-identical
    across light/heavy — see its docstring). Re-encoding a materialized tile
    (full read -> re-write) would change that sort key and diverge from heavy
    for overlapping tiles, so materialized bytes are NEVER round-tripped. Only
    a VIRTUAL input (raster None) is materialized here (it has no bytes yet)."""
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    elems = [t for t in tiles if t is not None and not _tile_is_empty(t)]
    if not elems:
        return None
    rasters = []
    dropped = 0
    for t in elems:
        try:
            vt = ot._to_virtual_tile(t)
            if vt.is_virtual():
                # Only virtual tiles (path+window, no bytes) need materializing.
                candidate = ot.materialize_to_bytes(vt).raster
            else:
                # Verbatim original bytes — preserves the sort key + heavy parity.
                candidate = bytes(vt.raster)
            # Validate the bytes are openable before collecting them; a corrupt
            # member that passes the empty/None filter but cannot be opened by
            # rasterio must be skipped here, not inside agg_core.merge_tiles
            # (which opens all bytes at once and re-raises on the first failure).
            with _serde.open_tile(candidate):
                pass
            rasters.append(candidate)
        except Exception:  # noqa: BLE001
            dropped += 1
            continue
    if not rasters:
        return None
    return agg_core.merge_tiles(rasters), dropped


@f.udf(V2_TILE_SCHEMA)
def _merge_udf(tiles):
    if not tiles:
        return None
    result = _merge_bytes(tiles)
    if result is None:
        return None
    new_bytes, dropped = result
    tile = _serde.build_tile(new_bytes, "GTiff", 0)
    if dropped:
        tile["metadata"][
            "last_error"
        ] = f"RST_Merge: skipped {dropped} corrupt input tile(s)"
    return tile


@f.udf(V2_TILE_SCHEMA)
def _merge_v2_udf(tiles, virtualize_dir, virtualize_prefix, materialize):
    # Force-output variant (Python API only): same mosaic via shape_output.
    if not tiles:
        return None
    result = _merge_bytes(tiles)
    if result is None:
        return None
    new_bytes, dropped = result
    row = _shaped_result_row(
        new_bytes, 0, virtualize_dir, virtualize_prefix, materialize
    )
    if row is not None and dropped:
        row["metadata"][
            "last_error"
        ] = f"RST_Merge: skipped {dropped} corrupt input tile(s)"
    return row


@f.udf(V2_TILE_SCHEMA)
def _uf_merge(tiles, file_refs):
    """FILE-aware rst_merge: reads each input via its per-element FileRef.

    ``tiles`` is ARRAY<tile struct>; ``file_refs`` is a parallel ARRAY<FileRef|null>
    (same length) minted in the plan via F.transform.  Materialized elements have
    file_refs[i]=None (try_to_file on NULL returns NULL); they fall through to verbatim
    bytes to preserve the sort-key invariant (no re-encode of materialized tiles).
    """
    if not tiles:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    frefs = list(file_refs) if file_refs else []
    while len(frefs) < len(tiles):
        frefs.append(None)

    rasters = []
    dropped = 0
    for t, fref in zip(tiles, frefs):
        if t is None or _tile_is_empty(t):
            continue
        try:
            vt = ot._to_virtual_tile(t)
            if vt.is_virtual():
                with ot._open(t, file_ref=fref) as ds:
                    candidate = _dataset_to_gtiff_bytes(ds)
            else:
                candidate = bytes(vt.raster)
            with _serde.open_tile(candidate):
                pass
            rasters.append(candidate)
        except Exception:  # noqa: BLE001
            dropped += 1
            continue

    if not rasters:
        return None
    new_bytes = agg_core.merge_tiles(rasters)
    tile = _serde.build_tile(new_bytes, "GTiff", 0)
    if dropped:
        tile["metadata"][
            "last_error"
        ] = f"RST_Merge: skipped {dropped} corrupt input tile(s)"
    return tile


def rst_merge(
    tiles: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Mosaic an ARRAY of tiles into one tile spanning their union extent.

    Mirrors the heavyweight ``gbx_rst_merge``: ``tiles`` is a single column of
    ARRAY<tile struct> (e.g. ``f.array("ta", "tb")``); each element's raster is
    placed by its own georeference and the output spans the union extent. On
    overlap the merge is first-tile-wins in array order. Output ``cellid`` is 0.

    Args:
        tiles: Column of ARRAY<tile struct>.
        virtualize_dir:    Force-output (light-tier, Python API only): write the
            merged result to a durable path and return a light virtual tile.
        virtualize_prefix: Optional filename prefix for ``virtualize_dir``.
        materialize:       Force-output: ``True`` ensures raster bytes. Mutually
            exclusive with ``virtualize_dir``.

    Returns:
        Tile struct spanning the union extent, or NULL on an empty array. When
        neither force-output param is set the SQL registered signature is used
        unchanged.
    """
    if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
        _validate_force_output(virtualize_dir, materialize)
        return _merge_v2_udf(
            _col(tiles),
            f.lit(virtualize_dir),
            f.lit(virtualize_prefix),
            f.lit(materialize),
        )
    from databricks.labs.gbx.pyrx._file_ref import file_supported

    tc = _col(tiles)
    if file_supported():
        file_refs_col = f.transform(
            tc, lambda t: f.call_function("try_to_file", t["path"])
        )
        return _uf_merge(tc, file_refs_col)
    return _merge_udf(tc)


# rst_combineavg: single ARRAY<tile struct> arg -> per-pixel mean tile.
# Mirrors gbx_rst_combineavg: NoData-aware per-pixel mean across the stack
# (reuses core.agg.combineavg_tiles). cellid follows the heavyweight rule —
# head's cellid if every element shares it, else -1.
def _combineavg_bytes(tiles):
    """Shared combineavg body: collect each input's GTiff bytes + resolve cellid.

    Materialized inputs pass their ORIGINAL bytes verbatim (no re-encode); only
    a VIRTUAL input (raster None) is materialized via the front-door. Returns
    ``(new_bytes, cellid, dropped)`` where ``dropped`` is the count of corrupt
    members skipped, or ``None`` on an empty/all-corrupt array."""
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    elems = [t for t in tiles if t is not None and not _tile_is_empty(t)]
    if not elems:
        return None
    rasters = []
    good_elems = []
    dropped = 0
    for t in elems:
        try:
            vt = ot._to_virtual_tile(t)
            if vt.is_virtual():
                candidate = ot.materialize_to_bytes(vt).raster
            else:
                candidate = bytes(vt.raster)
            # Validate openability before collecting — corrupt bytes must be
            # dropped here, not inside agg_core which opens all at once.
            with _serde.open_tile(candidate):
                pass
            rasters.append(candidate)
            good_elems.append(t)
        except Exception:  # noqa: BLE001
            dropped += 1
            continue
    if not rasters:
        return None
    cellids = {_tile_cellid(t) for t in good_elems}
    cellid = _tile_cellid(good_elems[0]) if len(cellids) == 1 else -1
    return agg_core.combineavg_tiles(rasters), cellid, dropped


@f.udf(V2_TILE_SCHEMA)
def _combineavg_udf(tiles):
    if not tiles:
        return None
    result = _combineavg_bytes(tiles)
    if result is None:
        return None
    new_bytes, cellid, dropped = result
    tile = _serde.build_tile(new_bytes, "GTiff", cellid)
    if dropped:
        tile["metadata"][
            "last_error"
        ] = f"RST_CombineAvg: skipped {dropped} corrupt input tile(s)"
    return tile


@f.udf(V2_TILE_SCHEMA)
def _combineavg_v2_udf(tiles, virtualize_dir, virtualize_prefix, materialize):
    if not tiles:
        return None
    result = _combineavg_bytes(tiles)
    if result is None:
        return None
    new_bytes, cellid, dropped = result
    row = _shaped_result_row(
        new_bytes, cellid, virtualize_dir, virtualize_prefix, materialize
    )
    if row is not None and dropped:
        row["metadata"][
            "last_error"
        ] = f"RST_CombineAvg: skipped {dropped} corrupt input tile(s)"
    return row


@f.udf(V2_TILE_SCHEMA)
def _uf_combineavg(tiles, file_refs):
    """FILE-aware rst_combineavg: reads each input via its per-element FileRef."""
    if not tiles:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    frefs = list(file_refs) if file_refs else []
    while len(frefs) < len(tiles):
        frefs.append(None)

    rasters = []
    good_elems = []
    dropped = 0
    for t, fref in zip(tiles, frefs):
        if t is None or _tile_is_empty(t):
            continue
        try:
            vt = ot._to_virtual_tile(t)
            if vt.is_virtual():
                with ot._open(t, file_ref=fref) as ds:
                    candidate = _dataset_to_gtiff_bytes(ds)
            else:
                candidate = bytes(vt.raster)
            with _serde.open_tile(candidate):
                pass
            rasters.append(candidate)
            good_elems.append(t)
        except Exception:  # noqa: BLE001
            dropped += 1
            continue

    if not rasters:
        return None
    cellids = {_tile_cellid(t) for t in good_elems}
    cellid = _tile_cellid(good_elems[0]) if len(cellids) == 1 else -1
    new_bytes = agg_core.combineavg_tiles(rasters)
    if new_bytes is None:
        return None
    tile = _serde.build_tile(new_bytes, "GTiff", cellid)
    if dropped:
        tile["metadata"][
            "last_error"
        ] = f"RST_CombineAvg: skipped {dropped} corrupt input tile(s)"
    return tile


def rst_combineavg(
    tiles: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
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
        virtualize_dir:    Force-output (light-tier, Python API only): write the
            result to a durable path and return a light virtual tile.
        virtualize_prefix: Optional filename prefix for ``virtualize_dir``.
        materialize:       Force-output: ``True`` ensures raster bytes. Mutually
            exclusive with ``virtualize_dir``.

    Returns:
        Tile struct of per-pixel means, or NULL on an empty array.
    """
    if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
        _validate_force_output(virtualize_dir, materialize)
        return _combineavg_v2_udf(
            _col(tiles),
            *_force_output_lits(virtualize_dir, virtualize_prefix, materialize),
        )
    from databricks.labs.gbx.pyrx._file_ref import file_supported

    tc = _col(tiles)
    if file_supported():
        file_refs_col = f.transform(
            tc, lambda t: f.call_function("try_to_file", t["path"])
        )
        return _uf_combineavg(tc, file_refs_col)
    return _combineavg_udf(tc)


# rst_frombands: single ARRAY<single-band tile> arg -> multi-band tile.
# Mirrors gbx_rst_frombands: array ORDER is band order (element 0 -> band 1).
# Reuses core.agg.frombands_tiles by pairing each element with its 0-based
# position as the band_index, so the reducer's ascending sort preserves order.
def _frombands_bytes(bands):
    """Shared frombands body: pair each input's GTiff bytes with its 0-based
    position (band index), assemble, resolve cellid.

    Materialized inputs pass their ORIGINAL bytes verbatim (no re-encode); only
    a VIRTUAL input (raster None) is materialized via the front-door. Returns
    ``(new_bytes, cellid, dropped)`` where ``dropped`` is the count of corrupt
    members skipped, or ``None`` on an empty/all-corrupt array."""
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    indexed = []
    dropped = 0
    first_good = None
    for i, t in enumerate(bands):
        if t is None or _tile_is_empty(t):
            continue
        try:
            vt = ot._to_virtual_tile(t)
            candidate = (
                ot.materialize_to_bytes(vt).raster
                if vt.is_virtual()
                else bytes(vt.raster)
            )
            # Validate openability before collecting — corrupt bytes must be
            # dropped here, not inside agg_core which opens all at once.
            with _serde.open_tile(candidate):
                pass
            indexed.append((i, candidate))
            if first_good is None:
                first_good = t
        except Exception:  # noqa: BLE001
            dropped += 1
            continue
    if not indexed:
        return None
    cellid = _tile_cellid(first_good) if first_good is not None else 0
    return agg_core.frombands_tiles(indexed), cellid, dropped


@f.udf(V2_TILE_SCHEMA)
def _frombands_udf(bands):
    if not bands:
        return None
    result = _frombands_bytes(bands)
    if result is None:
        return None
    new_bytes, cellid, dropped = result
    tile = _serde.build_tile(new_bytes, "GTiff", cellid)
    if dropped:
        tile["metadata"][
            "last_error"
        ] = f"RST_FromBands: skipped {dropped} corrupt input tile(s)"
    return tile


@f.udf(V2_TILE_SCHEMA)
def _frombands_v2_udf(bands, virtualize_dir, virtualize_prefix, materialize):
    if not bands:
        return None
    result = _frombands_bytes(bands)
    if result is None:
        return None
    new_bytes, cellid, dropped = result
    row = _shaped_result_row(
        new_bytes, cellid, virtualize_dir, virtualize_prefix, materialize
    )
    if row is not None and dropped:
        row["metadata"][
            "last_error"
        ] = f"RST_FromBands: skipped {dropped} corrupt input tile(s)"
    return row


@f.udf(V2_TILE_SCHEMA)
def _uf_frombands(bands, file_refs):
    """FILE-aware rst_frombands: reads each input via its per-element FileRef.

    ``bands`` is ARRAY<tile struct>; ``file_refs`` is a parallel ARRAY<FileRef|null>
    (same length) minted in the plan via F.transform.  Materialized elements have
    file_refs[i]=None (try_to_file on NULL returns NULL); they fall through to verbatim
    bytes to preserve the sort-key invariant (no re-encode of materialized tiles).
    """
    if not bands:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    frefs = list(file_refs) if file_refs else []
    while len(frefs) < len(bands):
        frefs.append(None)

    indexed = []
    dropped = 0
    first_good = None
    for i, (t, fref) in enumerate(zip(bands, frefs)):
        if t is None or _tile_is_empty(t):
            continue
        try:
            vt = ot._to_virtual_tile(t)
            if vt.is_virtual():
                with ot._open(t, file_ref=fref) as ds:
                    candidate = _dataset_to_gtiff_bytes(ds)
            else:
                candidate = bytes(vt.raster)
            with _serde.open_tile(candidate):
                pass
            indexed.append((i, candidate))
            if first_good is None:
                first_good = t
        except Exception:  # noqa: BLE001
            dropped += 1
            continue

    if not indexed:
        return None
    cellid = _tile_cellid(first_good) if first_good is not None else 0
    new_bytes = agg_core.frombands_tiles(indexed)
    tile = _serde.build_tile(new_bytes, "GTiff", cellid)
    if dropped:
        tile["metadata"][
            "last_error"
        ] = f"RST_FromBands: skipped {dropped} corrupt input tile(s)"
    return tile


def rst_frombands(
    bands: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Assemble an ARRAY of single-band tiles into one multi-band tile.

    Mirrors the heavyweight ``gbx_rst_frombands``: ``bands`` is a single column
    of ARRAY<tile struct> and the ARRAY ORDER is the band order (element 0 ->
    band 1, element 1 -> band 2, ...). Georef/CRS/dtype/nodata are taken from
    the first element. Output ``cellid`` carries from the first element.

    Args:
        bands: Column of ARRAY<single-band tile struct>, in band order.
        virtualize_dir:    Force-output (light-tier, Python API only): write the
            result to a durable path and return a light virtual tile.
        virtualize_prefix: Optional filename prefix for ``virtualize_dir``.
        materialize:       Force-output: ``True`` ensures raster bytes. Mutually
            exclusive with ``virtualize_dir``.

    Returns:
        Multi-band tile struct, or NULL on an empty array.
    """
    if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
        _validate_force_output(virtualize_dir, materialize)
        return _frombands_v2_udf(
            _col(bands),
            *_force_output_lits(virtualize_dir, virtualize_prefix, materialize),
        )
    from databricks.labs.gbx.pyrx._file_ref import file_supported

    tc = _col(bands)
    if file_supported():
        file_refs_col = f.transform(
            tc, lambda t: f.call_function("try_to_file", t["path"])
        )
        return _uf_frombands(tc, file_refs_col)
    return _frombands_udf(tc)


# --- Tier 1b: tile-returning warp UDFs -------------------------------------
def _transform_bytes(tile, target_srid, file_ref=None):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    # CRITICAL: for a MATERIALIZED tile with an identity transform (source EPSG ==
    # target_srid), return the ORIGINAL raster bytes verbatim — no re-encode.
    # ``agg_core.merge_tiles`` sorts inputs on their RAW GTiff byte content to
    # choose a deterministic, heavy-parity last-wins overlap winner.  Calling
    # ``warp.reproject_to_srid`` even on the identity path opens the dataset and
    # re-serialises it through a rasterio MemoryFile write, which produces
    # byte-different output (header metadata may differ even though pixels are
    # identical) and shifts the sort key — potentially flipping the overlap winner
    # vs. the heavy tier.  For VIRTUAL tiles (no original bytes) there is nothing
    # to return verbatim, so they fall through to the normal warp path (which
    # itself also identity-short-circuits at the pixel level).
    vt = ot._to_virtual_tile(tile)
    if not vt.is_virtual() and vt.raster is not None:
        target_srid_int = int(target_srid)
        with ot._open(tile, file_ref=file_ref) as ds:
            src_epsg = ds.crs.to_epsg() if ds.crs else None
            if src_epsg is not None and src_epsg == target_srid_int:
                # Identity on a materialized tile: original bytes, sort key intact.
                return bytes(vt.raster)
            return warp.reproject_to_srid(ds, target_srid_int)
    # Virtual tile: no original bytes — open and reproject (identity-short-circuits inside).
    with ot._open(tile, file_ref=file_ref) as ds:
        return warp.reproject_to_srid(ds, int(target_srid))


@f.udf(V2_TILE_SCHEMA)
def _transform_udf(tile, target_srid):
    if _tile_is_empty(tile):
        return None
    new_bytes = _transform_bytes(tile, target_srid)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
def _transform_v2_udf(
    tile, target_srid, virtualize_dir, virtualize_prefix, materialize
):
    if _tile_is_empty(tile):
        return None
    new_bytes = _transform_bytes(tile, target_srid)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


@f.udf(V2_TILE_SCHEMA)
def _uf_transform(tile, file_ref, target_srid):
    if _tile_is_empty(tile):
        return None
    new_bytes = _transform_bytes(tile, target_srid, file_ref=file_ref)
    if new_bytes is None:
        return None
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


def _to_webmercator_bytes(tile, resampling, file_ref=None):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile, file_ref=file_ref) as ds:
        return warp.reproject_to_srid(ds, 3857, resampling=str(resampling))


@f.udf(V2_TILE_SCHEMA)
def _to_webmercator_udf(tile, resampling):
    if _tile_is_empty(tile):
        return None
    new_bytes = _to_webmercator_bytes(tile, resampling)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
def _to_webmercator_v2_udf(
    tile, resampling, virtualize_dir, virtualize_prefix, materialize
):
    if _tile_is_empty(tile):
        return None
    new_bytes = _to_webmercator_bytes(tile, resampling)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


@f.udf(V2_TILE_SCHEMA)
def _uf_to_webmercator(tile, file_ref, resampling):
    if _tile_is_empty(tile):
        return None
    new_bytes = _to_webmercator_bytes(tile, resampling, file_ref=file_ref)
    if new_bytes is None:
        return None
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


def rst_transform(
    tile: ColLike,
    srid: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Reproject the raster to the target SRID (EPSG code).

    Identity (``srid`` == the source CRS's EPSG code) is a passthrough:
    no resample, no re-encode; the tile stays a reference/passthrough (so
    ``virtualize_dir`` is a no-op on an already-virtual input). A non-identity
    reproject PRODUCES new pixels and materializes; pass ``virtualize_dir`` to
    write the reprojected result to a durable path and get a light virtual row.

    Force-output (light-tier, Python API only): ``virtualize_dir`` writes the
    result to a durable path and returns a light virtual tile; ``materialize=True``
    forces raster bytes (mutually exclusive with ``virtualize_dir``).
    """
    if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
        _validate_force_output(virtualize_dir, materialize)
        return _transform_v2_udf(
            _col(tile),
            _col(srid),
            *_force_output_lits(virtualize_dir, virtualize_prefix, materialize),
        )
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_transform(tc, file_ref_arg(tc), _col(srid))


def rst_to_webmercator(
    tile: ColLike,
    resampling: ColLike = "bilinear",
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Reproject the tile to EPSG:3857 (web mercator). resampling defaults to 'bilinear'.

    Force-output (light-tier, Python API only): ``virtualize_dir`` writes the
    result to a durable path and returns a light virtual tile; ``materialize=True``
    forces raster bytes (mutually exclusive with ``virtualize_dir``).
    """
    resampling_col = (
        f.lit(resampling) if isinstance(resampling, str) else _col(resampling)
    )
    if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
        _validate_force_output(virtualize_dir, materialize)
        return _to_webmercator_v2_udf(
            _col(tile),
            resampling_col,
            *_force_output_lits(virtualize_dir, virtualize_prefix, materialize),
        )
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_to_webmercator(tc, file_ref_arg(tc), resampling_col)


# --- Tier 1c: tile-returning resample UDFs ----------------------------------
def _resample_bytes(tile, factor, algorithm, file_ref=None):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile, file_ref=file_ref) as ds:
        return resample.resample_by_factor(ds, float(factor), str(algorithm))


@f.udf(V2_TILE_SCHEMA)
def _resample_udf(tile, factor, algorithm):
    if _tile_is_empty(tile):
        return None
    new_bytes = _resample_bytes(tile, factor, algorithm)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
def _resample_v2_udf(
    tile, factor, algorithm, virtualize_dir, virtualize_prefix, materialize
):
    if _tile_is_empty(tile):
        return None
    new_bytes = _resample_bytes(tile, factor, algorithm)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


@f.udf(V2_TILE_SCHEMA)
def _uf_resample(tile, file_ref, factor, algorithm):
    if _tile_is_empty(tile):
        return None
    new_bytes = _resample_bytes(tile, factor, algorithm, file_ref=file_ref)
    if new_bytes is None:
        return None
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


def _resample_to_size_bytes(tile, width_px, height_px, algorithm, file_ref=None):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile, file_ref=file_ref) as ds:
        return resample.resample_to_size(
            ds, int(width_px), int(height_px), str(algorithm)
        )


@f.udf(V2_TILE_SCHEMA)
def _resample_to_size_udf(tile, width_px, height_px, algorithm):
    if _tile_is_empty(tile):
        return None
    new_bytes = _resample_to_size_bytes(tile, width_px, height_px, algorithm)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
def _resample_to_size_v2_udf(
    tile, width_px, height_px, algorithm, virtualize_dir, virtualize_prefix, materialize
):
    if _tile_is_empty(tile):
        return None
    new_bytes = _resample_to_size_bytes(tile, width_px, height_px, algorithm)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


@f.udf(V2_TILE_SCHEMA)
def _uf_resample_to_size(tile, file_ref, width_px, height_px, algorithm):
    if _tile_is_empty(tile):
        return None
    new_bytes = _resample_to_size_bytes(
        tile, width_px, height_px, algorithm, file_ref=file_ref
    )
    if new_bytes is None:
        return None
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


def _resample_to_res_bytes(tile, x_res, y_res, algorithm, file_ref=None):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile, file_ref=file_ref) as ds:
        return resample.resample_to_res(ds, float(x_res), float(y_res), str(algorithm))


@f.udf(V2_TILE_SCHEMA)
def _resample_to_res_udf(tile, x_res, y_res, algorithm):
    if _tile_is_empty(tile):
        return None
    new_bytes = _resample_to_res_bytes(tile, x_res, y_res, algorithm)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
def _resample_to_res_v2_udf(
    tile, x_res, y_res, algorithm, virtualize_dir, virtualize_prefix, materialize
):
    if _tile_is_empty(tile):
        return None
    new_bytes = _resample_to_res_bytes(tile, x_res, y_res, algorithm)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


@f.udf(V2_TILE_SCHEMA)
def _uf_resample_to_res(tile, file_ref, x_res, y_res, algorithm):
    if _tile_is_empty(tile):
        return None
    new_bytes = _resample_to_res_bytes(tile, x_res, y_res, algorithm, file_ref=file_ref)
    if new_bytes is None:
        return None
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


def rst_resample(
    tile: ColLike,
    factor: ColLike,
    algorithm: ColLike = "bilinear",
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Resample a raster tile by a multiplicative factor (>1 upsamples, 0<factor<1 downsamples).

    CRS and geographic extent are preserved; only the pixel grid changes.

    Force-output (light-tier, Python API only): ``virtualize_dir`` writes the
    result to a durable path and returns a light virtual tile; ``materialize=True``
    forces raster bytes (mutually exclusive with ``virtualize_dir``).
    """
    alg = f.lit(algorithm) if isinstance(algorithm, str) else _col(algorithm)
    if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
        _validate_force_output(virtualize_dir, materialize)
        return _resample_v2_udf(
            _col(tile),
            _col(factor),
            alg,
            *_force_output_lits(virtualize_dir, virtualize_prefix, materialize),
        )
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_resample(tc, file_ref_arg(tc), _col(factor), alg)


def rst_resample_to_size(
    tile: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    algorithm: ColLike = "bilinear",
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Resample a raster tile to exact pixel dimensions (width_px x height_px).

    CRS and geographic extent are preserved; only the pixel grid changes.

    Force-output (light-tier, Python API only): ``virtualize_dir`` writes the
    result to a durable path and returns a light virtual tile; ``materialize=True``
    forces raster bytes (mutually exclusive with ``virtualize_dir``).
    """
    alg = f.lit(algorithm) if isinstance(algorithm, str) else _col(algorithm)
    if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
        _validate_force_output(virtualize_dir, materialize)
        return _resample_to_size_v2_udf(
            _col(tile),
            _col(width_px),
            _col(height_px),
            alg,
            *_force_output_lits(virtualize_dir, virtualize_prefix, materialize),
        )
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_resample_to_size(
        tc, file_ref_arg(tc), _col(width_px), _col(height_px), alg
    )


def rst_resample_to_res(
    tile: ColLike,
    x_res: ColLike,
    y_res: ColLike,
    algorithm: ColLike = "bilinear",
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Resample a raster tile to a target ground resolution in CRS units.

    CRS and geographic extent are preserved; pixel count is derived from extent / resolution.

    Force-output (light-tier, Python API only): ``virtualize_dir`` writes the
    result to a durable path and returns a light virtual tile; ``materialize=True``
    forces raster bytes (mutually exclusive with ``virtualize_dir``).
    """
    alg = f.lit(algorithm) if isinstance(algorithm, str) else _col(algorithm)
    if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
        _validate_force_output(virtualize_dir, materialize)
        return _resample_to_res_v2_udf(
            _col(tile),
            _col(x_res),
            _col(y_res),
            alg,
            *_force_output_lits(virtualize_dir, virtualize_prefix, materialize),
        )
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_resample_to_res(tc, file_ref_arg(tc), _col(x_res), _col(y_res), alg)


# --- Tier 1d: tile-returning edit UDFs -------------------------------------
def _clip_bytes(tile, geom_wkb, all_touched, clip_crs=None):
    """Shared clip body: open the (virtual or materialized) tile, clip to the
    parsed cutline, return the clipped GTiff bytes (or None on no-overlap).

    ``clip_crs`` is a source-CRS override for a plain WKB/WKT cutline (int SRID or
    CRS string incl. ESRI/WKT); an EWKB/EWKT embedded SRID wins per-geom."""
    from databricks.labs.gbx._geom import parse_geom
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    # parse_geom keeps the SRID (EWKT/EWKB carry it) so clip_to_geom can
    # reproject the cutline to the raster CRS, mirroring heavy RST_Clip.
    geom = parse_geom(geom_wkb)
    if geom is None:
        return None
    with ot._open(tile) as ds:
        return edit.clip_to_geom(ds, geom, bool(all_touched), geom_crs=clip_crs)


@f.udf(V2_TILE_SCHEMA)
def _clip_udf(tile, geom_wkb, all_touched, clip_crs=None):
    if _tile_is_empty(tile) or geom_wkb is None:
        return None
    new_bytes = _clip_bytes(tile, geom_wkb, all_touched, clip_crs)
    if new_bytes is None:
        # Cutline does not overlap the raster -> null tile (no crash), mirroring
        # heavy GDAL Warp -cutline producing an empty result.
        return None
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
def _clip_v2_udf(
    tile,
    geom_wkb,
    all_touched,
    clip_crs,
    virtualize_dir,
    virtualize_prefix,
    materialize,
):
    # Force-output variant (Python API only): same clip math, but the produced
    # bytes are shaped via shape_output. Returns the 8-field v2 tile envelope.
    if _tile_is_empty(tile) or geom_wkb is None:
        return None
    new_bytes = _clip_bytes(tile, geom_wkb, all_touched, clip_crs)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


@f.udf(V2_TILE_SCHEMA)
def _uf_clip(tile, file_ref, geom_wkb, all_touched, clip_crs):
    """FILE-aware rst_clip: reads input tile via file_ref, then clips.

    C1 guard in open_tile: FILE fast-path applies when tile.clip_polygon is None
    (normal for rst_clip input) AND no warp is pending.  The explicit geom_wkb
    cutline is applied AFTER the tile bytes are read (not a tile-level clip_polygon).
    """
    if _tile_is_empty(tile) or geom_wkb is None:
        return None
    try:
        from databricks.labs.gbx._geom import parse_geom
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        geom = parse_geom(geom_wkb)
        if geom is None:
            return None
        with ot._open(tile, file_ref=file_ref) as ds:
            new_bytes = edit.clip_to_geom(
                ds, geom, bool(all_touched), geom_crs=clip_crs
            )
        if new_bytes is None:
            return None
        return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))
    except Exception:  # noqa: BLE001
        return None


def _update_type_bytes(tile, new_type, file_ref=None):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile, file_ref=file_ref) as ds:
        return edit.update_type(ds, str(new_type))


@f.udf(V2_TILE_SCHEMA)
def _update_type_udf(tile, new_type):
    if _tile_is_empty(tile):
        return None
    new_bytes = _update_type_bytes(tile, new_type)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
def _update_type_v2_udf(tile, new_type, virtualize_dir, virtualize_prefix, materialize):
    if _tile_is_empty(tile):
        return None
    new_bytes = _update_type_bytes(tile, new_type)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


@f.udf(V2_TILE_SCHEMA)
def _uf_update_type(tile, file_ref, new_type):
    if _tile_is_empty(tile):
        return None
    new_bytes = _update_type_bytes(tile, new_type, file_ref=file_ref)
    if new_bytes is None:
        return None
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


def _init_nodata_bytes(tile):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile) as ds:
        return edit.init_nodata(ds)


@f.udf(V2_TILE_SCHEMA)
def _init_nodata_udf(tile):
    if _tile_is_empty(tile):
        return None
    vt = ot._to_virtual_tile(tile)
    if vt.is_virtual():
        # record pending instruction; stay virtual (no pixel read)
        md = dict(vt.metadata or {})
        md.setdefault(ot.PENDING_NODATA, str(edit._DEFAULT_NODATA))
        vt.metadata = md
        return vt.to_row()
    # materialized input: apply eagerly to bytes (today's behavior), emit v2
    # materialized inputs carry no pending_* keys (invariant), so metadata is already clean
    new_bytes = _init_nodata_bytes(tile)
    return VirtualTile(
        cellid=_tile_cellid(tile), raster=new_bytes, metadata=dict(vt.metadata or {})
    ).to_row()


@f.udf(V2_TILE_SCHEMA)
def _init_nodata_v2_udf(tile, virtualize_dir, virtualize_prefix, materialize):
    if _tile_is_empty(tile):
        return None
    new_bytes = _init_nodata_bytes(tile)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


@f.udf(V2_TILE_SCHEMA)
def _uf_initnodata(tile, file_ref):
    """FILE-aware rst_initnodata.

    Virtual tile: records a pending_nodata instruction; stays virtual (no pixel
    read; file_ref unused — the pending path avoids reading entirely).
    Materialized tile: applies init_nodata via ot._open (file_ref=file_ref threads
    through open_tile; C1 guard auto-handles clip/warp cases).
    """
    if _tile_is_empty(tile):
        return None
    vt = ot._to_virtual_tile(tile)
    if vt.is_virtual():
        # Pending-instruction path: record intent, stay virtual (no pixel read).
        md = dict(vt.metadata or {})
        md.setdefault(ot.PENDING_NODATA, str(edit._DEFAULT_NODATA))
        vt.metadata = md
        return vt.to_row()
    # Materialized: apply eagerly; file_ref is passed through open_tile.
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile, file_ref=file_ref) as ds:
        new_bytes = edit.init_nodata(ds)
    return VirtualTile(
        cellid=_tile_cellid(tile), raster=new_bytes, metadata=dict(vt.metadata or {})
    ).to_row()


def rst_clip(
    tile: ColLike,
    geom: ColLike,
    cutline_all_touched: ColLike,
    clip_crs: ColLike = None,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Clip the raster to a geometry (WKB, EWKB, WKT, or EWKT). cutline_all_touched includes pixels touched by the boundary.

    ``clip_crs`` declares the source CRS of a plain WKB/WKT cutline (int SRID or
    CRS string — ``EPSG:x`` / ``ESRI:x`` / WKT); an EWKB/EWKT embedded SRID wins,
    and a cutline with neither is assumed already in the raster CRS (never errors).

    Force-output (light-tier only, Python API only): ``virtualize_dir`` writes
    the clipped result to a durable path and returns a light virtual tile;
    ``materialize=True`` forces raster bytes. ``virtualize_dir`` and
    ``materialize=True`` are mutually exclusive. When neither is set the SQL
    registered signature is used unchanged (materialized tile struct).
    """
    crs_col = f.lit(clip_crs) if clip_crs is not None else f.lit(None)
    if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
        _validate_force_output(virtualize_dir, materialize)
        return _clip_v2_udf(
            _col(tile),
            _col(geom),
            _col(cutline_all_touched),
            crs_col,
            f.lit(virtualize_dir),
            f.lit(virtualize_prefix),
            f.lit(materialize),
        )
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_clip(
        tc, file_ref_arg(tc), _col(geom), _col(cutline_all_touched), crs_col
    )


def rst_updatetype(
    tile: ColLike,
    new_type: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Cast all raster bands to a new GDAL data type (e.g. 'Int32', 'Float64').

    Force-output (light-tier, Python API only): ``virtualize_dir`` writes the
    result to a durable path and returns a light virtual tile; ``materialize=True``
    forces raster bytes (mutually exclusive with ``virtualize_dir``).
    """
    if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
        _validate_force_output(virtualize_dir, materialize)
        return _update_type_v2_udf(
            _col(tile),
            _col(new_type),
            *_force_output_lits(virtualize_dir, virtualize_prefix, materialize),
        )
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_update_type(tc, file_ref_arg(tc), _col(new_type))


def rst_initnodata(
    tile: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Ensure a NoData value is set on the raster tile; uses -9999.0 if not already set.

    Force-output (light-tier, Python API only): ``virtualize_dir`` writes the
    result to a durable path and returns a light virtual tile; ``materialize=True``
    forces raster bytes (mutually exclusive with ``virtualize_dir``).
    """
    if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
        _validate_force_output(virtualize_dir, materialize)
        return _init_nodata_v2_udf(
            _col(tile),
            *_force_output_lits(virtualize_dir, virtualize_prefix, materialize),
        )
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_initnodata(tc, file_ref_arg(tc))


# --- Tier 1d6: operations UDFs (tryopen, setsrid, band, asformat, ----------
# buildoverviews, sample) ----------------------------------------------------
@f.udf(BooleanType())
def _tryopen_udf(tile):
    if _tile_is_empty(tile):
        return False
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    # Virtual-aware: a materialized tile validates its bytes directly; a virtual
    # tile is proven openable through the front-door (any failure -> False).
    vt = ot._to_virtual_tile(tile)
    if not vt.is_virtual():
        return ops_core.try_open(bytes(vt.raster))
    try:
        with ot._open(vt):
            return True
    except Exception:
        return False


def _setsrid_bytes(tile, srid):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile) as ds:
        return edit.set_srid(ds, int(srid))


@f.udf(V2_TILE_SCHEMA)
def _setsrid_udf(tile, srid):
    if _tile_is_empty(tile) or srid is None:
        return None
    vt = ot._to_virtual_tile(tile)
    s = int(srid)
    if s <= 0:
        raise ValueError(f"rst_setsrid requires a positive EPSG code; got {s}")
    if vt.is_virtual():
        md = dict(vt.metadata or {})
        md[ot.PENDING_SRID] = str(s)
        # Remove any stale pending_crs so this int SRID is the sole authority
        # (mirrors _setcrs_udf popping pending_srid). Without this, a prior
        # rst_setcrs's pending_crs would supersede and silently ignore this call.
        md.pop(ot.PENDING_CRS, None)
        vt.metadata = md
        return vt.to_row()
    # materialized: apply eagerly to bytes, emit v2
    # materialized inputs carry no pending_* keys (invariant), so metadata is already clean
    new_bytes = _setsrid_bytes(tile, s)
    return VirtualTile(
        cellid=_tile_cellid(tile), raster=new_bytes, metadata=dict(vt.metadata or {})
    ).to_row()


@f.udf(V2_TILE_SCHEMA)
def _setsrid_v2_udf(tile, srid, virtualize_dir, virtualize_prefix, materialize):
    if _tile_is_empty(tile) or srid is None:
        return None
    new_bytes = _setsrid_bytes(tile, srid)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


@f.udf(V2_TILE_SCHEMA)
def _uf_setsrid(tile, file_ref, srid):
    """FILE-aware rst_setsrid (pending-instruction path for virtual tiles)."""
    if _tile_is_empty(tile) or srid is None:
        return None
    vt = ot._to_virtual_tile(tile)
    s = int(srid)
    if s <= 0:
        raise ValueError(f"rst_setsrid requires a positive EPSG code; got {s}")
    if vt.is_virtual():
        md = dict(vt.metadata or {})
        md[ot.PENDING_SRID] = str(s)
        md.pop(ot.PENDING_CRS, None)
        vt.metadata = md
        return vt.to_row()
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile, file_ref=file_ref) as ds:
        new_bytes = edit.set_srid(ds, s)
    return VirtualTile(
        cellid=_tile_cellid(tile), raster=new_bytes, metadata=dict(vt.metadata or {})
    ).to_row()


# --- rst_setcrs: string-taking CRS relabel (pending-instruction on virtual) --
def _setcrs_bytes(tile, crs_value):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile) as ds:
        return edit.set_crs(ds, crs_value)


@f.udf(V2_TILE_SCHEMA)
def _setcrs_udf(tile, crs_value):
    if _tile_is_empty(tile) or crs_value is None:
        return None
    from databricks.labs.gbx.pyrx.core.crs import crs_to_canonical, resolve_crs

    canonical = crs_to_canonical(resolve_crs(str(crs_value)))
    vt = ot._to_virtual_tile(tile)
    if vt.is_virtual():
        md = dict(vt.metadata or {})
        md[ot.PENDING_CRS] = canonical
        # Remove any stale pending_srid so the canonical CRS string is sole authority.
        md.pop(ot.PENDING_SRID, None)
        vt.metadata = md
        return vt.to_row()
    # Materialized: apply eagerly to bytes.
    new_bytes = _setcrs_bytes(tile, str(crs_value))
    return VirtualTile(
        cellid=_tile_cellid(tile), raster=new_bytes, metadata=dict(vt.metadata or {})
    ).to_row()


@f.udf(V2_TILE_SCHEMA)
def _uf_setcrs(tile, file_ref, crs_value):
    """FILE-aware rst_setcrs (pending-instruction path for virtual tiles)."""
    if _tile_is_empty(tile) or crs_value is None:
        return None
    from databricks.labs.gbx.pyrx.core.crs import crs_to_canonical, resolve_crs

    canonical = crs_to_canonical(resolve_crs(str(crs_value)))
    vt = ot._to_virtual_tile(tile)
    if vt.is_virtual():
        md = dict(vt.metadata or {})
        md[ot.PENDING_CRS] = canonical
        md.pop(ot.PENDING_SRID, None)
        vt.metadata = md
        return vt.to_row()
    # Materialized: apply eagerly via file_ref.
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile, file_ref=file_ref) as ds:
        new_bytes = edit.set_crs(ds, str(crs_value))
    return VirtualTile(
        cellid=_tile_cellid(tile), raster=new_bytes, metadata=dict(vt.metadata or {})
    ).to_row()


# --- rst_transformcrs: string-taking reproject (non-EPSG targets) -------------
def _transformcrs_bytes(tile, crs_value, file_ref=None):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile, file_ref=file_ref) as ds:
        return warp.reproject_to_crs(ds, str(crs_value))


@f.udf(V2_TILE_SCHEMA)
def _transformcrs_udf(tile, crs_value):
    if _tile_is_empty(tile) or crs_value is None:
        return None
    new_bytes = _transformcrs_bytes(tile, crs_value)
    return VirtualTile(
        cellid=_tile_cellid(tile), raster=new_bytes, metadata={}
    ).to_row()


@f.udf(V2_TILE_SCHEMA)
def _uf_transformcrs(tile, file_ref, crs_value):
    if _tile_is_empty(tile) or crs_value is None:
        return None
    new_bytes = _transformcrs_bytes(tile, crs_value, file_ref=file_ref)
    if new_bytes is None:
        return None
    return VirtualTile(
        cellid=_tile_cellid(tile), raster=new_bytes, metadata={}
    ).to_row()


def _band_bytes(tile, band_index):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile) as ds:
        return edit.band(ds, int(band_index))


@f.udf(V2_TILE_SCHEMA)
def _band_udf(tile, band_index):
    if _tile_is_empty(tile) or band_index is None:
        return None
    vt = ot._to_virtual_tile(tile)
    b = int(band_index)
    if b < 1:
        raise ValueError(f"rst_band: band_index {b} out of range (>=1)")
    if vt.is_virtual():
        # Guard: stacking a second band-select on a tile that already carries
        # pending_bands would silently compose indices or error with a confusing
        # out-of-range message.  Raise an explicit error instead.
        if ot.PENDING_BANDS in (vt.metadata or {}):
            raise ValueError(
                "rst_band: tile already has a pending band selection; "
                "materialize before selecting again"
            )
        md = dict(vt.metadata or {})
        md[ot.PENDING_BANDS] = str(b)
        vt.metadata = md
        return vt.to_row()
    # materialized: apply eagerly to bytes, emit v2
    # materialized inputs carry no pending_* keys (invariant), so metadata is already clean
    new_bytes = _band_bytes(tile, b)
    return VirtualTile(
        cellid=_tile_cellid(tile), raster=new_bytes, metadata=dict(vt.metadata or {})
    ).to_row()


@f.udf(V2_TILE_SCHEMA)
def _band_v2_udf(tile, band_index, virtualize_dir, virtualize_prefix, materialize):
    if _tile_is_empty(tile) or band_index is None:
        return None
    new_bytes = _band_bytes(tile, band_index)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


def _asformat_bytes(tile, new_format):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile) as ds:
        return ops_core.as_format(ds, str(new_format))


@f.udf(V2_TILE_SCHEMA)
def _asformat_udf(tile, new_format):
    if _tile_is_empty(tile) or new_format is None:
        return None
    new_bytes = _asformat_bytes(tile, new_format)
    # metadata.driver must reflect the requested output format.
    return _serde.build_tile(new_bytes, str(new_format), _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
def _asformat_v2_udf(tile, new_format, virtualize_dir, virtualize_prefix, materialize):
    if _tile_is_empty(tile) or new_format is None:
        return None
    new_bytes = _asformat_bytes(tile, new_format)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


def _buildoverviews_bytes(tile, levels, resampling):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    resamp = "average" if resampling is None else str(resampling)
    with ot._open(tile) as ds:
        return ops_core.build_overviews(ds, list(levels), resamp)


@f.udf(V2_TILE_SCHEMA)
def _buildoverviews_udf(tile, levels, resampling):
    if _tile_is_empty(tile):
        return None
    new_bytes = _buildoverviews_bytes(tile, levels, resampling)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
def _buildoverviews_v2_udf(
    tile, levels, resampling, virtualize_dir, virtualize_prefix, materialize
):
    if _tile_is_empty(tile):
        return None
    new_bytes = _buildoverviews_bytes(tile, levels, resampling)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


@f.udf(ArrayType(DoubleType()))
def _sample_udf(tile, geom_wkb, crs=None):
    # PIXEL accessor: needs the window pixels, so use the virtual-aware
    # _tile_is_empty guard (a virtual tile has raster None but a path -> NOT
    # empty) and open via ot._open below. A bytes-only guard would drop a
    # virtual input before ever materialising its window.
    if _tile_is_empty(tile) or geom_wkb is None:
        return None
    from databricks.labs.gbx._geom import parse_geom
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    # parse_geom keeps the SRID (EWKT/EWKB carry it) so ops_core.sample can
    # reproject the point to the raster CRS, mirroring heavy intent. `crs` is a
    # source-CRS override for a plain WKB/WKT point (ignored when EWKB carries one).
    geom = parse_geom(geom_wkb)
    if geom is None:
        return None
    with ot._open(tile) as ds:
        return ops_core.sample(ds, geom, geom_crs=crs)


def _proximity_bytes(tile, target_values, distunits, max_distance):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    units = "GEO" if distunits is None else str(distunits)
    tv = None if target_values is None else str(target_values)
    md = None if max_distance is None else float(max_distance)
    with ot._open(tile) as ds:
        return analysis_core.proximity(ds, tv, units, md)


@f.udf(V2_TILE_SCHEMA)
def _proximity_udf(tile, target_values, distunits, max_distance):
    if _tile_is_empty(tile):
        return None
    new_bytes = _proximity_bytes(tile, target_values, distunits, max_distance)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
def _proximity_v2_udf(
    tile,
    target_values,
    distunits,
    max_distance,
    virtualize_dir,
    virtualize_prefix,
    materialize,
):
    if _tile_is_empty(tile):
        return None
    new_bytes = _proximity_bytes(tile, target_values, distunits, max_distance)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


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
    if _tile_is_empty(tile):
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    lvls = [] if levels is None else [float(v) for v in levels if v is not None]
    iv = 0.0 if interval is None else float(interval)
    bs = 0.0 if base is None else float(base)
    attr = "elev" if attr_field is None else str(attr_field)
    with ot._open(tile) as ds:
        return analysis_core.contour(ds, lvls, iv, bs, attr)


def _viewshed_bytes(
    tile, observer_geom, observer_height, target_height, max_distance, crs=None
):
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
    with ot._open(tile) as ds:
        # CRS-align the observer (Rule 1): embedded SRID wins; else the explicit
        # `crs` (int SRID or CRS string incl. ESRI/WKT); else assume aligned. When
        # a source CRS is known and the raster has a CRS, reproject so a 4326
        # observer over a UTM DEM lands correctly. Transform failure -> use as-is.
        import shapely as _shapely

        from databricks.labs.gbx.pyrx.core.crs import (
            get_transformer,
            resolve_source_crs,
        )

        ox, oy = geom.x, geom.y
        src_crs = resolve_source_crs(_shapely.get_srid(geom), crs=crs)
        if src_crs is not None and ds.crs is not None:
            try:
                if src_crs != ds.crs:
                    ox, oy = get_transformer(src_crs, ds.crs).transform(ox, oy)
            except Exception:
                ox, oy = geom.x, geom.y
        return analysis_core.viewshed(ds, ox, oy, oh, th, md)


@f.udf(V2_TILE_SCHEMA)
def _viewshed_udf(
    tile, observer_geom, observer_height, target_height, max_distance, crs=None
):
    if _tile_is_empty(tile) or observer_geom is None:
        return None
    new_bytes = _viewshed_bytes(
        tile, observer_geom, observer_height, target_height, max_distance, crs
    )
    if new_bytes is None:
        return None
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
def _viewshed_v2_udf(
    tile,
    observer_geom,
    observer_height,
    target_height,
    max_distance,
    crs,
    virtualize_dir,
    virtualize_prefix,
    materialize,
):
    if _tile_is_empty(tile) or observer_geom is None:
        return None
    new_bytes = _viewshed_bytes(
        tile, observer_geom, observer_height, target_height, max_distance, crs
    )
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


def _cog_convert_bytes(tile, compression, blocksize, overview_resampling):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    comp = "DEFLATE" if compression is None else str(compression)
    bs = 512 if blocksize is None else int(blocksize)
    resamp = "AVERAGE" if overview_resampling is None else str(overview_resampling)
    with ot._open(tile) as ds:
        return analysis_core.cog_convert(ds, comp, bs, resamp)


@f.udf(V2_TILE_SCHEMA)
def _cog_convert_udf(tile, compression, blocksize, overview_resampling):
    if _tile_is_empty(tile):
        return None
    new_bytes = _cog_convert_bytes(tile, compression, blocksize, overview_resampling)
    # COG is a GTiff variant on disk; downstream readers see driver "GTiff".
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
def _cog_convert_v2_udf(
    tile,
    compression,
    blocksize,
    overview_resampling,
    virtualize_dir,
    virtualize_prefix,
    materialize,
):
    if _tile_is_empty(tile):
        return None
    new_bytes = _cog_convert_bytes(tile, compression, blocksize, overview_resampling)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


def rst_tryopen(tile: ColLike) -> Column:
    """Return BOOLEAN: True if the raster bytes open as a valid dataset.

    Mirrors the heavyweight ``gbx_rst_tryopen`` — any failure to open (corrupt
    bytes, unknown format) yields False rather than raising.
    """
    return _tryopen_udf(_col(tile))


def rst_setsrid(
    tile: ColLike,
    srid: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Stamp the CRS as ``EPSG:<srid>`` WITHOUT reprojecting the pixels.

    Equivalent to ``gdal_edit.py -a_srs``: pixel values and the GeoTransform
    are unchanged; only the CRS metadata is rewritten. Use ``rst_transform``
    for an actual reprojecting warp.

    Args:
        tile: Tile struct column.
        srid: Positive EPSG code to stamp.
        virtualize_dir:    Force-output (light-tier, Python API only): write the
            result to a durable path and return a light virtual tile.
        virtualize_prefix: Optional filename prefix for ``virtualize_dir``.
        materialize:       Force-output: ``True`` ensures raster bytes. Mutually
            exclusive with ``virtualize_dir``.

    Returns:
        Tile with the same pixels/transform but CRS = EPSG:srid.
    """
    if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
        _validate_force_output(virtualize_dir, materialize)
        return _setsrid_v2_udf(
            _col(tile),
            _col(srid),
            *_force_output_lits(virtualize_dir, virtualize_prefix, materialize),
        )
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_setsrid(tc, file_ref_arg(tc), _col(srid))


def rst_setcrs(
    tile: ColLike,
    crs: ColLike,
) -> Column:
    """Stamp the CRS WITHOUT reprojecting, accepting any CRS string.

    Accepts an int EPSG code, an int-castable string (``"4326"``), or any
    string accepted by ``rasterio.crs.CRS.from_user_input`` such as
    ``"ESRI:54008"``, WKT, or PROJ4 strings.  Pixel values and the
    GeoTransform are unchanged; only the CRS metadata is rewritten.  Use
    ``rst_transformcrs`` for an actual reprojecting warp.

    On a virtual tile the CRS relabel is recorded as a pending instruction
    (``pending_crs`` in metadata) and applied at the next read.  A pending
    ``pending_crs`` supersedes any stale ``pending_srid``.

    Note: equivalent to ``rst_setsrid`` when ``crs`` is an int-castable
    string (``"4326"`` == ``rst_setsrid(tile, 4326)``).

    Args:
        tile: Tile struct column.
        crs:  CRS descriptor — int EPSG code, int-castable string, authority
              string (``"EPSG:4326"``, ``"ESRI:54008"``), WKT, or PROJ4.

    Returns:
        Tile with the same pixels/transform but the new CRS.
    """
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_setcrs(tc, file_ref_arg(tc), _crs_col(crs))


def rst_transformcrs(
    tile: ColLike,
    crs: ColLike,
) -> Column:
    """Reproject the raster to a string-given target CRS.

    Accepts any CRS descriptor accepted by ``rasterio.crs.CRS.from_user_input``:
    int EPSG, int-castable string, authority string (``"EPSG:3857"``,
    ``"ESRI:54008"``), WKT, or PROJ4.  Unlike ``rst_transform`` (int EPSG only)
    this supports non-EPSG targets such as ESRI codes or custom projections.

    Pixel-producing: always materializes (a new reprojected tile is emitted).

    Args:
        tile: Tile struct column.
        crs:  Target CRS descriptor — int EPSG, int-castable string, authority
              string, WKT, or PROJ4.

    Returns:
        Tile reprojected to the target CRS.
    """
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_transformcrs(tc, file_ref_arg(tc), _crs_col(crs))


def rst_band(
    tile: ColLike,
    band_index: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Extract a single 1-based band as a new single-band tile.

    Equivalent to ``gdal_translate -b <band_index>``: the extracted tile
    preserves the source CRS, GeoTransform, nodata, and dtype; only the band
    count is reduced to 1. ``band_index`` is 1-based and must be in range.

    On a virtual tile the band selection is recorded as a pending instruction
    (``pending_bands`` in metadata) and applied at the next read.  Stacking a
    second ``rst_band`` call on a tile that already carries a pending band
    selection raises ``ValueError``; materialize first (``materialize=True``)
    before selecting again.

    Args:
        tile:       Tile struct column.
        band_index: 1-based band index in ``[1 .. numbands]``.
        virtualize_dir:    Force-output (light-tier, Python API only): write the
            result to a durable path and return a light virtual tile.
        virtualize_prefix: Optional filename prefix for ``virtualize_dir``.
        materialize:       Force-output: ``True`` ensures raster bytes. Mutually
            exclusive with ``virtualize_dir``.

    Returns:
        Single-band tile struct.
    """
    if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
        _validate_force_output(virtualize_dir, materialize)
        return _band_v2_udf(
            _col(tile),
            _col(band_index),
            *_force_output_lits(virtualize_dir, virtualize_prefix, materialize),
        )
    return _band_udf(_col(tile), _col(band_index))


def rst_asformat(
    tile: ColLike,
    new_format: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Re-encode the raster to another GDAL driver (e.g. 'PNG', 'GTiff').

    Mirrors the heavyweight ``gbx_rst_asformat``: the output tile's raster
    bytes are encoded in ``new_format`` and the tile metadata ``driver``
    reflects it. Raises if the requested driver is unavailable in this GDAL
    build.

    Args:
        tile:       Tile struct column.
        new_format: GDAL driver short name (e.g. 'GTiff', 'PNG').
        virtualize_dir:    Force-output (light-tier, Python API only): write the
            result to a durable path and return a light virtual tile.
        virtualize_prefix: Optional filename prefix for ``virtualize_dir``.
        materialize:       Force-output: ``True`` ensures raster bytes. Mutually
            exclusive with ``virtualize_dir``.

    Returns:
        Tile struct whose raster bytes are encoded in ``new_format``.
    """
    fmt = f.lit(new_format) if isinstance(new_format, str) else _col(new_format)
    if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
        _validate_force_output(virtualize_dir, materialize)
        return _asformat_v2_udf(
            _col(tile),
            fmt,
            *_force_output_lits(virtualize_dir, virtualize_prefix, materialize),
        )
    return _asformat_udf(_col(tile), fmt)


def rst_buildoverviews(
    tile: ColLike,
    levels: ColLike,
    resampling: ColLike = "average",
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
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
        virtualize_dir:    Force-output (light-tier, Python API only): write the
            result to a durable path and return a light virtual tile.
        virtualize_prefix: Optional filename prefix for ``virtualize_dir``.
        materialize:       Force-output: ``True`` ensures raster bytes. Mutually
            exclusive with ``virtualize_dir``.

    Returns:
        Tile struct with internal overviews embedded.
    """
    resamp = f.lit(resampling) if isinstance(resampling, str) else _col(resampling)
    if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
        _validate_force_output(virtualize_dir, materialize)
        return _buildoverviews_v2_udf(
            _col(tile),
            _col(levels),
            resamp,
            *_force_output_lits(virtualize_dir, virtualize_prefix, materialize),
        )
    return _buildoverviews_udf(_col(tile), _col(levels), resamp)


def rst_proximity(
    tile: ColLike,
    target_values: ColLike = None,
    dist_units: ColLike = "GEO",
    max_distance: ColLike = None,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
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
        dist_units:    ``"GEO"`` (default; CRS ground units, scaled by pixel
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
        if dist_units is None
        else (f.lit(dist_units) if isinstance(dist_units, str) else _col(dist_units))
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
    if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
        _validate_force_output(virtualize_dir, materialize)
        return _proximity_v2_udf(
            _col(tile),
            tv_col,
            units_col,
            md_col,
            *_force_output_lits(virtualize_dir, virtualize_prefix, materialize),
        )
    return _proximity_udf(_col(tile), tv_col, units_col, md_col)


def rst_cog_convert(
    tile: ColLike,
    compression: ColLike = "AUTO",
    blocksize: ColLike = 512,
    overview_resampling: ColLike = "AVERAGE",
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Convert a raster tile to a Cloud Optimized GeoTIFF (COG) layout.

    Mirrors the heavyweight ``gbx_rst_cog_convert`` (``gdal.Translate -of COG``),
    implemented via GDAL's native ``driver="COG"``.  The output tile's raster
    bytes are COG-layout GTiff; downstream readers see ``metadata.driver =
    "GTiff"`` (COG is a GTiff variant).

    Args:
        tile:                Tile struct column.
        compression:         COG compression (default "AUTO" = size-adaptive
                             ZSTD + dtype predictor; or a codec name: ZSTD,
                             DEFLATE, LZW, WEBP, JPEG, LERC, RAW).
        blocksize:           Internal tile size in pixels, square (default 512).
        overview_resampling: Overview-pyramid resampling (default "AVERAGE").
        virtualize_dir:    Force-output (light-tier, Python API only): write the
            result to a durable path and return a light virtual tile.
        virtualize_prefix: Optional filename prefix for ``virtualize_dir``.
        materialize:       Force-output: ``True`` ensures raster bytes. Mutually
            exclusive with ``virtualize_dir``.

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
    if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
        _validate_force_output(virtualize_dir, materialize)
        return _cog_convert_v2_udf(
            _col(tile),
            comp_col,
            bs_col,
            resamp_col,
            *_force_output_lits(virtualize_dir, virtualize_prefix, materialize),
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
    crs: ColLike = None,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Compute a binary viewshed (255 visible / 0 invisible) from a DEM tile.

    Mirrors the heavyweight ``gbx_rst_viewshed`` (GDAL ViewshedGenerate),
    implemented with ``xrspatial.viewshed``. ``crs`` declares the source CRS of a
    plain WKB/WKT observer point (int SRID or CRS string incl. ESRI/WKT); an
    EWKB/EWKT embedded SRID wins; absent + no SRID -> assumed in the raster CRS.

    Args:
        tile:            Tile struct column (the DEM).
        observer_geom:   POINT observer location in the raster's CRS, as WKB
                         (BINARY) or WKT (STRING). Non-POINT geometries raise.
        observer_height: Observer height above the DEM (>= 0). Defaults to 0.0.
        target_height:   Target height above the DEM at each tested cell (>= 0).
                         Defaults to 0.0.
        max_distance:    Optional analysis-distance cap in CRS ground units
                         (> 0). ``None`` = unlimited.
        virtualize_dir:    Force-output (light-tier, Python API only): write the
            result to a durable path and return a light virtual tile.
        virtualize_prefix: Optional filename prefix for ``virtualize_dir``.
        materialize:       Force-output: ``True`` ensures raster bytes. Mutually
            exclusive with ``virtualize_dir``.

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
    crs_col = f.lit(crs) if crs is not None else f.lit(None)
    if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
        _validate_force_output(virtualize_dir, materialize)
        return _viewshed_v2_udf(
            _col(tile),
            _col(observer_geom),
            oh,
            th,
            md,
            crs_col,
            *_force_output_lits(virtualize_dir, virtualize_prefix, materialize),
        )
    if crs is None:
        return _viewshed_udf(_col(tile), _col(observer_geom), oh, th, md)
    return _viewshed_udf(_col(tile), _col(observer_geom), oh, th, md, crs_col)


def rst_sample(tile: ColLike, geom: ColLike, crs: ColLike = None) -> Column:
    """Sample per-band raster values at a POINT geometry (WKB, EWKB, WKT, or EWKT).

    Mirrors the heavyweight ``gbx_rst_sample``: requires a POINT geometry
    (raises otherwise), reprojects the point to the raster CRS, and returns
    ARRAY<DOUBLE> with one value per band in band order. Points outside the
    raster extent return null.

    Args:
        tile: Tile struct column.
        geom: POINT geometry as WKB, EWKB, WKT, or EWKT.
        crs:  Optional source-CRS for a plain WKB/WKT point (int SRID or CRS
              string — ``EPSG:x`` / ``ESRI:x`` / WKT). An EWKB/EWKT embedded
              SRID wins; absent + no SRID -> assumed already in the raster CRS.

    Returns:
        ARRAY<DOUBLE>: one value per band, or null if the point is out of extent.
    """
    if crs is None:
        return _sample_udf(_col(tile), _col(geom))
    return _sample_udf(_col(tile), _col(geom), f.lit(crs))


# --- Tier 1d3: band-math / focal UDFs --------------------------------------
def _threshold_bytes(tile, op, value, file_ref=None):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile, file_ref=file_ref) as ds:
        return edit.threshold(ds, op, value)


@f.udf(V2_TILE_SCHEMA)
def _threshold_udf(tile, op, value):
    if _tile_is_empty(tile):
        return None
    new_bytes = _threshold_bytes(tile, op, value)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
def _threshold_v2_udf(tile, op, value, virtualize_dir, virtualize_prefix, materialize):
    if _tile_is_empty(tile):
        return None
    new_bytes = _threshold_bytes(tile, op, value)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


@f.udf(V2_TILE_SCHEMA)
def _uf_threshold(tile, file_ref, op, value):
    if _tile_is_empty(tile):
        return None
    new_bytes = _threshold_bytes(tile, op, value, file_ref=file_ref)
    if new_bytes is None:
        return None
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


def _filter_bytes(tile, kernel_size, operation):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile) as ds:
        return focal.filt(ds, int(kernel_size), str(operation))


@f.udf(V2_TILE_SCHEMA)
def _filter_udf(tile, kernel_size, operation):
    if _tile_is_empty(tile):
        return None
    new_bytes = _filter_bytes(tile, kernel_size, operation)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
def _filter_v2_udf(
    tile, kernel_size, operation, virtualize_dir, virtualize_prefix, materialize
):
    if _tile_is_empty(tile):
        return None
    new_bytes = _filter_bytes(tile, kernel_size, operation)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


def _convolve_bytes(tile, kernel):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile) as ds:
        return focal.convolve(ds, kernel)


@f.udf(V2_TILE_SCHEMA)
def _convolve_udf(tile, kernel):
    if _tile_is_empty(tile):
        return None
    new_bytes = _convolve_bytes(tile, kernel)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
def _convolve_v2_udf(tile, kernel, virtualize_dir, virtualize_prefix, materialize):
    if _tile_is_empty(tile):
        return None
    new_bytes = _convolve_bytes(tile, kernel)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


def rst_threshold(
    tile: ColLike,
    op: ColLike = None,
    value: ColLike = None,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Keep pixels satisfying the comparison; others become NoData.

    Args:
        tile:  Tile struct column.
        op:    Comparison operator: ">", "<", ">=", "<=", "==", "!=".
               Defaults to ">".
        value: Threshold scalar.  Defaults to 0.0.
        virtualize_dir:    Force-output (light-tier, Python API only): write the
            result to a durable path and return a light virtual tile.
        virtualize_prefix: Optional filename prefix for ``virtualize_dir``.
        materialize:       Force-output: ``True`` ensures raster bytes. Mutually
            exclusive with ``virtualize_dir``.

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
    if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
        _validate_force_output(virtualize_dir, materialize)
        return _threshold_v2_udf(
            _col(tile),
            op_col,
            val_col,
            *_force_output_lits(virtualize_dir, virtualize_prefix, materialize),
        )
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_threshold(tc, file_ref_arg(tc), op_col, val_col)


def rst_filter(
    tile: ColLike,
    kernel_size: ColLike,
    operation: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Apply a focal filter over a square window per band.

    Args:
        tile:        Tile struct column.
        kernel_size: Side length of the square neighbourhood (odd integer).
        operation:   One of "min", "max", "mean", "median".
        virtualize_dir:    Force-output (light-tier, Python API only): write the
            result to a durable path and return a light virtual tile.
        virtualize_prefix: Optional filename prefix for ``virtualize_dir``.
        materialize:       Force-output: ``True`` ensures raster bytes. Mutually
            exclusive with ``virtualize_dir``.

    Returns:
        Filtered tile; same band count.  "mean" returns Float32; others
        preserve the input dtype.
    """
    if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
        _validate_force_output(virtualize_dir, materialize)
        return _filter_v2_udf(
            _col(tile),
            _col(kernel_size),
            _col(operation),
            *_force_output_lits(virtualize_dir, virtualize_prefix, materialize),
        )
    return _filter_udf(_col(tile), _col(kernel_size), _col(operation))


def rst_convolve(
    tile: ColLike,
    kernel: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Convolve each band with a 2-D kernel (ARRAY<ARRAY<DOUBLE>>).

    Args:
        tile:   Tile struct column.
        kernel: 2-D array column of floats (e.g. built with ``f.array``).
        virtualize_dir:    Force-output (light-tier, Python API only): write the
            result to a durable path and return a light virtual tile.
        virtualize_prefix: Optional filename prefix for ``virtualize_dir``.
        materialize:       Force-output: ``True`` ensures raster bytes. Mutually
            exclusive with ``virtualize_dir``.

    Returns:
        Convolved tile with dtype Float64.
    """
    if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
        _validate_force_output(virtualize_dir, materialize)
        return _convolve_v2_udf(
            _col(tile),
            _col(kernel),
            *_force_output_lits(virtualize_dir, virtualize_prefix, materialize),
        )
    return _convolve_udf(_col(tile), _col(kernel))


# --- Tier 1d4: map algebra UDF ----------------------------------------------
def _mapalgebra_bytes(tiles, expression):
    """Shared map-algebra body: collect each input tile's GTiff bytes (array
    order preserved), evaluate the expression, return the produced GTiff bytes.

    Materialized inputs pass their ORIGINAL bytes verbatim (no re-encode).
    Virtual inputs use ``_tile_to_bytes`` (one ZSTD encode) instead of
    ``materialize_to_bytes`` (two ZSTD encode/decode cycles), avoiding the
    redundant compress→decompress→compress round-trip that ``materialize_to_bytes``
    performs when the caller only needs bytes for a downstream open (e.g.
    mapalgebra_core opens each input with its own MemoryFile)."""
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    elems = [t for t in tiles if t is not None and not _tile_is_empty(t)]
    if not elems:
        return None
    rasters = []
    for t in elems:
        vt = ot._to_virtual_tile(t)
        rasters.append(ot._tile_to_bytes(vt))
    return mapalgebra_core.mapalgebra(rasters, str(expression))


@f.udf(V2_TILE_SCHEMA)
def _mapalgebra_udf(tiles, expression):
    if tiles is None or expression is None:
        return None
    new_bytes = _mapalgebra_bytes(tiles, expression)
    if new_bytes is None:
        return None
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tiles[0]))


@f.udf(V2_TILE_SCHEMA)
def _mapalgebra_v2_udf(
    tiles, expression, virtualize_dir, virtualize_prefix, materialize
):
    if tiles is None or expression is None:
        return None
    new_bytes = _mapalgebra_bytes(tiles, expression)
    return _shaped_result_row(
        new_bytes,
        _tile_cellid(tiles[0]) if tiles else 0,
        virtualize_dir,
        virtualize_prefix,
        materialize,
    )


@f.udf(V2_TILE_SCHEMA)
def _uf_mapalgebra(tiles, file_refs, expression):
    """FILE-aware rst_mapalgebra: reads each input via its per-element FileRef.

    ``tiles`` is ARRAY<tile struct>; ``file_refs`` is a parallel ARRAY<FileRef|null>
    minted in the plan via F.transform.  Materialized elements (file_refs[i]=None)
    fall through to verbatim bytes.

    Deliberately no per-element try/except (unlike _uf_merge/_uf_combineavg/_uf_frombands
    which drop corrupt elements and continue): mapalgebra requires ALL inputs because a
    missing element shifts the band-index mapping (A[0] changes meaning when the array
    is shorter).  A corrupt input raises rather than silently changing the expression
    semantics.
    """
    if tiles is None or expression is None:
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    frefs = list(file_refs) if file_refs else []
    while len(frefs) < len(tiles):
        frefs.append(None)

    elems = [
        (t, fref)
        for t, fref in zip(tiles, frefs)
        if t is not None and not _tile_is_empty(t)
    ]
    if not elems:
        return None

    rasters = []
    first_tile = None
    for t, fref in elems:
        vt = ot._to_virtual_tile(t)
        if vt.is_virtual():
            with ot._open(t, file_ref=fref) as ds:
                rasters.append(_dataset_to_gtiff_bytes(ds))
        else:
            rasters.append(bytes(vt.raster))
        if first_tile is None:
            first_tile = t

    new_bytes = mapalgebra_core.mapalgebra(rasters, str(expression))
    if new_bytes is None:
        return None
    return _serde.build_tile(
        new_bytes, "GTiff", _tile_cellid(first_tile) if first_tile else 0
    )


def rst_mapalgebra(
    tiles: ColLike,
    json_spec: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Apply a map-algebra expression across an array of tiles.

    By default band 1 of each tile (in array order) binds to A, B, C, …; the
    expression is evaluated with numexpr (safe math only — no arbitrary code
    execution). Output is a single-band Float32 tile on the first input's
    georeference.

    Args:
        tiles:     Column of ARRAY<tile struct> (e.g. ``f.array("ta", "tb")``).
        json_spec: the map-algebra expression. Accepts either the gdal_calc JSON
            envelope the heavy/SQL tier uses (e.g. ``'{"calc": "(A - B)/(A + B)"}'``)
            or a bare numexpr string (e.g. ``"(A - B) / (A + B)"``). The envelope's
            per-variable keys ``A_index`` / ``A_band`` (and ``B_``, ``C_``, …)
            select the raster (0-based, into ``tiles``) and 1-based band each
            variable reads — so NDVI from bands 4 (NIR) and 3 (Red) of ONE
            multiband tile is ``'{"calc": "(A-B)/(A+B)", "A_index": 0,
            "B_index": 0, "A_band": 4, "B_band": 3}'`` (no need to decompose the
            raster). Only ``extra_options`` (gdal_calc CLI flags) is unsupported
            here and raises.
        virtualize_dir:    Force-output (light-tier, Python API only): write the
            result to a durable path and return a light virtual tile.
        virtualize_prefix: Optional filename prefix for ``virtualize_dir``.
        materialize:       Force-output: ``True`` ensures raster bytes. Mutually
            exclusive with ``virtualize_dir``.

    Returns:
        Single-band Float32 tile struct.
    """
    expr_col = f.lit(json_spec) if isinstance(json_spec, str) else _col(json_spec)
    if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
        _validate_force_output(virtualize_dir, materialize)
        return _mapalgebra_v2_udf(
            _col(tiles),
            expr_col,
            *_force_output_lits(virtualize_dir, virtualize_prefix, materialize),
        )
    from databricks.labs.gbx.pyrx._file_ref import file_supported

    tc = _col(tiles)
    if file_supported():
        file_refs_col = f.transform(
            tc, lambda t: f.call_function("try_to_file", t["path"])
        )
        return _uf_mapalgebra(tc, file_refs_col, expr_col)
    return _mapalgebra_udf(tc, expr_col)


# --- Tier 1d3: generic named-index dispatcher (rst_index) -------------------
def _index_bytes(tile, formula_name, band_map):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile) as ds:
        return indices.index(ds, str(formula_name), dict(band_map or {}))


@f.udf(V2_TILE_SCHEMA)
def _index_udf(tile, formula_name, band_map):
    if _tile_is_empty(tile) or formula_name is None:
        return None
    new_bytes = _index_bytes(tile, formula_name, band_map)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
def _index_v2_udf(
    tile, formula_name, band_map, virtualize_dir, virtualize_prefix, materialize
):
    if _tile_is_empty(tile) or formula_name is None:
        return None
    new_bytes = _index_bytes(tile, formula_name, band_map)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


def rst_index(
    tile: ColLike,
    formula_name: ColLike,
    band_map: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
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
        virtualize_dir:    Force-output (light-tier, Python API only): write the
            result to a durable path and return a light virtual tile.
        virtualize_prefix: Optional filename prefix for ``virtualize_dir``.
        materialize:       Force-output: ``True`` ensures raster bytes. Mutually
            exclusive with ``virtualize_dir``.

    Returns:
        Single-band Float32 tile struct.
    """
    name_col = (
        f.lit(formula_name) if isinstance(formula_name, str) else _col(formula_name)
    )
    if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
        _validate_force_output(virtualize_dir, materialize)
        return _index_v2_udf(
            _col(tile),
            name_col,
            _col(band_map),
            *_force_output_lits(virtualize_dir, virtualize_prefix, materialize),
        )
    return _index_udf(_col(tile), name_col, _col(band_map))


# --- Tier 1d2: spectral index UDFs -----------------------------------------
def _ndvi_bytes(tile, red_band, nir_band):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile) as ds:
        return indices.ndvi(ds, int(red_band), int(nir_band))


@f.udf(V2_TILE_SCHEMA)
def _ndvi_udf(tile, red_band, nir_band):
    if _tile_is_empty(tile):
        return None
    new_bytes = _ndvi_bytes(tile, red_band, nir_band)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
def _ndvi_v2_udf(
    tile, red_band, nir_band, virtualize_dir, virtualize_prefix, materialize
):
    if _tile_is_empty(tile):
        return None
    new_bytes = _ndvi_bytes(tile, red_band, nir_band)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


def _ndwi_bytes(tile, green_idx, nir_idx):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile) as ds:
        return indices.ndwi(ds, int(green_idx), int(nir_idx))


@f.udf(V2_TILE_SCHEMA)
def _ndwi_udf(tile, green_idx, nir_idx):
    if _tile_is_empty(tile):
        return None
    new_bytes = _ndwi_bytes(tile, green_idx, nir_idx)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
def _ndwi_v2_udf(
    tile, green_idx, nir_idx, virtualize_dir, virtualize_prefix, materialize
):
    if _tile_is_empty(tile):
        return None
    new_bytes = _ndwi_bytes(tile, green_idx, nir_idx)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


def _nbr_bytes(tile, nir_idx, swir_idx):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile) as ds:
        return indices.nbr(ds, int(nir_idx), int(swir_idx))


@f.udf(V2_TILE_SCHEMA)
def _nbr_udf(tile, nir_idx, swir_idx):
    if _tile_is_empty(tile):
        return None
    new_bytes = _nbr_bytes(tile, nir_idx, swir_idx)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
def _nbr_v2_udf(
    tile, nir_idx, swir_idx, virtualize_dir, virtualize_prefix, materialize
):
    if _tile_is_empty(tile):
        return None
    new_bytes = _nbr_bytes(tile, nir_idx, swir_idx)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


def _savi_bytes(tile, red_idx, nir_idx, l_val):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile) as ds:
        return indices.savi(ds, int(red_idx), int(nir_idx), l=float(l_val))


@f.udf(V2_TILE_SCHEMA)
def _savi_udf(tile, red_idx, nir_idx, l_val):
    if _tile_is_empty(tile):
        return None
    new_bytes = _savi_bytes(tile, red_idx, nir_idx, l_val)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
def _savi_v2_udf(
    tile, red_idx, nir_idx, l_val, virtualize_dir, virtualize_prefix, materialize
):
    if _tile_is_empty(tile):
        return None
    new_bytes = _savi_bytes(tile, red_idx, nir_idx, l_val)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


def _evi_bytes(tile, red_idx, nir_idx, blue_idx, l_val, c1, c2, g):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile) as ds:
        return indices.evi(
            ds,
            int(red_idx),
            int(nir_idx),
            int(blue_idx),
            l=float(l_val),
            c1=float(c1),
            c2=float(c2),
            g=float(g),
        )


@f.udf(V2_TILE_SCHEMA)
def _evi_udf(tile, red_idx, nir_idx, blue_idx, l_val, c1, c2, g):
    if _tile_is_empty(tile):
        return None
    new_bytes = _evi_bytes(tile, red_idx, nir_idx, blue_idx, l_val, c1, c2, g)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
def _evi_v2_udf(
    tile,
    red_idx,
    nir_idx,
    blue_idx,
    l_val,
    c1,
    c2,
    g,
    virtualize_dir,
    virtualize_prefix,
    materialize,
):
    if _tile_is_empty(tile):
        return None
    new_bytes = _evi_bytes(tile, red_idx, nir_idx, blue_idx, l_val, c1, c2, g)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


def _index_family_wrapper(
    udf_v1, udf_v2, tile, arg_cols, virtualize_dir, virtualize_prefix, materialize
):
    """Shared dispatch for the fixed-index-family wrappers (ndvi/ndwi/nbr)."""
    if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
        _validate_force_output(virtualize_dir, materialize)
        return udf_v2(
            _col(tile),
            *arg_cols,
            *_force_output_lits(virtualize_dir, virtualize_prefix, materialize),
        )
    return udf_v1(_col(tile), *arg_cols)


def rst_ndvi(
    tile: ColLike,
    red_idx: ColLike,
    nir_idx: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Compute NDVI = (NIR - Red) / (NIR + Red); single-band Float32 tile.

    Force-output (light-tier, Python API only): ``virtualize_dir`` / ``materialize``.
    """
    return _index_family_wrapper(
        _ndvi_udf,
        _ndvi_v2_udf,
        tile,
        (_col(red_idx), _col(nir_idx)),
        virtualize_dir,
        virtualize_prefix,
        materialize,
    )


def rst_ndwi(
    tile: ColLike,
    green_idx: ColLike,
    nir_idx: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Compute NDWI = (Green - NIR) / (Green + NIR); single-band Float32 tile.

    Force-output (light-tier, Python API only): ``virtualize_dir`` / ``materialize``.
    """
    return _index_family_wrapper(
        _ndwi_udf,
        _ndwi_v2_udf,
        tile,
        (_col(green_idx), _col(nir_idx)),
        virtualize_dir,
        virtualize_prefix,
        materialize,
    )


def rst_nbr(
    tile: ColLike,
    nir_idx: ColLike,
    swir_idx: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Compute NBR = (NIR - SWIR) / (NIR + SWIR); single-band Float32 tile.

    Force-output (light-tier, Python API only): ``virtualize_dir`` / ``materialize``.
    """
    return _index_family_wrapper(
        _nbr_udf,
        _nbr_v2_udf,
        tile,
        (_col(nir_idx), _col(swir_idx)),
        virtualize_dir,
        virtualize_prefix,
        materialize,
    )


def rst_savi(
    tile: ColLike,
    red_idx: ColLike,
    nir_idx: ColLike,
    l: ColLike = 0.5,  # noqa: E741
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Compute SAVI = (NIR - Red) / (NIR + Red + L) * (1 + L); single-band Float32 tile.

    Force-output (light-tier, Python API only): ``virtualize_dir`` / ``materialize``.
    """
    l_col = f.lit(l) if isinstance(l, (int, float)) else _col(l)
    return _index_family_wrapper(
        _savi_udf,
        _savi_v2_udf,
        tile,
        (_col(red_idx), _col(nir_idx), l_col),
        virtualize_dir,
        virtualize_prefix,
        materialize,
    )


def rst_evi(  # noqa: E741
    tile: ColLike,
    red_idx: ColLike,
    nir_idx: ColLike,
    blue_idx: ColLike,
    l: ColLike = 1.0,
    c1: ColLike = 6.0,
    c2: ColLike = 7.5,
    g: ColLike = 2.5,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Compute EVI = G * (NIR - Red) / (NIR + C1*Red - C2*Blue + L); single-band Float32 tile.

    Force-output (light-tier, Python API only): ``virtualize_dir`` / ``materialize``.
    """
    l_col = f.lit(l) if isinstance(l, (int, float)) else _col(l)
    c1_col = f.lit(c1) if isinstance(c1, (int, float)) else _col(c1)
    c2_col = f.lit(c2) if isinstance(c2, (int, float)) else _col(c2)
    g_col = f.lit(g) if isinstance(g, (int, float)) else _col(g)
    return _index_family_wrapper(
        _evi_udf,
        _evi_v2_udf,
        tile,
        (_col(red_idx), _col(nir_idx), _col(blue_idx), l_col, c1_col, c2_col, g_col),
        virtualize_dir,
        virtualize_prefix,
        materialize,
    )


# --- Tier 1e: constructor + fill UDFs (vector bridge) -----------------------
@f.udf(V2_TILE_SCHEMA)
def _rasterize_udf(
    geom_wkb, value, xmin, ymin, xmax, ymax, width_px, height_px, out_srid, out_crs=None
):
    if geom_wkb is None:
        return None
    from databricks.labs.gbx._geom import geom_to_wkb
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    new_bytes = features.rasterize_geom(
        geom_to_wkb(geom_wkb),
        value,
        xmin,
        ymin,
        xmax,
        ymax,
        width_px,
        height_px,
        out_srid=out_srid,
        out_crs=out_crs,
    )
    return _serde.build_tile(new_bytes, "GTiff", 0)


def rst_rasterize(
    geom: ColLike,
    value: ColLike,
    xmin: ColLike,
    ymin: ColLike,
    xmax: ColLike,
    ymax: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    out_srid: ColLike = None,
    out_crs: ColLike = None,
) -> Column:
    """Burn a geometry (WKB, EWKB, WKT, or EWKT) into a new raster tile at the given extent/size.

    Output CRS: ``out_crs`` (string — ``EPSG:x`` / ``ESRI:x`` / WKT) wins over the
    int ``out_srid``; both set -> error; neither -> the geometry's own source CRS
    is carried through. The geometry is reprojected from its source CRS (EWKB
    embedded SRID) into the output CRS before burning.
    """
    out_crs_col = f.lit(out_crs) if out_crs is not None else f.lit(None)
    return _rasterize_udf(
        _col(geom),
        _col(value),
        _col(xmin),
        _col(ymin),
        _col(xmax),
        _col(ymax),
        _col(width_px),
        _col(height_px),
        _col(out_srid) if out_srid is not None else f.lit(None),
        out_crs_col,
    )


def _fillnodata_bytes(tile, max_search_dist, smoothing_iter):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile) as ds:
        return features.fill_nodata(ds, max_search_dist, smoothing_iter)


@f.udf(V2_TILE_SCHEMA)
def _fillnodata_udf(tile, max_search_dist, smoothing_iter):
    if _tile_is_empty(tile):
        return None
    new_bytes = _fillnodata_bytes(tile, max_search_dist, smoothing_iter)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
def _fillnodata_v2_udf(
    tile,
    max_search_dist,
    smoothing_iter,
    virtualize_dir,
    virtualize_prefix,
    materialize,
):
    if _tile_is_empty(tile):
        return None
    new_bytes = _fillnodata_bytes(tile, max_search_dist, smoothing_iter)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


def rst_fillnodata(
    tile: ColLike,
    max_search_dist: ColLike = None,
    smoothing_iter: ColLike = None,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Interpolate across NoData gaps in the raster.

    Force-output (light-tier, Python API only): ``virtualize_dir`` / ``materialize``.
    """
    msd = f.lit(None) if max_search_dist is None else _col(max_search_dist)
    smi = f.lit(None) if smoothing_iter is None else _col(smoothing_iter)
    if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
        _validate_force_output(virtualize_dir, materialize)
        return _fillnodata_v2_udf(
            _col(tile),
            msd,
            smi,
            *_force_output_lits(virtualize_dir, virtualize_prefix, materialize),
        )
    return _fillnodata_udf(_col(tile), msd, smi)


# --- Tier 1e3: TIN / IDW constructors (point-array -> tile) -----------------
@f.udf(V2_TILE_SCHEMA)
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
    out_crs=None,
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
        out_srid=None if srid is None else int(srid),
        power=2.0 if power is None else float(power),
        max_pts=12 if max_pts is None else int(max_pts),
        out_crs=out_crs,
    )
    return _serde.build_tile(new_bytes, "GTiff", 0)


def rst_gridfrompoints(
    points_array: ColLike,
    values_array: ColLike,
    xmin: ColLike,
    ymin: ColLike,
    xmax: ColLike,
    ymax: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    out_srid: ColLike = None,
    power: ColLike = 2.0,
    max_pts: ColLike = 12,
    out_crs: ColLike = None,
) -> Column:
    """Inverse-distance-weighted (IDW) grid from an ARRAY of POINT WKB + values.

    ``points_array`` is ARRAY<BINARY> (WKB points), ``values_array`` is the parallel
    ARRAY<DOUBLE>. For each output cell center the value is the inverse-distance
    weighted mean of the nearest ``max_pts`` points (weight = 1/distance**power);
    a point coincident with a cell center returns that value. Output is a
    single-band Float64 tile over ``[xmin,ymin,xmax,ymax]`` at
    ``width_px x height_px``; NoData = -9999.0.

    Output CRS: ``out_crs`` (string) wins over the int ``out_srid``; both set ->
    error; neither -> CRS-less. The input points are assumed to be in the output
    CRS (this is a label, not a reprojection).

    Args:
        points_array:    ARRAY<BINARY> of WKB POINT geometries.
        values_array:    ARRAY<DOUBLE> parallel to ``points_array``.
        xmin..ymax: Output extent in CRS units.
        width_px, height_px: Output raster size in pixels.
        out_srid:  EPSG or ESRI code for the output CRS.
        out_crs:   CRS string for the output (``EPSG:x`` / ``ESRI:x`` / WKT).
        power:     IDW exponent (default 2.0).
        max_pts:   Max neighbours per cell (default 12).

    Returns:
        Single-band Float64 tile struct.
    """
    p = f.lit(power) if isinstance(power, (int, float)) else _col(power)
    m = f.lit(max_pts) if isinstance(max_pts, (int, float)) else _col(max_pts)
    return _gridfrompoints_udf(
        _col(points_array),
        _col(values_array),
        _col(xmin),
        _col(ymin),
        _col(xmax),
        _col(ymax),
        _col(width_px),
        _col(height_px),
        _col(out_srid) if out_srid is not None else f.lit(None),
        p,
        m,
        f.lit(out_crs) if out_crs is not None else f.lit(None),
    )


@f.udf(V2_TILE_SCHEMA)
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
    out_crs=None,
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
        out_srid=None if srid is None else int(srid),
        no_data=-9999.0 if no_data is None else float(no_data),
        out_crs=out_crs,
    )
    return _serde.build_tile(new_bytes, "GTiff", 0)


def rst_dtmfromgeoms(
    points_array: ColLike,
    breaklines_array: ColLike,
    merge_tolerance: ColLike,
    snap_tolerance: ColLike,
    xmin: ColLike,
    ymin: ColLike,
    xmax: ColLike,
    ymax: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    out_srid: ColLike = None,
    no_data: ColLike = -9999.0,
    out_crs: ColLike = None,
) -> Column:
    """Delaunay-TIN DTM from Z-valued POINT WKB (+ optional breaklines).

    ``points_array`` is ARRAY<BINARY> of WKB POINTs WITH Z; ``breaklines_array`` is
    ARRAY<BINARY> of WKB linestrings (may be null/empty). A Delaunay
    triangulation of the points' (x, y) is built and Z is barycentrically
    interpolated at each output cell center. Cells outside the convex hull
    become ``no_data``. Output is a single-band Float64 tile over the extent at
    ``width_px x height_px``.

    Output CRS: ``out_crs`` (string) wins over the int ``out_srid``; both set ->
    error; neither -> CRS-less. Input points are assumed already in the output
    CRS (label, not a reprojection).

    PARITY DIVERGENCE: the lightweight tier performs an UNCONSTRAINED Delaunay
    interpolation. ``breaklines_array`` are accepted but NOT enforced as hard edges
    (their vertices are folded in as extra triangulation points only), and
    ``merge_tolerance`` / ``snap_tolerance`` are accepted for signature parity
    but have no effect.

    Args:
        points_array:    ARRAY<BINARY> of WKB POINT-with-Z geometries.
        breaklines_array: ARRAY<BINARY> of WKB linestrings (accepted, not enforced).
        merge_tolerance: Accepted for parity; not applied.
        snap_tolerance:  Accepted for parity; not applied.
        xmin..ymax:      Output extent in CRS units.
        width_px, height_px: Output raster size in pixels.
        out_srid:        EPSG or ESRI code for the output CRS.
        out_crs:         CRS string for the output (``EPSG:x`` / ``ESRI:x`` / WKT).
        no_data:         NoData sentinel (default -9999.0).

    Returns:
        Single-band Float64 tile struct.
    """
    nd = f.lit(no_data) if isinstance(no_data, (int, float)) else _col(no_data)
    return _dtmfromgeoms_udf(
        _col(points_array),
        _col(breaklines_array),
        _col(merge_tolerance),
        _col(snap_tolerance),
        _col(xmin),
        _col(ymin),
        _col(xmax),
        _col(ymax),
        _col(width_px),
        _col(height_px),
        _col(out_srid) if out_srid is not None else f.lit(None),
        nd,
        f.lit(out_crs) if out_crs is not None else f.lit(None),
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
        if _tile_is_empty(tile):
            return
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        with ot._open(tile) as ds:
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
# Each UDTF row IS the tile struct (V2_TILE_SCHEMA: 8-field v2 tile with cellid, raster, metadata, etc).


@udtf(returnType=V2_TILE_SCHEMA)
class _RstSeparateBandsUDTF:
    """Streaming UDTF: yield one single-band tile struct per band."""

    def eval(self, tile, file_ref=None):
        if _tile_is_empty(tile):
            yield _serde.build_error_tile("RST_SeparateBands: empty or unreadable tile")
            return
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        # NOTE: a mid-iteration failure yields already-emitted good rows + one error row
        # (not exactly 1); accepted because fully-corrupt open-failure is the primary case.
        try:
            with ot._open(tile, file_ref=file_ref) as ds:
                for i, b in enumerate(tiling.iter_separate_bands(ds)):
                    yield _serde.build_tile(b, "GTiff", i)
        except Exception as e:  # noqa: BLE001
            yield _serde.build_error_tile(f"RST_SeparateBands: {e}")
            return


@udtf(returnType=V2_TILE_SCHEMA)
class _RstRetileUDTF:
    """Streaming UDTF: yield one sub-tile struct per non-overlapping window."""

    def eval(self, tile, tile_width, tile_height, file_ref=None):
        if _tile_is_empty(tile):
            yield _serde.build_error_tile("RST_ReTile: empty or unreadable tile")
            return
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        # NOTE: mid-iteration failure yields already-emitted good rows + one error row.
        try:
            with ot._open(tile, file_ref=file_ref) as ds:
                for i, b in enumerate(
                    tiling.iter_retile(ds, int(tile_width), int(tile_height))
                ):
                    yield _serde.build_tile(b, "GTiff", i)
        except Exception as e:  # noqa: BLE001
            yield _serde.build_error_tile(f"RST_ReTile: {e}")
            return


@udtf(returnType=V2_TILE_SCHEMA)
class _RstToOverlappingTilesUDTF:
    """Streaming UDTF: yield one sub-tile struct per overlapping window."""

    def eval(self, tile, tile_width, tile_height, overlap, file_ref=None):
        if _tile_is_empty(tile):
            yield _serde.build_error_tile(
                "RST_ToOverlappingTiles: empty or unreadable tile"
            )
            return
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        # NOTE: mid-iteration failure yields already-emitted good rows + one error row.
        try:
            with ot._open(tile, file_ref=file_ref) as ds:
                for i, b in enumerate(
                    tiling.iter_to_overlapping_tiles(
                        ds, int(tile_width), int(tile_height), int(overlap)
                    )
                ):
                    yield _serde.build_tile(b, "GTiff", i)
        except Exception as e:  # noqa: BLE001
            yield _serde.build_error_tile(f"RST_ToOverlappingTiles: {e}")
            return


@udtf(returnType=V2_TILE_SCHEMA)
class _RstMakeTilesUDTF:
    """Streaming UDTF: yield one sub-tile struct per power-of-4 split tile."""

    def eval(self, tile, size_in_mb, file_ref=None):
        if _tile_is_empty(tile):
            yield _serde.build_error_tile("RST_MakeTiles: empty or unreadable tile")
            return
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        # NOTE: mid-iteration failure yields already-emitted good rows + one error row.
        try:
            # Open via FILE stream (file_ref) when available; fall back to FUSE/bytes
            # when file_ref is None.  iter_make_tiles keys the power-of-4 split count
            # on the encoded byte length; _encoded_size_bytes re-encodes for sizing,
            # matching heavy BalancedSubdivision (which keys on GDAL's in-memory size).
            with ot._open(tile, file_ref=file_ref) as ds:
                for i, b in enumerate(tiling.iter_make_tiles(ds, float(size_in_mb))):
                    yield _serde.build_tile(b, "GTiff", i)
        except Exception as e:  # noqa: BLE001
            yield _serde.build_error_tile(f"RST_MakeTiles: {e}")
            return


@udtf(returnType=V2_TILE_SCHEMA)
class _RstH3TessellateUDTF:
    """Streaming UDTF: yield one clipped tile struct per overlapping H3 cell."""

    def eval(self, tile, resolution, mode=None, file_ref=None):
        if _tile_is_empty(tile) or resolution is None:
            yield _serde.build_error_tile("RST_H3_Tessellate: empty or unreadable tile")
            return
        effective_mode = mode if mode is not None else "covering"
        if effective_mode not in {"covering", "centroid"}:
            raise ValueError(
                f"rst_h3_tessellate: mode must be one of covering, centroid; "
                f"got '{effective_mode}'"
            )
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        # NOTE: mid-iteration failure yields already-emitted good rows + one error row.
        try:
            with ot._open(tile, file_ref=file_ref) as ds:
                for cellid, raster in tessellate_core.iter_tessellate_h3(
                    ds, int(resolution), mode=effective_mode
                ):
                    if raster is None:  # defensive: never emit a null-raster tile row
                        continue
                    yield _serde.build_tile(raster, "GTiff", cellid)
        except Exception as e:  # noqa: BLE001
            yield _serde.build_error_tile(f"RST_H3_Tessellate: {e}")
            return


def rst_separatebands(tile: ColLike):
    """Split a multi-band tile into single-band tiles (one row per band).

    Light tier is a Python UDTF — invoke as a SQL LATERAL table function::

        SELECT t.* FROM <df>, LATERAL gbx_rst_separatebands(tile) t

    Each output row is a tile struct carrying the same georeferencing and CRS
    as the input; one row per band.
    """
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _RstSeparateBandsUDTF(tc, file_ref_arg(tc))


def rst_retile(tile: ColLike, tile_width: ColLike, tile_height: ColLike):
    """Partition a tile into non-overlapping sub-tiles of the given pixel size.

    Edge tiles are narrower/shorter when the raster dimensions are not exact
    multiples of tile_width/tile_height. Each output tile carries the correct
    windowed transform and CRS.

    Light tier is a Python UDTF — invoke as a SQL LATERAL table function::

        SELECT t.* FROM <df>, LATERAL gbx_rst_retile(tile, tile_width, tile_height) t

    Each output row is a tile struct; one row per sub-tile.
    """
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _RstRetileUDTF(tc, _col(tile_width), _col(tile_height), file_ref_arg(tc))


def rst_tooverlappingtiles(
    tile: ColLike,
    tile_width: ColLike,
    tile_height: ColLike,
    overlap: ColLike,
):
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
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _RstToOverlappingTilesUDTF(
        tc, _col(tile_width), _col(tile_height), _col(overlap), file_ref_arg(tc)
    )


def rst_h3_tessellate(tile: ColLike, resolution: ColLike, mode: ColLike = "covering"):
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
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _RstH3TessellateUDTF(tc, _col(resolution), _col(mode), file_ref_arg(tc))


@udtf(returnType=V2_TILE_SCHEMA)
class _RstQuadbinTessellateUDTF:
    """Streaming UDTF: yield one clipped tile struct per overlapping quadbin cell."""

    def eval(self, tile, resolution, mode=None, file_ref=None):
        if _tile_is_empty(tile) or resolution is None:
            yield _serde.build_error_tile(
                "RST_Quadbin_Tessellate: empty or unreadable tile"
            )
            return
        effective_mode = mode if mode is not None else "covering"
        if effective_mode not in {"covering", "centroid"}:
            raise ValueError(
                f"rst_quadbin_tessellate: mode must be one of covering, centroid; "
                f"got '{effective_mode}'"
            )
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        # NOTE: mid-iteration failure yields already-emitted good rows + one error row.
        try:
            with ot._open(tile, file_ref=file_ref) as ds:
                for cellid, raster in tessellate_core.iter_tessellate_quadbin(
                    ds, int(resolution), mode=effective_mode
                ):
                    if raster is None:  # defensive: never emit a null-raster tile row
                        continue
                    yield _serde.build_tile(raster, "GTiff", cellid)
        except Exception as e:  # noqa: BLE001
            yield _serde.build_error_tile(f"RST_Quadbin_Tessellate: {e}")
            return


@udtf(returnType=V2_TILE_SCHEMA)
class _RstBngTessellateUDTF:
    """Streaming UDTF: yield one clipped tile struct per overlapping BNG cell.

    BNG cell ids are Strings (e.g. ``"TQ38SW"``). The tile struct ``cellid``
    field is LongType, so the String id is parsed back to its Long digit-id via
    ``pygx._bng.parse`` (round-trips cleanly for the ±1..±6 resolutions the
    iterator emits); the authoritative cell id is carried in the tile's
    ``cellid`` struct field (matching the heavy ``RST_BNG_Tessellate`` tier),
    NOT the metadata map.
    """

    def eval(self, tile, resolution, mode=None, file_ref=None):
        if _tile_is_empty(tile) or resolution is None:
            yield _serde.build_error_tile(
                "RST_BNG_Tessellate: empty or unreadable tile"
            )
            return
        effective_mode = mode if mode is not None else "covering"
        if effective_mode not in {"covering", "centroid"}:
            raise ValueError(
                f"rst_bng_tessellate: mode must be one of covering, centroid; "
                f"got '{effective_mode}'"
            )
        from databricks.labs.gbx.pygx import _bng
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        # NOTE: mid-iteration failure yields already-emitted good rows + one error row.
        try:
            with ot._open(tile, file_ref=file_ref) as ds:
                for cellid_str, raster in tessellate_core.iter_tessellate_bng(
                    ds, resolution, mode=effective_mode
                ):
                    if raster is None:  # defensive: never emit a null-raster tile row
                        continue
                    out = _serde.build_tile(raster, "GTiff", _bng.parse(cellid_str))
                    yield out
        except Exception as e:  # noqa: BLE001
            yield _serde.build_error_tile(f"RST_BNG_Tessellate: {e}")
            return


def rst_bng_tessellate(tile: ColLike, resolution: ColLike, mode: ColLike = "covering"):
    """Tessellate a raster into BNG cells (mirrors ``gbx_rst_bng_tessellate``).

    The raster is reprojected to EPSG:27700 (British National Grid) first
    (skipped if already 27700). For every BNG cell overlapping the raster's
    extent at *resolution*, the raster is clipped to that cell's square and one
    tile is produced; the BNG cell id (e.g. ``"TQ38SW"``) is carried in the
    tile's ``cellid`` struct field (matching heavy tier behaviour). Cell
    enumeration is
    boundary-complete: the bbox is buffered by the cell half-diagonal before
    polyfill so cells whose square overlaps the raster but whose centroid sits
    just outside the bbox are still emitted; out-of-GB cells are dropped.

    Light tier is a Python UDTF — invoke as a SQL LATERAL table function::

        SELECT t.* FROM <df>, LATERAL gbx_rst_bng_tessellate(tile, resolution) t
        SELECT t.* FROM <df>, LATERAL gbx_rst_bng_tessellate(tile, resolution, 'centroid') t

    Each output row is a tile struct; one row per overlapping BNG cell.

    Args:
        tile:       Tile struct column.
        resolution: BNG resolution — an Int index (``±1..±6``: 1=100km .. 6=1m,
                    negatives=quadrants) or a resolutionMap string key
                    (e.g. ``"1km"``, ``"100m"``).
        mode:       Tessellation mode: ``"covering"`` (default) — each BNG cell
                    overlapping the raster extent is clipped to its square;
                    ``"centroid"`` — each valid pixel is assigned to exactly one
                    cell by its centroid (strict partition, no overlap).
    """
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _RstBngTessellateUDTF(tc, _col(resolution), _col(mode), file_ref_arg(tc))


def rst_quadbin_tessellate(
    tile: ColLike, resolution: ColLike, mode: ColLike = "covering"
):
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
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _RstQuadbinTessellateUDTF(tc, _col(resolution), _col(mode), file_ref_arg(tc))


def rst_maketiles(tile: ColLike, size_in_mb: ColLike):
    """Split a raster into tiles of approximately size_in_mb each (one row per tile).

    Quad-splits the raster into a power-of-4 grid (1, 4, 16, ... tiles) until
    each tile's encoded size fits within the target MB budget, then partitions
    it into non-overlapping sub-tiles. Each output tile carries the correct
    windowed transform and CRS.

    Light tier is a Python UDTF — invoke as a SQL LATERAL table function::

        SELECT t.* FROM <df>, LATERAL gbx_rst_maketiles(tile, size_in_mb) t

    Each output row is a tile struct; one row per sub-tile.
    """
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _RstMakeTilesUDTF(tc, _col(size_in_mb), file_ref_arg(tc))


# --- Tier 1f: terrain UDFs (slope, aspect, hillshade) ----------------------
def _slope_bytes(tile, unit, xscale, yscale, file_ref=None):
    """Shared slope body: open the (virtual or materialized) tile, compute
    slope, return the produced GTiff bytes."""
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    xs = None if xscale is None else float(xscale)
    ys = None if yscale is None else float(yscale)
    with ot._open(tile, file_ref=file_ref) as ds:
        return terrain.slope(ds, unit=str(unit), xscale=xs, yscale=ys)


@f.udf(V2_TILE_SCHEMA)
def _slope_udf(tile, unit, xscale, yscale):
    if _tile_is_empty(tile):
        return None
    new_bytes = _slope_bytes(tile, unit, xscale, yscale)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
def _slope_v2_udf(
    tile, unit, xscale, yscale, virtualize_dir, virtualize_prefix, materialize
):
    # Force-output variant (Python API only): same slope math via shape_output.
    if _tile_is_empty(tile):
        return None
    new_bytes = _slope_bytes(tile, unit, xscale, yscale)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


@f.udf(V2_TILE_SCHEMA)
def _uf_slope(tile, file_ref, unit, xscale, yscale):
    if _tile_is_empty(tile):
        return None
    new_bytes = _slope_bytes(tile, unit, xscale, yscale, file_ref=file_ref)
    if new_bytes is None:
        return None
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


def _aspect_bytes(tile, trigonometric, zero_for_flat, file_ref=None):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile, file_ref=file_ref) as ds:
        return terrain.aspect(
            ds,
            trigonometric=bool(trigonometric),
            zero_for_flat=bool(zero_for_flat),
        )


@f.udf(V2_TILE_SCHEMA)
def _aspect_udf(tile, trigonometric, zero_for_flat):
    if _tile_is_empty(tile):
        return None
    new_bytes = _aspect_bytes(tile, trigonometric, zero_for_flat)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
def _aspect_v2_udf(
    tile, trigonometric, zero_for_flat, virtualize_dir, virtualize_prefix, materialize
):
    if _tile_is_empty(tile):
        return None
    new_bytes = _aspect_bytes(tile, trigonometric, zero_for_flat)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


@f.udf(V2_TILE_SCHEMA)
def _uf_aspect(tile, file_ref, trigonometric, zero_for_flat):
    if _tile_is_empty(tile):
        return None
    new_bytes = _aspect_bytes(tile, trigonometric, zero_for_flat, file_ref=file_ref)
    if new_bytes is None:
        return None
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


def _hillshade_bytes(tile, azimuth, altitude, z_factor, xscale, yscale, file_ref=None):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    xs = None if xscale is None else float(xscale)
    ys = None if yscale is None else float(yscale)
    with ot._open(tile, file_ref=file_ref) as ds:
        return terrain.hillshade(
            ds,
            azimuth=float(azimuth),
            altitude=float(altitude),
            z_factor=float(z_factor),
            xscale=xs,
            yscale=ys,
        )


@f.udf(V2_TILE_SCHEMA)
def _hillshade_udf(tile, azimuth, altitude, z_factor, xscale, yscale):
    if _tile_is_empty(tile):
        return None
    new_bytes = _hillshade_bytes(tile, azimuth, altitude, z_factor, xscale, yscale)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
def _hillshade_v2_udf(
    tile,
    azimuth,
    altitude,
    z_factor,
    xscale,
    yscale,
    virtualize_dir,
    virtualize_prefix,
    materialize,
):
    if _tile_is_empty(tile):
        return None
    new_bytes = _hillshade_bytes(tile, azimuth, altitude, z_factor, xscale, yscale)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


@f.udf(V2_TILE_SCHEMA)
def _uf_hillshade(tile, file_ref, azimuth, altitude, z_factor, xscale, yscale):
    if _tile_is_empty(tile):
        return None
    new_bytes = _hillshade_bytes(
        tile, azimuth, altitude, z_factor, xscale, yscale, file_ref=file_ref
    )
    if new_bytes is None:
        return None
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


def rst_slope(
    tile: ColLike,
    unit: ColLike = "degrees",
    xscale: ColLike = None,
    yscale: ColLike = None,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
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
        virtualize_dir:    Force-output (light-tier, Python API only): write the
            computed slope to a durable path and return a light virtual tile.
        virtualize_prefix: Optional filename prefix for ``virtualize_dir``.
        materialize:       Force-output: ``True`` ensures raster bytes. Mutually
            exclusive with ``virtualize_dir``.

    Returns:
        Single-band Float32 tile; nodata = -9999. When neither force-output param
        is set the SQL registered signature is used unchanged.
    """
    force = _force_output_requested(virtualize_dir, virtualize_prefix, materialize)
    if force:
        # Validate the conflict eagerly (before building any Column) so the
        # error surfaces without an active Spark session.
        _validate_force_output(virtualize_dir, materialize)
    unit_col = f.lit(unit) if isinstance(unit, str) else _col(unit)
    xs_col = f.lit(None) if xscale is None else _col(xscale)
    ys_col = f.lit(None) if yscale is None else _col(yscale)
    if force:
        return _slope_v2_udf(
            _col(tile),
            unit_col,
            xs_col,
            ys_col,
            f.lit(virtualize_dir),
            f.lit(virtualize_prefix),
            f.lit(materialize),
        )
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_slope(tc, file_ref_arg(tc), unit_col, xs_col, ys_col)


def rst_aspect(
    tile: ColLike,
    trigonometric: ColLike = False,
    zero_for_flat: ColLike = False,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
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
        virtualize_dir:    Force-output (light-tier, Python API only): write the
            result to a durable path and return a light virtual tile.
        virtualize_prefix: Optional filename prefix for ``virtualize_dir``.
        materialize:       Force-output: ``True`` ensures raster bytes. Mutually
            exclusive with ``virtualize_dir``.

    Returns:
        Single-band Float32 tile; nodata = -9999.
    """
    trig_col = (
        f.lit(trigonometric) if isinstance(trigonometric, bool) else _col(trigonometric)
    )
    zff_col = (
        f.lit(zero_for_flat) if isinstance(zero_for_flat, bool) else _col(zero_for_flat)
    )
    if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
        _validate_force_output(virtualize_dir, materialize)
        return _aspect_v2_udf(
            _col(tile),
            trig_col,
            zff_col,
            *_force_output_lits(virtualize_dir, virtualize_prefix, materialize),
        )
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_aspect(tc, file_ref_arg(tc), trig_col, zff_col)


def rst_hillshade(
    tile: ColLike,
    azimuth: ColLike = 315.0,
    altitude: ColLike = 45.0,
    z_factor: ColLike = 1.0,
    xscale: ColLike = None,
    yscale: ColLike = None,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
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
        virtualize_dir:    Force-output (light-tier, Python API only): write the
            result to a durable path and return a light virtual tile.
        virtualize_prefix: Optional filename prefix for ``virtualize_dir``.
        materialize:       Force-output: ``True`` ensures raster bytes. Mutually
            exclusive with ``virtualize_dir``.

    Returns:
        Single-band Byte (uint8) tile; values 0..255.
    """
    az_col = f.lit(azimuth) if isinstance(azimuth, (int, float)) else _col(azimuth)
    alt_col = f.lit(altitude) if isinstance(altitude, (int, float)) else _col(altitude)
    zf_col = f.lit(z_factor) if isinstance(z_factor, (int, float)) else _col(z_factor)
    xs_col = f.lit(None) if xscale is None else _col(xscale)
    ys_col = f.lit(None) if yscale is None else _col(yscale)
    if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
        _validate_force_output(virtualize_dir, materialize)
        return _hillshade_v2_udf(
            _col(tile),
            az_col,
            alt_col,
            zf_col,
            xs_col,
            ys_col,
            *_force_output_lits(virtualize_dir, virtualize_prefix, materialize),
        )
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_hillshade(tc, file_ref_arg(tc), az_col, alt_col, zf_col, xs_col, ys_col)


# --- Tier 1g: terrain ruggedness UDFs (tri, tpi, roughness) -----------------
def _ruggedness_bytes(tile, core_fn):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile) as ds:
        return core_fn(ds)


@f.udf(V2_TILE_SCHEMA)
def _tri_udf(tile):
    if _tile_is_empty(tile):
        return None
    new_bytes = _ruggedness_bytes(tile, terrain.tri)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
def _tri_v2_udf(tile, virtualize_dir, virtualize_prefix, materialize):
    if _tile_is_empty(tile):
        return None
    new_bytes = _ruggedness_bytes(tile, terrain.tri)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


@f.udf(V2_TILE_SCHEMA)
def _tpi_udf(tile):
    if _tile_is_empty(tile):
        return None
    new_bytes = _ruggedness_bytes(tile, terrain.tpi)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
def _tpi_v2_udf(tile, virtualize_dir, virtualize_prefix, materialize):
    if _tile_is_empty(tile):
        return None
    new_bytes = _ruggedness_bytes(tile, terrain.tpi)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


@f.udf(V2_TILE_SCHEMA)
def _roughness_udf(tile):
    if _tile_is_empty(tile):
        return None
    new_bytes = _ruggedness_bytes(tile, terrain.roughness)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
def _roughness_v2_udf(tile, virtualize_dir, virtualize_prefix, materialize):
    if _tile_is_empty(tile):
        return None
    new_bytes = _ruggedness_bytes(tile, terrain.roughness)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


def _ruggedness_wrapper(
    tile, udf_v1, udf_v2, virtualize_dir, virtualize_prefix, materialize
):
    """Shared single-arg (tile-only) ruggedness wrapper w/ force-output dispatch."""
    if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
        _validate_force_output(virtualize_dir, materialize)
        return udf_v2(
            _col(tile),
            *_force_output_lits(virtualize_dir, virtualize_prefix, materialize),
        )
    return udf_v1(_col(tile))


def rst_tri(
    tile: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Compute Terrain Ruggedness Index (TRI) from a single-band DEM tile.

    TRI = mean of the absolute differences between the center cell and each of
    its 8 neighbours (Wilson 2007).  Flat terrain yields 0.

    Force-output (light-tier, Python API only): ``virtualize_dir`` writes the
    result to a durable path and returns a light virtual tile; ``materialize=True``
    forces raster bytes (mutually exclusive with ``virtualize_dir``).

    Returns:
        Single-band Float32 tile; nodata = -9999.
    """
    return _ruggedness_wrapper(
        tile, _tri_udf, _tri_v2_udf, virtualize_dir, virtualize_prefix, materialize
    )


def rst_tpi(
    tile: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Compute Topographic Position Index (TPI) from a single-band DEM tile.

    TPI = center - mean(8 neighbours).  Positive = local high; negative = local
    low; flat terrain yields 0.

    Force-output (light-tier, Python API only): ``virtualize_dir`` writes the
    result to a durable path and returns a light virtual tile; ``materialize=True``
    forces raster bytes (mutually exclusive with ``virtualize_dir``).

    Returns:
        Single-band Float32 tile; nodata = -9999.
    """
    return _ruggedness_wrapper(
        tile, _tpi_udf, _tpi_v2_udf, virtualize_dir, virtualize_prefix, materialize
    )


def rst_roughness(
    tile: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Compute terrain roughness from a single-band DEM tile.

    Roughness = max(3x3 window) - min(3x3 window).  Flat terrain yields 0.

    Force-output (light-tier, Python API only): ``virtualize_dir`` writes the
    result to a durable path and returns a light virtual tile; ``materialize=True``
    forces raster bytes (mutually exclusive with ``virtualize_dir``).

    Returns:
        Single-band Float32 tile; nodata = -9999.
    """
    return _ruggedness_wrapper(
        tile,
        _roughness_udf,
        _roughness_v2_udf,
        virtualize_dir,
        virtualize_prefix,
        materialize,
    )


def _color_relief_bytes(tile, color_table_path):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile) as ds:
        return terrain.color_relief(ds, str(color_table_path))


@f.udf(V2_TILE_SCHEMA)
def _color_relief_udf(tile, color_table_path):
    if _tile_is_empty(tile) or color_table_path is None:
        return None
    new_bytes = _color_relief_bytes(tile, color_table_path)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
def _color_relief_v2_udf(
    tile, color_table_path, virtualize_dir, virtualize_prefix, materialize
):
    if _tile_is_empty(tile) or color_table_path is None:
        return None
    new_bytes = _color_relief_bytes(tile, color_table_path)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


def rst_color_relief(
    tile: ColLike,
    color_table_path: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Map a single-band DEM through a gdaldem color table to an RGB(A) Byte tile.

    Reads a gdaldem-style color file (elevation R G B [A] per line; ``nv`` for
    NoData pixels; ``<n>%`` for percentage of the band range).  Applies linear
    interpolation per channel.

    Args:
        tile:             Tile struct column containing a single-band DEM raster.
        color_table_path: Column or string path to a gdaldem color file.
        virtualize_dir:    Force-output (light-tier, Python API only): write the
            result to a durable path and return a light virtual tile.
        virtualize_prefix: Optional filename prefix for ``virtualize_dir``.
        materialize:       Force-output: ``True`` ensures raster bytes. Mutually
            exclusive with ``virtualize_dir``.

    Returns:
        3-band (RGB) or 4-band (RGBA) Byte tile.
    """
    ctp = (
        f.lit(color_table_path)
        if isinstance(color_table_path, str)
        else _col(color_table_path)
    )
    if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
        _validate_force_output(virtualize_dir, materialize)
        return _color_relief_v2_udf(
            _col(tile),
            ctp,
            *_force_output_lits(virtualize_dir, virtualize_prefix, materialize),
        )
    return _color_relief_udf(_col(tile), ctp)


# --- Tier 0: accessors ------------------------------------------------------
# Accessor wrappers pass the FULL tile struct (not the raster subfield) so a
# virtual tile's ``path`` is reachable; the header/pixel split is decided by the
# factory that built each ``_u_*`` UDF (open_header vs _open).
#
# FILE-aware wiring (Task 4): the public Python bindings now call the 2-arg
# ``_uf_*`` UDFs (Struct + FileRef), injecting a plan-level file_ref via
# ``file_ref_arg()``.  When FILE is unsupported (local / non-DBR / env-disabled)
# ``file_ref_arg`` returns ``F.lit(None)`` and the UDF falls back to the
# plain-path read — identical to the previous single-arg path.
# The SQL registry keeps pointing at the single-arg ``_u_*`` UDFs (fallback,
# no FILE acceleration for SQL per spec §4.3).
def rst_width(tile: ColLike) -> Column:
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_width(tc, file_ref_arg(tc))


def rst_height(tile: ColLike) -> Column:
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_height(tc, file_ref_arg(tc))


def rst_numbands(tile: ColLike) -> Column:
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_numbands(tc, file_ref_arg(tc))


def rst_srid(tile: ColLike) -> Column:
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_srid(tc, file_ref_arg(tc))


def rst_crs(tile: ColLike) -> Column:
    """Return the canonical CRS string for the raster tile.

    Returns an authority string (``'EPSG:4326'``, ``'ESRI:54008'``) when the
    CRS has a recognised authority code, or a WKT string for authority-less
    projections.  Returns NULL for a tile with no CRS.

    Distinct from ``rst_srid`` (int / NULL): ``rst_crs`` preserves non-EPSG
    authority codes (ESRI, IAU, etc.) and falls back to WKT rather than NULL.

    Args:
        tile: Tile struct column.

    Returns:
        STRING column — canonical CRS string, or NULL if CRS is absent.
    """
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_crs(tc, file_ref_arg(tc))


def rst_pixelwidth(tile: ColLike) -> Column:
    return _u_pixelwidth(_col(tile))


def rst_pixelheight(tile: ColLike) -> Column:
    return _u_pixelheight(_col(tile))


def rst_upperleftx(tile: ColLike) -> Column:
    return _u_upperleftx(_col(tile))


def rst_upperlefty(tile: ColLike) -> Column:
    return _u_upperlefty(_col(tile))


def rst_boundingbox(tile: ColLike) -> Column:
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_boundingbox(tc, file_ref_arg(tc))


def rst_metadata(tile: ColLike) -> Column:
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_metadata_udf(tc, file_ref_arg(tc))


def rst_scalex(tile: ColLike) -> Column:
    return _u_scalex(_col(tile))


def rst_scaley(tile: ColLike) -> Column:
    return _u_scaley(_col(tile))


def rst_isempty(tile: ColLike) -> Column:
    """True if the raster has no size or every band is entirely NoData; BOOLEAN."""
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_isempty(tc, file_ref_arg(tc))


def rst_type(tile: ColLike) -> Column:
    """Return the GDAL data-type name per band (e.g. ['Float32', 'Float32'])."""
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_type(tc, file_ref_arg(tc))


def rst_getnodata(tile: ColLike) -> Column:
    """Return the NoData value per band as an array of doubles, or null if not set."""
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_getnodata(tc, file_ref_arg(tc))


# --- Tier 1: coordinate transforms -----------------------------------------
# HEADER-ONLY accessors: pass the FULL tile struct (not the raster subfield) so
# a virtual tile's ``path`` is reachable; the UDF resolves via open_header.
def rst_rastertoworldcoordx(tile: ColLike, x: ColLike, y: ColLike) -> Column:
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_r2w_x(tc, file_ref_arg(tc), _col(x), _col(y))


def rst_rastertoworldcoordy(tile: ColLike, x: ColLike, y: ColLike) -> Column:
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_r2w_y(tc, file_ref_arg(tc), _col(x), _col(y))


def rst_worldtorastercoordx(tile: ColLike, x: ColLike, y: ColLike) -> Column:
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_w2r_x(tc, file_ref_arg(tc), _col(x), _col(y))


def rst_worldtorastercoordy(tile: ColLike, x: ColLike, y: ColLike) -> Column:
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_w2r_y(tc, file_ref_arg(tc), _col(x), _col(y))


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
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_avg(tc, file_ref_arg(tc))


def rst_min(tile: ColLike) -> Column:
    """Per-band minimum of valid (non-NoData) pixels; ARRAY<DOUBLE>.

    Empty / all-invalid bands return NULL.
    """
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_min(tc, file_ref_arg(tc))


def rst_max(tile: ColLike) -> Column:
    """Per-band maximum of valid (non-NoData) pixels; ARRAY<DOUBLE>.

    Empty / all-invalid bands return NULL.
    """
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_max(tc, file_ref_arg(tc))


def rst_median(tile: ColLike) -> Column:
    """Per-band median of valid (non-NoData) pixels; ARRAY<DOUBLE>.

    Empty / all-invalid bands return NULL.
    """
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_median(tc, file_ref_arg(tc))


def rst_pixelcount(tile: ColLike) -> Column:
    """Per-band count of valid (non-NoData) pixels; ARRAY<LONG>.

    Empty / all-invalid bands return 0.
    """
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_pixelcount(tc, file_ref_arg(tc))


def rst_memsize(tile: ColLike) -> Column:
    """Serialized size in bytes for a materialized tile (raster buffer length);
    for a virtual tile, the estimated decoded window footprint
    (count * width * height * itemsize). LONG."""
    return _memsize_struct_udf(_col(tile))


def rst_memsize_grouped(df, *, tile_col: str = "tile", out_col: str = "memsize"):
    """Partition-scoped rst_memsize for virtual/FILE tiles: amortizes source opens
    across a partition via the grouped executor.

    Returns the same value as per-row ``rst_memsize`` for **virtual** tiles (the
    decoded-window footprint = ``count * width * height * itemsize``).  This is
    the intended input: path-backed tiles where the open cost dominates.

    Note — for already-materialized tiles per-row ``rst_memsize`` returns the
    serialized buffer length instead; the grouped form (which decodes via rasterio)
    is **not** equivalent for that case and is not intended for it."""
    import numpy as np
    from pyspark.sql.types import LongType, StructField

    from .grouped_exec import grouped_tile_map

    def _core(ds, cellid):  # noqa: ARG001 — cellid passed by executor contract
        itemsize = np.dtype(ds.dtypes[0]).itemsize
        return int(ds.count * ds.width * ds.height * itemsize)

    return grouped_tile_map(
        df,
        _core,
        return_field=StructField(out_col, LongType()),
        tile_col=tile_col,
    )


def rst_clip_grouped(
    df,
    geom_wkb,
    all_touched: bool = False,
    clip_crs=None,
    *,
    tile_col: str = "tile",
    out_col: str = "tile",
):
    """Partition-scoped rst_clip via the grouped executor (FILE pixel fast path).

    Clips every tile in *df* to *geom_wkb* (WKB, EWKB, WKT, or EWKT) by routing
    pixel reads through ``grouped_tile_map`` with ``view="pixels"``.  Source opens
    are amortized per partition: each unique FILE source is opened once and reused
    for all windows that share it, making this the preferred form for virtual/FILE
    tiles where the open cost dominates.

    *geom_wkb* is a driver-side constant (one geometry for all tiles in the group),
    not a per-row column.  Per-row geometry is handled by the scalar ``rst_clip``.

    Returns a new DataFrame with the same columns as *df* plus *out_col* (a
    V2_TILE_SCHEMA struct); null when the clip geometry does not overlap the tile.
    """
    from .grouped_exec import grouped_tile_map

    # Capture raw bytes in closure; parse inside _core so shapely objects are
    # never pickled across the Spark worker boundary.
    def _core(ds, cellid):
        from databricks.labs.gbx._geom import parse_geom

        geom = parse_geom(geom_wkb)
        if geom is None:
            return None
        new_bytes = edit.clip_to_geom(ds, geom, bool(all_touched), geom_crs=clip_crs)
        return (
            None if new_bytes is None else _serde.build_tile(new_bytes, "GTiff", cellid)
        )

    return grouped_tile_map(
        df,
        _core,
        return_field=StructField(out_col, V2_TILE_SCHEMA),
        tile_col=tile_col,
        view="pixels",
    )


def rst_transform_grouped(
    df,
    target_srid,
    *,
    tile_col: str = "tile",
    out_col: str = "tile",
):
    """Partition-scoped rst_transform via the grouped executor (FILE pixel fast path).

    Reprojects every tile in *df* to *target_srid* (int EPSG code) by routing
    pixel reads through ``grouped_tile_map`` with ``view="pixels"``.  Source opens
    are amortized per partition, making this the preferred form for virtual/FILE
    tiles where the open cost dominates.

    *target_srid* is a driver-side constant (one EPSG code for all tiles).

    Returns a new DataFrame with the same columns as *df* plus *out_col* (a
    V2_TILE_SCHEMA struct).
    """
    from .grouped_exec import grouped_tile_map

    def _core(ds, cellid):
        new_bytes = warp.reproject_to_srid(ds, int(target_srid))
        return (
            None if new_bytes is None else _serde.build_tile(new_bytes, "GTiff", cellid)
        )

    return grouped_tile_map(
        df,
        _core,
        return_field=StructField(out_col, V2_TILE_SCHEMA),
        tile_col=tile_col,
        view="pixels",
    )


def rst_to_webmercator_grouped(
    df,
    resampling: str = "bilinear",
    *,
    tile_col: str = "tile",
    out_col: str = "tile",
):
    """Partition-scoped rst_to_webmercator via the grouped executor (FILE pixel fast path).

    Reprojects every tile in *df* to EPSG:3857 (web mercator) with the given
    *resampling* algorithm by routing pixel reads through ``grouped_tile_map``
    with ``view="pixels"``.  Source opens are amortized per partition.

    *resampling* is a driver-side constant (default ``"bilinear"``).

    Returns a new DataFrame with the same columns as *df* plus *out_col* (a
    V2_TILE_SCHEMA struct).
    """
    from .grouped_exec import grouped_tile_map

    _resampling = str(resampling)

    def _core(ds, cellid):
        new_bytes = warp.reproject_to_srid(ds, 3857, resampling=_resampling)
        return (
            None if new_bytes is None else _serde.build_tile(new_bytes, "GTiff", cellid)
        )

    return grouped_tile_map(
        df,
        _core,
        return_field=StructField(out_col, V2_TILE_SCHEMA),
        tile_col=tile_col,
        view="pixels",
    )


def rst_transformcrs_grouped(
    df,
    crs_value,
    *,
    tile_col: str = "tile",
    out_col: str = "tile",
):
    """Partition-scoped rst_transformcrs via the grouped executor (FILE pixel fast path).

    Reprojects every tile in *df* to *crs_value* (CRS string such as ``"EPSG:3857"``
    or a WKT string) by routing pixel reads through ``grouped_tile_map`` with
    ``view="pixels"``.  Source opens are amortized per partition.

    *crs_value* is a driver-side constant (one CRS for all tiles).

    Returns a new DataFrame with the same columns as *df* plus *out_col* (a
    V2_TILE_SCHEMA struct).
    """
    from .grouped_exec import grouped_tile_map

    _crs = str(crs_value)

    def _core(ds, cellid):
        new_bytes = warp.reproject_to_crs(ds, _crs)
        return (
            None if new_bytes is None else _serde.build_tile(new_bytes, "GTiff", cellid)
        )

    return grouped_tile_map(
        df,
        _core,
        return_field=StructField(out_col, V2_TILE_SCHEMA),
        tile_col=tile_col,
        view="pixels",
    )


def rst_resample_grouped(
    df,
    factor,
    algorithm: str = "bilinear",
    *,
    tile_col: str = "tile",
    out_col: str = "tile",
):
    """Partition-scoped rst_resample via the grouped executor (FILE pixel fast path).

    Resamples every tile in *df* by a multiplicative *factor* (>1 upsamples,
    0<factor<1 downsamples) using the given *algorithm*.  Pixel reads are routed
    through ``grouped_tile_map`` with ``view="pixels"``; source opens are amortized
    per partition.

    *factor* and *algorithm* are driver-side constants.

    Returns a new DataFrame with the same columns as *df* plus *out_col* (a
    V2_TILE_SCHEMA struct).
    """
    from .grouped_exec import grouped_tile_map

    _alg = str(algorithm)

    def _core(ds, cellid):
        new_bytes = resample.resample_by_factor(ds, float(factor), _alg)
        return (
            None if new_bytes is None else _serde.build_tile(new_bytes, "GTiff", cellid)
        )

    return grouped_tile_map(
        df,
        _core,
        return_field=StructField(out_col, V2_TILE_SCHEMA),
        tile_col=tile_col,
        view="pixels",
    )


def rst_resample_to_size_grouped(
    df,
    width_px,
    height_px,
    algorithm: str = "bilinear",
    *,
    tile_col: str = "tile",
    out_col: str = "tile",
):
    """Partition-scoped rst_resample_to_size via the grouped executor (FILE pixel fast path).

    Resamples every tile in *df* to exactly *width_px* × *height_px* pixels using
    *algorithm*.  Pixel reads are routed through ``grouped_tile_map`` with
    ``view="pixels"``; source opens are amortized per partition.

    *width_px*, *height_px*, and *algorithm* are driver-side constants.

    Returns a new DataFrame with the same columns as *df* plus *out_col* (a
    V2_TILE_SCHEMA struct).
    """
    from .grouped_exec import grouped_tile_map

    _alg = str(algorithm)

    def _core(ds, cellid):
        new_bytes = resample.resample_to_size(ds, int(width_px), int(height_px), _alg)
        return (
            None if new_bytes is None else _serde.build_tile(new_bytes, "GTiff", cellid)
        )

    return grouped_tile_map(
        df,
        _core,
        return_field=StructField(out_col, V2_TILE_SCHEMA),
        tile_col=tile_col,
        view="pixels",
    )


def rst_resample_to_res_grouped(
    df,
    x_res,
    y_res,
    algorithm: str = "bilinear",
    *,
    tile_col: str = "tile",
    out_col: str = "tile",
):
    """Partition-scoped rst_resample_to_res via the grouped executor (FILE pixel fast path).

    Resamples every tile in *df* to the target pixel resolution (*x_res*, *y_res*)
    using *algorithm*.  Pixel reads are routed through ``grouped_tile_map`` with
    ``view="pixels"``; source opens are amortized per partition.

    *x_res*, *y_res*, and *algorithm* are driver-side constants.

    Returns a new DataFrame with the same columns as *df* plus *out_col* (a
    V2_TILE_SCHEMA struct).
    """
    from .grouped_exec import grouped_tile_map

    _alg = str(algorithm)

    def _core(ds, cellid):
        new_bytes = resample.resample_to_res(ds, float(x_res), float(y_res), _alg)
        return (
            None if new_bytes is None else _serde.build_tile(new_bytes, "GTiff", cellid)
        )

    return grouped_tile_map(
        df,
        _core,
        return_field=StructField(out_col, V2_TILE_SCHEMA),
        tile_col=tile_col,
        view="pixels",
    )


def rst_threshold_grouped(
    df,
    op: str = ">",
    value: float = 0.0,
    *,
    tile_col: str = "tile",
    out_col: str = "tile",
):
    """Partition-scoped rst_threshold via the grouped executor (FILE pixel fast path).

    Applies a pixel threshold to every tile in *df*, setting pixels that do not
    satisfy ``pixel <op> value`` to NoData.  Pixel reads are routed through
    ``grouped_tile_map`` with ``view="pixels"``; source opens are amortized per
    partition.

    *op* (one of ``">"``, ``"<"``, ``">="`` ``"<="`` ``"=="`` ``"!="``) and
    *value* are driver-side constants.

    Returns a new DataFrame with the same columns as *df* plus *out_col* (a
    V2_TILE_SCHEMA struct).
    """
    from .grouped_exec import grouped_tile_map

    _op = str(op)
    _val = float(value)

    def _core(ds, cellid):
        new_bytes = edit.threshold(ds, _op, _val)
        return (
            None if new_bytes is None else _serde.build_tile(new_bytes, "GTiff", cellid)
        )

    return grouped_tile_map(
        df,
        _core,
        return_field=StructField(out_col, V2_TILE_SCHEMA),
        tile_col=tile_col,
        view="pixels",
    )


def rst_updatetype_grouped(
    df,
    new_type,
    *,
    tile_col: str = "tile",
    out_col: str = "tile",
):
    """Partition-scoped rst_updatetype via the grouped executor (FILE pixel fast path).

    Casts all raster bands in every tile to *new_type* (GDAL data type string such
    as ``"Float64"`` or ``"Int16"``).  Pixel reads are routed through
    ``grouped_tile_map`` with ``view="pixels"``; source opens are amortized per
    partition.

    *new_type* is a driver-side constant.

    Returns a new DataFrame with the same columns as *df* plus *out_col* (a
    V2_TILE_SCHEMA struct).
    """
    from .grouped_exec import grouped_tile_map

    _new_type = str(new_type)

    def _core(ds, cellid):
        new_bytes = edit.update_type(ds, _new_type)
        return (
            None if new_bytes is None else _serde.build_tile(new_bytes, "GTiff", cellid)
        )

    return grouped_tile_map(
        df,
        _core,
        return_field=StructField(out_col, V2_TILE_SCHEMA),
        tile_col=tile_col,
        view="pixels",
    )


def rst_slope_grouped(
    df,
    unit: str = "degrees",
    xscale=None,
    yscale=None,
    *,
    tile_col: str = "tile",
    out_col: str = "tile",
):
    """Partition-scoped rst_slope via the grouped executor (FILE pixel fast path).

    Computes terrain slope (Horn's 3x3 method) for every tile in *df*.  Pixel
    reads are routed through ``grouped_tile_map`` with ``view="pixels"``; source
    opens are amortized per partition.

    *unit* (``"degrees"`` or ``"percent"``), *xscale*, and *yscale* are
    driver-side constants.

    Returns a new DataFrame with the same columns as *df* plus *out_col* (a
    V2_TILE_SCHEMA struct).
    """
    from .grouped_exec import grouped_tile_map

    _unit = str(unit)
    _xs = None if xscale is None else float(xscale)
    _ys = None if yscale is None else float(yscale)

    def _core(ds, cellid):
        new_bytes = terrain.slope(ds, unit=_unit, xscale=_xs, yscale=_ys)
        return (
            None if new_bytes is None else _serde.build_tile(new_bytes, "GTiff", cellid)
        )

    return grouped_tile_map(
        df,
        _core,
        return_field=StructField(out_col, V2_TILE_SCHEMA),
        tile_col=tile_col,
        view="pixels",
    )


def rst_aspect_grouped(
    df,
    trigonometric=False,
    zero_for_flat=False,
    *,
    tile_col: str = "tile",
    out_col: str = "tile",
):
    """Partition-scoped rst_aspect via the grouped executor (FILE pixel fast path).

    Computes terrain aspect (Horn's 3x3 method) for every tile in *df*.  Pixel
    reads are routed through ``grouped_tile_map`` with ``view="pixels"``; source
    opens are amortized per partition.

    *trigonometric* and *zero_for_flat* are driver-side constants.

    Returns a new DataFrame with the same columns as *df* plus *out_col* (a
    V2_TILE_SCHEMA struct).
    """
    from .grouped_exec import grouped_tile_map

    _trig = bool(trigonometric)
    _zff = bool(zero_for_flat)

    def _core(ds, cellid):
        new_bytes = terrain.aspect(ds, trigonometric=_trig, zero_for_flat=_zff)
        return (
            None if new_bytes is None else _serde.build_tile(new_bytes, "GTiff", cellid)
        )

    return grouped_tile_map(
        df,
        _core,
        return_field=StructField(out_col, V2_TILE_SCHEMA),
        tile_col=tile_col,
        view="pixels",
    )


def rst_hillshade_grouped(
    df,
    azimuth: float = 315.0,
    altitude: float = 45.0,
    z_factor: float = 1.0,
    xscale=None,
    yscale=None,
    *,
    tile_col: str = "tile",
    out_col: str = "tile",
):
    """Partition-scoped rst_hillshade via the grouped executor (FILE pixel fast path).

    Computes hillshade (Horn's 3x3 method) for every tile in *df*.  Pixel reads
    are routed through ``grouped_tile_map`` with ``view="pixels"``; source opens
    are amortized per partition.

    *azimuth*, *altitude*, *z_factor*, *xscale*, and *yscale* are driver-side
    constants.

    Returns a new DataFrame with the same columns as *df* plus *out_col* (a
    V2_TILE_SCHEMA struct).
    """
    from .grouped_exec import grouped_tile_map

    _az = float(azimuth)
    _alt = float(altitude)
    _zf = float(z_factor)
    _xs = None if xscale is None else float(xscale)
    _ys = None if yscale is None else float(yscale)

    def _core(ds, cellid):
        new_bytes = terrain.hillshade(
            ds,
            azimuth=_az,
            altitude=_alt,
            z_factor=_zf,
            xscale=_xs,
            yscale=_ys,
        )
        return (
            None if new_bytes is None else _serde.build_tile(new_bytes, "GTiff", cellid)
        )

    return grouped_tile_map(
        df,
        _core,
        return_field=StructField(out_col, V2_TILE_SCHEMA),
        tile_col=tile_col,
        view="pixels",
    )


def rst_rotation(tile: ColLike) -> Column:
    """Rotation angle = atan(skewY / scaleX) in radians; DOUBLE."""
    return _u_rotation(_col(tile))


def rst_skewx(tile: ColLike) -> Column:
    """X skew of the geotransform (gt2); DOUBLE."""
    return _u_skewx(_col(tile))


def rst_skewy(tile: ColLike) -> Column:
    """Y skew of the geotransform (gt4); DOUBLE."""
    return _u_skewy(_col(tile))


def rst_format(tile: ColLike) -> Column:
    """GDAL driver short name of the raster (e.g. 'GTiff'); STRING."""
    return _u_format(_col(tile))


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


def rst_getsubdataset(tile: ColLike, subset_name: ColLike) -> Column:
    """Extract the named subdataset as a new raster tile struct.

    Raises if no subdataset matches ``subset_name`` (mirrors heavyweight).
    """
    nm = f.lit(subset_name) if isinstance(subset_name, str) else _col(subset_name)
    return _getsubdataset_udf(_col(tile), nm)


def rst_summary(tile: ColLike) -> Column:
    """gdalinfo-style JSON summary string with per-band statistics; STRING.

    The JSON shape is GeoBrix-specific (driver, size, crs, geoTransform, bands
    with min/max/mean/stdDev), not a byte-for-byte ``gdalinfo -json`` match.
    """
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    return _uf_summary_udf(tc, file_ref_arg(tc))


def rst_histogram(
    tile: ColLike,
    n_buckets: ColLike = 256,
    min_val: ColLike = None,
    max_val: ColLike = None,
    include_nodata: ColLike = False,
) -> Column:
    """Per-band histogram as MAP<STRING, ARRAY<LONG>> keyed by ``band_<i>``.

    Args:
        tile:           Tile struct column.
        n_buckets:      Number of equal-width buckets across [min_val, max_val] (default 256).
        min_val:        Lower bound; defaults to the band's valid-pixel minimum.
        max_val:        Upper bound; defaults to the band's valid-pixel maximum.
        include_nodata: Include masked pixels in the binning (default False).

    Values outside [min_val, max_val] are dropped (no out-of-range bucket).
    """
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    tc = _col(tile)
    nb = f.lit(n_buckets) if isinstance(n_buckets, int) else _col(n_buckets)
    lo = f.lit(None) if min_val is None else _col(min_val)
    hi = f.lit(None) if max_val is None else _col(max_val)
    inc = (
        f.lit(include_nodata)
        if isinstance(include_nodata, bool)
        else _col(include_nodata)
    )
    return _uf_histogram_udf(tc, file_ref_arg(tc), nb, lo, hi, inc)


# --- Tier 1d5: derived-band UDF ---------------------------------------------
def _derivedband_bytes(tile, pyfunc, func_name):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile) as ds:
        return derivedband_core.derivedband(ds, str(pyfunc), str(func_name))


@f.udf(V2_TILE_SCHEMA)
def _derivedband_udf(tile, pyfunc, func_name):
    if _tile_is_empty(tile) or pyfunc is None or func_name is None:
        return None
    new_bytes = _derivedband_bytes(tile, pyfunc, func_name)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
def _derivedband_v2_udf(
    tile, pyfunc, func_name, virtualize_dir, virtualize_prefix, materialize
):
    if _tile_is_empty(tile) or pyfunc is None or func_name is None:
        return None
    new_bytes = _derivedband_bytes(tile, pyfunc, func_name)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


def rst_derivedband(
    tile: ColLike,
    python_func: ColLike,
    func_name: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """Apply a user-provided Python function to the raster's bands.

    python_func follows GDAL's VRT pixel-function signature::

        func(in_ar, out_ar, xoff, yoff, xsize, ysize,
             raster_xsize, raster_ysize, buf_radius, gt, **kwargs)

    where ``in_ar`` is a list of 2-D NumPy arrays (one per input band) and
    ``out_ar`` is a preallocated 2-D output array the function fills in-place
    (``out_ar[:] = ...``). This matches GDAL's Python pixel-function contract,
    so a python_func authored for the heavyweight ``rst_derivedband`` works here.

    SECURITY: python_func is executed in-process without sandboxing — pass only
    trusted (your own) code, the same trust model as any Spark UDF.

    Args:
        tile:        Tile struct column.
        python_func: Python source code (string) defining the function.
        func_name:   Name of the callable within ``python_func``.
        virtualize_dir:    Force-output (light-tier, Python API only): write the
            result to a durable path and return a light virtual tile.
        virtualize_prefix: Optional filename prefix for ``virtualize_dir``.
        materialize:       Force-output: ``True`` ensures raster bytes. Mutually
            exclusive with ``virtualize_dir``.

    Returns:
        Single-band Float64 tile struct.
    """
    pf = f.lit(python_func) if isinstance(python_func, str) else _col(python_func)
    fn = f.lit(func_name) if isinstance(func_name, str) else _col(func_name)
    if _force_output_requested(virtualize_dir, virtualize_prefix, materialize):
        _validate_force_output(virtualize_dir, materialize)
        return _derivedband_v2_udf(
            _col(tile),
            pf,
            fn,
            *_force_output_lits(virtualize_dir, virtualize_prefix, materialize),
        )
    return _derivedband_udf(_col(tile), pf, fn)


# --- Tier 1h: web-mercator XYZ tiling UDFs ---------------------------------
@f.udf(BinaryType())
def _tilexyz_udf(tile, z, x, y, format, size, resampling, rescale=None):
    # Mirror heavyweight: rst_tilexyz NEVER returns null — a null/empty tile or
    # any hard failure yields a transparent PNG (slippy-map servers need a 200).
    sz = int(size) if size is not None else 256
    if _tile_is_empty(tile):
        return xyz.transparent_png(sz)
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    fmt = str(format) if format is not None else "PNG"
    resamp = str(resampling) if resampling is not None else "bilinear"
    rsc = rescale if rescale is not None else "auto"
    with ot._open(tile) as ds:
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
        if _tile_is_empty(tile):
            return
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        fmt = str(format) if format is not None else "PNG"
        sz = int(size) if size is not None else 256
        resamp = str(resampling) if resampling is not None else "bilinear"
        rsc = rescale if rescale is not None else "auto"
        with ot._open(tile) as ds:
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
    rescale: ColLike = "auto",
) -> None:
    """Render every web-mercator XYZ tile intersecting the raster across a zoom range.

    Computes the source extent in WGS84, enumerates intersecting (z, x, y) tiles
    for each zoom in [min_z, max_z] (WebMercatorQuad TMS, Y north-down), and
    renders each via the same path as :func:`rst_tilexyz`.

    Light tier is a Python UDTF — invoke as a SQL LATERAL table function::

        SELECT t.z, t.x, t.y, t.bytes
        FROM <df>, LATERAL gbx_rst_xyzpyramid(tile, min_z, max_z, format, size, resampling, rescale) t

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
        "LATERAL gbx_rst_xyzpyramid(tile, min_z, max_z, format, size, resampling, rescale) t"
    )


# --- Tier 1i: raster->grid aggregation UDTFs (h3 + quadbin) -----------------
# Streaming UDTFs: yield flat (band INT, cellID LONG, measure <T>) rows.
# band is 1-based (rasterio convention).  measure type matches Scala heavy:
#   h3 count  -> IntegerType  (INT)
#   quadbin count -> LongType (LONG)
#   avg/max/min/median -> DoubleType (DOUBLE)


def _grid_flat_schema(measure_type, cellid_type=LongType()):
    return StructType(
        [
            StructField("band", IntegerType(), False),
            StructField("cellID", cellid_type, True),
            StructField("measure", measure_type, True),
        ]
    )


_GRID_FLAT_DOUBLE_SCHEMA = _grid_flat_schema(DoubleType())
_GRID_FLAT_INT_SCHEMA = _grid_flat_schema(IntegerType())  # h3 count
_GRID_FLAT_LONG_SCHEMA = _grid_flat_schema(LongType())  # quadbin count
# BNG renders a formatted BNG string (e.g. "TQ3080") as cellID, matching heavy
# RST_BNG_RasterToGrid* (StringType cellID). avg/max/min/median -> DOUBLE; count
# -> INTEGER (heavy RST_BNG_RasterToGridCount measure is IntegerType).
_GRID_FLAT_STRING_SCHEMA = _grid_flat_schema(DoubleType(), StringType())
_GRID_FLAT_STRING_INT_SCHEMA = _grid_flat_schema(IntegerType(), StringType())


def _make_rastertogrid_udtf(grid, agg, flat_schema, cellid_is_str=False):
    @udtf(returnType=flat_schema)
    class _RasterToGridUDTF:
        def eval(self, tile, resolution):
            if _tile_is_empty(tile):
                return
            from databricks.labs.gbx.pyrx import _env

            _env.configure_gdal_env()
            with ot._open(tile) as ds:
                bands_data = gridagg.raster_to_grid(ds, resolution, grid, agg)
            # Yield flat rows (band, cellID, measure) — never buffer full nested list.
            # BNG cellIDs are already formatted strings from gridagg; H3/quadbin
            # cellIDs are Long ints (int() coerce numpy scalars).
            for band_idx, cells in enumerate(bands_data, start=1):
                for cell in cells:
                    cid = cell["cellID"] if cellid_is_str else int(cell["cellID"])
                    yield (band_idx, cid, cell["measure"])

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
_RstH3RasterToGridSumUDTF = _make_rastertogrid_udtf(
    "h3", "sum", _GRID_FLAT_DOUBLE_SCHEMA
)
_RstH3RasterToGridVarianceUDTF = _make_rastertogrid_udtf(
    "h3", "variance", _GRID_FLAT_DOUBLE_SCHEMA
)
_RstH3RasterToGridStddevUDTF = _make_rastertogrid_udtf(
    "h3", "stddev", _GRID_FLAT_DOUBLE_SCHEMA
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
_RstQuadbinRasterToGridSumUDTF = _make_rastertogrid_udtf(
    "quadbin", "sum", _GRID_FLAT_DOUBLE_SCHEMA
)
_RstQuadbinRasterToGridVarianceUDTF = _make_rastertogrid_udtf(
    "quadbin", "variance", _GRID_FLAT_DOUBLE_SCHEMA
)
_RstQuadbinRasterToGridStddevUDTF = _make_rastertogrid_udtf(
    "quadbin", "stddev", _GRID_FLAT_DOUBLE_SCHEMA
)
_RstBngRasterToGridAvgUDTF = _make_rastertogrid_udtf(
    "bng", "avg", _GRID_FLAT_STRING_SCHEMA, cellid_is_str=True
)
_RstBngRasterToGridCountUDTF = _make_rastertogrid_udtf(
    "bng", "count", _GRID_FLAT_STRING_INT_SCHEMA, cellid_is_str=True
)
_RstBngRasterToGridMaxUDTF = _make_rastertogrid_udtf(
    "bng", "max", _GRID_FLAT_STRING_SCHEMA, cellid_is_str=True
)
_RstBngRasterToGridMinUDTF = _make_rastertogrid_udtf(
    "bng", "min", _GRID_FLAT_STRING_SCHEMA, cellid_is_str=True
)
_RstBngRasterToGridMedianUDTF = _make_rastertogrid_udtf(
    "bng", "median", _GRID_FLAT_STRING_SCHEMA, cellid_is_str=True
)
_RstBngRasterToGridSumUDTF = _make_rastertogrid_udtf(
    "bng", "sum", _GRID_FLAT_STRING_SCHEMA, cellid_is_str=True
)
_RstBngRasterToGridVarianceUDTF = _make_rastertogrid_udtf(
    "bng", "variance", _GRID_FLAT_STRING_SCHEMA, cellid_is_str=True
)
_RstBngRasterToGridStddevUDTF = _make_rastertogrid_udtf(
    "bng", "stddev", _GRID_FLAT_STRING_SCHEMA, cellid_is_str=True
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


def rst_h3_rastertogridsum(tile: ColLike, resolution: ColLike) -> None:
    """Aggregate raster pixel values into H3 cells by sum, per band."""
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, LATERAL gbx_rst_h3_rastertogridsum(tile, resolution) t"
    )


def rst_h3_rastertogridvariance(tile: ColLike, resolution: ColLike) -> None:
    """Aggregate raster pixel values into H3 cells by population variance, per band."""
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, LATERAL gbx_rst_h3_rastertogridvariance(tile, resolution) t"
    )


def rst_h3_rastertogridstddev(tile: ColLike, resolution: ColLike) -> None:
    """Aggregate raster pixel values into H3 cells by population stddev, per band."""
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, LATERAL gbx_rst_h3_rastertogridstddev(tile, resolution) t"
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


def rst_quadbin_rastertogridsum(tile: ColLike, resolution: ColLike) -> None:
    """Aggregate raster pixel values into quadbin cells by sum, per band."""
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, LATERAL gbx_rst_quadbin_rastertogridsum(tile, resolution) t"
    )


def rst_quadbin_rastertogridvariance(tile: ColLike, resolution: ColLike) -> None:
    """Aggregate raster pixel values into quadbin cells by population variance, per band."""
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, LATERAL gbx_rst_quadbin_rastertogridvariance(tile, resolution) t"
    )


def rst_quadbin_rastertogridstddev(tile: ColLike, resolution: ColLike) -> None:
    """Aggregate raster pixel values into quadbin cells by population stddev, per band."""
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, LATERAL gbx_rst_quadbin_rastertogridstddev(tile, resolution) t"
    )


def rst_bng_rastertogridavg(tile: ColLike, resolution: ColLike) -> None:
    """Aggregate raster pixel values into BNG cells by mean, per band."""
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, LATERAL gbx_rst_bng_rastertogridavg(tile, resolution) t"
    )


def rst_bng_rastertogridcount(tile: ColLike, resolution: ColLike) -> None:
    """Count raster pixels falling in each BNG cell, per band."""
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, LATERAL gbx_rst_bng_rastertogridcount(tile, resolution) t"
    )


def rst_bng_rastertogridmax(tile: ColLike, resolution: ColLike) -> None:
    """Aggregate raster pixel values into BNG cells by maximum, per band."""
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, LATERAL gbx_rst_bng_rastertogridmax(tile, resolution) t"
    )


def rst_bng_rastertogridmin(tile: ColLike, resolution: ColLike) -> None:
    """Aggregate raster pixel values into BNG cells by minimum, per band."""
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, LATERAL gbx_rst_bng_rastertogridmin(tile, resolution) t"
    )


def rst_bng_rastertogridmedian(tile: ColLike, resolution: ColLike) -> None:
    """Aggregate raster pixel values into BNG cells by median, per band."""
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, LATERAL gbx_rst_bng_rastertogridmedian(tile, resolution) t"
    )


def rst_bng_rastertogridsum(tile: ColLike, resolution: ColLike) -> None:
    """Aggregate raster pixel values into BNG cells by sum, per band."""
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, LATERAL gbx_rst_bng_rastertogridsum(tile, resolution) t"
    )


def rst_bng_rastertogridvariance(tile: ColLike, resolution: ColLike) -> None:
    """Aggregate raster pixel values into BNG cells by population variance, per band."""
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, LATERAL gbx_rst_bng_rastertogridvariance(tile, resolution) t"
    )


def rst_bng_rastertogridstddev(tile: ColLike, resolution: ColLike) -> None:
    """Aggregate raster pixel values into BNG cells by population stddev, per band."""
    raise NotImplementedError(
        "Invoke the registered UDTF as a SQL LATERAL table function: "
        "SELECT t.* FROM <df>, LATERAL gbx_rst_bng_rastertogridstddev(tile, resolution) t"
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
rst_h3_rastertogridsum.__doc__ = _RASTERTOGRID_DOC.format(
    summary="Aggregate raster pixel values into H3 cells by sum, per band.",
    grid="H3",
    agg_desc="their sum (DOUBLE)",
    res_range="0..15",
    measure="DOUBLE",
    sql_name="h3_rastertogridsum",
)
rst_h3_rastertogridvariance.__doc__ = _RASTERTOGRID_DOC.format(
    summary="Aggregate raster pixel values into H3 cells by population variance, per band.",
    grid="H3",
    agg_desc="their population variance (DOUBLE)",
    res_range="0..15",
    measure="DOUBLE",
    sql_name="h3_rastertogridvariance",
)
rst_h3_rastertogridstddev.__doc__ = _RASTERTOGRID_DOC.format(
    summary="Aggregate raster pixel values into H3 cells by population standard deviation, per band.",
    grid="H3",
    agg_desc="their population standard deviation (DOUBLE)",
    res_range="0..15",
    measure="DOUBLE",
    sql_name="h3_rastertogridstddev",
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
rst_quadbin_rastertogridsum.__doc__ = _RASTERTOGRID_DOC.format(
    summary="Aggregate raster pixel values into quadbin cells by sum, per band.",
    grid="quadbin",
    agg_desc="their sum (DOUBLE)",
    res_range="0..20",
    measure="DOUBLE",
    sql_name="quadbin_rastertogridsum",
)
rst_quadbin_rastertogridvariance.__doc__ = _RASTERTOGRID_DOC.format(
    summary="Aggregate raster pixel values into quadbin cells by population variance, per band.",
    grid="quadbin",
    agg_desc="their population variance (DOUBLE)",
    res_range="0..20",
    measure="DOUBLE",
    sql_name="quadbin_rastertogridvariance",
)
rst_quadbin_rastertogridstddev.__doc__ = _RASTERTOGRID_DOC.format(
    summary="Aggregate raster pixel values into quadbin cells by population standard deviation, per band.",
    grid="quadbin",
    agg_desc="their population standard deviation (DOUBLE)",
    res_range="0..20",
    measure="DOUBLE",
    sql_name="quadbin_rastertogridstddev",
)

_BNG_RASTERTOGRID_DOC = """{summary}

    Per band, every valid (non-NoData) pixel is mapped to a British National Grid
    cell at the given ``resolution`` via its pixel-centroid coordinate; the pixel
    values falling in each cell are reduced by {agg_desc}. BNG has no lon/lat
    input path, so the raster is reprojected to EPSG:27700 (nearest-neighbour)
    first; pixels outside Great Britain are dropped.

    Light tier is a Python UDTF -- invoke as a SQL LATERAL table function::

        SELECT t.band, t.cellID, t.measure
        FROM <df>, LATERAL gbx_rst_{sql_name}(tile, resolution) t

    Each row: band INT (1-based), cellID STRING (BNG cell id e.g. ``TQ3080``),
    measure {measure}.

    Args:
        tile:       Tile struct column.
        resolution: BNG resolution: integer index +/-1..+/-6 (1=100km .. 6=1m;
                    negatives = quadrants) or a resolutionMap string key
                    (e.g. ``"1km"``, ``"100m"``).
    """

rst_bng_rastertogridavg.__doc__ = _BNG_RASTERTOGRID_DOC.format(
    summary="Aggregate raster pixel values into BNG cells by mean, per band.",
    agg_desc="their mean (DOUBLE)",
    measure="DOUBLE",
    sql_name="bng_rastertogridavg",
)
rst_bng_rastertogridcount.__doc__ = _BNG_RASTERTOGRID_DOC.format(
    summary="Count raster pixels falling in each BNG cell, per band.",
    agg_desc="a pixel count (INTEGER)",
    measure="INTEGER",
    sql_name="bng_rastertogridcount",
)
rst_bng_rastertogridmax.__doc__ = _BNG_RASTERTOGRID_DOC.format(
    summary="Aggregate raster pixel values into BNG cells by maximum, per band.",
    agg_desc="their maximum (DOUBLE)",
    measure="DOUBLE",
    sql_name="bng_rastertogridmax",
)
rst_bng_rastertogridmin.__doc__ = _BNG_RASTERTOGRID_DOC.format(
    summary="Aggregate raster pixel values into BNG cells by minimum, per band.",
    agg_desc="their minimum (DOUBLE)",
    measure="DOUBLE",
    sql_name="bng_rastertogridmin",
)
rst_bng_rastertogridmedian.__doc__ = _BNG_RASTERTOGRID_DOC.format(
    summary="Aggregate raster pixel values into BNG cells by median, per band.",
    agg_desc="their median (DOUBLE; even counts average the two middle values)",
    measure="DOUBLE",
    sql_name="bng_rastertogridmedian",
)
rst_bng_rastertogridsum.__doc__ = _BNG_RASTERTOGRID_DOC.format(
    summary="Aggregate raster pixel values into BNG cells by sum, per band.",
    agg_desc="their sum (DOUBLE)",
    measure="DOUBLE",
    sql_name="bng_rastertogridsum",
)
rst_bng_rastertogridvariance.__doc__ = _BNG_RASTERTOGRID_DOC.format(
    summary="Aggregate raster pixel values into BNG cells by population variance, per band.",
    agg_desc="their population variance (DOUBLE)",
    measure="DOUBLE",
    sql_name="bng_rastertogridvariance",
)
rst_bng_rastertogridstddev.__doc__ = _BNG_RASTERTOGRID_DOC.format(
    summary="Aggregate raster pixel values into BNG cells by population standard deviation, per band.",
    agg_desc="their population standard deviation (DOUBLE)",
    measure="DOUBLE",
    sql_name="bng_rastertogridstddev",
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

    Virtual-aware: a materialized tile yields its bytes verbatim; a virtual tile
    (raster None, path set) is materialized through the front-door so grouped-agg
    reducers never silently drop a bytes-free row. A row missing both raster and
    path is empty -> None.
    """
    if row is None:
        return None
    d = row.asDict() if hasattr(row, "asDict") else row
    raster = d["raster"] if "raster" in d else None
    if raster is not None:
        return bytes(raster)
    # No materialized bytes: a virtual tile carries a path -> materialize it.
    if d.get("path") if hasattr(d, "get") else None:
        return ot.materialize_to_bytes(ot._to_virtual_tile(row)).raster
    return None


# --- scalar as_tile UDFs: wrap an aggregated BINARY result into a tile struct
@f.udf(V2_TILE_SCHEMA)
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


_as_tile_cellid_envelope_udf = f.udf(V2_TILE_SCHEMA)(_as_tile_cellid_envelope_udf_fn)


# --- grouped-agg pandas_udf(BinaryType()) reducers --------------------------
@pandas_udf(BinaryType())
def _merge_agg_udf(tile: pd.Series) -> bytes:
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    rasters = []
    dropped = 0
    for r in tile:
        candidate = _tile_raster_bytes(r)
        if candidate is None:
            continue
        try:
            with _serde.open_tile(candidate):
                pass
            rasters.append(candidate)
        except Exception:  # noqa: BLE001
            dropped += 1
            continue
    if not rasters:
        return None
    # NOTE: drop-count has no metadata carrier at this layer (pandas_udf returns
    # bare bytes; no struct/metadata assembly here). The skip still stops raising.
    return agg_core.merge_tiles(rasters)


@pandas_udf(BinaryType())
def _merge_agg_file_udf(tile: pd.Series, file_ref: pd.Series) -> bytes:
    """FILE-aware merge aggregator: reads virtual tiles via per-row FileRef.

    For each row: if file_ref is not None AND tile is virtual, read via
    ot._open(tile_row, file_ref=fref); otherwise use verbatim bytes (fallback).
    Materialized tiles use verbatim bytes (sort-key invariant preserved).
    """
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    rasters = []
    dropped = 0
    for r, fref in zip(tile, file_ref):
        if r is None:
            continue
        try:
            vt = ot._to_virtual_tile(r)
            if vt.is_virtual():
                with ot._open(r, file_ref=fref) as ds:
                    candidate = _dataset_to_gtiff_bytes(ds)
            else:
                candidate = bytes(vt.raster)
            with _serde.open_tile(candidate):
                pass
            rasters.append(candidate)
        except Exception:  # noqa: BLE001
            dropped += 1
            continue
    if not rasters:
        return None
    return agg_core.merge_tiles(rasters)


@pandas_udf(BinaryType())
def _combineavg_agg_udf(tile: pd.Series) -> bytes:
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    rasters = []
    cellid = 0
    first = True
    dropped = 0
    for r in tile:
        candidate = _tile_raster_bytes(r)
        if candidate is None:
            continue
        try:
            with _serde.open_tile(candidate):
                pass
            if first:
                cid = r["cellid"]
                cellid = int(cid) if cid is not None else 0
                first = False
            rasters.append(candidate)
        except Exception:  # noqa: BLE001
            dropped += 1
            continue
    if not rasters:
        return None
    out = agg_core.combineavg_tiles(rasters)
    if out is None:
        return None
    # NOTE: drop-count has no metadata carrier at this layer (pandas_udf returns
    # bare bytes; no struct/metadata assembly here). The skip still stops raising.
    # Prepend an 8-byte big-endian cellid envelope (stripped by the scalar UDF).
    return cellid.to_bytes(8, "big", signed=True) + bytes(out)


@pandas_udf(BinaryType())
def _combineavg_agg_file_udf(tile: pd.Series, file_ref: pd.Series) -> bytes:
    """FILE-aware combineavg aggregator: reads virtual tiles via per-row FileRef."""
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    rasters = []
    cellid = 0
    first = True
    dropped = 0
    for r, fref in zip(tile, file_ref):
        if r is None:
            continue
        try:
            vt = ot._to_virtual_tile(r)
            if vt.is_virtual():
                with ot._open(r, file_ref=fref) as ds:
                    candidate = _dataset_to_gtiff_bytes(ds)
            else:
                candidate = bytes(vt.raster)
            with _serde.open_tile(candidate):
                pass
            if first:
                cid = r["cellid"] if hasattr(r, "__getitem__") else 0
                cellid = int(cid) if cid is not None else 0
                first = False
            rasters.append(candidate)
        except Exception:  # noqa: BLE001
            dropped += 1
            continue
    if not rasters:
        return None
    out = agg_core.combineavg_tiles(rasters)
    if out is None:
        return None
    return cellid.to_bytes(8, "big", signed=True) + bytes(out)


@pandas_udf(BinaryType())
def _combineavg_agg_sql_udf(tile: pd.Series) -> bytes:
    # SQL registration variant: returns raw GTiff bytes (no cellid envelope), so
    # SQL callers can wrap it directly with gbx_rst_fromcontent(<agg>, 'GTiff').
    # SQL has no tile cellid concept beyond the struct, and the envelope would
    # corrupt fromcontent — so the SQL aggregate drops the cellid (always 0).
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    rasters = []
    dropped = 0
    for r in tile:
        candidate = _tile_raster_bytes(r)
        if candidate is None:
            continue
        try:
            with _serde.open_tile(candidate):
                pass
            rasters.append(candidate)
        except Exception:  # noqa: BLE001
            dropped += 1
            continue
    if not rasters:
        return None
    # NOTE: drop-count has no metadata carrier at this layer (pandas_udf returns
    # bare bytes; no struct/metadata assembly here). The skip still stops raising.
    return agg_core.combineavg_tiles(rasters)


@pandas_udf(BinaryType())
def _frombands_agg_udf(tile: pd.Series, band_index: pd.Series) -> bytes:
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    indexed = []
    dropped = 0
    for r, idx in zip(tile, band_index):
        if idx is None:
            continue
        candidate = _tile_raster_bytes(r)
        if candidate is None:
            continue
        try:
            with _serde.open_tile(candidate):
                pass
            indexed.append((int(idx), candidate))
        except Exception:  # noqa: BLE001
            dropped += 1
            continue
    if not indexed:
        return None
    # NOTE: drop-count has no metadata carrier at this layer (pandas_udf returns
    # bare bytes; no struct/metadata assembly here). The skip still stops raising.
    return agg_core.frombands_tiles(indexed)


@pandas_udf(BinaryType())
def _frombands_agg_file_udf(
    tile: pd.Series, file_ref: pd.Series, band_index: pd.Series
) -> bytes:
    """FILE-aware frombands aggregator: reads virtual tiles via per-row FileRef."""
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    indexed = []
    dropped = 0
    for r, fref, idx in zip(tile, file_ref, band_index):
        if idx is None or r is None:
            continue
        try:
            vt = ot._to_virtual_tile(r)
            if vt.is_virtual():
                with ot._open(r, file_ref=fref) as ds:
                    candidate = _dataset_to_gtiff_bytes(ds)
            else:
                candidate = bytes(vt.raster)
            with _serde.open_tile(candidate):
                pass
            indexed.append((int(idx), candidate))
        except Exception:  # noqa: BLE001
            dropped += 1
            continue
    if not indexed:
        return None
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
    rasters = []
    dropped = 0
    for r in tile:
        candidate = _tile_raster_bytes(r)
        if candidate is None:
            continue
        try:
            with _serde.open_tile(candidate):
                pass
            rasters.append(candidate)
        except Exception:  # noqa: BLE001
            dropped += 1
            continue
    if not rasters:
        return None
    # NOTE: drop-count has no metadata carrier at this layer (pandas_udf returns
    # bare bytes; no struct/metadata assembly here). The skip still stops raising.
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
    out_crs: pd.Series = None,
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
    # Extent/size/out-CRS/power/max_pts are per-group constants; read from row 0.
    srid0 = srid.iloc[0]
    out_crs0 = None if out_crs is None else out_crs.iloc[0]
    return tin_core.idw_grid(
        xy,
        vals,
        xmin.iloc[0],
        ymin.iloc[0],
        xmax.iloc[0],
        ymax.iloc[0],
        int(width_px.iloc[0]),
        int(height_px.iloc[0]),
        out_srid=None if srid0 is None else int(srid0),
        power=2.0 if power is None else float(power.iloc[0]),
        max_pts=12 if max_pts is None else int(max_pts.iloc[0]),
        out_crs=out_crs0,
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
    out_crs: pd.Series = None,
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
    srid0 = srid.iloc[0]
    out_crs0 = None if out_crs is None else out_crs.iloc[0]
    return tin_core.delaunay_dtm(
        pts,
        bl,
        xmin.iloc[0],
        ymin.iloc[0],
        xmax.iloc[0],
        ymax.iloc[0],
        int(width_px.iloc[0]),
        int(height_px.iloc[0]),
        out_srid=None if srid0 is None else int(srid0),
        no_data=-9999.0 if no_data is None else float(no_data.iloc[0]),
        out_crs=out_crs0,
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
    out_crs: pd.Series = None,
) -> bytes:
    from databricks.labs.gbx.pyrx import _env
    from databricks.labs.gbx.pyrx.core import cellraster as cr

    _env.configure_gdal_env()
    # Zip cellid and value TOGETHER first so each value stays paired with its own
    # cellid, then drop pairs whose cellid is null.  Filtering cellid before zipping
    # would misalign values when nulls appear in the middle of the series.
    # pd.notna guards both Python None and float NaN (PySpark delivers a null Long
    # as float('nan') inside the pandas Series, so `c is not None` is insufficient).
    pairs = [(c, v) for c, v in zip(cellid, value) if pd.notna(c)]
    if not pairs:
        return None
    # Guard: if cellid arrived as float64 (a null-containing BIGINT column causes
    # PySpark/Arrow to upcast the Series), any H3 id > 2**53 has already been
    # silently rounded.  Fail loud instead of burning wrong pixels.  The Python
    # wrapper (rst_h3_rasterize_agg) casts to STRING before calling this UDF, so
    # it never triggers this guard.  SQL callers that pass a BIGINT column with
    # nulls should use CAST(cellid AS STRING).
    if pd.api.types.is_float_dtype(cellid) and any(
        abs(float(c)) > 2**53 for c, _ in pairs
    ):
        raise ValueError(
            "rst_h3_rasterize_agg: cellid column contains large H3 ids (> 2**53) "
            "but arrived as float64 — a null in a BIGINT cellid column caused "
            "PySpark/Arrow to upcast the Series, silently rounding cell ids.  "
            "Pass CAST(cellid AS STRING) to the SQL UDF, or use the Python "
            "rst_h3_rasterize_agg() wrapper which applies this cast automatically."
        )
    cells = [int(c) for c, _ in pairs]
    # Null value -> presence mask (1.0). A null in a typed (Double) value column
    # arrives as np.nan, not None, so guard with pd.isna (np.nan is not None).
    vals = [1.0 if v is None or pd.isna(v) else float(v) for _, v in pairs]
    cell_values = {}
    for c, v in zip(cells, vals):
        cell_values[c] = v  # last-wins (cells of one res don't overlap)

    res = cr._resolution([cr._h3_str(c) for c in cells])
    # Output CRS spec: out_crs (string) wins over the int srid; else grid-native
    # 4326. cellraster accepts an int SRID or a CRS string interchangeably.
    if out_crs is not None and out_crs.iloc[0] is not None:
        _srid = out_crs.iloc[0]
    elif srid is not None and srid.iloc[0] is not None:
        _srid = int(srid.iloc[0])
    else:
        _srid = 4326
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


@pandas_udf(BinaryType())
def _rst_quadbin_rasterize_agg_udf(
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
    out_crs: pd.Series = None,
) -> bytes:
    from databricks.labs.gbx.pyrx import _env
    from databricks.labs.gbx.pyrx.core import cellraster as cr

    _env.configure_gdal_env()
    # Zip cellid and value TOGETHER first so each value stays paired with its own
    # cellid, then drop pairs whose cellid is null.  Filtering cellid before zipping
    # would misalign values when nulls appear in the middle of the series.
    # pd.notna guards both Python None and float NaN (PySpark delivers a null Long
    # as float('nan') inside the pandas Series, so `c is not None` is insufficient).
    pairs = [(c, v) for c, v in zip(cellid, value) if pd.notna(c)]
    if not pairs:
        return None
    # Guard: if cellid arrived as float64 (a null-containing BIGINT column causes
    # PySpark/Arrow to upcast the Series), any quadbin id > 2**53 has already been
    # silently rounded.  Fail loud instead of burning wrong pixels.  The Python
    # wrapper (rst_quadbin_rasterize_agg) casts to STRING before calling this UDF,
    # so it never triggers this guard.  SQL callers that pass a BIGINT column with
    # nulls should use CAST(cellid AS STRING).
    if pd.api.types.is_float_dtype(cellid) and any(
        abs(float(c)) > 2**53 for c, _ in pairs
    ):
        raise ValueError(
            "rst_quadbin_rasterize_agg: cellid column contains large quadbin ids "
            "(> 2**53) but arrived as float64 — a null in a BIGINT cellid column "
            "caused PySpark/Arrow to upcast the Series, silently rounding cell ids.  "
            "Pass CAST(cellid AS STRING) to the SQL UDF, or use the Python "
            "rst_quadbin_rasterize_agg() wrapper which applies this cast automatically."
        )
    cells = [int(c) for c, _ in pairs]
    # Null value -> presence mask (1.0). A null in a typed (Double) value column
    # arrives as np.nan, not None, so guard with pd.isna (np.nan is not None).
    vals = [1.0 if v is None or pd.isna(v) else float(v) for _, v in pairs]
    cell_values = {}
    for c, v in zip(cells, vals):
        cell_values[c] = v  # last-wins (cells of one res don't overlap)

    _ad = cr._adapter("quadbin")
    res = _ad.resolution([_ad.to_key(c) for c in cells])
    # Output CRS spec: out_crs (string) wins over the int srid; else grid-native
    # 4326. cellraster accepts an int SRID or a CRS string interchangeably.
    if out_crs is not None and out_crs.iloc[0] is not None:
        _srid = out_crs.iloc[0]
    elif srid is not None and srid.iloc[0] is not None:
        _srid = int(srid.iloc[0])
    else:
        _srid = 4326
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
            cells, srid=_srid, pixel_size=_ps, mode=_mode, kring_pad=_kp, grid="quadbin"
        )
    return cr.cells_to_raster(cell_values, *grid, resolution=res, grid="quadbin")


@pandas_udf(BinaryType())
def _rst_bng_rasterize_agg_udf(
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
    out_crs: pd.Series = None,
) -> bytes:
    from databricks.labs.gbx.pyrx import _env
    from databricks.labs.gbx.pyrx.core import cellraster as cr

    _env.configure_gdal_env()
    # BNG cell ids are STRING on the public surface (e.g. "TQ3080"); the adapter
    # parses each to the internal Long via pygx._bng.parse (single source of truth).
    # Zip cellid and value TOGETHER first so each value stays paired with its own
    # cellid, then drop pairs whose cellid is null.  Filtering cellid before zipping
    # would misalign values when nulls appear in the middle of the series.
    # pd.notna guards both Python None and float NaN consistently across grid types.
    pairs = [(c, v) for c, v in zip(cellid, value) if pd.notna(c)]
    if not pairs:
        return None
    cells = [str(c) for c, _ in pairs]
    # Null value -> presence mask (1.0). A null in a typed (Double) value column
    # arrives as np.nan, not None, so guard with pd.isna (np.nan is not None).
    vals = [1.0 if v is None or pd.isna(v) else float(v) for _, v in pairs]
    cell_values = {}
    for c, v in zip(cells, vals):
        cell_values[c] = v  # last-wins (cells of one res don't overlap)

    _ad = cr._adapter("bng")
    res = _ad.resolution([_ad.to_key(c) for c in cells])
    # BNG is EPSG:27700-native; the srid arg is a no-op (forced 27700). The sample
    # points and output raster are both 27700 -- NO WGS84 hop.
    _srid = 27700
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
            cells, srid=_srid, pixel_size=_ps, mode=_mode, kring_pad=_kp, grid="bng"
        )
    return cr.cells_to_raster(cell_values, *grid, resolution=res, grid="bng")


# --- public Column wrappers (compose grouped-agg BINARY + scalar as_tile) ----
def rst_merge_agg(tile: ColLike) -> Column:
    """Merge a group's tile rasters into one spatial mosaic tile.

    Use inside ``.agg()``::

        df.groupBy(k).agg(prx.rst_merge_agg("tile").alias("merged"))

    Each tile carries its own georef/CRS, so the merge is spatial and the output
    spans the union extent. Returns a tile struct (cellid 0).
    """
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg, file_supported

    tc = _col(tile)
    if file_supported():
        return _as_tile_udf(_merge_agg_file_udf(tc, file_ref_arg(tc)))
    return _as_tile_udf(_merge_agg_udf(tc))


def rst_combineavg_agg(tile: ColLike) -> Column:
    """Per-pixel mean across a group's aligned tiles, ignoring NoData.

    Use inside ``.agg()``::

        df.groupBy(k).agg(prx.rst_combineavg_agg("tile").alias("avg"))

    Assumes the group's tiles are aligned (same shape/extent/CRS); raises if
    shapes differ. The output cellid is the group's first tile cellid. Returns a
    tile struct.
    """
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg, file_supported

    tc = _col(tile)
    if file_supported():
        return _as_tile_cellid_envelope_udf(
            _combineavg_agg_file_udf(tc, file_ref_arg(tc))
        )
    return _as_tile_cellid_envelope_udf(_combineavg_agg_udf(tc))


def rst_frombands_agg(tile: ColLike, band_index: ColLike) -> Column:
    """Stack a group's single-band tiles into one multi-band tile.

    Bands are ordered by ``band_index`` ASCENDING (the ordering guarantee).
    Use inside ``.agg()``::

        df.groupBy(k).agg(prx.rst_frombands_agg("tile", "band_index").alias("stacked"))

    Returns a tile struct (cellid 0).
    """
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg, file_supported

    tc = _col(tile)
    if file_supported():
        return _as_tile_udf(
            _frombands_agg_file_udf(tc, file_ref_arg(tc), _col(band_index))
        )
    return _as_tile_udf(_frombands_agg_udf(tc, _col(band_index)))


def rst_rasterize_agg(
    geom: ColLike,
    value: ColLike,
    xmin: ColLike,
    ymin: ColLike,
    xmax: ColLike,
    ymax: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    out_srid: ColLike,
) -> Column:
    """Burn a group's ``(geom, value)`` features into ONE tile.

    The extent/size/out_srid args are per-group constants. Overlap is last-wins.
    Use inside ``.agg()``::

        df.groupBy(k).agg(
            prx.rst_rasterize_agg("g", "v", 0, 0, 4, 4, 256, 256, 4326).alias("burned")
        )

    Returns a tile struct (cellid 0).
    """
    return _as_tile_udf(
        _rasterize_agg_udf(
            _col(geom),
            _col(value),
            _col(xmin),
            _col(ymin),
            _col(xmax),
            _col(ymax),
            _col(width_px),
            _col(height_px),
            _col(out_srid),
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
    out_srid: ColLike = None,
    power: ColLike = 2.0,
    max_pts: ColLike = 12,
    out_crs: ColLike = None,
) -> Column:
    """Streaming IDW grid per group: one ``(point, value)`` per row -> one tile.

    The extent/size/out-CRS/power/max_pts args are per-group constants. Equal to
    ``rst_gridfrompoints`` over the same points. ``out_crs`` (string) wins over
    the int ``out_srid``; both set -> error; neither -> CRS-less. Use in ``.agg()``::

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
            _col(out_srid) if out_srid is not None else f.lit(None),
            p,
            m,
            f.lit(out_crs) if out_crs is not None else f.lit(None),
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
    out_srid: ColLike = None,
    no_data: ColLike = -9999.0,
    out_crs: ColLike = None,
) -> Column:
    """Streaming Delaunay-TIN DTM per group: one Z-point per row -> one tile.

    ``breaklines`` is a per-group constant ARRAY<BINARY>; every other non-point
    arg is a per-group constant. Equal to ``rst_dtmfromgeoms`` over the same
    points. ``out_crs`` (string) wins over the int ``out_srid``; both -> error;
    neither -> CRS-less. Use inside ``.agg()``::

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
            _col(out_srid) if out_srid is not None else f.lit(None),
            nd,
            f.lit(out_crs) if out_crs is not None else f.lit(None),
        )
    )


def rst_h3_rasterize_agg(
    cellid: ColLike,
    value: ColLike = None,
    out_srid: ColLike = None,
    pixel_size: ColLike = None,
    xmin: ColLike = None,
    ymin: ColLike = None,
    xmax: ColLike = None,
    ymax: ColLike = None,
    width: ColLike = None,
    height: ColLike = None,
    mode: ColLike = None,
    kring_pad: ColLike = None,
    out_crs: ColLike = None,
) -> Column:
    """Rasterize a group's H3 cells into one tile (pixel-centroid burn).

    ``value`` omitted -> presence mask (1.0/NoData). Supply an explicit extent
    (xmin..height, e.g. from ``rst_h3_gridspec``) for aligned band stacking;
    else the grid is auto-derived per ``mode``/``kring_pad``. Output CRS:
    ``out_crs`` (string) wins over the int ``out_srid``; both -> error; neither
    -> grid-native EPSG:4326. Use inside ``.agg()``::

        df.groupBy(k).agg(prx.rst_h3_rasterize_agg("cellid").alias("tile"))

    SQL returns BINARY (the raw grouped-agg UDF); Python returns a tile struct
    (wrapped by ``_as_tile_udf``).
    """

    def _c(x, default):
        return _col(x) if x is not None else f.lit(default)

    _cellid_col = _col(cellid)
    if isinstance(_cellid_col, str):
        _cellid_col = f.col(_cellid_col)
    return _as_tile_udf(
        _rst_h3_rasterize_agg_udf(
            _cellid_col.cast("string"),
            _c(value, None),
            _c(out_srid, 4326),
            _c(pixel_size, None),
            _c(xmin, None),
            _c(ymin, None),
            _c(xmax, None),
            _c(ymax, None),
            _c(width, None),
            _c(height, None),
            _c(mode, "centroids"),
            _c(kring_pad, 1),
            _c(out_crs, None),
        )
    )


def rst_quadbin_rasterize_agg(
    cellid: ColLike,
    value: ColLike = None,
    out_srid: ColLike = None,
    pixel_size: ColLike = None,
    xmin: ColLike = None,
    ymin: ColLike = None,
    xmax: ColLike = None,
    ymax: ColLike = None,
    width: ColLike = None,
    height: ColLike = None,
    mode: ColLike = None,
    kring_pad: ColLike = None,
    out_crs: ColLike = None,
) -> Column:
    """Rasterize a group's quadbin cells into one tile (pixel-centroid burn).

    ``value`` omitted -> presence mask (1.0/NoData). Supply an explicit extent
    (xmin..height) for aligned band stacking; else the grid is auto-derived per
    ``mode``/``kring_pad``. Quadbin cells are lon/lat (EPSG:4326). Output CRS:
    ``out_crs`` (string) wins over the int ``out_srid``; both -> error; neither
    -> grid-native EPSG:4326. Use inside ``.agg()``::

        df.groupBy(k).agg(prx.rst_quadbin_rasterize_agg("cellid").alias("tile"))

    SQL returns BINARY (the raw grouped-agg UDF); Python returns a tile struct
    (wrapped by ``_as_tile_udf``).
    """

    def _c(x, default):
        return _col(x) if x is not None else f.lit(default)

    _cellid_col = _col(cellid)
    if isinstance(_cellid_col, str):
        _cellid_col = f.col(_cellid_col)
    return _as_tile_udf(
        _rst_quadbin_rasterize_agg_udf(
            _cellid_col.cast("string"),
            _c(value, None),
            _c(out_srid, 4326),
            _c(pixel_size, None),
            _c(xmin, None),
            _c(ymin, None),
            _c(xmax, None),
            _c(ymax, None),
            _c(width, None),
            _c(height, None),
            _c(mode, "centroids"),
            _c(kring_pad, 1),
            _c(out_crs, None),
        )
    )


def rst_bng_rasterize_agg(
    cellid: ColLike,
    value: ColLike = None,
    out_srid: ColLike = None,
    pixel_size: ColLike = None,
    xmin: ColLike = None,
    ymin: ColLike = None,
    xmax: ColLike = None,
    ymax: ColLike = None,
    width: ColLike = None,
    height: ColLike = None,
    mode: ColLike = None,
    kring_pad: ColLike = None,
    out_crs: ColLike = None,
) -> Column:
    """Rasterize a group's BNG cells into one tile (pixel-centroid burn).

    ``cellid`` is a STRING column of BNG cell ids (e.g. ``"TQ3080"``); each id is
    parsed via ``pygx._bng`` (single source of truth). ``value`` omitted ->
    presence mask (1.0/NoData). Supply an explicit extent (xmin..height) for
    aligned band stacking; else the grid is auto-derived per ``mode``/``kring_pad``.
    Use inside ``.agg()``::

        df.groupBy(k).agg(prx.rst_bng_rasterize_agg("cellid").alias("tile"))

    BNG is EPSG:27700-native (British National Grid): the ``out_srid`` / ``out_crs``
    arguments are accepted for API consistency with the H3/quadbin variants but are
    a **no-op** -- the output raster is always EPSG:27700, with no reprojection.

    SQL returns BINARY (the raw grouped-agg UDF); Python returns a tile struct
    (wrapped by ``_as_tile_udf``).
    """

    def _c(x, default):
        return _col(x) if x is not None else f.lit(default)

    return _as_tile_udf(
        _rst_bng_rasterize_agg_udf(
            _col(cellid),
            _c(value, None),
            _c(out_srid, 27700),
            _c(pixel_size, None),
            _c(xmin, None),
            _c(ymin, None),
            _c(xmax, None),
            _c(ymax, None),
            _c(width, None),
            _c(height, None),
            _c(mode, "centroids"),
            _c(kring_pad, 1),
            _c(out_crs, None),
        )
    )


# ---------------------------------------------------------------------------
# H3 cell bbox + gridspec helpers
# ---------------------------------------------------------------------------


@f.udf(_BBOX_SCHEMA)
def _h3_cell_bbox_udf(cellid, srid, mode, kring_pad, out_crs=None):
    """Return STRUCT<xmin,ymin,xmax,ymax> for one H3 cell in the output CRS.

    When *kring_pad* > 0 the cell is expanded to its k-ring neighbourhood
    before computing the bounding box, so the returned bbox covers the full
    padded neighbourhood of that cell. Output CRS: ``out_crs`` (string) wins over
    the int ``srid``; neither -> EPSG:4326.
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

    # Output-CRS spec: out_crs string wins over the int srid; else grid-native.
    # _cr._reproject accepts an int SRID or a CRS string interchangeably.
    if out_crs is not None:
        _srid = out_crs
    elif srid is not None:
        _srid = int(srid)
    else:
        _srid = 4326
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
    cellid: ColLike,
    out_srid: ColLike = None,
    mode: ColLike = None,
    out_crs: ColLike = None,
) -> Column:
    """Bounding box of one H3 cell in the output CRS (centroid point or envelope).

    Returns a ``STRUCT<xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE>``.

    Args:
        cellid:   Column holding the H3 cell id (integer).
        out_srid: Output CRS as an EPSG or ESRI code (int). Defaults to 4326.
        mode:     ``"centroids"`` (default) or ``"spatial_envelope"``.
        out_crs:  Output CRS string (``EPSG:x`` / ``ESRI:x`` / WKT); wins over
                  ``out_srid`` when set.
    """
    return _h3_cell_bbox_udf(
        _col(cellid),
        _col(out_srid) if out_srid is not None else f.lit(4326),
        _col(mode) if mode is not None else f.lit("centroids"),
        f.lit(0),
        f.lit(out_crs) if out_crs is not None else f.lit(None),
    )


def rst_h3_gridspec(
    df,
    cell_col="cellid",
    *group_cols,
    out_srid=4326,
    pixel_size=None,
    mode="centroids",
    kring_pad=1,
    out_crs=None,
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
        out_srid:    Output CRS as an EPSG or ESRI code (int). Defaults to 4326.
        pixel_size:  Ground resolution in CRS units. ``None`` = auto from H3
                     resolution (edge-length heuristic, same as
                     ``cellraster.compute_gridspec``).
        mode:        ``"centroids"`` (default) or ``"spatial_envelope"``.
        kring_pad:   k-ring padding applied per cell before computing its bbox.
                     Defaults to 1 (matches ``compute_gridspec`` default).
        out_crs:     Output CRS string (``EPSG:x`` / ``ESRI:x``); wins over
                     ``out_srid``. The ``grid.srid`` field carries its EPSG code
                     when it has one (else ``out_srid``), while the bbox
                     coordinates are computed in the resolved CRS.

    Returns:
        DataFrame grouped by *group_cols* with a ``grid`` struct column added.
    """
    # Resolve the output-CRS spec once (out_crs wins). The bbox coords use the
    # resolved CRS; the grid.srid INT field carries its EPSG code when available.
    _out_spec = out_crs if out_crs is not None else out_srid
    _resolved = cellraster_core._norm_out_crs(_out_spec)
    _is_geo = bool(_resolved.is_geographic)
    _epsg = _resolved.to_epsg()
    _srid_field = int(_epsg) if _epsg is not None else int(out_srid)
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

    # Per-cell expanded bbox (kring_pad applied inside the scalar UDF). The bbox is
    # computed in the resolved output CRS: pass out_crs when set (string wins),
    # else the int out_srid. _h3_cell_bbox_udf's out_crs wins over its srid arg.
    if out_crs is not None:
        b = _h3_cell_bbox_udf(
            _col(cell_col), f.lit(None), f.lit(mode), f.lit(kring_pad), f.lit(out_crs)
        )
    else:
        b = _h3_cell_bbox_udf(
            _col(cell_col), f.lit(out_srid), f.lit(mode), f.lit(kring_pad)
        )
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
    _srid_val = _srid_field

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
            if _is_geo:
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
    # Virtual-aware (header/pixel front-door). Every accessor is now the same
    # struct-accepting virtual-aware UDF the Python Column API uses (single-tile
    # arity, so the registered signature is unchanged). Header-only accessors go
    # through open_header (no pixel read); pixel accessors through _open.
    "gbx_rst_width": _u_width,
    "gbx_rst_height": _u_height,
    "gbx_rst_numbands": _u_numbands,
    "gbx_rst_srid": _u_srid,
    "gbx_rst_crs": _u_crs,
    "gbx_rst_pixelwidth": _u_pixelwidth,
    "gbx_rst_pixelheight": _u_pixelheight,
    "gbx_rst_upperleftx": _u_upperleftx,
    "gbx_rst_upperlefty": _u_upperlefty,
    "gbx_rst_scalex": _u_scalex,
    "gbx_rst_scaley": _u_scaley,
    "gbx_rst_isempty": _u_isempty,
    "gbx_rst_boundingbox": _u_boundingbox,
    "gbx_rst_metadata": _metadata_udf,
    "gbx_rst_type": _u_type,
    "gbx_rst_getnodata": _u_getnodata,
    # Virtual-aware header-only coord transforms (same UDF the Column API uses;
    # single-tile-struct + 2 scalar args, so the registered signature/arity is
    # unchanged).
    "gbx_rst_rastertoworldcoordx": _u_r2w_x,
    "gbx_rst_rastertoworldcoordy": _u_r2w_y,
    "gbx_rst_worldtorastercoordx": _u_w2r_x,
    "gbx_rst_worldtorastercoordy": _u_w2r_y,
    # Group 1 per-band statistics & scalar accessors (struct-accepting).
    "gbx_rst_avg": _u_avg,
    "gbx_rst_min": _u_min,
    "gbx_rst_max": _u_max,
    "gbx_rst_median": _u_median,
    "gbx_rst_pixelcount": _u_pixelcount,
    "gbx_rst_rotation": _u_rotation,
    "gbx_rst_skewx": _u_skewx,
    "gbx_rst_skewy": _u_skewy,
    "gbx_rst_format": _u_format,
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
    "gbx_rst_fromfile": _fromfile_sql_udf,
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
    "gbx_rst_setcrs": _setcrs_udf,
    "gbx_rst_transformcrs": _transformcrs_udf,
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
    "gbx_rst_quadbin_rasterize_agg": _rst_quadbin_rasterize_agg_udf,
    "gbx_rst_bng_rasterize_agg": _rst_bng_rasterize_agg_udf,
}

SQL_REGISTRY = {**_sql_accessors, **_sql_tile_ops, **_sql_aggregators}
