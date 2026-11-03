"""pyrx public API — Arrow-UDF Column wrappers (signatures mirror rasterx).

Swap-compatible with ``databricks.labs.gbx.rasterx.functions``:
    from databricks.labs.gbx.pyrx import functions as prx
    df.select(prx.rst_to_webmercator("tile"))
"""

# Before the pyspark imports: one clear error when PySpark is missing.
from databricks.labs.gbx import _pyspark  # noqa: F401

# isort: split

import contextlib
import functools
import json
import math
import os
import re
from typing import List, Optional

import pandas as pd
from pyspark.sql import Column, DataFrame, SparkSession
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

from databricks.labs.gbx import _gdal_policy, _register
from databricks.labs.gbx._gdal_policy import (
    DriverPolicyError,
    carrying_udf,
    data_path,
    driver_switch,
    open_bytes,
    open_data,
)
from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx._udf import ColLike, _col, _crs_col
from databricks.labs.gbx.pyrx.core import _nodata, accessors
from databricks.labs.gbx.pyrx.core import agg as agg_core
from databricks.labs.gbx.pyrx.core import analysis as analysis_core
from databricks.labs.gbx.pyrx.core import binning
from databricks.labs.gbx.pyrx.core import cellraster as cellraster_core
from databricks.labs.gbx.pyrx.core import chm as chm_core
from databricks.labs.gbx.pyrx.core import coords
from databricks.labs.gbx.pyrx.core import derivedband as derivedband_core
from databricks.labs.gbx.pyrx.core import edit, features, focal, gridagg, indices
from databricks.labs.gbx.pyrx.core import land_cover as land_cover_core
from databricks.labs.gbx.pyrx.core import mapalgebra as mapalgebra_core
from databricks.labs.gbx.pyrx.core import open_tile as ot
from databricks.labs.gbx.pyrx.core import ops as ops_core
from databricks.labs.gbx.pyrx.core import resample, stretch, terrain
from databricks.labs.gbx.pyrx.core import tessellate as tessellate_core
from databricks.labs.gbx.pyrx.core import tiling
from databricks.labs.gbx.pyrx.core import tin as tin_core
from databricks.labs.gbx.pyrx.core import warp, xyz
from databricks.labs.gbx.pyrx.core.compression import (
    internal_mask_env,
    match_alpha,
    source_profile,
    write_gtiff,
)
from databricks.labs.gbx.pyrx.core.escape import rst_apply, tile_to_numpy  # noqa: F401
from databricks.labs.gbx.pyrx.core.virtual_tile import (
    BAND_METADATA_KEY,
    V2_TILE_SCHEMA,
    VirtualTile,
)


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
        ("gbx_rst_custom_rastertogridavg", _RstCustomRasterToGridAvgUDTF),
        ("gbx_rst_custom_rastertogridcount", _RstCustomRasterToGridCountUDTF),
        ("gbx_rst_custom_rastertogridmax", _RstCustomRasterToGridMaxUDTF),
        ("gbx_rst_custom_rastertogridmin", _RstCustomRasterToGridMinUDTF),
        ("gbx_rst_custom_rastertogridmedian", _RstCustomRasterToGridMedianUDTF),
        ("gbx_rst_custom_rastertogridsum", _RstCustomRasterToGridSumUDTF),
        ("gbx_rst_custom_rastertogridvariance", _RstCustomRasterToGridVarianceUDTF),
        ("gbx_rst_custom_rastertogridstddev", _RstCustomRasterToGridStddevUDTF),
        ("gbx_rst_custom_tessellate", _RstCustomTessellateUDTF),
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

    # Override warp/clip SQL UDFs with fresh per-register factory builds so that
    # registered PROJ grid dirs are captured at registration time rather than at
    # module-import time (module-level singletons can't capture post-import dirs).
    def _reg_transformcrs(s):
        from databricks.labs.gbx.core import proj_grids

        _grid_dirs = tuple(proj_grids.get_registered_dirs())
        s.udf.register("gbx_rst_transformcrs", _build_transformcrs_sql_udf(_grid_dirs))

    def _reg_transform(s):
        from databricks.labs.gbx.core import proj_grids

        _grid_dirs = tuple(proj_grids.get_registered_dirs())
        s.udf.register("gbx_rst_transform", _build_transform_sql_udf(_grid_dirs))

    def _reg_to_webmercator(s):
        from databricks.labs.gbx.core import proj_grids

        _grid_dirs = tuple(proj_grids.get_registered_dirs())
        s.udf.register(
            "gbx_rst_to_webmercator", _build_to_webmercator_sql_udf(_grid_dirs)
        )

    def _reg_clip(s):
        from databricks.labs.gbx.core import proj_grids

        _grid_dirs = tuple(proj_grids.get_registered_dirs())
        s.udf.register("gbx_rst_clip", _build_clip_sql_udf(_grid_dirs))

    entries["gbx_rst_transformcrs"] = _reg_transformcrs
    entries["gbx_rst_transform"] = _reg_transform
    entries["gbx_rst_to_webmercator"] = _reg_to_webmercator
    entries["gbx_rst_clip"] = _reg_clip

    def _guard():
        from databricks.labs.gbx.pyrx import _env

        _env.assert_rasterio_available()

    return [(_guard, entries)]


def register(spark: SparkSession = None, only: Optional[List[str]] = None) -> None:
    """Explicitly register the pyrx functions as Spark SQL functions.

    Installs the same ``gbx_rst_*`` SQL names the heavyweight rasterx package
    uses, but powered by the pyspark/rasterio implementation (no JAR). Call this
    once when you want the functions from SQL. The Python Column API
    (``prx.rst_to_webmercator(col)``) works WITHOUT this call.

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
    globals()["_gbx_registered"] = True


# --- Virtual-aware tile helpers ---------------------------------------------
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


class _UnreadableTile(Exception):
    """A tile an op cannot process: it cannot be opened (corrupt bytes, a driver the policy refuses, decoded over
    the read cap), or it has no CRS to reproject from, or the target names no CRS.

    Carries the reason. :func:`_tile_op` turns it into the tile's error tile; an error in the op's own
    arguments is not one, and fails the call naming the value.
    """


def _is_gdal_io_error(exc) -> bool:
    """True for the error rasterio raises when GDAL cannot open or read a dataset (an ``OSError`` subclass)."""
    from rasterio.errors import RasterioIOError

    return isinstance(exc, RasterioIOError)


@contextlib.contextmanager
def _open_op(tile, file_ref=None, source=False, header=False):
    """:func:`ot._open` for a tile op: opening the tile fails as :class:`_UnreadableTile`, the op's own errors pass.
    ``source`` opens the tile's file whole, as GDAL does (:func:`ot.open_source`), for an op on a container.
    ``header`` opens it for its header only (:func:`ot.open_header`: CRS, size, transform; no window is read and the
    read cap's decoded-size gate does not apply), for a tile an op reads no pixel of.

    A file that cannot be reached is not an unreadable tile: an ``OSError`` that is not GDAL's (a
    ``FileNotFoundError`` from a UC Volume that has not caught up, once the staging retries are spent) fails
    the task so that Spark retries it, instead of becoming an error tile that stays.
    """
    with contextlib.ExitStack() as stack:
        try:
            if source:
                opened = ot.open_source(tile)
            elif header:
                opened = ot.open_header(tile, file_ref=file_ref)
            else:
                opened = ot._open(tile, file_ref=file_ref)
            ds = stack.enter_context(opened)
        except (MemoryError, RecursionError):
            raise
        except OSError as exc:
            if not _is_gdal_io_error(exc):
                raise
            raise _UnreadableTile(str(exc)) from exc
        except (
            Exception
        ) as exc:  # noqa: BLE001 - what GDAL or the policy says is the tile's reason
            raise _UnreadableTile(str(exc)) from exc
        yield ds


def _tile_op(*required, check=None, tiles=()):
    """Decorator for a tile-returning UDF body ``(tile, ..., <named parameters>)``.

    The checks run in this order. NULL in is NULL out: a NULL ``tile`` or a NULL argument named in ``required``
    gives NULL. A tile with neither bytes nor a path goes out as itself when it is an error tile (so the upstream
    reason survives) and is NULL otherwise. ``check``, when given, is then called with every argument by name and
    raises a ``ValueError`` that names a wrong one: that fails the call, also for a tile that cannot be opened.
    A tile the body cannot open is its error tile, with the reason (:class:`_UnreadableTile`).

    ``tiles`` names the other tile parameters of a function that takes more than one (``rst_align_to``'s
    reference): a NULL one is NULL, and an empty one goes out as the first error tile among them, as the first does.
    """

    def wrap(func):
        import inspect

        names = list(inspect.signature(func).parameters)
        at = [names.index(n) for n in required]
        tile_at = [0] + [names.index(n) for n in tiles]

        @functools.wraps(func)
        def run(*args):
            tile = args[0]
            if tile is None or any(args[i] is None for i in at + tile_at):
                return None
            empties = [
                _empty_tile_out(args[i]) for i in tile_at if _tile_is_empty(args[i])
            ]
            if (
                empties
            ):  # one per empty tile: an error tile out as itself, NULL for any other
                return next((e for e in empties if e is not None), None)
            if check is not None:
                check(**dict(zip(names, args)))
            try:
                return func(*args)
            except _UnreadableTile as exc:
                return _serde.build_error_tile(str(exc), _tile_cellid(tile))
            except OSError as exc:
                if not _is_gdal_io_error(
                    exc
                ):  # not GDAL's: the file is unreachable, fail the task
                    raise
                # GDAL failing to read the pixels of a tile that opened (truncated, corrupt)
                return _serde.build_error_tile(str(exc), _tile_cellid(tile))

        return run

    return wrap


def _dataset_to_gtiff_bytes(ds) -> bytes:
    """Encode an open rasterio dataset to standalone GTiff bytes (full extent).

    Used by multi-input ops (``rst_merge``) that consume their inputs through
    the ``_open_all`` front-door but need raster bytes for the core reducer.
    """
    from rasterio.io import MemoryFile

    data = ds.read()
    profile = source_profile(ds.profile, unless_jpeg=True)
    profile.update(driver="GTiff")
    # The source's own mask band (not an alpha band) is part of the raster: carry it.
    mask = ds.read_masks(1) if _nodata.has_own_mask(ds) else None
    with internal_mask_env(mask), MemoryFile() as mf:
        with mf.open(**profile) as dst:
            match_alpha(dst, ds.colorinterp)
            dst.write(data)
            if mask is not None:
                dst.write_mask(mask)
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


# --- Virtual-aware accessor UDF factories -----------------------------------
# Accessors take the FULL tile struct (not just the raster subfield) so a
# virtual tile's ``path`` is reachable. HEADER-ONLY accessors answer from the
# file header via ``open_header`` (no pixel read; window-correct for windowed
# virtual tiles); PIXEL accessors materialise the window via ``_open``. The
# core accessor math is never touched.
def _header_accessor_udf(core_fn, return_type):
    """Struct-accepting header-only accessor UDF (open_header, no pixel read)."""
    from databricks.labs.gbx.core import proj_grids
    from databricks.labs.gbx.pyrx import _env

    _grid_dirs = tuple(proj_grids.get_registered_dirs())  # captured at build → pickled

    @f.udf(return_type)
    def _udf(tile):
        if _tile_is_empty(tile):
            return None
        try:
            _env.configure_gdal_env(extra_proj_dirs=_grid_dirs)
            with ot.open_header(tile) as ds:
                return core_fn(ds)
        except Exception:  # noqa: BLE001
            return None

    return _udf


def _pixel_accessor_udf(core_fn, return_type, on_empty=None):
    """Struct-accepting pixel accessor UDF (_open, materialises the window). `on_empty` is the answer for an error tile (neither
    bytes nor a path, its reason in the metadata); a NULL tile, and any other tile with nothing in it, is NULL.
    """
    from databricks.labs.gbx.core import proj_grids
    from databricks.labs.gbx.pyrx import _env

    _grid_dirs = tuple(proj_grids.get_registered_dirs())  # captured at build → pickled

    @f.udf(return_type)
    def _udf(tile):
        if _tile_is_empty(tile):
            return on_empty if _error_tile_of(tile) is not None else None
        try:
            _env.configure_gdal_env(extra_proj_dirs=_grid_dirs)
            with ot._open(tile) as ds:
                return core_fn(ds)
        except Exception:  # noqa: BLE001
            return None

    return _udf


# --- FILE-aware 2-arg UDF factories ------------------
# These produce UDFs with signature (tile: Struct, file_ref: FileRef|null).
# The SQL registry keeps pointing at the single-arg ``_u_*`` UDFs (fallback path,
# no FILE acceleration for SQL); only the public Python Column bindings use ``_uf_*``.
def _header_accessor_udf_file(core_fn, return_type):
    """Struct + FileRef accepting header-only accessor UDF (2-arg).

    Signature: (tile: Struct, file_ref: FileRef|null) → return_type
    Calls open_header(tile, file_ref=file_ref) internally.  When file_ref is
    None the open_header front-door falls back to the plain-path read path.
    """
    from databricks.labs.gbx.core import proj_grids
    from databricks.labs.gbx.pyrx import _env

    _grid_dirs = tuple(proj_grids.get_registered_dirs())  # captured at build → pickled

    @f.udf(return_type)
    def _udf(tile, file_ref):
        if _tile_is_empty(tile):
            return None
        try:
            _env.configure_gdal_env(extra_proj_dirs=_grid_dirs)
            with ot.open_header(tile, file_ref=file_ref) as ds:
                return core_fn(ds)
        except Exception:  # noqa: BLE001
            return None

    return _udf


def _pixel_accessor_udf_file(core_fn, return_type, on_empty=None):
    """Struct + FileRef accepting pixel accessor UDF (2-arg). `on_empty` is as for `_pixel_accessor_udf`.

    Signature: (tile: Struct, file_ref: FileRef|null) → return_type
    Calls _open(tile, file_ref=file_ref) internally.  When file_ref is None
    the _open front-door falls back to the plain-path read path.
    """
    from databricks.labs.gbx.core import proj_grids
    from databricks.labs.gbx.pyrx import _env

    _grid_dirs = tuple(proj_grids.get_registered_dirs())  # captured at build → pickled

    @f.udf(return_type)
    def _udf(tile, file_ref):
        if _tile_is_empty(tile):
            return on_empty if _error_tile_of(tile) is not None else None
        try:
            _env.configure_gdal_env(extra_proj_dirs=_grid_dirs)
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
_u_isempty = _pixel_accessor_udf(accessors.isempty, BooleanType(), on_empty=True)


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


# --- FILE-aware 2-arg UDF singletons ----------------
# Used by the public Python Column bindings; SQL registry still uses ``_u_*``.
# Header-only (open_header — no pixel read):
_uf_height = _header_accessor_udf_file(accessors.height, IntegerType())
_uf_numbands = _header_accessor_udf_file(accessors.numbands, IntegerType())
_uf_srid = _header_accessor_udf_file(accessors.srid, IntegerType())
_uf_crs = _header_accessor_udf_file(accessors.crs, StringType())
_uf_width = _header_accessor_udf_file(accessors.width, IntegerType())
_uf_boundingbox = _header_accessor_udf_file(accessors.boundingbox, BinaryType())
# Pixel-reading (_open — materialises the window):
_uf_isempty = _pixel_accessor_udf_file(accessors.isempty, BooleanType(), on_empty=True)

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
    accessors.check_n_buckets(n_buckets)
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

# Back-compat aliases: these names once pointed at explicit UDF singletons; they now
# point at the factory-built virtual-aware UDFs.
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
    if _tile_is_empty(tile) or band is None:
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
    accessors.check_n_buckets(n_buckets)
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
@_tile_op("name")
def _getsubdataset_udf(tile, name):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    vt = ot._to_virtual_tile(tile)
    edits = [
        what
        for what, on in (
            ("a window", vt.window is not None),
            ("a clip", vt.clip_polygon is not None),
            ("pending edits", any(str(k).startswith("pending_") for k in vt.metadata)),
        )
        if on
    ]
    if edits:
        # a container has no pixel grid of its own for a window, a clip or an edit to apply to: the subdataset
        # of the whole file would silently ignore them
        raise _UnreadableTile(
            f"rst_getsubdataset: the tile carries {' and '.join(edits)}, which do not apply to the subdatasets of "
            "its container file. Fix: call gbx_rst_getsubdataset on the tile as read from the file (no window, clip "
            "or edits), then apply them to the subdataset it returns"
        )
    with _open_op(tile, source=True) as ds:
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
    if raster is None or drv is None:
        return None
    drv = ops_core.known_driver(drv, "rst_fromcontent")
    try:
        return _serde.build_tile(bytes(raster), drv)
    except (MemoryError, RecursionError):
        raise
    except (
        Exception
    ) as exc:  # noqa: BLE001 - what GDAL or the policy says is the content's reason
        return _serde.build_error_tile(
            f"rst_fromcontent: the content cannot be opened as a raster: {exc}. "
            "Fix: pass the bytes of a whole raster file in a format GDAL reads, such as the content column of "
            "spark.read.format('binaryFile').",
            0,
        )


def _fromfile_extra(path, driver, materialize):
    """The Python entry of ``rst_fromfile``: columns in, a tile Column out. ``materialize`` unset (None) or ``False`` is
    a virtual tile of the file (no pixel is read), as the light API always returned; ``True`` reads the pixels now, as
    the SQL name does, with the driver's materialize cap carried in (a worker cannot measure it).
    """
    from databricks.labs.gbx.ds.file_gbx import (
        _connect_aware_lru_sizing,
        _resolve_session_for_cap,
    )

    _cap = _connect_aware_lru_sizing(_resolve_session_for_cap())[0]
    udf = carrying_udf(_fromfile_udf, driver_switch())
    return udf(path, driver, materialize, f.lit(_cap))


# rst_fromfile: reference a raster on disk as a tile. LIGHT-TIER DEFAULT is a
# VIRTUAL tile — the source file IS the durable backing store, so the tile just
# points at the path (plus the whole-file window + metadata read from the header,
# no pixel I/O). ``materialize=True`` reads the pixels now and returns a
# materialized (bytes) tile. Because the path is already durable there is no
# ``virtualize_dir``/``virtualize_prefix`` (those exist only for functions that
# COMPUTE new pixels and need somewhere to write them).
# A bad/missing path yields NULL (matching the heavyweight's Option(...).orNull).
_RASTER_SUFFIXES = (
    ".tif",
    ".tiff",
    ".nc",
    ".png",
    ".jpg",
    ".jpeg",
    ".jp2",
    ".img",
    ".hdf",
    ".h5",
    ".grib",
    ".grb",
    ".grb2",
    ".asc",
    ".bil",
    ".dem",
)


def _names_a_vrt(local) -> bool:
    """Whether the file at ``local`` is a VRT: by its name, or because it starts with the VRT root element (after
    optional whitespace and an XML declaration). A file named for a known raster format is not opened to look.
    """
    name = str(local).lower()
    if name.endswith(".vrt"):
        return True
    if name.endswith(_RASTER_SUFFIXES):
        return False
    try:
        with open(local, "rb") as fh:
            head = fh.read(4096).lstrip()
    except OSError:
        return False
    if head.startswith(b"<?xml"):
        head = head[head.find(b"?>") + 2 :].lstrip() if b"?>" in head else b""
    return head.startswith(b"<VRTDataset")


def _fromfile_render_vrt(path, local, drv, cap_bytes):
    """``rst_fromfile(<a VRT>, materialize=True)``: render the VRT into a self-contained GTiff tile.

    A VRT's file holds XML that names its sources, and a tile's bytes may not (the policy refuses VRT bytes), so the
    VRT is opened by its path (where its relative sources resolve) and its pixels are written as GTiff by
    ``write_gtiff``, the canonical writer. The rendered size is gated first, from the header, through
    ``materialize_decision`` (kind ``read``): over the cap is an error tile, not a read. Every refusal is the
    tile's reason with a Fix: NULL is for a path that does not exist.
    """
    import os

    import numpy as np

    from databricks.labs.gbx.ds.file_gbx import materialize_decision

    if "://" in str(
        local
    ):  # an object-store or http URL: GDAL cannot resolve a VRT's relative sources there
        return _serde.build_error_tile(
            f"rst_fromfile materialize=True unsuccessful: the VRT {path} is not on local or FUSE-mounted "
            "storage, and a VRT must be on local or FUSE-mounted storage to render. Fix: use a virtual tile "
            "(omit materialize=True), or copy the VRT and its sources to a Volume.",
            0,
        )
    if not os.path.exists(local):
        return None
    try:
        with open_data(local) as src:
            decoded = (
                src.width
                * src.height
                * sum(int(np.dtype(dt).itemsize) for dt in src.dtypes)
            )
            if materialize_decision(decoded, "read", cap_bytes=cap_bytes) != "stream":
                mib = f"{decoded / 1024 ** 2:.1f} MiB"
                return _serde.build_error_tile(
                    f"rst_fromfile materialize=True unsuccessful: the VRT {path} renders to {mib} decoded, "
                    "over this compute's materialize cap. Fix: omit materialize=True (a virtual tile of the "
                    "VRT reads it in windows), or retile the VRT into smaller VRTs first.",
                    0,
                )
            data = src.read()
            mask = src.read_masks(1) if _nodata.has_own_mask(src) else None
            new_bytes = write_gtiff(
                src.profile, data, alpha_like=src.colorinterp, mask=mask
            )
    except DriverPolicyError as exc:
        return _serde.build_error_tile(str(exc), 0)
    except (
        Exception
    ) as exc:  # noqa: BLE001 — an unreadable VRT (a missing source) is the tile's reason
        return _serde.build_error_tile(
            f"rst_fromfile materialize=True unsuccessful: the VRT {path} could not be rendered: {exc}. "
            "Fix: check that every source file the VRT names exists and is readable here.",
            0,
        )
    return _serde.build_tile(new_bytes, drv, 0)


def _fromfile_impl(path, driver, materialize, cap_bytes=None):
    """Shared body for both the Python-binding UDF (4-arg) and the SQL UDF (2-arg,
    virtual). Returns a v2 tile row (virtual by default, materialized when asked),
    or None on a bad/missing path.

    ``cap_bytes`` is the DRIVER-captured connect-aware materialize cap (baked by
    ``rst_fromfile`` at Column-build time and carried in as an f.lit). It is used
    ONLY on the materialize=True path. _fromfile_impl runs inside a UDF on a
    session-less Serverless worker, so it must NOT re-resolve the cap from a live
    session (that would wrongly use the 256 MiB classic cap). When ``cap_bytes`` is
    None (the SQL UDF paths, which do not bake a driver cap), materialize_decision
    falls back to session resolution."""
    if path is None:
        return None
    from databricks.labs.gbx.ds.file_gbx import _stage_local_if_needed, to_local_path
    from databricks.labs.gbx.pyrx import _env

    # a column's path is data: a remote URL is refused here, at once, unless the driver's switch was carried in
    data_path(str(path), "rst_fromfile")
    _env.configure_gdal_env()
    drv = ops_core.known_driver("GTiff" if driver is None else driver, "rst_fromfile")
    local = to_local_path(str(path))

    if materialize and _names_a_vrt(local):
        return _fromfile_render_vrt(str(path), local, drv, cap_bytes)

    if materialize:
        # Eager path: read the source bytes SEQUENTIALLY (FUSE-safe) and re-encode
        # via an in-memory MemoryFile — a tiled/COG GTiff over a UC Volume seeks to
        # block offsets on read, which Volume FUSE can't serve, so we read the whole
        # file then open from memory rather than rasterio.open() on the path.
        # _stage_local_if_needed handles the probe→stage for eventual-consistency
        # on /Volumes paths (returns a temp local copy when staging is needed).
        from rasterio.io import MemoryFile

        from databricks.labs.gbx.ds.file_gbx import (
            StageTooLargeError,
            materialize_decision,
        )

        try:
            staged, is_temp = _stage_local_if_needed(local)
            try:
                # Serverless-cap guard: gate on the DECODED size (the RAM-relevant
                # quantity — the read below decodes the WHOLE raster into memory),
                # NOT the compressed on-disk size (getsize), which under-estimates
                # RAM for a highly-compressible source and would let a large-decoded
                # tile slip past the guard and OOM a ~1 GiB Serverless task. Read the
                # header only (cheap; no pixel decode) to compute
                # count*width*height*itemsize, then fail fast with an actionable error
                # BEFORE reading the file. kind="ingest" → "error" over cap. The cap
                # is the DRIVER-captured cap_bytes (baked by rst_fromfile); this runs
                # inside a UDF on a session-less worker, so resolving the cap here
                # would wrongly use the 256 MiB classic cap. The virtual default
                # (materialize=False) never reaches this branch.
                import numpy as _np_cap

                with open_data(staged) as _hsrc:
                    _decoded_bytes = (
                        _hsrc.count
                        * _hsrc.width
                        * _hsrc.height
                        * int(_np_cap.dtype(_hsrc.dtypes[0]).itemsize)
                    )
                if (
                    materialize_decision(_decoded_bytes, "ingest", cap_bytes=cap_bytes)
                    == "error"
                ):
                    raise StageTooLargeError(
                        f"rst_fromfile materialize=True unsuccessful: "
                        f"{_decoded_bytes / 1024 ** 2:.1f} MiB decoded exceeds the "
                        f"Serverless materialize cap. Omit materialize=True (the "
                        f"virtual default is Serverless-safe) or split the source "
                        f"(sizeInMB=)."
                    )
                with open(staged, "rb") as _fh:
                    _src_bytes = _fh.read()
            finally:
                if is_temp:
                    import os as _os

                    if _os.path.exists(staged):
                        _os.remove(staged)
            with MemoryFile(_src_bytes) as _src_mf, open_bytes(_src_mf) as src:
                data = src.read()
                # The file's own mask band (not an alpha band) is part of its bytes;
                # carry it, as a virtual tile of this file reads it.
                mask = src.read_masks(1) if _nodata.has_own_mask(src) else None
                new_bytes = write_gtiff(
                    src.profile, data, alpha_like=src.colorinterp, mask=mask
                )
        except StageTooLargeError:
            # Cap-guard error: propagate — this is an actionable, user-visible error,
            # not a bad-path that should silently null out. The UDF boundary converts
            # it to a task failure on Spark, but direct callers (e.g. tests) see it.
            raise
        except DriverPolicyError as exc:
            # the policy refused this file's format: the reason (and its Fix) is the answer, never NULL
            return _serde.build_error_tile(str(exc), 0)
        except (
            Exception
        ):  # noqa: BLE001 — a bad/missing path is NULL, matching heavyweight
            return None
        return _serde.build_tile(new_bytes, drv, 0)

    # Default (virtual): open the HEADER only (no pixel read; header reads don't
    # seek block offsets, so FUSE is fine) to record width/height/count, and
    # return a bytes-free tile that points at the path over its whole-file window.
    # Like the raster reader's passthrough tile, ``crs`` is intentionally left
    # NULL: that field doubles as open_tile's warp-target, so setting it to the
    # source CRS would make a later rst_setsrid see pending_srid != crs and warp
    # spuriously. The source CRS stays implicit in the path.

    try:
        with open_data(local) as src:
            width, height, count = src.width, src.height, src.count
    except DriverPolicyError as exc:
        # a refusal by the driver policy is the answer (reason and Fix), never NULL
        return _serde.build_error_tile(str(exc), 0)
    except Exception:  # noqa: BLE001 — a bad/missing path is NULL, matching heavyweight
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
def _fromfile_udf(path, driver, materialize, cap_bytes):
    """Python-binding UDF: 4-arg (path, driver, materialize, cap_bytes).

    ``cap_bytes`` is the DRIVER-captured connect-aware materialize cap baked by
    ``rst_fromfile`` as an f.lit — passed through so the worker-side cap guard
    uses the correct 64 MiB Serverless cap instead of re-resolving (session-less
    worker → wrong 256 MiB classic cap)."""
    return _fromfile_impl(path, driver, bool(materialize), cap_bytes=cap_bytes)


@f.udf(V2_TILE_SCHEMA)
def _fromfile_sql_udf(path, driver):
    """SQL UDF registered by the LIGHT (pyrx) tier: 2-arg (path, driver) → a
    VIRTUAL tile (materialize=False, the light-tier default). Keeps the historical
    2-arg SQL surface; the tier that registers decides virtual vs materialized."""
    return _fromfile_impl(path, driver, False)


def _build_fromfile_sql_materialized_udf(cap_bytes=None):
    """The SQL UDF behind ``gbx_rst_fromfile``: 2-arg (path, driver) → a MATERIALIZED tile (bytes present). Same call
    text as the light registration; the heavyweight tier reads the pixels because JVM/heavy expressions cannot use a
    virtual path-only tile. Whichever tier's register() ran last wins.

    ``cap_bytes`` is the driver's connect-aware materialize cap (64 MiB on Serverless), which the registrar captures
    (``factory_args: read_cap``): a worker has no session to measure it, and would use the classic 256 MiB.
    """

    @f.udf(V2_TILE_SCHEMA)
    def fromfile_sql_materialized_udf(path, driver):
        return _fromfile_impl(path, driver, True, cap_bytes=cap_bytes)

    return fromfile_sql_materialized_udf


_fromfile_sql_materialized_udf = _build_fromfile_sql_materialized_udf()


# rst_merge: single ARRAY<tile struct> arg in one row -> mosaic tile.
def _refusal_as_error_tile(func):
    """A tile-returning UDF body whose group has a member decoded over the read cap answers that group's error tile with
    the refusal (reason and Fix), never a result built from the other members."""

    @functools.wraps(func)
    def run(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except agg_core.ReadCapRefused as exc:
            return _serde.build_error_tile(str(exc), 0)

    return run


def _array_without_members(tiles):
    """What an array function answers when no member can be used: the first error tile when every element is one (as
    it is, so the upstream reason survives), else NULL."""
    return _error_tile_of_group(_only_error_tiles(tiles))


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
            # (which opens every member and re-raises on the first failure).
            with _serde.open_tile(
                candidate, gate=False
            ):  # a validity probe: the merge streams a group past the budget
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
    try:
        result = _merge_bytes(tiles)
    except (agg_core.MosaicTooLarge, agg_core.ReadCapRefused) as exc:
        return _serde.build_error_tile(
            str(exc), 0
        )  # this row's answer, not the query's failure
    if result is None:
        return _array_without_members(
            tiles
        )  # NULL unless every element is an error tile
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
    try:
        result = _merge_bytes(tiles)
    except (agg_core.MosaicTooLarge, agg_core.ReadCapRefused) as exc:
        return _serde.build_error_tile(str(exc), 0)
    if result is None:
        return _array_without_members(tiles)
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
            with _serde.open_tile(
                candidate, gate=False
            ):  # a validity probe: the merge streams a group past the budget
                pass
            rasters.append(candidate)
        except Exception:  # noqa: BLE001
            dropped += 1
            continue

    if not rasters:
        return _array_without_members(tiles)
    try:
        new_bytes = agg_core.merge_tiles(rasters)
    except (agg_core.MosaicTooLarge, agg_core.ReadCapRefused) as exc:
        return _serde.build_error_tile(str(exc), 0)
    tile = _serde.build_tile(new_bytes, "GTiff", 0)
    if dropped:
        tile["metadata"][
            "last_error"
        ] = f"RST_Merge: skipped {dropped} corrupt input tile(s)"
    return tile


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
            with _serde.open_tile(
                candidate, gate=False
            ):  # a validity probe: the combine folds a group past the budget window by window
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
@_refusal_as_error_tile
def _combineavg_udf(tiles):
    if not tiles:
        return None
    result = _combineavg_bytes(tiles)
    if result is None:
        return _array_without_members(tiles)
    new_bytes, cellid, dropped = result
    tile = _serde.build_tile(new_bytes, "GTiff", cellid)
    if dropped:
        tile["metadata"][
            "last_error"
        ] = f"RST_CombineAvg: skipped {dropped} corrupt input tile(s)"
    return tile


@f.udf(V2_TILE_SCHEMA)
@_refusal_as_error_tile
def _combineavg_v2_udf(tiles, virtualize_dir, virtualize_prefix, materialize):
    if not tiles:
        return None
    result = _combineavg_bytes(tiles)
    if result is None:
        return _array_without_members(tiles)
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
@_refusal_as_error_tile
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
            with _serde.open_tile(
                candidate, gate=False
            ):  # a validity probe: the combine folds a group past the budget window by window
                pass
            rasters.append(candidate)
            good_elems.append(t)
        except Exception:  # noqa: BLE001
            dropped += 1
            continue

    if not rasters:
        return _array_without_members(tiles)
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


# ---------------------------------------------------------------------------
# rst_combine{min,max,sum,count,median,stddev}: ARRAY<tile struct> -> tile.
# Mirrors gbx_rst_combine{min,max,median,sum,stddev,count}: per-pixel statistic
# across a stack of aligned tiles, NoData-aware (reuses agg_core.combine_*_tiles).
# Structure mirrors rst_combineavg: shared _bytes body + UDF wrappers + public API.
# ---------------------------------------------------------------------------


def _combine_stat_bytes(tiles, stat: str):
    """Shared body for the combine-family stats (mirrors _combineavg_bytes).

    Collects GTiff bytes from each non-empty, non-corrupt input tile, resolves
    cellid (shared if all match, else -1), then calls the stat-specific reducer.
    Returns ``(new_bytes, cellid, dropped)`` or ``None`` when all inputs are
    empty/corrupt.
    """
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
            with _serde.open_tile(
                candidate, gate=False
            ):  # a validity probe: the combine folds a group past the budget window by window
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
    _reducer = {
        "min": agg_core.combine_min_tiles,
        "max": agg_core.combine_max_tiles,
        "sum": agg_core.combine_sum_tiles,
        "count": agg_core.combine_count_tiles,
        "median": agg_core.combine_median_tiles,
        "stddev": agg_core.combine_stddev_tiles,
    }[stat]
    new_bytes = _reducer(rasters)
    return new_bytes, cellid, dropped


def _make_combine_stat_udf(stat: str):
    """Factory that produces a ``@f.udf(V2_TILE_SCHEMA)`` for the given stat."""
    _stat_cap = stat.capitalize()

    @_refusal_as_error_tile
    def _udf(tiles):
        if not tiles:
            return None
        result = _combine_stat_bytes(tiles, stat)
        if result is None:
            return _array_without_members(tiles)
        new_bytes, cellid, dropped = result
        tile = _serde.build_tile(new_bytes, "GTiff", cellid)
        if dropped:
            tile["metadata"][
                "last_error"
            ] = f"RST_Combine{_stat_cap}: skipped {dropped} corrupt input tile(s)"
        return tile

    _udf.__name__ = f"_combine{stat}_udf"
    return f.udf(V2_TILE_SCHEMA)(_udf)


def _make_combine_stat_v2_udf(stat: str):
    """Factory for the force-output (v2) variant of a combine-stat UDF."""
    _stat_cap = stat.capitalize()

    @_refusal_as_error_tile
    def _v2_udf(tiles, virtualize_dir, virtualize_prefix, materialize):
        if not tiles:
            return None
        result = _combine_stat_bytes(tiles, stat)
        if result is None:
            return _array_without_members(tiles)
        new_bytes, cellid, dropped = result
        row = _shaped_result_row(
            new_bytes, cellid, virtualize_dir, virtualize_prefix, materialize
        )
        if row is not None and dropped:
            row["metadata"][
                "last_error"
            ] = f"RST_Combine{_stat_cap}: skipped {dropped} corrupt input tile(s)"
        return row

    _v2_udf.__name__ = f"_combine{stat}_v2_udf"
    return f.udf(V2_TILE_SCHEMA)(_v2_udf)


# Module-level UDF objects for each stat (registered in _sql_tile_ops below).
_combinemin_udf = _make_combine_stat_udf("min")
_combinemax_udf = _make_combine_stat_udf("max")
_combinesum_udf = _make_combine_stat_udf("sum")
_combinecount_udf = _make_combine_stat_udf("count")
_combinemedian_udf = _make_combine_stat_udf("median")
_combinestddev_udf = _make_combine_stat_udf("stddev")

_combinemin_v2_udf = _make_combine_stat_v2_udf("min")
_combinemax_v2_udf = _make_combine_stat_v2_udf("max")
_combinesum_v2_udf = _make_combine_stat_v2_udf("sum")
_combinecount_v2_udf = _make_combine_stat_v2_udf("count")
_combinemedian_v2_udf = _make_combine_stat_v2_udf("median")
_combinestddev_v2_udf = _make_combine_stat_v2_udf("stddev")


# ---------------------------------------------------------------------------
# rst_align_to: warp a tile to match a reference tile's grid.
# Mirrors gbx_rst_align_to: nearest-neighbour resampling, output has exactly
# the same CRS, width, height, and geotransform as the reference tile.
# ---------------------------------------------------------------------------


def _align_to_bytes(tile, reference):
    """Shared align_to body: open both tiles and warp ``tile`` onto the grid of ``reference``.

    An unreadable or refused tile (either one), or ``tile`` over the read cap, is :class:`_UnreadableTile`. The
    reference gives only its CRS, size and transform, so it is opened for its header: no read cap applies to it.
    """
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _open_op(tile) as src, _open_op(reference, header=True) as ref:
        return agg_core.align_datasets(src, ref)


@f.udf(V2_TILE_SCHEMA)
@_tile_op(tiles=("reference",))
def _align_to_udf(tile, reference):
    new_bytes = _align_to_bytes(tile, reference)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
@_tile_op(tiles=("reference",))
def _align_to_v2_udf(tile, reference, virtualize_dir, virtualize_prefix, materialize):
    new_bytes = _align_to_bytes(tile, reference)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


# rst_chm: Canopy Height Model = clamp(align(DSM->DEM) - DEM, min=0).
# DSM is warped onto the DEM's grid; negative differences clamp to 0.
# NoData in either input propagates to the output.
# ---------------------------------------------------------------------------


@f.udf(V2_TILE_SCHEMA)
def _chm_udf(dsm_tile, dem_tile):
    if dsm_tile is None or dem_tile is None:
        return None
    empties = [_empty_tile_out(t) for t in (dsm_tile, dem_tile) if _tile_is_empty(t)]
    if empties:  # an error tile goes out as itself, so the upstream reason survives
        return next((e for e in empties if e is not None), None)
    try:
        vt_dsm = ot._to_virtual_tile(dsm_tile)
        dsm_bytes = (
            ot.materialize_to_bytes(vt_dsm).raster
            if vt_dsm.is_virtual()
            else bytes(vt_dsm.raster)
        )
        vt_dem = ot._to_virtual_tile(dem_tile)
        dem_bytes = (
            ot.materialize_to_bytes(vt_dem).raster
            if vt_dem.is_virtual()
            else bytes(vt_dem.raster)
        )
        new_bytes = chm_core.chm(dsm_bytes, dem_bytes)
    except (MemoryError, RecursionError):
        raise
    except (
        Exception
    ) as exc:  # noqa: BLE001 - a tile that cannot be read or aligned is data
        return _serde.build_error_tile(str(exc), _tile_cellid(dsm_tile))
    if new_bytes is None:
        return None
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(dsm_tile))


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
            with _serde.open_tile(
                candidate, gate=False
            ):  # a validity probe: the decode gates
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
@_refusal_as_error_tile
def _frombands_udf(bands):
    if not bands:
        return None
    result = _frombands_bytes(bands)
    if result is None:
        return _array_without_members(bands)
    new_bytes, cellid, dropped = result
    tile = _serde.build_tile(new_bytes, "GTiff", cellid)
    if dropped:
        tile["metadata"][
            "last_error"
        ] = f"RST_FromBands: skipped {dropped} corrupt input tile(s)"
    return tile


@f.udf(V2_TILE_SCHEMA)
@_refusal_as_error_tile
def _frombands_v2_udf(bands, virtualize_dir, virtualize_prefix, materialize):
    if not bands:
        return None
    result = _frombands_bytes(bands)
    if result is None:
        return _array_without_members(bands)
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
@_refusal_as_error_tile
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
            with _serde.open_tile(
                candidate, gate=False
            ):  # a validity probe: the decode gates
                pass
            indexed.append((i, candidate))
            if first_good is None:
                first_good = t
        except Exception:  # noqa: BLE001
            dropped += 1
            continue

    if not indexed:
        return _array_without_members(bands)
    cellid = _tile_cellid(first_good) if first_good is not None else 0
    new_bytes = agg_core.frombands_tiles(indexed)
    tile = _serde.build_tile(new_bytes, "GTiff", cellid)
    if dropped:
        tile["metadata"][
            "last_error"
        ] = f"RST_FromBands: skipped {dropped} corrupt input tile(s)"
    return tile


# --- Tier 1b: tile-returning warp UDFs -------------------------------------
def _reproject(who, ds, project):
    """``project(ds)``, the reprojection of an open tile, with the tile's own failures as its error tile.

    A tile with no CRS cannot be reprojected, and a target the CRS library does not know (an SRID that names
    nothing) is data too: both are the tile's error tile with the reason, not a failed task. A tile georeferenced
    by ground control points has a CRS: its GCP projection.
    """
    from rasterio.errors import CRSError

    if warp.source_crs(ds) is None:
        raise _UnreadableTile(
            f"{who}: the tile has no CRS, so it cannot be reprojected. "
            "Fix: set its CRS first with gbx_rst_setcrs"
        )
    try:
        return project(ds)
    except CRSError as exc:
        raise _UnreadableTile(f"{who}: {exc}") from exc


def _transform_bytes(tile, target_srid, file_ref=None):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    target = int(target_srid)

    def project(ds):
        # CRITICAL: for a MATERIALIZED tile with an identity transform (source EPSG ==
        # target_srid), return the ORIGINAL raster bytes verbatim — no re-encode.
        # ``agg_core.merge_tiles`` sorts inputs on their RAW GTiff byte content to
        # choose a deterministic, heavy-parity last-wins overlap winner.  Re-serialising
        # through a rasterio MemoryFile write produces byte-different output (header
        # metadata may differ even though pixels are identical) and shifts the sort key —
        # potentially flipping the overlap winner vs. the heavy tier.  For VIRTUAL tiles
        # (no original bytes) there is nothing to return verbatim, so they fall through
        # to the normal warp path (which itself identity-short-circuits at the pixel level).
        from databricks.labs.gbx.core.crs import authority_srid_of

        vt = ot._to_virtual_tile(tile)
        if not vt.is_virtual() and vt.raster is not None:
            src_epsg = authority_srid_of(ds.crs)
            if src_epsg is not None and src_epsg == target:
                return bytes(vt.raster)
        return warp.reproject_to_srid(ds, target)

    with _open_op(tile, file_ref=file_ref) as ds:
        return _reproject("RST_Transform", ds, project)


@f.udf(V2_TILE_SCHEMA)
@_tile_op("target_srid")
def _transform_udf(tile, target_srid):
    new_bytes = _transform_bytes(tile, target_srid)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
@_tile_op("target_srid")
def _transform_v2_udf(
    tile, target_srid, virtualize_dir, virtualize_prefix, materialize
):
    new_bytes = _transform_bytes(tile, target_srid)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


@f.udf(V2_TILE_SCHEMA)
@_tile_op("target_srid")
def _uf_transform(tile, file_ref, target_srid):
    new_bytes = _transform_bytes(tile, target_srid, file_ref=file_ref)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


def _build_uf_transform(grid_dirs):
    """FILE-aware transform UDF factory: captures grid_dirs at build time → pickled."""
    from databricks.labs.gbx.pyrx import _env

    @f.udf(V2_TILE_SCHEMA)
    @_tile_op("target_srid")
    def _udf(tile, file_ref, target_srid):
        _env.configure_gdal_env(extra_proj_dirs=grid_dirs)
        new_bytes = _transform_bytes(tile, target_srid, file_ref=file_ref)
        return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))

    return _udf


def _build_transform_v2_udf(grid_dirs):
    """Force-output transform UDF factory: captures grid_dirs at build time → pickled."""
    from databricks.labs.gbx.pyrx import _env

    @f.udf(V2_TILE_SCHEMA)
    @_tile_op("target_srid")
    def _udf(tile, target_srid, virtualize_dir, virtualize_prefix, materialize):
        _env.configure_gdal_env(extra_proj_dirs=grid_dirs)
        new_bytes = _transform_bytes(tile, target_srid)
        return _shaped_result_row(
            new_bytes,
            _tile_cellid(tile),
            virtualize_dir,
            virtualize_prefix,
            materialize,
        )

    return _udf


def _build_transform_sql_udf(grid_dirs):
    """SQL-registered (no file_ref) transform UDF factory: captures grid_dirs."""
    from databricks.labs.gbx.pyrx import _env

    @f.udf(V2_TILE_SCHEMA)
    @_tile_op("target_srid")
    def _udf(tile, target_srid):
        _env.configure_gdal_env(extra_proj_dirs=grid_dirs)
        new_bytes = _transform_bytes(tile, target_srid)
        return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))

    return _udf


def _check_resampling(*, resampling, **_):
    """A word that names no resampling raises, also for a tile already in 3857 (checked by ``_tile_op``)."""
    from databricks.labs.gbx.pyrx.core._util import resampling_enum

    resampling_enum(str(resampling))


def _to_webmercator_bytes(tile, resampling, file_ref=None):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _open_op(tile, file_ref=file_ref) as ds:
        return _reproject(
            "RST_ToWebMercator",
            ds,
            lambda d: warp.reproject_to_srid(d, 3857, resampling=str(resampling)),
        )


@f.udf(V2_TILE_SCHEMA)
@_tile_op("resampling", check=_check_resampling)
def _to_webmercator_udf(tile, resampling):
    new_bytes = _to_webmercator_bytes(tile, resampling)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
@_tile_op("resampling", check=_check_resampling)
def _to_webmercator_v2_udf(
    tile, resampling, virtualize_dir, virtualize_prefix, materialize
):
    new_bytes = _to_webmercator_bytes(tile, resampling)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


@f.udf(V2_TILE_SCHEMA)
@_tile_op("resampling", check=_check_resampling)
def _uf_to_webmercator(tile, file_ref, resampling):
    new_bytes = _to_webmercator_bytes(tile, resampling, file_ref=file_ref)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


def _build_uf_to_webmercator(grid_dirs):
    """FILE-aware to_webmercator UDF factory: captures grid_dirs at build time → pickled."""
    from databricks.labs.gbx.pyrx import _env

    @f.udf(V2_TILE_SCHEMA)
    @_tile_op("resampling", check=_check_resampling)
    def _udf(tile, file_ref, resampling):
        _env.configure_gdal_env(extra_proj_dirs=grid_dirs)
        new_bytes = _to_webmercator_bytes(tile, resampling, file_ref=file_ref)
        return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))

    return _udf


def _build_to_webmercator_v2_udf(grid_dirs):
    """Force-output to_webmercator UDF factory: captures grid_dirs at build time → pickled."""
    from databricks.labs.gbx.pyrx import _env

    @f.udf(V2_TILE_SCHEMA)
    @_tile_op("resampling", check=_check_resampling)
    def _udf(tile, resampling, virtualize_dir, virtualize_prefix, materialize):
        _env.configure_gdal_env(extra_proj_dirs=grid_dirs)
        new_bytes = _to_webmercator_bytes(tile, resampling)
        return _shaped_result_row(
            new_bytes,
            _tile_cellid(tile),
            virtualize_dir,
            virtualize_prefix,
            materialize,
        )

    return _udf


def _build_to_webmercator_sql_udf(grid_dirs):
    """SQL-registered (no file_ref) to_webmercator UDF factory: captures grid_dirs."""
    from databricks.labs.gbx.pyrx import _env

    @f.udf(V2_TILE_SCHEMA)
    @_tile_op("resampling", check=_check_resampling)
    def _udf(tile, resampling):
        _env.configure_gdal_env(extra_proj_dirs=grid_dirs)
        new_bytes = _to_webmercator_bytes(tile, resampling)
        return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))

    return _udf


# --- Tier 1c: tile-returning resample UDFs ----------------------------------
def _resample_bytes(tile, factor, algorithm, file_ref=None):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _open_op(tile, file_ref=file_ref) as ds:
        return resample.resample_by_factor(ds, float(factor), str(algorithm))


@f.udf(V2_TILE_SCHEMA)
@_tile_op("factor", "algorithm", check=resample.check_by_factor)
def _resample_udf(tile, factor, algorithm):
    new_bytes = _resample_bytes(tile, factor, algorithm)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
@_tile_op("factor", "algorithm", check=resample.check_by_factor)
def _resample_v2_udf(
    tile, factor, algorithm, virtualize_dir, virtualize_prefix, materialize
):
    new_bytes = _resample_bytes(tile, factor, algorithm)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


@f.udf(V2_TILE_SCHEMA)
@_tile_op("factor", "algorithm", check=resample.check_by_factor)
def _uf_resample(tile, file_ref, factor, algorithm):
    new_bytes = _resample_bytes(tile, factor, algorithm, file_ref=file_ref)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


def _resample_to_size_bytes(tile, width_px, height_px, algorithm, file_ref=None):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _open_op(tile, file_ref=file_ref) as ds:
        return resample.resample_to_size(
            ds, int(width_px), int(height_px), str(algorithm)
        )


@f.udf(V2_TILE_SCHEMA)
@_tile_op("width_px", "height_px", "algorithm", check=resample.check_to_size)
def _resample_to_size_udf(tile, width_px, height_px, algorithm):
    new_bytes = _resample_to_size_bytes(tile, width_px, height_px, algorithm)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
@_tile_op("width_px", "height_px", "algorithm", check=resample.check_to_size)
def _resample_to_size_v2_udf(
    tile, width_px, height_px, algorithm, virtualize_dir, virtualize_prefix, materialize
):
    new_bytes = _resample_to_size_bytes(tile, width_px, height_px, algorithm)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


@f.udf(V2_TILE_SCHEMA)
@_tile_op("width_px", "height_px", "algorithm", check=resample.check_to_size)
def _uf_resample_to_size(tile, file_ref, width_px, height_px, algorithm):
    new_bytes = _resample_to_size_bytes(
        tile, width_px, height_px, algorithm, file_ref=file_ref
    )
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


def _resample_to_res_bytes(tile, x_res, y_res, algorithm, file_ref=None):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _open_op(tile, file_ref=file_ref) as ds:
        return resample.resample_to_res(ds, float(x_res), float(y_res), str(algorithm))


@f.udf(V2_TILE_SCHEMA)
@_tile_op("x_res", "y_res", "algorithm", check=resample.check_to_res)
def _resample_to_res_udf(tile, x_res, y_res, algorithm):
    new_bytes = _resample_to_res_bytes(tile, x_res, y_res, algorithm)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
@_tile_op("x_res", "y_res", "algorithm", check=resample.check_to_res)
def _resample_to_res_v2_udf(
    tile, x_res, y_res, algorithm, virtualize_dir, virtualize_prefix, materialize
):
    new_bytes = _resample_to_res_bytes(tile, x_res, y_res, algorithm)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


@f.udf(V2_TILE_SCHEMA)
@_tile_op("x_res", "y_res", "algorithm", check=resample.check_to_res)
def _uf_resample_to_res(tile, file_ref, x_res, y_res, algorithm):
    new_bytes = _resample_to_res_bytes(tile, x_res, y_res, algorithm, file_ref=file_ref)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


# --- Tier 1d: tile-returning edit UDFs -------------------------------------
def _read_cutline(geom_wkb, clip_crs):
    """The cutline geometry of a clip (``None`` for an empty one), with its CRS checked.

    A cutline that cannot be read, or a ``clip_crs`` that names no CRS, is data the tile cannot be clipped by:
    the tile's error tile with the reason, as heavy ``RST_Clip`` gives it, not a failed task.
    """
    import shapely

    from databricks.labs.gbx._geom import parse_geom
    from databricks.labs.gbx.pyrx.core.crs import resolve_source_crs

    try:
        geom = parse_geom(geom_wkb)
        if geom is not None:
            resolve_source_crs(shapely.get_srid(geom), crs=clip_crs)
    except Exception as exc:
        raise _UnreadableTile(f"RST_Clip: the cutline cannot be used: {exc}") from exc
    return geom


def _clip_bytes(tile, geom_wkb, all_touched, clip_crs=None):
    """Shared clip body: open the (virtual or materialized) tile, clip to the
    parsed cutline, return the clipped GTiff bytes (or None on no-overlap).

    ``clip_crs`` is a source-CRS override for a plain WKB/WKT cutline (int SRID or
    CRS string incl. ESRI/WKT); an EWKB/EWKT embedded SRID wins per-geom."""
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    # The parsed cutline keeps the SRID (EWKT/EWKB carry it) so clip_to_geom can
    # reproject it to the raster CRS, mirroring heavy RST_Clip.
    geom = _read_cutline(geom_wkb, clip_crs)
    if geom is None:
        return None
    with _open_op(tile) as ds:
        return edit.clip_to_geom(ds, geom, bool(all_touched), geom_crs=clip_crs)


@f.udf(V2_TILE_SCHEMA)
@_tile_op("geom_wkb", "all_touched")
def _clip_udf(tile, geom_wkb, all_touched, clip_crs=None):
    new_bytes = _clip_bytes(tile, geom_wkb, all_touched, clip_crs)
    if new_bytes is None:
        # Cutline does not overlap the raster -> null tile (no crash), mirroring
        # heavy GDAL Warp -cutline producing an empty result.
        return None
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
@_tile_op("geom_wkb", "all_touched")
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
    # bytes are shaped via shape_output. Returns the 9-field v2 tile envelope.
    new_bytes = _clip_bytes(tile, geom_wkb, all_touched, clip_crs)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


@f.udf(V2_TILE_SCHEMA)
@_tile_op("geom_wkb", "all_touched")
def _uf_clip(tile, file_ref, geom_wkb, all_touched, clip_crs):
    """FILE-aware clip: reads the input tile through ``file_ref``, then clips.

    The explicit cutline is applied AFTER the tile bytes are read (it is not a tile-level
    ``clip_polygon``).
    """
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    geom = _read_cutline(geom_wkb, clip_crs)
    if geom is None:
        return None
    with _open_op(tile, file_ref=file_ref) as ds:
        new_bytes = edit.clip_to_geom(ds, geom, bool(all_touched), geom_crs=clip_crs)
    if new_bytes is None:
        return None
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


def _build_clip_v2_udf(grid_dirs):
    """Force-output clip UDF factory: captures grid_dirs at build time → pickled."""
    from databricks.labs.gbx.pyrx import _env

    @f.udf(V2_TILE_SCHEMA)
    @_tile_op("geom_wkb", "all_touched")
    def _udf(
        tile,
        geom_wkb,
        all_touched,
        clip_crs,
        virtualize_dir,
        virtualize_prefix,
        materialize,
    ):
        _env.configure_gdal_env(extra_proj_dirs=grid_dirs)
        new_bytes = _clip_bytes(tile, geom_wkb, all_touched, clip_crs)
        return _shaped_result_row(
            new_bytes,
            _tile_cellid(tile),
            virtualize_dir,
            virtualize_prefix,
            materialize,
        )

    return _udf


def _build_clip_sql_udf(grid_dirs):
    """SQL-registered (no file_ref) clip UDF factory: captures grid_dirs."""
    from databricks.labs.gbx.pyrx import _env

    @f.udf(V2_TILE_SCHEMA)
    @_tile_op("geom_wkb", "all_touched")
    def _udf(tile, geom_wkb, all_touched, clip_crs=None):
        _env.configure_gdal_env(extra_proj_dirs=grid_dirs)
        new_bytes = _clip_bytes(tile, geom_wkb, all_touched, clip_crs)
        if new_bytes is None:
            return None
        return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))

    return _udf


def _update_type_bytes(tile, new_type, file_ref=None):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _open_op(tile, file_ref=file_ref) as ds:
        return edit.update_type(ds, str(new_type))


def _check_update_type(*, new_type, **_):
    edit.gdal_type(new_type)


@f.udf(V2_TILE_SCHEMA)
@_tile_op("new_type", check=_check_update_type)
def _update_type_udf(tile, new_type):
    new_bytes = _update_type_bytes(tile, new_type)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
@_tile_op("new_type", check=_check_update_type)
def _update_type_v2_udf(tile, new_type, virtualize_dir, virtualize_prefix, materialize):
    new_bytes = _update_type_bytes(tile, new_type)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


@f.udf(V2_TILE_SCHEMA)
@_tile_op("new_type", check=_check_update_type)
def _uf_update_type(tile, file_ref, new_type):
    new_bytes = _update_type_bytes(tile, new_type, file_ref=file_ref)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


def _init_nodata_row(tile, file_ref=None):
    """The ``rst_initnodata`` result: a virtual tile records a pending instruction and stays virtual (no pixel
    read, ``file_ref`` unused); a materialized one is edited now."""
    vt = ot._to_virtual_tile(tile)
    if vt.is_virtual():
        md = dict(vt.metadata or {})
        md.setdefault(ot.PENDING_NODATA, ot.PENDING_NODATA_AUTO)
        vt.metadata = md
        return vt.to_row()
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _open_op(tile, file_ref=file_ref) as ds:
        new_bytes = edit.init_nodata(ds)
    # materialized inputs carry no pending_* keys (invariant), so metadata is already clean
    return VirtualTile(
        cellid=_tile_cellid(tile), raster=new_bytes, metadata=dict(vt.metadata or {})
    ).to_row()


def _init_nodata_bytes(tile):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _open_op(tile) as ds:
        return edit.init_nodata(ds)


@f.udf(V2_TILE_SCHEMA)
@_tile_op()
def _init_nodata_udf(tile):
    return _init_nodata_row(tile)


@f.udf(V2_TILE_SCHEMA)
@_tile_op()
def _init_nodata_v2_udf(tile, virtualize_dir, virtualize_prefix, materialize):
    new_bytes = _init_nodata_bytes(tile)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


@f.udf(V2_TILE_SCHEMA)
@_tile_op()
def _uf_initnodata(tile, file_ref):
    """FILE-aware rst_initnodata (a virtual tile: a pending instruction; ``file_ref`` unused)."""
    return _init_nodata_row(tile, file_ref=file_ref)


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


def _setsrid_bytes(tile, srid, file_ref=None):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _open_op(tile, file_ref=file_ref) as ds:
        return edit.set_srid(ds, int(srid))


def _check_srid(*, srid, **_):
    """``rst_setsrid``'s SRID: not negative, and a positive one an EPSG or ESRI code; ``0`` clears the CRS."""
    s = int(srid)
    if s < 0:
        raise ValueError(f"rst_setsrid: SRID must be >= 0; got {s}")
    if s > 0:
        from databricks.labs.gbx.pyrx.core.crs import resolve_crs

        try:
            resolve_crs(s)
        except (
            Exception
        ) as exc:  # noqa: BLE001 - whatever the resolver says, the SRID is the offender
            raise ValueError(
                f"rst_setsrid: SRID {s} is neither an EPSG nor an ESRI code"
            ) from exc


def _setsrid_row(tile, srid, file_ref=None):
    """The ``rst_setsrid`` result: a virtual tile records a pending instruction and stays virtual (``0``, which
    clears the CRS, cannot be recorded: that tile is read now); a materialized one is stamped now.
    """
    vt = ot._to_virtual_tile(tile)
    s = int(srid)
    if vt.is_virtual() and s > 0:
        md = dict(vt.metadata or {})
        md[ot.PENDING_SRID] = str(s)
        # Remove any stale pending_crs so this int SRID is the sole authority
        # (mirrors _setcrs_udf popping pending_srid). Without this, a prior
        # rst_setcrs's pending_crs would supersede and silently ignore this call.
        md.pop(ot.PENDING_CRS, None)
        vt.metadata = md
        return vt.to_row()
    new_bytes = _setsrid_bytes(tile, s, file_ref=file_ref)
    # a materialized tile carries no pending_* keys (invariant); a virtual one just read had them consumed
    return VirtualTile(
        cellid=_tile_cellid(tile),
        raster=new_bytes,
        metadata=ot._without_pending(vt.metadata),
    ).to_row()


@f.udf(V2_TILE_SCHEMA)
@_tile_op("srid", check=_check_srid)
def _setsrid_udf(tile, srid):
    return _setsrid_row(tile, srid)


@f.udf(V2_TILE_SCHEMA)
@_tile_op("srid", check=_check_srid)
def _setsrid_v2_udf(tile, srid, virtualize_dir, virtualize_prefix, materialize):
    new_bytes = _setsrid_bytes(tile, srid)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


@f.udf(V2_TILE_SCHEMA)
@_tile_op("srid", check=_check_srid)
def _uf_setsrid(tile, file_ref, srid):
    """FILE-aware rst_setsrid (pending-instruction path for virtual tiles)."""
    return _setsrid_row(tile, srid, file_ref=file_ref)


# --- rst_setcrs: string-taking CRS relabel (pending-instruction on virtual) --
def _setcrs_bytes(tile, crs_value, file_ref=None):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _open_op(tile, file_ref=file_ref) as ds:
        return edit.set_crs(ds, str(crs_value))


def _check_crs(*, crs_value, **_):
    """``rst_setcrs``'s CRS: one the resolver reads (an authority code, WKT, PROJ4, an integer string)."""
    from databricks.labs.gbx.pyrx.core.crs import resolve_crs

    try:
        resolve_crs(str(crs_value))
    except (
        Exception
    ) as exc:  # noqa: BLE001 - whatever the resolver says, the CRS is the offender
        raise ValueError(
            f"rst_setcrs: {str(crs_value)[:120]!r} is not a CRS: {exc}"
        ) from exc


def _setcrs_row(tile, crs_value, file_ref=None):
    """The ``rst_setcrs`` result: a virtual tile records a pending instruction and stays virtual; a
    materialized one is stamped now."""
    from databricks.labs.gbx.pyrx.core.crs import crs_to_canonical, resolve_crs

    vt = ot._to_virtual_tile(tile)
    if vt.is_virtual():
        md = dict(vt.metadata or {})
        md[ot.PENDING_CRS] = crs_to_canonical(resolve_crs(str(crs_value)))
        # Remove any stale pending_srid so the canonical CRS string is sole authority.
        md.pop(ot.PENDING_SRID, None)
        vt.metadata = md
        return vt.to_row()
    new_bytes = _setcrs_bytes(tile, crs_value, file_ref=file_ref)
    return VirtualTile(
        cellid=_tile_cellid(tile), raster=new_bytes, metadata=dict(vt.metadata or {})
    ).to_row()


@f.udf(V2_TILE_SCHEMA)
@_tile_op("crs_value", check=_check_crs)
def _setcrs_udf(tile, crs_value):
    return _setcrs_row(tile, crs_value)


@f.udf(V2_TILE_SCHEMA)
@_tile_op("crs_value", check=_check_crs)
def _uf_setcrs(tile, file_ref, crs_value):
    """FILE-aware rst_setcrs (pending-instruction path for virtual tiles)."""
    return _setcrs_row(tile, crs_value, file_ref=file_ref)


# --- rst_transformcrs: string-taking reproject (non-EPSG targets) -------------
def _transformcrs_bytes(tile, crs_value, file_ref=None):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with ot._open(tile, file_ref=file_ref) as ds:
        return warp.reproject_to_crs(ds, str(crs_value))


def _transformcrs_tile(tile, crs_value, file_ref=None, grid_dirs=None):
    """One reprojected tile row, or an error tile when the reprojection fails.

    An unreadable tile or an unparseable CRS is data, not a task failure: every route
    (SQL and Column API) returns the error tile the JVM tier returns
    (RST_ErrorHandler.safeEval), with the reason under metadata['last_error']. Like the
    JVM's NonFatal, resource exhaustion is not data: a MemoryError or RecursionError fails
    the task so it can be retried, instead of becoming an error tile.
    """
    try:
        if grid_dirs is not None:
            from databricks.labs.gbx.pyrx import _env

            _env.configure_gdal_env(extra_proj_dirs=grid_dirs)
        new_bytes = _transformcrs_bytes(tile, crs_value, file_ref=file_ref)
        if new_bytes is None:
            return None
        return VirtualTile(
            cellid=_tile_cellid(tile), raster=new_bytes, metadata={}
        ).to_row()
    except (MemoryError, RecursionError):
        raise
    except Exception as e:  # noqa: BLE001
        return _serde.build_error_tile(f"RST_TransformCrs: {e}", _tile_cellid(tile))


def _error_tile_of(tile):
    """*tile* as a dict when it is an error tile, else None. The one test every pass-through shares.

    An error tile is a swallowed failure: no raster, no path, and its reason under ``last_error`` or
    ``error_message``. A tile that still has pixels is never one (the merge aggregates stamp a warning
    under ``last_error`` on good tiles).
    """
    if tile is None or isinstance(tile, (bytes, bytearray, VirtualTile)):
        return None
    d = tile.asDict() if hasattr(tile, "asDict") else dict(tile)
    if d.get("raster") is not None or d.get("path") is not None:
        return None
    md = d.get("metadata") or {}
    return d if ("last_error" in md or "error_message" in md) else None


def _empty_tile_out(tile):
    """What a function returns for a tile with neither raster nor path.

    An error tile goes out unchanged, so the upstream reason survives every step, as it does on the
    JVM tier; any other empty tile is NULL.
    """
    return _error_tile_of(tile)


def _empty_tile_rows(tile, who):
    """The rows a tile generator emits for a tile with neither raster nor path (call it when
    :func:`_tile_is_empty`); the generator form of :func:`_empty_tile_out`.

    NULL has nothing to split: no rows, as ``explode(NULL)`` gives. An error tile comes out as itself,
    so the upstream reason survives; any other empty tile is one error tile naming *who*.
    """
    if tile is None:
        return []
    err = _error_tile_of(tile)
    return (
        [err]
        if err is not None
        else [_serde.build_error_tile(f"{who}: empty or unreadable tile")]
    )


@f.udf(V2_TILE_SCHEMA)
def _transformcrs_udf(tile, crs_value):
    if crs_value is None:
        return None
    if _tile_is_empty(tile):
        return _empty_tile_out(tile)
    return _transformcrs_tile(tile, crs_value)


@f.udf(V2_TILE_SCHEMA)
def _uf_transformcrs(tile, file_ref, crs_value):
    if crs_value is None:
        return None
    if _tile_is_empty(tile):
        return _empty_tile_out(tile)
    return _transformcrs_tile(tile, crs_value, file_ref=file_ref)


def _build_uf_transformcrs(grid_dirs):
    """FILE-aware transformcrs UDF factory: captures grid_dirs at build time → pickled."""

    @f.udf(V2_TILE_SCHEMA)
    def _udf(tile, file_ref, crs_value):
        if crs_value is None:
            return None
        if _tile_is_empty(tile):
            return _empty_tile_out(tile)
        return _transformcrs_tile(
            tile, crs_value, file_ref=file_ref, grid_dirs=grid_dirs
        )

    return _udf


def _build_transformcrs_sql_udf(grid_dirs):
    """SQL-registered (no file_ref) transformcrs UDF factory: captures grid_dirs."""

    @f.udf(V2_TILE_SCHEMA)
    def _udf(tile, crs_value):
        if crs_value is None:
            return None
        if _tile_is_empty(tile):
            return _empty_tile_out(tile)
        return _transformcrs_tile(tile, crs_value, grid_dirs=grid_dirs)

    return _udf


def _band_bytes(tile, band_index):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _open_op(tile) as ds:
        return edit.band(ds, int(band_index))


def _check_band(*, band_index, **_):
    b = int(band_index)
    if b < 1:
        raise ValueError(f"rst_band: band_index {b} out of range (>=1)")


@f.udf(V2_TILE_SCHEMA)
@_tile_op("band_index", check=_check_band)
def _band_udf(tile, band_index):
    vt = ot._to_virtual_tile(tile)
    b = int(band_index)
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
@_tile_op("band_index", check=_check_band)
def _band_v2_udf(tile, band_index, virtualize_dir, virtualize_prefix, materialize):
    new_bytes = _band_bytes(tile, band_index)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


def _asformat_bytes(tile, new_format):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _open_op(tile) as ds:
        return ops_core.as_format(ds, str(new_format))


def _check_asformat(*, new_format, **_):
    ops_core.driver_name(new_format)


@f.udf(V2_TILE_SCHEMA)
@_tile_op("new_format", check=_check_asformat)
def _asformat_udf(tile, new_format):
    new_bytes = _asformat_bytes(tile, new_format)
    # metadata.driver is the driver the bytes open as (COG opens as GTiff).
    return _serde.build_tile(
        new_bytes, ops_core.written_driver(new_format), _tile_cellid(tile)
    )


@f.udf(V2_TILE_SCHEMA)
@_tile_op("new_format", check=_check_asformat)
def _asformat_v2_udf(tile, new_format, virtualize_dir, virtualize_prefix, materialize):
    new_bytes = _asformat_bytes(tile, new_format)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


def _buildoverviews_bytes(tile, levels, resampling):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    resamp = "average" if resampling is None else str(resampling)
    with _open_op(tile) as ds:
        return ops_core.build_overviews(ds, list(levels), resamp)


def _check_overviews(*, levels, resampling=None, **_):
    ops_core.check_overview_args(levels, resampling)


@f.udf(V2_TILE_SCHEMA)
@_tile_op("levels", check=_check_overviews)
def _buildoverviews_udf(tile, levels, resampling="average"):
    new_bytes = _buildoverviews_bytes(tile, levels, resampling)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
@_tile_op("levels", check=_check_overviews)
def _buildoverviews_v2_udf(
    tile, levels, resampling, virtualize_dir, virtualize_prefix, materialize
):
    new_bytes = _buildoverviews_bytes(tile, levels, resampling)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


@f.udf(ArrayType(DoubleType()))
def _sample_udf(tile, geom_wkb, crs=None):
    # PIXEL accessor: needs pixels, so use the virtual-aware _tile_is_empty guard
    # (a virtual tile has raster None but a path -> NOT empty). A bytes-only guard
    # would drop a virtual input before ever reading it. ot.open_tile_points reads
    # only the pixel at the point from a virtual tile's source (opened once per
    # task), not its whole window: decoding and re-encoding the window per call
    # cost about a second a point on an hourly global GeoTIFF on Databricks.
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
    with ot.open_tile_points(tile) as ds:
        return ops_core.sample(ds, geom, geom_crs=crs)


def _proximity_bytes(tile, target_values, distunits, max_distance):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    units = "GEO" if distunits is None else str(distunits)
    tv = None if target_values is None else str(target_values)
    md = None if max_distance is None else float(max_distance)
    with _open_op(tile) as ds:
        return analysis_core.proximity(ds, tv, units, md)


@f.udf(V2_TILE_SCHEMA)
@_tile_op(check=analysis_core.check_proximity)
def _proximity_udf(tile, target_values=None, distunits="GEO", max_distance=None):
    new_bytes = _proximity_bytes(tile, target_values, distunits, max_distance)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
@_tile_op(check=analysis_core.check_proximity)
def _proximity_v2_udf(
    tile,
    target_values,
    distunits,
    max_distance,
    virtualize_dir,
    virtualize_prefix,
    materialize,
):
    new_bytes = _proximity_bytes(tile, target_values, distunits, max_distance)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


# rst_contour: tile + levels (ARRAY<DOUBLE>) + interval/base/attr_field ->
# ARRAY<struct(geom_wkb BINARY, value DOUBLE)> (mirrors _polygonize_udf shape).
_CONTOUR_SCHEMA = ArrayType(
    StructType(
        [
            StructField("geom_wkb", BinaryType(), nullable=True),
            StructField("value", DoubleType(), nullable=True),
        ]
    )
)


def _path_is_unreachable(tile) -> bool:
    """True for a path-backed tile whose local file is not there (a UC Volume path that has not caught up). Like
    the non-GDAL ``OSError`` that :func:`_open_op` lets through, it is not a tile whose bytes GDAL cannot read.
    A path this cannot test (a remote or ``/vsi`` path) is not called unreachable.
    """
    try:
        vt = ot._to_virtual_tile(tile)
    except Exception:  # noqa: BLE001 - a tile of no known shape has no path to test
        return False
    path = vt.path
    if vt.raster is not None or not path:
        return False
    if path.startswith("file:"):
        path = "/" + path[len("file:") :].lstrip("/")
    if "://" in path or path.startswith("/vsi") or path.startswith("dbfs:"):
        return False
    return not os.path.exists(path)


@contextlib.contextmanager
def _open_or_none(tile):
    """The tile's dataset, or None when its bytes are not a raster GDAL can read (a function that returns an array
    gives NULL for it). A refusal by the read cap or the driver policy is no such case and raises.
    """
    from rasterio.errors import RasterioIOError

    cm = ot._open(tile)
    try:
        ds = cm.__enter__()
    except RasterioIOError:
        if _path_is_unreachable(tile):  # not corrupt bytes: Spark retries the task
            raise
        yield None
        return
    try:
        yield ds
    finally:
        cm.__exit__(None, None, None)


@f.udf(_CONTOUR_SCHEMA)
def _contour_udf(
    tile, levels, interval=0.0, base=0.0, attr_field="elev"
):  # the row's defaults: no SQL wrapper
    if _tile_is_empty(tile):
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    lvls = [] if levels is None else [float(v) for v in levels if v is not None]
    iv = 0.0 if interval is None else float(interval)
    bs = 0.0 if base is None else float(base)
    attr = "elev" if attr_field is None else str(attr_field)
    with _open_or_none(tile) as ds:
        return None if ds is None else analysis_core.contour(ds, lvls, iv, bs, attr)


# rst_isoband: tile + breaks (ARRAY<DOUBLE>) ->
# ARRAY<struct(geom_wkb BINARY, band INT, lower DOUBLE, upper DOUBLE)>
_ISOBAND_SCHEMA = ArrayType(
    StructType(
        [
            StructField("geom_wkb", BinaryType(), nullable=True),
            StructField("band", IntegerType(), nullable=True),
            StructField("lower", DoubleType(), nullable=True),
            StructField("upper", DoubleType(), nullable=True),
        ]
    )
)


@f.udf(_ISOBAND_SCHEMA)
def _isoband_udf(tile, breaks):
    if breaks is not None:  # a PARAMETER error, as in the JVM: checked before the tile
        for i, b in enumerate(breaks):
            if b is None:
                raise ValueError(
                    f"rst_isoband: breaks must not contain NULL; element {i} is NULL. "
                    "Fix: remove the NULL from the breaks array."
                )
    if _tile_is_empty(tile):
        return None
    if not breaks:
        return None
    with _open_or_none(tile) as ds:
        return None if ds is None else features.isoband(ds, breaks)


def _viewshed_bytes(
    tile, observer_geom, observer_height, target_height, max_distance, crs=None
):
    from databricks.labs.gbx._geom import parse_geom
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    geom = parse_geom(
        observer_geom
    )  # a POINT as WKB, EWKB, WKT or EWKT (checked by _check_viewshed)
    oh = float(observer_height)
    th = 1.6 if target_height is None else float(target_height)
    md = None if max_distance is None else float(max_distance)
    with _open_op(tile) as ds:
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


def _check_viewshed(
    *, observer_geom, observer_height, target_height=None, max_distance=None, **_
):
    from databricks.labs.gbx._geom import parse_geom

    geom = parse_geom(observer_geom)
    if geom is None:
        raise ValueError(
            f"rst_viewshed: observer_geom {observer_geom!r} is not a geometry (WKB, EWKB, WKT or EWKT)"
        )
    if geom.geom_type != "Point":
        raise ValueError(
            f"rst_viewshed requires a POINT observer_geom; got {geom.geom_type}"
        )
    for name, v in (
        ("observer_height", observer_height),
        ("target_height", target_height),
    ):
        if v is not None and not (math.isfinite(float(v)) and float(v) >= 0):
            raise ValueError(f"rst_viewshed: {name} must be >= 0 and finite; got {v}")
    if max_distance is not None and not (
        math.isfinite(float(max_distance)) and float(max_distance) > 0
    ):
        raise ValueError(
            f"rst_viewshed: max_distance must be > 0 and finite; got {max_distance}"
        )


@f.udf(V2_TILE_SCHEMA)
@_tile_op("observer_geom", "observer_height", check=_check_viewshed)
def _viewshed_udf(
    tile, observer_geom, observer_height, target_height=1.6, max_distance=None, crs=None
):
    new_bytes = _viewshed_bytes(
        tile, observer_geom, observer_height, target_height, max_distance, crs
    )
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
@_tile_op("observer_geom", "observer_height", check=_check_viewshed)
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
    new_bytes = _viewshed_bytes(
        tile, observer_geom, observer_height, target_height, max_distance, crs
    )
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


def _cog_convert_bytes(tile, compression, blocksize, overview_resampling):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    comp = "AUTO" if compression is None else str(compression)
    bs = 512 if blocksize is None else int(blocksize)
    resamp = "AVERAGE" if overview_resampling is None else str(overview_resampling)
    with _open_op(tile) as ds:
        return analysis_core.cog_convert(ds, comp, bs, resamp)


def _check_cog_convert(
    *, compression=None, blocksize=None, overview_resampling=None, **_
):
    analysis_core.check_cog_args(
        "AUTO" if compression is None else compression,
        512 if blocksize is None else blocksize,
        "AVERAGE" if overview_resampling is None else overview_resampling,
    )


@f.udf(V2_TILE_SCHEMA)
@_tile_op(check=_check_cog_convert)
def _cog_convert_udf(
    tile, compression="AUTO", blocksize=512, overview_resampling="AVERAGE"
):
    new_bytes = _cog_convert_bytes(tile, compression, blocksize, overview_resampling)
    # COG is a GTiff variant on disk; downstream readers see driver "GTiff".
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
@_tile_op(check=_check_cog_convert)
def _cog_convert_v2_udf(
    tile,
    compression,
    blocksize,
    overview_resampling,
    virtualize_dir,
    virtualize_prefix,
    materialize,
):
    new_bytes = _cog_convert_bytes(tile, compression, blocksize, overview_resampling)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


def _light_rst_transformcrs(
    tile: ColLike,
    target_crs: ColLike,
) -> Column:
    """The light Column composition of ``gbx_rst_transformcrs`` (the bench's light form and the
    light tests). The Python API is ``databricks.labs.gbx.rasterx.functions.rst_transformcrs``.

    Reproject the raster to a string-given target CRS.

    Accepts int EPSG, an int-castable string, an authority string (``"EPSG:3857"``,
    ``"ESRI:54008"``), WKT, PROJJSON or PROJ4 (never a file name or URL, and no grid
    named by path).  Unlike ``rst_transform`` (int EPSG only)
    this supports non-EPSG targets such as ESRI codes or custom projections.

    Pixel-producing: always materializes (a new reprojected tile is emitted).

    Args:
        tile: Tile struct column.
        target_crs: Target CRS descriptor — int EPSG, int-castable string,
              authority string, WKT, or PROJ4.

    Returns:
        Tile reprojected to the target CRS.
    """
    from databricks.labs.gbx.core import proj_grids
    from databricks.labs.gbx.pyrx._file_ref import file_ref_arg

    _grid_dirs = tuple(proj_grids.get_registered_dirs())
    tc = _col(tile)
    return _build_uf_transformcrs(_grid_dirs)(
        tc, file_ref_arg(tc), _crs_col(target_crs)
    )


def _tower_window_paths(tile_map, x0, y0, tile_m, radius):
    """A function (x, y) -> the sorted DSM tile paths in reach of a tower, or None for a tower that stands on
    no tile (it is outside the DSM, or in a grid cell the manifest has no tile for) or has no usable coordinate.
    """

    def window_paths(obs_x, obs_y):
        import math

        from databricks.labs.gbx.pyrx.core.tiling import tile_range

        if obs_x is None or obs_y is None:
            return None
        obs_x, obs_y = float(obs_x), float(obs_y)
        if not (math.isfinite(obs_x) and math.isfinite(obs_y)):
            return None
        own = (math.floor((obs_x - x0) / tile_m), math.floor((obs_y - y0) / tile_m))
        if own not in tile_map:
            return None
        tx_lo, tx_hi = tile_range(obs_x, x0, radius, tile_m)
        ty_lo, ty_hi = tile_range(obs_y, y0, radius, tile_m)
        return sorted(
            {
                tile_map[(tx, ty)]
                for tx in range(tx_lo, tx_hi + 1)
                for ty in range(ty_lo, ty_hi + 1)
                if (tx, ty) in tile_map
            }
        )

    return window_paths


def _raise_if_null(check: Column, message: Column, keep: Column) -> Column:
    """`keep`, or a failed query with `message` where `check` is NULL."""
    return f.when(check.isNull(), f.raise_error(message)).otherwise(keep)


def rst_viewshed_towers(
    towers_df: DataFrame,
    dsm_tiles_df: DataFrame,
    radius_m: float,
    observer_height,
    *,
    x0: float,
    y0: float,
    tile_m: float,
    target_height: float = 1.6,
    to_crs: Optional[int] = None,
    vrt_dir: Optional[str] = None,
    tower_id_col: str = "tower_cellid",
    x_col: str = "x",
    y_col: str = "y",
    num_partitions: Optional[int] = None,
    _levels: Optional[str] = None,
) -> DataFrame:
    """Per-tower raster viewshed over a tiled DSM — one scalar row per tower.

    Packages the failure-avoiding pipeline validated on Databricks Serverless
    light: for each tower it selects the in-radius DSM tiles, mints ONE
    bytes-free VRT over them, runs a single pixel-level :func:`rst_viewshed`
    (materialized once, in-memory), and derives coverage counts from a
    histogram. No raster bytes ever cross a shuffle — each tower row carries a
    ~100-byte VRT path — so there is no FUSE read-back and no whole-AOI merge.

    The tile window is computed per tower with :func:`tile_range` (``x0``/``y0``
    grid origin, ``tile_m`` tile size — the grid the ``tx``/``ty`` indices in
    ``dsm_tiles_df`` reference), then the viewshed, histogram, georeference and
    optional reproject are chained as Spark ``withColumn`` steps that stay in a
    single post-``repartition`` stage so the mint and the DEM read run in the
    same task (the minted VRT is read before any further shuffle).

    Args:
        towers_df:    DataFrame of tower sites. Must carry ``tower_id_col`` plus
                      the projected observer coordinate columns ``x_col``/``y_col``
                      (EPSG:3857 metres, matching the DSM grid). When
                      ``observer_height`` is a column name, that column too.
        dsm_tiles_df: DSM tile manifest; must have columns ``path`` (absolute COG
                      path), ``tx`` and ``ty`` (integer grid indices). This is a
                      bytes-free manifest (paths, not raster bytes) and is
                      collected once on the driver.
        radius_m:     Viewshed analysis radius in projected metres (> 0). Also
                      caps the DSM window and the ``rst_viewshed`` max-distance.
        observer_height: Observer/mast height above the DSM surface — a scalar
                      (applied to every tower) or a column name (per-tower).
        x0, y0:       EPSG:3857 tile-grid origin (the ``tx``/``ty`` reference).
        tile_m:       Tile size in metres (> 0).
        target_height: Receiver height above the DSM at each tested cell.
                      Defaults to 1.6.
        to_crs:       Optional EPSG code to additionally reproject the viewshed
                      into (``rst_transform``, materialized once); when set, the
                      result gains ``visible_px_reproj`` / ``total_px_reproj``.
        vrt_dir:      Optional durable, worker-readable directory (e.g. a UC
                      Volume) for the per-tower VRTs (``tower_<id>.vrt``). When
                      omitted the VRTs are minted transiently on the worker and
                      read within the same task.
        tower_id_col: Tower identity column name. Defaults to "tower_cellid".
        x_col, y_col: Observer coordinate column names. Default "x" / "y".
        num_partitions: Fan-out for the heavy per-tower stage (``repartition`` by
                      ``tower_id_col``). Defaults to the distinct tower count
                      (~one tower per task).
        _levels:      Testing only. The level to run: ``"L3"`` (the default: all
                      light), ``"L1-steps"`` (the viewshed and the histogram are
                      the heavyweight's Catalyst steps) or ``"JVM-DF"`` (the whole
                      per-tower pipeline is one Scala DataFrame step through py4j;
                      no ``to_crs``, and ``vrt_dir`` is not used). The two heavy
                      levels need a dedicated cluster with the geobrix JAR; where
                      the session cannot run one, a line says why and L3 runs.

    The viewshed of a tower is the window around it, as :func:`rst_viewshed` returns
    it with a max distance: ``total_px`` is the window's pixel count (``radius_m``
    each way from the tower's pixel, clipped to the tiles) and ``visible_px`` the
    visible part of it. A tower stands on the DSM tile its coordinates fall in
    (tile ``i`` covers ``[origin + i * tile_m, origin + (i + 1) * tile_m)``): one
    outside the DSM, in a grid cell that has no tile, has no row. A NULL
    ``observer_height`` is 0.0. An unreadable DSM tile fails the query.

    Returns:
        DataFrame with one row per tower that has in-radius DSM coverage:
        ``tower_id_col``, ``observer_x``, ``observer_y``, ``visible_px`` (long),
        ``total_px`` (long), ``frac`` (double, visible/total), and — when
        ``to_crs`` is set — ``visible_px_reproj`` and ``total_px_reproj``.
    """
    analysis_core._validate_towers_inputs(dsm_tiles_df.columns, radius_m)
    if tile_m is None or tile_m <= 0:
        raise ValueError(f"tile_m must be > 0 (got {tile_m!r}).")
    from databricks.labs.gbx.manager import levels as manager_levels

    if (
        to_crs is not None
        and (_levels or manager_levels.PREFERENCE["rst_viewshed_towers"][0]) == "JVM-DF"
    ):
        raise ValueError("_levels='JVM-DF' does not support to_crs")
    level = manager_levels.towers_level(towers_df.sparkSession, _levels)

    # Bytes-free tile manifest -> {(tx, ty): path}. Only tile index + path
    # strings reach the driver (never raster bytes), so this is Connect/Serverless
    # safe and bounded by the DSM tile count, not the pixel volume.
    tile_rows = dsm_tiles_df.select("tx", "ty", "path").collect()
    tile_map = {(int(r["tx"]), int(r["ty"])): r["path"] for r in tile_rows if r["path"]}
    if not tile_map:
        raise ValueError(
            "dsm_tiles_df yielded no (tx, ty) -> path entries; every path is NULL "
            "(a materialized DSM STRUCT table?) — point dsm_tiles_df at the DSM "
            "COG directory with dsm_<tx>_<ty>.tif tiles."
        )

    _x0, _y0, _tile_m = float(x0), float(y0), float(tile_m)
    _radius = float(radius_m)
    _vrt_dir = vrt_dir

    window_paths = _tower_window_paths(tile_map, _x0, _y0, _tile_m, _radius)

    @f.udf("string")
    def _mint_window_vrt(tower_id, obs_x, obs_y):
        # Worker-side: pick the in-radius tiles via tile_range and mint ONE VRT
        # over their paths (bytes-free: the VRT is tiny XML referencing the COGs
        # by absolute path). Runs in the same task as the downstream DEM read.
        import os

        from databricks.labs.gbx.ds._mosaic import mint_vrt

        paths = window_paths(obs_x, obs_y)
        if not paths:
            return None
        if _vrt_dir:
            os.makedirs(_vrt_dir, exist_ok=True)
            return mint_vrt(paths, out=os.path.join(_vrt_dir, f"tower_{tower_id}.vrt"))
        return mint_vrt(paths)

    @f.udf("string")
    def _window_tiles(obs_x, obs_y):
        return ", ".join(window_paths(obs_x, obs_y) or [])

    base = towers_df.select(
        f.col(tower_id_col).alias("tower_id"),
        f.col(x_col).cast(DoubleType()).alias("observer_x"),
        f.col(y_col).cast(DoubleType()).alias("observer_y"),
        # A NULL mast height is 0.0 at every level (the light viewshed reads it so).
        f.coalesce(
            analysis_core._observer_height_col(observer_height).cast("double"),
            f.lit(0.0),
        ).alias("observer_height"),
    )

    def with_frac(df: DataFrame) -> DataFrame:
        return df.withColumn(
            "frac",
            f.when(
                f.col("total_px") > 0, f.col("visible_px") / f.col("total_px")
            ).otherwise(f.lit(0.0)),
        )

    # One tower per task: an explicit repartition by tower id is the only fan-out
    # Serverless AQE cannot coalesce. Repartition BEFORE the mint so the mint and
    # the DEM read share a task (no shuffle between them -> the local/Volume VRT
    # is readable where it was written).
    if num_partitions is None:
        num_partitions = max(base.select("tower_id").distinct().count(), 1)
    if level == "JVM-DF":
        from databricks.labs.gbx.manager import jvm

        # The Scala step repartitions by tower id itself. It takes the id as a string; as on L3, a tower with
        # no coordinate has no row.
        in_df = base.where(
            f.col("observer_x").isNotNull() & f.col("observer_y").isNotNull()
        ).select(
            f.col("tower_id").cast("string"),
            "observer_x",
            "observer_y",
            "observer_height",
        )
        done = jvm.viewshed_towers_jvm(
            towers_df.sparkSession,
            in_df,
            tile_map,
            _x0,
            _y0,
            _tile_m,
            _radius,
            float(target_height),
            int(num_partitions),
        )
        return with_frac(
            done.select(
                f.col("tower_id")
                .cast(base.schema["tower_id"].dataType)
                .alias(tower_id_col),
                "observer_x",
                "observer_y",
                "visible_px",
                "total_px",
            )
        )
    base = base.repartition(int(num_partitions), "tower_id")

    # Fixed-point WKT POINT (never scientific notation) in the raster CRS.
    observer_wkt = f.format_string(
        "POINT (%.6f %.6f)", f.col("observer_x"), f.col("observer_y")
    )

    # L3 is all light. At L1-steps the viewshed and the histogram are the heavyweight's Catalyst steps, which
    # take raster bytes: the DEM is materialized (through the same cap) instead of left a path.
    heavy = level == "L1-steps"

    def viewshed():
        args = (
            observer_wkt,
            f.col("observer_height"),
            f.lit(float(target_height)),
            f.lit(_radius),
        )
        if heavy:
            return f.call_function("gbx__l1_rst_viewshed", f.col("_dem"), *args)
        from databricks.labs.gbx.rasterx import functions as rx_api

        return rx_api.rst_viewshed(f.col("_dem"), *args, materialize=True)

    def histogram(tile_col: str):
        args = (f.lit(2), f.lit(0.0), f.lit(256.0), f.lit(False))
        if heavy:
            return f.call_function("gbx__l1_rst_histogram", f.col(tile_col), *args)
        return _light_rst_histogram(tile_col, *args)

    def cells(tile_col: str, vis, invis):
        """The cells of the viewshed: the two histogram classes, or at L1-steps the size of the raster. The
        heavyweight viewshed marks 0 as NoData and its histogram leaves NoData out, so it counts what is visible.
        """
        if heavy:
            w = f.call_function("gbx__l1_rst_width", f.col(tile_col))
            return w.cast("long") * f.call_function(
                "gbx__l1_rst_height", f.col(tile_col)
            )
        return invis + vis

    piped = (
        base.withColumn(
            "_vrt", _mint_window_vrt("tower_id", "observer_x", "observer_y")
        )
        .where(f.col("_vrt").isNotNull())
        .withColumn(
            "_dem", _fromfile_extra(f.col("_vrt"), f.lit("GTiff"), f.lit(bool(heavy)))
        )
    )
    if heavy:
        # The light read gives NULL for a tile it cannot read, and a heavyweight step gives an error tile for a
        # failure: either would become a (0, 0) row, where L3 fails the query. Fail it here, naming the cause.
        tower = f.col("tower_id").cast("string")
        piped = piped.withColumn(
            "_dem",
            _raise_if_null(
                f.col("_dem"),
                f.concat(
                    f.lit("rst_viewshed_towers: a DSM tile for tower "),
                    tower,
                    f.lit(" could not be read, one of: "),
                    _window_tiles("observer_x", "observer_y"),
                ),
                f.col("_dem"),
            ),
        ).withColumn("_vs", viewshed())
    else:
        piped = piped.withColumn("_vs", viewshed())
    # An unreadable DSM tile fails the query at every level: the viewshed of a tile op that cannot read its DEM is the
    # row's error tile (no raster), which the histogram would count as a (0, 0) row.
    tower = f.col("tower_id").cast("string")
    piped = piped.withColumn(
        "_vs",
        _raise_if_null(
            f.col("_vs")["raster"],
            f.concat(
                f.lit("rst_viewshed_towers: the viewshed of tower "),
                tower,
                f.lit(" failed: "),
                f.coalesce(
                    f.col("_vs")["metadata"]["last_error"], f.lit("no reason given")
                ),
            ),
            f.col("_vs"),
        ),
    )
    piped = piped.withColumn("_hist", histogram("_vs"))

    # Histogram bucket 1 = [128, 256) = the visible class (255); bucket 0 = invisible (0).
    vis = f.col("_hist")["band_1"].getItem(1)
    invis = f.col("_hist")["band_1"].getItem(0)
    out_cols = [
        f.col("tower_id").alias(tower_id_col),
        f.col("observer_x"),
        f.col("observer_y"),
        f.coalesce(vis, f.lit(0)).cast("long").alias("visible_px"),
        f.coalesce(cells("_vs", vis, invis), f.lit(0)).cast("long").alias("total_px"),
    ]

    if to_crs is not None:
        from databricks.labs.gbx.rasterx.functions import rst_transform

        piped = piped.withColumn(
            "_vs_reproj", rst_transform("_vs", f.lit(int(to_crs)), materialize=True)
        ).withColumn("_hist_reproj", histogram("_vs_reproj"))
        vis_r = f.col("_hist_reproj")["band_1"].getItem(1)
        invis_r = f.col("_hist_reproj")["band_1"].getItem(0)
        out_cols += [
            f.coalesce(vis_r, f.lit(0)).cast("long").alias("visible_px_reproj"),
            f.coalesce(cells("_vs_reproj", vis_r, invis_r), f.lit(0))
            .cast("long")
            .alias("total_px_reproj"),
        ]

    return with_frac(piped.select(*out_cols))


# --- Tier 1d3: band-math / focal UDFs --------------------------------------
def _threshold_bytes(tile, op, value, file_ref=None):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _open_op(tile, file_ref=file_ref) as ds:
        return edit.threshold(ds, op, value)


@f.udf(V2_TILE_SCHEMA)
@_tile_op("op", "value")
def _threshold_udf(tile, op, value):
    new_bytes = _threshold_bytes(tile, op, value)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
@_tile_op("op", "value")
def _threshold_v2_udf(tile, op, value, virtualize_dir, virtualize_prefix, materialize):
    new_bytes = _threshold_bytes(tile, op, value)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


@f.udf(V2_TILE_SCHEMA)
@_tile_op("op", "value")
def _uf_threshold(tile, file_ref, op, value):
    new_bytes = _threshold_bytes(tile, op, value, file_ref=file_ref)
    if new_bytes is None:
        return None
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


def _filter_bytes(tile, kernel_size, operation):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _open_op(tile) as ds:
        return focal.filt(ds, kernel_size, operation)


def _check_filter(*, kernel_size, operation, **_):
    focal.check_filter(kernel_size, operation)


@f.udf(V2_TILE_SCHEMA)
@_tile_op("kernel_size", "operation", check=_check_filter)
def _filter_udf(tile, kernel_size, operation):
    new_bytes = _filter_bytes(tile, kernel_size, operation)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
@_tile_op("kernel_size", "operation", check=_check_filter)
def _filter_v2_udf(
    tile, kernel_size, operation, virtualize_dir, virtualize_prefix, materialize
):
    new_bytes = _filter_bytes(tile, kernel_size, operation)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


def _convolve_bytes(tile, kernel):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _open_op(tile) as ds:
        return focal.convolve(ds, kernel)


def _check_convolve(*, kernel, **_):
    focal.check_kernel(kernel)


@f.udf(V2_TILE_SCHEMA)
@_tile_op("kernel", check=_check_convolve)
def _convolve_udf(tile, kernel):
    new_bytes = _convolve_bytes(tile, kernel)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
@_tile_op("kernel", check=_check_convolve)
def _convolve_v2_udf(tile, kernel, virtualize_dir, virtualize_prefix, materialize):
    new_bytes = _convolve_bytes(tile, kernel)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


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


def _light_rst_mapalgebra(
    tiles: ColLike,
    expression: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """The light Column composition of ``gbx_rst_mapalgebra`` (the bench's light form and the
    light tests). The Python API is ``databricks.labs.gbx.rasterx.functions.rst_mapalgebra``.

    Apply a map-algebra expression across an array of tiles.

    By default band 1 of each tile (in array order) binds to A, B, C, …; the
    expression is evaluated with numexpr (safe math only — no arbitrary code
    execution). Output is a single-band Float32 tile on the first input's
    georeference.

    Args:
        tiles:     Column of ARRAY<tile struct> (e.g. ``f.array("ta", "tb")``).
        expression: the map-algebra expression. Accepts either the gdal_calc JSON
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
    expr_col = f.lit(expression) if isinstance(expression, str) else _col(expression)
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
    with _open_op(tile) as ds:
        return indices.index(ds, str(formula_name), dict(band_map or {}))


@f.udf(V2_TILE_SCHEMA)
@_tile_op("formula_name", "band_map")
def _index_udf(tile, formula_name, band_map):
    new_bytes = _index_bytes(tile, formula_name, band_map)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
@_tile_op("formula_name", "band_map")
def _index_v2_udf(
    tile, formula_name, band_map, virtualize_dir, virtualize_prefix, materialize
):
    new_bytes = _index_bytes(tile, formula_name, band_map)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


# --- Tier 1d2: spectral index UDFs -----------------------------------------
def _ndvi_bytes(tile, red_band, nir_band):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _open_op(tile) as ds:
        return indices.ndvi(ds, int(red_band), int(nir_band))


@f.udf(V2_TILE_SCHEMA)
@_tile_op("red_band", "nir_band")
def _ndvi_udf(tile, red_band, nir_band):
    new_bytes = _ndvi_bytes(tile, red_band, nir_band)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
@_tile_op("red_band", "nir_band")
def _ndvi_v2_udf(
    tile, red_band, nir_band, virtualize_dir, virtualize_prefix, materialize
):
    new_bytes = _ndvi_bytes(tile, red_band, nir_band)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


def _ndwi_bytes(tile, green_idx, nir_idx):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _open_op(tile) as ds:
        return indices.ndwi(ds, int(green_idx), int(nir_idx))


@f.udf(V2_TILE_SCHEMA)
@_tile_op("green_idx", "nir_idx")
def _ndwi_udf(tile, green_idx, nir_idx):
    new_bytes = _ndwi_bytes(tile, green_idx, nir_idx)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
@_tile_op("green_idx", "nir_idx")
def _ndwi_v2_udf(
    tile, green_idx, nir_idx, virtualize_dir, virtualize_prefix, materialize
):
    new_bytes = _ndwi_bytes(tile, green_idx, nir_idx)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


def _nbr_bytes(tile, nir_idx, swir_idx):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _open_op(tile) as ds:
        return indices.nbr(ds, int(nir_idx), int(swir_idx))


@f.udf(V2_TILE_SCHEMA)
@_tile_op("nir_idx", "swir_idx")
def _nbr_udf(tile, nir_idx, swir_idx):
    new_bytes = _nbr_bytes(tile, nir_idx, swir_idx)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
@_tile_op("nir_idx", "swir_idx")
def _nbr_v2_udf(
    tile, nir_idx, swir_idx, virtualize_dir, virtualize_prefix, materialize
):
    new_bytes = _nbr_bytes(tile, nir_idx, swir_idx)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


def _savi_bytes(tile, red_idx, nir_idx, l_val):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _open_op(tile) as ds:
        return indices.savi(ds, int(red_idx), int(nir_idx), l=float(l_val))


@f.udf(V2_TILE_SCHEMA)
@_tile_op("red_idx", "nir_idx", "l_val")
def _savi_udf(tile, red_idx, nir_idx, l_val):
    new_bytes = _savi_bytes(tile, red_idx, nir_idx, l_val)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
@_tile_op("red_idx", "nir_idx", "l_val")
def _savi_v2_udf(
    tile, red_idx, nir_idx, l_val, virtualize_dir, virtualize_prefix, materialize
):
    new_bytes = _savi_bytes(tile, red_idx, nir_idx, l_val)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


def _evi_bytes(tile, red_idx, nir_idx, blue_idx, l_val, c1, c2, g):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _open_op(tile) as ds:
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
@_tile_op("red_idx", "nir_idx", "blue_idx", "l_val", "c1", "c2", "g")
def _evi_udf(tile, red_idx, nir_idx, blue_idx, l_val, c1, c2, g):
    new_bytes = _evi_bytes(tile, red_idx, nir_idx, blue_idx, l_val, c1, c2, g)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
@_tile_op("red_idx", "nir_idx", "blue_idx", "l_val", "c1", "c2", "g")
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


def _land_cover_bytes(tile, method, n_clusters, smooth):
    from databricks.labs.gbx.pyrx import _env
    from databricks.labs.gbx.pyrx.core._nodata import emit

    _env.configure_gdal_env()
    with _open_op(tile) as ds:
        if ds.count < 3:
            raise _UnreadableTile(
                f"rst_land_cover needs a tile with 3 or more bands (red, green, blue); this one has {ds.count}. "
                "Fix: stack the bands first (gbx_rst_frombands)"
            )
        arr = ds.read().transpose(1, 2, 0)  # (bands,H,W) -> HxWxbands
        labels, _names = land_cover_core.classify(
            arr,
            method=str(method).lower(),
            n_clusters=int(n_clusters),
            smooth=int(smooth),
        )
        return emit(ds, labels, -1, labels == -1, "int32")


def _check_land_cover(*, method, n_clusters, smooth, **_):
    land_cover_core.check_args(method, n_clusters, smooth)


@f.udf(V2_TILE_SCHEMA)
@_tile_op("method", "n_clusters", "smooth", check=_check_land_cover)
def _land_cover_udf(tile, method, n_clusters, smooth):
    new_bytes = _land_cover_bytes(tile, method, n_clusters, smooth)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
@_tile_op("method", "n_clusters", "smooth", check=_check_land_cover)
def _land_cover_v2_udf(
    tile, method, n_clusters, smooth, virtualize_dir, virtualize_prefix, materialize
):
    new_bytes = _land_cover_bytes(tile, method, n_clusters, smooth)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


def _percentile_stretch_bytes(tile, lo_pct, hi_pct):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _open_op(tile) as ds:
        return stretch.percentile_stretch(ds, float(lo_pct), float(hi_pct))


def _check_percentiles(*, lo_pct, hi_pct, **_):
    stretch.check_percentiles(lo_pct, hi_pct)


@f.udf(V2_TILE_SCHEMA)
@_tile_op("lo_pct", "hi_pct", check=_check_percentiles)
def _percentile_stretch_udf(tile, lo_pct, hi_pct):
    new_bytes = _percentile_stretch_bytes(tile, lo_pct, hi_pct)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
@_tile_op("lo_pct", "hi_pct", check=_check_percentiles)
def _percentile_stretch_v2_udf(
    tile, lo_pct, hi_pct, virtualize_dir, virtualize_prefix, materialize
):
    new_bytes = _percentile_stretch_bytes(tile, lo_pct, hi_pct)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


# --- Tier 1e: constructor + fill UDFs (vector bridge) -----------------------
def _check_grid(fn, xmin, ymin, xmax, ymax, width_px, height_px):
    """The extent and size of an output grid: a size above 0 and an extent with its far edge above its near one.
    ValueError naming the value."""
    if int(width_px) <= 0 or int(height_px) <= 0:
        raise ValueError(
            f"{fn}: width_px and height_px must be above 0 (got {width_px} x {height_px})"
        )
    if not float(xmax) > float(xmin) or not float(ymax) > float(ymin):
        raise ValueError(
            f"{fn}: the extent must have xmax above xmin and ymax above ymin (got x {xmin}..{xmax}, y {ymin}..{ymax})"
        )


@f.udf(V2_TILE_SCHEMA)
def _rasterize_udf(
    geom_wkb,
    value,
    xmin,
    ymin,
    xmax,
    ymax,
    width_px,
    height_px,
    out_srid=None,
    out_crs=None,
):
    if any(
        a is None
        for a in (geom_wkb, value, xmin, ymin, xmax, ymax, width_px, height_px)
    ):
        return None
    import shapely

    from databricks.labs.gbx._geom import parse_geom
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    _check_grid("rst_rasterize", xmin, ymin, xmax, ymax, width_px, height_px)
    try:
        geom = parse_geom(geom_wkb)
        if geom is None:  # an empty or blank string parses to nothing
            raise ValueError("it holds no geometry")
    except Exception as exc:  # noqa: BLE001 - shapely's text does not name the value
        raise ValueError(
            f"rst_rasterize: the geometry {str(geom_wkb)[:80]!r} is not WKB, EWKB, WKT or EWKT ({exc})"
        ) from exc
    new_bytes = features.rasterize_geom(
        # EWKB, so the SRID an EWKT or EWKB carries reaches the reprojection
        shapely.to_wkb(geom, include_srid=True),
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


def _fillnodata_bytes(tile, max_search_dist, smoothing_iter):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _open_op(tile) as ds:
        return features.fill_nodata(ds, max_search_dist, smoothing_iter)


@f.udf(V2_TILE_SCHEMA)
@_tile_op("max_search_dist", "smoothing_iter")
def _fillnodata_udf(tile, max_search_dist, smoothing_iter):
    new_bytes = _fillnodata_bytes(tile, max_search_dist, smoothing_iter)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
@_tile_op("max_search_dist", "smoothing_iter")
def _fillnodata_v2_udf(
    tile,
    max_search_dist,
    smoothing_iter,
    virtualize_dir,
    virtualize_prefix,
    materialize,
):
    new_bytes = _fillnodata_bytes(tile, max_search_dist, smoothing_iter)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


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
    srid=None,
    power=None,
    max_pts=None,
    out_crs=None,
):
    if any(
        a is None for a in (points, values, xmin, ymin, xmax, ymax, width_px, height_px)
    ):
        return None
    from databricks.labs.gbx.pyrx import _env
    from databricks.labs.gbx.pyrx.core.tin import _parse_geom_elem

    _env.configure_gdal_env()
    _check_grid("rst_gridfrompoints", xmin, ymin, xmax, ymax, width_px, height_px)
    if len(points) != len(values):
        raise ValueError(
            f"rst_gridfrompoints: points ({len(points)}) and values ({len(values)}) length mismatch"
        )
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


# --- Tier 1e4: point-cloud binning (array -> tile) ---------------------------
@f.udf(V2_TILE_SCHEMA)
def _binpoints_udf(
    x_arr, y_arr, z_arr, xmin, ymin, xmax, ymax, w, h, srid, statistic=None
):
    """Scalar: bin parallel ARRAY<DOUBLE> columns into a single-band GTiff tile.

    Produces one tile per row. Extent/size/srid/statistic are scalar args.
    NULL or empty input arrays return None.
    """
    if any(a is None for a in (y_arr, z_arr, xmin, ymin, xmax, ymax, w, h, srid)):
        return None
    if not x_arr:
        return None
    _check_grid("rst_binpoints", xmin, ymin, xmax, ymax, w, h)
    if not len(x_arr) == len(y_arr) == len(z_arr):
        raise ValueError(
            "rst_binpoints: x_array, y_array and z_array must have the same length "
            f"(got {len(x_arr)}, {len(y_arr)}, {len(z_arr)})"
        )
    b = binning.bin_points(
        x_arr,
        y_arr,
        z_arr,
        float(xmin),
        float(ymin),
        float(xmax),
        float(ymax),
        int(w),
        int(h),
        int(srid),
        str(statistic) if statistic is not None else "max",
    )
    return _serde.build_tile(b, "GTiff", 0)


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
    srid=None,
    no_data=None,
    out_crs=None,
):
    if any(
        a is None
        for a in (
            points,
            merge_tolerance,
            snap_tolerance,
            xmin,
            ymin,
            xmax,
            ymax,
            width_px,
            height_px,
        )
    ):
        return None
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    _check_grid("rst_dtmfromgeoms", xmin, ymin, xmax, ymax, width_px, height_px)
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

    def eval(self, tile, band=1, connectedness=4):
        band = 1 if band is None else int(band)  # a NULL optional is its default
        connectedness = 4 if connectedness is None else int(connectedness)
        if connectedness not in (4, 8):
            raise ValueError(
                f"rst_polygonize: connectedness must be 4 or 8; got {connectedness}"
            )
        if _tile_is_empty(tile):  # NULL, an error tile: nothing to polygonize
            return
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        with _open_or_none(tile) as ds:
            if ds is None:
                return
            if not 1 <= band <= ds.count:
                raise ValueError(
                    f"rst_polygonize: band {band} is outside the tile's {ds.count} band(s)"
                )
            for g, v in features.polygonize(ds, band, connectedness):
                yield (g, v)


# --- Tier 1e2: tiling UDTFs (separatebands, retile, tooverlappingtiles) -----
# All four fan out to ARRAY<tile> in the heavyweight tier; the light tier
# streams one tile struct per row via UDTFs (eval yields each tile dict
# incrementally from the iter_* cores — never buffers the full list).
# Each UDTF row IS the tile struct (V2_TILE_SCHEMA: 9-field v2 tile with cellid, raster, metadata, path_mode, etc).


@udtf(returnType=V2_TILE_SCHEMA)
class _RstSeparateBandsUDTF:
    """Streaming UDTF: yield one single-band tile struct per band."""

    def eval(self, tile, file_ref=None):
        if _tile_is_empty(tile):
            yield from _empty_tile_rows(tile, "RST_SeparateBands")
            return
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        # NOTE: a mid-iteration failure yields already-emitted good rows + one error row
        # (not exactly 1); accepted because fully-corrupt open-failure is the primary case.
        try:
            with ot._open(tile, file_ref=file_ref) as ds:
                # Parent cellid (null -> 0), matching heavy's getLong stamp.
                cellid = _tile_cellid(tile)
                for band, b in tiling.iter_separate_bands_indexed(ds):
                    # The band number marks this tile as one band of its parent.
                    yield _serde.build_tile(
                        b,
                        "GTiff",
                        cellid,
                        extra_metadata={BAND_METADATA_KEY: str(band)},
                    )
        except Exception as e:  # noqa: BLE001
            yield _serde.build_error_tile(f"RST_SeparateBands: {e}")
            return


def _cut_tile(raster_bytes, cellid, window, whole_input_of=None, band=None):
    """One output tile of a retile-family function.

    It records its pixel ``window`` within the INPUT tile, so a writer can tell
    siblings apart, and the input's ``band`` marker (see :func:`_serde.band_marker`)
    when it is a band of its parent, so the pieces of two bands stay apart. The one
    output that covers its whole input (``whole_input_of`` is that input tile) is
    the input itself and keeps the input's provenance instead; see
    :func:`_serde.inherit_provenance`.
    """
    if whole_input_of is not None:
        return _serde.inherit_provenance(
            _serde.build_tile(raster_bytes, "GTiff", cellid), whole_input_of
        )
    return _serde.build_tile(
        raster_bytes,
        "GTiff",
        cellid,
        window=window,
        extra_metadata=None if band is None else {BAND_METADATA_KEY: band},
    )


@udtf(returnType=V2_TILE_SCHEMA)
class _RstRetileUDTF:
    """Streaming UDTF: yield one sub-tile struct per non-overlapping window."""

    def eval(self, tile, tile_width, tile_height, file_ref=None):
        tiling.require_window(
            tile_width, tile_height
        )  # a parameter error raises, whatever the tile
        if _tile_is_empty(tile):
            yield from _empty_tile_rows(tile, "RST_ReTile")
            return
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        # NOTE: mid-iteration failure yields already-emitted good rows + one error row.
        try:
            with ot._open(tile, file_ref=file_ref, gate=False) as ds:
                cellid = _tile_cellid(
                    tile
                )  # parent cellid (null -> 0), as heavy stamps
                band = _serde.band_marker(tile)  # a band of its parent, or None
                whole = tiling.covers_whole_input(ds, tile_width, tile_height)
                for win, b in tiling.iter_retile_located(
                    ds, int(tile_width), int(tile_height)
                ):
                    yield _cut_tile(b, cellid, win, tile if whole else None, band)
        except Exception as e:  # noqa: BLE001
            yield _serde.build_error_tile(f"RST_ReTile: {e}")
            return


@udtf(returnType=V2_TILE_SCHEMA)
class _RstToOverlappingTilesUDTF:
    """Streaming UDTF: yield one sub-tile struct per overlapping window."""

    def eval(self, tile, tile_width, tile_height, overlap, file_ref=None):
        tiling.require_window(tile_width, tile_height, "gbx_rst_tooverlappingtiles")
        if overlap is not None:
            try:
                overlap = int(overlap)
            except (TypeError, ValueError, OverflowError):  # NaN, infinity, text
                raise ValueError(
                    f"gbx_rst_tooverlappingtiles: overlap must be a number from 0 to below 100 percent, got {overlap!r}. "
                    "Fix: pass the overlap as a percentage of the tile size."
                ) from None
            if not 0 <= overlap < 100:
                raise ValueError(
                    f"gbx_rst_tooverlappingtiles: overlap must be from 0 to below 100 percent, got {overlap}. "
                    "Fix: pass the overlap as a percentage of the tile size."
                )
        if _tile_is_empty(tile):
            yield from _empty_tile_rows(tile, "RST_ToOverlappingTiles")
            return
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        # NOTE: mid-iteration failure yields already-emitted good rows + one error row.
        try:
            with ot._open(tile, file_ref=file_ref, gate=False) as ds:
                cellid = _tile_cellid(
                    tile
                )  # parent cellid (null -> 0), as heavy stamps
                band = _serde.band_marker(tile)  # a band of its parent, or None
                whole = tiling.covers_whole_input(
                    ds, tile_width, tile_height, int(overlap)
                )
                for win, b in tiling.iter_to_overlapping_tiles_located(
                    ds, int(tile_width), int(tile_height), int(overlap)
                ):
                    yield _cut_tile(b, cellid, win, tile if whole else None, band)
        except Exception as e:  # noqa: BLE001
            yield _serde.build_error_tile(f"RST_ToOverlappingTiles: {e}")
            return


@udtf(returnType=V2_TILE_SCHEMA)
class _RstMakeTilesUDTF:
    """Streaming UDTF: yield one sub-tile struct per power-of-4 split tile."""

    def eval(self, tile, size_in_mb, file_ref=None):
        if _tile_is_empty(tile):
            yield from _empty_tile_rows(tile, "RST_MakeTiles")
            return
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        # NOTE: mid-iteration failure yields already-emitted good rows + one error row.
        try:
            # Key size_bytes on the materialized raster bytes when present (byte-identical
            # to pre-change behavior for materialized tiles; preserves the encoded byte
            # length the old path used via len(raster)). For virtual tiles (tile.raster
            # is None, whether opened via FILE stream or FUSE path), fall back to
            # _encoded_size_bytes (dataset re-encode), matching heavy BalancedSubdivision.
            raster_bytes = tile.raster
            size_bytes = len(raster_bytes) if raster_bytes is not None else None
            # NOTE: For virtual tiles (raster=None), size_bytes=None so iter_make_tiles
            # uses the re-encoded dataset size rather than the original bytes length.
            # With compressed virtual inputs the re-encoded size may differ slightly
            # from a materialized tile of the same content, so split-count may vary
            # slightly — this divergence is inherent to streaming and expected.
            with ot._open(tile, file_ref=file_ref, gate=False) as ds:
                cellid = _tile_cellid(
                    tile
                )  # parent cellid (null -> 0), as heavy stamps
                band = _serde.band_marker(tile)  # a band of its parent, or None
                tile_x, tile_y = tiling.make_tiles_size(
                    ds, float(size_in_mb), size_bytes=size_bytes
                )
                whole = tiling.covers_whole_input(ds, tile_x, tile_y)
                for win, b in tiling.iter_retile_located(ds, tile_x, tile_y):
                    yield _cut_tile(b, cellid, win, tile if whole else None, band)
        except Exception as e:  # noqa: BLE001
            yield _serde.build_error_tile(f"RST_MakeTiles: {e}")
            return


@udtf(returnType=V2_TILE_SCHEMA)
class _RstH3TessellateUDTF:
    """Streaming UDTF: yield one clipped tile struct per overlapping H3 cell."""

    def eval(self, tile, resolution, assignment=None, coverage=None, file_ref=None):
        if _tile_is_empty(tile) or resolution is None:
            yield from _empty_tile_rows(tile, "RST_H3_Tessellate")
            return
        effective_mode = assignment if assignment is not None else "centroid"
        if effective_mode not in {"covering", "centroid"}:
            raise ValueError(
                f"rst_h3_tessellate: assignment must be one of covering, centroid; "
                f"got '{effective_mode}'"
            )
        effective_coverage = coverage if coverage is not None else "complete"
        if effective_coverage not in {"sparse", "complete"}:
            raise ValueError(
                f"rst_h3_tessellate: coverage must be one of sparse, complete; "
                f"got '{effective_coverage}'"
            )
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        # NOTE: mid-iteration failure yields already-emitted good rows + one error row.
        try:
            with ot._open(tile, file_ref=file_ref) as ds:
                for cellid, raster in tessellate_core.iter_tessellate(
                    ds,
                    int(resolution),
                    "h3",
                    mode=effective_mode,
                    coverage=effective_coverage,
                ):
                    if raster is None:  # defensive: never emit a null-raster tile row
                        continue
                    yield _serde.build_tile(raster, "GTiff", cellid, grid_system="h3")
        except Exception as e:  # noqa: BLE001
            yield _serde.build_error_tile(f"RST_H3_Tessellate: {e}")
            return


def _light_rst_retile(tile: ColLike, tile_width: ColLike, tile_height: ColLike):
    """The light Column composition of ``gbx_rst_retile`` (the bench's light form and the
    light tests). The Python API is ``databricks.labs.gbx.rasterx.functions.rst_retile``.

    Partition a tile into non-overlapping sub-tiles of the given pixel size.

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


@udtf(returnType=V2_TILE_SCHEMA)
class _RstQuadbinTessellateUDTF:
    """Streaming UDTF: yield one clipped tile struct per overlapping quadbin cell."""

    def eval(self, tile, resolution, assignment=None, coverage=None, file_ref=None):
        if _tile_is_empty(tile) or resolution is None:
            yield from _empty_tile_rows(tile, "RST_Quadbin_Tessellate")
            return
        effective_mode = assignment if assignment is not None else "centroid"
        if effective_mode not in {"covering", "centroid"}:
            raise ValueError(
                f"rst_quadbin_tessellate: assignment must be one of covering, centroid; "
                f"got '{effective_mode}'"
            )
        effective_coverage = coverage if coverage is not None else "complete"
        if effective_coverage not in {"sparse", "complete"}:
            raise ValueError(
                f"rst_quadbin_tessellate: coverage must be one of sparse, complete; "
                f"got '{effective_coverage}'"
            )
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        # NOTE: mid-iteration failure yields already-emitted good rows + one error row.
        try:
            with ot._open(tile, file_ref=file_ref) as ds:
                for cellid, raster in tessellate_core.iter_tessellate(
                    ds,
                    int(resolution),
                    "quadbin",
                    mode=effective_mode,
                    coverage=effective_coverage,
                ):
                    if raster is None:  # defensive: never emit a null-raster tile row
                        continue
                    yield _serde.build_tile(
                        raster, "GTiff", cellid, grid_system="quadbin"
                    )
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

    def eval(self, tile, resolution, assignment=None, coverage=None, file_ref=None):
        if _tile_is_empty(tile) or resolution is None:
            yield from _empty_tile_rows(tile, "RST_BNG_Tessellate")
            return
        effective_mode = assignment if assignment is not None else "centroid"
        if effective_mode not in {"covering", "centroid"}:
            raise ValueError(
                f"rst_bng_tessellate: assignment must be one of covering, centroid; "
                f"got '{effective_mode}'"
            )
        effective_coverage = coverage if coverage is not None else "complete"
        if effective_coverage not in {"sparse", "complete"}:
            raise ValueError(
                f"rst_bng_tessellate: coverage must be one of sparse, complete; "
                f"got '{effective_coverage}'"
            )
        from databricks.labs.gbx.pygx import _bng
        from databricks.labs.gbx.pygx.functions import _norm_res
        from databricks.labs.gbx.pyrx import _env

        resolution = _norm_res(resolution)  # as the grid explodes read it
        _bng.get_resolution(
            resolution
        )  # a resolution BNG does not define is a parameter error: it raises
        _env.configure_gdal_env()
        # NOTE: mid-iteration failure yields already-emitted good rows + one error row.
        try:
            with ot._open(tile, file_ref=file_ref) as ds:
                for cellid_str, raster in tessellate_core.iter_tessellate(
                    ds,
                    resolution,
                    "bng",
                    mode=effective_mode,
                    coverage=effective_coverage,
                ):
                    if raster is None:  # defensive: never emit a null-raster tile row
                        continue
                    out = _serde.build_tile(
                        raster, "GTiff", _bng.parse(cellid_str), grid_system="bng"
                    )
                    yield out
        except Exception as e:  # noqa: BLE001
            yield _serde.build_error_tile(f"RST_BNG_Tessellate: {e}")
            return


# --- Custom-grid tessellate UDTF -------------------------------------------
# SQL arg order: (tile, grid, resolution, [assignment], [coverage])
# grid is the custom-grid struct (produced by gbx_custom_grid(...)).
# cellID is LONG (custom cell IDs are signed int64 by construction).


@udtf(returnType=V2_TILE_SCHEMA)
class _RstCustomTessellateUDTF:
    """Streaming UDTF: yield one clipped tile struct per overlapping custom-grid cell."""

    def eval(
        self,
        tile,
        grid_struct,
        resolution,
        assignment=None,
        coverage=None,
        file_ref=None,
    ):
        if _tile_is_empty(tile):
            yield from _empty_tile_rows(tile, "RST_Custom_Tessellate")
            return
        if resolution is None or grid_struct is None:
            yield _serde.build_error_tile(
                "RST_Custom_Tessellate: empty tile or missing grid/resolution"
            )
            return
        effective_mode = assignment if assignment is not None else "centroid"
        if effective_mode not in {"covering", "centroid"}:
            raise ValueError(
                f"rst_custom_tessellate: assignment must be one of covering, centroid; "
                f"got '{effective_mode}'"
            )
        effective_coverage = coverage if coverage is not None else "complete"
        if effective_coverage not in {"sparse", "complete"}:
            raise ValueError(
                f"rst_custom_tessellate: coverage must be one of sparse, complete; "
                f"got '{effective_coverage}'"
            )
        from databricks.labs.gbx.pygx import _custom as _custom_mod
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        try:
            conf = _custom_mod.conf_from_row(grid_struct)
            with ot._open(tile, file_ref=file_ref) as ds:
                for cellid, raster in tessellate_core.iter_tessellate(
                    ds,
                    int(resolution),
                    "custom",
                    mode=effective_mode,
                    coverage=effective_coverage,
                    conf=conf,
                ):
                    if raster is None:
                        continue
                    yield _serde.build_tile(
                        raster, "GTiff", cellid, grid_system="custom"
                    )
        except Exception as e:  # noqa: BLE001
            yield _serde.build_error_tile(f"RST_Custom_Tessellate: {e}")
            return


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
    with _open_op(tile, file_ref=file_ref) as ds:
        return terrain.aspect(
            ds,
            trigonometric=bool(trigonometric),
            zero_for_flat=bool(zero_for_flat),
        )


@f.udf(V2_TILE_SCHEMA)
@_tile_op("trigonometric", "zero_for_flat")
def _aspect_udf(tile, trigonometric, zero_for_flat):
    new_bytes = _aspect_bytes(tile, trigonometric, zero_for_flat)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
@_tile_op("trigonometric", "zero_for_flat")
def _aspect_v2_udf(
    tile, trigonometric, zero_for_flat, virtualize_dir, virtualize_prefix, materialize
):
    new_bytes = _aspect_bytes(tile, trigonometric, zero_for_flat)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


@f.udf(V2_TILE_SCHEMA)
@_tile_op("trigonometric", "zero_for_flat")
def _uf_aspect(tile, file_ref, trigonometric, zero_for_flat):
    new_bytes = _aspect_bytes(tile, trigonometric, zero_for_flat, file_ref=file_ref)
    if new_bytes is None:
        return None
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


def _hillshade_bytes(tile, azimuth, altitude, z_factor, xscale, yscale, file_ref=None):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    xs = None if xscale is None else float(xscale)
    ys = None if yscale is None else float(yscale)
    with _open_op(tile, file_ref=file_ref) as ds:
        return terrain.hillshade(
            ds,
            azimuth=float(azimuth),
            altitude=float(altitude),
            z_factor=float(z_factor),
            xscale=xs,
            yscale=ys,
        )


@f.udf(V2_TILE_SCHEMA)
@_tile_op("azimuth", "altitude", "z_factor")
def _hillshade_udf(tile, azimuth, altitude, z_factor, xscale, yscale):
    new_bytes = _hillshade_bytes(tile, azimuth, altitude, z_factor, xscale, yscale)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
@_tile_op("azimuth", "altitude", "z_factor")
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
    new_bytes = _hillshade_bytes(tile, azimuth, altitude, z_factor, xscale, yscale)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


@f.udf(V2_TILE_SCHEMA)
@_tile_op("azimuth", "altitude", "z_factor")
def _uf_hillshade(tile, file_ref, azimuth, altitude, z_factor, xscale, yscale):
    new_bytes = _hillshade_bytes(
        tile, azimuth, altitude, z_factor, xscale, yscale, file_ref=file_ref
    )
    if new_bytes is None:
        return None
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


def _light_rst_slope(
    tile: ColLike,
    unit: ColLike = "degrees",
    xscale: ColLike = None,
    yscale: ColLike = None,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
    materialize: Optional[bool] = None,
) -> Column:
    """The light Column composition of ``gbx_rst_slope`` (the bench's light form and the
    light tests). The Python API is ``databricks.labs.gbx.rasterx.functions.rst_slope``.

    Compute terrain slope from a single-band DEM tile (Horn's 3x3 method).

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


# --- Tier 1g: terrain ruggedness UDFs (tri, tpi, roughness) -----------------
def _ruggedness_bytes(tile, core_fn):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _open_op(tile) as ds:
        return core_fn(ds)


@f.udf(V2_TILE_SCHEMA)
@_tile_op()
def _tri_udf(tile):
    new_bytes = _ruggedness_bytes(tile, terrain.tri)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
@_tile_op()
def _tri_v2_udf(tile, virtualize_dir, virtualize_prefix, materialize):
    new_bytes = _ruggedness_bytes(tile, terrain.tri)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


@f.udf(V2_TILE_SCHEMA)
@_tile_op()
def _tpi_udf(tile):
    new_bytes = _ruggedness_bytes(tile, terrain.tpi)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
@_tile_op()
def _tpi_v2_udf(tile, virtualize_dir, virtualize_prefix, materialize):
    new_bytes = _ruggedness_bytes(tile, terrain.tpi)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


@f.udf(V2_TILE_SCHEMA)
@_tile_op()
def _roughness_udf(tile):
    new_bytes = _ruggedness_bytes(tile, terrain.roughness)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
@_tile_op()
def _roughness_v2_udf(tile, virtualize_dir, virtualize_prefix, materialize):
    new_bytes = _ruggedness_bytes(tile, terrain.roughness)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


def _color_relief_bytes(tile, color_table_path):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _open_op(tile) as ds:
        return terrain.color_relief(ds, str(color_table_path))


def _check_color_relief(*, color_table_path, **_):
    from databricks.labs.gbx.ds._listing import to_local_path

    path = to_local_path(str(color_table_path))
    # a remote path (a URI scheme) or a GDAL virtual one (/vsi...) is GDAL's to open, so only a local file is checked
    if re.match(r"^(/vsi|[A-Za-z][A-Za-z0-9+.-]*:)", path):
        return
    if not os.path.isfile(path):
        raise FileNotFoundError(
            f"rst_color_relief: the colour table {color_table_path!r} does not exist"
        )


@f.udf(V2_TILE_SCHEMA)
@_tile_op("color_table_path", check=_check_color_relief)
def _color_relief_udf(tile, color_table_path):
    new_bytes = _color_relief_bytes(tile, color_table_path)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
@_tile_op("color_table_path", check=_check_color_relief)
def _color_relief_v2_udf(
    tile, color_table_path, virtualize_dir, virtualize_prefix, materialize
):
    new_bytes = _color_relief_bytes(tile, color_table_path)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


# --- Accessors and statistics ----------------------------------------
# Their public Python API is the generated one (``databricks.labs.gbx.rasterx.functions``). The ``_u_*``
# and ``_uf_*`` UDFs above are the manifest's light kernels: a header accessor answers from the file header
# (``open_header``), a pixel accessor reads the window, and the ``_uf_*`` forms also take a FILE reference.


# --- Coordinate transforms -----------------------------------------
# HEADER-ONLY accessors: pass the FULL tile struct (not the raster subfield) so
# a virtual tile's ``path`` is reachable; the UDF resolves via open_header.


# --- Group 1: per-band statistics & accessors -------------------------------


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


def _light_rst_histogram(
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
    with _open_op(tile) as ds:
        return derivedband_core.derivedband(ds, str(pyfunc), str(func_name))


def _check_derivedband(*, pyfunc, func_name, **_):
    derivedband_core.check_function(pyfunc, func_name)


@f.udf(V2_TILE_SCHEMA)
@_tile_op("pyfunc", "func_name", check=_check_derivedband)
def _derivedband_udf(tile, pyfunc, func_name):
    new_bytes = _derivedband_bytes(tile, pyfunc, func_name)
    return _serde.build_tile(new_bytes, "GTiff", _tile_cellid(tile))


@f.udf(V2_TILE_SCHEMA)
@_tile_op("pyfunc", "func_name", check=_check_derivedband)
def _derivedband_v2_udf(
    tile, pyfunc, func_name, virtualize_dir, virtualize_prefix, materialize
):
    new_bytes = _derivedband_bytes(tile, pyfunc, func_name)
    return _shaped_result_row(
        new_bytes, _tile_cellid(tile), virtualize_dir, virtualize_prefix, materialize
    )


# --- Tier 1h: web-mercator XYZ tiling UDFs ---------------------------------
@f.udf(BinaryType())
def _tilexyz_udf(
    tile, z, x, y, format="PNG", size=256, resampling="bilinear", rescale="auto"
):
    # A NULL coordinate is NULL. Otherwise rst_tilexyz NEVER returns null: a null, error or unreadable tile, a tile
    # outside the raster and any hard failure yield a transparent image (slippy-map servers need a body). A bad
    # argument raises, also for such a tile.
    if z is None or x is None or y is None:
        return None
    sz = int(size) if size is not None else 256
    fmt = str(format) if format is not None else "PNG"
    resamp = str(resampling) if resampling is not None else "bilinear"
    rsc = rescale if rescale is not None else "auto"
    xyz.validate_render_args(fmt, sz, resamp, rsc)
    if _tile_is_empty(tile):
        return xyz.transparent_png(sz)
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    try:
        with _open_op(tile) as ds:
            return xyz.render_tile(
                ds, int(z), int(x), int(y), fmt, sz, resamp, rescale=rsc
            )
    except _UnreadableTile:
        return xyz.transparent_png(sz)


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
        # A NULL tile, an error tile and a tile that cannot be read have no tiles to render: no rows. A bad argument
        # raises, also for such a tile.
        if min_z is None or max_z is None:
            return
        fmt = str(format) if format is not None else "PNG"
        sz = int(size) if size is not None else 256
        resamp = str(resampling) if resampling is not None else "bilinear"
        rsc = rescale if rescale is not None else "auto"
        xyz.validate_render_args(fmt, sz, resamp, rsc)
        xyz._validate_zoom_range(min_z, max_z)
        if _tile_is_empty(tile):
            return
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        try:
            with _open_op(tile) as ds:
                for z, x, y, b in xyz.iter_pyramid(
                    ds, int(min_z), int(max_z), fmt, sz, resamp, rsc
                ):
                    yield (z, x, y, b)
        except _UnreadableTile:
            return


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
# BNG renders a formatted BNG string (e.g. "TQ3080") as cellID, matching heavy
# RST_BNG_RasterToGrid* (StringType cellID). All measures -> DOUBLE (count
# changed from INT to DOUBLE for heavy-tier parity).
_GRID_FLAT_STRING_SCHEMA = _grid_flat_schema(DoubleType(), StringType())


def _rastertogrid_rows(
    tile, resolution, grid, agg, coverage, assignment, cellid_is_str, grid_struct=None
):
    """The (band, cellID, measure) rows of one raster->grid call. A NULL tile, resolution or grid gives no rows. A
    resolution the grid does not define, or a coverage or assignment word that names none, raises naming it, also
    for a tile that is an error tile or cannot be read. Those tiles give no rows."""
    if tile is None or resolution is None or (grid == "custom" and grid_struct is None):
        return
    if grid == "bng":  # a size such as '1km' is its index, as in the grid functions
        from databricks.labs.gbx.pygx.functions import _norm_res

        resolution = _norm_res(resolution)
    coverage, assignment = coverage or "complete", assignment or "centroid"
    gridagg.check_arguments(resolution, grid, agg, coverage, assignment, grid_struct)
    if _tile_is_empty(tile):  # an error tile: nothing to aggregate
        return
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    with _open_or_none(tile) as ds:
        if ds is None:  # bytes GDAL cannot read
            return
        bands_data = gridagg.raster_to_grid(
            ds,
            resolution,
            grid,
            agg,
            coverage=coverage,
            assignment=assignment,
            grid_conf=grid_struct,
        )
    # Flat rows (band, cellID, measure), never a nested list of the whole raster. BNG cell ids are formatted
    # strings already; the others are Python ints, so a cell id past 2**53 never meets a float.
    for band_idx, cells in enumerate(bands_data, start=1):
        for cell in cells:
            cid = cell["cellID"] if cellid_is_str else int(cell["cellID"])
            yield (band_idx, cid, cell["measure"])


def _make_rastertogrid_udtf(grid, agg, flat_schema, cellid_is_str=False):
    """Factory for raster->grid aggregation UDTFs.

    For ``grid="custom"`` the UDTF eval signature includes a ``grid`` struct arg
    immediately after ``tile`` (SQL arg order: tile, grid, resolution, [coverage],
    [assignment]) — matching the pinned heavy arg order.  All other grids have
    the standard signature (tile, resolution, [coverage], [assignment]).
    """
    if grid == "custom":

        @udtf(returnType=flat_schema)
        class _CustomRasterToGridUDTF:
            def eval(
                self, tile, grid_struct, resolution, coverage=None, assignment=None
            ):
                yield from _rastertogrid_rows(
                    tile,
                    resolution,
                    grid,
                    agg,
                    coverage,
                    assignment,
                    cellid_is_str,
                    grid_struct,
                )

        return _CustomRasterToGridUDTF

    @udtf(returnType=flat_schema)
    class _RasterToGridUDTF:
        def eval(self, tile, resolution, coverage=None, assignment=None):
            yield from _rastertogrid_rows(
                tile, resolution, grid, agg, coverage, assignment, cellid_is_str
            )

    return _RasterToGridUDTF


_RstH3RasterToGridAvgUDTF = _make_rastertogrid_udtf(
    "h3", "avg", _GRID_FLAT_DOUBLE_SCHEMA
)
_RstH3RasterToGridCountUDTF = _make_rastertogrid_udtf(
    "h3", "count", _GRID_FLAT_DOUBLE_SCHEMA
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
    "quadbin", "count", _GRID_FLAT_DOUBLE_SCHEMA
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
    "bng", "count", _GRID_FLAT_STRING_SCHEMA, cellid_is_str=True
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

# --- Tier 1i: custom-grid rastertogrid UDTFs --------------------------------
# SQL arg order: (tile, grid, resolution, [coverage], [assignment])
# grid is a custom-grid struct (produced by gbx_custom_grid(...)).
# cellID is LONG (custom cell ids are always signed int64).
_RstCustomRasterToGridAvgUDTF = _make_rastertogrid_udtf(
    "custom", "avg", _GRID_FLAT_DOUBLE_SCHEMA
)
_RstCustomRasterToGridCountUDTF = _make_rastertogrid_udtf(
    "custom", "count", _GRID_FLAT_DOUBLE_SCHEMA
)
_RstCustomRasterToGridMaxUDTF = _make_rastertogrid_udtf(
    "custom", "max", _GRID_FLAT_DOUBLE_SCHEMA
)
_RstCustomRasterToGridMinUDTF = _make_rastertogrid_udtf(
    "custom", "min", _GRID_FLAT_DOUBLE_SCHEMA
)
_RstCustomRasterToGridMedianUDTF = _make_rastertogrid_udtf(
    "custom", "median", _GRID_FLAT_DOUBLE_SCHEMA
)
_RstCustomRasterToGridSumUDTF = _make_rastertogrid_udtf(
    "custom", "sum", _GRID_FLAT_DOUBLE_SCHEMA
)
_RstCustomRasterToGridVarianceUDTF = _make_rastertogrid_udtf(
    "custom", "variance", _GRID_FLAT_DOUBLE_SCHEMA
)
_RstCustomRasterToGridStddevUDTF = _make_rastertogrid_udtf(
    "custom", "stddev", _GRID_FLAT_DOUBLE_SCHEMA
)


# ---------------------------------------------------------------------------
# Grouped aggregators (rst_*_agg)
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


# An aggregate over nothing but error tiles gives the first member's error tile as it is where its result is a
# tile (every key of its metadata, so the upstream diagnostic survives), but the BINARY a grouped-agg UDF returns
# has no place for one. The Python API's variants spell it as this prefix and the tile's cell id and metadata
# (JSON); the scalar that wraps the result turns it back into the error tile. No GTiff starts with a NUL byte (II
# or MM), so a payload is never mistaken for a raster. The SQL variants give NULL instead.
_ERROR_MAGIC = b"\x00GBXERR:"


def _only_error_tiles(tiles):
    """The members of a group as error tiles (:func:`_error_tile_of`) when it has nothing else, NULL members
    aside; else an empty list."""
    members = [t for t in tiles if t is not None]
    errors = [_error_tile_of(t) for t in members]
    return errors if members and all(e is not None for e in errors) else []


def _error_tile_of_group(errors):
    """The first of a group's error tiles (`errors`, from :func:`_error_tile_of`), as it is. "First" is
    canonical, not arrival order: the least by cell id, then by the metadata sorted (the heavy tier orders them the
    same way). None for no errors."""

    def order(e):
        md = e.get("metadata") or {}
        cellid = e.get("cellid")
        return (
            -1 if cellid is None else int(cellid),
            "\0".join(f"{k}={v}" for k, v in sorted(md.items())),
        )

    return min(errors, key=order, default=None)


def _error_payload(errors):
    """:data:`_ERROR_MAGIC` payload for a group of nothing but error tiles (`errors`); None for no errors."""
    tile = _error_tile_of_group(errors)
    if tile is None:
        return None
    cellid = tile.get("cellid")
    return _ERROR_MAGIC + json.dumps(
        {
            "cellid": -1 if cellid is None else int(cellid),
            "metadata": dict(tile.get("metadata") or {}),
        }
    ).encode("utf-8")


def _tile_of_error_payload(payload):
    body = json.loads(payload[len(_ERROR_MAGIC) :].decode("utf-8"))
    tile = _serde.build_error_tile("", body["cellid"])
    tile["metadata"] = body["metadata"]
    return tile


# --- scalar as_tile UDFs: wrap an aggregated BINARY result into a tile struct
@f.udf(V2_TILE_SCHEMA)
def _as_tile_udf(raster_bytes):
    if raster_bytes is None:
        return None
    rb = bytes(raster_bytes)
    if len(rb) == 0:
        return None
    if rb.startswith(_ERROR_MAGIC):
        return _tile_of_error_payload(rb)
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
    if rb[8:].startswith(_ERROR_MAGIC):
        return _tile_of_error_payload(rb[8:])
    return _serde.build_tile(rb[8:], "GTiff", cellid)


_as_tile_cellid_envelope_udf = f.udf(V2_TILE_SCHEMA)(_as_tile_cellid_envelope_udf_fn)


# --- grouped-agg pandas_udf(BinaryType()) reducers --------------------------
# Magic marker used by the v2 force-output agg UDF to smuggle a file path
# through the BinaryType() return channel.  GTiff files always start with
# b"II" or b"MM", so a leading null byte unambiguously signals a path payload.
_VTPATH_MAGIC = b"\x00GBXVT:"


def _merge_agg_bytes(tile: pd.Series, error_channel: bool):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    rasters = []
    dropped = 0
    for r in tile:
        candidate = _tile_raster_bytes(r)
        if candidate is None:
            continue
        try:
            with _serde.open_tile(
                candidate, gate=False
            ):  # a validity probe: the decode gates
                pass
            rasters.append(candidate)
        except Exception:  # noqa: BLE001
            dropped += 1
            continue
    if not rasters:
        return _error_payload(_only_error_tiles(tile)) if error_channel else None
    # NOTE: drop-count has no metadata carrier at this layer (pandas_udf returns
    # bare bytes; no struct/metadata assembly here). The skip still stops raising.
    try:
        # the bytes come back whole in one row: bounded by the cap the members are
        return agg_core.merge_tiles(rasters, max_out=agg_core._merge_budget())
    except (agg_core.MosaicTooLarge, agg_core.ReadCapRefused) as exc:
        # Where the result is a tile this group's answer is its error tile. Bytes have no place for a reason and a
        # NULL reads as an empty group, so the aggregate raises with the Fix, never a silent NULL.
        if not error_channel:
            raise
        return _error_payload([_serde.build_error_tile(str(exc), 0)])


@pandas_udf(BinaryType())
def _merge_agg_udf(tile: pd.Series) -> bytes:
    return _merge_agg_bytes(tile, False)


@pandas_udf(BinaryType())
def _merge_agg_tile_udf(tile: pd.Series) -> bytes:
    """The Python API's variant of :func:`_merge_agg_udf`: a group of nothing but error tiles is the error payload."""
    return _merge_agg_bytes(tile, True)


@pandas_udf(BinaryType())
def _merge_agg_v2_udf(
    tile: pd.Series,
    virtualize_dir: pd.Series,
    virtualize_prefix: pd.Series,
) -> bytes:
    """Force-output variant of _merge_agg_udf (Python API only).

    Merges the group's tile rasters exactly like ``_merge_agg_udf``, then
    writes the result to ``<virtualize_dir>/<prefix_>*.tif`` via
    ``open_tile.shape_output`` (FUSE-safe local-temp→copyfile).  Returns the
    destination path encoded as ``_VTPATH_MAGIC + path.encode("utf-8")``; the
    companion scalar UDF ``_as_virtual_tile_from_path_udf`` decodes this back
    into a V2 tile struct with ``raster=None``.

    Every row in a group carries the same broadcast literal for
    ``virtualize_dir`` and ``virtualize_prefix``; we take ``iloc[0]``.
    """
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    vdir = virtualize_dir.iloc[0] if len(virtualize_dir) > 0 else None
    vprefix = virtualize_prefix.iloc[0] if len(virtualize_prefix) > 0 else None

    rasters = []
    for r in tile:
        candidate = _tile_raster_bytes(r)
        if candidate is None:
            continue
        try:
            with _serde.open_tile(
                candidate, gate=False
            ):  # a validity probe: the decode gates
                pass
            rasters.append(candidate)
        except Exception:  # noqa: BLE001
            continue
    if not rasters:
        return _error_payload(_only_error_tiles(tile))

    try:
        merged_bytes = agg_core.merge_tiles(rasters)
    except (agg_core.MosaicTooLarge, agg_core.ReadCapRefused) as exc:
        return _error_payload([_serde.build_error_tile(str(exc), 0)])
    if (
        vdir is None
    ):  # materialize=True alone: the merged bytes, as the natural form returns them
        return bytes(merged_bytes)
    vt = VirtualTile(cellid=0, raster=bytes(merged_bytes))
    shaped = ot.shape_output(vt, virtualize_dir=vdir, virtualize_prefix=vprefix)
    # Encode the path into the BinaryType return channel.
    return _VTPATH_MAGIC + shaped.path.encode("utf-8")


@f.udf(V2_TILE_SCHEMA)
def _as_virtual_tile_from_path_udf(encoded):
    """Decode a ``_merge_agg_v2_udf`` BinaryType result into a V2 tile struct.

    When the bytes start with ``_VTPATH_MAGIC`` the payload is a file path
    and the result is a path-only (bytes-free) virtual tile; with
    ``_ERROR_MAGIC`` it is the error tile of a group of nothing but error
    tiles (see :func:`_error_payload`).  Any other
    non-null payload is treated as raw GTiff bytes (fallback, same as
    ``_as_tile_udf``).
    """
    if encoded is None:
        return None
    eb = bytes(encoded)
    if eb.startswith(_ERROR_MAGIC):
        return _tile_of_error_payload(eb)
    if eb.startswith(_VTPATH_MAGIC):
        path = eb[len(_VTPATH_MAGIC) :].decode("utf-8")
        vt = VirtualTile(cellid=0, path=path)
        return vt.to_row()
    # Fallback: plain raster bytes.
    if len(eb) == 0:
        return None
    return _serde.build_tile(eb, "GTiff", 0)


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
            with _serde.open_tile(
                candidate, gate=False
            ):  # a validity probe: the decode gates
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
        payload = _error_payload(_only_error_tiles(tile))
        return None if payload is None else (0).to_bytes(8, "big") + payload
    # a group the fold cannot cover (a row of the stack over the budget, an encoded result over the cap of a row) RAISES
    # with the Fix: the bytes have no place for a reason, and a NULL reads as an empty group
    out = agg_core.combineavg_tiles(rasters, max_out=agg_core._merge_budget())
    if out is None:
        return None
    # NOTE: drop-count has no metadata carrier at this layer (pandas_udf returns
    # bare bytes; no struct/metadata assembly here). The skip still stops raising.
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
    rasters = []
    dropped = 0
    for r in tile:
        candidate = _tile_raster_bytes(r)
        if candidate is None:
            continue
        try:
            with _serde.open_tile(
                candidate, gate=False
            ):  # a validity probe: the decode gates
                pass
            rasters.append(candidate)
        except Exception:  # noqa: BLE001
            dropped += 1
            continue
    if not rasters:
        return None
    # NOTE: drop-count has no metadata carrier at this layer (pandas_udf returns
    # bare bytes; no struct/metadata assembly here). The skip still stops raising.
    return agg_core.combineavg_tiles(rasters, max_out=agg_core._merge_budget())


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
            with _serde.open_tile(
                candidate, gate=False
            ):  # a validity probe: the decode gates
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
            with _serde.open_tile(
                candidate, gate=False
            ):  # a validity probe: the decode gates
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
        if g is not None
        and not pd.isna(v)  # a NULL value arrives as NaN: the row is skipped
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


def _derivedband_agg_bytes(
    tile: pd.Series, python_func: pd.Series, func_name: pd.Series, error_channel: bool
):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    rasters = []
    dropped = 0
    for r in tile:
        candidate = _tile_raster_bytes(r)
        if candidate is None:
            continue
        try:
            with _serde.open_tile(
                candidate, gate=False
            ):  # a validity probe: the decode gates
                pass
            rasters.append(candidate)
        except Exception:  # noqa: BLE001
            dropped += 1
            continue
    if not rasters:
        return _error_payload(_only_error_tiles(tile)) if error_channel else None
    # NOTE: drop-count has no metadata carrier at this layer (pandas_udf returns
    # bare bytes; no struct/metadata assembly here). The skip still stops raising.
    # pyfunc/func_name are per-group constants; read them from the first row.
    try:
        return agg_core.derivedband_tiles(
            rasters, str(python_func.iloc[0]), str(func_name.iloc[0])
        )
    except (
        agg_core.ReadCapRefused
    ) as exc:  # this group's answer: its error tile where there is a channel, else it raises (never a silent NULL)
        if not error_channel:
            raise
        return _error_payload([_serde.build_error_tile(str(exc), 0)])


@pandas_udf(BinaryType())
def _derivedband_agg_udf(
    tile: pd.Series, python_func: pd.Series, func_name: pd.Series
) -> bytes:
    return _derivedband_agg_bytes(tile, python_func, func_name, False)


@pandas_udf(BinaryType())
def _derivedband_agg_tile_udf(
    tile: pd.Series, python_func: pd.Series, func_name: pd.Series
) -> bytes:
    """The Python API's variant of :func:`_derivedband_agg_udf`: a group of nothing but error tiles is the error payload."""
    return _derivedband_agg_bytes(tile, python_func, func_name, True)


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
    srid: pd.Series = None,
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
        if pd.isna(v) or g is None:  # a NULL value arrives as NaN
            continue
        geom = _parse_geom_elem(g)
        if geom is None or geom.is_empty:
            continue
        xy.append((geom.x, geom.y))
        vals.append(float(v))
    if not xy:
        return None
    # Extent/size/out-CRS/power/max_pts are per-group constants; read from row 0.
    srid0 = None if srid is None else srid.iloc[0]
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
def _binpoints_agg_udf(
    x: pd.Series,
    y: pd.Series,
    z: pd.Series,
    xmin: pd.Series,
    ymin: pd.Series,
    xmax: pd.Series,
    ymax: pd.Series,
    w: pd.Series,
    h: pd.Series,
    srid: pd.Series,
    statistic: pd.Series = None,
) -> bytes:
    """Grouped-agg: stream one (x, y, z) scalar point per row into one raster.

    Returns raw GTiff bytes (BINARY). SQL callers wrap via
    gbx_rst_fromcontent(<agg>, 'GTiff') to recover a tile struct; the Python
    public function rst_binpoints_agg wraps automatically via _as_tile_udf.

    Extent/size/srid/statistic are per-group constants; only the first row's
    value is read.  Returns None for an empty group.
    """
    if len(x) == 0:
        return None
    if any(pd.isna(c.iloc[0]) for c in (xmin, ymin, xmax, ymax, w, h, srid)):
        return None  # a NULL extent, size or srid: no raster
    stat = (
        str(statistic.iloc[0])
        if statistic is not None
        and len(statistic) > 0
        and statistic.iloc[0] is not None
        else "max"
    )
    return binning.bin_points(
        x.to_numpy(dtype="float64"),
        y.to_numpy(dtype="float64"),
        z.to_numpy(dtype="float64"),
        float(xmin.iloc[0]),
        float(ymin.iloc[0]),
        float(xmax.iloc[0]),
        float(ymax.iloc[0]),
        int(w.iloc[0]),
        int(h.iloc[0]),
        int(srid.iloc[0]),
        stat,
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
    srid: pd.Series = None,
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
    srid0 = None if srid is None else srid.iloc[0]
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


_NO_VALUE = object()


def _max_value_per_cell(cells, vals, key=None, typed=True):
    """``{cell: value}`` for the four cell rasterizers, one value per distinct cell.

    The duplicate-cell rule of ``rst_{h3,quadbin,bng,custom}_rasterize_agg``: a cell id
    that repeats keeps its LARGEST value, the same as the heavyweight tier, which sorts
    the group by ``(cellid, value)`` and lets the last write win. Arrival order inside a
    Spark group is not fixed, so "the last row wins" would change the raster from run to
    run; the largest value does not depend on order.

    A null value never wins. A typed Double column delivers a null as ``NaN`` and a real
    ``NaN`` looks the same, so both are read as null: they are ignored whenever the cell
    has a number, and a cell whose every value is null burns ``NaN``. ``typed`` is False
    when the value was omitted (the UDF then receives an object column of ``None``, not a
    float column): that is the presence mask, every cell burns ``1.0``. A typed column
    of nulls is data and follows the rule above.

    ``key`` maps a raw cell id to the id it is stored under (``adapter.to_key``), so two
    spellings of one cell (``TQ3080`` and ``TQ 3080``, or a quadbin id as signed and
    unsigned) are one cell here too. The result is keyed by the raw id first seen.
    """
    best = {}  # stored id -> [raw id, value or None]
    for cell, val in zip(cells, vals):
        k = cell if key is None else key(cell)
        v = None if val is None or val != val else float(val)
        cur = best.get(k, _NO_VALUE)
        if cur is _NO_VALUE:
            best[k] = [cell, v]
        elif v is not None and (cur[1] is None or v > cur[1]):
            cur[1] = v
    if not typed and all(v is None for _, v in best.values()):
        return {raw: 1.0 for raw, _ in best.values()}
    return {raw: float("nan") if v is None else v for raw, v in best.values()}


def _rasterize_agg_body(
    cell_values,
    cells,
    _ad,
    grid_name,
    _srid,
    pixel_size_s,
    xmin_s,
    ymin_s,
    xmax_s,
    ymax_s,
    width_s,
    height_s,
    mode_s,
    kring_pad_s,
):
    """Shared gridspec + burn body for per-grid rasterize_agg UDFs.

    ``cell_values``: {raw_id: float} — raw ids as the UDF received them.
    ``cells``:       [raw_id, ...] — same raw ids (for resolution + gridspec).
    ``_ad``:         per-grid adapter (``_CustomAdapter`` or a singleton from
                     ``cellraster._ADAPTERS``).
    ``grid_name``:   "h3" | "quadbin" | "bng" | "custom" — forwarded to
                     ``compute_gridspec`` and ``cells_to_raster`` as context.
    ``_srid``:       already-resolved output CRS (int SRID or CRS string).
    *_s params:      pandas Series from the UDF (may be None -> use default).

    Returns raw GTiff bytes or None.
    """
    from databricks.labs.gbx.pyrx.core import cellraster as cr

    res = _ad.resolution([_ad.to_key(c) for c in cells])
    _mode = (
        mode_s.iloc[0]
        if mode_s is not None and mode_s.iloc[0] is not None
        else "centroids"
    )
    _kp = (
        int(kring_pad_s.iloc[0])
        if kring_pad_s is not None and kring_pad_s.iloc[0] is not None
        else 1
    )

    def _has(s):
        return s is not None and s.iloc[0] is not None

    if _has(xmin_s) and _has(width_s):
        _gs = (
            float(xmin_s.iloc[0]),
            float(ymin_s.iloc[0]),
            float(xmax_s.iloc[0]),
            float(ymax_s.iloc[0]),
            (float(xmax_s.iloc[0]) - float(xmin_s.iloc[0])) / int(width_s.iloc[0]),
            int(width_s.iloc[0]),
            int(height_s.iloc[0]),
            _srid,
        )
    else:
        _ps = float(pixel_size_s.iloc[0]) if _has(pixel_size_s) else None
        _gs = cr.compute_gridspec(
            cells,
            srid=_srid,
            pixel_size=_ps,
            mode=_mode,
            kring_pad=_kp,
            grid=grid_name,
            _ad=_ad,
        )
    return cr.cells_to_raster(
        cell_values, *_gs, resolution=res, grid=grid_name, _ad=_ad
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
    # A null value arrives as None or np.nan (a typed Double column); the helper reads
    # both as null, and resolves a repeated cell by the id the grid stores it under.
    vals = [None if pd.isna(v) else v for _, v in pairs]
    cell_values = _max_value_per_cell(
        cells, vals, cr._adapter("h3").to_key, typed=value.dtype != object
    )
    # Output CRS spec: out_crs (string) wins over the int srid; else grid-native
    # 4326. cellraster accepts an int SRID or a CRS string interchangeably.
    if out_crs is not None and out_crs.iloc[0] is not None:
        _srid = out_crs.iloc[0]
    elif srid is not None and srid.iloc[0] is not None:
        _srid = int(srid.iloc[0])
    else:
        _srid = 4326
    return _rasterize_agg_body(
        cell_values,
        cells,
        cr._adapter("h3"),
        "h3",
        _srid,
        pixel_size,
        xmin,
        ymin,
        xmax,
        ymax,
        width,
        height,
        mode,
        kring_pad,
    )


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
    # A null value arrives as None or np.nan (a typed Double column); the helper reads
    # both as null, and resolves a repeated cell by the id the grid stores it under.
    vals = [None if pd.isna(v) else v for _, v in pairs]
    cell_values = _max_value_per_cell(
        cells, vals, cr._adapter("quadbin").to_key, typed=value.dtype != object
    )
    # Output CRS spec: out_crs (string) wins over the int srid; else grid-native
    # 4326. cellraster accepts an int SRID or a CRS string interchangeably.
    if out_crs is not None and out_crs.iloc[0] is not None:
        _srid = out_crs.iloc[0]
    elif srid is not None and srid.iloc[0] is not None:
        _srid = int(srid.iloc[0])
    else:
        _srid = 4326
    return _rasterize_agg_body(
        cell_values,
        cells,
        cr._adapter("quadbin"),
        "quadbin",
        _srid,
        pixel_size,
        xmin,
        ymin,
        xmax,
        ymax,
        width,
        height,
        mode,
        kring_pad,
    )


# The refusal of a non-NULL out_crs: the same text on both tiers (the heavy tier's RST_BNG_RasterizeAgg.OutCrsRefusal).
BNG_OUT_CRS_REFUSAL = (
    "gbx_rst_bng_rasterize_agg: out_crs is not supported; the output is always EPSG:27700. "
    "Fix: pass NULL for out_crs, then reproject the tile with gbx_rst_transformcrs."
)


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

    if out_crs is not None and any(pd.notna(v) for v in out_crs):
        raise ValueError(BNG_OUT_CRS_REFUSAL)
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
    # A null value arrives as None or np.nan (a typed Double column); the helper reads
    # both as null, and resolves a repeated cell by the id the grid stores it under.
    vals = [None if pd.isna(v) else v for _, v in pairs]
    cell_values = _max_value_per_cell(
        cells, vals, cr._adapter("bng").to_key, typed=value.dtype != object
    )
    # BNG is EPSG:27700-native; the srid arg is a no-op (forced 27700). The sample
    # points and output raster are both 27700 -- NO WGS84 hop.
    return _rasterize_agg_body(
        cell_values,
        cells,
        cr._adapter("bng"),
        "bng",
        27700,
        pixel_size,
        xmin,
        ymin,
        xmax,
        ymax,
        width,
        height,
        mode,
        kring_pad,
    )


# --- custom rasterize_agg (13-arg: cellid, value, grid struct, then options) ---


@pandas_udf(BinaryType())
def _rst_custom_rasterize_agg_udf(
    cellid: pd.Series,
    value: pd.Series,
    grid: pd.Series,
    out_srid: pd.Series,
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
    """Rasterize custom-grid cells onto a regular pixel grid (centroid burn).

    Arg order (pinned, must match heavy RST_Custom_RasterizeAgg):
        cellid, value, grid, out_srid, pixel_size,
        xmin, ymin, xmax, ymax, width, height, mode, kring_pad  -- 13 args.

    ``grid`` is a CUSTOM_GRID_SCHEMA struct column (produced by
    ``gbx_custom_grid(...)``).  All rows in a group carry the same grid conf;
    only the first row is read.  ``out_srid`` defaults to the grid's ``srid``
    when null; if both are absent (grid.srid==-1 and out_srid is null),
    raises ValueError.
    """
    from databricks.labs.gbx.pygx._custom import conf_from_row
    from databricks.labs.gbx.pyrx import _env
    from databricks.labs.gbx.pyrx.core import cellraster as cr

    _env.configure_gdal_env()
    # Zip cellid and value TOGETHER first so each value stays paired with its own
    # cellid, then drop pairs whose cellid is null.
    # pd.notna guards both Python None and float NaN (PySpark delivers a null Long
    # as float('nan') inside the pandas Series, so `c is not None` is insufficient).
    pairs = [(c, v) for c, v in zip(cellid, value) if pd.notna(c)]
    if not pairs:
        return None
    # Guard: if cellid arrived as float64 (a null-containing BIGINT column causes
    # PySpark/Arrow to upcast the Series), any custom id > 2**53 has already been
    # silently rounded.  The Python wrapper (rst_custom_rasterize_agg) casts to
    # STRING before calling this UDF, so it never triggers.  SQL callers that pass
    # a BIGINT column with nulls should use CAST(cellid AS STRING).
    if pd.api.types.is_float_dtype(cellid) and any(
        abs(float(c)) > 2**53 for c, _ in pairs
    ):
        raise ValueError(
            "rst_custom_rasterize_agg: cellid column contains large custom ids "
            "(> 2**53) but arrived as float64 — a null in a BIGINT cellid column "
            "caused PySpark/Arrow to upcast the Series, silently rounding cell ids.  "
            "Pass CAST(cellid AS STRING) to the SQL UDF, or use the Python "
            "rst_custom_rasterize_agg() wrapper which applies this cast automatically."
        )
    cells = [int(c) for c, _ in pairs]
    # A null value arrives as None or np.nan (a typed Double column); the helper reads
    # both as null. Custom ids are stored as the int they arrive as, so no key is needed.
    vals = [None if pd.isna(v) else v for _, v in pairs]
    cell_values = _max_value_per_cell(cells, vals, typed=value.dtype != object)

    # Extract the grid conf from the first row (per-group constant).
    conf = conf_from_row(grid.iloc[0])

    # Resolve output CRS: out_srid arg wins; else grid's srid; else error.
    if out_srid is not None and out_srid.iloc[0] is not None:
        _srid = int(out_srid.iloc[0])
    elif conf.srid != -1:
        _srid = conf.srid
    else:
        raise ValueError(
            "rst_custom_rasterize_agg: grid has no CRS (srid=-1); "
            "supply out_srid explicitly."
        )
    # Build a per-call custom adapter; fallback_srid makes _reproject a no-op
    # when the grid has srid==-1 and out_srid was supplied.
    _ad = cr._CustomAdapter(conf, fallback_srid=_srid)
    return _rasterize_agg_body(
        cell_values,
        cells,
        _ad,
        "custom",
        _srid,
        pixel_size,
        xmin,
        ymin,
        xmax,
        ymax,
        width,
        height,
        mode,
        kring_pad,
    )


# --- the scalar twins of the cell rasterizers: the group arrives as an array of (cellid, value) structs ------------
# The same answer as the aggregate over the same cells (the rules are `_max_value_per_cell` and `_rasterize_agg_body`),
# for a group that is built in a step of its own (`collect_list(struct(cellid, value))`). A NULL cell id in the array is
# skipped: an array holds the exact ints, so the float64 rounding that makes an aggregate refuse a NULL beside BIGINT ids
# cannot happen here.


def _as_series(v):
    """One per-group constant as the Series the aggregates' bodies read it from."""
    return pd.Series([v], dtype=object)


def _twin_cells(cells):
    """(ids, values) of the array's structs, the NULL cell ids dropped; values stay as they are (None or NaN)."""
    rows = [
        (c["cellid"], c["value"])
        for c in (cells or ())
        if c is not None and c["cellid"] is not None
    ]
    return [r[0] for r in rows], [r[1] for r in rows]


def _twin_burn(
    grid,
    ad,
    key,
    ids,
    values,
    srid,
    pixel_size,
    xmin,
    ymin,
    xmax,
    ymax,
    width,
    height,
    mode,
    kring_pad,
):
    from databricks.labs.gbx.pyrx import _env

    _env.configure_gdal_env()
    vals = [None if v is None or v != v else float(v) for v in values]
    cell_values = _max_value_per_cell(ids, vals, key, typed=True)
    return _rasterize_agg_body(
        cell_values,
        ids,
        ad,
        grid,
        srid,
        *(
            _as_series(v)
            for v in (
                pixel_size,
                xmin,
                ymin,
                xmax,
                ymax,
                width,
                height,
                mode,
                kring_pad,
            )
        ),
    )


@f.udf(BinaryType())
def _rst_bng_rasterize_udf(
    cells,
    out_srid,
    pixel_size,
    xmin,
    ymin,
    xmax,
    ymax,
    width,
    height,
    mode,
    kring_pad,
    out_crs=None,
):
    from databricks.labs.gbx.pyrx.core import cellraster as cr

    if out_crs is not None:
        raise ValueError(BNG_OUT_CRS_REFUSAL.replace("_agg:", ":", 1))
    ids, values = _twin_cells(cells)
    if not ids:
        return None
    ad = cr._adapter("bng")
    return _twin_burn(
        "bng", ad, ad.to_key, [str(c) for c in ids], values, 27700, pixel_size,
        xmin, ymin, xmax, ymax, width, height, mode, kring_pad,
    )  # fmt: skip


def _twin_lonlat(
    grid,
    cells,
    out_srid,
    pixel_size,
    xmin,
    ymin,
    xmax,
    ymax,
    width,
    height,
    mode,
    kring_pad,
    out_crs,
):
    from databricks.labs.gbx.pyrx.core import cellraster as cr

    ids, values = _twin_cells(cells)
    if not ids:
        return None
    ad = cr._adapter(grid)
    # out_crs (a string) wins over the int out_srid; neither: the grid-native EPSG:4326
    srid = (
        out_crs
        if out_crs is not None
        else (int(out_srid) if out_srid is not None else 4326)
    )
    return _twin_burn(
        grid, ad, ad.to_key, [int(c) for c in ids], values, srid, pixel_size,
        xmin, ymin, xmax, ymax, width, height, mode, kring_pad,
    )  # fmt: skip


@f.udf(BinaryType())
def _rst_h3_rasterize_udf(
    cells,
    out_srid,
    pixel_size,
    xmin,
    ymin,
    xmax,
    ymax,
    width,
    height,
    mode,
    kring_pad,
    out_crs=None,
):
    return _twin_lonlat(
        "h3",
        cells,
        out_srid,
        pixel_size,
        xmin,
        ymin,
        xmax,
        ymax,
        width,
        height,
        mode,
        kring_pad,
        out_crs,
    )


@f.udf(BinaryType())
def _rst_quadbin_rasterize_udf(
    cells,
    out_srid,
    pixel_size,
    xmin,
    ymin,
    xmax,
    ymax,
    width,
    height,
    mode,
    kring_pad,
    out_crs=None,
):
    return _twin_lonlat(
        "quadbin",
        cells,
        out_srid,
        pixel_size,
        xmin,
        ymin,
        xmax,
        ymax,
        width,
        height,
        mode,
        kring_pad,
        out_crs,
    )


@f.udf(BinaryType())
def _rst_custom_rasterize_udf(
    cells,
    grid,
    out_srid,
    pixel_size,
    xmin,
    ymin,
    xmax,
    ymax,
    width,
    height,
    mode,
    kring_pad,
):
    from databricks.labs.gbx.pygx._custom import conf_from_row
    from databricks.labs.gbx.pyrx.core import cellraster as cr

    ids, values = _twin_cells(cells)
    if not ids:
        return None
    conf = conf_from_row(grid)
    if out_srid is not None:
        srid = int(out_srid)
    elif conf.srid != -1:
        srid = conf.srid
    else:
        raise ValueError(
            "rst_custom_rasterize: grid has no CRS (srid=-1); supply out_srid explicitly."
        )
    ad = cr._CustomAdapter(conf, fallback_srid=srid)
    return _twin_burn(
        "custom", ad, None, [int(c) for c in ids], values, srid, pixel_size,
        xmin, ymin, xmax, ymax, width, height, mode, kring_pad,
    )  # fmt: skip


# --- the table functions: a grouped merge / rasterize over `TABLE(...) PARTITION BY` --------------------------------
#
# `gbx_rst_merge_tvf` and `gbx_rst_bng_rasterize_tvf` answer what `gbx_rst_merge_agg` and `gbx_rst_bng_rasterize_agg`
# answer, one row per partition, from a table argument a task reads a row at a time. The rasterize keeps only
# cell -> value (bounded by the distinct cells of a partition), so a group of millions of cells never sits in one
# Arrow batch the way an aggregate's group does; the merge holds the partition's tiles, as the aggregate does. A repeated
# cell keeps its largest value, a NULL value (None or NaN) never wins over a number, and a cell of nothing but
# NULLs burns NaN (`_max_value_per_cell`).


def _keep_larger(best, cell, value):
    """Fold one (cell, value) into `best` ({cell: value, or None for null}) by the rule of `_max_value_per_cell`.

    The one place the table function decides what a repeated cell keeps: it sees the group a row at a time.
    """
    v = None if value is None or value != value else float(value)
    cur = best.get(cell, _NO_VALUE)
    if cur is _NO_VALUE or (v is not None and (cur is None or v > cur)):
        best[cell] = v


def _burn_best(best, xmin, ymin, xmax, ymax, width, height):
    """The canvas of `best` ({cell: value or None}) as GTiff bytes: what the aggregate gives for the same cells."""
    ad = cellraster_core._adapter("bng")
    cells = list(best)
    s = _as_series  # _rasterize_agg_body reads .iloc[0]
    return _rasterize_agg_body(
        _max_value_per_cell(cells, [best[c] for c in cells], ad.to_key),
        cells,
        ad,
        "bng",
        27700,
        s(None),
        s(xmin),
        s(ymin),
        s(xmax),
        s(ymax),
        s(width),
        s(height),
        s("centroids"),
        s(0),
    )


def _member_bytes(tile):
    """The raster bytes of a group member, or None for a member a merge drops: an error tile (it failed upstream), an
    empty one, or one that does not open. Not a place to fail: the group still has an answer.
    """
    if _error_tile_of(tile) is not None:
        return None
    candidate = _tile_raster_bytes(tile)  # front-door bytes: a virtual tile is materialized
    if candidate is None:
        return None
    try:
        with _serde.open_tile(
            candidate, gate=False
        ):  # a validity probe: the decode gates
            return candidate
    except Exception:  # noqa: BLE001
        return None


def _table_column(row, fn_name, column, columns):
    """`row[column]`, or a ValueError that names the columns the table argument must have."""
    try:
        return row[column]
    except (KeyError, ValueError, AttributeError):
        raise ValueError(
            f"{fn_name}: the table argument has no column {column!r}. "
            f"Fix: pass TABLE(SELECT {', '.join(columns)} FROM ...) PARTITION BY (key)."
        ) from None


@udtf(returnType="key string, raster binary")
class _RstMergeTvf:
    """`gbx_rst_merge_tvf`: the partition's tiles merged into one mosaic (the table function of `gbx_rst_merge_agg`)."""

    def __init__(self):
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        self.key, self.rasters = None, []

    def eval(self, row):
        self.key = _table_column(row, "gbx_rst_merge_tvf", "key", ("key", "tile"))
        b = _member_bytes(
            _table_column(row, "gbx_rst_merge_tvf", "tile", ("key", "tile"))
        )
        if b is not None:
            self.rasters.append(b)

    def terminate(self):
        if self.rasters:
            # as the aggregate: a group past what a merge can build raises with the Fix, never a silent missing group
            yield self.key, agg_core.merge_tiles(
                self.rasters, max_out=agg_core._merge_budget()
            )


@udtf(returnType="key string, raster binary")
class _RstBngRasterizeTvf:
    """`gbx_rst_bng_rasterize_tvf`: the partition's BNG cells burned onto an explicit canvas in EPSG:27700."""

    _COLUMNS = ("key", "cellid", "value")

    def __init__(self):
        from databricks.labs.gbx.pyrx import _env

        _env.configure_gdal_env()
        self.key, self.best, self.canvas = None, {}, None

    def eval(self, row, xmin, ymin, xmax, ymax, width, height):
        name = "gbx_rst_bng_rasterize_tvf"
        self.key = _table_column(row, name, "key", self._COLUMNS)
        cell = _table_column(row, name, "cellid", self._COLUMNS)
        value = _table_column(row, name, "value", self._COLUMNS)
        if cell is None:
            return
        _keep_larger(self.best, cell, value)
        self.canvas = (xmin, ymin, xmax, ymax, width, height)

    def terminate(self):
        if self.best:
            yield self.key, _burn_best(self.best, *self.canvas)


# --- public Column wrappers (compose grouped-agg BINARY + scalar as_tile) ----
def _merge_agg_forced(tile, virtualize_dir, virtualize_prefix, materialize):
    """The force-output form of ``gbx_rst_merge_agg`` that ``rasterx.functions.rst_merge_agg`` runs when
    ``virtualize_dir``, ``virtualize_prefix`` or ``materialize`` is set: the group's mosaic as a tile struct, written
    under ``virtualize_dir`` (a path-only tile) so a large mosaic never travels as Arrow bytes. The arguments are
    Columns (``_api.force_output`` passes the force-output literals as ``lit``)."""
    return _as_virtual_tile_from_path_udf(
        _merge_agg_v2_udf(tile, virtualize_dir, virtualize_prefix)
    )


def _light_rst_merge_agg(
    tile: ColLike,
    virtualize_dir: Optional[str] = None,
    virtualize_prefix: Optional[str] = None,
) -> Column:
    """The light Column composition of ``gbx_rst_merge_agg`` (the bench's light form and the
    light tests). The Python API is ``databricks.labs.gbx.rasterx.functions.rst_merge_agg``.

    Merge a group's tile rasters into one spatial mosaic tile.

    Use inside ``.agg()``::

        df.groupBy(k).agg(prx._light_rst_merge_agg("tile").alias("merged"))

    Each tile carries its own georef/CRS, so the merge is spatial and the output
    spans the union extent. Returns a tile struct (cellid 0).

    FILE tiles are opened via the FUSE path (``tile.path``) — the aggregator
    treats every tile as EXTERNAL.  Using ``try_to_file`` here would inject a
    nondeterministic expression into the aggregate, which Spark 4 rejects with
    ``AGGREGATE_FUNCTION_WITH_NONDETERMINISTIC_EXPRESSION``.

    Args:
        tile:              Column of tile structs to merge.
        virtualize_dir:    Force-output (light-tier, Python API only): write the
            merged result to a durable path and return a light virtual tile
            (``raster=None``, ``path`` set).  Avoids materialising the full
            merged mosaic as Arrow bytes — essential for large mosaics on
            Serverless where the per-task Arrow budget is ~64 MiB.
        virtualize_prefix: Optional filename prefix inside ``virtualize_dir``.

    Returns:
        Tile struct spanning the union extent, or NULL on an empty group.
        With ``virtualize_dir`` the tile is path-only (bytes-free). A group of
        nothing but error tiles gives the first of them as it is (an error tile:
        a NULL raster and its metadata, ``last_error`` included), also with
        ``virtualize_dir``; the SQL function, which returns BINARY, gives NULL.
        Tiles far apart make a mosaic that is nearly all NoData: past what a
        merge can stream (10 GiB decoded) that group is an error tile whose
        ``last_error`` says so with a ``Fix:`` (NULL from the SQL function);
        the other groups still merge.
    """
    tc = _col(tile)
    if virtualize_dir is not None:
        return _as_virtual_tile_from_path_udf(
            _merge_agg_v2_udf(tc, f.lit(virtualize_dir), f.lit(virtualize_prefix))
        )
    return _as_tile_udf(_merge_agg_tile_udf(tc))


def _light_rst_combineavg_agg(tile: ColLike) -> Column:
    """Per-pixel mean across a group's aligned tiles, ignoring NoData.

    Use inside ``.agg()``::

        df.groupBy(k).agg(prx.rst_combineavg_agg("tile").alias("avg"))

    Assumes the group's tiles are aligned (same shape/extent/CRS); raises if
    shapes differ. The output cellid is the group's first tile cellid. Returns a
    tile struct.

    FILE tiles are opened via the FUSE path (``tile.path``) — the aggregator
    treats every tile as EXTERNAL.  See ``rst_merge_agg`` for the Spark 4
    nondeterminism rationale.
    """
    tc = _col(tile)
    return _as_tile_cellid_envelope_udf(_combineavg_agg_udf(tc))


def _light_rst_frombands_agg(tile: ColLike, band_index: ColLike) -> Column:
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


def _light_rst_rasterize_agg(
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


def _light_rst_derivedband_agg(
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
    return _as_tile_udf(_derivedband_agg_tile_udf(_col(tile), pf, fn))


def _light_rst_gridfrompoints_agg(
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


def _light_rst_binpoints_agg(
    x: ColLike,
    y: ColLike,
    z: ColLike,
    xmin: ColLike,
    ymin: ColLike,
    xmax: ColLike,
    ymax: ColLike,
    width_px: ColLike,
    height_px: ColLike,
    srid: ColLike,
    statistic: ColLike = "max",
) -> Column:
    """Stream one scalar (x, y, z) point per row into a single-band raster tile.

    Groups the stream by the enclosing ``groupBy`` key and bins each group's
    points into a ``width_px × height_px`` raster over the given extent.
    ``xmin``/``ymin``/``xmax``/``ymax``/``width_px``/``height_px``/``srid``/
    ``statistic`` are per-group constants. Equal to running ``rst_binpoints``
    on a pre-collected ARRAY column.

    Use inside ``.agg()``::

        df.groupBy(k).agg(
            prx._light_rst_binpoints_agg(
                "x", "y", "z", xmin, ymin, xmax, ymax, w, h, srid
            ).alias("dsm")
        )

    Returns a tile struct (cellid 0). The raw BINARY form is available through
    the SQL aggregate ``gbx_rst_binpoints_agg``; wrap it with
    ``gbx_rst_fromcontent(<agg>, 'GTiff')`` to recover a tile struct.

    Args:
        x, y:       Scalar DOUBLE columns of point coordinates in CRS of srid.
        z:          Scalar DOUBLE column of point z-values.
        xmin, ymin,
        xmax, ymax: Spatial extent of the output raster in CRS units (constants).
        width_px,
        height_px:  Output raster dimensions in pixels (constants).
        srid:       EPSG code for the output CRS (constant).
        statistic:  One of ``"max"`` (default), ``"min"``, ``"mean"``,
                    ``"median"``, ``"count"``, or ``"percentile:<p>"``.

    Returns:
        Single-band Float32 tile struct (cellid 0).
    """
    stat = f.lit(statistic) if isinstance(statistic, str) else _col(statistic)
    return _as_tile_udf(
        _binpoints_agg_udf(
            _col(x),
            _col(y),
            _col(z),
            _col(xmin),
            _col(ymin),
            _col(xmax),
            _col(ymax),
            _col(width_px),
            _col(height_px),
            _col(srid),
            stat,
        )
    )


def bin_points_tiled(
    df, *, x, y, z, by, xmin, ymin, xmax, ymax, width, height, srid, stat="max"
):
    """Bounded-memory tiled point binning — memory is the raster grid (W×H), not the
    point count. Two-stage native pre-aggregation: per-point pixel index -> native
    groupBy(tile, cell).agg(stat) -> place the <=W*H per-cell values via
    _light_rst_binpoints_agg. Output matches df.groupBy(by).agg(_light_rst_binpoints_agg(x,y,z,...,
    stat)) for max/min/mean/count. median/percentile delegate to that buffered
    aggregator (memory-heavy; use decimate or smaller tiles). Returns [*by, 'tile']."""
    from pyspark.sql import functions as F

    if isinstance(by, str):
        by = [by]
    w, h, sid, st = int(width), int(height), int(srid), str(stat).lower()

    if st == "median" or st.startswith("percentile"):
        return df.groupBy(*by).agg(
            _light_rst_binpoints_agg(
                x, y, z, xmin, ymin, xmax, ymax, F.lit(w), F.lit(h), F.lit(sid), stat
            ).alias("tile")
        )

    xr = F.col(xmax) - F.col(xmin)
    yr = F.col(ymax) - F.col(ymin)
    col = F.floor((F.col(x) - F.col(xmin)) / xr * F.lit(w)).cast("int")
    row = F.floor((F.col(ymax) - F.col(y)) / yr * F.lit(h)).cast("int")
    celled = (
        df.withColumn("_col", col)
        .withColumn("_row", row)
        .where(
            (F.col("_col") >= 0)
            & (F.col("_col") < F.lit(w))
            & (F.col("_row") >= 0)
            & (F.col("_row") < F.lit(h))
        )
        .withColumn("_cx", F.col(xmin) + (F.col("_col") + F.lit(0.5)) * xr / F.lit(w))
        .withColumn("_cy", F.col(ymax) - (F.col("_row") + F.lit(0.5)) * yr / F.lit(h))
    )

    # NULL-z parity with the reference (rst_binpoints / binning.py): count includes
    # every in-bounds point regardless of z, and a NULL z contaminates the cell's
    # mean to NoData (emit NaN, which the placement's fmax skips -> the cell stays
    # NoData). max/min already match — Spark max/min ignore NULL like the reference.
    reducer = {
        "max": F.max(F.col(z)),
        "min": F.min(F.col(z)),
        "mean": F.when(
            F.max(F.col(z).isNull().cast("int")) == 1, F.lit(float("nan"))
        ).otherwise(F.avg(F.col(z))),
        "count": F.count(F.lit(1)).cast("double"),
    }
    if st not in reducer:
        raise ValueError(f"bin_points_tiled: unsupported statistic {stat!r}")
    per_cell = celled.groupBy(*by, xmin, ymin, xmax, ymax, "_cx", "_cy").agg(
        reducer[st].alias("_v")
    )

    return per_cell.groupBy(*by).agg(
        _light_rst_binpoints_agg(
            "_cx",
            "_cy",
            "_v",
            xmin,
            ymin,
            xmax,
            ymax,
            F.lit(w),
            F.lit(h),
            F.lit(sid),
            "max",
        ).alias("tile")
    )


def _light_rst_dtmfromgeoms_agg(
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


def _light_rst_h3_rasterize_agg(
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


def _light_rst_quadbin_rasterize_agg(
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


def _light_rst_bng_rasterize_agg(
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
    """The light Column composition of ``gbx_rst_bng_rasterize_agg`` (the bench's light form and the
    light tests). The Python API is ``databricks.labs.gbx.rasterx.functions.rst_bng_rasterize_agg``.

    Rasterize a group's BNG cells into one tile (pixel-centroid burn).

    ``cellid`` is a STRING column of BNG cell ids (e.g. ``"TQ3080"``); each id is
    parsed via ``pygx._bng`` (single source of truth). ``value`` omitted ->
    presence mask (1.0/NoData). Supply an explicit extent (xmin..height) for
    aligned band stacking; else the grid is auto-derived per ``mode``/``kring_pad``.
    Use inside ``.agg()``::

        df.groupBy(k).agg(prx._light_rst_bng_rasterize_agg("cellid").alias("tile"))

    BNG is EPSG:27700-native (British National Grid): the output raster is always
    EPSG:27700, and ``out_srid`` is not read. A non-NULL ``out_crs`` is refused on
    both tiers: pass NULL, then reproject with ``rst_transformcrs``.

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


def _light_rst_custom_rasterize_agg(
    cellid: ColLike,
    grid: ColLike,
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
) -> Column:
    """Rasterize a group's custom-grid cells into one tile (pixel-centroid burn).

    ``grid`` is a CUSTOM_GRID_SCHEMA struct column (produced by
    ``gbx_custom_grid(...)``).  ``value`` omitted -> presence mask
    (1.0/NoData).  Supply an explicit extent (``xmin`` .. ``height``) for
    aligned band stacking; else the grid is auto-derived per
    ``mode``/``kring_pad``.  Output CRS: ``out_srid`` wins; falls back to the
    grid's own ``srid`` when not supplied.  Use inside ``.agg()``::

        df.groupBy(k).agg(prx.rst_custom_rasterize_agg("cellid", "grid").alias("tile"))

    SQL returns BINARY (the raw grouped-agg UDF); Python returns a tile struct
    (wrapped by ``_as_tile_udf``).
    """

    def _c(x, default):
        return _col(x) if x is not None else f.lit(default)

    _cellid_col = _col(cellid)
    if isinstance(_cellid_col, str):
        _cellid_col = f.col(_cellid_col)
    return _as_tile_udf(
        _rst_custom_rasterize_agg_udf(
            _cellid_col.cast("string"),
            _c(value, None),
            _col(grid),
            _c(out_srid, None),
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
    from databricks.labs.gbx.core.crs import authority_srid_of

    _epsg = authority_srid_of(_resolved)
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
    "gbx_rst_combinemin": _combinemin_udf,
    "gbx_rst_combinemax": _combinemax_udf,
    "gbx_rst_combinesum": _combinesum_udf,
    "gbx_rst_combinecount": _combinecount_udf,
    "gbx_rst_combinemedian": _combinemedian_udf,
    "gbx_rst_combinestddev": _combinestddev_udf,
    "gbx_rst_align_to": _align_to_udf,
    "gbx_rst_chm": _chm_udf,
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
    "gbx_rst_isoband": _isoband_udf,
    "gbx_rst_viewshed": _viewshed_udf,
    "gbx_rst_cog_convert": _cog_convert_udf,
    "gbx_rst_fillnodata": _fillnodata_udf,
    "gbx_rst_rasterize": _rasterize_udf,
    "gbx_rst_gridfrompoints": _gridfrompoints_udf,
    "gbx_rst_dtmfromgeoms": _dtmfromgeoms_udf,
    "gbx_rst_binpoints": _binpoints_udf,
    # gbx_rst_polygonize is a UDTF registered separately in register() via
    # spark.udtf.register — UDTFs cannot go through spark.udf.register.
    # gbx_rst_{h3,quadbin}_rastertogrid* are UDTFs registered separately in
    # register() via spark.udtf.register — UDTFs cannot go through spark.udf.register.
    "gbx_rst_ndvi": _ndvi_udf,
    "gbx_rst_ndwi": _ndwi_udf,
    "gbx_rst_nbr": _nbr_udf,
    "gbx_rst_savi": _savi_udf,
    "gbx_rst_evi": _evi_udf,
    "gbx_rst_percentile_stretch": _percentile_stretch_udf,
    # light-only classifier (rst_ pixel classification; no Scala/heavy equivalent)
    "gbx_rst_land_cover": _land_cover_udf,
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
    "gbx_rst_binpoints_agg": _binpoints_agg_udf,
    "gbx_rst_h3_rasterize_agg": _rst_h3_rasterize_agg_udf,
    "gbx_rst_quadbin_rasterize_agg": _rst_quadbin_rasterize_agg_udf,
    "gbx_rst_bng_rasterize_agg": _rst_bng_rasterize_agg_udf,
    "gbx_rst_custom_rasterize_agg": _rst_custom_rasterize_agg_udf,
}

SQL_REGISTRY = {**_sql_accessors, **_sql_tile_ops, **_sql_aggregators}

# A Column-API call on a module-level UDF / UDTF builds its column on the driver, where the session's read cap is known
# (a standard-access worker cannot work it out itself): each call carries the cap to the workers, as a registration does.
# Last, so the registries above hold the plain objects, which a registrar carries through ``carrying_session``.
_gdal_policy.carry_read_cap_in_column_api(globals())
