"""Arrow-vectorized UDF harness lowering Spark-free core fns to Columns.

Each public rst_* function extracts the BINARY ``raster`` subfield from the tile
struct column and feeds it (plus any scalar args) to a pandas_udf that opens the
bytes with rasterio and calls a core function. Returning a Column preserves the
one-line swap contract with the heavyweight rasterx wrappers.
"""

from typing import Callable, Union

import pandas as pd
from pyspark.sql import Column
from pyspark.sql import functions as f
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DataType

from databricks.labs.gbx.pyrx import _env, _serde

ColLike = Union[Column, str, bool, int, float, bytes]


def _col(x: ColLike) -> Union[Column, str]:
    """Mirror rasterx: auto-wrap bool/int/float/bytes; pass str/Column through.

    The ``str`` passthrough is deliberate — it is how ``rst_avg("tile")`` names a column.
    For CRS-shaped arguments that reading is wrong; use :func:`_crs_col` there.
    """
    if isinstance(x, (Column, str)):
        return x
    return f.lit(x)


def _crs_col(x: ColLike) -> Column:
    """Coerce a CRS argument: plain str -> ``f.lit`` (CRS literal, NOT a column name).

    A CRS descriptor is itself a string, so ``_col`` would hand ``"EPSG:4326"`` to Spark as a
    column reference and the plan would die with
    ``UNRESOLVED_COLUMN.WITH_SUGGESTION: ... name `EPSG:4326` cannot be resolved`` — while the
    identical heavy-tier call succeeds, because heavy's ``String`` overloads ``lit()``-wrap.
    A bare literal is the overwhelmingly common way to name a CRS, so it must be the shape
    that works. Mirrors ``pyvx.functions._crs_col``.

    Pass a ``Column`` (e.g. ``f.col("crs_column")``) to read the CRS per row; ``f.lit`` returns
    a ``Column`` unchanged, so wrapping an already-lifted value stays correct.
    """
    if isinstance(x, str):
        return f.lit(x)
    return _col(x)


def _raster_field(tile: ColLike) -> Column:
    """Resolve a tile arg to its BINARY ``raster`` subfield Column."""
    c = f.col(tile) if isinstance(tile, str) else tile
    return c.getField("raster")


def tile_scalar_udf(core_fn: Callable, return_type: DataType):
    """Build a pandas_udf: (raster: Series[bytes]) -> Series, calling core_fn(ds)."""

    @pandas_udf(return_type)
    def _udf(raster: pd.Series) -> pd.Series:
        _env.configure_gdal_env()  # runs on the worker process
        out = []
        # Per-row loop: rasterio MemoryFile open is inherently per-raster; the
        # Arrow batch still crosses the JVM<->Python boundary once per batch.
        for b in raster:
            if b is None:
                out.append(None)
                continue
            with _serde.open_tile(bytes(b)) as ds:
                out.append(core_fn(ds))
        return pd.Series(out, dtype="object")

    return _udf


def tile_scalar_udf2(core_fn: Callable, return_type: DataType):
    """Build a pandas_udf: (raster, a, b) -> Series, calling core_fn(ds, a, b)."""

    @pandas_udf(return_type)
    def _udf(raster: pd.Series, a: pd.Series, b: pd.Series) -> pd.Series:
        _env.configure_gdal_env()
        out = []
        # Per-row loop: rasterio MemoryFile open is inherently per-raster; the
        # Arrow batch still crosses the JVM<->Python boundary once per batch.
        for rb, av, bv in zip(raster, a, b):
            if rb is None:
                out.append(None)
                continue
            with _serde.open_tile(bytes(rb)) as ds:
                out.append(core_fn(ds, av, bv))
        return pd.Series(out, dtype="object")

    return _udf


def sql_scalar_udf(core_fn: Callable, return_type: DataType):
    """Regular @f.udf taking a tile struct -> scalar via core_fn(ds). For SQL registration.

    Unlike the pandas_udf path (which takes the raw raster bytes subfield), this
    UDF accepts the full tile struct Row, extracts the ``raster`` bytes, and calls
    core_fn on the opened DatasetReader. Used only for spark.udf.register()
    entries; the Python Column API still goes through the pandas_udf path.
    """

    @f.udf(return_type)
    def _udf(tile):
        if tile is None or tile["raster"] is None:
            return None
        _env.configure_gdal_env()
        with _serde.open_tile(bytes(tile["raster"])) as ds:
            return core_fn(ds)

    return _udf


def sql_scalar_udf2(core_fn: Callable, return_type: DataType):
    """Regular @f.udf taking (tile struct, a, b) -> scalar via core_fn(ds, a, b).

    Struct-accepting counterpart to tile_scalar_udf2, for SQL registration.
    """

    @f.udf(return_type)
    def _udf(tile, a, b):
        if tile is None or tile["raster"] is None:
            return None
        _env.configure_gdal_env()
        with _serde.open_tile(bytes(tile["raster"])) as ds:
            return core_fn(ds, a, b)

    return _udf
