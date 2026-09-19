"""H3 geometry-aware kring/kloop and H3 aggregators — GridX H3 public API.

The geometry-aware functions (geomkring/geomkloop) are light-tier only: they run
in pure Python via the ``h3`` library and have no Scala/heavy equivalent.

``h3_cellfill`` is a grouped aggregator available on both tiers. The heavy Scala
tier returns ``ARRAY<STRUCT<cellid BIGINT, value DOUBLE>>``; the lightweight
pygx tier returns ``BINARY`` (see :func:`h3_cellfill` for the divergence note).

    from databricks.labs.gbx.gridx.h3 import functions as hx

    hx.register(spark)  # registers heavy H3_CellFill from the JAR
    df.groupBy("region").agg(hx.h3_cellfill("cellid", "value"))

For geometry-aware kring/kloop, ``register`` (pygx) is sufficient::

    from databricks.labs.gbx.pygx.functions import register
    from databricks.labs.gbx.gridx.h3.functions import geomkring

    register(spark)
    df.withColumn("kring", geomkring("geom_col", resolution=9, k=1))
"""

from typing import Union

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F

from databricks.labs.gbx.pygx import functions as _pygx

ColLike = Union[Column, str, bool, int, float, bytes]


def register(_spark: SparkSession) -> None:
    """Register GeoBrix H3 functions (heavy tier) with the Spark session.

    Loads ``gbx_h3_cellfill`` from the GeoBrix JAR. Call once per session
    before using :func:`h3_cellfill` with the heavy tier.

    Args:
        _spark: Active Spark session.
    """
    _spark = SparkSession.builder.getOrCreate()
    _spark.read.format("register_ds").option("functions", "gridx.h3").load().collect()


def _geom(x: Union[str, Column]) -> Column:
    """A bare string is a column NAME here (per the documented usage); wrap it."""
    return F.col(x) if isinstance(x, str) else x


def _col(x: ColLike) -> Column:
    """Auto-promote scalars via F.lit(); strings stay as column references."""
    if isinstance(x, Column):
        return x
    if isinstance(x, str):
        return F.col(x)
    return F.lit(x)


def h3_cellfill(
    cellid: ColLike,
    value: ColLike,
    k: ColLike = 1,
    method: ColLike = "mean",
    power: ColLike = 2.0,
) -> Column:
    """Grouped aggregator: fill NULL H3 cells from valid neighbours (heavy h3 tier).

    Use with ``groupBy(...).agg(hx.h3_cellfill(...))`` to interpolate missing
    values. Returns ``ARRAY<STRUCT<cellid BIGINT, value DOUBLE>>``.

    Args:
        cellid: Column of BIGINT H3 cell ids.
        value: Column of DOUBLE values (NULL marks cells to be filled).
        k: Neighbour ring radius (default ``1``).
        method: Interpolation method — ``'mean'`` (default) or ``'idw'``.
        power: IDW power parameter (default ``2.0``; ignored for ``'mean'``).

    Returns:
        Column of ``ARRAY<STRUCT<cellid BIGINT, value DOUBLE>>``.
    """
    _method = F.lit(method) if isinstance(method, str) else _col(method)
    return F.call_function(
        "gbx_h3_cellfill",
        _col(cellid),
        _col(value),
        _col(k),
        _method,
        _col(power),
    )


def geomkring(
    geom_col: Union[str, Column],
    resolution: int,
    k: Union[int, ColLike],
    mode: str = "boundary-out",
) -> Column:
    """ARRAY<BIGINT> H3 geometry-aware k-ring (filled disk) from a geometry column.

    Args:
        geom_col:   Geometry column (WKB BINARY or WKT STRING).
        resolution: H3 resolution (0..15).
        k:          Ring distance (0 = covering set only).
        mode:       Dilation mode (default "boundary-out"); one of the 6 modes.

    Returns:
        Column of ARRAY<BIGINT> H3 cell ids.
    """
    return _pygx.h3_geomkring(_geom(geom_col), resolution, k, mode)


def geomkloop(
    geom_col: Union[str, Column],
    resolution: int,
    k: Union[int, ColLike],
    mode: str = "boundary-out",
) -> Column:
    """ARRAY<BIGINT> H3 geometry-aware k-loop (hollow shell at exactly k steps).

    See :func:`geomkring` for parameters.
    """
    return _pygx.h3_geomkloop(_geom(geom_col), resolution, k, mode)
