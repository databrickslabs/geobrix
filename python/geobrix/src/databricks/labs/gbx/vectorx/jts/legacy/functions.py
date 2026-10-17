"""VectorX JTS legacy Python API.

Thin wrappers for legacy (Mosaic-compatible) vector functions. Register with
the appropriate module then use on geometry columns. For full descriptions
and examples, see the API docs or SQL:
  DESCRIBE FUNCTION EXTENDED gbx_st_legacyaswkb;

Arg types: every wrapper accepts either a pyspark ``Column`` or a plain
Python scalar. Non-string scalars (``bool``/``int``/``float``/``bytes``) are
auto-wrapped with ``f.lit(...)``; strings and ``Column`` values pass through
(pyspark treats a bare string as a dataframe column reference). Wrap in
``f.lit(...)`` to pass a string literal.
"""

# Before the pyspark imports: one clear error when PySpark is missing.
from databricks.labs.gbx import _pyspark  # noqa: F401

# isort: split

from typing import Union

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as f

ColLike = Union[Column, str, bool, int, float, bytes]


def _col(x: ColLike) -> Union[Column, str]:
    """Auto-wrap bool/int/float/bytes scalars via f.lit(); pass strings and Columns through."""
    if isinstance(x, Column) or isinstance(x, str):
        return x
    return f.lit(x)


def register(spark: SparkSession = None) -> None:
    """Register VectorX JTS legacy functions with the Spark session.

    Call once so that gbx_st_legacyaswkb (and related) SQL functions are
    available. Uses the active Spark session if not provided.

    Args:
        spark: Spark session (optional; uses active session if not provided).
    """
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    spark.read.format("register_ds").option(
        "functions", "vectorx.jts.legacy"
    ).load().collect()


def st_legacyaswkb(geom: ColLike) -> Column:
    """Return the legacy vector geometry as Well-Known Binary.

    Converts the internal legacy geometry format (e.g. from Mosaic) to WKB
    for use with GeoBrix or other tools that expect WKB.

    Args:
        geom: Legacy geometry column (internal format).

    Returns:
        Column of WKB (binary).
    """
    return f.call_function("gbx_st_legacyaswkb", _col(geom))
