"""VectorX JTS legacy Python API.

Thin wrappers for legacy (Mosaic-compatible) vector functions. Register with
the appropriate module then use on geometry columns. For full descriptions
and examples, see the API docs or SQL:
  DESCRIBE FUNCTION EXTENDED gbx_st_legacyaswkb;
"""

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as f


def register(_spark: SparkSession) -> None:
    """Register VectorX JTS legacy functions with the Spark session.

    Call once so that gbx_st_legacyaswkb (and related) SQL functions are
    available. Uses the active Spark session if needed.

    Args:
        _spark: Spark session (optional; uses active session if not provided).
    """
    _spark = SparkSession.builder.getOrCreate()
    _spark.read.format("register_ds").option(
        "functions", "vectorx.jts.legacy"
    ).load().collect()


def st_legacyaswkb(geom: Column) -> Column:
    """Return the legacy vector geometry as Well-Known Binary.

    Converts the internal legacy geometry format (e.g. from Mosaic) to WKB
    for use with GeoBrix or other tools that expect WKB.

    Args:
        geom: Legacy geometry column (internal format).

    Returns:
        Column of WKB (binary).
    """
    return f.call_function("gbx_st_legacyaswkb", geom)
