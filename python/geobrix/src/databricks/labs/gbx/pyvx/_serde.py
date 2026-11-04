"""Geometry + attribute marshalling for pyvx MVT encoding (Spark-free)."""

# Before the pyspark imports: one clear error when PySpark is missing.
from databricks.labs.gbx import _pyspark  # noqa: F401

# isort: split

from pyspark.sql.types import BinaryType, IntegerType, StructField, StructType

# Output tile struct, identical to the heavy generator's row shape.
TILE_SCHEMA = StructType(
    [
        StructField("z", IntegerType(), False),
        StructField("x", IntegerType(), False),
        StructField("y", IntegerType(), False),
        StructField("mvt_bytes", BinaryType(), True),
    ]
)

# TIN triangulate output: one 2D-WKB triangle (Polygon) per row.
TRIANGLE_SCHEMA = StructType([StructField("triangle", BinaryType(), False)])

# Elevation/interpolation output: one WKB elevation point per row.
ELEVATION_SCHEMA = StructType([StructField("elevation_point", BinaryType(), False)])

from databricks.labs.gbx.pyvx._props import _NATIVE, to_native_props  # noqa: E402,F401
