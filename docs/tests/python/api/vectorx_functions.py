"""
VectorX Function Reference Examples (minimal).

Single example: convert legacy Mosaic geometry struct to WKB.
Used in docs/docs/api/vectorx-functions.mdx. Tested by test_vectorx_functions.py.
Legacy format matches InternalGeometry: typeId, srid, boundaries, holes.
"""

try:
    from pyspark.sql import SparkSession
except ImportError:
    SparkSession = None

try:
    from databricks.labs.gbx.vectorx.jts.legacy import functions as vx
except ImportError:
    vx = None


# ============================================================================
# COMMON SETUP
# ============================================================================

def vectorx_setup_example(spark):
    """Common setup: register VectorX (legacy geometry). Run once before examples."""
    from databricks.labs.gbx.vectorx.jts.legacy import functions as vx
    vx.register(spark)
    return None


vectorx_setup_example_output = """
VectorX registered. You can now use st_legacyaswkb in Python and gbx_st_legacyaswkb in SQL.
"""


def _legacy_point_struct_schema():
    """Schema for legacy point (InternalGeometry: typeId, srid, boundaries, holes)."""
    from pyspark.sql.types import (
        ArrayType,
        DoubleType,
        IntegerType,
        StructField,
        StructType,
    )
    return StructType([
        StructField("typeId", IntegerType()),
        StructField("srid", IntegerType()),
        StructField("boundaries", ArrayType(ArrayType(ArrayType(DoubleType())))),
        StructField("holes", ArrayType(ArrayType(ArrayType(ArrayType(DoubleType()))))),
    ])


def st_legacyaswkb_python_example(spark):
    """Convert a legacy point geometry to WKB (single row). Requires vectorx_setup_example first."""
    from pyspark.sql import Row
    from pyspark.sql.types import StructField, StructType

    # Point (30, 10): typeId=1 (POINT), srid=0, boundaries=[[[30.0, 10.0]]], holes=[]
    legacy_schema = _legacy_point_struct_schema()
    schema = StructType([StructField("geom_legacy", legacy_schema)])
    row = Row(geom_legacy=(1, 0, [[[30.0, 10.0]]], []))
    shapes = spark.createDataFrame([row], schema)
    shapes.select(vx.st_legacyaswkb("geom_legacy").alias("wkb")).show()
    return shapes.select(vx.st_legacyaswkb("geom_legacy").alias("wkb"))


st_legacyaswkb_python_example_output = """
+-----------+
|wkb        |
+-----------+
|[BINARY]   |
+-----------+
"""

# SQL example (after registering VectorX and creating a table with geom_legacy column)
ST_LEGACYASWKB_SQL_EXAMPLE = """
SELECT gbx_st_legacyaswkb(geom_legacy) AS wkb FROM legacy_table;
"""

ST_LEGACYASWKB_SQL_EXAMPLE_output = """
One row per input legacy geometry; wkb column contains binary WKB.
"""
