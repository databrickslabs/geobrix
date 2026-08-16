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

    return StructType(
        [
            StructField("typeId", IntegerType()),
            StructField("srid", IntegerType()),
            StructField("boundaries", ArrayType(ArrayType(ArrayType(DoubleType())))),
            StructField(
                "holes", ArrayType(ArrayType(ArrayType(ArrayType(DoubleType()))))
            ),
        ]
    )


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


# ---------------------------------------------------------------------------
# Vector-tile family — heavy (vectorx) Python examples
# Fixture: ``mvt_features`` view — 2 rows: tile-local WKB POINTs in (z=0,x=0,y=0)
# ---------------------------------------------------------------------------

_WKB_POINT_0_0 = "010100000000000000000000000000000000000000"


def st_asmvt_python_heavy_example(spark):
    """Aggregate tile-local features into an MVT protobuf blob per tile (heavy vectorx tier)."""
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.vectorx import functions as vx  # noqa: PLC0415

    df = spark.table("mvt_features")
    result = df.groupBy("z", "x", "y").agg(
        vx.st_asmvt(f.col("geom_wkb"), f.col("attrs"), f.lit("layer")).alias("mvt")
    )
    row = result.first()
    return row["mvt"]


st_asmvt_python_heavy_example_output = """
+---+---+---+---------+
|  z|  x|  y|      mvt|
+---+---+---+---------+
|  0|  0|  0|[binary] |
+---+---+---+---------+
... (MVT binary)
"""


def st_asmvt_pyramid_python_heavy_example(spark):
    """Explode a WGS-84 feature into per-tile MVT rows using the Generator Column API (heavy tier).

    Creates a single-row DataFrame with WGS-84 POINT(0, 0), then invokes
    ``st_asmvt_pyramid`` as a generator Column.  The generator returns one struct
    row per intersecting tile; ``selectExpr`` flattens the struct fields into
    individual columns for readability.
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.vectorx import functions as vx  # noqa: PLC0415

    df = spark.sql(f"""
        SELECT unhex('{_WKB_POINT_0_0}') AS geom_wkb,
               named_struct('name', 'origin', 'id', 1L) AS attrs
        """)
    result = df.select(
        vx.st_asmvt_pyramid(
            f.col("geom_wkb"), f.col("attrs"), f.lit(0), f.lit(2), f.lit("layer")
        ).alias("t")
    ).selectExpr("t.z AS z", "t.x AS x", "t.y AS y", "t.mvt_bytes AS mvt_bytes")
    rows = result.collect()
    return rows[0]["mvt_bytes"] if rows else None


st_asmvt_pyramid_python_heavy_example_output = """
+---+---+---+-----------+
|  z|  x|  y|  mvt_bytes|
+---+---+---+-----------+
|  0|  0|  0|  [binary] |
|  1|  1|  1|  [binary] |
|  2|  2|  2|  [binary] |
+---+---+---+-----------+
... (MVT binary — one row per intersecting tile across zoom levels 0–2)
"""


# ---------------------------------------------------------------------------
# CRS family — heavy (vectorx) Python examples
# Fixture: ``vector_geoms`` view — 1 row: geom STRING = 'SRID=4326;POINT (13 42)'
# ---------------------------------------------------------------------------


def st_crs_python_heavy_example(spark):
    """Return the canonical CRS string for a geometry's embedded SRID (heavy Python)."""
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.vectorx import functions as vx  # noqa: PLC0415

    df = spark.table("vector_geoms")
    result = df.select(vx.st_crs(f.col("geom")).alias("crs")).first()
    return result["crs"]


st_crs_python_heavy_example_output = """
+---------+
|crs      |
+---------+
|EPSG:4326|
+---------+
"""


def st_setcrs_python_heavy_example(spark):
    """Stamp a CRS on a geometry without reprojecting (heavy Python)."""
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.vectorx import functions as vx  # noqa: PLC0415

    df = spark.table("vector_geoms")
    result = df.select(
        vx.st_setcrs(f.col("geom"), "EPSG:4326").alias("stamped")
    ).first()
    return result["stamped"]


st_setcrs_python_heavy_example_output = """
+---------+
|stamped  |
+---------+
|[binary] |
+---------+
(EWKB binary — coordinates preserved, SRID=4326 embedded)
"""


def st_transformcrs_python_heavy_example(spark):
    """Reproject a geometry to a target CRS (heavy Python)."""
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.vectorx import functions as vx  # noqa: PLC0415

    df = spark.table("vector_geoms")
    result = df.select(
        vx.st_transformcrs(f.col("geom"), "EPSG:32633").alias("utm33n")
    ).first()
    return result["utm33n"]


st_transformcrs_python_heavy_example_output = """
+--------+
|utm33n  |
+--------+
|[binary]|
+--------+
(EWKB binary — POINT(13, 42) reprojected from EPSG:4326 to EPSG:32633)
"""
