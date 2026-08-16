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
# TIN family — heavy (vectorx) Python examples
# Fixture: ``tin_survey`` view — 1 row: pts ARRAY<BINARY> (4 WKB POINT Z),
#          bl ARRAY<BINARY> (empty). The 4 points form a 10×10 m square:
#          (0,0,0), (10,0,0), (10,10,10), (0,10,5) → 2 Delaunay triangles.
# ---------------------------------------------------------------------------

# WKB POINT(0, 10) — plain 2D, used as grid_origin for st_interpolateelevationgeom.
# Little-endian: 01 01000000 0000000000000000 0000000000002440
_WKB_POINT_0_10 = "010100000000000000000000000000000000002440"


def st_triangulate_python_heavy_example(spark):
    """Build a Delaunay TIN and emit one triangle polygon per row (heavy vectorx tier).

    Reads the ``tin_survey`` setup view (4 POINT Z corners of a 10×10 m square).
    Constrained Delaunay triangulation of 4 points produces 2 triangle polygons.
    The generator Column is available in the heavyweight tier only; in the
    lightweight tier call the registered UDTF via SQL ``LATERAL`` instead.
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.vectorx import functions as vx  # noqa: PLC0415

    df = spark.table("tin_survey")
    result = df.select(
        vx.st_triangulate(
            f.col("pts"), f.col("bl"),
            f.lit(0), f.lit(0), f.lit("NONENCROACHING"),
            "constrained",
        ).alias("triangle")
    )
    rows = result.collect()
    return rows[0]["triangle"] if rows else None


st_triangulate_python_heavy_example_output = """
+--------+
|triangle|
+--------+
|[binary]|
|[binary]|
+--------+
... (WKB binary — 2 Delaunay triangle polygons)
"""


def st_interpolateelevationbbox_python_heavy_example(spark):
    """Sample TIN elevation on a regular bounding-box grid (heavy vectorx tier).

    Reads the ``tin_survey`` setup view and interpolates elevation on a 3×3 grid
    spanning the full 10×10 m extent (SRID=0).  All 9 cell centres fall inside
    the TIN convex hull, so the generator emits 9 ``STRUCT<elevation_point BINARY>``
    rows — one WKB POINT Z per cell.
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.vectorx import functions as vx  # noqa: PLC0415

    df = spark.table("tin_survey")
    result = df.select(
        vx.st_interpolateelevationbbox(
            f.col("pts"), f.col("bl"),
            f.lit(0), f.lit(0), f.lit("NONENCROACHING"),
            f.lit(0), f.lit(0), f.lit(10), f.lit(10),
            f.lit(3), f.lit(3), f.lit(0),
            "constrained",
        ).alias("elevation_point")
    )
    rows = result.collect()
    return rows[0]["elevation_point"] if rows else None


st_interpolateelevationbbox_python_heavy_example_output = """
+---------------+
|elevation_point|
+---------------+
|[binary]       |
|[binary]       |
|...            |
+---------------+
... (WKB binary — POINT Z geometries, 3×3 elevation grid, 9 rows)
"""


def st_interpolateelevationgeom_python_heavy_example(spark):
    """Sample TIN elevation on a grid anchored to a geometry origin (heavy vectorx tier).

    Reads the ``tin_survey`` setup view and interpolates elevation on a 3×3 grid
    anchored to POINT(0, 10) — top-left corner of the TIN extent.  Cell sizes are
    3.0 m × 3.0 m (negative Y steps downward per raster convention).  The grid
    origin is added as a derived ``origin`` column from a WKB literal.  All 9 cell
    centres fall inside the TIN hull; output SRID is 0 (plain WKB origin).
    """
    from pyspark.sql import functions as f  # noqa: PLC0415
    from databricks.labs.gbx.vectorx import functions as vx  # noqa: PLC0415

    df = spark.table("tin_survey").withColumn(
        "origin", f.expr(f"unhex('{_WKB_POINT_0_10}')")
    )
    result = df.select(
        vx.st_interpolateelevationgeom(
            f.col("pts"), f.col("bl"),
            f.lit(0), f.lit(0), f.lit("NONENCROACHING"),
            f.col("origin"), f.lit(3), f.lit(3), f.lit(3), f.lit(-3),
            "constrained",
        ).alias("elevation_point")
    )
    rows = result.collect()
    return rows[0]["elevation_point"] if rows else None


st_interpolateelevationgeom_python_heavy_example_output = """
+---------------+
|elevation_point|
+---------------+
|[binary]       |
|[binary]       |
|...            |
+---------------+
... (WKB binary — POINT Z geometries, 3×3 origin-anchored grid, 9 rows)
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
