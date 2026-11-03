"""
VectorX Function Reference Examples (heavy Python tier).

Heavy-Python examples for all 9 VectorX functions: CRS stamping and reprojection
(st_setcrs, st_transformcrs, st_crs), vector-tile encoding (st_asmvt,
st_asmvt_pyramid), TIN elevation surface modeling (st_triangulate,
st_interpolateelevationbbox, st_interpolateelevationgeom), and legacy Mosaic
geometry migration (st_legacyaswkb).
Used in docs/docs/api/vectorx-functions.mdx. Tested by test_vectorx_functions.py.
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


# ---------------------------------------------------------------------------
# CRS family — heavy (vectorx) Python examples
# Fixture: ``vector_geoms`` view — 1 row: geom STRING = 'SRID=4326;POINT (13 42)'
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Legacy Mosaic conversion family — heavy (vectorx.jts.legacy) Python examples
# Fixture: ``legacy_geoms`` view — 1 row: geom_legacy STRUCT (Mosaic InternalGeometry)
# Encodes POINT(13, 42): typeId=1, srid=0, boundaries=[[[13.0, 42.0]]], holes=[].
# Import path (heavy): databricks.labs.gbx.vectorx.jts.legacy.functions
# Import path (light): databricks.labs.gbx.pyvx.functions (same function, same output)
# ---------------------------------------------------------------------------

