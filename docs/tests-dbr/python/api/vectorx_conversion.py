"""
VectorX Conversion Functions - DBR Integration Example

Python equivalent of the Scala "Conversion Functions" example in docs/docs/api/scala.mdx.
Requires Databricks Runtime (st_geomfromwkb). Validated under docs/tests-dbr/.
"""

try:
    from pyspark.sql.functions import col, expr
    PYSPARK_AVAILABLE = True
except ImportError:
    col = None
    expr = None
    PYSPARK_AVAILABLE = False

# Display constant for docs/docs/api/scala.mdx#conversion-functions
VECTORX_CONVERSION_EXAMPLE = """# VectorX conversion: legacy → WKB → Databricks GEOMETRY (Python)
from databricks.labs.gbx.vectorx.jts.legacy import functions as vx
from pyspark.sql.functions import col, expr

vx.register(spark)

# Convert legacy geometries (e.g. from Mosaic) to WKB
legacy = spark.table("legacy_mosaic_table")
converted = legacy.select(
  col("feature_id"),
  vx.st_legacyaswkb(col("mosaic_geom")).alias("wkb_geom")
)

# Convert to Databricks GEOMETRY type
geometry_df = converted.select(
  col("feature_id"),
  col("wkb_geom"),
  expr("st_geomfromwkb(wkb_geom)").alias("geometry")
)

geometry_df.write.mode("overwrite").saveAsTable("converted_features")"""

VECTORX_CONVERSION_EXAMPLE_output = """Table converted_features: feature_id, wkb_geom, geometry columns"""

# SQL example for docs/docs/api/sql.mdx#legacy-geometry-conversion (uses st_geomfromwkb; DBR only)
SQL_VECTORX_LEGACY_CONVERSION = """-- Convert legacy Mosaic geometries
CREATE OR REPLACE TEMP VIEW converted_geometries AS
SELECT
    feature_id,
    properties,
    gbx_st_legacyaswkb(mosaic_geom) as wkb_geom,
    st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom)) as geometry
FROM legacy_mosaic_table;

SELECT * FROM converted_geometries;"""

SQL_VECTORX_LEGACY_CONVERSION_output = """
+----------+----------+--------+--------------------+
|feature_id|properties|wkb_geom|geometry            |
+----------+----------+--------+--------------------+
|1         |{...}     |[BINARY]|POLYGON ((...))     |
|...       |...       |...     |...                 |
+----------+----------+--------+--------------------+
"""

# Display constant for docs/docs/api/scala.mdx#complete-example-2 (VectorX Complete Example)
VECTORX_COMPLETE_EXAMPLE = """# VectorX full migration workflow (Python) - docs/docs/api/scala#complete-example-2
from databricks.labs.gbx.vectorx.jts.legacy import functions as vx
from pyspark.sql.functions import col, expr

vx.register(spark)

legacy_table = spark.table("legacy_mosaic_geometries")
migrated = legacy_table
  .select(
    col("*"),
    expr("st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))").alias("geometry")
  )
  .select(
    col("feature_id"),
    col("properties"),
    col("geometry"),
    expr("st_isvalid(geometry)").alias("is_valid"),
    expr("st_area(geometry)").alias("area")
  )
  .filter(col("is_valid") == True)

migrated.write.mode("overwrite").saveAsTable("migrated_features")"""

VECTORX_COMPLETE_EXAMPLE_output = """Table migrated_features: feature_id, properties, geometry, is_valid, area"""

# For docs/docs/api/python.mdx#complete-example-2 (CodeFromTest outputConstant)
vectorx_complete_example_output = """
Table migrated_features: feature_id, properties, geometry, is_valid, area (valid rows only).
"""


def vectorx_conversion_example(spark, table_name="legacy_mosaic_table"):
    """
    Run VectorX conversion workflow (requires DBR).
    Skips if table does not exist or st_geomfromwkb not available.
    """
    legacy = spark.table(table_name)
    from databricks.labs.gbx.vectorx.jts.legacy import functions as vx
    vx.register(spark)
    converted = legacy.select(
        col("feature_id"),
        vx.st_legacyaswkb(col("mosaic_geom")).alias("wkb_geom")
    )
    geometry_df = converted.select(
        col("feature_id"),
        col("wkb_geom"),
        expr("st_geomfromwkb(wkb_geom)").alias("geometry")
    )
    return geometry_df


def vectorx_complete_example(spark, table_name="legacy_mosaic_geometries"):
    """
    Run VectorX complete migration workflow (requires DBR).
    Skips if table does not exist or st_* not available.
    """
    from databricks.labs.gbx.vectorx.jts.legacy import functions as vx
    vx.register(spark)
    legacy_table = spark.table(table_name)
    migrated = legacy_table.select(
        col("*"),
        expr("st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))").alias("geometry")
    ).select(
        col("feature_id"),
        col("properties"),
        col("geometry"),
        expr("st_isvalid(geometry)").alias("is_valid"),
        expr("st_area(geometry)").alias("area")
    ).filter(col("is_valid") == True)
    return migrated
