"""
Limitations workaround examples – single source for docs/docs/limitations.mdx.

Convert GeoBrix WKB output to Databricks GEOMETRY and use ST functions.
Requires Databricks Runtime 17.1+ (st_geomfromwkb, st_area).

Documentation: docs/docs/limitations.mdx#workaround
Tested by: docs/tests-dbr/python/limitations/test_examples.py
"""

# =============================================================================
# DISPLAY CODE (shown in documentation)
# =============================================================================

CONVERT_TO_DATABRICKS_GEOMETRY_WORKAROUND = """from pyspark.sql.functions import expr

# Read with GeoBrix
df = spark.read.format("shapefile_ogr").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/subway/nyc_subway.shp.zip")

# Convert to Databricks built-in GEOMETRY type
geometry_df = df.select(
    "*",
    expr("st_geomfromwkb(geom_0)").alias("geometry")
)

# Now use built-in ST functions
result = geometry_df.select(
    "geometry",
    expr("st_area(geometry)").alias("area")
)
result.limit(5).show()"""

# =============================================================================
# EXAMPLE OUTPUT (for docs "Example output" block via CodeFromTest outputConstant)
# =============================================================================

CONVERT_TO_DATABRICKS_GEOMETRY_WORKAROUND_output = """
+------------------------------+-----+
|geometry                      |area |
+------------------------------+-----+
|SRID=4326;POINT (-73.99 40.73)|0.0  |
|SRID=4326;POINT (-73.98 40.75)|0.0  |
|...                           |...  |
+------------------------------+-----+
"""

# =============================================================================
# TEST HELPER (verify display code works)
# =============================================================================

try:
    from pyspark.sql.functions import expr
except ImportError:
    def expr(x):
        return None


def convert_to_databricks_geometry_workaround(spark, path):
    """
    Run the workaround: read shapefile with GeoBrix, convert to GEOMETRY, compute area.
    Used by test_examples.py to validate the documented snippet.
    """
    df = spark.read.format("shapefile_ogr").load(path)
    geometry_df = df.select("*", expr("st_geomfromwkb(geom_0)").alias("geometry"))
    result = geometry_df.select("geometry", expr("st_area(geometry)").alias("area"))
    return result
