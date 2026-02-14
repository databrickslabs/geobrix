"""OGR Reader DBR Examples - User-Facing Code

Examples for docs/docs/readers/ogr.mdx#databricks-integration.
Same pattern applies to any OGR-based reader (shapefile_ogr, geojson_ogr, etc.).
"""

# ============================================================================
# DISPLAY CODE (shown in documentation)
# ============================================================================

CONVERT_TO_GEOMETRY = """# Convert WKB to Databricks GEOMETRY type
df = spark.read.format("shapefile_ogr").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/subway/nyc_subway.shp.zip")
df_with_geom = df.select("*", expr("st_geomfromwkb(geom_0)").alias("geometry"))"""

SQL_READ_SHAPEFILE = """-- Read shapefile and convert to GEOMETRY in SQL
CREATE OR REPLACE TEMP VIEW stations AS
SELECT *, st_geomfromwkb(geom_0) as geometry
FROM shapefile_ogr.`/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/subway/nyc_subway.shp.zip`;

SELECT name, geometry FROM stations LIMIT 10;"""


# ============================================================================
# TEST FUNCTIONS (verify display code works)
# ============================================================================

try:
    from pyspark.sql.functions import expr
except ImportError:
    def expr(x):
        return None


def convert_to_geometry(spark, path="/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/subway/nyc_subway.shp.zip"):
    """Verify CONVERT_TO_GEOMETRY pattern works."""
    df = spark.read.format("shapefile_ogr").load(path)
    return df.select("*", expr("st_geomfromwkb(geom_0)").alias("geometry"))
