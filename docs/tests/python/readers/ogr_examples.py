"""
OGR Reader Examples - Single Source of Truth

All code examples shown in docs/docs/readers/ogr.mdx are imported from this file.
Uses sample-data Volumes path; output constants for Example output blocks.
"""

# Sample-data Volumes path (NYC boroughs GeoJSON - same as other reader docs)
SAMPLE_VECTOR_PATH = "/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/boroughs/nyc_boroughs.geojson"

# Display constants (payload only)
READ_OGR = """# OGR reader (sample-data Volumes path)
df = spark.read.format("ogr").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/boroughs/nyc_boroughs.geojson")
df.show()"""

READ_OGR_output = """+--------------------+-----------+-----+
|geom_0              |geom_0_srid|...  |
+--------------------+-----------+-----+
|[BINARY]            |4326       |...  |
|...                 |...        |...  |
+--------------------+-----------+-----+"""

READ_WITH_DRIVER = """# Explicit driver (sample-data Volumes path)
df = spark.read.format("ogr") \\
    .option("driverName", "GeoJSON") \\
    .load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/boroughs/nyc_boroughs.geojson")
df.show()"""

READ_WITH_DRIVER_output = """+--------------------+-----------+-----+
|geom_0              |geom_0_srid|...  |
+--------------------+-----------+-----+
|[BINARY]            |4326       |...  |
|...                 |...        |...  |
+--------------------+-----------+-----+"""

SQL_OGR = """-- Read with OGR in SQL (sample-data Volumes path)
SELECT * FROM ogr.`/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/boroughs/nyc_boroughs.geojson`;"""

SQL_OGR_output = """+--------------------+-----------+-----+
|geom_0              |geom_0_srid|...  |
+--------------------+-----------+-----+
|[BINARY]            |4326       |...  |
|...                 |...        |...  |
+--------------------+-----------+-----+"""

# Named Readers vs OGR (one-copy: sample-data path)
NAMED_VS_OGR = """# Named reader (recommended for common formats)
df = spark.read.format("shapefile_ogr").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/subway/nyc_subway.shp.zip")
# OGR with explicit driver (same result)
df = spark.read.format("ogr").option("driverName", "ESRI Shapefile").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/subway/nyc_subway.shp.zip")"""

# Test functions (validate logic; use SAMPLE_VECTOR_PATH when path not given)
def read_ogr(spark, path=None):
    """Verify READ_OGR pattern works."""
    return spark.read.format("ogr").load(path or SAMPLE_VECTOR_PATH)


def read_with_driver(spark, path=None):
    """Verify READ_WITH_DRIVER pattern works."""
    return spark.read.format("ogr").option("driverName", "GeoJSON").load(path or SAMPLE_VECTOR_PATH)
