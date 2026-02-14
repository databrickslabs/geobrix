"""
GeoJSON Reader Examples - Single Source of Truth

All code examples shown in docs/docs/readers/geojson.mdx are imported from this file.
Uses sample-data Volumes path; output constants for Example output blocks.
"""

# Sample-data Volumes path (NYC boroughs)
SAMPLE_GEOJSON_PATH = "/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/boroughs/nyc_boroughs.geojson"

# Display constants (payload only)
READ_GEOJSON = """# Read standard GeoJSON (sample-data Volumes path)
df = spark.read.format("geojson_ogr") \\
    .option("multi", "false") \\
    .load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/boroughs/nyc_boroughs.geojson")
df.show()"""

READ_GEOJSON_output = """+--------------------+-----------+---------+
|geom_0              |geom_0_srid|BoroName |
+--------------------+-----------+---------+
|[BINARY]            |4326       |Manhattan|
|...                 |...        |...      |
+--------------------+-----------+---------+"""

READ_GEOJSONSEQ = """# Read GeoJSONSeq (newline-delimited, sample-data path)
df = spark.read.format("geojson_ogr").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/boroughs/nyc_boroughs.geojsonl")
# Or explicitly: .option("multi", "true")
df.show()"""

READ_GEOJSONSEQ_output = """+--------------------+-----------+---------+
|geom_0              |geom_0_srid|BoroName |
+--------------------+-----------+---------+
|[BINARY]            |4326       |...      |
|...                 |...        |...      |
+--------------------+-----------+---------+"""

SQL_GEOJSON = """-- Read GeoJSON in SQL (sample-data Volumes path)
SELECT * FROM geojson_ogr.`/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/boroughs/nyc_boroughs.geojson`;"""

SQL_GEOJSON_output = """+--------------------+-----------+---------+
|geom_0              |geom_0_srid|BoroName |
+--------------------+-----------+---------+
|[BINARY]            |4326       |...      |
|...                 |...        |...      |
+--------------------+-----------+---------+"""

# Test functions (validate logic; use SAMPLE_GEOJSON_PATH when path not given)
def read_geojson(spark, path=None):
    """Verify READ_GEOJSON pattern works."""
    return spark.read.format("geojson_ogr").option("multi", "false").load(path or SAMPLE_GEOJSON_PATH)


def read_geojsonseq(spark, path="/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/boroughs/nyc_boroughs.geojsonl"):
    """Verify READ_GEOJSONSEQ pattern works."""
    return spark.read.format("geojson_ogr").load(path)
