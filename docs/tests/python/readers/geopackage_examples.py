"""
GeoPackage Reader Examples - Single Source of Truth

All code examples shown in docs/docs/readers/geopackage.mdx are imported from this file.
Uses sample-data Volumes path; output constants for Example output blocks.
"""

# Sample-data path at runtime (path_config)
from path_config import SAMPLE_DATA_BASE
SAMPLE_GEOPACKAGE_PATH = f"{SAMPLE_DATA_BASE}/nyc/geopackage/nyc_complete.gpkg"

# Display constants (payload only)
READ_GEOPACKAGE = """# Read GeoPackage (sample-data Volumes path)
df = spark.read.format("gpkg_ogr").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/geopackage/nyc_complete.gpkg")
df.show()"""

READ_GEOPACKAGE_output = """+--------------------+--------------+---------+
|shape               |shape_srid    |BoroName |
+--------------------+--------------+---------+
|[BINARY]            |4326          |Manhattan|
|...                 |...           |...      |
+--------------------+--------------+---------+"""

READ_SPECIFIC_LAYER = """# Read specific layer (sample-data Volumes path)
boroughs = spark.read.format("gpkg_ogr") \\
    .option("layerName", "boroughs") \\
    .load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/geopackage/nyc_complete.gpkg")
boroughs.show()"""

READ_SPECIFIC_LAYER_output = """+--------------------+--------------+---------+
|shape               |shape_srid    |BoroName |
+--------------------+--------------+---------+
|[BINARY]            |4326          |...      |
|...                 |...           |...      |
+--------------------+--------------+---------+"""

SQL_GEOPACKAGE = """-- Read GeoPackage in SQL (sample-data Volumes path)
SELECT * FROM gpkg_ogr.`/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/geopackage/nyc_complete.gpkg`;"""

SQL_GEOPACKAGE_output = """+--------------------+--------------+---------+
|shape               |shape_srid    |BoroName |
+--------------------+--------------+---------+
|[BINARY]            |4326          |...      |
|...                 |...           |...      |
+--------------------+--------------+---------+"""

# Test functions (validate logic; use SAMPLE_GEOPACKAGE_PATH when path not given)
def read_geopackage(spark, path=None):
    """Verify READ_GEOPACKAGE pattern works."""
    return spark.read.format("gpkg_ogr").load(path or SAMPLE_GEOPACKAGE_PATH)


def read_specific_layer(spark, path=None, layer="boroughs"):
    """Verify READ_SPECIFIC_LAYER pattern works."""
    return spark.read.format("gpkg_ogr").option("layerName", layer).load(path or SAMPLE_GEOPACKAGE_PATH)
