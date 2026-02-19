"""Shapefile Reader Examples - Single Source of Truth

Uses sample-data Volumes path; output constants for Example output blocks.
"""

# Sample-data Volumes path
from path_config import SAMPLE_DATA_BASE
SAMPLE_SHAPEFILE_PATH = f"{SAMPLE_DATA_BASE}/nyc/subway/nyc_subway.shp.zip"

# Display constants (payload only)
READ_SHAPEFILE = """# Read shapefile (sample-data Volumes path)
df = spark.read.format("shapefile_ogr").load("{SAMPLE_SHAPEFILE_PATH}")
df.show()"""

READ_SHAPEFILE_output = """+--------------------+-----------+----+
|geom_0              |geom_0_srid|name|
+--------------------+-----------+----+
|[BINARY]            |4326       |... |
|...                 |...        |... |
+--------------------+-----------+----+"""

READ_WITH_OPTIONS = """# Adjust chunk size (sample-data Volumes path)
df = spark.read.format("shapefile_ogr") \\
    .option("chunkSize", "50000") \\
    .load("{SAMPLE_SHAPEFILE_PATH}")
df.show()"""

READ_WITH_OPTIONS_output = """+--------------------+-----------+----+
|geom_0              |geom_0_srid|name|
+--------------------+-----------+----+
|[BINARY]            |4326       |... |
|...                 |...        |... |
+--------------------+-----------+----+"""

SQL_SHAPEFILE = """-- Read shapefile in SQL (sample-data Volumes path)
SELECT * FROM shapefile_ogr.`{SAMPLE_SHAPEFILE_PATH}`;"""

SQL_SHAPEFILE_output = """+--------------------+-----------+----+
|geom_0              |geom_0_srid|name|
+--------------------+-----------+----+
|[BINARY]            |4326       |... |
|...                 |...        |... |
+--------------------+-----------+----+"""


# ============================================================================
# TEST FUNCTIONS (verify display code works)
# ============================================================================

def read_shapefile(spark, path=None):
    """Verify READ_SHAPEFILE pattern works."""
    return spark.read.format("shapefile_ogr").load(path or SAMPLE_SHAPEFILE_PATH)


def read_with_options(spark, path=None):
    """Verify READ_WITH_OPTIONS pattern works."""
    return spark.read.format("shapefile_ogr").option("chunkSize", "50000").load(path or SAMPLE_SHAPEFILE_PATH)
