"""
File Geodatabase Reader Examples - Single Source of Truth

All code examples shown in docs/docs/readers/filegdb.mdx are imported from this file.
Uses sample-data Volumes path; output constants for Example output blocks.
"""

# Sample-data Volumes path (.gdb.zip; reader supports it via vsizip)
FILEGDB_PATH = "/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/filegdb/NYC_Sample.gdb.zip"

# Display constants (payload only)
READ_FILEGDB = """# Read File Geodatabase (sample-data Volumes path)
df = spark.read.format("file_gdb_ogr").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/filegdb/NYC_Sample.gdb.zip")
df.show()"""

READ_FILEGDB_output = """+--------------------+--------------+---------+
|SHAPE               |SHAPE_srid    |BoroName |
+--------------------+--------------+---------+
|[BINARY]            |4326          |...      |
|...                 |...           |...      |
+--------------------+--------------+---------+"""

READ_WITH_LAYER = """# Read specific feature class (sample-data Volumes path)
df = spark.read.format("file_gdb_ogr") \\
    .option("layerName", "NYC_Boroughs") \\
    .load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/filegdb/NYC_Sample.gdb.zip")
df.show()"""

READ_WITH_LAYER_output = """+--------------------+--------------+---------+
|SHAPE               |SHAPE_srid    |BoroName |
+--------------------+--------------+---------+
|[BINARY]            |4326          |...      |
|...                 |...           |...      |
+--------------------+--------------+---------+"""

SQL_FILEGDB = """-- Read File Geodatabase in SQL (sample-data Volumes path)
SELECT * FROM file_gdb_ogr.`/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/filegdb/NYC_Sample.gdb.zip` LIMIT 10;"""

SQL_FILEGDB_output = """+--------------------+--------------+---------+
|SHAPE               |SHAPE_srid    |BoroName |
+--------------------+--------------+---------+
|[BINARY]            |4326          |...      |
|...                 |...           |...      |
+--------------------+--------------+---------+"""

# Test functions (validate logic)
def read_filegdb(spark, path=None):
    """Verify READ_FILEGDB pattern works."""
    return spark.read.format("file_gdb_ogr").load(path or FILEGDB_PATH)


def read_with_layer(spark, path=None, layer="NYC_Boroughs"):
    """Verify READ_WITH_LAYER pattern works."""
    return spark.read.format("file_gdb_ogr").option("layerName", layer).load(path or FILEGDB_PATH)
