"""
GeoTIFF Reader Examples - Single Source of Truth

All code examples shown in docs/docs/readers/gtiff.mdx are imported from this file.
Uses sample-data Volumes path; output constants for Example output blocks.
"""

# Sample-data Volumes path (same as other reader docs)
SAMPLE_GTIFF_PATH = "/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif"

# Display constants (payload only)
READ_GTIFF = """# Read GeoTIFF file (sample-data Volumes path)
df = spark.read.format("gtiff_gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
df.show()"""

READ_GTIFF_output = """+--------------------------------------------------+-----+
|path                                              |tile |
+--------------------------------------------------+-----+
|/Volumes/.../nyc_sentinel2_red.tif                |{...}|
+--------------------------------------------------+-----+"""

READ_WITH_OPTIONS = """# Read GeoTIFF with options (sample-data Volumes path)
df = spark.read.format("gtiff_gdal") \\
    .option("readSubdatasets", "false") \\
    .load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
df.show()"""

READ_WITH_OPTIONS_output = """+--------------------------------------------------+-----+
|path                                              |tile |
+--------------------------------------------------+-----+
|/Volumes/.../nyc_sentinel2_red.tif                |{...}|
+--------------------------------------------------+-----+"""

SQL_GTIFF = """-- Read GeoTIFF in SQL (sample-data Volumes path)
SELECT * FROM gtiff_gdal.`/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif` LIMIT 10;"""

SQL_GTIFF_output = """+--------------------------------------------------+-----+
|path                                              |tile |
+--------------------------------------------------+-----+
|/Volumes/.../nyc_sentinel2_red.tif                |{...}|
+--------------------------------------------------+-----+"""

# GeoTIFF vs GDAL Reader comparison (sample-data path)
GTIFF_VS_GDAL = """# GeoTIFF reader (recommended for .tif files)
df = spark.read.format("gtiff_gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")

# GDAL reader (same result, explicit driver)
df = spark.read.format("gdal").option("driver", "GTiff").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")"""

# Cloud-Optimized GeoTIFF (COG) - sample path for local; doc notes S3 for cloud
COG_EXAMPLE = """# COG files read like regular GeoTIFFs (sample-data path for local)
cog_df = spark.read.format("gtiff_gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
# For cloud: spark.read.format("gtiff_gdal").load("s3://bucket/cog-file.tif")"""

# Test functions (validate logic; use SAMPLE_GTIFF_PATH when path not given)
def read_gtiff(spark, path=None):
    """Verify READ_GTIFF pattern works."""
    return spark.read.format("gtiff_gdal").load(path or SAMPLE_GTIFF_PATH)


def read_with_options(spark, path=None):
    """Verify READ_WITH_OPTIONS pattern works."""
    return spark.read.format("gtiff_gdal").option("readSubdatasets", "false").load(path or SAMPLE_GTIFF_PATH)
