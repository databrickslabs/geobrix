"""
GDAL Reader Examples - Single Source of Truth

All code examples shown in docs/docs/readers/gdal.mdx are imported from this file.
Uses sample-data Volumes path; output constants for Example output blocks.
"""

# Sample-data Volumes paths (same as other reader/sample-data docs)
SAMPLE_RASTER_PATH = "/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif"
SAMPLE_HRRR_PATH = "/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/hrrr-weather/hrrr_nyc_*.grib2"

# Display constants (payload only)
READ_GDAL = """# Read raster file (sample-data Volumes path)
df = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
df.show()"""

READ_GDAL_output = """+--------------------------------------------------+-----+
|path                                              |tile |
+--------------------------------------------------+-----+
|/Volumes/.../nyc_sentinel2_red.tif                |{...}|
+--------------------------------------------------+-----+"""

READ_WITH_DRIVER = """# Read with explicit driver (sample-data Volumes path)
df = spark.read.format("gdal") \\
    .option("driver", "GTiff") \\
    .load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
df.show()"""

READ_WITH_DRIVER_output = """+--------------------------------------------------+-----+
|path                                              |tile |
+--------------------------------------------------+-----+
|/Volumes/.../nyc_sentinel2_red.tif                |{...}|
+--------------------------------------------------+-----+"""

SQL_GDAL = """-- Read raster in SQL (sample-data Volumes path)
SELECT * FROM gdal.`/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif` LIMIT 10;"""

SQL_GDAL_output = """+--------------------------------------------------+-----+
|path                                              |tile |
+--------------------------------------------------+-----+
|/Volumes/.../nyc_sentinel2_red.tif                |{...}|
+--------------------------------------------------+-----+"""

# GRIB2 - HRRR weather (sample-data hrrr-weather)
READ_GRIB2 = """# GRIB2 weather data (sample-data HRRR)
df = spark.read.format("gdal") \\
    .option("driver", "GRIB") \\
    .load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/hrrr-weather/hrrr_nyc_*.grib2")"""

READ_GRIB2_output = """+--------------------------------------------------+-----+
|path                                              |tile |
+--------------------------------------------------+-----+
|.../nyc/hrrr-weather/hrrr_nyc_....grib2           |{...}|
+--------------------------------------------------+-----+"""

# Test functions (validate logic; use SAMPLE_RASTER_PATH when path not given)
def read_gdal(spark, path=None):
    """Verify READ_GDAL pattern works."""
    return spark.read.format("gdal").load(path or SAMPLE_RASTER_PATH)


def read_with_driver(spark, path=None):
    """Verify READ_WITH_DRIVER pattern works."""
    return spark.read.format("gdal").option("driver", "GTiff").load(path or SAMPLE_RASTER_PATH)


def read_grib2(spark, path=None):
    """Verify READ_GRIB2 pattern works (HRRR weather from sample-data)."""
    return spark.read.format("gdal").option("driver", "GRIB").load(path or SAMPLE_HRRR_PATH)
