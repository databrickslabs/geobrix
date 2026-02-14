"""
Python code examples for RasterX Function Reference documentation.
Single source of truth for docs/docs/api/rasterx-functions.mdx

Imports and registration are in the common setup only. SQL examples are in rasterx_functions_sql.py.
"""

try:
    from databricks.labs.gbx.rasterx import functions as rx
except ImportError:
    rx = None

# Sample data path for doc examples (mounted Volumes)
SAMPLE_RASTER_PATH = "/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif"


def rasterx_setup_example(spark):
    """Common setup: import, register RasterX, and load sample rasters. Run once before examples."""
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)
    rasters = spark.read.format("gdal").load(SAMPLE_RASTER_PATH)
    rasters.createOrReplaceTempView("rasters")
    return rasters


rasterx_setup_example_output = """
RasterX registered. Temp view `rasters` created from sample raster.
"""
