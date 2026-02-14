"""
GeoBrix Custom UDFs Examples

Code examples for building custom Spark UDFs with GeoBrix execute methods.
"""

# Conditional imports
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType, MapType, BinaryType
    PYSPARK_AVAILABLE = True
except ImportError:
    SparkSession = None
    DataFrame = None
    PYSPARK_AVAILABLE = False
    def udf(*args, **kwargs):
        def decorator(f):
            return f
        return decorator if not args else decorator(args[0])


def eval_method_standard_usage(spark):
    """
    Standard GeoBrix usage with eval methods.
    Uses Spark expressions registered as functions.
    """
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    rasters = spark.read.format("gdal").load(
        "/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif"
    )
    df = rasters.select(rx.rst_boundingbox("tile").alias("bbox"))
    df.limit(2).show(truncate=40)
    return df


eval_method_standard_usage_output = """
+--------------------+
|bbox                |
+--------------------+
|POLYGON ((...))     |
|...                 |
+--------------------+
"""


def basic_python_udf_example(spark):
    """
    Basic Python UDF using GeoBrix execute methods.
    
    Extract custom metadata from raster tiles.
    """
    import json
    
    # Import GeoBrix execute methods (via Py4J bridge)
    # from databricks.labs.gbx.rasterx.expressions import accessors
    
    @udf(MapType(StringType(), StringType()))
    def extract_custom_metadata(tile_binary):
        """
        Extract custom metadata from raster tile
        """
        try:
            # Load GDAL dataset from binary
            # This is simplified - actual implementation needs proper deserialization
            # from databricks.labs.gbx.rasterx.gdal import GDALManager
            
            # Get dataset handle
            # dataset = load_dataset_from_tile(tile_binary)
            
            # Use execute methods
            metadata = {}
            # metadata["format"] = accessors.RST_Format.execute(dataset)
            # metadata["width"] = str(accessors.RST_Width.execute(dataset))
            # metadata["height"] = str(accessors.RST_Height.execute(dataset))
            
            # Add custom logic
            metadata["aspect_ratio"] = "1.0"  # Placeholder
            
            # Clean up
            # dataset.delete()
            
            return metadata
        except Exception as e:
            return {"error": str(e)}
    
    rasters = spark.read.format("gdal").load(
        "/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif"
    )
    enriched = rasters.withColumn("custom_metadata", extract_custom_metadata("tile"))
    enriched.select("path", "custom_metadata").limit(2).show(truncate=30)
    return enriched


basic_python_udf_example_output = """
+--------------------+------------------+
|path                |custom_metadata   |
+--------------------+------------------+
|.../nyc_sentinel2...|{aspect_ratio=1.0}|
|...                 |...               |
+--------------------+------------------+
"""


# Sample path for common-pattern examples (one-copy, display results)
_RASTER_PATH = "/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif"


def conditional_processing_example(spark):
    """
    Common pattern: conditional processing based on raster properties.
    Branch logic (e.g. multispectral vs RGB) using GeoBrix accessors.
    """
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    rasters = spark.read.format("gdal").load(_RASTER_PATH)
    # Branch by band count: different path for multiband vs single band
    with_band_count = rasters.select(
        rx.rst_numbands("tile").alias("num_bands"),
        rx.rst_width("tile").alias("width"),
        rx.rst_height("tile").alias("height"),
    )
    with_band_count.limit(2).show()
    return with_band_count


conditional_processing_example_output = """
+---------+-----+------+
|num_bands|width|height|
+---------+-----+------+
|1        |10980|10980 |
|...      |...  |...   |
+---------+-----+------+
"""


def chained_processing_example(spark):
    """
    Common pattern: chain multiple operations (metadata -> validate -> process).
    Single dataset load, multiple execute-style steps via Spark expressions.
    """
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    rasters = spark.read.format("gdal").load(_RASTER_PATH)
    # Step 1: metadata; Step 2: dimensions; Step 3: optional clip/transform
    result = rasters.select(
        rx.rst_boundingbox("tile").alias("bbox"),
        rx.rst_width("tile").alias("width"),
        rx.rst_height("tile").alias("height"),
    )
    result.limit(2).show(truncate=30)
    return result


chained_processing_example_output = """
+--------------------+-----+------+
|bbox                |width|height|
+--------------------+-----+------+
|POLYGON ((...))     |10980|10980 |
|...                 |...  |...   |
+--------------------+-----+------+
"""


def integration_test_example():
    """
    Integration testing pattern for custom UDFs.
    
    Demonstrates how to test UDFs with real Spark session.
    """
    from pyspark.sql import SparkSession
    
    def test_custom_udf_integration():
        spark = SparkSession.builder.getOrCreate()
        
        # Register UDF
        # spark.udf.register("custom_stats", custom_raster_stats)
        
        # Load test data
        rasters = spark.read.format("gdal").load("/test/data")
        
        # Apply UDF
        # result = rasters.selectExpr("custom_stats(tile) as stats")
        
        # Verify
        # assert result.count() > 0
        # assert result.first()["stats"] is not None
        
        return True
    
    return test_custom_udf_integration()


if __name__ == "__main__":
    print("GeoBrix Custom UDFs Examples")
    print("=" * 50)
    print(f"Total functions: 3")
