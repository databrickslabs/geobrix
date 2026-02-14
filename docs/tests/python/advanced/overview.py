"""
GeoBrix Advanced Overview Examples

Code examples for the advanced usage overview page.
"""

# Conditional imports
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import udf, lit
    from pyspark.sql.types import MapType, StringType
    PYSPARK_AVAILABLE = True
except ImportError:
    SparkSession = None
    DataFrame = None
    PYSPARK_AVAILABLE = False
    def udf(*args, **kwargs):
        def decorator(f):
            return f
        return decorator if not args else decorator(args[0])
    def lit(x):
        return None


# Sample data path (same as quick-start / sample-data)
_RASTER_PATH = "/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif"


def spark_expressions_standard_usage(spark):
    """
    Standard GeoBrix usage with Spark expressions.
    Demonstrates the eval interface through registered functions.
    """
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    # Uses Spark's columnar expression engine
    rasters = spark.read.format("gdal").load(_RASTER_PATH)
    df = rasters.select(rx.rst_boundingbox("tile").alias("bbox"))
    df.limit(3).show(truncate=40)
    return df


# Example output for docs (CodeFromTest outputConstant)
spark_expressions_standard_usage_output = """
+--------------------+
|bbox                |
+--------------------+
|POLYGON ((...))     |
|POLYGON ((...))     |
|...                 |
+--------------------+
"""


def end_to_end_advanced_pipeline(spark):
    """
    Complete end-to-end advanced pipeline.
    
    Demonstrates combining GDAL CLI preprocessing, custom UDFs,
    standard GeoBrix operations, and library integration.
    """
    # 1. Preprocess with GDAL CLI (via subprocess or notebook magic)
    # !gdalwarp -t_srs EPSG:4326 input.tif reprojected.tif
    
    # 2. Read with GeoBrix
    rasters = spark.read.format("gdal").load("/data/reprojected.tif")
    
    # 3. Apply custom UDF for specialized logic
    from databricks.labs.gbx.rasterx.expressions.accessors import RST_Metadata
    
    @udf(MapType(StringType(), StringType()))
    def extract_custom_metadata(tile_binary):
        # Custom logic using execute methods
        # (This is simplified - see Custom UDFs guide for details)
        from datetime import datetime
        
        dataset = None  # load_dataset_from_binary(tile_binary)
        # metadata = RST_Metadata.execute(dataset)
        # Add custom processing
        metadata = {}
        metadata["processed_date"] = datetime.now().isoformat()
        return metadata
    
    enriched = rasters.withColumn("custom_metadata", extract_custom_metadata("tile"))
    
    # 4. Use standard GeoBrix for distributed operations
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)
    
    aoi_geometry = None  # Placeholder
    result = enriched.select(
        "*",
        rx.rst_boundingbox("tile").alias("bbox"),
        rx.rst_clip("tile", aoi_geometry, lit(True)).alias("clipped")
    )
    
    # 5. Integrate with xarray for analysis (see Library Integration guide)
    # Convert to xarray for advanced array operations
    # ...
    
    # 6. Save results (optional: result.limit(3).show() to inspect)
    result.limit(3).show(truncate=30)
    # result.write.mode("overwrite").saveAsTable("processed_rasters")

    return result


# Example output for docs (CodeFromTest outputConstant)
end_to_end_advanced_pipeline_output = """
+----+--------------------+-------+-------+
|path|bbox                |...    |clipped|
+----+--------------------+-------+-------+
|... |POLYGON ((...))     |...    |...    |
+----+--------------------+-------+-------+
"""


if __name__ == "__main__":
    print("GeoBrix Advanced Overview Examples")
    print("=" * 50)
    print(f"Total functions: 2")
