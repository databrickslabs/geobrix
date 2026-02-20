"""
API Reference Overview Examples

This module contains all Python code examples for the API Reference Overview page.
Functions demonstrate registration, usage patterns, and common operations.

Paths use sample data from mounted Volumes (see docs/docs/sample-data.mdx).
"""

from path_config import SAMPLE_DATA_BASE
SAMPLE_NYC_RASTER = f"{SAMPLE_DATA_BASE}/nyc/sentinel2/nyc_sentinel2_red.tif"

# Conditional imports for documentation testing
try:
    from pyspark.sql import SparkSession, functions as f
    from pyspark.sql.types import *
except ImportError:
    SparkSession = None
    f = None

# GeoBrix imports with fallback
try:
    from databricks.labs.gbx.rasterx import functions as rx
    from databricks.labs.gbx.gridx.bng import functions as bx
    from databricks.labs.gbx.vectorx.jts.legacy import functions as vx
except ImportError:
    rx = None
    bx = None
    vx = None


def register_packages_python(spark):
    """Register GeoBrix packages in Python."""
    from databricks.labs.gbx.rasterx import functions as rx
    from databricks.labs.gbx.gridx.bng import functions as bx
    from databricks.labs.gbx.vectorx.jts.legacy import functions as vx
    
    # Register each package
    rx.register(spark)
    bx.register(spark)
    vx.register(spark)
    return rx, bx, vx


# SQL registration constant
SQL_SHOW_FUNCTIONS = """-- No registration needed in SQL
-- Functions are available after Python/Scala registration

SHOW FUNCTIONS LIKE 'gbx_*';"""

# Example output (show-type result for docs, same style as quick-start)
register_packages_python_output = """
Registered RasterX, GridX, and VectorX functions.
"""

SQL_SHOW_FUNCTIONS_output = """
+--------------------+
|function            |
+--------------------+
|gbx_rst_asformat    |
|gbx_rst_avg         |
|gbx_rst_bandmetadata|
+--------------------+
"""


def pattern_import_register(spark):
    """Pattern 1: Import and Register."""
    # Import functions with alias
    from databricks.labs.gbx.rasterx import functions as rx
    
    # Register with Spark
    rx.register(spark)
    
    # Load rasters (sample data from mounted Volumes)
    rasters = spark.read.format("gdal").load(SAMPLE_NYC_RASTER)
    
    # Use in DataFrame operations
    df = rasters.select(rx.rst_boundingbox("tile"))
    
    # Or use in SQL after registration
    sql_result = spark.sql("SELECT gbx_rst_boundingbox(tile) FROM rasters")
    
    return df, sql_result


def pattern_mixed_language(spark):
    """Pattern 2: Mixed Language Usage."""
    # Register in Python
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)
    
    # Load rasters (sample data from mounted Volumes)
    rasters = spark.read.format("gdal").load(SAMPLE_NYC_RASTER)
    
    # Use in Python
    python_result = rasters.select(rx.rst_boundingbox("tile"))
    
    # Use in SQL
    sql_result = spark.sql("""
        SELECT gbx_rst_boundingbox(tile) as bbox
        FROM rasters
    """)
    
    # Both return the same results
    return python_result, sql_result


def pattern_chaining_operations(spark):
    """Pattern 3: Chaining Operations."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql.functions import expr
    
    rx.register(spark)
    
    # Load rasters (sample data from mounted Volumes)
    rasters = spark.read.format("gdal").load(SAMPLE_NYC_RASTER)
    
    # Chain multiple operations
    result = (
        rasters
        .select(
            "path",
            rx.rst_clip("tile", f.lit("POLYGON((-122 37, -122 38, -121 38, -121 37, -122 37))"), f.lit(True)).alias("clipped")
        )
        .select(
            "path",
            "clipped",
            rx.rst_boundingbox("clipped").alias("new_bounds")
        )
    )
    return result


def error_check_function_availability(spark):
    """Check Function Availability."""
    # List registered functions
    functions_df = spark.sql("SHOW FUNCTIONS LIKE 'gbx_*'")
    functions_df.limit(3).show()
    
    # Describe a specific function
    desc_df = spark.sql("DESCRIBE FUNCTION EXTENDED gbx_rst_boundingbox")
    desc_df.show(vertical=True)
    
    return functions_df, desc_df


def error_functions_not_registered(spark):
    """Solution for Functions Not Registered error."""
    # Error: Function 'gbx_rst_boundingbox' not found
    
    # Solution: Register functions first
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)
    return rx


def error_import_errors():
    """Solution for Import Errors."""
    # Error: No module named 'databricks.labs.gbx'
    
    # Solution: Ensure the wheel is installed on the cluster
    # Check cluster libraries
    import sys
    gbx_installed = any('databricks.labs.gbx' in str(path) for path in sys.path)
    return gbx_installed


def performance_register_once(spark):
    """Performance Tip 1: Register Once."""
    # Register functions once at the start of your notebook
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)
    
    # Then use throughout the notebook
    # Don't re-register in every cell
    return rx


def performance_use_dataframe_api(spark):
    """Performance Tip 2: Use DataFrame API."""
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)
    
    df = spark.read.format("gdal").load(SAMPLE_NYC_RASTER)
    
    # Prefer DataFrame API for complex operations
    result = df.select(rx.rst_boundingbox("tile"))
    
    # Over repeated SQL calls
    # result = spark.sql("SELECT gbx_rst_boundingbox(tile) FROM df")
    
    return result


def performance_batch_operations(spark):
    """Performance Tip 3: Batch Operations."""
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)
    
    df = spark.read.format("gdal").load(SAMPLE_NYC_RASTER)
    
    # Process multiple columns at once
    result = df.select(
        rx.rst_boundingbox("tile").alias("bbox"),
        rx.rst_width("tile").alias("width"),
        rx.rst_height("tile").alias("height"),
        rx.rst_metadata("tile").alias("metadata")
    )
    return result


# ============================================================================
# EXAMPLE OUTPUT (for docs "Example output" block via CodeFromTest outputConstant)
# ============================================================================

error_check_function_availability_output = """
+------------------+
|function          |
+------------------+
|gbx_rst_asformat  |
|gbx_rst_avg       |
|gbx_rst_bandmetadata|
+------------------+
Only showing top 3 rows

-DESCRIBE FUNCTION EXTENDED gbx_rst_boundingbox
Function: gbx_rst_boundingbox
...
"""


if __name__ == "__main__":
    # Test that all functions are importable
    print("✓ All API overview examples defined")
