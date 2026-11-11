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
    from databricks.labs.gbx.vectorx import functions as vx
except ImportError:
    rx = None
    bx = None
    vx = None


def register_packages_python(spark):
    """Register GeoBrix packages in Python."""
    from databricks.labs import gbx
    from databricks.labs.gbx.rasterx import functions as rx
    from databricks.labs.gbx.gridx.bng import functions as bx
    from databricks.labs.gbx.vectorx import functions as vx
    
    # Register each package
    gbx.register(spark)
    return rx, bx, vx


# SQL registration constant
SQL_DESCRIBE_FUNCTION = """-- No registration needed in SQL
-- Functions are available after Python/Scala registration

DESCRIBE FUNCTION gbx_rst_width;"""

# Example output (show-type result for docs, same style as quick-start)
register_packages_python_output = """
Registered RasterX, GridX, and VectorX functions.
"""

SQL_DESCRIBE_FUNCTION_output = """
Function: gbx_rst_width
Type: ...
"""


def pattern_import_register(spark):
    """Pattern 1: Import and Register."""
    from databricks.labs import gbx
    # Import functions with alias
    from databricks.labs.gbx.rasterx import functions as rx
    
    # Register with Spark
    gbx.register(spark)
    
    # Load rasters (sample data from mounted Volumes)
    rasters = spark.read.format("raster_gbx").load(SAMPLE_NYC_RASTER)
    
    # Use in DataFrame operations
    df = rasters.select(rx.rst_boundingbox("tile"))
    
    # Or use in SQL after registration
    sql_result = spark.sql("SELECT gbx_rst_boundingbox(tile) FROM rasters")
    
    return df, sql_result


def pattern_mixed_language(spark):
    """Pattern 2: Mixed Language Usage."""
    from databricks.labs import gbx
    # Register in Python
    from databricks.labs.gbx.rasterx import functions as rx
    gbx.register(spark)
    
    # Load rasters (sample data from mounted Volumes)
    rasters = spark.read.format("raster_gbx").load(SAMPLE_NYC_RASTER)
    
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
    from databricks.labs import gbx
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql.functions import expr
    
    gbx.register(spark)
    
    # Load rasters (sample data from mounted Volumes)
    rasters = spark.read.format("raster_gbx").load(SAMPLE_NYC_RASTER)
    
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
    # Describe a specific function
    desc_df = spark.sql("DESCRIBE FUNCTION EXTENDED gbx_rst_boundingbox")
    desc_df.show(vertical=True)

    return desc_df


def error_functions_not_registered(spark):
    """Solution for Functions Not Registered error."""
    from databricks.labs import gbx
    # Error: Function 'gbx_rst_boundingbox' not found
    
    # Solution: Register functions first
    from databricks.labs.gbx.rasterx import functions as rx
    gbx.register(spark)
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
    from databricks.labs import gbx
    # Register functions once at the start of your notebook
    from databricks.labs.gbx.rasterx import functions as rx
    gbx.register(spark)
    
    # Then use throughout the notebook
    # Don't re-register in every cell
    return rx


def performance_use_dataframe_api(spark):
    """Performance Tip 2: Use DataFrame API."""
    from databricks.labs import gbx
    from databricks.labs.gbx.rasterx import functions as rx
    gbx.register(spark)
    
    df = spark.read.format("raster_gbx").load(SAMPLE_NYC_RASTER)
    
    # Prefer DataFrame API for complex operations
    result = df.select(rx.rst_boundingbox("tile"))
    
    # Over repeated SQL calls
    # result = spark.sql("SELECT gbx_rst_boundingbox(tile) FROM df")
    
    return result


def performance_batch_operations(spark):
    """Performance Tip 3: Batch Operations."""
    from databricks.labs import gbx
    from databricks.labs.gbx.rasterx import functions as rx
    gbx.register(spark)
    
    df = spark.read.format("raster_gbx").load(SAMPLE_NYC_RASTER)
    
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
-DESCRIBE FUNCTION EXTENDED gbx_rst_boundingbox
Function: gbx_rst_boundingbox
...
"""


if __name__ == "__main__":
    # Test that all functions are importable
    print("✓ All API overview examples defined")
