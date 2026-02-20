"""
Python API Reference Examples

This module contains all Python code examples for the Python API Reference page.
Functions demonstrate installation, import patterns, and usage of GeoBrix functions.

All paths use sample data from the mounted Volumes (see docs/docs/sample-data.mdx).
Run doc tests in Docker where /Volumes/main/default/geobrix_samples/geobrix-examples/ is available.
"""

# Sample data paths at runtime (path_config: minimal bundle or GBX_SAMPLE_DATA_ROOT)
from path_config import SAMPLE_DATA_BASE

SAMPLE_NYC_RASTER = f"{SAMPLE_DATA_BASE}/nyc/sentinel2/nyc_sentinel2_red.tif"
SAMPLE_NYC_RASTERS = f"{SAMPLE_DATA_BASE}/nyc/sentinel2/*.tif"
SAMPLE_NYC_ELEVATION = f"{SAMPLE_DATA_BASE}/nyc/elevation/srtm_n40w073.tif"

# Conditional imports for documentation testing
try:
    from pyspark.sql import SparkSession, DataFrame, functions as f
    from pyspark.sql.types import *
except ImportError:
    SparkSession = None
    DataFrame = None
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


def verify_installation():
    """Verify GeoBrix installation."""
    # Verify installation
    import databricks.labs.gbx
    print("GeoBrix installed successfully")
    return True


def import_pattern_rasterx(spark):
    """Import pattern for RasterX."""
    from databricks.labs.gbx.rasterx import functions as rx
    
    # Register functions
    rx.register(spark)
    
    raster_path = SAMPLE_NYC_RASTER
    rasters = spark.read.format("gdal").load(raster_path)
    
    # Use functions
    df = rasters.select(rx.rst_boundingbox("tile"))
    df.limit(1).show(truncate=False)
    return df


def import_pattern_gridx(spark):
    """Import pattern for GridX (BNG)."""
    from databricks.labs.gbx.gridx.bng import functions as bx
    
    # Register functions
    bx.register(spark)
    
    # Use functions (gbx_bng_cellarea returns square kilometres)
    df = spark.sql("SELECT gbx_bng_cellarea('TQ', 1000) as area_km2")
    df.show()
    return df


def import_pattern_vectorx(spark):
    """Import pattern for VectorX."""
    from databricks.labs.gbx.vectorx.jts.legacy import functions as vx
    
    # Register functions
    vx.register(spark)
    
    # Load legacy data
    legacy_data = spark.table("legacy_geometries")
    
    # Use functions
    df = legacy_data.select(vx.st_legacyaswkb("mosaic_geom"))
    return df


def rst_boundingbox_example(spark):
    """Get the bounding box of a raster."""
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)
    raster_path = SAMPLE_NYC_RASTER
    rasters = spark.read.format("gdal").load(raster_path)
    bbox_df = rasters.select(
        "path",
        rx.rst_boundingbox("tile").alias("bbox")
    )
    bbox_df.limit(3).show()
    return bbox_df


def rst_width_example(spark):
    """Get raster width in pixels."""
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)
    raster_path = SAMPLE_NYC_RASTER
    rasters = spark.read.format("gdal").load(raster_path)
    width_df = rasters.select(rx.rst_width("tile").alias("width"))
    width_df.limit(1).show()
    return width_df


def rst_height_example(spark):
    """Get raster height in pixels."""
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)
    raster_path = SAMPLE_NYC_RASTER
    rasters = spark.read.format("gdal").load(raster_path)
    height_df = rasters.select(rx.rst_height("tile").alias("height"))
    height_df.limit(1).show()
    return height_df


def rst_numbands_example(spark):
    """Get number of bands in raster."""
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)
    raster_path = SAMPLE_NYC_RASTER
    rasters = spark.read.format("gdal").load(raster_path)
    bands_df = rasters.select(rx.rst_numbands("tile").alias("num_bands"))
    bands_df.limit(1).show()
    return bands_df


def rst_metadata_example(spark):
    """Get raster metadata."""
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)
    raster_path = SAMPLE_NYC_RASTER
    rasters = spark.read.format("gdal").load(raster_path)
    metadata_df = rasters.select(rx.rst_metadata("tile").alias("metadata"))
    metadata_df.limit(1).show(truncate=False)
    return metadata_df


def rst_srid_example(spark):
    """Get spatial reference identifier."""
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)
    raster_path = SAMPLE_NYC_RASTER
    rasters = spark.read.format("gdal").load(raster_path)
    srid_df = rasters.select(rx.rst_srid("tile").alias("srid"))
    srid_df.limit(1).show()
    return srid_df


def rst_clip_example(spark):
    """Clip raster by geometry. Geometry must be WKT or WKB (GeoBrix does not accept DBR st_geomfromtext)."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql.functions import lit
    
    rx.register(spark)
    raster_path = SAMPLE_NYC_RASTER
    rasters = spark.read.format("gdal").load(raster_path)
    clip_wkt = "POLYGON((-122 37, -122 38, -121 38, -121 37, -122 37))"
    clipped = rasters.select(
        rx.rst_clip("tile", lit(clip_wkt), lit(True)).alias("clipped_tile")
    )
    clipped.limit(1).show(truncate=False)
    return clipped


def rasterx_complete_example(spark):
    """Complete RasterX example."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql.functions import expr
    
    # Register functions
    rx.register(spark)
    
    raster_path = SAMPLE_NYC_RASTER
    rasters = spark.read.format("gdal").load(raster_path)
    
    # Extract metadata and process
    result = rasters.select(
        "path",
        rx.rst_boundingbox("tile").alias("bbox"),
        rx.rst_width("tile").alias("width"),
        rx.rst_height("tile").alias("height"),
        rx.rst_numbands("tile").alias("bands"),
        rx.rst_metadata("tile").alias("metadata")
    ).filter(
        "width > 1000 AND height > 1000"
    )
    
    result.limit(3).show()
    return result


def bng_cellarea_example(spark):
    """Calculate area of a BNG grid cell (returns square kilometres)."""
    from databricks.labs.gbx.gridx.bng import functions as bx
    bx.register(spark)
    
    # Calculate cell area (result in km²)
    area = spark.sql("SELECT gbx_bng_cellarea('TQ', 1000) as area_km2")
    area.show()
    return area


def bng_pointtocell_example(spark):
    """Convert point to BNG grid cell. Point must be WKT or WKB (GeoBrix does not accept st_point)."""
    from databricks.labs.gbx.gridx.bng import functions as bx
    from pyspark.sql.functions import lit
    
    bx.register(spark)
    # Point in BNG coordinates (eastings, northings); resolution '1km' or integer 3
    df = spark.range(1).select(
        bx.bng_pointascell(lit("POINT(530000 180000)"), lit("1km")).alias("bng_cell")
    )
    df.show()
    return df


def gridx_complete_example(spark):
    """Complete GridX example. Point as WKT (GeoBrix does not accept st_point)."""
    from databricks.labs.gbx.gridx.bng import functions as bx
    from pyspark.sql.functions import count
    
    # Register functions
    bx.register(spark)
    
    # Aggregate points by BNG cell (point as WKT in SQL)
    result = spark.sql("""
        SELECT
            gbx_bng_pointascell(concat('POINT(', cast(longitude as string), ' ', cast(latitude as string), ')'), 1000) as bng_cell,
            COUNT(*) as point_count,
            AVG(value) as avg_value
        FROM measurements
        WHERE country = 'GB'
        GROUP BY bng_cell
    """)
    
    result.write.mode("overwrite").saveAsTable("bng_aggregated")
    return result


def st_legacyaswkb_example(spark):
    """Convert legacy Mosaic geometry to WKB."""
    from databricks.labs.gbx.vectorx.jts.legacy import functions as vx
    from pyspark.sql.functions import expr
    
    # Register functions
    vx.register(spark)
    
    # Convert legacy geometries
    legacy = spark.table("legacy_mosaic_table")
    converted = legacy.select(
        "feature_id",
        vx.st_legacyaswkb("mosaic_geom").alias("wkb_geom")
    )
    
    # Convert to Databricks GEOMETRY type
    geometry_df = converted.select(
        "feature_id",
        "wkb_geom",
        expr("st_geomfromwkb(wkb_geom)").alias("geometry")
    )
    
    geometry_df.write.mode("overwrite").saveAsTable("converted_features")
    return geometry_df


def dataframe_select_operations(spark):
    """DataFrame select operations."""
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)
    
    raster_path = SAMPLE_NYC_RASTER
    df = spark.read.format("gdal").load(raster_path)
    
    # Single function
    result = df.select(rx.rst_boundingbox("tile"))
    
    # Multiple functions
    result = df.select(
        rx.rst_boundingbox("tile").alias("bbox"),
        rx.rst_width("tile").alias("width"),
        rx.rst_height("tile").alias("height")
    )
    
    # With column renaming
    result = df.select(
        "path",
        rx.rst_metadata("tile").alias("raster_metadata")
    )
    return result


def dataframe_filter_operations(spark):
    """DataFrame filter operations."""
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)
    
    raster_path = SAMPLE_NYC_RASTER
    df = spark.read.format("gdal").load(raster_path)
    
    # Filter based on GeoBrix function results
    result = df.filter(
        rx.rst_width("tile") > 1000
    )
    
    # Complex filters
    result = df.filter(
        (rx.rst_width("tile") > 1000) &
        (rx.rst_height("tile") > 1000) &
        (rx.rst_numbands("tile") >= 3)
    )
    return result


def dataframe_withcolumn_operations(spark):
    """DataFrame withColumn operations."""
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)
    
    raster_path = SAMPLE_NYC_RASTER
    df = spark.read.format("gdal").load(raster_path)
    
    # Add new columns
    result = df.withColumn("bbox", rx.rst_boundingbox("tile"))
    result = df.withColumn("width", rx.rst_width("tile"))
    result = df.withColumn("height", rx.rst_height("tile"))
    
    # Chain operations
    result = (
        df
        .withColumn("bbox", rx.rst_boundingbox("tile"))
        .withColumn("width", rx.rst_width("tile"))
        .withColumn("height", rx.rst_height("tile"))
    )
    return result


def using_with_sql(spark):
    """Using GeoBrix functions with SQL."""
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)
    
    raster_path = SAMPLE_NYC_RASTER
    rasters = spark.read.format("gdal").load(raster_path)
    # Create temp view
    rasters.createOrReplaceTempView("rasters")
    
    # Use in SQL
    result = spark.sql("""
        SELECT
            path,
            gbx_rst_boundingbox(tile) as bbox,
            gbx_rst_width(tile) as width,
            gbx_rst_height(tile) as height
        FROM rasters
        WHERE gbx_rst_width(tile) > 1000
    """)
    return result


def type_hints_and_ide_support(spark):
    """Type hints and IDE support example."""
    from pyspark.sql import DataFrame
    from databricks.labs.gbx.rasterx import functions as rx
    
    def process_rasters(df: DataFrame) -> DataFrame:
        """
        Process rasters and extract metadata.
        
        Args:
            df: DataFrame with 'tile' column
            
        Returns:
            DataFrame with extracted metadata
        """
        rx.register(df.sparkSession)
        
        return df.select(
            "path",
            rx.rst_boundingbox("tile").alias("bbox"),
            rx.rst_width("tile").alias("width"),
            rx.rst_height("tile").alias("height")
        )
    
    raster_path = SAMPLE_NYC_RASTER
    rasters = spark.read.format("gdal").load(raster_path)
    result = process_rasters(rasters)
    return result


def error_handling_example(spark):
    """Error handling example."""
    from databricks.labs.gbx.rasterx import functions as rx
    
    raster_path = SAMPLE_NYC_RASTER
    df = spark.read.format("gdal").load(raster_path)
    
    try:
        rx.register(spark)
        result = df.select(rx.rst_boundingbox("tile"))
        result.limit(3).show()
        return result
    except Exception as e:
        print(f"Error processing rasters: {e}")
        return None


# ============================================================================
# EXAMPLE OUTPUT (for docs "Example output" block via CodeFromTest outputConstant)
# Same style as quick-start: show-type result below each code block.
# ============================================================================

verify_installation_output = """
GeoBrix installed successfully
"""

import_pattern_rasterx_output = """
+--------------------------------------------------+----------------------------------+
|path                                              |bbox                              |
+--------------------------------------------------+----------------------------------+
|.../nyc_sentinel2/nyc_sentinel2_red.tif           |POLYGON ((-74.26 40.49, ...))     |
+--------------------------------------------------+----------------------------------+
"""

import_pattern_gridx_output = """
+----------+
|area_km2  |
+----------+
|1.0       |
+----------+
"""

import_pattern_vectorx_output = """
+-----------+
|wkb        |
+-----------+
|[BINARY]   |
+-----------+
"""

rst_boundingbox_example_output = """
+----------------------------------------------------------+----------------------------------+
|path                                                      |bbox                              |
+----------------------------------------------------------+----------------------------------+
|.../nyc/sentinel2/nyc_sentinel2_red.tif                   |POLYGON ((-74.26 40.49, ...))     |
+----------------------------------------------------------+----------------------------------+
"""

rst_width_example_output = """
+------+
|width |
+------+
|10980 |
+------+
"""

rst_height_example_output = """
+------+
|height|
+------+
|10980 |
+------+
"""

rst_numbands_example_output = """
+---------+
|num_bands|
+---------+
|1        |
+---------+
"""

rst_metadata_example_output = """
+------------------+
|metadata          |
+------------------+
|{driver=GTiff,...}|
+------------------+
"""

rst_srid_example_output = """
+-----+
|srid |
+-----+
|32618|
+-----+
"""

rst_clip_example_output = """
+----------------------------------+
|clipped_tile                      |
+----------------------------------+
|[STRUCT cellid, raster, metadata] |
+----------------------------------+
"""

rasterx_complete_example_output = """
+----------------------------------------------------------+-----+------+-----+------------------+
|path                                                      |bbox |width |...  |metadata          |
+----------------------------------------------------------+-----+------+-----+------------------+
|.../nyc/sentinel2/nyc_sentinel2_red.tif                   |...  |10980 |...  |{driver=GTiff,...}|
+----------------------------------------------------------+-----+------+-----+------------------+
"""

bng_cellarea_example_output = """
+----------+
|area_km2  |
+----------+
|1.0       |
+----------+
"""

bng_pointtocell_example_output = """
+----------+
|bng_cell  |
+----------+
|TQ 30 80  |
+----------+
"""

gridx_complete_example_output = """
+----------+-----------+---------+
|bng_cell  |point_count|avg_value|
+----------+-----------+---------+
|TQ3080    |42        |15.3      |
+----------+-----------+---------+
"""

st_legacyaswkb_example_output = """
+--------+--------+----------+
|feature_id|wkb_geom|geometry|
+--------+--------+----------+
|1       |[BINARY]|...       |
+--------+--------+----------+
"""

if __name__ == "__main__":
    # Test that all functions are importable
    print("✓ All Python API examples defined")
