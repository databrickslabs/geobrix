"""
GeoBrix Examples Overview

This module contains quick example code for the examples overview page.
All examples are tested and serve as the single source of truth for docs.
"""

# Conditional imports for compatibility
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import expr
    PYSPARK_AVAILABLE = True
except ImportError:
    SparkSession = None
    DataFrame = None
    PYSPARK_AVAILABLE = False
    def expr(x):
        return None


def example_read_catalog_rasters(spark):
    """
    Example 1: Read and catalog raster files.
    
    Demonstrates reading rasters with GDAL and building a catalog
    with metadata.
    """
    from databricks.labs.gbx.rasterx import functions as rx
    
    rx.register(spark)
    
    # Read rasters
    rasters = spark.read.format("gdal").load("/data/satellite")
    
    # Build catalog
    catalog = rasters.select(
        "path",
        rx.rst_boundingbox("tile").alias("bounds"),
        rx.rst_width("tile").alias("width"),
        rx.rst_height("tile").alias("height"),
        rx.rst_metadata("tile").alias("metadata")
    )
    
    catalog.write.mode("overwrite").saveAsTable("raster_catalog")
    return catalog


def example_spatial_aggregation_bng(spark):
    """
    Example 2: Spatial aggregation with British National Grid.
    
    Aggregate point data by BNG grid cells.
    """
    from databricks.labs.gbx.gridx.bng import functions as bx
    
    bx.register(spark)
    
    # Aggregate points by BNG cell
    result = spark.sql("""
        SELECT
            gbx_bng_pointtocell(st_point(longitude, latitude), 1000) as bng_cell,
            COUNT(*) as count,
            AVG(value) as avg_value
        FROM measurements
        WHERE country = 'GB'
        GROUP BY bng_cell
    """)
    
    result.write.mode("overwrite").saveAsTable("bng_aggregated")
    return result


def example_migrate_from_mosaic(spark):
    """
    Example 3: Migrate from legacy Mosaic geometries.
    
    Convert legacy Mosaic geometry columns to modern WKB/GEOMETRY format.
    """
    from databricks.labs.gbx.vectorx.jts.legacy import functions as vx
    
    vx.register(spark)
    
    # Convert legacy geometries
    legacy = spark.table("legacy_mosaic_table")
    
    migrated = legacy.select(
        "*",
        expr("st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))").alias("geometry")
    ).drop("mosaic_geom")
    
    migrated.write.mode("overwrite").saveAsTable("migrated_table")
    return migrated


if __name__ == "__main__":
    print("GeoBrix Examples Overview")
    print("=" * 50)
    print(f"Total functions: {len([name for name in dir() if callable(globals()[name]) and not name.startswith('_')])}")
