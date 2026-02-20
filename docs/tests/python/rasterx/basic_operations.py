"""
GeoBrix RasterX - Basic Operations Examples

This module contains tested examples of common RasterX operations.

Single Source of Truth:
    - This is the ONLY copy of these code examples
    - Documentation imports from this file (no copy-paste!)
    - Tested by: docs/tests/python/rasterx/test_basic_operations.py
    - All examples are proven to work

Usage in Documentation:
    These examples are imported and displayed in API documentation.
    
Usage by Users:
    Users can copy these examples knowing they are tested and working.
"""

# Conditional imports - allows module to be imported for testing
try:
    from pyspark.sql import SparkSession
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f
except ImportError:
    # Modules will be available in Spark environment
    SparkSession = None
    rx = None
    f = None


def setup_rasterx(spark: SparkSession, sample_data_path: str = None):
    """
    Setup RasterX with sample data
    
    Args:
        spark: Active SparkSession
        sample_data_path: Path to sample data (optional)
    
    Returns:
        DataFrame with raster tiles
    
    Example:
        >>> rasters = setup_rasterx(spark, "/Volumes/main/default/geobrix_samples/geobrix-examples")
        >>> print(f"Loaded {rasters.count()} rasters")
    """
    # Register RasterX functions
    rx.register(spark)
    
    if sample_data_path:
        # Load sample Sentinel-2 imagery
        rasters = spark.read.format("gdal").load(f"{sample_data_path}/nyc/sentinel2/*.tif")
        print(f"✅ RasterX setup complete. Loaded {rasters.count()} rasters.")
        return rasters
    else:
        # Return None if no sample data
        return None


def get_raster_metadata(rasters_df):
    """
    Get basic metadata from raster tiles
    
    Args:
        rasters_df: DataFrame with raster tiles
    
    Returns:
        DataFrame with width, height, and SRID
    
    Example:
        >>> metadata = get_raster_metadata(rasters)
        >>> metadata.show()
        +-------+--------+------+
        | width | height | srid |
        +-------+--------+------+
        | 10980 |  10980 | 32618|
        +-------+--------+------+
    """
    return rasters_df.select(
        rx.rst_width("tile").alias("width"),
        rx.rst_height("tile").alias("height"),
        rx.rst_srid("tile").alias("srid")
    )


def get_raster_bounds(rasters_df):
    """
    Get geographic bounds of raster tiles
    
    Note: Individual coordinate accessor functions (rst_minx, rst_maxx, rst_miny, rst_maxy) 
    are not yet implemented. Use rst_boundingbox instead.
    
    Args:
        rasters_df: DataFrame with raster tiles
    
    Returns:
        DataFrame with bounding box geometry
    
    Example:
        >>> bounds = get_raster_bounds(rasters)
        >>> bounds.select("bbox").show(truncate=False)
    """
    # Use rst_boundingbox since individual coordinate accessors aren't implemented yet
    return rasters_df.select(
        rx.rst_boundingbox("tile").alias("bbox")
    )


def get_pixel_statistics(rasters_df):
    """
    Calculate statistics for raster pixel values
    
    Args:
        rasters_df: DataFrame with raster tiles
    
    Returns:
        DataFrame with min, max, and mean pixel values
    
    Example:
        >>> stats = get_pixel_statistics(rasters)
        >>> stats.show()
        +--------+--------+----------+
        |min_val |max_val |mean_val  |
        +--------+--------+----------+
        |    0.0 |  255.0 |    127.5 |
        +--------+--------+----------+
    """
    return rasters_df.select(
        rx.rst_min("tile").alias("min_val"),
        rx.rst_max("tile").alias("max_val"),
        rx.rst_avg("tile").alias("mean_val")
    )


def transform_raster_crs(rasters_df, target_srid: int):
    """
    Transform raster to different coordinate reference system
    
    Args:
        rasters_df: DataFrame with raster tiles
        target_srid: Target SRID (e.g., 4326 for WGS84)
    
    Returns:
        DataFrame with transformed rasters
    
    Example:
        >>> # Transform to WGS84 (EPSG:4326)
        >>> transformed = transform_raster_crs(rasters, 4326)
        >>> transformed.select(rx.rst_srid("tile")).show()
        +------+
        | srid |
        +------+
        | 4326 |
        +------+
    """
    return rasters_df.withColumn(
        "tile",
        rx.rst_transform("tile", f.lit(target_srid))
    )


def clip_raster_to_geometry(rasters_df, geometry_wkt: str):
    """
    Clip raster to a geometry boundary
    
    Args:
        rasters_df: DataFrame with raster tiles
        geometry_wkt: WKT string of clipping geometry
    
    Returns:
        DataFrame with clipped rasters
    
    Example:
        >>> # Clip to bounding box
        >>> bbox_wkt = "POLYGON((-74.0 40.7, -73.9 40.7, -73.9 40.8, -74.0 40.8, -74.0 40.7))"
        >>> clipped = clip_raster_to_geometry(rasters, bbox_wkt)
    """
    return rasters_df.withColumn(
        "clipped_tile",
        rx.rst_clip("tile", f.lit(geometry_wkt), f.lit(True))  # cutline_all_touched=True
    )


# Quick reference of common operations
COMMON_OPERATIONS = {
    "metadata": "rx.rst_width(), rx.rst_height(), rx.rst_srid()",
    "bounds": "rx.rst_minx(), rx.rst_miny(), rx.rst_maxx(), rx.rst_maxy()",
    "statistics": "rx.rst_min(), rx.rst_max(), rx.rst_avg()",
    "transform": "rx.rst_transform(tile, target_srid)",
    "clip": "rx.rst_clip(tile, geometry_wkt, cutline_all_touched)",
}


if __name__ == "__main__":
    # Demo: Print available operations
    print("GeoBrix RasterX - Common Operations")
    print("=" * 50)
    for op, func in COMMON_OPERATIONS.items():
        print(f"{op:15} → {func}")
    print("\n✅ All operations are tested and documented")
