"""
Tile Structure Documentation Examples

This module contains all Python code examples for the Tile Structure page.
Functions demonstrate accessing tile fields, working with binary/file-based tiles,
and various tile manipulation patterns.

Paths use sample data from mounted Volumes (see docs/docs/sample-data.mdx).
"""

from path_config import SAMPLE_DATA_BASE
SAMPLE_NYC_RASTER = f"{SAMPLE_DATA_BASE}/nyc/sentinel2/nyc_sentinel2_red.tif"
SAMPLE_NYC_RASTERS = f"{SAMPLE_DATA_BASE}/nyc/sentinel2/*.tif"

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
except ImportError:
    rx = None


# SQL constant for cellid example (use your sample raster path)
SQL_CELLID_NON_TESSELLATED = f"""-- Non-tessellated: cellid is null
SELECT tile.cellid 
FROM gdal.`{SAMPLE_NYC_RASTER}`;
-- Returns: null"""

SQL_CELLID_TESSELLATED = f"""-- Tessellated: cellid contains H3 cell ID
SELECT tile.cellid 
FROM (
  SELECT explode(gbx_rst_h3_tessellate(tile, 7)) as tile
  FROM gdal.`{SAMPLE_NYC_RASTER}`
);
-- Returns: 604189641255419903, 604189641255420159, ..."""


def access_path_and_binary(spark):
    """Access path from rst_fromfile and binary from GDAL reader."""
    from databricks.labs.gbx.rasterx import functions as rx
    
    # Access path from rst_fromfile (sample data path)
    df = spark.range(1).select(
        rx.rst_fromfile(f.lit(SAMPLE_NYC_RASTER), f.lit("GTiff")).alias("tile")
    )
    
    path_df = df.select(f.col("tile.raster").alias("raster_path"))
    # Returns: path string to the raster
    
    # Access binary from GDAL reader (sample data from mounted Volumes)
    df = spark.read.format("gdal").load(SAMPLE_NYC_RASTER)
    binary_df = df.select(f.col("tile.raster").alias("raster_binary"))
    # Returns: b'\x4d\x4d\x00\x2a...' (binary GeoTIFF data)
    
    return path_df, binary_df


def access_metadata_fields(spark):
    """Access metadata fields from tile."""
    df = spark.read.format("gdal").load(SAMPLE_NYC_RASTERS)
    
    metadata_df = df.select(
        f.col("tile.metadata").alias("metadata"),
        f.col("tile.metadata.driver").alias("driver"),
        f.col("tile.metadata.extension").alias("extension"),
        f.col("tile.metadata.size").alias("size")
    )
    
    # Returns:
    # metadata: {"driver": "GTiff", "extension": ".tif", "size": "2345678"}
    # driver: "GTiff"
    # extension: ".tif"
    # size: "2345678"
    return metadata_df


def accessing_tile_fields_python(spark):
    """Accessing Tile Fields in Python."""
    from pyspark.sql import functions as f
    from databricks.labs.gbx.rasterx import functions as rx
    
    df = spark.read.format("gdal").load(SAMPLE_NYC_RASTERS)
    
    # Access individual fields
    df.select(
        f.col("tile.cellid"),
        f.col("tile.raster"),
        f.col("tile.metadata"),
        f.col("tile.metadata.driver")
    )
    return df


SQL_ACCESSING_TILE_FIELDS = f"""SELECT 
    tile.cellid,
    tile.raster,
    tile.metadata,
    tile.metadata['driver'] as driver
FROM gdal.`{SAMPLE_NYC_RASTER}`;"""


def filtering_by_metadata(spark):
    """Filter tiles based on driver or other metadata."""
    df = spark.read.format("gdal").load(SAMPLE_NYC_RASTERS)
    
    # Filter by driver
    gtiff_only = df.filter(f.col("tile.metadata.driver") == "GTiff")
    
    # Filter by file extension
    tif_files = df.filter(f.col("tile.metadata.extension") == ".tif")
    return gtiff_only, tif_files


def using_tiles_in_custom_udfs(spark):
    """Access tile components for custom processing."""
    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType
    
    @udf(IntegerType())
    def get_raster_size(raster_binary, metadata):
        """Get size of raster data"""
        if metadata and "size" in metadata:
            return int(metadata["size"])
        elif raster_binary:
            return len(raster_binary)
        return 0
    
    df = spark.read.format("gdal").load(SAMPLE_NYC_RASTERS)
    df_with_size = df.withColumn(
        "data_size",
        get_raster_size(f.col("tile.raster"), f.col("tile.metadata"))
    )
    return df_with_size


def processing_binary_raster_data(spark):
    """Process binary raster data with rasterio."""
    from rasterio.io import MemoryFile
    from pyspark.sql.functions import udf
    from pyspark.sql.types import DoubleType
    
    @udf(DoubleType())
    def compute_mean_from_tile(raster_binary):
        """Compute mean from binary raster data"""
        import numpy as np
        
        if raster_binary is None:
            return None
        
        # Convert to bytes if needed
        tile_data = bytes(raster_binary)
        
        # Open with rasterio
        with MemoryFile(tile_data) as memfile:
            with memfile.open() as src:
                data = src.read(1)
                return float(np.mean(data))
    
    # Use with tiles from content or GDAL reader (sample data)
    df = spark.read.format("gdal").load(SAMPLE_NYC_RASTER)
    stats_df = df.withColumn(
        "mean_value",
        compute_mean_from_tile(f.col("tile.raster"))
    )
    return stats_df


def comparing_file_vs_binary_tiles(spark):
    """Comparing File-Based vs Binary Tiles."""
    from databricks.labs.gbx.rasterx import functions as rx
    
    # File-based tile (sample data path)
    file_tile = spark.range(1).select(
        rx.rst_fromfile(f.lit(SAMPLE_NYC_RASTER), f.lit("GTiff")).alias("tile")
    )
    
    file_tile.select(f.col("tile.raster")).show(truncate=False)
    # +----------------------------------------------------------+
    # |raster                                                    |
    # +----------------------------------------------------------+
    # |/Volumes/.../nyc/sentinel2/nyc_sentinel2_red.tif          |
    # +----------------------------------------------------------+
    
    # Binary tile (same file read as binary)
    binary_tile = spark.read.format("binaryFile").load(SAMPLE_NYC_RASTER).select(
        rx.rst_fromcontent(f.col("content"), f.lit("GTiff")).alias("tile")
    )
    
    binary_tile.select(f.length(f.col("tile.raster")).alias("size_bytes")).show()
    # +-----------+
    # |size_bytes |
    # +-----------+
    # |2345678    |
    # +-----------+
    return file_tile, binary_tile


def non_tessellated_tiles(spark):
    """Non-Tessellated Tiles."""
    df = spark.read.format("gdal").load(SAMPLE_NYC_RASTER)
    
    df.select(
        f.col("tile.cellid"),      # null
        f.col("tile.raster"),      # binary data
        f.col("tile.metadata")     # {driver: "GTiff", ...}
    ).show()
    return df


def tessellated_tiles(spark):
    """Tessellated Tiles."""
    from databricks.labs.gbx.rasterx import functions as rx
    
    df = spark.read.format("gdal").load(SAMPLE_NYC_RASTER).select(
        f.explode(rx.rst_h3_tessellate(f.col("tile"), f.lit(7))).alias("tile")
    )
    
    df.select(
        f.col("tile.cellid"),      # H3 cell ID (e.g., 604189641255419903)
        f.col("tile.raster"),      # binary data (clipped to cell)
        f.col("tile.metadata")     # {driver: "GTiff", RASTERX_CELL_ID: "604...", ...}
    ).show()
    return df


def best_practice_access_fields_efficiently(spark):
    """Best Practice: Access multiple fields in one select."""
    df = spark.read.format("gdal").load(SAMPLE_NYC_RASTERS)
    
    # ✅ Good: Access multiple fields in one select
    df.select(
        f.col("tile.cellid"),
        f.col("tile.metadata.driver"),
        f.col("tile.metadata.extension")
    )
    
    # ❌ Avoid: Multiple separate selects
    # df.select(f.col("tile.cellid"))
    # df.select(f.col("tile.metadata.driver"))
    return df


def best_practice_filter_early_on_metadata(spark):
    """Best Practice: Filter before expensive operations."""
    from databricks.labs.gbx.rasterx import functions as rx
    
    df = spark.read.format("gdal").load(SAMPLE_NYC_RASTERS)
    boundary = None  # placeholder
    
    # ✅ Good: Filter before expensive operations
    result = df.filter(f.col("tile.metadata.driver") == "GTiff") \
      .select(rx.rst_clip(f.col("tile"), boundary, f.lit(True)))
    
    # ❌ Avoid: Process then filter
    # df.select(rx.rst_clip(f.col("tile"), boundary, f.lit(True))) \
    #   .filter(f.col("tile.metadata.driver") == "GTiff")
    return result


def best_practice_use_accessor_functions(spark):
    """Best Practice: Use accessor functions when possible."""
    from databricks.labs.gbx.rasterx import functions as rx
    
    df = spark.read.format("gdal").load(SAMPLE_NYC_RASTERS)
    
    # ✅ Preferred: Use accessor functions
    result1 = df.select(rx.rst_metadata(f.col("tile")))
    
    # ✅ Also fine: Direct field access
    result2 = df.select(f.col("tile.metadata"))
    
    return result1, result2


def pattern_conditional_processing_based_on_metadata(spark):
    """Pattern 1: Conditional Processing Based on Metadata."""
    from pyspark.sql.functions import when
    from databricks.labs.gbx.rasterx import functions as rx
    
    df = spark.read.format("gdal").load(SAMPLE_NYC_RASTERS)
    aoi = None  # placeholder
    
    processed = df.withColumn(
        "result",
        when(f.col("tile.metadata.driver") == "GTiff", 
             rx.rst_clip(f.col("tile"), aoi, f.lit(True)))
        .when(f.col("tile.metadata.driver") == "NetCDF",
             rx.rst_subdatasets(f.col("tile")))
        .otherwise(f.col("tile"))
    )
    return processed


def pattern_joining_tiles_by_cell_id(spark):
    """Pattern 2: Joining Tiles by Cell ID."""
    from databricks.labs.gbx.rasterx import functions as rx
    
    # Tessellate two rasters to same grid (sample data)
    rasters1 = spark.read.format("gdal").load(SAMPLE_NYC_RASTER).select(
        f.explode(rx.rst_h3_tessellate(f.col("tile"), f.lit(7))).alias("tile1")
    )
    
    rasters2 = spark.read.format("gdal").load(SAMPLE_NYC_RASTER).select(
        f.explode(rx.rst_h3_tessellate(f.col("tile"), f.lit(7))).alias("tile2")
    )
    
    # Join on cellid
    joined = rasters1.join(
        rasters2,
        f.col("tile1.cellid") == f.col("tile2.cellid")
    )
    return joined


def pattern_extract_binary_for_external_processing(spark):
    """Pattern 3: Extract Binary for External Processing."""
    df = spark.read.format("gdal").load(SAMPLE_NYC_RASTER)
    
    # Extract binary rasters for download or external processing
    export_path = f"{SAMPLE_DATA_BASE}/export"
    export_df = df.select(
        f.col("path"),
        f.col("tile.raster").alias("raster_bytes")
    ).write.parquet(export_path)
    
    # Or iterate rows (path, binary) for external processing
    # for row in df.select("path", "tile.raster").collect(): ...
    return export_df


def performance_io_patterns(spark):
    """Performance: File-based for initial read, binary for processing."""
    from databricks.labs.gbx.rasterx import functions as rx
    
    # Pattern: File-based for initial read, binary for processing (sample data)
    df = spark.read.format("gdal").load(SAMPLE_NYC_RASTERS)
    aoi = None  # placeholder
    
    # Operations automatically handle deserialization
    processed = df.select(
        rx.rst_clip(f.col("tile"), aoi, f.lit(True))  # Reads from file as needed
    )
    
    # Materialize to binary for repeated operations
    cached = processed.select(
        rx.rst_fromcontent(
            rx.rst_tobinary(f.col("tile")),  # Convert to binary
            f.col("tile.metadata.driver")
        ).alias("tile")
    ).cache()
    return cached


def troubleshooting_cast_string_to_binary(spark):
    """Troubleshooting: Cannot cast string to binary."""
    df = spark.read.format("gdal").load(SAMPLE_NYC_RASTERS)
    
    # Check raster type
    df.select(f.col("tile.raster").cast("string")).show(truncate=False)
    # If shows path → file-based
    # If shows [binary] → binary-based
    return df


def troubleshooting_null_cellid(spark):
    """Troubleshooting: NullPointerException on cellid."""
    df = spark.read.format("gdal").load(SAMPLE_NYC_RASTERS)
    
    # Filter out non-tessellated tiles
    tessellated_only = df.filter(f.col("tile.cellid").isNotNull())
    return tessellated_only


# =============================================================================
# EXAMPLE OUTPUT (show-type result for docs, same style as quick-start)
# =============================================================================

SQL_CELLID_NON_TESSELLATED_output = """
+------+
|cellid|
+------+
|null  |
+------+
"""

SQL_CELLID_TESSELLATED_output = """
+-------------------+
|cellid             |
+-------------------+
|604189641255419903 |
+-------------------+
"""

access_path_and_binary_output = """
+----------------------------------------------------------+
|raster_path / raster_binary                               |
+----------------------------------------------------------+
|.../nyc/sentinel2/nyc_sentinel2_red.tif or [BINARY]       |
+----------------------------------------------------------+
"""

access_metadata_fields_output = """
+------------------+-------+----------+------+
|metadata          |driver |extension |size  |
+------------------+-------+----------+------+
|{driver=GTiff,...}|GTiff  |.tif      |...   |
+------------------+-------+----------+------+
"""

accessing_tile_fields_python_output = """
+------+--------+------------------+-------+
|cellid|raster  |metadata          |driver |
+------+--------+------------------+-------+
|null  |[BINARY]|{driver=GTiff,...}|GTiff  |
+------+--------+------------------+-------+
"""

SQL_ACCESSING_TILE_FIELDS_output = """
+------+--------+------------------+-------+
|cellid|raster  |metadata          |driver |
+------+--------+------------------+-------+
|null  |[BINARY]|{driver=GTiff,...}|GTiff  |
+------+--------+------------------+-------+
"""

filtering_by_metadata_output = """
Filtered DataFrame (e.g. driver = GTiff or extension = .tif).
"""

using_tiles_in_custom_udfs_output = """
+----+---------+
|path|data_size|
+----+---------+
|... |12345678 |
+----+---------+
"""

processing_binary_raster_data_output = """
+----+----------+
|path|mean_value|
+----+----------+
|... |0.42      |
+----+----------+
"""

comparing_file_vs_binary_tiles_output = """
+----------------------------------------------------------+
|raster (path) or size_bytes                               |
+----------------------------------------------------------+
|/Volumes/.../nyc_sentinel2_red.tif  or  2345678           |
+----------------------------------------------------------+
"""

non_tessellated_tiles_output = """
+----+--------+------------------+
|cellid|raster |metadata         |
+----+--------+------------------+
|null|[BINARY]|{driver=GTiff,...}|
+----+--------+------------------+
"""

tessellated_tiles_output = """
+-------------------+--------+------------------+
|cellid             |raster  |metadata          |
+-------------------+--------+------------------+
|604189641255419903 |[BINARY]|{RASTERX_CELL_ID..|
+-------------------+--------+------------------+
"""


if __name__ == "__main__":
    # Test that all functions are importable
    print("✓ All tile structure examples defined")
