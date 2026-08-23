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

try:
    from databricks.labs.gbx.pyrx import functions as pyrx
except ImportError:
    pyrx = None


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
    """Distinguish virtual from materialized tiles by inspecting path and raster.

    A **virtual** tile (default for the light tier) has ``tile.raster = null``
    and ``tile.path`` set to the source file path — it carries no bytes, only
    a reference. A **materialized** tile is the opposite: ``tile.raster``
    holds the encoded bytes and ``tile.path`` is null. Checking both fields
    is the correct way to classify a tile at runtime.
    """
    from databricks.labs.gbx.pyrx import functions as pyrx

    # Light-tier rst_fromfile returns a VIRTUAL tile by default:
    # raster=null, path set to the source path.
    virtual_df = spark.range(1).select(
        pyrx.rst_fromfile(f.lit(SAMPLE_NYC_RASTER), f.lit("GTiff")).alias("tile")
    ).select(
        f.col("tile.path").alias("path"),          # non-null: source file path
        f.col("tile.raster").isNull().alias("is_virtual"),  # True: bytes-free
    )
    # Returns: path=/Volumes/.../nyc_sentinel2_red.tif, is_virtual=true

    # Materialized tile: binaryFile reader + rst_fromcontent embeds bytes.
    # tile.raster holds the encoded GeoTIFF; tile.path is null.
    materialized_df = (
        spark.read.format("binaryFile").load(SAMPLE_NYC_RASTER)
        .select(
            rx.rst_fromcontent(f.col("content"), f.lit("GTiff")).alias("tile")
        )
        .select(
            f.col("tile.path").isNull().alias("path_null"),    # True: no source path
            f.col("tile.raster").isNull().alias("is_virtual"), # False: bytes present
        )
    )
    # Returns: path_null=true, is_virtual=false

    return virtual_df, materialized_df


def tile_path_mode_storage(spark):
    """Inspect tile.path_mode to determine how a tile's pixels are stored.

    ``tile.path_mode`` records the storage model for a virtual tile's backing
    data:

    - ``null`` — either a materialized tile (``raster`` bytes present) or a
      plain FUSE-path virtual tile (path points to a Volume or local path).
    - ``"external"`` — FILE EXTERNAL: the tile is backed by a governed
      Databricks FILE column (externally managed lifecycle).
    - ``"managed"``  — FILE MANAGED: like external, but lifecycle is
      managed by Delta.

    Because both materialized tiles and plain virtual tiles have
    ``path_mode = null``, use ``tile.raster is null`` (or ``tile.path``) to
    tell them apart.
    """
    from databricks.labs.gbx.pyrx import functions as pyrx

    # Virtual tile (plain FUSE path) — raster null, path set, path_mode null
    virtual_df = spark.range(1).select(
        pyrx.rst_fromfile(f.lit(SAMPLE_NYC_RASTER), f.lit("GTiff")).alias("tile")
    ).select(
        f.col("tile.path_mode").alias("path_mode"),          # null (plain FUSE virtual)
        f.col("tile.raster").isNull().alias("is_virtual"),   # True
        f.col("tile.path").isNull().alias("path_null"),      # False
    )
    # Returns: path_mode=null, is_virtual=true, path_null=false

    # Materialized tile — raster bytes present, path null, path_mode null
    materialized_df = (
        spark.read.format("binaryFile").load(SAMPLE_NYC_RASTER)
        .select(
            rx.rst_fromcontent(f.col("content"), f.lit("GTiff")).alias("tile")
        )
        .select(
            f.col("tile.path_mode").alias("path_mode"),          # null (materialized)
            f.col("tile.raster").isNull().alias("is_virtual"),   # False
            f.col("tile.path").isNull().alias("path_null"),      # True
        )
    )
    # Returns: path_mode=null, is_virtual=false, path_null=true

    return virtual_df, materialized_df


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


def comparing_fromfile_vs_fromcontent_tiles(spark):
    """Compare rst_fromfile (virtual by default) and rst_fromcontent (always materialized).

    In the light tier, ``rst_fromfile`` returns a **virtual** tile — bytes-free,
    path+window set. Pass ``materialize=True`` to force bytes. ``rst_fromcontent``
    always embeds the bytes you supply, returning a materialized tile in both tiers.
    """
    from databricks.labs.gbx.pyrx import functions as pyrx

    # Light-tier rst_fromfile: VIRTUAL tile (raster=null, path set)
    fromfile_tile = spark.range(1).select(
        pyrx.rst_fromfile(f.lit(SAMPLE_NYC_RASTER), f.lit("GTiff")).alias("tile")
    )

    fromfile_tile.select(
        f.col("tile.raster").isNull().alias("raster_null"),  # True: virtual tile
        f.col("tile.path").isNull().alias("path_null"),      # False: path set
    ).show()
    # +-----------+---------+
    # |raster_null|path_null|
    # +-----------+---------+
    # |true       |false    |
    # +-----------+---------+

    # rst_fromcontent takes bytes you already have in a column (e.g. from binaryFile)
    fromcontent_tile = spark.read.format("binaryFile").load(SAMPLE_NYC_RASTER).select(
        pyrx.rst_fromcontent(f.col("content"), f.lit("GTiff")).alias("tile")
    )

    fromcontent_tile.select(
        f.col("tile.raster").isNull().alias("raster_null"),  # False: bytes present
        f.col("tile.path").isNull().alias("path_null"),      # True: no source path
    ).show()
    # +-----------+---------+
    # |raster_null|path_null|
    # +-----------+---------+
    # |false      |true     |
    # +-----------+---------+
    return fromfile_tile, fromcontent_tile


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
        f.col("tile.metadata")     # {driver: "GTiff", gridSystem: "h3", width: "...", ...}
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
    """Performance: tiles carry binary content, so cache after expensive ops."""
    from databricks.labs.gbx.rasterx import functions as rx

    df = spark.read.format("gdal").load(SAMPLE_NYC_RASTERS)
    aoi = None  # placeholder

    # Tile ops pass binary content through the plan
    processed = df.select(
        rx.rst_clip(f.col("tile"), aoi, f.lit(True)).alias("tile")
    )

    # Cache the materialized binary tiles for repeated operations
    cached = processed.cache()
    return cached


def troubleshooting_inspect_raster_field(spark):
    """Troubleshooting: inspect the raster field's type and size."""
    df = spark.read.format("gdal").load(SAMPLE_NYC_RASTERS)

    # Raster is always BinaryType; length gives the on-wire byte size.
    df.select(
        f.length(f.col("tile.raster")).alias("raster_bytes"),
        f.col("tile.metadata.size").alias("reported_size"),
    ).show()
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
Virtual tile — raster null, path set:
+-----------------------------------------------------+----------+
|path                                                 |is_virtual|
+-----------------------------------------------------+----------+
|/Volumes/main/.../nyc_sentinel2_red.tif              |true      |
+-----------------------------------------------------+----------+

Materialized tile — raster bytes present, path null:
+---------+----------+
|path_null|is_virtual|
+---------+----------+
|true     |false     |
+---------+----------+
"""

tile_path_mode_storage_output = """
Virtual tile (plain FUSE) — path_mode null, raster null:
+---------+----------+---------+
|path_mode|is_virtual|path_null|
+---------+----------+---------+
|null     |true      |false    |
+---------+----------+---------+

Materialized tile — path_mode null, raster bytes present:
+---------+----------+---------+
|path_mode|is_virtual|path_null|
+---------+----------+---------+
|null     |false     |true     |
+---------+----------+---------+
(path_mode is "external" for FILE EXTERNAL tiles, "managed" for FILE MANAGED tiles)
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

comparing_fromfile_vs_fromcontent_tiles_output = """
rst_fromfile (light tier) — VIRTUAL tile:
+-----------+---------+
|raster_null|path_null|
+-----------+---------+
|true       |false    |
+-----------+---------+

rst_fromcontent — MATERIALIZED tile (bytes embedded):
+-----------+---------+
|raster_null|path_null|
+-----------+---------+
|false      |true     |
+-----------+---------+
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
