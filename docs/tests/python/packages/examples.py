"""
GeoBrix Packages Examples

This module contains all code examples for GeoBrix packages documentation.
All examples are tested and serve as the single source of truth for docs.
"""

import os
import sys
from pathlib import Path

# Ensure path_config is importable when run from packages/
_python = Path(__file__).resolve().parent.parent
if str(_python) not in sys.path:
    sys.path.insert(0, str(_python))

# Sample data path at runtime (path_config or GEOBRIX_SAMPLE_RASTER env)
try:
    from path_config import SAMPLE_DATA_BASE
    _default_raster = f"{SAMPLE_DATA_BASE}/nyc/sentinel2/nyc_sentinel2_red.tif"
except ImportError:
    _default_raster = "/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif"
SAMPLE_RASTER_PATH = os.environ.get("GEOBRIX_SAMPLE_RASTER", _default_raster)

# Conditional imports for compatibility
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import expr, count, avg, explode, col, lit
    PYSPARK_AVAILABLE = True
except ImportError:
    SparkSession = None
    DataFrame = None
    PYSPARK_AVAILABLE = False
    def expr(x):
        return None
    def count():
        return None
    def avg(x):
        return None
    def explode(x):
        return None
    def col(x):
        return None
    def lit(x):
        return None


# ============================================================================
# Packages Overview Examples
# ============================================================================

def register_all_packages(spark):
    """
    Register all GeoBrix packages at once.
    
    Imports and registers RasterX, GridX (BNG), and VectorX functions.
    """
    # Register all packages
    from databricks.labs.gbx.rasterx import functions as rx
    from databricks.labs.gbx.gridx.bng import functions as bx
    from databricks.labs.gbx.vectorx.jts.legacy import functions as vx
    
    rx.register(spark)
    bx.register(spark)
    vx.register(spark)
    
    return rx, bx, vx


def register_only_rasterx(spark):
    """
    Register only RasterX functions.
    
    Use when you only need raster processing capabilities.
    """
    # Only register RasterX
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)
    
    return rx


# ============================================================================
# RasterX Examples
# ============================================================================

def rasterx_basic_usage(spark):
    """
    Basic RasterX usage: read rasters and extract metadata.

    Uses sample data path (Volumes). Displays width, height, bands, SRID.
    """
    from databricks.labs.gbx.rasterx import functions as rx

    # Sample data path (see Sample Data guide; use your Volume path if different)
    raster_path = SAMPLE_RASTER_PATH

    rx.register(spark)

    raster_df = spark.read.format("gdal").load(raster_path)
    
    metadata_df = raster_df.select(
        "source",
        rx.rst_width("tile").alias("width"),
        rx.rst_height("tile").alias("height"),
        rx.rst_numbands("tile").alias("bands"),
        rx.rst_srid("tile").alias("srid"),
    )
    metadata_df.show()
    return metadata_df


rasterx_basic_usage_output = """+--------------------+-----+------+-----+----+
|source              |width|height|bands|srid|
+--------------------+-----+------+-----+----+
|.../nyc_sentinel2...|10980|10980 |1    |4326|
+--------------------+-----+------+-----+----+"""


def rasterx_clip_raster(spark):
    """
    Clip raster by geometry using RasterX.
    
    Shows how to use rst_clip function with a geometry.
    """
    from databricks.labs.gbx.rasterx import functions as rx
    
    rx.register(spark)
    
    raster_df = spark.read.format("gdal").load("/path/to/geotiffs")
    
    # Clip raster by geometry (WKT or WKB; GeoBrix does not accept st_geomfromtext)
    clipped_df = raster_df.select(
        rx.rst_clip("tile", lit("POLYGON((-122 37, -122 38, -121 38, -121 37, -122 37))"), lit(True)).alias("clipped_tile")
    )
    
    return clipped_df


def rasterx_cataloging_workflow(spark):
    """
    Create a catalog of raster files with metadata.
    
    Complete workflow for building a raster metadata catalog.
    """
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)
    
    # Read all rasters
    rasters = spark.read.format("gdal").load("/data/satellite_imagery")
    
    # Build catalog
    catalog = rasters.select(
        "path",
        rx.rst_boundingbox("tile").alias("bounds"),
        rx.rst_width("tile").alias("width"),
        rx.rst_height("tile").alias("height"),
        rx.rst_numbands("tile").alias("bands"),
        rx.rst_srid("tile").alias("crs"),
        rx.rst_metadata("tile").alias("metadata")
    )
    
    # Save as Delta table
    catalog.write.mode("overwrite").saveAsTable("raster_catalog")
    return catalog


def rasterx_processing_pipeline(spark):
    """
    Complete raster processing pipeline.
    
    Clip and extract metadata from rasters.
    """
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)
    
    # Read rasters
    rasters = spark.read.format("gdal").load("/data/input")
    
    # Process: clip, extract metadata (aoi_wkt column holds WKT; GeoBrix does not accept st_geomfromwkt)
    processed = rasters.select(
        "path",
        rx.rst_clip("tile", col("aoi_wkt"), lit(True)).alias("clipped")
    ).select(
        "path",
        "clipped",
        rx.rst_metadata("clipped").alias("output_metadata")
    )
    
    # Write results
    processed.write.mode("overwrite").format("delta").save("/data/processed")
    return processed


def rasterx_multiband_analysis(spark):
    """
    Multi-band raster analysis workflow.
    
    Separate and extract individual bands from multi-band raster.
    """
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)
    
    # Read multi-band raster (e.g., Landsat)
    landsat = spark.read.format("gdal").load("/data/landsat")
    
    # Separate bands
    bands = landsat.select(
        "path",
        rx.rst_separatebands("tile").alias("bands")
    )
    
    # Extract individual bands
    red_band = bands.select(
        "path",
        rx.rst_getband("bands", 3).alias("red")
    )
    
    nir_band = bands.select(
        "path",
        rx.rst_getband("bands", 4).alias("nir")
    )
    
    return red_band, nir_band


def rasterx_tiling_performance(spark):
    """
    Raster tiling for improved performance.
    
    Tessellate large rasters into smaller tiles for parallel processing.
    """
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)
    
    # Read large raster
    large_raster = spark.read.format("gdal").option("sizeInMB", "16").load("/data/large.tif")
    
    # Tessellate into smaller tiles
    tiles = large_raster.select(
        rx.rst_tessellate("tile", 256).alias("small_tile")
    )
    
    # Process tiles in parallel
    processed_tiles = tiles.select(
        # Your processing here
        rx.rst_boundingbox("small_tile").alias("tile_bounds")
    )
    
    return processed_tiles


def rasterx_delta_integration(spark):
    """
    Save processed rasters to Delta Lake.
    
    Integration example with Delta and Unity Catalog.
    """
    catalog = None  # Placeholder - would be actual catalog DataFrame
    rasters = None  # Placeholder - would be actual rasters DataFrame
    
    # Save raster metadata to Delta
    if catalog is not None:
        catalog.write.mode("overwrite").format("delta").saveAsTable("raster_metadata")
    
    # Save binary raster data
    if rasters is not None:
        rasters.write.mode("overwrite").format("delta").save("/data/rasters_delta")
    
    # Write to Unity Catalog
    if catalog is not None:
        catalog.write.mode("overwrite").saveAsTable("catalog.schema.raster_catalog")
    
    return True


# ============================================================================
# VectorX Examples
# ============================================================================

def vectorx_basic_migration(spark):
    """
    Basic VectorX usage: migrate legacy Mosaic geometries.
    
    Convert legacy Mosaic geometry to WKB for use with Databricks spatial functions.
    """
    from databricks.labs.gbx.vectorx.jts.legacy import functions as vx
    
    # Register VectorX functions
    vx.register(spark)
    
    # Convert legacy Mosaic geometries
    legacy_table = spark.table("old_mosaic_features")
    
    migrated = legacy_table.select(
        "*",
        expr("st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))").alias("geometry")
    ).drop("mosaic_geom")
    
    # Now use Databricks built-in functions
    result = migrated.select(
        "feature_id",
        "geometry",
        expr("st_area(geometry)").alias("area"),
        expr("st_centroid(geometry)").alias("centroid")
    )
    
    result.write.mode("overwrite").saveAsTable("migrated_features")
    return result


def vectorx_sql_migration(spark):
    """
    Migrate legacy Mosaic data using SQL.
    
    SQL-based approach to converting Mosaic geometries.
    """
    from databricks.labs.gbx.vectorx.jts.legacy import functions as vx
    vx.register(spark)
    
    # Use SQL for migration
    result = spark.sql("""
        CREATE OR REPLACE TABLE modern_features AS
        SELECT
            feature_id,
            properties,
            st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom)) as geometry
        FROM legacy_mosaic_table
    """)
    
    return result


def vectorx_transition_validation(spark):
    """
    Support both old and new formats during migration.
    
    Keep both formats for validation during transition.
    """
    from databricks.labs.gbx.vectorx.jts.legacy import functions as vx
    vx.register(spark)
    
    legacy_table = spark.table("legacy_table")
    
    # Keep both for validation
    transitional = legacy_table.select(
        "*",
        vx.st_legacyaswkb("mosaic_geom").alias("wkb_geom"),
        expr("st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom))").alias("geometry")
    )
    
    # Validate conversion
    validation = transitional.select(
        "feature_id",
        expr("st_isvalid(geometry)").alias("is_valid"),
        expr("st_geometrytype(geometry)").alias("geom_type")
    )
    
    return validation


def vectorx_enable_spatial_analysis(spark):
    """
    Enable modern spatial analysis after conversion.
    
    Examples of using Databricks spatial functions after migration.
    """
    # This would be done in SQL after migration
    # Showing the SQL patterns that become available
    
    # Spatial joins
    result1 = spark.sql("""
        SELECT a.id, b.id
        FROM migrated_features a
        JOIN other_features b
          ON st_intersects(a.geometry, b.geometry)
    """)
    
    # Spatial aggregations
    result2 = spark.sql("""
        SELECT 
            region,
            st_union_agg(geometry) as merged_geometry,
            COUNT(*) as feature_count
        FROM migrated_features
        GROUP BY region
    """)
    
    # Spatial operations
    result3 = spark.sql("""
        SELECT
            feature_id,
            st_buffer(geometry, 100) as buffered,
            st_envelope(geometry) as bbox,
            st_area(geometry) as area_sqm
        FROM migrated_features
    """)
    
    return result1, result2, result3


def vectorx_migration_backup(spark):
    """
    Step 1: Backup legacy table before migration.
    
    Always create a backup before converting data.
    """
    # Create backup of legacy table
    spark.sql("CREATE TABLE legacy_backup AS SELECT * FROM legacy_table")
    return True


def vectorx_migration_convert(spark):
    """
    Step 2: Convert legacy geometries to modern format.
    
    Perform the actual conversion from Mosaic to WKB/GEOMETRY.
    """
    from databricks.labs.gbx.vectorx.jts.legacy import functions as vx
    vx.register(spark)
    
    converted = spark.sql("""
        SELECT
            *,
            st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom)) as geometry
        FROM legacy_table
    """)
    
    return converted


def vectorx_migration_validate(spark, converted):
    """
    Step 3: Validate converted data.
    
    Check for conversion issues and validate geometries.
    """
    # Check for issues
    validation = converted.select(
        expr("COUNT(*) as total"),
        expr("COUNT(geometry) as with_geometry"),
        expr("SUM(CASE WHEN st_isvalid(geometry) THEN 1 ELSE 0 END) as valid")
    )
    validation.show()
    return validation


def vectorx_complete_migration_example(spark):
    """
    Complete migration workflow from start to finish.
    
    Full example showing all steps of a Mosaic to modern format migration.
    """
    from databricks.labs.gbx.vectorx.jts.legacy import functions as vx
    
    # Register
    vx.register(spark)
    
    # Read legacy data
    legacy = spark.table("production.legacy_mosaic_parcels")
    
    # Convert and enrich
    migrated = legacy.select(
        "parcel_id",
        "owner",
        "land_use",
        "assessed_value",
        vx.st_legacyaswkb("mosaic_geom").alias("wkb_geom")
    ).select(
        "*",
        expr("st_geomfromwkb(wkb_geom)").alias("geometry")
    ).select(
        "parcel_id",
        "owner",
        "land_use",
        "assessed_value",
        "geometry",
        # Add spatial metrics
        expr("st_area(geometry)").alias("area_sqm"),
        expr("st_perimeter(geometry)").alias("perimeter_m"),
        expr("st_centroid(geometry)").alias("centroid")
    )
    
    # Validate
    print(f"Total records: {migrated.count()}")
    print(f"Valid geometries: {migrated.filter('st_isvalid(geometry)').count()}")
    
    # Save
    migrated.write.mode("overwrite").saveAsTable("production.modern_parcels")
    
    # Optimize for spatial queries
    spark.sql("OPTIMIZE production.modern_parcels ZORDER BY (geometry)")
    
    print("✅ Migration complete!")
    return migrated


# ============================================================================
# GridX (BNG) Examples
# ============================================================================

def gridx_basic_usage(spark):
    """
    Basic GridX BNG usage: convert a point to a BNG cell and get that cell's area.
    The point must be in BNG coordinates (eastings, northings, EPSG:27700), not WGS84.
    Resolution can be given in metres (e.g. 1000 for 1 km). Register BNG functions first.
    """
    from databricks.labs.gbx.gridx.bng import functions as bx

    bx.register(spark)

    # London (TQ area) in BNG: easting 530000, northing 180000
    # Resolution: BNG resolution index (3 = 1 km) or string ('1km'). gbx_bng_cellarea returns km².
    bng_cells = spark.sql(
        """
        SELECT
          gbx_bng_pointascell('POINT(530000 180000)', '1km') as bng_cell,
          gbx_bng_cellarea(gbx_bng_pointascell('POINT(530000 180000)', '1km')) as cell_area_km2
        """
    )
    bng_cells.show()
    return bng_cells


gridx_basic_usage_output = """+----------+--------------+
|bng_cell  |cell_area_km2 |
+----------+--------------+
|TQ3080    |1.0           |
+----------+--------------+"""


def gridx_spatial_aggregation(spark):
    """
    Spatial aggregation using BNG grid cells.
    
    Aggregate data points into BNG grid cells.
    """
    from databricks.labs.gbx.gridx.bng import functions as bx
    bx.register(spark)
    
    # Aggregate points by BNG cell
    aggregated = spark.sql("""
        SELECT
            gbx_bng_pointtocell(st_point(longitude, latitude), 1000) as bng_cell,
            COUNT(*) as point_count,
            AVG(value) as avg_value
        FROM measurements
        WHERE country = 'GB'
        GROUP BY bng_cell
    """)
    
    aggregated.show()
    return aggregated


def gridx_grid_based_joins(spark):
    """
    Grid-based spatial joins using BNG indexing.
    
    Join datasets using BNG grid cells for efficient spatial joins.
    """
    from databricks.labs.gbx.gridx.bng import functions as bx
    bx.register(spark)
    
    # Index both datasets with BNG
    locations_indexed = spark.sql("""
        SELECT
            *,
            gbx_bng_pointtocell(st_point(lon, lat), 1000) as bng_cell
        FROM locations
    """)
    
    poi_indexed = spark.sql("""
        SELECT
            *,
            gbx_bng_pointtocell(st_point(lon, lat), 1000) as bng_cell
        FROM points_of_interest
    """)
    
    # Join on BNG cell
    joined = locations_indexed.join(
        poi_indexed,
        on="bng_cell",
        how="inner"
    )
    
    joined.show()
    return joined


def gridx_multi_resolution_analysis(spark):
    """
    Analyze data at multiple BNG resolutions.
    
    Create and analyze data at different grid resolutions (10km, 1km, 100m).
    """
    from databricks.labs.gbx.gridx.bng import functions as bx
    bx.register(spark)
    
    # Create multi-resolution grid
    multi_res = spark.sql("""
        SELECT
            location_id,
            latitude,
            longitude,
            gbx_bng_pointtocell(st_point(longitude, latitude), 10000) as bng_10km,
            gbx_bng_pointtocell(st_point(longitude, latitude), 1000) as bng_1km,
            gbx_bng_pointtocell(st_point(longitude, latitude), 100) as bng_100m
        FROM uk_locations
    """)
    
    # Aggregate at different resolutions
    agg_10km = multi_res.groupBy("bng_10km").count()
    agg_1km = multi_res.groupBy("bng_1km").count()
    agg_100m = multi_res.groupBy("bng_100m").count()
    
    return agg_10km, agg_1km, agg_100m


def gridx_kring_analysis(spark):
    """
    K-ring analysis for finding nearby locations.
    
    Use BNG k-ring to find all locations within a certain distance.
    """
    from databricks.labs.gbx.gridx.bng import functions as bx
    bx.register(spark)
    
    # Get all cells within k-ring
    nearby_cells = spark.sql("""
        SELECT
            location_id,
            center_bng_cell,
            gbx_bng_cellkring(center_bng_cell, 3) as nearby_cells
        FROM important_sites
    """)
    
    # Explode array to individual cells
    expanded = nearby_cells.select(
        "location_id",
        "center_bng_cell",
        explode("nearby_cells").alias("nearby_cell")
    )
    
    # Join with data in those cells
    # results = expanded.join(data_by_cell, ...)
    
    return expanded


def gridx_partition_strategy(spark):
    """
    Partition data by BNG grid for efficient queries.
    
    Use BNG indexing for partitioning strategy.
    """
    # Partition data by BNG grid
    # df.repartition("bng_cell").write.partitionBy("bng_cell").saveAsTable("data_by_bng")
    
    return True


def gridx_zorder_optimization(spark):
    """
    Z-order optimization for BNG-indexed data.
    
    Apply Z-ordering on BNG cell columns for better performance.
    """
    # Z-order by BNG cell for better performance
    spark.sql("""
        OPTIMIZE uk_locations
        ZORDER BY (bng_cell)
    """)
    
    return True


def gridx_rasterx_integration(spark):
    """
    Combine GridX BNG with RasterX for raster aggregation.
    
    Aggregate raster values by BNG grid cells.
    """
    from databricks.labs.gbx.gridx.bng import functions as bx
    from databricks.labs.gbx.rasterx import functions as rx
    
    bx.register(spark)
    rx.register(spark)
    
    # Aggregate raster values by BNG cells
    raster_by_bng = spark.sql("""
        SELECT
            gbx_bng_pointtocell(centroid, 1000) as bng_cell,
            AVG(pixel_value) as avg_value
        FROM raster_pixels
        GROUP BY bng_cell
    """)
    
    return raster_by_bng


def gridx_vectorx_integration(spark):
    """
    Use GridX BNG for vector data indexing with VectorX.
    
    Index vector data with BNG grid cells.
    """
    from databricks.labs.gbx.gridx.bng import functions as bx
    from databricks.labs.gbx.vectorx.jts.legacy import functions as vx
    
    bx.register(spark)
    vx.register(spark)
    
    # Index vector data with BNG
    indexed_vectors = spark.sql("""
        SELECT
            feature_id,
            gbx_st_legacyaswkb(geom) as geometry_wkb,
            gbx_bng_pointtocell(st_centroid(st_geomfromwkb(gbx_st_legacyaswkb(geom))), 1000) as bng_cell
        FROM vector_features
    """)
    
    return indexed_vectors


# SQL Constants (used in packages/rasterx.mdx; tests run these when sample data available)
SQL_RASTERX_USAGE = f"""-- Register functions first in Python/Scala notebook
-- Then use in SQL

-- Read raster data (sample data path; see Sample Data guide)
CREATE OR REPLACE TEMP VIEW rasters AS
SELECT * FROM gdal.`{SAMPLE_RASTER_PATH}`;

-- Extract metadata
SELECT
    path,
    gbx_rst_width(tile) as width,
    gbx_rst_height(tile) as height,
    gbx_rst_numbands(tile) as num_bands,
    gbx_rst_srid(tile) as srid
FROM rasters;"""

SQL_RASTERX_USAGE_output = """+--------------------+-----+------+----------+----+
|path                |width|height|num_bands |srid|
+--------------------+-----+------+----------+----+
|.../nyc_sentinel2...|10980|10980 |1         |4326|
+--------------------+-----+------+----------+----+"""


SQL_GRIDX_BNG_USAGE = """-- Register functions first in Python/Scala notebook

-- Point in BNG coordinates (eastings, northings, EPSG:27700). Resolution: BNG resolution string ('1km') or index (3).
-- gbx_bng_cellarea returns square kilometres.
SELECT
  gbx_bng_pointascell('POINT(530000 180000)', '1km') as bng_cell_1km,
  gbx_bng_cellarea(gbx_bng_pointascell('POINT(530000 180000)', '1km')) as area_km2;"""

SQL_GRIDX_BNG_USAGE_output = """+------------+----------+
|bng_cell_1km|area_km2  |
+------------+----------+
|TQ3080      |1.0       |
+------------+----------+"""


SQL_VECTORX_MIGRATION = """-- In Python/Scala, register functions first
-- Then use in SQL:

CREATE OR REPLACE TABLE modern_features AS
SELECT
    feature_id,
    properties,
    st_geomfromwkb(gbx_st_legacyaswkb(mosaic_geom)) as geometry
FROM legacy_mosaic_table;"""


if __name__ == "__main__":
    print("GeoBrix Packages Examples")
    print("=" * 50)
    print(f"Total functions: {len([name for name in dir() if callable(globals()[name]) and not name.startswith('_')])}")
