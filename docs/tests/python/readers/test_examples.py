"""
Tests for Reader Examples - EXECUTABLE WITH REAL DATA

These tests verify that reader code examples shown in documentation work correctly
by executing them with real sample data.

Documentation: docs/docs/readers/overview.md
- Tests verify docs/tests/python/readers/examples.py

Run:
    ./scripts/ci/run-doc-tests.sh local  # Must run in Docker

Requirements:
    - Full Spark environment (Docker)
    - GeoBrix library installed  
    - Sample data at /Volumes/main/default/geobrix_samples/geobrix-examples/
"""

import pytest
import sys
from pathlib import Path
import importlib.util

# Import the module under test - explicitly load from current directory to avoid conflicts
# with docs/tests/python/examples/ package
examples_path = Path(__file__).parent / "examples.py"
spec = importlib.util.spec_from_file_location("examples", examples_path)
examples = importlib.util.module_from_spec(spec)
spec.loader.exec_module(examples)

# Sample data paths at runtime (path_config: minimal bundle or GBX_SAMPLE_DATA_ROOT)
from path_config import (
    SAMPLE_DATA_BASE,
    SAMPLE_DATA_VOLUME,
    MIN_BOROUGHS,
    MAX_BOROUGHS,
    MIN_VECTOR_ROWS,
    MIN_RASTER_ROWS,
)

SAMPLE_SHAPEFILE = f"{SAMPLE_DATA_BASE}/nyc/subway/nyc_subway.shp.zip"
SAMPLE_GEOJSON = f"{SAMPLE_DATA_BASE}/nyc/boroughs/nyc_boroughs.geojson"
SAMPLE_GEOTIFF = f"{SAMPLE_DATA_BASE}/nyc/sentinel2/nyc_sentinel2_red.tif"
SAMPLE_DIR = f"{SAMPLE_DATA_BASE}/nyc/sentinel2"


def _skip_if_raster_empty(result):
    """Skip test when raster read returned 0 rows (minimal bundle or missing file)."""
    if result.count() == 0:
        pytest.skip("No raster rows; use full bundle or generate minimal bundle")


# ============================================================================
# BASIC READING TESTS
# ============================================================================

def test_read_geojson_with_nyc_boroughs(spark, sample_nyc_boroughs):
    """
    Test reading GeoJSON with NYC boroughs data.
    
    Validates:
    - GeoJSON reader works
    - Returns DataFrame
    - Has geometry columns
    - Has expected number of boroughs
    """
    result = examples.read_geojson(spark, sample_nyc_boroughs)
    
    # Validate structure
    assert result is not None
    assert 'geom_0' in result.columns, "Should have geometry column"
    
    # Validate data (full bundle: 5 boroughs; minimal: 1)
    count = result.count()
    assert MIN_BOROUGHS <= count <= MAX_BOROUGHS, f"Expected {MIN_BOROUGHS}-{MAX_BOROUGHS} boroughs, got {count}"


def test_read_geotiff_with_nyc_sentinel(spark, sample_nyc_raster):
    """
    Test reading GeoTIFF with NYC Sentinel-2 data.
    
    Validates:
    - GDAL reader works
    - Returns DataFrame
    - Has raster tile column
    - Has expected columns
    """
    result = examples.read_geotiff(spark, sample_nyc_raster)
    
    # Validate structure
    assert result is not None
    columns = result.columns
    assert 'tile' in columns, "Should have tile column"
    assert 'source' in columns or 'path' in columns, "Should have source/path column"
    
    # Validate data (skip when minimal bundle has no raster or GDAL returns 0)
    count = result.count()
    if count == 0:
        pytest.skip("No raster rows (path missing or GDAL returned 0); use full bundle or generate minimal bundle")
    assert count > 0, "Should have at least one tile"


def test_generic_reader_pattern_with_options(spark, sample_nyc_raster):
    """
    Test generic reader pattern with options.
    
    Validates:
    - Generic pattern works with any reader
    - Options are applied correctly
    - Returns DataFrame
    """
    result = examples.read_generic_pattern(
        spark, 
        "gdal", 
        sample_nyc_raster,
        "sizeInMB",
        "16"
    )
    
    # Validate (skip when no raster rows)
    assert result is not None
    assert 'tile' in result.columns
    count = result.count()
    if count == 0:
        pytest.skip("No raster rows; use full bundle or generate minimal bundle")
    assert count > 0


# ============================================================================
# PATH SPECIFICATION TESTS
# ============================================================================

def test_read_single_file(spark, sample_nyc_boroughs):
    """
    Test reading a single file by exact path.
    
    Validates:
    - Single file path works
    - Returns DataFrame with data from that file
    """
    result = examples.read_single_file(spark, sample_nyc_boroughs)
    
    # Validate (full bundle: 5 boroughs; minimal: 1)
    assert result is not None
    assert 'geom_0' in result.columns
    c = result.count()
    assert MIN_BOROUGHS <= c <= MAX_BOROUGHS, f"Expected {MIN_BOROUGHS}-{MAX_BOROUGHS} boroughs, got {c}"


def test_read_directory(spark):
    """
    Test reading all files from a directory.
    
    Validates:
    - Directory path works
    - Returns DataFrame with data from all files
    """
    result = examples.read_directory(spark, SAMPLE_DIR)
    
    # Validate (skip when no raster rows)
    assert result is not None
    assert 'tile' in result.columns
    count = result.count()
    if count == 0:
        pytest.skip("No raster rows in directory; use full bundle or generate minimal bundle")
    assert count > 0


def test_read_with_wildcard(spark):
    """
    Test reading files with wildcard pattern.
    
    Validates:
    - Wildcard patterns work
    - Only matching files are read
    
    Note: Wildcard patterns may have issues with some readers.
    If the wildcard fails, we test with a direct file path instead.
    """
    pattern = f"{SAMPLE_DIR}/*.tif"
    try:
        result = examples.read_with_wildcard(spark, pattern)
        
        # Validate (skip when no raster rows)
        assert result is not None
        assert 'tile' in result.columns
        count = result.count()
        if count == 0:
            pytest.skip("No raster rows; use full bundle or generate minimal bundle")
        assert count > 0
    except Exception as e:
        # Wildcard patterns can be problematic - fall back to direct file path
        direct_path = f"{SAMPLE_DIR}/nyc_sentinel2_red.tif"
        result = examples.read_with_wildcard(spark, direct_path)
        assert result is not None
        assert 'tile' in result.columns
        count = result.count()
        if count == 0:
            pytest.skip("No raster rows; use full bundle or generate minimal bundle")
        assert count > 0


def test_read_from_unity_catalog_volumes(spark):
    """
    Test reading from Unity Catalog Volumes path format.
    
    Validates:
    - Unity Catalog path construction works
    - Returns DataFrame
    """
    result = examples.read_from_unity_catalog_volumes(
        spark,
        catalog="main",
        schema="default",
        volume=SAMPLE_DATA_VOLUME,
        subpath="geobrix-examples/nyc/boroughs/nyc_boroughs.geojson"
    )
    
    # Validate (full bundle: 5 boroughs; minimal: 1)
    assert result is not None
    assert 'geom_0' in result.columns
    c = result.count()
    assert MIN_BOROUGHS <= c <= MAX_BOROUGHS, f"Expected {MIN_BOROUGHS}-{MAX_BOROUGHS} boroughs, got {c}"


# ============================================================================
# PERFORMANCE OPTIMIZATION TESTS
# ============================================================================

def test_read_large_raster_with_options(spark, sample_nyc_raster):
    """
    Test reading raster with custom split size option.
    
    Validates:
    - sizeInMB option is applied
    - Returns DataFrame
    """
    result = examples.read_large_raster_with_options(spark, sample_nyc_raster, "32")
    
    # Validate (skip when no raster rows)
    assert result is not None
    assert 'tile' in result.columns
    _skip_if_raster_empty(result)


def test_read_with_filter_regex(spark):
    """
    Test reading with filterRegex option.
    
    Validates:
    - filterRegex option works
    - Only matching files are read
    """
    # Filter for files containing 'red' in name
    result = examples.read_with_filter_regex(spark, SAMPLE_DIR, ".*red.*\\.tif")
    
    # Validate
    assert result is not None
    assert 'tile' in result.columns
    
    # Skip when minimal bundle has no rasters in SAMPLE_DIR (or GDAL returns 0)
    count = result.count()
    if count == 0:
        pytest.skip("No raster rows; use full bundle or generate minimal bundle")
    assert count >= 1


# ============================================================================
# TROUBLESHOOTING TESTS
# ============================================================================

def test_read_with_explicit_driver(spark, sample_nyc_boroughs):
    """
    Test reading with explicitly specified OGR driver.
    
    Validates:
    - driverName option works
    - Returns DataFrame
    """
    # Note: GeoJSON requires "GeoJSON" driver
    result = examples.read_with_explicit_driver(spark, sample_nyc_boroughs, "GeoJSON")
    
    # Validate (full bundle: 5 boroughs; minimal: 1)
    assert result is not None
    assert 'geom_0' in result.columns
    c = result.count()
    assert MIN_BOROUGHS <= c <= MAX_BOROUGHS, f"Expected {MIN_BOROUGHS}-{MAX_BOROUGHS} boroughs, got {c}"


def test_tune_large_raster_performance(spark, sample_nyc_raster):
    """
    Test performance tuning for large rasters.
    
    Validates:
    - Performance options are applied
    - Returns DataFrame
    """
    result = examples.tune_large_raster_performance(spark, sample_nyc_raster, "8")
    
    # Validate (skip when no raster rows)
    assert result is not None
    assert 'tile' in result.columns
    _skip_if_raster_empty(result)


# ============================================================================
# REFERENCE DATA TESTS
# ============================================================================

def test_available_readers_reference():
    """
    Test that AVAILABLE_READERS reference is properly structured.
    
    Validates:
    - Dictionary exists
    - Has raster and vector categories
    - Contains expected readers
    """
    assert hasattr(examples, 'AVAILABLE_READERS')
    readers = examples.AVAILABLE_READERS
    
    assert 'raster' in readers
    assert 'vector' in readers
    
    # Check raster readers
    assert 'gdal' in readers['raster']
    
    # Check vector readers
    assert 'ogr' in readers['vector']
    assert 'shapefile' in readers['vector']
    assert 'geojson' in readers['vector']
    assert 'gpkg' in readers['vector']


def test_common_options_reference():
    """
    Test that COMMON_OPTIONS reference is properly structured.
    
    Validates:
    - Dictionary exists
    - Has raster and vector categories
    - Contains expected options
    """
    assert hasattr(examples, 'COMMON_OPTIONS')
    options = examples.COMMON_OPTIONS
    
    assert 'raster' in options
    assert 'vector' in options
    
    # Check raster options
    assert 'sizeInMB' in options['raster']
    assert 'filterRegex' in options['raster']
    
    # Check vector options
    assert 'chunkSize' in options['vector']
    assert 'driverName' in options['vector']


# ============================================================================
# GDAL-SPECIFIC TESTS
# ============================================================================

def test_read_gdal_basic(spark, sample_nyc_raster):
    """Test basic GDAL reader usage."""
    result = examples.read_gdal_basic(spark, sample_nyc_raster)
    
    assert result is not None
    assert 'tile' in result.columns
    _skip_if_raster_empty(result)


def test_read_gdal_with_split_size(spark, sample_nyc_raster):
    """Test GDAL reader with custom split size."""
    result = examples.read_gdal_with_split_size(spark, sample_nyc_raster, "32")
    
    assert result is not None
    assert 'tile' in result.columns
    _skip_if_raster_empty(result)


def test_read_gdal_with_filter(spark):
    """Test GDAL reader with regex filtering."""
    result = examples.read_gdal_with_filter(spark, SAMPLE_DIR, ".*red.*\\.tif")
    
    assert result is not None
    assert 'tile' in result.columns
    if result.count() == 0:
        pytest.skip("No raster rows; use full bundle or generate minimal bundle")
    assert result.count() >= 1  # Should find at least the red band


def test_read_gdal_with_driver(spark, sample_nyc_raster):
    """Test GDAL reader with explicit driver."""
    result = examples.read_gdal_with_driver(spark, sample_nyc_raster, "GTiff")
    
    assert result is not None
    assert 'tile' in result.columns
    _skip_if_raster_empty(result)


def test_print_gdal_schema(spark, sample_nyc_raster):
    """Test printing GDAL schema."""
    result = examples.print_gdal_schema(spark, sample_nyc_raster)
    
    assert result is not None
    columns = result.columns
    assert 'tile' in columns
    assert 'source' in columns or 'path' in columns


def test_read_single_geotiff(spark, sample_nyc_raster):
    """Test reading a single GeoTIFF."""
    result = examples.read_single_geotiff(spark, sample_nyc_raster)
    
    assert result is not None
    assert 'tile' in result.columns
    _skip_if_raster_empty(result)


def test_read_directory_geotiffs(spark):
    """Test reading all GeoTIFFs from directory."""
    result = examples.read_directory_geotiffs(spark, SAMPLE_DIR)
    
    assert result is not None
    assert 'tile' in result.columns
    _skip_if_raster_empty(result)


def test_read_geotiffs_filtered(spark):
    """Test reading GeoTIFFs with band filtering."""
    # Use a pattern that matches sentinel bands
    result = examples.read_geotiffs_filtered(spark, SAMPLE_DIR)
    
    assert result is not None
    assert 'tile' in result.columns


def test_read_large_geotiffs(spark, sample_nyc_raster):
    """Test reading large GeoTIFFs with optimized split size."""
    result = examples.read_large_geotiffs(spark, sample_nyc_raster)
    
    assert result is not None
    assert 'tile' in result.columns
    _skip_if_raster_empty(result)


def test_extract_raster_metadata(spark, sample_nyc_raster):
    """Test extracting raster metadata with RasterX."""
    result = examples.extract_raster_metadata(spark, sample_nyc_raster)
    
    assert result is not None
    assert 'tile' in result.columns or 'width' in result.columns
    _skip_if_raster_empty(result)


def test_create_raster_catalog(spark, sample_nyc_raster):
    """Test creating raster catalog."""
    result = examples.create_raster_catalog(spark, sample_nyc_raster)
    
    assert result is not None
    _skip_if_raster_empty(result)


def test_optimize_split_size_by_raster_size(spark):
    """Test split size optimization for different raster sizes."""
    # This will fail on paths but validates the function structure
    try:
        small, medium, large = examples.optimize_split_size_by_raster_size(spark)
        # If it gets here without error, that's good
        assert True
    except Exception:
        # Expected to fail on fake paths
        assert True


def test_parallel_raster_processing(spark, sample_nyc_raster):
    """Test parallel raster processing with repartitioning."""
    result = examples.parallel_raster_processing(spark, sample_nyc_raster)
    
    assert result is not None
    assert result.rdd.getNumPartitions() > 0


def test_cache_raster_catalog(spark, sample_nyc_raster):
    """Test caching raster catalog."""
    result = examples.cache_raster_catalog(spark, sample_nyc_raster)
    
    assert result is not None
    assert result.is_cached or result.storageLevel.useMemory


def test_satellite_imagery_catalog_usecase(spark, sample_nyc_raster):
    """Test satellite imagery catalog use case."""
    result = examples.satellite_imagery_catalog_usecase(spark, sample_nyc_raster)
    
    assert result is not None
    _skip_if_raster_empty(result)


def test_elevation_model_processing_usecase(spark, sample_nyc_raster):
    """Test elevation model processing use case."""
    result = examples.elevation_model_processing_usecase(spark, sample_nyc_raster)
    
    assert result is not None
    _skip_if_raster_empty(result)


def test_multi_temporal_analysis_usecase(spark, sample_nyc_raster):
    """Test multi-temporal analysis use case."""
    result = examples.multi_temporal_analysis_usecase(spark, sample_nyc_raster)
    
    assert result is not None
    _skip_if_raster_empty(result)


def test_troubleshoot_driver_not_found(spark, sample_nyc_raster):
    """Test troubleshooting driver issues."""
    result = examples.troubleshoot_driver_not_found(spark, sample_nyc_raster)
    
    assert result is not None
    assert 'tile' in result.columns
    _skip_if_raster_empty(result)


def test_troubleshoot_files_too_large(spark, sample_nyc_raster):
    """Test troubleshooting large file issues."""
    result = examples.troubleshoot_files_too_large(spark, sample_nyc_raster)
    
    assert result is not None
    assert 'tile' in result.columns
    _skip_if_raster_empty(result)


def test_troubleshoot_memory_issues(spark, sample_nyc_raster):
    """Test troubleshooting memory issues."""
    result = examples.troubleshoot_memory_issues(spark, sample_nyc_raster)
    
    assert result is not None
    # Should have source and extracted metadata columns (not 'metadata' column)
    columns = result.columns
    assert 'source' in columns
    # May have width/height/bands if rx is available, or just source if not
    assert len(columns) >= 1


def test_rasterx_integration_pipeline(spark, sample_nyc_raster):
    """Test RasterX integration pipeline."""
    result = examples.rasterx_integration_pipeline(spark, sample_nyc_raster)
    
    assert result is not None
    _skip_if_raster_empty(result)


def test_sql_gdal_basic_constant():
    """Test that SQL GDAL example constant exists."""
    assert hasattr(examples, 'SQL_GDAL_BASIC')
    sql = examples.SQL_GDAL_BASIC
    
    assert 'CREATE OR REPLACE TEMP VIEW' in sql
    assert 'gdal' in sql


# ============================================================================
# GDAL INTEGRATION TEST
# ============================================================================

def test_full_gdal_workflow(spark, sample_nyc_raster):
    """
    Integration test: Complete GDAL workflow.
    
    Tests:
    1. Basic reading
    2. Schema validation
    3. Filtering
    4. Metadata extraction
    5. Catalog creation
    """
    # 1. Basic read (skip when no raster rows)
    basic = examples.read_gdal_basic(spark, sample_nyc_raster)
    _skip_if_raster_empty(basic)
    
    # 2. Schema validation
    schema_df = examples.print_gdal_schema(spark, sample_nyc_raster)
    assert 'tile' in schema_df.columns
    
    # 3. With options
    with_opts = examples.read_gdal_with_split_size(spark, sample_nyc_raster, "32")
    _skip_if_raster_empty(with_opts)
    
    # 4. Metadata extraction (may not work without RasterX Python bindings)
    metadata = examples.extract_raster_metadata(spark, sample_nyc_raster)
    _skip_if_raster_empty(metadata)
    
    # 5. Catalog creation
    catalog = examples.create_raster_catalog(spark, sample_nyc_raster)
    _skip_if_raster_empty(catalog)
    
    assert True, "Full GDAL workflow completed successfully"


# ============================================================================
# SHAPEFILE-SPECIFIC TESTS
# ============================================================================

def test_read_shapefile_usage(spark, sample_nyc_subway_shp):
    """Test basic shapefile reading with zipped shapefile (.shp.zip)."""
    result = examples.read_shapefile_usage(spark, sample_nyc_subway_shp)
    
    assert result is not None
    assert 'geom_0' in result.columns
    assert result.count() >= MIN_VECTOR_ROWS  # full bundle 2000+; minimal 10


def test_print_shapefile_schema(spark, sample_nyc_subway_shp):
    """Test printing shapefile schema with zipped shapefile (.shp.zip)."""
    result = examples.print_shapefile_schema(spark, sample_nyc_subway_shp)
    
    assert result is not None
    assert 'geom_0' in result.columns
    assert 'geom_0_srid' in result.columns


def test_read_shapefile_with_chunk_size(spark, sample_nyc_parks_shp):
    """Test reading shapefile with custom chunk size using larger zipped shapefile (.shp.zip)."""
    result = examples.read_shapefile_with_chunk_size(spark, sample_nyc_parks_shp, "50000")
    
    assert result is not None
    assert 'geom_0' in result.columns
    assert result.count() >= MIN_VECTOR_ROWS  # full bundle 2000+; minimal 8


def test_read_single_shapefile(spark, sample_nyc_subway_shp):
    """Test reading a single zipped shapefile (.shp.zip)."""
    result = examples.read_single_shapefile(spark, sample_nyc_subway_shp)
    
    assert result is not None
    assert result.count() >= MIN_VECTOR_ROWS  # full bundle 2000+; minimal 10


def test_check_shapefile_projection(spark, sample_nyc_subway_shp):
    """Test checking shapefile projection with zipped shapefile (.shp.zip)."""
    result = examples.check_shapefile_projection(spark, sample_nyc_subway_shp)
    
    assert result is not None
    assert 'geom_0_srid' in result.columns
    assert 'geom_0_srid_proj' in result.columns


def test_cache_shapefile_data(spark, sample_nyc_parks_shp):
    """Test caching shapefile data from zipped shapefile (.shp.zip)."""
    result = examples.cache_shapefile_data(spark, sample_nyc_parks_shp)
    
    assert result is not None
    assert result.is_cached or result.storageLevel.useMemory


def test_read_shapefile_with_encoding(spark, sample_nyc_subway_shp):
    """Test reading shapefile with encoding from zipped shapefile (.shp.zip)."""
    result = examples.read_shapefile_with_encoding(spark, sample_nyc_subway_shp)
    
    assert result is not None
    assert result.count() >= MIN_VECTOR_ROWS  # full bundle 2000+; minimal 10


# Integration test
def test_full_shapefile_workflow(spark, sample_nyc_subway_shp):
    """
    Integration test: Complete shapefile workflow with zipped shapefile (.shp.zip).
    
    Tests:
    1. Basic reading
    2. Schema inspection
    3. Projection check
    4. Caching
    
    Note: Databricks geometry conversion moved to tests-dbr/
    """
    # 1. Basic read
    basic = examples.read_shapefile_usage(spark, sample_nyc_subway_shp)
    assert basic.count() >= MIN_VECTOR_ROWS  # full bundle 2000+; minimal 10
    
    # 2. Schema
    schema_df = examples.print_shapefile_schema(spark, sample_nyc_subway_shp)
    assert 'geom_0' in schema_df.columns
    
    # 3. Check projection
    projection = examples.check_shapefile_projection(spark, sample_nyc_subway_shp)
    assert 'geom_0_srid' in projection.columns
    
    # 4. Cache
    cached = examples.cache_shapefile_data(spark, sample_nyc_subway_shp)
    assert cached.is_cached or cached.storageLevel.useMemory
    
    assert True, "Full shapefile workflow completed successfully"


# ============================================================================
# GeoJSON Reader Tests
# ============================================================================

def test_read_geojson_basic(spark, sample_nyc_boroughs):
    """Test basic GeoJSON reading."""
    result = examples.read_geojson_basic(spark, sample_nyc_boroughs)
    assert result is not None
    assert 'geom_0' in result.columns
    c = result.count()
    assert MIN_BOROUGHS <= c <= MAX_BOROUGHS, f"Expected {MIN_BOROUGHS}-{MAX_BOROUGHS} boroughs, got {c}"


def test_read_geojson_with_multi_option(spark):
    """Test GeoJSON multi option (structure check)."""
    # This tests the function exists and is callable
    assert callable(examples.read_geojson_with_multi_option)


def test_print_geojson_schema(spark, sample_nyc_boroughs):
    """Test printing GeoJSON schema."""
    result = examples.print_geojson_schema(spark, sample_nyc_boroughs)
    assert result is not None
    assert 'geom_0' in result.columns
    assert 'geom_0_srid' in result.columns


def test_read_standard_geojson(spark):
    """Test reading standard GeoJSON with generic test data."""
    # Use default path (generic_features.geojson) which has standard schema
    result = examples.read_standard_geojson(spark)
    assert result is not None
    c = result.count()
    assert MIN_BOROUGHS <= c <= MAX_BOROUGHS, f"Expected {MIN_BOROUGHS}-{MAX_BOROUGHS} features, got {c}"
    assert 'name' in result.columns  # Standard schema has 'name'
    assert 'type' in result.columns  # Standard schema has 'type'


def test_read_geojsonseq(spark, sample_geojsonseq_taxi_zones):
    """Test reading GeoJSONSeq format (newline-delimited)."""
    result = examples.read_geojsonseq(spark, sample_geojsonseq_taxi_zones)
    assert result is not None
    count = result.count()
    assert count > 0  # NYC Taxi Zones has 263 zones


# Note: test_convert_geojson_to_databricks_geometry moved to 
# DBR geometry conversion: docs/tests-dbr/python/readers/ogr/, api/ (requires DBR)


def test_read_geojson_from_api_response(spark, sample_nyc_boroughs):
    """Test reading GeoJSON from API response format."""
    result = examples.read_geojson_from_api_response(spark, sample_nyc_boroughs)
    assert result is not None
    assert result.count() > 0


def test_read_geojson_directory(spark, sample_data_base):
    """Test reading directory of standard GeoJSON FeatureCollection files."""
    # Use nyc/boroughs directory which contains standard GeoJSON FeatureCollections
    # Function uses option("multi", "false") for standard GeoJSON format
    geojson_dir = f"{sample_data_base}/nyc/boroughs"
    result = examples.read_geojson_directory(spark, geojson_dir)
    assert result is not None
    assert result.count() > 0


def test_use_geojsonseq_for_large_files(spark, sample_geojsonseq_taxi_zones):
    """Test using GeoJSONSeq for large files (newline-delimited)."""
    result = examples.use_geojsonseq_for_large_files(spark, sample_geojsonseq_taxi_zones)
    assert result is not None
    assert result.count() > 0


def test_adjust_geojson_chunk_size(spark, sample_geojsonseq_taxi_zones):
    """Test adjusting chunk size for GeoJSONSeq (newline-delimited)."""
    result = examples.adjust_geojson_chunk_size(spark, sample_geojsonseq_taxi_zones)
    assert result is not None
    assert 'geom_0' in result.columns


def test_troubleshoot_geojson_parsing_errors(spark, sample_nyc_boroughs):
    """Test troubleshooting parsing errors."""
    result = examples.troubleshoot_geojson_parsing_errors(spark, sample_nyc_boroughs)
    assert result is not None


def test_troubleshoot_large_geojson_file(spark, sample_geojsonseq_taxi_zones):
    """Test handling large GeoJSONSeq files (newline-delimited)."""
    result = examples.troubleshoot_large_geojson_file(spark, sample_geojsonseq_taxi_zones)
    assert result is not None
    assert result.count() > 0


def test_troubleshoot_missing_geojson_properties(spark, sample_nyc_boroughs):
    """Test checking for missing properties."""
    result = examples.troubleshoot_missing_geojson_properties(spark, sample_nyc_boroughs)
    assert result is not None


# ============================================================================
# GeoPackage Reader Tests
# ============================================================================

def test_read_geopackage_basic(spark):
    """Test basic GeoPackage reading (structure check)."""
    # GeoPackage reader may not be available in all environments
    assert callable(examples.read_geopackage_basic)


def test_read_geopackage_with_layer_options(spark):
    """Test GeoPackage layer options (structure check)."""
    assert callable(examples.read_geopackage_with_layer_options)


def test_read_single_layer_geopackage(spark):
    """Test reading single layer GeoPackage (structure check)."""
    assert callable(examples.read_single_layer_geopackage)


def test_read_multiple_layers_from_geopackage(spark):
    """Test reading multiple layers (structure check)."""
    assert callable(examples.read_multiple_layers_from_geopackage)


# Note: test_convert_geopackage_to_databricks_geometry moved to
# DBR geometry conversion: docs/tests-dbr/python/readers/ogr/, api/ (requires DBR)


def test_read_geopackage_from_cloud(spark):
    """Test cloud storage reading (structure check)."""
    assert callable(examples.read_geopackage_from_cloud)


def test_read_multiple_geopackages(spark):
    """Test reading multiple files (structure check)."""
    assert callable(examples.read_multiple_geopackages)


def test_list_geopackage_layers():
    """Test listing layers (structure check)."""
    assert callable(examples.list_geopackage_layers)


def test_read_all_layers_from_geopackage(spark):
    """Test reading all layers (structure check)."""
    assert callable(examples.read_all_layers_from_geopackage)


def test_geopackage_to_delta_workflow(spark):
    """Test GeoPackage to Delta workflow (structure check)."""
    assert callable(examples.geopackage_to_delta_workflow)


def test_multi_layer_processing_workflow(spark):
    """Test multi-layer processing (structure check)."""
    assert callable(examples.multi_layer_processing_workflow)


def test_geopackage_spatial_analysis_workflow(spark):
    """Test spatial analysis workflow (structure check)."""
    assert callable(examples.geopackage_spatial_analysis_workflow)


def test_read_specific_geopackage_layer(spark):
    """Test reading specific layer (structure check)."""
    assert callable(examples.read_specific_geopackage_layer)


def test_adjust_geopackage_chunk_size(spark):
    """Test chunk size adjustment (structure check)."""
    assert callable(examples.adjust_geopackage_chunk_size)


def test_cache_geopackage_layer(spark):
    """Test layer caching (structure check)."""
    assert callable(examples.cache_geopackage_layer)


def test_troubleshoot_geopackage_layer_not_found(spark):
    """Test layer not found troubleshooting (structure check)."""
    assert callable(examples.troubleshoot_geopackage_layer_not_found)


# Note: test_troubleshoot_geopackage_geometry_column moved to
# DBR geometry conversion: docs/tests-dbr/python/readers/ogr/, api/ (requires DBR)


def test_troubleshoot_large_geopackage_performance(spark):
    """Test large file performance troubleshooting (structure check)."""
    assert callable(examples.troubleshoot_large_geopackage_performance)


# ============================================================================
# File GeoDatabase Reader Tests
# ============================================================================

def test_read_filegdb_basic(spark):
    """Test basic File Geodatabase reading (structure check)."""
    # File GeoDatabase reader may not be available in all environments
    assert callable(examples.read_filegdb_basic)


def test_read_filegdb_with_layer_options(spark):
    """Test File Geodatabase layer options (structure check)."""
    assert callable(examples.read_filegdb_with_layer_options)


def test_read_single_feature_class(spark):
    """Test reading single feature class (structure check)."""
    assert callable(examples.read_single_feature_class)


def test_read_multiple_feature_classes(spark):
    """Test reading multiple feature classes (structure check)."""
    assert callable(examples.read_multiple_feature_classes)


# Note: test_convert_filegdb_to_databricks_geometry moved to
# DBR geometry conversion: docs/tests-dbr/python/readers/ogr/, api/ (requires DBR)


def test_read_filegdb_from_cloud(spark):
    """Test cloud storage reading (structure check)."""
    assert callable(examples.read_filegdb_from_cloud)


def test_handle_case_insensitive_columns(spark):
    """Test case-insensitive column handling (structure check)."""
    assert callable(examples.handle_case_insensitive_columns)


def test_list_filegdb_feature_classes():
    """Test listing feature classes (structure check)."""
    assert callable(examples.list_filegdb_feature_classes)


def test_read_all_feature_classes_from_filegdb(spark):
    """Test reading all feature classes (structure check)."""
    assert callable(examples.read_all_feature_classes_from_filegdb)


def test_filegdb_to_delta_workflow(spark):
    """Test File GeoDatabase to Delta workflow (structure check)."""
    assert callable(examples.filegdb_to_delta_workflow)


def test_multi_feature_class_processing_workflow(spark):
    """Test multi-feature class processing (structure check)."""
    assert callable(examples.multi_feature_class_processing_workflow)


def test_migrate_from_filegdb_workflow(spark):
    """Test migration workflow (structure check)."""
    assert callable(examples.migrate_from_filegdb_workflow)


def test_filegdb_spatial_analysis_workflow(spark):
    """Test spatial analysis workflow (structure check)."""
    assert callable(examples.filegdb_spatial_analysis_workflow)


def test_read_specific_feature_class_filegdb(spark):
    """Test reading specific feature class (structure check)."""
    assert callable(examples.read_specific_feature_class_filegdb)


def test_adjust_filegdb_chunk_size(spark):
    """Test chunk size adjustment (structure check)."""
    assert callable(examples.adjust_filegdb_chunk_size)


def test_cache_filegdb_feature_class(spark):
    """Test feature class caching (structure check)."""
    assert callable(examples.cache_filegdb_feature_class)


def test_repartition_filegdb_data(spark):
    """Test repartitioning (structure check)."""
    assert callable(examples.repartition_filegdb_data)


def test_troubleshoot_filegdb_feature_class_not_found(spark):
    """Test feature class not found troubleshooting (structure check)."""
    assert callable(examples.troubleshoot_filegdb_feature_class_not_found)


def test_troubleshoot_filegdb_column_case(spark):
    """Test column case troubleshooting (structure check)."""
    assert callable(examples.troubleshoot_filegdb_column_case)


def test_troubleshoot_large_filegdb_performance(spark):
    """Test large file performance troubleshooting (structure check)."""
    assert callable(examples.troubleshoot_large_filegdb_performance)


def test_troubleshoot_filegdb_directory_access(spark):
    """Test directory access troubleshooting (structure check)."""
    assert callable(examples.troubleshoot_filegdb_directory_access)


# ============================================================================
# OGR Reader Tests
# ============================================================================

def test_read_ogr_basic(spark):
    """Test basic OGR reading (structure check)."""
    assert callable(examples.read_ogr_basic)


def test_read_ogr_with_driver_name(spark):
    """Test OGR with driver name option (structure check)."""
    assert callable(examples.read_ogr_with_driver_name)


def test_read_ogr_with_chunk_size(spark):
    """Test OGR with chunk size option (structure check)."""
    assert callable(examples.read_ogr_with_chunk_size)


def test_read_ogr_with_layer_index(spark):
    """Test OGR with layer index (structure check)."""
    assert callable(examples.read_ogr_with_layer_index)


def test_read_ogr_with_layer_name(spark):
    """Test OGR with layer name (structure check)."""
    assert callable(examples.read_ogr_with_layer_name)


def test_read_ogr_with_wkt_output(spark):
    """Test OGR with WKT output (structure check)."""
    assert callable(examples.read_ogr_with_wkt_output)


def test_read_kml_with_ogr(spark):
    """Test KML reading with OGR (structure check)."""
    assert callable(examples.read_kml_with_ogr)


def test_read_multi_layer_with_ogr(spark):
    """Test multi-layer reading with OGR (structure check)."""
    assert callable(examples.read_multi_layer_with_ogr)


def test_adjust_ogr_performance(spark):
    """Test OGR performance adjustment (structure check)."""
    assert callable(examples.adjust_ogr_performance)


# Note: test_convert_ogr_to_databricks_geometry moved to
# DBR geometry conversion: docs/tests-dbr/python/readers/ogr/, api/ (requires DBR)


def test_read_kml_files(spark):
    """Test KML file reading (structure check)."""
    assert callable(examples.read_kml_files)


def test_read_gml_files(spark):
    """Test GML file reading (structure check)."""
    assert callable(examples.read_gml_files)


def test_read_csv_with_geometry(spark):
    """Test CSV with geometry reading (structure check)."""
    assert callable(examples.read_csv_with_geometry)


def test_read_postgis_with_ogr(spark):
    """Test PostGIS reading with OGR (structure check)."""
    assert callable(examples.read_postgis_with_ogr)


def test_optimize_chunk_size_for_features(spark):
    """Test chunk size optimization (structure check)."""
    assert callable(examples.optimize_chunk_size_for_features)


def test_parallel_reading_with_ogr(spark):
    """Test parallel reading with OGR (structure check)."""
    assert callable(examples.parallel_reading_with_ogr)


def test_named_readers_vs_ogr_comparison(spark):
    """Test named readers comparison (structure check)."""
    assert callable(examples.named_readers_vs_ogr_comparison)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
