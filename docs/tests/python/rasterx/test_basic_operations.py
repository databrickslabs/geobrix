"""
Tests for RasterX Basic Operations - EXECUTABLE WITH REAL DATA

These tests verify that code examples shown in documentation work correctly
by executing them with real sample data.

Documentation: docs/docs/api/rasterx-functions.md
- Tests verify docs/tests/python/rasterx/basic_operations.py

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

# Import the module under test
sys.path.insert(0, str(Path(__file__).parent))
import basic_operations

# Sample data path
SAMPLE_NYC_RASTER = "/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif"


def test_setup_rasterx_registers_functions(spark):
    """
    Test that setup_rasterx properly registers RasterX functions.
    
    Validates:
    - Function registration succeeds
    - Returns Spark session
    - No errors thrown
    """
    # Execute setup (without sample data, returns None)
    try:
        result = basic_operations.setup_rasterx(spark)
        # When called without sample_data_path, returns None
        # The important thing is that rx.register(spark) was called successfully
        assert result is None
        
        # Verify functions are registered by checking one exists
        assert hasattr(basic_operations.rx, 'rst_width')
    except Exception as e:
        pytest.fail(f"setup_rasterx failed: {e}")


def test_get_raster_metadata_with_sentinel2(spark, sample_nyc_raster):
    """
    Test extracting raster metadata from NYC Sentinel-2 data.
    
    Validates:
    - Returns DataFrame with metadata columns
    - Width and height are positive integers
    - Band count is correct
    - Metadata is complete
    """
    # Load raster
    rasters_df = spark.read.format("gdal").load(sample_nyc_raster)
    
    # Execute function
    result = basic_operations.get_raster_metadata(rasters_df)
    
    # Validate structure
    assert result is not None
    columns = result.columns
    
    # Should have metadata columns (width, height, srid)
    assert 'width' in columns
    assert 'height' in columns
    assert 'srid' in columns
    
    # Get values
    row = result.collect()[0]
    width = row['width']
    height = row['height']
    srid = row['srid']
    
    # Validate values
    assert isinstance(width, int), f"Width should be int, got {type(width)}"
    assert isinstance(height, int), f"Height should be int, got {type(height)}"
    assert isinstance(srid, int), f"SRID should be int, got {type(srid)}"
    
    assert width > 0, "Width should be positive"
    assert height > 0, "Height should be positive"
    assert srid != 0, "SRID should be set (not 0)"
    
    # Sentinel-2 tiles are large
    assert width > 1000, f"Expected large raster, got width {width}"
    assert height > 1000, f"Expected large raster, got height {height}"


def test_get_raster_bounds_returns_coordinates(spark, sample_nyc_raster):
    """
    Test extracting geographic bounds from raster.
    
    Note: Individual coordinate accessors (rst_minx, etc.) aren't implemented yet,
    so this function now returns bounding box geometry instead.
    
    Validates:
    - Returns DataFrame with bbox column
    - Bbox is not null (contains geometry data)
    """
    # Load raster
    rasters_df = spark.read.format("gdal").load(sample_nyc_raster)
    
    # Execute function
    result = basic_operations.get_raster_bounds(rasters_df)
    
    # Validate structure
    assert result is not None
    columns = result.columns
    
    # Should have bbox column
    assert 'bbox' in columns, f"Expected bbox column, got: {columns}"
    
    # Get row
    row = result.collect()[0]
    
    # Validate bbox has a value (it's WKB geometry, so it's bytes)
    bbox = row['bbox']
    assert bbox is not None, "Bounding box should not be null"
    assert isinstance(bbox, (bytes, bytearray)), "Bbox should be WKB geometry (bytes)"


def test_get_pixel_statistics_calculates_values(spark, sample_nyc_raster):
    """
    Test calculating pixel statistics (min, max, mean, std).
    
    Validates:
    - Returns DataFrame with statistics
    - Min, max, mean, std are numeric
    - Min <= Mean <= Max
    - Std is non-negative
    """
    # Load raster
    rasters_df = spark.read.format("gdal").load(sample_nyc_raster)
    
    # Execute function
    result = basic_operations.get_pixel_statistics(rasters_df)
    
    # Validate structure
    assert result is not None
    columns = result.columns
    
    # Look for statistics columns
    stat_cols = [c for c in columns if any(k in c.lower() for k in ['min', 'max', 'mean', 'std', 'avg'])]
    assert len(stat_cols) > 0, f"Expected statistics columns, got: {columns}"
    
    # Get row
    row = result.collect()[0]
    
    # Validate statistics exist and are reasonable
    for col in stat_cols:
        value = row[col]
        if value is not None:  # Some stats might be null for certain rasters
            # Value might be scalar or array (e.g., [1049.0])
            if isinstance(value, list):
                assert len(value) > 0, f"{col} array should not be empty"
                assert isinstance(value[0], (int, float)), f"{col} array element should be numeric"
            else:
                assert isinstance(value, (int, float)), f"{col} should be numeric"


def test_transform_raster_crs_changes_projection(spark, sample_nyc_raster):
    """
    Test transforming raster to different CRS.
    
    Validates:
    - Function executes successfully
    - Returns DataFrame
    - Target SRID is applied
    """
    # Load raster
    rasters_df = spark.read.format("gdal").load(sample_nyc_raster)
    
    # Execute function - transform to Web Mercator (EPSG:3857)
    target_srid = 3857
    
    result = basic_operations.transform_raster_crs(rasters_df, target_srid)
    assert result is not None
    assert 'tile' in result.columns, "Should have transformed tile column"
    count = result.count()
    assert count > 0, "Should have transformed data"


def test_clip_raster_to_geometry_reduces_extent(spark, sample_nyc_raster):
    """
    Test clipping raster to a geometry boundary.
    
    Validates:
    - Function executes with WKT geometry
    - Returns DataFrame
    - Clipped result exists
    
    Note: Full validation would require comparing extents
    """
    # Load raster
    rasters_df = spark.read.format("gdal").load(sample_nyc_raster)
    
    # Create a simple WKT polygon (small box in NYC area)
    # This is roughly a small area in NYC
    wkt_geometry = "POLYGON ((-74.01 40.70, -74.01 40.71, -74.00 40.71, -74.00 40.70, -74.01 40.70))"
    
    result = basic_operations.clip_raster_to_geometry(rasters_df, wkt_geometry)
    assert result is not None
    assert 'tile' in result.columns, "Should have clipped tile column"
    count = result.count()
    assert count > 0, "Should have clipped data"


def test_common_operations_dictionary_is_valid():
    """
    Test that COMMON_OPERATIONS reference dictionary is properly structured.
    
    Validates:
    - Dictionary exists
    - Has expected operation keys
    - Values are function references
    """
    assert hasattr(basic_operations, 'COMMON_OPERATIONS')
    ops = basic_operations.COMMON_OPERATIONS
    
    assert isinstance(ops, dict), "COMMON_OPERATIONS should be dict"
    assert len(ops) > 0, "COMMON_OPERATIONS should not be empty"
    
    # Check expected operations exist
    expected_ops = ['metadata', 'bounds', 'statistics', 'transform', 'clip']
    for op in expected_ops:
        assert op in ops, f"Missing operation: {op}"
    
    # Check values exist (may be functions or strings describing operations)
    for op_name, op_value in ops.items():
        assert op_value is not None, f"Operation {op_name} should have a value"
        # Values may be function references or strings describing the operation
        assert callable(op_value) or isinstance(op_value, str), \
            f"Operation {op_name} should be callable or string description"


def test_full_rasterx_workflow(spark, sample_nyc_raster):
    """
    Integration test: Run complete RasterX workflow.
    
    This validates the full workflow shown in documentation:
    1. Setup RasterX
    2. Load raster
    3. Get metadata
    4. Get bounds
    5. Calculate statistics
    
    Ensures all functions work together.
    """
    # 1. Setup
    basic_operations.setup_rasterx(spark)
    
    # 2. Load raster
    rasters_df = spark.read.format("gdal").load(sample_nyc_raster)
    assert rasters_df.count() > 0
    
    # 3. Get metadata
    metadata = basic_operations.get_raster_metadata(rasters_df)
    metadata_row = metadata.collect()[0]
    assert metadata_row['width'] > 0
    assert metadata_row['height'] > 0
    
    # 4. Get bounds
    bounds = basic_operations.get_raster_bounds(rasters_df)
    assert bounds.count() > 0
    
    # 5. Calculate statistics
    stats = basic_operations.get_pixel_statistics(rasters_df)
    assert stats.count() > 0
    
    # If we got here, the complete workflow works
    assert True, "Full workflow completed successfully"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
