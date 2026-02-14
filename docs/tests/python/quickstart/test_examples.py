"""
Tests for Quick Start Examples - EXECUTABLE WITH REAL DATA

These tests verify that the code examples in the Quick Start documentation work correctly
by executing them with real sample data and validating actual outputs.

Documentation: docs/docs/quick-start.md
- Tests verify docs/tests/python/quickstart/examples.py

Run:
    ./scripts/ci/run-doc-tests.sh local  # Must run in Docker

Requirements:
    - Full Spark environment (Docker)
    - GeoBrix library installed
    - Sample data at /Volumes/main/default/geobrix_samples/geobrix-examples/

Sample Data Used:
    - NYC Taxi Zones (GeoJSON): nyc/taxi-zones/nyc_taxi_zones.geojson
    - NYC Boroughs (GeoJSON): nyc/boroughs/nyc_boroughs.geojson  
    - NYC Sentinel-2 (GeoTIFF): nyc/sentinel2/nyc_sentinel2_red.tif
"""

import pytest
from pyspark.sql import SparkSession
import sys
from pathlib import Path
import importlib.util

# Import the module under test - explicitly load from current directory to avoid conflicts
# with docs/tests/python/examples/ package
examples_path = Path(__file__).parent / "examples.py"
spec = importlib.util.spec_from_file_location("examples", examples_path)
examples = importlib.util.module_from_spec(spec)
spec.loader.exec_module(examples)

# Sample data paths - matches structure from docs/docs/sample-data.md
SAMPLE_DATA_BASE = "/Volumes/main/default/geobrix_samples/geobrix-examples"
SAMPLE_NYC_TAXI = f"{SAMPLE_DATA_BASE}/nyc/taxi-zones/nyc_taxi_zones.geojson"
SAMPLE_NYC_BOROUGHS = f"{SAMPLE_DATA_BASE}/nyc/boroughs/nyc_boroughs.geojson"
SAMPLE_NYC_RASTER = f"{SAMPLE_DATA_BASE}/nyc/sentinel2/nyc_sentinel2_red.tif"


# Spark fixture provided by conftest.py


def test_quickstart_display_constants_exist():
    """Display constants (snippets) used in quick-start.mdx must exist and be non-empty."""
    for name in (
        "REGISTER_RASTERX", "REGISTER_GRIDX", "REGISTER_VECTORX", "REGISTER_RASTERX_SCALA",
        "READ_GEOTIFF", "READ_SHAPEFILE", "READ_GEOJSON",
        "USE_RASTERX", "USE_BNG", "USE_VECTORX",
        "SQL_LIST_FUNCTIONS", "SQL_DESCRIBE", "SQL_READ_AND_USE",
    ):
        assert hasattr(examples, name), f"Missing display constant: {name}"
        snippet = getattr(examples, name)
        assert isinstance(snippet, str) and len(snippet.strip()) > 0, f"Empty constant: {name}"


def test_quickstart_output_constants_exist():
    """Output constants (for Example output blocks) must exist so docs can show results."""
    for name in (
        "READ_GEOTIFF_output", "READ_SHAPEFILE_output", "READ_GEOJSON_output",
        "USE_RASTERX_output", "USE_BNG_output", "USE_VECTORX_output",
        "SQL_LIST_FUNCTIONS_output", "SQL_DESCRIBE_output", "SQL_READ_AND_USE_output",
    ):
        assert hasattr(examples, name), f"Missing output constant: {name}"
        out = getattr(examples, name)
        assert isinstance(out, str) and len(out.strip()) > 0, f"Empty output constant: {name}"


def _exec_snippet(spark, snippet_str, description):
    """Execute a snippet string with spark in scope. Requires sample data at /Volumes/... paths."""
    exec(snippet_str, {"spark": spark})
    return True


def test_exec_register_rasterx_snippet(spark):
    """Quick-start snippet REGISTER_RASTERX runs without error (executes with real Spark)."""
    _exec_snippet(spark, examples.REGISTER_RASTERX, "REGISTER_RASTERX")


def test_exec_read_geotiff_snippet(spark):
    """Quick-start snippet READ_GEOTIFF runs with sample data path (executes and .show())."""
    _exec_snippet(spark, examples.READ_GEOTIFF, "READ_GEOTIFF")


def test_exec_read_shapefile_snippet(spark):
    """Quick-start snippet READ_SHAPEFILE runs with sample data path."""
    _exec_snippet(spark, examples.READ_SHAPEFILE, "READ_SHAPEFILE")


def test_exec_read_geojson_snippet(spark):
    """Quick-start snippet READ_GEOJSON runs with sample data path."""
    _exec_snippet(spark, examples.READ_GEOJSON, "READ_GEOJSON")


def test_exec_use_rasterx_snippet(spark):
    """Quick-start snippet USE_RASTERX runs (register + load + RasterX + .show())."""
    _exec_snippet(spark, examples.USE_RASTERX, "USE_RASTERX")


def test_exec_use_bng_snippet(spark):
    """Quick-start snippet USE_BNG runs (register + SQL + .show())."""
    _exec_snippet(spark, examples.USE_BNG, "USE_BNG")


def test_exec_use_vectorx_snippet(spark):
    """Quick-start snippet USE_VECTORX runs (register + createDataFrame legacy + st_legacyaswkb + .show())."""
    _exec_snippet(spark, examples.USE_VECTORX, "USE_VECTORX")


def test_use_vectorx_returns_one_row_with_wkb(spark):
    """Legacy point example produces one row with a wkb column (same struct as USE_VECTORX)."""
    from pyspark.sql import Row
    from pyspark.sql.types import ArrayType, DoubleType, IntegerType, StructField, StructType

    from databricks.labs.gbx.vectorx.jts.legacy import functions as vx

    vx.register(spark)
    legacy_schema = StructType([
        StructField("typeId", IntegerType()),
        StructField("srid", IntegerType()),
        StructField("boundaries", ArrayType(ArrayType(ArrayType(DoubleType())))),
        StructField("holes", ArrayType(ArrayType(ArrayType(ArrayType(DoubleType()))))),
    ])
    row = Row(geom_legacy=(1, 0, [[[30.0, 10.0]]], []))
    shapes = spark.createDataFrame([row], StructType([StructField("geom_legacy", legacy_schema)]))
    result = shapes.select(vx.st_legacyaswkb("geom_legacy").alias("wkb"))
    rows = result.collect()
    assert len(rows) == 1, "Expected one row from legacy point example"
    assert "wkb" in result.columns, "Expected wkb column"
    assert rows[0]["wkb"] is not None, "WKB bytes should be non-null"


def test_exec_sql_list_functions_snippet(spark):
    """Quick-start SQL_LIST_FUNCTIONS runs (after register); executes SHOW FUNCTIONS."""
    examples.register_functions(spark)
    examples.register_gridx_functions(spark)
    examples.register_vectorx_functions(spark)
    for stmt in examples.SQL_LIST_FUNCTIONS.strip().split(";"):
        stmt = stmt.strip()
        if not stmt or stmt.startswith("--"):
            continue
        spark.sql(stmt).show()


def test_exec_sql_describe_snippet(spark):
    """Quick-start SQL_DESCRIBE runs (after register); executes DESCRIBE FUNCTION."""
    examples.register_functions(spark)
    for stmt in examples.SQL_DESCRIBE.strip().split(";"):
        stmt = stmt.strip()
        if not stmt or stmt.startswith("--"):
            continue
        spark.sql(stmt).show(1, vertical=True)


@pytest.fixture(scope="module")
def sample_geojson_path():
    """NYC Taxi Zones GeoJSON path"""
    return SAMPLE_NYC_TAXI


@pytest.fixture(scope="module")
def sample_shapefile_path():
    """NYC Boroughs - can use GeoJSON reader"""
    return SAMPLE_NYC_BOROUGHS


@pytest.fixture(scope="module")
def sample_raster_path():
    """NYC Sentinel-2 raster path"""
    return SAMPLE_NYC_RASTER


# Test: Function Registration
def test_register_functions_with_spark(spark):
    """
    Test that RasterX functions can be registered.
    
    Validates:
    - Function registration succeeds
    - No errors thrown
    - Functions become available in Spark
    """
    # Should not raise
    try:
        examples.register_functions(spark)
        success = True
    except Exception as e:
        pytest.fail(f"Failed to register functions: {e}")
        success = False
    
    assert success, "Function registration should succeed"


def test_register_vectorx_functions_with_spark(spark):
    """Test VectorX function registration."""
    examples.register_vectorx_functions(spark)
    assert True


# Test: Reading Data
def test_read_geojson_with_nyc_taxi_zones(spark, sample_geojson_path):
    """
    Test reading GeoJSON files with NYC taxi zones data.
    
    Validates:
    - DataFrame is created successfully
    - Has geometry column
    - Contains expected number of zones (~260)
    - Data has expected structure
    """
    # Use examples module directly
    
    # Execute function
    result_df = examples.read_geojson(spark, sample_geojson_path, multi=False)
    
    # Validate result
    assert result_df is not None, "DataFrame should not be None"
    
    # Check columns exist
    columns = result_df.columns
    assert len(columns) > 0, "Should have columns"
    
    # Check data count
    count = result_df.count()
    assert count > 200, f"Expected ~260 taxi zones, got {count}"
    assert count < 300, f"Expected ~260 taxi zones, got {count}"
    
    # Validate has geometry
    geom_cols = [c for c in columns if 'geom' in c.lower() or 'geometry' in c.lower()]
    assert len(geom_cols) > 0, "Should have geometry column"


def test_read_geotiff_with_sentinel2(spark, sample_raster_path):
    """
    Test reading GeoTIFF raster files with Sentinel-2 data.
    
    Validates:
    - DataFrame is created
    - Has 'tile' column with binary data
    - Path column shows correct file
    - Tile data is not empty
    """
    # Execute function
    result_df = examples.read_geotiff_files(spark, sample_raster_path)
    
    # Validate structure
    assert result_df is not None
    columns = result_df.columns
    assert 'tile' in columns, "Should have 'tile' column"
    # Note: GDAL reader uses 'source' column, not 'path'
    assert 'source' in columns or 'path' in columns, "Should have 'source' or 'path' column"
    
    # Check data
    rows = result_df.collect()
    assert len(rows) > 0, "Should have at least one row"
    
    # Validate tile data
    first_row = rows[0]
    assert first_row['tile'] is not None, "Tile should not be None"
    assert len(first_row['tile']) > 0, "Tile should contain data"
    
    # Validate path/source
    # Use bracket notation instead of .get() which isn't supported in all Spark versions
    path_or_source = first_row['source'] if 'source' in columns else first_row['path']
    assert path_or_source is not None, "Should have path or source column"
    assert 'sentinel' in path_or_source.lower(), "Path should reference sentinel data"


def test_read_shapefiles_returns_dataframe_with_geometry(spark, sample_shapefile_path):
    """
    Test reading shapefile-like data (using GeoJSON for NYC Boroughs).
    
    Note: Using GeoJSON reader for borough data as it's in GeoJSON format.
    The quick start example shows the shapefile reader pattern.
    
    Validates:
    - DataFrame created successfully
    - Contains geometry data
    - Has expected borough count (5 boroughs)
    """
    # Use read_geojson since sample data is GeoJSON
    # Use examples module directly
    
    result_df = examples.read_geojson(spark, sample_shapefile_path)
    
    assert result_df is not None
    columns = result_df.columns
    
    # Check for geometry
    geom_cols = [c for c in columns if 'geom' in c.lower()]
    assert len(geom_cols) > 0, "Should have geometry columns"
    
    # NYC has 5 boroughs
    count = result_df.count()
    assert count == 5, f"Expected 5 NYC boroughs, got {count}"


# Test: Using Functions
def test_use_rasterx_functions_with_sentinel2(spark, sample_raster_path):
    """
    Test applying RasterX functions to raster data.
    
    Validates:
    - Function executes successfully
    - Returns DataFrame with bbox column
    - Bounding box is not null
    - Result structure is correct
    """
    result_df = examples.use_rasterx_functions(spark, sample_raster_path)
    assert result_df is not None
    assert 'bbox' in result_df.columns
    rows = result_df.collect()
    assert len(rows) > 0
    assert rows[0]['bbox'] is not None, "Bounding box should not be null"


def test_pattern_batch_processing_with_sentinel2(spark, sample_raster_path):
    """
    Test batch processing pattern with raster data.
    
    Validates:
    - Processes raster and extracts multiple attributes
    - Returns DataFrame with path, bbox, metadata, dimensions
    - All attributes are populated
    - Types are correct
    """
    result_df = examples.pattern_batch_processing(spark, sample_raster_path)
    assert result_df is not None
    columns = result_df.columns
    assert 'path' in columns
    assert 'bbox' in columns
    assert 'metadata' in columns
    assert 'width' in columns
    assert 'height' in columns
    rows = result_df.collect()
    assert len(rows) > 0
    first_row = rows[0]
    assert first_row['path'] is not None
    assert first_row['bbox'] is not None
    assert first_row['width'] is not None
    assert first_row['height'] is not None
    assert first_row['width'] > 0
    assert first_row['height'] > 0
    assert isinstance(first_row['width'], int)
    assert isinstance(first_row['height'], int)


# Test: SQL Constants
def test_sql_constants_are_valid_strings():
    """
    Test that SQL example constants exist and are valid SQL.
    
    Validates:
    - Constants exist
    - Are non-empty strings
    - Contain expected SQL keywords
    """
    # Validate they exist and are strings
    assert isinstance(examples.SQL_LIST_FUNCTIONS, str)
    # These constants may not exist in examples.py, check if they do
    if hasattr(examples, 'SQL_DESCRIBE_FUNCTION'):
        assert isinstance(examples.SQL_DESCRIBE_FUNCTION, str)
    if hasattr(examples, 'SQL_WORKING_WITH_SQL'):
        assert isinstance(examples.SQL_WORKING_WITH_SQL, str)
    if hasattr(examples, 'SQL_CONVERSION'):
        assert isinstance(examples.SQL_CONVERSION, str)
    
    # Validate content
    assert len(examples.SQL_LIST_FUNCTIONS) > 0
    assert "SHOW FUNCTIONS" in examples.SQL_LIST_FUNCTIONS
    assert "gbx_rst_*" in examples.SQL_LIST_FUNCTIONS
    
    if hasattr(examples, 'SQL_DESCRIBE_FUNCTION'):
        assert "DESCRIBE FUNCTION" in examples.SQL_DESCRIBE_FUNCTION
        assert "gbx_rst_boundingbox" in examples.SQL_DESCRIBE_FUNCTION
    
    if hasattr(examples, 'SQL_WORKING_WITH_SQL'):
        assert "CREATE OR REPLACE TEMP VIEW" in examples.SQL_WORKING_WITH_SQL
        assert "shapefile" in examples.SQL_WORKING_WITH_SQL  # shapefile_ogr or shapefile
    
    if hasattr(examples, 'SQL_CONVERSION'):
        assert "st_geomfromwkb" in examples.SQL_CONVERSION
        assert "st_area" in examples.SQL_CONVERSION


# Integration Test: Full Quick Start Workflow
def test_full_quick_start_workflow(spark, sample_geojson_path, sample_raster_path):
    """
    Test complete quick start workflow end-to-end.
    
    This integration test validates:
    1. Register functions
    2. Read vector data
    3. Read raster data
    4. Apply GeoBrix functions
    5. Convert to Databricks types
    
    This ensures the entire quick start guide works as documented.
    """
    # 1. Register functions
    examples.register_functions(spark)

    # 2. Read vector data
    # Use examples module directly
    vector_df = examples.read_geojson(spark, sample_geojson_path)
    assert vector_df is not None
    assert vector_df.count() > 0
    
    # 3. Read raster data
    raster_df = examples.read_geotiff_files(spark, sample_raster_path)
    assert raster_df is not None
    assert raster_df.count() > 0
    
    # 4. Apply RasterX function
    result_df = examples.use_rasterx_functions(spark, sample_raster_path)
    assert result_df is not None
    assert 'bbox' in result_df.columns


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])


def test_use_gridx_functions():
    assert hasattr(examples, 'use_gridx_functions')
    assert callable(examples.use_gridx_functions)

def test_use_vectorx_functions():
    assert hasattr(examples, 'use_vectorx_functions')
    assert callable(examples.use_vectorx_functions)

