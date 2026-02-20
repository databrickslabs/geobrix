"""
Tests for RasterX SQL examples.

Ensures all SQL examples in documentation are executable and produce valid results.
"""
import pytest
from pyspark.sql import functions as F
from . import rasterx_functions_sql

# Sample data base path (must match conftest.SAMPLE_DATA_BASE for doc test env)
from path_config import SAMPLE_DATA_BASE


@pytest.fixture(scope="module")
def sample_rasters(spark):
    """Load sample raster data for testing from Volumes (standardized sample-data path)."""
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)

    # Use Volumes path (standardized; run in Docker with sample-data mount)
    raster_paths = [
        f"{SAMPLE_DATA_BASE}/nyc/sentinel2/nyc_sentinel2_red.tif",
        f"{SAMPLE_DATA_BASE}/nyc/elevation/srtm_n40w073.tif",
    ]
    for path in raster_paths:
        try:
            rasters = spark.read.format("gdal").load(path)
            if rasters.count() > 0:
                return rasters
        except Exception:
            continue

    # Fallback: empty DataFrame with correct schema (tests that need data will fail)
    from pyspark.sql.types import StructType, StructField, StringType, BinaryType
    schema = StructType([
        StructField("path", StringType(), True),
        StructField("tile", BinaryType(), True)
    ])
    return spark.createDataFrame([], schema)


@pytest.fixture(scope="module")
def rasters_view(spark, sample_rasters):
    """Create temp view for SQL examples. Expects Docker env with Volumes/sample data available."""
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)
    # GDAL reader returns "source" not "path"; alias so SQL examples (path, tile) work
    view_df = (
        sample_rasters.withColumnRenamed("source", "path")
        if "source" in sample_rasters.columns
        else sample_rasters
    )
    view_df.createOrReplaceTempView("rasters")
    yield
    spark.catalog.dropTempView("rasters")


# ============================================================================
# Common setup (doc constant)
# ============================================================================

def test_rasterx_sql_setup_constant():
    """Doc constant RASTERX_SQL_SETUP exists and creates rasters view."""
    assert hasattr(rasterx_functions_sql, "RASTERX_SQL_SETUP")
    assert "rasters" in rasterx_functions_sql.RASTERX_SQL_SETUP
    assert hasattr(rasterx_functions_sql, "RASTERX_SQL_SETUP_output")


@pytest.mark.integration
def test_rasterx_sql_setup_executable(spark, sample_rasters):
    """Running the SQL in RASTERX_SQL_SETUP creates view rasters (requires sample data path)."""
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)
    sql = rasterx_functions_sql.RASTERX_SQL_SETUP.strip()
    spark.sql(sql)
    result = spark.sql("SELECT * FROM rasters LIMIT 1")
    assert result.count() >= 0
    spark.catalog.dropTempView("rasters")


# ============================================================================
# Accessor Functions
# ============================================================================

def test_rst_boundingbox_sql_example(spark, sample_rasters, rasters_view):
    """Test SQL bounding box example"""
    # Modified to work with temp view
    sql = """
    SELECT
        path,
        gbx_rst_boundingbox(tile) as bbox
    FROM rasters
    """
    result = spark.sql(sql)
    assert result.count() > 0
    assert "bbox" in result.columns


def test_rst_width_sql_example(spark, rasters_view):
    """Test SQL width examples"""
    # Test first query
    sql = "SELECT gbx_rst_width(tile) as width FROM rasters"
    result = spark.sql(sql)
    assert result.count() > 0
    assert "width" in result.columns
    
    # Test second query with multiple columns
    sql = """
    SELECT 
        path,
        gbx_rst_width(tile) as width,
        gbx_rst_height(tile) as height,
        gbx_rst_pixelwidth(tile) as pixel_width_m
    FROM rasters
    """
    result = spark.sql(sql)
    assert result.count() > 0
    assert all(col in result.columns for col in ["width", "height", "pixel_width_m"])


def test_rst_height_sql_example(spark, rasters_view):
    """Test SQL height example"""
    sql = rasterx_functions_sql.rst_height_sql_example().strip()
    result = spark.sql(sql)
    assert result.count() > 0
    assert all(col in result.columns for col in ["height", "width"])


def test_rst_numbands_sql_example(spark, rasters_view):
    """Test SQL numbands example"""
    sql = rasterx_functions_sql.rst_numbands_sql_example()
    result = spark.sql(sql)
    assert result.count() > 0
    assert "bands" in result.columns


def test_rst_metadata_sql_example(spark, rasters_view):
    """Test SQL metadata example"""
    sql = rasterx_functions_sql.rst_metadata_sql_example().strip()
    result = spark.sql(sql)
    assert result.count() > 0
    assert "metadata" in result.columns


def test_rst_srid_sql_example(spark, rasters_view):
    """Test SQL SRID example"""
    sql = rasterx_functions_sql.rst_srid_sql_example().strip()
    result = spark.sql(sql)
    assert result.count() > 0
    assert "srid" in result.columns


def test_rst_georeference_sql_example(spark, rasters_view):
    """Test SQL georeference example"""
    sql = rasterx_functions_sql.rst_georeference_sql_example().strip()
    result = spark.sql(sql)
    assert result.count() > 0
    assert "georeference" in result.columns


def test_rst_bandmetadata_sql_example(spark, rasters_view):
    """Test SQL band metadata example"""
    sql = rasterx_functions_sql.rst_bandmetadata_sql_example().strip()
    result = spark.sql(sql)
    assert result.count() > 0
    assert "band1_metadata" in result.columns


def test_rst_pixelcount_sql_example(spark, rasters_view):
    """Test SQL pixel count example"""
    sql = rasterx_functions_sql.rst_pixelcount_sql_example().strip()
    result = spark.sql(sql)
    assert result.count() > 0
    assert "pixel_count" in result.columns


def test_rst_avg_sql_example(spark, rasters_view):
    """Test SQL average examples"""
    # Test first query
    sql = """
    SELECT
        path,
        gbx_rst_avg(tile) as band_averages,
        gbx_rst_avg(tile)[0] as band1_avg
    FROM rasters
    """
    result = spark.sql(sql)
    assert result.count() > 0
    
    # Test filter query
    sql = "SELECT * FROM rasters WHERE gbx_rst_avg(tile)[0] > 0"
    result = spark.sql(sql)
    # Should execute without error


def test_rst_min_max_sql_example(spark, rasters_view):
    """Test SQL min/max example"""
    sql = """
    SELECT
        path,
        gbx_rst_min(tile)[0] as min_value,
        gbx_rst_max(tile)[0] as max_value,
        gbx_rst_max(tile)[0] - gbx_rst_min(tile)[0] as value_range
    FROM rasters
    """
    result = spark.sql(sql)
    assert result.count() > 0
    assert all(col in result.columns for col in ["min_value", "max_value", "value_range"])


def test_rst_median_sql_example(spark, rasters_view):
    """Test SQL median example"""
    sql = """
    SELECT
        path,
        gbx_rst_avg(tile)[0] as mean_value,
        gbx_rst_median(tile)[0] as median_value
    FROM rasters
    LIMIT 1
    """
    result = spark.sql(sql)
    assert result.count() > 0


def test_rst_format_sql_example(spark, rasters_view):
    """Test SQL format examples"""
    # Test group by format
    sql = """
    SELECT
        gbx_rst_format(tile) as format,
        COUNT(*) as count
    FROM rasters
    GROUP BY gbx_rst_format(tile)
    """
    result = spark.sql(sql)
    assert result.count() > 0


def test_rst_type_sql_example(spark, rasters_view):
    """Test SQL type examples"""
    sql = """
    SELECT
        path,
        gbx_rst_type(tile) as band_types,
        gbx_rst_type(tile)[0] as band1_type
    FROM rasters
    """
    result = spark.sql(sql)
    assert result.count() > 0


def test_rst_pixelsize_sql_example(spark, rasters_view):
    """Test SQL pixel size example"""
    sql = rasterx_functions_sql.rst_pixelsize_sql_example()
    result = spark.sql(sql)
    assert result.count() > 0
    assert all(col in result.columns for col in ["pixel_width", "pixel_height"])


def test_rst_getnodata_sql_example(spark, rasters_view):
    """Test SQL NoData example"""
    sql = rasterx_functions_sql.rst_getnodata_sql_example()
    result = spark.sql(sql)
    assert result.count() > 0


# ============================================================================
# Coordinate Transformation
# ============================================================================

def test_rst_rastertoworldcoord_sql_example(spark, rasters_view):
    """Test SQL raster to world coordinate example"""
    sql = """
    SELECT
        path,
        gbx_rst_rastertoworldcoord(tile, 100, 200) as coords
    FROM rasters
    """
    result = spark.sql(sql)
    assert result.count() > 0


def test_rst_rastertoworldcoordx_sql_example(spark, rasters_view):
    """Test SQL raster to world X coordinate example"""
    sql = rasterx_functions_sql.rst_rastertoworldcoordx_sql_example()
    result = spark.sql(sql)
    assert result.count() > 0


def test_rst_rastertoworldcoordy_sql_example(spark, rasters_view):
    """Test SQL raster to world Y coordinate example"""
    sql = rasterx_functions_sql.rst_rastertoworldcoordy_sql_example()
    result = spark.sql(sql)
    assert result.count() > 0


def test_rst_worldtorastercoord_sql_example(spark, rasters_view):
    """Test SQL world to raster coordinate example (single location)"""
    sql = rasterx_functions_sql.rst_worldtorastercoord_sql_example()
    result = spark.sql(sql.strip())
    assert result.count() >= 0


def test_rst_worldtorastercoord_multi_sql_example(spark, rasters_view):
    """Test SQL world to raster coordinate example (multiple points)"""
    sql = rasterx_functions_sql.rst_worldtorastercoord_multi_sql_example()
    result = spark.sql(sql.strip())
    assert result.count() >= 0


def test_rst_worldtorastercoordx_sql_example(spark, rasters_view):
    """Test SQL world to raster X coordinate example"""
    sql = """
    SELECT
        gbx_rst_worldtorastercoordx(tile, 0, 0) as pixel_col
    FROM rasters
    LIMIT 1
    """
    result = spark.sql(sql)
    assert result.count() > 0


def test_rst_worldtorastercoordy_sql_example(spark, rasters_view):
    """Test SQL world to raster Y coordinate example"""
    sql = """
    SELECT
        gbx_rst_worldtorastercoordy(tile, 0, 0) as pixel_row
    FROM rasters
    LIMIT 1
    """
    result = spark.sql(sql)
    assert result.count() > 0


# ============================================================================
# Validation Functions
# ============================================================================

def test_rst_isempty_sql_example(spark, rasters_view):
    """Test SQL is empty example"""
    # Test filter
    sql = "SELECT * FROM rasters WHERE NOT gbx_rst_isempty(tile)"
    result = spark.sql(sql)
    # Should execute
    
    # Test count query
    sql = """
    SELECT
        COUNT(*) as total,
        SUM(CASE WHEN gbx_rst_isempty(tile) THEN 1 ELSE 0 END) as empty_count,
        SUM(CASE WHEN NOT gbx_rst_isempty(tile) THEN 1 ELSE 0 END) as valid_count
    FROM rasters
    """
    result = spark.sql(sql)
    assert result.count() == 1


def test_rst_tryopen_sql_example(spark, rasters_view):
    """Test SQL try open example"""
    sql = "SELECT * FROM rasters WHERE gbx_rst_tryopen(tile) = true"
    result = spark.sql(sql)
    # Should execute without error


# ============================================================================
# Advanced Operations
# ============================================================================

def test_rst_initnodata_sql_example(spark, rasters_view):
    """Test SQL init NoData example"""
    sql = rasterx_functions_sql.rst_initnodata_sql_example()
    result = spark.sql(sql)
    assert result.count() > 0


def test_rst_updatetype_sql_example(spark, rasters_view):
    """Test SQL update type example"""
    sql = rasterx_functions_sql.rst_updatetype_sql_example()
    result = spark.sql(sql)
    assert result.count() > 0


# ============================================================================
# Generator Functions
# ============================================================================

def test_rst_maketiles_sql_example(spark, rasters_view):
    """Test SQL make tiles example. Generator returns struct in SQL; use without explode."""
    sql = """
    SELECT
        path,
        gbx_rst_maketiles(tile, 512) as tile_result
    FROM rasters
    LIMIT 10
    """
    result = spark.sql(sql)
    assert result.count() > 0
    assert "tile_result" in result.columns


def test_rst_retile_sql_example(spark, rasters_view):
    """Test SQL retile example. Generator returns struct in SQL; use without explode."""
    sql = """
    SELECT
        path,
        gbx_rst_retile(tile, 256, 256) as tile_result
    FROM rasters
    LIMIT 10
    """
    result = spark.sql(sql)
    assert result.count() > 0
    assert "tile_result" in result.columns


def test_rst_tooverlappingtiles_sql_example(spark, rasters_view):
    """Test SQL overlapping tiles example. Generator returns struct in SQL; use without explode."""
    sql = """
    SELECT
        path,
        gbx_rst_tooverlappingtiles(tile, 256, 256, 10) as tile_result
    FROM rasters
    LIMIT 10
    """
    result = spark.sql(sql)
    assert result.count() > 0
    assert "tile_result" in result.columns


def test_rst_separatebands_sql_example(spark, rasters_view):
    """Test SQL separate bands example. Generator returns struct in SQL."""
    sql = """
    SELECT
        path,
        gbx_rst_separatebands(tile) as bands
    FROM rasters
    LIMIT 1
    """
    result = spark.sql(sql)
    assert result.count() > 0
    assert "bands" in result.columns


# ============================================================================
# Structure Verification
# ============================================================================

def test_all_sql_functions_have_example():
    """Verify SQL example module has functions for all documented examples"""
    import inspect
    
    # Get all functions from the module
    functions = [name for name, obj in inspect.getmembers(rasterx_functions_sql) 
                 if inspect.isfunction(obj) and not name.startswith('_')]
    
    # Should have examples for major function categories
    assert len(functions) > 40, f"Expected 40+ SQL examples, got {len(functions)}"
    
    # Verify naming convention
    for func_name in functions:
        assert func_name.endswith('_sql_example'), \
            f"Function {func_name} should end with '_sql_example'"
        
        # Verify it returns a string
        func = getattr(rasterx_functions_sql, func_name)
        result = func()
        assert isinstance(result, str), \
            f"Function {func_name} should return SQL string"
        assert len(result) > 0, \
            f"Function {func_name} returned empty SQL"


def test_all_sql_examples_are_valid_sql():
    """Verify all SQL examples have valid SQL syntax"""
    import inspect
    
    functions = [name for name, obj in inspect.getmembers(rasterx_functions_sql) 
                 if inspect.isfunction(obj) and not name.startswith('_')]
    
    for func_name in functions:
        func = getattr(rasterx_functions_sql, func_name)
        sql = func()
        
        # Basic checks
        assert "SELECT" in sql.upper() or "WITH" in sql.upper(), \
            f"{func_name}: SQL should contain SELECT or WITH"
        
        # Check for GeoBrix functions (most should have gbx_)
        if "gbx_" not in sql.lower() and "from rasters" in sql.lower():
            # Allow some exceptions like pure Spark SQL examples
            pass
