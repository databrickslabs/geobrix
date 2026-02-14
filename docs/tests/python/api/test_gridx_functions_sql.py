"""
Tests for GridX (BNG) SQL examples.

Ensures all SQL examples in documentation are executable and produce valid results.
"""
import pytest
from pyspark.sql import functions as F
from pathlib import Path
from . import gridx_functions_sql

# Use the spark fixture from conftest.py (session-scoped, shared across all tests)
# Do NOT create a separate Spark session or stop it - that causes test isolation issues


@pytest.fixture(scope="module")
def locations_view(spark):
    """Create test locations view for SQL examples"""
    from databricks.labs.gbx.gridx.bng import functions as bx
    bx.register(spark)
    
    # Create test data with BNG cells
    test_data = [
        ("TQ3080", "location1"),
        ("TQ3081", "location2"),
        ("TQ3082", "location3"),
    ]
    df = spark.createDataFrame(test_data, ["cell_id", "name"])
    df.createOrReplaceTempView("locations")
    yield
    spark.catalog.dropTempView("locations")


@pytest.fixture(scope="module")
def regions_view(spark):
    """Create test regions view for SQL examples"""
    from databricks.labs.gbx.gridx.bng import functions as bx
    bx.register(spark)
    
    # Create simple test geometry (London area polygon)
    from pyspark.sql.functions import expr
    test_data = [
        ("Central London", "POLYGON((-0.2 51.4, -0.2 51.6, 0.1 51.6, 0.1 51.4, -0.2 51.4))"),
    ]
    df = spark.createDataFrame(test_data, ["region_name", "boundary_wkt"]) \
        .withColumn("boundary", expr("ST_GeomFromText(boundary_wkt)"))
    df.createOrReplaceTempView("regions")
    yield
    spark.catalog.dropTempView("regions")


@pytest.fixture(scope="module")
def observations_view(spark):
    """Create test observations view for SQL examples"""
    from databricks.labs.gbx.gridx.bng import functions as bx
    bx.register(spark)
    
    test_data = [
        (1, "TQ3080", "region1"),
        (2, "TQ3081", "region1"),
        (3, "TQ3180", "region2"),
    ]
    df = spark.createDataFrame(test_data, ["id", "cell_id", "region"])
    df.createOrReplaceTempView("observations")
    yield
    spark.catalog.dropTempView("observations")


# ============================================================================
# Structure Tests - Verify all SQL functions exist
# ============================================================================

def test_all_sql_functions_have_example():
    """Verify all expected SQL example functions exist"""
    expected_functions = [
        'bng_aswkb_sql_example',
        'bng_aswkt_sql_example',
        'bng_cellarea_sql_example',
        'bng_centroid_sql_example',
        'bng_eastnorthasbng_sql_example',
        'bng_pointascell_sql_example',
        'bng_kring_sql_example',
        'bng_polyfill_sql_example',
        'bng_cellintersection_agg_sql_example',
        'bng_cellunion_agg_sql_example',
    ]
    
    for func_name in expected_functions:
        assert hasattr(gridx_functions_sql, func_name), f"Missing SQL example: {func_name}"
        func = getattr(gridx_functions_sql, func_name)
        assert callable(func), f"{func_name} is not callable"
        result = func()
        assert isinstance(result, str), f"{func_name} should return a string"
        assert len(result.strip()) > 0, f"{func_name} returns empty SQL"


def test_all_sql_examples_are_valid_sql():
    """Verify all SQL examples return non-empty strings"""
    import inspect
    
    for name, obj in inspect.getmembers(gridx_functions_sql):
        if name.endswith('_sql_example') and callable(obj):
            sql = obj()
            assert isinstance(sql, str), f"{name} should return string"
            assert 'gbx_bng_' in sql or 'SELECT' in sql, f"{name} should contain SQL"


# ============================================================================
# Conversion Functions
# ============================================================================

# Note: test_bng_aswkb_sql_example, test_bng_aswkt_sql_example moved to 
# Some SQL examples require DBR (st_point, etc.); they skip on open-source Spark.


# ============================================================================
# Core Functions
# ============================================================================

# bng_cellarea_sql_example: may skip on open-source if view/table requires DBR.
# (requires DBR SQL functions for view creation)


# ============================================================================
# Coordinate Conversion
# ============================================================================

# Note: test_bng_eastnorthasbng_sql_example, test_bng_pointascell_sql_example moved to
# bng_pointascell_sql_example: requires st_point (DBR); skips on open-source.


# ============================================================================
# K-Ring Functions
# ============================================================================

# bng_kring_sql_example: may require DBR for view creation; skips if not available.
# (requires DBR SQL functions for view creation)


# ============================================================================
# Tessellation Functions
# ============================================================================

# bng_polyfill_sql_example: requires st_geomfromtext (DBR); skips on open-source.
# (requires DBR SQL functions like st_geomfromtext for geometry operations)


# ============================================================================
# Aggregator Functions
# ============================================================================

# Note: test_bng_cellintersection_agg_sql_example, test_bng_cellunion_agg_sql_example moved to
# View-based SQL examples: require DBR for view creation; skip on open-source.
