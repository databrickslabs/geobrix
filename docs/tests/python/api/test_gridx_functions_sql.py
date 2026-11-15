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
    from databricks.labs.gbx import manager as gbx_manager
    gbx_manager.register_family(spark, ('gbx_bng_',))
    
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
    from databricks.labs.gbx import manager as gbx_manager
    gbx_manager.register_family(spark, ('gbx_bng_',))
    
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
    from databricks.labs.gbx import manager as gbx_manager
    gbx_manager.register_family(spark, ('gbx_bng_',))
    
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


# ============================================================================
# Array forms of the cell aggregates — run each SQL example and hold its result
# ============================================================================


@pytest.fixture(scope="module")
def array_forms_registered(spark):
    """Register the four grid families so the ``*_array`` SQL functions resolve."""
    from databricks.labs.gbx import manager as gbx_manager

    gbx_manager.register_family(
        spark, ("gbx_bng_", "gbx_custom_", "gbx_quadbin_", "gbx_h3_")
    )
    yield spark


def _run(spark, sql):
    return spark.sql(sql.strip().rstrip(";")).collect()


def _filled(row):
    """The filled array of one group as ``{cellid: value}``."""
    return {c["cellid"]: c["value"] for c in row["filled"]}


@pytest.mark.parametrize(
    "name, center, cells",
    [
        ("bng", "TQ300800", 5),
        ("quadbin", 5234158540624494591, 3),
        ("custom", 216172782113787048, 3),
        ("h3", 612934495919669247, 3),
    ],
)
def test_cellfill_array_sql_examples(array_forms_registered, name, center, cells):
    """Each ``*_cellfill_array`` example fills the NULL center cell to the 5.0 of its neighbours."""
    sql = getattr(gridx_functions_sql, f"{name}_cellfill_array_sql_example")()
    rows = _run(array_forms_registered, sql)
    assert len(rows) == 1 and rows[0]["region"] == 1
    filled = _filled(rows[0])
    assert len(filled) == cells
    assert filled[center] == 5.0, f"{name}: the NULL cell was not filled: {filled}"
    assert all(v == 5.0 for v in filled.values())
    assert list(filled) == sorted(filled), "cells come back in ascending id order"


def test_quadbin_cellfill_sql_example(array_forms_registered):
    """The example builds the ring in a CTE (Spark refuses ``explode(gbx_...)``) and the NULL centre takes 5.0."""
    from databricks.labs.gbx.pygx import _cellfill, _quadbin

    sql = gridx_functions_sql.quadbin_cellfill_sql_example()
    assert "explode(gbx_" not in sql
    rows = _run(array_forms_registered, sql)
    assert len(rows) == 1 and rows[0]["region"] == 1
    cells = dict(_cellfill.decode(bytes(rows[0]["filled"])))
    centre = _quadbin.point_as_cell(-0.1, 51.5, 10)
    assert centre == 5234158540624494591
    assert set(cells) == set(_quadbin.k_ring(centre, 1)), "the centre and its 8 ring-1 neighbours"
    assert cells[centre] == 5.0, f"the NULL centre was not filled: {cells}"
    assert set(cells.values()) == {5.0}


def test_quadbin_cellunion_agg_sql_example(array_forms_registered):
    """The 9-cell ring, exploded from a subquery, dissolves into one geometry that covers the centre cell."""
    from shapely.geometry import Point
    from shapely.wkb import loads

    sql = gridx_functions_sql.quadbin_cellunion_agg_sql_example()
    assert "explode(gbx_" not in sql
    rows = _run(array_forms_registered, sql)
    assert len(rows) == 1 and rows[0]["region"] == "R1"
    geom = loads(bytes(rows[0]["coverage"]))
    assert geom.is_valid and geom.contains(Point(-122.4194, 37.7749))


def test_bng_chip_array_sql_examples(array_forms_registered):
    """The two chips of TQ3080 union to the whole cell and intersect in the shared strip."""
    from shapely import box
    from shapely.wkb import loads

    spark = array_forms_registered
    union = _run(spark, gridx_functions_sql.bng_cellunion_array_sql_example())
    assert len(union) == 1
    assert loads(bytes(union[0]["union_chip"])).equals(box(530000, 180000, 531000, 181000))
    common = _run(spark, gridx_functions_sql.bng_cellintersection_array_sql_example())
    assert len(common) == 1
    assert loads(bytes(common[0]["common_chip"])).equals(box(530400, 180000, 530600, 181000))
