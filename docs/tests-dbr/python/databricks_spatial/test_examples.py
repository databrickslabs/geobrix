"""
Tests for Databricks Spatial examples (GEOMETRY, GEOGRAPHY, ST, H3).

Requires Databricks Runtime 17.1+ with ST and H3 support. Tests skip when run
outside DBR (e.g. open-source Spark). Validates examples in
docs/docs/databricks-spatial.mdx.
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import examples


# ---- Constants (structure) ----

@pytest.mark.structure
def test_geometry_constants_defined():
    """All GEOMETRY example constants exist."""
    assert hasattr(examples, "GEOMETRY_CREATE_FROM_WKT")
    assert hasattr(examples, "GEOMETRY_CREATE_FROM_GEOJSON")
    assert hasattr(examples, "GEOMETRY_EXPORT")
    assert "st_geomfromtext" in examples.GEOMETRY_CREATE_FROM_WKT
    assert "st_geomfromgeojson" in examples.GEOMETRY_CREATE_FROM_GEOJSON


@pytest.mark.structure
def test_geography_constants_defined():
    """All GEOGRAPHY example constants exist."""
    assert hasattr(examples, "GEOGRAPHY_CREATE")
    assert hasattr(examples, "GEOGRAPHY_EXPORT")
    assert "st_geogfromtext" in examples.GEOGRAPHY_CREATE


@pytest.mark.structure
def test_st_constants_defined():
    """All ST function example constants exist."""
    assert hasattr(examples, "ST_IMPORT_PYTHON")
    assert hasattr(examples, "ST_MEASUREMENTS")
    assert hasattr(examples, "ST_PREDICATES")
    assert hasattr(examples, "ST_CONSTRUCTORS")
    assert "st_area" in examples.ST_MEASUREMENTS or "dbf.st" in examples.ST_IMPORT_PYTHON


@pytest.mark.structure
def test_h3_constants_defined():
    """All H3 example constants exist."""
    assert hasattr(examples, "H3_IMPORT_PYTHON")
    assert hasattr(examples, "H3_POINT_INDEX")
    assert hasattr(examples, "H3_POLYFILL")
    assert hasattr(examples, "H3_NEIGHBORS")
    assert hasattr(examples, "H3_BOUNDARY")
    assert "h3_longlatash3" in examples.H3_POINT_INDEX


# ---- Execution (DBR-only; skip if ST/H3 not available) ----

def test_geometry_create_from_wkt(spark):
    """Run GEOMETRY create from WKT. Skips if st_geomfromtext not available."""
    try:
        rows = examples.run_geometry_create_from_wkt(spark)
        assert rows is not None
        assert len(rows) == 1
    except RuntimeError as e:
        if "DBR not available" in str(e):
            pytest.skip("Databricks Runtime ST functions not available")
        raise


def test_geography_create(spark):
    """Run GEOGRAPHY create. Skips if st_geogfromtext not available."""
    try:
        rows = examples.run_geography_create(spark)
        assert rows is not None
        assert len(rows) == 1
    except RuntimeError as e:
        if "DBR not available" in str(e):
            pytest.skip("Databricks Runtime ST functions not available")
        raise


def test_st_measurements(spark):
    """Run ST area example. Skips if st_area not available."""
    try:
        rows = examples.run_st_measurements(spark)
        assert rows is not None
        assert len(rows) == 1
    except RuntimeError as e:
        if "DBR not available" in str(e):
            pytest.skip("Databricks Runtime ST functions not available")
        raise


def test_h3_point_index(spark):
    """Run H3 point index. Skips if h3_longlatash3 not available."""
    try:
        rows = examples.run_h3_point_index(spark)
        assert rows is not None
        assert len(rows) == 1
    except RuntimeError as e:
        if "DBR not available" in str(e):
            pytest.skip("Databricks Runtime H3 functions not available")
        raise
