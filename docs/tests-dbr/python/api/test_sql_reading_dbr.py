"""
Tests for SQL reading-data examples that use DBR ST_ functions.

Validates constants for docs/docs/api/sql.mdx#reading-data-with-sql.
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import sql_reading_dbr


def test_sql_read_shapefile_constants():
    """SQL constants for Reading Data with SQL (Shapefile) are defined."""
    assert hasattr(sql_reading_dbr, "SQL_READ_SHAPEFILE")
    assert hasattr(sql_reading_dbr, "SQL_READ_SHAPEFILE_output")
    assert "st_geomfromwkb" in sql_reading_dbr.SQL_READ_SHAPEFILE
    assert "st_area" in sql_reading_dbr.SQL_READ_SHAPEFILE
    assert "st_centroid" in sql_reading_dbr.SQL_READ_SHAPEFILE
