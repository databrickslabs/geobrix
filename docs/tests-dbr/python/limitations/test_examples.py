"""
Tests for limitations workaround examples (docs/docs/limitations.mdx#workaround).

Requires Databricks Runtime 17.1+ for st_geomfromwkb / st_area.
Skips when run on open-source Spark.
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import examples


def test_convert_to_databricks_geometry_workaround(spark, sample_nyc_subway_shp):
    """Validate the workaround: GeoBrix shapefile -> GEOMETRY -> st_area."""
    try:
        result = examples.convert_to_databricks_geometry_workaround(spark, sample_nyc_subway_shp)
        assert "geometry" in result.columns
        assert "area" in result.columns
        n = result.count()
        assert n > 0, "expected at least one row"
    except Exception as e:
        err = str(e).lower()
        if "st_geomfromwkb" in err or "unresolved_routine" in err or "analysis" in err:
            pytest.skip(f"Databricks Runtime required (st_geomfromwkb): {e}")
        raise


def test_workaround_constant_defined():
    """Display constant used by docs must exist."""
    assert hasattr(examples, "CONVERT_TO_DATABRICKS_GEOMETRY_WORKAROUND")
    assert "st_geomfromwkb" in examples.CONVERT_TO_DATABRICKS_GEOMETRY_WORKAROUND
    assert "st_area" in examples.CONVERT_TO_DATABRICKS_GEOMETRY_WORKAROUND
