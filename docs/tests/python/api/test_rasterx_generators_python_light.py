"""
Tests for RasterX generator functions (light tier).

These are doc-tests for the light-tier (pyrx) generator examples in
rasterx_generators_python_light.py. All generators are UDTFs accessed via SQL
LATERAL syntax, not direct df.select calls.
"""

import pytest
from rasterx_generators_python_light import (
    rst_retile_python_light_example,
    rst_tooverlappingtiles_python_light_example,
    rst_separatebands_python_light_example,
    rst_polygonize_python_light_example,
    rst_maketiles_python_light_example,
    rst_rasterize_python_light_example,
)


def test_rst_retile_python_light_example(spark):
    """Light-tier retile returns array of sub-tiles."""
    result = rst_retile_python_light_example(spark)
    assert isinstance(result, list)
    assert len(result) > 0
    assert all(hasattr(row, "asDict") or isinstance(row, dict) for row in result)


def test_rst_tooverlappingtiles_python_light_example(spark):
    """Light-tier overlapping tiles returns array of tiles."""
    result = rst_tooverlappingtiles_python_light_example(spark)
    assert isinstance(result, list)
    assert len(result) > 0


def test_rst_separatebands_python_light_example(spark):
    """Light-tier separatebands returns one row per band."""
    result = rst_separatebands_python_light_example(spark)
    assert isinstance(result, list)
    assert len(result) == 3  # multiband fixture has 3 bands


def test_rst_polygonize_python_light_example(spark):
    """Light-tier polygonize returns one row per region (geom_wkb, value)."""
    result = rst_polygonize_python_light_example(spark)
    assert isinstance(result, list)
    assert len(result) >= 1
    assert "geom_wkb" in result[0].asDict() and "value" in result[0].asDict()


def test_rst_maketiles_python_light_example(spark):
    """Light-tier maketiles returns array of sub-tiles."""
    result = rst_maketiles_python_light_example(spark)
    assert isinstance(result, list)
    assert len(result) >= 1


def test_rst_rasterize_python_light_example(spark):
    """Light-tier rasterize returns a materialized tile struct."""
    result = rst_rasterize_python_light_example(spark)
    assert result is not None
    # Should be a struct with tile fields
    assert hasattr(result, "asDict") or isinstance(result, dict)
