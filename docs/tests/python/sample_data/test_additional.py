"""
Tests for Additional page code examples (constants = payload only in docs).

Documentation: docs/docs/sample-data/additional.mdx
Run: pytest docs/tests/python/sample_data/test_additional.py -v
"""
import pytest
from . import additional


@pytest.mark.structure
def test_synthetic_raster_constant():
    assert isinstance(additional.SYNTHETIC_RASTER, str)
    assert "gdal" in additional.SYNTHETIC_RASTER
    assert "100x100" in additional.SYNTHETIC_RASTER
    assert "rst_width" in additional.SYNTHETIC_RASTER


@pytest.mark.structure
def test_stac_any_location_constant():
    assert isinstance(additional.STAC_ANY_LOCATION, str)
    assert "pystac_client" in additional.STAC_ANY_LOCATION
    assert "sentinel-2-l2a" in additional.STAC_ANY_LOCATION
    assert "my_bbox" in additional.STAC_ANY_LOCATION
