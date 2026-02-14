"""
Tests for Raster Data page code examples (constants = payload only in docs).

Documentation: docs/docs/sample-data/raster-data.mdx
Run: pytest docs/tests/python/sample_data/test_raster_data.py -v
"""
import pytest
from . import raster_data


_RASTER_CONSTANTS = [
    ("DOWNLOAD_SENTINEL_NYC", "pystac_client"),
    ("USAGE_SENTINEL_NYC", "sentinel2"),
    ("DOWNLOAD_SENTINEL_LONDON", "london_bbox"),
    ("USAGE_SENTINEL_LONDON", "london/sentinel2"),
    ("DOWNLOAD_SRTM_NYC", "N40W074"),
    ("USAGE_SRTM_NYC", "nyc-elevation"),
    ("DOWNLOAD_SRTM_LONDON", "N51W001"),
    ("USAGE_SRTM_LONDON", "london-elevation"),
    ("DOWNLOAD_HRRR_NYC", "grib2"),
    ("USAGE_HRRR_NYC", "hrrr-weather"),
]

_RASTER_OUTPUT_CONSTANTS = [
    ("DOWNLOAD_SENTINEL_LONDON_output", "width"),
    ("DOWNLOAD_SRTM_LONDON_output", "min_elevation"),
    ("DOWNLOAD_HRRR_NYC_output", "Subdatasets"),
]


@pytest.mark.structure
@pytest.mark.parametrize("const_name,key", _RASTER_CONSTANTS)
def test_raster_constant_exists_and_contains_key(const_name, key):
    val = getattr(raster_data, const_name)
    assert isinstance(val, str), f"{const_name} should be str"
    assert key in val, f"{const_name} should contain '{key}'"


@pytest.mark.structure
@pytest.mark.parametrize("const_name,key", _RASTER_OUTPUT_CONSTANTS)
def test_raster_output_constant_exists_and_contains_key(const_name, key):
    val = getattr(raster_data, const_name)
    assert isinstance(val, str), f"{const_name} should be str"
    assert key in val, f"{const_name} should contain '{key}'"
