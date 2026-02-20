"""
Tests for Vector Data page code examples (constants = payload only in docs).

Documentation: docs/docs/sample-data/vector-data.mdx
Run: pytest docs/tests/python/sample_data/test_vector_data.py -v
"""
import pytest
from . import vector_data


_VECTOR_CONSTANTS = [
    ("DOWNLOAD_NYC_TAXI_ZONES", "geojson_ogr"),
    ("USAGE_NYC_TAXI_ZONES", "nyc/taxi-zones"),
    ("DOWNLOAD_LONDON_POSTCODES", "london_postcodes"),
    ("USAGE_LONDON_POSTCODES", "london/postcodes"),
    ("DOWNLOAD_NYC_BOROUGHS", "boro_name"),
    ("USAGE_NYC_BOROUGHS", "nyc/boroughs"),
    ("DOWNLOAD_NYC_NTA", "ntaname"),
    ("USAGE_NYC_NTA", "nyc-neighborhoods"),
    ("DOWNLOAD_LONDON_BOROUGHS", "London_Borough"),
    ("USAGE_LONDON_BOROUGHS", "geojson_ogr"),
    ("DOWNLOAD_NYC_PARKS", "parks"),
    ("USAGE_NYC_PARKS", "nyc/parks"),
    ("DOWNLOAD_NYC_SUBWAY", "Subway"),
    ("USAGE_NYC_SUBWAY", "nyc/subway"),
    ("CREATE_GEOPACKAGE_NYC", "GPKG"),
    ("USAGE_GEOPACKAGE_NYC", "layerName"),
    ("CREATE_FILEGDB_NYC", "CreateLayer"),
    ("USAGE_FILEGDB_NYC", "filegdb"),
]

_OUTPUT_CONSTANTS = [
    ("DOWNLOAD_LONDON_POSTCODES_output", "Features"),
    ("DOWNLOAD_NYC_NTA_output", "Neighborhoods"),
    ("DOWNLOAD_LONDON_BOROUGHS_output", "Boroughs"),
    ("DOWNLOAD_NYC_PARKS_output", "Parks"),
    ("DOWNLOAD_NYC_SUBWAY_output", "Stations"),
    ("CREATE_GEOPACKAGE_NYC_output", "Layers"),
    ("CREATE_FILEGDB_NYC_output", "Feature Classes"),
]


@pytest.mark.structure
@pytest.mark.parametrize("const_name,key", _VECTOR_CONSTANTS)
def test_vector_constant_exists_and_contains_key(const_name, key):
    val = getattr(vector_data, const_name)
    assert isinstance(val, str), f"{const_name} should be str"
    assert key in val, f"{const_name} should contain '{key}'"


@pytest.mark.structure
@pytest.mark.parametrize("const_name,key", _OUTPUT_CONSTANTS)
def test_vector_output_constant_exists_and_contains_key(const_name, key):
    val = getattr(vector_data, const_name)
    assert isinstance(val, str), f"{const_name} should be str"
    assert key in val, f"{const_name} should contain '{key}'"
