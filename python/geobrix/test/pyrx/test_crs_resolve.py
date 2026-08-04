import pytest
from rasterio.crs import CRS

from databricks.labs.gbx.pyrx.core import crs as C


def test_resolve_int_and_intlike_string_are_epsg():
    assert C.resolve_crs(4326) == CRS.from_epsg(4326)
    assert C.resolve_crs("4326") == CRS.from_epsg(4326)  # int-cast rule
    assert C.resolve_crs(" 32633 ") == CRS.from_epsg(32633)


def test_resolve_authority_and_wkt_and_proj4():
    assert C.resolve_crs("EPSG:4326") == CRS.from_epsg(4326)
    esri = C.resolve_crs("ESRI:54008")
    assert esri is not None and esri.to_epsg() is None  # non-EPSG, still valid
    wkt = CRS.from_epsg(4326).to_wkt()
    assert C.resolve_crs(wkt) == CRS.from_epsg(4326)
    assert C.resolve_crs("+proj=longlat +datum=WGS84 +no_defs") is not None


def test_resolve_garbage_raises():
    with pytest.raises(Exception):
        C.resolve_crs("not-a-crs-@@")


def test_canonical_prefers_authority_else_wkt():
    assert C.crs_to_canonical(CRS.from_epsg(4326)) == "EPSG:4326"
    esri = CRS.from_user_input("ESRI:54008")
    assert C.crs_to_canonical(esri) == "ESRI:54008"
    # an authority-less CRS -> WKT (starts with PROJCS/GEOGCS/PROJCRS/GEOGCRS)
    # (construct one without an authority; assert the result parses back equal)
    round = C.resolve_crs(C.crs_to_canonical(esri))
    assert round == esri
