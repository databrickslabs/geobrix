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


# --- Spec R2: integer SRID classified via authoritative PROJ code sets --------


def test_resolve_int_epsg_vs_esri_authoritative():
    # EPSG-only codes -> EPSG authority
    assert C.resolve_crs(4326).to_authority() == ("EPSG", "4326")
    assert C.resolve_crs(27700).to_authority() == ("EPSG", "27700")
    # ESRI-only codes -> ESRI authority (NOT mislabeled EPSG by the lenient constructor)
    assert C.resolve_crs(54008).to_authority() == ("ESRI", "54008")
    assert C.resolve_crs(102008).to_authority() == ("ESRI", "102008")
    assert C.resolve_crs("54008").to_authority() == ("ESRI", "54008")  # int-like string
    # int-classified == explicit string form
    assert C.resolve_crs(54008) == C.resolve_crs("ESRI:54008")


def test_resolve_unresolvable_int_raises():
    with pytest.raises(ValueError, match="valid EPSG or ESRI"):
        C.resolve_crs(99999999)


def test_canonical_labels_esri_code_as_esri():
    assert C.crs_to_canonical(C.resolve_crs(54008)) == "ESRI:54008"
    assert C.crs_to_canonical(C.resolve_crs(4326)) == "EPSG:4326"


def test_resolve_numpy_int_srid():
    # shapely.get_srid returns np.int32 — resolve_crs must treat it as an int SRID,
    # not fall to the string branch (which raises CRSError on a numpy scalar).
    import numpy as np

    assert C.resolve_crs(np.int32(4326)) == CRS.from_epsg(4326)
    assert C.resolve_crs(np.int64(54008)).to_authority() == ("ESRI", "54008")
    # bool is an int subclass but is not a SRID — must not be treated as int-like
    assert not C._is_intlike(True)


def test_resolve_deprecated_epsg_still_epsg():
    # A deprecated-but-real EPSG code must classify as EPSG (allow_deprecated), not raise.
    # EPSG:4226 (Cape, deprecated) is a classic deprecated geographic CRS.
    c = C.resolve_crs(4226)
    assert c.to_authority()[0] == "EPSG"
