import pyproj
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


_WKT_32633 = CRS.from_epsg(32633).to_wkt()
_GRID_WKT = CRS.from_epsg(4326).to_wkt()[:-1] + ',EXTENSION["PROJ4_GRIDS","{}"]]'
CRS_VALUES = [
    # (value, accepted?, what the refusal names)
    ("secret_crs", False, "file"),
    ("./secret_crs", False, "file"),
    ("{abs}", False, "file"),
    (
        "+proj=longlat +datum=WGS84 +nadgrids=/etc/hostname +type=crs",
        False,
        "/etc/hostname",
    ),
    (
        "+proj=longlat +datum=WGS84 +geoidgrids=./egm96.gtx +type=crs",
        False,
        "./egm96.gtx",
    ),
    (_GRID_WKT.format("/etc/hostname"), False, "/etc/hostname"),
    ("+proj=longlat +datum=WGS84 +init=/tmp/epsg:4326", False, "/tmp/epsg:4326"),
    ("http://example.com/crs.wkt", False, "URL"),
    ("+proj=merc +a=6378137 +b=6378137 +nadgrids=@null +wktext +no_defs", True, None),
    (
        "+proj=longlat +datum=WGS84 +nadgrids=@gbx_registered_grid.gsb +type=crs",
        True,
        None,
    ),
    ("EPSG:3857", True, None),
    ("esri:102003", True, None),
    ("OGC:CRS84", True, None),
    (_WKT_32633, True, None),
    (pyproj.CRS.from_epsg(3857).to_json(), True, None),
    # m-4: a PROJJSON parameter that names a grid by path (the `"value"` key branch), and a bare registered grid name
    (
        '{"type": "ProjectedCRS", "name": "gbx", "base_crs": {"name": "WGS 84"}, "conversion": {"name": "gbx", '
        '"method": {"name": "Transverse Mercator"}, "parameters": [{"name": "Latitude of natural origin", '
        '"value": "/etc/hostname", "unit": "degree"}]}}',
        False,
        "/etc/hostname",
    ),
    (
        "+proj=longlat +datum=WGS84 +nadgrids=gbx_registered_grid.gsb +type=crs",
        True,
        None,
    ),
    # m-5: the named datums and an OGC URN are admitted (a ruling, disclosed): both resolve without reading a file
    ("WGS84", True, None),
    ("urn:ogc:def:crs:EPSG::4326", True, None),
    # compound codes (a horizontal CRS plus a vertical one), as the docs spell them; a path after the + is refused
    ("EPSG:4326+5773", True, None),
    ("EPSG:4258+5701", True, None),
    ("EPSG:4326+./secret_crs", False, "file"),
    # an AUTHORITY:CODE PROJ does not know is a file name to GDAL (a planted "GBX:secret_crs" below): refused
    ("GBX:secret_crs", False, "file"),
    ("EPSG:4326+GBX:secret_crs", False, "file"),
]


def test_a_crs_object_passes_and_a_parameter_mapping_naming_a_grid_path_is_refused():
    """resolve_crs takes a CRS as it is, and a mapping of PROJ parameters under the string rule (a grid by path is
    refused, naming it)."""
    from rasterio.crs import CRS as RioCRS

    crs = RioCRS.from_epsg(4326)
    assert C.resolve_crs(crs) is crs
    with pytest.raises(ValueError, match="/etc/hostname"):
        C.resolve_crs(
            {"proj": "longlat", "datum": "WGS84", "nadgrids": "/etc/hostname"}
        )
    assert (
        C.resolve_crs({"proj": "longlat", "datum": "WGS84", "no_defs": True})
        is not None
    )


def test_a_named_datum_is_the_datum_never_a_planted_file_of_that_name(
    tmp_path, monkeypatch
):
    """m-5: `WGS84` names a datum; a file of that name in the working directory is not read (GDAL resolves named
    datums before its file check, the order this rule relies on)."""
    (tmp_path / "WGS84").write_text(_WKT_32633)
    monkeypatch.chdir(tmp_path)
    assert C.resolve_crs("WGS84").to_epsg() == 4326


@pytest.mark.parametrize("value,accepted,named", CRS_VALUES)
def test_a_crs_value_never_reaches_a_file_or_the_network(
    tmp_path, monkeypatch, value, accepted, named
):
    """The one matrix of the rule: a file name, a URL or a grid named by path is refused (with what it is and a
    Fix), never read; the CRS spellings GeoBrix documents, bare and optional grid names included, resolve.
    """
    secret = tmp_path / "secret_crs"
    secret.write_text(_WKT_32633)  # a planted file the parser must never read
    (tmp_path / "GBX:secret_crs").write_text(_WKT_32633)  # the same, named as a code
    monkeypatch.chdir(tmp_path)
    value = value.replace("{abs}", str(secret))
    if accepted:
        assert C.resolve_crs(value) is not None
        return
    with pytest.raises(ValueError) as exc:
        C.resolve_crs(value)
    msg = str(exc.value)
    assert (named if named in ("file", "URL") else named) in msg and "Fix:" in msg
