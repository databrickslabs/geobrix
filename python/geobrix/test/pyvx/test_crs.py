"""Tests for pyvx CRS functions: st_crs, st_setcrs, st_transformcrs.

Covers:
- Core medium-preserving layer (bytes in -> bytes out; str in -> str out)
- SQL/UDF layer (always BINARY / STRING return types)
- Encoding matrix: WKB / EWKB / WKT / EWKT inputs
- ESRI code round-trip via authoritative classification
- authority-less CRS rejection in st_setcrs
- never-error invariant for st_transformcrs on unresolvable source
"""

import pytest

shapely = pytest.importorskip("shapely")
import shapely as _shapely  # noqa: E402 (already imported above via importorskip)
from shapely import from_wkb, set_srid, to_wkb, to_wkt  # noqa: E402
from shapely.geometry import Point  # noqa: E402

from databricks.labs.gbx.pyvx import _crs  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ewkb(srid):
    """WKB bytes with embedded SRID (EWKB)."""
    return to_wkb(_shapely.set_srid(Point(11.0, 42.0), srid), include_srid=True)


def _ewkt(srid):
    """EWKT string with embedded SRID prefix."""
    return f"SRID={srid};POINT (11 42)"


def _plain_wkb():
    """Plain WKB bytes (no SRID)."""
    return to_wkb(Point(11.0, 42.0))


def _plain_wkt():
    """Plain WKT string (no SRID prefix)."""
    return "POINT (11 42)"


# ---------------------------------------------------------------------------
# st_crs
# ---------------------------------------------------------------------------


def test_st_crs_reads_embedded_srid():
    assert _crs.st_crs(_ewkb(4326)) == "EPSG:4326"
    assert _crs.st_crs(_ewkb(54008)) == "ESRI:54008"
    assert _crs.st_crs(_plain_wkb()) is None  # plain WKB -> null
    assert _crs.st_crs(_plain_wkt()) is None  # plain WKT -> null


def test_st_crs_ewkt_input():
    """EWKT input (text medium) — read embedded SRID."""
    assert _crs.st_crs(_ewkt(4326)) == "EPSG:4326"
    assert _crs.st_crs(_ewkt(54008)) == "ESRI:54008"


def test_st_crs_plain_wkt_null():
    """Plain WKT (no SRID) -> None."""
    assert _crs.st_crs("POINT (0 0)") is None


def test_st_crs_esri_roundtrip():
    """ESRI code stored as bare int in EWKB must be re-classified as ESRI, not EPSG."""
    # shapely stores srid as a bare int; get_srid returns 54008 (int).
    # resolve_crs(54008) must return ESRI:54008 via authoritative classification.
    result = _crs.st_crs(_ewkb(54008))
    assert result == "ESRI:54008"


# ---------------------------------------------------------------------------
# st_setcrs — medium-preserving layer
# ---------------------------------------------------------------------------


def test_st_setcrs_stamps_srid_encoding_preserving():
    # WKB in -> EWKB bytes out
    out = _crs.st_setcrs(to_wkb(Point(0, 0)), "EPSG:32633")
    assert isinstance(out, (bytes, bytearray))
    assert _shapely.get_srid(from_wkb(out)) == 32633

    # WKT in -> EWKT str out
    out2 = _crs.st_setcrs("POINT (0 0)", "ESRI:54008")
    assert isinstance(out2, str)
    assert out2.upper().startswith("SRID=54008;")


def test_st_setcrs_authority_less_raises():
    """WKT/PROJ4 CRS (no EPSG/ESRI authority) must raise ValueError."""
    with pytest.raises(ValueError):
        _crs.st_setcrs(to_wkb(Point(0, 0)), "+proj=aea +lat_1=29.5 +datum=WGS84 +no_defs")


def test_st_setcrs_ewkb_input():
    """EWKB in -> EWKB out (replaces existing SRID)."""
    ewkb_in = _ewkb(4326)
    out = _crs.st_setcrs(ewkb_in, "EPSG:32633")
    assert isinstance(out, (bytes, bytearray))
    assert _shapely.get_srid(from_wkb(out)) == 32633


def test_st_setcrs_ewkt_input():
    """EWKT in -> EWKT out (replaces existing SRID prefix)."""
    ewkt_in = _ewkt(4326)
    out = _crs.st_setcrs(ewkt_in, "EPSG:32633")
    assert isinstance(out, str)
    assert out.upper().startswith("SRID=32633;")


def test_st_setcrs_wkt_authority_less_wkt_raises():
    """Full WKT CRS with no authority must raise."""
    wkt_crs = (
        'PROJCS["Custom_TM",'
        'GEOGCS["WGS 84",'
        'DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],'
        'PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]],'
        "PROJECTION[\"Transverse_Mercator\"],"
        'PARAMETER["central_meridian",13.7],'
        'PARAMETER["scale_factor",0.9996],'
        'UNIT["metre",1]]'
    )
    with pytest.raises(ValueError):
        _crs.st_setcrs(to_wkb(Point(0, 0)), wkt_crs)


# ---------------------------------------------------------------------------
# st_transformcrs — medium-preserving layer
# ---------------------------------------------------------------------------

_CUSTOM_TM_WKT = (
    'PROJCS["Custom_TM",'
    'GEOGCS["WGS 84",'
    'DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],'
    'PRIMEM["Greenwich",0],'
    'UNIT["degree",0.0174532925199433]],'
    "PROJECTION[\"Transverse_Mercator\"],"
    'PARAMETER["central_meridian",13.7],'
    'PARAMETER["scale_factor",0.9996],'
    'UNIT["metre",1]]'
)


def test_st_transformcrs_matrix():
    # EWKB + EPSG target -> EWKB, SRID=target; coords reprojected
    out = _crs.st_transformcrs(_ewkb(4326), "EPSG:32633")
    g = from_wkb(out)
    assert isinstance(out, (bytes, bytearray))
    assert _shapely.get_srid(g) == 32633
    assert g.x > 100_000  # easting in UTM33N

    # EWKB + WKT target -> WKB (SRID cleared, authority-less)
    out2 = _crs.st_transformcrs(_ewkb(4326), _CUSTOM_TM_WKT)
    assert isinstance(out2, (bytes, bytearray))
    assert _shapely.get_srid(from_wkb(out2)) == 0

    # plain WKB, no source_crs -> returned unchanged (never-error invariant)
    plain = _plain_wkb()
    assert _crs.st_transformcrs(plain, "EPSG:32633") == plain

    # plain WKB + explicit source_crs -> reprojected
    out3 = _crs.st_transformcrs(plain, "EPSG:32633", source_crs="EPSG:4326")
    assert from_wkb(out3).x > 100_000


def test_st_transformcrs_ewkt_input_text_medium_preserved():
    """EWKT text in -> WKT/EWKT text out (medium preserved)."""
    ewkt_in = _ewkt(4326)

    # EWKT + EPSG target -> EWKT out, SRID=target
    out = _crs.st_transformcrs(ewkt_in, "EPSG:32633")
    assert isinstance(out, str)
    assert out.upper().startswith("SRID=32633;")

    # EWKT + WKT target -> WKT out (plain, SRID cleared)
    out2 = _crs.st_transformcrs(ewkt_in, _CUSTOM_TM_WKT)
    assert isinstance(out2, str)
    assert "SRID=" not in out2.upper()

    # EWKT + explicit source override uses embedded SRID (not the override)
    # per resolve_source_crs: embedded wins -> no-op on source
    out3 = _crs.st_transformcrs(ewkt_in, "EPSG:32633", source_crs="EPSG:4326")
    assert isinstance(out3, str)
    assert out3.upper().startswith("SRID=32633;")


def test_st_transformcrs_plain_wkt_with_source_crs():
    """Plain WKT string + explicit source_crs -> text out, reprojected."""
    out = _crs.st_transformcrs(_plain_wkt(), "EPSG:32633", source_crs="EPSG:4326")
    assert isinstance(out, str)
    # should have easting > 100_000 when parsed
    import shapely as _sh
    from shapely import from_wkt

    wkt_clean = out
    if wkt_clean.upper().startswith("SRID="):
        _, _, wkt_clean = wkt_clean.partition(";")
    assert from_wkt(wkt_clean).x > 100_000


# ---------------------------------------------------------------------------
# SQL / UDF layer — BINARY / STRING normalization
# ---------------------------------------------------------------------------


def test_udf_st_crs_returns_string():
    """Registered UDF path: return type is STRING (not bytes)."""
    result = _crs._udf_st_crs(_ewkb(4326))
    assert isinstance(result, str)
    assert result == "EPSG:4326"

    result_null = _crs._udf_st_crs(_plain_wkb())
    assert result_null is None


def test_udf_st_setcrs_returns_binary():
    """Registered UDF path: return type is BINARY (bytes), even for text inputs."""
    out = _crs._udf_st_setcrs("POINT (0 0)", "EPSG:4326")
    assert isinstance(out, (bytes, bytearray))
    assert _shapely.get_srid(from_wkb(out)) == 4326


def test_udf_st_transformcrs_returns_binary():
    """Registered UDF path: return type is BINARY (bytes), even for text inputs."""
    out = _crs._udf_st_transformcrs(_ewkt(4326), "EPSG:32633")
    assert isinstance(out, (bytes, bytearray))
    assert _shapely.get_srid(from_wkb(out)) == 32633
