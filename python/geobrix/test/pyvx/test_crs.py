"""Tests for pyvx CRS functions: st_crs, st_setcrs, st_transformcrs.

Covers:
- Core medium-preserving layer (bytes in -> bytes out; str in -> str out)
- SQL/UDF layer (always BINARY / STRING return types)
- Full encoding matrix: WKB / EWKB / WKT / EWKT inputs x authority / authority-less targets
- ESRI code and PROJ4 target paths
- authority-less CRS rejection in st_setcrs
- Never-error invariant: unresolvable embedded SRID and explicit source_crs both degrade
- Coordinate preservation (rounding_precision=-1)
- Text-vs-binary SQL-surface symmetry
- Unresolvable target raises
"""

import pytest
from shapely import from_wkb, from_wkt, get_srid, to_wkb, to_wkt
from shapely.geometry import Point

from databricks.labs.gbx.pyvx import _crs

shapely = pytest.importorskip("shapely")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# POINT(11, 42) in EPSG:4326 -> EPSG:32633 (UTM33N) = (168701.015089, 4657521.062150)
_UTM33N_X = pytest.approx(168701.015, rel=1e-4)
_UTM33N_Y = pytest.approx(4657521.0, rel=1e-4)

# POINT(11, 42) in EPSG:4326 -> ESRI:54008 (Sinusoidal) = (911358.377, 4651636.879)
_ESRI54008_X = pytest.approx(911358.377, rel=1e-4)

# Custom TM (central_meridian=13.7) applied to (11, 42) => x < 0
_CUSTOM_TM_X_NEGATIVE = True

_CUSTOM_TM_WKT = (
    'PROJCS["Custom_TM",'
    'GEOGCS["WGS 84",'
    'DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],'
    'PRIMEM["Greenwich",0],'
    'UNIT["degree",0.0174532925199433]],'
    'PROJECTION["Transverse_Mercator"],'
    'PARAMETER["central_meridian",13.7],'
    'PARAMETER["scale_factor",0.9996],'
    'UNIT["metre",1]]'
)

_PROJ4_UTM33 = "+proj=utm +zone=33 +datum=WGS84 +units=m +no_defs"

# Dutch RD (Amersfoort / RD New) written as PROJ4 with a NULL datum shift.
# PROJ's fuzzy matcher pairs this with EPSG:28992 at its default 70% confidence, but the
# +towgs84=0,0,0,0,0,0,0 forces a ballpark (identity) datum transformation, putting
# coordinates ~177 m away from real EPSG:28992. It is therefore the case that catches a
# fuzzy authority leaking into the reprojection MATH, which _PROJ4_UTM33 cannot: for
# UTM33/WGS84 the fuzzy match and the exact definition happen to coincide numerically.
_PROJ4_RD_NULL_SHIFT = (
    "+proj=sterea +lat_0=52.15616055555555 +lon_0=5.38763888888889 +k=0.9999079 "
    "+x_0=155000 +y_0=463000 +ellps=bessel +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
)

# A point inside the Dutch RD domain, where the null-shift error is large.
_RD_LON, _RD_LAT = 5.39, 52.16


def _ewkb_at(lon, lat, srid):
    """EWKB bytes for POINT(lon, lat) with the given SRID."""
    import shapely as _sh

    return to_wkb(_sh.set_srid(Point(lon, lat), srid), include_srid=True)


def _ewkb(srid):
    """EWKB bytes for POINT(11, 42) with given SRID."""
    import shapely as _sh

    return to_wkb(_sh.set_srid(Point(11.0, 42.0), srid), include_srid=True)


def _ewkt(srid):
    """EWKT string POINT(11, 42) with SRID prefix."""
    return f"SRID={srid};POINT (11 42)"


def _plain_wkb():
    return to_wkb(Point(11.0, 42.0))


def _plain_wkt():
    return "POINT (11 42)"


# ---------------------------------------------------------------------------
# st_crs
# ---------------------------------------------------------------------------


def test_st_crs_reads_embedded_srid_bytes():
    assert _crs.st_crs(_ewkb(4326)) == "EPSG:4326"
    assert _crs.st_crs(_ewkb(54008)) == "ESRI:54008"
    assert _crs.st_crs(_plain_wkb()) is None


def test_st_crs_reads_embedded_srid_text():
    assert _crs.st_crs(_ewkt(4326)) == "EPSG:4326"
    assert _crs.st_crs(_ewkt(54008)) == "ESRI:54008"
    assert _crs.st_crs(_plain_wkt()) is None


def test_st_crs_none_input():
    assert _crs.st_crs(None) is None


def test_st_crs_unresolvable_srid_degrades():
    """SRID that is not in EPSG or ESRI registries -> None (never-error)."""
    assert _crs.st_crs(_ewkb(999999)) is None


# ---------------------------------------------------------------------------
# st_setcrs — medium-preserving layer
# ---------------------------------------------------------------------------


def test_st_setcrs_wkb_in_ewkb_out():
    out = _crs.st_setcrs(_plain_wkb(), "EPSG:32633")
    assert isinstance(out, (bytes, bytearray))
    assert get_srid(from_wkb(out)) == 32633


def test_st_setcrs_ewkb_in_ewkb_out_replaces_srid():
    out = _crs.st_setcrs(_ewkb(4326), "EPSG:32633")
    assert isinstance(out, (bytes, bytearray))
    assert get_srid(from_wkb(out)) == 32633


def test_st_setcrs_wkt_in_ewkt_out():
    out = _crs.st_setcrs(_plain_wkt(), "ESRI:54008")
    assert isinstance(out, str)
    assert out.upper().startswith("SRID=54008;")


def test_st_setcrs_ewkt_in_ewkt_out_replaces_srid():
    out = _crs.st_setcrs(_ewkt(4326), "EPSG:32633")
    assert isinstance(out, str)
    assert out.upper().startswith("SRID=32633;")


def test_st_setcrs_coordinate_preservation():
    """Coordinates must not be rounded — rounding_precision=-1 required."""
    pt_wkt = "POINT (11.123456789 42.987654321)"
    out = _crs.st_setcrs(pt_wkt, "EPSG:4326")
    assert isinstance(out, str)
    g = from_wkt(out.split(";", 1)[1] if ";" in out else out)
    assert g.x == pytest.approx(11.123456789, abs=1e-9)
    assert g.y == pytest.approx(42.987654321, abs=1e-9)


def test_st_setcrs_none_geom_returns_none():
    assert _crs.st_setcrs(None, "EPSG:4326") is None


def test_st_setcrs_empty_string_returns_none():
    assert _crs.st_setcrs("", "EPSG:4326") is None


def test_st_setcrs_authority_less_proj4_raises():
    with pytest.raises(ValueError, match="authority-less"):
        _crs.st_setcrs(_plain_wkb(), "+proj=aea +lat_1=29.5 +datum=WGS84 +no_defs")


def test_st_setcrs_authority_less_wkt_raises():
    with pytest.raises(ValueError, match="authority-less"):
        _crs.st_setcrs(_plain_wkb(), _CUSTOM_TM_WKT)


# ---------------------------------------------------------------------------
# st_transformcrs — per-cell matrix tests
# ---------------------------------------------------------------------------


def test_st_transformcrs_ewkb_epsg_target():
    """EWKB + authority-coded EPSG target -> EWKB, SRID=target, coords reprojected."""
    out = _crs.st_transformcrs(_ewkb(4326), "EPSG:32633")
    assert isinstance(out, (bytes, bytearray))
    g = from_wkb(out)
    assert get_srid(g) == 32633
    assert g.x == _UTM33N_X
    assert g.y == _UTM33N_Y


def test_st_transformcrs_ewkb_esri_target():
    """EWKB + authority-coded ESRI target -> EWKB, SRID=54008, coords reprojected."""
    out = _crs.st_transformcrs(_ewkb(4326), "ESRI:54008")
    assert isinstance(out, (bytes, bytearray))
    g = from_wkb(out)
    assert get_srid(g) == 54008
    assert g.x == _ESRI54008_X


def test_st_transformcrs_ewkb_authority_less_wkt_target():
    """EWKB + authority-less WKT target -> plain WKB, SRID cleared, coords reprojected."""
    out = _crs.st_transformcrs(_ewkb(4326), _CUSTOM_TM_WKT)
    assert isinstance(out, (bytes, bytearray))
    g = from_wkb(out)
    assert get_srid(g) == 0
    assert g.x < 0  # central_meridian=13.7, point at lon=11 -> negative easting


def test_st_transformcrs_ewkb_proj4_target():
    """EWKB + PROJ4 target -> plain WKB, SRID cleared, coords reprojected.

    A PROJ4 string carries no authority code. PROJ's fuzzy matcher *would* pair
    '+proj=utm +zone=33 +datum=WGS84' with EPSG:32633 at its default 70% confidence,
    but a geometry SRID is an exact integer identity — a fuzzy match must never be
    written into one. The authority probe therefore runs at full confidence, so a
    PROJ4 target is authority-less: coordinates are reprojected and the stale source
    SRID is cleared. This is also what GDAL reports on the heavyweight tier.
    """
    out = _crs.st_transformcrs(_ewkb(4326), _PROJ4_UTM33)
    assert isinstance(out, (bytes, bytearray))
    g = from_wkb(out)
    assert get_srid(g) == 0
    assert g.x == _UTM33N_X


def test_st_setcrs_proj4_raises_no_fuzzy_epsg_stamp():
    """st_setcrs must NOT stamp a fuzzy-matched EPSG code for a PROJ4 CRS."""
    with pytest.raises(ValueError, match="authority-less"):
        _crs.st_setcrs(_plain_wkb(), _PROJ4_UTM33)


# ---------------------------------------------------------------------------
# Non-numeric authority codes (OGC:CRS84, IGNF:LAMB93)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("crs", ["OGC:CRS84", "IGNF:LAMB93"])
def test_st_transformcrs_non_numeric_authority_code_clears_srid(crs):
    """A resolvable CRS with a non-numeric code takes the authority-less path.

    'OGC:CRS84' and 'IGNF:LAMB93' are real CRSes, but their authority codes
    ('CRS84', 'LAMB93') are not integers, so no SRID can be carried. Under the
    never-error invariant this must reproject and clear the SRID, not raise.
    """
    out = _crs.st_transformcrs(_ewkb(4326), crs)
    assert isinstance(out, (bytes, bytearray))
    assert get_srid(from_wkb(out)) == 0


@pytest.mark.parametrize("crs", ["OGC:CRS84", "IGNF:LAMB93"])
def test_st_setcrs_non_numeric_authority_code_raises(crs):
    """st_setcrs on a non-numeric authority code raises a clean ValueError."""
    with pytest.raises(ValueError, match="authority-less"):
        _crs.st_setcrs(_plain_wkb(), crs)


def test_st_transformcrs_plain_wkb_no_source_unchanged():
    """Plain WKB, no source_crs -> returned unchanged (never-error invariant)."""
    plain = _plain_wkb()
    assert _crs.st_transformcrs(plain, "EPSG:32633") == plain


def test_st_transformcrs_plain_wkb_authority_less_target_unchanged():
    """Plain WKB + authority-less target, no source_crs -> unchanged."""
    plain = _plain_wkb()
    assert _crs.st_transformcrs(plain, _CUSTOM_TM_WKT) == plain


def test_st_transformcrs_plain_wkb_with_explicit_source():
    """Plain WKB + explicit source_crs -> reprojected."""
    out = _crs.st_transformcrs(_plain_wkb(), "EPSG:32633", source_crs="EPSG:4326")
    g = from_wkb(out)
    assert get_srid(g) == 32633
    assert g.x == _UTM33N_X


def test_st_transformcrs_ewkt_epsg_target_text_medium():
    """EWKT + EPSG target -> EWKT str, SRID=target, coords reprojected."""
    out = _crs.st_transformcrs(_ewkt(4326), "EPSG:32633")
    assert isinstance(out, str)
    assert out.upper().startswith("SRID=32633;")
    g = from_wkt(out.split(";", 1)[1])
    assert get_srid(from_wkb(to_wkb(g))) == 0  # plain parse of WKT part has no srid
    assert g.x == _UTM33N_X


def test_st_transformcrs_ewkt_authority_less_target_text_medium():
    """EWKT + authority-less WKT target -> plain WKT str, SRID cleared, coords reprojected."""
    out = _crs.st_transformcrs(_ewkt(4326), _CUSTOM_TM_WKT)
    assert isinstance(out, str)
    assert "SRID=" not in out.upper()
    g = from_wkt(out)
    assert g.x < 0


def test_st_transformcrs_plain_wkt_no_source_unchanged():
    """Plain WKT, no source -> unchanged."""
    plain = _plain_wkt()
    assert _crs.st_transformcrs(plain, "EPSG:32633") == plain


def test_st_transformcrs_plain_wkt_authority_less_target_unchanged():
    """Plain WKT + authority-less target, no source -> unchanged."""
    plain = _plain_wkt()
    assert _crs.st_transformcrs(plain, _CUSTOM_TM_WKT) == plain


def test_st_transformcrs_plain_wkt_with_explicit_source():
    """Plain WKT + explicit source_crs -> text output, reprojected."""
    out = _crs.st_transformcrs(_plain_wkt(), "EPSG:32633", source_crs="EPSG:4326")
    assert isinstance(out, str)
    assert out.upper().startswith("SRID=32633;")
    g = from_wkt(out.split(";", 1)[1])
    assert g.x == _UTM33N_X


# ---------------------------------------------------------------------------
# Never-error invariant: unresolvable source
# ---------------------------------------------------------------------------


def test_st_transformcrs_unresolvable_embedded_srid_unchanged():
    """EWKB with SRID not in EPSG or ESRI -> returned unchanged, no exception."""
    bad = _ewkb(999999)
    assert _crs.st_transformcrs(bad, "EPSG:32633") == bad


def test_st_transformcrs_unresolvable_explicit_source_unchanged():
    """Plain WKB + invalid explicit source_crs string -> unchanged, no exception."""
    plain = _plain_wkb()
    assert _crs.st_transformcrs(plain, "EPSG:32633", source_crs="NOT_A_CRS") == plain


# ---------------------------------------------------------------------------
# st_transformcrs: unresolvable TARGET raises
# ---------------------------------------------------------------------------


def test_st_transformcrs_unresolvable_target_raises():
    """An explicitly invalid target CRS string must raise (not return unchanged)."""
    with pytest.raises(Exception):
        _crs.st_transformcrs(_ewkb(4326), "GARBAGE_CRS_STRING_XXXX")


# ---------------------------------------------------------------------------
# Coordinate preservation in st_transformcrs text medium
# ---------------------------------------------------------------------------


def test_st_transformcrs_coordinate_preservation_text():
    """to_wkt rounding_precision=-1 must be used; no 6-dp truncation."""
    # Reproject to UTM33N then back to 4326; round-trip error should be < 1 mm
    out_utm = _crs.st_transformcrs(_ewkb(4326), "EPSG:32633")
    out_back = _crs.st_transformcrs(out_utm, "EPSG:4326")
    g = from_wkb(out_back)
    assert g.x == pytest.approx(11.0, abs=1e-8)
    assert g.y == pytest.approx(42.0, abs=1e-8)


# ---------------------------------------------------------------------------
# SQL/UDF layer — BINARY / STRING normalization
# ---------------------------------------------------------------------------


def test_udf_st_crs_returns_string():
    assert _crs.st_crs(_ewkb(4326)) == "EPSG:4326"
    assert _crs.st_crs(_plain_wkb()) is None


def test_udf_st_setcrs_returns_binary_for_text_input():
    """SQL surface returns BINARY (bytes) even when input is text."""
    out = _crs._udf_st_setcrs("POINT (0 0)", "EPSG:4326")
    assert isinstance(out, (bytes, bytearray))
    assert get_srid(from_wkb(out)) == 4326


def test_udf_st_setcrs_text_binary_symmetry():
    """SQL surface: text input and bytes input must produce byte-identical EWKB."""
    pt_wkb = to_wkb(Point(11.123456789, 42.987654321))
    pt_wkt = "POINT (11.123456789 42.987654321)"
    out_bytes = _crs._udf_st_setcrs(pt_wkb, "EPSG:4326")
    out_text = _crs._udf_st_setcrs(pt_wkt, "EPSG:4326")
    assert isinstance(out_bytes, (bytes, bytearray))
    assert isinstance(out_text, (bytes, bytearray))
    g_b = from_wkb(out_bytes)
    g_t = from_wkb(out_text)
    assert g_b.x == pytest.approx(g_t.x, abs=1e-9)
    assert g_b.y == pytest.approx(g_t.y, abs=1e-9)
    assert get_srid(g_b) == get_srid(g_t) == 4326


def test_udf_st_transformcrs_returns_binary_for_text_input():
    """SQL surface returns BINARY even when input is EWKT."""
    out = _crs._udf_st_transformcrs(_ewkt(4326), "EPSG:32633")
    assert isinstance(out, (bytes, bytearray))
    assert get_srid(from_wkb(out)) == 32633


def test_udf_st_transformcrs_null_target_returns_none():
    """NULL target_crs must return None, not raise."""
    assert _crs._udf_st_transformcrs(_ewkb(4326), None) is None


# ---------------------------------------------------------------------------
# Z handling — clean 3D preserved, partial Z handled as 2D (never-error)
# ---------------------------------------------------------------------------


def _mixed_z_linestring_ewkb():
    """EWKB (3D) for a LINESTRING whose second vertex has no Z."""
    import shapely as _sh
    from shapely.geometry import LineString

    ls = LineString([(11.0, 42.0, 5.0), (12.0, 43.0, float("nan"))])
    return to_wkb(_sh.set_srid(ls, 4326), include_srid=True, output_dimension=3)


def test_st_transformcrs_clean_3d_preserves_z_binary():
    """Every vertex has a finite Z -> Z survives the reprojection (33-byte 3D EWKB)."""
    import shapely as _sh

    pt = _sh.set_srid(from_wkt("POINT Z (11 42 500)"), 4326)
    out = _crs.st_transformcrs(
        to_wkb(pt, include_srid=True, output_dimension=3), "EPSG:32633"
    )
    g = from_wkb(out)
    assert g.has_z
    assert g.z == pytest.approx(500.0, abs=1e-9)
    assert g.x == _UTM33N_X


def test_st_transformcrs_clean_3d_preserves_z_text():
    out = _crs.st_transformcrs("SRID=4326;POINT Z (11 42 500)", "EPSG:32633")
    assert isinstance(out, str)
    assert out.upper().startswith("SRID=32633;")
    g = from_wkt(out.split(";", 1)[1])
    assert g.has_z and g.z == pytest.approx(500.0, abs=1e-9)


def test_st_transformcrs_2d_stays_2d_binary():
    """A genuinely 2D geometry must not gain a Z slot: 2D EWKB is 25 bytes."""
    assert len(_crs.st_transformcrs(_ewkb(4326), "EPSG:32633")) == 25
    assert len(_crs.st_setcrs(_plain_wkb(), "EPSG:4326")) == 25


def test_st_transformcrs_partial_z_binary_is_2d_no_coord_corruption():
    """Partial-Z LINESTRING reprojects as 2D — X/Y correct, no NaN anywhere."""
    out = _crs.st_transformcrs(_mixed_z_linestring_ewkb(), "EPSG:32633")
    g = from_wkb(out)
    assert not g.has_z, "partial-Z input must be handled as 2D"
    coords = list(g.coords)
    assert coords[0][0] == _UTM33N_X
    for x, y in coords:
        assert x == x and y == y, f"coordinate corrupted to NaN: {coords}"


def test_st_transformcrs_partial_z_text_is_2d_no_coord_corruption():
    """Partial-Z LINESTRING in text medium: no throw, no 'NaN NaN NaN' vertex."""
    out = _crs.st_transformcrs(
        "SRID=4326;LINESTRING Z (11 42 5, 12 43 NaN)", "EPSG:32633"
    )
    assert isinstance(out, str)
    assert "NAN" not in out.upper(), f"X/Y corrupted by non-finite Z: {out}"
    g = from_wkt(out.split(";", 1)[1])
    assert not g.has_z
    assert list(g.coords)[0][0] == _UTM33N_X


def test_st_transformcrs_mixed_dimensionality_wkt_does_not_raise():
    """A WKT body whose parts disagree about dimensionality must not throw.

    GEOS rejects this WKT outright, so the text is normalized to uniform 3D and then —
    because the Z is only partial — reprojected as 2D, so no non-finite ordinate can
    propagate into X or Y.
    """
    out = _crs.st_transformcrs(
        "SRID=4326;GEOMETRYCOLLECTION Z (POINT Z (11 42 5), POINT (12 43))",
        "EPSG:32633",
    )
    assert isinstance(out, str)
    assert out.upper().startswith("SRID=32633;")
    # Reprojected output must carry no NaN: X and Y are intact for every vertex.
    assert "NAN" not in out.upper()


def test_st_setcrs_mixed_dimensionality_wkt_does_not_raise():
    """st_setcrs on mixed-dimensionality WKT must not throw, and keeps uniform 3D.

    Unlike the transform, st_setcrs never touches coordinates, so there is no reason to
    drop the Z: the vertex that had none keeps none, written as the NaN marker. This
    matches the heavyweight tier, whose JTS reader produces exactly that geometry.
    """
    out = _crs.st_setcrs(
        "SRID=4326;GEOMETRYCOLLECTION Z (POINT Z (11 42 5), POINT (12 43))",
        "EPSG:32633",
    )
    assert isinstance(out, str)
    assert out.upper().startswith("SRID=32633;")
    # NaN here is the ABSENCE of a Z, not a fabricated value — and it is what heavy emits.
    assert "NAN" in out.upper()
    assert "11 42 5" in out, "the vertex that had a Z must keep its exact value"


def test_st_setcrs_partial_z_binary_preserves_z_verbatim():
    """st_setcrs never touches coordinates, so a partial Z is written back as-is."""
    out = _crs.st_setcrs(_mixed_z_linestring_ewkb(), "EPSG:32633")
    g = from_wkb(out)
    assert get_srid(g) == 32633
    assert g.has_z, "st_setcrs must not silently downcast a 3D geometry"
    zs = [c[2] for c in g.coords]
    assert zs[0] == pytest.approx(5.0)
    assert zs[1] != zs[1], "the missing Z must stay missing, never fabricated as 0"


def test_st_setcrs_never_fabricates_z_for_2d_input():
    """A 2D input must never come back with a Z ordinate of 0."""
    g = from_wkb(_crs.st_setcrs(_plain_wkb(), "EPSG:4326"))
    assert not g.has_z


# ---------------------------------------------------------------------------
# A fuzzy authority must not leak into the reprojection MATH (cache key)
# ---------------------------------------------------------------------------


def test_transformer_cache_key_separates_fuzzy_matched_crs():
    """A PROJ4 CRS and the EPSG code it fuzzy-matches must NOT share a cache entry.

    The transformer cache used to be keyed on ``crs_to_canonical``, which resolves at
    PROJ's default 70% confidence — so this PROJ4 string and real EPSG:28992 produced the
    SAME key. Whichever was requested first won the entry and silently answered for the
    other, making the output depend on cache order: a ~177 m error, in both directions.
    """
    from rasterio.crs import CRS

    from databricks.labs.gbx.core.crs import _transformer_key, crs_to_canonical

    proj4 = CRS.from_user_input(_PROJ4_RD_NULL_SHIFT)
    epsg = CRS.from_epsg(28992)

    # The precondition that made this dangerous: they share a canonical NAME.
    assert crs_to_canonical(proj4) == crs_to_canonical(epsg) == "EPSG:28992"
    # ...but must not share a transformer cache KEY.
    assert _transformer_key(proj4) != _transformer_key(epsg)
    assert _transformer_key(epsg) == "EPSG:28992"


def test_transformer_cache_key_still_shares_equivalent_spellings():
    """Different spellings of ONE CRS must still share a key (cache hit rate matters)."""
    from rasterio.crs import CRS

    from databricks.labs.gbx.core.crs import _transformer_cache, get_transformer

    _transformer_cache().clear()
    assert get_transformer(4326, 3857) is get_transformer("EPSG:4326", "3857")
    assert len(_transformer_cache()) == 1, "equivalent spellings must not double-cache"
    assert (
        CRS.from_epsg(4326).to_authority(confidence_threshold=100) is not None
    ), "a registry CRS must still be identified at full confidence"


@pytest.mark.parametrize("first", ["proj4", "epsg"], ids=["proj4-first", "epsg-first"])
def test_st_transformcrs_result_independent_of_cache_order(first):
    """The reprojected coordinates must not depend on which target was requested first.

    Both orders are exercised, and each target's answer is pinned to the value its OWN
    definition produces — so a cache collision in either direction fails.
    """
    from databricks.labs.gbx.core.crs import _transformer_cache

    geom = _ewkb_at(_RD_LON, _RD_LAT, 4326)
    targets = (
        [_PROJ4_RD_NULL_SHIFT, "EPSG:28992"]
        if first == "proj4"
        else ["EPSG:28992", _PROJ4_RD_NULL_SHIFT]
    )

    _transformer_cache().clear()
    got = {}
    for target in targets:
        key = "proj4" if target == _PROJ4_RD_NULL_SHIFT else "epsg"
        got[key] = from_wkb(_crs.st_transformcrs(geom, target))

    # Each CRS's own exact answer, independent of request order.
    assert got["epsg"].x == pytest.approx(155191.353812, abs=1e-3)
    assert got["epsg"].y == pytest.approx(463537.136273, abs=1e-3)
    assert got["proj4"].x == pytest.approx(155161.545139, abs=1e-3)
    assert got["proj4"].y == pytest.approx(463362.663804, abs=1e-3)
    # They genuinely differ — which is exactly why they must not share a cache entry.
    separation = (
        (got["epsg"].x - got["proj4"].x) ** 2 + (got["epsg"].y - got["proj4"].y) ** 2
    ) ** 0.5
    assert separation > 100.0, f"expected a large true separation, got {separation} m"


# ---------------------------------------------------------------------------
# Never-error invariant AT THE REGISTERED UDF LEVEL (not just the core)
# ---------------------------------------------------------------------------

_MIXED_DIM_WKT = "GEOMETRYCOLLECTION Z (POINT Z (11 42 5), POINT (12 43))"


@pytest.mark.parametrize(
    "geom,target,source",
    [
        (f"SRID=999999;{_MIXED_DIM_WKT}", "EPSG:32633", None),
        (_MIXED_DIM_WKT, "EPSG:32633", None),
        (_MIXED_DIM_WKT, "EPSG:32633", "NOT_A_CRS_XYZ"),
    ],
    ids=["unresolvable-srid", "no-source", "bad-source-crs"],
)
def test_udf_st_transformcrs_mixed_dim_wkt_degrade_never_raises(geom, target, source):
    """The registered UDF must not raise on mixed-dimensionality WKT in a DEGRADE path.

    The core degrades by handing back the caller's ORIGINAL string; the UDF then has to
    re-encode that string as BINARY. A bare re-parse there raised GEOSException — so the
    core was correct while the surface users actually call still failed the stage.
    """
    out = _crs._udf_st_transformcrs(geom, target, source)
    assert isinstance(out, (bytes, bytearray))
    g = from_wkb(bytes(out))
    assert g.geom_type == "GeometryCollection"
    # Degrade = unchanged coordinates.
    coords = shapely.get_coordinates(g).tolist()
    assert coords == [[11.0, 42.0], [12.0, 43.0]]


def test_udf_st_setcrs_mixed_dim_wkt_never_raises():
    """The registered st_setcrs UDF must not raise on mixed-dimensionality WKT either."""
    out = _crs._udf_st_setcrs(f"SRID=4326;{_MIXED_DIM_WKT}", "EPSG:32633")
    assert isinstance(out, (bytes, bytearray))
    assert get_srid(from_wkb(bytes(out))) == 32633


def test_udf_st_crs_mixed_dim_wkt_never_raises():
    """st_crs must read the SRID off mixed-dimensionality WKT rather than raising."""
    assert _crs.st_crs(f"SRID=4326;{_MIXED_DIM_WKT}") == "EPSG:4326"


# ---------------------------------------------------------------------------
# Mixed-dim WKT shapes BEYOND the flat GC(POINT Z, POINT) case
# ---------------------------------------------------------------------------

# Each of these is mixed-dimensionality (so it only reaches the normalizer after a failed
# parse) but exercises a different structural feature: an EMPTY component with no
# ordinates to pad, an M ordinate, and a NESTED collection whose own tag must be
# re-derived. Byte counts are heavy's, measured through the JAR.
_SHAPES_BEYOND_FLAT = [
    (
        "empty-component",
        "SRID=4326;GEOMETRYCOLLECTION Z (POINT Z (11 42 5), POINT EMPTY)",
        71,
    ),
    (
        "zm-ordinates",
        "SRID=4326;GEOMETRYCOLLECTION ZM (POINT ZM (1 2 3 4), POINT (5 6))",
        71,
    ),
    (
        "nested-collection",
        "SRID=4326;GEOMETRYCOLLECTION Z (POINT Z (11 42 5), "
        "GEOMETRYCOLLECTION (POINT (1 2)))",
        80,
    ),
]


@pytest.mark.parametrize(
    "wkt",
    [w for _, w, _ in _SHAPES_BEYOND_FLAT],
    ids=[n for n, _, _ in _SHAPES_BEYOND_FLAT],
)
def test_mixed_dim_wkt_shapes_never_return_none(wkt):
    """None of these WKT shapes may degrade to NULL from ANY of the three functions.

    An earlier normalizer handled only the flat ``GC Z (POINT Z, POINT)`` shape and turned
    all three of these into NULL: the EMPTY component had no ordinates to pad, the M
    ordinate was truncated leaving a ``ZM`` tag with 3 ordinates, and a nested collection
    got padded coordinates but kept no ``Z`` tag. Each produced WKT that GEOS still
    refused, so the parse returned None and every function answered NULL.

    ``st_crs`` returning None here is the sharpest symptom: the geometry HAS an SRID, so
    None is a WRONG answer, not merely a missing one.
    """
    assert (
        _crs.st_crs(wkt) == "EPSG:4326"
    ), "st_crs must read the SRID it plainly carries"
    assert _crs._udf_st_setcrs(wkt, "EPSG:32633") is not None
    assert _crs._udf_st_transformcrs(wkt, "EPSG:32633") is not None
    # Core layer too, not just the UDFs.
    assert _crs.st_setcrs(wkt, "EPSG:32633") is not None
    assert _crs.st_transformcrs(wkt, "EPSG:32633") is not None


@pytest.mark.parametrize(
    "wkt,heavy_bytes",
    [(w, b) for _, w, b in _SHAPES_BEYOND_FLAT],
    ids=[n for n, _, _ in _SHAPES_BEYOND_FLAT],
)
def test_mixed_dim_wkt_shapes_match_heavy_encoding(wkt, heavy_bytes):
    """st_setcrs on each shape must produce byte-identical output to the heavy tier.

    The byte counts are measured from heavy through the JAR. Pinning the exact length is a
    cheap proxy for "same dimensionality at every nesting level": an EMPTY component that
    came back 2D, or a nested collection that lost its Z, changes the encoded size.
    """
    out = _crs._udf_st_setcrs(wkt, "EPSG:32633")
    assert len(bytes(out)) == heavy_bytes, (
        f"encoding diverged from heavy: got {len(bytes(out))} bytes, "
        f"heavy emits {heavy_bytes} for {to_wkt(from_wkb(bytes(out)))}"
    )


def test_mixed_dim_wkt_zm_drops_m_like_heavy():
    """M is DROPPED, matching heavy: JTS is XYZ-only, so 'POINT ZM (1 2 3 4)' -> Z (1 2 3).

    Keeping the measure would make the tiers disagree, so the M ordinate is truncated
    rather than preserved. The Z value must survive intact.
    """
    out = _crs._udf_st_setcrs(
        "SRID=4326;GEOMETRYCOLLECTION ZM (POINT ZM (1 2 3 4), POINT (5 6))",
        "EPSG:32633",
    )
    g = from_wkb(bytes(out))
    coords = shapely.get_coordinates(g, include_z=True).tolist()
    assert coords[0] == [1.0, 2.0, 3.0], "Z must be kept and M dropped, not shifted"
    assert coords[1][:2] == [5.0, 6.0]
    assert coords[1][2] != coords[1][2], "the vertex with no Z stays NaN"


def test_mixed_dim_wkt_empty_component_stays_empty():
    """An EMPTY component must remain empty — never gain a fabricated coordinate."""
    out = _crs._udf_st_setcrs(
        "SRID=4326;GEOMETRYCOLLECTION Z (POINT Z (11 42 5), POINT EMPTY)", "EPSG:32633"
    )
    g = from_wkb(bytes(out))
    assert [p.is_empty for p in g.geoms] == [False, True]
    # Only the non-empty component contributes coordinates.
    assert shapely.get_coordinates(g).tolist() == [[11.0, 42.0]]


def test_mixed_dim_wkt_nested_collection_preserves_structure():
    """A nested collection keeps its nesting and gains the right dimensionality tag."""
    out = _crs._udf_st_setcrs(
        "SRID=4326;GEOMETRYCOLLECTION Z (POINT Z (11 42 5), "
        "GEOMETRYCOLLECTION (POINT (1 2)))",
        "EPSG:32633",
    )
    g = from_wkb(bytes(out))
    assert g.geom_type == "GeometryCollection"
    assert [p.geom_type for p in g.geoms] == ["Point", "GeometryCollection"]
    assert g.has_z


def test_wkt_pad_z_precondition_would_alter_clean_2d():
    """Documents the normalizer's precondition: it must only see WKT that failed to parse.

    Called on clean 2D WKT it pads to 3D, which would silently turn 2D geometries 3D. The
    guard is structural — ``_parse_geom_safe`` tries a plain parse first and only falls
    back to the normalizer — so this test pins the hazard so a future unconditional caller
    is an obvious mistake rather than a subtle one.
    """
    assert "NaN" in _crs._wkt_pad_z("POINT (11 42)")
    # ...and the guarded path a caller actually uses leaves clean 2D alone.
    g = _crs._parse_geom_safe("POINT (11 42)")
    assert not g.has_z
    assert len(_crs._udf_st_setcrs("POINT (11 42)", "EPSG:4326")) == 25


def test_mixed_dim_wkt_setcrs_preserves_uniform_3d():
    """Mixed-dim WKT normalizes UP to 3D (NaN for the absent Z), matching heavy's JTS.

    st_setcrs never touches coordinates, so the vertex that had no Z keeps no Z — encoded
    as the NaN marker both tiers already use internally, not as a fabricated 0.
    """
    out = _crs._udf_st_setcrs(f"SRID=4326;{_MIXED_DIM_WKT}", "EPSG:32633")
    g = from_wkb(bytes(out))
    assert g.has_z, "heavy returns uniform 3D here; light must match"
    zs = shapely.get_coordinates(g, include_z=True)[:, 2].tolist()
    assert zs[0] == pytest.approx(5.0)
    assert zs[1] != zs[1], "absent Z stays absent (NaN), never fabricated as 0"


# ---------------------------------------------------------------------------
# Spark registration — Column wrappers and registrar group
# ---------------------------------------------------------------------------


def test_column_wrappers_accept_literal_crs_string(spark):
    """st_crs/st_setcrs/st_transformcrs Column wrappers must accept plain CRS strings."""
    from databricks.labs.gbx.pyvx import functions as pvx

    pvx.register(spark)

    ewkb_hex = _ewkb(4326).hex()
    df = spark.sql(f"SELECT unhex('{ewkb_hex}') AS geom")

    # st_crs
    result_crs = df.select(pvx.st_crs("geom")).first()[0]
    assert result_crs == "EPSG:4326"

    # st_setcrs — CRS string literal must be wrapped as f.lit, not a column name
    result_set = bytes(df.select(pvx.st_setcrs("geom", "EPSG:32633")).first()[0])
    assert get_srid(from_wkb(result_set)) == 32633

    # st_transformcrs — same
    result_tr = bytes(df.select(pvx.st_transformcrs("geom", "EPSG:32633")).first()[0])
    assert get_srid(from_wkb(result_tr)) == 32633


# ---------------------------------------------------------------------------
# UNIFORM ZM geometries — M truncated to XYZ, matching heavy
# ---------------------------------------------------------------------------

# Every shape here is UNIFORM ZM: one dimensionality for the whole geometry, so it parses
# on the FIRST attempt and never reaches the mixed-dimensionality normalizer. That is
# exactly why the gap was invisible for so long — the only ZM coverage that existed was a
# MIXED-dim collection, which the partial-Z rescue turns 2D before any M could matter.
#
# Two real divergences from heavy lived in this blind spot, both on the registered SQL
# surface: st_setcrs KEPT the measure (heavy drops it), and st_transformcrs RAISED
# ValueError("The ordinate (last) dimension should be 2 or 3, got 4") because
# shapely.ops.transform hands all four ordinates to a transformer that takes 2 or 3 —
# a never-error violation where one ZM row failed the whole stage.
_UNIFORM_ZM_SHAPES = [
    ("point", "POINT ZM (11 42 5 99)", [[11.0, 42.0, 5.0]]),
    (
        "linestring",
        "LINESTRING ZM (11 42 5 99, 12 43 6 98)",
        [[11.0, 42.0, 5.0], [12.0, 43.0, 6.0]],
    ),
    (
        "polygon",
        "POLYGON ZM ((11 42 5 99, 12 42 5 98, 12 43 5 97, 11 42 5 99))",
        [
            [11.0, 42.0, 5.0],
            [12.0, 42.0, 5.0],
            [12.0, 43.0, 5.0],
            [11.0, 42.0, 5.0],
        ],
    ),
    (
        "multipoint",
        "MULTIPOINT ZM ((11 42 5 99), (12 43 6 98))",
        [[11.0, 42.0, 5.0], [12.0, 43.0, 6.0]],
    ),
    (
        "geometrycollection",
        "GEOMETRYCOLLECTION ZM (POINT ZM (11 42 5 99), "
        "LINESTRING ZM (11 42 5 99, 12 43 6 98))",
        [[11.0, 42.0, 5.0], [11.0, 42.0, 5.0], [12.0, 43.0, 6.0]],
    ),
]


def _zm_ewkt(wkt, srid=4326):
    """EWKT for a uniform-ZM shape."""
    return f"SRID={srid};{wkt}"


def _zm_ewkb(wkt, srid=4326):
    """EWKB (extended flavor, 4 ordinates) for a uniform-ZM shape.

    ``output_dimension=4`` is what makes this a real ZM input rather than a silently
    truncated one — shapely's default would drop the M at the writer and the test would
    prove nothing about the reader path.
    """
    import shapely as _sh

    return to_wkb(
        _sh.set_srid(from_wkt(wkt), srid),
        output_dimension=4,
        flavor="extended",
        include_srid=True,
    )


def _zm_media(wkt):
    """The (id, input) pairs for both media a caller can hand a ZM geometry in."""
    return [("ewkt", _zm_ewkt(wkt)), ("ewkb", _zm_ewkb(wkt))]


@pytest.mark.parametrize(
    "wkt,expected_xyz",
    [(w, c) for _, w, c in _UNIFORM_ZM_SHAPES],
    ids=[n for n, _, _ in _UNIFORM_ZM_SHAPES],
)
@pytest.mark.parametrize("medium", ["ewkt", "ewkb"])
def test_uniform_zm_setcrs_truncates_m_to_xyz(wkt, expected_xyz, medium):
    """st_setcrs on a uniform-ZM geometry returns X, Y, Z — the measure is gone.

    Asserted at BOTH layers: the medium-preserving core (str->str, bytes->bytes) and the
    registered UDF (always BINARY). The core's text answer is the one that used to carry
    ``ZM`` straight through in its output string.
    """
    geom = dict(_zm_media(wkt))[medium]

    # --- core layer, medium-preserving ---
    core = _crs.st_setcrs(geom, "EPSG:4326")
    if medium == "ewkt":
        assert isinstance(core, str), "text in must stay text out"
        assert "ZM" not in core.upper(), f"core kept the measure: {core}"
        g_core = from_wkt(core.split(";", 1)[1])
    else:
        assert isinstance(core, (bytes, bytearray)), "bytes in must stay bytes out"
        g_core = from_wkb(bytes(core))
    assert not shapely.has_m(g_core), "M must be dropped, matching heavy's XYZ-only JTS"
    assert g_core.has_z, "Z must survive the M drop"
    assert shapely.get_coordinates(g_core, include_z=True).tolist() == expected_xyz

    # --- registered UDF layer, always BINARY ---
    out = _crs._udf_st_setcrs(geom, "EPSG:4326")
    assert isinstance(out, (bytes, bytearray))
    g = from_wkb(bytes(out))
    assert not shapely.has_m(g)
    assert g.has_z
    assert get_srid(g) == 4326
    assert shapely.get_coordinates(g, include_z=True).tolist() == expected_xyz


@pytest.mark.parametrize(
    "wkt,expected_xyz",
    [(w, c) for _, w, c in _UNIFORM_ZM_SHAPES],
    ids=[n for n, _, _ in _UNIFORM_ZM_SHAPES],
)
@pytest.mark.parametrize("medium", ["ewkt", "ewkb"])
def test_uniform_zm_transformcrs_never_raises_and_drops_m(wkt, expected_xyz, medium):
    """st_transformcrs on a uniform-ZM geometry reprojects X/Y, keeps Z, and drops M.

    This is the never-error regression: every one of these five shapes raised
    ``ValueError: The ordinate (last) dimension should be 2 or 3, got 4`` in both media and
    through the registered UDF, so a single ZM row killed the stage.
    """
    geom = dict(_zm_media(wkt))[medium]
    expected_z = [c[2] for c in expected_xyz]

    # --- core layer, medium-preserving ---
    core = _crs.st_transformcrs(geom, "EPSG:32633")
    if medium == "ewkt":
        assert isinstance(core, str)
        assert core.startswith("SRID=32633;"), f"reprojected, not degraded: {core}"
        assert "ZM" not in core.upper(), f"core kept the measure: {core}"
        g_core = from_wkt(core.split(";", 1)[1])
    else:
        assert isinstance(core, (bytes, bytearray))
        g_core = from_wkb(bytes(core))
        assert get_srid(g_core) == 32633, "reprojected, not degraded"
    assert not shapely.has_m(g_core)
    assert g_core.has_z, "Z rides through the reprojection unchanged"

    # --- registered UDF layer, always BINARY ---
    out = _crs._udf_st_transformcrs(geom, "EPSG:32633")
    assert isinstance(out, (bytes, bytearray))
    g = from_wkb(bytes(out))
    assert not shapely.has_m(g)
    assert g.has_z
    assert get_srid(g) == 32633
    coords = shapely.get_coordinates(g, include_z=True)
    # Z is a passthrough (metres of elevation are not reprojected by a 2D CRS change).
    assert coords[:, 2].tolist() == pytest.approx(expected_z)
    # X/Y actually moved into UTM33N metres rather than staying in degrees.
    assert coords[0][0] == _UTM33N_X
    assert coords[0][1] == _UTM33N_Y


def test_uniform_zm_point_matches_heavy_byte_count():
    """A ZM POINT comes back as the 33-byte 3D EWKB heavy emits, from BOTH functions.

    Heavy reads ``POINT ZM (11 42 5 99)`` through JTS (XYZ-only) as ``POINT Z (11 42 5)``
    and encodes 33 bytes. Pinning the length catches an M that survived (41 bytes) and a Z
    that did not (25 bytes) in one assertion.
    """
    for medium, geom in _zm_media("POINT ZM (11 42 5 99)"):
        assert (
            len(bytes(_crs._udf_st_setcrs(geom, "EPSG:4326"))) == 33
        ), f"st_setcrs diverged from heavy's 33-byte XYZ EWKB ({medium})"
        assert (
            len(bytes(_crs._udf_st_transformcrs(geom, "EPSG:32633"))) == 33
        ), f"st_transformcrs diverged from heavy's 33-byte XYZ EWKB ({medium})"


def test_m_only_geometry_becomes_2d_without_fabricating_z():
    """An ``M``-only geometry drops to plain 2D — it must NOT gain an invented ``Z=0``.

    ``shapely.force_3d`` would produce ``POINT Z (11 42 0)`` here, and a downstream consumer
    could not tell that fabricated 0 from a surveyed sea-level elevation. Dropping is
    correct; inventing is not.
    """
    for geom in ("SRID=4326;POINT M (11 42 99)", _zm_ewkb("POINT M (11 42 99)")):
        for out in (
            _crs._udf_st_setcrs(geom, "EPSG:4326"),
            _crs._udf_st_transformcrs(geom, "EPSG:32633"),
        ):
            g = from_wkb(bytes(out))
            assert not shapely.has_m(g), "the measure is dropped"
            assert not g.has_z, "no Z is invented to replace it"
            assert len(bytes(out)) == 25, "plain 2D EWKB"


def test_uniform_zm_st_crs_reads_srid():
    """st_crs must read the SRID off a uniform-ZM geometry rather than returning None."""
    for _, geom in _zm_media("POINT ZM (11 42 5 99)"):
        assert _crs.st_crs(geom) == "EPSG:4326"


def test_uniform_zm_degrade_returns_input_unchanged():
    """A ZM geometry with no resolvable source CRS degrades to the input, still not raising.

    The never-error invariant is medium-preserving on the degrade path too: the core hands
    back the caller's own object untouched (measure included — nothing was reprojected, so
    nothing was dropped), and the UDF re-encodes it to BINARY without raising.
    """
    plain_wkt = "POINT ZM (11 42 5 99)"
    plain_wkb = to_wkb(from_wkt(plain_wkt), output_dimension=4, flavor="iso")

    assert _crs.st_transformcrs(plain_wkt, "EPSG:32633") == plain_wkt
    assert _crs.st_transformcrs(plain_wkb, "EPSG:32633") == plain_wkb
    # Registered surface: BINARY out, no raise.
    for geom in (plain_wkt, plain_wkb):
        out = _crs._udf_st_transformcrs(geom, "EPSG:32633")
        assert isinstance(out, (bytes, bytearray))
        assert get_srid(from_wkb(bytes(out))) == 0, "no SRID was stamped by a degrade"


# ---------------------------------------------------------------------------
# Non-finite (Infinity) reprojection results -> NULL
# ---------------------------------------------------------------------------
#
# PROJ returns Infinity (not an error) for out-of-domain inputs.  Infinity
# asserts a *location* — it poisons extents, indexes, and aggregations and is
# indistinguishable from real data.  NULL is the correct answer.
#
# Trigger 1: mislabelled CRS.  UTM easting/northing (500000, 5600000) claimed
#   as EPSG:4326 (WGS84 geographic), then reprojected to EPSG:32633 (UTM33N).
#   PROJ cannot reconcile the coordinate magnitude with the geographic domain
#   and returns Infinity.
# Trigger 2: out-of-domain latitude.  lat=100 is outside [-90, 90], so PROJ
#   cannot produce a valid UTM projection.
#
# Scope boundary (pinned): ``POINT (200 42)`` in EPSG:4326 reprojects to a
#   finite — though meaningless — UTM coordinate.  It must NOT be nulled: only
#   non-finite X/Y are caught here; broader domain validation is queued.


def _ewkb_pt(x, y, srid):
    """EWKB for POINT(x, y) at the given SRID."""
    import shapely as _sh

    return to_wkb(_sh.set_srid(Point(x, y), srid), include_srid=True)


def _ewkt_pt(x, y, srid):
    return f"SRID={srid};POINT ({x} {y})"


@pytest.mark.parametrize(
    "geom,target,label",
    [
        # Trigger 1: UTM coords mislabelled as EPSG:4326 -> Infinity after reproject
        (_ewkb_pt(500000.0, 5600000.0, 4326), "EPSG:32633", "mislabelled-ewkb"),
        (_ewkt_pt(500000.0, 5600000.0, 4326), "EPSG:32633", "mislabelled-ewkt"),
        # Trigger 2: invalid latitude (100 > 90) in geographic CRS
        (_ewkb_pt(10.0, 100.0, 4326), "EPSG:32633", "invalid-lat-ewkb"),
        (_ewkt_pt(10.0, 100.0, 4326), "EPSG:32633", "invalid-lat-ewkt"),
    ],
    ids=lambda x: x if isinstance(x, str) else "",
)
def test_st_transformcrs_nonfinite_result_returns_none_core(geom, target, label):
    """Core st_transformcrs returns None (not Infinity) when reprojection yields non-finite X/Y.

    Both triggers, both media.  The core is medium-preserving so a None return (not a
    string 'None', not bytes) is the unambiguous signal that no geometry was produced.
    """
    result = _crs.st_transformcrs(geom, target)
    assert (
        result is None
    ), f"[{label}] expected None for non-finite reprojection result, got {result!r}"


@pytest.mark.parametrize(
    "geom,target,label",
    [
        (_ewkb_pt(500000.0, 5600000.0, 4326), "EPSG:32633", "mislabelled-ewkb"),
        (_ewkt_pt(500000.0, 5600000.0, 4326), "EPSG:32633", "mislabelled-ewkt"),
        (_ewkb_pt(10.0, 100.0, 4326), "EPSG:32633", "invalid-lat-ewkb"),
        (_ewkt_pt(10.0, 100.0, 4326), "EPSG:32633", "invalid-lat-ewkt"),
    ],
    ids=lambda x: x if isinstance(x, str) else "",
)
def test_udf_st_transformcrs_nonfinite_result_returns_none(geom, target, label):
    """Registered UDF returns NULL (None) for both triggers in both media.

    The UDF wraps the core: if the core returns None, the UDF must also return None
    (not attempt to encode None as BINARY, which would raise TypeError).
    """
    result = _crs._udf_st_transformcrs(geom, target)
    assert (
        result is None
    ), f"[{label}] UDF expected None for non-finite reprojection result, got {result!r}"


def test_st_transformcrs_finite_but_meaningless_not_nulled():
    """SCOPE BOUNDARY: finite-but-meaningless coordinates must NOT be returned as None.

    ``POINT (200 42)`` in EPSG:4326 (lon=200, outside [-180,180]) reprojects to a real,
    finite UTM easting (~85856 m).  This is geometrically invalid but the reprojection
    arithmetic produces a finite number, so this guard does NOT apply and the result must
    be a real geometry.  Broader domain validation (catching lon>180 etc.) is a separate
    queued workstream — this test pins the scope boundary so any future widening is
    deliberate.
    """
    import math

    geom = _ewkb_pt(200.0, 42.0, 4326)
    result = _crs.st_transformcrs(geom, "EPSG:32633")
    assert (
        result is not None
    ), "POINT(200 42) reprojects to finite coordinates and must NOT be nulled"
    g = from_wkb(result)
    assert math.isfinite(g.x) and math.isfinite(
        g.y
    ), f"expected finite coordinates for lon=200, got ({g.x}, {g.y})"


def test_st_transformcrs_nonfinite_does_not_break_nan_z_handling():
    """Non-finite guard only inspects X/Y; a legitimate NaN Z is unaffected.

    A partial-Z geometry is dropped to 2D BEFORE the transform (by ``_drop_partial_z``),
    so the guard never sees NaN in the Z column of the reprojected geometry.  A clean-2D
    geometry reprojected with a valid input must return a real result — the guard must not
    mistake a NaN-free 2D reprojection for a non-finite result.

    This test also directly verifies the mixed-NaN-Z path is unbroken: a partial-Z
    LINESTRING reprojects to 2D with correct X/Y, and the result is not None.
    """
    import shapely as _sh
    from shapely.geometry import LineString

    # Partial-Z geometry: one vertex has Z, one does not.
    ls = LineString([(11.0, 42.0, 5.0), (12.0, 43.0, float("nan"))])
    geom = to_wkb(_sh.set_srid(ls, 4326), include_srid=True, output_dimension=3)
    result = _crs.st_transformcrs(geom, "EPSG:32633")
    assert (
        result is not None
    ), "partial-Z reprojection must NOT be treated as non-finite"
    g = from_wkb(result)
    assert not g.has_z, "partial-Z input must still come back 2D"
    for x, y in g.coords:
        assert (
            x == x and y == y
        ), f"X/Y must not be NaN after partial-Z reproject: {list(g.coords)}"


# ---------------------------------------------------------------------------
# in_target_domain helper
# ---------------------------------------------------------------------------

import numpy as np
import pyproj
from databricks.labs.gbx.core.crs import in_target_domain


def test_in_target_domain_inside_gb():
    # London lon/lat is inside EPSG:27700 (British National Grid) area_of_use.
    tgt = pyproj.CRS.from_epsg(27700)
    assert in_target_domain(np.array([[-0.13, 51.5]]), tgt) is True


def test_in_target_domain_far_outside_gb():
    # lon 150, lat -80 is nowhere near GB — the finite-nonsense survivor.
    tgt = pyproj.CRS.from_epsg(27700)
    assert in_target_domain(np.array([[150.0, -80.0]]), tgt) is False


def test_in_target_domain_straddling_is_false():
    tgt = pyproj.CRS.from_epsg(27700)
    coords = np.array([[-0.13, 51.5], [150.0, -80.0]])  # one in, one out
    assert in_target_domain(coords, tgt) is False


def test_in_target_domain_bounds_absent_returns_none():
    # A CRS with no area_of_use (raw PROJ4 with no bounds) -> None (skip).
    tgt = pyproj.CRS.from_proj4("+proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs")
    result = in_target_domain(np.array([[0.0, 0.0]]), tgt)
    assert result is None or result is True  # None if no area_of_use; helper must not raise
