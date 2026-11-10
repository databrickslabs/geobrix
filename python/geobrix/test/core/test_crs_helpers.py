"""New core/crs.py helpers — to_pyproj_crs, crs_equal, crs_to_proj4, shim."""

import warnings

import pyproj
import pytest
from rasterio.crs import CRS

from databricks.labs.gbx.core.crs import resolve_crs

# ---------------------------------------------------------------------------
# to_pyproj_crs
# ---------------------------------------------------------------------------


def test_to_pyproj_crs_from_rasterio_crs():
    from databricks.labs.gbx.core.crs import to_pyproj_crs

    pc = to_pyproj_crs(resolve_crs(4326))
    assert isinstance(pc, pyproj.CRS)
    assert pc.to_epsg() == 4326


def test_to_pyproj_crs_from_esri_authority_string():
    from databricks.labs.gbx.core.crs import to_pyproj_crs

    pc = to_pyproj_crs("ESRI:54008")
    assert isinstance(pc, pyproj.CRS)
    assert pc.to_authority(min_confidence=100) == ("ESRI", "54008")


def test_to_pyproj_crs_from_wkt():
    from databricks.labs.gbx.core.crs import to_pyproj_crs

    wkt = resolve_crs(4326).to_wkt()
    pc = to_pyproj_crs(wkt)
    assert isinstance(pc, pyproj.CRS)
    assert pc.to_epsg() == 4326


def test_to_pyproj_crs_from_int():
    from databricks.labs.gbx.core.crs import to_pyproj_crs

    pc = to_pyproj_crs(4326)
    assert isinstance(pc, pyproj.CRS)
    assert pc.to_epsg() == 4326


def test_to_pyproj_crs_ogc_crs84_preserves_authority():
    from databricks.labs.gbx.core.crs import to_pyproj_crs

    pc = to_pyproj_crs("OGC:CRS84")
    assert isinstance(pc, pyproj.CRS)
    assert pc.to_authority(min_confidence=100) == ("OGC", "CRS84")


# ---------------------------------------------------------------------------
# crs_equal
# ---------------------------------------------------------------------------


def test_crs_equal_epsg_epsg():
    from databricks.labs.gbx.core.crs import crs_equal

    assert crs_equal(resolve_crs(4326), resolve_crs(4326)) is True


def test_crs_equal_esri_esri():
    from databricks.labs.gbx.core.crs import crs_equal

    assert crs_equal(resolve_crs(54008), resolve_crs(54008)) is True


def test_crs_equal_ogc_ogc():
    from databricks.labs.gbx.core.crs import crs_equal

    assert crs_equal(resolve_crs("OGC:CRS84"), resolve_crs("OGC:CRS84")) is True


def test_crs_equal_epsg_esri_unequal():
    from databricks.labs.gbx.core.crs import crs_equal

    assert crs_equal(resolve_crs(4326), resolve_crs(54008)) is False


def test_crs_equal_proj4_near_neighbour_is_false():
    """Dutch-RD PROJ4 with null towgs84 shift is NOT equal to EPSG:28992.

    The string ``+proj=sterea ... +towgs84=0,0,0,0,0,0,0`` canonicalizes to
    EPSG:28992 at ~70% confidence but carries a NULL datum shift — making it a
    genuinely distinct CRS (roughly 177 m off the correct Bessel 1841 → WGS84
    transformation). ``crs_equal`` delegates to ``pyproj.CRS.equals()``, which
    distinguishes the null-shift variant from the authoritative EPSG:28992 and
    correctly returns ``False``.
    """
    from databricks.labs.gbx.core.crs import crs_equal

    dutch_rd_null_shift = CRS.from_proj4(
        "+proj=sterea +lat_0=52.15616055555555 +lon_0=5.38763888888889"
        " +k=0.9999079 +x_0=155000 +y_0=463000 +ellps=bessel"
        " +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
    )
    epsg28992 = resolve_crs(28992)
    assert crs_equal(dutch_rd_null_shift, epsg28992) is False


def test_crs_equal_none_none():
    from databricks.labs.gbx.core.crs import crs_equal

    assert crs_equal(None, None) is True


def test_crs_equal_none_crs():
    from databricks.labs.gbx.core.crs import crs_equal

    assert crs_equal(None, resolve_crs(4326)) is False


def test_crs_equal_crs_none():
    from databricks.labs.gbx.core.crs import crs_equal

    assert crs_equal(resolve_crs(4326), None) is False


# ---------------------------------------------------------------------------
# crs_to_proj4
# ---------------------------------------------------------------------------


def test_crs_to_proj4_epsg_returns_proj4_string():
    from databricks.labs.gbx.core.crs import crs_to_proj4

    p4 = crs_to_proj4(resolve_crs(4326))
    assert p4 is not None
    assert "+proj=" in p4


def test_crs_to_proj4_esri_returns_proj4_string():
    from databricks.labs.gbx.core.crs import crs_to_proj4

    p4 = crs_to_proj4(resolve_crs(54008))
    assert p4 is not None
    assert "+proj=" in p4


def test_crs_to_proj4_none_safe():
    from databricks.labs.gbx.core.crs import crs_to_proj4

    assert crs_to_proj4(None) is None


def test_crs_to_proj4_suppresses_deprecation_warning():
    """crs_to_proj4 must not leak pyproj's PROJ4 deprecation UserWarning."""
    from databricks.labs.gbx.core.crs import crs_to_proj4

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        crs_to_proj4(resolve_crs(4326))
    leaked = [
        w
        for w in caught
        if issubclass(w.category, UserWarning) and "proj4" in str(w.message).lower()
    ]
    assert len(leaked) == 0, f"Unexpected PROJ4 warnings: {leaked}"


# ---------------------------------------------------------------------------
# shim re-export
# ---------------------------------------------------------------------------


def test_shim_exports_authority_srid_of():
    from databricks.labs.gbx.pyrx.core.crs import authority_srid_of as shim_fn

    assert shim_fn(resolve_crs(4326)) == 4326
    assert shim_fn(resolve_crs(54008)) == 54008
    assert shim_fn(resolve_crs("OGC:CRS84")) is None


def test_shim_exports_new_helpers():
    from databricks.labs.gbx.pyrx.core.crs import (  # noqa: F401
        crs_equal,
        crs_to_proj4,
        to_pyproj_crs,
    )


# --- in_target_domain and the transformer cache (moved from the pyvx CRS tests: they judge core.crs) ----------------


def test_in_target_domain_judges_every_point_against_the_area_of_use():
    import numpy as np

    from databricks.labs.gbx.core.crs import in_target_domain

    gb = pyproj.CRS.from_epsg(27700)
    london, antarctic = [-0.13, 51.5], [150.0, -80.0]
    assert in_target_domain(np.array([london]), gb) is True
    assert in_target_domain(np.array([antarctic]), gb) is False
    assert in_target_domain(np.array([london, antarctic]), gb) is False
    # no area of use, no verdict: the check is skipped
    no_bounds = pyproj.CRS.from_proj4(
        "+proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    )
    assert in_target_domain(np.array([[0.0, 0.0]]), no_bounds) in (None, True)


_RD_NULL_SHIFT = (
    "+proj=sterea +lat_0=52.15616055555555 +lon_0=5.38763888888889 +k=0.9999079 "
    "+x_0=155000 +y_0=463000 +ellps=bessel +towgs84=0,0,0,0,0,0,0 +units=m +no_defs"
)


def test_a_fuzzy_authority_never_shares_a_transformer_with_the_exact_crs():
    """PROJ matches this PROJ4 string to EPSG:28992 at 70%, yet its null datum shift puts points ~177 m away. The cache
    was once keyed on the canonical name, so whichever came first answered for both, in either order.
    """
    from databricks.labs.gbx.core.crs import (
        _transformer_cache,
        _transformer_key,
        crs_to_canonical,
        get_transformer,
    )

    proj4, epsg = CRS.from_user_input(_RD_NULL_SHIFT), CRS.from_epsg(28992)
    assert crs_to_canonical(proj4) == crs_to_canonical(epsg) == "EPSG:28992"
    assert _transformer_key(proj4) != _transformer_key(epsg)
    assert _transformer_key(epsg) == "EPSG:28992"
    for first, second, want in (
        (
            _RD_NULL_SHIFT,
            "EPSG:28992",
            ((155161.545139, 463362.663804), (155191.353812, 463537.136273)),
        ),
        (
            "EPSG:28992",
            _RD_NULL_SHIFT,
            ((155161.545139, 463362.663804), (155191.353812, 463537.136273)),
        ),
    ):
        _transformer_cache().clear()
        got = {
            t: get_transformer(4326, t).transform(5.39, 52.16) for t in (first, second)
        }
        assert got[_RD_NULL_SHIFT] == pytest.approx(want[0], abs=1e-3)
        assert got["EPSG:28992"] == pytest.approx(want[1], abs=1e-3)


def test_equivalent_spellings_of_one_crs_share_one_transformer():
    from databricks.labs.gbx.core.crs import _transformer_cache, get_transformer

    _transformer_cache().clear()
    assert get_transformer(4326, 3857) is get_transformer("EPSG:4326", "3857")
    assert len(_transformer_cache()) == 1


def test_crs_equal_of_two_texts_is_kept_for_the_next_call(monkeypatch):
    """A per-row caller asks the same pair of CRS texts again and again: the pair is parsed once."""
    from databricks.labs.gbx.core import crs as crs_mod

    crs_mod._crs_text_equal.cache_clear()
    calls = []
    real = crs_mod.to_pyproj_crs
    monkeypatch.setattr(crs_mod, "to_pyproj_crs", lambda c: calls.append(c) or real(c))
    wkt = resolve_crs(32632).to_wkt()
    assert crs_mod.crs_equal(wkt, "EPSG:32632") and crs_mod.crs_equal(wkt, "EPSG:32632")
    assert len(calls) == 2  # both texts, once
    assert not crs_mod.crs_equal(wkt, "EPSG:4326")
