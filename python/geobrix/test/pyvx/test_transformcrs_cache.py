"""``st_transformcrs`` resolves each (source, target) pair once per thread, not once per row.

An authority-less target (the NWS Stage IV HRAP WKT) costs a full-confidence authority search to identify, 4 ms of
the 5 ms a row took on a laptop (170 ms on a Serverless worker). The answer is a pure function of the pair, so the
row path reads it from a cache. The outputs do not change, byte for byte.
"""

import pytest
from pyproj import CRS as ProjCRS
from shapely import from_wkb, set_srid, to_wkb
from shapely.geometry import Point

from databricks.labs.gbx.pyvx import _crs

HRAP4 = (
    "+proj=stere +lat_0=90 +lat_ts=60 +lon_0=-105 +k=1 +x_0=0 +y_0=0 "
    "+a=6371200 +b=6371200 +units=m +no_defs"
)
HRAP_WKT = ProjCRS.from_proj4(HRAP4).to_wkt()
PTS = [(-100.0 + 0.37 * i, 35.0 + 0.11 * i) for i in range(40)]


def _wkb(x, y, srid=None):
    g = Point(x, y) if srid is None else set_srid(Point(x, y), srid)
    return to_wkb(g, include_srid=srid is not None)


@pytest.fixture(autouse=True)
def _cold():
    _crs._clear_pair_cache()
    yield
    _crs._clear_pair_cache()


@pytest.fixture
def spy(monkeypatch):
    calls = {"authority": 0, "resolve": 0, "transformer": 0}
    for name, key in (
        ("authority_srid_of", "authority"),
        ("resolve_crs", "resolve"),
        ("get_transformer", "transformer"),
    ):
        real = getattr(_crs, name)

        def counted(*a, _real=real, _key=key, **k):
            calls[_key] += 1
            return _real(*a, **k)

        monkeypatch.setattr(_crs, name, counted)
    return calls


def test_a_wkt_target_is_identified_once_not_per_row(spy):
    for x, y in PTS:
        assert _crs._udf_st_transformcrs(_wkb(x, y), HRAP_WKT, "EPSG:4326") is not None
    assert spy["authority"] == 1
    assert spy["resolve"] <= 2  # the source and the target, once
    assert spy["transformer"] <= 2  # the pair; the source to lon/lat only if projected


def test_a_projected_source_reads_its_lonlat_transformer_once(spy):
    src = _crs._udf_st_transformcrs(_wkb(-100.0, 35.0), HRAP_WKT, "EPSG:4326")
    x, y = from_wkb(src).x, from_wkb(src).y
    spy["transformer"] = 0
    for _ in range(30):
        _crs._udf_st_transformcrs(_wkb(x, y), "EPSG:4326", HRAP_WKT)
    assert spy["transformer"] <= 2


@pytest.mark.parametrize("target", [HRAP_WKT, HRAP4, "EPSG:5070", 5070])
@pytest.mark.parametrize("medium", ["wkb", "ewkb", "wkt", "ewkt"])
def test_the_warm_answer_is_the_cold_answer_byte_for_byte(target, medium):
    def geom(x, y):
        return {
            "wkb": _wkb(x, y),
            "ewkb": _wkb(x, y, 4326),
            "wkt": f"POINT ({x!r} {y!r})",
            "ewkt": f"SRID=4326;POINT ({x!r} {y!r})",
        }[medium]

    src = None if medium in ("ewkb", "ewkt") else "EPSG:4326"
    cold = []
    for x, y in PTS:
        _crs._clear_pair_cache()
        cold.append(_crs.st_transformcrs(geom(x, y), target, src))
    _crs._clear_pair_cache()
    warm = [_crs.st_transformcrs(geom(x, y), target, src) for x, y in PTS]
    assert warm == cold
    assert all(o is not None for o in warm)


def test_the_output_srid_still_follows_the_target():
    assert _crs.st_transformcrs("POINT (-100 35)", HRAP_WKT, 4326).startswith("POINT")
    assert _crs.st_transformcrs("POINT (-100 35)", 5070, 4326).startswith("SRID=5070;")
    # a second call of each, from the cache
    assert _crs.st_transformcrs("POINT (-100 35)", HRAP_WKT, 4326).startswith("POINT")
    assert _crs.st_transformcrs("POINT (-100 35)", 5070, 4326).startswith("SRID=5070;")


def test_errors_are_not_cached():
    for _ in range(2):  # a bad explicit source raises every time
        with pytest.raises(ValueError):
            _crs.st_transformcrs("POINT (1 1)", 5070, "not a crs")
    for _ in range(2):  # a bad target raises every time
        with pytest.raises(ValueError):
            _crs.st_transformcrs("POINT (1 1)", "not a crs", 4326)
    assert _crs.st_transformcrs("POINT (1 1)", 5070) is None  # no source: NULL
    assert (
        _crs.st_transformcrs("SRID=999999;POINT (1 1)", 5070) is None
    )  # unknown SRID: NULL


def test_the_domain_guard_still_nulls_a_point_outside_the_target_area():
    for _ in range(2):
        assert _crs.st_transformcrs("POINT (150 -80)", 27700, 4326) is None


def test_the_cache_is_bounded():
    for i in range(_crs._PAIR_CACHE_SIZE + 10):
        _crs.st_transformcrs("POINT (1 1)", f"+proj=utm +zone=33 +x_0={i}", 4326)
    assert len(_crs._pair_cache()) <= _crs._PAIR_CACHE_SIZE


def _the_pair():
    (pair,) = _crs._pair_cache().values()
    return pair


def test_a_pair_does_not_survive_a_transformer_cache_clear_and_refill():
    """The way to take up grid dirs registered after a transformer was built is to drop the transformer cache. A
    clear in place followed by any other transformer lookup refills it: the pair must still be rebuilt.
    """
    from databricks.labs.gbx.core import crs as corecrs

    _crs.st_transformcrs("POINT (-100 35)", 5070, 4326)
    before = _the_pair()
    corecrs._transformer_cache().clear()
    corecrs.get_transformer(4326, 3857)  # refills the cache, as other code does
    _crs.st_transformcrs("POINT (-100 35)", 5070, 4326)
    after = _the_pair()
    assert after is not before
    assert after.tr is not before.tr


def test_a_pair_does_not_survive_the_cache_being_replaced():
    from databricks.labs.gbx.core import crs as corecrs

    _crs.st_transformcrs("POINT (-100 35)", 5070, 4326)
    before = _the_pair()
    corecrs._thread_local.__dict__.pop("transformers", None)
    corecrs.get_transformer(4326, 3857)
    _crs.st_transformcrs("POINT (-100 35)", 5070, 4326)
    assert _the_pair() is not before


def test_an_untouched_cache_keeps_its_pair():
    _crs.st_transformcrs("POINT (-100 35)", 5070, 4326)
    before = _the_pair()
    _crs.st_transformcrs("POINT (-99 36)", 5070, 4326)
    assert _the_pair() is before
