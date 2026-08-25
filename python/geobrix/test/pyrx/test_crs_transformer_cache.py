"""Task 1 (RasterX CRS-100): transformer cache + resolve_source_crs.

- get_transformer: thread-local, LRU-bounded, canonical-keyed, always_xy.
- resolve_source_crs: Rule 1 per-geom (embedded wins; single explicit param;
  both -> error; neither -> None). Mixed-column safe.
"""

import threading

import pytest
from rasterio.crs import CRS

from databricks.labs.gbx.pyrx.core import crs as C


def test_get_transformer_reuses_same_object_for_equivalent_crs():
    t1 = C.get_transformer(4326, 32633)
    t2 = C.get_transformer("4326", "EPSG:32633")  # equivalent spellings
    assert t1 is t2  # same cached object (same thread)


def test_get_transformer_is_thread_local():
    results = {}

    def grab(name):
        results[name] = C.get_transformer(4326, 3857)

    threads = [threading.Thread(target=grab, args=(f"t{i}",)) for i in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # different threads get independent instances (no shared mutable object)
    assert results["t0"] is not results["t1"]


def test_get_transformer_always_xy():
    t = C.get_transformer(4326, 3857)
    # lon=0, lat=0 -> x=0, y=0 in web mercator; always_xy means (lon, lat) input.
    x, y = t.transform(0.0, 0.0)
    assert abs(x) < 1e-6 and abs(y) < 1e-6


def test_get_transformer_lru_evicts_beyond_cap():
    # Fill well beyond the cap with distinct valid CRS *pairs*; oldest evicted,
    # no error, cache stays bounded. 120 valid UTM zones x 2 targets = 240 pairs.
    zones = list(range(32601, 32661)) + list(range(32701, 32761))  # 120 valid zones
    for zone in zones:
        C.get_transformer(zone, 4326)
        C.get_transformer(zone, 3857)
    cache = C._transformer_cache()
    assert len(cache) <= C._TRANSFORMER_CACHE_SIZE  # bounded (LRU-evicted)
    # a freshly requested pair still works after eviction churn
    assert C.get_transformer(4326, 3857) is not None


def test_resolve_source_crs_rule1():
    # embedded SRID wins
    assert C.resolve_source_crs(4326, None, None) == CRS.from_epsg(4326)
    # embedded ESRI code -> ESRI
    assert C.resolve_source_crs(54008, None, None).to_authority() == ("ESRI", "54008")
    # plain geom + explicit srid
    assert C.resolve_source_crs(0, 32633, None) == CRS.from_epsg(32633)
    # plain geom + explicit crs string
    assert C.resolve_source_crs(0, None, "ESRI:54008").to_authority() == (
        "ESRI",
        "54008",
    )
    # neither -> None (CRS-less)
    assert C.resolve_source_crs(0, None, None) is None
    # both srid and crs -> error
    with pytest.raises(ValueError, match="srid OR crs"):
        C.resolve_source_crs(0, 4326, "EPSG:3857")
    # embedded SRID present: param ignored per-geom (NO error - mixed-column safe)
    assert C.resolve_source_crs(4326, 32633, None) == CRS.from_epsg(4326)
