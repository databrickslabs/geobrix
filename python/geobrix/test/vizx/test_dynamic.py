"""Tests for vizx._dynamic (Phase-1.5 dynamic zoom cut-over widget).

Logic-level tests: widget construction, ESM content, and the seam-gate helper.
Browser comm round-trip is proven by Spike B (model.send -> on_msg -> trait ->
change handler); that path is not unit-testable headlessly.
"""

import pytest

anywidget = pytest.importorskip("anywidget")

from databricks.labs.gbx.vizx._simplify import _resolve_tippecanoe_bin  # noqa: E402

# These build simplified overview tiles, which shells out to tippecanoe. CI installs
# it from the env locks; the dev container deliberately does not (it segfaults
# natively there), so skip with a clear reason rather than fail.
_needs_tippecanoe = pytest.mark.skipif(
    _resolve_tippecanoe_bin() is None, reason="tippecanoe not installed"
)

import geopandas as gpd  # noqa: E402
from shapely.geometry import Point  # noqa: E402

from databricks.labs.gbx.vizx._dynamic import (  # noqa: E402
    _PrefetchWorker,
    _tile_bbox,
    _TileCache,
    plot_interactive_dynamic,
)
from databricks.labs.gbx.vizx._layers import vector_layer  # noqa: E402


def _widget(**kw):
    gdf = gpd.GeoDataFrame({"v": [1]}, geometry=[Point(-122.4, 37.7)], crs="EPSG:4326")
    return plot_interactive_dynamic(
        [vector_layer(gdf)], simplify_tiles_spec={"max_z": 8}, **kw
    )


@_needs_tippecanoe
def test_widget_builds_and_syncs_esm_and_detail_to_the_frontend():
    w = _widget()
    assert isinstance(w, anywidget.AnyWidget)
    assert w.traits()["detail"].metadata.get("sync") is True
    # The ESM must be in the synced state or the widget renders blank in a notebook.
    state = w.get_state()
    esm = (
        state["_esm"][0] if isinstance(state["_esm"], (list, tuple)) else state["_esm"]
    )
    assert "detail" in state
    for token in ("moveend", "model.send", "change:detail", "maplibre"):
        assert token in esm.lower()


@_needs_tippecanoe
def test_viewport_callback_fires_above_the_seam_and_hits_the_cache_after():
    calls = []

    def callback(bbox, zoom):
        calls.append((bbox, zoom))
        return b"FAKE_PMTILES_BYTES"

    w = _widget(on_viewport=callback)
    bbox = [-122.5, 37.7, -122.4, 37.8]
    w._gbx_handle_msg({"bbox": bbox, "zoom": 7})  # at or below max_z (8): no callback
    w._gbx_handle_msg({"bbox": bbox, "zoom": 8})
    assert calls == []
    w._gbx_handle_msg({"bbox": bbox, "zoom": 12})
    assert calls == [(bbox, 12)]
    assert w.detail != ""
    w._gbx_handle_msg({"bbox": bbox, "zoom": 12})  # same viewport: served from cache
    assert len(calls) == 1


def test_tile_cache_keeps_viewed_tiles_and_prefetch_fills_the_neighbor_ring():
    cache = _TileCache(max_entries=2)
    cache.put((1, 0, 0), b"viewed", viewed=True)
    cache.put((1, 1, 0), b"prefetched", viewed=False)
    cache.put((1, 2, 0), b"new", viewed=True)  # full: the unviewed entry goes first
    assert cache.get((1, 1, 0)) is None and cache.get((1, 0, 0)) == b"viewed"
    # A later prefetch of a viewed tile must not demote it.
    cache.put_if_absent((1, 0, 0), b"prefetch-attempt")
    assert cache.get((1, 0, 0)) == b"viewed"

    calls = []

    def tiler(z, x, y):
        calls.append((z, x, y))
        return b"tile"

    ring = {(12, x, y) for x in (4, 5, 6) for y in (4, 5, 6)} - {(12, 5, 5)}
    cache = _TileCache(max_entries=64)
    worker = _PrefetchWorker(cache=cache, tiler=tiler)
    try:
        worker.on_viewport_served(z=12, x=5, y=5)
        worker.flush()
    finally:
        worker.shutdown()
    assert set(calls) == ring and all(cache.get(k) == b"tile" for k in ring)


def test_tile_bbox_covers_the_world_at_z0_and_neighbors_get_distinct_bboxes():
    min_lon, min_lat, max_lon, max_lat = _tile_bbox(0, 0, 0)
    assert abs(min_lon + 180.0) < 1e-9 and abs(max_lon - 180.0) < 1e-9
    assert min_lat < -80.0 and max_lat > 80.0
    ring = [
        (12, 100 + dx, 100 + dy)
        for dx in (-1, 0, 1)
        for dy in (-1, 0, 1)
        if (dx, dy) != (0, 0)
    ]
    assert len({_tile_bbox(*k) for k in ring}) == 8
    assert abs(_tile_bbox(10, 512, 512)[2] - _tile_bbox(10, 513, 512)[0]) < 1e-9
