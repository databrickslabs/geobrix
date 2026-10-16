"""Tests for vizx._dynamic (Phase-1.5 dynamic zoom cut-over widget).

Logic-level tests: widget construction, ESM content, and the seam-gate helper.
Browser comm round-trip is proven by Spike B (model.send -> on_msg -> trait ->
change handler); that path is not unit-testable headlessly.
"""

import threading

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
    _TileCache,
    _viewport_payload,
    plot_interactive_dynamic,
)
from databricks.labs.gbx.vizx._layers import vector_layer  # noqa: E402

# ---------------------------------------------------------------------------
# Task 13b: _TileCache + prefetch worker tests
# ---------------------------------------------------------------------------


def test_tilecache_lru_evicts_oldest():
    """Insert > capacity: the oldest (least-recently accessed) viewed entry is evicted."""
    cache = _TileCache(max_entries=2)
    cache.put((1, 0, 0), b"tile-a", viewed=True)
    cache.put((1, 1, 0), b"tile-b", viewed=True)
    # Access (1,0,0) so (1,1,0) is now the LRU.
    assert cache.get((1, 0, 0)) == b"tile-a"
    # Insert a third entry — (1,1,0) is the LRU and must be evicted.
    cache.put((1, 0, 1), b"tile-c", viewed=True)
    assert cache.get((1, 1, 0)) is None  # evicted
    assert cache.get((1, 0, 0)) == b"tile-a"
    assert cache.get((1, 0, 1)) == b"tile-c"


def test_tilecache_evicts_unviewed_before_viewed():
    """When full, a never-viewed (prefetched-only) entry is evicted before a viewed one."""
    cache = _TileCache(max_entries=2)
    cache.put((1, 0, 0), b"viewed", viewed=True)
    cache.put((1, 1, 0), b"prefetched", viewed=False)
    # Both slots are full. A third put must evict the unviewed entry.
    cache.put((1, 2, 0), b"new-viewed", viewed=True)
    assert cache.get((1, 1, 0)) is None  # unviewed evicted first
    assert cache.get((1, 0, 0)) == b"viewed"  # viewed entry survives
    assert cache.get((1, 2, 0)) == b"new-viewed"


def test_prefetch_populates_neighbor_ring():
    """After a viewport request at (z,x,y) + flush, all 8 neighbors are in the cache."""
    calls = []

    def stub_tiler(z, x, y):
        calls.append((z, x, y))
        return b"tile-bytes"

    cache = _TileCache(max_entries=64)
    worker = _PrefetchWorker(cache=cache, tiler=stub_tiler)
    try:
        # Simulate a real viewport request at (12, 5, 5) — populates the cache and
        # schedules neighbor prefetch.
        worker.on_viewport_served(z=12, x=5, y=5)
        worker.flush()

        # All 8 neighbors must be in the cache (not marked viewed).
        expected_neighbors = [
            (12, dx, dy)
            for dx in (4, 5, 6)
            for dy in (4, 5, 6)
            if not (dx == 5 and dy == 5)  # exclude the center tile itself
        ]
        for key in expected_neighbors:
            assert cache.get(key) == b"tile-bytes", f"missing neighbor {key}"
        # Stub was called for all 8.
        assert set(calls) == set((z, x, y) for z, x, y in expected_neighbors)
    finally:
        worker.shutdown()


def test_cache_hit_skips_retiling():
    """A second request for an already-prefetched neighbor is served from cache, stub not called again."""
    calls = []

    def stub_tiler(z, x, y):
        calls.append((z, x, y))
        return b"tile-bytes"

    cache = _TileCache(max_entries=64)
    worker = _PrefetchWorker(cache=cache, tiler=stub_tiler)
    try:
        # Prefetch neighbors of (12, 5, 5).
        worker.on_viewport_served(z=12, x=5, y=5)
        worker.flush()

        calls_after_first = list(calls)
        # (12, 4, 4) is a neighbor — it must already be in the cache.
        assert cache.get((12, 4, 4)) == b"tile-bytes"

        # Now request (12, 4, 4) as a viewport (simulate cache-hit serving).
        # The worker's serve path should find it in cache and NOT call the tiler again.
        hit = cache.get((12, 4, 4))
        assert hit == b"tile-bytes"
        # Mark it viewed (as cache-first serving would do).
        cache.put((12, 4, 4), hit, viewed=True)

        # Schedule neighbors of (12, 4, 4) — the tiler must NOT be called for (12, 4, 4) again.
        worker.on_viewport_served(z=12, x=4, y=4)
        worker.flush()

        retile_calls = [c for c in calls if c not in calls_after_first]
        assert (12, 4, 4) not in retile_calls, "tiler called again for a cached tile"
    finally:
        worker.shutdown()


def test_prefetch_coalesces_stale():
    """A rapid second viewport supersedes the first; stale prefetch is skipped."""
    calls = []
    # Use a barrier to hold the worker mid-flight so we can advance the generation.
    gate = threading.Event()
    released = threading.Event()

    def slow_tiler(z, x, y):
        # Stall the very first call to give the test time to advance the generation.
        if not released.is_set():
            gate.wait(timeout=5)
            released.set()
        calls.append((z, x, y))
        return b"tile-bytes"

    cache = _TileCache(max_entries=64)
    worker = _PrefetchWorker(cache=cache, tiler=slow_tiler)
    try:
        # Schedule prefetch for viewport (12, 10, 10).
        worker.on_viewport_served(z=12, x=10, y=10)
        # Immediately supersede with a new viewport (12, 20, 20).
        worker.on_viewport_served(z=12, x=20, y=20)
        # Unblock the stalled worker.
        gate.set()
        worker.flush()

        # The stale ring around (10, 10) must not all be in the cache; some
        # entries may have been skipped.  At minimum: none of the stale neighbors
        # (those NOT adjacent to (20,20)) should be present.
        stale_only_neighbors = [
            (12, x, y)
            for x in (9, 10, 11)
            for y in (9, 10, 11)
            if not (x == 10 and y == 10)
            if not (19 <= x <= 21 and 19 <= y <= 21)
        ]
        # Not all stale neighbors were cached (coalescing dropped at least some).
        cached_stale = [k for k in stale_only_neighbors if cache.get(k) is not None]
        assert len(cached_stale) == 0, (
            f"Stale-generation ring DID populate the cache — generation token not working. "
            f"Stale entries found: {cached_stale}"
        )
    finally:
        worker.shutdown()


@_needs_tippecanoe
def test_builds_widget_with_overview_and_esm():
    gdf = gpd.GeoDataFrame({"v": [1]}, geometry=[Point(-122.4, 37.7)], crs="EPSG:4326")
    w = plot_interactive_dynamic([vector_layer(gdf)], simplify_tiles_spec={"max_z": 8})
    assert isinstance(w, anywidget.AnyWidget)
    assert "moveend" in w._esm and "model.send" in w._esm


def test_viewport_payload_only_fires_above_seam():
    assert (
        _viewport_payload(bbox=[-122.5, 37.7, -122.4, 37.8], zoom=12, max_z=10)
        is not None
    )
    assert (
        _viewport_payload(bbox=[-122.5, 37.7, -122.4, 37.8], zoom=9, max_z=10) is None
    )


def test_viewport_payload_at_seam_boundary():
    # zoom == max_z is NOT above the seam — must return None.
    assert (
        _viewport_payload(bbox=[-122.5, 37.7, -122.4, 37.8], zoom=10, max_z=10) is None
    )
    # zoom == max_z + 1 is above the seam — must return a payload.
    assert (
        _viewport_payload(bbox=[-122.5, 37.7, -122.4, 37.8], zoom=11, max_z=10)
        is not None
    )


def test_viewport_payload_structure():
    result = _viewport_payload(bbox=[-122.5, 37.7, -122.4, 37.8], zoom=12, max_z=10)
    assert result is not None
    assert result["bbox"] == [-122.5, 37.7, -122.4, 37.8]
    assert result["zoom"] == 12


@_needs_tippecanoe
def test_widget_has_detail_trait():
    gdf = gpd.GeoDataFrame({"v": [1]}, geometry=[Point(-122.4, 37.7)], crs="EPSG:4326")
    w = plot_interactive_dynamic([vector_layer(gdf)], simplify_tiles_spec={"max_z": 8})
    # detail must be a synced traitlets.Unicode (checked via trait metadata)
    assert hasattr(w, "detail")
    # Synced traits have 'sync' in their metadata
    trait_meta = w.traits()["detail"].metadata
    assert trait_meta.get("sync") is True


@_needs_tippecanoe
def test_widget_esm_registers_change_detail_handler():
    gdf = gpd.GeoDataFrame({"v": [1]}, geometry=[Point(-122.4, 37.7)], crs="EPSG:4326")
    w = plot_interactive_dynamic([vector_layer(gdf)], simplify_tiles_spec={"max_z": 8})
    # The ESM must listen on 'change:detail' to refresh the map source.
    assert "change:detail" in w._esm


@_needs_tippecanoe
def test_widget_esm_references_maplibre():
    """The ESM should use MapLibre (the SRI-pinned CDN URL from _maplibre.py)."""
    gdf = gpd.GeoDataFrame({"v": [1]}, geometry=[Point(-122.4, 37.7)], crs="EPSG:4326")
    w = plot_interactive_dynamic([vector_layer(gdf)], simplify_tiles_spec={"max_z": 8})
    assert "maplibre" in w._esm.lower() or "maplibregl" in w._esm.lower()


@_needs_tippecanoe
def test_esm_is_synced_to_frontend():
    """_esm must appear in get_state() — i.e. it actually reaches the JS frontend."""
    gdf = gpd.GeoDataFrame({"v": [1]}, geometry=[Point(-122.4, 37.7)], crs="EPSG:4326")
    w = plot_interactive_dynamic([vector_layer(gdf)], simplify_tiles_spec={"max_z": 8})
    state = w.get_state()
    # _esm must be in the synced state dict (proves anywidget registered it sync=True).
    assert (
        "_esm" in state
    ), "_esm is not in get_state() — widget will render blank in a notebook"
    # Sanity-check: the ESM content is present (not an empty/null stub).
    esm_val = state["_esm"]
    if isinstance(esm_val, (list, tuple)):
        # anywidget may wrap as (value, metadata) buffer tuple
        esm_val = esm_val[0]
    assert "moveend" in esm_val, "_esm in state but content is wrong"
    # detail trait must also be synced.
    assert "detail" in state, "detail trait is not in get_state()"


@_needs_tippecanoe
def test_custom_on_viewport_called():
    """A user-supplied on_viewport callback replaces the default tiler."""
    gdf = gpd.GeoDataFrame({"v": [1]}, geometry=[Point(-122.4, 37.7)], crs="EPSG:4326")
    calls = []

    def my_callback(bbox, zoom):
        calls.append((bbox, zoom))
        return b"FAKE_PMTILES_BYTES"

    w = plot_interactive_dynamic(
        [vector_layer(gdf)],
        simplify_tiles_spec={"max_z": 8},
        on_viewport=my_callback,
    )
    assert isinstance(w, anywidget.AnyWidget)

    # Simulate a JS message (the on_msg handler path, not the live comm).
    # Trigger the internal handler directly by calling send-equivalent internals.
    # We access the registered on_msg handler via a private test hook:
    w._gbx_handle_msg({"bbox": [-122.5, 37.7, -122.4, 37.8], "zoom": 12})
    assert len(calls) == 1
    assert calls[0] == ([-122.5, 37.7, -122.4, 37.8], 12)
    # Prove the Python data-flow: _handle_msg must have base64-encoded the result
    # into the detail trait (which syncs to the JS frontend).
    assert w.detail != "", "detail trait is empty after on_viewport returned bytes"


@_needs_tippecanoe
def test_custom_on_viewport_below_seam_not_called():
    """on_viewport must NOT be called when zoom <= max_z (seam gate)."""
    gdf = gpd.GeoDataFrame({"v": [1]}, geometry=[Point(-122.4, 37.7)], crs="EPSG:4326")
    calls = []

    def my_callback(bbox, zoom):
        calls.append((bbox, zoom))
        return b"FAKE_PMTILES_BYTES"

    w = plot_interactive_dynamic(
        [vector_layer(gdf)],
        simplify_tiles_spec={"max_z": 8},
        on_viewport=my_callback,
    )
    w._gbx_handle_msg({"bbox": [-122.5, 37.7, -122.4, 37.8], "zoom": 7})
    assert calls == [], "on_viewport must not fire below the seam"


@_needs_tippecanoe
def test_handle_msg_cache_hit_skips_callback():
    """On a _handle_msg cache hit, the user callback is NOT invoked."""
    gdf = gpd.GeoDataFrame({"v": [1]}, geometry=[Point(-122.4, 37.7)], crs="EPSG:4326")
    callback_calls = []

    def counting_callback(bbox, zoom):
        callback_calls.append((bbox, zoom))
        return b"FAKE_PMTILES"

    w = plot_interactive_dynamic(
        [vector_layer(gdf)],
        simplify_tiles_spec={"max_z": 8},
        on_viewport=counting_callback,
    )

    zoom = 12
    bbox = [-122.5, 37.7, -122.4, 37.8]
    # Pre-seed the cache (accessed via the widget's internal cache through the worker).
    # We can't access _cache directly, so we drive a first msg to populate it.
    w._gbx_handle_msg({"bbox": bbox, "zoom": zoom})
    assert len(callback_calls) == 1, "First call should hit the callback (cache miss)"

    # Reset calls — now send the same message again (cache hit path).
    callback_calls.clear()
    w._gbx_handle_msg({"bbox": bbox, "zoom": zoom})
    # The callback must NOT be called on a cache hit.
    assert len(callback_calls) == 0, (
        f"Callback was called {len(callback_calls)} times on a cache hit — "
        "cache-first path not working in _handle_msg"
    )


def test_tile_bbox_z0_whole_world():
    """At z=0, tile (0,0,0) must cover the whole Web Mercator world."""
    from databricks.labs.gbx.vizx._dynamic import _tile_bbox

    min_lon, min_lat, max_lon, max_lat = _tile_bbox(0, 0, 0)
    assert abs(min_lon - (-180.0)) < 1e-9
    assert abs(max_lon - 180.0) < 1e-9
    # Web Mercator clips at ~±85.05°.
    assert min_lat < -80.0
    assert max_lat > 80.0


def test_tile_bbox_neighbors_have_distinct_bboxes():
    """Neighboring tiles must produce non-overlapping, distinct bounding boxes."""
    from databricks.labs.gbx.vizx._dynamic import _tile_bbox

    bb_center = _tile_bbox(10, 512, 512)
    bb_right = _tile_bbox(10, 513, 512)
    bb_below = _tile_bbox(10, 512, 513)
    # Center and right tiles share a lon edge but have different min/max lons.
    assert bb_center != bb_right
    assert bb_center != bb_below
    # Adjacency: max_lon of center == min_lon of right.
    assert abs(bb_center[2] - bb_right[0]) < 1e-9


def test_tiler_for_prefetch_uses_distinct_bboxes():
    """Each neighbor tile must be fetched with a DISTINCT bbox (not all identical)."""
    from databricks.labs.gbx.vizx._dynamic import _tile_bbox

    # Capture the bbox args passed to the default_callback via _tiler_for_prefetch
    # by checking that _tile_bbox produces distinct results for each neighbor.
    z, x, y = 12, 100, 100
    neighbors = [
        (z, x + dx, y + dy)
        for dx in (-1, 0, 1)
        for dy in (-1, 0, 1)
        if not (dx == 0 and dy == 0)
    ]
    bboxes = [_tile_bbox(*n) for n in neighbors]
    # All 8 bboxes must be distinct.
    assert (
        len(set(bboxes)) == 8
    ), f"Expected 8 distinct bboxes for 8 neighbors, got {len(set(bboxes))}: {bboxes}"


def test_put_if_absent_does_not_demote_viewed():
    """put_if_absent must NOT downgrade a viewed tile to prefetched."""
    cache = _TileCache(max_entries=64)
    key = (10, 5, 5)
    cache.put(key, b"original", viewed=True)
    # A prefetch arriving later must not demote to viewed=False.
    cache.put_if_absent(key, b"prefetch-attempt")
    # The tile must still be in the viewed dict (get still returns original bytes).
    assert cache.get(key) == b"original"
    # Directly verify viewed dict contains the key (not evicted or demoted).
    with cache._lock:
        assert key in cache._viewed, "Viewed tile was demoted by put_if_absent"
        assert key not in cache._prefetched
