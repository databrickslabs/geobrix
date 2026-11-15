"""Phase-1.5 dynamic zoom cut-over: AnyWidget-based MapLibre viewer.

``plot_interactive_dynamic`` embeds a low-zoom (min_z..max_z) PMTiles
overview as the base MapLibre source (exactly like ``build_html`` does).
When the user pans/zooms above ``max_z``, the JS side fires a
``model.send({bbox, zoom})`` message; the Python ``on_msg`` handler calls
``on_viewport(bbox, zoom)``, base64s the returned PMTiles bytes into the
``detail`` synced trait, and the JS ``change:detail`` handler swaps the
detail source into the live map.

The comm round-trip (``model.send`` → ``on_msg`` → trait → ``change``
handler) is proven on Serverless by Spike B and is not unit-testable
headlessly.

Helper:
  ``_viewport_payload(bbox, zoom, max_z)`` → payload dict when ``zoom > max_z``,
  else ``None`` — the seam gate used by both the JS and the Python handler.
"""

from __future__ import annotations

import base64
import json
import threading
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Optional, Tuple

import anywidget
import traitlets

from databricks.labs.gbx.vizx._maplibre import (
    _CARTO_STYLE,
    _MAPLIBRE_CSS,
    _MAPLIBRE_CSS_SRI,
    _MAPLIBRE_JS,
    _MAPLIBRE_JS_SRI,
    _PMTILES_JS,
    _PMTILES_JS_SRI,
    _json_for_script,
    layer_to_sources_layers,
)
from databricks.labs.gbx.vizx._simplify import (
    normalize_spec,
    simplify_tiles_from_source,
)

# ---------------------------------------------------------------------------
# Tile cache
# ---------------------------------------------------------------------------

_TileKey = Tuple[int, int, int]  # (z, x, y)


class _TileCache:
    """Bounded LRU tile cache keyed by ``(z, x, y)``.

    Eviction policy (priority order when full):
    1. Never-viewed (speculatively prefetched) entries are evicted before viewed ones,
       so eager prefetch cannot push out real user-visited tiles.
    2. Within each class (viewed / unviewed), evict the least-recently used.

    # TODO: distance-aware eviction (evict farthest from current viewport center) is
    # a documented future refinement.
    """

    def __init__(self, max_entries: int = 64) -> None:
        self._max = max_entries
        # Separate LRU-ordered dicts for each class.
        # OrderedDict preserves insertion order; move_to_end keeps LRU at front.
        self._viewed: OrderedDict[_TileKey, bytes] = OrderedDict()
        self._prefetched: OrderedDict[_TileKey, bytes] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: _TileKey) -> Optional[bytes]:
        """Return cached bytes for *key*, updating recency, or ``None`` on miss."""
        with self._lock:
            if key in self._viewed:
                self._viewed.move_to_end(key)
                return self._viewed[key]
            if key in self._prefetched:
                self._prefetched.move_to_end(key)
                return self._prefetched[key]
            return None

    def put(self, key: _TileKey, value: bytes, *, viewed: bool) -> None:
        """Store *value* under *key*.

        ``viewed=True`` marks a tile that was actually served to the viewport;
        ``viewed=False`` marks a speculatively prefetched tile.  If the entry
        already exists in the other class, it is promoted/demoted accordingly.
        """
        with self._lock:
            # Remove from whichever class currently holds this key (if any).
            self._viewed.pop(key, None)
            self._prefetched.pop(key, None)

            target = self._viewed if viewed else self._prefetched
            target[key] = value
            target.move_to_end(key)

            # Evict if over capacity: unviewed first, then viewed (LRU within each).
            while len(self._viewed) + len(self._prefetched) > self._max:
                if self._prefetched:
                    # Evict LRU unviewed (first item = oldest).
                    self._prefetched.popitem(last=False)
                else:
                    # All entries are viewed — evict LRU viewed.
                    self._viewed.popitem(last=False)

    def put_if_absent(self, key: _TileKey, value: bytes) -> None:
        """Insert *key* as a prefetched (viewed=False) entry only if it is not
        already present in either the viewed or prefetched dict.

        This prevents a background prefetch from downgrading a tile that the
        main thread has already served (viewed=True) to viewed=False.
        """
        with self._lock:
            if key in self._viewed or key in self._prefetched:
                return
            self._prefetched[key] = value
            self._prefetched.move_to_end(key)

            # Evict if over capacity: unviewed first, then viewed (LRU within each).
            while len(self._viewed) + len(self._prefetched) > self._max:
                if self._prefetched:
                    self._prefetched.popitem(last=False)
                else:
                    self._viewed.popitem(last=False)

    def __contains__(self, key: _TileKey) -> bool:
        with self._lock:
            return key in self._viewed or key in self._prefetched


# ---------------------------------------------------------------------------
# Prefetch worker
# ---------------------------------------------------------------------------


class _PrefetchWorker:
    """Background daemon worker that prefetches the 8-neighbor ring around
    each served viewport tile.

    Coalescing: each call to ``on_viewport_served`` increments a generation
    counter.  The worker checks the generation before each tile fetch; if the
    generation has advanced the viewport has moved and stale work is skipped.

    Test-injectable: pass *tiler* (a ``(z, x, y) -> bytes`` callable) and
    call ``flush()`` to synchronously wait for pending prefetch to finish.
    Production callers pass *tiler* = None and supply a real tiler separately
    via ``set_tiler``.
    """

    def __init__(
        self,
        cache: _TileCache,
        tiler: Optional[Callable[[int, int, int], bytes]] = None,
        *,
        max_workers: int = 1,
    ) -> None:
        self._cache = cache
        self._tiler = tiler
        self._gen = 0
        self._lock = threading.Lock()
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="gbx-prefetch"
        )
        self._futures: list = []

    def set_tiler(self, tiler: Callable[[int, int, int], bytes]) -> None:
        self._tiler = tiler

    def on_viewport_served(self, *, z: int, x: int, y: int) -> None:
        """Called after a tile at ``(z, x, y)`` has been served to the viewport.

        Schedules prefetch of the 8-neighbor ring in the background.
        A new call supersedes any in-flight prefetch for a stale viewport.
        """
        with self._lock:
            self._gen += 1
            gen = self._gen

        neighbors = [
            (z, x + dx, y + dy)
            for dx in (-1, 0, 1)
            for dy in (-1, 0, 1)
            if not (dx == 0 and dy == 0)
        ]

        future = self._executor.submit(self._prefetch_ring, neighbors, gen)
        with self._lock:
            self._futures.append(future)

    def _prefetch_ring(self, neighbors: list, gen: int) -> None:
        for key in neighbors:
            with self._lock:
                current_gen = self._gen
            if current_gen != gen:
                # Viewport has moved on — skip remaining stale work.
                break
            if key in self._cache:
                continue
            if self._tiler is not None:
                try:
                    data = self._tiler(*key)
                    # Only write if generation is still current.
                    with self._lock:
                        still_current = self._gen == gen
                    if still_current:
                        self._cache.put_if_absent(key, data)
                except Exception:
                    pass

    def flush(self) -> None:
        """Synchronously wait for all pending prefetch futures to complete."""
        with self._lock:
            futures = list(self._futures)
            self._futures.clear()
        for f in futures:
            try:
                f.result(timeout=30)
            except Exception:
                pass

    def shutdown(self) -> None:
        self._executor.shutdown(wait=False)


# ---------------------------------------------------------------------------
# Seam-gate helper
# ---------------------------------------------------------------------------


def _viewport_payload(
    bbox: list[float],
    zoom: float,
    max_z: int,
) -> Optional[dict]:
    """Return a payload dict when ``zoom > max_z``, else ``None``.

    This is the seam gate: at or below ``max_z`` the overview archive is
    sufficient and no viewport tiling is triggered; above ``max_z`` the
    caller should tile the current viewport.

    Args:
        bbox:  ``[west, south, east, north]`` in lon/lat degrees.
        zoom:  Current map zoom level (float).
        max_z: The overview max zoom (from the simplify_tiles_spec).

    Returns:
        ``{"bbox": bbox, "zoom": zoom}`` when ``zoom > max_z``, else ``None``.
    """
    if zoom > max_z:
        return {"bbox": bbox, "zoom": zoom}
    return None


# ---------------------------------------------------------------------------
# Default on_viewport implementation
# ---------------------------------------------------------------------------


def _default_on_viewport(
    source,
    max_z: int,
    spec: dict,
) -> Callable[[list, float], bytes]:
    """Return a closure that tiles the current viewport from *source*.

    The viewport is tiled via ``simplify_tiles_from_source`` using
    ``min_z=max_z+1`` so the detail level is always above the overview seam.
    The bbox is passed through so only features within the viewport are tiled.

    Args:
        source: The original data source (GeoDataFrame, path, etc.) to tile from.
        max_z:  The overview max zoom — detail starts at ``max_z + 1``.
        spec:   The base simplify spec; min_z is overridden to ``max_z + 1``.

    Returns:
        A callable ``(bbox, zoom) -> bytes`` that runs tippecanoe for the
        requested viewport at ``min_z=max_z+1, max_z=zoom`` (clamped to
        ``spec["max_z"]`` if the caller's spec has a lower ceiling).
    """

    def _tile_viewport(bbox: list[float], zoom: float) -> bytes:
        detail_min_z = max_z + 1
        detail_max_z = max(int(zoom), detail_min_z)
        viewport_spec = dict(spec)
        viewport_spec["min_z"] = detail_min_z
        viewport_spec["max_z"] = detail_max_z
        # Pass the bbox so only features within the viewport are tiled.
        clip_bbox = tuple(bbox) if bbox else None
        return simplify_tiles_from_source(source, spec=viewport_spec, bbox=clip_bbox)

    return _tile_viewport


# ---------------------------------------------------------------------------
# ESM builder
# ---------------------------------------------------------------------------


def _build_esm(
    overview_sources_json: str,
    overview_layers_json: str,
    pm_register_js: str,
    max_z: int,
    center: list,
    zoom: int,
) -> str:
    """Build the AnyWidget ``_esm`` JavaScript module.

    The module:
    1. Loads MapLibre and pmtiles via CDN ``<script>`` tags injected into
       ``document.head`` (anywidget cannot use external ``<link>``/``<script>``
       in the ESM directly, but injecting into the head works on Databricks).
    2. Creates the MapLibre map with the embedded overview.
    3. Registers a ``moveend`` handler that, when ``map.getZoom() > max_z``,
       calls ``model.send({bbox, zoom})``.
    4. Listens on ``change:detail`` and adds/replaces a ``detail`` source +
       layer from the base64-encoded PMTiles archive in the trait.

    All JSON embedded in the script uses ``_json_for_script`` escaping to
    prevent breakout through ``</script>`` or ``<``/``>``/``&`` in data.

    Args:
        overview_sources_json: JSON string of the MapLibre sources dict.
        overview_layers_json:  JSON string of the MapLibre layers list.
        pm_register_js:        JS snippet(s) that register PMTiles sources
                               with the pmtiles protocol (from ``_maplibre``).
        max_z:                 The overview seam — JS fires only above this.
        center:                ``[lon, lat]`` map centre.
        zoom:                  Initial zoom level.
    """
    center_json = json.dumps(center)
    max_z_js = int(max_z)
    zoom_js = int(zoom)

    return f"""\
function _gbxLoadScript(src, integrity) {{
  return new Promise((resolve, reject) => {{
    if (document.querySelector('script[src="' + src + '"]')) {{ resolve(); return; }}
    const s = document.createElement('script');
    s.src = src;
    if (integrity) {{ s.integrity = integrity; s.crossOrigin = 'anonymous'; }}
    s.onload = resolve;
    s.onerror = reject;
    document.head.appendChild(s);
  }});
}}
function _gbxLoadCss(href, integrity) {{
  if (document.querySelector('link[href="' + href + '"]')) return;
  const l = document.createElement('link');
  l.rel = 'stylesheet'; l.href = href;
  if (integrity) {{ l.integrity = integrity; l.crossOrigin = 'anonymous'; }}
  document.head.appendChild(l);
}}

async function render({{ model, el }}) {{
  // Load MapLibre + pmtiles from SRI-pinned CDN.
  _gbxLoadCss({json.dumps(_MAPLIBRE_CSS)}, {json.dumps(_MAPLIBRE_CSS_SRI)});
  await _gbxLoadScript({json.dumps(_MAPLIBRE_JS)}, {json.dumps(_MAPLIBRE_JS_SRI)});
  await _gbxLoadScript({json.dumps(_PMTILES_JS)}, {json.dumps(_PMTILES_JS_SRI)});

  const mapDiv = document.createElement('div');
  mapDiv.style.cssText = 'height:480px;width:100%;';
  el.appendChild(mapDiv);

  // Register pmtiles protocol.
  const proto = new pmtiles.Protocol();
  maplibregl.addProtocol('pmtiles', proto.tile.bind(proto));

  // Register the embedded overview archive(s) with the protocol.
  {pm_register_js}

  // Build the map.
  const map = new maplibregl.Map({{
    container: mapDiv,
    style: {json.dumps(_CARTO_STYLE)},
    center: {center_json},
    zoom: {zoom_js},
  }});

  const overviewSources = {overview_sources_json};
  const overviewLayers = {overview_layers_json};

  map.on('load', () => {{
    for (const [sid, sdef] of Object.entries(overviewSources)) {{
      map.addSource(sid, sdef);
    }}
    for (const ly of overviewLayers) {{
      map.addLayer(ly);
    }}
  }});

  // moveend: fire model.send when zoom > max_z (above the seam).
  const MAX_Z = {max_z_js};
  map.on('moveend', () => {{
    const z = map.getZoom();
    if (z > MAX_Z) {{
      const b = map.getBounds();
      model.send({{
        bbox: [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()],
        zoom: z,
      }});
    }}
  }});

  // change:detail: add or replace the detail source from the synced trait.
  model.on('change:detail', () => {{
    const b64 = model.get('detail');
    if (!b64) return;
    try {{
      const raw = Uint8Array.from(atob(b64), c => c.charCodeAt(0));
      const pm = new pmtiles.PMTiles(new pmtiles.FileSource(
        new File([raw.buffer], 'detail.pmtiles')
      ));
      proto.add(pm);
      if (map.getSource('gbx_detail')) {{
        map.removeLayer('gbx_detail-fill');
        map.removeSource('gbx_detail');
      }}
      map.addSource('gbx_detail', {{ type: 'vector', url: 'pmtiles://gbx_detail' }});
      pm.getHeader().then(h => {{
        const meta = h.metadata || {{}};
        const vl = (meta.vector_layers || []);
        const srcLayer = vl.length > 0 ? vl[0].id : 'buildings';
        map.addLayer({{
          id: 'gbx_detail-fill',
          type: 'fill',
          source: 'gbx_detail',
          'source-layer': srcLayer,
          paint: {{ 'fill-color': '#3388ff', 'fill-opacity': 0.5 }},
        }});
      }}).catch(() => {{
        // Raster PMTiles — try as a raster layer instead.
        if (map.getSource('gbx_detail')) {{
          map.removeSource('gbx_detail');
        }}
        map.addSource('gbx_detail', {{ type: 'raster', url: 'pmtiles://gbx_detail' }});
        map.addLayer({{
          id: 'gbx_detail-fill',
          type: 'raster',
          source: 'gbx_detail',
          paint: {{ 'raster-opacity': 0.8 }},
        }});
      }});
    }} catch(e) {{
      console.warn('[gbx dynamic] failed to load detail tiles:', e);
    }}
  }});
}}
export default {{ render }};
"""


# ---------------------------------------------------------------------------
# Slippy-map tile → geographic bbox helper
# ---------------------------------------------------------------------------


def _tile_bbox(z: int, x: int, y: int) -> tuple:
    """Return the geographic bounding box of slippy-map tile ``(z, x, y)``.

    Uses the standard Web Mercator / OSM tile formula:
    https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames#Tile_numbers_to_lon./lat.

    Returns:
        ``(min_lon, min_lat, max_lon, max_lat)`` in WGS-84 degrees.
    """
    import math

    n = 2**z
    min_lon = x / n * 360.0 - 180.0
    max_lon = (x + 1) / n * 360.0 - 180.0

    def _tile_lat(tile_y: int) -> float:
        lat_rad = math.atan(math.sinh(math.pi * (1.0 - 2.0 * tile_y / n)))
        return math.degrees(lat_rad)

    max_lat = _tile_lat(y)
    min_lat = _tile_lat(y + 1)
    return (min_lon, min_lat, max_lon, max_lat)


# ---------------------------------------------------------------------------
# Widget class
# ---------------------------------------------------------------------------


def _build_dynamic_widget(
    layers,
    *,
    simplify_tiles_spec: Optional[dict],
    on_viewport: Optional[Callable],
    center: Optional[list],
    zoom: Optional[int],
    prefetch: bool = True,
) -> anywidget.AnyWidget:
    """Internal factory — builds the AnyWidget subclass and wires the comm."""
    from databricks.labs.gbx.vizx._layers import as_layers
    from databricks.labs.gbx.vizx._maplibre import _pmtiles_register_js

    lyrs = as_layers(layers)
    spec = normalize_spec(simplify_tiles_spec)
    max_z = spec["max_z"]
    _center = center if center is not None else [-122.43, 37.77]
    _zoom = zoom if zoom is not None else 11

    # Build the overview PMTiles archive from each layer via simplify_tiles_from_source,
    # then run layer_to_sources_layers on the resulting pmtiles_layer so the HTML
    # builder path (sources/layers JSON + pm sidecar) is reused verbatim.
    from databricks.labs.gbx.vizx._layers import pmtiles_layer as _pmtiles_layer

    overview_sources: dict = {}
    overview_layers: list = []
    pm_register_snippets: list[str] = []
    # Keep a reference to the first vector/raster source for the default on_viewport.
    first_source = None

    for idx, layer in enumerate(lyrs):
        kind = getattr(layer, "kind", None)
        if kind in ("vector", "grid"):
            if first_source is None:
                from databricks.labs.gbx.vizx._maplibre import _gdf_for

                first_source = _gdf_for(layer).to_crs(4326)
            overview_bytes = simplify_tiles_from_source(
                first_source if first_source is not None else layer.data,
                spec=spec,
            )
            pm_layer = _pmtiles_layer(overview_bytes, label=layer.label)
        elif kind == "pmtiles":
            # Already a PMTiles layer — embed it as-is (no re-tiling).
            pm_layer = layer
        else:
            # Raster: simplify via the raster path.
            overview_bytes = simplify_tiles_from_source(layer.data, spec=spec)
            pm_layer = _pmtiles_layer(overview_bytes, label=layer.label)

        entry = layer_to_sources_layers(pm_layer, idx)
        s, ls, _eb = entry
        for sid, sdef in s.items():
            if "_gbx_pmtiles" in sdef:
                pm_info = sdef["_gbx_pmtiles"]
                clean = {k: v for k, v in sdef.items() if k != "_gbx_pmtiles"}
                overview_sources[sid] = clean
                pm_register_snippets.append(_pmtiles_register_js(sid, pm_info))
            else:
                overview_sources[sid] = sdef
        overview_layers.extend(ls)

    # Escape overview sources/layers for safe JS embedding.
    overview_sources_json = _json_for_script(overview_sources)
    overview_layers_json = _json_for_script(overview_layers)
    pm_register_js = "".join(pm_register_snippets)

    esm = _build_esm(
        overview_sources_json,
        overview_layers_json,
        pm_register_js,
        max_z=max_z,
        center=_center,
        zoom=_zoom,
    )

    # Resolve the on_viewport callback (default: tile from first source).
    if on_viewport is not None:
        _callback = on_viewport
    else:
        _callback = _default_on_viewport(first_source, max_z, spec)

    # Build the AnyWidget subclass dynamically (traitlets require class-level
    # declarations; we create a one-shot class here to carry the ESM and traits).
    _esm_val = esm
    _max_z_val = max_z

    class _GbxDynamicWidget(anywidget.AnyWidget):
        # Plain str — anywidget sees has_trait("_esm") == False and registers it
        # as a sync=True trait automatically.  A traitlets.Unicode declaration
        # here causes anywidget to skip that step → _esm never reaches the JS
        # frontend and the widget renders blank.
        _esm = _esm_val
        detail = traitlets.Unicode("").tag(sync=True)

    widget = _GbxDynamicWidget()

    # --- Cache + prefetch setup ---
    _cache = _TileCache()

    # Prefetch always uses the default tiler (never the user's custom on_viewport),
    # so custom callbacks are called only for real viewport requests.
    _default_callback = _default_on_viewport(first_source, max_z, spec)

    def _tiler_for_prefetch(z: int, x: int, y: int) -> bytes:
        # Compute the geographic bbox for this tile so each neighbor gets
        # its own spatially-clipped archive (not a full-source re-tile).
        tile_bounds = _tile_bbox(z, x, y)
        return _default_callback(list(tile_bounds), float(z))

    _prefetch_worker: Optional[_PrefetchWorker] = (
        _PrefetchWorker(cache=_cache, tiler=_tiler_for_prefetch) if prefetch else None
    )

    def _handle_msg(widget_instance, content, buffers=None):
        """Python-side handler for JS model.send({bbox, zoom}) messages."""
        if not isinstance(content, dict):
            return
        bbox = content.get("bbox")
        zoom_val = content.get("zoom")
        if bbox is None or zoom_val is None:
            return
        # Seam gate: only act when zoom > max_z.
        if _viewport_payload(bbox=bbox, zoom=zoom_val, max_z=_max_z_val) is None:
            return

        # Derive a tile key from (zoom, bbox-centre) for cache lookup.
        # Use a simple integer zoom as the z coordinate.
        z = int(zoom_val)
        west, south, east, north = bbox
        cx = (west + east) / 2
        cy = (south + north) / 2
        # Convert lon/lat to tile XY at zoom z.
        import math

        lat_rad = math.radians(max(-85.0511, min(85.0511, cy)))
        n = 2**z
        tx = int((cx + 180.0) / 360.0 * n)
        ty = int(
            (1.0 - math.log(math.tan(lat_rad) + 1.0 / math.cos(lat_rad)) / math.pi)
            / 2.0
            * n
        )
        key = (z, tx, ty)

        # Cache-first: serve from cache on hit, prepare + cache on miss.
        cached = _cache.get(key)
        if cached is not None:
            result = cached
            _cache.put(key, result, viewed=True)
        else:
            result = _callback(bbox, zoom_val)
            if isinstance(result, (bytes, bytearray)):
                _cache.put(key, bytes(result), viewed=True)

        if isinstance(result, (bytes, bytearray)):
            widget_instance.detail = base64.b64encode(bytes(result)).decode("ascii")

        # Schedule prefetch of the neighbor ring in the background.
        if _prefetch_worker is not None:
            _prefetch_worker.on_viewport_served(z=z, x=tx, y=ty)

    widget.on_msg(_handle_msg)
    # Expose the handler under a test-accessible name for direct invocation in tests.
    widget._gbx_handle_msg = lambda content: _handle_msg(widget, content)

    return widget


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def plot_interactive_dynamic(
    layers,
    *,
    simplify_tiles_spec: Optional[dict] = None,
    on_viewport: Optional[Callable] = None,
    center: Optional[list] = None,
    zoom: Optional[int] = None,
    prefetch: bool = True,
    **kw: Any,
) -> anywidget.AnyWidget:
    """Render an AnyWidget-based dynamic zoom cut-over map.

    Embeds a simplified ``min_z..max_z`` PMTiles overview as the base MapLibre
    source.  When the user pans or zooms above ``max_z``, the widget's JS side
    sends the current viewport ``{bbox, zoom}`` to the Python kernel via
    ``model.send``; the Python ``on_msg`` handler calls ``on_viewport(bbox,
    zoom)``, base64s the returned PMTiles bytes into the ``detail`` synced
    trait, and the JS ``change:detail`` handler adds/replaces a detail source
    in the live map.

    The comm round-trip is proven on Serverless by Spike B and requires a live
    kernel (notebook / Databricks interactive).  The widget does not render
    meaningfully in a static GitHub view — use ``plot_interactive`` for a
    static-fallback-compatible view.

    Args:
        layers:               A list of :class:`~databricks.labs.gbx.vizx._layers.Layer`
                              or any input accepted by
                              :func:`~databricks.labs.gbx.vizx._layers.as_layers`.
        simplify_tiles_spec:  Optional spec dict (see
                              :func:`~databricks.labs.gbx.vizx._simplify.normalize_spec`).
                              ``max_z`` controls the seam between the overview
                              and the on-demand detail tier (default ``10``).
        on_viewport:          Optional ``(bbox: list[float], zoom: float) -> bytes``
                              callback.  Receives ``[west, south, east, north]``
                              and the current zoom; must return PMTiles bytes.
                              Default: tiles the first vector/grid/raster layer
                              from source via ``simplify_tiles_from_source`` at
                              ``min_z=max_z+1``.
        center:               ``[lon, lat]`` map centre (default
                              ``[-122.43, 37.77]``).
        zoom:                 Initial zoom level (default ``11``).
        prefetch:             Enable predictive tile prefetch (default ``True``).
                              When ``True``, a background daemon thread prepares the
                              8-neighbor ring around each served viewport tile into a
                              bounded LRU cache so panning to an adjacent tile is
                              served instantly.  Set to ``False`` to disable.
        **kw:                 Reserved for future keyword arguments (ignored).

    Returns:
        An :class:`anywidget.AnyWidget` instance.  Display it in a Databricks
        notebook cell by returning it as the last expression, or pass it to
        ``IPython.display.display(w)``.
    """
    return _build_dynamic_widget(
        layers,
        simplify_tiles_spec=simplify_tiles_spec,
        on_viewport=on_viewport,
        center=center,
        zoom=zoom,
        prefetch=prefetch,
    )
