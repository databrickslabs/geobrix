"""MapLibre GL per-layer adapters and HTML builder for the VizX interactive compositor.

``layer_to_sources_layers(layer, idx)`` converts one :class:`~databricks.labs.gbx.vizx._layers.Layer`
into the MapLibre GL ``sources`` dict, ``layers`` list, and ``embed_bytes`` integer
that ``build_html`` (Task 5) stitches together into a self-contained HTML viewer.

``build_html(prepared, *, basemap, center, zoom)`` assembles N per-layer outputs into
one self-contained HTML page with SRI-pinned ``<script>`` tags, a CARTO basemap,
and pmtiles protocol registration (embed or url mode per layer).

Dispatch by ``layer.kind``:

* ``"vector"`` / ``"grid"`` — inline ``geojson`` source reprojected to EPSG:4326;
  fill/line/circle sub-layers chosen by geometry type.
* ``"raster"`` — ``image`` source with 4-corner ``coordinates`` in lon/lat;
  PNG rendered via rasterio (decimated to ≤ ``raster_max_px``).
* ``"pmtiles"`` — ``raster|vector`` source with ``pmtiles://gbx{idx}`` URL plus a
  ``_gbx_pmtiles`` sidecar dict recording embed mode or remote URL; consumed and
  popped by the HTML builder.
"""

from __future__ import annotations

import base64
import io
import json
import warnings as _warnings
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from databricks.labs.gbx.vizx._layers import Layer

# ---------------------------------------------------------------------------
# embed-budget defaults (Serverless cell-output cap aware)
# ---------------------------------------------------------------------------
#
# GeoBrix vizx targets Databricks notebooks ONLY. On current Serverless notebooks
# (serverless environment 3 / DBR 17.0+) the per-cell output is capped at 10 MB
# by default and at most 20 MB via ``%set_cell_max_output_size_in_mb`` -- output
# over the cap is TRUNCATED, which silently breaks a base64-embedded interactive
# map.
#
# CRUCIAL: the cell counts MUCH more than the archive bytes. build_html base64-embeds
# the archive (archive x 4/3), and then Databricks' displayHTML wraps that whole page
# into its sandboxed iframe with further encoding -- so the cell payload is MEASURED at
# ~2-3.3x the build_html size, NOT a clean single (or double) base64. This was pinned by
# LIVE Serverless renders, not modeled (the (4/3)^2 ~= 1.78x guess was still too low):
#   - build_html 6.1 MB  -> EMBEDS at the raised 20 MB cap.
#   - build_html 13.1 MB -> TRUNCATED even at the raised 20 MB cap.
#
# The over-budget guard compares archive x _BASE64_INFLATION (the build_html size) to
# max_embed_mb, so max_embed_mb IS the build_html ceiling. Set it to the measured
# proven-safe build_html: ~6 MB at the raised 20 MB cap, and proportionally ~3 MB at the
# 10 MB default cap. Larger archives auto-fit via interactive_fit='downzoom' (the
# plot_pmtiles default), route to the static fallback, or should be staged at a URL.
DEFAULT_MAX_EMBED_MB: float = 3

# Base64 inflation factor for embedded payloads (3 raw bytes -> 4 ASCII chars).
_BASE64_INFLATION = 4.0 / 3.0

# Databricks Serverless caps cell output at 10 MB by default, 20 MB max (raised via
# the %set_cell_max_output_size_in_mb magic). vizx interactive embeds base64 into one
# cell, so when set_cell_max_output is on we raise the cap to its max and size the
# embed budget for it; otherwise we stay conservative for the 10 MB default cap.
CELL_OUTPUT_CAP_MAX_MB: int = 20
# Safe embed budget when the cap is raised to its 20 MB max. max_embed_mb is the
# build_html-size ceiling; the displayHTML iframe then inflates the cell payload to
# ~2-3.3x that (see above). Calibrated from measured renders: build_html 6.1 MB embeds,
# 13 MB truncates -> proven-safe ceiling ~6 MB. (Do NOT bump this without a fresh live
# render; 14 and 18 both embedded-then-truncated.)
MAX_EMBED_MB_CAP_RAISED: float = 6

# Default verbosity for the [vizx] status lines the plot entrypoints emit.
DEFAULT_DEBUG_MODE: int = 1

# ---------------------------------------------------------------------------
# emphasis styling defaults (interactive / MapLibre tier)
# ---------------------------------------------------------------------------
#
# ``emphasis="data"`` makes a newly-added data layer visually pop
# against the full-strength basemap; ``emphasis="blend"`` (default) reproduces the prior
# soft composite exactly. These are DEFAULTS only -- an explicit user style kwarg
# (color/opacity/width on the Layer) always wins, so the per-layer builders mark
# which emphasis-controlled paint keys they left at default and ``build_html``
# fills those (and only those) per the chosen emphasis.
_MAPLIBRE_EMPHASIS = {
    "data": {
        "fill_opacity": 0.85,  # firmer than blend's prior 0.8; pop is the outline
        "fill_outline_color": "#222222",
        "line_width": 2.0,
        "raster_opacity": 1.0,
    },
    "blend": {
        # EXACTLY prior behavior (the "current look" the user wants preserved):
        # vector_layer's factory fill-opacity was 0.8, raster default 1.0, no outline.
        "fill_opacity": 0.8,
        "fill_outline_color": None,  # no distinct outline (prior behavior)
        "line_width": 1.0,
        "raster_opacity": 1.0,
    },
}

# Sidecar key recorded on each built MapLibre layer dict: lists the emphasis-
# controlled paint properties the user did NOT set, so build_html may fill them.
_GBX_EMPHASIS = "_gbx_emphasis"


def _validate_emphasis(emphasis: str) -> str:
    if emphasis not in _MAPLIBRE_EMPHASIS:
        raise ValueError(f"emphasis must be 'data' or 'blend'; got {emphasis!r}")
    return emphasis


def _apply_emphasis_paint(layer_dict: dict, emphasis: str) -> dict:
    """Fill the emphasis-controlled paint keys this layer left at default.

    The per-layer builders record an ``_gbx_emphasis`` sidecar listing which
    paint keys are emphasis-defaultable (i.e. the user did not set them). This
    rewrites only those keys per *emphasis* and strips the sidecar. Keys the
    user set explicitly are left untouched.
    """
    pending = layer_dict.pop(_GBX_EMPHASIS, None)
    if not pending:
        return layer_dict
    vals = _MAPLIBRE_EMPHASIS[emphasis]
    paint = layer_dict.setdefault("paint", {})
    for key in pending:
        if key == "fill-opacity":
            paint["fill-opacity"] = vals["fill_opacity"]
        elif key == "fill-outline-color":
            if vals["fill_outline_color"] is not None:
                paint["fill-outline-color"] = vals["fill_outline_color"]
            else:
                paint.pop("fill-outline-color", None)
        elif key == "line-width":
            paint["line-width"] = vals["line_width"]
        elif key == "raster-opacity":
            paint["raster-opacity"] = vals["raster_opacity"]
    return layer_dict


def _emit(msg: str, *, level: int = 1, debug_mode: int = DEFAULT_DEBUG_MODE) -> None:
    """Print a ``[vizx]`` status line only when ``debug_mode >= level``.

    debug_mode: 0 = silent (no status lines; genuine warnings still fire),
    1 = concise notes (default), 2 = + diagnostics. ``level`` is the minimum
    debug_mode at which this message shows (1 = a normal note, 2 = a diagnostic).
    """
    if debug_mode >= level:
        print(msg)


def _resolve_embed_budget(max_embed_mb, set_cell_max_output: bool) -> float:
    """Resolve the effective embed budget in MB.

    An explicit ``max_embed_mb`` always wins. Otherwise the default tracks whether
    the Serverless cell-output cap will be raised: ``MAX_EMBED_MB_CAP_RAISED``
    (6 MB, sized for the 20 MB max cap) when ``set_cell_max_output`` is on, else
    the conservative ``DEFAULT_MAX_EMBED_MB`` (3 MB, safe for the 10 MB default cap).
    """
    if max_embed_mb is not None:
        return max_embed_mb
    return MAX_EMBED_MB_CAP_RAISED if set_cell_max_output else DEFAULT_MAX_EMBED_MB


# ---------------------------------------------------------------------------
# security helper
# ---------------------------------------------------------------------------


def _json_for_script(obj) -> str:
    """json.dumps escaped for safe embedding inside an HTML <script> block.

    json.dumps alone does NOT escape ``</script>``, ``<``, ``>``, or ``&``,
    so a crafted value in user data can break out of the script context.
    The Unicode escapes produced here are valid JSON/JS string escapes —
    ``JSON.parse`` and the JS engine see the original characters at runtime.
    """
    return (
        json.dumps(obj)
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("&", "\\u0026")
        .replace(" ", "\\u2028")
        .replace(" ", "\\u2029")
    )


# ---------------------------------------------------------------------------
# SRI-pinned CDN constants — hashes are finalised in Task 12.
# ---------------------------------------------------------------------------

_MAPLIBRE_JS = "https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.js"
_MAPLIBRE_JS_SRI = (
    "sha384-SYKAG6cglRMN0RVvhNeBY0r3FYKNOJtznwA0v7B5Vp9tr31xAHsZC0DqkQ/pZDmj"
)
_MAPLIBRE_CSS = "https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.css"
_MAPLIBRE_CSS_SRI = (
    "sha384-MinO0mNliZ3vwppuPOUnGa+iq619pfMhLVUXfC4LHwSCvF9H+6P/KO4Q7qBOYV5V"
)
_PMTILES_JS = "https://unpkg.com/pmtiles@3.2.0/dist/pmtiles.js"
_PMTILES_JS_SRI = (
    "sha384-QfbOCebHNw8pQiPAOd2IFee2v2A5VYZxBk0+JGZ5H+3mfzVIp6zsQNkTsfGJot93"
)
_CARTO_STYLE = "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"

_DEFAULT_RASTER_MAX_PX = 1024

# Cycled per source-layer for a multi-layer vector archive rendered without an
# explicit pmtiles_layer color, so each layer gets its own hue (blue/orange/teal/
# violet/rose/green).
_VECTOR_PALETTE = ["#1F6FB5", "#E04E2A", "#0F8E8B", "#6B4FA0", "#C2255C", "#2FA56A"]


# ---------------------------------------------------------------------------
# public entry point
# ---------------------------------------------------------------------------


def layer_to_sources_layers(
    layer,
    idx: int,
    *,
    raster_max_px: int = _DEFAULT_RASTER_MAX_PX,
    emphasis: str = "blend",
) -> tuple[dict, list[dict], int]:
    """Convert *layer* to MapLibre GL ``(sources, layers, embed_bytes)``.

    Args:
        layer:        A :class:`~databricks.labs.gbx.vizx._layers.Layer`.
        idx:          Integer index; drives the source key ``f"gbx{idx}"`` and
                      layer ids ``f"gbx{idx}-{type}"``.
        raster_max_px: Maximum pixel size (longest edge) for decimated raster PNG.
        emphasis:     ``"data"`` makes the data layer pop against the
                      basemap; ``"blend"`` (default) reproduces the prior soft composite.
                      Each built layer records which emphasis-controlled paint
                      keys the user did NOT set via a ``_gbx_emphasis`` sidecar;
                      :func:`build_html` fills those per its own ``emphasis``.

    Returns:
        ``(sources, layers, embed_bytes)`` where *sources* is a dict of MapLibre
        source entries, *layers* is a list of MapLibre layer dicts, and
        *embed_bytes* reports the driver-side payload (GeoJSON bytes for vector/grid,
        PNG bytes for raster, archive bytes for pmtiles embed mode, 0 for url mode).
    """
    _validate_emphasis(emphasis)
    kind = getattr(layer, "kind", None)
    if kind in ("vector", "grid"):
        return _vector_or_grid(layer, idx)
    if kind == "raster":
        return _raster(layer, idx, raster_max_px=raster_max_px)
    if kind == "pmtiles":
        return _pmtiles(layer, idx)
    raise ValueError(f"layer_to_sources_layers: unknown layer.kind={kind!r}")


# ---------------------------------------------------------------------------
# public HTML builder
# ---------------------------------------------------------------------------


def _fit_zoom_for_bounds(w, s, e, n, viewport_px=760):
    """Web-mercator zoom that frames the bounds ``[w, s, e, n]`` (lon/lat degrees) in a
    ~``viewport_px`` square, computed WITHOUT the map container.

    build_html opens the auto-view via an explicit center+zoom rather than MapLibre's
    container-measured fitBounds (unreliable in a notebook iframe — see build_html). This
    derives that opening zoom from the data extent alone: fit the more constraining of the
    lon/lat spans into the viewport with a small padding margin. Degenerate extent -> a
    sensible mid zoom.
    """
    import math

    def _merc_y_frac(lat):
        lat = max(min(float(lat), 85.05112878), -85.05112878)
        return (
            math.log(math.tan(math.pi / 4 + math.radians(lat) / 2)) / (2 * math.pi)
            + 0.5
        )

    x_frac = abs(float(e) - float(w)) / 360.0
    y_frac = abs(_merc_y_frac(n) - _merc_y_frac(s))
    frac = max(x_frac, y_frac)
    if frac <= 0:
        return 11.0
    # World at zoom z is 256*2^z px wide; fit `frac` of the world into viewport_px with a
    # 0.9 padding margin so the extent doesn't touch the edges.
    z = math.log2(viewport_px * 0.9 / (256.0 * frac))
    return max(0.0, min(22.0, z))


def _auto_view_from_pmtiles(pm_pairs):
    """Derive an opening view from the first EMBEDDED pmtiles archive's header.

    Returns ``(bounds, min_zoom, max_zoom, center)`` where ``bounds`` is
    ``[w, s, e, n]`` (for MapLibre ``fitBounds`` — frames the whole data extent
    rather than guessing a fixed zoom). ``min_zoom``/``max_zoom`` are the archive's
    zoom range: the opening view MUST stay within it — MapLibre does not render a
    raster/vector source below its min zoom (opening wider than the archive's coarsest
    level shows a blank map), so ``fitBounds`` is clamped to ``[min_zoom, max_zoom]``.
    ``center`` is the header centre, kept as a fallback when bounds are absent.

    Returns ``(None, None, None, None)`` when no embedded archive is inspectable
    (e.g. url-only) so the caller keeps its default. Parse failures are swallowed.
    """
    from databricks.labs.gbx.pmtiles import pmtiles_info

    for _sid, info in pm_pairs:
        raw = info.get("bytes")
        if not raw:
            continue  # url mode (no local bytes to inspect)
        try:
            hdr = pmtiles_info(raw)
        except Exception:  # noqa: BLE001 — best-effort; fall back to caller default
            continue
        b = hdr.get("bounds")
        bounds = (
            [float(b[0]), float(b[1]), float(b[2]), float(b[3])]
            if b and len(b) >= 4
            else None
        )
        c = hdr.get("center")
        center = [c[0], c[1]] if c and len(c) >= 2 else None
        mn, mx = hdr.get("min_zoom"), hdr.get("max_zoom")
        min_zoom = int(mn) if mn is not None else None
        max_zoom = int(mx) if mx is not None else min_zoom
        return bounds, min_zoom, max_zoom, center
    return None, None, None, None


def build_html(
    prepared,
    *,
    basemap: str = "carto-positron",
    center=None,
    zoom=None,
    emphasis: str = "blend",
) -> str:
    """Assemble N per-layer adapter outputs into one self-contained HTML viewer.

    Args:
        prepared: A list of ``(sources, layers, embed_bytes)`` tuples as returned
                  by :func:`layer_to_sources_layers`.  The ``sources`` dicts are
                  mutated in-place: any ``_gbx_pmtiles`` sidecar key is popped
                  before the source is serialised into the MapLibre style.
        basemap:  ``"carto-positron"`` (default) uses the CARTO Positron style as
                  the base layer.  Pass ``"none"`` to render a blank dark canvas.
        center:   ``[lon, lat]`` map centre (default San Francisco ``[-122.43, 37.77]``).
        zoom:     Initial zoom level (default ``11``).
        emphasis: ``"data"`` fills the emphasis-controlled paint keys so
                  data layers pop against the basemap; ``"blend"`` (default) reproduces the
                  prior soft composite. Per-layer ``_gbx_emphasis`` sidecars name
                  the keys the user left at default — only those are filled, so
                  explicit user style kwargs always win.

    Returns:
        A self-contained HTML string with inline ``<script>`` tags, SRI hashes,
        and base64-embedded PMTiles archives (or URL-referenced ones).
    """
    _validate_emphasis(emphasis)
    # Per-map id: namespaces the container div, the embedded-archive protocol keys, and
    # an IIFE scope so MULTIPLE vizx maps in one document (e.g. §7 then §8) don't collide
    # -- without it the second map's top-level `const proto` re-declares the first's
    # ("Identifier 'proto' has already been declared"), both target #gbx-map, and the
    # shared pmtiles registry keys (gbx0) clash.
    import uuid

    uid = uuid.uuid4().hex[:10]

    sources: dict = {}
    layers: list = []
    pm_pairs: list[tuple[str, dict]] = []

    legends: list[dict] = []
    for s, ls, *_rest in prepared:
        for sid, sdef in s.items():
            # Collect a data-driven colormap legend (vector/grid), then strip the sidecar
            # from every source copy below — MapLibre rejects unknown source keys.
            leg = sdef.get("_gbx_legend")
            if leg:
                legends.append(leg)
            if "_gbx_pmtiles" in sdef:
                # Read sidecar WITHOUT mutating the caller's dict — Task 6 calls
                # build_html twice (size-check then real build) and must find the
                # sidecar intact on the second call.
                info_sc = sdef["_gbx_pmtiles"]
                pm_pairs.append((sid, info_sc))
                # Build a clean copy for the overlay (MapLibre rejects unknown keys).
                clean = {
                    k: v
                    for k, v in sdef.items()
                    if k not in ("_gbx_pmtiles", "_gbx_legend")
                }
                # Embedded archives register under a per-map key (uid_sid) on a shared
                # protocol, so point this source's URL at that key. URL mode keeps the
                # remote URL as its key (already unique).
                if info_sc.get("mode") == "embed":
                    clean["url"] = f"pmtiles://{uid}_{sid}"
                sources[sid] = clean
            else:
                sources[sid] = {k: v for k, v in sdef.items() if k != "_gbx_legend"}
        # Apply emphasis to each layer's emphasis-controlled paint keys, then strip
        # the _gbx_emphasis sidecar (MapLibre rejects unknown layer keys). Copy so
        # the caller's prepared dicts stay intact across repeated build_html calls
        # (prepare_layers/audit_layers call build_html for measurement first).
        for ly in ls:
            if _GBX_EMPHASIS in ly:
                ly = {
                    **ly,
                    "paint": dict(ly.get("paint", {})),
                    _GBX_EMPHASIS: ly[_GBX_EMPHASIS],
                }
                _apply_emphasis_paint(ly, emphasis)
            layers.append(ly)

    if basemap and basemap != "none":
        base_js = f'"{_CARTO_STYLE}"'
    else:
        base_js = "{version:8,sources:{},layers:[]}"

    overlay_json = _json_for_script({"sources": sources, "layers": layers})
    pm_js = "".join(_pmtiles_register_js(sid, info, uid) for sid, info in pm_pairs)

    # Open ON the data. When the caller passes neither center nor zoom, FIT the first
    # embedded archive's bounds (frames the whole extent) instead of guessing a fixed
    # zoom -- clamped to [min_zoom, max_zoom] because MapLibre does not render a source
    # below its min zoom (opening wider than the coarsest level = a blank map). An
    # explicit center/zoom from the caller wins; the SF default is the last resort when
    # no embedded archive is inspectable (e.g. url-only).
    auto_bounds = auto_min_zoom = auto_max_zoom = auto_center = None
    if center is None and zoom is None:
        auto_bounds, auto_min_zoom, auto_max_zoom, auto_center = (
            _auto_view_from_pmtiles(pm_pairs)
        )

    # Both view modes open via an EXPLICIT center+zoom (constructor + a jumpTo once the
    # container is measured). We deliberately do NOT use MapLibre's constructor `bounds`
    # or fitBounds: both measure the container, which inside a notebook iframe is 0-sized
    # at construction and unreliable even on load — a container-measured fit then collapses
    # to minZoom (a globe when min zoom is low) or leaves the map blank. An explicit zoom
    # (auto-derived from the archive extent, container-INDEPENDENTLY, for the auto-view
    # path) opens framed regardless of when the container is measured.
    if center is None and zoom is None and auto_bounds is not None:
        w, s, e, n = auto_bounds
        # minZoom floor keeps a wide drag from underzooming into blank tiles; maxZoom cap
        # keeps a tiny AOI from opening over-zoomed past the archive's detail.
        min_z = auto_min_zoom if auto_min_zoom is not None else 0
        max_z = auto_max_zoom if auto_max_zoom is not None else 22
        eff_center = (
            auto_center if auto_center is not None else [(w + e) / 2, (s + n) / 2]
        )
        eff_zoom = max(min_z, min(max_z, _fit_zoom_for_bounds(w, s, e, n)))
        init_view_js = f"center: {_json_for_script(eff_center)}, zoom: {eff_zoom}, minZoom: {min_z}"
        load_view_js = f"map.jumpTo({{ center: {_json_for_script(eff_center)}, zoom: {eff_zoom} }});"
    else:
        eff_center = center if center is not None else [-122.43, 37.77]
        eff_zoom = zoom if zoom is not None else 11
        init_view_js = f"center: {_json_for_script(eff_center)}, zoom: {eff_zoom}"
        load_view_js = f"map.jumpTo({{ center: {_json_for_script(eff_center)}, zoom: {eff_zoom} }});"

    container = f"gbx-map-{uid}"
    legend_html = _legend_html(legends)
    return f"""\
<div style="position:relative">
<div id="{container}" style="height:720px"></div>
{legend_html}
</div>
<link href="{_MAPLIBRE_CSS}" rel="stylesheet" integrity="{_MAPLIBRE_CSS_SRI}" crossorigin="anonymous"/>
<script src="{_MAPLIBRE_JS}" integrity="{_MAPLIBRE_JS_SRI}" crossorigin="anonymous"></script>
<script src="{_PMTILES_JS}" integrity="{_PMTILES_JS_SRI}" crossorigin="anonymous"></script>
<script>
(function() {{
  // ONE shared pmtiles protocol per document, registered once: multiple vizx maps in
  // the same document add their archives (under per-map keys) to the same registry, so
  // the global 'pmtiles' protocol resolves every map's tiles. Everything else lives in
  // this IIFE so each map's locals (map, overlay, _b*) never collide across maps.
  const proto = window.__gbxPmtilesProto
    || (window.__gbxPmtilesProto = new pmtiles.Protocol());
  if (!window.__gbxPmtilesRegistered) {{
    maplibregl.addProtocol('pmtiles', proto.tile.bind(proto));
    window.__gbxPmtilesRegistered = true;
  }}
  {pm_js}
  const map = new maplibregl.Map({{
    container: '{container}',
    style: {base_js},
    {init_view_js}
  }});
  const overlay = {overlay_json};
  // Frame + size the canvas only once the container has REAL dimensions. In a notebook
  // iframe the map is built before layout (container 0-sized), so we defer resize()+
  // framing until the container is measured — via map load AND a ResizeObserver, since
  // Databricks often sizes the output iframe after the map is constructed.
  let _gbxFramed = false;
  const _gbxApplyView = () => {{
    const c = map.getContainer();
    if (_gbxFramed || !c || c.clientWidth === 0 || c.clientHeight === 0) {{
      map.resize();
      return;
    }}
    _gbxFramed = true;
    map.resize();
    {load_view_js}
  }};
  map.on('load', () => {{
    for (const [sid, sdef] of Object.entries(overlay.sources)) {{
      map.addSource(sid, sdef);
    }}
    for (const ly of overlay.layers) {{
      map.addLayer(ly);
    }}
    _gbxApplyView();
  }});
  if (typeof ResizeObserver !== 'undefined') {{
    new ResizeObserver(_gbxApplyView).observe(map.getContainer());
  }}
}})();
</script>"""


# ---------------------------------------------------------------------------
# budget ladder
# ---------------------------------------------------------------------------


def _simplify_layer(layer, spec: dict, max_embed_mb: float):
    """Route the layer through the appropriate simplify engine and return a pmtiles_layer.

    Existing PMTiles archives are reduced with the binary-free ``autofit_archive``
    down-zoom (drop the highest zoom levels until the base64-rendered archive fits
    ``max_embed_mb``) — no tippecanoe, no tile-join. Source-carrying layers
    (vector/grid/raster) re-tile from source via ``simplify_tiles_from_source``.
    """
    from databricks.labs.gbx.vizx._layers import pmtiles_layer as _pmtiles_layer

    kind = getattr(layer, "kind", None)

    if kind == "pmtiles":
        # Existing archive: reduce it to the embed budget by dropping the highest
        # (densest) zoom levels. Binary-free, tier-agnostic (raster or vector tiles).
        data = layer.data
        if isinstance(data, (bytes, bytearray)):
            raw = bytes(data)
        elif isinstance(data, str) and not (
            data.startswith("http://") or data.startswith("https://")
        ):
            import pathlib

            raw = pathlib.Path(data).read_bytes()
        else:
            # URL mode — can't simplify a remote archive; return as-is.
            return layer

        from databricks.labs.gbx.vizx._pmtiles_autofit import autofit_archive

        reduced, _report = autofit_archive(raw, max_embed_mb=max_embed_mb)
        return _pmtiles_layer(reduced, label=layer.label)
    else:
        # vector/grid/raster layer carrying SOURCE data.
        # For vector/grid, extract the GeoDataFrame; for raster, pass layer.data directly.
        from databricks.labs.gbx.vizx._simplify import simplify_tiles_from_source

        if kind in ("vector", "grid"):
            source = _gdf_for(layer).to_crs(4326)
        else:
            source = layer.data
        simplified = simplify_tiles_from_source(source, spec=spec)
        return _pmtiles_layer(simplified, label=layer.label)


def _pmtiles_is_url(layer) -> bool:
    """Return True when the pmtiles layer carries an explicit http(s) URL."""
    data = getattr(layer, "data", None)
    if not isinstance(data, str):
        return False
    return data.startswith("http://") or data.startswith("https://")


def _layer_label(layer, idx: int) -> str:
    """Return a human-readable label for *layer* (for warning messages)."""
    lbl = getattr(layer, "label", None)
    if lbl:
        return repr(lbl)
    kind = getattr(layer, "kind", "unknown")
    return f"layer[{idx}] ({kind})"


def _decode_pmtiles_for_static(layer) -> "Layer":
    """Convert a pmtiles Layer into a raster_layer or vector_layer for plot_static.

    Reads the raw archive bytes, calls the appropriate static fallback renderer
    to produce a decoded representation, but rather than *rendering* it here we
    return a Layer that plot_static can accept.

    For raster pmtiles: mosaic the finest zoom that fits a tile budget →
        raster_layer(ndarray) (finest level preserves source contrast/detail).
    For vector pmtiles: decode the coarsest (min) zoom → vector_layer(GeoDataFrame).

    Raises ValueError when the archive contains no renderable tiles.
    """
    from databricks.labs.gbx.pmtiles import pmtiles_info
    from databricks.labs.gbx.vizx._layers import raster_layer, vector_layer
    from databricks.labs.gbx.vizx._pmtiles import (
        MemorySource,
        _decode_mvt_to_geoms,
        _is_raster_type,
        _maybe_gunzip,
        all_tiles,
    )

    data = layer.data
    if isinstance(data, (bytes, bytearray)):
        raw = bytes(data)
    elif isinstance(data, str):
        with open(data, "rb") as f:
            raw = f.read()
    else:
        raise TypeError(
            f"_decode_pmtiles_for_static: unsupported data type {type(data).__name__!r}"
        )

    info = pmtiles_info(raw)
    if _is_raster_type(info["tile_type"]):
        # Build an overview image from the FINEST zoom that fits a tile budget: web-map
        # tiles are images (PNG/JPEG/WEBP, possibly gzip-wrapped), NOT georeferenced
        # rasters, so they can't go to plot_cog/rasterio. Mosaic the chosen level's tiles
        # by their (x, y) grid position, then crop to the non-transparent data extent so a
        # small AOI doesn't render as a speck in the corner of one coarse tile. plot_static
        # imshows the resulting ndarray.
        #
        # Why the finest (not coarsest) zoom: unlike vector tiles — where every level
        # re-encodes the same features so the coarsest is sufficient — raster pyramid
        # levels are independently resampled. The coarsest (min-zoom) overview averages
        # many source pixels into few, which lowers contrast and lightens the image (the
        # NB02 "washed-out" basemap). The finest level preserves the source imagery's
        # contrast and detail. For a bounded AOI the finest level is only a handful of
        # tiles; we cap the count and step down to a coarser level if it would be too
        # large, so a wide-area archive still renders a sane overview.
        import io

        import numpy as np
        from PIL import Image

        # Tile counts per zoom; pick the highest zoom whose count is within budget.
        _MAX_OVERVIEW_TILES = 256
        per_zoom: dict = {}
        for (z, x, y), _payload in all_tiles(MemorySource(raw)):
            per_zoom.setdefault(z, 0)
            per_zoom[z] += 1
        if not per_zoom:
            raise ValueError(
                "prepare_layers static fallback: raster pmtiles archive has no tiles"
            )
        zooms_desc = sorted(per_zoom, reverse=True)
        target_z = next(
            (z for z in zooms_desc if per_zoom[z] <= _MAX_OVERVIEW_TILES),
            zooms_desc[-1],  # all levels exceed budget -> coarsest is the smallest
        )

        placed = []
        for (z, x, y), payload in all_tiles(MemorySource(raw)):
            if z != target_z:
                continue
            img = Image.open(io.BytesIO(_maybe_gunzip(payload))).convert("RGBA")
            placed.append((x, y, np.asarray(img)))
        if not placed:
            raise ValueError(
                "prepare_layers static fallback: raster pmtiles archive has no tiles"
            )
        th, tw = placed[0][2].shape[:2]
        xs = [p[0] for p in placed]
        ys = [p[1] for p in placed]
        minx, maxx, miny, maxy = min(xs), max(xs), min(ys), max(ys)
        canvas = np.zeros(((maxy - miny + 1) * th, (maxx - minx + 1) * tw, 4), np.uint8)
        for x, y, a in placed:
            r0, c0 = (y - miny) * th, (x - minx) * tw
            canvas[r0 : r0 + th, c0 : c0 + tw] = a[:th, :tw]
        alpha = canvas[..., 3]
        rows, cols = np.any(alpha > 0, axis=1), np.any(alpha > 0, axis=0)
        if rows.any() and cols.any():
            r = np.where(rows)[0][[0, -1]]
            c = np.where(cols)[0][[0, -1]]
            canvas = canvas[r[0] : r[1] + 1, c[0] : c[1] + 1]
        return raster_layer(canvas, opacity=layer.opacity)
    else:
        # Vector (MVT): decode the COARSEST zoom that actually has features. A normal
        # pyramid (e.g. gbx_st_asmvt_pyramid) carries every feature at every zoom, so the
        # min zoom suffices and decoding just one level avoids re-rendering N_levels x the
        # geometries (a 171k-feature z12-z16 archive took minutes). But a tippecanoe
        # drop_densest OVERVIEW drops most/all features at the lowest zooms, so the min
        # zoom can decode to nothing -> scan UPWARD and stop at the first non-empty zoom
        # (still cheap: coarse zooms have few tiles, and we stop as soon as features appear).
        from collections import defaultdict

        import geopandas as gpd

        tiles_by_z: dict = defaultdict(list)
        for (z, x, y), payload in all_tiles(MemorySource(raw)):
            tiles_by_z[z].append((x, y, payload))

        # A drop_densest overview drops most features at low zooms, so the min zoom can
        # be empty or near-empty. Scan zooms ascending and use the COARSEST one that is
        # populated enough to read as an overview (>= _STATIC_MIN_FEATURES); fall back to
        # the richest zoom seen if none clears the bar. One zoom only -- a normal pyramid
        # repeats features at every level, so we must never sum across zooms (that would
        # render N_levels x the geometries). A raw min_zoom=12 archive hits its first
        # (coarsest) zoom already populated, so this keeps the prior fast path for it.
        _STATIC_MIN_FEATURES = 50
        geoms, rows = [], []
        for z in sorted(tiles_by_z):
            zg, zr = [], []
            for x, y, payload in tiles_by_z[z]:
                for geom, props in _decode_mvt_to_geoms(payload, z, x, y):
                    zg.append(geom)
                    zr.append(props)
            if len(zg) > len(geoms):  # track the richest zoom seen
                geoms, rows = zg, zr
            if len(zg) >= _STATIC_MIN_FEATURES:
                break  # coarsest sufficiently-populated zoom -> stop (don't decode finer)
        if not geoms:
            raise ValueError(
                "prepare_layers static fallback: vector pmtiles archive decoded to no geometries"
            )
        gdf = gpd.GeoDataFrame(rows, geometry=geoms, crs=4326)
        return vector_layer(
            gdf, opacity=layer.opacity, color=layer.color, label=layer.label
        )


def _pmtiles_raw_bytes(layer):
    """Return the raw archive bytes for an embedded pmtiles layer, or None for url mode."""
    data = getattr(layer, "data", None)
    if isinstance(data, (bytes, bytearray)):
        return bytes(data)
    if isinstance(data, str) and not (
        data.startswith("http://") or data.startswith("https://")
    ):
        try:
            with open(data, "rb") as f:
                return f.read()
        except OSError:
            return None
    return None  # url mode or unrecognised


def _layer_audit_entry(layer, idx: int, embed_bytes: int) -> dict:
    """Return the per-layer dict for the audit structure."""
    label = getattr(layer, "label", None) or f"layer[{idx}]"
    kind = getattr(layer, "kind", "unknown")
    entry: dict = {"label": label, "kind": kind, "embed_bytes": embed_bytes}

    # For embedded pmtiles archives, report the max single-tile size.
    if kind == "pmtiles":
        raw = _pmtiles_raw_bytes(layer)
        if raw is not None:
            max_tile = _max_tile_bytes(raw)
            entry["max_tile_bytes"] = max_tile
        else:
            entry["max_tile_bytes"] = None
    else:
        entry["max_tile_bytes"] = None

    return entry


def _max_tile_bytes(raw: bytes) -> int | None:
    """Return the maximum single-tile decompressed byte size in a pmtiles archive.

    Iterates the archive's actual tiles via ``all_tiles``.
    Returns ``None`` if the archive contains no tiles or cannot be parsed.
    """
    try:
        import gzip

        from pmtiles.reader import MemorySource, all_tiles

        max_size = 0
        found = False
        for (z, x, y), payload in all_tiles(MemorySource(raw)):
            found = True
            data = payload
            if data[:2] == b"\x1f\x8b":
                try:
                    data = gzip.decompress(data)
                except Exception:
                    pass
            max_size = max(max_size, len(data))
        return max_size if found else None
    except Exception:
        return None


def _build_audit(
    layers, prepared, total_embed_bytes, max_embed_bytes, simplify_tiles_spec
) -> dict:
    """Build the audit dict from layers + their prepared (sources, layers, embed_bytes) entries."""
    audit_layers_list = []
    for idx, layer in enumerate(layers):
        entry = prepared[idx] if idx < len(prepared) else None
        eb = entry[2] if (entry is not None and len(entry) > 2) else 0
        audit_layers_list.append(_layer_audit_entry(layer, idx, eb))

    fits = total_embed_bytes <= max_embed_bytes

    # Determine verdict.
    # "url"      — all over-budget pmtiles have an http(s) URL (zero embed cost).
    # "embed"    — total fits within budget.
    # "simplify" — over budget but a simplify spec / layer.simplify is present.
    # "static"   — over budget, no simplify path.
    if fits:
        verdict = "embed"
    else:
        # "url" — at least one pmtiles layer exists AND every pmtiles layer is
        # url-mode (no archive needs embedding; non-pmtiles layers are irrelevant).
        pmtiles_layers = [
            lyr for lyr in layers if getattr(lyr, "kind", None) == "pmtiles"
        ]
        all_url = bool(pmtiles_layers) and all(
            _pmtiles_is_url(lyr) for lyr in pmtiles_layers
        )
        if all_url:
            verdict = "url"
        elif simplify_tiles_spec or any(
            getattr(layer, "simplify", None) for layer in layers
        ):
            verdict = "simplify"
        else:
            verdict = "static"

    return {
        "layers": audit_layers_list,
        "total_embed_bytes": total_embed_bytes,
        "max_embed_bytes": max_embed_bytes,
        "fits": fits,
        "verdict": verdict,
    }


def audit_layers(
    layers,
    *,
    max_embed_mb: float = DEFAULT_MAX_EMBED_MB,
    simplify_tiles_spec=None,
    emphasis: str = "blend",
) -> dict:
    """Dry pre-flight embed-size audit — no render, no displayHTML.

    Coerces *layers* via :func:`~databricks.labs.gbx.vizx._layers.as_layers`,
    prepares each layer (without simplification), measures the assembled-HTML
    size, and returns an audit dict:

    .. code-block:: python

        {
            "layers": [
                {"label": str, "kind": str, "embed_bytes": int, "max_tile_bytes": int | None},
                ...
            ],
            "total_embed_bytes": int,   # len(build_html(prepared).encode())
            "max_embed_bytes": int,     # int(max_embed_mb * 1_048_576)
            "fits": bool,               # total_embed_bytes <= max_embed_bytes
            "verdict": "embed" | "simplify" | "url" | "static",
        }

    ``total_embed_bytes`` is the authoritative embed measure — the actual
    assembled-HTML byte length that the browser must load.  ``max_tile_bytes``
    is the gzip-decompressed max single-tile size for pmtiles archive layers
    (``None`` for other layer kinds and url-mode pmtiles).

    ``verdict`` interpretation:

    - ``"embed"``    — total fits within budget; inline embed is viable.
    - ``"url"``      — all pmtiles layers are remote http(s) URLs (zero embed cost).
    - ``"simplify"`` — over budget but a ``simplify_tiles_spec`` or per-layer
                       ``simplify`` dict is present to reduce the payload.
    - ``"static"``   — over budget with no simplify path; only static render viable.

    Args:
        layers:              A list of :class:`~databricks.labs.gbx.vizx._layers.Layer`
                             or any input accepted by
                             :func:`~databricks.labs.gbx.vizx._layers.as_layers`.
        max_embed_mb:        HTML size threshold in mebibytes (default ``DEFAULT_MAX_EMBED_MB`` = 8).
        simplify_tiles_spec: Optional spec dict; its presence drives
                             ``verdict="simplify"`` when the budget is exceeded.

    Returns:
        Audit dict as described above.
    """
    from databricks.labs.gbx.vizx._layers import as_layers

    lyrs = as_layers(layers) if not isinstance(layers, list) else layers
    max_embed_bytes = int(max_embed_mb * 1_048_576)

    # Prepare each layer without triggering simplification or fallback.
    prepared: list = []
    for idx, layer in enumerate(lyrs):
        kind = getattr(layer, "kind", None)
        if kind == "pmtiles" and _pmtiles_is_url(layer):
            entry = layer_to_sources_layers(layer, idx, emphasis=emphasis)
            prepared.append(entry)
            continue
        if kind == "pmtiles":
            raw = _pmtiles_raw_bytes(layer)
            # Compare the RENDERED embed size (base64-inflated ~4/3x), not the raw
            # archive bytes — the Serverless cell-output cap counts rendered HTML.
            if raw is not None and len(raw) * _BASE64_INFLATION > max_embed_bytes:
                # Over-budget before even assembling HTML — record zero embed bytes
                # in the prepared list (won't be included in build_html).
                prepared.append(None)
                continue
        try:
            entry = layer_to_sources_layers(layer, idx, emphasis=emphasis)
            prepared.append(entry)
        except Exception:
            prepared.append(None)

    valid_prepared = [e for e in prepared if e is not None]
    if valid_prepared:
        total_embed_bytes = len(build_html(valid_prepared, emphasis=emphasis).encode())
    else:
        # All layers were over-budget individually — estimate the RENDERED embed
        # size as the base64-inflated (~4/3x) sum of raw archive bytes, matching
        # the per-layer guard above (the cell-output cap counts rendered HTML).
        total_embed_bytes = int(
            sum(
                len(_pmtiles_raw_bytes(layer) or b"")
                for layer in lyrs
                if getattr(layer, "kind", None) == "pmtiles"
            )
            * _BASE64_INFLATION
        )

    return _build_audit(
        lyrs, prepared, total_embed_bytes, max_embed_bytes, simplify_tiles_spec
    )


def _autofit_pmtiles_layer(layer, per_pmtiles_mb: float):
    """Down-zoom an embedded pmtiles layer to fit ``per_pmtiles_mb`` (its budget share).

    Returns ``(layer, reduced)``. A no-op returning ``(layer, False)`` when the archive
    already fits its share, isn't an embeddable local archive, or autofit fails (the
    caller then lets the over-budget gate decide). On a real reduction returns a fresh
    ``pmtiles_layer`` wrapping the down-zoomed bytes and ``True``.
    """
    raw = _pmtiles_raw_bytes(layer)
    if raw is None or len(raw) * _BASE64_INFLATION <= per_pmtiles_mb * 1_048_576:
        return layer, False
    from databricks.labs.gbx.vizx._layers import pmtiles_layer as _pml
    from databricks.labs.gbx.vizx._pmtiles_autofit import autofit_archive

    try:
        reduced, report = autofit_archive(raw, max_embed_mb=per_pmtiles_mb)
    except Exception:  # noqa: BLE001 — fall through to the over-budget gate
        return layer, False
    if not report["dropped_zooms"]:
        return layer, False
    return _pml(reduced, label=getattr(layer, "label", None)), True


def prepare_layers(
    layers,
    *,
    max_embed_mb: float = DEFAULT_MAX_EMBED_MB,
    simplify_tiles_spec=None,
    fallback: bool = True,
    emphasis: str = "blend",
) -> dict:
    """Decide, per layer, whether the interactive map can be embedded or must fall back.

    Rung order per layer:

    1. pmtiles layer whose source is an explicit ``http(s)://`` URL -> url-stream
       mode (0 embed cost; always interactive regardless of budget).
    2. Embedded pmtiles layers whose raw archive bytes already exceed the budget
       are flagged before calling ``layer_to_sources_layers`` (which would call
       ``pmtiles_info`` on potentially-malformed archives and throw).  These are
       immediately routed to the fallback path.
    3. All other layers -> :func:`layer_to_sources_layers` (embed bytes measured).
    4. **(Simplify hook -- Task 11)** if ``simplify_tiles_spec`` or
       ``layer.simplify`` is present, invoke :func:`_simplify_layer`.  Since the
       engine is not yet implemented the stub raises :exc:`NotImplementedError`;
       this rung is therefore **not** entered unless a spec is explicitly supplied.
    5. If the fully-assembled HTML (via :func:`build_html`) exceeds *max_embed_mb*:
       - ``fallback=True`` (default) -> ``mode="static"`` with a loud warning
         naming the offending layer(s) and the three remedies.  pmtiles layers
         are decoded to raster/vector layers for ``plot_static`` so they are never
         silently dropped.
       - ``fallback=False`` -> raises :exc:`ValueError`.

    The budget gate is the **actual assembled-HTML byte-length**, not a per-layer
    sum.  :func:`build_html` is called once (non-mutating) to measure, then the
    result is returned in ``"prepared"``.

    Args:
        layers:               A list of :class:`~databricks.labs.gbx.vizx._layers.Layer`.
        max_embed_mb:         HTML size threshold in mebibytes (default ``DEFAULT_MAX_EMBED_MB`` = 8).
        simplify_tiles_spec:  Optional spec dict passed to the Task-11 simplify
                              engine.  Currently wired only; passing a non-``None``
                              value invokes the stub and raises
                              :exc:`NotImplementedError`.
        fallback:             When ``True`` (default), degrade gracefully to a
                              static render if the budget is exceeded.  When
                              ``False``, raise :exc:`ValueError` instead.

    Returns:
        ``{"mode": "interactive"|"static", "prepared": [...], "warnings": [str, ...]}``.
        On the ``"interactive"`` path, ``"prepared"`` is the list of
        ``(sources, layers, embed_bytes)`` tuples suitable for :func:`build_html`.
        On the ``"static"`` path, ``"prepared"`` is a list of
        :class:`~databricks.labs.gbx.vizx._layers.Layer` objects suitable for
        ``plot_static``.
    """
    from databricks.labs.gbx.vizx._layers import as_layers

    layers = as_layers(layers) if not isinstance(layers, list) else layers
    budget_bytes = int(max_embed_mb * 1_048_576)

    remedy_msg = (
        "Remedies: (1) stage the archive at a reachable https:// URL and pass it "
        "to pmtiles_layer() (zero embed cost, always interactive); (2) pre-tile or "
        "shard your data into a smaller PMTiles archive (reduce AOI or max zoom). "
        "Note: a Databricks Serverless notebook caps cell output at 10 MB by "
        "default (20 MB max via %set_cell_max_output_size_in_mb), and displayHTML "
        "inflates the cell payload to ~2-3x the embedded HTML (measured) -- so "
        "raising max_embed_mb alone cannot exceed the cell ceiling; an archive "
        "over ~4-5 MB cannot embed inline even at the raised 20 MB cap and must "
        "use a URL, a smaller/sharded archive, or the static fallback."
    )

    prepared: list = []
    warn_msgs: list[str] = []
    # Layers that are known-oversize before even trying to process them.
    early_oversize_labels: list[str] = []
    # Labels of layers that were simplified (for interactive-mode warning).
    simplified_labels: list[str] = []

    # Auto-fit: when a MULTI-layer overlay has >=2 embedded pmtiles layers (e.g. a NAIP
    # raster basemap + buildings), split the budget across them and down-zoom each to its
    # share below so the overlay embeds instead of busting the cell cap (autofit is a
    # no-op when an archive already fits). URL-mode pmtiles stream for free and are
    # excluded. A SINGLE archive is left alone here -- plot_pmtiles already governs its
    # reduction via interactive_fit (so interactive_fit=None still means "do not reduce").
    _n_embed_pm = sum(
        1
        for _l in layers
        if getattr(_l, "kind", None) == "pmtiles" and not _pmtiles_is_url(_l)
    )
    per_pmtiles_mb = (max_embed_mb / _n_embed_pm) if _n_embed_pm >= 2 else 0

    for idx, layer in enumerate(layers):
        kind = getattr(layer, "kind", None)

        # ------------------------------------------------------------------ #
        # Rung 1 -- pmtiles with an explicit http(s) URL: zero embed cost.   #
        # Always interactive; skip budget accounting entirely.                #
        # ------------------------------------------------------------------ #
        if kind == "pmtiles" and _pmtiles_is_url(layer):
            entry = layer_to_sources_layers(layer, idx, emphasis=emphasis)
            prepared.append(entry)
            continue

        # ------------------------------------------------------------------ #
        # Rung 3 -- simplify hook (Task 11).                                 #
        # Runs BEFORE the raw-oversize bail so an over-budget archive gets a  #
        # chance to be reduced under budget and stay interactive.             #
        # URL-mode pmtiles skip simplify (remote archive, zero embed cost).   #
        # ------------------------------------------------------------------ #
        spec = simplify_tiles_spec or getattr(layer, "simplify", None)
        # The url-mode clause is defensive: URL-mode pmtiles already `continue`d
        # at Rung 1, so they never reach here -- the guard just documents intent.
        if spec is not None and not (kind == "pmtiles" and _pmtiles_is_url(layer)):
            layer = _simplify_layer(layer, spec, max_embed_mb)
            simplified_labels.append(_layer_label(layer, idx))

        # ------------------------------------------------------------------ #
        # Rung 2.5 -- auto-fit: down-zoom an embedded pmtiles archive to its  #
        # share of the budget so a multi-layer overlay embeds (the same       #
        # binary-free downzoom plot_pmtiles does for a single archive). No-op  #
        # when the archive already fits its share; failures fall through to    #
        # the over-budget gate below.                                          #
        # ------------------------------------------------------------------ #
        if kind == "pmtiles" and per_pmtiles_mb > 0:
            layer, _reduced = _autofit_pmtiles_layer(layer, per_pmtiles_mb)
            if _reduced:
                simplified_labels.append(_layer_label(layer, idx))

        # ------------------------------------------------------------------ #
        # Rung 2 (pre-check) -- for embedded pmtiles, guard against archives  #
        # whose raw bytes already exceed the budget.  This avoids calling     #
        # pmtiles_info on potentially-invalid archives and keeps the error    #
        # boundary clean.  Measured on the (possibly-simplified) archive so   #
        # that a spec that reduces the archive under budget falls through to   #
        # normal interactive preparation below.                               #
        # ------------------------------------------------------------------ #
        if kind == "pmtiles":
            raw = _pmtiles_raw_bytes(layer)
            # Compare the RENDERED embed size (base64-inflated ~4/3x), not the raw
            # archive bytes — the Serverless cell-output cap counts rendered HTML.
            if raw is not None and len(raw) * _BASE64_INFLATION > budget_bytes:
                early_oversize_labels.append(_layer_label(layer, idx))
                # Placeholder so indices stay aligned for the static-path pass.
                prepared.append(None)
                continue

        # ------------------------------------------------------------------ #
        # Rung 2 (normal) -- prepare via layer_to_sources_layers.            #
        # ------------------------------------------------------------------ #
        entry = layer_to_sources_layers(layer, idx, emphasis=emphasis)
        prepared.append(entry)

    # ------------------------------------------------------------------ #
    # If any layer was early-flagged as oversize, skip the HTML build and #
    # jump straight to the fallback decision.                             #
    # ------------------------------------------------------------------ #
    oversize_labels: list[str] = list(early_oversize_labels)
    html_bytes = None

    if not early_oversize_labels:
        # Budget gate: measure actual assembled-HTML size.
        # (build_html is non-mutating -- safe to call for measurement.)
        valid_prepared = [e for e in prepared if e is not None]
        html_bytes = len(build_html(valid_prepared, emphasis=emphasis).encode())
        if html_bytes <= budget_bytes:
            if simplified_labels:
                for lbl in simplified_labels:
                    msg = f"simplified {lbl}"
                    warn_msgs.append(msg)
                    _warnings.warn(msg, stacklevel=2)
            audit = _build_audit(
                layers, prepared, html_bytes, budget_bytes, simplify_tiles_spec
            )
            return {
                "mode": "interactive",
                "prepared": valid_prepared,
                "warnings": warn_msgs,
                "audit": audit,
            }

        # Over budget -- identify embedded layer(s) for the warning.
        for i, (lyr, entry) in enumerate(zip(layers, prepared)):
            eb = entry[2] if entry is not None and len(entry) > 2 else 0
            if eb > 0:
                oversize_labels.append(_layer_label(lyr, i))
        if not oversize_labels:
            oversize_labels = [_layer_label(l, i) for i, l in enumerate(layers)]

    # Unified over-budget measure: the actual assembled-HTML size when we built it,
    # else the base64-inflated (~4/3x) RENDERED estimate of the pmtiles archives
    # (the Serverless cell-output cap counts rendered bytes, not raw archive bytes).
    # The SAME measure feeds the warning, the ValueError, and the audit below -- so
    # the audit verdict can never disagree with the mode decision.
    total_bytes = (
        html_bytes
        if html_bytes is not None
        else int(
            sum(
                len(_pmtiles_raw_bytes(layer) or b"")
                for layer in layers
                if getattr(layer, "kind", None) == "pmtiles"
            )
            * _BASE64_INFLATION
        )
    )
    size_desc = f"{total_bytes / 1_048_576:.1f} MB" + (
        "" if html_bytes is not None else " (rendered est.)"
    )

    if not fallback:
        raise ValueError(
            f"prepare_layers: assembled HTML exceeds budget ({size_desc} > "
            f"{max_embed_mb} MB). {remedy_msg}"
        )
    warn_text = (
        f"prepare_layers: embedded HTML ({size_desc}) exceeds "
        f"max_embed_mb={max_embed_mb}; falling back to static render. "
        f"Offending layer(s): {', '.join(oversize_labels)}. {remedy_msg}"
    )
    warn_msgs.append(warn_text)
    _warnings.warn(warn_text, stacklevel=2)

    # Build static-path layer list: decode pmtiles -> raster/vector for plot_static.
    static_layers = []
    for idx, layer in enumerate(layers):
        if getattr(layer, "kind", None) == "pmtiles":
            try:
                decoded = _decode_pmtiles_for_static(layer)
                static_layers.append(decoded)
            except Exception as e:
                msg = (
                    f"prepare_layers: could not decode pmtiles {_layer_label(layer, idx)} "
                    f"for static fallback: {e}"
                )
                warn_msgs.append(msg)
                _warnings.warn(msg, stacklevel=2)
        else:
            static_layers.append(layer)

    # Build audit for the static-fallback path. total_bytes (computed above) is the
    # SAME rendered measure as the mode decision, so the audit verdict matches mode.
    audit = _build_audit(
        layers, prepared, total_bytes, budget_bytes, simplify_tiles_spec
    )
    return {
        "mode": "static",
        "prepared": static_layers,
        "warnings": warn_msgs,
        "audit": audit,
    }


def _pmtiles_register_js(sid: str, info: dict, uid: str = "") -> str:
    """Return the JS snippet that registers one PMTiles source with the protocol.

    For ``"url"`` mode the archive is fetched on demand from the remote URL (keyed by
    that URL). For ``"embed"`` mode the bytes are base64-encoded into a JS ``Uint8Array``
    wrapped in a ``File`` → ``pmtiles.FileSource`` keyed by ``uid_sid`` (per-map unique)
    when ``uid`` is given, or by ``sid`` alone when it is empty.
    """
    if info["mode"] == "url":
        return f"  proto.add(new pmtiles.PMTiles({_json_for_script(info['url'])}));\n"
    # Embed mode: base64 → Uint8Array → File → FileSource.
    # The File name IS the protocol key (FileSource.getKey() returns it) and MUST match
    # the source URL (see build_html). A per-map `uid` prefix keeps keys unique when
    # several maps add archives to the one shared protocol (URL "pmtiles://<uid>_<sid>");
    # callers that keep the bare "pmtiles://<sid>" URL (e.g. the dynamic widget) pass no
    # uid and key by `sid` alone. The name must NOT carry a ".pmtiles" suffix (that
    # mismatched the URL -> blank map).
    key = f"{uid}_{sid}" if uid else sid
    b64 = base64.b64encode(info["bytes"]).decode("ascii")
    return (
        f"  const _b{sid} = Uint8Array.from(atob({json.dumps(b64)}), c => c.charCodeAt(0));\n"
        f"  proto.add(new pmtiles.PMTiles(new pmtiles.FileSource("
        f"new File([_b{sid}.buffer], '{key}'))));\n"
    )


# ---------------------------------------------------------------------------
# vector / grid
# ---------------------------------------------------------------------------


def _gdf_for(layer) -> Any:
    """Return a GeoDataFrame for *layer* (vector or grid)."""
    from databricks.labs.gbx.vizx import _vector

    if layer.kind == "grid":
        # Carry the value column through so the data-driven fill/legend can read it —
        # cells_as_gdf drops everything except cell_col + extra_cols, so a grid heatmap
        # colored by `column` would otherwise lose its values and render one flat fill.
        extra = [layer.column] if getattr(layer, "column", None) else ()
        return _vector.cells_as_gdf(
            layer.data, cell_col=layer.cellid_col or "cellid", extra_cols=extra
        )
    data = layer.data
    # Already a GeoDataFrame (has a .geometry attribute).
    if hasattr(data, "geometry"):
        return data
    # Spark DataFrame with a WKT column — collect and wrap.
    wkt_col = layer.geom_col or "wkt"
    return _vector.as_gdf(data, wkt_col=wkt_col)


def _cmap_hex_colors(values, cmap_name, scale="linear"):
    """Map numeric ``values`` through a matplotlib colormap to hex color strings.

    ``scale`` controls the value -> ramp-position mapping:
      * ``"linear"`` (default) -- position = (x - min) / (max - min).
      * ``"quantile"`` -- position = the value's percentile rank, so a skewed
        distribution (e.g. most H3 cells holding 1..25 of a 1..128 roof-count range)
        spreads across the full ramp instead of collapsing into one end of the colormap.

    Finite values map to a color; non-finite/None -> neutral grey. A single distinct
    finite value -> mid-ramp. MapLibre has no server-side colormap, so the interactive
    vector/grid renderer precomputes a per-feature color; this is what makes a grid
    heatmap render a real ramp instead of one flat fill.
    """
    import math

    import matplotlib
    import matplotlib.colors as mcolors

    cmap = matplotlib.colormaps[cmap_name]
    nums = []
    for v in values:
        try:
            nums.append(float(v))
        except (TypeError, ValueError):
            nums.append(math.nan)
    finite = [x for x in nums if math.isfinite(x)]
    if not finite:
        return ["#cccccc"] * len(nums)
    lo, hi = min(finite), max(finite)
    span = hi - lo

    if span == 0:
        positions = [None if not math.isfinite(x) else 0.5 for x in nums]
    elif scale == "quantile":
        import numpy as np

        srt = np.sort(np.asarray(finite, dtype=float))
        n = len(srt)
        positions = []
        for x in nums:
            if not math.isfinite(x):
                positions.append(None)
                continue
            lo_i = int(np.searchsorted(srt, x, side="left"))
            hi_i = int(np.searchsorted(srt, x, side="right"))
            positions.append(min(max(((lo_i + hi_i) / 2.0) / n, 0.0), 1.0))
    else:  # linear
        positions = [None if not math.isfinite(x) else (x - lo) / span for x in nums]

    return ["#cccccc" if p is None else mcolors.to_hex(cmap(p)) for p in positions]


def _legend_for(values, cmap_name, label, scale="linear"):
    """Legend descriptor for a data-driven layer: ``{label, vmin, vmax, stops[, mid]}``
    or None.

    ``stops`` are 9 hex colors sampled evenly along the colormap for the CSS gradient
    bar; ``vmin``/``vmax`` are the finite value range. For ``scale="quantile"`` the ramp
    position is percentile rank, so the bar midpoint is the median — ``mid`` carries it
    for a center tick, keeping the labels honest about the non-linear mapping. None when
    no finite values.
    """
    import math

    import matplotlib
    import matplotlib.colors as mcolors

    nums = []
    for v in values:
        try:
            f = float(v)
        except (TypeError, ValueError):
            continue
        if math.isfinite(f):
            nums.append(f)
    if not nums:
        return None
    cmap = matplotlib.colormaps[cmap_name]
    stops = [mcolors.to_hex(cmap(i / 8.0)) for i in range(9)]
    lg = {"label": str(label), "vmin": min(nums), "vmax": max(nums), "stops": stops}
    if scale == "quantile" and len(nums) > 2:
        import numpy as np

        lg["mid"] = float(np.median(np.asarray(nums, dtype=float)))
    return lg


def _legend_html(legends):
    """An absolute-positioned colormap legend overlay for data-driven layers (or "")."""
    if not legends:
        return ""
    import html as _html

    entries = []
    for lg in legends:
        if lg.get("items"):  # categorical: a colored swatch + label per item
            rows = []
            if lg.get("label"):
                rows.append(
                    '<div style="font:600 11px sans-serif;color:#222;margin-bottom:3px">'
                    f'{_html.escape(str(lg["label"]))}</div>'
                )
            for it in lg["items"]:
                rows.append(
                    '<div style="display:flex;align-items:center;gap:6px;margin:2px 0">'
                    '<span style="display:inline-block;width:12px;height:12px;'
                    "border-radius:2px;border:1px solid #aaa;"
                    f'background:{_html.escape(str(it["color"]))}"></span>'
                    '<span style="font:11px sans-serif;color:#333">'
                    f'{_html.escape(str(it["label"]))}</span></div>'
                )
            entries.append('<div style="margin:3px 0 5px">' + "".join(rows) + "</div>")
            continue
        grad = ", ".join(lg["stops"])
        entries.append(
            '<div style="margin:3px 0 5px">'
            '<div style="font:600 11px sans-serif;color:#222;margin-bottom:2px">'
            f'{_html.escape(lg["label"])}</div>'
            '<div style="width:130px;height:10px;border:1px solid #aaa;'
            f'background:linear-gradient(to right,{grad})"></div>'
            '<div style="display:flex;justify-content:space-between;'
            'font:10px sans-serif;color:#555">'
            f'<span>{lg["vmin"]:.3g}</span>'
            + (f'<span>{lg["mid"]:.3g}</span>' if lg.get("mid") is not None else "")
            + f'<span>{lg["vmax"]:.3g}</span></div>'
            "</div>"
        )
    return (
        '<div style="position:absolute;bottom:14px;right:14px;z-index:2;'
        "background:rgba(255,255,255,0.92);padding:8px 10px;border-radius:6px;"
        'box-shadow:0 1px 4px rgba(0,0,0,0.3)">' + "".join(entries) + "</div>"
    )


def _vector_or_grid(layer, idx: int) -> tuple[dict, list[dict], int]:
    sid = f"gbx{idx}"
    gdf = _gdf_for(layer).to_crs(4326)

    # Data-driven fill: map layer.column through layer.cmap to a per-feature hex color
    # (MapLibre has no server-side colormap, so precompute per feature). An explicit
    # scalar layer.color overrides; no column -> flat default. This is what makes a grid
    # heatmap (e.g. an H3 solar-score layer) render a real ramp, not one flat color.
    fill_color_expr = None
    legend = None
    if layer.color is None and layer.column is not None and layer.column in gdf.columns:
        gdf = gdf.copy()
        _vals = gdf[layer.column].tolist()
        _scale = getattr(layer, "scale", "linear")
        gdf["_gbx_color"] = _cmap_hex_colors(_vals, layer.cmap, _scale)
        fill_color_expr = ["get", "_gbx_color"]
        legend = _legend_for(_vals, layer.cmap, layer.label or layer.column, _scale)

    gj = json.loads(gdf.to_json())

    src = {sid: {"type": "geojson", "data": gj}}
    if legend:
        src[sid]["_gbx_legend"] = legend

    # Collect all geometry types present in this feature collection.
    geom_types: set[str] = set()
    for feat in gj.get("features", []):
        geom = feat.get("geometry") or {}
        t = geom.get("type", "")
        if t:
            geom_types.add(t)

    layers: list[dict] = []
    color = layer.color or "#3388ff"
    fill_color = fill_color_expr if fill_color_expr is not None else color
    # User-set opacity / width win over emphasis defaults; record the keys left at
    # default in the _gbx_emphasis sidecar so build_html fills them per emphasis.
    user_opacity = layer.opacity is not None
    user_width = layer.width is not None
    opacity = layer.opacity if user_opacity else 0.5

    # Polygons → fill + outline line.
    if geom_types & {"Polygon", "MultiPolygon"}:
        if getattr(layer, "fill", True):
            fill_paint = {"fill-color": fill_color, "fill-opacity": opacity}
            fill_pending = []
            if not user_opacity:
                fill_pending.append("fill-opacity")
            # The contrasting dark outline is purely emphasis-driven (no user
            # kwarg controls it), so it is always emphasis-defaultable.
            fill_pending.append("fill-outline-color")
            layers.append(
                {
                    "id": f"{sid}-fill",
                    "type": "fill",
                    "source": sid,
                    "paint": fill_paint,
                    _GBX_EMPHASIS: fill_pending,
                }
            )
        line_paint = {
            "line-color": layer.color or "#1f6fb5",
            "line-width": layer.width or 1.0,
        }
        layers.append(
            {
                "id": f"{sid}-line",
                "type": "line",
                "source": sid,
                "paint": line_paint,
                _GBX_EMPHASIS: [] if user_width else ["line-width"],
            }
        )
    # Lines (no polygon — those already got an outline above).
    if geom_types & {"LineString", "MultiLineString"}:
        layers.append(
            {
                "id": f"{sid}-line",
                "type": "line",
                "source": sid,
                "paint": {
                    "line-color": layer.color or "#1f6fb5",
                    "line-width": layer.width or 1.0,
                },
                _GBX_EMPHASIS: [] if user_width else ["line-width"],
            }
        )
    # Points.
    if geom_types & {"Point", "MultiPoint"}:
        layers.append(
            {
                "id": f"{sid}-circle",
                "type": "circle",
                "source": sid,
                "paint": {
                    "circle-color": layer.color or "#e04e2a",
                    "circle-radius": 4,
                },
            }
        )

    embed_bytes = len(json.dumps(gj).encode())
    return src, layers, embed_bytes


# ---------------------------------------------------------------------------
# raster
# ---------------------------------------------------------------------------


def _raster_to_image(
    layer, *, raster_max_px: int = _DEFAULT_RASTER_MAX_PX
) -> tuple[str, list]:
    """Render *layer.data* to a base64 PNG + 4-corner lon/lat coordinates.

    *layer.data* may be:
    - A filesystem path (str) to a GeoTIFF/COG.
    - ``bytes`` or ``bytearray`` of an in-memory GeoTIFF (e.g. a tile's
      ``raster`` field).
    - A bare ``numpy.ndarray`` (no geo metadata; unit-square [0,1] corners
      are synthesised).

    Returns ``(png_b64, corners)`` where *png_b64* is a URL-safe base64 string
    (no newlines) and *corners* is
    ``[[ulx,uly],[urx,ury],[lrx,lry],[llx,lly]]`` in lon/lat degrees.
    """
    import numpy as np

    data = layer.data

    # --- numpy ndarray path: no spatial metadata ----------------------------
    if isinstance(data, np.ndarray):
        png_b64 = _ndarray_to_png_b64(data)
        # Synthesise unit-square corners (MapLibre image source requires 4 corners).
        corners = [[0.0, 1.0], [1.0, 1.0], [1.0, 0.0], [0.0, 0.0]]
        return png_b64, corners

    # --- rasterio path (path or bytes) -------------------------------------
    import rasterio
    from rasterio.io import MemoryFile

    if isinstance(data, (bytes, bytearray)):
        # MemoryFile needs a double context-manager: outer opens the file object,
        # inner .open() returns the rasterio DatasetReader.
        with MemoryFile(bytes(data)) as mf:
            with mf.open() as src:
                png_b64, corners = _src_to_png_b64(src, raster_max_px)
        return png_b64, corners
    elif isinstance(data, str):
        # Strip dbfs:/file: scheme prefixes (mirroring _raster.py).
        path = data
        for scheme in ("dbfs:", "file:"):
            if path.startswith(scheme):
                path = path[len(scheme) :]
                break
        if path.startswith("//"):
            path = "/" + path.lstrip("/")
        with rasterio.open(path) as src:
            png_b64, corners = _src_to_png_b64(src, raster_max_px)
        return png_b64, corners
    else:
        raise TypeError(
            f"_raster_to_image: unsupported data type {type(data).__name__!r}; "
            "expected str path, bytes, or numpy.ndarray"
        )


def _src_to_png_b64(src, raster_max_px: int) -> tuple[str, list]:
    """Read, decimate, render to RGBA PNG, base64-encode; extract corners."""
    import rasterio
    from rasterio.warp import transform_bounds

    # Decimate so longest edge ≤ raster_max_px.
    scale = max(src.width, src.height) / raster_max_px
    if scale > 1:
        out_h = max(1, int(src.height // scale))
        out_w = max(1, int(src.width // scale))
        data = src.read(
            out_shape=(src.count, out_h, out_w),
            resampling=rasterio.enums.Resampling.bilinear,
            masked=True,
        )
    else:
        data = src.read(masked=True)
        out_h, out_w = src.height, src.width

    # Reproject bounding box to EPSG:4326 to get lon/lat corners.
    try:
        bounds_4326 = transform_bounds(src.crs, "EPSG:4326", *src.bounds)
    except Exception:
        # Fall back to treating the existing bounds as lon/lat.
        bounds_4326 = src.bounds

    min_lon, min_lat, max_lon, max_lat = bounds_4326
    # MapLibre image source corner order: ul, ur, lr, ll (lon, lat).
    corners = [
        [min_lon, max_lat],  # upper-left
        [max_lon, max_lat],  # upper-right
        [max_lon, min_lat],  # lower-right
        [min_lon, min_lat],  # lower-left
    ]

    png_b64 = _data_to_png_b64(data, out_h, out_w)
    return png_b64, corners


def _data_to_png_b64(data, height: int, width: int) -> str:
    """Render a (bands, H, W) masked array to a base64 RGBA PNG string."""
    import numpy as np
    from PIL import Image

    # Normalise to [0, 255] uint8 for PNG encoding.
    if isinstance(data, np.ma.MaskedArray):
        arr = data.filled(0)
    else:
        arr = np.asarray(data)

    if arr.ndim == 2:
        arr = arr[np.newaxis, ...]  # treat as single band

    n_bands = arr.shape[0]
    if n_bands == 1:
        # Greyscale → viridis-like: just map to grey for simplicity, with alpha.
        band = arr[0].astype(np.float64)
        bmin, bmax = band.min(), band.max()
        rng = max(bmax - bmin, 1e-9)
        norm = ((band - bmin) / rng * 255).astype(np.uint8)
        rgba = np.stack([norm, norm, norm, np.full_like(norm, 255)], axis=-1)
    elif n_bands >= 3:
        # Take first 3 bands as RGB.
        bands = []
        for i in range(3):
            b = arr[i].astype(np.float64)
            bmin, bmax = b.min(), b.max()
            rng = max(bmax - bmin, 1e-9)
            bands.append(((b - bmin) / rng * 255).astype(np.uint8))
        alpha = np.full((height, width), 255, dtype=np.uint8)
        rgba = np.stack(bands + [alpha], axis=-1)
    else:
        # 2 bands: treat as greyscale from first band.
        band = arr[0].astype(np.float64)
        bmin, bmax = band.min(), band.max()
        rng = max(bmax - bmin, 1e-9)
        norm = ((band - bmin) / rng * 255).astype(np.uint8)
        rgba = np.stack([norm, norm, norm, np.full_like(norm, 255)], axis=-1)

    img = Image.fromarray(rgba, mode="RGBA")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode("ascii")


def _ndarray_to_png_b64(arr) -> str:
    """Render a bare ndarray (2-D or 3-D CxHxW) to a base64 PNG string."""
    if arr.ndim == 2:
        h, w = arr.shape
    elif arr.ndim == 3:
        _, h, w = arr.shape
    else:
        # Flatten extra dims.
        arr = arr.reshape(-1, arr.shape[-2], arr.shape[-1])
        _, h, w = arr.shape
    return _data_to_png_b64(arr, h, w)


def _raster(
    layer, idx: int, *, raster_max_px: int = _DEFAULT_RASTER_MAX_PX
) -> tuple[dict, list[dict], int]:
    sid = f"gbx{idx}"
    png_b64, corners = _raster_to_image(layer, raster_max_px=raster_max_px)
    url = f"data:image/png;base64,{png_b64}"
    src = {
        sid: {
            "type": "image",
            "url": url,
            "coordinates": corners,
        }
    }
    user_opacity = layer.opacity is not None
    lyr = {
        "id": f"{sid}-raster",
        "type": "raster",
        "source": sid,
        "paint": {"raster-opacity": layer.opacity if user_opacity else 1.0},
        _GBX_EMPHASIS: [] if user_opacity else ["raster-opacity"],
    }
    embed_bytes = len(png_b64.encode())
    return src, [lyr], embed_bytes


# ---------------------------------------------------------------------------
# pmtiles
# ---------------------------------------------------------------------------


def _extract_vector_layer_names(metadata: dict) -> list[str]:
    """Extract declared vector-layer names from TileJSON-style metadata.

    The standard TileJSON ``vector_layers`` key holds a list of objects each
    with an ``"id"`` string.  Returns that list of ids (preserving order).
    Falls back to an empty list when the key is absent or malformed.
    """
    layers = metadata.get("vector_layers", [])
    if not isinstance(layers, list):
        return []
    names = []
    for entry in layers:
        if isinstance(entry, dict) and isinstance(entry.get("id"), str):
            names.append(entry["id"])
    return names


def _mvt_layer_names_from_bytes(raw: bytes, *, max_tiles: int = 40) -> list:
    """Discover MVT source-layer names by decoding sample tiles from the archive.

    Used when the archive declares no ``vector_layers`` metadata, so we render the
    source-layers the tiles ACTUALLY contain rather than guessing a name. Scans up to
    ``max_tiles`` tiles and unions their layer keys (a single low-zoom tile may not
    hold every layer). Best-effort: returns ``[]`` if the archive can't be decoded.
    """
    try:
        import mapbox_vector_tile as mvt
        from pmtiles.reader import MemorySource, all_tiles

        from databricks.labs.gbx.vizx._pmtiles import _maybe_gunzip

        names, seen = [], set()
        for i, (_key, payload) in enumerate(all_tiles(MemorySource(raw))):
            if i >= max_tiles:
                break
            for lname in mvt.decode(_maybe_gunzip(payload)).keys():
                if lname not in seen:
                    seen.add(lname)
                    names.append(lname)
        return names
    except Exception:  # noqa: BLE001 — best-effort discovery
        return []


def _vector_layer_names(metadata: dict, raw: bytes, tile_type: str) -> list:
    """Source-layer names for a vector archive: prefer the declared TileJSON
    ``vector_layers``; else decode the tiles (embed archives self-describe). No
    domain-specific name is ever guessed."""
    names = _extract_vector_layer_names(metadata or {})
    if not names and raw and (tile_type or "").lower() == "mvt":
        names = _mvt_layer_names_from_bytes(raw)
    return names


def _resolve_pmtiles_bytes_or_url(layer) -> dict:
    """Return a sidecar info dict for the pmtiles layer.

    Returns one of:
    - ``{"mode": "url", "url": <str>, "tile_type": <str>, "vector_layer_names": <list>}``
      — when ``layer.data`` is an ``http(s)://`` URL (no local bytes needed).
      ``vector_layer_names`` is empty because we cannot inspect a remote archive
      without fetching it.
    - ``{"mode": "embed", "bytes": <bytes>, "tile_type": <str>, "vector_layer_names": <list>}``
      — when ``layer.data`` is a path or bytes archive; ``pmtiles_info`` is called
      to detect the tile type and extract declared vector layer names from metadata.
    """
    from databricks.labs.gbx.pmtiles import pmtiles_info

    data = layer.data

    # Remote URL: no need to read bytes.
    if isinstance(data, str) and (
        data.startswith("http://") or data.startswith("https://")
    ):
        # We cannot call pmtiles_info on a remote URL without fetching it.
        # Report tile_type as "unknown" for the url mode; the Task-5 HTML builder
        # can default to "vector" or the caller can supply a style.
        return {
            "mode": "url",
            "url": data,
            "tile_type": "unknown",
            "vector_layer_names": [],
        }

    # Path on disk.
    if isinstance(data, str):
        with open(data, "rb") as f:
            raw = f.read()
        info = pmtiles_info(raw)
        return {
            "mode": "embed",
            "bytes": raw,
            "tile_type": info["tile_type"],
            "vector_layer_names": _vector_layer_names(
                info.get("metadata", {}), raw, info["tile_type"]
            ),
        }

    # Already bytes.
    if isinstance(data, (bytes, bytearray)):
        raw = bytes(data)
        info = pmtiles_info(raw)
        return {
            "mode": "embed",
            "bytes": raw,
            "tile_type": info["tile_type"],
            "vector_layer_names": _vector_layer_names(
                info.get("metadata", {}), raw, info["tile_type"]
            ),
        }

    raise TypeError(
        f"_resolve_pmtiles_bytes_or_url: unsupported data type "
        f"{type(data).__name__!r}; expected str path, http(s) URL, or bytes"
    )


def _is_raster_tile_type(tile_type: str) -> bool:
    return tile_type.lower() in ("png", "jpeg", "webp", "avif")


def _raster_tile_px(info: dict) -> int:
    """Pixel side of the archive's raster tiles, for the MapLibre raster ``tileSize``.

    Reads the first tile's PNG IHDR width (raster tiles are stored uncompressed PNG).
    Defaults to 256 -- the gbx_rst_xyzpyramid default and the web-tile standard -- when
    the bytes aren't available (url mode) or the header can't be parsed. A correct
    tileSize is what places the raster at the right scale/position on the map.
    """
    raw = info.get("bytes")
    if not raw:
        return 256
    try:
        import struct

        from pmtiles.reader import MemorySource, all_tiles

        for _key, payload in all_tiles(MemorySource(raw)):
            if payload[:8] == b"\x89PNG\r\n\x1a\n" and len(payload) >= 24:
                return int(struct.unpack(">I", payload[16:20])[0])
            break  # only the first tile; non-PNG -> fall back to the default
    except Exception:  # noqa: BLE001 — best-effort; default to 256
        pass
    return 256


def _pmtiles(layer, idx: int) -> tuple[dict, list[dict], int]:
    sid = f"gbx{idx}"
    info = _resolve_pmtiles_bytes_or_url(layer)
    tile_type = info.get("tile_type", "unknown")
    is_raster = _is_raster_tile_type(tile_type)

    # pmtiles.js resolves a source by the key in its "pmtiles://<key>" URL, which MUST
    # equal the registered archive's key: for an embedded FileSource that key is the
    # File name (we register it as `sid`), and for url mode it is the remote URL string
    # (PMTiles(url) keys by the URL). A mismatch means MapLibre can't find the archive
    # -> no tiles load -> a silently blank map. So the source URL key is mode-dependent.
    pm_key = info["url"] if info["mode"] == "url" else sid
    src: dict[str, Any] = {
        sid: {
            "type": "raster" if is_raster else "vector",
            "url": f"pmtiles://{pm_key}",
        }
    }
    if is_raster:
        # MapLibre's raster source defaults to tileSize 512, but gbx_rst_xyzpyramid emits
        # 256px tiles -- a 256 tile rendered as 512 lands at 2x scale and the wrong place.
        # Pin the actual tile pixel size so the raster registers correctly on the map.
        src[sid]["tileSize"] = _raster_tile_px(info)
    # Sidecar consumed (and popped) by the Task-5 HTML builder.
    src[sid]["_gbx_pmtiles"] = info

    if is_raster:
        user_opacity = layer.opacity is not None
        layers: list[dict] = [
            {
                "id": f"{sid}-raster",
                "type": "raster",
                "source": sid,
                "paint": {"raster-opacity": layer.opacity if user_opacity else 1.0},
                _GBX_EMPHASIS: [] if user_opacity else ["raster-opacity"],
            }
        ]
    else:
        # Render EVERY source-layer the archive declares (TileJSON `vector_layers`
        # ids, else names discovered from the tiles), not just the first — a
        # multi-layer archive (e.g. hotspots + plumes + wells) otherwise loses all
        # but one layer. For each source-layer add fill + line + circle sub-layers so
        # polygons, lines and points all render (MapLibre draws each feature only in
        # the type-matching sub-layer); a single pmtiles_layer color applies to all,
        # else a palette gives each source-layer its own color.
        vector_names = info.get("vector_layer_names", [])
        if not vector_names:
            import warnings

            warnings.warn(
                "plot_pmtiles: no vector source-layer could be resolved for this "
                "archive (no vector_layers metadata and none discoverable from its "
                "tiles) — pass metadata_json with vector_layers to gbx_pmtiles_agg. "
                "Rendering the basemap only.",
                stacklevel=2,
            )
        user_opacity = layer.opacity is not None
        layers = []
        legend_items: list[dict] = []
        for li, sl in enumerate(vector_names):
            col = layer.color or _VECTOR_PALETTE[li % len(_VECTOR_PALETTE)]
            legend_items.append({"label": sl, "color": col})
            fill_pending = ([] if user_opacity else ["fill-opacity"]) + [
                "fill-outline-color"
            ]
            layers.append(
                {
                    "id": f"{sid}-{sl}-fill",
                    "type": "fill",
                    "source": sid,
                    "source-layer": sl,
                    "paint": {
                        "fill-color": col,
                        "fill-opacity": layer.opacity if user_opacity else 0.5,
                    },
                    _GBX_EMPHASIS: fill_pending,
                }
            )
            layers.append(
                {
                    "id": f"{sid}-{sl}-line",
                    "type": "line",
                    "source": sid,
                    "source-layer": sl,
                    "paint": {"line-color": col, "line-width": 1.4},
                    _GBX_EMPHASIS: ["line-width"],
                }
            )
            layers.append(
                {
                    "id": f"{sid}-{sl}-circle",
                    "type": "circle",
                    "source": sid,
                    "source-layer": sl,
                    "paint": {
                        "circle-color": col,
                        "circle-radius": 4,
                        "circle-stroke-color": "#FFFFFF",
                        "circle-stroke-width": 0.8,
                    },
                    _GBX_EMPHASIS: [],
                }
            )
        # Categorical legend (color swatch -> source-layer name) so a multi-layer
        # archive is legible without guessing which color is which layer.
        if legend_items:
            src[sid]["_gbx_legend"] = {"label": layer.label, "items": legend_items}

    embed_bytes = len(info["bytes"]) if info["mode"] == "embed" else 0
    return src, layers, embed_bytes
