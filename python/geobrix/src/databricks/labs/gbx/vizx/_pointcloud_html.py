"""deck.gl interactive HTML backend for the VizX 3D point-cloud renderer.

``build_pointcloud_html`` renders a point cloud as a self-contained HTML
document: a deck.gl ``PointCloudLayer`` under an ``OrbitView`` (orbit/pan/
zoom via deck.gl's own controller, RGB or ``cmap``-mapped color, no basemap --
a local scene frame rather than a geographic one). deck.gl is pinned via
unpkg + SRI, mirroring ``_maplibre.build_html``'s CDN pattern. The point
positions/colors are embedded as base64 binary buffers that the inline
``<script>`` ``atob``-decodes into typed arrays and feeds to deck.gl's binary
``PointCloudLayer`` attributes -- no pydeck/ipywidgets dependency, and no
JSON-serialized per-point arrays.

The WebGL render itself is validated in-notebook, not unit-tested here --
this module's test coverage is HTML-assembly only (CDN pin, SRI, layer/view
class names, buffer-decode marker, point count).
"""

from __future__ import annotations

import base64
import re

import numpy as np

from databricks.labs.gbx.vizx._maplibre import _json_for_script

# Safe background values: a hex color (#rgb/#rgba/#rrggbb/#rrggbbaa), a plain CSS
# color keyword, or an rgb()/rgba() function call. Anything else falls back to the
# default -- `background` is caller-supplied and interpolated into a style attribute.
_SAFE_BACKGROUND_RE = re.compile(
    r"^(#[0-9a-fA-F]{3,4}|#[0-9a-fA-F]{6}|#[0-9a-fA-F]{8}|[a-zA-Z]+|rgba?\([^)]*\))$"
)

# ---------------------------------------------------------------------------
# SRI-pinned CDN constants -- mirrors _maplibre's _MAPLIBRE_JS/_MAPLIBRE_JS_SRI
# pattern. Hash computed via:
#   curl -sL https://unpkg.com/deck.gl@9.0.0/dist.min.js \
#     | openssl dgst -sha384 -binary | openssl base64 -A
# ---------------------------------------------------------------------------

_DECKGL_JS = "https://unpkg.com/deck.gl@9.0.0/dist.min.js"
_DECKGL_JS_SRI = (
    "sha384-swFovX3ZcjmbLcV6TDG5hfxTmX+r/s/2lxRXi2wD1At3LGvgDY/YkSVyzuvQmJys"
)


def build_pointcloud_html(
    x,
    y,
    z,
    *,
    rgb=None,
    values=None,
    cmap="viridis",
    point_size=2.0,
    background="#111111",
    title=None,
) -> str:
    """Build a self-contained deck.gl HTML viewer for a point cloud.

    ``x``/``y``/``z`` are plain coordinate arrays (see
    :func:`~databricks.labs.gbx.vizx._pointcloud.load_point_cloud` for
    building them from a LAS/LAZ path or a DataFrame). The cloud is centered
    on its own mean on each axis (an empty cloud skips centering rather than
    dividing by zero). Color is true RGB (``rgb``, uint8) when given, else
    ``values`` (falling back to ``z``) mapped through ``cmap`` and scaled to
    ``0..255``.

    Point positions and colors are packed as raw little-endian binary
    buffers (``float32`` positions, ``uint8`` colors), base64-encoded, and
    embedded inline; the page's JS ``atob``-decodes them into
    ``Float32Array``/``Uint8Array`` and wires them into deck.gl's binary
    ``PointCloudLayer`` ``data.attributes`` (no per-point JSON). Rendering is
    local-frame (``deck.COORDINATE_SYSTEM.CARTESIAN``) under
    ``deck.OrbitView`` -- there is no basemap or geographic projection.

    Returns the HTML string. Does not call ``displayHTML`` -- the caller
    (``plot_point_cloud``) decides notebook-display vs. returning the string.
    """
    point_size = float(point_size)  # coerce once; a non-numeric caller value raises here
    if not _SAFE_BACKGROUND_RE.match(str(background)):
        background = "#111111"

    x = np.asarray(x, dtype="float64")
    y = np.asarray(y, dtype="float64")
    z = np.asarray(z, dtype="float64")
    n = x.shape[0]

    if n > 0:
        x = x - x.mean()
        y = y - y.mean()
        z = z - z.mean()

    import math

    if n > 0:
        _span = float(max(np.ptp(x), np.ptp(y), np.ptp(z)))
    else:
        _span = 0.0
    # OrbitView scale ~= 2^zoom px/world-unit; frame the span to ~90% of a nominal
    # ~600px viewport. Scale-invariant: a UTM-meter cloud and a unit cloud both open framed.
    if _span > 0:
        _zoom = math.log2(0.9 * 600.0 / _span)
        _zoom = max(-20.0, min(24.0, _zoom))
    else:
        _zoom = 3.0  # degenerate (single point / all-identical) -> old default

    positions = np.column_stack([x, y, z]).astype("float32")

    if rgb is not None:
        colors = np.asarray(rgb).astype("uint8")
    else:
        import matplotlib

        cmap_obj = matplotlib.colormaps[cmap]
        src = values if values is not None else z
        src = np.asarray(src, dtype="float64")
        if n == 0:
            colors = np.zeros((0, 3), dtype="uint8")
        else:
            lo, hi = np.nanmin(src), np.nanmax(src)
            span = hi - lo
            norm = np.full(n, 0.5) if span == 0 else (src - lo) / span
            norm = np.nan_to_num(norm, nan=0.5)
            colors = (cmap_obj(norm)[:, :3] * 255).astype("uint8")

    pos_b64 = base64.b64encode(positions.tobytes()).decode("ascii")
    col_b64 = base64.b64encode(colors.tobytes()).decode("ascii")

    title_html = ""
    if title:
        import html as _html

        title_html = (
            '<div style="position:absolute;top:12px;left:14px;z-index:2;'
            'font:600 14px sans-serif;color:#eee;text-shadow:0 1px 3px #000">'
            f"{_html.escape(str(title))}</div>"
        )

    import uuid

    container = f"gbx-pc-{uuid.uuid4().hex[:8]}"
    return f"""\
<div style="position:relative">
<div id="{container}" style="height:600px;background:{background}"></div>
{title_html}
</div>
<script src="{_DECKGL_JS}" integrity="{_DECKGL_JS_SRI}" crossorigin="anonymous"></script>
<script>
(function() {{
  const _posBytes = Uint8Array.from(atob({_json_for_script(pos_b64)}), c => c.charCodeAt(0));
  const _colBytes = Uint8Array.from(atob({_json_for_script(col_b64)}), c => c.charCodeAt(0));
  const positions = new Float32Array(_posBytes.buffer);
  const colors = new Uint8Array(_colBytes.buffer);
  const N = {n};
  const POINT_SIZE = {point_size};
  new deck.DeckGL({{
    container: "{container}",
    views: [new deck.OrbitView({{}})],
    controller: true,
    initialViewState: {{target: [0, 0, 0], rotationX: 30, rotationOrbit: 30, zoom: {_zoom}}},
    layers: [new deck.PointCloudLayer({{
      id: "pc",
      data: {{
        length: N,
        attributes: {{
          getPosition: {{value: positions, size: 3}},
          getColor: {{value: colors, size: 3}}
        }}
      }},
      pointSize: POINT_SIZE,
      sizeUnits: "pixels",
      coordinateSystem: deck.COORDINATE_SYSTEM.CARTESIAN
    }})]
  }});
}})();
</script>"""
