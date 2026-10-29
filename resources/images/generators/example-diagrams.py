#!/usr/bin/env python3
"""Generate conceptual pipeline diagrams for example notebooks.

Renders three SVGs (xview-clipping, h3-rasterize, zarr-weather-features) + matching PNGs.

Re-render after editing:

    python3 resources/images/generators/example-diagrams.py
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
        --headless --disable-gpu --hide-scrollbars \
        --force-device-scale-factor=2 --window-size=1500,820 \
        --screenshot=resources/images/diagrams/xview/xview-clipping.png \
        resources/images/diagrams/xview/xview-clipping.svg
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
        --headless --disable-gpu --hide-scrollbars \
        --force-device-scale-factor=2 --window-size=1500,820 \
        --screenshot=resources/images/diagrams/h3-rasterize/h3-rasterize.png \
        resources/images/diagrams/h3-rasterize/h3-rasterize.svg
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
        --headless --disable-gpu --hide-scrollbars \
        --force-device-scale-factor=2 --window-size=1500,820 \
        --screenshot=resources/images/diagrams/zarr-weather-features/zarr-weather-features.png \
        resources/images/diagrams/zarr-weather-features/zarr-weather-features.svg
    python3 -c "
from PIL import Image, ImageChops
for p in ['resources/images/diagrams/xview/xview-clipping.png', 'resources/images/diagrams/h3-rasterize/h3-rasterize.png',
          'resources/images/diagrams/zarr-weather-features/zarr-weather-features.png']:
    img = Image.open(p).convert('RGB')
    diff = ImageChops.difference(img, Image.new('RGB', img.size, (255,255,255)))
    bbox = diff.getbbox()
    if bbox:
        img.crop(bbox).save(p)
"
"""
import importlib.util
import math
import os
import sys
from textwrap import dedent

# ---------------------------------------------------------------------------
# Import eo-series.py via importlib (filename has a hyphen)
# ---------------------------------------------------------------------------

_HERE = os.path.dirname(os.path.abspath(__file__))
_EO_PATH = os.path.join(_HERE, "eo-series.py")

spec = importlib.util.spec_from_file_location("eo_series", _EO_PATH)
eo = importlib.util.module_from_spec(spec)
spec.loader.exec_module(eo)

# Pull shared primitives and constants into module namespace for convenience
card        = eo.card
chip        = eo.chip
arrow       = eo.arrow
text        = eo.text
mono        = eo.mono
top_stripe  = eo.top_stripe
_wrap_text  = eo._wrap_text
render_stage = eo.render_stage
Stage       = eo.Stage
esc         = eo.esc

C_INK    = eo.C_INK
C_INK_2  = eo.C_INK_2
C_MUTED  = eo.C_MUTED
C_MUTED_2 = eo.C_MUTED_2
C_MUTED_3 = eo.C_MUTED_3
C_BORDER  = eo.C_BORDER

CANVAS_W      = eo.CANVAS_W
CANVAS_H      = eo.CANVAS_H
PAD           = eo.PAD
HEADER_H      = eo.HEADER_H
STAGE_TOP_GAP = eo.STAGE_TOP_GAP
STAGE_H       = eo.STAGE_H
STAGE_GLYPH_H = eo.STAGE_GLYPH_H
ARROW_W       = eo.ARROW_W
FOOTER_TOP_GAP = eo.FOOTER_TOP_GAP
FOOTER_H      = eo.FOOTER_H


# ---------------------------------------------------------------------------
# 1. render_header_generic
# ---------------------------------------------------------------------------

def render_header_generic(badge_label, title, subtitle, series_text, accent):
    """Like eo.render_header but badge shows a short string, pill shows series_text."""
    out = []
    bsize = 60
    bx, by = PAD, PAD + 4
    # Badge square
    out.append(
        f'<rect x="{bx}" y="{by}" rx="14" ry="14" width="{bsize}" height="{bsize}" '
        f'fill="{accent}"/>'
        f'<text x="{bx + bsize/2}" y="{by + bsize/2 + 9}" text-anchor="middle" '
        f'font-family="Inter, -apple-system, system-ui, sans-serif" '
        f'font-size="24" font-weight="900" '
        f'fill="#FFFFFF">{esc(badge_label)}</text>'
    )
    # Title + subtitle
    tx = bx + bsize + 18
    out.append(text(tx, by + 28, title, size=28, weight=800, fill=C_INK))
    out.append(text(tx, by + 54, subtitle, size=14, fill=C_MUTED))
    # Series pill (top-right)
    pill_text = series_text
    pw = int(len(pill_text) * 6.6) + 24
    out.append(
        f'<rect x="{CANVAS_W - PAD - pw}" y="{PAD + 12}" rx="13" ry="13" '
        f'width="{pw}" height="26" fill="{C_INK}"/>'
        f'<text x="{CANVAS_W - PAD - pw/2}" y="{PAD + 30}" text-anchor="middle" '
        f'font-family="Inter, -apple-system, system-ui, sans-serif" '
        f'font-size="12" font-weight="700" '
        f'fill="#FFFFFF">{esc(pill_text)}</text>'
    )
    return "".join(out)


# ---------------------------------------------------------------------------
# 2. render_footer_generic
# ---------------------------------------------------------------------------

def render_footer_generic(chips, accent, tint, note, label="KEY FUNCTIONS"):
    """Like eo.render_footer but the right-aligned note is the passed string."""
    out = []
    fy = CANVAS_H - PAD - FOOTER_H
    out.append(text(PAD, fy + 16, label,
                    size=10, weight=700, fill=C_MUTED_3, letter_spacing="1.6"))
    cx = PAD + 130
    cy = fy + 6
    for c in chips:
        chip_svg, cw = chip(cx, cy, c, fg=accent, bg=tint, mono_font=True, h=24)
        out.append(chip_svg)
        cx += cw + 8
    out.append(text(CANVAS_W - PAD, fy + 16, note,
                    size=11, fill=C_MUTED_3, anchor="end"))
    return "".join(out)


# ---------------------------------------------------------------------------
# 3. g_isobands — nested concentric contour blobs
# ---------------------------------------------------------------------------

def g_isobands(cx, cy, color, tint):
    """Nested concentric contour blobs: 4 graduated-opacity rounded ellipses."""
    out = []
    # Outer → inner: increasing opacity, slightly randomised contour offsets
    levels = [
        # (rx, ry, rotation_deg, opacity, dx, dy)
        (72, 52, -8,  0.18, -4,  6),
        (56, 40,  5,  0.32,  2, -2),
        (38, 28, -3,  0.52,  0,  4),
        (22, 16,  8,  0.76, -2,  0),
    ]
    for rx, ry, rot, opacity, dx, dy in levels:
        out.append(
            f'<ellipse cx="{cx + dx}" cy="{cy + dy}" rx="{rx}" ry="{ry}" '
            f'transform="rotate({rot} {cx + dx} {cy + dy})" '
            f'fill="{color}" fill-opacity="{opacity:.2f}" '
            f'stroke="{color}" stroke-width="1.5" stroke-opacity="0.55"/>'
        )
    return "".join(out)


# ---------------------------------------------------------------------------
# 4. g_h3_band_stack — elevation-labeled stacked tile rectangles
# ---------------------------------------------------------------------------

def g_h3_band_stack(cx, cy, color, tint):
    """4 offset/stacked rects labeled by elevation band, in graduated tint shades."""
    labels = ["0 m", "200 m", "400 m", "600 m"]
    # Opacity steps: outermost (bottom of stack) most faded, top most opaque
    opacities = [0.18, 0.32, 0.50, 0.72]
    w, h = 130, 90
    skew = 12
    out = []
    for i, (lbl, opacity) in enumerate(zip(labels, opacities)):
        x = cx - w / 2 + (3 - i) * skew
        y = cy - h / 2 + (3 - i) * skew - 30
        out.append(
            f'<rect x="{x}" y="{y}" rx="6" ry="6" width="{w}" height="{h}" '
            f'fill="{color}" fill-opacity="{opacity:.2f}" '
            f'stroke="{color}" stroke-width="2"/>'
            f'<text x="{x + 10}" y="{y + 16}" '
            f'font-family="ui-monospace, SFMono-Regular, Menlo, Consolas, monospace" '
            f'font-size="11" font-weight="700" fill="{color}">{esc(lbl)}</text>'
        )
    return "".join(out)


# ---------------------------------------------------------------------------
# 4b. g_zarr_cube — a time x latitude x longitude cube of chunk slabs
# ---------------------------------------------------------------------------

def g_zarr_cube(cx, cy, color, tint):
    """Three offset grid slabs (one per variable) with chunk lines, labeled by variable."""
    labels = ["t2m", "u10", "v10"]
    opacities = [0.20, 0.36, 0.56]
    w, h = 120, 84
    skew = 16
    out = []
    for i, (lbl, opacity) in enumerate(zip(labels, opacities)):
        x = cx - w / 2 + (2 - i) * skew - skew
        y = cy - h / 2 + (2 - i) * skew - skew - 10
        out.append(
            f'<rect x="{x}" y="{y}" rx="4" ry="4" width="{w}" height="{h}" '
            f'fill="{color}" fill-opacity="{opacity:.2f}" '
            f'stroke="{color}" stroke-width="2"/>'
        )
        # chunk grid: 2 x 2 inner chunks per slab
        out.append(
            f'<line x1="{x + w / 2}" y1="{y}" x2="{x + w / 2}" y2="{y + h}" '
            f'stroke="{color}" stroke-opacity="0.6" stroke-width="1.2"/>'
            f'<line x1="{x}" y1="{y + h / 2}" x2="{x + w}" y2="{y + h / 2}" '
            f'stroke="{color}" stroke-opacity="0.6" stroke-width="1.2"/>'
        )
        out.append(
            f'<text x="{x + 8}" y="{y + 15}" '
            f'font-family="ui-monospace, SFMono-Regular, Menlo, Consolas, monospace" '
            f'font-size="11" font-weight="700" fill="{color}">{esc(lbl)}</text>'
        )
    return "".join(out)


def g_wind_from_components(cx, cy, color, tint):
    """Two component grids (u10, v10) combine into one derived grid (wind_speed)."""
    out = []

    def grid(x, y, w, h, label, opacity):
        out.append(
            f'<rect x="{x}" y="{y}" rx="4" ry="4" width="{w}" height="{h}" '
            f'fill="{color}" fill-opacity="{opacity:.2f}" stroke="{color}" stroke-width="2"/>'
            f'<line x1="{x + w / 2}" y1="{y}" x2="{x + w / 2}" y2="{y + h}" '
            f'stroke="{color}" stroke-opacity="0.6" stroke-width="1.2"/>'
            f'<line x1="{x}" y1="{y + h / 2}" x2="{x + w}" y2="{y + h / 2}" '
            f'stroke="{color}" stroke-opacity="0.6" stroke-width="1.2"/>'
            f'<text x="{x + w / 2}" y="{y + h + 15}" text-anchor="middle" '
            f'font-family="ui-monospace, SFMono-Regular, Menlo, Consolas, monospace" '
            f'font-size="11" font-weight="700" fill="{color}">{esc(label)}</text>'
        )

    grid(cx - 78, cy - 82, 56, 40, "u10", 0.22)
    grid(cx + 22, cy - 82, 56, 40, "v10", 0.22)
    out.append(
        f'<text x="{cx}" y="{cy - 55}" text-anchor="middle" '
        f'font-family="Inter, sans-serif" font-size="20" font-weight="800" '
        f'fill="{color}">+</text>'
    )
    out.append(
        f'<line x1="{cx}" y1="{cy - 18}" x2="{cx}" y2="{cy - 2}" '
        f'stroke="{color}" stroke-width="2.5" stroke-linecap="round"/>'
        f'<polygon points="{cx},{cy + 6} {cx - 6},{cy - 3} {cx + 6},{cy - 3}" fill="{color}"/>'
    )
    grid(cx - 50, cy + 12, 100, 44, "wind_speed", 0.55)
    return "".join(out)


# ---------------------------------------------------------------------------
# 5. render_diagram — full SVG builder
# ---------------------------------------------------------------------------

def render_diagram(badge_label, title, subtitle, series_text,
                   accent, tint, stages, footer_chips, footer_note):
    """Build and return the full SVG string for one pipeline diagram."""
    parts = []
    parts.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'viewBox="0 0 {CANVAS_W} {CANVAS_H}" '
        f'width="{CANVAS_W}" height="{CANVAS_H}" '
        f'style="font-family: Inter, -apple-system, system-ui, sans-serif;">'
    )
    parts.append(dedent('''\
        <defs>
          <filter id="card-shadow" x="-5%" y="-5%" width="110%" height="115%">
            <feDropShadow dx="0" dy="2" stdDeviation="6"
                          flood-color="#0F1B2A" flood-opacity="0.08"/>
          </filter>
          <linearGradient id="bg" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0" stop-color="#FAFBFC"/>
            <stop offset="1" stop-color="#F1F4F8"/>
          </linearGradient>
        </defs>
        '''))
    parts.append(f'<rect x="0" y="0" width="{CANVAS_W}" height="{CANVAS_H}" fill="url(#bg)"/>')

    # Header
    parts.append(render_header_generic(badge_label, title, subtitle, series_text, accent))

    # Stages
    stage_y = PAD + HEADER_H + STAGE_TOP_GAP
    inner_w = CANVAS_W - 2 * PAD
    n = len(stages)
    arrows_total = (n - 1) * ARROW_W
    stage_w = (inner_w - arrows_total) // n
    cur_x = PAD
    for i, stg in enumerate(stages):
        parts.append(render_stage(cur_x, stage_y, stage_w, stg, accent, tint))
        cur_x += stage_w
        if i < n - 1:
            parts.append(arrow(cur_x + 8, stage_y + STAGE_H / 2 - 30,
                               cur_x + ARROW_W - 8, color=accent))
            cur_x += ARROW_W

    # Footer
    parts.append(render_footer_generic(footer_chips, accent, tint, footer_note))

    parts.append("</svg>")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# 6. Diagram definitions
# ---------------------------------------------------------------------------

XVIEW_CLIPPING = dict(
    badge_label="xV",
    title="Clip aerial rasters to labeled objects",
    subtitle="xView GeoTIFFs → per-feature clipped tiles, CRS-safe",
    series_text="xView · Per-object clipping",
    accent="#1F6FB5",
    tint="#E3EEF8",
    stages=[
        Stage(
            title="xView GeoTIFFs",
            subtitle="High-res aerial tiles loaded with the gtiff reader — one whole-image tile per file",
            glyph=eo.g_raster_scene,
            chip_text="gtiff_gbx",
        ),
        Stage(
            title="Object polygons",
            subtitle="GeoJSON labels → EWKT (SRID=4326) via st_geomfromgeojson + st_asewkt",
            glyph=eo.g_polygon_ak,
            chip_text="st_asewkt",
        ),
        Stage(
            title="Per-object clip",
            subtitle="rst_clip reprojects the EWKT cutline to the raster CRS, then clips",
            glyph=eo.g_clip_with_buffer,
            chip_text="rst_clip",
        ),
        Stage(
            title="Clipped TIFs",
            subtitle="gtiff writer materializes one TIF per object to the Volume",
            glyph=lambda cx, cy, c, t: eo.g_delta_table(cx, cy, c, t, label="clip/*.tif"),
            chip_text="gtiff_gbx",
        ),
    ],
    footer_chips=["gtiff_gbx", "st_geomfromgeojson", "st_asewkt", "rst_clip", "rst_boundingbox"],
    footer_note="databrickslabs/geobrix · xView Detection Challenge",
)

H3_RASTERIZE = dict(
    badge_label="H3",
    title="Rasterize H3 cells into a band stack",
    subtitle="DEM elevation bands → H3 cells → aligned multi-band raster",
    series_text="H3 Rasterize · Isobands",
    accent="#2F8F4E",
    tint="#DCEFE0",
    stages=[
        Stage(
            title="SF Bay Area DEM",
            subtitle="SRTM elevation tile read with rasterio on the driver",
            glyph=eo.g_raster_scene,
            chip_text="rasterio",
        ),
        Stage(
            title="Elevation isobands",
            subtitle="Quantize into 100 m bands — one polygon per band level",
            glyph=g_isobands,
            chip_text="features.shapes",
        ),
        Stage(
            title="H3 polyfill (res 8)",
            subtitle="Band polygons → H3 cells; rst_h3_gridspec snaps a shared canvas",
            glyph=eo.g_dense_hex_grid,
            chip_text="rst_h3_gridspec",
        ),
        Stage(
            title="Rasterize → band stack",
            subtitle="rst_h3_rasterize_agg burns each band; rst_frombands_agg stacks them",
            glyph=g_h3_band_stack,
            chip_text="rst_h3_rasterize_agg",
        ),
    ],
    footer_chips=["rst_h3_gridspec", "rst_h3_rasterize_agg", "rst_frombands_agg",
                  "plot_mask_layers", "plot_raster"],
    footer_note="databrickslabs/geobrix · SRTM · San Francisco",
)

ZARR_WEATHER_FEATURES = dict(
    badge_label="Zr",
    title="Weather features from a Zarr cube",
    subtitle="Register in place → sample → write back → map as tiles → tag a version",
    series_text="Zarr · Weather features",
    accent="#7A5AA6",
    tint="#ECE5F5",
    stages=[
        Stage(
            title="Register in place",
            subtitle="The cube becomes a Delta manifest of its own files; nothing is copied",
            glyph=g_zarr_cube,
            chip_text="gbx_zarr_register",
        ),
        Stage(
            title="Read and sample",
            subtitle="A box as blocks and a table; 100 sites' hourly series to a features table",
            glyph=eo.g_timeseries_in_hex,
            chip_text="gbx_zarr_sample",
        ),
        Stage(
            title="Write wind speed back",
            subtitle="wind_speed from u10 and v10, block by block, in one transactional update",
            glyph=g_wind_from_components,
            chip_text="gbx_zarr_write",
        ),
        Stage(
            title="Map as raster tiles",
            subtitle="One hour of wind speed as GeoBrix tiles, clipped to a polygon",
            glyph=eo.g_clip_with_buffer,
            chip_text="rst_clip",
        ),
        Stage(
            title="History and tag",
            subtitle="Every write is a version; tag the one the training features need",
            glyph=lambda cx, cy, c, t: eo.g_delta_table(cx, cy, c, t, label="tag:training"),
            chip_text="gbx_zarr_tag",
        ),
    ],
    footer_chips=["gbx_zarr_register", "gbx_zarr_read", "gbx_zarr_sample",
                  "gbx_zarr_write", "gbx_zarr_history", "gbx_zarr_tag", "rst_clip"],
    footer_note="databrickslabs/geobrix · synthetic ERA5-like cube",
)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    here = os.path.dirname(os.path.abspath(__file__))

    _NAME_TO_SUBDIR = {
        "xview-clipping": os.path.join(here, "..", "diagrams", "xview"),
        "h3-rasterize":   os.path.join(here, "..", "diagrams", "h3-rasterize"),
        "zarr-weather-features": os.path.join(here, "..", "diagrams", "zarr-weather-features"),
    }

    diagrams = [
        ("xview-clipping", XVIEW_CLIPPING),
        ("h3-rasterize",   H3_RASTERIZE),
        ("zarr-weather-features", ZARR_WEATHER_FEATURES),
    ]

    for name, kwargs in diagrams:
        os.makedirs(_NAME_TO_SUBDIR[name], exist_ok=True)
        svg_path = os.path.join(_NAME_TO_SUBDIR[name], f"{name}.svg")
        svg = render_diagram(**kwargs)
        with open(svg_path, "w", encoding="utf-8") as f:
            f.write(svg)
        print(f"wrote {svg_path}")
