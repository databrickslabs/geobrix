#!/usr/bin/env python3
"""Generate the RasterX function-categories infographic SVG (portrait + landscape).

Re-render after adding/removing/renaming a RasterX function:

    python3 resources/images/rasterx-function-categories.py
    # writes both:
    #   resources/images/rasterx-function-categories.svg          (portrait, 2-col)
    #   resources/images/rasterx-function-categories_landscape.svg (landscape, 3-col)

Rasterize portrait PNG (used by docs/packages/rasterx.mdx + docs/api/raster-functions.mdx).
The height MUST match the "portrait canvas" the script prints — it grows as functions
are added, and a too-short window clips the bottom cards (e.g. Vector-Raster Bridge):
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \\
        --headless --disable-gpu --hide-scrollbars \\
        --force-device-scale-factor=2 --window-size=1416,<portrait_height> \\
        --screenshot=resources/images/rasterx-function-categories.png \\
        resources/images/rasterx-function-categories.svg

Rasterize landscape PNG (for slides / 16:9 decks):
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \\
        --headless --disable-gpu --hide-scrollbars \\
        --force-device-scale-factor=2 --window-size=2100,<landscape_height> \\
        --screenshot=resources/images/rasterx-function-categories_landscape.png \\
        resources/images/rasterx-function-categories_landscape.svg
    # Landscape canvas is 2100x1200 (printed by the script on each run).
"""
from dataclasses import dataclass, field
from textwrap import dedent

# --- Data: 108 functions, organized by category --------------------------------

@dataclass
class Section:
    label: str
    fns: list

@dataclass
class Card:
    title: str
    subtitle: str
    color: str            # accent (header)
    tint: str             # light fill used for pill background
    sections: list = field(default_factory=list)
    fns: list = field(default_factory=list)   # if no sections, flat list

CARDS_LEFT = [
    Card(
        title="Constructors",
        subtitle="Load rasters from path, bytes, or bands",
        color="#E04E2A", tint="#FCE9E2",
        fns=["rst_fromfile", "rst_fromcontent", "rst_frombands"],
    ),
    Card(
        title="Accessors",
        subtitle="Read raster metadata, geometry, dimensions, statistics",
        color="#1F6FB5", tint="#E3EEF8",
        sections=[
            Section("Geo & extent", [
                "rst_boundingbox", "rst_srid", "rst_georeference",
                "rst_upperleftx", "rst_upperlefty",
                "rst_scalex", "rst_scaley",
                "rst_skewx", "rst_skewy", "rst_rotation",
            ]),
            Section("Dimensions", [
                "rst_width", "rst_height",
                "rst_pixelwidth", "rst_pixelheight",
                "rst_pixelcount", "rst_memsize",
            ]),
            Section("Bands & types", [
                "rst_numbands", "rst_bandmetadata", "rst_type",
                "rst_getnodata", "rst_subdatasets", "rst_getsubdataset",
                "rst_format", "rst_metadata",
            ]),
            Section("Statistics", [
                "rst_min", "rst_max", "rst_avg", "rst_median", "rst_summary",
                "rst_sample", "rst_histogram",
            ]),
        ],
    ),
    Card(
        title="Aggregators",
        subtitle="Combine tiles in GROUP BY",
        color="#7A4FD3", tint="#ECE6FA",
        fns=[
            "rst_combineavg_agg", "rst_derivedband_agg", "rst_merge_agg",
            "rst_frombands_agg", "rst_rasterize_agg",
            "rst_dtmfromgeoms_agg", "rst_gridfrompoints_agg",
            "rst_h3_rasterize_agg",
        ],
    ),
    Card(
        title="Terrain Analysis",
        subtitle="Elevation-derived surface models via gdaldem",
        color="#8B5E3C", tint="#F5EDE4",
        fns=[
            "rst_slope", "rst_aspect", "rst_hillshade",
            "rst_tri", "rst_tpi", "rst_roughness",
            "rst_color_relief", "rst_viewshed",
        ],
    ),
    Card(
        title="Spectral Indices",
        subtitle="Band-math indices for vegetation, water, and fire",
        color="#2E8B57", tint="#E0F4EA",
        fns=["rst_ndvi", "rst_evi", "rst_savi", "rst_ndwi", "rst_nbr", "rst_index"],
    ),
]

CARDS_RIGHT = [
    Card(
        title="Generators",
        subtitle="Explode a tile into many tiles or bands",
        color="#D49213", tint="#FBEED1",
        fns=[
            "rst_maketiles", "rst_retile", "rst_tooverlappingtiles",
            "rst_separatebands", "rst_h3_tessellate",
        ],
    ),
    Card(
        title="Operations",
        subtitle="Transform pixels, geometry, format, and coordinates",
        color="#1F8F5A", tint="#DDF1E6",
        sections=[
            Section("Transform", [
                "rst_clip", "rst_transform", "rst_merge",
                "rst_asformat", "rst_updatetype",
                "rst_resample", "rst_resample_to_res", "rst_resample_to_size",
                "rst_setsrid", "rst_band",
            ]),
            Section("Compute", [
                "rst_filter", "rst_convolve",
                "rst_mapalgebra", "rst_combineavg",
                "rst_derivedband", "rst_initnodata",
                "rst_threshold", "rst_fillnodata", "rst_proximity", "rst_contour",
            ]),
            Section("Optimise", [
                "rst_buildoverviews", "rst_cog_convert",
            ]),
            Section("Coordinates", [
                "rst_rastertoworldcoord",
                "rst_rastertoworldcoordx", "rst_rastertoworldcoordy",
                "rst_worldtorastercoord",
                "rst_worldtorastercoordx", "rst_worldtorastercoordy",
            ]),
            Section("Validity", ["rst_isempty", "rst_tryopen"]),
        ],
    ),
    Card(
        title="Vector-Raster Bridge",
        subtitle="Convert between vector geometries and raster tiles",
        color="#6B48A8", tint="#EEE8F8",
        fns=[
            "rst_rasterize", "rst_polygonize",
            "rst_dtmfromgeoms", "rst_gridfrompoints",
        ],
    ),
    Card(
        title="H3 Grid",
        subtitle="Aggregate raster values onto H3 cells",
        color="#0F8E8B", tint="#D5ECEC",
        fns=[
            "rst_h3_rastertogridavg", "rst_h3_rastertogridcount",
            "rst_h3_rastertogridmax", "rst_h3_rastertogridmin",
            "rst_h3_rastertogridmedian",
        ],
    ),
    Card(
        title="Quadbin Grid",
        subtitle="Aggregate raster values onto Quadbin cells",
        color="#1571A8", tint="#DFF0FA",
        fns=[
            "rst_quadbin_rastertogridavg", "rst_quadbin_rastertogridcount",
            "rst_quadbin_rastertogridmax", "rst_quadbin_rastertogridmin",
            "rst_quadbin_rastertogridmedian",
        ],
    ),
    Card(
        title="Web-Mercator Tile Output",
        subtitle="Reproject and slice rasters to XYZ/web-mercator tiles",
        color="#D44E12", tint="#FAECE3",
        fns=["rst_to_webmercator", "rst_tilexyz", "rst_xyzpyramid"],
    ),
]

# --- Layout -------------------------------------------------------------------

PAD = 36
COL_GAP = 24
CARD_GAP = 22
TITLE_BLOCK_H = 110
CARD_W = 660
CANVAS_W = PAD * 2 + CARD_W * 2 + COL_GAP

CARD_PAD_X = 22
CARD_PAD_TOP = 22
CARD_HEAD_GAP = 14
SECTION_LABEL_H = 22
SECTION_GAP = 14
PILL_H = 28
PILL_GAP_X = 8
PILL_GAP_Y = 8
PILL_PAD_X = 12
CHAR_W = 8.0     # approx for 13px monospace (SF Mono / Menlo); tuned visually
PILL_FONT = 13

def pill_width(text):
    return max(int(len(text) * CHAR_W) + PILL_PAD_X * 2, 60)

def layout_pills(fns, max_w):
    """Return (rows, total_height) where rows = list of [(x, w, text), ...]."""
    rows = []
    cur = []
    cur_w = 0
    for f in fns:
        w = pill_width(f)
        needed = w if not cur else cur_w + PILL_GAP_X + w
        if cur and needed > max_w:
            rows.append(cur)
            cur = [(0, w, f)]
            cur_w = w
        else:
            x = 0 if not cur else cur_w + PILL_GAP_X
            cur.append((x, w, f))
            cur_w = needed
    if cur:
        rows.append(cur)
    h = len(rows) * PILL_H + max(0, len(rows) - 1) * PILL_GAP_Y
    return rows, h

def card_height(card):
    inner_w = CARD_W - 2 * CARD_PAD_X
    h = CARD_PAD_TOP + 28 + 6 + 16 + CARD_HEAD_GAP   # title + subtitle + gap
    if card.sections:
        for i, sec in enumerate(card.sections):
            if i > 0:
                h += SECTION_GAP
            h += SECTION_LABEL_H
            _, ph = layout_pills(sec.fns, inner_w)
            h += ph
    else:
        _, ph = layout_pills(card.fns, inner_w)
        h += ph
    h += CARD_PAD_TOP   # bottom padding
    return h

def column_height(cards):
    return sum(card_height(c) for c in cards) + (len(cards) - 1) * CARD_GAP

# --- Render -------------------------------------------------------------------

def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def render_pill(x, y, w, text, color, tint):
    return (
        f'<rect x="{x:.1f}" y="{y:.1f}" rx="13" ry="13" '
        f'width="{w}" height="{PILL_H}" fill="{tint}" stroke="{color}" '
        f'stroke-opacity="0.35" stroke-width="1"/>'
        f'<text x="{x + w/2:.1f}" y="{y + PILL_H/2 + 4:.1f}" '
        f'text-anchor="middle" font-family="ui-monospace, SFMono-Regular, Menlo, Consolas, monospace" '
        f'font-size="{PILL_FONT}" fill="{color}" font-weight="600">{esc(text)}</text>'
    )

def render_card(x, y, card):
    inner_w = CARD_W - 2 * CARD_PAD_X
    h = card_height(card)
    out = []

    out.append(
        f'<rect x="{x}" y="{y}" rx="14" ry="14" width="{CARD_W}" height="{h}" '
        f'fill="#FFFFFF" stroke="#E5E7EB" stroke-width="1" '
        f'filter="url(#card-shadow)"/>'
    )
    # Top accent stripe - rounded only on top corners
    r = 14
    stripe_h = 5
    out.append(
        f'<path d="M {x} {y + r} '
        f'A {r} {r} 0 0 1 {x + r} {y} '
        f'H {x + CARD_W - r} '
        f'A {r} {r} 0 0 1 {x + CARD_W} {y + r} '
        f'V {y + stripe_h} '
        f'H {x} Z" fill="{card.color}"/>'
    )

    # Title block
    tx = x + CARD_PAD_X
    ty = y + CARD_PAD_TOP + 18
    # Function count badge to the right
    total = len(card.fns) if not card.sections else sum(len(s.fns) for s in card.sections)
    badge_text = f"{total} fns"
    badge_w = int(len(badge_text) * 7) + 22
    badge_x = x + CARD_W - CARD_PAD_X - badge_w
    badge_y = y + CARD_PAD_TOP + 4
    out.append(
        f'<rect x="{badge_x}" y="{badge_y}" rx="11" ry="11" '
        f'width="{badge_w}" height="22" fill="{card.color}"/>'
        f'<text x="{badge_x + badge_w/2}" y="{badge_y + 15.5}" '
        f'text-anchor="middle" font-family="Inter, -apple-system, system-ui, sans-serif" '
        f'font-size="12" font-weight="700" fill="#FFFFFF">{esc(badge_text)}</text>'
    )

    out.append(
        f'<text x="{tx}" y="{ty}" font-family="Inter, -apple-system, system-ui, sans-serif" '
        f'font-size="20" font-weight="800" fill="#1B3139">{esc(card.title)}</text>'
    )
    out.append(
        f'<text x="{tx}" y="{ty + 20}" font-family="Inter, -apple-system, system-ui, sans-serif" '
        f'font-size="12" fill="#5A6878">{esc(card.subtitle)}</text>'
    )

    cy = ty + 20 + CARD_HEAD_GAP + 8

    if card.sections:
        for i, sec in enumerate(card.sections):
            if i > 0:
                cy += SECTION_GAP
            out.append(
                f'<text x="{tx}" y="{cy + 14}" font-family="Inter, -apple-system, system-ui, sans-serif" '
                f'font-size="11" font-weight="700" fill="#7A8794" letter-spacing="1.2">'
                f'{esc(sec.label.upper())}</text>'
            )
            cy += SECTION_LABEL_H
            rows, ph = layout_pills(sec.fns, inner_w)
            for r_idx, row in enumerate(rows):
                row_y = cy + r_idx * (PILL_H + PILL_GAP_Y)
                for px, pw, name in row:
                    out.append(render_pill(tx + px, row_y, pw, name, card.color, card.tint))
            cy += ph
    else:
        rows, ph = layout_pills(card.fns, inner_w)
        for r_idx, row in enumerate(rows):
            row_y = cy + r_idx * (PILL_H + PILL_GAP_Y)
            for px, pw, name in row:
                out.append(render_pill(tx + px, row_y, pw, name, card.color, card.tint))

    return "\n".join(out), h


def render():
    # Compute total canvas height from taller column.
    h_left = column_height(CARDS_LEFT)
    h_right = column_height(CARDS_RIGHT)
    body_h = max(h_left, h_right)
    canvas_h = PAD + TITLE_BLOCK_H + body_h + PAD

    parts = []
    parts.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {CANVAS_W} {canvas_h}" '
        f'width="{CANVAS_W}" height="{canvas_h}" '
        f'style="font-family: Inter, -apple-system, system-ui, sans-serif;">'
    )
    # Defs
    parts.append(dedent('''\
        <defs>
          <filter id="card-shadow" x="-5%" y="-5%" width="110%" height="115%">
            <feDropShadow dx="0" dy="2" stdDeviation="6" flood-color="#0F1B2A" flood-opacity="0.08"/>
          </filter>
          <linearGradient id="bg" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0" stop-color="#FAFBFC"/>
            <stop offset="1" stop-color="#F1F4F8"/>
          </linearGradient>
        </defs>
        '''))
    # Background
    parts.append(f'<rect x="0" y="0" width="{CANVAS_W}" height="{canvas_h}" fill="url(#bg)"/>')

    # Header block
    parts.append(
        f'<text x="{PAD}" y="{PAD + 28}" font-size="30" font-weight="800" fill="#0F1B2A">'
        f'GeoBrix &#183; RasterX'
        f'</text>'
    )
    parts.append(
        f'<text x="{PAD}" y="{PAD + 56}" font-size="15" fill="#3F4D5E">'
        f'108 SQL functions for raster data on Spark &#8212; registered as '
        f'<tspan font-family="ui-monospace, SFMono-Regular, Menlo, monospace" '
        f'font-weight="700" fill="#0F1B2A">gbx_rst_*</tspan>'
        f' &#183; also available in Python &amp; Scala as '
        f'<tspan font-family="ui-monospace, SFMono-Regular, Menlo, monospace" '
        f'font-weight="700" fill="#0F1B2A">rst_*</tspan>'
        f'</text>'
    )
    # Version pill (top-right)
    pill_text = "v0.4.0  *  Beta"
    pw = int(len(pill_text) * 6.8) + 24
    parts.append(
        f'<rect x="{CANVAS_W - PAD - pw}" y="{PAD + 8}" rx="13" ry="13" '
        f'width="{pw}" height="26" fill="#0F1B2A"/>'
        f'<text x="{CANVAS_W - PAD - pw/2}" y="{PAD + 26}" text-anchor="middle" '
        f'font-size="12" font-weight="700" fill="#FFFFFF">{pill_text}</text>'
    )

    # Cards
    body_y = PAD + TITLE_BLOCK_H
    # Left column
    cy = body_y
    for c in CARDS_LEFT:
        s, h = render_card(PAD, cy, c)
        parts.append(s)
        cy += h + CARD_GAP
    # Right column
    cy = body_y
    for c in CARDS_RIGHT:
        s, h = render_card(PAD + CARD_W + COL_GAP, cy, c)
        parts.append(s)
        cy += h + CARD_GAP

    # Footer
    parts.append(
        f'<text x="{PAD}" y="{canvas_h - 14}" font-size="11" fill="#7A8794">'
        f'databrickslabs/geobrix &#183; DBR 17.3 / 18 LTS &#183; Scala 2.13.16 / Spark 4.0&#8211;4.1 / Python 3.12'
        f'</text>'
    )
    parts.append(
        f'<text x="{CANVAS_W - PAD}" y="{canvas_h - 14}" text-anchor="end" '
        f'font-size="11" fill="#7A8794">'
        f'docs/api/rasterx-functions'
        f'</text>'
    )

    parts.append('</svg>')
    return "\n".join(parts)


def render_landscape():
    """Render a 3-column landscape variant — better aspect ratio for 16:9 slides.

    All cards from CARDS_LEFT + CARDS_RIGHT are distributed across 3 columns
    using a greedy height-balance algorithm: each card is placed into the
    currently shortest column (by cumulative card_height + CARD_GAP).
    """
    NCOLS = 3
    LANDSCAPE_W = PAD * 2 + CARD_W * NCOLS + COL_GAP * (NCOLS - 1)

    all_cards = CARDS_LEFT + CARDS_RIGHT

    # Greedy height-balanced column assignment.
    # col_cards[i] = list of cards in column i
    # col_h[i] = running pixel height of column i (cards + gaps so far)
    col_cards = [[] for _ in range(NCOLS)]
    col_h = [0] * NCOLS

    for card in all_cards:
        ch = card_height(card)
        # Find the column with minimum current height
        min_col = col_h.index(min(col_h))
        if col_cards[min_col]:
            col_h[min_col] += CARD_GAP
        col_h[min_col] += ch
        col_cards[min_col].append(card)

    body_h = max(col_h)
    canvas_h = PAD + TITLE_BLOCK_H + body_h + PAD

    parts = []
    parts.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {LANDSCAPE_W} {canvas_h}" '
        f'width="{LANDSCAPE_W}" height="{canvas_h}" '
        f'style="font-family: Inter, -apple-system, system-ui, sans-serif;">'
    )
    # Defs (identical structure to portrait)
    parts.append(dedent('''\
        <defs>
          <filter id="card-shadow" x="-5%" y="-5%" width="110%" height="115%">
            <feDropShadow dx="0" dy="2" stdDeviation="6" flood-color="#0F1B2A" flood-opacity="0.08"/>
          </filter>
          <linearGradient id="bg" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0" stop-color="#FAFBFC"/>
            <stop offset="1" stop-color="#F1F4F8"/>
          </linearGradient>
        </defs>
        '''))
    # Background
    parts.append(f'<rect x="0" y="0" width="{LANDSCAPE_W}" height="{canvas_h}" fill="url(#bg)"/>')

    # Header block (same title, same subtitle, same version pill)
    parts.append(
        f'<text x="{PAD}" y="{PAD + 28}" font-size="30" font-weight="800" fill="#0F1B2A">'
        f'GeoBrix &#183; RasterX'
        f'</text>'
    )
    parts.append(
        f'<text x="{PAD}" y="{PAD + 56}" font-size="15" fill="#3F4D5E">'
        f'108 SQL functions for raster data on Spark &#8212; registered as '
        f'<tspan font-family="ui-monospace, SFMono-Regular, Menlo, monospace" '
        f'font-weight="700" fill="#0F1B2A">gbx_rst_*</tspan>'
        f' &#183; also available in Python &amp; Scala as '
        f'<tspan font-family="ui-monospace, SFMono-Regular, Menlo, monospace" '
        f'font-weight="700" fill="#0F1B2A">rst_*</tspan>'
        f'</text>'
    )
    # Version pill (top-right)
    pill_text = "v0.4.0  *  Beta"
    pw = int(len(pill_text) * 6.8) + 24
    parts.append(
        f'<rect x="{LANDSCAPE_W - PAD - pw}" y="{PAD + 8}" rx="13" ry="13" '
        f'width="{pw}" height="26" fill="#0F1B2A"/>'
        f'<text x="{LANDSCAPE_W - PAD - pw/2}" y="{PAD + 26}" text-anchor="middle" '
        f'font-size="12" font-weight="700" fill="#FFFFFF">{pill_text}</text>'
    )

    # Cards — 3 columns
    body_y = PAD + TITLE_BLOCK_H
    for col_i, cards in enumerate(col_cards):
        col_x = PAD + col_i * (CARD_W + COL_GAP)
        cy = body_y
        for card in cards:
            s, h = render_card(col_x, cy, card)
            parts.append(s)
            cy += h + CARD_GAP

    # Footer
    parts.append(
        f'<text x="{PAD}" y="{canvas_h - 14}" font-size="11" fill="#7A8794">'
        f'databrickslabs/geobrix &#183; DBR 17.3 / 18 LTS &#183; Scala 2.13.16 / Spark 4.0&#8211;4.1 / Python 3.12'
        f'</text>'
    )
    parts.append(
        f'<text x="{LANDSCAPE_W - PAD}" y="{canvas_h - 14}" text-anchor="end" '
        f'font-size="11" fill="#7A8794">'
        f'docs/api/rasterx-functions'
        f'</text>'
    )

    parts.append('</svg>')
    return "\n".join(parts), canvas_h


if __name__ == "__main__":
    import os
    import sys

    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_portrait = os.path.join(script_dir, "rasterx-function-categories.svg")
    default_landscape = os.path.join(script_dir, "rasterx-function-categories_landscape.svg")

    # Portrait (unchanged behaviour: optional explicit path as first arg)
    out_portrait = sys.argv[1] if len(sys.argv) > 1 else default_portrait
    with open(out_portrait, "w") as f:
        f.write(render())
    print(f"wrote {out_portrait}")
    portrait_h = PAD + TITLE_BLOCK_H + max(column_height(CARDS_LEFT), column_height(CARDS_RIGHT)) + PAD
    print(f"portrait canvas: {CANVAS_W} x {portrait_h}  "
          f"(use --window-size={CANVAS_W},{portrait_h} for Chrome — it grows as functions are added)")

    # Landscape (always next to portrait)
    landscape_svg, landscape_h = render_landscape()
    with open(default_landscape, "w") as f:
        f.write(landscape_svg)
    print(f"wrote {default_landscape}")
    print(f"landscape canvas: {PAD * 2 + CARD_W * 3 + COL_GAP * 2} x {landscape_h}  "
          f"(use --window-size={PAD * 2 + CARD_W * 3 + COL_GAP * 2},{landscape_h} for Chrome)")
