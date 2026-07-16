#!/usr/bin/env python3
"""Generate the Vapor-Eyes Lakeflow function-palette infographic (landscape only).

Reuses the card/pill/header/footer rendering system from
``rasterx-function-categories.py`` (imported directly, since that filename has
hyphens) rather than reinventing it. Two family columns sit side by side —
"GeoBrix" (3 cards, cool blue/teal/green accents) on the left, "Databricks
built-in" (2 cards, orange/amber accents) on the right — each with a small
section header + rule above it so the split reads at a glance.

Re-render after the example's function list changes:

    python3 resources/images/generators/vapor-eyes-lakeflow-functions.py
    # writes:
    #   resources/images/diagrams/vapor-eyes/vapor-eyes-lakeflow-functions.svg

Rasterize (2x device scale for a crisp slide). The window height MUST match
the canvas height the script prints, or the bottom row clips:

    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \\
        --headless --disable-gpu --hide-scrollbars \\
        --force-device-scale-factor=2 --window-size=<CANVAS_W>,<canvas_h> \\
        --screenshot=resources/images/diagrams/vapor-eyes/vapor-eyes-lakeflow-functions.png \\
        resources/images/diagrams/vapor-eyes/vapor-eyes-lakeflow-functions.svg
"""
import importlib.util
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_rfc_path = os.path.join(SCRIPT_DIR, "rasterx-function-categories.py")
_spec = importlib.util.spec_from_file_location("rasterx_function_categories", _rfc_path)
rfc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rfc)

Card = rfc.Card
esc = rfc.esc

# --- Layout ---------------------------------------------------------------

PAD = 36
COL_GAP = 24
CARD_GAP = 22
TITLE_BLOCK_H = 110
FAMILY_HEADER_H = 26
HEADER_CARD_GAP = 14

# Two family columns side by side (not the RasterX reference's 3-col grid) —
# narrower cards than the 660px reference so the overall canvas lands near a
# 16:9 slide aspect ratio once the two uneven-height columns are accounted for.
CARD_W = 580
LANDSCAPE_W = PAD * 2 + CARD_W * 2 + COL_GAP

# --- Data: 26 functions used by the Vapor-Eyes Lakeflow example -----------

GEOBRIX_CARDS = [
    Card(
        title="RasterX",
        subtitle="Raster ops (pyrx / gbx_rst_*)",
        color="#1F6FB5", tint="#E3EEF8",
        fns=["rst_clip", "rst_mapalgebra", "rst_summary", "rst_threshold", "rst_h3_tessellate"],
    ),
    Card(
        title="VectorX + PMTiles",
        subtitle="Vector tiles & PMTiles output",
        color="#0F8E8B", tint="#D5ECEC",
        fns=["gbx_st_asmvt_pyramid", "pmtiles_agg"],
    ),
    Card(
        title="Light readers",
        subtitle="Serverless DataSources (no JAR)",
        color="#2E8B57", tint="#E0F4EA",
        fns=["netcdf_gbx", "geojson_gbx", "shapefile_gbx", "gtiff_gbx"],
    ),
]

DATABRICKS_CARDS = [
    Card(
        title="Databricks · Spatial (ST)",
        subtitle="Built-in geometry SQL",
        color="#D49213", tint="#FBEED1",
        fns=[
            "st_point", "st_setsrid", "st_geomfromwkb", "st_geomfromgeojson",
            "st_geomfromtext", "st_asgeojson", "st_centroid", "st_x", "st_y",
            "st_contains", "st_intersects", "st_distancesphere",
        ],
    ),
    Card(
        title="Databricks · H3",
        subtitle="Built-in H3 grid SQL",
        color="#D44E12", tint="#FAECE3",
        fns=["h3_longlatash3", "h3_boundaryaswkb", "h3_centeraswkb"],
    ),
]

# --- Render -----------------------------------------------------------------

def render_family_header(x, y, width, label, color):
    """Small section header: colored chip + bold label + trailing rule."""
    text_w = int(len(label) * 9.2) + 4
    line_x1 = x + 22 + text_w + 14
    return (
        f'<rect x="{x}" y="{y + 3}" width="14" height="14" rx="3" ry="3" fill="{color}"/>'
        f'<text x="{x + 22}" y="{y + 15}" font-family="Inter, -apple-system, system-ui, sans-serif" '
        f'font-size="16" font-weight="800" fill="#1B3139">{esc(label)}</text>'
        f'<line x1="{line_x1:.1f}" y1="{y + 10.5:.1f}" x2="{x + width:.1f}" y2="{y + 10.5:.1f}" '
        f'stroke="#E5E7EB" stroke-width="1.5"/>'
    )


def render():
    # Same CARD_W for both columns — set module CARD_W once before measuring/rendering.
    rfc.CARD_W = CARD_W

    def column_h(cards):
        return sum(rfc.card_height(c) for c in cards) + (len(cards) - 1) * CARD_GAP

    left_h = column_h(GEOBRIX_CARDS)
    right_h = column_h(DATABRICKS_CARDS)
    body_h = max(left_h, right_h)

    canvas_h = (
        PAD + TITLE_BLOCK_H
        + FAMILY_HEADER_H + HEADER_CARD_GAP + body_h
        + PAD
    )

    parts = []
    parts.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {LANDSCAPE_W} {canvas_h}" '
        f'width="{LANDSCAPE_W}" height="{canvas_h}" '
        f'style="font-family: Inter, -apple-system, system-ui, sans-serif;">'
    )
    # Defs (identical structure to the RasterX reference).
    parts.append(
        '<defs>'
        '<filter id="card-shadow" x="-5%" y="-5%" width="110%" height="115%">'
        '<feDropShadow dx="0" dy="2" stdDeviation="6" flood-color="#0F1B2A" flood-opacity="0.08"/>'
        '</filter>'
        '<linearGradient id="bg" x1="0" y1="0" x2="0" y2="1">'
        '<stop offset="0" stop-color="#FAFBFC"/>'
        '<stop offset="1" stop-color="#F1F4F8"/>'
        '</linearGradient>'
        '</defs>'
    )
    parts.append(f'<rect x="0" y="0" width="{LANDSCAPE_W}" height="{canvas_h}" fill="url(#bg)"/>')

    # Header block.
    parts.append(
        f'<text x="{PAD}" y="{PAD + 28}" font-size="30" font-weight="800" fill="#0F1B2A">'
        f'GeoBrix &#183; Vapor-Eyes Lakeflow'
        f'</text>'
    )
    parts.append(
        f'<text x="{PAD}" y="{PAD + 56}" font-size="15" fill="#3F4D5E">'
        f'Every GeoBrix and Databricks built-in spatial/H3 function used in the example'
        f'</text>'
    )
    # Version pill (top-right).
    pill_text = "v0.4.1 • Beta"
    pw = int(len(pill_text) * 6.8) + 24
    parts.append(
        f'<rect x="{LANDSCAPE_W - PAD - pw}" y="{PAD + 8}" rx="13" ry="13" '
        f'width="{pw}" height="26" fill="#0F1B2A"/>'
        f'<text x="{LANDSCAPE_W - PAD - pw / 2}" y="{PAD + 26}" text-anchor="middle" '
        f'font-size="12" font-weight="700" fill="#FFFFFF">{esc(pill_text)}</text>'
    )

    # Left column — GeoBrix.
    body_y = PAD + TITLE_BLOCK_H
    left_x = PAD
    right_x = PAD + CARD_W + COL_GAP
    header_y = body_y
    parts.append(render_family_header(left_x, header_y, CARD_W, "GeoBrix", "#1F6FB5"))
    parts.append(render_family_header(right_x, header_y, CARD_W, "Databricks built-in", "#D49213"))

    cards_y = header_y + FAMILY_HEADER_H + HEADER_CARD_GAP
    rfc.CARD_W = CARD_W

    cy = cards_y
    for card in GEOBRIX_CARDS:
        s, h = rfc.render_card(left_x, cy, card)
        parts.append(s)
        cy += h + CARD_GAP

    # Right column — Databricks built-in.
    cy = cards_y
    for card in DATABRICKS_CARDS:
        s, h = rfc.render_card(right_x, cy, card)
        parts.append(s)
        cy += h + CARD_GAP

    # Footer.
    parts.append(
        f'<text x="{PAD}" y="{canvas_h - 14}" font-size="11" fill="#7A8794">'
        f'databrickslabs/geobrix &#183; Lakeflow SDP + AI/BI'
        f'</text>'
    )
    parts.append(
        f'<text x="{LANDSCAPE_W - PAD}" y="{canvas_h - 14}" text-anchor="end" '
        f'font-size="11" fill="#7A8794">'
        f'notebooks/examples/vapor-eyes/lakeflow'
        f'</text>'
    )

    parts.append('</svg>')
    return "\n".join(parts), canvas_h


if __name__ == "__main__":
    import sys

    default_out = os.path.join(
        SCRIPT_DIR, "..", "diagrams", "vapor-eyes", "vapor-eyes-lakeflow-functions.svg"
    )
    out_path = sys.argv[1] if len(sys.argv) > 1 else default_out

    svg, canvas_h = render()
    with open(out_path, "w") as f:
        f.write(svg)
    print(f"wrote {out_path}")
    print(f"landscape canvas: {LANDSCAPE_W} x {canvas_h}  "
          f"(use --window-size={LANDSCAPE_W},{canvas_h} for Chrome)")
