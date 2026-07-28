#!/usr/bin/env python3
"""Generate the RasterX tile-structure infographic SVG.

Re-render after a change to the tile schema:

    python3 resources/images/generators/rasterx-tile-structure.py
    # then rasterize to PNG (used by docs/api/tile-structure.mdx and slides):
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \\
        --headless --disable-gpu --hide-scrollbars \\
        --force-device-scale-factor=2 --window-size=1480,880 \\
        --screenshot=resources/images/diagrams/rasterx/rasterx-tile-structure.png \\
        resources/images/diagrams/rasterx/rasterx-tile-structure.svg
"""
from dataclasses import dataclass
from textwrap import dedent

# --- Palette (shared with rasterx-function-categories.py) ---------------------

C_INK         = "#0F1B2A"
C_INK_2       = "#1B3139"
C_MUTED       = "#3F4D5E"
C_MUTED_2     = "#5A6878"
C_MUTED_3     = "#7A8794"
C_BORDER      = "#E5E7EB"

# Field accents
ACCENT_CELLID  = "#0F8E8B"; TINT_CELLID  = "#D5ECEC"   # teal — matches H3 Grid card
ACCENT_RASTER  = "#E04E2A"; TINT_RASTER  = "#FCE9E2"   # orange — matches Constructors
ACCENT_META    = "#1F6FB5"; TINT_META    = "#E3EEF8"   # blue — matches Accessors

# --- Schema -------------------------------------------------------------------

@dataclass
class Field:
    name: str
    typ: str
    required: bool
    glyph: str
    purpose: str
    example: str
    accent: str
    tint: str

FIELDS = [
    Field(
        name="cellid",
        typ="bigint",
        required=False,
        glyph="hex",
        purpose="Grid cell identifier for tessellated rasters",
        example="617733604892049407   ·   null when not tessellated",
        accent=ACCENT_CELLID, tint=TINT_CELLID,
    ),
    Field(
        name="raster",
        typ="binary",
        required=True,
        glyph="grid",
        purpose="Self-contained raster payload (full file in memory)",
        example="<GeoTIFF · 1.24 MB>",
        accent=ACCENT_RASTER, tint=TINT_RASTER,
    ),
    Field(
        name="metadata",
        typ="map<string,string>",
        required=False,
        glyph="kv",
        purpose="Driver, extension, size, and format-specific keys",
        example='{driver→"GTiff", extension→".tif", size→"1300312"}',
        accent=ACCENT_META, tint=TINT_META,
    ),
]

# Example tiles for the comparison row
NON_TESS_EXAMPLE = [
    ("cellid",   "null"),
    ("raster",   "<bytes · 1.24 MB>"),
    ("metadata", '{driver→"GTiff",  extension→".tif",  size→"1300312"}'),
]
TESS_EXAMPLE = [
    ("cellid",   "617733604892049407"),
    ("raster",   "<bytes · 184 KB>     ← clipped to cell"),
    ("metadata", '{driver→"GTiff",  extension→".tif",  size→"188416",'),
    ("",         ' RASTERX_CELL_ID→"617733604892049407"}'),
]

# --- Layout -------------------------------------------------------------------

PAD          = 36
CANVAS_W     = 1480

HEADER_H     = 100
HERO_TOP_GAP = 18
HERO_PAD     = 22
HERO_TITLE_H = 56
FIELD_GAP    = 18
FIELD_H      = 290

CMP_TOP_GAP   = 22
CMP_LABEL_H   = 26
CMP_PAD       = 22
CMP_HEADER_H  = 60
CMP_ROW_H     = 30
CMP_BODY_PAD  = 14

FOOTER_H     = 30

# --- Helpers ------------------------------------------------------------------

def esc(s):
    return (s.replace("&", "&amp;")
             .replace("<", "&lt;")
             .replace(">", "&gt;"))

def text(x, y, s, *, size=13, weight=400, fill=C_INK,
         family="Inter, -apple-system, system-ui, sans-serif",
         anchor="start", letter_spacing=None):
    ls = f' letter-spacing="{letter_spacing}"' if letter_spacing else ""
    return (f'<text x="{x}" y="{y}" font-family="{family}" '
            f'font-size="{size}" font-weight="{weight}" fill="{fill}" '
            f'text-anchor="{anchor}"{ls}>{esc(s)}</text>')

def mono(x, y, s, *, size=13, weight=500, fill=C_INK, anchor="start"):
    return text(x, y, s, size=size, weight=weight, fill=fill, anchor=anchor,
                family="ui-monospace, SFMono-Regular, Menlo, Consolas, monospace")

def card(x, y, w, h, *, fill="#FFFFFF", stroke=C_BORDER, r=14, shadow=True):
    flt = ' filter="url(#card-shadow)"' if shadow else ""
    return (f'<rect x="{x}" y="{y}" rx="{r}" ry="{r}" width="{w}" height="{h}" '
            f'fill="{fill}" stroke="{stroke}" stroke-width="1"{flt}/>')

def top_stripe(x, y, w, color, *, r=14, h=5):
    """Rounded-top accent stripe sitting on top of a card."""
    return (f'<path d="M {x} {y + r} '
            f'A {r} {r} 0 0 1 {x + r} {y} '
            f'H {x + w - r} '
            f'A {r} {r} 0 0 1 {x + w} {y + r} '
            f'V {y + h} '
            f'H {x} Z" fill="{color}"/>')

def chip(x, y, txt, *, fg=C_INK, bg="#F1F4F8", border=None, mono_font=False, h=22):
    """Small rounded-rect chip with text. Returns (svg, width)."""
    char_w = 7.2 if mono_font else 6.8
    pad_x = 12
    w = int(len(txt) * char_w) + pad_x * 2
    family = ("ui-monospace, SFMono-Regular, Menlo, Consolas, monospace"
              if mono_font else "Inter, -apple-system, system-ui, sans-serif")
    bs = f' stroke="{border}" stroke-width="1"' if border else ""
    svg = (f'<rect x="{x}" y="{y}" rx="{h/2:.0f}" ry="{h/2:.0f}" '
           f'width="{w}" height="{h}" fill="{bg}"{bs}/>'
           f'<text x="{x + w/2}" y="{y + h/2 + 4}" '
           f'text-anchor="middle" font-family="{family}" '
           f'font-size="12" font-weight="700" fill="{fg}">{esc(txt)}</text>')
    return svg, w

# --- Glyphs (SVG fragments) ---------------------------------------------------

def glyph_hex(cx, cy, color, tint):
    """An H3-style hex cell, with a small interior dot (point in cell)."""
    import math
    R = 36
    pts = []
    for i in range(6):
        a = math.radians(60 * i - 30)  # flat-top hex looks better here
        pts.append(f"{cx + R*math.cos(a):.1f},{cy + R*math.sin(a):.1f}")
    return (f'<polygon points="{" ".join(pts)}" fill="{tint}" '
            f'stroke="{color}" stroke-width="2"/>'
            f'<circle cx="{cx}" cy="{cy}" r="3.5" fill="{color}"/>')

def glyph_grid(cx, cy, color, tint):
    """A 4x4 raster pixel grid with subtle value variation."""
    cells = 4
    s = 16          # cell size
    span = cells * s
    x0 = cx - span / 2
    y0 = cy - span / 2
    parts = [f'<rect x="{x0:.1f}" y="{y0:.1f}" width="{span}" height="{span}" '
             f'fill="{tint}" stroke="{color}" stroke-width="2"/>']
    # Pixel values — gradient-like alpha
    vals = [
        [0.30, 0.55, 0.40, 0.20],
        [0.55, 0.85, 0.65, 0.35],
        [0.45, 0.70, 0.95, 0.55],
        [0.20, 0.40, 0.60, 0.30],
    ]
    for r in range(cells):
        for c in range(cells):
            parts.append(
                f'<rect x="{x0 + c*s:.1f}" y="{y0 + r*s:.1f}" '
                f'width="{s}" height="{s}" fill="{color}" '
                f'fill-opacity="{vals[r][c]:.2f}"/>'
            )
    # Inner gridlines
    for i in range(1, cells):
        parts.append(
            f'<line x1="{x0 + i*s:.1f}" y1="{y0:.1f}" '
            f'x2="{x0 + i*s:.1f}" y2="{y0 + span:.1f}" '
            f'stroke="#FFFFFF" stroke-width="1.2" stroke-opacity="0.7"/>'
        )
        parts.append(
            f'<line x1="{x0:.1f}" y1="{y0 + i*s:.1f}" '
            f'x2="{x0 + span:.1f}" y2="{y0 + i*s:.1f}" '
            f'stroke="#FFFFFF" stroke-width="1.2" stroke-opacity="0.7"/>'
        )
    return "".join(parts)

def glyph_kv(cx, cy, color, tint):
    """Three key→value rows."""
    w_total = 96
    h_total = 70
    x0 = cx - w_total / 2
    y0 = cy - h_total / 2
    parts = [f'<rect x="{x0:.1f}" y="{y0:.1f}" rx="8" ry="8" '
             f'width="{w_total}" height="{h_total}" '
             f'fill="{tint}" stroke="{color}" stroke-width="2"/>']
    rows = [("driver", "GTiff"), ("ext", ".tif"), ("size", "…")]
    row_h = h_total / 3
    for i, (k, v) in enumerate(rows):
        ry = y0 + (i + 0.5) * row_h
        parts.append(
            f'<text x="{x0 + 10}" y="{ry + 4:.1f}" '
            f'font-family="ui-monospace, SFMono-Regular, Menlo, monospace" '
            f'font-size="10" font-weight="700" fill="{color}">{k}</text>'
        )
        parts.append(
            f'<text x="{x0 + w_total - 10}" y="{ry + 4:.1f}" text-anchor="end" '
            f'font-family="ui-monospace, SFMono-Regular, Menlo, monospace" '
            f'font-size="10" fill="{color}">{v}</text>'
        )
        if i < len(rows) - 1:
            ly = y0 + (i + 1) * row_h
            parts.append(
                f'<line x1="{x0 + 6:.1f}" y1="{ly:.1f}" '
                f'x2="{x0 + w_total - 6:.1f}" y2="{ly:.1f}" '
                f'stroke="{color}" stroke-opacity="0.25" stroke-width="1"/>'
            )
    return "".join(parts)

GLYPH_FNS = {"hex": glyph_hex, "grid": glyph_grid, "kv": glyph_kv}

# --- Components ---------------------------------------------------------------

def render_field_card(x, y, w, h, fld):
    out = []
    out.append(card(x, y, w, h))
    out.append(top_stripe(x, y, w, fld.accent))

    # Glyph (top-right area)
    glyph_cx = x + w - 70
    glyph_cy = y + 80
    out.append(GLYPH_FNS[fld.glyph](glyph_cx, glyph_cy, fld.accent, fld.tint))

    # Field name (mono, bold, large)
    name_x = x + HERO_PAD
    name_y = y + 50
    out.append(mono(name_x, name_y, fld.name, size=24, weight=700, fill=C_INK))

    # Type chip on its own line
    type_y = name_y + 22
    type_svg, type_w = chip(name_x, type_y, fld.typ,
                            fg=fld.accent, bg=fld.tint, mono_font=True)
    out.append(type_svg)

    # Required / Optional badge
    badge_text = "required" if fld.required else "nullable"
    badge_bg = fld.accent if fld.required else "#FFFFFF"
    badge_fg = "#FFFFFF" if fld.required else fld.accent
    badge_border = None if fld.required else fld.accent
    badge_svg, _ = chip(name_x + type_w + 8, type_y, badge_text,
                        fg=badge_fg, bg=badge_bg, border=badge_border)
    out.append(badge_svg)

    # Purpose (wrap to two lines if needed)
    purpose_y = y + 150
    out.append(text(name_x, purpose_y, fld.purpose,
                    size=14, weight=500, fill=C_MUTED))

    # Example label
    ex_label_y = y + 200
    out.append(text(name_x, ex_label_y, "EXAMPLE",
                    size=10, weight=700, fill=C_MUTED_3, letter_spacing="1.4"))

    # Example value box
    ex_box_y = ex_label_y + 10
    ex_box_h = 50
    out.append(
        f'<rect x="{name_x}" y="{ex_box_y}" rx="8" ry="8" '
        f'width="{w - 2*HERO_PAD}" height="{ex_box_h}" '
        f'fill="{fld.tint}" fill-opacity="0.55" '
        f'stroke="{fld.accent}" stroke-opacity="0.25" stroke-width="1"/>'
    )
    # Wrap example into up to two lines
    ex = fld.example
    line1, line2 = ex, ""
    if len(ex) > 32:
        # Try to break at a separator near the middle
        for sep in ["   ·   ", ", ", " "]:
            mid = len(ex) // 2
            idx = ex.rfind(sep, 0, mid + 12)
            if idx > 0:
                line1 = ex[:idx].rstrip()
                line2 = ex[idx + len(sep):].lstrip() if sep == " " else ex[idx + len(sep):].lstrip()
                if sep == ", " and not line2.startswith("{"):
                    line2 = line2  # keep
                break
    out.append(mono(name_x + 12, ex_box_y + 22, line1,
                    size=12, weight=600, fill=fld.accent))
    if line2:
        out.append(mono(name_x + 12, ex_box_y + 38, line2,
                        size=12, weight=600, fill=fld.accent))

    return "".join(out)


def render_hero(x, y, w):
    field_w = (w - 2 * HERO_PAD - 2 * FIELD_GAP) // 3
    h = HERO_TITLE_H + FIELD_H + 2 * HERO_PAD
    out = [card(x, y, w, h)]
    out.append(top_stripe(x, y, w, C_INK_2))

    # Eyebrow + title row
    out.append(text(x + HERO_PAD, y + 30, "TILE SCHEMA",
                    size=11, weight=700, fill=C_MUTED_3, letter_spacing="1.6"))
    out.append(text(x + HERO_PAD, y + 56,
                    "A typed struct with 3 fields — not a binary blob",
                    size=18, weight=700, fill=C_INK))

    # Inline mono signature on the right
    sig = "struct<cellid: bigint, raster: binary, metadata: map<string,string>>"
    out.append(mono(x + w - HERO_PAD, y + 56, sig,
                    size=13, weight=600, fill=C_MUTED, anchor="end"))

    # Field cards
    fy = y + HERO_PAD + HERO_TITLE_H
    fx = x + HERO_PAD
    for i, fld in enumerate(FIELDS):
        out.append(render_field_card(fx, fy, field_w, FIELD_H, fld))
        fx += field_w + FIELD_GAP

    return "".join(out), h


def render_example_tile(x, y, w, h, *, label, sublabel, accent, tint, rows, cellid_null):
    out = [card(x, y, w, h)]
    out.append(top_stripe(x, y, w, accent))

    # Header
    out.append(text(x + CMP_PAD, y + 32, label,
                    size=18, weight=800, fill=C_INK))
    out.append(mono(x + CMP_PAD, y + 52, sublabel,
                    size=12, weight=600, fill=accent))

    # Constructor chip on the right
    chip_label = "cellid = null" if cellid_null else "cellid = H3"
    chip_bg = "#FFFFFF"
    chip_svg, chip_w = chip(x + w - CMP_PAD - 0, y + 28, chip_label,
                            fg=accent, bg=chip_bg, border=accent, mono_font=True)
    # right-align by re-emitting at adjusted x
    out.append(
        f'<g transform="translate({-chip_w}, 0)">' + chip_svg + "</g>"
    )

    # Body rows: key → value, monospaced
    body_y = y + CMP_HEADER_H + CMP_BODY_PAD
    key_x = x + CMP_PAD
    val_x = x + CMP_PAD + 110
    for i, (k, v) in enumerate(rows):
        ry = body_y + i * CMP_ROW_H + 18
        if k:
            out.append(mono(key_x, ry, f"{k}:",
                            size=13, weight=700, fill=C_MUTED_2))
        out.append(mono(val_x, ry, v,
                        size=13, weight=600, fill=C_INK))
    return "".join(out)


# --- Main render --------------------------------------------------------------

def render():
    inner_w = CANVAS_W - 2 * PAD

    # Heights
    hero_h_estimated = HERO_TITLE_H + FIELD_H + 2 * HERO_PAD
    cmp_card_h = CMP_HEADER_H + CMP_BODY_PAD * 2 + CMP_ROW_H * max(len(NON_TESS_EXAMPLE), len(TESS_EXAMPLE))
    canvas_h = (PAD + HEADER_H + HERO_TOP_GAP + hero_h_estimated
                + CMP_TOP_GAP + CMP_LABEL_H + cmp_card_h + FOOTER_H + PAD)

    parts = []
    parts.append(
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'viewBox="0 0 {CANVAS_W} {canvas_h}" '
        f'width="{CANVAS_W}" height="{canvas_h}" '
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
    parts.append(f'<rect x="0" y="0" width="{CANVAS_W}" height="{canvas_h}" fill="url(#bg)"/>')

    # ---- Header ----
    parts.append(text(PAD, PAD + 28, "GeoBrix · Tile Structure",
                      size=30, weight=800, fill=C_INK))

    sub = ('A RasterX <tspan font-family="ui-monospace, SFMono-Regular, Menlo, monospace" '
           f'font-weight="700" fill="{C_INK}">tile</tspan> is a typed struct, '
           'carrying raster bytes alongside grid-cell and format metadata through every operator')
    parts.append(
        f'<text x="{PAD}" y="{PAD + 56}" font-size="15" fill="{C_MUTED}">{sub}</text>'
    )

    # Version pill
    pill_text = "v0.4.3  ·  Beta"
    pw = int(len(pill_text) * 6.8) + 24
    parts.append(
        f'<rect x="{CANVAS_W - PAD - pw}" y="{PAD + 8}" rx="13" ry="13" '
        f'width="{pw}" height="26" fill="{C_INK}"/>'
        f'<text x="{CANVAS_W - PAD - pw/2}" y="{PAD + 26}" text-anchor="middle" '
        f'font-size="12" font-weight="700" fill="#FFFFFF">{pill_text}</text>'
    )

    # ---- Hero ----
    hero_y = PAD + HEADER_H + HERO_TOP_GAP
    hero_svg, hero_h = render_hero(PAD, hero_y, inner_w)
    parts.append(hero_svg)

    # ---- Comparison row ----
    cmp_label_y = hero_y + hero_h + CMP_TOP_GAP
    parts.append(text(PAD, cmp_label_y + 16, "EXAMPLE TILES",
                      size=11, weight=700, fill=C_MUTED_3, letter_spacing="1.6"))
    parts.append(text(CANVAS_W - PAD, cmp_label_y + 16,
                      "produced by constructors / readers vs. tessellation",
                      size=12, weight=500, fill=C_MUTED_2, anchor="end"))

    cmp_y = cmp_label_y + CMP_LABEL_H
    col_w = (inner_w - FIELD_GAP) // 2

    parts.append(render_example_tile(
        PAD, cmp_y, col_w, cmp_card_h,
        label="Non-tessellated",
        sublabel="rst_fromfile  /  rst_fromcontent  /  GDAL reader",
        accent=ACCENT_RASTER, tint=TINT_RASTER,
        rows=NON_TESS_EXAMPLE, cellid_null=True,
    ))
    parts.append(render_example_tile(
        PAD + col_w + FIELD_GAP, cmp_y, col_w, cmp_card_h,
        label="Tessellated",
        sublabel="rst_h3_tessellate(tile, resolution)",
        accent=ACCENT_CELLID, tint=TINT_CELLID,
        rows=TESS_EXAMPLE, cellid_null=False,
    ))

    # ---- Footer ----
    parts.append(text(PAD, canvas_h - 14,
                      "databrickslabs/geobrix  ·  DBR 17.3 / 18 LTS  ·  Scala 2.13.16 / Spark 4.0–4.1 / Python 3.12",
                      size=11, fill=C_MUTED_3))
    parts.append(text(CANVAS_W - PAD, canvas_h - 14,
                      "docs/api/tile-structure",
                      size=11, fill=C_MUTED_3, anchor="end"))

    parts.append("</svg>")
    return "\n".join(parts)


if __name__ == "__main__":
    import os
    import sys

    default = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "..", "diagrams", "rasterx", "rasterx-tile-structure.svg")
    out = sys.argv[1] if len(sys.argv) > 1 else default
    with open(out, "w") as f:
        f.write(render())
    print(f"wrote {out}")
