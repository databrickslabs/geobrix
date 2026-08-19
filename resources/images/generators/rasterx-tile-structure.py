#!/usr/bin/env python3
"""Generate the RasterX tile-structure infographic SVG (v2 tile struct).

Renders the 9-field v2 tile struct as a 3x3 card grid, plus a materialized-vs-
virtual example-tile comparison. Used by docs/api/tile-structure.mdx and slides.

Re-render after a change to the tile schema:

    python3 resources/images/generators/rasterx-tile-structure.py
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \\
        --headless --disable-gpu --hide-scrollbars \\
        --force-device-scale-factor=2 --window-size=1480,900 \\
        --screenshot=resources/images/diagrams/rasterx/rasterx-tile-structure.png \\
        resources/images/diagrams/rasterx/rasterx-tile-structure.svg
    python3 -c "
from PIL import Image, ImageChops
p='resources/images/diagrams/rasterx/rasterx-tile-structure.png'
img=Image.open(p).convert('RGB')
bbox=ImageChops.difference(img, Image.new('RGB', img.size, (255,255,255))).getbbox()
if bbox: img.crop(bbox).save(p)
"
"""
from dataclasses import dataclass
from textwrap import dedent

# --- Palette (shared with the other rasterx generators) -----------------------

C_INK    = "#0F1B2A"
C_INK_2  = "#1B3139"
C_MUTED  = "#3F4D5E"
C_MUTED_2 = "#5A6878"
C_MUTED_3 = "#7A8794"
C_BORDER = "#E5E7EB"

# Field grouping accents:
#   identity  = teal   (cellid)
#   payload   = orange (raster)   — the materialized/virtual axis
#   reference = indigo (path, window)  — set when virtual
#   provenance= violet (clip_polygon, clip_crs, crs)
#   meta      = blue   (metadata)
#   storage   = green  (path_mode) — FILE storage mode indicator
A_IDENT   = "#0F8E8B"; T_IDENT   = "#D5ECEC"
A_RASTER  = "#E04E2A"; T_RASTER  = "#FCE9E2"
A_REF     = "#3B5BDB"; T_REF     = "#E5E9FB"
A_PROV    = "#7A5AA6"; T_PROV    = "#ECE5F5"
A_META    = "#1F6FB5"; T_META    = "#E3EEF8"
A_STORAGE = "#1E7D4B"; T_STORAGE = "#E0F2E9"

# --- Schema -------------------------------------------------------------------

@dataclass
class Field:
    name: str
    typ: str
    nullable: bool
    group: str        # short group label
    purpose: str
    accent: str
    tint: str

FIELDS = [
    Field("cellid", "bigint", True, "IDENTITY",
          "Grid cell id — null unless tessellated", A_IDENT, T_IDENT),
    Field("raster", "binary", True, "PAYLOAD",
          "Encoded raster bytes — null when virtual", A_RASTER, T_RASTER),
    Field("path", "string", True, "REFERENCE",
          "Source path — set when virtual (bytes-free)", A_REF, T_REF),
    Field("path_mode", "string", True, "STORAGE",
          "null / 'external' / 'managed' (FILE storage mode)", A_STORAGE, T_STORAGE),
    Field("window", "struct<col,row,w,h>", True, "REFERENCE",
          "Pixel window to read / already read", A_REF, T_REF),
    Field("clip_polygon", "binary", True, "PROVENANCE",
          "Clip geometry (WKB) — instruction / applied", A_PROV, T_PROV),
    Field("clip_crs", "string", True, "PROVENANCE",
          "CRS of clip_polygon (e.g. EPSG:4326)", A_PROV, T_PROV),
    Field("crs", "string", True, "PROVENANCE",
          "Working / target CRS", A_PROV, T_PROV),
    Field("metadata", "map<string,string>", True, "META",
          "driver, extension, size, format keys", A_META, T_META),
]

# Example tiles: materialized vs virtual
MATERIALIZED_EXAMPLE = [
    ("cellid", "null"),
    ("raster", "<GeoTIFF · 184 KB>"),
    ("path", "null"),
    ("path_mode", "null"),
    ("window", "0,0,512,512"),
    ("clip_polygon", "null"),
    ("clip_crs", "null"),
    ("crs", "EPSG:32633"),
    ("metadata", '{driver→"GTiff", size→"188416"}'),
]
VIRTUAL_EXAMPLE = [
    ("cellid", "null"),
    ("raster", "null            ← bytes-free"),
    ("path", "/Vol/…/scene.cog"),
    ("path_mode", '"external"'),
    ("window", "512,0,512,512"),
    ("clip_polygon", "<WKB polygon>"),
    ("clip_crs", "EPSG:4326"),
    ("crs", "EPSG:32633"),
    ("metadata", '{driver→"GTiff"}'),
]

# --- Layout -------------------------------------------------------------------

PAD        = 36
CANVAS_W   = 1480
HEADER_H   = 100
HERO_TOP_GAP = 16
HERO_PAD   = 22
HERO_TITLE_H = 52
GRID_COLS  = 3
GRID_ROWS  = 3
CARD_GAP   = 16
CARD_H     = 118
CMP_TOP_GAP = 20
CMP_LABEL_H = 24
CMP_PAD    = 20
CMP_HEADER_H = 56
CMP_ROW_H  = 26
CMP_BODY_PAD = 12
FOOTER_H   = 30

# --- Helpers ------------------------------------------------------------------

def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def text(x, y, s, *, size=13, weight=400, fill=C_INK,
         family="Inter, -apple-system, system-ui, sans-serif",
         anchor="start", letter_spacing=None):
    ls = f' letter-spacing="{letter_spacing}"' if letter_spacing else ""
    return (f'<text x="{x}" y="{y}" font-family="{family}" font-size="{size}" '
            f'font-weight="{weight}" fill="{fill}" text-anchor="{anchor}"{ls}>'
            f'{esc(s)}</text>')

def mono(x, y, s, *, size=13, weight=500, fill=C_INK, anchor="start"):
    return text(x, y, s, size=size, weight=weight, fill=fill, anchor=anchor,
                family="ui-monospace, SFMono-Regular, Menlo, Consolas, monospace")

def card(x, y, w, h, *, fill="#FFFFFF", stroke=C_BORDER, r=14, shadow=True, dash=None):
    flt = ' filter="url(#card-shadow)"' if shadow else ""
    ds = f' stroke-dasharray="{dash}"' if dash else ""
    return (f'<rect x="{x}" y="{y}" rx="{r}" ry="{r}" width="{w}" height="{h}" '
            f'fill="{fill}" stroke="{stroke}" stroke-width="1"{ds}{flt}/>')

def top_stripe(x, y, w, color, *, r=14, h=5):
    return (f'<path d="M {x} {y + r} A {r} {r} 0 0 1 {x + r} {y} '
            f'H {x + w - r} A {r} {r} 0 0 1 {x + w} {y + r} V {y + h} H {x} Z" '
            f'fill="{color}"/>')

def chip(x, y, txt, *, fg=C_INK, bg="#F1F4F8", border=None, mono_font=False, h=20, size=11):
    char_w = (0.60 if mono_font else 0.56) * size
    w = int(len(txt) * char_w) + 20
    family = ("ui-monospace, SFMono-Regular, Menlo, Consolas, monospace"
              if mono_font else "Inter, -apple-system, system-ui, sans-serif")
    bs = f' stroke="{border}" stroke-width="1"' if border else ""
    svg = (f'<rect x="{x}" y="{y}" rx="{h/2:.0f}" ry="{h/2:.0f}" width="{w}" '
           f'height="{h}" fill="{bg}"{bs}/>'
           f'<text x="{x + w/2}" y="{y + h/2 + 4}" text-anchor="middle" '
           f'font-family="{family}" font-size="{size}" font-weight="700" '
           f'fill="{fg}">{esc(txt)}</text>')
    return svg, w

# --- Field card (compact, 4x2 grid) -------------------------------------------

def render_field_card(x, y, w, h, fld):
    out = [card(x, y, w, h)]
    out.append(top_stripe(x, y, w, fld.accent))
    px = x + 16
    # group eyebrow
    out.append(text(px, y + 26, fld.group, size=9, weight=700,
                    fill=fld.accent, letter_spacing="1.1"))
    # field name (mono, bold)
    out.append(mono(px, y + 50, fld.name, size=18, weight=700, fill=C_INK))
    # type chip
    tchip, _ = chip(px, y + 60, fld.typ, fg=fld.accent, bg=fld.tint, mono_font=True)
    out.append(tchip)
    # nullable marker (top-right)
    ntxt = "nullable" if fld.nullable else "required"
    nchip, nw = chip(0, 0, ntxt)
    nchip, _ = chip(x + w - 16 - nw, y + 16, ntxt,
                    fg=(fld.accent if fld.nullable else "#FFFFFF"),
                    bg=("#FFFFFF" if fld.nullable else fld.accent),
                    border=(fld.accent if fld.nullable else None))
    out.append(nchip)
    # purpose
    out.append(text(px, y + 100, fld.purpose, size=10.5, weight=500, fill=C_MUTED))
    return "".join(out)

def render_example_tile(x, y, w, h, *, label, sublabel, accent, tint, rows, virtual):
    dash = "5 4" if virtual else None
    out = [card(x, y, w, h, stroke=accent, dash=dash, shadow=not virtual)]
    out.append(top_stripe(x, y, w, accent))
    out.append(text(x + CMP_PAD, y + 32, label, size=17, weight=800, fill=C_INK))
    out.append(mono(x + CMP_PAD, y + 50, sublabel, size=11, weight=600, fill=accent))
    bchip, bw = chip(0, 0, "VIRTUAL" if virtual else "MATERIALIZED")
    bchip, _ = chip(x + w - CMP_PAD - bw, y + 22, "VIRTUAL" if virtual else "MATERIALIZED",
                    fg="#FFFFFF", bg=accent)
    out.append(bchip)
    body_y = y + CMP_HEADER_H + CMP_BODY_PAD
    key_x = x + CMP_PAD
    val_x = x + CMP_PAD + 130
    for i, (k, v) in enumerate(rows):
        ry = body_y + i * CMP_ROW_H + 16
        out.append(mono(key_x, ry, f"{k}:", size=12, weight=700, fill=C_MUTED_2))
        out.append(mono(val_x, ry, v, size=12, weight=600, fill=C_INK))
    return "".join(out)

# --- Render -------------------------------------------------------------------

def render():
    inner_w = CANVAS_W - 2 * PAD

    hero_h = HERO_PAD + HERO_TITLE_H + GRID_ROWS * CARD_H + (GRID_ROWS - 1) * CARD_GAP + HERO_PAD
    cmp_card_h = CMP_HEADER_H + CMP_BODY_PAD * 2 + CMP_ROW_H * max(len(MATERIALIZED_EXAMPLE), len(VIRTUAL_EXAMPLE))
    canvas_h = (PAD + HEADER_H + HERO_TOP_GAP + hero_h
                + CMP_TOP_GAP + CMP_LABEL_H + cmp_card_h + FOOTER_H + PAD)

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {CANVAS_W} {canvas_h}" '
        f'width="{CANVAS_W}" height="{canvas_h}" '
        f'style="font-family: Inter, -apple-system, system-ui, sans-serif;">'
    ]
    parts.append(dedent('''\
        <defs>
          <filter id="card-shadow" x="-5%" y="-5%" width="110%" height="115%">
            <feDropShadow dx="0" dy="2" stdDeviation="6" flood-color="#0F1B2A" flood-opacity="0.08"/>
          </filter>
          <linearGradient id="bg" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0" stop-color="#FAFBFC"/><stop offset="1" stop-color="#F1F4F8"/>
          </linearGradient>
        </defs>
        '''))
    parts.append(f'<rect x="0" y="0" width="{CANVAS_W}" height="{canvas_h}" fill="url(#bg)"/>')

    # ---- Header ----
    parts.append(text(PAD, PAD + 28, "GeoBrix · Tile Structure", size=30, weight=800, fill=C_INK))
    sub = ('A RasterX <tspan font-family="ui-monospace, SFMono-Regular, Menlo, monospace" '
           f'font-weight="700" fill="{C_INK}">tile</tspan> is one typed struct — shared by both '
           'tiers — carrying raster bytes (or a bytes-free reference) with grid, clip, CRS and format metadata')
    parts.append(f'<text x="{PAD}" y="{PAD + 56}" font-size="15" fill="{C_MUTED}">{sub}</text>')
    pill_text = "v0.5.0  ·  Beta"
    pw = int(len(pill_text) * 6.8) + 24
    parts.append(
        f'<rect x="{CANVAS_W - PAD - pw}" y="{PAD + 8}" rx="13" ry="13" width="{pw}" height="26" fill="{C_INK}"/>'
        f'<text x="{CANVAS_W - PAD - pw/2}" y="{PAD + 26}" text-anchor="middle" '
        f'font-size="12" font-weight="700" fill="#FFFFFF">{pill_text}</text>')

    # ---- Hero (schema card + 4x2 field grid) ----
    hero_y = PAD + HEADER_H + HERO_TOP_GAP
    parts.append(card(PAD, hero_y, inner_w, hero_h))
    parts.append(top_stripe(PAD, hero_y, inner_w, C_INK_2))
    parts.append(text(PAD + HERO_PAD, hero_y + 30, "TILE SCHEMA — 9 FIELDS",
                      size=11, weight=700, fill=C_MUTED_3, letter_spacing="1.6"))
    parts.append(mono(CANVAS_W - PAD - HERO_PAD, hero_y + 30,
                      "struct<cellid, raster, path, path_mode, window, clip_polygon, clip_crs, crs, metadata>",
                      size=12.5, weight=600, fill=C_MUTED, anchor="end"))
    # field grid
    field_w = (inner_w - 2 * HERO_PAD - (GRID_COLS - 1) * CARD_GAP) / GRID_COLS
    gy0 = hero_y + HERO_PAD + HERO_TITLE_H
    for i, fld in enumerate(FIELDS):
        r, c = divmod(i, GRID_COLS)
        fx = PAD + HERO_PAD + c * (field_w + CARD_GAP)
        fyy = gy0 + r * (CARD_H + CARD_GAP)
        parts.append(render_field_card(fx, fyy, field_w, CARD_H, fld))

    # ---- Comparison row ----
    cmp_label_y = hero_y + hero_h + CMP_TOP_GAP
    parts.append(text(PAD, cmp_label_y + 16, "EXAMPLE TILES",
                      size=11, weight=700, fill=C_MUTED_3, letter_spacing="1.6"))
    parts.append(text(CANVAS_W - PAD, cmp_label_y + 16,
                      "same struct · bytes present vs. bytes-free reference",
                      size=12, weight=500, fill=C_MUTED_2, anchor="end"))
    cmp_y = cmp_label_y + CMP_LABEL_H
    col_w = (inner_w - CARD_GAP) // 2
    parts.append(render_example_tile(
        PAD, cmp_y, col_w, cmp_card_h,
        label="Materialized", sublabel="rst_fromcontent  /  GDAL reader  /  any rst_* output",
        accent=A_RASTER, tint=T_RASTER, rows=MATERIALIZED_EXAMPLE, virtual=False))
    parts.append(render_example_tile(
        PAD + col_w + CARD_GAP, cmp_y, col_w, cmp_card_h,
        label="Virtual", sublabel="cog_gbx reader (virtualTiles) — lightweight tier",
        accent=A_REF, tint=T_REF, rows=VIRTUAL_EXAMPLE, virtual=True))

    # ---- Footer ----
    parts.append(text(PAD, canvas_h - 12,
                      "databrickslabs/geobrix  ·  DBR 17.3 / 18 LTS  ·  Scala 2.13.16 / Spark 4.0 / Python 3.12",
                      size=11, fill=C_MUTED_3))
    parts.append(text(CANVAS_W - PAD, canvas_h - 12, "docs/api/tile-structure",
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
