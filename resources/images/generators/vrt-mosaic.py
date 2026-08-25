#!/usr/bin/env python3
"""Generate the VRT-mosaic hero diagram SVG.

The eye-catcher for the VRT-mosaic storage form: a left->right 4-stage flow
(SOURCE -> TILE -> INDEX -> READ) showing how one raster too large for a single
COG or for per-task memory becomes a directory of bounded mini-COGs plus a
lightweight GDAL VRT index, then expands back into one virtual tile per member
for distributed per-tile work. Virtual vs materialized keeps its fixed meaning:
teal marks the expanded member tiles (bytes-free virtual tiles); the mini-COG
files on disk and every stage frame use neutral chrome accents, never teal or
orange. Designed to stand alone (legible without geobrix context) for reuse in
slides.

Re-render after a change:

    python3 resources/images/generators/vrt-mosaic.py
    # then rasterize to PNG (used by docs/api/vrt-mosaic.mdx and slides):
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \\
        --headless --disable-gpu --hide-scrollbars \\
        --force-device-scale-factor=2 --window-size=1680,900 \\
        --screenshot=resources/images/diagrams/rasterx/vrt-mosaic.png \\
        resources/images/diagrams/rasterx/vrt-mosaic.svg
    # then auto-crop whitespace:
    python3 -c "
from PIL import Image, ImageChops
p='resources/images/diagrams/rasterx/vrt-mosaic.png'
img=Image.open(p).convert('RGB')
bbox=ImageChops.difference(img, Image.new('RGB', img.size, (255,255,255))).getbbox()
if bbox: img.crop(bbox).save(p)
"
"""
from textwrap import dedent

# --- Palette (shared with virtual-tiles-lifecycle.py / file-gbx-flow.py) ------

C_INK     = "#0F1B2A"
C_INK_2   = "#1B3139"
C_MUTED   = "#3F4D5E"
C_MUTED_2 = "#5A6878"
C_MUTED_3 = "#7A8794"
C_BORDER  = "#E5E7EB"

# Stage accents.
# IMPORTANT: teal and orange are RESERVED for the tile-state meaning (virtual /
# materialized). Stage chrome must NOT reuse them. SOURCE + READ share the read
# family blue; TILE (the cog_gbx writer) uses violet; INDEX (the mosaic.vrt lens)
# uses a neutral slate so no stage color competes with the semantic tile colors.
ACCENT_SOURCE = "#1F6FB5"; TINT_SOURCE = "#E3EEF8"   # blue — the raster source
ACCENT_TILE   = "#7A5AA6"; TINT_TILE   = "#ECE5F5"   # violet — the cog_gbx writer
ACCENT_INDEX  = "#41546A"; TINT_INDEX  = "#EAEEF3"   # neutral slate — the VRT lens
ACCENT_READ   = "#1F6FB5"; TINT_READ   = "#E3EEF8"   # blue — the reader

# Tile-state accent — the ONLY teal in the diagram (the meaning): the reader
# expands each mini-COG member into one bytes-free VIRTUAL tile row.
C_VIRTUAL     = "#0F8E8B"; TINT_VIRTUAL = "#E4F3F2"  # teal, hollow/dashed

# --- Canvas -------------------------------------------------------------------

PAD       = 40
CANVAS_W  = 1680
HEADER_H  = 96
FOOTER_H  = 30

STAGE_GAP = 26
STAGE_TOP = PAD + HEADER_H + 18
STAGE_H   = 524

# --- SVG helpers (shared conventions) -----------------------------------------

def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def text(x, y, s, *, size=13, weight=400, fill=C_INK,
         family="Inter, -apple-system, system-ui, sans-serif",
         anchor="start", letter_spacing=None, opacity=None):
    ls = f' letter-spacing="{letter_spacing}"' if letter_spacing else ""
    op = f' opacity="{opacity}"' if opacity is not None else ""
    return (f'<text x="{x}" y="{y}" font-family="{family}" font-size="{size}" '
            f'font-weight="{weight}" fill="{fill}" text-anchor="{anchor}"{ls}{op}>'
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

def chip(x, y, txt, *, fg=C_INK, bg="#F1F4F8", border=None, mono_font=False,
         h=22, size=12, pad_x=12):
    char_w = (0.60 if mono_font else 0.56) * size
    w = int(len(txt) * char_w) + pad_x * 2
    family = ("ui-monospace, SFMono-Regular, Menlo, Consolas, monospace"
              if mono_font else "Inter, -apple-system, system-ui, sans-serif")
    bs = f' stroke="{border}" stroke-width="1"' if border else ""
    svg = (f'<rect x="{x}" y="{y}" rx="{h/2:.0f}" ry="{h/2:.0f}" width="{w}" '
           f'height="{h}" fill="{bg}"{bs}/>'
           f'<text x="{x + w/2}" y="{y + h/2 + 4}" text-anchor="middle" '
           f'font-family="{family}" font-size="{size}" font-weight="700" '
           f'fill="{fg}">{esc(txt)}</text>')
    return svg, w

def arrow(x1, y1, x2, y2, *, marker="arrow", color=C_MUTED_2, width=2.4, dash=None):
    ds = f' stroke-dasharray="{dash}"' if dash else ""
    return (f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" '
            f'stroke-width="{width}"{ds} marker-end="url(#{marker})"/>')

# --- Glyphs -------------------------------------------------------------------

def glyph_big_raster(cx, cy, s, n, color, tint):
    """One large raster: a solid subdivided square (the whole image)."""
    x0, y0 = cx - s / 2, cy - s / 2
    parts = [f'<rect x="{x0:.1f}" y="{y0:.1f}" width="{s}" height="{s}" rx="4" '
             f'fill="{tint}" stroke="{color}" stroke-width="2.6"/>']
    for i in range(1, n):
        parts.append(f'<line x1="{x0 + i*s/n:.1f}" y1="{y0:.1f}" '
                     f'x2="{x0 + i*s/n:.1f}" y2="{y0+s:.1f}" stroke="{color}" '
                     f'stroke-width="1" stroke-opacity="0.32"/>')
        parts.append(f'<line x1="{x0:.1f}" y1="{y0 + i*s/n:.1f}" '
                     f'x2="{x0+s:.1f}" y2="{y0 + i*s/n:.1f}" stroke="{color}" '
                     f'stroke-width="1" stroke-opacity="0.32"/>')
    return "".join(parts)

def glyph_tile_grid(cx, cy, cols, rows, cell, gap, color, tint, *, hollow=False):
    """A cols×rows grid of small mini-COG tile squares. Returns (svg, w, h)."""
    grid_w = cols * cell + (cols - 1) * gap
    grid_h = rows * cell + (rows - 1) * gap
    x0, y0 = cx - grid_w / 2, cy - grid_h / 2
    parts = []
    for r in range(rows):
        for c in range(cols):
            tx = x0 + c * (cell + gap)
            ty = y0 + r * (cell + gap)
            fill = "#FFFFFF" if hollow else tint
            parts.append(f'<rect x="{tx:.1f}" y="{ty:.1f}" width="{cell}" '
                         f'height="{cell}" rx="2" fill="{fill}" stroke="{color}" '
                         f'stroke-width="1.5"/>')
            parts.append(f'<line x1="{tx + cell/2:.1f}" y1="{ty:.1f}" '
                         f'x2="{tx + cell/2:.1f}" y2="{ty + cell:.1f}" '
                         f'stroke="{color}" stroke-width="0.8" stroke-opacity="0.4"/>')
            parts.append(f'<line x1="{tx:.1f}" y1="{ty + cell/2:.1f}" '
                         f'x2="{tx + cell:.1f}" y2="{ty + cell/2:.1f}" '
                         f'stroke="{color}" stroke-width="0.8" stroke-opacity="0.4"/>')
    return "".join(parts), grid_w, grid_h

def glyph_file(cx, cy, color, tint, s=40):
    """A document/index page with a folded top-right corner + text lines."""
    w = s * 0.78
    h = s
    x0, y0 = cx - w / 2, cy - h / 2
    fold = s * 0.30
    d = (f"M {x0:.1f} {y0:.1f} H {x0 + w - fold:.1f} L {x0 + w:.1f} {y0 + fold:.1f} "
         f"V {y0 + h:.1f} H {x0:.1f} Z")
    parts = [f'<path d="{d}" fill="{tint}" stroke="{color}" stroke-width="2" '
             f'stroke-linejoin="round"/>']
    parts.append(f'<path d="M {x0 + w - fold:.1f} {y0:.1f} V {y0 + fold:.1f} '
                 f'H {x0 + w:.1f} Z" fill="{color}" fill-opacity="0.28" '
                 f'stroke="{color}" stroke-width="1.4" stroke-linejoin="round"/>')
    for i in range(3):
        ly = y0 + h * 0.52 + i * (s * 0.17)
        parts.append(f'<line x1="{x0 + w * 0.18:.1f}" y1="{ly:.1f}" '
                     f'x2="{x0 + w * 0.80:.1f}" y2="{ly:.1f}" stroke="{color}" '
                     f'stroke-width="1.6" stroke-opacity="0.5"/>')
    return "".join(parts)

def glyph_reader(cx, cy, color, s=44):
    """A funnel/arrow-into-columns reader mark, with a teal output arrow."""
    parts = [f'<path d="M {cx - s/2:.1f} {cy - s/2:.1f} L {cx + s/2:.1f} {cy - s/2:.1f} '
             f'L {cx + 8:.1f} {cy:.1f} L {cx + 8:.1f} {cy + s/2:.1f} '
             f'L {cx - 8:.1f} {cy + s/2:.1f} L {cx - 8:.1f} {cy:.1f} Z" '
             f'fill="{color}" fill-opacity="0.14" stroke="{color}" stroke-width="2"/>']
    parts.append(f'<line x1="{cx:.1f}" y1="{cy + 4:.1f}" x2="{cx:.1f}" '
                 f'y2="{cy + s/2 + 12:.1f}" stroke="{C_VIRTUAL}" stroke-width="2.6" '
                 f'marker-end="url(#arrow-teal)"/>')
    return "".join(parts)

# --- Composite renderers ------------------------------------------------------

def stage_frame(x, w, *, num, title, subtitle, accent):
    out = [card(x, STAGE_TOP, w, STAGE_H, fill="#FFFFFF", stroke=C_BORDER)]
    out.append(top_stripe(x, STAGE_TOP, w, accent))
    out.append(f'<circle cx="{x + 26}" cy="{STAGE_TOP + 34}" r="13" fill="{accent}"/>')
    out.append(text(x + 26, STAGE_TOP + 39, str(num), size=15, weight=800,
                    fill="#FFFFFF", anchor="middle"))
    out.append(text(x + 48, STAGE_TOP + 32, title, size=17, weight=800, fill=C_INK))
    out.append(text(x + 48, STAGE_TOP + 50, subtitle, size=11.5, weight=500,
                    fill=C_MUTED_2))
    return "".join(out)

def note_box(x, y, w, h, accent, tint, lines, *, title=None):
    """A tinted callout box; lines is a list of (text, mono?) tuples."""
    out = [f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" '
           f'fill="{tint}" fill-opacity="0.5" stroke="{accent}" stroke-width="1.3"/>']
    ly = y + 20
    if title:
        out.append(text(x + 14, ly, title, size=9, weight=700, fill=accent,
                        letter_spacing="1.1"))
        ly += 18
    for ln, is_mono in lines:
        if is_mono:
            out.append(mono(x + 14, ly, ln, size=10, weight=600, fill=accent))
        else:
            out.append(text(x + 14, ly, ln, size=10, weight=500, fill=C_MUTED))
        ly += 16
    return "".join(out)

def route_card(x, y, w, h, *, accent, tint, badge, title, lines, dash=None):
    """A sub-card branch — one acquisition route for the index."""
    out = [card(x, y, w, h, stroke=accent, dash=dash, shadow=dash is None)]
    out.append(top_stripe(x, y, w, accent, r=14))
    filled = dash is None
    bg = accent if filled else "#FFFFFF"
    fg = "#FFFFFF" if filled else accent
    border = None if filled else accent
    bc, bw = chip(x + 12, y + 13, badge, fg=fg, bg=bg, border=border, h=20, size=10.5)
    out.append(bc)
    out.append(text(x + 12 + bw + 9, y + 27, title, size=11, weight=700, fill=C_INK))
    ly = y + 48
    for ln, is_mono in lines:
        if is_mono:
            out.append(mono(x + 15, ly, ln, size=9.5, weight=600, fill=accent))
        else:
            out.append(text(x + 15, ly, ln, size=9.5, weight=500, fill=C_MUTED))
        ly += 15
    return "".join(out)

def virtual_tile_deck(x, y, w):
    """A deck of teal, dashed VIRTUAL tile cards — one per expanded member."""
    accent = C_VIRTUAL
    rows = [("path", "…/tile_…_0_0.tif"), ("raster", "NULL"), ("window", "NULL")]
    row_h = 20
    h = 58 + len(rows) * row_h + 10
    out = []
    for k in (2, 1):
        off = k * 7
        out.append(f'<rect x="{x + off}" y="{y + off}" rx="12" ry="12" width="{w}" '
                   f'height="{h}" fill="#FFFFFF" stroke="{accent}" stroke-width="1.5" '
                   f'stroke-opacity="{0.35 - (k-1)*0.12:.2f}" stroke-dasharray="5 4"/>')
    out.append(card(x, y, w, h, stroke=accent, dash="5 4", shadow=False, r=12))
    out.append(top_stripe(x, y, w, accent, r=12))
    badge, bw = chip(x + 12, y + 16, "VIRTUAL", fg="#FFFFFF", bg=accent, h=20, size=10.5)
    out.append(badge)
    out.append(text(x + 12 + bw + 8, y + 30, "one tile / member", size=10,
                    weight=600, fill=C_MUTED_2))
    ry = y + 58
    for k, v in rows:
        out.append(mono(x + 14, ry, k, size=10.5, weight=700, fill=accent))
        out.append(mono(x + w - 14, ry, v, size=10.5, weight=500, fill=C_MUTED,
                        anchor="end"))
        ry += row_h
    return "".join(out), h

# --- Render -------------------------------------------------------------------

def render():
    canvas_h = STAGE_TOP + STAGE_H + FOOTER_H + PAD
    inner_w = CANVAS_W - 2 * PAD
    col_w = (inner_w - 3 * STAGE_GAP) / 4
    x1 = PAD
    x2 = x1 + col_w + STAGE_GAP
    x3 = x2 + col_w + STAGE_GAP
    x4 = x3 + col_w + STAGE_GAP

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {CANVAS_W} {canvas_h}" '
        f'width="{CANVAS_W}" height="{canvas_h}" '
        f'style="font-family: Inter, -apple-system, system-ui, sans-serif;">'
    ]
    parts.append(dedent(f'''\
        <defs>
          <filter id="card-shadow" x="-5%" y="-5%" width="110%" height="115%">
            <feDropShadow dx="0" dy="2" stdDeviation="6" flood-color="#0F1B2A" flood-opacity="0.08"/>
          </filter>
          <linearGradient id="bg" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0" stop-color="#FAFBFC"/><stop offset="1" stop-color="#F1F4F8"/>
          </linearGradient>
          <marker id="arrow" markerWidth="10" markerHeight="10" refX="7" refY="3"
                  orient="auto" markerUnits="userSpaceOnUse">
            <path d="M0,0 L7,3 L0,6 Z" fill="{C_MUTED_2}"/>
          </marker>
          <marker id="arrow-tile" markerWidth="10" markerHeight="10" refX="7" refY="3"
                  orient="auto" markerUnits="userSpaceOnUse">
            <path d="M0,0 L7,3 L0,6 Z" fill="{ACCENT_TILE}"/>
          </marker>
          <marker id="arrow-index" markerWidth="10" markerHeight="10" refX="7" refY="3"
                  orient="auto" markerUnits="userSpaceOnUse">
            <path d="M0,0 L7,3 L0,6 Z" fill="{ACCENT_INDEX}"/>
          </marker>
          <marker id="arrow-read" markerWidth="10" markerHeight="10" refX="7" refY="3"
                  orient="auto" markerUnits="userSpaceOnUse">
            <path d="M0,0 L7,3 L0,6 Z" fill="{ACCENT_READ}"/>
          </marker>
          <marker id="arrow-teal" markerWidth="10" markerHeight="10" refX="7" refY="3"
                  orient="auto" markerUnits="userSpaceOnUse">
            <path d="M0,0 L7,3 L0,6 Z" fill="{C_VIRTUAL}"/>
          </marker>
        </defs>
        '''))
    parts.append(f'<rect x="0" y="0" width="{CANVAS_W}" height="{canvas_h}" fill="url(#bg)"/>')

    # ---- Header ----
    parts.append(text(PAD, PAD + 30, "GeoBrix · VRT Mosaics",
                      size=30, weight=800, fill=C_INK))
    parts.append(text(PAD, PAD + 58,
                      "Split one raster too big for a single COG into bounded mini-COGs plus a "
                      "portable GDAL VRT index — then expand it back into one virtual tile per "
                      "member for distributed rst_* work",
                      size=14.5, fill=C_MUTED))
    # legend (top-right): the teal virtual-tile meaning
    lx = CANVAS_W - PAD - 232
    parts.append(f'<rect x="{lx}" y="{PAD + 14}" width="16" height="16" rx="3" '
                 f'fill="#FFFFFF" stroke="{C_VIRTUAL}" stroke-width="2" stroke-dasharray="4 3"/>')
    parts.append(text(lx + 24, PAD + 27, "virtual tile (bytes-free)", size=12,
                      weight=600, fill=C_MUTED))

    # ============================ STAGE 1 — SOURCE ============================
    parts.append(stage_frame(x1, col_w, num=1, title="Source",
                             subtitle="one large raster", accent=ACCENT_SOURCE))
    c1 = x1 + col_w / 2
    parts.append(glyph_big_raster(c1, STAGE_TOP + 176, 150, 8, ACCENT_SOURCE, TINT_SOURCE))
    parts.append(text(c1, STAGE_TOP + 282, "a single large image", size=13.5,
                      weight=800, fill=C_INK, anchor="middle"))
    parts.append(text(c1, STAGE_TOP + 300, "striped / tiled GeoTIFF · COG · NetCDF",
                      size=10, weight=500, fill=C_MUTED_2, anchor="middle"))
    # facts box
    fx = x1 + 20; fw = col_w - 40
    parts.append(card(fx, STAGE_TOP + 322, fw, 92, stroke=C_BORDER, shadow=False, r=10))
    for i, (k, v) in enumerate([("dimensions", "40000 × 30000 px"),
                                ("dtype", "uint16 · 1 band"),
                                ("size", "≈ 2.3 GB")]):
        ry = STAGE_TOP + 348 + i * 22
        parts.append(mono(fx + 16, ry, k, size=10.5, weight=700, fill=ACCENT_SOURCE))
        parts.append(mono(fx + fw - 16, ry, v, size=10.5, weight=500, fill=C_MUTED,
                          anchor="end"))
    parts.append(note_box(fx, STAGE_TOP + 432, fw, 66, ACCENT_SOURCE, TINT_SOURCE,
                          [("Too big to hold as one COG", False),
                           ("— or to fit in per-task RAM", False)],
                          title="THE PROBLEM"))

    # ============================ STAGE 2 — TILE ==============================
    parts.append(stage_frame(x2, col_w, num=2, title="Tile → mini-COGs",
                             subtitle="cog_gbx writer · mosaic mode", accent=ACCENT_TILE))
    # option chips
    rx = x2 + 20
    for opt, is_mono in [("cog_gbx", True), ("vrtMosaic=true", True), ("tileSize=1024", True)]:
        c, cw = chip(rx, STAGE_TOP + 76, opt, fg=ACCENT_TILE, bg=TINT_TILE,
                     border=ACCENT_TILE, mono_font=True, size=10, pad_x=9)
        parts.append(c)
        rx += cw + 6
    # split-grid glyph
    c2 = x2 + col_w / 2
    g, gw, gh = glyph_tile_grid(c2, STAGE_TOP + 184, 5, 4, 30, 6, ACCENT_TILE, TINT_TILE)
    parts.append(g)
    parts.append(text(c2, STAGE_TOP + 270, "one bounded mini-COG per window",
                      size=11, weight=700, fill=C_INK, anchor="middle"))
    # filenames
    fx2 = x2 + 20; fw2 = col_w - 40
    for i, fn in enumerate(["tile_<disc>_0_0.tif", "tile_<disc>_0_1.tif", "…"]):
        parts.append(mono(fx2 + 4, STAGE_TOP + 294 + i * 17, fn, size=9.5,
                          weight=500, fill=C_MUTED_2))
    # window-by-window callout
    parts.append(note_box(fx2, STAGE_TOP + 352, fw2, 82, ACCENT_TILE, TINT_TILE,
                          [("Source read window-by-window —", False),
                           ("never fully materialised in RAM", False),
                           ("each tile ≤ per-task cap · ≈ 2 MB", False)],
                          title="SERVERLESS-SAFE BY CONSTRUCTION"))
    parts.append(note_box(fx2, STAGE_TOP + 444, fw2, 50, C_MUTED_2, "#EEF1F5",
                          [("pruneEmpty · overlapPercent ·", True),
                           ("mergeStrategy", True)]))

    # ============================ STAGE 3 — INDEX =============================
    parts.append(stage_frame(x3, col_w, num=3, title="Index = mosaic.vrt",
                             subtitle="a portable GDAL lens over the tiles",
                             accent=ACCENT_INDEX))
    c3 = x3 + col_w / 2
    parts.append(glyph_file(c3, STAGE_TOP + 128, ACCENT_INDEX, TINT_INDEX, s=52))
    # small tile grid under the index doc, with faint connectors = "lens over tiles"
    lg, lgw, lgh = glyph_tile_grid(c3, STAGE_TOP + 174, 5, 1, 15, 4, ACCENT_INDEX,
                                   TINT_INDEX, hollow=True)
    lg_x0 = c3 - lgw / 2
    for i in range(5):
        tx = lg_x0 + i * (15 + 4) + 7.5
        parts.append(f'<line x1="{c3:.1f}" y1="{STAGE_TOP + 154:.1f}" x2="{tx:.1f}" '
                     f'y2="{STAGE_TOP + 167:.1f}" stroke="{ACCENT_INDEX}" '
                     f'stroke-width="1" stroke-opacity="0.4"/>')
    parts.append(lg)
    parts.append(text(c3, STAGE_TOP + 202, "lightweight VRT XML — indexes every tile",
                      size=10, weight=600, fill=C_MUTED_2, anchor="middle"))
    # two acquisition routes
    rx3 = x3 + 20; rw3 = col_w - 40
    parts.append(route_card(rx3, STAGE_TOP + 218, rw3, 74, accent=ACCENT_INDEX,
                            tint=TINT_INDEX, badge="PERSISTED", title="written by the writer",
                            lines=[("writeVrt=true · vrtPaths=\"relative\"", True),
                                   ("whole directory is portable / movable", False)]))
    parts.append(route_card(rx3, STAGE_TOP + 302, rw3, 74, accent=ACCENT_INDEX,
                            tint=TINT_INDEX, badge="ON-DEMAND", title="minted transiently",
                            dash="5 4",
                            lines=[("mint_vrt(paths) · absolute paths", True),
                                   ("a VRT over a dynamic tile subset", False)]))
    # portability callout
    parts.append(note_box(rx3, STAGE_TOP + 392, rw3, 106, C_INK_2, "#EDEFF3",
                          [("Opens in any GDAL tool —", False),
                           ("QGIS · gdalinfo · rio-tiler —", True),
                           ("with no GeoBrix needed.", False),
                           ("The index is just standard VRT XML.", False)],
                          title="PORTABLE ARTIFACT"))

    # ============================ STAGE 4 — READ ==============================
    parts.append(stage_frame(x4, col_w, num=4, title="Read → expand",
                             subtitle="one virtual tile per member",
                             accent=ACCENT_READ))
    c4 = x4 + col_w / 2
    # reader chips
    rx4 = x4 + 20
    for opt in ["raster_gbx", "cog_gbx"]:
        c, cw = chip(rx4, STAGE_TOP + 76, opt, fg=ACCENT_READ, bg=TINT_READ,
                     border=ACCENT_READ, mono_font=True, size=10.5, pad_x=10)
        parts.append(c)
        rx4 += cw + 8
    parts.append(text(x4 + 20, STAGE_TOP + 106, "pointed at mosaic.vrt", size=10,
                      weight=500, fill=C_MUTED_2))
    # reader funnel
    parts.append(glyph_reader(c4, STAGE_TOP + 150, ACCENT_READ, s=44))
    # expanded virtual-tile deck
    deck_w = col_w - 72
    deck, deck_h = virtual_tile_deck(x4 + 26, STAGE_TOP + 214, deck_w)
    parts.append(deck)
    # distributed note
    parts.append(note_box(x4 + 20, STAGE_TOP + 360, col_w - 40, 66, C_VIRTUAL, TINT_VIRTUAL,
                          [("rst_* run per-tile, distributed", False),
                           ("across the cluster — read lazily", False)],
                          title="DISTRIBUTED PER-TILE"))
    # windowed-read note
    parts.append(note_box(x4 + 20, STAGE_TOP + 436, col_w - 40, 62, C_MUTED_2, "#EEF1F5",
                          [("A windowed read touches ONLY", False),
                           ("the tiles intersecting the viewport", False)],
                          title="WINDOWED LOCALITY"))

    # ---- Inter-stage arrows ----
    arrow_y = STAGE_TOP + 190
    parts.append(arrow(x1 + col_w + 3, arrow_y, x2 - 3, arrow_y,
                       marker="arrow-tile", color=ACCENT_TILE, width=2.6))
    parts.append(arrow(x2 + col_w + 3, arrow_y, x3 - 3, arrow_y,
                       marker="arrow-index", color=ACCENT_INDEX, width=2.6))
    parts.append(arrow(x3 + col_w + 3, arrow_y, x4 - 3, arrow_y,
                       marker="arrow-read", color=ACCENT_READ, width=2.6))

    # ---- Footer ----
    parts.append(text(PAD, canvas_h - 12,
                      "databrickslabs/geobrix  ·  lightweight tier (pyrx) · Serverless-ready · no JAR",
                      size=11, fill=C_MUTED_3))
    parts.append(text(CANVAS_W - PAD, canvas_h - 12, "docs/api/vrt-mosaic",
                      size=11, fill=C_MUTED_3, anchor="end"))

    parts.append("</svg>")
    return "\n".join(parts)


if __name__ == "__main__":
    import os
    import sys
    default = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "..", "diagrams", "rasterx", "vrt-mosaic.svg")
    out = sys.argv[1] if len(sys.argv) > 1 else default
    with open(out, "w") as f:
        f.write(render())
    print(f"wrote {out}")
