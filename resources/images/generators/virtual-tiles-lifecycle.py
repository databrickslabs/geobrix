#!/usr/bin/env python3
"""Generate the Virtual-Tile lifecycle hero diagram SVG.

The capstone eye-catcher for the v2 virtual-tile capability: a left->right
4-stage lifecycle (SOURCE -> LOAD -> OPERATE -> WRITE). Virtual vs materialized
is a per-tile BADGE state that threads through the stages, not a swimlane.
Designed to stand alone (legible without geobrix context) for reuse in slides.

Re-render after a change:

    python3 resources/images/generators/virtual-tiles-lifecycle.py
    # then rasterize to PNG (used by docs/api/virtual-tiles.mdx and slides):
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \\
        --headless --disable-gpu --hide-scrollbars \\
        --force-device-scale-factor=2 --window-size=1640,940 \\
        --screenshot=resources/images/diagrams/rasterx/virtual-tiles-lifecycle.png \\
        resources/images/diagrams/rasterx/virtual-tiles-lifecycle.svg
    # then auto-crop whitespace:
    python3 -c "
from PIL import Image, ImageChops
p='resources/images/diagrams/rasterx/virtual-tiles-lifecycle.png'
img=Image.open(p).convert('RGB')
bbox=ImageChops.difference(img, Image.new('RGB', img.size, (255,255,255))).getbbox()
if bbox: img.crop(bbox).save(p)
"
"""
import math
from textwrap import dedent

# --- Palette (shared with rasterx-tile-structure.py) --------------------------

C_INK         = "#0F1B2A"
C_INK_2       = "#1B3139"
C_MUTED       = "#3F4D5E"
C_MUTED_2     = "#5A6878"
C_MUTED_3     = "#7A8794"
C_BORDER      = "#E5E7EB"

# Stage accents.
# IMPORTANT: teal and orange are RESERVED for the tile-state meaning (virtual /
# materialized, see legend + lanes). Stage chrome must NOT reuse them, or the
# viewer wrongly infers "Load => virtual" / "Operate => materialized". Source
# (blue) and Write (violet) are safe; Load + Operate use a neutral slate so no
# stage color competes with the semantic tile colors.
ACCENT_SOURCE = "#1F6FB5"; TINT_SOURCE = "#E3EEF8"   # blue
ACCENT_LOAD   = "#41546A"; TINT_LOAD   = "#EAEEF3"   # neutral slate (was teal)
ACCENT_OP     = "#41546A"; TINT_OP     = "#EAEEF3"   # neutral slate (was orange)
ACCENT_WRITE  = "#7A5AA6"; TINT_WRITE  = "#ECE5F5"   # violet

# Tile-state accents — the ONLY teal/orange in the diagram (the meaning).
C_VIRTUAL     = "#0F8E8B"; TINT_VIRTUAL = "#E4F3F2"  # teal, hollow/dashed
C_MATERIAL    = "#E04E2A"; TINT_MATERIAL = "#FCE9E2" # orange, filled

# FILE / governed-storage accent — used only for MANAGED FILE callouts (Box 1 + Box 4).
# Indigo-slate: distinct from Source blue, Write violet, teal, and orange.
C_FILE_MANAGED = "#5060A8"

# --- Canvas -------------------------------------------------------------------

PAD       = 40
CANVAS_W  = 1640
HEADER_H  = 92
FOOTER_H  = 30

# 4 stage columns
STAGE_GAP = 26
STAGE_TOP = PAD + HEADER_H + 14
# STAGE_H is set so the Source (1) and Write (4) side columns bottom-align with
# the middle region (Load/Operate header boxes + the two tile lanes):
#   header box (196) + gap (20) + 2 lanes (168 each) + lane gap (26) = 578
STAGE_H   = 578

# --- SVG helpers (shared conventions) -----------------------------------------

def esc(s):
    return (s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))

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

def arrow(x1, y1, x2, y2, *, color=C_MUTED_2, width=2.4, dash=None):
    ds = f' stroke-dasharray="{dash}"' if dash else ""
    return (f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" '
            f'stroke-width="{width}"{ds} marker-end="url(#arrow)"/>')

# --- Glyphs -------------------------------------------------------------------

def glyph_striped(cx, cy, color, tint, s=46):
    """Full-width horizontal strips (striped GeoTIFF)."""
    x0, y0 = cx - s/2, cy - s/2
    parts = [f'<rect x="{x0:.1f}" y="{y0:.1f}" width="{s}" height="{s}" '
             f'fill="{tint}" stroke="{color}" stroke-width="2"/>']
    for i in range(1, 5):
        parts.append(f'<line x1="{x0:.1f}" y1="{y0 + i*s/5:.1f}" '
                     f'x2="{x0+s:.1f}" y2="{y0 + i*s/5:.1f}" '
                     f'stroke="{color}" stroke-width="1.5" stroke-opacity="0.6"/>')
    return "".join(parts)

def glyph_tiled(cx, cy, color, tint, s=46):
    """Block grid (tiled GeoTIFF)."""
    x0, y0 = cx - s/2, cy - s/2
    parts = [f'<rect x="{x0:.1f}" y="{y0:.1f}" width="{s}" height="{s}" '
             f'fill="{tint}" stroke="{color}" stroke-width="2"/>']
    for i in range(1, 3):
        parts.append(f'<line x1="{x0 + i*s/3:.1f}" y1="{y0:.1f}" '
                     f'x2="{x0 + i*s/3:.1f}" y2="{y0+s:.1f}" '
                     f'stroke="{color}" stroke-width="1.5" stroke-opacity="0.6"/>')
        parts.append(f'<line x1="{x0:.1f}" y1="{y0 + i*s/3:.1f}" '
                     f'x2="{x0+s:.1f}" y2="{y0 + i*s/3:.1f}" '
                     f'stroke="{color}" stroke-width="1.5" stroke-opacity="0.6"/>')
    return "".join(parts)

def _iso_plane(cx, cy, hw, hh, n, color, tint, *, fill_op=1.0):
    """One isometric raster plane (a rhombus) centered at (cx,cy) subdivided into
    an n×n grid — the affine image of a unit square with basis u,v."""
    Tx, Ty = cx, cy - hh                 # top corner
    ux, uy = hw, hh                      # T->R
    vx, vy = -hw, hh                     # T->L
    def pt(a, b):
        return (Tx + a * ux + b * vx, Ty + a * uy + b * vy)
    R = pt(1, 0); B = pt(1, 1); L = pt(0, 1)
    out = [f'<path d="M {Tx:.1f} {Ty:.1f} L {R[0]:.1f} {R[1]:.1f} '
           f'L {B[0]:.1f} {B[1]:.1f} L {L[0]:.1f} {L[1]:.1f} Z" '
           f'fill="{tint}" fill-opacity="{fill_op:.2f}" stroke="{color}" stroke-width="2"/>']
    for i in range(1, n):
        p0 = pt(i / n, 0); p1 = pt(i / n, 1)
        q0 = pt(0, i / n); q1 = pt(1, i / n)
        out.append(f'<line x1="{p0[0]:.1f}" y1="{p0[1]:.1f}" x2="{p1[0]:.1f}" '
                   f'y2="{p1[1]:.1f}" stroke="{color}" stroke-width="1.3" stroke-opacity="0.6"/>')
        out.append(f'<line x1="{q0[0]:.1f}" y1="{q0[1]:.1f}" x2="{q1[0]:.1f}" '
                   f'y2="{q1[1]:.1f}" stroke="{color}" stroke-width="1.3" stroke-opacity="0.6"/>')
    return "".join(out)

def glyph_cog(cx, cy, color, tint, s=44, layers=3):
    """COG = an overview PYRAMID of stacked isometric raster planes, coarser
    (fewer tiles) toward the top, full-res tiled grid at the base — the iconic
    cloud-optimized GeoTIFF look. `layers` sets stack depth (3 = hero, 2 = a
    compact variant that fits alongside single-plane glyphs)."""
    hw, hh = s * 0.64, s * 0.32
    sep = s * 0.52
    parts = []
    if layers == 2:
        # base (full-res 4x4) + one coarser overview (2x2) on top
        parts.append(_iso_plane(cx, cy + sep / 2, hw, hh, 4, color, tint, fill_op=0.85))
        parts.append(_iso_plane(cx, cy - sep / 2, hw, hh, 2, color, tint, fill_op=0.6))
    else:
        parts.append(_iso_plane(cx, cy + sep, hw, hh, 4, color, tint, fill_op=0.85))
        parts.append(_iso_plane(cx, cy, hw, hh, 2, color, tint, fill_op=0.7))
        parts.append(_iso_plane(cx, cy - sep, hw, hh, 1, color, tint, fill_op=0.55))
    return "".join(parts)

def cog_glyph_half_height(s=44, layers=3):
    """Half of the vertical extent of glyph_cog(s) — for label placement."""
    if layers == 2:
        return s * 0.52 / 2 + s * 0.32   # sep/2 + hh
    return s * 0.52 + s * 0.32           # sep + hh

def glyph_netcdf(cx, cy, color, tint, s=42):
    """Layered cube (multidimensional NetCDF)."""
    parts = []
    for i, dy in enumerate([8, 0, -8]):
        op = 0.4 + i * 0.25
        parts.append(f'<rect x="{cx - s/2:.1f}" y="{cy + dy - s/2:.1f}" width="{s}" '
                     f'height="{s*0.7:.1f}" rx="3" fill="{tint}" stroke="{color}" '
                     f'stroke-width="1.8" fill-opacity="{op:.2f}"/>')
    return "".join(parts)

def glyph_table(cx, cy, color, tint, s=52, highlight_col=2):
    """Table with a highlighted tile column."""
    cols, rows = 3, 4
    cw, rh = s / cols, s / rows
    x0, y0 = cx - s/2, cy - s/2
    parts = [f'<rect x="{x0:.1f}" y="{y0:.1f}" width="{s}" height="{s}" rx="3" '
             f'fill="#FFFFFF" stroke="{color}" stroke-width="2"/>']
    # header
    parts.append(f'<rect x="{x0:.1f}" y="{y0:.1f}" width="{s}" height="{rh:.1f}" '
                 f'fill="{color}" fill-opacity="0.85"/>')
    # highlighted tile column
    parts.append(f'<rect x="{x0 + highlight_col*cw:.1f}" y="{y0 + rh:.1f}" '
                 f'width="{cw:.1f}" height="{s - rh:.1f}" fill="{tint}"/>')
    for i in range(1, cols):
        parts.append(f'<line x1="{x0 + i*cw:.1f}" y1="{y0:.1f}" x2="{x0 + i*cw:.1f}" '
                     f'y2="{y0+s:.1f}" stroke="{color}" stroke-width="1" stroke-opacity="0.4"/>')
    for i in range(1, rows):
        parts.append(f'<line x1="{x0:.1f}" y1="{y0 + i*rh:.1f}" x2="{x0+s:.1f}" '
                     f'y2="{y0 + i*rh:.1f}" stroke="{color}" stroke-width="1" stroke-opacity="0.4"/>')
    return "".join(parts)

def glyph_reader(cx, cy, color, s=46):
    """A funnel/arrow-into-columns reader mark."""
    parts = [f'<path d="M {cx - s/2:.1f} {cy - s/2:.1f} L {cx + s/2:.1f} {cy - s/2:.1f} '
             f'L {cx + 8:.1f} {cy:.1f} L {cx + 8:.1f} {cy + s/2:.1f} '
             f'L {cx - 8:.1f} {cy + s/2:.1f} L {cx - 8:.1f} {cy:.1f} Z" '
             f'fill="{color}" fill-opacity="0.14" stroke="{color}" stroke-width="2"/>']
    parts.append(f'<line x1="{cx:.1f}" y1="{cy + 4:.1f}" x2="{cx:.1f}" y2="{cy + s/2 + 10:.1f}" '
                 f'stroke="{color}" stroke-width="2.4" marker-end="url(#arrow-teal)"/>')
    return "".join(parts)

# --- Tile-card renderer (the badge that threads through the diagram) ----------

def tile_card(x, y, w, *, virtual, label, detail_rows, note=None):
    """A v2 tile struct card. virtual=True -> hollow/dashed teal (bytes-free);
    virtual=False -> filled orange (raster bytes present)."""
    accent = C_VIRTUAL if virtual else C_MATERIAL
    tint = TINT_VIRTUAL if virtual else TINT_MATERIAL
    row_h = 22
    badge_block = 44          # vertical room for the top-stripe + badge row
    h = badge_block + len(detail_rows) * row_h + (18 if note else 0) + 16
    out = [card(x, y, w, h, fill="#FFFFFF", stroke=accent,
                dash="5 4" if virtual else None, shadow=not virtual)]
    out.append(top_stripe(x, y, w, accent))
    badge, bw = chip(x + 12, y + 18, "VIRTUAL" if virtual else "MATERIALIZED",
                     fg="#FFFFFF", bg=accent)
    out.append(badge)
    out.append(text(x + 12 + bw + 8, y + 18 + 15, label, size=11,
                    weight=600, fill=C_MUTED_2))
    ry = y + badge_block + 16
    for k, v in detail_rows:
        out.append(mono(x + 14, ry, k, size=11, weight=700, fill=accent))
        out.append(mono(x + w - 14, ry, v, size=11, weight=500,
                        fill=C_MUTED, anchor="end"))
        ry += row_h
    if note:
        out.append(text(x + 14, ry + 4, note, size=10.5, weight=600,
                        fill=accent, opacity="0.9"))
    return "".join(out), h

def tile_deck(x, y, w, *, virtual, label, detail_rows):
    """A tile card with two offset cards peeking out behind it (a 'deck' of
    many identical-shape rows)."""
    accent = C_VIRTUAL if virtual else C_MATERIAL
    dash_attr = ' stroke-dasharray="5 4"' if virtual else ""
    out = []
    # two shadow cards behind, offset down-right
    _, front_h = tile_card(x, y, w, virtual=virtual, label=label, detail_rows=detail_rows)
    for k in (2, 1):
        off = k * 8
        out.append(f'<rect x="{x + off}" y="{y + off}" rx="14" ry="14" '
                   f'width="{w}" height="{front_h}" fill="#FFFFFF" stroke="{accent}" '
                   f'stroke-width="1.5" stroke-opacity="{0.35 - (k-1)*0.12:.2f}"{dash_attr}/>')
    front, _ = tile_card(x, y, w, virtual=virtual, label=label, detail_rows=detail_rows)
    out.append(front)
    return "".join(out), front_h

def header_box(x, w, h, *, num, title, subtitle, accent):
    out = [card(x, STAGE_TOP, w, h, fill="#FFFFFF", stroke=C_BORDER)]
    out.append(top_stripe(x, STAGE_TOP, w, accent))
    out.append(f'<circle cx="{x + 26}" cy="{STAGE_TOP + 34}" r="13" fill="{accent}"/>')
    out.append(text(x + 26, STAGE_TOP + 39, str(num), size=15, weight=800,
                    fill="#FFFFFF", anchor="middle"))
    out.append(text(x + 48, STAGE_TOP + 32, title, size=17, weight=800, fill=C_INK))
    out.append(text(x + 48, STAGE_TOP + 50, subtitle, size=11.5, weight=500, fill=C_MUTED_2))
    return "".join(out)

def lane_band(x, y, w, h, *, virtual, title, tag, tag_bold):
    accent = C_VIRTUAL if virtual else C_MATERIAL
    tint = TINT_VIRTUAL if virtual else TINT_MATERIAL
    dash_attr = ' stroke-dasharray="6 4"' if virtual else ""
    out = [f'<rect x="{x}" y="{y}" rx="16" ry="16" width="{w}" height="{h}" '
           f'fill="{tint}" fill-opacity="0.4" stroke="{accent}" stroke-width="1.5"{dash_attr}/>']
    # left rail label
    badge, bw = chip(x + 18, y + 16, title, fg="#FFFFFF", bg=accent)
    out.append(badge)
    tx = x + 18 + bw + 10
    out.append(text(tx, y + 16 + 15, tag, size=11, weight=600, fill=C_MUTED_2))
    tw = int(len(tag) * 6.6)
    out.append(text(tx + tw, y + 16 + 15, tag_bold, size=11, weight=800, fill=accent))
    return "".join(out)

# --- Stage frame --------------------------------------------------------------

def stage_frame(x, w, *, num, title, subtitle, accent, tint):
    out = [card(x, STAGE_TOP, w, STAGE_H, fill="#FFFFFF", stroke=C_BORDER)]
    out.append(top_stripe(x, STAGE_TOP, w, accent))
    # numbered eyebrow
    out.append(f'<circle cx="{x + 26}" cy="{STAGE_TOP + 34}" r="13" fill="{accent}"/>')
    out.append(text(x + 26, STAGE_TOP + 39, str(num), size=15, weight=800,
                    fill="#FFFFFF", anchor="middle"))
    out.append(text(x + 48, STAGE_TOP + 32, title, size=17, weight=800, fill=C_INK))
    out.append(text(x + 48, STAGE_TOP + 50, subtitle, size=11.5, weight=500,
                    fill=C_MUTED_2))
    return "".join(out)

# --- Render -------------------------------------------------------------------

def render():
    canvas_h = STAGE_TOP + STAGE_H + FOOTER_H + PAD
    inner_w = CANVAS_W - 2 * PAD
    col_w = (inner_w - 3 * STAGE_GAP) / 4
    x_source = PAD
    x_load = x_source + col_w + STAGE_GAP
    x_op = x_load + col_w + STAGE_GAP
    x_write = x_op + col_w + STAGE_GAP

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
          <marker id="arrow-teal" markerWidth="10" markerHeight="10" refX="7" refY="3"
                  orient="auto" markerUnits="userSpaceOnUse">
            <path d="M0,0 L7,3 L0,6 Z" fill="{ACCENT_LOAD}"/>
          </marker>
        </defs>
        '''))
    parts.append(f'<rect x="0" y="0" width="{CANVAS_W}" height="{canvas_h}" fill="url(#bg)"/>')

    # ---- Header ----
    parts.append(text(PAD, PAD + 30, "GeoBrix · Virtual & Materialized Tiles",
                      size=30, weight=800, fill=C_INK))
    parts.append(text(PAD, PAD + 58,
                      "Read huge rasters as bytes-free references with virtual tiles — carry paths + "
                      "windows through the DataFrame, read pixels only when an operation needs them",
                      size=15, fill=C_MUTED))
    # legend (top-right): virtual vs materialized
    lx = CANVAS_W - PAD - 340
    parts.append(f'<rect x="{lx}" y="{PAD + 12}" width="16" height="16" rx="3" '
                 f'fill="#FFFFFF" stroke="{C_VIRTUAL}" stroke-width="2" stroke-dasharray="4 3"/>')
    parts.append(text(lx + 24, PAD + 25, "virtual (bytes-free)", size=12, weight=600, fill=C_MUTED))
    parts.append(f'<rect x="{lx + 168}" y="{PAD + 12}" width="16" height="16" rx="3" '
                 f'fill="{TINT_MATERIAL}" stroke="{C_MATERIAL}" stroke-width="2"/>')
    parts.append(text(lx + 192, PAD + 25, "materialized (bytes)", size=12, weight=600, fill=C_MUTED))

    # ============================ STAGE 1 — SOURCE ============================
    parts.append(stage_frame(x_source, col_w, num=1, title="Source",
                             subtitle="Use as-is — or optimize to COG",
                             accent=ACCENT_SOURCE, tint=TINT_SOURCE))
    sy = STAGE_TOP + 96
    left_fmts = [
        ("striped GeoTIFFs", glyph_striped, "full-width strips", True),
        ("tiled GeoTIFFs", glyph_tiled, "block-aligned", True),
        ("NetCDFs", glyph_netcdf, "multidimensional", True),
        ("…", None, "other raster formats", False),
        ("tables / DataFrames", glyph_table, "tile-struct columns", False),
    ]
    gx = x_source + 44
    row_gap = 92
    left_cys = []
    file_cys = []                              # rows that DO converge to COG
    for i, (name, gfn, sub, to_cog) in enumerate(left_fmts):
        cy = sy + i * row_gap
        left_cys.append(cy)
        if to_cog:
            file_cys.append(cy)
        if gfn is None:
            # "..." ellipsis source in a box (matches the ~34px glyph footprint)
            bs = 34
            parts.append(f'<rect x="{gx - bs/2:.1f}" y="{cy - bs/2:.1f}" width="{bs}" '
                         f'height="{bs}" rx="6" fill="{TINT_SOURCE}" stroke="{ACCENT_SOURCE}" '
                         f'stroke-width="2" stroke-dasharray="4 3"/>')
            parts.append(text(gx, cy + 7, "···", size=20, weight=800,
                              fill=ACCENT_SOURCE, anchor="middle"))
        else:
            parts.append(gfn(gx, cy, ACCENT_SOURCE, TINT_SOURCE, s=34))
        parts.append(text(gx + 34, cy - 3, name, size=12, weight=700, fill=C_INK))
        parts.append(text(gx + 34, cy + 13, sub, size=9.5, weight=500, fill=C_MUTED_2))
        if i == len(left_fmts) - 1:
            # "tables / DataFrames" row only: note the MANAGED|EXTERNAL FILE read option
            _fc, _ = chip(gx + 34, cy + 26, "MANAGED | EXTERNAL FILE",
                          fg="#FFFFFF", bg=C_FILE_MANAGED, h=24, size=9.5, pad_x=13)
            parts.append(_fc)
    # COG offset to the right = the (optional) optimization target; dotted
    # convergence arrows from each FILE format show "prepare any file -> COG".
    # The "..." (other formats) and tables/DataFrames do NOT arrow to COG;
    # persisting DataFrame tiles AS COG is the cog_gbx writer at the WRITE stage.
    cog_cx = x_source + col_w - 66
    cog_cy = (file_cys[0] + file_cys[-1]) / 2
    for cy in file_cys:
        parts.append(
            f'<path d="M {gx + 40} {cy} C {cog_cx - 90} {cy} {cog_cx - 78} {cog_cy} '
            f'{cog_cx - 34} {cog_cy}" fill="none" stroke="{ACCENT_SOURCE}" '
            f'stroke-width="1.5" stroke-dasharray="4 4" stroke-opacity="0.5" '
            f'marker-end="url(#arrow)"/>')
    parts.append(glyph_cog(cog_cx, cog_cy, ACCENT_SOURCE, TINT_SOURCE, s=38))
    cog_lbl_y = cog_cy + cog_glyph_half_height(38) + 20   # clear of the base plane
    parts.append(text(cog_cx, cog_lbl_y, "COGs", size=13.5, weight=800,
                      fill=C_INK, anchor="middle"))
    parts.append(text(cog_cx, cog_lbl_y + 16, "tiled + overviews", size=9.5,
                      weight=500, fill=C_MUTED_2, anchor="middle"))
    pc, pcw = chip(0, 0, "prepare_cogs", mono_font=True)
    pc, _ = chip(cog_cx - pcw / 2, cog_lbl_y + 30, "prepare_cogs",
                 fg=ACCENT_SOURCE, bg=TINT_SOURCE, border=ACCENT_SOURCE, mono_font=True)
    parts.append(pc)
    # two centered lines so the note never spills past the card's right edge
    parts.append(text(cog_cx, cog_lbl_y + 68, "optional —", size=9.5,
                      weight=600, fill=C_MUTED_2, anchor="middle"))
    parts.append(text(cog_cx, cog_lbl_y + 82, "read any format as-is", size=9.5,
                      weight=600, fill=C_MUTED_2, anchor="middle"))

    amid = STAGE_TOP + STAGE_H / 2
    # (no SOURCE -> LOAD arrow — Source frames the inputs; the flow starts at Load)

    # ===== STAGES 2 & 3 — LOAD + OPERATE as headers over two shared lanes =====
    mid_x = x_load                       # left edge of the combined middle region
    mid_w = (x_op + col_w) - x_load      # spans Load + Operate columns
    HEADER_BOX_H = 196

    # --- LOAD header (left half of the middle) ---
    parts.append(header_box(x_load, col_w, HEADER_BOX_H, num=2, title="Distributed Load",
                            subtitle="Readers fan out — one window per tile",
                            accent=ACCENT_LOAD))
    hy = STAGE_TOP + 78
    parts.append(text(x_load + 24, hy, "READERS", size=9, weight=700,
                      fill=ACCENT_LOAD, letter_spacing="1.2"))
    rx = x_load + 22
    for opt in ["cog_gbx", "gdal", "netcdf_gbx", "…"]:
        c, cw = chip(rx, hy + 10, opt, fg=ACCENT_LOAD, bg=TINT_LOAD,
                     border=ACCENT_LOAD, mono_font=(opt != "…"))
        parts.append(c)
        rx += cw + 6
    parts.append(text(x_load + 24, hy + 60, "OPTIONS", size=9, weight=700,
                      fill=ACCENT_LOAD, letter_spacing="1.2"))
    rx = x_load + 22
    for opt in ["tileSize", "overlapPercent", "clipPolygons", "…"]:
        c, cw = chip(rx, hy + 70, opt, fg=C_MUTED, bg="#F1F4F8",
                     border=C_BORDER, mono_font=(opt != "…"), size=10.5, pad_x=8)
        parts.append(c)
        rx += cw + 5
    parts.append(text(x_load + 24, hy + 112, "partitioned across executors · COG-optimized per-window range-reads",
                      size=9.5, weight=500, fill=C_MUTED_2))

    # --- OPERATE header (right half of the middle) ---
    parts.append(header_box(x_op, col_w, HEADER_BOX_H, num=3, title="Operate",
                            subtitle="Compose ops — every rst_* takes either tile",
                            accent=ACCENT_OP))
    oy = STAGE_TOP + 82
    ox = x_op + 22
    for row_i, row in enumerate([["rst_clip", "rst_transform", "rst_merge"],
                                 ["rst_slope", "…"]]):
        rx = ox
        for opt in row:
            c, cw = chip(rx, oy + row_i * 32, opt, fg=C_INK, bg="#FFFFFF",
                         border=ACCENT_OP, mono_font=(opt != "…"))
            parts.append(c); rx += cw + 6
    # "output is your choice" — two lines to fit inside the box width
    parts.append(text(ox, oy + 88, "Output is your choice", size=12, weight=700, fill=ACCENT_OP))
    parts.append(text(ox, oy + 106, "auto-materialize · or virtualize_dir=… to stay bytes-free",
                      size=10.5, weight=500, fill=C_MUTED))

    # LOAD -> OPERATE arrow (between the two header boxes)
    parts.append(arrow(x_load + col_w + 3, STAGE_TOP + HEADER_BOX_H/2,
                       x_op - 3, STAGE_TOP + HEADER_BOX_H/2, color=ACCENT_OP, width=2.6))

    # --- Two foundational lanes running UNDER Load + Operate ---
    lane_top = STAGE_TOP + HEADER_BOX_H + 20
    lane_h = 168
    lane_gap = 26
    # VIRTUAL lane
    parts.append(lane_band(mid_x, lane_top, mid_w, lane_h, virtual=True,
                           title="VIRTUAL TILES", tag="bytes-free · ",
                           tag_bold="lightweight tier only"))
    vt_rows = [("path", "/Vol/…/A.tif"), ("window", "0,0,512,512")]
    dc, _ = tile_deck(mid_x + 24, lane_top + 42, 250, virtual=True,
                      label="one window / tile", detail_rows=vt_rows)
    parts.append(dc)
    # notes on the right of the virtual lane
    nx = mid_x + 320
    parts.append(text(nx, lane_top + 56, "≈100 B per row", size=15, weight=800, fill=C_VIRTUAL))
    parts.append(text(nx, lane_top + 76, "~1,400–5,000× smaller than materialized → no ingest OOM",
                      size=10.5, weight=600, fill=C_MUTED))
    parts.append(text(nx, lane_top + 100, "· pixels read lazily, one window at a time",
                      size=10.5, weight=500, fill=C_MUTED_2))
    parts.append(text(nx, lane_top + 118, "· clip / transform / mosaic compose while staying virtual",
                      size=10.5, weight=500, fill=C_MUTED_2))
    parts.append(text(nx, lane_top + 136, "· the DataFrame carries references, not pixels",
                      size=10.5, weight=500, fill=C_MUTED_2))

    # MATERIALIZED lane
    ml_top = lane_top + lane_h + lane_gap
    parts.append(lane_band(mid_x, ml_top, mid_w, lane_h, virtual=False,
                           title="MATERIALIZED TILES", tag="raster bytes in hand · ",
                           tag_bold="both tiers"))
    mt_rows = [("raster", "<GeoTIFF · 184 KB>"), ("window", "0,0,512,512")]
    dc, _ = tile_deck(mid_x + 24, ml_top + 42, 250, virtual=False,
                      label="decoded window", detail_rows=mt_rows)
    parts.append(dc)
    parts.append(text(nx, ml_top + 56, "148–527 KB per row", size=15, weight=800, fill=C_MATERIAL))
    parts.append(text(nx, ml_top + 76, "decoded pixels in the row — ~12–24× faster repeat compute",
                      size=10.5, weight=600, fill=C_MUTED))
    parts.append(text(nx, ml_top + 100, "· self-describing bytes (CRS / nodata / overviews)",
                      size=10.5, weight=500, fill=C_MUTED_2))
    parts.append(text(nx, ml_top + 118, "· required to hand a tile to the heavyweight tier",
                      size=10.5, weight=500, fill=C_MUTED_2))
    parts.append(text(nx, ml_top + 136, "· write-back is a materialization boundary",
                      size=10.5, weight=500, fill=C_MUTED_2))

    # vertical connector between the two lanes = the state switch (both directions)
    cxs = mid_x + 292
    parts.append(f'<line x1="{cxs}" y1="{lane_top + lane_h}" x2="{cxs}" y2="{ml_top}" '
                 f'stroke="{C_MATERIAL}" stroke-width="2.4" marker-end="url(#arrow)"/>')
    parts.append(f'<line x1="{cxs + 22}" y1="{ml_top}" x2="{cxs + 22}" y2="{lane_top + lane_h}" '
                 f'stroke="{C_VIRTUAL}" stroke-width="2.4" stroke-dasharray="5 4" marker-end="url(#arrow-teal)"/>')
    parts.append(text(cxs + 44, (lane_top + lane_h + ml_top)/2 - 4, "materialize", size=9.5,
                      weight=700, fill=C_MATERIAL))
    parts.append(text(cxs + 44, (lane_top + lane_h + ml_top)/2 + 12, "virtualize_dir",
                      size=9.5, weight=700, fill=C_VIRTUAL))

    # (no OPERATE -> WRITE arrow — Write frames the outputs/sinks)

    # ============================ STAGE 4 — WRITE =============================
    parts.append(stage_frame(x_write, col_w, num=4, title="Write",
                             subtitle="Persist to files or tables",
                             accent=ACCENT_WRITE, tint=TINT_WRITE))
    wy = STAGE_TOP + 92
    # sink 1: writers -> file
    parts.append(f'<rect x="{x_write + 22}" y="{wy}" width="{col_w - 44}" height="128" rx="12" '
                 f'fill="#FFFFFF" stroke="{ACCENT_WRITE}" stroke-width="1.5" filter="url(#card-shadow)"/>')
    parts.append(top_stripe(x_write + 22, wy, col_w - 44, ACCENT_WRITE, r=12))
    parts.append(text(x_write + 38, wy + 30, "writers → file", size=14.5, weight=800, fill=C_INK))
    wfmts = [("COG", "cog"), ("GeoTIFF", glyph_tiled), ("NetCDF", glyph_netcdf)]
    n_slots = len(wfmts) + 1  # + a "..." slot for other writers
    slot_w = (col_w - 44 - 24) / n_slots
    gy = wy + 78
    lbl_y = wy + 116          # shared label baseline (ample pad below glyphs)
    for i, (nm, gfn) in enumerate(wfmts):
        gx2 = x_write + 34 + slot_w * (i + 0.5)
        if gfn == "cog":
            # 2-layer COG so it sits at the same visual scale as the flat glyphs
            parts.append(glyph_cog(gx2, gy - 4, ACCENT_WRITE, TINT_WRITE, s=26, layers=2))
        else:
            parts.append(gfn(gx2, gy, ACCENT_WRITE, TINT_WRITE, s=30))
        parts.append(text(gx2, lbl_y, nm, size=10, weight=700, fill=C_MUTED,
                          anchor="middle"))
    # "..." slot = other writers (shapefile/geojson/gpkg/pmtiles/...)
    gx2 = x_write + 34 + slot_w * (n_slots - 0.5)
    parts.append(text(gx2, gy + 6, "···", size=22, weight=800, fill=ACCENT_WRITE,
                      anchor="middle"))
    parts.append(text(gx2, lbl_y, "+ others", size=10, weight=700, fill=C_MUTED,
                      anchor="middle"))
    # sink 2: databricks sql -> table (height extended by 30px to accommodate MANAGED FILE chip)
    wy2 = wy + 152
    parts.append(f'<rect x="{x_write + 22}" y="{wy2}" width="{col_w - 44}" height="158" rx="12" '
                 f'fill="#FFFFFF" stroke="{ACCENT_WRITE}" stroke-width="1.5" filter="url(#card-shadow)"/>')
    parts.append(top_stripe(x_write + 22, wy2, col_w - 44, ACCENT_WRITE, r=12))
    parts.append(text(x_write + 38, wy2 + 30, "Databricks SQL → table", size=14.5,
                      weight=800, fill=C_INK))
    parts.append(glyph_table(x_write + col_w/2, wy2 + 80, ACCENT_WRITE, TINT_WRITE, s=48))
    parts.append(mono(x_write + col_w/2, wy2 + 116, "CREATE TABLE … AS SELECT", size=10.5,
                      weight=600, fill=C_MUTED, anchor="middle"))
    # MANAGED FILE write option — symmetric to Box 1 callout; centered under the mono line
    _mfw = int(len("MANAGED | EXTERNAL FILE") * 0.56 * 9.5) + 13 * 2
    _fc2, _ = chip(x_write + col_w / 2 - _mfw / 2, wy2 + 130, "MANAGED | EXTERNAL FILE",
                   fg="#FFFFFF", bg=C_FILE_MANAGED, h=24, size=9.5, pad_x=13)
    parts.append(_fc2)

    # ---- Footer ----
    parts.append(text(PAD, canvas_h - 12,
                      "databrickslabs/geobrix  ·  lightweight tier (pyrx) · Serverless-ready · no JAR",
                      size=11, fill=C_MUTED_3))
    parts.append(text(CANVAS_W - PAD, canvas_h - 12, "docs/api/virtual-tiles",
                      size=11, fill=C_MUTED_3, anchor="end"))

    parts.append("</svg>")
    return "\n".join(parts)


if __name__ == "__main__":
    import os
    import sys
    default = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "..", "diagrams", "rasterx", "virtual-tiles-lifecycle.svg")
    out = sys.argv[1] if len(sys.argv) > 1 else default
    with open(out, "w") as f:
        f.write(render())
    print(f"wrote {out}")
