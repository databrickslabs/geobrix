#!/usr/bin/env python3
"""Generate the file_gbx access-flow diagram SVG (light-tier read + write).

Two lanes — READ (top) and WRITE (bottom) — each a left->right stage sequence
in the shared GeoBrix glyph style. The READ lane resolves a source through
`open_for_read`, a once-per-session capability-tier probe, size-adaptive
routing (FILE stream / FUSE-of-FILE / staging), into the reader with a
per-partition open-LRU amortization motif. The WRITE lane resolves writer
output through `open_for_write` into MANAGED / EXTERNAL / FUSE modes and their
targets. A no-gating callout closes the picture. Grounded in
python/geobrix/src/databricks/labs/gbx/ds/file_gbx.py.

Re-render after a change:

    python3 resources/images/generators/file-gbx-flow.py
    # then rasterize to PNG (used by docs/readers-writers.mdx):
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \\
        --headless --disable-gpu --hide-scrollbars \\
        --force-device-scale-factor=2 --window-size=1720,1120 \\
        --screenshot=resources/images/diagrams/rasterx/file-gbx-flow.png \\
        resources/images/diagrams/rasterx/file-gbx-flow.svg
    # then auto-crop whitespace:
    python3 -c "
from PIL import Image, ImageChops
p='resources/images/diagrams/rasterx/file-gbx-flow.png'
img=Image.open(p).convert('RGB')
bbox=ImageChops.difference(img, Image.new('RGB', img.size, (255,255,255))).getbbox()
if bbox: img.crop(bbox).save(p)
"
"""
from textwrap import dedent

# --- Palette (shared with rasterx-tile-structure.py / virtual-tiles-lifecycle.py) ---

C_INK     = "#0F1B2A"
C_INK_2   = "#1B3139"
C_MUTED   = "#3F4D5E"
C_MUTED_2 = "#5A6878"
C_MUTED_3 = "#7A8794"
C_BORDER  = "#E5E7EB"

# Lane accents (from the shared palette): READ = Source blue, WRITE = violet.
ACCENT_READ  = "#1F6FB5"; TINT_READ  = "#E3EEF8"   # blue
ACCENT_WRITE = "#7A5AA6"; TINT_WRITE = "#ECE5F5"   # violet

# FILE / governed-storage accent — the indigo used for MANAGED | EXTERNAL FILE
# callouts across the doc set (matches virtual-tiles-lifecycle C_FILE_MANAGED).
C_FILE   = "#5060A8"; TINT_FILE = "#E7E9F6"

# FUSE floor — neutral slate so it reads as the always-available fallback,
# never competing with the FILE indigo.
C_FUSE   = "#41546A"; TINT_FUSE = "#EAEEF3"

# No-gating semantics: green = graceful auto-downgrade, red = explicit-FILE error.
C_OK  = "#1E7D4B"; TINT_OK  = "#E0F2E9"
C_ERR = "#C0392B"; TINT_ERR = "#FBE7E4"

# --- Canvas -------------------------------------------------------------------

PAD       = 40
CANVAS_W  = 1720
HEADER_H  = 96
FOOTER_H  = 30

LANE_GAP  = 26
LANE_H    = 372
BAND_PAD  = 22
STAGE_H   = LANE_H - 2 * BAND_PAD          # white stage-card height inside a lane
NG_H      = 92                             # no-gating callout strip height
PILL_OVERHANG = 24                         # how far the lane pill rises above its band

# Four stage columns inside each lane band.
C1_X, C1_W = 68,   270
C2_X, C2_W = 398,  268
C3_X, C3_W = 726,  420
C4_X, C4_W = 1206, 450

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

def arrow_line(x1, y1, x2, y2, *, marker="arrow", color=C_MUTED_2, width=2.4, dash=None):
    ds = f' stroke-dasharray="{dash}"' if dash else ""
    return (f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{color}" '
            f'stroke-width="{width}"{ds} marker-end="url(#{marker})"/>')

def arrow_curve(x1, y1, x2, y2, *, marker="arrow", color=C_MUTED_2, width=2.2, dash=None):
    mx = (x1 + x2) / 2
    ds = f' stroke-dasharray="{dash}"' if dash else ""
    return (f'<path d="M {x1:.1f} {y1:.1f} C {mx:.1f} {y1:.1f} {mx:.1f} {y2:.1f} '
            f'{x2:.1f} {y2:.1f}" fill="none" stroke="{color}" stroke-width="{width}"'
            f'{ds} marker-end="url(#{marker})"/>')

# --- Glyphs -------------------------------------------------------------------

def glyph_file(cx, cy, color, tint, s=40):
    """A document/FILE page with a folded top-right corner + text lines."""
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

def glyph_volume(cx, cy, color, tint, s=40):
    """A UC Volume — a cylinder (governed storage)."""
    w = s * 0.92
    h = s * 0.92
    x0 = cx - w / 2
    y0 = cy - h / 2
    ry = h * 0.16
    body_top = y0 + ry
    body_bot = y0 + h - ry
    parts = [f'<path d="M {x0:.1f} {body_top:.1f} V {body_bot:.1f} '
             f'A {w/2:.1f} {ry:.1f} 0 0 0 {x0 + w:.1f} {body_bot:.1f} '
             f'V {body_top:.1f} Z" fill="{tint}" stroke="{color}" stroke-width="2"/>']
    parts.append(f'<ellipse cx="{cx:.1f}" cy="{body_top:.1f}" rx="{w/2:.1f}" '
                 f'ry="{ry:.1f}" fill="{tint}" stroke="{color}" stroke-width="2"/>')
    for f in (0.42, 0.72):
        yy = body_top + (body_bot - body_top) * f
        parts.append(f'<path d="M {x0:.1f} {yy:.1f} A {w/2:.1f} {ry:.1f} 0 0 0 '
                     f'{x0 + w:.1f} {yy:.1f}" fill="none" stroke="{color}" '
                     f'stroke-width="1.3" stroke-opacity="0.5"/>')
    return "".join(parts)

def glyph_table(cx, cy, color, tint, s=44, highlight_col=2):
    """Table with a highlighted (FILE) column — a Delta table target."""
    cols, rows = 3, 4
    cw, rh = s / cols, s / rows
    x0, y0 = cx - s / 2, cy - s / 2
    parts = [f'<rect x="{x0:.1f}" y="{y0:.1f}" width="{s}" height="{s}" rx="3" '
             f'fill="#FFFFFF" stroke="{color}" stroke-width="2"/>']
    parts.append(f'<rect x="{x0:.1f}" y="{y0:.1f}" width="{s}" height="{rh:.1f}" '
                 f'fill="{color}" fill-opacity="0.85"/>')
    parts.append(f'<rect x="{x0 + highlight_col*cw:.1f}" y="{y0 + rh:.1f}" '
                 f'width="{cw:.1f}" height="{s - rh:.1f}" fill="{tint}"/>')
    for i in range(1, cols):
        parts.append(f'<line x1="{x0 + i*cw:.1f}" y1="{y0:.1f}" x2="{x0 + i*cw:.1f}" '
                     f'y2="{y0+s:.1f}" stroke="{color}" stroke-width="1" stroke-opacity="0.4"/>')
    for i in range(1, rows):
        parts.append(f'<line x1="{x0:.1f}" y1="{y0 + i*rh:.1f}" x2="{x0+s:.1f}" '
                     f'y2="{y0 + i*rh:.1f}" stroke="{color}" stroke-width="1" stroke-opacity="0.4"/>')
    return "".join(parts)

def glyph_reader(cx, cy, color, tint, s=40):
    """A funnel/reader mark — many pixels in, a stream out."""
    top_w = s
    mid = s * 0.26
    x0, y0 = cx - top_w / 2, cy - s / 2
    d = (f"M {x0:.1f} {y0:.1f} L {x0 + top_w:.1f} {y0:.1f} "
         f"L {cx + mid/2:.1f} {cy:.1f} L {cx + mid/2:.1f} {cy + s*0.36:.1f} "
         f"L {cx - mid/2:.1f} {cy + s*0.36:.1f} L {cx - mid/2:.1f} {cy:.1f} Z")
    return (f'<path d="{d}" fill="{tint}" stroke="{color}" stroke-width="2" '
            f'stroke-linejoin="round"/>')

def glyph_lru(x, y, color, tint):
    """Amortization motif: one open handle serving many windows of a source.

    A folded FILE at the left, a single 'open() x1' pipe, and four window
    squares fanning out on the right — the OpenResourceLRU keeping one open
    warm across all the windows of a source. Anchored at top-left (x, y);
    occupies ~ 220 x 52.
    """
    parts = []
    # source file (small)
    parts.append(glyph_file(x + 16, y + 22, color, tint, s=34))
    # single open pipe
    parts.append(f'<line x1="{x + 34}" y1="{y + 22}" x2="{x + 84}" y2="{y + 22}" '
                 f'stroke="{color}" stroke-width="2.4" marker-end="url(#arrow-file)"/>')
    # a small "1x" open badge on the pipe
    ob, obw = chip(x + 40, y + 2, "open()×1", fg=color, bg="#FFFFFF",
                   border=color, mono_font=True, h=17, size=9, pad_x=6)
    parts.append(ob)
    # four window squares, fanning
    wx = x + 96
    for i, dy in enumerate((-16, -1, 14, 29)):
        sq = 13
        parts.append(f'<rect x="{wx:.1f}" y="{y + 22 + dy - sq/2:.1f}" width="{sq}" '
                     f'height="{sq}" rx="2" fill="{tint}" stroke="{color}" '
                     f'stroke-width="1.4"/>')
        parts.append(f'<line x1="{x + 84}" y1="{y + 22}" x2="{wx:.1f}" '
                     f'y2="{y + 22 + dy:.1f}" stroke="{color}" stroke-width="1.2" '
                     f'stroke-opacity="0.5"/>')
    parts.append(text(wx + 22, y + 12, "windows share", size=9.5, weight=700, fill=color))
    parts.append(text(wx + 22, y + 25, "one open", size=9.5, weight=700, fill=color))
    parts.append(text(wx + 22, y + 40, "per-partition LRU", size=9, weight=500, fill=C_MUTED_2))
    return "".join(parts)

# --- Composite renderers ------------------------------------------------------

def stage_card(x, y, w, *, num, title, subtitle, accent, h=STAGE_H):
    """A numbered white stage card with a colored top-stripe. Caller adds body."""
    out = [card(x, y, w, h)]
    out.append(top_stripe(x, y, w, accent))
    out.append(f'<circle cx="{x + 26}" cy="{y + 34}" r="13" fill="{accent}"/>')
    out.append(text(x + 26, y + 39, str(num), size=15, weight=800,
                    fill="#FFFFFF", anchor="middle"))
    out.append(text(x + 48, y + 31, title, size=16, weight=800, fill=C_INK))
    out.append(text(x + 48, y + 49, subtitle, size=11, weight=500, fill=C_MUTED_2))
    return out

def route_card(x, y, w, h, *, accent, tint, badge, title, lines,
               dash=None, filled=True):
    """A branch sub-card (a read route or a write mode)."""
    body_fill = "#FFFFFF"
    out = [card(x, y, w, h, stroke=accent, dash=dash, shadow=dash is None, fill=body_fill)]
    out.append(top_stripe(x, y, w, accent))
    bg = accent if filled else "#FFFFFF"
    fg = "#FFFFFF" if filled else accent
    border = None if filled else accent
    bc, bw = chip(x + 12, y + 13, badge, fg=fg, bg=bg, border=border, h=20, size=10.5)
    out.append(bc)
    out.append(text(x + 12 + bw + 9, y + 27, title, size=11.5, weight=700, fill=C_INK))
    ly = y + 48
    for ln in lines:
        mono_ln = ln.startswith("`")
        s = ln.replace("`", "")
        if mono_ln:
            out.append(mono(x + 15, ly, s, size=10, weight=600, fill=accent))
        else:
            out.append(text(x + 15, ly, s, size=10, weight=500, fill=C_MUTED))
        ly += 15
    return "".join(out), y + h / 2  # svg + vertical center

def lane_band(y, accent, tint, badge, descriptor):
    """The rounded lane container with a solid accent pill overhanging the top."""
    out = [f'<rect x="{PAD}" y="{y}" rx="18" ry="18" width="{CANVAS_W - 2*PAD}" '
           f'height="{LANE_H}" fill="{tint}" fill-opacity="0.35" '
           f'stroke="{accent}" stroke-width="1.5"/>']
    # accent tab down the left edge
    out.append(f'<rect x="{PAD}" y="{y}" rx="18" ry="18" width="10" '
               f'height="{LANE_H}" fill="{accent}"/>')
    out.append(f'<rect x="{PAD + 5}" y="{y}" width="5" height="{LANE_H}" fill="{accent}"/>')
    # pill sits as a folder-tab above the band; descriptor baseline clears the top border
    pill, pw = chip(PAD + 22, y - PILL_OVERHANG, badge, fg="#FFFFFF", bg=accent,
                    h=30, size=15, pad_x=16)
    out.append(pill)
    out.append(mono(PAD + 22 + pw + 12, y - PILL_OVERHANG + 20, descriptor,
                    size=12, weight=600, fill=C_MUTED_2))
    return "".join(out)

# --- Render -------------------------------------------------------------------

def render():
    lane1_top = PAD + HEADER_H + 34               # room above for the READ pill
    lane2_top = lane1_top + LANE_H + LANE_GAP + 32  # room for the WRITE pill overhang
    ng_top = lane2_top + LANE_H + LANE_GAP + 12
    canvas_h = ng_top + NG_H + 14 + FOOTER_H + PAD

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
          <marker id="arrow-read" markerWidth="10" markerHeight="10" refX="7" refY="3"
                  orient="auto" markerUnits="userSpaceOnUse">
            <path d="M0,0 L7,3 L0,6 Z" fill="{ACCENT_READ}"/>
          </marker>
          <marker id="arrow-write" markerWidth="10" markerHeight="10" refX="7" refY="3"
                  orient="auto" markerUnits="userSpaceOnUse">
            <path d="M0,0 L7,3 L0,6 Z" fill="{ACCENT_WRITE}"/>
          </marker>
          <marker id="arrow-file" markerWidth="10" markerHeight="10" refX="7" refY="3"
                  orient="auto" markerUnits="userSpaceOnUse">
            <path d="M0,0 L7,3 L0,6 Z" fill="{C_FILE}"/>
          </marker>
          <marker id="arrow-fuse" markerWidth="10" markerHeight="10" refX="7" refY="3"
                  orient="auto" markerUnits="userSpaceOnUse">
            <path d="M0,0 L7,3 L0,6 Z" fill="{C_FUSE}"/>
          </marker>
        </defs>
        '''))
    parts.append(f'<rect x="0" y="0" width="{CANVAS_W}" height="{canvas_h}" fill="url(#bg)"/>')

    # ---- Header ----
    parts.append(text(PAD, PAD + 30, "GeoBrix · file_gbx Access Flow",
                      size=30, weight=800, fill=C_INK))
    parts.append(text(PAD, PAD + 58,
                      "One file-access base under every lightweight reader & writer — capability-tiered "
                      "FILE / FUSE routing on read, governed MANAGED / EXTERNAL / FUSE modes on write, "
                      "chosen per runtime",
                      size=14.5, fill=C_MUTED))
    # legend (top-right): FILE vs FUSE meaning
    lx = CANVAS_W - PAD - 372
    parts.append(f'<rect x="{lx}" y="{PAD + 12}" width="16" height="16" rx="3" '
                 f'fill="{TINT_FILE}" stroke="{C_FILE}" stroke-width="2"/>')
    parts.append(text(lx + 24, PAD + 25, "FILE — managed / external (governed)",
                      size=12, weight=600, fill=C_MUTED))
    parts.append(f'<rect x="{lx}" y="{PAD + 36}" width="16" height="16" rx="3" '
                 f'fill="{TINT_FUSE}" stroke="{C_FUSE}" stroke-width="2" stroke-dasharray="4 3"/>')
    parts.append(text(lx + 24, PAD + 49, "FUSE — always-available floor (no FILE column)",
                      size=12, weight=600, fill=C_MUTED))

    # ============================ READ LANE ============================
    parts.append(lane_band(lane1_top, ACCENT_READ, TINT_READ, "READ",
                           "open_for_read(source, access=\"auto\")"))
    sy = lane1_top + BAND_PAD

    # --- Stage 1 — Source ---
    parts += stage_card(C1_X, sy, C1_W, num=1, title="Source",
                        subtitle="path · directory · FILE ref", accent=ACCENT_READ)
    parts.append(glyph_file(C1_X + 52, sy + 108, ACCENT_READ, TINT_READ, s=56))
    parts.append(text(C1_X + 100, sy + 98, "a raster or vector file", size=11.5,
                      weight=700, fill=C_INK))
    parts.append(text(C1_X + 100, sy + 116, "on a UC Volume", size=10, weight=500, fill=C_MUTED_2))
    for i, (k, v) in enumerate([("path", "/Volumes/…/scene.tif"),
                                ("directory", "recursive enumerate"),
                                ("FILE ref", "read_files / list_files")]):
        ry = sy + 150 + i * 22
        parts.append(mono(C1_X + 18, ry, k, size=10, weight=700, fill=ACCENT_READ))
        parts.append(text(C1_X + C1_W - 16, ry, v, size=9.5, weight=500,
                          fill=C_MUTED, anchor="end"))
    _fc, _fw = chip(C1_X + 18, sy + STAGE_H - 40, "MANAGED | EXTERNAL FILE",
                    fg="#FFFFFF", bg=C_FILE, h=24, size=10, pad_x=13)
    parts.append(_fc)

    # --- Stage 2 — open_for_read + capability-tier probe ---
    parts += stage_card(C2_X, sy, C2_W, num=2, title="Resolve access",
                        subtitle="capability tier · no gating", accent=ACCENT_READ)
    _rc, _rw = chip(C2_X + 18, sy + 66, "open_for_read", fg=ACCENT_READ, bg=TINT_READ,
                    border=ACCENT_READ, mono_font=True, h=22, size=11.5)
    parts.append(_rc)
    parts.append(text(C2_X + 18, sy + 108, "CAPABILITY TIER", size=9, weight=700,
                      fill=C_MUTED_3, letter_spacing="1.2"))
    parts.append(text(C2_X + C2_W - 16, sy + 108, "probed 1×/session", size=9,
                      weight=600, fill=C_MUTED_2, anchor="end"))
    tiers = [("read_files", "DBR 13.3+", C_FILE, TINT_FILE, False),
             ("list_files", "DBR 18+", C_FILE, TINT_FILE, False),
             ("FUSE", "floor · always", C_FUSE, TINT_FUSE, True)]
    for i, (nm, dbr, ac, tn, dash) in enumerate(tiers):
        ty = sy + 122 + i * 40
        parts.append(card(C2_X + 16, ty, C2_W - 32, 32, stroke=ac,
                          dash="4 3" if dash else None, shadow=False, r=8, fill=tn))
        parts.append(mono(C2_X + 28, ty + 21, nm, size=11.5, weight=700, fill=ac))
        parts.append(text(C2_X + C2_W - 28, ty + 20, dbr, size=10, weight=600,
                          fill=C_MUTED, anchor="end"))

    # --- Stage 3 — Size-adaptive routing (three read routes) ---
    parts += stage_card(C3_X, sy, C3_W, num=3, title="Size-adaptive routing",
                        subtitle="the resolver picks per source & size", accent=ACCENT_READ)
    route_x = C3_X + 18
    route_w = C3_W - 36
    r_h = 74
    r_gap = 14
    r_top = sy + 64
    read_routes = [
        (C_FILE, TINT_FILE, "FILE-STREAM", "byte-range", None, True,
         ["tiled / COG ≤ GBX_STREAM_MAX_BYTES", "`fr.open()` → /vsimem seekable stream"]),
        (C_FILE, TINT_FILE, "FUSE-of-FILE", "large / striped", "5 4", False,
         ["blocks fetched on demand, no full load", "`fr.as_local_file()` (FILE-backed FUSE)"]),
        (C_FUSE, TINT_FUSE, "STAGING", "probe → stage", "5 4", True,
         ["FUSE tier or direct-open probe fails", "sequential copy ≤ GBX_STAGE_MAX_BYTES"]),
    ]
    read_centers = []
    for i, (ac, tn, badge, title, dash, filled, lines) in enumerate(read_routes):
        ry = r_top + i * (r_h + r_gap)
        svg, cy = route_card(route_x, ry, route_w, r_h, accent=ac, tint=tn,
                             badge=badge, title=title, lines=lines, dash=dash, filled=filled)
        parts.append(svg)
        read_centers.append(cy)

    # --- Stage 4 — Reader + LRU amortization ---
    parts += stage_card(C4_X, sy, C4_W, num=4, title="Reader",
                        subtitle="the light-tier engine reads the window", accent=ACCENT_READ)
    parts.append(glyph_reader(C4_X + 48, sy + 96, ACCENT_READ, TINT_READ, s=44))
    rc1, rw1 = chip(C4_X + 92, sy + 78, "rasterio", fg=ACCENT_READ, bg=TINT_READ,
                    border=ACCENT_READ, mono_font=True)
    parts.append(rc1)
    parts.append(text(C4_X + 92 + rw1 + 8, sy + 93, "raster", size=11, weight=600, fill=C_MUTED_2))
    rc2, rw2 = chip(C4_X + 92, sy + 104, "pyogrio", fg=ACCENT_READ, bg=TINT_READ,
                    border=ACCENT_READ, mono_font=True)
    parts.append(rc2)
    parts.append(text(C4_X + 92 + rw2 + 8, sy + 119, "vector", size=11, weight=600, fill=C_MUTED_2))
    # amortization panel
    amp_y = sy + 150
    parts.append(f'<rect x="{C4_X + 16}" y="{amp_y}" width="{C4_W - 32}" height="98" rx="10" '
                 f'fill="{TINT_FILE}" fill-opacity="0.45" stroke="{C_FILE}" stroke-width="1.3"/>')
    parts.append(text(C4_X + 30, amp_y + 22, "OPEN-COST AMORTIZATION", size=9, weight=700,
                      fill=C_FILE, letter_spacing="1.1"))
    parts.append(glyph_lru(C4_X + 26, amp_y + 30, C_FILE, TINT_FILE))

    # READ arrows: 1→2, 2→(3 routes), (3 routes)→4
    parts.append(arrow_line(C1_X + C1_W, sy + STAGE_H / 2, C2_X, sy + STAGE_H / 2,
                            marker="arrow-read", color=ACCENT_READ, width=2.6))
    for cy in read_centers:
        parts.append(arrow_curve(C2_X + C2_W, sy + STAGE_H / 2, C3_X, cy,
                                 marker="arrow-read", color=ACCENT_READ))
    reader_in_y = sy + 96
    for cy in read_centers:
        parts.append(arrow_curve(C3_X + C3_W, cy, C4_X, reader_in_y,
                                 marker="arrow-read", color=ACCENT_READ))

    # ============================ WRITE LANE ============================
    parts.append(lane_band(lane2_top, ACCENT_WRITE, TINT_WRITE, "WRITE",
                           "open_for_write(spark, df, target, file_mode=\"auto\", layout=…)"))
    wy = lane2_top + BAND_PAD

    # --- Stage 1 — Writer output ---
    parts += stage_card(C1_X, wy, C1_W, num=1, title="Writer output",
                        subtitle="a tile DataFrame", accent=ACCENT_WRITE)
    parts.append(glyph_table(C1_X + 52, wy + 118, ACCENT_WRITE, TINT_WRITE, s=58))
    parts.append(text(C1_X + 100, wy + 96, "rst_* / st_* result", size=11.5,
                      weight=700, fill=C_INK))
    parts.append(text(C1_X + 100, wy + 114, "one row per tile", size=10, weight=500, fill=C_MUTED_2))
    for i, (k, v) in enumerate([("path", "source path (STRING)"),
                                ("raster", "encoded bytes (BINARY)")]):
        ry = wy + 158 + i * 22
        parts.append(mono(C1_X + 18, ry, k, size=10, weight=700, fill=ACCENT_WRITE))
        parts.append(text(C1_X + C1_W - 16, ry, v, size=9.5, weight=500,
                          fill=C_MUTED, anchor="end"))

    # --- Stage 2 — open_for_write + auto-select rule ---
    parts += stage_card(C2_X, wy, C2_W, num=2, title="Resolve mode",
                        subtitle="file_mode=\"auto\" · no gating", accent=ACCENT_WRITE)
    _wc, _ww = chip(C2_X + 18, wy + 66, "open_for_write", fg=ACCENT_WRITE, bg=TINT_WRITE,
                    border=ACCENT_WRITE, mono_font=True, h=22, size=11.5)
    parts.append(_wc)
    parts.append(text(C2_X + 18, wy + 108, "AUTO-SELECT", size=9, weight=700,
                      fill=C_MUTED_3, letter_spacing="1.2"))
    rules = [("filespace given", "→ MANAGED", C_FILE),
             ("no filespace", "→ EXTERNAL", C_FILE),
             ("no FILE (fuse)", "→ FUSE", C_FUSE)]
    for i, (cond, res, ac) in enumerate(rules):
        ty = wy + 122 + i * 40
        parts.append(card(C2_X + 16, ty, C2_W - 32, 32, stroke=ac, shadow=False, r=8,
                          fill="#FFFFFF"))
        parts.append(text(C2_X + 28, ty + 20, cond, size=10.5, weight=600, fill=C_MUTED))
        parts.append(mono(C2_X + C2_W - 28, ty + 20, res, size=10.5, weight=700,
                          fill=ac, anchor="end"))

    # --- Stage 3 — Write modes ---
    parts += stage_card(C3_X, wy, C3_W, num=3, title="Write mode",
                        subtitle="MANAGED owns bytes · EXTERNAL references · FUSE plain",
                        accent=ACCENT_WRITE)
    w_top = wy + 64
    write_modes = [
        (C_FILE, TINT_FILE, "MANAGED", "create_file", None, True,
         ["bytes copied into UC-managed storage", "`FILE MANAGED` column · filespace=/Volumes/…"]),
        (C_FILE, TINT_FILE, "EXTERNAL", "try_to_file", "5 4", False,
         ["references an existing Volume path", "`FILE EXTERNAL` column · no copy"]),
        (C_FUSE, TINT_FUSE, "FUSE", "plain Delta write", "5 4", True,
         ["no FILE column — path STRING / raster BINARY", "direct `/Volumes` write"]),
    ]
    write_centers = []
    for i, (ac, tn, badge, title, dash, filled, lines) in enumerate(write_modes):
        ry = w_top + i * (r_h + r_gap)
        svg, cy = route_card(route_x, ry, route_w, r_h, accent=ac, tint=tn,
                             badge=badge, title=title, lines=lines, dash=dash, filled=filled)
        parts.append(svg)
        write_centers.append(cy)

    # --- Stage 4 — Targets + layout ---
    parts += stage_card(C4_X, wy, C4_W, num=4, title="Target",
                        subtitle="where the rows land", accent=ACCENT_WRITE)
    # target 1: FILE-column Delta table (MANAGED / EXTERNAL)
    t1_y = wy + 62
    parts.append(f'<rect x="{C4_X + 16}" y="{t1_y}" width="{C4_W - 32}" height="78" rx="10" '
                 f'fill="#FFFFFF" stroke="{C_FILE}" stroke-width="1.4" filter="url(#card-shadow)"/>')
    parts.append(top_stripe(C4_X + 16, t1_y, C4_W - 32, C_FILE, r=10))
    parts.append(glyph_table(C4_X + 52, t1_y + 42, C_FILE, TINT_FILE, s=40))
    parts.append(text(C4_X + 88, t1_y + 28, "FILE-column Delta table", size=12.5,
                      weight=800, fill=C_INK))
    parts.append(text(C4_X + 88, t1_y + 46, "MANAGED / EXTERNAL — governed, lifecycle-tracked",
                      size=9.5, weight=500, fill=C_MUTED_2))
    _tfc, _ = chip(C4_X + 88, t1_y + 54, "path STRING · tile_file FILE", fg=C_FILE,
                   bg=TINT_FILE, border=C_FILE, mono_font=True, h=18, size=9, pad_x=8)
    parts.append(_tfc)
    # target 2: Volume path (FUSE)
    t2_y = t1_y + 90
    parts.append(f'<rect x="{C4_X + 16}" y="{t2_y}" width="{C4_W - 32}" height="60" rx="10" '
                 f'fill="#FFFFFF" stroke="{C_FUSE}" stroke-width="1.4" filter="url(#card-shadow)"/>')
    parts.append(top_stripe(C4_X + 16, t2_y, C4_W - 32, C_FUSE, r=10))
    parts.append(glyph_volume(C4_X + 52, t2_y + 32, C_FUSE, TINT_FUSE, s=38))
    parts.append(text(C4_X + 88, t2_y + 28, "Volume path", size=12.5, weight=800, fill=C_INK))
    parts.append(text(C4_X + 88, t2_y + 45, "FUSE — plain Delta, no FILE column",
                      size=9.5, weight=500, fill=C_MUTED_2))
    # layout note strip
    ly_y = t2_y + 70
    parts.append(f'<rect x="{C4_X + 16}" y="{ly_y}" width="{C4_W - 32}" height="42" rx="8" '
                 f'fill="{TINT_WRITE}" fill-opacity="0.5" stroke="{ACCENT_WRITE}" stroke-width="1.2"/>')
    parts.append(text(C4_X + 30, ly_y + 18, "LAYOUT", size=9, weight=700,
                      fill=ACCENT_WRITE, letter_spacing="1.1"))
    lc1, lcw1 = chip(C4_X + 88, ly_y + 6, "ORDER BY path", fg=ACCENT_WRITE, bg="#FFFFFF",
                     border=ACCENT_WRITE, mono_font=True, h=18, size=9, pad_x=7)
    parts.append(lc1)
    parts.append(text(C4_X + 88 + lcw1 + 6, ly_y + 19, "default", size=9, weight=600, fill=C_MUTED_2))
    parts.append(text(C4_X + 30, ly_y + 34, "opt-in CLUSTER BY path — then run OPTIMIZE <table>",
                      size=9.5, weight=500, fill=C_MUTED))

    # WRITE arrows: 1→2, 2→(3 modes), managed+external→table, fuse→volume
    parts.append(arrow_line(C1_X + C1_W, wy + STAGE_H / 2, C2_X, wy + STAGE_H / 2,
                            marker="arrow-write", color=ACCENT_WRITE, width=2.6))
    for cy in write_centers:
        parts.append(arrow_curve(C2_X + C2_W, wy + STAGE_H / 2, C3_X, cy,
                                 marker="arrow-write", color=ACCENT_WRITE))
    table_in_y = t1_y + 35
    vol_in_y = t2_y + 30
    # managed (0) + external (1) → FILE table
    for i in (0, 1):
        parts.append(arrow_curve(C3_X + C3_W, write_centers[i], C4_X, table_in_y,
                                 marker="arrow-file", color=C_FILE))
    # fuse (2) → Volume
    parts.append(arrow_curve(C3_X + C3_W, write_centers[2], C4_X, vol_in_y,
                             marker="arrow-fuse", color=C_FUSE))

    # ============================ NO-GATING CALLOUT ============================
    parts.append(f'<rect x="{PAD}" y="{ng_top}" rx="14" ry="14" width="{CANVAS_W - 2*PAD}" '
                 f'height="{NG_H}" fill="#FFFFFF" stroke="{C_BORDER}" stroke-width="1" '
                 f'filter="url(#card-shadow)"/>')
    parts.append(top_stripe(PAD, ng_top, CANVAS_W - 2 * PAD, C_INK_2))
    parts.append(text(PAD + 26, ng_top + 34, "No gating", size=17, weight=800, fill=C_INK))
    parts.append(text(PAD + 26, ng_top + 56, "the same pipeline runs on any runtime",
                      size=11, weight=500, fill=C_MUTED_2))
    parts.append(text(PAD + 26, ng_top + 74, "opt in to FILE only for its governance",
                      size=11, weight=500, fill=C_MUTED_2))
    # OK path
    ox = PAD + 340
    parts.append(f'<rect x="{ox}" y="{ng_top + 20}" width="26" height="26" rx="13" '
                 f'fill="{TINT_OK}" stroke="{C_OK}" stroke-width="2"/>')
    parts.append(text(ox + 13, ng_top + 39, "✓", size=15, weight=800, fill=C_OK, anchor="middle"))
    parts.append(mono(ox + 40, ng_top + 32, 'access="auto"', size=12, weight=700, fill=C_OK))
    parts.append(text(ox + 40, ng_top + 50, "graceful FUSE fallback — never errors", size=11,
                      weight=500, fill=C_MUTED))
    parts.append(text(ox + 40, ng_top + 66, "the default on every lightweight reader & writer",
                      size=10, weight=500, fill=C_MUTED_2))
    # ERR path
    ex = PAD + 800
    parts.append(f'<rect x="{ex}" y="{ng_top + 20}" width="26" height="26" rx="13" '
                 f'fill="{TINT_ERR}" stroke="{C_ERR}" stroke-width="2"/>')
    parts.append(text(ex + 13, ng_top + 39, "!", size=15, weight=800, fill=C_ERR, anchor="middle"))
    parts.append(mono(ex + 40, ng_top + 32, 'access="managed" / "external"', size=12,
                      weight=700, fill=C_ERR))
    parts.append(text(ex + 40, ng_top + 50, "explicit FILE on a FUSE-only runtime → clear ValueError",
                      size=11, weight=500, fill=C_MUTED))
    parts.append(text(ex + 40, ng_top + 66, "the message names the DBR upgrade path & the auto fallback",
                      size=10, weight=500, fill=C_MUTED_2))

    # ---- Footer ----
    parts.append(text(PAD, canvas_h - 12,
                      "databrickslabs/geobrix  ·  lightweight tier (pyrx)",
                      size=11, fill=C_MUTED_3))
    parts.append(text(CANVAS_W - PAD, canvas_h - 12, "docs/readers-writers",
                      size=11, fill=C_MUTED_3, anchor="end"))

    parts.append("</svg>")
    return "\n".join(parts)


if __name__ == "__main__":
    import os
    import sys
    default = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "..", "diagrams", "rasterx", "file-gbx-flow.svg")
    out = sys.argv[1] if len(sys.argv) > 1 else default
    with open(out, "w") as f:
        f.write(render())
    print(f"wrote {out}")
