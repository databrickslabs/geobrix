#!/usr/bin/env python3
"""Generate the five Orthomosaic notebook series diagrams.

One SVG per diagram: an **overview** of the whole series (config_nb -> 01a ->
01b -> 02_publish -> 03_segment, with the `monitor` utility notebook shown as
a side watcher off 01a and CPU/GPU stages marked), plus one four-stage
pipeline diagram per numbered notebook (**01a**, **01b**, **02**, **03**) —
each with a custom hero glyph per stage and a footer of GeoBrix / Databricks
function chips that the notebook actually uses. Matches the style of
`helios.py` / `eo-series.py` (same palette, card, chip, and footer
conventions) with one addition: a small CPU/GPU compute badge next to a
stage's function chip where the series draws a hardware distinction.

Re-render after editing this script:

    python3 resources/images/generators/orthomosaic.py
    for n in overview 01a 01b 02 03; do
      # window-size is wider/taller than the SVG to absorb Chrome's default
      # body margin; the bbox-trim step below crops it back to SVG bounds.
      "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \\
          --headless --disable-gpu --hide-scrollbars \\
          --force-device-scale-factor=2 --window-size=1560,880 \\
          --screenshot=resources/images/diagrams/orthomosaic/orthomosaic-$n.png \\
          resources/images/diagrams/orthomosaic/orthomosaic-$n.svg
    done
    python3 -c "from PIL import Image, ImageChops; \\
      [Image.open(p).convert('RGB').crop(ImageChops.difference( \\
        Image.open(p).convert('RGB'), \\
        Image.new('RGB', Image.open(p).size, (255,255,255))).getbbox()).save(p) \\
       for p in [f'resources/images/diagrams/orthomosaic/orthomosaic-{n}.png' \\
                 for n in ('overview', '01a', '01b', '02', '03')]]"
"""
import math
import os
from dataclasses import dataclass
from textwrap import dedent

# --- Palette (matches helios.py / eo-series.py exactly) ------------------------

C_INK     = "#0F1B2A"
C_INK_2   = "#1B3139"
C_MUTED   = "#3F4D5E"
C_MUTED_2 = "#5A6878"
C_MUTED_3 = "#7A8794"
C_BORDER  = "#E5E7EB"

# Per-notebook themes
THEMES = {
    "overview": {"accent": "#334155", "tint": "#E7EAEE"},  # slate — whole series
    "01a":      {"accent": "#1F6FB5", "tint": "#E3EEF8"},  # blue   — CPU sparse SfM
    "01b":      {"accent": "#6B4FA0", "tint": "#EDE8F5"},  # violet — GPU dense MVS
    "02":       {"accent": "#E04E2A", "tint": "#FCE9E2"},  # orange — publish
    "03":       {"accent": "#0F8E8B", "tint": "#D5ECEC"},  # teal   — segment + serve
}

# Fixed compute-type badges — orthogonal to the per-notebook accent above, so
# "this stage runs on the GPU" reads the same color in every diagram.
CPU_FG, CPU_BG, CPU_BORDER = "#1F6FB5", "#EAF1F8", "#1F6FB5"
GPU_FG, GPU_BG, GPU_BORDER = "#C05B12", "#FDECD9", "#C05B12"

# --- Layout (per-notebook diagrams) --------------------------------------------

CANVAS_W = 1480
CANVAS_H = 720
PAD      = 36

HEADER_H      = 96
STAGE_TOP_GAP = 28
STAGE_H       = 380
STAGE_GLYPH_H = 200
ARROW_W       = 56

FOOTER_H = 50

# --- Primitives (matches helios.py / eo-series.py) -----------------------------

def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

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

def card(x, y, w, h, *, fill="#FFFFFF", stroke=C_BORDER, r=14, shadow=True, dash=None):
    flt = ' filter="url(#card-shadow)"' if shadow else ""
    ds = f' stroke-dasharray="{dash}"' if dash else ""
    return (f'<rect x="{x}" y="{y}" rx="{r}" ry="{r}" width="{w}" height="{h}" '
            f'fill="{fill}" stroke="{stroke}" stroke-width="1"{ds}{flt}/>')

def top_stripe(x, y, w, color, *, r=14, h=5):
    return (f'<path d="M {x} {y + r} '
            f'A {r} {r} 0 0 1 {x + r} {y} '
            f'H {x + w - r} '
            f'A {r} {r} 0 0 1 {x + w} {y + r} '
            f'V {y + h} '
            f'H {x} Z" fill="{color}"/>')

def chip(x, y, txt, *, fg=C_INK, bg="#F1F4F8", border=None, mono_font=False, h=22, size=12):
    char_w = 7.0 if mono_font else 6.6
    pad_x = 12
    w = int(len(txt) * char_w * (size / 12)) + pad_x * 2
    family = ("ui-monospace, SFMono-Regular, Menlo, Consolas, monospace"
              if mono_font else "Inter, -apple-system, system-ui, sans-serif")
    bs = f' stroke="{border}" stroke-width="1"' if border else ""
    svg = (f'<rect x="{x}" y="{y}" rx="{h/2:.0f}" ry="{h/2:.0f}" '
           f'width="{w}" height="{h}" fill="{bg}"{bs}/>'
           f'<text x="{x + w/2}" y="{y + h/2 + size/3:.0f}" '
           f'text-anchor="middle" font-family="{family}" '
           f'font-size="{size}" font-weight="700" fill="{fg}">{esc(txt)}</text>')
    return svg, w

def compute_chip(x, y, kind):
    """Small fixed-color CPU/GPU badge — orthogonal to the per-notebook accent."""
    if kind == "GPU":
        return chip(x, y, "GPU", fg=GPU_FG, bg=GPU_BG, border=GPU_BORDER,
                    mono_font=True, h=20, size=10.5)
    return chip(x, y, "CPU", fg=CPU_FG, bg=CPU_BG, border=CPU_BORDER,
                mono_font=True, h=20, size=10.5)

def arrow(x1, y, x2, *, color=C_MUTED_3, head=10):
    """Horizontal arrow from (x1, y) to (x2, y)."""
    return (f'<line x1="{x1}" y1="{y}" x2="{x2 - head}" y2="{y}" '
            f'stroke="{color}" stroke-width="2.5" stroke-linecap="round"/>'
            f'<polygon points="{x2},{y} {x2 - head},{y - head/1.5} '
            f'{x2 - head},{y + head/1.5}" fill="{color}"/>')

def arrow_xy(x1, y1, x2, y2, *, color=C_MUTED_3, head=9, dash=None, width=2.2):
    """Point-to-point arrow (used for the monitor side-branch connector)."""
    ds = f' stroke-dasharray="{dash}"' if dash else ""
    ang = math.atan2(y2 - y1, x2 - x1)
    hx = x2 - head * math.cos(ang)
    hy = y2 - head * math.sin(ang)
    perp = ang + math.pi / 2
    p1 = (hx + head * 0.55 * math.cos(perp), hy + head * 0.55 * math.sin(perp))
    p2 = (hx - head * 0.55 * math.cos(perp), hy - head * 0.55 * math.sin(perp))
    return (f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{hx:.1f}" y2="{hy:.1f}" '
            f'stroke="{color}" stroke-width="{width}" stroke-linecap="round"{ds}/>'
            f'<polygon points="{x2:.1f},{y2:.1f} {p1[0]:.1f},{p1[1]:.1f} '
            f'{p2[0]:.1f},{p2[1]:.1f}" fill="{color}"/>')

# --- Glyphs (each centered on cx, cy; scale=1.0 for the full-size per-notebook
#     diagrams, smaller for the mini stage nodes in the overview) -------------

def g_config_panel(cx, cy, color, tint, *, scale=1.0):
    """Settings panel — 3 labeled rows (GPS cluster / camera+GSD / paths)."""
    w, h = 156 * scale, 108 * scale
    x, y = cx - w / 2, cy - h / 2
    out = [
        f'<rect x="{x:.1f}" y="{y:.1f}" rx="{10*scale:.1f}" ry="{10*scale:.1f}" '
        f'width="{w:.1f}" height="{h:.1f}" fill="{tint}" stroke="{color}" '
        f'stroke-width="{2*scale:.1f}"/>'
    ]
    rows = ["GPS cluster", "camera / GSD", "paths"]
    row_h = h / 3.4
    for i, lbl in enumerate(rows):
        ry = y + 14 * scale + i * row_h
        out.append(
            f'<circle cx="{x + 16*scale:.1f}" cy="{ry:.1f}" r="{4*scale:.1f}" fill="{color}"/>'
            f'<text x="{x + 28*scale:.1f}" y="{ry + 4*scale:.1f}" '
            f'font-family="ui-monospace, Menlo, monospace" font-size="{11*scale:.1f}" '
            f'font-weight="600" fill="{color}">{esc(lbl)}</text>'
        )
    return "".join(out)

def g_exif_camera(cx, cy, color, tint, *, scale=1.0):
    """Camera body + lens with a GPS pin badge — EXIF/GPS extraction glyph."""
    w, h = 120 * scale, 78 * scale
    x, y = cx - w / 2, cy - h / 2 + 6 * scale
    out = [
        f'<rect x="{x:.1f}" y="{y:.1f}" rx="{10*scale:.1f}" ry="{10*scale:.1f}" '
        f'width="{w:.1f}" height="{h:.1f}" fill="{tint}" stroke="{color}" '
        f'stroke-width="{2.2*scale:.1f}"/>'
        f'<rect x="{x + 14*scale:.1f}" y="{y - 12*scale:.1f}" width="{34*scale:.1f}" '
        f'height="{14*scale:.1f}" rx="{4*scale:.1f}" fill="{color}"/>'
        f'<circle cx="{cx:.1f}" cy="{y + h/2:.1f}" r="{22*scale:.1f}" '
        f'fill="#FFFFFF" stroke="{color}" stroke-width="{2.2*scale:.1f}"/>'
        f'<circle cx="{cx:.1f}" cy="{y + h/2:.1f}" r="{10*scale:.1f}" fill="{color}" '
        f'fill-opacity="0.55"/>'
    ]
    # GPS pin badge, upper-right
    px, py = x + w - 6 * scale, y - 4 * scale
    out.append(
        f'<circle cx="{px:.1f}" cy="{py:.1f}" r="{13*scale:.1f}" fill="{color}"/>'
        f'<circle cx="{px:.1f}" cy="{py:.1f}" r="{5*scale:.1f}" fill="#FFFFFF"/>'
    )
    return "".join(out)

def g_feature_matches(cx, cy, color, tint, *, scale=1.0):
    """Two overlapping frames with matched feature points — distributed matching glyph."""
    w, h = 92 * scale, 68 * scale
    out = []
    for dx, opacity in ((-26 * scale, 0.55), (26 * scale, 1.0)):
        x = cx + dx - w / 2
        y = cy - h / 2
        out.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" rx="{8*scale:.1f}" ry="{8*scale:.1f}" '
            f'width="{w:.1f}" height="{h:.1f}" fill="{tint}" fill-opacity="{opacity}" '
            f'stroke="{color}" stroke-width="{1.8*scale:.1f}"/>'
        )
    pts_l = [(-52, -14), (-40, 10), (-18, -18), (-30, 20)]
    pts_r = [(2, -10), (16, 14), (36, -16), (24, 18)]
    for (lx, ly), (rx_, ry) in zip(pts_l, pts_r):
        x1, y1 = cx + lx * scale, cy + ly * scale
        x2, y2 = cx + rx_ * scale, cy + ry * scale
        out.append(
            f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" '
            f'stroke="{color}" stroke-width="{1.2*scale:.1f}" stroke-opacity="0.55"/>'
            f'<circle cx="{x1:.1f}" cy="{y1:.1f}" r="{3*scale:.1f}" fill="{color}"/>'
            f'<circle cx="{x2:.1f}" cy="{y2:.1f}" r="{3*scale:.1f}" fill="{color}"/>'
        )
    return "".join(out)

def g_incremental_poses(cx, cy, color, tint, *, scale=1.0):
    """Camera frustums in a ring around a sparse point cloud — SfM mapping glyph."""
    out = []
    R = 58 * scale
    n = 7
    rng = [0.35, 0.6, 0.5, 0.75, 0.42, 0.68, 0.55]
    for i in range(n):
        a = 2 * math.pi * i / n
        px = cx + R * rng[i] * math.cos(a) * 0.4 + R * 0.55 * math.cos(a)
        py = cy + R * rng[i] * math.sin(a) * 0.4 + R * 0.55 * math.sin(a)
        out.append(f'<circle cx="{px:.1f}" cy="{py:.1f}" r="{2.6*scale:.1f}" '
                   f'fill="{color}" fill-opacity="0.85"/>')
    # camera frustums (small triangles) pointing inward, on a ring
    for i in range(6):
        a = 2 * math.pi * i / 6 - math.pi / 2
        bx = cx + R * math.cos(a)
        by = cy + R * math.sin(a)
        tip_a = a + math.pi  # pointing toward center
        tx1 = bx + 11 * scale * math.cos(tip_a + 0.4)
        ty1 = by + 11 * scale * math.sin(tip_a + 0.4)
        tx2 = bx + 11 * scale * math.cos(tip_a - 0.4)
        ty2 = by + 11 * scale * math.sin(tip_a - 0.4)
        out.append(
            f'<polygon points="{bx:.1f},{by:.1f} {tx1:.1f},{ty1:.1f} {tx2:.1f},{ty2:.1f}" '
            f'fill="{tint}" stroke="{color}" stroke-width="{1.6*scale:.1f}"/>'
        )
    out.append(f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="{4*scale:.1f}" fill="{color}"/>')
    return "".join(out)

def g_sparse_ortho(cx, cy, color, tint, *, scale=1.0):
    """Overlapping drone-photo footprints blended into one georeferenced tile."""
    out = []
    w, h = 78 * scale, 56 * scale
    positions = [(-26, -10), (0, -16), (26, -8), (-12, 14), (14, 16)]
    for (dx, dy) in positions:
        x = cx + dx * scale - w / 2
        y = cy + dy * scale - h / 2
        out.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" rx="{6*scale:.1f}" ry="{6*scale:.1f}" '
            f'width="{w:.1f}" height="{h:.1f}" fill="{tint}" fill-opacity="0.45" '
            f'stroke="{color}" stroke-width="{1.4*scale:.1f}"/>'
        )
    out.append(
        f'<rect x="{cx - w:.1f}" y="{cy - h*0.7:.1f}" rx="{8*scale:.1f}" '
        f'width="{2*w:.1f}" height="{1.4*h:.1f}" fill="none" '
        f'stroke="{color}" stroke-width="{2.2*scale:.1f}" stroke-dasharray="{6*scale:.0f} {4*scale:.0f}"/>'
    )
    return "".join(out)

def g_sparse_model_load(cx, cy, color, tint, *, scale=1.0):
    """Folder icon holding a persisted sparse model + gps.json — load glyph."""
    fw, fh = 128 * scale, 84 * scale
    fx, fy = cx - fw / 2, cy - fh / 2 + 6 * scale
    tab_w = 46 * scale
    out = [
        f'<path d="M {fx:.1f} {fy:.1f} H {fx+tab_w:.1f} L {fx+tab_w+10*scale:.1f} {fy-12*scale:.1f} '
        f'H {fx+fw:.1f} V {fy+fh:.1f} Q {fx+fw:.1f} {fy+fh+8*scale:.1f} {fx+fw-8*scale:.1f} {fy+fh+8*scale:.1f} '
        f'H {fx+8*scale:.1f} Q {fx:.1f} {fy+fh+8*scale:.1f} {fx:.1f} {fy+fh:.1f} Z" '
        f'fill="{tint}" stroke="{color}" stroke-width="{2*scale:.1f}" stroke-linejoin="round"/>'
    ]
    out.append(
        f'<rect x="{fx+14*scale:.1f}" y="{fy+22*scale:.1f}" width="{fw-28*scale:.1f}" '
        f'height="{16*scale:.1f}" rx="{4*scale:.1f}" fill="#FFFFFF" stroke="{color}" '
        f'stroke-width="{1.2*scale:.1f}"/>'
        f'<text x="{fx+18*scale:.1f}" y="{fy+34*scale:.1f}" '
        f'font-family="ui-monospace, Menlo, monospace" font-size="{9*scale:.1f}" '
        f'font-weight="700" fill="{color}">sparse/*.bin</text>'
        f'<rect x="{fx+14*scale:.1f}" y="{fy+44*scale:.1f}" width="{fw-28*scale:.1f}" '
        f'height="{16*scale:.1f}" rx="{4*scale:.1f}" fill="#FFFFFF" stroke="{color}" '
        f'stroke-width="{1.2*scale:.1f}"/>'
        f'<text x="{fx+18*scale:.1f}" y="{fy+56*scale:.1f}" '
        f'font-family="ui-monospace, Menlo, monospace" font-size="{9*scale:.1f}" '
        f'font-weight="700" fill="{color}">gps.json</text>'
    )
    return "".join(out)

def g_dense_mvs_gpu(cx, cy, color, tint, *, scale=1.0):
    """Patch-match depth grid with a small GPU chip badge — dense MVS glyph."""
    cells, s = 6, 13 * scale
    span = cells * s
    x0, y0 = cx - span / 2, cy - span / 2
    out = [f'<rect x="{x0:.1f}" y="{y0:.1f}" width="{span:.1f}" height="{span:.1f}" '
           f'fill="{tint}" stroke="{color}" stroke-width="{2*scale:.1f}"/>']
    for r in range(cells):
        for c in range(cells):
            depth = 0.15 + 0.7 * ((math.sin(r * 1.3 + c * 0.7) + 1) / 2)
            out.append(f'<rect x="{x0+c*s:.1f}" y="{y0+r*s:.1f}" width="{s:.1f}" '
                       f'height="{s:.1f}" fill="{color}" fill-opacity="{depth:.2f}"/>')
    # GPU chip badge overlay, upper-left
    gx, gy = x0 - 14 * scale, y0 - 10 * scale
    out.append(
        f'<rect x="{gx:.1f}" y="{gy:.1f}" width="{30*scale:.1f}" height="{20*scale:.1f}" '
        f'rx="{4*scale:.1f}" fill="{GPU_BG}" stroke="{GPU_BORDER}" stroke-width="{1.6*scale:.1f}"/>'
        f'<text x="{gx+15*scale:.1f}" y="{gy+14*scale:.1f}" text-anchor="middle" '
        f'font-family="ui-monospace, Menlo, monospace" font-size="{9*scale:.1f}" '
        f'font-weight="800" fill="{GPU_FG}">GPU</text>'
    )
    return "".join(out)

def g_shared_enu_products(cx, cy, color, tint, *, scale=1.0):
    """Multiple cluster blobs aligned onto one shared ENU axis frame."""
    out = []
    ax_len = 46 * scale
    ox, oy = cx - 8 * scale, cy + 20 * scale
    out.append(
        f'<line x1="{ox:.1f}" y1="{oy:.1f}" x2="{ox+ax_len:.1f}" y2="{oy:.1f}" '
        f'stroke="{color}" stroke-width="{1.6*scale:.1f}"/>'
        f'<line x1="{ox:.1f}" y1="{oy:.1f}" x2="{ox:.1f}" y2="{oy-ax_len:.1f}" '
        f'stroke="{color}" stroke-width="{1.6*scale:.1f}"/>'
    )
    clusters = [(-34, -34, 12), (6, -46, 9), (30, -18, 14), (-10, -8, 8)]
    for (dx, dy, r) in clusters:
        out.append(
            f'<circle cx="{cx+dx*scale:.1f}" cy="{cy+dy*scale:.1f}" r="{r*scale:.1f}" '
            f'fill="{tint}" fill-opacity="0.8" stroke="{color}" stroke-width="{1.6*scale:.1f}"/>'
        )
    return "".join(out)

def g_dense_cloud(cx, cy, color, tint, *, scale=1.0):
    """A dense scattered point cloud with a small LAZ tag — dense product glyph."""
    out = []
    R = 52 * scale
    n = 90
    for i in range(n):
        a = (i * 2.399963)  # golden-angle spiral for a natural-looking dense fill
        r = R * math.sqrt((i + 0.5) / n)
        px = cx + r * math.cos(a)
        py = cy - 6 * scale + r * math.sin(a) * 0.62
        out.append(f'<circle cx="{px:.1f}" cy="{py:.1f}" r="{1.6*scale:.1f}" '
                   f'fill="{color}" fill-opacity="0.75"/>')
    out.append(
        f'<rect x="{cx-24*scale:.1f}" y="{cy+38*scale:.1f}" width="{48*scale:.1f}" '
        f'height="{18*scale:.1f}" rx="{5*scale:.1f}" fill="{color}"/>'
        f'<text x="{cx:.1f}" y="{cy+50*scale:.1f}" text-anchor="middle" '
        f'font-family="ui-monospace, Menlo, monospace" font-size="{10*scale:.1f}" '
        f'font-weight="800" fill="#FFFFFF">.laz</text>'
    )
    return "".join(out)

def g_percentile_stretch(cx, cy, color, tint, *, scale=1.0):
    """Histogram with two percentile-clip lines — color correction glyph."""
    out = []
    w, h = 130 * scale, 78 * scale
    x0, y0 = cx - w / 2, cy + h / 2
    bars = [0.2, 0.35, 0.55, 0.8, 0.95, 0.9, 0.7, 0.5, 0.32, 0.18, 0.1, 0.06]
    bw = w / len(bars)
    for i, v in enumerate(bars):
        bh = v * h
        out.append(f'<rect x="{x0+i*bw:.1f}" y="{y0-bh:.1f}" width="{bw*0.8:.1f}" '
                   f'height="{bh:.1f}" fill="{tint}" stroke="{color}" '
                   f'stroke-width="{1*scale:.1f}"/>')
    for frac in (0.16, 0.86):
        lx = x0 + frac * w
        out.append(f'<line x1="{lx:.1f}" y1="{y0-h:.1f}" x2="{lx:.1f}" y2="{y0:.1f}" '
                   f'stroke="{color}" stroke-width="{2.2*scale:.1f}" stroke-dasharray="'
                   f'{5*scale:.0f} {3*scale:.0f}"/>')
    return "".join(out)

def g_cog_file(cx, cy, color, tint, *, scale=1.0):
    """A single dog-eared COG file icon — cloud-optimised GeoTIFF glyph."""
    w, h = 76 * scale, 96 * scale
    x, y = cx - w / 2, cy - h / 2
    ear = 16 * scale
    out = [
        f'<path d="M {x:.1f} {y:.1f} H {x+w-ear:.1f} L {x+w:.1f} {y+ear:.1f} '
        f'V {y+h:.1f} H {x:.1f} Z" fill="{tint}" stroke="{color}" '
        f'stroke-width="{2.2*scale:.1f}" stroke-linejoin="round"/>'
        f'<path d="M {x+w-ear:.1f} {y:.1f} V {y+ear:.1f} H {x+w:.1f}" '
        f'fill="none" stroke="{color}" stroke-width="{1.8*scale:.1f}"/>'
    ]
    for i in range(3):
        ry = y + h * 0.5 + i * 14 * scale
        out.append(f'<line x1="{x+12*scale:.1f}" y1="{ry:.1f}" x2="{x+w-12*scale:.1f}" '
                   f'y2="{ry:.1f}" stroke="{color}" stroke-opacity="0.4" '
                   f'stroke-width="{1.4*scale:.1f}"/>')
    out.append(
        f'<rect x="{x+8*scale:.1f}" y="{y+16*scale:.1f}" width="{w-16*scale:.1f}" '
        f'height="{18*scale:.1f}" rx="{4*scale:.1f}" fill="{color}"/>'
        f'<text x="{cx:.1f}" y="{y+29*scale:.1f}" text-anchor="middle" '
        f'font-family="ui-monospace, Menlo, monospace" font-size="{10*scale:.1f}" '
        f'font-weight="800" fill="#FFFFFF">COG</text>'
    )
    return "".join(out)

def g_xyz_pyramid(cx, cy, color, tint, *, scale=1.0):
    """XYZ tile pyramid — stacked levels narrowing upward (matches helios.py)."""
    out = []
    levels = [(3, -50, 26, "z16"), (2, -28, 50, "z14"), (1, -4, 78, "z12"), (0, 20, 106, "z0 ")]
    h_each = 18 * scale
    for (lvl, rel_y, w, label) in levels:
        ww = w * scale
        x = cx - ww / 2
        y = cy + rel_y * scale
        alpha = 0.5 + lvl * 0.15
        out.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" rx="{4*scale:.1f}" ry="{4*scale:.1f}" '
            f'width="{ww:.1f}" height="{h_each:.1f}" fill="{tint}" '
            f'fill-opacity="{alpha:.2f}" stroke="{color}" stroke-width="{1.6*scale:.1f}"/>'
            f'<text x="{cx:.1f}" y="{y+h_each/2+3.5*scale:.1f}" text-anchor="middle" '
            f'font-family="ui-monospace, Menlo, monospace" font-size="{9*scale:.1f}" '
            f'font-weight="700" fill="{color}">{label}</text>'
        )
    return "".join(out)

def g_stacked_archive(cx, cy, color, tint, *, scale=1.0):
    """Stacked-archive icon — PMTiles archive glyph (matches helios.py)."""
    out = []
    layers = [(10, 16), (5, 8), (0, 0)]
    w, h = 100 * scale, 60 * scale
    for (dx, dy) in layers:
        x = cx - w / 2 + dx * scale
        y = cy - h / 2 + dy * scale - 12 * scale
        fill = tint if dx > 0 else "#FFFFFF"
        opacity = "0.7" if dx > 0 else "1.0"
        out.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" rx="{7*scale:.1f}" ry="{7*scale:.1f}" '
            f'width="{w:.1f}" height="{h:.1f}" fill="{fill}" fill-opacity="{opacity}" '
            f'stroke="{color}" stroke-width="{1.8*scale:.1f}"/>'
        )
    front_x, front_y = cx - w / 2, cy - h / 2 - 12 * scale
    out.append(
        f'<rect x="{front_x:.1f}" y="{front_y:.1f}" width="{w:.1f}" height="{16*scale:.1f}" '
        f'rx="{7*scale:.1f}" fill="{color}"/>'
        f'<text x="{cx:.1f}" y="{front_y+11*scale:.1f}" text-anchor="middle" '
        f'font-family="Inter, sans-serif" font-size="{9*scale:.1f}" font-weight="800" '
        f'fill="#FFFFFF">.pmtiles</text>'
    )
    return "".join(out)

def g_cog_input(cx, cy, color, tint, *, scale=1.0):
    """A published COG being read — input glyph (smaller COG file, no write accents)."""
    return g_cog_file(cx, cy, color, tint, scale=scale * 0.85)

def g_segment_objects(cx, cy, color, tint, *, scale=1.0):
    """Raster grid with instance-object polygons overlaid — GeoSAM objects glyph."""
    out = []
    w, h = 130 * scale, 100 * scale
    x, y = cx - w / 2, cy - h / 2
    out.append(f'<rect x="{x:.1f}" y="{y:.1f}" rx="{8*scale:.1f}" width="{w:.1f}" '
               f'height="{h:.1f}" fill="{tint}" stroke="{color}" stroke-width="{2*scale:.1f}"/>')
    blobs = [
        ([(-40, -24), (-14, -32), (2, -10), (-20, 6), (-46, -2)], "0"),
        ([(14, -6), (44, -14), (52, 12), (28, 26), (8, 16)], "1"),
        ([(-30, 24), (-6, 20), (0, 40), (-24, 44)], "2"),
    ]
    for (pts, lbl) in blobs:
        poly = " ".join(f"{cx+px*scale:.1f},{cy+py*scale:.1f}" for px, py in pts)
        cxp = cx + sum(p[0] for p in pts) / len(pts) * scale
        cyp = cy + sum(p[1] for p in pts) / len(pts) * scale
        out.append(f'<polygon points="{poly}" fill="{color}" fill-opacity="0.32" '
                   f'stroke="{color}" stroke-width="{1.8*scale:.1f}"/>'
                   f'<text x="{cxp:.1f}" y="{cyp+3.5*scale:.1f}" text-anchor="middle" '
                   f'font-family="ui-monospace, Menlo, monospace" font-size="{10*scale:.1f}" '
                   f'font-weight="800" fill="{color}">{lbl}</text>')
    return "".join(out)

def g_landcover_regions(cx, cy, color, tint, *, scale=1.0):
    """A tile split into 4 land-cover class regions — vegetation/bare/impervious/dark."""
    out = []
    w, h = 130 * scale, 100 * scale
    x, y = cx - w / 2, cy - h / 2
    classes = [
        ("#6AAA6A", 0.0, 0.0, 0.55, 0.6),   # vegetation
        ("#B09070", 0.55, 0.0, 0.45, 0.35), # bare
        ("#9AABB8", 0.0, 0.6, 0.4, 0.4),    # impervious
        ("#3F4D5E", 0.4, 0.6, 0.6, 0.4),    # dark
    ]
    out.append(f'<rect x="{x:.1f}" y="{y:.1f}" rx="{8*scale:.1f}" width="{w:.1f}" '
               f'height="{h:.1f}" fill="#FFFFFF" stroke="{color}" stroke-width="{2*scale:.1f}"/>')
    for (col, fx, fy, fw, fh) in classes:
        out.append(f'<rect x="{x+fx*w:.1f}" y="{y+fy*h:.1f}" width="{fw*w:.1f}" '
                   f'height="{fh*h:.1f}" fill="{col}" fill-opacity="0.75"/>')
    out.append(f'<rect x="{x:.1f}" y="{y:.1f}" rx="{8*scale:.1f}" width="{w:.1f}" '
               f'height="{h:.1f}" fill="none" stroke="{color}" stroke-width="{2*scale:.1f}"/>')
    return "".join(out)

def g_served_endpoint(cx, cy, color, tint, *, scale=1.0):
    """A model-serving endpoint box behind a gateway — served endpoint glyph."""
    w, h = 120 * scale, 70 * scale
    x, y = cx - w / 2, cy - h / 2 + 6 * scale
    out = [
        f'<rect x="{x:.1f}" y="{y:.1f}" rx="{10*scale:.1f}" width="{w:.1f}" '
        f'height="{h:.1f}" fill="{tint}" stroke="{color}" stroke-width="{2.2*scale:.1f}"/>'
    ]
    for i in range(3):
        ry = y + 14 * scale + i * 16 * scale
        out.append(f'<circle cx="{x+16*scale:.1f}" cy="{ry:.1f}" r="{3*scale:.1f}" '
                   f'fill="{color}"/>'
                   f'<line x1="{x+26*scale:.1f}" y1="{ry:.1f}" x2="{x+w-14*scale:.1f}" '
                   f'y2="{ry:.1f}" stroke="{color}" stroke-opacity="0.45" '
                   f'stroke-width="{1.6*scale:.1f}"/>')
    # antenna / API arrows
    ax, ay = cx, y - 4 * scale
    out.append(f'<line x1="{ax:.1f}" y1="{ay:.1f}" x2="{ax:.1f}" y2="{ay-14*scale:.1f}" '
               f'stroke="{color}" stroke-width="{2*scale:.1f}"/>'
               f'<circle cx="{ax:.1f}" cy="{ay-16*scale:.1f}" r="{4*scale:.1f}" fill="{color}"/>')
    return "".join(out)

def g_pulse_monitor(cx, cy, color, *, scale=1.0):
    """A small pulse/heartbeat line — the `monitor` live-progress watcher glyph."""
    w = 60 * scale
    pts = [(-30, 0), (-16, 0), (-8, -14), (0, 12), (8, -20), (16, 0), (30, 0)]
    poly = " ".join(f"{cx+px*scale:.1f},{cy+py*scale:.1f}" for px, py in pts)
    return (f'<polyline points="{poly}" fill="none" stroke="{color}" '
            f'stroke-width="{2.2*scale:.1f}" stroke-linecap="round" '
            f'stroke-linejoin="round"/>')

# --- Stage container (matches helios.py) ---------------------------------------

@dataclass
class Stage:
    title: str
    subtitle: str = ""
    glyph: callable = None
    chip_text: str = ""
    compute: str = None  # None | "CPU" | "GPU"

def render_stage(x, y, w, stage, accent, tint):
    """Render a single pipeline-stage card."""
    h = STAGE_H
    out = [card(x, y, w, h)]
    out.append(top_stripe(x, y, w, accent))

    if stage.glyph:
        out.append(stage.glyph(x + w / 2, y + 20 + STAGE_GLYPH_H / 2, accent, tint))

    title_y = y + 20 + STAGE_GLYPH_H + 16
    out.append(text(x + w / 2, title_y, stage.title,
                    size=16, weight=800, fill=C_INK, anchor="middle"))

    row_y = title_y + 14
    if stage.chip_text or stage.compute:
        widgets = []
        if stage.chip_text:
            widgets.append(("chip", stage.chip_text))
        if stage.compute:
            widgets.append(("compute", stage.compute))
        total_w = 0
        rendered = []
        for kind, val in widgets:
            if kind == "chip":
                svg, cw = chip(0, 0, val, fg=accent, bg=tint, mono_font=True)
            else:
                svg, cw = compute_chip(0, 0, val)
            rendered.append((svg, cw))
            total_w += cw
        total_w += 8 * (len(rendered) - 1)
        cx0 = x + (w - total_w) / 2
        pieces = []
        cxr = cx0
        for (kind, val), (svg, cw) in zip(widgets, rendered):
            if kind == "chip":
                svg2, _ = chip(cxr, row_y, val, fg=accent, bg=tint, mono_font=True)
            else:
                svg2, _ = compute_chip(cxr, row_y, val)
            pieces.append(svg2)
            cxr += cw + 8
        out.append("".join(pieces))
        cap_top = row_y + 48
    else:
        cap_top = title_y + 34

    if stage.subtitle:
        out.extend(_wrap_text(x + 14, cap_top, w - 28, stage.subtitle,
                              size=12, fill=C_MUTED, line_h=16))
    return "".join(out)

def _wrap_text(x, y, max_w, s, *, size=12, fill=C_MUTED, line_h=16):
    """Naive word-wrap for sans-serif text."""
    char_w = size * 0.55
    max_chars = max(8, int(max_w / char_w))
    words = s.split()
    lines = []
    cur = ""
    for w in words:
        test = (cur + " " + w).strip()
        if len(test) > max_chars and cur:
            lines.append(cur)
            cur = w
        else:
            cur = test
    if cur:
        lines.append(cur)
    out = []
    for i, line in enumerate(lines[:3]):
        out.append(
            f'<text x="{x + max_w / 2}" y="{y + i * line_h}" text-anchor="middle" '
            f'font-family="Inter, sans-serif" font-size="{size}" '
            f'fill="{fill}">{esc(line)}</text>'
        )
    return out

# --- Header / footer (matches helios.py) ---------------------------------------

def render_header(badge_label, title, subtitle, accent, series_pill_text, *,
                  compute_badge=None):
    out = []
    bsize = 60
    bx, by = PAD, PAD + 4
    label_size = 32 if len(badge_label) <= 2 else 22
    out.append(
        f'<rect x="{bx}" y="{by}" rx="14" ry="14" width="{bsize}" height="{bsize}" '
        f'fill="{accent}"/>'
        f'<text x="{bx + bsize/2}" y="{by + bsize/2 + 10}" text-anchor="middle" '
        f'font-family="Inter, sans-serif" font-size="{label_size}" font-weight="900" '
        f'fill="#FFFFFF">{esc(badge_label.upper())}</text>'
    )
    tx = bx + bsize + 18
    out.append(text(tx, by + 28, title, size=28, weight=800, fill=C_INK))
    out.append(text(tx, by + 54, subtitle, size=14, fill=C_MUTED))
    if compute_badge:
        label, fg, bg, border = compute_badge
        pill_svg, _ = chip(tx, by + 68, label, fg=fg, bg=bg, border=border,
                           mono_font=True, h=22, size=11)
        out.append(pill_svg)
    pill_text = series_pill_text
    pw = int(len(pill_text) * 6.6) + 24
    out.append(
        f'<rect x="{CANVAS_W - PAD - pw}" y="{PAD + 12}" rx="13" ry="13" '
        f'width="{pw}" height="26" fill="{C_INK}"/>'
        f'<text x="{CANVAS_W - PAD - pw/2}" y="{PAD + 30}" text-anchor="middle" '
        f'font-family="Inter, sans-serif" font-size="12" font-weight="700" '
        f'fill="#FFFFFF">{esc(pill_text)}</text>'
    )
    return "".join(out)

def render_footer(chips, accent, tint, label="KEY FUNCTIONS", *, canvas_h=CANVAS_H):
    """Footer band of mono chips for the headline functions used in the notebook."""
    out = []
    fy = canvas_h - PAD - FOOTER_H
    out.append(text(PAD, fy + 16, label,
                    size=10, weight=700, fill=C_MUTED_3, letter_spacing="1.6"))
    cx = PAD + 130
    cy = fy + 6
    for c in chips:
        chip_svg, cw = chip(cx, cy, c, fg=accent, bg=tint, mono_font=True, h=24)
        out.append(chip_svg)
        cx += cw + 8
    out.append(text(CANVAS_W - PAD, fy + 16,
                    "databrickslabs/geobrix  ·  pycolmap  ·  GeoSAM",
                    size=11, fill=C_MUTED_3, anchor="end"))
    return "".join(out)

# --- Per-notebook content -------------------------------------------------------

NOTEBOOKS = {
    "01a": {
        "badge": "01a", "series_pill": "Orthomosaic Series  ·  01a (CPU)",
        "title": "Sparse SfM: telemetry to orthomosaic",
        "subtitle": "EXIF/GPS extraction → distributed matching → incremental mapping → sparse orthomosaic",
        "compute_badge": ("CPU · Serverless env5", CPU_FG, CPU_BG, CPU_BORDER),
        "stages": [
            Stage(title="EXIF + GPS QC",
                  subtitle="exif_gbx reads GPS, camera model, and focal length per image; gsd_from_telemetry confirms the target GSD; qc mode drops images below MIN_SHARPNESS",
                  glyph=g_exif_camera,
                  chip_text="exif_gbx"),
            Stage(title="Distributed matching",
                  subtitle="sfm.run_sfm fans SIFT feature extraction out one image per Spark task, then distributes candidate-pair matching across Spark tasks with pycolmap's CPU matcher",
                  glyph=g_feature_matches,
                  chip_text="sfm.run_sfm"),
            Stage(title="Incremental mapping",
                  subtitle="pycolmap.incremental_mapping recovers camera poses and a sparse 3-D point cloud from the matched pairs, on the driver",
                  glyph=g_incremental_poses,
                  chip_text="pycolmap"),
            Stage(title="Sparse orthomosaic",
                  subtitle="ortho.sparse_orthomosaic places each GPS cluster in one shared ENU frame (place_clusters_shared_enu) and blends back-projected images into orthomosaic.tif",
                  glyph=g_sparse_ortho,
                  chip_text="ortho.sparse_orthomosaic"),
        ],
        "footer_chips": ["exif_gbx", "gsd_from_telemetry", "sfm.run_sfm", "pycolmap",
                         "ortho.sparse_orthomosaic", "place_clusters_shared_enu"],
    },
    "01b": {
        "badge": "01b", "series_pill": "Orthomosaic Series  ·  01b (GPU, optional)",
        "title": "Dense MVS: GPU upgrade to the sparse model",
        "subtitle": "load persisted sparse model → dense_orthomosaic (one call) → render the dense cloud",
        "compute_badge": ("GPU · 8×H100 (Serverless AI Runtime)", GPU_FG, GPU_BG, GPU_BORDER),
        "stages": [
            Stage(title="Load sparse model",
                  subtitle="Loads each GPS cluster's persisted sparse COLMAP model + gps.json (cluster_models) from 01a's output directory",
                  glyph=g_sparse_model_load,
                  chip_text="cluster_models"),
            Stage(title="Dense MVS — one call",
                  subtitle="ortho.dense_orthomosaic undistorts each cluster and runs patch_match_stereo across the node's GPUs via dense_mvs_pool, fusing depth maps to fused.ply",
                  glyph=g_dense_mvs_gpu,
                  chip_text="ortho.dense_orthomosaic"),
            Stage(title="Shared-ENU + products",
                  subtitle="dense_reconstruct_clusters + dense_clusters_to_products place every cluster in one shared ENU frame and rasterize dense ortho + DSM GeoTIFFs",
                  glyph=g_shared_enu_products,
                  chip_text="dense_reconstruct_clusters"),
            Stage(title="Dense cloud + LAZ",
                  subtitle="lidar_gbx writes sharded per-cluster LAZ then a seam-de-duplicated dense_merged.laz; point_cloud_layer renders the dense cloud inline",
                  glyph=g_dense_cloud,
                  chip_text="lidar_gbx"),
        ],
        "footer_chips": ["ortho.dense_orthomosaic", "dense_reconstruct_clusters",
                         "dense_mvs_pool", "lidar_gbx", "point_cloud_layer"],
    },
    "02": {
        "badge": "02", "series_pill": "Orthomosaic Series  ·  02_publish",
        "title": "Publish: color correction to PMTiles",
        "subtitle": "percentile stretch → cloud-optimised GeoTIFF → XYZ pyramid → PMTiles archive",
        "compute_badge": None,
        "stages": [
            Stage(title="Color correction",
                  subtitle="rst_percentile_stretch applies a per-channel percentile clip, producing orthomosaic_corrected.tif with balanced contrast",
                  glyph=g_percentile_stretch,
                  chip_text="rst_percentile_stretch"),
            Stage(title="Cloud-optimised GeoTIFF",
                  subtitle="cog_gbx writer converts the corrected raster to a COG with internal tiling and overview levels — orthomosaic_cog.tif",
                  glyph=g_cog_file,
                  chip_text="cog_gbx"),
            Stage(title="XYZ tile pyramid",
                  subtitle="gbx_rst_xyzpyramid warps the COG onto the WebMercatorQuad grid, generating a z/x/y tile pyramid",
                  glyph=g_xyz_pyramid,
                  chip_text="gbx_rst_xyzpyramid"),
            Stage(title="PMTiles archive",
                  subtitle="pmtiles_gbx encodes the pyramid into a self-contained orthomosaic.pmtiles archive, previewable in a browser with no tile server",
                  glyph=g_stacked_archive,
                  chip_text="pmtiles_gbx"),
        ],
        "footer_chips": ["rst_percentile_stretch", "cog_gbx", "gbx_rst_xyzpyramid", "pmtiles_gbx"],
    },
    "03": {
        "badge": "03", "series_pill": "Orthomosaic Series  ·  03_segment (optional)",
        "title": "Segment: objects, land cover, and serving",
        "subtitle": "published COG → GeoSAM instance objects + RasterX land cover → served endpoint",
        "compute_badge": None,
        "stages": [
            Stage(title="Published COG",
                  subtitle="03_segment reads orthomosaic_cog.tif from 02_publish — no new photogrammetry, standalone notebook",
                  glyph=g_cog_input,
                  chip_text="orthomosaic_cog.tif"),
            Stage(title="Instance objects",
                  subtitle="segment_raster chips the COG, runs GeoSAM's automatic-mask segmenter across the node's GPUs, and stitches georeferenced object polygons",
                  glyph=g_segment_objects,
                  chip_text="segment_raster",
                  compute="GPU"),
            Stage(title="Land cover",
                  subtitle="rst_land_cover classifies a downsampled tile into vegetation / bare / impervious / dark region classes (spectral, kmeans, or hybrid)",
                  glyph=g_landcover_regions,
                  chip_text="rst_land_cover",
                  compute="CPU"),
            Stage(title="Served endpoint",
                  subtitle="register_to_unity_gateway + create_endpoint stand up the same GeoSAM backend behind a GPU Model Serving endpoint; ai_query calls it inline from SQL",
                  glyph=g_served_endpoint,
                  chip_text="register_to_unity_gateway"),
        ],
        "footer_chips": ["segment_raster", "rst_land_cover", "register_to_unity_gateway",
                         "create_endpoint", "ai_query"],
    },
}

# --- Main render (per-notebook diagrams) ---------------------------------------

def render_notebook(key):
    nb = NOTEBOOKS[key]
    accent = THEMES[key]["accent"]
    tint = THEMES[key]["tint"]

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

    parts.append(render_header(nb["badge"], nb["title"], nb["subtitle"], accent,
                               nb["series_pill"], compute_badge=nb["compute_badge"]))

    stage_y = PAD + HEADER_H + STAGE_TOP_GAP
    inner_w = CANVAS_W - 2 * PAD
    n = len(nb["stages"])
    arrows_total = (n - 1) * ARROW_W
    stage_w = (inner_w - arrows_total) // n
    cur_x = PAD
    for i, stg in enumerate(nb["stages"]):
        parts.append(render_stage(cur_x, stage_y, stage_w, stg, accent, tint))
        cur_x += stage_w
        if i < n - 1:
            parts.append(arrow(cur_x + 8, stage_y + STAGE_H / 2 - 30,
                               cur_x + ARROW_W - 8, color=accent))
            cur_x += ARROW_W

    parts.append(render_footer(nb["footer_chips"], accent, tint))

    parts.append("</svg>")
    return "\n".join(parts)

# --- Overview diagram (bespoke: 5 main stages + monitor side branch) -----------

OV_CANVAS_W = CANVAS_W
OV_PAD      = PAD
OV_HEADER_H = 118
OV_STAGE_TOP_GAP = 24
OV_STAGE_H  = 300
OV_GLYPH_H  = 118
OV_ARROW_W  = 40
OV_BRANCH_GAP = 34
OV_BRANCH_H = 96
OV_FOOTER_H = 46
OV_CANVAS_H = (OV_PAD + OV_HEADER_H + OV_STAGE_TOP_GAP + OV_STAGE_H +
               OV_BRANCH_GAP + OV_BRANCH_H + 20 + OV_FOOTER_H + OV_PAD)

OV_STAGES = [
    {"key": "config_nb", "title": "config_nb", "subtitle": "shared config: GPS clustering, camera/GSD, paths",
     "glyph": g_config_panel, "compute": None},
    {"key": "01a", "title": "01a — sparse SfM", "subtitle": "exif_gbx → distributed matching → pycolmap",
     "glyph": g_feature_matches, "compute": "CPU"},
    {"key": "01b", "title": "01b — dense MVS", "subtitle": "dense_orthomosaic (optional, GPU upgrade)",
     "glyph": g_dense_mvs_gpu, "compute": "GPU", "optional": True},
    {"key": "02", "title": "02_publish", "subtitle": "color correct → COG → PMTiles",
     "glyph": g_stacked_archive, "compute": None},
    {"key": "03", "title": "03_segment", "subtitle": "objects + land cover + served endpoint (optional)",
     "glyph": g_segment_objects, "compute": "GPU", "compute2": "CPU", "optional": True},
]

def render_overview_stage(x, y, w, stage, accent, tint, *, dashed=False):
    h = OV_STAGE_H
    out = [card(x, y, w, h, dash="6 5" if dashed else None)]
    out.append(top_stripe(x, y, w, accent))

    if stage["glyph"]:
        gy = y + 18 + OV_GLYPH_H / 2
        out.append(stage["glyph"](x + w / 2, gy, accent, tint, scale=0.62))

    title_y = y + 18 + OV_GLYPH_H + 22
    out.append(text(x + w / 2, title_y, stage["title"],
                    size=15, weight=800, fill=C_INK, anchor="middle"))

    row_y = title_y + 12
    computes = [c for c in (stage.get("compute"), stage.get("compute2")) if c]
    if computes:
        widths = [compute_chip(0, 0, c)[1] for c in computes]
        total_w = sum(widths) + 6 * (len(widths) - 1)
        cxr = x + (w - total_w) / 2
        for c, cw in zip(computes, widths):
            svg, _ = compute_chip(cxr, row_y, c)
            out.append(svg)
            cxr += cw + 6
        cap_top = row_y + 34
    else:
        cap_top = title_y + 24

    out.extend(_wrap_text(x + 12, cap_top, w - 24, stage["subtitle"],
                          size=11, fill=C_MUTED, line_h=14))

    if stage.get("optional"):
        out.append(text(x + w / 2, y + h - 12, "optional",
                        size=10, weight=700, fill=C_MUTED_3, anchor="middle",
                        letter_spacing="1"))
    return "".join(out)

def render_overview():
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'viewBox="0 0 {OV_CANVAS_W} {OV_CANVAS_H}" '
        f'width="{OV_CANVAS_W}" height="{OV_CANVAS_H}" '
        f'style="font-family: Inter, -apple-system, system-ui, sans-serif;">'
    ]
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
    parts.append(f'<rect x="0" y="0" width="{OV_CANVAS_W}" height="{OV_CANVAS_H}" '
                 f'fill="url(#bg)"/>')

    accent = THEMES["overview"]["accent"]
    parts.append(render_header(
        "OV", "Orthomosaic Series — drone imagery to served objects + land cover",
        "config_nb → 01a sparse SfM (CPU) → 01b dense MVS (GPU, optional) → 02_publish → 03_segment (optional)",
        accent, "Orthomosaic Series  ·  Overview"))

    # Compute legend, upper-right under the series pill
    leg_x = OV_CANVAS_W - OV_PAD - 210
    leg_y = OV_PAD + 46
    cpu_svg, cpu_w = compute_chip(leg_x, leg_y, "CPU")
    parts.append(cpu_svg)
    parts.append(text(leg_x + cpu_w + 8, leg_y + 14, "CPU-bound stage", size=11, fill=C_MUTED))
    gpu_svg, gpu_w = compute_chip(leg_x, leg_y + 26, "GPU")
    parts.append(gpu_svg)
    parts.append(text(leg_x + gpu_w + 8, leg_y + 40, "GPU-bound stage", size=11, fill=C_MUTED))

    stage_y = OV_PAD + OV_HEADER_H + OV_STAGE_TOP_GAP
    inner_w = OV_CANVAS_W - 2 * OV_PAD
    n = len(OV_STAGES)
    arrows_total = (n - 1) * OV_ARROW_W
    stage_w = (inner_w - arrows_total) // n
    cur_x = OV_PAD
    monitor_anchor = None
    for i, stg in enumerate(OV_STAGES):
        accent_i = THEMES[stg["key"]]["accent"] if stg["key"] in THEMES else accent
        tint_i = THEMES[stg["key"]]["tint"] if stg["key"] in THEMES else THEMES["overview"]["tint"]
        parts.append(render_overview_stage(cur_x, stage_y, stage_w, stg, accent_i, tint_i,
                                           dashed=stg.get("optional", False)))
        if stg["key"] == "01a":
            monitor_anchor = (cur_x + stage_w / 2, stage_y + OV_STAGE_H)
        cur_x += stage_w
        if i < n - 1:
            parts.append(arrow(cur_x + 6, stage_y + OV_STAGE_H / 2 - 20,
                               cur_x + OV_ARROW_W - 6, color=C_MUTED_3))
            cur_x += OV_ARROW_W

    # Monitor side branch — small watcher card off 01a
    if monitor_anchor:
        mx, my0 = monitor_anchor
        branch_y = stage_y + OV_STAGE_H + OV_BRANCH_GAP
        mw, mh = 230, OV_BRANCH_H
        mx0 = mx - mw / 2
        parts.append(arrow_xy(mx, my0 + 4, mx, branch_y - 6,
                              color=THEMES["01a"]["accent"], dash="5 4", width=2))
        parts.append(card(mx0, branch_y, mw, mh, dash="5 4", shadow=False,
                          stroke=THEMES["01a"]["accent"]))
        parts.append(g_pulse_monitor(mx, branch_y + 34, THEMES["01a"]["accent"], scale=0.9))
        parts.append(text(mx, branch_y + 62, "monitor  (optional)",
                          size=12.5, weight=800, fill=C_INK, anchor="middle"))
        parts.append(text(mx, branch_y + 80, "live SfM progress watcher, off 01a",
                          size=10.5, fill=C_MUTED, anchor="middle"))

    parts.append(render_footer(
        ["config_nb", "01a_sfm_orthomosaic", "01b_sfm_orthomosaic_gpu",
         "02_publish", "03_segment"],
        accent, THEMES["overview"]["tint"], label="SERIES NOTEBOOKS",
        canvas_h=OV_CANVAS_H))

    parts.append("</svg>")
    return "\n".join(parts)


if __name__ == "__main__":
    here = os.path.dirname(os.path.abspath(__file__))
    out_dir = os.path.join(here, "..", "diagrams", "orthomosaic")
    os.makedirs(out_dir, exist_ok=True)

    out = os.path.join(out_dir, "orthomosaic-overview.svg")
    with open(out, "w") as f:
        f.write(render_overview())
    print(f"wrote {out}")

    for key in ("01a", "01b", "02", "03"):
        out = os.path.join(out_dir, f"orthomosaic-{key}.svg")
        with open(out, "w") as f:
            f.write(render_notebook(key))
        print(f"wrote {out}")
