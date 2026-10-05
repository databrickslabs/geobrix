#!/usr/bin/env python3
"""Generate per-notebook pipeline diagrams for the wireless-coverage series.

One SVG per notebook: 01, 02a, 02b, 03a, 03b, 04, overview.
Mirrors the orthomosaic series structure; reuses eo-series.py primitives.

Re-render after editing this script:

    python3 resources/images/generators/wireless-coverage.py
    for n in 01 02a 02b 03a 03b 04 overview; do
      "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \\
          --headless --disable-gpu --hide-scrollbars \\
          --force-device-scale-factor=2 --window-size=1500,820 \\
          --screenshot=resources/images/diagrams/wireless-coverage/wireless-coverage-$n.png \\
          resources/images/diagrams/wireless-coverage/wireless-coverage-$n.svg
    done
    python3 -c "
    from PIL import Image, ImageChops
    import glob
    for p in glob.glob('resources/images/diagrams/wireless-coverage/wireless-coverage-*.png'):
        img = Image.open(p).convert('RGB')
        bbox = ImageChops.difference(img, Image.new('RGB', img.size, (255,255,255))).getbbox()
        if bbox: img.crop(bbox).save(p)
    "
    (For the overview use --window-size=1620,880 to avoid card clipping.)
"""
import importlib.util
import math
import os
from dataclasses import dataclass
from textwrap import dedent

_HERE = os.path.dirname(os.path.abspath(__file__))
_EO = os.path.join(_HERE, "eo-series.py")
_spec = importlib.util.spec_from_file_location("eo_series", _EO)
eo = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(eo)

# Shared primitives / constants from eo-series
text, chip, arrow, render_stage, esc = (
    eo.text, eo.chip, eo.arrow, eo.render_stage, eo.esc
)
C_INK, C_MUTED, C_MUTED_2, C_MUTED_3 = eo.C_INK, eo.C_MUTED, eo.C_MUTED_2, eo.C_MUTED_3
C_BORDER = eo.C_BORDER
CANVAS_W, CANVAS_H, PAD = eo.CANVAS_W, eo.CANVAS_H, eo.PAD
HEADER_H, STAGE_TOP_GAP, STAGE_H = eo.HEADER_H, eo.STAGE_TOP_GAP, eo.STAGE_H
ARROW_W, FOOTER_H = eo.ARROW_W, eo.FOOTER_H
card, top_stripe = eo.card, eo.top_stripe
STAGE_GLYPH_H = eo.STAGE_GLYPH_H


# ---------------------------------------------------------------------------
# Stage dataclass (adds compute field, compatible with eo-series render_stage)
# ---------------------------------------------------------------------------

@dataclass
class Stage:
    title: str
    subtitle: str = ""
    glyph: object = None
    chip_text: str = ""
    compute: object = None  # unused in WC (no GPU stages); kept for schema parity


# ---------------------------------------------------------------------------
# Per-notebook themes
# ---------------------------------------------------------------------------

THEMES = {
    "overview": {"accent": "#334155", "tint": "#E7EAEE"},  # slate — series
    "01":       {"accent": "#1F6FB5", "tint": "#E3EEF8"},  # blue  — LiDAR
    "02a":      {"accent": "#1A7D4A", "tint": "#D4EFE0"},  # green — TIN DTM
    "02b":      {"accent": "#627D1A", "tint": "#EBF0C8"},  # olive — morph DTM
    "03a":      {"accent": "#0F8E8B", "tint": "#D5ECEC"},  # teal  — H3 gridding
    "03b":      {"accent": "#6B4FA0", "tint": "#EDE8F5"},  # violet— raster pixel
    "04":       {"accent": "#C47A15", "tint": "#FAECD0"},  # amber — tower siting
}

# Secondary accent for Databricks product built-in functions (all notebooks)
DBX_FG, DBX_BG = "#1F6FB5", "#E3EEF8"


# ---------------------------------------------------------------------------
# Local helpers (supplement eo-series primitives)
# ---------------------------------------------------------------------------

def _card(x, y, w, h, *, fill="#FFFFFF", stroke=C_BORDER, r=14, shadow=True, dash=None):
    """card() with optional stroke-dasharray (eo-series card lacks dash param)."""
    flt = ' filter="url(#card-shadow)"' if shadow else ""
    ds = f' stroke-dasharray="{dash}"' if dash else ""
    return (f'<rect x="{x}" y="{y}" rx="{r}" ry="{r}" width="{w}" height="{h}" '
            f'fill="{fill}" stroke="{stroke}" stroke-width="1"{ds}{flt}/>')


# ---------------------------------------------------------------------------
# Helper: arrow_xy (point-to-point; mirrors orthomosaic.py)
# ---------------------------------------------------------------------------

def arrow_xy(x1, y1, x2, y2, *, color=C_MUTED_3, head=9, dash=None, width=2.2):
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


# ---------------------------------------------------------------------------
# Glyph helpers
# ---------------------------------------------------------------------------

def _hex_pts(cx, cy, R):
    pts = []
    for i in range(6):
        a = math.radians(60 * i - 90)
        pts.append(f"{cx + R * math.cos(a):.1f},{cy + R * math.sin(a):.1f}")
    return " ".join(pts)


# ── NB1 glyphs ─────────────────────────────────────────────────────────────

def g_ept_nodes(cx, cy, color, tint, *, scale=1.0):
    """EPT octree: three levels of branching LAZ nodes."""
    out = []
    bw, bh = int(56 * scale), int(28 * scale)
    # root
    rx0, ry0 = cx - bw // 2, int(cy - 52 * scale)
    out.append(
        f'<rect x="{rx0}" y="{ry0}" rx="{int(5*scale)}" width="{bw}" height="{bh}" '
        f'fill="{tint}" stroke="{color}" stroke-width="{1.6*scale:.1f}"/>'
        f'<text x="{rx0 + bw//2}" y="{ry0 + bh//2 + 4}" text-anchor="middle" '
        f'font-family="ui-monospace,Menlo,monospace" font-size="{int(9*scale)}" '
        f'font-weight="700" fill="{color}">.laz</text>'
    )
    # two child nodes
    for i, dx in enumerate([-int(38*scale), int(38*scale)]):
        cx2 = cx + dx
        cy2 = int(cy - 4 * scale)
        out.append(
            f'<line x1="{cx}" y1="{ry0 + bh}" x2="{cx2}" y2="{cy2}" '
            f'stroke="{color}" stroke-width="{1.2*scale:.1f}" stroke-opacity="0.6"/>'
        )
        out.append(
            f'<rect x="{cx2 - bw//2}" y="{cy2}" rx="{int(5*scale)}" '
            f'width="{bw}" height="{bh}" fill="{tint}" stroke="{color}" '
            f'stroke-width="{1.4*scale:.1f}"/>'
            f'<text x="{cx2}" y="{cy2 + bh//2 + 4}" text-anchor="middle" '
            f'font-family="ui-monospace,Menlo,monospace" font-size="{int(9*scale)}" '
            f'font-weight="700" fill="{color}">.laz</text>'
        )
        # grandchild
        gc_y = int(cy + 42 * scale)
        out.append(
            f'<line x1="{cx2}" y1="{cy2 + bh}" x2="{cx2}" y2="{gc_y}" '
            f'stroke="{color}" stroke-width="{1.0*scale:.1f}" stroke-opacity="0.4"/>'
        )
        out.append(
            f'<rect x="{cx2 - int(24*scale)}" y="{gc_y}" rx="{int(4*scale)}" '
            f'width="{int(48*scale)}" height="{int(20*scale)}" fill="{tint}" '
            f'fill-opacity="0.7" stroke="{color}" stroke-width="{1.0*scale:.1f}"/>'
        )
    return "".join(out)


def g_lidar_tile_stamp(cx, cy, color, tint, *, scale=1.0):
    """Scattered LiDAR points with a tile-key stamp grid."""
    out = []
    # 4×4 tile grid background
    tw = int(120 * scale)
    cols, rows = 4, 4
    cs = tw // cols
    x0, y0 = cx - tw // 2, int(cy - 52 * scale)
    out.append(
        f'<rect x="{x0}" y="{y0}" width="{tw}" height="{tw}" '
        f'fill="{tint}" fill-opacity="0.4" stroke="{color}" stroke-width="{1.4*scale:.1f}"/>'
    )
    for i in range(1, cols):
        out.append(
            f'<line x1="{x0 + i*cs}" y1="{y0}" x2="{x0 + i*cs}" y2="{y0 + tw}" '
            f'stroke="{color}" stroke-opacity="0.3" stroke-width="{0.8*scale:.1f}"/>'
        )
        out.append(
            f'<line x1="{x0}" y1="{y0 + i*cs}" x2="{x0 + tw}" y2="{y0 + i*cs}" '
            f'stroke="{color}" stroke-opacity="0.3" stroke-width="{0.8*scale:.1f}"/>'
        )
    # Scattered points
    _pts_rel = [
        (0.15, 0.12), (0.35, 0.08), (0.62, 0.18), (0.80, 0.10),
        (0.10, 0.35), (0.28, 0.40), (0.55, 0.32), (0.78, 0.38),
        (0.20, 0.60), (0.45, 0.65), (0.68, 0.58), (0.88, 0.62),
        (0.12, 0.82), (0.38, 0.78), (0.72, 0.85), (0.90, 0.80),
    ]
    for fx, fy in _pts_rel:
        px = x0 + int(fx * tw)
        py = y0 + int(fy * tw)
        op = 0.5 + 0.5 * math.sin(fx * 7 + fy * 5)
        out.append(
            f'<circle cx="{px}" cy="{py}" r="{2.4*scale:.1f}" '
            f'fill="{color}" fill-opacity="{op:.2f}"/>'
        )
    return "".join(out)


def g_bin_rasters(cx, cy, color, tint, *, scale=1.0):
    """Two overlapping raster grids: DSM (bright) and DTM (muted)."""
    out = []
    cells, s = 5, int(18 * scale)
    span = cells * s
    # DTM (offset, faded)
    dx0, dy0 = cx - span // 2 + int(14 * scale), int(cy - span // 2 - 12 * scale)
    out.append(
        f'<rect x="{dx0}" y="{dy0}" width="{span}" height="{span}" '
        f'fill="{tint}" fill-opacity="0.5" stroke="{color}" stroke-width="{1.4*scale:.1f}"/>'
    )
    for r in range(cells):
        for c in range(cells):
            v = 0.08 + 0.28 * ((math.sin(r * 0.9 + c * 1.1) + 1) / 2)
            out.append(
                f'<rect x="{dx0 + c*s}" y="{dy0 + r*s}" width="{s}" height="{s}" '
                f'fill="{color}" fill-opacity="{v:.2f}"/>'
            )
    out.append(
        f'<text x="{dx0 + span//2}" y="{dy0 - 4}" text-anchor="middle" '
        f'font-family="ui-monospace,Menlo,monospace" font-size="{int(8*scale)}" '
        f'font-weight="700" fill="{color}" fill-opacity="0.6">DTM</text>'
    )
    # DSM (front, brighter)
    sx0, sy0 = cx - span // 2, int(cy - span // 2 + 10 * scale)
    out.append(
        f'<rect x="{sx0}" y="{sy0}" width="{span}" height="{span}" '
        f'fill="white" fill-opacity="0.9" stroke="{color}" stroke-width="{1.8*scale:.1f}"/>'
    )
    for r in range(cells):
        for c in range(cells):
            v = 0.15 + 0.55 * ((math.sin(r * 1.1 + c * 0.8) + 1) / 2)
            out.append(
                f'<rect x="{sx0 + c*s}" y="{sy0 + r*s}" width="{s}" height="{s}" '
                f'fill="{color}" fill-opacity="{v:.2f}"/>'
            )
    out.append(
        f'<text x="{sx0 + span//2}" y="{sy0 - 4}" text-anchor="middle" '
        f'font-family="ui-monospace,Menlo,monospace" font-size="{int(8*scale)}" '
        f'font-weight="700" fill="{color}">DSM</text>'
    )
    return "".join(out)


def g_chm_output(cx, cy, color, tint, *, scale=1.0):
    """CHM canopy profile + stacked output file stack."""
    out = []
    # Canopy height profile (peaks = trees/structures)
    pw = int(110 * scale)
    px0 = cx - pw // 2
    ph = int(50 * scale)
    py0 = int(cy - 55 * scale)
    peaks = [0.0, 0.3, 0.7, 1.0, 0.85, 0.55, 0.2, 0.05]
    w_each = pw / (len(peaks) - 1)
    pts_path = []
    for i, v in enumerate(peaks):
        pts_path.append(f"{px0 + int(i * w_each)},{int(py0 + ph * (1 - v))}")
    pts_path.append(f"{px0 + pw},{py0 + ph}")
    pts_path.append(f"{px0},{py0 + ph}")
    out.append(
        f'<polygon points="{" ".join(pts_path)}" fill="{color}" fill-opacity="0.35" '
        f'stroke="{color}" stroke-width="{1.6*scale:.1f}" stroke-linejoin="round"/>'
    )
    # CHM label
    out.append(
        f'<text x="{cx}" y="{py0 - 4}" text-anchor="middle" '
        f'font-family="ui-monospace,Menlo,monospace" font-size="{int(9*scale)}" '
        f'font-weight="800" fill="{color}">CHM</text>'
    )
    # Stacked output files
    labels = ["DSM", "DTM", "CHM"]
    fw, fh = int(80 * scale), int(36 * scale)
    offsets = [(int(12*scale), int(12*scale)), (int(6*scale), int(6*scale)), (0, 0)]
    ops = [0.4, 0.65, 1.0]
    for i, (dx, dy) in enumerate(offsets):
        fx = cx - fw // 2 + dx
        fy = int(cy + 14 * scale) + dy
        out.append(
            f'<rect x="{fx}" y="{fy}" rx="{int(5*scale)}" width="{fw}" height="{fh}" '
            f'fill="{"white" if i == 2 else tint}" fill-opacity="{ops[i]}" '
            f'stroke="{color}" stroke-width="{1.4*scale:.1f}"/>'
            f'<text x="{fx + fw//2}" y="{fy + fh//2 + 4}" text-anchor="middle" '
            f'font-family="ui-monospace,Menlo,monospace" font-size="{int(9*scale)}" '
            f'font-weight="800" fill="{color}" fill-opacity="{ops[i]}">{labels[i]}.tif</text>'
        )
    return "".join(out)


# ── NB2A glyphs ────────────────────────────────────────────────────────────

def g_two_delta_tables(cx, cy, color, tint, *, scale=1.0):
    """Two stacked Delta-table icons: wc_lidar_pts and wc_lidar_dsm."""
    labels = ["wc_lidar_dsm", "wc_lidar_pts"]
    w, h = int(112 * scale), int(52 * scale)
    out = []
    offsets = [(int(10*scale), int(10*scale)), (0, 0)]
    ops = [0.5, 1.0]
    for i, (dx, dy) in enumerate(offsets):
        x = cx - w // 2 + dx
        y = int(cy - h // 2 + dy - 12 * scale)
        out.append(
            f'<rect x="{x}" y="{y}" rx="{int(7*scale)}" width="{w}" height="{h}" '
            f'fill="{"white" if i==1 else tint}" fill-opacity="{ops[i]}" '
            f'stroke="{color}" stroke-width="{1.6*scale:.1f}"/>'
            f'<rect x="{x}" y="{y}" width="{w}" height="{int(16*scale)}" '
            f'rx="{int(7*scale)}" fill="{color}" fill-opacity="{ops[i]}"/>'
            f'<rect x="{x}" y="{y + int(9*scale)}" width="{w}" height="{int(7*scale)}" '
            f'fill="{color}" fill-opacity="{ops[i]}"/>'
            f'<text x="{x + w//2}" y="{y + int(11*scale)}" text-anchor="middle" '
            f'font-family="ui-monospace,Menlo,monospace" font-size="{int(8*scale)}" '
            f'font-weight="800" fill="white">{esc(labels[i])}</text>'
        )
        for r in range(2):
            ry = y + int((26 + r * 11) * scale)
            out.append(
                f'<line x1="{x + int(8*scale)}" y1="{ry}" '
                f'x2="{x + w - int(8*scale)}" y2="{ry}" '
                f'stroke="{color}" stroke-opacity="0.3" stroke-width="{1.0*scale:.1f}"/>'
            )
    return "".join(out)


def g_tin_mesh(cx, cy, color, tint, *, scale=1.0):
    """Delaunay-TIN triangulation mesh (ground returns → bare-earth terrain)."""
    out = []
    # Control points for a believable terrain-like mesh
    pts = [
        (-50, 20), (-28, -30), (0, 10), (28, -25), (52, 15),
        (-40, 50), (-12, 40), (16, 45), (44, 48),
    ]
    # Triangles connecting the points
    tris = [
        (0, 1, 2), (1, 3, 2), (3, 4, 2),
        (0, 2, 5), (2, 6, 5), (2, 7, 6), (2, 3, 7), (3, 4, 7), (4, 8, 7), (6, 7, 8),
    ]
    for ti, (a, b, c) in enumerate(tris):
        ax, ay = cx + pts[a][0] * scale, cy + pts[a][1] * scale
        bx, by = cx + pts[b][0] * scale, cy + pts[b][1] * scale
        cxp, cyp = cx + pts[c][0] * scale, cy + pts[c][1] * scale
        # Elevation-based fill
        mid_y = (ay + by + cyp) / 3
        v = 0.12 + 0.40 * (1 - (mid_y - cy + 60 * scale) / (120 * scale))
        out.append(
            f'<polygon points="{ax:.1f},{ay:.1f} {bx:.1f},{by:.1f} {cxp:.1f},{cyp:.1f}" '
            f'fill="{color}" fill-opacity="{max(0.08, min(0.48, v)):.2f}" '
            f'stroke="{color}" stroke-width="{1.0*scale:.1f}" stroke-opacity="0.7"/>'
        )
    # Ground return dots
    for px, py in pts:
        out.append(
            f'<circle cx="{cx + px*scale:.1f}" cy="{cy + py*scale:.1f}" '
            f'r="{2.8*scale:.1f}" fill="{color}" fill-opacity="0.9"/>'
        )
    return "".join(out)


def g_raster_diff(cx, cy, color, tint, *, scale=1.0):
    """Two overlapping rasters with a minus sign — CHM = DSM − DTM."""
    out = []
    cells, s = 4, int(16 * scale)
    span = cells * s
    # DTM (offset back)
    dx0 = cx - span // 2 + int(18 * scale)
    dy0 = int(cy - span // 2 - 14 * scale)
    out.append(
        f'<rect x="{dx0}" y="{dy0}" width="{span}" height="{span}" '
        f'fill="{tint}" fill-opacity="0.5" stroke="{color}" stroke-width="{1.2*scale:.1f}"/>'
    )
    for r in range(cells):
        for c in range(cells):
            v = 0.1 + 0.3 * ((math.sin(r * 0.9 + c * 1.1) + 1) / 2)
            out.append(
                f'<rect x="{dx0 + c*s}" y="{dy0 + r*s}" width="{s}" height="{s}" '
                f'fill="{color}" fill-opacity="{v:.2f}"/>'
            )
    out.append(
        f'<text x="{dx0 + span//2}" y="{dy0 - 4}" text-anchor="middle" '
        f'font-family="ui-monospace,Menlo,monospace" font-size="{int(8*scale)}" '
        f'font-weight="700" fill="{color}" fill-opacity="0.6">DTM</text>'
    )
    # DSM (front)
    sx0 = cx - span // 2
    sy0 = int(cy - span // 2 + 8 * scale)
    out.append(
        f'<rect x="{sx0}" y="{sy0}" width="{span}" height="{span}" '
        f'fill="white" fill-opacity="0.9" stroke="{color}" stroke-width="{1.8*scale:.1f}"/>'
    )
    for r in range(cells):
        for c in range(cells):
            v = 0.15 + 0.55 * ((math.sin(r * 1.1 + c * 0.8) + 1) / 2)
            out.append(
                f'<rect x="{sx0 + c*s}" y="{sy0 + r*s}" width="{s}" height="{s}" '
                f'fill="{color}" fill-opacity="{v:.2f}"/>'
            )
    out.append(
        f'<text x="{sx0 + span//2}" y="{sy0 - 4}" text-anchor="middle" '
        f'font-family="ui-monospace,Menlo,monospace" font-size="{int(8*scale)}" '
        f'font-weight="700" fill="{color}">DSM</text>'
    )
    # Minus sign between
    out.append(
        f'<text x="{cx}" y="{int(cy + 52*scale)}" text-anchor="middle" '
        f'font-family="Inter,sans-serif" font-size="{int(22*scale)}" '
        f'font-weight="900" fill="{color}" fill-opacity="0.7">−</text>'
    )
    return "".join(out)


def g_surface_outputs(cx, cy, color, tint, *, scale=1.0):
    """Three GeoTIFF output icons for DSM / DTM / CHM — canonical surface products."""
    labels = ["DSM", "DTM", "CHM"]
    w, h = int(80 * scale), int(52 * scale)
    out = []
    offsets = [(int(16*scale), int(16*scale)), (int(8*scale), int(8*scale)), (0, 0)]
    ops = [0.40, 0.65, 1.0]
    for i, (dx, dy) in enumerate(offsets):
        x = cx - w // 2 + dx
        y = int(cy - h // 2 + dy - 16 * scale)
        out.append(
            f'<rect x="{x}" y="{y}" rx="{int(6*scale)}" width="{w}" height="{h}" '
            f'fill="{"white" if i==2 else tint}" fill-opacity="{ops[i]}" '
            f'stroke="{color}" stroke-width="{1.6*scale:.1f}"/>'
            f'<rect x="{x}" y="{y}" width="{w}" height="{int(16*scale)}" '
            f'rx="{int(6*scale)}" fill="{color}" fill-opacity="{ops[i]}"/>'
            f'<rect x="{x}" y="{y + int(9*scale)}" width="{w}" height="{int(7*scale)}" '
            f'fill="{color}" fill-opacity="{ops[i]}"/>'
            f'<text x="{x + w//2}" y="{y + int(11*scale)}" text-anchor="middle" '
            f'font-family="ui-monospace,Menlo,monospace" font-size="{int(8*scale)}" '
            f'font-weight="800" fill="white">{labels[i]}.tif</text>'
        )
        for r in range(2):
            ry = y + int((24 + r * 11) * scale)
            out.append(
                f'<line x1="{x + int(8*scale)}" y1="{ry}" '
                f'x2="{x + w - int(8*scale)}" y2="{ry}" '
                f'stroke="{color}" stroke-opacity="0.3" stroke-width="{1.0*scale:.1f}"/>'
            )
    return "".join(out)


# ── NB2B glyphs ────────────────────────────────────────────────────────────

def g_dsm_raster_input(cx, cy, color, tint, *, scale=1.0):
    """A single DSM GeoTIFF raster tile with CRS badge."""
    out = []
    cells, s = 5, int(18 * scale)
    span = cells * s
    x0, y0 = cx - span // 2, int(cy - span // 2 - 4 * scale)
    out.append(
        f'<rect x="{x0}" y="{y0}" width="{span}" height="{span}" '
        f'fill="{tint}" stroke="{color}" stroke-width="{2.0*scale:.1f}" rx="{int(4*scale)}"/>'
    )
    for r in range(cells):
        for c in range(cells):
            v = 0.12 + 0.55 * ((math.sin(r * 1.1 + c * 0.8) + 1) / 2)
            out.append(
                f'<rect x="{x0 + c*s}" y="{y0 + r*s}" width="{s}" height="{s}" '
                f'fill="{color}" fill-opacity="{v:.2f}"/>'
            )
    # DSM badge top-left
    out.append(
        f'<rect x="{x0}" y="{y0}" rx="{int(4*scale)}" '
        f'width="{int(36*scale)}" height="{int(16*scale)}" fill="{color}"/>'
        f'<text x="{x0 + int(18*scale)}" y="{y0 + int(11*scale)}" text-anchor="middle" '
        f'font-family="ui-monospace,Menlo,monospace" font-size="{int(9*scale)}" '
        f'font-weight="800" fill="white">DSM</text>'
    )
    return "".join(out)


def g_morph_opening(cx, cy, color, tint, *, scale=1.0):
    """Morphological opening: erosion (min-filter) then dilation (max-filter)."""
    out = []
    cells, s = 5, int(16 * scale)
    span = cells * s
    # Input: raster with tall structure
    x0, y0 = cx - span // 2, int(cy - span // 2 - 20 * scale)
    for r in range(cells):
        for c in range(cells):
            # Simulate a tall building in the center
            if r == 2 and c == 2:
                v = 0.85
            else:
                v = 0.15 + 0.30 * ((math.sin(r * 0.9 + c * 1.0) + 1) / 2)
            out.append(
                f'<rect x="{x0 + c*s}" y="{y0 + r*s}" width="{s}" height="{s}" '
                f'fill="{color}" fill-opacity="{v:.2f}" stroke="{tint}" stroke-width="0.4"/>'
            )
    out.append(
        f'<rect x="{x0}" y="{y0}" width="{span}" height="{span}" '
        f'fill="none" stroke="{color}" stroke-width="{1.6*scale:.1f}" rx="{int(4*scale)}"/>'
        f'<text x="{cx}" y="{y0 - 4}" text-anchor="middle" '
        f'font-family="ui-monospace,Menlo,monospace" font-size="{int(8*scale)}" '
        f'font-weight="700" fill="{color}">DSM in</text>'
    )
    # Arrow down
    ay_top = y0 + span + int(4 * scale)
    ay_bot = int(cy + 14 * scale)
    out.append(
        f'<line x1="{cx}" y1="{ay_top}" x2="{cx}" y2="{ay_bot - int(8*scale)}" '
        f'stroke="{color}" stroke-width="{1.4*scale:.1f}" stroke-linecap="round"/>'
        f'<polygon points="{cx},{ay_bot} {cx - int(5*scale)},{ay_bot - int(8*scale)} '
        f'{cx + int(5*scale)},{ay_bot - int(8*scale)}" fill="{color}"/>'
    )
    # Output: smoothed bare-earth (building removed)
    bx0 = cx - span // 2
    by0 = int(cy + 2 * scale)
    for r in range(cells):
        for c in range(cells):
            v = 0.15 + 0.30 * ((math.sin(r * 0.9 + c * 1.0) + 1) / 2)
            out.append(
                f'<rect x="{bx0 + c*s}" y="{by0 + r*s}" width="{s}" height="{s}" '
                f'fill="{color}" fill-opacity="{v:.2f}" stroke="{tint}" stroke-width="0.4"/>'
            )
    out.append(
        f'<rect x="{bx0}" y="{by0}" width="{span}" height="{span}" '
        f'fill="none" stroke="{color}" stroke-width="{1.6*scale:.1f}" rx="{int(4*scale)}"/>'
        f'<text x="{cx}" y="{by0 + span + int(6*scale)}" text-anchor="middle" '
        f'font-family="ui-monospace,Menlo,monospace" font-size="{int(8*scale)}" '
        f'font-weight="700" fill="{color}">DTM out</text>'
    )
    return "".join(out)


# ── NB3B glyphs ────────────────────────────────────────────────────────────

def g_pixel_vs_hex_contrast(cx, cy, color, tint, *, scale=1.0):
    """Side-by-side: pixel grid (left) vs H3 hexagons (right)."""
    out = []
    half_w = int(52 * scale)
    # Left: pixel grid
    px0 = cx - half_w - int(6 * scale)
    cells_per = 4
    ps = int(24 * scale)
    py0 = int(cy - cells_per * ps // 2)
    for r in range(cells_per):
        for c in range(cells_per):
            v = 0.12 + 0.55 * ((math.sin(r * 1.2 + c * 0.9) + 1) / 2)
            out.append(
                f'<rect x="{px0 + c*ps}" y="{py0 + r*ps}" width="{ps}" height="{ps}" '
                f'fill="{color}" fill-opacity="{v:.2f}" stroke="white" stroke-width="1"/>'
            )
    # "1 m pixels" centred under the pixel block.  Use an explicit cx-relative
    # offset (not derived from px0+half_width) so the two sub-labels never
    # collide regardless of scale: pixel-block centre ≈ cx-32, hex-block ≈ cx+42.
    lbl_y = int(cy + cells_per * ps // 2 + 14 * scale)
    out.append(
        f'<text x="{cx - int(32 * scale)}" y="{lbl_y}" '
        f'text-anchor="middle" font-family="ui-monospace,Menlo,monospace" '
        f'font-size="{int(9*scale)}" font-weight="700" fill="{color}">1 m pixels</text>'
    )
    # Divider
    mid_x = cx - int(4 * scale)
    out.append(
        f'<line x1="{mid_x}" y1="{int(cy - 55*scale)}" x2="{mid_x}" y2="{int(cy + 55*scale)}" '
        f'stroke="{color}" stroke-opacity="0.25" stroke-width="{1.2*scale:.1f}" '
        f'stroke-dasharray="4 3"/>'
    )
    # Right: hex grid
    R = int(18 * scale)
    dx_h = R * math.sqrt(3)
    dy_h = R * 1.5
    hx0 = cx + int(6 * scale)
    hy0 = int(cy - 36 * scale)
    for row in range(3):
        for col in range(2):
            hx = hx0 + col * dx_h + (dx_h / 2 if row % 2 else 0)
            hy = hy0 + row * dy_h
            intensity = 0.15 + 0.60 * abs(math.sin(row * 1.3 + col * 0.9))
            out.append(
                f'<polygon points="{_hex_pts(hx, hy, R)}" '
                f'fill="{color}" fill-opacity="{intensity:.2f}" '
                f'stroke="white" stroke-width="{1.2*scale:.1f}"/>'
            )
    # "H3 cells" centred under the hex block — cx+42 keeps at least 14 px
    # clearance from the right edge of the "1 m pixels" label at all scales.
    out.append(
        f'<text x="{cx + int(42 * scale)}" y="{lbl_y}" '
        f'text-anchor="middle" font-family="ui-monospace,Menlo,monospace" '
        f'font-size="{int(9*scale)}" font-weight="700" fill="{color}">H3 cells</text>'
    )
    return "".join(out)


def g_spread_candidates(cx, cy, color, tint, *, scale=1.0):
    """Spread pool of candidate towers across the LiDAR footprint."""
    out = []
    R = int(16 * scale)
    dx_h = R * math.sqrt(3)
    dy_h = R * 1.5
    rows, cols = 4, 4
    x0 = cx - (cols - 1) * dx_h / 2 - dx_h / 4 + int(4 * scale)
    y0 = int(cy - (rows - 1) * dy_h / 2 - 8 * scale)
    for row in range(rows):
        for col in range(cols):
            hx = x0 + col * dx_h + (dx_h / 2 if row % 2 else 0)
            hy = y0 + row * dy_h
            intensity = 0.10 + 0.50 * abs(math.sin(row * 1.3 + col * 0.9))
            out.append(
                f'<polygon points="{_hex_pts(hx, hy, R)}" '
                f'fill="{tint}" fill-opacity="0.65" '
                f'stroke="{color}" stroke-width="{1.2*scale:.1f}" stroke-opacity="0.55"/>'
            )
            # Candidate centroid dot
            out.append(
                f'<circle cx="{hx:.1f}" cy="{hy:.1f}" r="{2.8*scale:.1f}" '
                f'fill="{color}" fill-opacity="0.9"/>'
            )
    return "".join(out)


def g_raster_viewshed_fan(cx, cy, color, tint, *, scale=1.0):
    """Pixel-level viewshed fan from a tower — rst_viewshed_towers."""
    out = []
    # Viewshed raster grid (visible vs not)
    cells, s = 7, int(13 * scale)
    span = cells * s
    gx0, gy0 = cx - span // 2, int(cy - span // 2 + 10 * scale)
    # Tower location (top-center of grid)
    tx_, ty_ = cx, gy0 - int(20 * scale)
    # Determine visibility based on direction from tower
    for r in range(cells):
        for c in range(cells):
            cell_cx = gx0 + c * s + s // 2
            cell_cy = gy0 + r * s + s // 2
            d = math.sqrt((cell_cx - tx_) ** 2 + (cell_cy - ty_) ** 2)
            ang = math.atan2(cell_cy - ty_, cell_cx - tx_)
            visible = (d < span * 0.55
                       and not (abs(math.degrees(ang) - 110) < 20
                                and d > span * 0.25))
            v = 0.55 if visible else 0.10
            fill_c = color if visible else tint
            out.append(
                f'<rect x="{gx0 + c*s}" y="{gy0 + r*s}" width="{s}" height="{s}" '
                f'fill="{fill_c}" fill-opacity="{v:.2f}" '
                f'stroke="white" stroke-width="0.6"/>'
            )
    out.append(
        f'<rect x="{gx0}" y="{gy0}" width="{span}" height="{span}" '
        f'fill="none" stroke="{color}" stroke-width="{1.8*scale:.1f}" rx="{int(4*scale)}"/>'
    )
    # Tower icon above grid
    out.append(
        f'<rect x="{tx_ - int(4*scale)}" y="{ty_ - int(16*scale)}" '
        f'width="{int(8*scale)}" height="{int(20*scale)}" '
        f'fill="{color}" fill-opacity="0.9" rx="{int(2*scale)}"/>'
        f'<circle cx="{tx_}" cy="{ty_ - int(20*scale)}" r="{int(3*scale)}" fill="{color}"/>'
    )
    return "".join(out)


def g_h3_los_compare(cx, cy, color, tint, *, scale=1.0):
    """H3-cell LOS viewshed + coverage metric comparison table."""
    out = []
    # Small H3 coverage hex grid
    R = int(14 * scale)
    dx_h = R * math.sqrt(3)
    dy_h = R * 1.5
    rows, cols = 3, 4
    hx0 = cx - (cols - 1) * dx_h / 2 - dx_h / 4
    hy0 = int(cy - 56 * scale)
    for row in range(rows):
        for col in range(cols):
            hx = hx0 + col * dx_h + (dx_h / 2 if row % 2 else 0)
            hy = hy0 + row * dy_h
            # LOS depth varies
            depth = int(1 + 3 * abs(math.sin(row * 1.1 + col * 0.8)))
            v = 0.10 + 0.18 * depth
            out.append(
                f'<polygon points="{_hex_pts(hx, hy, R)}" '
                f'fill="{color}" fill-opacity="{v:.2f}" '
                f'stroke="{color}" stroke-width="{1.0*scale:.1f}" stroke-opacity="0.6"/>'
            )
    # Comparison "table" rows
    ty0 = int(cy + 4 * scale)
    tw = int(130 * scale)
    th = int(24 * scale)
    headers = ["raster", "H3", "ratio"]
    col_w = tw // 3
    for ci, h in enumerate(headers):
        out.append(
            f'<rect x="{cx - tw//2 + ci*col_w}" y="{ty0}" width="{col_w}" height="{th}" '
            f'fill="{color}" fill-opacity="0.85"/>'
            f'<text x="{cx - tw//2 + ci*col_w + col_w//2}" y="{ty0 + int(16*scale)}" '
            f'text-anchor="middle" font-family="ui-monospace,Menlo,monospace" '
            f'font-size="{int(8*scale)}" font-weight="800" fill="white">{h}</text>'
        )
    # Two data rows — values kept to ≤8 chars so they fit the 43 px column width
    for ri, (v1, v2, v3) in enumerate([("px-exact", "optimist.", "~1.3×"),
                                        ("1/tower", "LOS fast", "10×")]):
        ry = ty0 + th + ri * th
        for ci, val in enumerate([v1, v2, v3]):
            bg_op = "0.06" if ri % 2 == 0 else "0.12"
            out.append(
                f'<rect x="{cx - tw//2 + ci*col_w}" y="{ry}" width="{col_w}" height="{th}" '
                f'fill="{color}" fill-opacity="{bg_op}"/>'
                f'<text x="{cx - tw//2 + ci*col_w + col_w//2}" y="{ry + int(16*scale)}" '
                f'text-anchor="middle" font-family="ui-monospace,Menlo,monospace" '
                f'font-size="{int(7*scale)}" fill="{color}">{val}</text>'
            )
    out.append(
        f'<rect x="{cx - tw//2}" y="{ty0}" width="{tw}" height="{th + 2*th}" '
        f'fill="none" stroke="{color}" stroke-width="{1.4*scale:.1f}" rx="{int(4*scale)}"/>'
    )
    return "".join(out)


# ── Reuse existing NB3A / NB4 glyphs (kept from previous version) ──────────

def g_three_surface_tables(cx, cy, color, tint, *, scale=1.0):
    """Three stacked Delta-table icons labelled DTM / DSM / CHM."""
    labels = ["DTM", "DSM", "CHM"]
    w, h = int(100 * scale), int(60 * scale)
    out = []
    offsets = [(int(16*scale), int(16*scale)), (int(8*scale), int(8*scale)), (0, 0)]
    opacities = [0.45, 0.65, 1.0]
    for i, (dx, dy) in enumerate(offsets):
        x = cx - w // 2 + dx
        y = int(cy - h // 2 + dy - 16 * scale)
        op = opacities[i]
        fill = "#FFFFFF" if i == 2 else tint
        lbl = labels[i]
        out.append(
            f'<rect x="{x}" y="{y}" rx="{int(7*scale)}" width="{w}" height="{h}" '
            f'fill="{fill}" fill-opacity="{op}" stroke="{color}" stroke-width="{1.8*scale:.1f}"/>'
            f'<rect x="{x}" y="{y}" width="{w}" height="{int(18*scale)}" '
            f'rx="{int(7*scale)}" fill="{color}" fill-opacity="{op}"/>'
            f'<rect x="{x}" y="{y + int(10*scale)}" width="{w}" height="{int(8*scale)}" '
            f'fill="{color}" fill-opacity="{op}"/>'
            f'<text x="{x + w//2}" y="{y + int(13*scale)}" text-anchor="middle" '
            f'font-family="ui-monospace,Menlo,monospace" font-size="{int(9*scale)}" '
            f'font-weight="800" fill="#FFFFFF" fill-opacity="1">{lbl}</text>'
        )
        for r in range(2):
            ry = y + int((28 + r * 12) * scale)
            out.append(
                f'<line x1="{x + int(8*scale)}" y1="{ry}" x2="{x + w - int(8*scale)}" y2="{ry}" '
                f'stroke="{color}" stroke-opacity="0.35" stroke-width="{1.2*scale:.1f}"/>'
            )
    return "".join(out)


def g_reproject_clip(cx, cy, color, tint, *, scale=1.0):
    """A raster tile with a 4326 CRS badge and clip envelope."""
    cells, s = 5, int(20 * scale)
    span = cells * s
    x0 = cx - span // 2
    y0 = int(cy - span // 2 - 4 * scale)
    out = [
        f'<rect x="{x0}" y="{y0}" width="{span}" height="{span}" '
        f'fill="{tint}" stroke="{color}" stroke-width="2" rx="{int(4*scale)}"/>'
    ]
    for i in range(1, cells):
        out.append(
            f'<line x1="{x0 + i*s}" y1="{y0}" x2="{x0 + i*s}" y2="{y0 + span}" '
            f'stroke="{color}" stroke-opacity="0.25" stroke-width="{0.8*scale:.1f}"/>'
            f'<line x1="{x0}" y1="{y0 + i*s}" x2="{x0 + span}" y2="{y0 + i*s}" '
            f'stroke="{color}" stroke-opacity="0.25" stroke-width="{0.8*scale:.1f}"/>'
        )
    for r in range(cells):
        for c in range(cells):
            v = 0.12 + 0.55 * ((math.sin(r * 1.1 + c * 0.8) + 1) / 2)
            out.append(
                f'<rect x="{x0 + c*s}" y="{y0 + r*s}" width="{s}" height="{s}" '
                f'fill="{color}" fill-opacity="{v:.2f}"/>'
            )
    bw, bh = int(56*scale), int(18*scale)
    bx = x0 + span - bw + int(2*scale)
    by = y0 + span - bh + int(2*scale)
    out.append(
        f'<rect x="{bx}" y="{by}" rx="{int(5*scale)}" width="{bw}" height="{bh}" '
        f'fill="{color}"/>'
        f'<text x="{bx + bw//2}" y="{by + int(13*scale)}" text-anchor="middle" '
        f'font-family="ui-monospace,Menlo,monospace" font-size="{int(9*scale)}" '
        f'font-weight="800" fill="#FFFFFF">EPSG:4326</text>'
    )
    cw, ch = int(68*scale), int(60*scale)
    out.append(
        f'<rect x="{cx - cw//2}" y="{cy - ch//2 - int(6*scale)}" width="{cw}" height="{ch}" '
        f'rx="{int(12*scale)}" fill="none" stroke="{color}" stroke-width="{2.4*scale:.1f}" '
        f'stroke-dasharray="{int(6*scale)} {int(4*scale)}"/>'
    )
    return "".join(out)


def g_three_path_h3(cx, cy, color, tint, *, scale=1.0):
    """Hex grid (CHM rastertogridmax) + isoband contour lines."""
    out = []
    R = int(18 * scale)
    dx_h = R * math.sqrt(3)
    dy_h = R * 1.5
    rows, cols = 4, 4
    x0 = cx - (cols - 1) * dx_h / 2 - dx_h / 4 + int(4 * scale)
    y0 = int(cy - (rows - 1) * dy_h / 2 - 8 * scale)
    for r in range(rows):
        for c in range(cols):
            hx = x0 + c * dx_h + (dx_h / 2 if r % 2 else 0)
            hy = y0 + r * dy_h
            intensity = 0.12 + 0.72 * abs(math.sin(r * 1.3 + c * 0.9))
            out.append(
                f'<polygon points="{_hex_pts(hx, hy, R)}" '
                f'fill="{color}" fill-opacity="{intensity:.2f}" '
                f'stroke="{color}" stroke-width="{1.2*scale:.1f}" stroke-opacity="0.6"/>'
            )
    for offset, op in [(-22, 0.8), (0, 0.9)]:
        pts_line = []
        for i in range(7):
            fx = i / 6
            sx = cx - int(52 * scale) + fx * int(104 * scale)
            sy = (cy + offset * scale - int(4 * scale)
                  + int(14 * scale) * math.sin(fx * math.pi * 1.4)
                  - int(10 * scale) * math.cos(fx * math.pi * 0.8))
            pts_line.append(f"{sx:.1f},{sy:.1f}")
        out.append(
            f'<polyline points="{" ".join(pts_line)}" fill="none" '
            f'stroke="#FFFFFF" stroke-width="{2.2*scale:.1f}" stroke-linecap="round" '
            f'stroke-linejoin="round" stroke-opacity="{op}"/>'
        )
    return "".join(out)


def g_three_h3_output_tables(cx, cy, color, tint, *, scale=1.0):
    """Three stacked Delta-table icons for H3 output tables (DEM/DSM/CHM) with hex badges."""
    labels = ["DEM", "DSM", "CHM"]
    w, h = int(100 * scale), int(60 * scale)
    out = []
    offsets = [(int(16*scale), int(16*scale)), (int(8*scale), int(8*scale)), (0, 0)]
    opacities = [0.45, 0.65, 1.0]
    for i, (dx, dy) in enumerate(offsets):
        x = cx - w // 2 + dx
        y = int(cy - h // 2 + dy - 16 * scale)
        op = opacities[i]
        fill = "#FFFFFF" if i == 2 else tint
        lbl = labels[i]
        out.append(
            f'<rect x="{x}" y="{y}" rx="{int(7*scale)}" width="{w}" height="{h}" '
            f'fill="{fill}" fill-opacity="{op}" stroke="{color}" stroke-width="{1.8*scale:.1f}"/>'
            f'<rect x="{x}" y="{y}" width="{w}" height="{int(18*scale)}" '
            f'rx="{int(7*scale)}" fill="{color}" fill-opacity="{op}"/>'
            f'<rect x="{x}" y="{y + int(10*scale)}" width="{w}" height="{int(8*scale)}" '
            f'fill="{color}" fill-opacity="{op}"/>'
            f'<text x="{x + w//2}" y="{y + int(13*scale)}" text-anchor="middle" '
            f'font-family="ui-monospace,Menlo,monospace" font-size="{int(9*scale)}" '
            f'font-weight="800" fill="#FFFFFF" fill-opacity="1">wc_h3_{lbl.lower()}</text>'
        )
        for r in range(2):
            ry = y + int((28 + r * 12) * scale)
            out.append(
                f'<line x1="{x + int(8*scale)}" y1="{ry}" x2="{x + w - int(8*scale)}" y2="{ry}" '
                f'stroke="{color}" stroke-opacity="0.35" stroke-width="{1.2*scale:.1f}"/>'
            )
        hx_b = x + w - int(2 * scale)
        hy_b = y - int(2 * scale)
        hr_b = int(7 * scale)
        out.append(
            f'<polygon points="{_hex_pts(hx_b, hy_b, hr_b)}" fill="{color}" '
            f'fill-opacity="{op}" stroke="#FFFFFF" stroke-width="0.8"/>'
        )
    return "".join(out)


# NB4 glyphs (unchanged from previous version) ─────────────────────────────

def g_h3_input_tables(cx, cy, color, tint, *, scale=1.0):
    """Three stacked Delta-table icons for H3 input tables (DEM/DSM/CHM) with hex badges."""
    labels = ["DEM", "DSM", "CHM"]
    w, h = int(100 * scale), int(60 * scale)
    out = []
    offsets = [(int(16*scale), int(16*scale)), (int(8*scale), int(8*scale)), (0, 0)]
    opacities = [0.45, 0.65, 1.0]
    for i, (dx, dy) in enumerate(offsets):
        x = cx - w // 2 + dx
        y = int(cy - h // 2 + dy - 16 * scale)
        op = opacities[i]
        fill = "#FFFFFF" if i == 2 else tint
        lbl = labels[i]
        out.append(
            f'<rect x="{x}" y="{y}" rx="{int(7*scale)}" width="{w}" height="{h}" '
            f'fill="{fill}" fill-opacity="{op}" stroke="{color}" stroke-width="{1.8*scale:.1f}"/>'
            f'<rect x="{x}" y="{y}" width="{w}" height="{int(18*scale)}" '
            f'rx="{int(7*scale)}" fill="{color}" fill-opacity="{op}"/>'
            f'<rect x="{x}" y="{y + int(10*scale)}" width="{w}" height="{int(8*scale)}" '
            f'fill="{color}" fill-opacity="{op}"/>'
            f'<text x="{x + w//2}" y="{y + int(13*scale)}" text-anchor="middle" '
            f'font-family="ui-monospace,Menlo,monospace" font-size="{int(9*scale)}" '
            f'font-weight="800" fill="#FFFFFF" fill-opacity="1">wc_h3_{lbl.lower()}</text>'
        )
        for r in range(2):
            ry = y + int((28 + r * 12) * scale)
            out.append(
                f'<line x1="{x + int(8*scale)}" y1="{ry}" x2="{x + w - int(8*scale)}" y2="{ry}" '
                f'stroke="{color}" stroke-opacity="0.35" stroke-width="{1.2*scale:.1f}"/>'
            )
        hx_b = x + w - int(2 * scale)
        hy_b = y - int(2 * scale)
        hr_b = int(7 * scale)
        out.append(
            f'<polygon points="{_hex_pts(hx_b, hy_b, hr_b)}" fill="{color}" '
            f'fill-opacity="{op}" stroke="#FFFFFF" stroke-width="0.8"/>'
        )
    return "".join(out)


def g_candidate_lattice_and_quickpass(cx, cy, color, tint, *, scale=1.0):
    """Hex candidate grid (top half) + funnel filter (bottom half)."""
    R = int(14 * scale)
    dx_h = R * math.sqrt(3)
    dy_h = R * 1.5
    rows_g, cols_g = 2, 4
    x0 = cx - (cols_g - 1) * dx_h / 2 - dx_h / 4 + int(2 * scale)
    y0 = int(cy - 70 * scale)
    out = []
    for r in range(rows_g):
        for c in range(cols_g):
            hx = x0 + c * dx_h + (dx_h / 2 if r % 2 else 0)
            hy = y0 + r * dy_h
            out.append(
                f'<polygon points="{_hex_pts(hx, hy, R)}" '
                f'fill="{tint}" fill-opacity="0.65" '
                f'stroke="{color}" stroke-width="{1.2*scale:.1f}" stroke-opacity="0.55"/>'
                f'<circle cx="{hx:.1f}" cy="{hy:.1f}" r="{2.5*scale:.1f}" '
                f'fill="{color}" fill-opacity="0.8"/>'
            )
    fy0 = int(cy - 14 * scale)
    fw2, fb2, fh2 = int(72 * scale), int(28 * scale), int(52 * scale)
    pfx = cx - fw2 // 2
    pts_f = (f"{pfx},{fy0} {pfx + fw2},{fy0} "
             f"{cx + fb2//2},{fy0 + fh2} {cx - fb2//2},{fy0 + fh2}")
    out.append(
        f'<polygon points="{pts_f}" fill="{tint}" fill-opacity="0.55" '
        f'stroke="{color}" stroke-width="{1.8*scale:.1f}" stroke-linejoin="round"/>'
        f'<text x="{cx}" y="{fy0 + fh2//2 + int(5*scale)}" text-anchor="middle" '
        f'font-family="ui-monospace,Menlo,monospace" font-size="{int(9*scale)}" '
        f'font-weight="700" fill="{color}">≥ 30%</text>'
    )
    for dx2 in [-int(9*scale), 0, int(9*scale)]:
        out.append(
            f'<circle cx="{cx + dx2}" cy="{fy0 + fh2 + int(12*scale)}" r="{3.5*scale:.1f}" '
            f'fill="{color}" fill-opacity="0.9"/>'
        )
    return "".join(out)


def g_los_viewshed(cx, cy, color, tint, *, scale=1.0):
    """Tower with LOS rays fanning out to H3 cells — h3_los_visible."""
    out = []
    tx_, ty_, tw_, th_ = int(cx - 5*scale), int(cy - 36*scale), int(10*scale), int(38*scale)
    out.append(
        f'<rect x="{tx_}" y="{ty_}" width="{tw_}" height="{th_}" '
        f'fill="{color}" fill-opacity="0.8" rx="{int(2*scale)}"/>'
        f'<line x1="{cx}" y1="{ty_}" x2="{cx}" y2="{ty_ - int(14*scale)}" '
        f'stroke="{color}" stroke-width="{2*scale:.1f}" stroke-linecap="round"/>'
        f'<circle cx="{cx}" cy="{ty_ - int(17*scale)}" r="{int(3*scale)}" fill="{color}"/>'
    )
    base_y = ty_ + th_ // 2
    ray_targets = [(-52, -8), (-42, 22), (-30, 42), (30, -28), (50, 0), (44, 30), (0, 48)]
    for i, (rdx, rdy) in enumerate(ray_targets):
        op = 0.55 + 0.1 * (i % 3)
        out.append(
            f'<line x1="{cx}" y1="{base_y}" '
            f'x2="{cx + rdx*scale:.1f}" y2="{base_y + rdy*scale:.1f}" '
            f'stroke="{color}" stroke-width="{1.4*scale:.1f}" stroke-opacity="{op:.2f}" '
            f'stroke-linecap="round"/>'
            f'<circle cx="{cx + rdx*scale:.1f}" cy="{base_y + rdy*scale:.1f}" r="{4*scale:.1f}" '
            f'fill="{tint}" stroke="{color}" stroke-width="{1.2*scale:.1f}" fill-opacity="0.9"/>'
        )
    return "".join(out)


def g_coverage_output_tables(cx, cy, color, tint, *, scale=1.0):
    """Two stacked output Delta tables (candidate_sites + coverage) with hex badges."""
    labels = ["candidate_sites", "coverage"]
    w, h = int(116 * scale), int(58 * scale)
    out = []
    offsets = [(int(10*scale), int(10*scale)), (0, 0)]
    opacities = [0.5, 1.0]
    for i, (dx, dy) in enumerate(offsets):
        x = cx - w // 2 + dx
        y = int(cy - h // 2 + dy - int(12 * scale))
        op = opacities[i]
        fill = "#FFFFFF" if i == 1 else tint
        lbl = labels[i]
        out.append(
            f'<rect x="{x}" y="{y}" rx="{int(7*scale)}" width="{w}" height="{h}" '
            f'fill="{fill}" fill-opacity="{op}" stroke="{color}" stroke-width="{1.8*scale:.1f}"/>'
            f'<rect x="{x}" y="{y}" width="{w}" height="{int(18*scale)}" '
            f'rx="{int(7*scale)}" fill="{color}" fill-opacity="{op}"/>'
            f'<rect x="{x}" y="{y + int(10*scale)}" width="{w}" height="{int(8*scale)}" '
            f'fill="{color}" fill-opacity="{op}"/>'
            f'<text x="{x + w//2}" y="{y + int(13*scale)}" text-anchor="middle" '
            f'font-family="ui-monospace,Menlo,monospace" font-size="{int(8*scale)}" '
            f'font-weight="800" fill="#FFFFFF" fill-opacity="1">wc_h3_{lbl}</text>'
        )
        for r in range(2):
            ry = y + int((28 + r * 12) * scale)
            out.append(
                f'<line x1="{x + int(8*scale)}" y1="{ry}" x2="{x + w - int(8*scale)}" y2="{ry}" '
                f'stroke="{color}" stroke-opacity="0.35" stroke-width="{1.2*scale:.1f}"/>'
            )
        out.append(
            f'<line x1="{x + w//2}" y1="{y + int(20*scale)}" '
            f'x2="{x + w//2}" y2="{y + h - int(4*scale)}" '
            f'stroke="{color}" stroke-opacity="0.25" stroke-width="{0.8*scale:.1f}"/>'
        )
        hx_b = x + w - int(2 * scale)
        hy_b = y - int(2 * scale)
        hr_b = int(7 * scale)
        out.append(
            f'<polygon points="{_hex_pts(hx_b, hy_b, hr_b)}" fill="{color}" '
            f'fill-opacity="{op}" stroke="#FFFFFF" stroke-width="0.8"/>'
        )
    return "".join(out)


# ---------------------------------------------------------------------------
# Header / footer
# ---------------------------------------------------------------------------

def render_header(badge, title, subtitle, accent, series_text):
    out = []
    bsize = 60
    bx, by = PAD, PAD + 4
    label_size = 32 if len(badge) <= 2 else 18
    out.append(
        f'<rect x="{bx}" y="{by}" rx="14" ry="14" width="{bsize}" height="{bsize}" '
        f'fill="{accent}"/>'
        f'<text x="{bx + bsize//2}" y="{by + bsize//2 + 10}" text-anchor="middle" '
        f'font-family="Inter,-apple-system,system-ui,sans-serif" '
        f'font-size="{label_size}" font-weight="900" fill="#FFFFFF">{esc(badge)}</text>'
    )
    tx = bx + bsize + 18
    out.append(text(tx, by + 28, title, size=28, weight=800, fill=C_INK))
    out.append(text(tx, by + 54, subtitle, size=14, fill=C_MUTED))
    pw = int(len(series_text) * 6.6) + 24
    out.append(
        f'<rect x="{CANVAS_W - PAD - pw}" y="{PAD + 12}" rx="13" ry="13" '
        f'width="{pw}" height="26" fill="{C_INK}"/>'
        f'<text x="{CANVAS_W - PAD - pw//2}" y="{PAD + 30}" text-anchor="middle" '
        f'font-family="Inter,-apple-system,system-ui,sans-serif" '
        f'font-size="12" font-weight="700" fill="#FFFFFF">{esc(series_text)}</text>'
    )
    return "".join(out)


def render_footer(chips, chip_colors, note):
    """Footer with per-chip color coding: GeoBrix functions vs. product built-ins."""
    out = []
    fy = CANVAS_H - PAD - FOOTER_H
    out.append(text(PAD, fy + 16, "KEY FUNCTIONS",
                    size=10, weight=700, fill=C_MUTED_3, letter_spacing="1.6"))
    cx, cy = PAD + 130, fy + 6
    for c, (fg, bg) in zip(chips, chip_colors):
        chip_svg, cw = chip(cx, cy, c, fg=fg, bg=bg, mono_font=True, h=24)
        out.append(chip_svg)
        cx += cw + 8
    out.append(text(CANVAS_W - PAD, fy + 16, note, size=11, fill=C_MUTED_3, anchor="end"))
    return "".join(out)


# ---------------------------------------------------------------------------
# Notebook content dicts
# ---------------------------------------------------------------------------

# DBX product built-in chip color (used across all notebooks)
def _dbx(text_val):
    return text_val, (DBX_FG, DBX_BG)


def _gbx(text_val, accent, tint):
    return text_val, (accent, tint)


# ── NB1 ─────────────────────────────────────────────────────────────────────
_A1, _T1 = THEMES["01"]["accent"], THEMES["01"]["tint"]
NB1 = dict(
    badge="01",
    series_pill="Wireless Coverage  ·  Part 1",
    title="Pure LiDAR → Canopy Height Model",
    subtitle=(
        "Load 3DEP LiDAR EPT tiles; bin ground returns to DTM (min-z) and all returns "
        "to DSM (max-z); derive CHM = rst_chm(DSM, DTM)"
    ),
    stages=[
        Stage(
            title="Stage LiDAR from EPT",
            subtitle=(
                "LidarDownloader walks the AWS 3DEP EPT octree for the AOI and writes each "
                "node as a .laz tile to the Volume."
            ),
            glyph=g_ept_nodes,
            chip_text="LidarDownloader",
        ),
        Stage(
            title="Load & tile point cloud",
            subtitle=(
                "lidar_gbx reads every .laz in LAZ_DIR; each return gets a tile key (tx, ty) "
                "and H3 res-15 cell id, saved as wc_lidar_pts."
            ),
            glyph=g_lidar_tile_stamp,
            chip_text="lidar_gbx · h3_longlatash3",
        ),
        Stage(
            title="Bin to DSM / DTM per tile",
            subtitle=(
                "bin_points_tiled grids each 1024 m tile: all-return max(z) = DSM, "
                "ground-return (class 2) min(z) = DTM."
            ),
            glyph=g_bin_rasters,
            chip_text="bin_points_tiled",
        ),
        Stage(
            title="CHM + write GeoTIFF surfaces",
            subtitle=(
                "rst_chm subtracts DTM from DSM (clamped ≥ 0) → canopy height per pixel; "
                "gtiff_gbx writes DSM, DTM, CHM tiles to OUT_DIR."
            ),
            glyph=g_chm_output,
            chip_text="rst_chm · gtiff_gbx",
        ),
    ],
    footer_chips=[
        "lidar_gbx",
        "bin_points_tiled",
        "rst_chm",
        "h3_longlatash3",
        "gtiff_gbx",
    ],
    footer_chip_colors=[
        (_A1, _T1),       # lidar_gbx — GeoBrix
        (_A1, _T1),       # bin_points_tiled — GeoBrix
        (_A1, _T1),       # rst_chm — GeoBrix
        (DBX_FG, DBX_BG), # h3_longlatash3 — product built-in
        (_A1, _T1),       # gtiff_gbx — GeoBrix
    ],
    note="databrickslabs/geobrix  ·  USGS 3DEP LiDAR  ·  EPSG:3857",
)

# ── NB2A ────────────────────────────────────────────────────────────────────
_A2A, _T2A = THEMES["02a"]["accent"], THEMES["02a"]["tint"]
NB2A = dict(
    badge="2a",
    series_pill="Wireless Coverage  ·  Part 2a",
    title="Surfaces from Point Cloud (TIN bare-earth DTM)",
    subtitle=(
        "Delaunay-TIN DTM from LiDAR ground returns (the production surface); "
        "rst_chm for CHM; writes canonical wc_surface_* tables + GeoTIFF tiles"
    ),
    stages=[
        Stage(
            title="Read Part 1 tables",
            subtitle=(
                "Reads wc_lidar_pts and wc_lidar_dsm from Delta — no .laz re-scan; "
                "the Part 1 max-z DSM carries forward unchanged."
            ),
            glyph=g_two_delta_tables,
            chip_text="wc_lidar_pts / dsm",
        ),
        Stage(
            title="TIN bare-earth DTM",
            subtitle=(
                "Ground returns (class 2) only; rst_dtmfromgeoms_agg builds a Delaunay TIN "
                "per tile → continuous bare-earth DTM."
            ),
            glyph=g_tin_mesh,
            chip_text="rst_dtmfromgeoms_agg",
        ),
        Stage(
            title="CHM = DSM − TIN DTM",
            subtitle=(
                "rst_chm joins DSM and TIN DTM on the tile key, warps DSM to DTM grid, "
                "and subtracts (clamped ≥ 0) → wc_surface_chm."
            ),
            glyph=g_raster_diff,
            chip_text="rst_chm",
        ),
        Stage(
            title="Write canonical surfaces",
            subtitle=(
                "gtiff_gbx writes DSM, TIN DTM, and CHM to OUT_DIR; also persisted as "
                "wc_surface_* Delta tables for Parts 3–4."
            ),
            glyph=g_surface_outputs,
            chip_text="gtiff_gbx",
        ),
    ],
    footer_chips=[
        "rst_dtmfromgeoms_agg",
        "rst_chm",
        "gtiff_gbx",
    ],
    footer_chip_colors=[
        (_A2A, _T2A),  # rst_dtmfromgeoms_agg — GeoBrix
        (_A2A, _T2A),  # rst_chm — GeoBrix
        (_A2A, _T2A),  # gtiff_gbx — GeoBrix
    ],
    note="databrickslabs/geobrix  ·  3DEP LiDAR  ·  Delaunay TIN bare earth",
)

# ── NB2B ────────────────────────────────────────────────────────────────────
_A2B, _T2B = THEMES["02b"]["accent"], THEMES["02b"]["tint"]
NB2B = dict(
    badge="2b",
    series_pill="Wireless Coverage  ·  Part 2b",
    title="Surfaces from DSM Raster (pixel-origin alternative)",
    subtitle=(
        "Alternative on-ramp: start from a DSM raster, not a point cloud. "
        "Morphological opening (rst_filter min→max) approximates bare earth; rst_chm for CHM"
    ),
    stages=[
        Stage(
            title="Read DSM GeoTIFF tiles",
            subtitle=(
                "rst_fromfile reads DSM GeoTIFF tiles from DSM_IN_DIR; "
                "tile indices (tx, ty) are parsed from the filename."
            ),
            glyph=g_dsm_raster_input,
            chip_text="rst_fromfile",
        ),
        Stage(
            title="Approximate bare earth",
            subtitle=(
                "rst_filter(DSM, K, 'min') erodes structures; "
                "rst_filter(eroded, K, 'max') restores terrain — DTM approximation."
            ),
            glyph=g_morph_opening,
            chip_text="rst_filter (min → max)",
        ),
        Stage(
            title="CHM = DSM − approx DTM",
            subtitle=(
                "rst_chm subtracts the morphological DTM from DSM (clamped ≥ 0) → "
                "canopy/structure height as wc_surface_chm."
            ),
            glyph=g_raster_diff,
            chip_text="rst_chm",
        ),
        Stage(
            title="Write canonical surfaces",
            subtitle=(
                "gtiff_gbx writes DSM, morphological DTM, and CHM to OUT_DIR as wc_surface_* "
                "— same outputs as Part 2a."
            ),
            glyph=g_surface_outputs,
            chip_text="gtiff_gbx",
        ),
    ],
    footer_chips=[
        "rst_fromfile",
        "rst_filter",
        "rst_chm",
        "gtiff_gbx",
    ],
    footer_chip_colors=[
        (_A2B, _T2B),  # rst_fromfile — GeoBrix
        (_A2B, _T2B),  # rst_filter — GeoBrix
        (_A2B, _T2B),  # rst_chm — GeoBrix
        (_A2B, _T2B),  # gtiff_gbx — GeoBrix
    ],
    note="databrickslabs/geobrix  ·  DSM raster → morphological bare earth  ·  approx. only",
)

# ── NB3A ────────────────────────────────────────────────────────────────────
_A3A, _T3A = THEMES["03a"]["accent"], THEMES["03a"]["tint"]
NB3A = dict(
    badge="3a",
    series_pill="Wireless Coverage  ·  Part 3a",
    title="H3-gridded signal surfaces",
    subtitle=(
        "Three parallel paths: DEM/DSM isobands → H3, CHM max-z → H3, "
        "then IDW gap-fill"
    ),
    stages=[
        Stage(
            title="Surface rasters in",
            subtitle=(
                "wc_surface_dtm, wc_surface_dsm, and wc_surface_chm — canonical Delta "
                "surfaces from Part 2a/2b."
            ),
            glyph=g_three_surface_tables,
            chip_text="wc_surface_dtm / dsm / chm",
        ),
        Stage(
            title="Reproject + clip",
            subtitle=(
                "rst_transform reprojects to 4326 to align with H3's lon/lat grid; "
                "rst_clip masks ocean tiles and no-data edges."
            ),
            glyph=g_reproject_clip,
            chip_text="rst_transform · rst_clip",
        ),
        Stage(
            title="H3 gridding — 3 paths",
            subtitle=(
                "DTM/DSM: rst_isoband isobands → h3_try_coverash3 cumulative tiers; "
                "CHM: gbx_rst_h3_rastertogridmax max-z per cell."
            ),
            glyph=g_three_path_h3,
            chip_text="rst_isoband · h3_try_coverash3",
        ),
        Stage(
            title="IDW gap fill + output",
            subtitle=(
                "h3_kring(k=1) finds cell neighbors; h3_cellfill(IDW) fills remaining "
                "gaps → wc_h3_dem, wc_h3_dsm, wc_h3_chm."
            ),
            glyph=g_three_h3_output_tables,
            chip_text="h3_kring · h3_cellfill",
        ),
    ],
    footer_chips=[
        "rst_transform",
        "rst_clip",
        "rst_isoband",
        "h3_try_coverash3",
        "gbx_rst_h3_rastertogridmax",
        "h3_kring",
        "h3_cellfill",
    ],
    footer_chip_colors=[
        (_A3A, _T3A),      # rst_transform — GeoBrix
        (_A3A, _T3A),      # rst_clip — GeoBrix
        (_A3A, _T3A),      # rst_isoband — GeoBrix
        (DBX_FG, DBX_BG),  # h3_try_coverash3 — product built-in
        (_A3A, _T3A),      # gbx_rst_h3_rastertogridmax — GeoBrix
        (DBX_FG, DBX_BG),  # h3_kring — product built-in
        (_A3A, _T3A),      # h3_cellfill — GeoBrix
    ],
    note="databrickslabs/geobrix  ·  3DEP LiDAR  ·  H3 res-9",
)

# ── NB3B ────────────────────────────────────────────────────────────────────
_A3B, _T3B = THEMES["03b"]["accent"], THEMES["03b"]["tint"]
NB3B = dict(
    badge="3b",
    series_pill="Wireless Coverage  ·  Part 3b",
    title="Raster-pixel surfaces (the pixel counterpoint)",
    subtitle=(
        "Side-by-side: 1 m-pixel DSM vs H3 DGGS cells; then raster vs H3 viewsheds "
        "on the same towers — rst_viewshed_towers vs h3_los_visible"
    ),
    stages=[
        Stage(
            title="Representation contrast",
            subtitle=(
                "Same area as 1 m pixel raster vs H3 cells at res 10. "
                "H3 is a resolution dial: res-15 cells are ~0.9 m²."
            ),
            glyph=g_pixel_vs_hex_contrast,
            chip_text="h3_try_coverash3 · h3_toparent",
        ),
        Stage(
            title="Candidate towers (spread pool)",
            subtitle=(
                "Res-9 centroid lattice over the DSM footprint, spread to cover clearings "
                "and elevated terrain across the window."
            ),
            glyph=g_spread_candidates,
            chip_text="h3_try_coverash3",
        ),
        Stage(
            title="Raster viewshed per tower",
            subtitle=(
                "rst_viewshed_towers runs a GDAL viewshed per tower "
                "(observer +10 m, target +1.6 m, 2.5 km radius) → visible_px."
            ),
            glyph=g_raster_viewshed_fan,
            chip_text="rst_viewshed_towers",
        ),
        Stage(
            title="H3 viewshed + comparison",
            subtitle=(
                "h3_los_visible bins DSM to res-12 H3 and runs LOS; raster vs H3 viewshed "
                "coverage compared on a 2.5 km-disk metric."
            ),
            glyph=g_h3_los_compare,
            chip_text="h3_los_visible",
        ),
    ],
    footer_chips=[
        "rst_viewshed_towers",
        "h3_los_visible",
        "h3_try_coverash3",
        "gbx_rst_h3_rastertogridmax",
        "h3_toparent",
    ],
    footer_chip_colors=[
        (_A3B, _T3B),      # rst_viewshed_towers — GeoBrix pyrx
        (_A3B, _T3B),      # h3_los_visible — GeoBrix pygx
        (DBX_FG, DBX_BG),  # h3_try_coverash3 — product built-in
        (_A3B, _T3B),      # gbx_rst_h3_rastertogridmax — GeoBrix
        (DBX_FG, DBX_BG),  # h3_toparent — product built-in
    ],
    note="databrickslabs/geobrix  ·  raster-pixel vs H3 DGGS  ·  2.5 km disk",
)

# ── NB4 ─────────────────────────────────────────────────────────────────────
_A4, _T4 = THEMES["04"]["accent"], THEMES["04"]["tint"]
NB4 = dict(
    badge="04",
    series_pill="Wireless Coverage  ·  Part 4",
    title="Naive H3-native tower siting",
    subtitle=(
        "Candidate lattice → quick-pass rule-out (~65% discarded) → "
        "exact H3 LOS via h3_los_visible → two-factor ranked sites + coverage"
    ),
    stages=[
        Stage(
            title="H3 surfaces in",
            subtitle=(
                "wc_h3_dem, wc_h3_dsm, and wc_h3_chm — H3-gridded surface tables "
                "from Part 3a."
            ),
            glyph=g_h3_input_tables,
            chip_text="wc_h3_dem / dsm / chm",
        ),
        Stage(
            title="Candidate lattice + quick pass",
            subtitle=(
                "One tower per H3 res-9 centroid over the AOI; coarse viewshed (res-10) "
                "rules out sites below 30% coverage."
            ),
            glyph=g_candidate_lattice_and_quickpass,
            chip_text="h3_try_coverash3 · h3_los_visible",
        ),
        Stage(
            title="Exact H3 line-of-sight",
            subtitle=(
                "h3_los_visible (pygx) reads DSM at +10 m and DTM at +1.6 m across "
                "a 2.5 km radius; DTM-less cells use nearest-ground."
            ),
            glyph=g_los_viewshed,
            chip_text="h3_los_visible",
        ),
        Stage(
            title="Ranked sites + coverage",
            subtitle=(
                "Two-factor rank: required_mast (CHM + clearance) and viewshed_cells "
                "(coverage). wc_h3_coverage: per-cell LOS depth."
            ),
            glyph=g_coverage_output_tables,
            chip_text="wc_h3_candidate_sites · wc_h3_coverage",
        ),
    ],
    footer_chips=[
        "h3_try_coverash3",
        "h3_los_visible",
        "gbx_rst_h3_rastertogridmax",
        "h3_kring",
    ],
    footer_chip_colors=[
        (DBX_FG, DBX_BG),  # h3_try_coverash3 — product built-in
        (_A4, _T4),        # h3_los_visible — GeoBrix pygx
        (_A4, _T4),        # gbx_rst_h3_rastertogridmax — GeoBrix
        (DBX_FG, DBX_BG),  # h3_kring — product built-in
    ],
    note="databrickslabs/geobrix  ·  3DEP LiDAR  ·  H3 res-9 lattice",
)


# ---------------------------------------------------------------------------
# Render per-notebook diagram (mirrors orthomosaic.render_notebook)
# ---------------------------------------------------------------------------

NOTEBOOKS = {
    "01":  NB1,
    "02a": NB2A,
    "02b": NB2B,
    "03a": NB3A,
    "03b": NB3B,
    "04":  NB4,
}


def render_notebook(key):
    nb = NOTEBOOKS[key]
    accent = THEMES[key]["accent"]
    tint = THEMES[key]["tint"]

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'viewBox="0 0 {CANVAS_W} {CANVAS_H}" '
        f'width="{CANVAS_W}" height="{CANVAS_H}" '
        f'style="font-family: Inter,-apple-system,system-ui,sans-serif;">'
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
    parts.append(f'<rect x="0" y="0" width="{CANVAS_W}" height="{CANVAS_H}" fill="url(#bg)"/>')

    parts.append(
        render_header(nb["badge"], nb["title"], nb["subtitle"],
                      accent, nb["series_pill"])
    )

    stage_y = PAD + HEADER_H + STAGE_TOP_GAP
    inner_w = CANVAS_W - 2 * PAD
    n = len(nb["stages"])
    arrows_total = (n - 1) * ARROW_W
    stage_w = (inner_w - arrows_total) // n
    cur_x = PAD
    for i, stg in enumerate(nb["stages"]):
        # render_stage from eo-series expects Stage with .glyph(cx,cy,color,tint) — no scale arg
        # call glyph directly via a wrapper Stage
        _stg = eo.Stage(
            title=stg.title,
            subtitle=stg.subtitle,
            glyph=(lambda g: (lambda cx, cy, c, t: g(cx, cy, c, t, scale=1.0)))(stg.glyph)
            if stg.glyph else None,
            chip_text=stg.chip_text,
        )
        parts.append(render_stage(cur_x, stage_y, stage_w, _stg, accent, tint))
        cur_x += stage_w
        if i < n - 1:
            parts.append(arrow(cur_x + 8, stage_y + STAGE_H / 2 - 30,
                               cur_x + ARROW_W - 8, color=accent))
            cur_x += ARROW_W

    parts.append(
        render_footer(nb["footer_chips"], nb["footer_chip_colors"], nb["note"])
    )
    parts.append("</svg>")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Overview diagram
# ---------------------------------------------------------------------------

OV_CANVAS_W = 1560
OV_PAD      = PAD
OV_HEADER_H = 118
OV_STAGE_TOP_GAP = 24
OV_STAGE_H  = 290
OV_GLYPH_H  = 112
OV_ARROW_W  = 36
OV_BRANCH_GAP = 28
OV_BRANCH_H = 90
OV_FOOTER_H = 46
OV_CANVAS_H = (OV_PAD + OV_HEADER_H + OV_STAGE_TOP_GAP + OV_STAGE_H
               + OV_BRANCH_GAP + OV_BRANCH_H + 20 + OV_FOOTER_H + OV_PAD)

# 6 main stages (config_nb acts as stage 0 to maintain readable flow)
OV_STAGES = [
    {"key": "config_nb",
     "title": "config_nb",
     "subtitle": "shared config: AOI, LAZ_DIR, CATALOG, SCHEMA, H3_RESOLUTION, helpers",
     "glyph": None,  # simple settings icon drawn inline
     "optional": False},
    {"key": "01",
     "title": "01 — LiDAR → CHM",
     "subtitle": "lidar_gbx → bin_points_tiled → rst_chm → GeoTIFF tiles",
     "glyph": g_lidar_tile_stamp,
     "optional": False},
    {"key": "02a",
     "title": "02a — TIN surfaces",
     "subtitle": "rst_dtmfromgeoms_agg → rst_chm → canonical wc_surface_*",
     "glyph": g_tin_mesh,
     "optional": False},
    {"key": "03a",
     "title": "03a — H3 gridding",
     "subtitle": "rst_isoband + h3_try_coverash3 + gbx_rst_h3_rastertogridmax",
     "glyph": g_three_path_h3,
     "optional": False},
    {"key": "03b",
     "title": "03b — pixel foil",
     "subtitle": "rst_viewshed_towers vs h3_los_visible — raster pixel counterpoint",
     "glyph": g_raster_viewshed_fan,
     "optional": False},
    {"key": "04",
     "title": "04 — tower siting",
     "subtitle": "candidate lattice → quick-pass → h3_los_visible → ranked sites",
     "glyph": g_los_viewshed,
     "optional": False},
]


def _wrap_text_ov(x, y, max_w, s, *, size=11, fill=C_MUTED, line_h=14):
    """Naive word-wrap for overview stage captions."""
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
            f'font-family="Inter,sans-serif" font-size="{size}" '
            f'fill="{fill}">{esc(line)}</text>'
        )
    return out


def render_overview_stage(x, y, w, stage, accent, tint, *, dashed=False):
    h = OV_STAGE_H
    out = [_card(x, y, w, h, dash="6 5" if dashed else None)]
    out.append(top_stripe(x, y, w, accent))

    if stage.get("glyph"):
        gy = y + 16 + OV_GLYPH_H // 2
        out.append(stage["glyph"](x + w // 2, gy, accent, tint, scale=0.60))
    else:
        # config_nb: simple settings panel (3 labeled rows)
        px0, py0 = x + int(w * 0.2), y + 24
        pw, php = int(w * 0.6), int(OV_GLYPH_H * 0.75)
        out.append(
            f'<rect x="{px0}" y="{py0}" rx="8" width="{pw}" height="{php}" '
            f'fill="{tint}" stroke="{accent}" stroke-width="1.8"/>'
        )
        for ri, lbl in enumerate(["AOI / LAZ_DIR", "H3_RESOLUTION", "CATALOG.SCHEMA"]):
            ry = py0 + 14 + ri * (php // 3 - 2)
            out.append(
                f'<circle cx="{px0 + 12}" cy="{ry}" r="3" fill="{accent}"/>'
                f'<text x="{px0 + 22}" y="{ry + 4}" '
                f'font-family="ui-monospace,Menlo,monospace" font-size="9" '
                f'font-weight="600" fill="{accent}">{esc(lbl)}</text>'
            )

    title_y = y + 16 + OV_GLYPH_H + 18
    out.append(text(x + w // 2, title_y, stage["title"],
                    size=14, weight=800, fill=C_INK, anchor="middle"))

    cap_top = title_y + 14
    out.extend(_wrap_text_ov(x + 10, cap_top, w - 20, stage["subtitle"],
                              size=10.5, fill=C_MUTED, line_h=13))

    if stage.get("optional"):
        out.append(text(x + w // 2, y + h - 12, "optional",
                        size=10, weight=700, fill=C_MUTED_3, anchor="middle",
                        letter_spacing="1"))
    return "".join(out)


def render_overview():
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'viewBox="0 0 {OV_CANVAS_W} {OV_CANVAS_H}" '
        f'width="{OV_CANVAS_W}" height="{OV_CANVAS_H}" '
        f'style="font-family: Inter,-apple-system,system-ui,sans-serif;">'
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

    accent_ov = THEMES["overview"]["accent"]
    tint_ov = THEMES["overview"]["tint"]

    # Header
    pw_s = "Wireless Coverage  ·  Overview"
    pw = int(len(pw_s) * 6.6) + 24
    parts.append(
        f'<rect x="{OV_PAD}" y="{OV_PAD + 4}" rx="14" width="60" height="60" '
        f'fill="{accent_ov}"/>'
        f'<text x="{OV_PAD + 30}" y="{OV_PAD + 38}" text-anchor="middle" '
        f'font-family="Inter,sans-serif" font-size="18" font-weight="900" '
        f'fill="#FFFFFF">OV</text>'
    )
    tx = OV_PAD + 78
    parts.append(text(tx, OV_PAD + 30, "Wireless Coverage Series — LiDAR to H3 tower viewsheds",
                      size=26, weight=800, fill=C_INK))
    parts.append(text(tx, OV_PAD + 54,
                      "config_nb → 01 (LiDAR) → 02a/02b (surfaces) → 03a (H3) → 03b (raster foil) → 04 (siting)",
                      size=13, fill=C_MUTED))
    parts.append(
        f'<rect x="{OV_CANVAS_W - OV_PAD - pw}" y="{OV_PAD + 12}" rx="13" '
        f'width="{pw}" height="26" fill="{accent_ov}"/>'
        f'<text x="{OV_CANVAS_W - OV_PAD - pw//2}" y="{OV_PAD + 30}" text-anchor="middle" '
        f'font-family="Inter,sans-serif" font-size="12" font-weight="700" '
        f'fill="#FFFFFF">{esc(pw_s)}</text>'
    )

    # Main stage row
    stage_y = OV_PAD + OV_HEADER_H + OV_STAGE_TOP_GAP
    inner_w = OV_CANVAS_W - 2 * OV_PAD
    n = len(OV_STAGES)
    arrows_total = (n - 1) * OV_ARROW_W
    stage_w = (inner_w - arrows_total) // n

    cur_x = OV_PAD
    ov2a_anchor = None  # center-bottom of 02a stage, for the 02b branch connector

    for i, stg in enumerate(OV_STAGES):
        key = stg["key"]
        if key in THEMES:
            acc_i = THEMES[key]["accent"]
            tint_i = THEMES[key]["tint"]
        else:
            acc_i = accent_ov
            tint_i = tint_ov

        parts.append(render_overview_stage(cur_x, stage_y, stage_w, stg, acc_i, tint_i,
                                           dashed=stg.get("optional", False)))
        if key == "02a":
            ov2a_anchor = (cur_x + stage_w // 2, stage_y + OV_STAGE_H)

        cur_x += stage_w
        if i < n - 1:
            parts.append(arrow(cur_x + 5, stage_y + OV_STAGE_H // 2 - 16,
                               cur_x + OV_ARROW_W - 5, color=C_MUTED_3))
            cur_x += OV_ARROW_W

    # 02b side branch (alternative path — dashed card below 02a)
    if ov2a_anchor:
        bx_center, by_top = ov2a_anchor
        branch_y = stage_y + OV_STAGE_H + OV_BRANCH_GAP
        bw, bh = int(stage_w * 1.15), OV_BRANCH_H
        bx0 = bx_center - bw // 2
        acc_2b = THEMES["02b"]["accent"]
        tint_2b = THEMES["02b"]["tint"]
        parts.append(arrow_xy(bx_center, by_top + 4, bx_center, branch_y - 6,
                              color=acc_2b, dash="5 4", width=2))
        parts.append(_card(bx0, branch_y, bw, bh, dash="5 4", shadow=False, stroke=acc_2b))
        parts.append(top_stripe(bx0, branch_y, bw, acc_2b, h=4))
        # Small DSM raster icon
        parts.append(
            f'<text x="{bx_center}" y="{branch_y + 20}" text-anchor="middle" '
            f'font-family="Inter,sans-serif" font-size="12" font-weight="800" '
            f'fill="{acc_2b}">02b — DSM raster alternative</text>'
        )
        parts.append(
            f'<text x="{bx_center}" y="{branch_y + 38}" text-anchor="middle" '
            f'font-family="Inter,sans-serif" font-size="10.5" fill="{C_MUTED}">'
            f'rst_filter (morph. opening) → rst_chm</text>'
        )
        parts.append(
            f'<text x="{bx_center}" y="{branch_y + 55}" text-anchor="middle" '
            f'font-family="Inter,sans-serif" font-size="10" font-weight="600" '
            f'fill="{acc_2b}">same wc_surface_* output as 02a</text>'
        )
        parts.append(
            f'<text x="{bx_center}" y="{branch_y + bh - 8}" text-anchor="middle" '
            f'font-family="Inter,sans-serif" font-size="9.5" font-weight="700" '
            f'fill="{C_MUTED_3}" letter-spacing="0.8">alternative</text>'
        )

    # Footer
    fy = OV_CANVAS_H - OV_PAD - OV_FOOTER_H
    series_chips = ["config_nb", "01_pure_lidar_chm", "02a_surfaces_dsm_dtm_chm",
                    "03a_h3_gridding", "03b_raster_pixel_surfaces", "04_tower_viewsheds"]
    parts.append(text(OV_PAD, fy + 14, "SERIES NOTEBOOKS",
                      size=10, weight=700, fill=C_MUTED_3, letter_spacing="1.6"))
    cx_f, cy_f = OV_PAD + 160, fy + 4
    for nb_name in series_chips:
        chip_svg, cw = chip(cx_f, cy_f, nb_name, fg=accent_ov, bg=tint_ov,
                            mono_font=True, h=22)
        parts.append(chip_svg)
        cx_f += cw + 8
    parts.append(text(OV_CANVAS_W - OV_PAD, fy + 14,
                      "databrickslabs/geobrix  ·  USGS 3DEP  ·  H3 DGGS",
                      size=11, fill=C_MUTED_3, anchor="end"))

    parts.append("</svg>")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    out_dir = os.path.join(_HERE, "..", "diagrams", "wireless-coverage")
    os.makedirs(out_dir, exist_ok=True)

    # Per-notebook diagrams
    for key in ("01", "02a", "02b", "03a", "03b", "04"):
        path = os.path.join(out_dir, f"wireless-coverage-{key}.svg")
        with open(path, "w") as f:
            f.write(render_notebook(key))
            f.write("\n")
        print(f"wrote {path}")

    # Series overview
    path = os.path.join(out_dir, "wireless-coverage-overview.svg")
    with open(path, "w") as f:
        f.write(render_overview())
        f.write("\n")
    print(f"wrote {path}")


if __name__ == "__main__":
    main()
