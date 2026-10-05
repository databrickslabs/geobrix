#!/usr/bin/env python3
"""Generate nb3 and nb4 pipeline diagrams for the wireless-coverage series.

Mirrors vapor-eyes.py: imports eo-series.py primitives, adds custom glyphs,
produces one SVG per notebook.  Renders SVGs here; turn them into PNGs with
headless Chrome, then crop whitespace:

    python3 resources/images/generators/wireless-coverage.py
    for n in 03 04; do
      "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \\
          --headless --disable-gpu --hide-scrollbars \\
          --force-device-scale-factor=2 --window-size=1500,820 \\
          --screenshot=resources/images/diagrams/wireless-coverage/wireless-coverage-$n.png \\
          resources/images/diagrams/wireless-coverage/wireless-coverage-$n.svg
    done
    python3 -c "
    from PIL import Image, ImageChops
    import glob
    for p in glob.glob('resources/images/diagrams/wireless-coverage/wireless-coverage-0*.png'):
        img = Image.open(p).convert('RGB')
        bbox = ImageChops.difference(img, Image.new('RGB', img.size, (255,255,255))).getbbox()
        if bbox: img.crop(bbox).save(p)
    "
"""
import importlib.util
import math
import os

_HERE = os.path.dirname(os.path.abspath(__file__))
_EO = os.path.join(_HERE, "eo-series.py")
_spec = importlib.util.spec_from_file_location("eo_series", _EO)
eo = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(eo)

# Shared primitives / constants from eo-series
text, chip, arrow, render_stage, Stage, esc = (
    eo.text, eo.chip, eo.arrow, eo.render_stage, eo.Stage, eo.esc
)
C_INK, C_MUTED, C_MUTED_2, C_MUTED_3 = eo.C_INK, eo.C_MUTED, eo.C_MUTED_2, eo.C_MUTED_3
C_BORDER = eo.C_BORDER
CANVAS_W, CANVAS_H, PAD = eo.CANVAS_W, eo.CANVAS_H, eo.PAD
HEADER_H, STAGE_TOP_GAP, STAGE_H = eo.HEADER_H, eo.STAGE_TOP_GAP, eo.STAGE_H
ARROW_W, FOOTER_H = eo.ARROW_W, eo.FOOTER_H
card, top_stripe = eo.card, eo.top_stripe
STAGE_GLYPH_H = eo.STAGE_GLYPH_H

# --- nb3 theme (teal — H3 gridding step) ---------------------------------------

ACCENT = "#0F8E8B"
TINT   = "#D5ECEC"

# Secondary accent for product functions (Databricks built-ins)
ACCENT_2 = "#1F6FB5"
TINT_2   = "#E3EEF8"


# --- Custom glyphs --------------------------------------------------------------

def g_three_surface_tables(cx, cy, color, tint):
    """Three stacked Delta-table icons labelled DTM / DSM / CHM."""
    labels = ["DTM", "DSM", "CHM"]
    w, h = 100, 60
    # render back-to-front (CHM at back, DTM at front)
    out = []
    offsets = [(16, 16), (8, 8), (0, 0)]
    opacities = [0.45, 0.65, 1.0]
    for i, (dx, dy) in enumerate(offsets):
        x = cx - w / 2 + dx
        y = cy - h / 2 + dy - 16
        op = opacities[i]
        fill = "#FFFFFF" if i == 2 else tint
        lbl = labels[i]
        out.append(
            f'<rect x="{x}" y="{y}" rx="7" ry="7" width="{w}" height="{h}" '
            f'fill="{fill}" fill-opacity="{op}" stroke="{color}" stroke-width="1.8"/>'
        )
        # header stripe
        out.append(
            f'<rect x="{x}" y="{y}" width="{w}" height="18" rx="7" ry="7" '
            f'fill="{color}" fill-opacity="{op}"/>'
            f'<rect x="{x}" y="{y + 10}" width="{w}" height="8" '
            f'fill="{color}" fill-opacity="{op}"/>'
            f'<text x="{x + w/2}" y="{y + 13}" text-anchor="middle" '
            f'font-family="ui-monospace, Menlo, monospace" font-size="9" '
            f'font-weight="800" fill="#FFFFFF" fill-opacity="1">{lbl}</text>'
        )
        # data rows
        for r in range(2):
            ry = y + 28 + r * 12
            out.append(
                f'<line x1="{x + 8}" y1="{ry}" x2="{x + w - 8}" y2="{ry}" '
                f'stroke="{color}" stroke-opacity="0.35" stroke-width="1.2"/>'
            )
    return "".join(out)


def g_reproject_clip(cx, cy, color, tint):
    """A raster tile with an overlaid 4326 coordinate badge and clip envelope."""
    # Background raster (simplified grid)
    cells, s = 5, 20
    span = cells * s
    x0 = cx - span / 2
    y0 = cy - span / 2 - 4
    out = [
        f'<rect x="{x0}" y="{y0}" width="{span}" height="{span}" '
        f'fill="{tint}" stroke="{color}" stroke-width="2" rx="4"/>'
    ]
    # Gridlines
    for i in range(1, cells):
        out.append(
            f'<line x1="{x0 + i*s}" y1="{y0}" x2="{x0 + i*s}" y2="{y0 + span}" '
            f'stroke="{color}" stroke-opacity="0.25" stroke-width="0.8"/>'
        )
        out.append(
            f'<line x1="{x0}" y1="{y0 + i*s}" x2="{x0 + span}" y2="{y0 + i*s}" '
            f'stroke="{color}" stroke-opacity="0.25" stroke-width="0.8"/>'
        )
    # Cell fill gradient (pseudo-elevation)
    for r in range(cells):
        for c in range(cells):
            v = 0.12 + 0.55 * ((math.sin(r * 1.1 + c * 0.8) + 1) / 2)
            out.append(
                f'<rect x="{x0 + c*s}" y="{y0 + r*s}" width="{s}" height="{s}" '
                f'fill="{color}" fill-opacity="{v:.2f}"/>'
            )
    # CRS badge — bottom right corner
    bw, bh = 56, 18
    bx = x0 + span - bw + 2
    by = y0 + span - bh + 2
    out.append(
        f'<rect x="{bx}" y="{by}" rx="5" ry="5" width="{bw}" height="{bh}" '
        f'fill="{color}"/>'
        f'<text x="{bx + bw/2}" y="{by + 13}" text-anchor="middle" '
        f'font-family="ui-monospace, Menlo, monospace" font-size="9" '
        f'font-weight="800" fill="#FFFFFF">EPSG:4326</text>'
    )
    # Clip envelope (dashed rounded rect)
    cw, ch = 68, 60
    out.append(
        f'<rect x="{cx - cw/2}" y="{cy - ch/2 - 6}" width="{cw}" height="{ch}" '
        f'rx="12" ry="12" fill="none" stroke="{color}" stroke-width="2.4" '
        f'stroke-dasharray="6 4"/>'
    )
    return "".join(out)


def g_three_path_h3(cx, cy, color, tint):
    """Hex grid (CHM rastertogridmax) + isoband contour lines (DEM/DSM path)."""
    out = []
    # Hex grid — 3×4 array
    R = 18
    dx_h = R * math.sqrt(3)
    dy_h = R * 1.5
    rows, cols = 4, 4
    x0 = cx - (cols - 1) * dx_h / 2 - dx_h / 4 + 4
    y0 = cy - (rows - 1) * dy_h / 2 - 8
    for r in range(rows):
        for c in range(cols):
            x = x0 + c * dx_h + (dx_h / 2 if r % 2 else 0)
            y = y0 + r * dy_h
            pts = []
            for i in range(6):
                a = math.radians(60 * i - 90)
                pts.append(f"{x + R*math.cos(a):.1f},{y + R*math.sin(a):.1f}")
            # Vary fill opacity to simulate different H3 values
            intensity = 0.12 + 0.72 * abs(math.sin(r * 1.3 + c * 0.9))
            out.append(
                f'<polygon points="{" ".join(pts)}" '
                f'fill="{color}" fill-opacity="{intensity:.2f}" '
                f'stroke="{color}" stroke-width="1.2" stroke-opacity="0.6"/>'
            )
    # Isoband contour lines (DEM/DSM path) — overlaid on top
    # Two curved iso-contours suggesting height bands
    for offset, op in [(-22, 0.8), (0, 0.9)]:
        pts_line = []
        for i in range(7):
            fx = (i / 6)
            sx = cx - 52 + fx * 104
            sy = (cy + offset - 4
                  + 14 * math.sin(fx * math.pi * 1.4)
                  - 10 * math.cos(fx * math.pi * 0.8))
            pts_line.append(f"{sx:.1f},{sy:.1f}")
        out.append(
            f'<polyline points="{" ".join(pts_line)}" fill="none" '
            f'stroke="#FFFFFF" stroke-width="2.2" stroke-linecap="round" '
            f'stroke-linejoin="round" stroke-opacity="{op}"/>'
        )
    return "".join(out)


def g_three_h3_output_tables(cx, cy, color, tint):
    """Three stacked Delta-table icons for the H3 output tables (DEM / DSM / CHM).

    Mirrors g_three_surface_tables (Stage 1 inputs) so the diagram reads
    symmetrically: Delta tables in → pipeline → Delta tables out.
    A small hex badge on each header marks them as H3 cell tables.
    """
    labels = ["DEM", "DSM", "CHM"]
    w, h = 100, 60
    out = []
    offsets = [(16, 16), (8, 8), (0, 0)]
    opacities = [0.45, 0.65, 1.0]
    for i, (dx, dy) in enumerate(offsets):
        x = cx - w / 2 + dx
        y = cy - h / 2 + dy - 16
        op = opacities[i]
        fill = "#FFFFFF" if i == 2 else tint
        lbl = labels[i]
        out.append(
            f'<rect x="{x}" y="{y}" rx="7" ry="7" width="{w}" height="{h}" '
            f'fill="{fill}" fill-opacity="{op}" stroke="{color}" stroke-width="1.8"/>'
        )
        out.append(
            f'<rect x="{x}" y="{y}" width="{w}" height="18" rx="7" ry="7" '
            f'fill="{color}" fill-opacity="{op}"/>'
            f'<rect x="{x}" y="{y + 10}" width="{w}" height="8" '
            f'fill="{color}" fill-opacity="{op}"/>'
            f'<text x="{x + w/2}" y="{y + 13}" text-anchor="middle" '
            f'font-family="ui-monospace, Menlo, monospace" font-size="9" '
            f'font-weight="800" fill="#FFFFFF" fill-opacity="1">wc_h3_{lbl.lower()}</text>'
        )
        # Data rows
        for r in range(2):
            ry = y + 28 + r * 12
            out.append(
                f'<line x1="{x + 8}" y1="{ry}" x2="{x + w - 8}" y2="{ry}" '
                f'stroke="{color}" stroke-opacity="0.35" stroke-width="1.2"/>'
            )
        # Hex badge (top-right corner) — marks this as an H3 cell table
        hx, hy, hr = x + w - 2, y - 2, 7
        hex_pts = " ".join(
            f"{hx + hr*math.cos(math.radians(60*k-90)):.1f},"
            f"{hy + hr*math.sin(math.radians(60*k-90)):.1f}"
            for k in range(6)
        )
        out.append(
            f'<polygon points="{hex_pts}" fill="{color}" fill-opacity="{op}" '
            f'stroke="#FFFFFF" stroke-width="0.8"/>'
        )
    return "".join(out)


# --- Header / footer -----------------------------------------------------------

def render_header(badge, title, subtitle, accent, series_text):
    out = []
    bsize = 60
    bx, by = PAD, PAD + 4
    label_size = 32 if len(badge) <= 2 else 18
    out.append(
        f'<rect x="{bx}" y="{by}" rx="14" ry="14" width="{bsize}" height="{bsize}" '
        f'fill="{accent}"/>'
        f'<text x="{bx + bsize/2}" y="{by + bsize/2 + 10}" text-anchor="middle" '
        f'font-family="Inter, -apple-system, system-ui, sans-serif" '
        f'font-size="{label_size}" font-weight="900" fill="#FFFFFF">{esc(badge)}</text>'
    )
    tx = bx + bsize + 18
    out.append(text(tx, by + 28, title, size=28, weight=800, fill=C_INK))
    out.append(text(tx, by + 54, subtitle, size=14, fill=C_MUTED))
    pw = int(len(series_text) * 6.6) + 24
    out.append(
        f'<rect x="{CANVAS_W - PAD - pw}" y="{PAD + 12}" rx="13" ry="13" '
        f'width="{pw}" height="26" fill="{C_INK}"/>'
        f'<text x="{CANVAS_W - PAD - pw/2}" y="{PAD + 30}" text-anchor="middle" '
        f'font-family="Inter, -apple-system, system-ui, sans-serif" '
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


# --- Diagram -------------------------------------------------------------------

NB3 = dict(
    badge="03",
    title="H3-gridded signal surfaces",
    subtitle=(
        "Three parallel paths: DEM/DSM isobands → H3, CHM max-z → H3, "
        "then IDW gap-fill"
    ),
    series_pill="Wireless Coverage  ·  Part 3",
    stages=[
        Stage(
            title="Surface rasters in",
            subtitle=(
                "wc_surface_dtm, wc_surface_dsm, and wc_surface_chm — Delta tables "
                "from the LiDAR run (bare-earth DTM, first-return DSM, canopy-height CHM)"
            ),
            glyph=g_three_surface_tables,
            chip_text="wc_surface_dtm / dsm / chm",
        ),
        Stage(
            title="Reproject + clip",
            subtitle=(
                "rst_transform(3857→4326) aligns each surface raster to H3's "
                "lon/lat grid; rst_clip(land) masks ocean tiles and no-data borders"
            ),
            glyph=g_reproject_clip,
            chip_text="rst_transform · rst_clip",
        ),
        Stage(
            title="H3 gridding — 3 paths",
            subtitle=(
                "DTM/DSM: rst_isoband(breaks) → h3_try_coverash3 + explode → "
                "F.sequence cumulative tiers.  "
                "CHM: gbx_rst_h3_rastertogridmax → max-z per H3 cell"
            ),
            glyph=g_three_path_h3,
            chip_text="rst_isoband · h3_try_coverash3 · gbx_rst_h3_rastertogridmax",
        ),
        Stage(
            title="IDW gap fill + output",
            subtitle=(
                "h3_kring(k=1) identifies neighbors of populated H3 cells; "
                "h3_cellfill(IDW) interpolates remaining gaps → wc_h3_dem, wc_h3_dsm, wc_h3_chm"
            ),
            glyph=g_three_h3_output_tables,
            chip_text="h3_kring · h3_cellfill",
        ),
    ],
    # Footer chip palette: GeoBrix = ACCENT/TINT, DBX product = ACCENT_2/TINT_2
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
        (ACCENT, TINT),      # rst_transform — GeoBrix
        (ACCENT, TINT),      # rst_clip — GeoBrix
        (ACCENT, TINT),      # rst_isoband — GeoBrix
        (ACCENT_2, TINT_2),  # h3_try_coverash3 — product built-in
        (ACCENT, TINT),      # gbx_rst_h3_rastertogridmax — GeoBrix
        (ACCENT_2, TINT_2),  # h3_kring — product built-in
        (ACCENT, TINT),      # h3_cellfill — GeoBrix
    ],
    note="databrickslabs/geobrix  ·  3DEP LiDAR  ·  H3 res-9",
)


# --- nb4 theme (amber — tower siting step) ------------------------------------

ACCENT_4 = "#C47A15"
TINT_4   = "#FAECD0"

# Secondary accent for product functions (Databricks built-ins) — reuse nb3 blue
ACCENT_4_2 = "#1F6FB5"
TINT_4_2   = "#E3EEF8"


# --- nb4 custom glyphs ---------------------------------------------------------

def g_h3_input_tables(cx, cy, color, tint):
    """Three stacked Delta-table icons for the H3 input tables (DEM / DSM / CHM)
    with a hex badge — mirrors g_three_h3_output_tables from nb3."""
    labels = ["DEM", "DSM", "CHM"]
    w, h = 100, 60
    out = []
    offsets = [(16, 16), (8, 8), (0, 0)]
    opacities = [0.45, 0.65, 1.0]
    for i, (dx, dy) in enumerate(offsets):
        x = cx - w / 2 + dx
        y = cy - h / 2 + dy - 16
        op = opacities[i]
        fill = "#FFFFFF" if i == 2 else tint
        lbl = labels[i]
        out.append(
            f'<rect x="{x}" y="{y}" rx="7" ry="7" width="{w}" height="{h}" '
            f'fill="{fill}" fill-opacity="{op}" stroke="{color}" stroke-width="1.8"/>'
        )
        out.append(
            f'<rect x="{x}" y="{y}" width="{w}" height="18" rx="7" ry="7" '
            f'fill="{color}" fill-opacity="{op}"/>'
            f'<rect x="{x}" y="{y + 10}" width="{w}" height="8" '
            f'fill="{color}" fill-opacity="{op}"/>'
            f'<text x="{x + w/2}" y="{y + 13}" text-anchor="middle" '
            f'font-family="ui-monospace, Menlo, monospace" font-size="9" '
            f'font-weight="800" fill="#FFFFFF" fill-opacity="1">wc_h3_{lbl.lower()}</text>'
        )
        for r in range(2):
            ry = y + 28 + r * 12
            out.append(
                f'<line x1="{x + 8}" y1="{ry}" x2="{x + w - 8}" y2="{ry}" '
                f'stroke="{color}" stroke-opacity="0.35" stroke-width="1.2"/>'
            )
        # Hex badge top-right
        hx, hy, hr = x + w - 2, y - 2, 7
        hex_pts = " ".join(
            f"{hx + hr*math.cos(math.radians(60*k-90)):.1f},"
            f"{hy + hr*math.sin(math.radians(60*k-90)):.1f}"
            for k in range(6)
        )
        out.append(
            f'<polygon points="{hex_pts}" fill="{color}" fill-opacity="{op}" '
            f'stroke="#FFFFFF" stroke-width="0.8"/>'
        )
    return "".join(out)


def g_candidate_lattice(cx, cy, color, tint):
    """H3 res-9 hex lattice with centroid markers — the candidate tower grid."""
    R = 20
    dx_h = R * math.sqrt(3)
    dy_h = R * 1.5
    rows, cols = 3, 4
    x0 = cx - (cols - 1) * dx_h / 2 - dx_h / 4 + 2
    y0 = cy - (rows - 1) * dy_h / 2 - 6
    out = []
    for r in range(rows):
        for c in range(cols):
            x = x0 + c * dx_h + (dx_h / 2 if r % 2 else 0)
            y = y0 + r * dy_h
            pts = []
            for i in range(6):
                a = math.radians(60 * i - 90)
                pts.append(f"{x + R*math.cos(a):.1f},{y + R*math.sin(a):.1f}")
            out.append(
                f'<polygon points="{" ".join(pts)}" '
                f'fill="{tint}" fill-opacity="0.7" '
                f'stroke="{color}" stroke-width="1.4" stroke-opacity="0.6"/>'
            )
            # Tower centroid dot
            out.append(
                f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3.5" '
                f'fill="{color}" fill-opacity="0.85"/>'
            )
    return "".join(out)


def g_quick_pass_funnel(cx, cy, color, tint):
    """A funnel/filter: many points in, fewer survivors out."""
    out = []
    # Funnel body
    fw, ft, fb, fh = 90, 10, 38, 70
    fx = cx - fw / 2
    fy = cy - fh / 2 - 10
    pts = (f"{fx},{fy} {fx + fw},{fy} "
           f"{cx + fb/2},{fy + fh} {cx - fb/2},{fy + fh}")
    out.append(
        f'<polygon points="{pts}" fill="{tint}" fill-opacity="0.7" '
        f'stroke="{color}" stroke-width="2" stroke-linejoin="round"/>'
    )
    # "In" dots (many candidates — top of funnel)
    for idx, dx in enumerate([-32, -16, 0, 16, 32]):
        out.append(
            f'<circle cx="{cx + dx}" cy="{fy - 14}" r="4.5" '
            f'fill="{color}" fill-opacity="{0.5 + 0.1 * idx}"/>'
        )
    # Threshold label inside funnel
    out.append(
        f'<text x="{cx}" y="{fy + fh/2 + 5}" text-anchor="middle" '
        f'font-family="ui-monospace, Menlo, monospace" font-size="9" '
        f'font-weight="700" fill="{color}">≥ 30%</text>'
    )
    # "Out" dots (fewer survivors — bottom of funnel)
    for idx, dx in enumerate([-10, 0, 10]):
        out.append(
            f'<circle cx="{cx + dx}" cy="{fy + fh + 14}" r="4.5" '
            f'fill="{color}" fill-opacity="0.85"/>'
        )
    # X marks for ruled-out candidates (faded)
    for dx in [-26, 24]:
        out.append(
            f'<line x1="{cx + dx - 5}" y1="{fy + fh + 9}" '
            f'x2="{cx + dx + 5}" y2="{fy + fh + 19}" '
            f'stroke="{color}" stroke-width="2" stroke-opacity="0.35"/>'
            f'<line x1="{cx + dx + 5}" y1="{fy + fh + 9}" '
            f'x2="{cx + dx - 5}" y2="{fy + fh + 19}" '
            f'stroke="{color}" stroke-width="2" stroke-opacity="0.35"/>'
        )
    return "".join(out)


def g_los_viewshed(cx, cy, color, tint):
    """Tower with LOS rays fanning out to H3 cells — h3_los_visible."""
    out = []
    # Tower body (simplified)
    tx, ty, tw, th = cx - 5, cy - 36, 10, 38
    out.append(
        f'<rect x="{tx}" y="{ty}" width="{tw}" height="{th}" '
        f'fill="{color}" fill-opacity="0.8" rx="2"/>'
    )
    # Antenna spike
    out.append(
        f'<line x1="{cx}" y1="{ty}" x2="{cx}" y2="{ty - 14}" '
        f'stroke="{color}" stroke-width="2" stroke-linecap="round"/>'
        f'<circle cx="{cx}" cy="{ty - 17}" r="3" fill="{color}"/>'
    )
    # LOS rays to surrounding hex cells
    base_y = ty + th // 2
    ray_targets = [
        (-52, -8), (-42, 22), (-30, 42),
        (30, -28), (50, 0), (44, 30),
        (0, 48),
    ]
    for i, (rdx, rdy) in enumerate(ray_targets):
        op = 0.55 + 0.1 * (i % 3)
        out.append(
            f'<line x1="{cx}" y1="{base_y}" '
            f'x2="{cx + rdx}" y2="{base_y + rdy}" '
            f'stroke="{color}" stroke-width="1.4" stroke-opacity="{op:.2f}" '
            f'stroke-linecap="round"/>'
        )
        out.append(
            f'<circle cx="{cx + rdx}" cy="{base_y + rdy}" r="4" '
            f'fill="{tint}" stroke="{color}" stroke-width="1.2" '
            f'fill-opacity="0.9"/>'
        )
    return "".join(out)


def g_coverage_output_tables(cx, cy, color, tint):
    """Two stacked output Delta tables (candidate_sites + coverage) with hex badges."""
    labels = ["candidate_sites", "coverage"]
    w, h = 116, 58
    out = []
    offsets = [(10, 10), (0, 0)]
    opacities = [0.5, 1.0]
    for i, (dx, dy) in enumerate(offsets):
        x = cx - w / 2 + dx
        y = cy - h / 2 + dy - 12
        op = opacities[i]
        fill = "#FFFFFF" if i == 1 else tint
        lbl = labels[i]
        out.append(
            f'<rect x="{x}" y="{y}" rx="7" ry="7" width="{w}" height="{h}" '
            f'fill="{fill}" fill-opacity="{op}" stroke="{color}" stroke-width="1.8"/>'
        )
        out.append(
            f'<rect x="{x}" y="{y}" width="{w}" height="18" rx="7" ry="7" '
            f'fill="{color}" fill-opacity="{op}"/>'
            f'<rect x="{x}" y="{y + 10}" width="{w}" height="8" '
            f'fill="{color}" fill-opacity="{op}"/>'
            f'<text x="{x + w/2}" y="{y + 13}" text-anchor="middle" '
            f'font-family="ui-monospace, Menlo, monospace" font-size="8.5" '
            f'font-weight="800" fill="#FFFFFF" fill-opacity="1">wc_h3_{lbl}</text>'
        )
        # Two-column data rows (required_mast | viewshed_cells)
        for r in range(2):
            ry = y + 28 + r * 12
            out.append(
                f'<line x1="{x + 8}" y1="{ry}" x2="{x + w - 8}" y2="{ry}" '
                f'stroke="{color}" stroke-opacity="0.35" stroke-width="1.2"/>'
            )
        # Column divider (two-factor table)
        out.append(
            f'<line x1="{x + w//2}" y1="{y + 20}" '
            f'x2="{x + w//2}" y2="{y + h - 4}" '
            f'stroke="{color}" stroke-opacity="0.25" stroke-width="0.8"/>'
        )
        # Hex badge
        hx, hy, hr = x + w - 2, y - 2, 7
        hex_pts = " ".join(
            f"{hx + hr*math.cos(math.radians(60*k-90)):.1f},"
            f"{hy + hr*math.sin(math.radians(60*k-90)):.1f}"
            for k in range(6)
        )
        out.append(
            f'<polygon points="{hex_pts}" fill="{color}" fill-opacity="{op}" '
            f'stroke="#FFFFFF" stroke-width="0.8"/>'
        )
    return "".join(out)


def g_candidate_lattice_and_quickpass(cx, cy, color, tint):
    """Hex candidate grid (top half) with a funnel filter (bottom half)."""
    # Mini hex grid — upper portion
    R = 14
    dx_h = R * math.sqrt(3)
    dy_h = R * 1.5
    rows_g, cols_g = 2, 4
    x0 = cx - (cols_g - 1) * dx_h / 2 - dx_h / 4 + 2
    y0 = cy - 70
    out = []
    for r in range(rows_g):
        for c in range(cols_g):
            x = x0 + c * dx_h + (dx_h / 2 if r % 2 else 0)
            y = y0 + r * dy_h
            pts = []
            for i in range(6):
                a = math.radians(60 * i - 90)
                pts.append(f"{x + R*math.cos(a):.1f},{y + R*math.sin(a):.1f}")
            out.append(
                f'<polygon points="{" ".join(pts)}" '
                f'fill="{tint}" fill-opacity="0.65" '
                f'stroke="{color}" stroke-width="1.2" stroke-opacity="0.55"/>'
            )
            out.append(
                f'<circle cx="{x:.1f}" cy="{y:.1f}" r="2.5" '
                f'fill="{color}" fill-opacity="0.8"/>'
            )
    # Funnel — lower portion (compact)
    fy0 = cy - 14
    fw2, fb2, fh2 = 72, 28, 52
    pfx = cx - fw2 / 2
    pts_f = (f"{pfx},{fy0} {pfx + fw2},{fy0} "
             f"{cx + fb2/2},{fy0 + fh2} {cx - fb2/2},{fy0 + fh2}")
    out.append(
        f'<polygon points="{pts_f}" fill="{tint}" fill-opacity="0.55" '
        f'stroke="{color}" stroke-width="1.8" stroke-linejoin="round"/>'
    )
    out.append(
        f'<text x="{cx}" y="{fy0 + fh2/2 + 5}" text-anchor="middle" '
        f'font-family="ui-monospace, Menlo, monospace" font-size="9" '
        f'font-weight="700" fill="{color}">≥ 30%</text>'
    )
    # Survivors
    for dx2 in [-9, 0, 9]:
        out.append(
            f'<circle cx="{cx + dx2}" cy="{fy0 + fh2 + 12}" r="3.5" '
            f'fill="{color}" fill-opacity="0.9"/>'
        )
    return "".join(out)


NB4 = dict(
    badge="04",
    title="Naive H3-native tower siting",
    subtitle=(
        "Candidate lattice → quick-pass rule-out (~65% discarded) → "
        "exact H3 LOS via h3_los_visible → two-factor ranked sites + coverage"
    ),
    series_pill="Wireless Coverage  ·  Part 4",
    stages=[
        Stage(
            title="H3 surfaces in",
            subtitle=(
                "wc_h3_dem, wc_h3_dsm, and wc_h3_chm — the H3-gridded surface "
                "tables from Part 3 (bare-earth DTM, first-return DSM, canopy CHM)"
            ),
            glyph=g_h3_input_tables,
            chip_text="wc_h3_dem / dsm / chm",
        ),
        Stage(
            title="Candidate lattice + quick pass",
            subtitle=(
                "One tower per H3 res-9 centroid over the AOI. Cheap coarse viewshed "
                "(res-10) rules out sites below 30% coverage before any fine-res work "
                "(176 → 62 survivors in the demo)"
            ),
            glyph=g_candidate_lattice_and_quickpass,
            chip_text="h3_try_coverash3 · h3_los_visible (coarse)",
        ),
        Stage(
            title="Exact H3 line-of-sight",
            subtitle=(
                "h3_los_visible (pygx): observer = DSM + 10 m antenna; "
                "target = DTM + 1.6 m receiver; 2.5 km buffer. "
                "Nearest-ground fallback for DTM-less cells"
            ),
            glyph=g_los_viewshed,
            chip_text="h3_los_visible",
        ),
        Stage(
            title="Ranked sites + coverage",
            subtitle=(
                "Two-factor table: required_mast (CHM + clearance; lower = cheaper) "
                "and viewshed_cells (coverage; higher = better). "
                "wc_h3_coverage: per-cell LOS depth over all/top-10 sites"
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
        (ACCENT_4_2, TINT_4_2),  # h3_try_coverash3 — product built-in
        (ACCENT_4, TINT_4),      # h3_los_visible — GeoBrix pygx
        (ACCENT_4, TINT_4),      # gbx_rst_h3_rastertogridmax — GeoBrix
        (ACCENT_4_2, TINT_4_2),  # h3_kring — product built-in
    ],
    note="databrickslabs/geobrix  ·  3DEP LiDAR  ·  H3 res-9 lattice",
)


def render_diagram(nb, accent, tint):
    from textwrap import dedent

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" '
        f'viewBox="0 0 {CANVAS_W} {CANVAS_H}" '
        f'width="{CANVAS_W}" height="{CANVAS_H}" '
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
        parts.append(render_stage(cur_x, stage_y, stage_w, stg, accent, tint))
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


def main():
    out_dir = os.path.join(_HERE, "..", "diagrams", "wireless-coverage")
    os.makedirs(out_dir, exist_ok=True)

    for nb, accent, tint, fname in [
        (NB3, ACCENT,   TINT,   "wireless-coverage-03.svg"),
        (NB4, ACCENT_4, TINT_4, "wireless-coverage-04.svg"),
    ]:
        path = os.path.join(out_dir, fname)
        with open(path, "w") as f:
            f.write(render_diagram(nb, accent, tint))
            f.write("\n")
        print(f"wrote {path}")


if __name__ == "__main__":
    main()
