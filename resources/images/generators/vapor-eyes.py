#!/usr/bin/env python3
"""Generate conceptual pipeline diagrams for the vapor-eyes methane series.

Mirrors eo-series.py: per-notebook Stage lists -> one SVG each. Renders SVGs here;
turn them into PNGs with headless Chrome, then crop whitespace:

    python3 resources/images/generators/vapor-eyes.py
    for n in 01; do
      "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
        --headless --disable-gpu --hide-scrollbars \
        --force-device-scale-factor=2 --window-size=1500,820 \
        --screenshot=resources/images/diagrams/vapor-eyes/vapor-eyes-$n.png \
        resources/images/diagrams/vapor-eyes/vapor-eyes-$n.svg
    done
    python3 -c "
    from PIL import Image, ImageChops
    import glob
    for p in glob.glob('resources/images/diagrams/vapor-eyes/vapor-eyes-*.png'):
        img = Image.open(p).convert('RGB')
        bbox = ImageChops.difference(img, Image.new('RGB', img.size, (255,255,255))).getbbox()
        if bbox: img.crop(bbox).save(p)
    "
"""
import importlib.util
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
C_INK, C_MUTED, C_MUTED_3 = eo.C_INK, eo.C_MUTED, eo.C_MUTED_3
CANVAS_W, CANVAS_H, PAD = eo.CANVAS_W, eo.CANVAS_H, eo.PAD
HEADER_H, STAGE_TOP_GAP, STAGE_H = eo.HEADER_H, eo.STAGE_TOP_GAP, eo.STAGE_H
ARROW_W, FOOTER_H = eo.ARROW_W, eo.FOOTER_H

# vapor-eyes series accent (emissions amber) + tint.
ACCENT = "#DD6B20"
TINT = "#FBEEE2"
N_NOTEBOOKS = 5


def render_header_generic(badge_label, title, subtitle, series_text, accent):
    out = []
    bsize = 60
    bx, by = PAD, PAD + 4
    out.append(
        f'<rect x="{bx}" y="{by}" rx="14" ry="14" width="{bsize}" height="{bsize}" '
        f'fill="{accent}"/>'
        f'<text x="{bx + bsize/2}" y="{by + bsize/2 + 9}" text-anchor="middle" '
        f'font-family="Inter, -apple-system, system-ui, sans-serif" '
        f'font-size="24" font-weight="900" fill="#FFFFFF">{esc(badge_label)}</text>'
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


def render_footer_generic(chips, accent, tint, note, label="KEY FUNCTIONS"):
    out = []
    fy = CANVAS_H - PAD - FOOTER_H
    out.append(text(PAD, fy + 16, label, size=10, weight=700, fill=C_MUTED_3,
                    letter_spacing="1.6"))
    cx, cy = PAD + 130, fy + 6
    for c in chips:
        chip_svg, cw = chip(cx, cy, c, fg=accent, bg=tint, mono_font=True, h=24)
        out.append(chip_svg)
        cx += cw + 8
    out.append(text(CANVAS_W - PAD, fy + 16, note, size=11, fill=C_MUTED_3,
                    anchor="end"))
    return "".join(out)


def render_diagram(badge, title, subtitle, series, accent, tint, stages, chips, note):
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {CANVAS_W} {CANVAS_H}" '
        f'width="{CANVAS_W}" height="{CANVAS_H}" '
        f'style="font-family: Inter, -apple-system, system-ui, sans-serif;">',
        '<defs>'
        '<filter id="card-shadow" x="-5%" y="-5%" width="110%" height="115%">'
        '<feDropShadow dx="0" dy="2" stdDeviation="6" flood-color="#0F1B2A" '
        'flood-opacity="0.08"/></filter>'
        '<linearGradient id="bg" x1="0" y1="0" x2="0" y2="1">'
        '<stop offset="0" stop-color="#FAFBFC"/>'
        '<stop offset="1" stop-color="#F1F4F8"/></linearGradient></defs>',
        f'<rect x="0" y="0" width="{CANVAS_W}" height="{CANVAS_H}" fill="url(#bg)"/>',
        render_header_generic(badge, title, subtitle, series, accent),
    ]
    stage_y = PAD + HEADER_H + STAGE_TOP_GAP
    inner_w = CANVAS_W - 2 * PAD
    n = len(stages)
    stage_w = (inner_w - (n - 1) * ARROW_W) // n
    cur_x = PAD
    for i, stg in enumerate(stages):
        parts.append(render_stage(cur_x, stage_y, stage_w, stg, accent, tint))
        cur_x += stage_w
        if i < n - 1:
            parts.append(arrow(cur_x + 8, stage_y + STAGE_H / 2 - 30,
                               cur_x + ARROW_W - 8, color=accent))
            cur_x += ARROW_W
    parts.append(render_footer_generic(chips, accent, tint, note))
    parts.append("</svg>")
    return "\n".join(parts)


NOTEBOOKS = {
    1: dict(
        title="Wide-area screening",
        subtitle="Sentinel-5P TROPOMI methane → per-pixel points → H3 hotspot cells",
        stages=[
            Stage(title="Sentinel-5P swath",
                  subtitle="TropomiDownloader stages daily L2 CH4 granules for the AOI from Planetary Computer",
                  chip_text="TropomiDownloader"),
            Stage(title="Per-pixel points",
                  subtitle="netcdf_gbx vector mode reads the netCDF-4 swath as one point per ground pixel — no regridding",
                  chip_text="netcdf_gbx · vector"),
            Stage(title="Quality + H3 bin",
                  subtitle="Filter on qa_value, then bin column enhancements to H3 res-6 and aggregate per cell",
                  chip_text="h3_longlatash3"),
            Stage(title="Hotspot map",
                  subtitle="Per-cell CH4 mean/max surfaces candidate super-emitter cells on a basemap",
                  chip_text="cells_as_gdf"),
        ],
        chips=["TropomiDownloader", "netcdf_gbx", "st_geomfromwkb", "h3_longlatash3",
               "h3_centeraswkb"],
        note="databrickslabs/geobrix  ·  Sentinel-5P L2 CH4  ·  Planetary Computer",
    ),
}


def main():
    out_dir = os.path.join(_HERE, "..", "diagrams", "vapor-eyes")
    os.makedirs(out_dir, exist_ok=True)
    for num, nb in NOTEBOOKS.items():
        svg = render_diagram(
            f"{num:02d}", nb["title"], nb["subtitle"],
            f"vapor-eyes  ·  Notebook {num} of {N_NOTEBOOKS}",
            ACCENT, TINT, nb["stages"], nb["chips"], nb["note"],
        )
        path = os.path.join(out_dir, f"vapor-eyes-{num:02d}.svg")
        with open(path, "w") as f:
            f.write(svg)
        print(f"wrote {path}")


if __name__ == "__main__":
    main()
