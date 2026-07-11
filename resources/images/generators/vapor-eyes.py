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

# helios glyphs (COG catalog, map pin, tile pyramid, archives, globe) reused below
_HEL = os.path.join(_HERE, "helios.py")
_hspec = importlib.util.spec_from_file_location("helios_gen", _HEL)
hel = importlib.util.module_from_spec(_hspec)
_hspec.loader.exec_module(hel)


# --- methane-domain custom glyphs (eo/helios cover the rest) ------------------
def g_satellite(cx, cy, color, tint):
    """A satellite with solar panels over a ground swath cone (TROPOMI/EMIT)."""
    out = [
        f'<polygon points="{cx-8},{cy-8} {cx+8},{cy-8} {cx+54},{cy+62} '
        f'{cx-54},{cy+62}" fill="{tint}" stroke="{color}" stroke-width="1.6" '
        f'stroke-linejoin="round" fill-opacity="0.65"/>',
        f'<rect x="{cx-14}" y="{cy-48}" width="28" height="26" rx="4" '
        f'fill="{tint}" stroke="{color}" stroke-width="2.4"/>',
        f'<rect x="{cx-48}" y="{cy-44}" width="26" height="18" rx="2" '
        f'fill="#FFFFFF" stroke="{color}" stroke-width="1.8"/>',
        f'<rect x="{cx+22}" y="{cy-44}" width="26" height="18" rx="2" '
        f'fill="#FFFFFF" stroke="{color}" stroke-width="1.8"/>',
        f'<circle cx="{cx}" cy="{cy-35}" r="4" fill="{color}"/>',
    ]
    for dx in (-42, -35, 27, 34):
        out.append(
            f'<line x1="{cx+dx}" y1="{cy-44}" x2="{cx+dx}" y2="{cy-26}" '
            f'stroke="{color}" stroke-width="0.8" stroke-opacity="0.5"/>'
        )
    return "".join(out)


def g_pixel_points(cx, cy, color, tint):
    """A grid of per-pixel sample points with a hot cluster highlighted."""
    cols, rows, sp = 6, 5, 18
    x0 = cx - (cols - 1) * sp / 2
    y0 = cy - (rows - 1) * sp / 2
    hot = {(2, 2), (2, 3), (3, 3)}
    out = []
    for r in range(rows):
        for c in range(cols):
            x, y = x0 + c * sp, y0 + r * sp
            fill = color if (r, c) in hot else tint
            rad = 4.6 if (r, c) in hot else 3.2
            out.append(
                f'<circle cx="{x:.0f}" cy="{y:.0f}" r="{rad}" fill="{fill}" '
                f'stroke="{color}" stroke-width="1"/>'
            )
    return "".join(out)


def _well(x, y, color, tint, r=9):
    return (
        f'<circle cx="{x}" cy="{y}" r="{r}" fill="{tint}" stroke="{color}" '
        f'stroke-width="2"/>'
        f'<polygon points="{x-4},{y+3} {x+4},{y+3} {x},{y-6}" fill="{color}"/>'
    )


def g_well_pads(cx, cy, color, tint):
    """A scatter of well-pad markers (surface holes)."""
    return "".join(
        _well(cx + dx, cy + dy, color, tint)
        for dx, dy in [(-40, -20), (4, -32), (36, 8), (-16, 26), (22, -2)]
    )


def g_layers(cx, cy, color, tint):
    """Three stacked map layers (hotspots / plumes / wells) with a hex motif."""
    out = [
        f'<rect x="{cx-54+dx}" y="{cy-30+dy}" width="108" height="58" rx="9" '
        f'fill="{tint}" fill-opacity="{op}" stroke="{color}" stroke-width="2"/>'
        for dx, dy, op in [(18, 14, 0.5), (0, 0, 0.72), (-18, -14, 0.95)]
    ]
    hx, hy, R = cx - 18, cy - 14, 13
    pts = [(0, -1), (0.866, -0.5), (0.866, 0.5), (0, 1), (-0.866, 0.5), (-0.866, -0.5)]
    poly = " ".join(f"{hx+px*R:.1f},{hy+py*R:.1f}" for px, py in pts)
    out.append(
        f'<polygon points="{poly}" fill="#FFFFFF" stroke="{color}" stroke-width="1.8"/>'
    )
    return "".join(out)


def g_nearest_wells(cx, cy, color, tint):
    """A plume origin linked to its nearest candidate well pads."""
    wells = [(-42, 20), (32, 28), (46, -14)]
    out = [
        f'<line x1="{cx}" y1="{cy}" x2="{cx+dx}" y2="{cy+dy}" stroke="{color}" '
        f'stroke-width="1.4" stroke-dasharray="4 3" stroke-opacity="0.7"/>'
        for dx, dy in wells
    ]
    out += [_well(cx + dx, cy + dy, color, tint, r=8) for dx, dy in wells]
    out.append(f'<circle cx="{cx}" cy="{cy}" r="10" fill="{color}"/>')
    out.append(
        f'<circle cx="{cx}" cy="{cy}" r="16" fill="none" stroke="{color}" '
        f'stroke-width="1.5" stroke-opacity="0.5"/>'
    )
    return "".join(out)

# Per-notebook accent progression — the same sequence helios/eo-series use
# (blue -> orange -> teal -> violet), extended with a fifth (rose/crimson) for
# the NB05 portfolio synthesis. accent = stage stripe + header badge; tint =
# chip fill.
THEMES = {
    1: {"accent": "#1F6FB5", "tint": "#E3EEF8"},   # blue   — regional screen
    2: {"accent": "#E04E2A", "tint": "#FCE9E2"},   # orange — targeted detect
    3: {"accent": "#0F8E8B", "tint": "#D5ECEC"},   # teal   — quantify
    4: {"accent": "#6B4FA0", "tint": "#EDE8F5"},   # violet — attribution
    5: {"accent": "#C2255C", "tint": "#FADCE6"},   # rose   — portfolio synthesis
}
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
                  chip_text="TropomiDownloader", glyph=g_satellite),
            Stage(title="Per-pixel points",
                  subtitle="netcdf_gbx vector mode reads the netCDF-4 swath as one point per ground pixel — no regridding",
                  chip_text="netcdf_gbx · vector", glyph=g_pixel_points),
            Stage(title="Quality + H3 bin",
                  subtitle="Filter on qa_value, then bin column enhancements to H3 res-6 and aggregate per cell",
                  chip_text="h3_longlatash3", glyph=eo.g_hex_grid),
            Stage(title="Hotspot map",
                  subtitle="Per-cell CH4 mean/max surfaces candidate super-emitter cells on a basemap",
                  chip_text="cells_as_gdf", glyph=eo.g_dense_hex_grid),
        ],
        chips=["TropomiDownloader", "netcdf_gbx", "st_geomfromwkb", "h3_longlatash3",
               "h3_centeraswkb"],
        note="databrickslabs/geobrix  ·  Sentinel-5P L2 CH4  ·  Planetary Computer",
    ),
    2: dict(
        title="Targeted detection",
        subtitle="Sentinel-2 20 m SWIR (MBMP) at the flagged hotspot → H3 plume cells",
        stages=[
            Stage(title="Hotspot cell",
                  subtitle="Take the strongest s5p_hotspots cell (NB01) and use its footprint as the AOI",
                  chip_text="s5p_hotspots", glyph=eo.g_single_hex),
            Stage(title="Sentinel-2 SWIR",
                  subtitle="StacClient stages B11/B12 SWIR COGs windowed to the cell, from the least-cloudy item",
                  chip_text="StacClient · B11/B12",
                  glyph=lambda cx, cy, c, t: eo.g_band_fanout(cx, cy)),
            Stage(title="SWIR index",
                  subtitle="rst_mapalgebra computes (B11-B12)/(B11+B12) — high where B12 absorbs: a methane proxy",
                  chip_text="rst_mapalgebra", glyph=eo.g_multiband_tile),
            Stage(title="H3 plume cells",
                  subtitle="rst_h3_tessellate grids the index into fine H3 cells for a per-cell plume fraction",
                  chip_text="rst_h3_tessellate", glyph=eo.g_dense_hex_grid),
        ],
        chips=["StacClient", "gtiff_gbx", "rst_mapalgebra", "rst_h3_tessellate",
               "rst_summary"],
        note="databrickslabs/geobrix  ·  Sentinel-2 L2A  ·  Planetary Computer",
    ),
    3: dict(
        title="Quantification",
        subtitle="EMIT 60 m CH4 enhancement + plume complexes → clip, summarize, emission rate",
        stages=[
            Stage(title="EMIT products",
                  subtitle="EmitDownloader stages the CH4 enhancement COG + plume-complex GeoJSON for the AOI from NASA LP DAAC",
                  chip_text="EmitDownloader", glyph=hel.g_cog_catalog),
            Stage(title="Plume metadata",
                  subtitle="geojson_gbx reads each plume outline plus JPL's emission-rate + max-concentration estimate",
                  chip_text="geojson_gbx", glyph=eo.g_polygon_ak),
            Stage(title="Clip + summarize",
                  subtitle="rst_clip cuts the enhancement raster to each plume outline; rst_summary measures it",
                  chip_text="rst_clip · rst_summary", glyph=eo.g_clip_with_buffer),
            Stage(title="Emission rate",
                  subtitle="Per-plume kg/hr over a basemap; GeoBrix's clipped-raster max cross-checks JPL's reported peak",
                  chip_text="plume_quant",
                  glyph=lambda cx, cy, c, t: eo.g_delta_table(cx, cy, c, t, label="plume_quant")),
        ],
        chips=["EmitDownloader", "geojson_gbx", "raster_gbx", "rst_clip",
               "rst_summary"],
        note="databrickslabs/geobrix  ·  EMIT L2B CH4  ·  NASA LP DAAC",
    ),
    4: dict(
        title="Attribution",
        subtitle="EMIT plume origin → nearest candidate wells → operator shortlist",
        stages=[
            Stage(title="Plume origin",
                  subtitle="Each EMIT plume's max-concentration point (lon/lat) from NB03's emit_plumes is the origin to attribute",
                  chip_text="emit_plumes", glyph=hel.g_map_pin),
            Stage(title="TX RRC wells",
                  subtitle="WellsDownloader stages WellSHL surface-hole locations for the AOI; geojson_gbx reads operator + API + point",
                  chip_text="WellsDownloader", glyph=g_well_pads),
            Stage(title="Nearest wells",
                  subtitle="st_distancesphere over st_point / st_geomfromwkb ranks the K nearest permitted wells per plume",
                  chip_text="st_distancesphere", glyph=g_nearest_wells),
            Stage(title="Operator shortlist",
                  subtitle="Each plume tied to a ranked short-list of candidate operator well pads; wind transport narrows the emitter",
                  chip_text="plume_candidate_wells",
                  glyph=lambda cx, cy, c, t: eo.g_delta_table(cx, cy, c, t, label="candidates")),
        ],
        chips=["WellsDownloader", "geojson_gbx", "st_point", "st_geomfromwkb",
               "st_distancesphere"],
        note="databrickslabs/geobrix  ·  TX RRC WellSHL  ·  ArcGIS REST",
    ),
    5: dict(
        title="Synthesis",
        subtitle="Hotspots + plumes + wells → vector PMTiles → one shareable methane portfolio",
        stages=[
            Stage(title="Three layers",
                  subtitle="S5P hotspot hexagons (h3_boundaryaswkb), EMIT plume outlines and TX RRC wells from the cascade tables",
                  chip_text="cascade tables", glyph=g_layers),
            Stage(title="MVT pyramid",
                  subtitle="gbx_st_asmvt_pyramid encodes each layer into tile-local vector tiles across the zoom range",
                  chip_text="gbx_st_asmvt_pyramid", glyph=hel.g_xyz_pyramid),
            Stage(title="PMTiles archive",
                  subtitle="gbx_pmtiles_agg folds the whole pyramid into one PMTiles v3 archive, merging features per layer",
                  chip_text="gbx_pmtiles_agg", glyph=hel.g_stacked_archive),
            Stage(title="Shareable portfolio",
                  subtitle="One self-contained vapor_eyes.pmtiles — screen, detect, quantify, attribute — panned in-browser, no server",
                  chip_text="vapor_eyes.pmtiles", glyph=hel.g_webmercator_globe),
        ],
        chips=["h3_boundaryaswkb", "gbx_st_asmvt_pyramid", "gbx_pmtiles_agg",
               "show_pmtiles"],
        note="databrickslabs/geobrix  ·  vector PMTiles  ·  lightweight tier",
    ),
}


def main():
    out_dir = os.path.join(_HERE, "..", "diagrams", "vapor-eyes")
    os.makedirs(out_dir, exist_ok=True)
    for num, nb in NOTEBOOKS.items():
        theme = THEMES[num]
        svg = render_diagram(
            f"{num:02d}", nb["title"], nb["subtitle"],
            f"vapor-eyes  ·  Notebook {num} of {N_NOTEBOOKS}",
            theme["accent"], theme["tint"], nb["stages"], nb["chips"], nb["note"],
        )
        path = os.path.join(out_dir, f"vapor-eyes-{num:02d}.svg")
        with open(path, "w") as f:
            f.write(svg)
        print(f"wrote {path}")


if __name__ == "__main__":
    main()
