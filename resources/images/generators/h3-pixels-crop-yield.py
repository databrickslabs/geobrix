"""Generate the per-notebook diagrams of the h3-pixels-crop-yield series (one SVG each).

Reuses vapor-eyes.py's layout (render_diagram, THEMES) and the eo-series / helios glyphs. Then PNGs with headless
Chrome and a whitespace crop, as in vapor-eyes.py's docstring:

    python3 resources/images/generators/h3-pixels-crop-yield.py
    for n in 01 02 03 04 05 06; do
      "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" --headless --disable-gpu --hide-scrollbars \
        --force-device-scale-factor=2 --window-size=1500,820 \
        --screenshot=resources/images/diagrams/h3-pixels-crop-yield/h3-pixels-crop-yield-$n.png \
        resources/images/diagrams/h3-pixels-crop-yield/h3-pixels-crop-yield-$n.svg
    done
    python3 -c "
    from PIL import Image, ImageChops
    import glob
    for p in glob.glob('resources/images/diagrams/h3-pixels-crop-yield/h3-pixels-crop-yield-*.png'):
        img = Image.open(p).convert('RGB')
        bbox = ImageChops.difference(img, Image.new('RGB', img.size, (255,255,255))).getbbox()
        if bbox: img.crop(bbox).save(p)
    "
"""
import importlib.util
import os

_HERE = os.path.dirname(os.path.abspath(__file__))


def _load(name, file):
    spec = importlib.util.spec_from_file_location(name, os.path.join(_HERE, file))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


ve = _load("vapor_eyes", "vapor-eyes.py")
eo, hel = ve.eo, ve.hel
Stage = ve.Stage
N = 6
SERIES = "h3-pixels-crop-yield"

# vapor-eyes defines five accents; the sixth notebook gets a green one.
THEMES = dict(ve.THEMES)
THEMES[6] = {"accent": "#2E7D4F", "tint": "#DDEEE3"}


def g_table(cx, cy, color, tint, *, label):
    """Table icon sized to its real table name (eo.g_delta_table fits only short labels)."""
    w, h = max(110, int(len(label) * 6.9) + 26), 80
    x, y = cx - w / 2, cy - h / 2
    out = [f'<rect x="{x}" y="{y}" rx="8" ry="8" width="{w}" height="{h}" fill="{tint}" stroke="{color}" stroke-width="2"/>',
           f'<rect x="{x}" y="{y}" width="{w}" height="22" rx="8" ry="8" fill="{color}"/>',
           f'<rect x="{x}" y="{y + 12}" width="{w}" height="10" fill="{color}"/>',
           f'<text x="{cx}" y="{y + 16}" text-anchor="middle" font-family="ui-monospace, Menlo, monospace" '
           f'font-size="11" font-weight="700" fill="#FFFFFF">{label}</text>']
    for i in range(3):
        ry = y + 30 + i * 14
        out.append(f'<line x1="{x + 10}" y1="{ry}" x2="{x + w - 10}" y2="{ry}" stroke="{color}" stroke-opacity="0.45" stroke-width="1.4"/>')
    return "".join(out)


def g_folder(cx, cy, color, tint, *, label, exts):
    """Folder of files, like helios.g_cog_catalog but with its own label and file extensions."""
    fw, fh = 130, 90
    fx, fy, tab_w = cx - fw / 2, cy - fh / 2 + 4, 52
    out = [f'<path d="M {fx} {fy} H {fx + tab_w} L {fx + tab_w + 10} {fy - 14} H {fx + fw} V {fy + fh} '
           f'Q {fx + fw} {fy + fh + 8} {fx + fw - 8} {fy + fh + 8} H {fx + 8} Q {fx} {fy + fh + 8} {fx} {fy + fh} Z" '
           f'fill="{tint}" stroke="{color}" stroke-width="2" stroke-linejoin="round"/>']
    for dx, ext in zip((-36, -6, 24), exts):
        ex, ey, ew, eh, ear = cx + dx, fy + 32, 24, 30, 7
        out.append(f'<path d="M {ex} {ey} H {ex + ew - ear} L {ex + ew} {ey + ear} V {ey + eh} H {ex} Z" fill="#FFFFFF" stroke="{color}" stroke-width="1.4"/>'
                   f'<path d="M {ex + ew - ear} {ey} V {ey + ear} H {ex + ew}" fill="none" stroke="{color}" stroke-width="1.2"/>'
                   f'<text x="{ex + ew / 2}" y="{ey + eh - 8}" text-anchor="middle" font-family="ui-monospace, Menlo, monospace" '
                   f'font-size="8" font-weight="700" fill="{color}">{ext}</text>')
    out.append(f'<text x="{fx + 12}" y="{fy + 18}" font-family="Inter, sans-serif" font-size="9" font-weight="700" fill="{color}">{label}</text>')
    return "".join(out)


def _table(label):
    return lambda cx, cy, c, t: g_table(cx, cy, c, t, label=label)


def _folder(label, exts):
    return lambda cx, cy, c, t: g_folder(cx, cy, c, t, label=label, exts=exts)


NOTEBOOKS = {
    1: dict(title="Download", subtitle="Six public sources for the AOI and seasons, staged once to a Volume",
            stages=[Stage(title="Public sources", subtitle="Stage IV, normals, gridMET, GRACE-DA, HLS, CSB", chip_text="Downloader.download()", glyph=_folder("HTTPS files", (".nc", ".tif", ".zip"))),
                    Stage(title="Validate files", subtitle="Each file is read-checked; valid files are kept on a re-run", chip_text="Downloader.repair()", glyph=eo.g_clip_with_buffer),
                    Stage(title="Record the files", subtitle="One row per file with its path, size and validity", chip_text="downloads", glyph=_table("downloads"))],
            chips=["StageIvDownloader", "GridmetDownloader", "NclimgridDownloader", "GraceDaDownloader", "HlsDownloader", "CsbDownloader"],
            note="databrickslabs/geobrix  ·  public datasets  ·  Python level"),
    2: dict(title="Fit a resolution", subtitle="Each dataset's pixel at the AOI → the coarsest H3 cell no larger than it",
            stages=[Stage(title="One tile per dataset", subtitle="Clipped to the AOI so the centre is at the AOI latitude", chip_text="gbx_rst_clip", glyph=eo.g_clip_with_buffer),
                    Stage(title="Centre pixel", subtitle="Geodesic area of the pixel at the tile centre", chip_text="gbx_rst_h3_fitresolution", glyph=eo.g_single_hex),
                    Stage(title="Record resolutions", subtitle="Res 7, 7, 6, 12 beside the pixel and cell areas", chip_text="dataset_resolutions", glyph=_table("dataset_resolutions"))],
            chips=["gbx_rst_clip", "gbx_rst_h3_fitresolution", "h3_pointash3", "h3_boundaryaswkb", "st_area"],
            note="databrickslabs/geobrix  ·  H3 average cell areas  ·  Python level"),
    3: dict(title="Datasets to H3", subtitle="One covering pass per dataset at its fitted resolution, then months",
            stages=[Stage(title="Source rasters", subtitle="NetCDF and COG tiles, cut to the area and retiled; Stage IV from file bytes", chip_text="netcdf_gbx · gbx_rst_fromfile", glyph=_folder("rasters", (".nc", ".tif", ".nc"))),
                    Stage(title="Covering", subtitle="Each pixel shared over the cells it overlaps, by area", chip_text="gbx_rst_h3_rastertogrid_partial", glyph=eo.g_dense_hex_grid),
                    Stage(title="Months and NDVI", subtitle="Daily sums and means per month; NDVI per August scene", chip_text="gbx_rst_mapalgebra", glyph=_table("h3_dataset_measures"))],
            chips=["gbx_rst_fromcontent", "gbx_rst_getsubdataset", "gbx_rst_retile", "gbx_rst_h3_rastertogrid_partial", "gbx_rst_mapalgebra"],
            note="databrickslabs/geobrix  ·  covering assignment  ·  Python level"),
    4: dict(title="Field polys to cells", subtitle="CSB corn polys to res-13 cells with edge and centroid distances",
            stages=[Stage(title="Load corn polys", subtitle="Corn per season, nearest the Story County centre; kept as field_polys", chip_text="gpkg_gbx", glyph=eo.g_polygon_ak),
                    Stage(title="Res-13 fill", subtitle="Cells whose centre is in the poly", chip_text="h3_polyfillash3", glyph=eo.g_dense_hex_grid),
                    Stage(title="Add distances", subtitle="Distance to the edge and to the centroid, in metres", chip_text="st_distance · st_boundary", glyph=_table("h3_field_cells"))],
            chips=["h3_polyfillash3", "h3_centeraswkb", "st_transform", "st_boundary", "st_distance"],
            note="databrickslabs/geobrix  ·  USDA CSB  ·  built-in H3 and ST"),
    5: dict(title="Field measures", subtitle="Link once, then raw, weighted and blended scores per field",
            stages=[Stage(title="Link cells", subtitle="h3_field_cell_links: parent cell, or exact overlap for straddle cells (they cross a parent edge)", chip_text="h3_toparent · h3_kring", glyph=eo.g_hex_grid),
                    Stage(title="Join and weight", subtitle="Key join and setback; h3_field_measures_weighted is the overlap-weighted mean", chip_text="h3_field_measures_perc", glyph=_table("h3_field_measures_perc")),
                    Stage(title="Blend per field", subtitle="Plain and edge-weighted field scores; August NDVI mean", chip_text="h3_field_measures_blended", glyph=_table("h3_field_measures_blended"))],
            chips=["h3_toparent", "h3_kring", "st_contains", "st_intersection", "st_area"],
            note="databrickslabs/geobrix  ·  built-in H3 and ST"),
    6: dict(title="Centroid baseline", subtitle="The pixel under the centroid and the NDVI zonal mean, beside the H3 scores",
            stages=[Stage(title="Field centroids", subtitle="One point per field and season", chip_text="field_polys", glyph=hel.g_map_pin),
                    Stage(title="Sample and zonal mean", subtitle="Source pixel per date; all-touched NDVI mean per scene", chip_text="gbx_rst_sample · gbx_rst_clip · gbx_rst_avg", glyph=eo.g_clip_with_buffer),
                    Stage(title="Compare with H3", subtitle="Gaps per attribute, interior fields apart", chip_text="h3_field_measures_centroid", glyph=_table("h3_field_measures_centroid"))],
            chips=["gbx_rst_sample", "gbx_rst_clip", "gbx_rst_avg", "gbx_rst_fromfile"],
            note="databrickslabs/geobrix  ·  current method baseline  ·  Python level"),
}


def main():
    out_dir = os.path.join(_HERE, "..", "diagrams", SERIES)
    os.makedirs(out_dir, exist_ok=True)
    for num, nb in NOTEBOOKS.items():
        theme = THEMES[num]
        svg = ve.render_diagram(f"{num:02d}", nb["title"], nb["subtitle"], f"{SERIES}  ·  Notebook {num} of {N}",
                                theme["accent"], theme["tint"], nb["stages"], nb["chips"], nb["note"])
        path = os.path.join(out_dir, f"{SERIES}-{num:02d}.svg")
        with open(path, "w", encoding="utf-8") as f:
            f.write(svg)
        print(f"wrote {path}")


if __name__ == "__main__":
    main()
