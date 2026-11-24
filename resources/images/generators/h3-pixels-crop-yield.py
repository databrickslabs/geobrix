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


def _table(label):
    return lambda cx, cy, c, t: eo.g_delta_table(cx, cy, c, t, label=label)


NOTEBOOKS = {
    1: dict(title="Download", subtitle="Six public sources for the AOI and seasons, staged once to a Volume",
            stages=[Stage(title="Public sources", subtitle="Stage IV, normals, gridMET, GRACE-DA, HLS, CSB", chip_text="sample downloaders", glyph=hel.g_cog_catalog),
                    Stage(title="Validated files", subtitle="Each file read-checked; valid files kept on a re-run", chip_text="is_out_file_valid", glyph=eo.g_clip_with_buffer),
                    Stage(title="downloads", subtitle="One row per file with its path, size and validity", chip_text="downloads", glyph=_table("downloads"))],
            chips=["StageIvDownloader", "GridmetDownloader", "NclimgridDownloader", "GraceDaDownloader", "HlsDownloader", "CsbDownloader"],
            note="databrickslabs/geobrix  ·  public datasets  ·  Python level"),
    2: dict(title="Fit a resolution", subtitle="Each dataset's pixel at the AOI -> the coarsest H3 cell no larger than it",
            stages=[Stage(title="One tile per dataset", subtitle="Clipped to the AOI so the centre is at the AOI latitude", chip_text="rst_clip", glyph=eo.g_clip_with_buffer),
                    Stage(title="Centre pixel", subtitle="Geodesic area of the pixel at the tile centre", chip_text="rst_h3_fitresolution", glyph=eo.g_single_hex),
                    Stage(title="dataset_resolutions", subtitle="Res 7, 7, 6, 12 beside the pixel and cell areas", chip_text="dataset_resolutions", glyph=_table("resolutions"))],
            chips=["gbx_rst_h3_fitresolution", "h3_pointash3", "h3_boundaryaswkb", "st_area"],
            note="databrickslabs/geobrix  ·  H3 average cell areas  ·  Python level"),
    3: dict(title="Datasets to H3", subtitle="One covering pass per dataset at its fitted resolution, then months",
            stages=[Stage(title="Source rasters", subtitle="NetCDF and COG tiles, cut to the area and retiled", chip_text="netcdf_gbx · gbx_rst_fromfile", glyph=hel.g_cog_catalog),
                    Stage(title="Covering", subtitle="Each pixel shared over the cells it overlaps, by area", chip_text="rst_h3_rastertogrid_partial", glyph=eo.g_dense_hex_grid),
                    Stage(title="Months and NDVI", subtitle="Daily sums and means per month; NDVI per August scene", chip_text="h3_dataset_measures", glyph=_table("measures"))],
            chips=["gbx_rst_retile", "gbx_rst_h3_rastertogrid_partial", "gbx_rst_mapalgebra", "netcdf_gbx"],
            note="databrickslabs/geobrix  ·  covering assignment  ·  Python level"),
    4: dict(title="Field polys to cells", subtitle="CSB corn polys -> res-13 cells with edge and centroid distances",
            stages=[Stage(title="CSB corn polys", subtitle="Corn per season, nearest the Story County centre", chip_text="gpkg_gbx", glyph=eo.g_polygon_ak),
                    Stage(title="Res-13 fill", subtitle="Cells whose centre is in the poly", chip_text="h3_polyfillash3", glyph=eo.g_dense_hex_grid),
                    Stage(title="h3_field_cells", subtitle="Distance to the edge and to the centroid, in metres", chip_text="st_distance · st_boundary", glyph=_table("field cells"))],
            chips=["h3_polyfillash3", "h3_centeraswkb", "st_transform", "st_boundary", "st_distance"],
            note="databrickslabs/geobrix  ·  USDA CSB  ·  built-in H3 and ST"),
    5: dict(title="Field measures", subtitle="Link once, then raw, weighted and blended scores per field",
            stages=[Stage(title="Links", subtitle="Parent cell, or exact overlap for straddlers", chip_text="h3_toparent · h3_kring", glyph=eo.g_hex_grid),
                    Stage(title="Raw and weighted", subtitle="Key join, setback, overlap-weighted mean", chip_text="h3_field_measures_perc", glyph=_table("perc")),
                    Stage(title="Blended", subtitle="Plain and edge-weighted field scores; August NDVI mean", chip_text="h3_field_measures_blended", glyph=_table("blended"))],
            chips=["h3_toparent", "h3_kring", "st_contains", "st_intersection", "st_area"],
            note="databrickslabs/geobrix  ·  built-in H3 and ST"),
    6: dict(title="Centroid baseline", subtitle="The pixel under the centroid and the NDVI zonal mean, beside the H3 scores",
            stages=[Stage(title="Field centroids", subtitle="One point per field and season", chip_text="field_polys", glyph=hel.g_map_pin),
                    Stage(title="Sample and zonal mean", subtitle="Source pixel per date; all-touched NDVI mean per scene", chip_text="rst_sample · rst_clip · rst_avg", glyph=eo.g_clip_with_buffer),
                    Stage(title="Comparison", subtitle="Gaps per attribute, interior fields apart", chip_text="h3_field_measures_centroid", glyph=_table("compare"))],
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
        with open(path, "w") as f:
            f.write(svg)
        print(f"wrote {path}")


if __name__ == "__main__":
    main()
