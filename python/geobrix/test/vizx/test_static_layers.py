"""Task 3: plot_static multi-layer compositor tests."""

import matplotlib

matplotlib.use("Agg")

import warnings  # noqa: E402

import geopandas as gpd  # noqa: E402
import pytest  # noqa: E402
from shapely.geometry import Point, Polygon  # noqa: E402

from databricks.labs.gbx.vizx._layers import (  # noqa: E402
    pmtiles_layer,
    raster_layer,
    vector_layer,
)
from databricks.labs.gbx.vizx._static_map import plot_static  # noqa: E402


def _gdf(geoms):
    return gpd.GeoDataFrame({"v": range(len(geoms))}, geometry=geoms, crs="EPSG:4326")


def _write_cog_4326(
    tmp_path, bands=3, size=64, bounds=(-77.9670, 43.2300, -77.9640, 43.2320)
):
    # A small geographic (EPSG:4326) COG, degree-scale bounds -- the CRS class
    # that reproduces the raster/vector misalignment bug (composite renders
    # blank because the raster stays in native degrees while the vector branch
    # reprojects to Web Mercator).
    import numpy as np
    import rasterio
    from rasterio.transform import from_bounds

    path = tmp_path / "ortho_4326.tif"
    rng = np.random.default_rng(0)
    data = rng.integers(30, 225, size=(bands, size, size), dtype="uint8")
    transform = from_bounds(*bounds, size, size)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=size,
        width=size,
        count=bands,
        dtype="uint8",
        crs="EPSG:4326",
        transform=transform,
    ) as dst:
        dst.write(data)
    return str(path), bounds


def test_two_vector_layers_one_axes():
    pts = _gdf([Point(-122.4, 37.7), Point(-122.41, 37.72)])
    polys = _gdf(
        [Polygon([(-122.5, 37.7), (-122.4, 37.7), (-122.4, 37.8), (-122.5, 37.8)])]
    )
    ax = plot_static(
        [vector_layer(polys, column="v"), vector_layer(pts, color="red")],
        basemap=False,
    )
    # both layers drew: at least one collection from polys + one from pts
    assert len(ax.collections) >= 2


def test_legacy_single_dataframe_call_still_works():
    pts = _gdf([Point(-122.4, 37.7)])
    ax = plot_static(pts, column="v", basemap=False)
    assert ax is not None


def test_plot_static_empty_list_raises():
    with pytest.raises(ValueError):
        plot_static([])


def test_plot_static_pmtiles_layer_warns():
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        plot_static([pmtiles_layer(b"PMTiles\x03")], basemap=False)
    assert any("pmtiles" in str(x.message).lower() for x in w)


# --- raster + vector composite alignment (regression: blank 4326-raster composite) ---


def test_raster_and_vector_layers_share_mercator_scale(tmp_path):
    # A composite raster layer (georeferenced COG path/dataset) must warp to
    # the SAME 3857 the vector branch already reprojects to. Checking the axes
    # xlim alone is not sufficient (matplotlib autoscales to the union of both
    # ranges, so a huge vector-only range can dominate the view even when the
    # raster stays mis-scaled) -- assert the RASTER'S OWN rendered extent is on
    # the mercator scale and overlaps the vector's reprojected bounds.
    from shapely.geometry import box

    cog_path, (minx, miny, maxx, maxy) = _write_cog_4326(tmp_path)
    poly = box(minx + 0.0005, miny + 0.0005, minx + 0.0015, miny + 0.0015)
    gdf = _gdf([poly])
    v_minx, v_miny, v_maxx, v_maxy = gdf.to_crs(3857).total_bounds

    ax = plot_static(
        [raster_layer(cog_path), vector_layer(gdf, fill=False, color="red", width=1.5)],
        basemap=False,
    )
    images = ax.get_images()
    assert images, "raster layer did not render an image"
    r_left, r_right, r_bottom, r_top = images[0].get_extent()
    assert (
        max(abs(r_left), abs(r_right)) > 1e6
    ), f"raster stayed at native (degree) scale: extent={images[0].get_extent()}"
    # Same coordinate space -> extents overlap (raster contains the vector box).
    assert r_left <= v_maxx and r_right >= v_minx
    assert min(r_bottom, r_top) <= v_maxy and max(r_bottom, r_top) >= v_miny


def test_raster_and_vector_composite_is_not_blank(tmp_path):
    # End-to-end non-blank check: render to a PNG and assert the grayscale std
    # is well above "blank" -- the pre-fix code (raster drawn in native 4326
    # degrees, vector in 3857 meters) collapses both to sub-pixel specks.
    import numpy as np
    from PIL import Image
    from shapely.geometry import box

    cog_path, (minx, miny, maxx, maxy) = _write_cog_4326(tmp_path)
    poly = box(minx + 0.0005, miny + 0.0005, minx + 0.0015, miny + 0.0015)
    gdf = _gdf([poly])

    ax = plot_static(
        [raster_layer(cog_path), vector_layer(gdf, fill=False, color="red", width=1.5)],
        basemap=False,
    )
    png_path = tmp_path / "composite.png"
    ax.figure.savefig(png_path, dpi=55)
    std = float(np.asarray(Image.open(png_path).convert("L")).std())
    assert (
        std > 40
    ), f"composite is near-blank (std={std}); raster/vector CRS misaligned"


def test_vector_layer_category_colors_maps_each_class():
    # An explicit category->color map colors each category by its mapped color
    # (not a colormap's arbitrary/alphabetical order), so a land-cover legend can
    # read vegetation=green / impervious=gray instead of tab10's backwards order.
    from matplotlib.colors import to_rgb
    from shapely.geometry import box

    gdf = gpd.GeoDataFrame(
        {"class": ["vegetation", "impervious"]},
        geometry=[box(-122.5, 37.7, -122.4, 37.8), box(-122.4, 37.7, -122.3, 37.8)],
        crs="EPSG:4326",
    )
    cc = {
        "vegetation": "#2ca02c",
        "bare": "#c2a878",
        "impervious": "#808080",
        "dark": "#2c2c54",
    }
    ax = plot_static(
        [vector_layer(gdf, column="class", category_colors=cc, opacity=1.0)],
        basemap=False,
        legend=True,
    )
    # The drawn polygon collection carries one facecolor per row, matching the map.
    fcs = ax.collections[-1].get_facecolors()
    assert len(fcs) == 2
    got = {tuple(round(x, 3) for x in fc[:3]) for fc in fcs}
    want = {
        tuple(round(x, 3) for x in to_rgb(cc[c])) for c in ("vegetation", "impervious")
    }
    assert got == want, f"facecolors {got} != mapped {want}"
    # A discrete legend labels only the categories present, colored by the map.
    leg = ax.get_legend()
    assert leg is not None
    assert {t.get_text() for t in leg.get_texts()} == {"vegetation", "impervious"}


def test_vector_layer_category_colors_legend_order_follows_map():
    # Legend order follows the category_colors map insertion order (filtered to
    # present categories), not the data/alphabetical order -- so a land-cover
    # legend reads vegetation, bare, impervious, dark as authored.
    from shapely.geometry import box

    gdf = gpd.GeoDataFrame(
        {"class": ["impervious", "vegetation", "bare"]},
        geometry=[
            box(-122.5, 37.7, -122.4, 37.8),
            box(-122.4, 37.7, -122.3, 37.8),
            box(-122.3, 37.7, -122.2, 37.8),
        ],
        crs="EPSG:4326",
    )
    cc = {
        "vegetation": "#2ca02c",
        "bare": "#c2a878",
        "impervious": "#808080",
        "dark": "#2c2c54",
    }
    ax = plot_static(
        [vector_layer(gdf, column="class", category_colors=cc, opacity=1.0)],
        basemap=False,
    )
    labels = [t.get_text() for t in ax.get_legend().get_texts()]
    assert labels == ["vegetation", "bare", "impervious"]  # map order, "dark" absent


def test_vector_layer_renders_above_raster(tmp_path):
    # plot_cog draws a raster at zorder=2 (so it sits above a basemap at zorder=1); a
    # vector layer must composite ABOVE the raster or the raster occludes it entirely
    # and the composite renders raster-only. Assert the vector's colour actually reaches
    # the output (pre-fix, with the vector at geopandas' lower default zorder, this is 0).
    import numpy as np
    from PIL import Image
    from shapely.geometry import box

    cog_path, (minx, miny, maxx, maxy) = _write_cog_4326(tmp_path)
    poly = box(minx + 0.0008, miny + 0.0008, maxx - 0.0008, maxy - 0.0008)
    gdf = _gdf([poly])

    ax = plot_static(
        [
            raster_layer(cog_path),
            vector_layer(gdf, color="red", opacity=1.0, width=2.0),
        ],
        basemap=False,
    )
    png_path = tmp_path / "occlusion.png"
    ax.figure.savefig(png_path, dpi=60)
    a = np.asarray(Image.open(png_path).convert("RGB")).astype(int)
    red = int(((a[:, :, 0] > 150) & (a[:, :, 1] < 90) & (a[:, :, 2] < 90)).sum())
    assert red > 50, f"vector occluded by raster (red px={red}); must draw above raster"
