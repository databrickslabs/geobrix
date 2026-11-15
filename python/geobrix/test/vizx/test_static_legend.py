"""plot_static categorical legend layout: few entries inside, many entries outside."""

from test._volume_safe import open_raster

import matplotlib

matplotlib.use("Agg")

import geopandas as gpd  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import pytest  # noqa: E402
from shapely.geometry import box  # noqa: E402

from databricks.labs.gbx.vizx._layers import raster_layer, vector_layer  # noqa: E402
from databricks.labs.gbx.vizx._static_map import plot_static  # noqa: E402

_BOUNDS = (-77.9670, 43.2300, -77.9640, 43.2320)


@pytest.fixture(autouse=True)
def _close_figures():
    yield
    plt.close("all")


def _objects_gdf(n):
    # n small boxes in a 7-wide grid over _BOUNDS, one category per box -- the
    # shape of a GeoSAM instance-segmentation result ("object 0" .. "object n-1").
    x0, y0, x1, y1 = _BOUNDS
    cols = 7
    rows = (n + cols - 1) // cols
    dx, dy = (x1 - x0) / cols, (y1 - y0) / rows
    geoms = [
        box(
            x0 + (i % cols) * dx,
            y0 + (i // cols) * dy,
            x0 + (i % cols + 0.8) * dx,
            y0 + (i // cols + 0.8) * dy,
        )
        for i in range(n)
    ]
    return gpd.GeoDataFrame(
        {"object": [f"object {i}" for i in range(n)]}, geometry=geoms, crs=4326
    )


def _write_cog(tmp_path, size=64):
    import numpy as np
    from rasterio.transform import from_bounds

    path = tmp_path / "ortho.tif"
    data = np.random.default_rng(0).integers(30, 225, (3, size, size), dtype="uint8")
    with open_raster(
        path,
        "w",
        driver="GTiff",
        height=size,
        width=size,
        count=3,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_bounds(*_BOUNDS, size, size),
    ) as dst:
        dst.write(data)
    return str(path)


def _extents(ax):
    fig = ax.figure
    fig.canvas.draw()
    leg = ax.get_legend()
    assert leg is not None
    return leg, leg.get_window_extent(), ax.get_window_extent(), fig.bbox


def _inside(inner, outer, tol=1.0):
    return (
        inner.x0 >= outer.x0 - tol
        and inner.y0 >= outer.y0 - tol
        and inner.x1 <= outer.x1 + tol
        and inner.y1 <= outer.y1 + tol
    )


def test_49_categories_put_the_legend_beside_the_image_not_over_it(tmp_path):
    ax = plot_static(
        [
            raster_layer(_write_cog(tmp_path)),
            vector_layer(_objects_gdf(49), column="object", cmap="tab20", opacity=0.55),
        ],
        basemap=False,
        legend=True,
    )
    leg, leg_bb, ax_bb, fig_bb = _extents(ax)
    assert len(leg.get_texts()) == 49
    img_bb = ax.get_images()[0].get_window_extent()
    assert not leg_bb.overlaps(img_bb), f"legend {leg_bb} covers the image {img_bb}"
    assert not leg_bb.overlaps(ax_bb), f"legend {leg_bb} overlaps the axes {ax_bb}"
    assert leg_bb.x0 >= ax_bb.x1  # to the right of the map
    # The figure was widened so nothing is clipped, even without a tight bbox.
    assert _inside(leg_bb, fig_bb), f"legend {leg_bb} clipped by figure {fig_bb}"
    # Several columns, so it fits the map height rather than running off the bottom.
    assert leg._ncols > 1
    assert leg_bb.height <= ax_bb.height + 1.0


def test_49_categories_on_the_single_data_path_also_sit_outside():
    ax = plot_static(_objects_gdf(49), column="object", cmap="tab20", basemap=False)
    leg, leg_bb, ax_bb, fig_bb = _extents(ax)
    assert not leg_bb.overlaps(ax_bb)
    assert _inside(leg_bb, fig_bb)


def test_a_many_entry_legend_uses_a_smaller_font_than_a_few_entry_one():
    few = plot_static(_objects_gdf(3), column="object", basemap=False).get_legend()
    many = plot_static(_objects_gdf(49), column="object", basemap=False).get_legend()
    assert many.get_texts()[0].get_fontsize() < few.get_texts()[0].get_fontsize()


def test_3_categories_keep_the_legend_inside_the_axes(tmp_path):
    ax = plot_static(
        [
            raster_layer(_write_cog(tmp_path)),
            vector_layer(_objects_gdf(3), column="object", cmap="tab20"),
        ],
        basemap=False,
        legend=True,
    )
    fig_w_before = 10  # plot_static's default fig_w
    leg, leg_bb, ax_bb, _ = _extents(ax)
    assert len(leg.get_texts()) == 3
    assert _inside(leg_bb, ax_bb), f"legend {leg_bb} left the axes {ax_bb}"
    assert ax.figure.get_size_inches()[0] == pytest.approx(fig_w_before)


def test_a_caller_owned_axes_gets_the_legend_outside_but_keeps_its_figure_size():
    fig, ax = plt.subplots(figsize=(6, 6))
    plot_static(_objects_gdf(49), column="object", basemap=False, ax=ax)
    leg, leg_bb, ax_bb, _ = _extents(ax)
    assert not leg_bb.overlaps(ax_bb)
    assert tuple(fig.get_size_inches()) == (6, 6)


def test_a_palette_smaller_than_the_categories_is_called_out(capsys):
    # tab20 has 20 colours: 49 categories reuse them, so the legend shows the same
    # swatch for different labels. Say so rather than imply each is distinct.
    plot_static(_objects_gdf(49), column="object", cmap="tab20", basemap=False)
    out = capsys.readouterr().out
    assert "49 categories" in out and "20 colours" in out


def test_no_palette_note_when_the_palette_covers_the_categories(capsys):
    plot_static(_objects_gdf(3), column="object", cmap="tab20", basemap=False)
    plot_static(_objects_gdf(49), column="object", cmap="viridis", basemap=False)
    assert "colours" not in capsys.readouterr().out


def test_the_palette_note_respects_debug_mode_0(capsys):
    plot_static(
        _objects_gdf(49), column="object", cmap="tab20", basemap=False, debug_mode=0
    )
    assert "colours" not in capsys.readouterr().out


def test_a_map_filling_the_figure_width_widens_the_figure_and_keeps_the_map_size():
    # A wide extent fills the subplot's width, so the outside legend needs new room.
    gdf = gpd.GeoDataFrame(
        {"object": [f"object {i}" for i in range(49)]},
        geometry=[box(i, 0.0, i + 0.8, 0.5) for i in range(49)],
        crs=4326,
    )
    fig_before, ax_before = plt.subplots(figsize=(10, 10))
    plot_static(gdf, column="object", basemap=False, legend=False, ax=ax_before)
    fig_before.canvas.draw()
    map_w = ax_before.get_window_extent().width

    ax = plot_static(gdf, column="object", basemap=False)
    leg, leg_bb, ax_bb, fig_bb = _extents(ax)
    assert ax.figure.get_size_inches()[0] > 10
    assert not leg_bb.overlaps(ax_bb)
    assert _inside(leg_bb, fig_bb)
    assert ax_bb.width == pytest.approx(map_w, rel=0.01)
