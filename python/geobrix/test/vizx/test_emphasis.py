"""Tests for the vizx ``emphasis`` styling mode (data vs blend).

``emphasis="data"`` makes a newly-added layer pop against the basemap; ``emphasis="blend"``
(the default) is the soft composite. Explicit user styling kwargs always win.
"""

import builtins
import inspect
import json
import re

import geopandas as gpd
import matplotlib
import pytest

matplotlib.use("Agg")  # headless
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
from shapely.geometry import LineString, Point, Polygon  # noqa: E402

import databricks.labs.gbx.vizx._interactive as itx  # noqa: E402
from databricks.labs.gbx.vizx import (  # noqa: E402
    plot_cog,
    plot_interactive,
    plot_pmtiles,
    plot_raster,
    plot_static,
)
from databricks.labs.gbx.vizx._layers import raster_layer, vector_layer  # noqa: E402
from databricks.labs.gbx.vizx._maplibre import (  # noqa: E402
    build_html,
    layer_to_sources_layers,
)


@pytest.fixture(autouse=True)
def _close_figures():
    plt.close("all")
    yield
    plt.close("all")


def _gdf(geom):
    return gpd.GeoDataFrame({"v": [1]}, geometry=[geom], crs=4326)


def _gdf_polygon():
    return _gdf(Polygon([(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]))


def _last(ax):
    return ax.collections[-1]


def test_emphasis_defaults_to_blend_and_debug_mode_to_1_on_the_entrypoints():
    for fn in (plot_static, plot_cog, plot_raster, plot_interactive, plot_pmtiles):
        assert inspect.signature(fn).parameters["emphasis"].default == "blend"
    for fn in (plot_static, plot_cog, plot_raster):
        assert inspect.signature(fn).parameters["debug_mode"].default == 1


def test_static_polygons_data_pops_blend_is_soft_and_explicit_kwargs_win():
    coll = _last(plot_static(_gdf_polygon(), basemap=False, emphasis="data"))
    edge = coll.get_edgecolor()
    assert edge.size > 0 and float(edge[0][3]) > 0.0 and float(edge[0][0]) < 0.3
    assert coll.get_alpha() is not None and coll.get_alpha() > 0.7
    plt.close("all")

    coll = _last(plot_static(_gdf_polygon(), basemap=False, emphasis="blend"))
    assert coll.get_alpha() == 0.8
    assert np.allclose(coll.get_edgecolor(), coll.get_facecolor())  # edgecolor="face"
    plt.close("all")

    ax = plot_static(
        _gdf_polygon(), basemap=False, emphasis="data", edgecolor="red", alpha=0.3
    )
    coll = _last(ax)
    assert (
        float(coll.get_edgecolor()[0][0]) > 0.8
        and float(coll.get_edgecolor()[0][1]) < 0.2
    )
    assert coll.get_alpha() == 0.3
    plt.close("all")

    # emphasis only sets edge/alpha defaults: a column still drives the colouring
    assert plot_static(_gdf_polygon(), column="v", basemap=False, emphasis="data")


def test_static_points_and_lines_get_a_dark_ring_and_a_wider_line_unless_overridden():
    pts = _gdf(Point(0, 0))
    coll = _last(plot_static(pts, basemap=False, emphasis="data"))
    edge = coll.get_edgecolor()
    assert edge.size > 0 and float(edge[0][3]) > 0.0 and float(edge[0][0]) < 0.3
    plt.close("all")
    ax = plot_static(pts, basemap=False, emphasis="data", markersize=3)
    assert float(_last(ax).get_sizes()[0]) == pytest.approx(3.0)
    plt.close("all")

    line = _gdf(LineString([(0, 0), (1, 1)]))
    lw_data = float(
        _last(plot_static(line, basemap=False, emphasis="data")).get_linewidth()[0]
    )
    plt.close("all")
    lw_blend = float(
        _last(plot_static(line, basemap=False, emphasis="blend")).get_linewidth()[0]
    )
    assert lw_data > lw_blend


def test_a_cog_is_never_dimmed_by_either_emphasis(tmp_path):
    import rasterio
    from rasterio.transform import from_bounds

    path = str(tmp_path / "cog.tif")
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=16,
        width=16,
        count=1,
        dtype="uint8",
        crs="EPSG:3857",
        transform=from_bounds(-1.36e7, 4.5e6, -1.35e7, 4.51e6, 16, 16),
    ) as dst:
        dst.write((np.random.rand(1, 16, 16) * 200).astype("uint8"))
    for emphasis in ("data", "blend"):
        ax = plot_cog(path, basemap=False, emphasis=emphasis)
        # a raster fully covers the basemap, so there is no "blend" to dim it to
        assert float(ax.get_images()[-1].get_alpha() or 1.0) == 1.0
        plt.close("all")


def _overlay(html):
    m = re.search(r"const overlay = (\{.*?\});", html, re.S)
    assert m, "overlay object not found in html"
    return json.loads(m.group(1))


def _paint(overlay, suffix):
    for ly in overlay["layers"]:
        if ly["id"].endswith(suffix):
            return ly["paint"]
    raise AssertionError(f"no layer ending in {suffix!r}")


def _html(layer, emphasis):
    prepared = [layer_to_sources_layers(layer, 0, emphasis=emphasis)]
    return _overlay(build_html(prepared, emphasis=emphasis))


def _raster(**kw):
    layer = raster_layer((np.random.rand(3, 8, 8) * 255).astype("uint8"), **kw)
    layer.opacity = kw.get("opacity")  # None: emphasis controls it
    return layer


def test_interactive_polygon_paint_pops_in_data_is_soft_in_blend_and_user_values_win():
    data = _html(vector_layer(_gdf_polygon()), "data")
    fill = _paint(data, "-fill")
    assert fill["fill-opacity"] == pytest.approx(0.85)
    assert fill["fill-outline-color"].lower() in ("#222222", "#222")
    assert _paint(data, "-line")["line-width"] > 1.0

    blend = _html(vector_layer(_gdf_polygon()), "blend")
    fill = _paint(blend, "-fill")
    assert fill["fill-opacity"] == pytest.approx(0.8)  # vector_layer's own default
    assert "fill-outline-color" not in fill
    assert _paint(blend, "-line")["line-width"] == pytest.approx(1.0)

    user = _html(vector_layer(_gdf_polygon(), color="#00ff00", opacity=0.2), "data")
    fill = _paint(user, "-fill")
    assert fill["fill-color"] == "#00ff00" and fill["fill-opacity"] == pytest.approx(
        0.2
    )


def test_interactive_rasters_stay_at_full_opacity_unless_the_user_sets_one():
    for emphasis in ("data", "blend"):
        paint = _paint(_html(_raster(), emphasis), "-raster")
        assert paint["raster-opacity"] == pytest.approx(1.0)
    paint = _paint(_html(_raster(opacity=0.3), "data"), "-raster")
    assert paint["raster-opacity"] == pytest.approx(0.3)


def test_plot_interactive_threads_emphasis_through(monkeypatch):
    monkeypatch.setattr(itx, "_notebook_display_html", lambda: None)
    real_import = builtins.__import__

    def _no_ipython_display(name, *args, **kwargs):
        if name == "IPython.display":
            raise ImportError("disabled")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _no_ipython_display)
    html = plot_interactive(
        [vector_layer(_gdf_polygon())], emphasis="data", debug_mode=0
    )
    fill = _paint(_overlay(html), "-fill")
    assert fill["fill-opacity"] == pytest.approx(0.85)
    assert "fill-outline-color" in fill
