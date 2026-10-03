import inspect
import logging
import warnings

import matplotlib
import pytest

matplotlib.use("Agg")  # headless: no display needed
import matplotlib.pyplot as plt  # noqa: E402
from pyspark.sql.types import BinaryType, LongType, StringType  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    logging.getLogger("py4j").setLevel(logging.ERROR)
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]")
        .appName("viz-static-map-tests")
        .getOrCreate()
    )
    yield s


# --- _geom_strategy (pure, no Spark) ---


def test_geom_strategy_string_binary_native_and_error():
    from databricks.labs.gbx.vizx import _static_map as sm

    assert sm._geom_strategy(StringType()) == "string"
    assert sm._geom_strategy(BinaryType()) == "binary"


def test_geom_strategy_rejects_unsupported():
    from databricks.labs.gbx.vizx import _static_map as sm

    with pytest.raises(ValueError):
        sm._geom_strategy(LongType())


class _FakeGeoType:
    # mimics a Databricks GEOMETRY/GEOGRAPHY dataType for routing tests
    def __init__(self, name):
        self._name = name

    def typeName(self):
        return self._name

    def simpleString(self):
        return self._name


def test_geom_strategy_native_for_geometry_and_geography():
    from databricks.labs.gbx.vizx import _static_map as sm

    assert sm._geom_strategy(_FakeGeoType("geometry")) == "native"
    assert sm._geom_strategy(_FakeGeoType("geography")) == "native"


# --- _resolve_gdf geometry path ---


def test_resolve_gdf_wkt_string(spark):
    from databricks.labs.gbx.vizx import _static_map as sm

    df = spark.createDataFrame(
        [("a", "POINT (1 2)"), ("b", "POINT (3 4)")], ["name", "wkt"]
    )
    gdf = sm._resolve_gdf(df, None, None, 10_000, None)
    assert gdf.crs.to_epsg() == 4326
    assert list(gdf["name"]) == ["a", "b"]
    assert "wkt" not in gdf.columns
    assert [g.x for g in gdf.geometry] == [1.0, 3.0]


def test_resolve_gdf_wkb_matches_wkt(spark):
    import shapely

    from databricks.labs.gbx.vizx import _static_map as sm

    wkb = bytearray(shapely.to_wkb(shapely.from_wkt("POINT (5 6)")))
    df = spark.createDataFrame([(wkb,)], ["geometry"])
    gdf = sm._resolve_gdf(df, None, None, 10_000, None)
    assert (gdf.geometry.iloc[0].x, gdf.geometry.iloc[0].y) == (5.0, 6.0)


def test_resolve_gdf_passes_through_geodataframe():
    import geopandas as gpd
    from shapely.geometry import Point

    from databricks.labs.gbx.vizx import _static_map as sm

    g = gpd.GeoDataFrame({"v": [1]}, geometry=[Point(0, 0)], crs=4326)
    assert sm._resolve_gdf(g, None, None, 10_000, None) is g


def test_resolve_gdf_unknown_column_type_raises(spark):
    from databricks.labs.gbx.vizx import _static_map as sm

    df = spark.createDataFrame([(1,)], ["geometry"])  # LongType, no grid_system
    with pytest.raises(ValueError):
        sm._resolve_gdf(df, None, None, 10_000, None)


def test_resolve_gdf_truncates_and_warns(spark):
    from databricks.labs.gbx.vizx import _static_map as sm

    df = spark.range(5).selectExpr("concat('POINT (', id, ' 0)') AS wkt")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        gdf = sm._resolve_gdf(df, None, None, 2, None)
    assert len(gdf) == 2
    assert any("max_rows" in str(w.message) for w in caught)


def _ny_hex_string():
    import h3

    return h3.latlng_to_cell(40.7, -74.0, 9)  # string h3 index


def test_resolve_cells_h3_string_and_long_match(spark):
    import h3

    from databricks.labs.gbx.vizx import _static_map as sm

    s = _ny_hex_string()
    as_long = h3.str_to_int(s)

    df_str = spark.createDataFrame([(s,)], ["cellid"])
    df_long = spark.createDataFrame([(as_long,)], ["cellid"])

    g_str = sm._resolve_gdf(df_str, None, "h3", 10_000, None)
    g_long = sm._resolve_gdf(df_long, None, "h3", 10_000, None)

    assert g_str.crs.to_epsg() == 4326
    # identical boundary polygon from either id form
    assert g_str.geometry.iloc[0].equals(g_long.geometry.iloc[0])


def test_resolve_cells_carries_attribute_columns(spark):
    from databricks.labs.gbx.vizx import _static_map as sm

    s = _ny_hex_string()
    df = spark.createDataFrame([(s, 7)], ["cellid", "count"])
    gdf = sm._resolve_gdf(df, "cellid", "h3", 10_000, None)
    assert list(gdf["count"]) == [7]
    assert "cellid" not in gdf.columns


def test_resolve_cells_quadbin(spark):
    # quadbin cell ids are bigint; boundary is a lon/lat box (EPSG:4326).
    import quadbin

    from databricks.labs.gbx.vizx import _static_map as sm

    cell = quadbin.point_to_cell(-74.0, 40.7, 10)  # int cell over NYC
    df = spark.createDataFrame([(cell,)], ["cellid"])
    gdf = sm._resolve_gdf(df, "cellid", "quadbin", 10_000, None)
    assert gdf.crs.to_epsg() == 4326
    assert gdf.geometry.iloc[0].geom_type == "Polygon"
    minx, miny, maxx, maxy = gdf.geometry.iloc[0].bounds
    assert -75 < minx < maxx < -73 and 40 < miny < maxy < 41  # lon/lat near NYC


def test_resolve_cells_bng(spark):
    # BNG cell ids are STRING; boundary is in EPSG:27700 eastings/northings.
    from databricks.labs.gbx.pygx import _bng
    from databricks.labs.gbx.vizx import _static_map as sm

    cellid = _bng.point_as_cell(530000.0, 180000.0, "1km")  # central London, 1km
    df = spark.createDataFrame([(cellid,)], ["cellid"])
    gdf = sm._resolve_gdf(df, "cellid", "bng", 10_000, None)
    assert gdf.crs.to_epsg() == 27700
    assert gdf.geometry.iloc[0].geom_type == "Polygon"
    minx, miny, _, _ = gdf.geometry.iloc[0].bounds
    assert 500_000 < minx < 560_000 and 150_000 < miny < 200_000  # 27700 metres


def _custom_grid(srid=27700):
    # Minimal custom-grid spec (the struct conf_from_row consumes).
    return {
        "bound_x_min": 0,
        "bound_x_max": 1000,
        "bound_y_min": 0,
        "bound_y_max": 1000,
        "cell_splits": 2,
        "root_cell_size_x": 100,
        "root_cell_size_y": 100,
        "srid": srid,
    }


def test_resolve_cells_custom_with_grid_conf(spark):
    # custom grids need the grid spec; CRS comes from the grid's srid.
    from databricks.labs.gbx.pygx import _custom
    from databricks.labs.gbx.vizx import _static_map as sm

    grid = _custom_grid(srid=27700)
    conf = _custom.conf_from_row(grid)
    cell = _custom.point_to_cell_id(conf, 500.0, 500.0, 1)
    df = spark.createDataFrame([(cell,)], ["cellid"])
    gdf = sm._resolve_gdf(df, "cellid", "custom", 10_000, None, grid_conf=grid)
    assert gdf.crs.to_epsg() == 27700
    assert gdf.geometry.iloc[0].geom_type == "Polygon"
    minx, miny, maxx, maxy = gdf.geometry.iloc[0].bounds
    assert 0 <= minx < maxx <= 1000 and 0 <= miny < maxy <= 1000


def test_resolve_cells_custom_requires_grid_conf(spark):
    from databricks.labs.gbx.vizx import _static_map as sm

    df = spark.createDataFrame([(1,)], ["cellid"])
    with pytest.raises(ValueError):
        sm._resolve_gdf(df, "cellid", "custom", 10_000, None)  # no grid_conf


def test_resolve_cells_custom_no_srid_has_no_crs(spark):
    # srid<=0 -> the grid declares no CRS, so the GeoDataFrame crs is None.
    from databricks.labs.gbx.pygx import _custom
    from databricks.labs.gbx.vizx import _static_map as sm

    grid = _custom_grid(srid=-1)
    conf = _custom.conf_from_row(grid)
    cell = _custom.point_to_cell_id(conf, 500.0, 500.0, 1)
    df = spark.createDataFrame([(cell,)], ["cellid"])
    gdf = sm._resolve_gdf(df, "cellid", "custom", 10_000, None, grid_conf=grid)
    assert gdf.crs is None


def test_resolve_cells_unknown_grid_system_raises(spark):
    from databricks.labs.gbx.vizx import _static_map as sm

    df = spark.createDataFrame([(1,)], ["cellid"])
    with pytest.raises(ValueError):
        sm._resolve_gdf(df, "cellid", "geohash", 10_000, None)


def test_plot_static_returns_axes_and_one_figure(spark):
    from databricks.labs.gbx.vizx import plot_static

    plt.close("all")
    df = spark.createDataFrame([("POINT (1 2)",)], ["wkt"])
    ax = plot_static(df, basemap=False)
    assert ax is not None
    assert len(plt.get_fignums()) == 1
    plt.close("all")


def test_plot_static_choropleth_column_with_legend(spark):
    from databricks.labs.gbx.vizx import plot_static

    plt.close("all")
    df = spark.createDataFrame(
        [("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", 3)], ["wkt", "v"]
    )
    ax = plot_static(df, column="v", basemap=False)
    assert ax.get_figure() is not None
    plt.close("all")


def test_plot_static_overlay_reuses_axes(spark):
    from databricks.labs.gbx.vizx import plot_static

    plt.close("all")
    df1 = spark.createDataFrame([("POINT (1 1)",)], ["wkt"])
    df2 = spark.createDataFrame([("POINT (2 2)",)], ["wkt"])
    ax = plot_static(df1, basemap=False)
    ax2 = plot_static(df2, basemap=False, ax=ax)
    assert ax2 is ax
    assert len(plt.get_fignums()) == 1  # no new figure created for the overlay
    plt.close("all")


def test_plot_static_reprojects_to_3857_even_without_basemap(spark):
    # Overlays must share a CRS to align. plot_static reprojects every layer to
    # Web Mercator (EPSG:3857) regardless of `basemap`, so a basemap=False
    # overlay lands in the same coordinate space as a basemap layer rather than
    # in raw 4326 degrees. Guards against the layers-misaligned regression.
    from databricks.labs.gbx.vizx import plot_static

    plt.close("all")
    df = spark.createDataFrame([("POINT (-122 37)",)], ["wkt"])
    ax = plot_static(df, basemap=False)
    x = float(ax.collections[-1].get_offsets()[0][0])
    # -122 deg lon -> ~-1.358e7 m in EPSG:3857; in 4326 it would be ~-122.
    assert abs(x) > 1000, f"expected web-mercator meters, got {x}"
    plt.close("all")


def test_detect_geom_col_no_geometry_column_raises(spark):
    # grid_system=None and no native geo / wkt / geometry / geom column -> error.
    from databricks.labs.gbx.vizx import _static_map as sm

    df = spark.createDataFrame([(1, 2.0)], ["a", "b"])
    with pytest.raises(ValueError):
        sm._detect_geom_col(df, None)


def test_detect_geom_col_ambiguous_cell_column_raises(spark):
    # grid_system set, several columns, none named like a cell id -> error.
    from databricks.labs.gbx.vizx import _static_map as sm

    df = spark.createDataFrame([(1, 2)], ["a", "b"])
    with pytest.raises(ValueError):
        sm._detect_geom_col(df, "h3")


def test_detect_geom_col_single_column_with_grid_system(spark):
    # grid_system set + a lone column -> that column is used even if unnamed.
    from databricks.labs.gbx.vizx import _static_map as sm

    df = spark.createDataFrame([(123,)], ["my_cells"])
    assert sm._detect_geom_col(df, "h3") == "my_cells"


def _last_facecolor(ax):
    # geopandas draws polygons as a collection; return its facecolor RGBA array.
    return ax.collections[-1].get_facecolor()


def test_plot_static_fill_true_fills_polygon(spark):
    from databricks.labs.gbx.vizx import plot_static

    plt.close("all")
    df = spark.createDataFrame([("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",)], ["wkt"])
    ax = plot_static(df, basemap=False)  # fill=True default
    fc = _last_facecolor(ax)
    assert fc.size > 0 and float(fc[0][3]) > 0.0  # visible (alpha>0) face
    plt.close("all")


def test_plot_static_fill_false_draws_outline_only(spark):
    from databricks.labs.gbx.vizx import plot_static

    plt.close("all")
    df = spark.createDataFrame([("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))",)], ["wkt"])
    ax = plot_static(df, basemap=False, fill=False, edgecolor="red")
    fc = _last_facecolor(ax)
    # facecolor "none" -> empty array or a fully transparent (alpha 0) face.
    assert fc.size == 0 or float(fc[0][3]) == 0.0
    plt.close("all")


def test_plot_static_does_not_call_show(spark, monkeypatch):
    # plot_static must NOT call pyplot.show() (the figure auto-displays at cell
    # end); calling it on the creating call would flush the base before an
    # ax= overlay is added. Guards the overlay-rendering regression.
    import matplotlib.pyplot as plt_mod

    from databricks.labs.gbx.vizx import plot_static

    calls = []
    monkeypatch.setattr(plt_mod, "show", lambda *a, **k: calls.append(1))
    plt.close("all")
    df = spark.createDataFrame([("POINT (1 2)",)], ["wkt"])
    plot_static(df, basemap=False)
    assert calls == []  # show() never called
    plt.close("all")


def test_plot_static_basemap_fallback_warns(spark, monkeypatch):
    import contextily

    from databricks.labs.gbx.vizx import plot_static

    def _boom(*a, **k):
        raise RuntimeError("no egress")

    monkeypatch.setattr(contextily, "add_basemap", _boom)
    plt.close("all")
    df = spark.createDataFrame([("POINT (1 2)",)], ["wkt"])
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        ax = plot_static(df, basemap=True)
    assert ax is not None
    assert len(plt.get_fignums()) == 1  # figure still produced
    assert any("basemap unavailable" in str(w.message) for w in caught)
    plt.close("all")


def test_plot_static_skips_basemap_when_no_crs(spark):
    # A custom grid with srid<=0 has no CRS, so the basemap is skipped (with a
    # warning) rather than placed against arbitrary coordinates.
    from databricks.labs.gbx.pygx import _custom
    from databricks.labs.gbx.vizx import plot_static

    grid = _custom_grid(srid=-1)
    conf = _custom.conf_from_row(grid)
    cell = _custom.point_to_cell_id(conf, 500.0, 500.0, 1)
    df = spark.createDataFrame([(cell,)], ["cellid"])
    plt.close("all")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        ax = plot_static(df, grid_system="custom", grid_conf=grid, basemap=True)
    assert ax is not None
    assert any("no CRS" in str(w.message) for w in caught)
    plt.close("all")


def test_contextily_add_basemap_accepts_headers():
    """contextily.add_basemap must accept a headers= kwarg (added in 1.7.0).

    The _basemap module passes _TILE_USER_AGENT via headers= on every
    cx.add_basemap call to satisfy OSM's UA policy.  This test hits the real
    contextily signature (not a mock) so a future contextily API change that
    removes the parameter is caught immediately rather than silently swallowed
    at render time.  Skipped on contextily < 1.7 (dev-container pin) where the
    parameter did not yet exist.
    """
    contextily = pytest.importorskip("contextily")
    ver = tuple(int(x) for x in contextily.__version__.split(".")[:2])
    if ver < (1, 7):
        pytest.skip(
            f"contextily {contextily.__version__} < 1.7 — headers= not yet present; "
            "CI lock uses 1.7.1 which is the correct minimum."
        )
    sig = inspect.signature(contextily.add_basemap)
    assert "headers" in sig.parameters, (
        "contextily.add_basemap no longer accepts headers=; "
        "update _basemap._TILE_USER_AGENT wiring."
    )


def test_plot_static_enables_tile_cache(spark, monkeypatch):
    """plot_static(basemap=True) must call cx.set_cache_dir before add_basemap.

    The tile cache prevents re-fetching the same OSM/provider tiles across
    multiple renders of the same AOI (access-blocked tile grids on the second+
    render).  set_cache_dir is session-global, so the flag should flip once and
    every subsequent call should be a no-op (idempotent).
    """
    import contextily

    from databricks.labs.gbx.vizx import _basemap, plot_static

    # Reset the session flag so the call actually fires for this test.
    monkeypatch.setattr(_basemap, "_tile_cache_enabled", False)

    set_cache_calls = []
    monkeypatch.setattr(
        contextily, "set_cache_dir", lambda p: set_cache_calls.append(p)
    )
    monkeypatch.setattr(contextily, "add_basemap", lambda *a, **k: None)

    df = spark.createDataFrame([("POINT (1 2)",)], ["wkt"])
    plt.close("all")
    plot_static(df, basemap=True)
    plt.close("all")

    assert (
        set_cache_calls
    ), "cx.set_cache_dir was not called — tile cache was not enabled before add_basemap"

    # Idempotence: a second plot_static call must NOT call set_cache_dir again.
    set_cache_calls.clear()
    plot_static(df, basemap=True)
    plt.close("all")
    assert (
        not set_cache_calls
    ), "cx.set_cache_dir called twice — _enable_tile_cache is not idempotent"


# --- _basemap_add_kwargs: both branches ---


def _fake_add_basemap_with_headers(ax, source, crs, headers=None, **kwargs):
    """Fake add_basemap that accepts headers= (simulates contextily >= 1.7)."""


def _fake_add_basemap_without_headers(ax, source, crs):
    """Fake add_basemap that does NOT accept headers= (simulates contextily < 1.7)."""


def test_basemap_add_kwargs_returns_headers_when_supported(monkeypatch):
    """_basemap_add_kwargs returns {'headers': _TILE_USER_AGENT} when the installed
    contextily.add_basemap signature includes a headers= parameter.

    Monkeypatches cx.add_basemap with a fake that accepts headers= so both code
    paths are exercised without depending on the installed contextily version.
    """
    import contextily

    from databricks.labs.gbx.vizx._basemap import _TILE_USER_AGENT, _basemap_add_kwargs

    monkeypatch.setattr(contextily, "add_basemap", _fake_add_basemap_with_headers)
    result = _basemap_add_kwargs()
    assert result == {
        "headers": _TILE_USER_AGENT
    }, f"Expected {{'headers': _TILE_USER_AGENT}}, got {result!r}"


def test_basemap_add_kwargs_returns_empty_when_unsupported(monkeypatch):
    """_basemap_add_kwargs returns {} when the installed contextily.add_basemap
    signature does NOT include headers=, so the call degrades gracefully instead
    of raising TypeError on older cluster contextily versions.
    """
    import contextily

    from databricks.labs.gbx.vizx._basemap import _basemap_add_kwargs

    monkeypatch.setattr(contextily, "add_basemap", _fake_add_basemap_without_headers)
    result = _basemap_add_kwargs()
    assert (
        result == {}
    ), f"Expected empty dict for old contextily signature, got {result!r}"


# --- _resolve_basemap_source ---


def test_resolve_basemap_source_none_returns_world_street_map(monkeypatch):
    """_resolve_basemap_source(None) returns Esri.WorldStreetMap (the default)."""
    import contextily as cx

    from databricks.labs.gbx.vizx import _basemap
    from databricks.labs.gbx.vizx._basemap import _resolve_basemap_source

    # Reset the cached presets so this test always populates them fresh.
    monkeypatch.setattr(_basemap, "_BASEMAP_PRESETS", None)

    result = _resolve_basemap_source(None)
    assert (
        result == cx.providers.Esri.WorldStreetMap
    ), f"Expected Esri.WorldStreetMap for None, got {result!r}"


def test_resolve_basemap_source_imagery_returns_world_imagery(monkeypatch):
    """_resolve_basemap_source('imagery') returns Esri.WorldImagery."""
    import contextily as cx

    from databricks.labs.gbx.vizx import _basemap
    from databricks.labs.gbx.vizx._basemap import _resolve_basemap_source

    monkeypatch.setattr(_basemap, "_BASEMAP_PRESETS", None)

    result = _resolve_basemap_source("imagery")
    assert (
        result == cx.providers.Esri.WorldImagery
    ), f"Expected Esri.WorldImagery for 'imagery', got {result!r}"


def test_resolve_basemap_source_topo_returns_world_topo_map(monkeypatch):
    """_resolve_basemap_source('topo') returns Esri.WorldTopoMap."""
    import contextily as cx

    from databricks.labs.gbx.vizx import _basemap
    from databricks.labs.gbx.vizx._basemap import _resolve_basemap_source

    monkeypatch.setattr(_basemap, "_BASEMAP_PRESETS", None)

    result = _resolve_basemap_source("topo")
    assert (
        result == cx.providers.Esri.WorldTopoMap
    ), f"Expected Esri.WorldTopoMap for 'topo', got {result!r}"


def test_resolve_basemap_source_unknown_string_raises_value_error(monkeypatch):
    """_resolve_basemap_source raises ValueError for an unknown preset string."""
    import contextily as cx  # noqa: F401 — ensure cx available so presets load

    from databricks.labs.gbx.vizx import _basemap
    from databricks.labs.gbx.vizx._basemap import _resolve_basemap_source

    monkeypatch.setattr(_basemap, "_BASEMAP_PRESETS", None)

    with pytest.raises(ValueError, match="Unknown basemap_source preset"):
        _resolve_basemap_source("not_a_real_preset")


def test_resolve_basemap_source_provider_object_passthrough(monkeypatch):
    """_resolve_basemap_source passes through a contextily provider object unchanged."""
    import contextily as cx

    from databricks.labs.gbx.vizx import _basemap
    from databricks.labs.gbx.vizx._basemap import _resolve_basemap_source

    monkeypatch.setattr(_basemap, "_BASEMAP_PRESETS", None)

    provider = cx.providers.Esri.WorldImagery
    result = _resolve_basemap_source(provider)
    assert (
        result is provider
    ), f"Expected passthrough of provider object, got {result!r}"


# --- raster_layer zorder: in-memory bytes path draws above basemap ---


def test_raster_layer_inmem_zorder_is_2(monkeypatch):
    """In-memory raster_layer renders at zorder=2, above the basemap (zorder=1).

    Root cause of cmd23 bug: plot_raster's ax.imshow uses the default zorder=0;
    cx.add_basemap is called AFTER the raster (also at zorder=0 but added later),
    so the basemap covers the raster entirely — only basemap shows.

    Fix: _draw_one_layer sets zorder=2 on all AxesImage objects added by
    plot_raster, matching plot_cog's convention.
    """
    import io

    import matplotlib
    import numpy as np
    import rasterio
    from rasterio.io import MemoryFile
    from rasterio.transform import from_bounds

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    import databricks.labs.gbx.vizx._static_map as sm

    # Build a tiny 4×4 float32 GeoTIFF in EPSG:3857
    width, height = 4, 4
    data = np.linspace(0.0, 100.0, width * height, dtype=np.float32).reshape(
        1, height, width
    )
    transform = from_bounds(-100.0, -100.0, 100.0, 100.0, width, height)
    buf = io.BytesIO()
    with rasterio.open(
        buf,
        "w",
        driver="GTiff",
        height=height,
        width=width,
        count=1,
        dtype=np.float32,
        crs="EPSG:3857",
        transform=transform,
    ) as dst:
        dst.write(data)
    tile_bytes = buf.getvalue()

    from databricks.labs.gbx.vizx._layers import raster_layer

    lyr = raster_layer(tile_bytes, band=1, cmap="viridis")

    plt.close("all")
    _, ax = plt.subplots()
    sm._draw_one_layer(lyr, ax, max_rows=5000, sample_seed=0, srid=3857, legend=False, emphasis="blend")

    images = ax.get_images()
    assert images, "No AxesImage added by _draw_one_layer for raster_layer"
    for img in images:
        assert img.get_zorder() == 2, (
            f"Raster image zorder is {img.get_zorder()}, expected 2 "
            "(must be above basemap at zorder=1)"
        )
    plt.close("all")
