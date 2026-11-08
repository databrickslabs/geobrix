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


@pytest.fixture(autouse=True)
def _close_figures():
    plt.close("all")
    yield
    plt.close("all")


class _FakeGeoType:
    # mimics a Databricks GEOMETRY/GEOGRAPHY dataType for routing tests
    def __init__(self, name):
        self._name = name

    def typeName(self):
        return self._name

    def simpleString(self):
        return self._name


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


def _custom_cell(grid):
    from databricks.labs.gbx.pygx import _custom

    conf = _custom.conf_from_row(grid)
    return _custom.point_to_cell_id(conf, 500.0, 500.0, 1)


def _make_polygon_gdf():
    """Three polygons over a wide lon range, with a continuous 'value' column."""
    import geopandas as gpd
    from shapely.geometry import box

    geoms = [
        box(-74.1, 40.6, -73.9, 40.8),
        box(-80.0, 25.7, -79.8, 25.9),
        box(-87.7, 41.8, -87.5, 42.0),
    ]
    return gpd.GeoDataFrame({"value": [1.0, 2.0, 3.0]}, geometry=geoms, crs=4326)


_SQUARE = "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"


def test_geometry_column_strategy_by_type():
    from databricks.labs.gbx.vizx import _static_map as sm

    assert sm._geom_strategy(StringType()) == "string"
    assert sm._geom_strategy(BinaryType()) == "binary"
    assert sm._geom_strategy(_FakeGeoType("geometry")) == "native"
    assert sm._geom_strategy(_FakeGeoType("geography")) == "native"
    with pytest.raises(ValueError):
        sm._geom_strategy(LongType())


def test_resolve_gdf_reads_wkt_and_wkb_passes_geodataframes_and_caps_rows(spark):
    import geopandas as gpd
    import shapely
    from shapely.geometry import Point

    from databricks.labs.gbx.vizx import _static_map as sm

    df = spark.createDataFrame(
        [("a", "POINT (1 2)"), ("b", "POINT (3 4)")], ["name", "wkt"]
    )
    gdf = sm._resolve_gdf(df, None, None, 10_000, None)
    assert gdf.crs.to_epsg() == 4326
    assert list(gdf["name"]) == ["a", "b"] and "wkt" not in gdf.columns
    assert [g.x for g in gdf.geometry] == [1.0, 3.0]

    wkb = bytearray(shapely.to_wkb(shapely.from_wkt("POINT (5 6)")))
    got = sm._resolve_gdf(
        spark.createDataFrame([(wkb,)], ["geometry"]), None, None, 10_000, None
    )
    assert (got.geometry.iloc[0].x, got.geometry.iloc[0].y) == (5.0, 6.0)

    g = gpd.GeoDataFrame({"v": [1]}, geometry=[Point(0, 0)], crs=4326)
    assert sm._resolve_gdf(g, None, None, 10_000, None) is g

    with pytest.raises(ValueError):  # LongType column, no grid_system
        sm._resolve_gdf(
            spark.createDataFrame([(1,)], ["geometry"]), None, None, 10_000, None
        )

    many = spark.range(5).selectExpr("concat('POINT (', id, ' 0)') AS wkt")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert len(sm._resolve_gdf(many, None, None, 2, None)) == 2
    assert any("max_rows" in str(w.message) for w in caught)


def test_resolve_cells_h3_string_and_long_ids_give_the_same_boundary(spark):
    import h3

    from databricks.labs.gbx.vizx import _static_map as sm

    s = h3.latlng_to_cell(40.7, -74.0, 9)
    g_str = sm._resolve_gdf(
        spark.createDataFrame([(s,)], ["cellid"]), None, "h3", 10_000, None
    )
    g_long = sm._resolve_gdf(
        spark.createDataFrame([(h3.str_to_int(s),)], ["cellid"]),
        None,
        "h3",
        10_000,
        None,
    )
    assert g_str.crs.to_epsg() == 4326
    assert g_str.geometry.iloc[0].equals(g_long.geometry.iloc[0])
    # attribute columns are carried, the cell column is replaced by the boundary
    attrs = sm._resolve_gdf(
        spark.createDataFrame([(s, 7)], ["cellid", "count"]),
        "cellid",
        "h3",
        10_000,
        None,
    )
    assert list(attrs["count"]) == [7] and "cellid" not in attrs.columns


def test_resolve_cells_quadbin_bng_and_custom_grids_carry_their_own_crs(spark):
    import quadbin

    from databricks.labs.gbx.pygx import _bng
    from databricks.labs.gbx.vizx import _static_map as sm

    def resolve(cell, system, **kw):
        df = spark.createDataFrame([(cell,)], ["cellid"])
        return sm._resolve_gdf(df, "cellid", system, 10_000, None, **kw)

    q = resolve(
        quadbin.point_to_cell(-74.0, 40.7, 10), "quadbin"
    )  # lon/lat box over NYC
    assert q.crs.to_epsg() == 4326 and q.geometry.iloc[0].geom_type == "Polygon"
    minx, miny, maxx, maxy = q.geometry.iloc[0].bounds
    assert -75 < minx < maxx < -73 and 40 < miny < maxy < 41

    b = resolve(
        _bng.point_as_cell(530000.0, 180000.0, "1km"), "bng"
    )  # eastings/northings
    assert b.crs.to_epsg() == 27700
    minx, miny, _, _ = b.geometry.iloc[0].bounds
    assert 500_000 < minx < 560_000 and 150_000 < miny < 200_000

    grid = _custom_grid(27700)
    c = resolve(_custom_cell(grid), "custom", grid_conf=grid)
    assert c.crs.to_epsg() == 27700
    minx, miny, maxx, maxy = c.geometry.iloc[0].bounds
    assert 0 <= minx < maxx <= 1000 and 0 <= miny < maxy <= 1000
    no_crs = _custom_grid(-1)  # srid <= 0: the grid declares no CRS
    assert resolve(_custom_cell(no_crs), "custom", grid_conf=no_crs).crs is None

    with pytest.raises(ValueError):
        resolve(1, "custom")  # custom grids need the grid spec
    with pytest.raises(ValueError):
        resolve(1, "geohash")


def test_geometry_column_detection_errors_and_single_column_fallback(spark):
    from databricks.labs.gbx.vizx import _static_map as sm

    with pytest.raises(ValueError):  # no native geo / wkt / geometry / geom column
        sm._detect_geom_col(spark.createDataFrame([(1, 2.0)], ["a", "b"]), None)
    with pytest.raises(ValueError):  # several columns, none named like a cell id
        sm._detect_geom_col(spark.createDataFrame([(1, 2)], ["a", "b"]), "h3")
    # a lone column is used even if unnamed
    assert (
        sm._detect_geom_col(spark.createDataFrame([(123,)], ["my_cells"]), "h3")
        == "my_cells"
    )


def test_plot_static_draws_on_one_figure_reuses_axes_and_never_calls_show(
    spark, monkeypatch
):
    from databricks.labs.gbx.vizx import plot_static

    shows = []  # show() would flush the base before an ax= overlay is added
    monkeypatch.setattr(plt, "show", lambda *a, **k: shows.append(1))
    ax = plot_static(spark.createDataFrame([("POINT (1 1)",)], ["wkt"]), basemap=False)
    assert ax is not None and len(plt.get_fignums()) == 1
    ax2 = plot_static(
        spark.createDataFrame([("POINT (2 2)",)], ["wkt"]), basemap=False, ax=ax
    )
    assert ax2 is ax and len(plt.get_fignums()) == 1
    assert shows == []
    plot_static(
        spark.createDataFrame([(_SQUARE, 3)], ["wkt", "v"]), column="v", basemap=False
    )


def test_plot_static_reprojects_every_layer_to_web_mercator_even_without_a_basemap(
    spark,
):
    # Overlays must share a CRS to align: -122 deg lon is ~-1.358e7 m in EPSG:3857.
    from databricks.labs.gbx.vizx import plot_static

    ax = plot_static(
        spark.createDataFrame([("POINT (-122 37)",)], ["wkt"]), basemap=False
    )
    assert abs(float(ax.collections[-1].get_offsets()[0][0])) > 1000


def test_plot_static_fill_true_fills_and_fill_false_draws_the_outline_only(spark):
    from databricks.labs.gbx.vizx import plot_static

    df = spark.createDataFrame([(_SQUARE,)], ["wkt"])
    fc = plot_static(df, basemap=False).collections[-1].get_facecolor()
    assert fc.size > 0 and float(fc[0][3]) > 0.0  # visible (alpha > 0) face
    plt.close("all")
    ax = plot_static(df, basemap=False, fill=False, edgecolor="red")
    fc = ax.collections[-1].get_facecolor()
    assert fc.size == 0 or float(fc[0][3]) == 0.0


def test_plot_static_degrades_to_no_basemap_with_a_warning(spark, monkeypatch):
    import contextily

    from databricks.labs.gbx.vizx import plot_static

    def _boom(*a, **k):
        raise RuntimeError("no egress")

    monkeypatch.setattr(contextily, "add_basemap", _boom)
    df = spark.createDataFrame([("POINT (1 2)",)], ["wkt"])
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert plot_static(df, basemap=True) is not None
    assert len(plt.get_fignums()) == 1 and caught  # the figure is still produced
    plt.close("all")

    # A custom grid without a CRS cannot be placed on a basemap: skipped with a warning.
    grid = _custom_grid(-1)
    cells = spark.createDataFrame([(_custom_cell(grid),)], ["cellid"])
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert (
            plot_static(cells, grid_system="custom", grid_conf=grid, basemap=True)
            is not None
        )
    assert caught


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
    from importlib.metadata import PackageNotFoundError
    from importlib.metadata import version as dist_version

    from packaging.version import Version

    try:  # newer contextily releases carry no module-level __version__
        cx_version = dist_version("contextily")
    except PackageNotFoundError:
        pytest.skip(
            "contextily imports but has no distribution metadata to read its version from"
        )
    ver = Version(cx_version).release[
        :2
    ]  # a release candidate or dev version still has its release numbers
    if ver < (1, 7):
        pytest.skip(
            f"contextily {cx_version} < 1.7 — headers= not yet present; "
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


def _fake_add_basemap_with_headers(ax, source, crs, headers=None, **kwargs):
    """Accepts headers= (simulates contextily >= 1.7)."""


def _fake_add_basemap_without_headers(ax, source, crs):
    """Does NOT accept headers= (simulates contextily < 1.7)."""


def test_basemap_headers_are_passed_only_when_contextily_supports_them(monkeypatch):
    import contextily

    from databricks.labs.gbx.vizx._basemap import _TILE_USER_AGENT, _basemap_add_kwargs

    monkeypatch.setattr(contextily, "add_basemap", _fake_add_basemap_with_headers)
    assert _basemap_add_kwargs() == {"headers": _TILE_USER_AGENT}
    # older cluster contextily: degrade to no kwargs instead of raising TypeError
    monkeypatch.setattr(contextily, "add_basemap", _fake_add_basemap_without_headers)
    assert _basemap_add_kwargs() == {}


def test_basemap_source_presets_unknown_names_and_provider_passthrough(monkeypatch):
    import contextily as cx

    from databricks.labs.gbx.vizx import _basemap
    from databricks.labs.gbx.vizx._basemap import _resolve_basemap_source

    monkeypatch.setattr(_basemap, "_BASEMAP_PRESETS", None)  # always populate fresh
    esri = cx.providers.Esri
    assert _resolve_basemap_source(None) == esri.WorldStreetMap
    assert _resolve_basemap_source("imagery") == esri.WorldImagery
    assert _resolve_basemap_source("topo") == esri.WorldTopoMap
    assert _resolve_basemap_source(esri.WorldImagery) is esri.WorldImagery
    with pytest.raises(ValueError, match="Unknown basemap_source preset"):
        _resolve_basemap_source("not_a_real_preset")


def test_in_memory_raster_layer_draws_above_the_basemap(monkeypatch):
    """A raster_layer image must be at zorder 2, above the basemap (zorder 1): imshow's
    default zorder 0 let the later-added basemap cover the raster entirely."""
    import io

    import numpy as np
    import rasterio
    from rasterio.transform import from_bounds

    import databricks.labs.gbx.vizx._static_map as sm
    from databricks.labs.gbx.vizx._layers import raster_layer

    data = np.linspace(0.0, 100.0, 16, dtype=np.float32).reshape(1, 4, 4)
    buf = io.BytesIO()
    with rasterio.open(
        buf,
        "w",
        driver="GTiff",
        height=4,
        width=4,
        count=1,
        dtype=np.float32,
        crs="EPSG:3857",
        transform=from_bounds(-100.0, -100.0, 100.0, 100.0, 4, 4),
    ) as dst:
        dst.write(data)
    lyr = raster_layer(buf.getvalue(), band=1, cmap="viridis")
    _, ax = plt.subplots()
    sm._draw_one_layer(
        lyr, ax, max_rows=5000, sample_seed=0, srid=3857, legend=False, emphasis="blend"
    )
    assert ax.get_images()
    assert all(img.get_zorder() == 2 for img in ax.get_images())


def test_the_colorbar_matches_the_map_height_only_on_axes_plot_static_created():
    """geopandas sized the colorbar to the full subplot while the equal-aspect map realised
    a shorter box, so the colorbar overran the title (~2-3x). Caller-owned axes stay as given.
    """
    from databricks.labs.gbx.vizx import plot_static

    gdf = _make_polygon_gdf()
    ax = plot_static(gdf, column="value", legend=True, basemap=False)
    extra = [a for a in ax.figure.axes if a is not ax]
    if extra:  # no colorbar when geopandas picks a discrete legend
        assert len(extra) == 1
        map_h = ax.get_position().height
        assert abs(extra[0].get_position().height - map_h) <= 0.15 * map_h
    plt.close("all")

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    for a in axes:
        plot_static(gdf, column="value", legend=True, basemap=False, ax=a)
    assert len(fig.axes) >= 2 and len(plt.get_fignums()) == 1
