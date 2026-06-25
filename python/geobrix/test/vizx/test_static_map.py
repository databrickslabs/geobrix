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


@pytest.mark.parametrize("gs", ["quadbin", "bng", "custom"])
def test_resolve_cells_fast_follow_not_implemented(spark, gs):
    from databricks.labs.gbx.vizx import _static_map as sm

    df = spark.createDataFrame([(1,)], ["cellid"])
    with pytest.raises(NotImplementedError):
        sm._resolve_gdf(df, "cellid", gs, 10_000, None)


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
