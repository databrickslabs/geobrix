import logging
import warnings

import pytest
from pyspark.sql.types import BinaryType, LongType, StringType


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
