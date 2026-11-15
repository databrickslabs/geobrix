import logging
import warnings
from unittest.mock import patch

import geopandas as gpd
import h3
import pandas as pd
import pytest
from shapely.geometry import Polygon

import databricks.labs.gbx.vizx._vector as _v
from databricks.labs.gbx.vizx import as_gdf, cells_as_gdf, grid_as_gdf


@pytest.fixture(scope="module")
def spark():
    logging.getLogger("py4j").setLevel(logging.ERROR)
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]")
        .appName("viz-vector-tests")
        .getOrCreate()
    )
    yield s


class _FakeSparkDF:
    """Minimal Spark-DataFrame stand-in for the sampling and dissolve paths.

    Records the args passed to ``.sample`` and returns a pandas frame from ``.toPandas``
    so the adapters can build a gdf without a Spark session.
    """

    def __init__(self, pdf, *, count, columns=None):
        self._pdf = pdf
        self._count = count
        self.columns = columns if columns is not None else list(pdf.columns)
        self.sample_calls = []

    def count(self):
        return self._count

    def select(self, *cols):
        return self

    def sample(self, withReplacement, fraction, seed):
        self.sample_calls.append(
            {"withReplacement": withReplacement, "fraction": fraction, "seed": seed}
        )
        return self

    def limit(self, n):
        self._pdf = self._pdf.iloc[:n]
        return self

    def toPandas(self):
        return self._pdf


def _cell(lat=0.0, lng=0.0):
    return h3.str_to_int(h3.latlng_to_cell(lat, lng, 5))


def _fake_cells(n=1, count=None, cols=("cellid", "band_level")):
    pdf = pd.DataFrame({cols[0]: [_cell()] * n, cols[1]: list(range(1, n + 1))})
    return _FakeSparkDF(pdf, count=count or n, columns=list(cols))


def _disk(lat, lng):
    return [h3.str_to_int(c) for c in h3.grid_disk(h3.latlng_to_cell(lat, lng, 5), 1)]


def test_as_gdf_has_crs_geometry_and_columns_and_truncates_with_a_warning(spark):
    df = spark.createDataFrame(
        [("a", "POINT (1 2)"), ("b", "POINT (3 4)")], ["name", "wkt"]
    )
    gdf = as_gdf(df)
    assert gdf.crs.to_epsg() == 4326
    assert list(gdf["name"]) == ["a", "b"] and "wkt" not in gdf.columns
    assert all(gdf.geometry.is_valid)

    many = spark.range(5).selectExpr("id", "concat('POINT (', id, ' 0)') AS wkt")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert len(as_gdf(many, max_rows=2)) == 2
    assert any("truncated" in str(w.message).lower() for w in caught)


def test_sample_seed_samples_with_that_seed_and_none_keeps_the_first_n(spark):
    pdf = pd.DataFrame(
        {"name": list("abcd"), "wkt": [f"POINT ({i} 0)" for i in range(4)]}
    )
    fake = _FakeSparkDF(pdf, count=1000)
    gdf = as_gdf(fake, max_rows=10, sample_seed=42)
    (call,) = fake.sample_calls
    assert call["seed"] == 42 and call["withReplacement"] is False
    assert abs(call["fraction"] - 10 * 1.3 / 1000) < 1e-12
    assert gdf.crs.to_epsg() == 4326

    cells = _fake_cells(2, count=500, cols=("cellid", "count"))
    cells_as_gdf(cells, extra_cols=["count"], max_rows=10, sample_seed=11)
    (call,) = cells.sample_calls
    assert call["seed"] == 11 and abs(call["fraction"] - 10 * 1.3 / 500) < 1e-12

    # None: the first-N .limit path, no .sample and no (expensive) count
    first = _FakeSparkDF(pdf, count=5)
    first.count = lambda: pytest.fail("count() must not be called on the None path")
    assert len(as_gdf(first, max_rows=2)) == 2 and first.sample_calls == []

    df = spark.range(2000).selectExpr("id", "concat('POINT (', id, ' 0)') AS wkt")
    a, b, c = (as_gdf(df, max_rows=50, sample_seed=s) for s in (7, 7, 99))
    assert list(a["id"]) == list(b["id"]) and list(a["id"]) != list(c["id"])


def test_cells_as_gdf_builds_h3_boundaries_with_the_extra_columns(spark):
    df = spark.createDataFrame([(_cell(), 7)], ["cellid", "count"])
    gdf = cells_as_gdf(df, extra_cols=["count"])
    assert gdf.crs.to_epsg() == 4326 and list(gdf["count"]) == [7]
    assert gdf.geometry.iloc[0].geom_type == "Polygon"

    cells = [(c, 1) for c in h3.grid_disk(h3.latlng_to_cell(0, 0, 5), 2)]
    many = spark.createDataFrame(
        [(h3.str_to_int(c), n) for c, n in cells], ["cellid", "count"]
    )
    assert len(cells_as_gdf(many, extra_cols=["count"], max_rows=3)) == 3


def test_grid_as_gdf_takes_a_dict_or_row_reprojects_and_lets_srid_override():
    grid = {
        "xmin": -73.0,
        "ymin": 40.0,
        "xmax": -72.0,
        "ymax": 41.0,
        "srid": 4326,
        "pixel_size": 0.01,
        "width": 100,
        "height": 100,
    }
    gdf = grid_as_gdf(grid)
    assert len(gdf) == 1 and gdf.crs.to_epsg() == 4326
    assert gdf.geometry.iloc[0].geom_type == "Polygon"
    assert gdf.geometry.iloc[0].bounds == pytest.approx((-73.0, 40.0, -72.0, 41.0))
    assert (gdf["pixel_size"].iloc[0], gdf["width"].iloc[0]) == (0.01, 100)

    from pyspark.sql import Row

    row = grid_as_gdf(Row(xmin=-73.0, ymin=40.0, xmax=-72.0, ymax=41.0, srid=4326))
    assert row.geometry.iloc[0].bounds == pytest.approx((-73.0, 40.0, -72.0, 41.0))

    # a projected box (BNG, London) is reprojected to lon/lat
    bng = grid_as_gdf(
        {
            "xmin": 525000.0,
            "ymin": 175000.0,
            "xmax": 535000.0,
            "ymax": 185000.0,
            "srid": 27700,
        }
    )
    minx, miny, maxx, maxy = bng.geometry.iloc[0].bounds
    assert bng.crs.to_epsg() == 4326 and -1.0 < minx < 0.0 and maxx > minx
    assert 51.0 < miny < maxy < 52.0

    plain = {"xmin": -73.0, "ymin": 40.0, "xmax": -72.0, "ymax": 41.0}
    assert grid_as_gdf(plain, srid=4326).crs.to_epsg() == 4326


@pytest.mark.parametrize("engine", ["auto", "geopandas"])
def test_dissolve_by_merges_the_cells_of_each_group(spark, engine):
    rows = [(c, 1) for c in _disk(0.0, 0.0)] + [(c, 2) for c in _disk(10.0, 10.0)]
    df = spark.createDataFrame(rows, ["cellid", "band_level"])
    gdf = cells_as_gdf(
        df, extra_cols=["band_level"], dissolve_by="band_level", dissolve_engine=engine
    )
    assert len(gdf) == 2 and set(gdf["band_level"]) == {1, 2}
    assert all(gdf.geometry.is_valid) and not any(g.is_empty for g in gdf.geometry)
    single = cells_as_gdf(
        spark.createDataFrame(rows[:1], ["cellid", "band_level"]),
        extra_cols=["band_level"],
    )
    assert gdf.geometry.iloc[0].area > single.geometry.iloc[0].area


def test_dissolve_arguments_are_validated(spark):
    df = spark.createDataFrame([(_cell(), 7)], ["cellid", "count"])
    with pytest.raises(ValueError, match="dissolve_by"):
        cells_as_gdf(df, extra_cols=["count"], dissolve_by="band_level")
    with pytest.raises(ValueError, match="dissolve_engine"):
        cells_as_gdf(
            _fake_cells(1, cols=("cellid", "count")),
            extra_cols=["count"],
            dissolve_by="count",
            dissolve_engine="spark",
        )


def test_dissolve_engines_fall_back_from_product_to_geopandas_only_on_auto():
    geom = Polygon(
        [(lng, lat) for lat, lng in h3.cell_to_boundary(h3.int_to_str(_cell()))]
    )
    result_gdf = gpd.GeoDataFrame(
        {"band_level": [1, 2]}, geometry=[geom, geom], crs=4326
    )
    kw = dict(extra_cols=["band_level"], dissolve_by="band_level")

    gp_calls = []

    def _gp(*a, **k):
        gp_calls.append(1)
        return result_gdf

    # auto: product raises -> the geopandas fallback produces the result, once
    with patch.object(_v, "_dissolve_product", side_effect=RuntimeError("no product")):
        with patch.object(_v, "_dissolve_geopandas", side_effect=_gp):
            out = cells_as_gdf(_fake_cells(2), dissolve_engine="auto", **kw)
    assert len(gp_calls) == 1 and len(out) == 2

    # auto: product succeeds -> geopandas is never called
    gp_calls.clear()
    with patch.object(_v, "_dissolve_product", return_value=result_gdf) as prod:
        with patch.object(_v, "_dissolve_geopandas", side_effect=_gp):
            out = cells_as_gdf(_fake_cells(2), dissolve_engine="auto", **kw)
    assert prod.called and not gp_calls and out is result_gdf

    # product: no fallback, the failure surfaces
    with patch.object(_v, "_dissolve_product", side_effect=RuntimeError("no product")):
        with patch.object(_v, "_dissolve_geopandas", side_effect=_gp):
            with pytest.raises(RuntimeError):
                cells_as_gdf(_fake_cells(2), dissolve_engine="product", **kw)
    assert not gp_calls


def test_an_oversize_cell_frame_is_refused_unless_max_rows_is_explicit():
    big = _v._DRIVER_SAFE_CELLS + 1
    with pytest.raises(ValueError) as exc:
        cells_as_gdf(
            _fake_cells(1, count=big, cols=("cellid", "count")), extra_cols=["count"]
        )
    for remedy in ("max_rows=", "sample_seed=", "dissolve_by="):
        assert remedy in str(exc.value)  # the error names the ways out
    explicit = cells_as_gdf(
        _fake_cells(1, count=big, cols=("cellid", "count")),
        extra_cols=["count"],
        max_rows=1,
    )
    assert len(explicit) == 1
