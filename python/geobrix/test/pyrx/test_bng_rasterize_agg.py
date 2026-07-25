"""Spark-level tests for the light BNG rasterize_agg grouped-agg UDF.

BNG is EPSG:27700-native (British National Grid): ``cellid`` is a STRING column
(e.g. ``"TQ3080"``), the ``srid`` arg is a no-op (output is always 27700), and
there is NO WGS84 reprojection anywhere in the path. BNG cell math is sourced
exclusively from ``pygx._bng``.
"""

from shapely.geometry import box

from databricks.labs.gbx.pygx import _bng as bng
from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx import functions as rx


def _bng_cell_strs(res=3):
    # London-area box in EPSG:27700 eastings/northings -> STRING cell ids.
    poly = box(530000, 180000, 534000, 183000)
    return [bng.format(c) for c in bng.polyfill(poly, res)]


def test_rst_bng_rasterize_agg_presence_mask(spark):
    cells = _bng_cell_strs(3)
    assert len(cells) >= 2
    df = spark.createDataFrame([(c, "TX1") for c in cells], ["cellid", "tx"])
    out = (
        df.groupBy("tx").agg(rx.rst_bng_rasterize_agg("cellid").alias("tile")).collect()
    )
    tile = out[0]["tile"]
    assert tile is not None and tile["raster"] is not None
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        arr = ds.read(1)
        assert (arr == 1.0).sum() >= len(cells)
        assert ds.nodata == -9999.0
        assert ds.crs.to_epsg() == 27700, "BNG output must be EPSG:27700 (27700-native)"


def test_rst_bng_rasterize_agg_burns_value(spark):
    c = bng.point_as_cell(530000, 180000, 3)  # STRING id
    df = spark.createDataFrame([(c, 42.0, "TX1")], ["cellid", "val", "tx"])
    out = (
        df.groupBy("tx")
        .agg(rx.rst_bng_rasterize_agg("cellid", "val").alias("tile"))
        .collect()
    )
    with _serde.open_tile(bytes(out[0]["tile"]["raster"])) as ds:
        arr = ds.read(1)
        assert (arr == 42.0).sum() >= 1
        assert ds.crs.to_epsg() == 27700
