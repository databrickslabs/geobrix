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


def test_rst_bng_rasterize_agg_null_cellid_value_alignment(spark):
    """Null cellids must not shift value pairing for subsequent valid rows.

    Before the fix: ``cells`` filtered None from cellid but ``vals`` iterated
    the full value series, so ``zip(cells, vals)`` misaligned — the Nth valid
    cellid was paired with the Nth overall value, not its own.

    This test places a null cellid between two valid (cellid, value) rows and
    asserts that each valid cell receives its OWN value, not its neighbour's.
    """
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    cells = _bng_cell_strs(3)
    assert len(cells) >= 2, "need at least 2 BNG cells for alignment test"
    c0, c1 = cells[0], cells[1]

    schema = StructType(
        [
            StructField("cellid", StringType(), True),
            StructField("val", DoubleType(), True),
            StructField("tx", StringType(), False),
        ]
    )
    # Row ordering: (c0, 10.0), (None, 99.0), (c1, 20.0)
    # After the fix: c0->10.0, c1->20.0.
    # Bug behaviour: c0->10.0, c1->99.0 (null row's value assigned to c1).
    rows = [(c0, 10.0, "TX1"), (None, 99.0, "TX1"), (c1, 20.0, "TX1")]
    df = spark.createDataFrame(rows, schema)
    out = (
        df.groupBy("tx")
        .agg(rx.rst_bng_rasterize_agg("cellid", "val").alias("tile"))
        .collect()
    )
    tile = out[0]["tile"]
    assert tile is not None and tile["raster"] is not None
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        arr = ds.read(1)
        # Both valid cells must be covered (non-nodata).
        covered = arr[arr != ds.nodata]
        assert covered.size >= 2, f"expected >=2 covered pixels, got {covered.size}"
        # Neither cell should have burned 99.0 (the null-cellid row's value).
        assert 99.0 not in covered, (
            f"99.0 (null-cellid row value) appeared in output — cellid/value "
            f"misalignment bug is present: covered={sorted(set(covered.tolist()))}"
        )
        # Cell c0 should carry 10.0 and cell c1 should carry 20.0.
        assert 10.0 in covered, "c0's value 10.0 not found in output"
        assert 20.0 in covered, "c1's value 20.0 not found in output"
