import h3
import numpy as np
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx import functions as rx


def test_rst_h3_rasterize_agg_presence_mask(spark):
    res = 9
    poly = h3.LatLngPoly([(0.0, 0.0), (0.0, 0.02), (0.02, 0.02), (0.02, 0.0)])
    cells = [h3.str_to_int(c) for c in h3.polygon_to_cells(poly, res)]
    df = spark.createDataFrame([(int(c), "TX1") for c in cells], ["cellid", "tx"])
    out = (
        df.groupBy("tx").agg(rx.rst_h3_rasterize_agg("cellid").alias("tile")).collect()
    )
    tile = out[0]["tile"]
    assert tile is not None and tile["raster"] is not None
    with _serde.open_tile(bytes(tile["raster"])) as ds:
        arr = ds.read(1)
        # presence mask -> covered pixels are 1.0, count matches >=1 per cell
        assert (arr == 1.0).sum() >= len(cells)
        assert ds.nodata == -9999.0


def test_rst_h3_rasterize_agg_burns_value(spark):
    res = 9
    c = h3.str_to_int(h3.latlng_to_cell(0.0, 0.0, res))
    df = spark.createDataFrame([(int(c), 42.0, "TX1")], ["cellid", "val", "tx"])
    out = (
        df.groupBy("tx")
        .agg(rx.rst_h3_rasterize_agg("cellid", "val").alias("tile"))
        .collect()
    )
    with _serde.open_tile(bytes(out[0]["tile"]["raster"])) as ds:
        arr = ds.read(1)
        assert (arr == 42.0).sum() >= 1


def test_rst_h3_rasterize_agg_null_cellid_value_alignment():
    """Null cellids must not shift value pairing for subsequent valid rows.

    Before the fix: ``cells`` filtered None from cellid but ``vals`` iterated
    the full value series, so ``zip(cells, vals)`` misaligned — the Nth valid
    cellid was paired with the Nth overall value, not its own.

    This test exercises the inner UDF function directly (not via Spark) to avoid
    the float64 precision loss that PySpark's Arrow bridge applies to nullable
    LongType pandas Series — H3 cell IDs are 60-bit values that exceed float64's
    53-bit mantissa and would be corrupted if passed through a Spark null-long
    series.  Calling the unwrapped function with a pandas Int64Dtype Series
    (which uses ``pd.NA`` for nulls, not NaN) exercises exactly the same code path
    as the real pandas_udf runtime.
    """
    import pandas as pd

    from databricks.labs.gbx.pyrx.functions import _rst_h3_rasterize_agg_udf

    res = 9
    c0 = int(h3.str_to_int(h3.latlng_to_cell(0.0, 0.0, res)))
    c1 = int(h3.str_to_int(h3.latlng_to_cell(0.05, 0.05, res)))
    assert c0 != c1, "c0 and c1 must be different H3 cells at res 9"

    # pd.NA with Int64Dtype preserves full 64-bit precision for valid values.
    # Wrap in pd.Series so .iloc indexing works (the UDF internally checks .iloc[0]).
    cellid_s = pd.Series(pd.array([c0, pd.NA, c1], dtype="Int64"))
    value_s = pd.Series([10.0, 99.0, 20.0], dtype="float64")
    # The UDF uses `s is not None and s.iloc[0] is not None` guards.
    # Pass Python None (not a NaN-filled Series) to trigger the `s is None` path
    # and use default values (srid=4326, mode=centroids, kring_pad=1).
    none_s = None

    # The decorated function's underlying callable — calling it directly gives us
    # the same logic the pandas_udf runtime would execute.
    fn = _rst_h3_rasterize_agg_udf.func
    result = fn(
        cellid_s,
        value_s,
        none_s,
        none_s,  # srid, pixel_size
        none_s,
        none_s,
        none_s,
        none_s,  # xmin, ymin, xmax, ymax
        none_s,
        none_s,  # width, height
        none_s,
        none_s,  # mode, kring_pad
    )
    assert result is not None, "UDF returned None — no valid cells after null filter"
    with _serde.open_tile(bytes(result)) as ds:
        arr = ds.read(1)
        covered = arr[arr != ds.nodata]
        assert covered.size >= 1, f"expected >=1 covered pixel, got {covered.size}"
        # Key assertion: 99.0 (the null-cellid row's value) must NOT appear.
        # Before the fix, zip(cells, vals) misaligned: c1 paired with 99.0 (the
        # null-row value) instead of 20.0, so 99.0 would appear in the output.
        assert 99.0 not in covered, (
            f"99.0 (null-cellid row value) appeared — cellid/value misalignment "
            f"bug still present: covered={sorted(set(covered.tolist()))}"
        )
        # At least one valid value (10.0 or 20.0) must be present.
        assert any(
            v in covered for v in (10.0, 20.0)
        ), f"neither 10.0 nor 20.0 found in output: {sorted(set(covered.tolist()))}"


def test_rst_h3_rasterize_agg_null_typed_value_column_is_presence(spark):
    """A null in a TYPED (Double) value column must burn presence 1.0, not NaN.

    Regression: pandas delivers a typed null as np.nan, and `np.nan is not None`
    is True, so the presence guard burned float(np.nan)=NaN. The cluster benchmark
    surfaced this as a heavy(1.0)-vs-light(NaN) divergence.
    """
    import numpy as np
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    res = 9
    poly = h3.LatLngPoly([(0.0, 0.0), (0.0, 0.02), (0.02, 0.02), (0.02, 0.0)])
    cells = [h3.str_to_int(c) for c in h3.polygon_to_cells(poly, res)]
    schema = StructType(
        [
            StructField("cellid", LongType(), False),
            StructField("val", DoubleType(), True),  # nullable; all null -> presence
            StructField("tx", StringType(), False),
        ]
    )
    df = spark.createDataFrame([(int(c), None, "TX1") for c in cells], schema)
    out = (
        df.groupBy("tx")
        .agg(rx.rst_h3_rasterize_agg("cellid", "val").alias("tile"))
        .collect()
    )
    with _serde.open_tile(bytes(out[0]["tile"]["raster"])) as ds:
        arr = ds.read(1)
        covered = arr[arr != ds.nodata]
        assert covered.size >= len(cells)
        assert not np.isnan(covered).any(), "null value column burned NaN, not presence"
        assert np.all(covered == 1.0), f"expected all 1.0, got {np.unique(covered)}"


def test_rst_h3_rasterize_agg_null_cellid_no_precision_loss(spark):
    """Null cellid in a BIGINT column must not corrupt surviving large cell ids.

    H3 cell ids (~6e17) exceed float64's exact-integer ceiling (2**53 ~= 9e15).
    When a group has ANY null cellid, PySpark/Arrow upcast the LongType Series to
    float64, rounding every id.  The Python wrapper must cast cellid to STRING on
    the wire so the Series arrives as object-dtype (strings + None), preserving
    full 64-bit precision.

    Test: a group of one large H3 id + one null-cellid row.  The result must be
    IDENTICAL to the control group that has only the single large id (no null row).
    Before the fix the two tiles differ because the rounded id maps to a different
    (adjacent) H3 cell.
    """
    res = 9
    # A concrete res-9 cell whose integer id is well above 2**53.
    large_cell_str = h3.latlng_to_cell(51.5, -0.1, res)  # London, res 9
    large_cell_id = int(h3.str_to_int(large_cell_str))
    assert (
        large_cell_id > 2**53
    ), f"chosen H3 id {large_cell_id} is not > 2**53; pick a finer resolution"

    schema = StructType(
        [
            StructField("cellid", LongType(), True),  # nullable BIGINT
            StructField("val", DoubleType(), True),
            StructField("group", StringType(), False),
        ]
    )
    # Control: single large cell, no null.
    df_control = spark.createDataFrame([(large_cell_id, 7.0, "control")], schema)
    # Test: same large cell + a null-cellid row in the same group.
    df_test = spark.createDataFrame(
        [(large_cell_id, 7.0, "test"), (None, 99.0, "test")], schema
    )

    def _rasterize(df):
        return (
            df.groupBy("group")
            .agg(rx.rst_h3_rasterize_agg("cellid", "val").alias("tile"))
            .collect()[0]["tile"]["raster"]
        )

    raster_control = bytes(_rasterize(df_control))
    raster_test = bytes(_rasterize(df_test))

    with _serde.open_tile(raster_control) as ds_ctrl:
        arr_ctrl = ds_ctrl.read(1)
        nodata = ds_ctrl.nodata

    with _serde.open_tile(raster_test) as ds_test:
        arr_test = ds_test.read(1)

    # The burned pixels (non-nodata) must match exactly.  Before the fix, the
    # rounded cellid maps to a different cell → different pixel position → mismatch.
    burned_ctrl = set(zip(*np.where(arr_ctrl != nodata)))
    burned_test = set(zip(*np.where(arr_test != nodata)))
    assert burned_ctrl == burned_test, (
        f"Null-cellid float64 precision loss corrupted the surviving cell id: "
        f"control pixels={burned_ctrl}, test pixels={burned_test}.  "
        f"large_cell_id={large_cell_id}"
    )
    # The burned value must be 7.0 (not 99.0 from the null-cellid row).
    for r, c in burned_test:
        assert (
            arr_test[r, c] == 7.0
        ), f"Expected value 7.0 at ({r},{c}) but got {arr_test[r, c]}"
