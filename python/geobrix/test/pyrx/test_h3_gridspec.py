import h3
import pytest
from pyspark.sql.types import LongType, StringType, StructField, StructType

from databricks.labs.gbx.rasterx import helpers as rx_helpers


def test_rst_h3_gridspec_matches_core(spark):
    res = 9
    poly = h3.LatLngPoly([(0.0, 0.0), (0.0, 0.02), (0.02, 0.02), (0.02, 0.0)])
    cells = [h3.str_to_int(c) for c in h3.polygon_to_cells(poly, res)]
    df = spark.createDataFrame([(int(c), "TX1") for c in cells], ["cellid", "tx"])
    out = rx_helpers.rst_h3_gridspec(df, "cellid", "tx", pixel_size=0.005).collect()
    assert len(out) == 1
    g = out[0]["grid"]
    from databricks.labs.gbx.pyrx.core import cellraster as cr

    exp = cr.compute_gridspec(cells, pixel_size=0.005)
    assert g["width"] == exp[5] and g["height"] == exp[6]
    assert abs(g["xmin"] - exp[0]) < 1e-9


def test_rst_h3_gridspec_auto_pixel_size_matches_core(spark):
    """Auto pixel_size=None path must reproduce compute_gridspec exactly."""
    res = 9
    poly = h3.LatLngPoly([(0.0, 0.0), (0.0, 0.02), (0.02, 0.02), (0.02, 0.0)])
    cells = [h3.str_to_int(c) for c in h3.polygon_to_cells(poly, res)]
    df = spark.createDataFrame([(int(c), "TX1") for c in cells], ["cellid", "tx"])
    # No pixel_size: exercises the auto edge-length-from-resolution + lat-cosine path.
    out = rx_helpers.rst_h3_gridspec(df, "cellid", "tx").collect()
    assert len(out) == 1
    g = out[0]["grid"]

    from databricks.labs.gbx.pyrx.core import cellraster as cr

    exp = cr.compute_gridspec(cells)  # same default: no pixel_size
    assert g["width"] == exp[5], f"width {g['width']} != {exp[5]}"
    assert g["height"] == exp[6], f"height {g['height']} != {exp[6]}"
    assert abs(g["xmin"] - exp[0]) < 1e-9, f"xmin {g['xmin']} != {exp[0]}"
    assert abs(g["ymin"] - exp[1]) < 1e-9, f"ymin {g['ymin']} != {exp[1]}"
    assert (
        abs(g["pixel_size"] - exp[4]) < 1e-9
    ), f"pixel_size {g['pixel_size']} != {exp[4]}"


def test_rst_h3_gridspec_empty_raises(spark):
    """Empty input DataFrame must raise ValueError (mirrors compute_gridspec)."""
    schema = StructType(
        [StructField("cellid", LongType(), True), StructField("tx", StringType(), True)]
    )
    empty_df = spark.createDataFrame([], schema)
    with pytest.raises(ValueError, match="empty cell set"):
        rx_helpers.rst_h3_gridspec(empty_df, "cellid", "tx")
