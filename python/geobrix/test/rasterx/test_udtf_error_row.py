"""Test that light streaming UDTFs yield exactly ONE error-tile row on corrupt input.

Cross-tier parity: heavy generators emit one error-tile row on empty/corrupt
input; light UDTFs must do the same so row-counts match.  Each test below
asserts len(rows) == 1 and raster is None — the exact same contract as the
heavy-tier generators (confirmed by RST_ErrorHandlingParityTest.scala on the
heavy side).

XYZPyramid and Polygonize UDTFs use different row schemas and are deliberately
left out of scope — they cannot use build_error_tile without a schema change.
"""

import logging

import pytest
from pyspark.sql import SparkSession

# All tests here exercise the light pyrx UDTF path (pandas_udf → pandas/rasterio),
# which the heavy CI phase does not install (requirements-ci.txt). Skip the whole
# module there; it runs in the light job + Docker where the deps are present.
pytest.importorskip("pandas")
pytest.importorskip("rasterio")


@pytest.fixture(scope="module")
def spark():
    logging.getLogger("py4j").setLevel(logging.ERROR)
    session = (
        SparkSession.builder.master("local[2]")
        .appName("pyrx-udtf-error-row-tests")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    yield session


def _corrupt_tile_df(spark):
    """One-row DataFrame with a tile struct carrying corrupt raster bytes.

    Constructs the struct directly so the corrupt bytes reach the UDTF
    without going through rst_fromcontent (which would raise before the UDTF).
    Mirrors the pattern in test_rst_tryopen_false_on_garbage.
    """
    return spark.createDataFrame(
        [((0, b"NOT A RASTER", {"driver": "GTiff"}),)],
        "tile struct<cellid:bigint,raster:binary,metadata:map<string,string>>",
    )


def test_separatebands_corrupt_yields_one_error_row(spark):
    """A corrupt tile fed to rst_separatebands yields exactly one error-tile row."""
    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark)
    df = _corrupt_tile_df(spark)
    df.createOrReplaceTempView("_udtf_err_sep")
    rows = spark.sql(
        "SELECT t.cellid, t.raster, t.metadata "
        "FROM _udtf_err_sep, LATERAL gbx_rst_separatebands(tile) t"
    ).collect()
    assert len(rows) == 1, f"expected 1 error row, got {len(rows)}"
    row = rows[0]
    assert row["raster"] is None, "error row must have raster=None"
    assert row["metadata"] is not None
    assert "last_error" in row["metadata"]
    assert (
        "RST_SeparateBands" in row["metadata"]["last_error"]
    ), f"expected 'RST_SeparateBands' in last_error, got: {row['metadata']['last_error']!r}"


def test_retile_corrupt_yields_one_error_row(spark):
    """A corrupt tile fed to rst_retile yields exactly one error-tile row."""
    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark)
    df = _corrupt_tile_df(spark)
    df.createOrReplaceTempView("_udtf_err_retile")
    rows = spark.sql(
        "SELECT t.cellid, t.raster, t.metadata "
        "FROM _udtf_err_retile, LATERAL gbx_rst_retile(tile, 256, 256) t"
    ).collect()
    assert len(rows) == 1, f"expected 1 error row, got {len(rows)}"
    row = rows[0]
    assert row["raster"] is None, "error row must have raster=None"
    assert row["metadata"] is not None
    assert "last_error" in row["metadata"]
    assert (
        "RST_ReTile" in row["metadata"]["last_error"]
    ), f"expected 'RST_ReTile' in last_error, got: {row['metadata']['last_error']!r}"


def test_tooverlappingtiles_corrupt_yields_one_error_row(spark):
    """A corrupt tile fed to rst_tooverlappingtiles yields exactly one error-tile row.

    Count-parity assertion: light UDTF error-row count (1) must match the
    heavy-tier generator contract (also 1), confirmed by
    RST_ErrorHandlingParityTest.scala on the heavy side.
    """
    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark)
    df = _corrupt_tile_df(spark)
    df.createOrReplaceTempView("_udtf_err_overlapping")
    rows = spark.sql(
        "SELECT t.cellid, t.raster, t.metadata "
        "FROM _udtf_err_overlapping, LATERAL gbx_rst_tooverlappingtiles(tile, 256, 256, 0) t"
    ).collect()
    assert len(rows) == 1, f"expected 1 error row, got {len(rows)}"
    row = rows[0]
    assert row["raster"] is None, "error row must have raster=None"
    assert row["metadata"] is not None
    assert "last_error" in row["metadata"]
    assert (
        "RST_ToOverlappingTiles" in row["metadata"]["last_error"]
    ), f"expected 'RST_ToOverlappingTiles' in last_error, got: {row['metadata']['last_error']!r}"
