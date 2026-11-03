"""
Tests for PMTiles SQL examples.

Ensures all SQL examples in `pmtiles_functions_sql.py` execute against the
real `gbx_pmtiles_agg` UDAF and produce valid PMTile v3 binary blobs.
"""
import struct
import pytest

from . import pmtiles_functions_sql


@pytest.fixture(scope="module")
def tiles_view(spark):
    """Create a test (z, x, y, bytes) view that the SQL examples reference."""
    from databricks.labs.gbx import manager

    manager.register(spark, only=["gbx_pmtiles_agg"])

    test_data = [
        (2, x, y, f"tile_{x}_{y}".encode("utf-8"))
        for x in range(3)
        for y in range(3)
    ]
    df = spark.createDataFrame(test_data, ["z", "x", "y", "bytes"])
    df.createOrReplaceTempView("tiles_z2")
    yield
    spark.catalog.dropTempView("tiles_z2")


def test_all_sql_functions_have_example():
    """Verify all expected SQL example functions exist in pmtiles_functions_sql."""
    expected_functions = [
        "pmtiles_agg_sql_example",
        "pmtiles_agg_4arg_sql_example",
    ]
    actual_functions = [
        name
        for name in dir(pmtiles_functions_sql)
        if name.endswith("_sql_example") and callable(getattr(pmtiles_functions_sql, name))
    ]
    missing = set(expected_functions) - set(actual_functions)
    assert not missing, f"missing SQL examples: {missing}"


def _validate_pmtile(blob):
    """Assert that `blob` is a well-formed PMTile v3 archive."""
    assert blob is not None
    assert blob[:7] == b"PMTiles", f"bad magic: {blob[:8]!r}"
    assert blob[7] == 3, f"bad version byte: {blob[7]}"
    addressed = struct.unpack_from("<Q", blob, 72)[0]
    assert addressed == 9, f"expected 9 addressed tiles; got {addressed}"


def test_pmtiles_agg_sql_example(spark, tiles_view):
    """Run the 5-arg SQL example and validate the resulting PMTile blob."""
    sql = pmtiles_functions_sql.pmtiles_agg_sql_example()
    row = spark.sql(sql).collect()[0]
    _validate_pmtile(row["pmt"])


def test_pmtiles_agg_4arg_sql_example(spark, tiles_view):
    """Run the 4-arg SQL example (default metadata) and validate the resulting PMTile blob."""
    sql = pmtiles_functions_sql.pmtiles_agg_4arg_sql_example()
    row = spark.sql(sql).collect()[0]
    _validate_pmtile(row["pmt"])
