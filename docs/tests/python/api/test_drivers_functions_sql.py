"""Runs the SQL example of ``gbx_drivers`` against the live table function."""

import pytest

from . import drivers_functions_sql as ex


@pytest.fixture(scope="module")
def drivers_registered(spark):
    from databricks.labs.gbx import manager
    from databricks.labs.gbx.engine import run

    if run.unavailable_reason():
        pytest.skip(run.unavailable_reason())
    manager.register(spark, only=["gbx_drivers"])


def test_drivers_sql_example_lists_vector_writers(spark, drivers_registered):
    rows = spark.sql(ex.drivers_sql_example().strip().rstrip(";")).collect()
    names = [r["driver"] for r in rows]
    assert names == sorted(names), "the example orders by driver"
    assert "ESRI Shapefile" in names and "GeoJSON" in names
    assert "AAIGrid" not in names, "a raster driver is not in the vector listing"
    assert all(r["extensions"] is not None for r in rows)
