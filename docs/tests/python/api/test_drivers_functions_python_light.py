"""Runs the Python example of ``drivers()`` against the live table function."""

import pytest

try:
    from . import drivers_functions_python_light as ex
except (ModuleNotFoundError, ImportError):
    import drivers_functions_python_light as ex


def test_drivers_python_example(spark):
    from databricks.labs.gbx.engine import run

    if run.unavailable_reason():
        pytest.skip(run.unavailable_reason())
    rows = ex.drivers_python_example(spark).collect()
    names = [r["driver"] for r in rows]
    assert names == sorted(names)
    assert "GeoJSON" in names and "AAIGrid" not in names
