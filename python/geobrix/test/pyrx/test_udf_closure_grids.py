"""TDD: verify that light UDF factories embed registered PROJ grid dirs in their closures."""

import pytest
from pyspark.sql.types import IntegerType

from databricks.labs.gbx.core import proj_grids
from databricks.labs.gbx.pyrx import _udf


@pytest.fixture(autouse=True)
def _clear():
    proj_grids.set_registered_dirs([], replace=True)
    yield
    proj_grids.set_registered_dirs([], replace=True)


def _closure_consts(fn):
    # pandas_udf/udf wrap the inner function; reach the wrapped callable's constants.
    inner = getattr(fn, "func", None) or getattr(fn, "__wrapped__", None) or fn
    consts = set()
    code = getattr(inner, "__code__", None)
    if code:
        consts |= set(c for c in code.co_consts if isinstance(c, tuple))
    # closure freevars
    if getattr(inner, "__closure__", None):
        for cell in inner.__closure__:
            try:
                v = cell.cell_contents
            except ValueError:
                continue
            if isinstance(v, tuple):
                consts.add(v)
    return consts


# ---------------------------------------------------------------------------
# Task 4 (original): _udf.py pandas_udf/scalar_udf factories
# ---------------------------------------------------------------------------


def test_factory_captures_registered_dirs(
    spark,
):  # noqa: ARG001 — activates SparkSession
    proj_grids.set_registered_dirs(["/Volumes/a/grids"])
    udf = _udf.tile_scalar_udf(core_fn=lambda ds: 1, return_type=IntegerType())
    assert ("/Volumes/a/grids",) in _closure_consts(udf)


def test_factory_captures_empty_when_none_registered(spark):  # noqa: ARG001
    udf = _udf.tile_scalar_udf(core_fn=lambda ds: 1, return_type=IntegerType())
    assert () in _closure_consts(udf)


# ---------------------------------------------------------------------------
# Fix: pyrx functions.py raster CRS-transform / clip factory UDFs
# ---------------------------------------------------------------------------


def test_transformcrs_sql_factory_captures_dirs(spark):
    """_build_transformcrs_sql_udf captures registered dirs into the UDF closure."""
    proj_grids.set_registered_dirs(["/Volumes/proj/grids"])
    from databricks.labs.gbx.pyrx import functions as prx

    _grid_dirs = tuple(proj_grids.get_registered_dirs())
    udf = prx._build_transformcrs_sql_udf(_grid_dirs)
    assert ("/Volumes/proj/grids",) in _closure_consts(udf)


def test_transformcrs_column_api_factory_captures_dirs(spark):
    """_build_uf_transformcrs (used by rst_transformcrs per-call) captures dirs."""
    proj_grids.set_registered_dirs(["/Volumes/proj/grids"])
    from databricks.labs.gbx.pyrx import functions as prx

    _grid_dirs = tuple(proj_grids.get_registered_dirs())
    udf = prx._build_uf_transformcrs(_grid_dirs)
    assert ("/Volumes/proj/grids",) in _closure_consts(udf)


def test_transformcrs_factories_empty_when_none_registered(spark):
    """Both transformcrs factories capture empty tuple when no dirs registered."""
    from databricks.labs.gbx.pyrx import functions as prx

    sql_udf = prx._build_transformcrs_sql_udf(())
    col_udf = prx._build_uf_transformcrs(())
    assert () in _closure_consts(sql_udf)
    assert () in _closure_consts(col_udf)
