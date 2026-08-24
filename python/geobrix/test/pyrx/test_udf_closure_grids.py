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


def test_factory_captures_registered_dirs(spark):  # noqa: ARG001 — activates SparkSession
    proj_grids.set_registered_dirs(["/Volumes/a/grids"])
    udf = _udf.tile_scalar_udf(core_fn=lambda ds: 1, return_type=IntegerType())
    assert ("/Volumes/a/grids",) in _closure_consts(udf)


def test_factory_captures_empty_when_none_registered(spark):  # noqa: ARG001
    udf = _udf.tile_scalar_udf(core_fn=lambda ds: 1, return_type=IntegerType())
    assert () in _closure_consts(udf)
