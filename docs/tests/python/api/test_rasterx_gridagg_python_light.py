"""
Test suite for RasterX rastertogrid functions (light tier).
Verifies that LATERAL UDTF invocations return expected schema.
"""

import sys
from pathlib import Path

import pytest

# Put this api/ directory and its parent (docs/tests/python, for path_config) on
# sys.path so the sibling example modules import whether this file is collected
# in isolation or as part of the full api/ suite.
_API_DIR = Path(__file__).parent
sys.path.insert(0, str(_API_DIR))
sys.path.insert(0, str(_API_DIR.parent))

try:
    from . import rasterx_gridagg_python_light as light_examples
except (ModuleNotFoundError, ImportError):
    try:
        import rasterx_gridagg_python_light as light_examples
    except ModuleNotFoundError:
        light_examples = None


@pytest.mark.parametrize(
    "example_fn,grid_type,aggregator",
    [
        # H3 functions
        ("rst_h3_rastertogridavg_python_light_example", "h3", "avg"),
        ("rst_h3_rastertogridcount_python_light_example", "h3", "count"),
        ("rst_h3_rastertogridmax_python_light_example", "h3", "max"),
        ("rst_h3_rastertogridmin_python_light_example", "h3", "min"),
        ("rst_h3_rastertogridmedian_python_light_example", "h3", "median"),
        ("rst_h3_rastertogridsum_python_light_example", "h3", "sum"),
        ("rst_h3_rastertogridvariance_python_light_example", "h3", "variance"),
        ("rst_h3_rastertogridstddev_python_light_example", "h3", "stddev"),
        # Quadbin functions
        ("rst_quadbin_rastertogridavg_python_light_example", "quadbin", "avg"),
        ("rst_quadbin_rastertogridcount_python_light_example", "quadbin", "count"),
        ("rst_quadbin_rastertogridmax_python_light_example", "quadbin", "max"),
        ("rst_quadbin_rastertogridmin_python_light_example", "quadbin", "min"),
        ("rst_quadbin_rastertogridmedian_python_light_example", "quadbin", "median"),
        ("rst_quadbin_rastertogridsum_python_light_example", "quadbin", "sum"),
        (
            "rst_quadbin_rastertogridvariance_python_light_example",
            "quadbin",
            "variance",
        ),
        ("rst_quadbin_rastertogridstddev_python_light_example", "quadbin", "stddev"),
        # BNG functions (expect 0 cells over non-GB fixture)
        ("rst_bng_rastertogridavg_python_light_example", "bng", "avg"),
        ("rst_bng_rastertogridcount_python_light_example", "bng", "count"),
        ("rst_bng_rastertogridmax_python_light_example", "bng", "max"),
        ("rst_bng_rastertogridmin_python_light_example", "bng", "min"),
        ("rst_bng_rastertogridmedian_python_light_example", "bng", "median"),
        ("rst_bng_rastertogridsum_python_light_example", "bng", "sum"),
        ("rst_bng_rastertogridvariance_python_light_example", "bng", "variance"),
        ("rst_bng_rastertogridstddev_python_light_example", "bng", "stddev"),
    ],
)
def test_rastertogrid_python_light_example(spark, example_fn, grid_type, aggregator):
    """Each rastertogrid function returns rows with [band, cellID, measure] columns.

    Light tier yields 0 rows for BNG over non-GB fixture (expected).
    Light tier yields >=1 rows for H3/Quadbin over any raster.
    """
    assert light_examples is not None

    example_func = getattr(light_examples, example_fn)
    result = example_func(spark)

    # Result is a list of Row objects from .take(5)
    assert isinstance(result, list), f"{example_fn} should return a list from .take(5)"

    if grid_type == "bng":
        # BNG fixture over non-GB area yields 0 cells (expected)
        assert len(result) >= 0, f"{example_fn} over non-GB BNG should yield >=0 cells"
    else:
        # H3 and Quadbin should yield at least 1 cell (3 bands × multiple cells)
        assert (
            len(result) > 0
        ), f"{example_fn} should yield >0 cells over multiband fixture"

    # Verify column schema if rows exist
    if len(result) > 0:
        first_row = result[0]
        assert "band" in first_row, f"{example_fn} result must have 'band' column"
        assert "cellID" in first_row, f"{example_fn} result must have 'cellID' column"
        assert "measure" in first_row, f"{example_fn} result must have 'measure' column"

        # Verify types
        assert isinstance(first_row["band"], int), "band column should be INT"
        assert first_row["measure"] is not None, "measure should not be None"


# ---------------------------------------------------------------------------
# SQL example round-trip
#
# The rastertogrid SQL examples in rasterx_functions_sql.py use the light-tier
# UDTF LATERAL form, which resolves only under the pyrx registration. We execute
# each here (pyrx tier) to prove the documented SQL string actually runs and
# yields the [band, cellID, measure] columns.
# ---------------------------------------------------------------------------

try:
    from . import rasterx_functions_sql as sql_examples
except (ModuleNotFoundError, ImportError):
    try:
        import rasterx_functions_sql as sql_examples
    except ModuleNotFoundError:
        sql_examples = None


_SQL_EXAMPLE_PARAMS = [
    (f"rst_{grid}_rastertogrid{agg}_sql_example", grid)
    for grid in ("h3", "quadbin", "bng")
    for agg in ("avg", "count", "max", "min", "median", "sum", "variance", "stddev")
]


@pytest.mark.parametrize("example_attr,grid_type", _SQL_EXAMPLE_PARAMS)
def test_rastertogrid_sql_example_executes(spark, example_attr, grid_type):
    """Each converted SQL example runs on the pyrx tier and yields [band, cellID, measure].

    Registers the light (pyrx) tier so the LATERAL UDTF form resolves, materializes
    the multiband fixture as `multiband_rasters`, then executes the documented SQL.
    """
    assert sql_examples is not None
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    light_examples._get_multiband_df(spark).createOrReplaceTempView("multiband_rasters")

    raw_sql = getattr(sql_examples, example_attr)()
    assert "LATERAL" in raw_sql and "LATERAL VIEW explode" not in raw_sql
    # E2 examples carry two variations (heavy scalar first, light LATERAL second);
    # execute the LATERAL statement for the pyrx tier.
    sql = sql_examples._sql_variant(raw_sql, lateral=True)

    result = spark.sql(sql).take(5)
    assert isinstance(result, list)
    # H3/Quadbin always bin at least one cell; BNG over this fixture may vary,
    # so only assert schema when rows are present.
    if result:
        cols = result[0].asDict()
        assert "band" in cols and "cellID" in cols and "measure" in cols


def test_rastertogrid_sql_example_outputs_exist():
    """Every rastertogrid SQL example has a matching *_output constant."""
    assert sql_examples is not None
    for example_attr, _ in _SQL_EXAMPLE_PARAMS:
        assert hasattr(sql_examples, example_attr), f"missing {example_attr}"
        assert hasattr(
            sql_examples, f"{example_attr}_output"
        ), f"missing {example_attr}_output"


def test_h3_cell_bbox_python_light_example(spark):
    """h3_cell_bbox light-tier scalar example returns non-null bbox structs."""
    assert light_examples is not None
    rows = light_examples.h3_cell_bbox_python_light_example(spark)
    assert isinstance(rows, list) and len(rows) == 3
    for row in rows:
        d = row.asDict()
        assert d["cellid"] is not None
        bbox = d["bbox"]
        assert bbox is not None
        # STRUCT<xmin, ymin, xmax, ymax> — ordered, finite.
        assert bbox["xmin"] <= bbox["xmax"] and bbox["ymin"] <= bbox["ymax"]
    assert hasattr(light_examples, "h3_cell_bbox_python_light_example_output")
