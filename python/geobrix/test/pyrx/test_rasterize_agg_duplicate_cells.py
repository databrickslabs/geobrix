"""A cell id that repeats in a group burns the LARGEST of its values, whatever the row order.

The four cell rasterizers (``rst_{h3,quadbin,bng,custom}_rasterize_agg``) take one
``(cellid, value)`` per row. When the same cell appears more than once with different
values, the lightweight tier used to keep whichever row arrived last, and the arrival
order inside a Spark group is not fixed, so the raster could change from run to run. The
heavyweight tier sorts the group by ``(cellid, value)`` before the lookup is built, so the
largest value wins; the lightweight tier now follows the same rule.

The UDF bodies are called directly (``.func``) so the row order is exactly the order given.

A null value never wins: a typed Double column delivers a null as ``NaN``, which looks like
a real ``NaN``, so both are ignored while the cell has a number, and a cell with only nulls
burns ``NaN``. A group with no number at all is the presence mask and burns 1.0.
"""

import random

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("rasterio")

from test.pyrx import _duplicate_cells as dc  # noqa: E402

from databricks.labs.gbx.pyrx import _serde  # noqa: E402
from databricks.labs.gbx.pyrx import functions as rx  # noqa: E402


def _burn(grid, rows):
    """The first band of the tile one group of ``rows`` produces, in the order given."""
    cellid = pd.Series(
        [c for c, _ in rows], dtype="object" if grid == "bng" else "int64"
    )
    value = pd.Series([v for _, v in rows], dtype="float64")
    if grid == "custom":
        from pyspark.sql import Row

        udf = rx._rst_custom_rasterize_agg_udf
        # cellid, value, grid, then out_srid .. kring_pad (pixel_size at 4, kring_pad last).
        args = [cellid, value, pd.Series([Row(**dc.CUSTOM_GRID)])] + [None] * 10
        args[4], args[-1] = pd.Series([dc.CUSTOM_PIXEL_SIZE]), pd.Series([0])
    else:
        udf = getattr(rx, f"_rst_{grid}_rasterize_agg_udf")
        args = [cellid, value] + [None] * 10
    raster = udf.func(*args)
    assert raster is not None, f"{grid}: no raster for a group with valid cells"
    with _serde.open_tile(bytes(raster)) as ds:
        return ds.read(1), ds.nodata


def test_max_value_per_cell_keeps_the_largest_in_any_order():
    cells, vals = [7, 8, 7, 8, 7], [3.0, -1.0, 9.0, -4.0, 5.0]
    assert rx._max_value_per_cell(cells, vals) == {7: 9.0, 8: -1.0}
    assert rx._max_value_per_cell(cells[::-1], vals[::-1]) == {7: 9.0, 8: -1.0}
    assert rx._max_value_per_cell([], []) == {}


def test_max_value_per_cell_ignores_null_unless_every_value_is_null():
    nan = float("nan")
    cells, vals = [1, 1, 2, 2, 3], [0.5, nan, nan, None, 4.0]
    for order in (slice(None), slice(None, None, -1)):
        got = rx._max_value_per_cell(cells[order], vals[order])
        assert got[1] == 0.5 and got[3] == 4.0 and np.isnan(got[2])
    # No number anywhere (the value omitted, or a column of nulls): the presence mask.
    assert rx._max_value_per_cell([1, 2, 2], [None, nan, None]) == {1: 1.0, 2: 1.0}


def test_max_value_per_cell_merges_spellings_of_one_cell():
    # "TQ3080" and "TQ 3080" are one BNG cell: the largest value wins whatever the order.
    from databricks.labs.gbx.pyrx.core import cellraster as cr

    key = cr._adapter("bng").to_key
    cells, vals = ["TQ3080", "TQ 3080", "TQ3081"], [9.0, 2.0, 1.0]
    for order in (slice(None), slice(None, None, -1)):
        got = rx._max_value_per_cell(cells[order], vals[order], key)
        assert sorted(got.values()) == [1.0, 9.0] and len(got) == 2


@pytest.mark.parametrize("grid", sorted(dc.GRIDS))
def test_repeated_cells_burn_the_largest_value_in_any_row_order(grid):
    cells = dc.GRIDS[grid]()
    assert len(cells) >= 4, f"{grid}: need four cells, got {len(cells)}"
    rows, expected = dc.duplicate_rows(cells)

    shuffled = list(rows)
    random.Random(7).shuffle(shuffled)
    orders = {"as given": rows, "reversed": rows[::-1], "shuffled": shuffled}
    arrays = {}
    for name, order in orders.items():
        arrays[name], nodata = _burn(grid, order)
        covered = set(arrays[name][arrays[name] != nodata].tolist())
        assert covered, f"{grid} ({name}): nothing burned"
        assert covered == set(expected.values()), (
            f"{grid} ({name}): burned {sorted(covered)}, "
            f"expected exactly {sorted(set(expected.values()))}"
        )

    # The same cells in another order give the same raster, pixel for pixel.
    for name in ("reversed", "shuffled"):
        assert np.array_equal(
            arrays["as given"], arrays[name]
        ), f"{grid}: raster differs between the row order 'as given' and '{name}'"


@pytest.mark.parametrize("grid", sorted(dc.GRIDS))
def test_a_null_value_never_wins_a_repeated_cell(grid):
    cells = dc.GRIDS[grid]()
    rows, expected = dc.null_rows(cells)
    shuffled = list(rows)
    random.Random(11).shuffle(shuffled)
    arrays = {}
    for name, order in {
        "as given": rows,
        "reversed": rows[::-1],
        "shuffled": shuffled,
    }.items():
        arrays[name], nodata = _burn(grid, order)
        burned = arrays[name][arrays[name] != nodata]
        numbers = {v for v in burned.tolist() if v == v}
        assert numbers == {v for v in expected.values() if v == v}, f"{grid} ({name})"
        assert np.isnan(
            burned
        ).any(), f"{grid} ({name}): the all-null cell must burn NaN"
    for name in ("reversed", "shuffled"):
        assert np.array_equal(arrays["as given"], arrays[name], equal_nan=True)
