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
burns ``NaN``. An omitted value is not a null column: the UDF receives an object column of
``None``, and that is the presence mask, 1.0 for every cell.
"""

import random

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("rasterio")

from test.pyrx import _duplicate_cells as dc  # noqa: E402

from databricks.labs.gbx.pyrx import _serde  # noqa: E402
from databricks.labs.gbx.pyrx import functions as rx  # noqa: E402


def _burn(grid, rows, omitted=False):
    """The first band of the tile one group of ``rows`` produces, in the order given.

    ``omitted``: the value was left out, which reaches the UDF as an object column of None
    (a typed Double column of nulls is float64 NaN).
    """
    cellid = pd.Series(
        [c for c, _ in rows], dtype="object" if grid == "bng" else "int64"
    )
    value = (
        pd.Series([None] * len(rows), dtype=object)
        if omitted
        else pd.Series([v for _, v in rows], dtype="float64")
    )
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


@pytest.mark.parametrize("grid", sorted(dc.GRIDS))
def test_a_typed_column_of_nulls_burns_nan_but_an_omitted_value_is_the_presence_mask(
    grid,
):
    rows = [(c, np.nan) for c in dc.GRIDS[grid]()[:3]]
    typed, nodata = _burn(grid, rows)
    burned = typed[typed != nodata]
    assert burned.size and np.isnan(burned).all(), f"{grid}: typed nulls must burn NaN"
    omitted, nodata = _burn(grid, rows, omitted=True)
    assert set(omitted[omitted != nodata].tolist()) == {1.0}, f"{grid}: no mask"


def test_two_spellings_of_one_bng_cell_burn_the_largest_value_in_any_order():
    cells = dc.GRIDS["bng"]()
    spaced = cells[0][:2] + " " + cells[0][2:]  # TQ3080 and TQ 3080 are one cell
    others = [(c, 4.0 + i) for i, c in enumerate(cells[1:4])]
    rows = [(cells[0], 9.0), (spaced, 2.0)] + others
    for order in (rows, rows[::-1]):
        pixels, nodata = _burn("bng", order)
        burned = set(pixels[pixels != nodata].tolist())
        assert 9.0 in burned and 2.0 not in burned, burned
