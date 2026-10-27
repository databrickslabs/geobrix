"""A cell id that repeats in a group burns the LARGEST of its values, whatever the row order.

The four cell rasterizers (``rst_{h3,quadbin,bng,custom}_rasterize_agg``) take one
``(cellid, value)`` per row. When the same cell appears more than once with different
values, the lightweight tier used to keep whichever row arrived last, and the arrival
order inside a Spark group is not fixed, so the raster could change from run to run. The
heavyweight tier sorts the group by ``(cellid, value)`` before the lookup is built, so the
largest value wins; the lightweight tier now follows the same rule.

The UDF bodies are called directly (``.func``) so the row order is exactly the order given.
A null value is the presence mask (1.0) and is compared as 1.0, like any other value.
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
        assert covered <= set(expected.values()), (
            f"{grid} ({name}): a smaller duplicate was burned: "
            f"{sorted(covered - set(expected.values()))}"
        )

    # The same cells in another order give the same raster, pixel for pixel.
    for name in ("reversed", "shuffled"):
        assert np.array_equal(
            arrays["as given"], arrays[name]
        ), f"{grid}: raster differs between the row order 'as given' and '{name}'"
