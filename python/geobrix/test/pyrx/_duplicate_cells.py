"""Shared fixtures for the duplicate-cell rule of the four cell rasterizers.

A cell id that repeats in a group burns the LARGEST of its values. These builders give the
unit test (the lightweight UDF bodies, row order controlled) and the cross-tier parity test
(both tiers through Spark) the same cells, the same repeated rows and the same expectation.
``null_rows`` is the lightweight tier only: the heavyweight tier still counts a null value as
``1.0`` in the comparison, so the null rule is not part of the cross-tier rows.
"""

import numpy as np

# Every "largest" value is above 5000 and every smaller duplicate is below 3000, so a
# smaller duplicate can never be mistaken for the expected one. One special cell burns
# -2.0 (largest means largest, not magnitude).
LOW_A, LOW_B, TOP = 1000.0, 2000.0, 5000.0
SMALLER = {LOW_A + i for i in range(6)} | {LOW_B + i for i in range(6)} | {-5.0}

# The custom grid of the custom-grid parity suite: 60 km square, resolution 1 = 10 km cells.
CUSTOM_GRID = dict(
    bound_x_min=510000,
    bound_x_max=570000,
    bound_y_min=165000,
    bound_y_max=225000,
    cell_splits=2,
    root_cell_size_x=20000,
    root_cell_size_y=20000,
    srid=27700,
)
CUSTOM_PIXEL_SIZE = (
    5000.0  # one pixel on each cell of the 2 x 2 block, no ring (kring_pad 0)
)


def h3_cells():
    import h3

    poly = h3.LatLngPoly([(0.0, 0.0), (0.0, 0.02), (0.02, 0.02), (0.02, 0.0)])
    return sorted(int(h3.str_to_int(c)) for c in h3.polygon_to_cells(poly, 9))[:6]


def quadbin_cells():
    from shapely import set_srid, to_wkb
    from shapely.geometry import box

    from databricks.labs.gbx.pygx import _quadbin as qb

    ewkb = to_wkb(set_srid(box(-0.13, 51.50, -0.06, 51.55), 4326), include_srid=True)
    return sorted(int(c) for c in qb.polyfill(ewkb, 12))[:6]


def bng_cells():
    from shapely.geometry import box

    from databricks.labs.gbx.pygx import _bng as bng

    poly = box(530000, 180000, 534000, 183000)
    return sorted(bng.format(c) for c in bng.polyfill(poly, 3))[:6]


def custom_cells():
    from shapely.geometry import box

    from databricks.labs.gbx.pygx._custom import CustomGridConf
    from databricks.labs.gbx.pygx._custom import polyfill as custom_polyfill

    conf = CustomGridConf(**CUSTOM_GRID)
    # The inner 2 x 2 block of resolution-1 cells, far from the grid bounds.
    poly = box(522000, 187000, 539000, 203000)
    return sorted(int(c) for c in custom_polyfill(conf, poly, 1))


GRIDS = {
    "h3": h3_cells,
    "quadbin": quadbin_cells,
    "bng": bng_cells,
    "custom": custom_cells,
}


def duplicate_rows(cells):
    """``([(cell, value)], {cell: expected burn})`` with every cell repeated."""
    rows, expected = [], {}
    for i, cell in enumerate(cells[:-1]):
        top = TOP + i
        rows += [(cell, LOW_A + i), (cell, top), (cell, LOW_B + i)]
        expected[cell] = top
    negative_cell = cells[-1]
    rows += [(negative_cell, -5.0), (negative_cell, -2.0)]
    expected[negative_cell] = -2.0
    return rows, expected


def null_rows(cells):
    """``([(cell, value)], {cell: expected burn})`` for cells that hold a null (``NaN``).

    A null never wins, so a cell with a number burns the largest number, and a cell
    whose every value is null burns ``NaN``. The other cell is a plain control.
    """
    with_number, only_null, mixed, plain = cells[:4]
    rows = [(with_number, 0.5), (with_number, np.nan)]
    rows += [(only_null, np.nan), (only_null, np.nan)]
    rows += [(mixed, np.nan), (mixed, -3.0), (mixed, -7.0), (mixed, np.nan)]
    rows += [(plain, 4.0)]
    return rows, {with_number: 0.5, only_null: np.nan, mixed: -3.0, plain: 4.0}
