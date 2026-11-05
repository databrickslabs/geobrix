"""The UC forms of raster to grid: for each grid (H3, quadbin, BNG, custom) a private routine per statistic that computes
ONLY that statistic, and a partial for the other grids than H3 (the H3 one is in ``uc.tier_b``).

The table function ``gbx_rst_<grid>_rastertogrid<stat>`` is ``inline`` of ``gbx__rst_<grid>_rastertogrid<stat>_cells``, an
array of ``(band, cellID, measure)``. The pass of one statistic is ``core.gridagg.raster_to_grid_single``: a sum or a
count does not make the M2, the minimum and the maximum the partial state holds, which costs about twice as much. The
partial stays for a statistic across many tiles, or several statistics of one tile, in one pass.
"""

import importlib
from typing import Callable, Dict

from databricks.labs.gbx.uc import handlers
from databricks.labs.gbx.uc.tier_b import (
    _CORES,
    _Parameter,
    h3_cover_cells_bound,
    with_dataset,
)

STATS = ("avg", "count", "max", "median", "min", "stddev", "sum", "variance")
GRIDS = ("h3", "quadbin", "bng", "custom")
_ROW_BYTES = (
    40  # band (4) + cellID (8, or a BNG string) + measure (8), with the row's overhead
)
_PARTIAL_ROW_BYTES = 64  # the same for the partial's seven fields
_RESOLUTIONS = {
    "h3": "a resolution from 0 to 15",
    "quadbin": "a resolution from 0 to 20",
    "bng": "a BNG resolution such as 3 or '1km'",
    "custom": "a resolution within the grid",
}
_LOWER = "use a coarser resolution, or retile the raster first (gbx_rst_retile) so each tile covers fewer cells"


def _run(
    fn,
    grid,
    stat,
    tile,
    resolution,
    coverage,
    assignment,
    grid_struct=None,
    partial=False,
):
    """The rows of one raster-to-grid call: ``stat`` alone, or (``partial``) the partial state; None for a NULL tile,
    resolution or grid, an error tile and unreadable bytes. A wrong argument raises with a Fix before the tile is read.
    """
    if tile is None or resolution is None or (grid == "custom" and grid_struct is None):
        return None
    gridagg = importlib.import_module(f"{_CORES}.gridagg")
    coverage = "complete" if coverage is None else coverage
    assignment = "centroid" if assignment is None else assignment
    if grid == "bng":
        resolution = handlers._bng_res(fn, resolution)
    else:
        resolution = int(resolution)
    if grid == "custom":
        handlers._custom_conf(fn, grid_struct)
    try:
        gridagg.check_arguments(
            resolution,
            grid,
            "count" if partial else stat,
            coverage,
            assignment,
            grid_struct,
        )
    except ValueError as exc:
        raise _Parameter(
            f"{fn}: {exc}. Fix: pass {_RESOLUTIONS[grid]}, a coverage of 'complete' or 'sparse', "
            "and an assignment of 'centroid' or 'covering'"
        ) from exc
    per_row = _PARTIAL_ROW_BYTES if partial else _ROW_BYTES
    text = grid == "bng"

    def kernel(ds):
        if (
            grid == "h3"
        ):  # the most cells the tile can make, from its bounds, before a pixel is binned
            bound = h3_cover_cells_bound(ds, resolution, coverage, assignment)
            if bound is not None:
                handlers._check_bytes(
                    fn,
                    bound * ds.count * per_row,
                    "the cells of the tile (an upper estimate)",
                    lower=_LOWER,
                )
        if partial:
            bands = gridagg.raster_to_grid_partial(
                ds, resolution, grid, coverage, assignment, grid_conf=grid_struct
            )
        else:
            bands = gridagg.raster_to_grid_single(
                ds, resolution, grid, stat, coverage, assignment, grid_conf=grid_struct
            )
        rows = []
        for band, cells in enumerate(bands, start=1):
            for c in cells:
                cell = c["cellID"] if text else int(c["cellID"])
                if partial:
                    rows.append(
                        {
                            "band": band,
                            "cellid": cell,
                            **{k: c[k] for k in gridagg.PARTIAL_FIELDS},
                        }
                    )
                else:
                    rows.append({"band": band, "cellID": cell, "measure": c["measure"]})
        handlers._check_bytes(
            fn, len(rows) * per_row, "the cells of the tile", lower=_LOWER
        )
        return rows

    return with_dataset(fn, tile, kernel)


def _cells(grid, stat, tile, resolution, coverage, assignment, grid_struct=None):
    fn = f"gbx_rst_{grid}_rastertogrid{stat}"
    return _run(fn, grid, stat, tile, resolution, coverage, assignment, grid_struct)


def _partial(grid, tile, resolution, coverage, assignment, grid_struct=None):
    fn = f"gbx_rst_{grid}_rastertogrid_partial"
    return _run(
        fn, grid, "count", tile, resolution, coverage, assignment, grid_struct, True
    )


# One function per grid and statistic, written out so a manifest ref resolves to a name the surface check finds.


def rst_h3_rastertogridavg_cells(tile, resolution, coverage, assignment):
    """gbx__rst_h3_rastertogridavg_cells: the avg alone of each h3 cell of the tile, per band, as (band, cellID, measure)."""
    return _cells("h3", "avg", tile, resolution, coverage, assignment)


def rst_h3_rastertogridcount_cells(tile, resolution, coverage, assignment):
    """gbx__rst_h3_rastertogridcount_cells: the count alone of each h3 cell of the tile, per band, as (band, cellID, measure)."""
    return _cells("h3", "count", tile, resolution, coverage, assignment)


def rst_h3_rastertogridmax_cells(tile, resolution, coverage, assignment):
    """gbx__rst_h3_rastertogridmax_cells: the max alone of each h3 cell of the tile, per band, as (band, cellID, measure)."""
    return _cells("h3", "max", tile, resolution, coverage, assignment)


def rst_h3_rastertogridmedian_cells(tile, resolution, coverage, assignment):
    """gbx__rst_h3_rastertogridmedian_cells: the median alone of each h3 cell of the tile, per band, as (band, cellID, measure)."""
    return _cells("h3", "median", tile, resolution, coverage, assignment)


def rst_h3_rastertogridmin_cells(tile, resolution, coverage, assignment):
    """gbx__rst_h3_rastertogridmin_cells: the min alone of each h3 cell of the tile, per band, as (band, cellID, measure)."""
    return _cells("h3", "min", tile, resolution, coverage, assignment)


def rst_h3_rastertogridstddev_cells(tile, resolution, coverage, assignment):
    """gbx__rst_h3_rastertogridstddev_cells: the stddev alone of each h3 cell of the tile, per band, as (band, cellID, measure)."""
    return _cells("h3", "stddev", tile, resolution, coverage, assignment)


def rst_h3_rastertogridsum_cells(tile, resolution, coverage, assignment):
    """gbx__rst_h3_rastertogridsum_cells: the sum alone of each h3 cell of the tile, per band, as (band, cellID, measure)."""
    return _cells("h3", "sum", tile, resolution, coverage, assignment)


def rst_h3_rastertogridvariance_cells(tile, resolution, coverage, assignment):
    """gbx__rst_h3_rastertogridvariance_cells: the variance alone of each h3 cell of the tile, per band, as (band, cellID, measure)."""
    return _cells("h3", "variance", tile, resolution, coverage, assignment)


def rst_quadbin_rastertogridavg_cells(tile, resolution, coverage, assignment):
    """gbx__rst_quadbin_rastertogridavg_cells: the avg alone of each quadbin cell of the tile, per band, as (band, cellID, measure)."""
    return _cells("quadbin", "avg", tile, resolution, coverage, assignment)


def rst_quadbin_rastertogridcount_cells(tile, resolution, coverage, assignment):
    """gbx__rst_quadbin_rastertogridcount_cells: the count alone of each quadbin cell of the tile, per band, as (band, cellID, measure)."""
    return _cells("quadbin", "count", tile, resolution, coverage, assignment)


def rst_quadbin_rastertogridmax_cells(tile, resolution, coverage, assignment):
    """gbx__rst_quadbin_rastertogridmax_cells: the max alone of each quadbin cell of the tile, per band, as (band, cellID, measure)."""
    return _cells("quadbin", "max", tile, resolution, coverage, assignment)


def rst_quadbin_rastertogridmedian_cells(tile, resolution, coverage, assignment):
    """gbx__rst_quadbin_rastertogridmedian_cells: the median alone of each quadbin cell of the tile, per band, as (band, cellID, measure)."""
    return _cells("quadbin", "median", tile, resolution, coverage, assignment)


def rst_quadbin_rastertogridmin_cells(tile, resolution, coverage, assignment):
    """gbx__rst_quadbin_rastertogridmin_cells: the min alone of each quadbin cell of the tile, per band, as (band, cellID, measure)."""
    return _cells("quadbin", "min", tile, resolution, coverage, assignment)


def rst_quadbin_rastertogridstddev_cells(tile, resolution, coverage, assignment):
    """gbx__rst_quadbin_rastertogridstddev_cells: the stddev alone of each quadbin cell of the tile, per band, as (band, cellID, measure)."""
    return _cells("quadbin", "stddev", tile, resolution, coverage, assignment)


def rst_quadbin_rastertogridsum_cells(tile, resolution, coverage, assignment):
    """gbx__rst_quadbin_rastertogridsum_cells: the sum alone of each quadbin cell of the tile, per band, as (band, cellID, measure)."""
    return _cells("quadbin", "sum", tile, resolution, coverage, assignment)


def rst_quadbin_rastertogridvariance_cells(tile, resolution, coverage, assignment):
    """gbx__rst_quadbin_rastertogridvariance_cells: the variance alone of each quadbin cell of the tile, per band, as (band, cellID, measure)."""
    return _cells("quadbin", "variance", tile, resolution, coverage, assignment)


def rst_quadbin_rastertogrid_partial(tile, resolution, coverage, assignment):
    """gbx_rst_quadbin_rastertogrid_partial: the mergeable state of each quadbin cell of the tile, per band, in one pass."""
    return _partial("quadbin", tile, resolution, coverage, assignment)


def rst_bng_rastertogridavg_cells(tile, resolution, coverage, assignment):
    """gbx__rst_bng_rastertogridavg_cells: the avg alone of each bng cell of the tile, per band, as (band, cellID, measure)."""
    return _cells("bng", "avg", tile, resolution, coverage, assignment)


def rst_bng_rastertogridcount_cells(tile, resolution, coverage, assignment):
    """gbx__rst_bng_rastertogridcount_cells: the count alone of each bng cell of the tile, per band, as (band, cellID, measure)."""
    return _cells("bng", "count", tile, resolution, coverage, assignment)


def rst_bng_rastertogridmax_cells(tile, resolution, coverage, assignment):
    """gbx__rst_bng_rastertogridmax_cells: the max alone of each bng cell of the tile, per band, as (band, cellID, measure)."""
    return _cells("bng", "max", tile, resolution, coverage, assignment)


def rst_bng_rastertogridmedian_cells(tile, resolution, coverage, assignment):
    """gbx__rst_bng_rastertogridmedian_cells: the median alone of each bng cell of the tile, per band, as (band, cellID, measure)."""
    return _cells("bng", "median", tile, resolution, coverage, assignment)


def rst_bng_rastertogridmin_cells(tile, resolution, coverage, assignment):
    """gbx__rst_bng_rastertogridmin_cells: the min alone of each bng cell of the tile, per band, as (band, cellID, measure)."""
    return _cells("bng", "min", tile, resolution, coverage, assignment)


def rst_bng_rastertogridstddev_cells(tile, resolution, coverage, assignment):
    """gbx__rst_bng_rastertogridstddev_cells: the stddev alone of each bng cell of the tile, per band, as (band, cellID, measure)."""
    return _cells("bng", "stddev", tile, resolution, coverage, assignment)


def rst_bng_rastertogridsum_cells(tile, resolution, coverage, assignment):
    """gbx__rst_bng_rastertogridsum_cells: the sum alone of each bng cell of the tile, per band, as (band, cellID, measure)."""
    return _cells("bng", "sum", tile, resolution, coverage, assignment)


def rst_bng_rastertogridvariance_cells(tile, resolution, coverage, assignment):
    """gbx__rst_bng_rastertogridvariance_cells: the variance alone of each bng cell of the tile, per band, as (band, cellID, measure)."""
    return _cells("bng", "variance", tile, resolution, coverage, assignment)


def rst_bng_rastertogrid_partial(tile, resolution, coverage, assignment):
    """gbx_rst_bng_rastertogrid_partial: the mergeable state of each bng cell of the tile, per band, in one pass."""
    return _partial("bng", tile, resolution, coverage, assignment)


def rst_custom_rastertogridavg_cells(
    tile, grid_struct, resolution, coverage, assignment
):
    """gbx__rst_custom_rastertogridavg_cells: the avg alone of each custom cell of the tile, per band, as (band, cellID, measure)."""
    return _cells("custom", "avg", tile, resolution, coverage, assignment, grid_struct)


def rst_custom_rastertogridcount_cells(
    tile, grid_struct, resolution, coverage, assignment
):
    """gbx__rst_custom_rastertogridcount_cells: the count alone of each custom cell of the tile, per band, as (band, cellID, measure)."""
    return _cells(
        "custom", "count", tile, resolution, coverage, assignment, grid_struct
    )


def rst_custom_rastertogridmax_cells(
    tile, grid_struct, resolution, coverage, assignment
):
    """gbx__rst_custom_rastertogridmax_cells: the max alone of each custom cell of the tile, per band, as (band, cellID, measure)."""
    return _cells("custom", "max", tile, resolution, coverage, assignment, grid_struct)


def rst_custom_rastertogridmedian_cells(
    tile, grid_struct, resolution, coverage, assignment
):
    """gbx__rst_custom_rastertogridmedian_cells: the median alone of each custom cell of the tile, per band, as (band, cellID, measure)."""
    return _cells(
        "custom", "median", tile, resolution, coverage, assignment, grid_struct
    )


def rst_custom_rastertogridmin_cells(
    tile, grid_struct, resolution, coverage, assignment
):
    """gbx__rst_custom_rastertogridmin_cells: the min alone of each custom cell of the tile, per band, as (band, cellID, measure)."""
    return _cells("custom", "min", tile, resolution, coverage, assignment, grid_struct)


def rst_custom_rastertogridstddev_cells(
    tile, grid_struct, resolution, coverage, assignment
):
    """gbx__rst_custom_rastertogridstddev_cells: the stddev alone of each custom cell of the tile, per band, as (band, cellID, measure)."""
    return _cells(
        "custom", "stddev", tile, resolution, coverage, assignment, grid_struct
    )


def rst_custom_rastertogridsum_cells(
    tile, grid_struct, resolution, coverage, assignment
):
    """gbx__rst_custom_rastertogridsum_cells: the sum alone of each custom cell of the tile, per band, as (band, cellID, measure)."""
    return _cells("custom", "sum", tile, resolution, coverage, assignment, grid_struct)


def rst_custom_rastertogridvariance_cells(
    tile, grid_struct, resolution, coverage, assignment
):
    """gbx__rst_custom_rastertogridvariance_cells: the variance alone of each custom cell of the tile, per band, as (band, cellID, measure)."""
    return _cells(
        "custom", "variance", tile, resolution, coverage, assignment, grid_struct
    )


def rst_custom_rastertogrid_partial(
    tile, grid_struct, resolution, coverage, assignment
):
    """gbx_rst_custom_rastertogrid_partial: the mergeable state of each custom cell of the tile, per band, in one pass."""
    return _partial("custom", tile, resolution, coverage, assignment, grid_struct)


HANDLERS: Dict[str, Callable] = {
    "gbx__rst_h3_rastertogridavg_cells": rst_h3_rastertogridavg_cells,
    "gbx__rst_h3_rastertogridcount_cells": rst_h3_rastertogridcount_cells,
    "gbx__rst_h3_rastertogridmax_cells": rst_h3_rastertogridmax_cells,
    "gbx__rst_h3_rastertogridmedian_cells": rst_h3_rastertogridmedian_cells,
    "gbx__rst_h3_rastertogridmin_cells": rst_h3_rastertogridmin_cells,
    "gbx__rst_h3_rastertogridstddev_cells": rst_h3_rastertogridstddev_cells,
    "gbx__rst_h3_rastertogridsum_cells": rst_h3_rastertogridsum_cells,
    "gbx__rst_h3_rastertogridvariance_cells": rst_h3_rastertogridvariance_cells,
    "gbx__rst_quadbin_rastertogridavg_cells": rst_quadbin_rastertogridavg_cells,
    "gbx__rst_quadbin_rastertogridcount_cells": rst_quadbin_rastertogridcount_cells,
    "gbx__rst_quadbin_rastertogridmax_cells": rst_quadbin_rastertogridmax_cells,
    "gbx__rst_quadbin_rastertogridmedian_cells": rst_quadbin_rastertogridmedian_cells,
    "gbx__rst_quadbin_rastertogridmin_cells": rst_quadbin_rastertogridmin_cells,
    "gbx__rst_quadbin_rastertogridstddev_cells": rst_quadbin_rastertogridstddev_cells,
    "gbx__rst_quadbin_rastertogridsum_cells": rst_quadbin_rastertogridsum_cells,
    "gbx__rst_quadbin_rastertogridvariance_cells": rst_quadbin_rastertogridvariance_cells,
    "gbx_rst_quadbin_rastertogrid_partial": rst_quadbin_rastertogrid_partial,
    "gbx__rst_bng_rastertogridavg_cells": rst_bng_rastertogridavg_cells,
    "gbx__rst_bng_rastertogridcount_cells": rst_bng_rastertogridcount_cells,
    "gbx__rst_bng_rastertogridmax_cells": rst_bng_rastertogridmax_cells,
    "gbx__rst_bng_rastertogridmedian_cells": rst_bng_rastertogridmedian_cells,
    "gbx__rst_bng_rastertogridmin_cells": rst_bng_rastertogridmin_cells,
    "gbx__rst_bng_rastertogridstddev_cells": rst_bng_rastertogridstddev_cells,
    "gbx__rst_bng_rastertogridsum_cells": rst_bng_rastertogridsum_cells,
    "gbx__rst_bng_rastertogridvariance_cells": rst_bng_rastertogridvariance_cells,
    "gbx_rst_bng_rastertogrid_partial": rst_bng_rastertogrid_partial,
    "gbx__rst_custom_rastertogridavg_cells": rst_custom_rastertogridavg_cells,
    "gbx__rst_custom_rastertogridcount_cells": rst_custom_rastertogridcount_cells,
    "gbx__rst_custom_rastertogridmax_cells": rst_custom_rastertogridmax_cells,
    "gbx__rst_custom_rastertogridmedian_cells": rst_custom_rastertogridmedian_cells,
    "gbx__rst_custom_rastertogridmin_cells": rst_custom_rastertogridmin_cells,
    "gbx__rst_custom_rastertogridstddev_cells": rst_custom_rastertogridstddev_cells,
    "gbx__rst_custom_rastertogridsum_cells": rst_custom_rastertogridsum_cells,
    "gbx__rst_custom_rastertogridvariance_cells": rst_custom_rastertogridvariance_cells,
    "gbx_rst_custom_rastertogrid_partial": rst_custom_rastertogrid_partial,
}
