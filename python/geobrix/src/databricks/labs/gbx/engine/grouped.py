"""The engine core of the Python-only `*_grouped` DataFrame helpers: GDAL's algorithm on one open tile's bytes."""

from __future__ import annotations

from typing import Any, Callable, Mapping


def core(sql_name: str, values: Mapping[str, Any]) -> Callable:
    """The per-tile body a `*_grouped` helper runs on an open dataset: the engine on that tile's GeoTIFF bytes.

    ``values`` are the row's parameters by name (the same names the SQL function binds), for example
    ``{"unit": "degrees", "xscale": None, "yscale": None}`` for ``gbx_rst_slope``.
    """
    from databricks.labs.gbx import manifest
    from databricks.labs.gbx.core import proj_grids
    from databricks.labs.gbx.engine import l3, run

    fn = manifest.load().functions[sql_name]
    grid_dirs = tuple(
        proj_grids.get_registered_dirs()
    )  # the driver's, carried to the worker in the closure
    params = dict(values)

    def _core(ds, cellid):
        from databricks.labs.gbx.pyrx import functions as prx

        l3._setup(grid_dirs)
        return l3._tile(
            run.run(fn, [prx._dataset_to_gtiff_bytes(ds)], dict(params)), cellid
        )

    return _core
