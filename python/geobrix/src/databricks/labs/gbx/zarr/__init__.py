"""GeoBrix transactional, versioned Zarr stores (light tier, no JAR).

Public names are imported lazily (PEP 562): importing ``databricks.labs.gbx.zarr``
never imports ``zarr``, ``numcodecs``, ``tensorstore``, ``obstore`` or PySpark.
Importing a name (``from databricks.labs.gbx.zarr import ZarrWriteConflict``) loads
only the module that defines it. Install the stack with the per-runtime extra:
``pip install 'geobrix[light_dbr19,zarr_dbr19]'`` on DBR 19, or
``geobrix[light_env6,zarr_env6]`` on Serverless environment 6.
"""

import importlib

# public name -> (module, attribute). Later tasks add their functions here.
_EXPORTS = {
    "ZarrError": ("databricks.labs.gbx.zarr.errors", "ZarrError"),
    "ZarrWriteConflict": ("databricks.labs.gbx.zarr.errors", "ZarrWriteConflict"),
    "ZarrStoreDrift": ("databricks.labs.gbx.zarr.errors", "ZarrStoreDrift"),
    "ZarrReadOnly": ("databricks.labs.gbx.zarr.errors", "ZarrReadOnly"),
    "ZarrGridMismatch": ("databricks.labs.gbx.zarr.errors", "ZarrGridMismatch"),
    "ZarrVersionUnavailable": (
        "databricks.labs.gbx.zarr.errors",
        "ZarrVersionUnavailable",
    ),
    "ZarrAccessUnavailable": (
        "databricks.labs.gbx.zarr.errors",
        "ZarrAccessUnavailable",
    ),
    "ZarrUnsupported": ("databricks.labs.gbx.zarr.errors", "ZarrUnsupported"),
    "ZarrOwnershipError": ("databricks.labs.gbx.zarr.errors", "ZarrOwnershipError"),
    "gbx_zarr_register": ("databricks.labs.gbx.zarr.functions", "gbx_zarr_register"),
    "gbx_zarr_read": ("databricks.labs.gbx.zarr.functions", "gbx_zarr_read"),
    "gbx_zarr_write": ("databricks.labs.gbx.zarr.functions", "gbx_zarr_write"),
    "BLOCKS_SCHEMA": ("databricks.labs.gbx.zarr.functions", "BLOCKS_SCHEMA"),
    "block_to_numpy": ("databricks.labs.gbx.zarr.functions", "block_to_numpy"),
    "block_to_xarray": ("databricks.labs.gbx.zarr.functions", "block_to_xarray"),
}

__all__ = list(_EXPORTS)


def __getattr__(name):
    try:
        module, attr = _EXPORTS[name]
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None
    value = getattr(importlib.import_module(module), attr)
    globals()[name] = value
    return value


def __dir__():
    return sorted([*globals(), *_EXPORTS])
