"""Python API for GeoBrix transactional Zarr stores.

The functions are added to this module as each capability lands: ``gbx_zarr_read``,
``gbx_zarr_sample``, ``gbx_zarr_write``, ``gbx_zarr_register``, ``gbx_zarr_sync``,
``gbx_zarr_optimize``, ``gbx_zarr_vacuum``, ``gbx_zarr_tag``, ``gbx_zarr_untag``,
``gbx_zarr_history``, ``gbx_zarr_restore`` and ``gbx_zarr_export``. Import them from
``databricks.labs.gbx.zarr``. They run in the light tier and need no JAR.

``BLOCKS_SCHEMA``, ``block_to_numpy`` and ``block_to_xarray`` belong to the blocks that
``gbx_zarr_read`` returns.
"""

from databricks.labs.gbx.zarr._read import (
    BLOCKS_SCHEMA,
    block_to_numpy,
    block_to_xarray,
    gbx_zarr_read,
)
from databricks.labs.gbx.zarr._register import gbx_zarr_register
from databricks.labs.gbx.zarr._sample import gbx_zarr_sample
from databricks.labs.gbx.zarr._sync import gbx_zarr_sync
from databricks.labs.gbx.zarr._write import gbx_zarr_write

__all__ = [
    "BLOCKS_SCHEMA",
    "block_to_numpy",
    "block_to_xarray",
    "gbx_zarr_read",
    "gbx_zarr_register",
    "gbx_zarr_sample",
    "gbx_zarr_sync",
    "gbx_zarr_write",
]
