"""Python API for GeoBrix transactional Zarr stores.

The functions are added to this module as each capability lands: ``gbx_zarr_read``,
``gbx_zarr_sample``, ``gbx_zarr_write``, ``gbx_zarr_register``, ``gbx_zarr_sync``,
``gbx_zarr_optimize``, ``gbx_zarr_vacuum``, ``gbx_zarr_tag``, ``gbx_zarr_untag``,
``gbx_zarr_history``, ``gbx_zarr_restore`` and ``gbx_zarr_export``. Import them from
``databricks.labs.gbx.zarr``. They run in the light tier and need no JAR.
"""
