"""pyrx: the internal implementation package behind ``databricks.labs.gbx.rasterx``.

Nothing here is public API: the API is ``databricks.labs.gbx.rasterx`` (and ``gridx``, ``vectorx``).
What this package used to export moved there, and an old path raises with a ``Fix:`` that names the new one.
Importing the package configures the bundled GDAL/PROJ environment on the driver; worker processes call
``configure_gdal_env()`` again inside each UDF body.
"""

from databricks.labs.gbx import _moved
from databricks.labs.gbx.pyrx._env import configure_gdal_env

configure_gdal_env()

_RX = "databricks.labs.gbx.rasterx"

__getattr__ = _moved.install(
    __name__,
    {
        # modules
        **{
            m: f"{_RX}.{m}"
            for m in (
                "checkpoint",
                "mvs",
                "ortho",
                "sfm",
                "imagery",
                "file_table",
                "helpers",
            )
        },
        # names the package exported
        **{
            n: f"{_RX}.checkpoint.{n}"
            for n in ("Manifest", "checkpoint_skip", "input_signature")
        },
        **{
            n: f"{_RX}.mvs.{n}"
            for n in (
                "dense_fuse",
                "dense_mvs_pool",
                "dense_patch_match",
                "dense_reconstruct_clusters",
                "dense_undistort",
                "gpu_infra",
                "recommend_dense_allocation",
            )
        },
        "tile_range": f"{_RX}.helpers.tile_range",
        "rst_viewshed_towers": f"{_RX}.helpers.rst_viewshed_towers",
    },
)
