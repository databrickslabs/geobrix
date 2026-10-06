"""pyrx — lightweight, JAR-free raster API (PySpark + rasterio).

Pure-Python/PySpark sibling of the heavyweight ``rasterx`` package. Function
names and signatures mirror ``rasterx`` exactly so swapping the import is a
one-line upgrade/downgrade:

    from databricks.labs.gbx.rasterx import functions as rx   # heavyweight (JAR)
    from databricks.labs.gbx.pyrx import functions as prx     # lightweight (no JAR)
"""

from databricks.labs.gbx.pyrx._env import assert_rasterio_available, configure_gdal_env
from databricks.labs.gbx.pyrx.checkpoint import (
    Manifest,
    checkpoint_skip,
    input_signature,
)
from databricks.labs.gbx.pyrx.mvs import (
    dense_fuse,
    dense_mvs_pool,
    dense_patch_match,
    dense_reconstruct_clusters,
    dense_undistort,
    gpu_infra,
    recommend_dense_allocation,
)

# Configure the bundled GDAL/PROJ env on import (driver side). Worker processes
# call configure_gdal_env() again inside each UDF body.
configure_gdal_env()


def __getattr__(name: str):
    """Lazy exports for symbols whose transitive imports pull rasterio.

    ``tile_range`` and ``rst_viewshed_towers`` are advertised in ``__all__`` and
    fully usable via ``pyrx.tile_range`` / ``from … pyrx import tile_range``, but
    their modules are NOT imported until first access.  This keeps a bare
    ``import databricks.labs.gbx.pyrx`` (and any pyrx submodule import) free of
    the rasterio dependency, which is absent in the heavy-tier build environment.
    """
    if name == "tile_range":
        from databricks.labs.gbx.pyrx.core.tiling import tile_range

        return tile_range
    if name == "rst_viewshed_towers":
        from databricks.labs.gbx.pyrx.functions import rst_viewshed_towers

        return rst_viewshed_towers
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "assert_rasterio_available",
    "checkpoint_skip",
    "configure_gdal_env",
    "dense_fuse",
    "dense_mvs_pool",
    "dense_patch_match",
    "dense_reconstruct_clusters",
    "dense_undistort",
    "gpu_infra",
    "input_signature",
    "Manifest",
    "recommend_dense_allocation",
    "rst_viewshed_towers",
    "tile_range",
]
