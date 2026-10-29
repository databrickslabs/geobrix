__version__ = "0.6.0"

# An env wheel refuses the other environment's runtime, then serves its native payload (the
# bindings under their public names, GDAL/PROJ data, CA): both before anything imports a binding.
from databricks.labs.gbx import _env_guard, _native  # noqa: E402

_env_guard.check()
_native.activate(by="import")

# The process-level driver policy: its registration-time part is set here, in the environment GDAL reads when it
# registers its drivers, so a rasterio or osgeo that registers them later never registers a refused one. It imports no
# GDAL; the activation (the mode, the deregistration, the partial-mode check) runs at the first GeoBrix GDAL use.
from databricks.labs.gbx import _gdal_policy  # noqa: E402

_gdal_policy.prepare()

from databricks.labs.gbx.crs_grids import register_proj_grids  # noqa: E402


def register(spark=None, target="session", **kwargs):
    """Register GeoBrix SQL functions on the best level this session offers.

    See ``databricks.labs.gbx.manager.register`` for the arguments.
    """
    from databricks.labs.gbx.manager import register as _register

    return _register(spark, target, **kwargs)


def __getattr__(name):
    if name == "uc":
        import importlib

        try:
            return importlib.import_module("databricks.labs.gbx.uc")
        except ModuleNotFoundError as exc:
            if exc.name == "databricks.labs.gbx.uc":
                raise AttributeError(name) from exc
            raise
    raise AttributeError(name)


__all__ = ["register_proj_grids", "register"]
