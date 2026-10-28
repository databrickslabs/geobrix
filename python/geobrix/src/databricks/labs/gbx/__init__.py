__version__ = "0.6.0"

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
