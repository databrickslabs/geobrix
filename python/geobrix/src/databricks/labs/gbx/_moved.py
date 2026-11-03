"""Old import paths of the Python-only API: each raises with a ``Fix:`` naming the new path.

The light packages (``pyrx``, ``pygx``, ``pyvx``) are the implementation behind ``rasterx``, ``gridx`` and
``vectorx``. What they used to export moved there, and no alias keeps the old path working.
"""

import functools
import sys
from typing import Callable, Dict, Tuple

# A Python-only function the light ``functions`` modules used to define -> the module that defines it now.
HELPER_HOMES: Dict[str, str] = {
    "rst_viewshed_towers": "databricks.labs.gbx.rasterx.helpers",
    "rst_memsize_grouped": "databricks.labs.gbx.rasterx.helpers",
    "rst_clip_grouped": "databricks.labs.gbx.rasterx.helpers",
    "rst_transform_grouped": "databricks.labs.gbx.rasterx.helpers",
    "rst_to_webmercator_grouped": "databricks.labs.gbx.rasterx.helpers",
    "rst_transformcrs_grouped": "databricks.labs.gbx.rasterx.helpers",
    "rst_resample_grouped": "databricks.labs.gbx.rasterx.helpers",
    "rst_resample_to_size_grouped": "databricks.labs.gbx.rasterx.helpers",
    "rst_resample_to_res_grouped": "databricks.labs.gbx.rasterx.helpers",
    "rst_threshold_grouped": "databricks.labs.gbx.rasterx.helpers",
    "rst_updatetype_grouped": "databricks.labs.gbx.rasterx.helpers",
    "rst_slope_grouped": "databricks.labs.gbx.rasterx.helpers",
    "rst_aspect_grouped": "databricks.labs.gbx.rasterx.helpers",
    "rst_hillshade_grouped": "databricks.labs.gbx.rasterx.helpers",
    "bin_points_tiled": "databricks.labs.gbx.rasterx.helpers",
    "rst_h3_gridspec": "databricks.labs.gbx.rasterx.helpers",
    "tile_to_numpy": "databricks.labs.gbx.rasterx.helpers",
    "rst_apply": "databricks.labs.gbx.rasterx.helpers",
    "coverage_simplify": "databricks.labs.gbx.vectorx.helpers",
}

_MOVED_PATHS: Dict[str, str] = {}  # old dotted path -> new dotted path


def _fix(new: str) -> str:
    module, _, name = new.rpartition(".")
    return f"Fix: from {module} import {name}"


class _Finder:
    """``import databricks.labs.gbx.pyrx.mvs`` and ``from ...pyrx import mvs`` for a moved name raise the
    ``Fix:`` (an ImportError that is not a ModuleNotFoundError, so ``from x import y`` does not swallow it).
    """

    @staticmethod
    def find_spec(fullname, path=None, target=None):
        new = _MOVED_PATHS.get(fullname)
        if new is not None:
            raise ImportError(
                f"{fullname} moved to {new} in 0.6.0. {_fix(new)}", name=fullname
            )
        return None


def install(package: str, moved: Dict[str, str]) -> Callable[[str], object]:
    """Register ``moved`` (a name of ``package`` -> its new dotted path); return the package's ``__getattr__``."""
    for name, new in moved.items():
        _MOVED_PATHS[f"{package}.{name}"] = new
    if _Finder not in sys.meta_path:
        sys.meta_path.insert(0, _Finder)

    def __getattr__(name: str):
        new = moved.get(name)
        if new is not None:
            raise AttributeError(
                f"{package}.{name} moved to {new} in 0.6.0. {_fix(new)}"
            )
        raise AttributeError(f"module {package!r} has no attribute {name!r}")

    return __getattr__


@functools.lru_cache(maxsize=1)
def _removed_index() -> Tuple[Dict[str, str], Dict[str, str]]:
    """The manifest's Python names, once: (SQL function's Python name -> its module, helper name -> its module).

    One caching rule for success and failure: the result is cached either way, so a ``hasattr`` probe (or pickle's
    ``whichmodule`` scan) of a missing name does not reload the manifest each time. The index is therefore fixed for the
    life of the process: a manifest patched or reloaded after the first lookup is not seen until
    ``_removed_index.cache_clear()``. A manifest that does not load gives empty indexes, which read as "no such name".
    """
    fns: Dict[str, str] = {}
    helpers: Dict[str, str] = {}
    try:
        from databricks.labs.gbx import manifest

        man = manifest.load()
        for fn in man.functions.values():
            if fn.python is not None:
                fns.setdefault(fn.python.name, fn.python.module)
        for h in man.helpers:
            helpers.setdefault(h.name, h.module)
    except Exception:  # noqa: BLE001 - a lookup failure must read as "no such name"
        return {}, {}
    return fns, helpers


def removed_name(module: str, name: str):
    """Raise the AttributeError of a name a light ``functions`` module no longer defines.

    A SQL function's Python entry is in the generated module; a helper is in ``helpers``. Whatever goes wrong in the
    lookup (the manifest does not load, see :func:`_removed_index`) ends as the plain AttributeError: ``hasattr``, ``getattr(..., default)`` and
    pickle's ``whichmodule`` catch nothing else.
    """
    if name == "register":
        raise AttributeError(
            f"{module}.register was removed in 0.6.0: SQL functions register through the manager. "
            "Fix: databricks.labs.gbx.register(spark)"
        )
    fns, helpers = _removed_index()
    pkg = fns.get(name)
    if pkg is not None:
        raise AttributeError(
            f"{module}.{name} was removed in 0.6.0; use databricks.labs.gbx.{pkg}.functions.{name}. "
            f"Fix: from databricks.labs.gbx.{pkg} import functions"
        )
    home = helpers.get(name) or HELPER_HOMES.get(name)
    if home is not None:
        raise AttributeError(
            f"{module}.{name} moved to {home}.{name}. Fix: from {home} import {name}"
        )
    raise AttributeError(f"module {module!r} has no attribute {name!r}")
