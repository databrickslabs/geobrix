"""GDAL's own Python bindings (``osgeo``) for the code that calls GDAL directly.

The env wheels carry them in the native payload, built from the same GDAL as the vendored
rasterio and the JNI: ``osgeo`` (the SWIG modules) under ``_vendor/<arch>``, ``osgeo_utils``
(pure Python) under ``_vendor/common``. A process that imports rasterio and ``osgeo`` then maps
one libgdal, and the light and heavy tiers run the same GDAL code.

Every module of the package gets them here and imports neither package itself (a test holds that):

- ``bindings(fn)``: ``gdal``, ``ogr``, ``osr`` and ``gdal_array`` (None when it does not import;
  it needs numpy's C API), with GDAL exceptions on;
- ``utils(name, fn)``: the module ``osgeo_utils.<name>``, e.g.
  ``utils("samples.validate_cloud_optimized_geotiff")``.

Where ``osgeo`` does not import (a PyPI install, whose rasterio bundles a GDAL without Python
bindings; the macOS host) both raise ``OsgeoUnavailable``, an ``ImportError`` whose message names
the function and the cause and ends with the fix. ``fn`` is the name the message starts with.
"""

import importlib
from types import ModuleType
from typing import NamedTuple, Optional


class OsgeoUnavailable(ImportError):
    """GDAL's Python bindings do not import in this environment."""


class Bindings(NamedTuple):
    gdal: ModuleType
    ogr: ModuleType
    osr: ModuleType
    gdal_array: Optional[ModuleType]


def _unavailable(fn: str, exc: ImportError) -> OsgeoUnavailable:
    from databricks.labs.gbx import __version__

    v = __version__.split("+")[0]
    return OsgeoUnavailable(
        f"{fn} needs GDAL's Python bindings (osgeo) from the GeoBrix env wheel, and they do not "
        f"import here ({exc}). Fix: install the env wheel for the runtime (Linux x86_64 or "
        f"aarch64, Python 3.12): geobrix-{v}+env6-py3-none-any.whl on DBR 19 and Serverless "
        f"environment 6, geobrix-{v}+env5-py3-none-any.whl on DBR 17.3, DBR 18 and Serverless "
        "environment 5."
    )


def bindings(fn: str = "this function") -> Bindings:
    """``gdal``, ``ogr``, ``osr`` and ``gdal_array`` with exceptions on; ``OsgeoUnavailable`` if absent."""
    try:
        from osgeo import gdal, ogr, osr
    except ImportError as exc:
        raise _unavailable(fn, exc) from exc
    for mod in (gdal, ogr, osr):
        mod.UseExceptions()
    try:
        from osgeo import gdal_array
    except ImportError:
        gdal_array = None
    return Bindings(gdal, ogr, osr, gdal_array)


def utils(name: str, fn: str = "this function") -> ModuleType:
    """The module ``osgeo_utils.<name>``; ``OsgeoUnavailable`` when ``osgeo`` or ``osgeo_utils`` is absent."""
    bindings(fn)
    try:
        importlib.import_module("osgeo_utils")
    except ImportError as exc:
        raise _unavailable(fn, exc) from exc
    return importlib.import_module(f"osgeo_utils.{name}")
