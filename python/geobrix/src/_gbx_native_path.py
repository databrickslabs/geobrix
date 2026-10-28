"""Startup activation of the GeoBrix env wheel's native payload (ruling NP-2).

Imported at interpreter start by ``geobrix-native.pth`` (one line: ``import _gbx_native_path``),
which the env wheel installs beside this module in site-packages. It imports nothing from
geobrix and never raises: the rules come from ``databricks/labs/gbx/_native/_rules.py`` (the
same file ``databricks.labs.gbx._native`` uses), executed by path so that no geobrix package is
imported. Off Linux, with ``GBX_NATIVE_DISABLE=1`` or ``GBX_NATIVE_PTH=0``, or where the rules
say the payload cannot load (another architecture, an interpreter other than CPython 3.12, no
payload), it does nothing.

Otherwise it puts ``databricks/labs/gbx/_vendor/<arch>`` and then ``_vendor/common`` just before
the first site-packages entry of ``sys.path``: after the standard library and ahead of every
installed package, so ``import rasterio`` serves the payload's build (which must match the
payload's GDAL) even before geobrix is imported, and even when the user has another rasterio,
pyogrio, pyproj, shapely, netCDF4, osgeo, rio-tiler, morecantile or mapbox-vector-tile installed
(``GBX_NATIVE_DISABLE=1`` turns the payload off). It sets ``GDAL_DATA``, ``PROJ_DATA`` and the
GDAL and PROJ CA bundles where the user has not (the user's own CA wins), and ``ACTIVE = True``.
"""

import os
import sys

ACTIVE = False
_rules = None


def _load_rules(here):
    path = os.path.join(here, "databricks", "labs", "gbx", "_native", "_rules.py")
    mod = type(sys)("_gbx_native_rules")
    mod.__file__ = path
    with open(path) as fh:
        exec(compile(fh.read(), path, "exec"), mod.__dict__)
    return mod


def _activate():
    global ACTIVE, _rules
    env = os.environ
    if env.get("GBX_NATIVE_DISABLE") == "1" or env.get("GBX_NATIVE_PTH") == "0":
        return
    if not sys.platform.startswith("linux"):
        return
    here = os.path.dirname(os.path.abspath(__file__))
    if _rules is None:
        _rules = _load_rules(here)
    root = env.get("GBX_NATIVE_ROOT") or os.path.join(here, "databricks", "labs", "gbx")
    ACTIVE = _rules.activate(root, os.uname().machine)


try:
    _activate()
except Exception:  # noqa: BLE001 - a .pth import must never stop the interpreter
    pass
