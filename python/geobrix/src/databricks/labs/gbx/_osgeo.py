"""GDAL's own Python bindings (``osgeo``) for the code that calls GDAL directly.

The env wheels carry them in the native payload, built from the same GDAL as the vendored
rasterio and the JNI: ``osgeo`` (the SWIG modules) under ``_vendor/<arch>``, ``osgeo_utils``
(pure Python) under ``_vendor/common``. A process that imports rasterio and ``osgeo`` then maps
one libgdal, and the light and heavy tiers run the same GDAL code.

Every module of the package gets them here and imports neither package itself (a test holds that):

- ``bindings(fn)``: ``gdal``, ``ogr``, ``osr`` and ``gdal_array`` (None when it does not import;
  it needs numpy's C API), with GDAL exceptions on;
- ``utils(name, fn)``: the module ``osgeo_utils.<name>``, e.g.
  ``utils("samples.validate_cloud_optimized_geotiff")``;
- ``libgdal_report()``: osgeo's GDAL against rasterio's, for diagnostics; it never refuses.

Both refuse an ``osgeo`` that is not rasterio's GDAL. Two GDALs in one process share no datasets,
``/vsimem`` files or configuration, so anything handed from one to the other fails or reads
nothing. The dev container's system ``osgeo`` beside its PyPI rasterio is such a pair. The test is
direct: ``gdal.VersionInfo()`` must equal ``rasterio.__gdal_version__``, and a ``/vsimem`` GeoTIFF
rasterio writes must open in osgeo, and one osgeo writes must open in rasterio (``/vsimem`` lives
inside one libgdal). Counting the libgdal files a process maps is no test: a library that bundles
its own GDAL (fiona, a PyPI pyogrio), loaded first, adds a file while osgeo and rasterio still
share one GDAL; and off Linux there is nothing to count. The verdict is taken once per process and
kept: the ``osgeo`` and ``rasterio`` modules a process imported cannot move to another libgdal,
and nothing loaded later changes whether they share one. ``same_libgdal=False`` lifts the check
for a caller that only opens and writes files by path and never shares a dataset or a ``/vsimem``
file with rasterio; two callers may (a test holds that): the dev-only sample bundle and the
test-only COG validator.

Where ``osgeo`` does not import, or is refused, both raise ``OsgeoUnavailable``, an
``ImportError`` whose message names the function and the cause and ends with the fix. ``fn`` is
the name the message starts with.
"""

import functools
import importlib
import re
from types import ModuleType
from typing import List, NamedTuple, Optional

LIBGDAL = re.compile(r"/libgdal(-[0-9a-f]{8})?\.so[.\d]*$")


class OsgeoUnavailable(ImportError):
    """GDAL's Python bindings do not import in this environment, or are not rasterio's GDAL."""


class Bindings(NamedTuple):
    gdal: ModuleType
    ogr: ModuleType
    osr: ModuleType
    gdal_array: Optional[ModuleType]


def _wheels() -> str:
    from databricks.labs.gbx import __version__

    v = __version__.split("+")[0]
    return (
        "install the env wheel for the runtime (Linux x86_64 or aarch64, Python 3.12): "
        f"geobrix-{v}+env6-py3-none-any.whl on DBR 19 and Serverless environment 6, "
        f"geobrix-{v}+env5-py3-none-any.whl on DBR 17.3, DBR 18 and Serverless environment 5"
    )


def _unavailable(fn: str, exc: ImportError) -> OsgeoUnavailable:
    return OsgeoUnavailable(
        f"{fn} needs GDAL's Python bindings (osgeo) from the GeoBrix env wheel, and they do not "
        f"import here ({exc}). Fix: {_wheels()}."
    )


def _not_shared(fn: str, r: dict) -> OsgeoUnavailable:
    why = (
        "; a /vsimem file one writes does not open in the other"
        if r["osgeo_gdal"] == r["rasterio_gdal"]
        else ""
    )
    maps = (
        ""
        if r["libgdal"] is None
        else f"; the process maps {len(r['libgdal'])} libgdal files: {', '.join(r['libgdal'])}"
    )
    return OsgeoUnavailable(
        f"{fn} needs GDAL's Python bindings (osgeo) on rasterio's GDAL, and this osgeo is another "
        f"GDAL (osgeo GDAL {r['osgeo_gdal']} from {r['osgeo']}, rasterio GDAL "
        f"{r['rasterio_gdal']} from {r['rasterio']}{why}{maps}). Fix: {_wheels()}; in the dev "
        "container, start Python with GBX_NATIVE_ROOT set to a built payload "
        "(build/env-wheel/env6/payload, from gbx:build:wheel --env 6)."
    )


def mapped_libgdal() -> Optional[List[str]]:
    """The libgdal files this process maps (Linux ``/proc/self/maps``), else None."""
    try:
        with open("/proc/self/maps") as fh:
            paths = {ln.split(None, 5)[-1].strip() for ln in fh if "/" in ln}
    except OSError:
        return None
    return sorted(p for p in paths if LIBGDAL.search(p))


def vsimem_shared(gdal) -> bool:
    """True when osgeo's ``gdal`` and rasterio run one GDAL: a 1x1 GeoTIFF rasterio writes to
    ``/vsimem`` opens in osgeo with its pixel, and one osgeo writes opens in rasterio. Each
    libgdal has its own ``/vsimem``, so a second GDAL sees neither file."""
    import uuid

    import numpy as np
    import rasterio
    from rasterio.io import MemoryFile

    theirs = f"/vsimem/gbx-osgeo-check-{uuid.uuid4().hex}.tif"
    gdal.PushErrorHandler("CPLQuietErrorHandler")
    try:
        with MemoryFile(ext=".tif") as mem:
            with mem.open(
                driver="GTiff", width=1, height=1, count=1, dtype="uint8"
            ) as dst:
                dst.write(np.full((1, 1, 1), 7, dtype="uint8"))
            ds = gdal.Open(mem.name)
            if ds is None or ds.GetRasterBand(1).ReadRaster() != b"\x07":
                return False
            ds = None
        out = gdal.GetDriverByName("GTiff").Create(theirs, 1, 1, 1, gdal.GDT_Byte)
        out.GetRasterBand(1).WriteRaster(0, 0, 1, 1, b"\x09")
        out = None  # closes (and flushes) the dataset
        with rasterio.open(theirs) as src:
            return int(src.read(1)[0, 0]) == 9
    except (
        Exception
    ):  # noqa: BLE001 - any failure to see the other's file: not one GDAL
        return False
    finally:
        try:
            gdal.Unlink(theirs)
        except (
            Exception
        ):  # noqa: BLE001 - nothing was written (osgeo raises with exceptions on)
            pass
        gdal.PopErrorHandler()


def libgdal_report() -> dict:
    """osgeo's GDAL against rasterio's: both versions and module files, ``gdal_array`` (it
    imports), ``libgdal`` (the files mapped, a diagnostic; None off Linux), ``shared`` (one GDAL:
    the same version and the ``/vsimem`` round trip of ``vsimem_shared``) and ``error`` (why
    osgeo does not import, else None)."""
    import rasterio

    r = {
        "osgeo_gdal": None,
        "osgeo": None,
        "rasterio_gdal": rasterio.__gdal_version__,
        "rasterio": rasterio.__file__,
        "gdal_array": False,
        "libgdal": None,
        "shared": False,
        "error": None,
    }
    try:
        from osgeo import gdal
    except ImportError as exc:
        r.update(libgdal=mapped_libgdal(), error=str(exc))
        return r
    try:
        from osgeo import gdal_array  # noqa: F401
    except ImportError:
        pass
    else:
        r["gdal_array"] = True
    version = gdal.VersionInfo("RELEASE_NAME")
    r.update(
        osgeo_gdal=version,
        osgeo=gdal.__file__,
        libgdal=mapped_libgdal(),
        shared=version == r["rasterio_gdal"] and vsimem_shared(gdal),
    )
    return r


@functools.lru_cache(maxsize=1)
def _same_libgdal_report() -> dict:
    """``libgdal_report()`` once per process. The imported ``osgeo`` and ``rasterio`` cannot move
    to another libgdal, and a library loaded later does not change whether they share one, so
    the first answer holds for the life of the process."""
    return libgdal_report()


def bindings(fn: str = "this function", *, same_libgdal: bool = True) -> Bindings:
    """``gdal``, ``ogr``, ``osr`` and ``gdal_array`` with exceptions on; ``OsgeoUnavailable`` if
    absent or (unless ``same_libgdal=False``) not rasterio's GDAL."""
    try:
        from osgeo import gdal, ogr, osr
    except ImportError as exc:
        raise _unavailable(fn, exc) from exc
    if same_libgdal and not _same_libgdal_report()["shared"]:
        raise _not_shared(fn, _same_libgdal_report())
    for mod in (gdal, ogr, osr):
        mod.UseExceptions()
    try:
        from osgeo import gdal_array
    except ImportError:
        gdal_array = None
    return Bindings(gdal, ogr, osr, gdal_array)


def utils(
    name: str, fn: str = "this function", *, same_libgdal: bool = True
) -> ModuleType:
    """The module ``osgeo_utils.<name>``; ``OsgeoUnavailable`` as for ``bindings``, or when
    ``osgeo_utils`` is absent."""
    bindings(fn, same_libgdal=same_libgdal)
    try:
        importlib.import_module("osgeo_utils")
    except ImportError as exc:
        raise _unavailable(fn, exc) from exc
    return importlib.import_module(f"osgeo_utils.{name}")
