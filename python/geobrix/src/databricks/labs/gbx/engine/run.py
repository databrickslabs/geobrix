"""One engine call on ``osgeo`` (the light tier); the JVM's ``GdalSpecEngine`` makes the same call.

1. The input bytes are sniffed (GeoTIFF magic only), written to ``/vsimem/gbx-spec/<uuid>/in<i>.tif`` and their
   headers read (size, bands, type, CRS); the call must fit the decoded budget.
2. The argv is bound (``argv.build``) and checked; the algorithm is instantiated (single use), parsed, run and
   finalized; the output is read from ``/vsimem``.
3. Every file under the call's prefix is removed, whatever happened.

``osgeo`` comes only from the shared-GDAL helper (``_osgeo.bindings``), which refuses an ``osgeo`` that is not
rasterio's GDAL. Every GDAL call is judged on its return value with GDAL's exceptions off, after ``ErrorReset``,
exactly as the JVM's binding is, so one argv has one outcome on both tiers:
- GDAL's run failing is a data error (an error tile with GDAL's message, its ``/vsimem`` names shown as
  ``<input>``/``<output>``);
- GDAL refusing the argv is a data error when the call bound a ``bad: data`` value (that value is the cause),
  else a parameter error;
- a map-algebra expression GDAL cannot evaluate is a parameter error, found by a dry run once per expression.

The engine's own GDAL settings (``ENGINE_SETTINGS``) are thread-local for the call, so the user's process keeps its
own configuration. The virtual-tile driver policy (design section 4) has two GDAL-level parts: a virtual tile's own
file must identify with a driver on the manifest's allowlist (``virtual_source``), and the manifest's refused drivers
are deregistered from this process once, before the engine's first call (``apply_driver_policy``), because GDAL
opens a VRT's sources, an index's tiles and a pipeline's inputs itself, through the same registry.
"""

from __future__ import annotations

import contextlib
import functools
import threading
import uuid
from dataclasses import dataclass
from typing import Mapping, Optional, Sequence

from databricks.labs.gbx import _gdal_policy, manifest
from databricks.labs.gbx.engine import argv as argv_mod
from databricks.labs.gbx.engine.spec import OUTPUT_FORMATS, DataError, ParamError

PREFIX = "/vsimem/gbx-spec"
_TIFF_MAGIC = (
    b"II*\x00",
    b"MM\x00*",
    b"II+\x00",
    b"MM\x00+",
)  # TIFF and BigTIFF, both byte orders
# The engine's GDAL settings, set thread-local for each call on both tiers (the JVM's GdalSpecEngine applies the
# same table), so they never change the user's process: one thread per call (a Spark task is one core), no Python
# in a VRT pixel function, and no VRT raw band (it reads a file's bytes as pixels without a driver, which a driver
# policy cannot judge). Process-wide settings stay as the process has them: GDAL_CACHEMAX (GDALManager's 512 MB on the
# JVM, GDAL's default in Python), GDAL_PAM_ENABLED (on: a tile's NoData may live in its .aux.xml; the engine's own
# sidecars are under the call's prefix and go with it) and GDAL_SKIP, which only acts when drivers register (the
# policy deregisters the refused drivers instead). Pinned by a golden row, read by both suites.
ENGINE_SETTINGS = {
    "GDAL_NUM_THREADS": "1",
    "GDAL_VRT_ENABLE_PYTHON": "NO",
    "GDAL_VRT_ENABLE_RAWRASTERBAND": "NO",
}
_validated: dict = (
    {}
)  # dry-run outcomes of map-algebra expressions, by (expression, inputs)
_validated_lock = threading.Lock()


@dataclass(frozen=True)
class Result:
    """A call's outcome: the output GeoTIFF, or the reason it failed (an error tile), and the argv it ran."""

    raster: Optional[bytes]
    error: Optional[str]
    command: Optional[str]


@functools.lru_cache(maxsize=1)
def _gdal():
    from databricks.labs.gbx import _osgeo

    return _osgeo.bindings("the GDAL spec engine").gdal


@functools.lru_cache(maxsize=1)
def unavailable_reason() -> Optional[str]:
    """None when this process can run the engine: ``osgeo`` is rasterio's GDAL, at the manifest's pin, and every
    allowlisted algorithm instantiates. Otherwise why not (taken once per process)."""
    try:
        gdal = _gdal()
    except ImportError as exc:
        return str(exc)
    pin = manifest.load().gdal.pin
    release = gdal.VersionInfo("RELEASE_NAME")
    if release != pin:
        return f"the GDAL spec engine needs GDAL {pin}; osgeo here is GDAL {release}"
    for alg in manifest.load().gdal.allow:
        try:
            _instantiate(alg)
        except Exception as exc:  # noqa: BLE001 - an algorithm this build lacks
            return f"GDAL {release} has no '{alg}' ({exc})"
    return None


def gdal_release() -> str:
    return _gdal().VersionInfo("RELEASE_NAME")


def _instantiate(path: str):
    """A new (single-use) algorithm for `path` ('raster slope'), and its parent group, which it outlives."""
    top, sub = path.split(" ", 1)
    group = _gdal().GetGlobalAlgorithmRegistry().InstantiateAlg(top)
    return group.InstantiateSubAlgorithm(sub), group


def usage_json(path: str) -> str:
    alg, _group = _instantiate(path)
    return alg.GetUsageAsJSON()


def decoded_budget() -> int:
    """The decoded bytes one call may hold (its inputs plus an output of the same size)."""
    from databricks.labs.gbx.pyrx.core.budget import decoded_budget_bytes

    return decoded_budget_bytes("auto")


@contextlib.contextmanager
def engine_settings():
    """The engine's GDAL settings (``ENGINE_SETTINGS``) for this thread, for the duration of the block."""
    with _gdal().config_options(ENGINE_SETTINGS, thread_local=True):
        yield


@contextlib.contextmanager
def _gdal_status():
    """GDAL's exceptions off and its error state clear: the caller reads each call's return value and then
    ``GetLastErrorMsg()``, as the JVM does."""
    gdal = _gdal()
    with gdal.ExceptionMgr(useExceptions=False):
        gdal.ErrorReset()
        yield gdal


def virtual_drivers() -> tuple:
    """The drivers a virtual tile's own file may open with: every raster driver GeoBrix opens (the manifest's
    classification, ``_gdal_policy.allowed_names``). The refused drivers are gone from the process at activation.
    """
    return _gdal_policy.allowed_names()


def refused_drivers() -> tuple:
    """The drivers the policy refuses (``_gdal_policy.refused_names``)."""
    return _gdal_policy.refused_names()


def apply_driver_policy() -> dict:
    """The process's driver policy: activated at package import, idempotent (``_gdal_policy.activate``)."""
    return _gdal_policy.activate()


# What the policy refused, for a GDAL message raised while a virtual tile was read (the shared mapping).
policy_reason = _gdal_policy.policy_reason


def virtual_source(sql_name: str, path: str) -> None:
    """Raise DataError unless `path` (a virtual tile's own file) identifies with a driver on the virtual-tile
    allowlist, and, for a VRT, opens as a VRT. Only the header is read: a driver off the allowlist is identified,
    never opened. Its sources are not walked here: GDAL opens them when the tile is read, and the refused drivers are
    not registered then (``apply_driver_policy``); a source with a driver off the allowlist is read as GDAL allows.
    """
    apply_driver_policy()
    with engine_settings(), _gdal_status() as gdal:
        drv = gdal.IdentifyDriverEx(
            path, gdal.OF_RASTER, allowed_drivers=list(virtual_drivers())
        )
        if drv is None:
            gdal.ErrorReset()
            other = gdal.IdentifyDriverEx(path, gdal.OF_RASTER)
            what = (
                f"a {other.ShortName} file"
                if other is not None
                else "a file no driver on the virtual-tile allowlist reads"
            )
            raise DataError(
                f"{sql_name}: the virtual tile's file {path} is {what}, which GeoBrix does not open for a virtual tile"
            )
        if drv.ShortName != "VRT":
            return
        ds = gdal.OpenEx(
            path, gdal.OF_RASTER | gdal.OF_READONLY, allowed_drivers=["VRT"]
        )
        if ds is None:
            raise DataError(
                f"{sql_name}: {gdal.GetLastErrorMsg() or path + ' is not a readable VRT'}"
            )
        ds = None


def _props(gdal, sql_name: str, name: str, needs_crs: bool) -> argv_mod.RasterProps:
    gdal.ErrorReset()
    ds = gdal.OpenEx(name, gdal.OF_RASTER | gdal.OF_READONLY, allowed_drivers=["GTiff"])
    if ds is None:
        raise DataError(
            gdal.GetLastErrorMsg() or f"{sql_name}: the input is not a readable GeoTIFF"
        )
    try:
        if needs_crs and ds.GetSpatialRef() is None:
            raise DataError(
                f"{sql_name}: the tile has no CRS, so it cannot be reprojected. "
                "Fix: set its CRS first with gbx_rst_setcrs"
            )
        band = ds.GetRasterBand(1)
        return argv_mod.RasterProps(
            ds.RasterXSize,
            ds.RasterYSize,
            ds.RasterCount,
            gdal.GetDataTypeName(band.DataType),
        )
    finally:
        ds = None


def _read(gdal, name: str) -> bytes:
    size = gdal.VSIStatL(name).size
    fh = gdal.VSIFOpenL(name, "rb")
    try:
        return bytes(gdal.VSIFReadL(1, size, fh))
    finally:
        gdal.VSIFCloseL(fh)


def _remove(gdal, prefix: str) -> None:
    for rel in gdal.ReadDirRecursive(prefix) or ():
        if not rel.endswith("/"):
            gdal.Unlink(f"{prefix}/{rel}")
    gdal.RmdirRecursive(
        prefix
    )  # nothing left to remove is not an error here (exceptions are off)


def _shown(text: str, names: Mapping[str, str]) -> str:
    for real, placeholder in names.items():
        text = text.replace(real, placeholder)
    return text.replace(PREFIX + "/", "")


def _execute(gdal, alg_path: str, tokens, shown, refused):
    """Instantiate, parse, run and finalize. `refused(message)` makes the exception for a refused argv."""
    alg, group = _instantiate(alg_path)
    try:
        gdal.ErrorReset()
        if not alg.ParseCommandLineArguments(tokens):
            raise refused(
                _shown(gdal.GetLastErrorMsg() or "GDAL refused the arguments", shown)
            )
        gdal.ErrorReset()
        if not (alg.Run() and alg.Finalize()):
            raise DataError(gdal.GetLastErrorMsg() or f"GDAL's {alg_path} failed")
    finally:
        del alg, group  # release both handles, the step before its group


def _check_expression(gdal, fn, tokens, props, prefix, shown) -> None:
    """A map-algebra expression GDAL cannot evaluate is a parameter error: GDAL's own check (one pixel, through a
    VRT output) runs once per expression and input props, before the first real run."""
    key = tuple(t for t in tokens if t.startswith(("--calc=", "--dialect="))) + tuple(
        props
    )
    with _validated_lock:
        known = _validated.get(key)
    if known is None:
        dry = [t for t in tokens if not t.startswith(("--output", "--creation-option"))]
        dry += [f"--output={prefix}/check.vrt", "--output-format=VRT"]
        try:
            _execute(gdal, fn.gdal.alg, dry, shown, ParamError)
            known = ""
        except (DataError, ParamError) as exc:
            known = str(exc)
        with _validated_lock:
            if len(_validated) > 1024:
                _validated.clear()
            _validated[key] = known
    if known:
        expr = next(t for t in tokens if t.startswith("--calc=")).split("=", 1)[1]
        raise ParamError(
            f"{fn.sql_name}: GDAL cannot evaluate the expression '{expr}' ({known})"
        )


def run(
    fn, rasters: Sequence[bytes], values: Mapping, section=None
) -> Optional[Result]:
    """One call of `fn`'s spec over `rasters` (GeoTIFF bytes) with `values` (param -> value): its Result, or
    None for a NULL result. Raises ParamError for a value the function refuses. `section` is the allowlist
    (default the manifest's `gdal` section)."""
    apply_driver_policy()
    block = fn.gdal
    prefix = f"{PREFIX}/{uuid.uuid4().hex}"
    names = [f"{prefix}/in{i}.tif" for i in range(len(rasters))]
    out = f"{prefix}/out.tif"
    placeholders = (
        ["<input>"]
        if block.inputs[0][1] == "raster"
        else [f"<input{i}>" for i in range(len(rasters))]
    )
    shown = {**dict(zip(names, placeholders)), out: "<output>"}
    command = None
    with engine_settings(), _gdal_status() as gdal:
        try:
            for i, raster in enumerate(rasters):
                if bytes(raster[:4]) not in _TIFF_MAGIC:
                    raise DataError(f"{fn.sql_name}: input {i} is not a GeoTIFF")
                gdal.FileFromMemBuffer(names[i], bytes(raster))
            props = [_props(gdal, fn.sql_name, n, block.needs_crs) for n in names]
            decoded = sum(
                p.width * p.height * p.bands * argv_mod._ITEMSIZE.get(p.dtype, 8)
                for p in props
            )
            budget = decoded_budget()
            if budget and 2 * decoded > budget:
                raise DataError(
                    f"{fn.sql_name}: the call needs {2 * decoded // 2**20} MiB decoded (input and output), over "
                    f"this compute's {budget // 2**20} MiB. Fix: retile the tile first (gbx_rst_retile)"
                )
            tokens = argv_mod.build(fn, values, props, names, out, section)
            if tokens is None:
                return None
            command = _shown(" ".join([block.alg] + tokens), shown)
            fmt = next(
                t.split("=", 1)[1] for t in tokens if t.startswith("--output-format=")
            )
            if fmt not in OUTPUT_FORMATS:
                raise ParamError(f"{fn.sql_name}: the engine does not write {fmt}")
            data = argv_mod.data_bound(fn, values)

            def refused(msg):
                return (DataError if data else ParamError)(f"{fn.sql_name}: {msg}")

            if block.alg == "raster calc":
                _check_expression(gdal, fn, tokens, props, prefix, shown)
            _execute(gdal, block.alg, tokens, shown, refused)
            return Result(_read(gdal, out), None, command)
        except DataError as exc:
            return Result(None, _shown(str(exc), shown), command)
        finally:
            _remove(gdal, prefix)
