"""One engine call on ``osgeo`` (the light tier); the JVM's ``GdalSpecEngine`` makes the same call.

1. The input bytes are sniffed (GeoTIFF magic only), written to ``/vsimem/gbx-spec/<uuid>/in<i>.tif`` and their
   headers read (size, bands, type, CRS); the call must fit the decoded budget.
2. The argv is bound (``argv.build``) and checked; the algorithm is instantiated (single use), parsed, run and
   finalized; the output is read from ``/vsimem``.
3. Every file under the call's prefix is removed, whatever happened.

``osgeo`` comes only from the shared-GDAL helper (``_osgeo.bindings``), which refuses an ``osgeo`` that is not
rasterio's GDAL. A failure of GDAL's own run is a data error (an error tile with GDAL's message, its ``/vsimem``
names shown as ``<input>``/``<output>``); a value the binders or GDAL's parser refuse is a parameter error.
"""

from __future__ import annotations

import functools
import uuid
from dataclasses import dataclass
from typing import Mapping, Optional, Sequence

from databricks.labs.gbx import manifest
from databricks.labs.gbx.engine import argv as argv_mod
from databricks.labs.gbx.engine.spec import OUTPUT_FORMATS, DataError, ParamError

PREFIX = "/vsimem/gbx-spec"
_TIFF_MAGIC = (
    b"II*\x00",
    b"MM\x00*",
    b"II+\x00",
    b"MM\x00+",
)  # TIFF and BigTIFF, both byte orders


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


def _props(gdal, sql_name: str, name: str, needs_crs: bool) -> argv_mod.RasterProps:
    try:
        ds = gdal.OpenEx(
            name, gdal.OF_RASTER | gdal.OF_READONLY, allowed_drivers=["GTiff"]
        )
    except RuntimeError as exc:
        raise DataError(str(exc)) from None
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
    try:
        gdal.RmdirRecursive(prefix)
    except RuntimeError:  # nothing left to remove
        pass


def _shown(text: str, names: Mapping[str, str]) -> str:
    for real, placeholder in names.items():
        text = text.replace(real, placeholder)
    return text.replace(PREFIX + "/", "")


def run(fn, rasters: Sequence[bytes], values: Mapping) -> Optional[Result]:
    """One call of `fn`'s spec over `rasters` (GeoTIFF bytes) with `values` (param -> value): its Result, or
    None for a NULL result. Raises ParamError for a value the function refuses."""
    gdal = _gdal()
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
                f"{fn.sql_name}: the call needs {2 * decoded // 2**20} MiB decoded (input and output), over this "
                f"compute's {budget // 2**20} MiB. Fix: retile the tile first (gbx_rst_retile)"
            )
        tokens = argv_mod.build(fn, values, props, names, out)
        if tokens is None:
            return None
        command = _shown(" ".join([block.alg] + tokens), shown)
        fmt = next(
            t.split("=", 1)[1] for t in tokens if t.startswith("--output-format=")
        )
        if fmt not in OUTPUT_FORMATS:
            raise ParamError(f"{fn.sql_name}: the engine does not write {fmt}")
        alg, group = _instantiate(block.alg)
        try:
            try:
                parsed = alg.ParseCommandLineArguments(tokens)
            except RuntimeError as exc:
                raise ParamError(f"{fn.sql_name}: {_shown(str(exc), shown)}") from None
            if not parsed:
                raise ParamError(
                    f"{fn.sql_name}: {_shown(gdal.GetLastErrorMsg(), shown)}"
                )
            try:
                ran = alg.Run() and alg.Finalize()
            except RuntimeError as exc:
                raise DataError(str(exc)) from None
            if not ran:
                raise DataError(
                    gdal.GetLastErrorMsg()
                    or f"{fn.sql_name}: GDAL's {block.alg} failed"
                )
        finally:
            del alg, group  # release both handles, the step before its group
        return Result(_read(gdal, out), None, command)
    except DataError as exc:
        return Result(None, _shown(str(exc), shown), command)
    finally:
        _remove(gdal, prefix)
