"""One engine call on ``osgeo`` (the light tier).

1. The input bytes are written to ``/vsimem/gbx-spec/<uuid>/in<i>.tif`` and their headers read (size, bands, type,
   CRS); each tile must be within the read cap and the call within the decoded budget. Bytes without the GeoTIFF
   magic (a PNG, netCDF or HFA tile kept in its own driver) are written as ``src<i>.bin`` and read under the light
   bytes rule (``_gdal_policy.open_bytes``): a driver that can name other files (the manifest's
   ``bytes_refused_light`` class: a VRT, a label format) is named from the bytes and refused with a Fix before
   anything is opened, and any other allowed driver reads them. They are judged by the cap and budget from their
   header, and only then translated to ``in<i>.tif`` (GTiff, no other option); the source goes before the run.
2. The text arguments (a color table) are checked and written to ``arg_<k>.txt`` under the call's prefix; the argv is
   bound (``argv.build``) and checked; the algorithm is instantiated (single use), parsed, run and
   finalized; the output is read from ``/vsimem``.
3. Every file under the call's prefix is removed, whatever happened.

``osgeo`` comes only from the shared-GDAL helper (``_osgeo.bindings``), which refuses an ``osgeo`` that is not
rasterio's GDAL. Every GDAL call is judged on its return value with GDAL's exceptions off, after ``ErrorReset``:
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
import dataclasses
import functools
import threading
import uuid
from dataclasses import dataclass
from typing import Mapping, Optional, Sequence

from databricks.labs.gbx import _gdal_policy, manifest
from databricks.labs.gbx.engine import argv as argv_mod
from databricks.labs.gbx.engine.spec import OUTPUT_FORMAT_OF, DataError, ParamError

PREFIX = "/vsimem/gbx-spec"
_TIFF_MAGIC = (
    b"II*\x00",
    b"MM\x00*",
    b"II+\x00",
    b"MM\x00+",
)  # TIFF and BigTIFF, both byte orders
# The engine's GDAL settings, set thread-local for each call, so they never change the user's process: one thread per call (a Spark task is one core), no Python
# in a VRT pixel function, and no VRT raw band (it reads a file's bytes as pixels without a driver, which a driver
# policy cannot judge). Process-wide settings stay as the process has them: the raster block cache size (GDALManager's 512 MB on the
# JVM, GDAL's default in Python), GDAL_PAM_ENABLED (on: a tile's NoData may live in its .aux.xml; the engine's own
# sidecars are under the call's prefix and go with it) and GDAL_SKIP, which only acts when drivers register (the
# policy deregisters the refused drivers instead). Pinned by a golden row.
ENGINE_SETTINGS = {
    "GDAL_NUM_THREADS": "1",
    "GDAL_VRT_ENABLE_PYTHON": "NO",
    "GDAL_VRT_ENABLE_RAWRASTERBAND": "NO",
}
_validated: dict = (
    {}
)  # dry-run outcomes of map-algebra expressions, by (expression, inputs)
_validated_lock = threading.Lock()


class MissingSourceError(FileNotFoundError):
    """A virtual tile's file (or a FILE reference's) is still missing after the transient retries: raised, never an
    error tile or a NULL, so Spark retries the task (a UC Volume read can lag a write) and the failure names the path.
    """


@dataclass(frozen=True)
class Result:
    """A call's outcome: the output GeoTIFF, or the reason it failed (an error tile), and the argv it ran."""

    raster: Optional[bytes]
    error: Optional[str]
    command: Optional[str]
    # a features output: one tuple per feature, in layer order: (WKB or None, the declared fields' values...)
    features: Optional[list] = None


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
    ``GetLastErrorMsg()``."""
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
    The path is data: a remote location is refused first (``_gdal_policy.data_path``), before GDAL reads anything.
    """
    _gdal_policy.data_path(path, f"{sql_name}: the virtual tile's file")
    apply_driver_policy()
    with engine_settings(), _gdal_status() as gdal:
        drv = gdal.IdentifyDriverEx(
            path, gdal.OF_RASTER, allowed_drivers=list(virtual_drivers())
        )
        if drv is None:
            gdal.ErrorReset()
            if gdal.VSIStatL(
                path
            ) is None and not _gdal_policy._names_a_refused_connection(
                path, at_start=True
            ):
                # a file that is not there is no driver's to identify: a missing file, which the caller retries
                # (a UC Volume path can lag a write) and then raises, never a data error
                raise FileNotFoundError(f"{path}: {_gdal_policy._MISSING}")
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


def _props(
    gdal, sql_name: str, name: str, needs_crs: bool, drivers: Sequence[str] = ("GTiff",)
) -> argv_mod.RasterProps:
    gdal.ErrorReset()
    ds = gdal.OpenEx(
        name, gdal.OF_RASTER | gdal.OF_READONLY, allowed_drivers=list(drivers)
    )
    if ds is None:
        raise DataError(
            gdal.GetLastErrorMsg()
            or f"{sql_name}: the input is not a raster GeoBrix reads"
        )
    try:
        # a tile georeferenced by ground control points has a CRS: its GCP projection
        if (
            needs_crs
            and ds.GetSpatialRef() is None
            and not (ds.GetGCPCount() and ds.GetGCPSpatialRef() is not None)
        ):
            raise DataError(
                f"{sql_name}: the tile has no CRS, so it cannot be reprojected. "
                "Fix: set its CRS first with gbx_rst_setcrs"
            )
        band = ds.GetRasterBand(1)
        gt = ds.GetGeoTransform()
        return argv_mod.RasterProps(
            ds.RasterXSize,
            ds.RasterYSize,
            ds.RasterCount,
            gdal.GetDataTypeName(band.DataType),
            band.GetNoDataValue(),
            px_x=abs(gt[1]),
            px_y=abs(gt[5]),
        )
    finally:
        ds = None


BYTES_FIX = _gdal_policy.BYTES_FIX


def _open_drivers(section) -> tuple:
    """The drivers an open of tile bytes may use: the allowlist this call runs under less the light bytes rule's
    refused class (``_gdal_policy.open_bytes`` lists the same)."""
    refused = set(section.bytes_refused_light_drivers)
    return tuple(n for n in section.allowed_drivers if n not in refused)


def _bytes_source(
    gdal, sql_name: str, i: int, src: str, data: bytes, drivers: Sequence[str]
) -> None:
    """Raise DataError (with a Fix) unless the tile bytes `data` (written at `src`, without the TIFF magic) are read
    under the light tier's bytes rule (``_gdal_policy.open_bytes``): a driver that can name other files (a VRT, a
    label format: the manifest's ``bytes_refused_light`` class) is named from the bytes and never opened, and any
    other driver of `drivers` (the allowed ones less that class: netCDF, HFA, ...) reads them. Only the header is read.
    The JVM's ``TileBytes`` applies the same rule to the heavy functions that read tile bytes.
    """
    mode = apply_driver_policy()["mode"]
    refused = _gdal_policy._class_of_bytes(data[:4096], mode)
    if refused is not None:
        raise DataError(
            f"{sql_name}: input {i} is a {refused} raster, a format that can name other files, "
            f"{_gdal_policy.REFUSED_PHRASE} from tile bytes. {BYTES_FIX}"
        )
    gdal.ErrorReset()
    if (
        gdal.IdentifyDriverEx(src, gdal.OF_RASTER, allowed_drivers=list(drivers))
        is None
    ):
        raise DataError(
            f"{sql_name}: input {i} is not a raster GeoBrix reads from tile bytes. {BYTES_FIX}"
        )


def _to_gtiff(gdal, sql_name: str, src: str, dst: str, drivers: Sequence[str]) -> None:
    """Translate `src` (raster bytes in another driver) to the GeoTIFF `dst`, then remove `src`: the engine's
    algorithms read GeoTIFF inputs."""
    gdal.ErrorReset()
    ds = gdal.OpenEx(
        src, gdal.OF_RASTER | gdal.OF_READONLY, allowed_drivers=list(drivers)
    )
    if ds is None:
        raise DataError(
            gdal.GetLastErrorMsg()
            or f"{sql_name}: the input is not a raster GeoBrix reads"
        )
    try:
        gdal.ErrorReset()
        out = gdal.Translate(dst, ds, options=["-of", "GTiff"])
        if out is None:
            raise DataError(
                gdal.GetLastErrorMsg()
                or f"{sql_name}: the input could not be converted to GeoTIFF"
            )
        out = None
    finally:
        ds = None
    gdal.Unlink(src)


def _read(gdal, name: str) -> bytes:
    size = gdal.VSIStatL(name).size
    fh = gdal.VSIFOpenL(name, "rb")
    try:
        return bytes(gdal.VSIFReadL(1, size, fh))
    finally:
        gdal.VSIFCloseL(fh)


def _remask_zero_for_flat(gdal, src: str, out: str) -> None:
    """Aspect with ``--zero-for-flat``: GDAL drops the output's NoData value, so every cell it cannot compute (the
    border, a cell whose 3 x 3 window holds NoData) comes out 0, which reads as a valid aspect. Declare NoData -9999
    again and write it in those cells; a flat cell keeps its 0. The cells are the input's: a cell is computed when it
    and its 8 neighbours are data (not the NoData value, not NaN or infinite)."""
    import numpy as np

    sds = _open_geotiff(gdal, src)
    band = sds.GetRasterBand(1)
    w, h = sds.RasterXSize, sds.RasterYSize
    z = np.frombuffer(
        band.ReadRaster(0, 0, w, h, buf_type=gdal.GDT_Float64), dtype="<f8"
    ).reshape(h, w)
    nodata = band.GetNoDataValue()
    valid = np.isfinite(z) if nodata is None else (np.isfinite(z) & (z != nodata))
    del band, sds
    invalid = np.ones((h, w), bool)
    if h > 2 and w > 2:
        inner = np.ones((h - 2, w - 2), bool)
        for dy in range(3):
            for dx in range(3):
                inner &= valid[dy : h - 2 + dy, dx : w - 2 + dx]
        invalid[1:-1, 1:-1] = ~inner
    ods = gdal.OpenEx(out, gdal.OF_RASTER | gdal.OF_UPDATE, allowed_drivers=["GTiff"])
    ob = ods.GetRasterBand(1)
    a = np.frombuffer(
        ob.ReadRaster(0, 0, w, h, buf_type=gdal.GDT_Float32), dtype="<f4"
    ).reshape(h, w)
    a = np.where(invalid, np.float32(-9999.0), a).astype("<f4")
    ob.WriteRaster(0, 0, w, h, a.tobytes(), buf_type=gdal.GDT_Float32)
    ob.SetNoDataValue(-9999.0)
    ob.FlushCache()
    del ob, ods


def _open_geotiff(gdal, name: str, update: bool = False):
    """The engine's own GeoTIFF `name` (its input or its output under the call's /vsimem prefix), opened as GTiff only."""
    flags = gdal.OF_RASTER | (gdal.OF_UPDATE if update else gdal.OF_READONLY)
    return gdal.OpenEx(name, flags, allowed_drivers=["GTiff"])


def _keeps_input(gdal, fn, tokens, name: str) -> bool:
    """True when the row keeps a tile already in the CRS it asks for (``same_crs: "keep"``) and the input `name` is in
    that CRS: the result is the input as it is, not a warp of it. Both CRSs are compared in the one canonical form the
    argv carries (``argv.canonical_crs``): an authority code, else WKT. A tile whose CRS has no such form is warped.
    """
    if fn.gdal.same_crs != "keep":
        return False
    target = next(
        (t.split("=", 1)[1] for t in tokens if t.startswith("--output-crs=")), None
    )
    ds = _open_geotiff(gdal, name)
    try:
        have = ds.GetSpatialRef() if ds is not None else None
        if target is None or have is None:
            return False
        try:
            return argv_mod.canonical_crs(have.ExportToWkt()) == target
        except ValueError:
            return False
    finally:
        ds = None


def _carry_own_mask(gdal, tokens, src: str, out: str, prefix: str, shown) -> None:
    """A source with its own mask band (not an alpha band: JPEG-compressed imagery often has one) keeps it through a
    warp. GDAL's warp reads the mask but writes none, so the masked pixels would come out valid. The mask is warped
    like the pixels, as data (nearest), by the same arguments: it is written inverted, so a pixel no source pixel
    reaches stays valid, as the pixels outside the footprint do. The result is the output's own mask band.
    """
    import numpy as np

    ds = _open_geotiff(gdal, src)
    try:
        band = ds.GetRasterBand(1)
        if band.GetMaskFlags() != gdal.GMF_PER_DATASET:
            return
        w, h = ds.RasterXSize, ds.RasterYSize
        mask = np.frombuffer(band.GetMaskBand().ReadRaster(0, 0, w, h), dtype="u1")
        inverted = np.where(mask == 0, 255, 0).astype("u1")
        if not inverted.any():
            return
        mem = gdal.GetDriverByName("MEM").Create("", w, h, 1, gdal.GDT_Byte)
        mem.SetGeoTransform(ds.GetGeoTransform())
        mem.SetProjection(ds.GetProjection())
        mem.GetRasterBand(1).WriteRaster(0, 0, w, h, inverted.tobytes())
        gdal.GetDriverByName("GTiff").CreateCopy(f"{prefix}/mask_in.tif", mem)
        mem = None
    finally:
        ds = None
    args = [
        t
        for t in tokens
        if not t.startswith(
            ("--input=", "--output=", "--creation-option=", "--resampling=")
        )
    ]
    mask_out = f"{prefix}/mask_out.tif"
    _execute(
        gdal,
        "raster reproject",
        [
            f"--input={prefix}/mask_in.tif",
            f"--output={mask_out}",
            "--resampling=nearest",
        ]
        + args,
        shown,
        DataError,
    )
    wd = _open_geotiff(gdal, mask_out)
    od = None
    try:
        warped = np.frombuffer(
            wd.GetRasterBand(1).ReadRaster(0, 0, wd.RasterXSize, wd.RasterYSize),
            dtype="u1",
        )
        with gdal.config_options({"GDAL_TIFF_INTERNAL_MASK": "YES"}, thread_local=True):
            od = _open_geotiff(gdal, out, update=True)
            if (od.RasterXSize, od.RasterYSize) != (wd.RasterXSize, wd.RasterYSize):
                raise DataError(
                    "the source's mask band does not warp onto the output grid"
                )
            od.CreateMaskBand(gdal.GMF_PER_DATASET)
            valid = np.where(warped == 0, 255, 0).astype("u1")
            od.GetRasterBand(1).GetMaskBand().WriteRaster(
                0, 0, od.RasterXSize, od.RasterYSize, valid.tobytes()
            )
            od.FlushCache()
    finally:
        od = wd = None


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


def _same(x):
    return x


def _features(gdal, alg, fields, snap=None) -> list:
    """The output layer's features as (ISO WKB, field values...) tuples, read before ``Finalize`` (which releases the
    in-memory dataset). A field that is unset or NULL on a feature is None. ``snap`` is ``(layer field, function)``
    (``argv.level_snapper``): the function gives the exact contour level back from the layer's rounded one.
    """
    fix = {src: snap[1] for _n, _t, src in fields if snap and src == snap[0]}
    ds = alg.Output()
    if ds is None:
        raise DataError(gdal.GetLastErrorMsg() or "GDAL returned no output layer")
    lyr = ds.GetLayer(0)
    if lyr is None:
        raise DataError("GDAL returned no output layer")
    lyr.ResetReading()
    read = {
        "DOUBLE": "GetFieldAsDouble",
        "INT": "GetFieldAsInteger",
        "BIGINT": "GetFieldAsInteger64",
        "STRING": "GetFieldAsString",
    }
    out = []
    for f in lyr:
        g = f.GetGeometryRef()
        out.append(
            (None if g is None else bytes(g.ExportToIsoWkb()),)
            + tuple(
                (
                    fix.get(src, _same)(getattr(f, read[t])(src))
                    if f.IsFieldSetAndNotNull(src)
                    else None
                )
                for _name, t, src in fields
            )
        )
    del lyr, ds
    return out


def _execute(gdal, alg_path: str, tokens, shown, refused, collect=None):
    """Instantiate, parse, run and finalize. `refused(message)` makes the exception for a refused argv. `collect(alg)`
    reads the output after the run and before ``Finalize``; its value is returned."""
    alg, group = _instantiate(alg_path)
    try:
        gdal.ErrorReset()
        if not alg.ParseCommandLineArguments(tokens):
            raise refused(
                _shown(gdal.GetLastErrorMsg() or "GDAL refused the arguments", shown)
            )
        gdal.ErrorReset()
        if not alg.Run():
            raise DataError(gdal.GetLastErrorMsg() or f"GDAL's {alg_path} failed")
        got = collect(alg) if collect is not None else None
        if not alg.Finalize():
            raise DataError(gdal.GetLastErrorMsg() or f"GDAL's {alg_path} failed")
        return got
    finally:
        del alg, group  # release both handles, the step before its group


def _blank_inputs(gdal, names, props, prefix, shown) -> dict:
    """For each input name, a blank GeoTIFF with only its props (size, bands, type; no pixels written, no
    georeferencing): what the expression check runs on. Returns real name -> blank name; ``shown`` names each
    blank as its input."""
    blanks = {}
    drv = gdal.GetDriverByName("GTiff")
    for i, (name, p) in enumerate(zip(names, props)):
        blank = f"{prefix}/check{i}.tif"
        ds = drv.Create(
            blank,
            p.width,
            p.height,
            p.bands,
            gdal.GetDataTypeByName(p.dtype),
            options=["SPARSE_OK=TRUE"],
        )
        if ds is None:
            raise DataError(
                gdal.GetLastErrorMsg() or f"cannot make the check input for {name}"
            )
        ds = None
        blanks[name] = blank
        shown[blank] = shown.get(name, name)
    return blanks


def _check_expression(gdal, fn, tokens, props, names, prefix, shown) -> None:
    """A map-algebra expression GDAL cannot evaluate is a parameter error: GDAL's own check (through a VRT output,
    so no pixel is computed) runs once per expression and input props, before the first real run. It runs on blank
    inputs made from the props alone (``_blank_inputs``), so its outcome is a function of its cache key: a fault of
    the data itself (inputs whose CRS differ) is left to the real run, where it is that row's error tile and is not
    cached."""
    key = tuple(t for t in tokens if t.startswith(("--calc=", "--dialect="))) + tuple(
        props
    )
    with _validated_lock:
        known = _validated.get(key)
    if known is None:
        blanks = _blank_inputs(gdal, names, props, prefix, shown)
        dry = [t for t in tokens if not t.startswith(("--output", "--creation-option"))]
        for real, blank in blanks.items():
            dry = [t.replace(real, blank) for t in dry]
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
    fn, rasters: Sequence[bytes], values: Mapping, section=None, src_sizes=None
) -> Optional[Result]:
    """One call of `fn`'s spec over `rasters` (raster bytes: GeoTIFF, or another driver GeoBrix reads, translated
    first) with `values` (param -> value): its Result, or
    None for a NULL result. Raises ParamError for a value the function refuses. `section` is the allowlist
    (default the manifest's `gdal` section). `src_sizes` is one ``(width, height)`` or None per raster: the
    window's own size where the raster is a decimated read of it, which a size taken from a factor is a share of.
    """
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
            # A raster without the GeoTIFF magic is read under the light bytes rule (a driver that can name other
            # files is refused), and translated to GeoTIFF once the budget below holds (its props are its header's).
            sources = {}
            payloads = {}
            for i, raster in enumerate(rasters):
                data = bytes(raster)
                if data[:4] in _TIFF_MAGIC:
                    gdal.FileFromMemBuffer(names[i], data)
                else:
                    sources[i] = f"{prefix}/src{i}.bin"
                    shown[sources[i]] = shown.get(names[i], f"<input{i}>")
                    gdal.FileFromMemBuffer(sources[i], data)
                    payloads[i] = data
            drivers = _open_drivers(section or manifest.load().gdal)
            for i, src in sources.items():
                _bytes_source(gdal, fn.sql_name, i, src, payloads[i], drivers)
            props = [
                (
                    _props(gdal, fn.sql_name, sources[i], block.needs_crs, drivers)
                    if i in sources
                    else _props(gdal, fn.sql_name, n, block.needs_crs)
                )
                for i, n in enumerate(names)
            ]
            if src_sizes:
                props = [
                    (
                        dataclasses.replace(p, src_width=ss[0], src_height=ss[1])
                        if ss
                        else p
                    )
                    for p, ss in zip(props, src_sizes)
                ]
            sizes = [
                p.width * p.height * p.bands * argv_mod._ITEMSIZE.get(p.dtype, 8)
                for p in props
            ]
            # the read cap of every tile read from bytes (light's ``_serde.open_tile``), judged from its header
            from databricks.labs.gbx.pyrx.core.budget import (
                read_cap_refusal,
                tile_read_cap,
            )

            cap = tile_read_cap()
            for size in sizes:
                if size > cap:
                    raise DataError(f"{fn.sql_name}: {read_cap_refusal(size, cap)}")
            decoded = sum(sizes)
            budget = decoded_budget()
            # the texts of the call's text arguments (a color table) are files of their own under the prefix
            texts = argv_mod.text_values(fn, values)
            text_names = [f"{prefix}/arg_{k}.txt" for k in range(len(texts))]
            for k, (name, text) in enumerate(zip(text_names, texts)):
                gdal.FileFromMemBuffer(name, text.encode("utf-8"))
                shown[name] = f"<text{k}>"
            tokens = argv_mod.build(
                fn, values, props, names, out, section, texts=text_names
            )
            if tokens is None:
                return None
            # the call holds its inputs and its estimated output at once (spec 4.1)
            need = decoded + argv_mod.estimate_output(fn, tokens, props)
            if budget and need > budget:
                raise DataError(
                    f"{fn.sql_name}: the call needs {need // 2**20} MiB decoded (input and output), over this "
                    f"compute's {budget // 2**20} MiB. Fix: retile the tile first (gbx_rst_retile), or ask for a "
                    "smaller output"
                )
            for i, src in sources.items():
                _to_gtiff(gdal, fn.sql_name, src, names[i], drivers)
            command = _shown(" ".join([block.alg] + tokens), shown)
            fmt = next(
                t.split("=", 1)[1] for t in tokens if t.startswith("--output-format=")
            )
            if fmt != OUTPUT_FORMAT_OF.get(block.out_kind):
                raise ParamError(
                    f"{fn.sql_name}: the engine does not write {fmt} for a {block.out_kind} output"
                )
            data = argv_mod.data_bound(fn, values)

            def refused(msg):
                return (DataError if data else ParamError)(f"{fn.sql_name}: {msg}")

            if _keeps_input(gdal, fn, tokens, names[0]):
                return Result(_read(gdal, names[0]), None, command)
            if block.alg == "raster calc":
                _check_expression(gdal, fn, tokens, props, names, prefix, shown)
            if block.out_kind == "features":
                feats = _execute(
                    gdal,
                    block.alg,
                    tokens,
                    shown,
                    refused,
                    lambda alg: _features(
                        gdal,
                        alg,
                        block.out_fields,
                        argv_mod.level_snapper(fn, values),
                    ),
                )
                return Result(None, None, command, features=feats)
            _execute(gdal, block.alg, tokens, shown, refused)
            if block.alg == "raster aspect" and "--zero-for-flat" in tokens:
                _remask_zero_for_flat(gdal, names[0], out)
            if block.alg == "raster reproject":
                _carry_own_mask(gdal, tokens, names[0], out, prefix, shown)
            return Result(_read(gdal, out), None, command)
        except DataError as exc:
            return Result(None, _shown(str(exc), shown), command)
        finally:
            _remove(gdal, prefix)
