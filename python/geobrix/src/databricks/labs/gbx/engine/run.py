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
import math
import shutil
import threading
import uuid
from dataclasses import dataclass
from typing import Iterable, Mapping, Optional, Sequence

from databricks.labs.gbx import _gdal_policy, manifest
from databricks.labs.gbx.engine import argv as argv_mod
from databricks.labs.gbx.engine.spec import DataError, ParamError, output_format

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
    # a features output: one tuple per feature, in layer order: (WKB or None, the declared fields' values...). A list, or for
    # a streamed output (``FeatureStream``) an iterator that reads them from a file on local disk; the caller closes it.
    features: Optional[Iterable] = None
    # a JSON output: GDAL's text, with the engine's /vsimem names shown as <input>
    text: Optional[str] = None


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


def _open_source(gdal, sql_name: str, name: str, drivers: Sequence[str]):
    """The input `name` opened read-only with the `drivers` the call allows (its header is read from this handle), else
    a DataError carrying GDAL's reason."""
    gdal.ErrorReset()
    ds = gdal.OpenEx(
        name, gdal.OF_RASTER | gdal.OF_READONLY, allowed_drivers=list(drivers)
    )
    if ds is None:
        raise DataError(
            gdal.GetLastErrorMsg()
            or f"{sql_name}: the input is not a raster GeoBrix reads"
        )
    return ds


def _props(
    gdal,
    sql_name: str,
    name: str,
    needs_crs: bool,
    drivers: Sequence[str] = ("GTiff",),
    no_band_ok: bool = False,
) -> argv_mod.RasterProps:
    """The header of the input `name`. A dataset with no band (a netCDF or HDF container of subdatasets) is a DataError,
    unless the row only describes it (`no_band_ok`, a JSON row): its props then hold no band.
    """
    ds = _open_source(gdal, sql_name, name, drivers)
    try:
        if ds.RasterCount == 0:
            if not no_band_ok:
                raise DataError(f"{sql_name}: the tile has no band to read")
            return argv_mod.RasterProps(ds.RasterXSize, ds.RasterYSize, 0, "Byte")
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
            mask=_carries_mask(gdal, ds),
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


def _describe_in_place(gdal, tokens, names, sources, drivers) -> list:
    """The argv of a JSON row with each input that was not a GeoTIFF read where it is, in the driver that identifies it
    (its `src<i>.bin`, already judged by the light bytes rule), not the translated GeoTIFF.
    """
    out = list(tokens)
    for i, src in sources.items():
        drv = gdal.IdentifyDriverEx(src, gdal.OF_RASTER, allowed_drivers=list(drivers))
        out = [
            (
                f"--input={src}"
                if t == f"--input={names[i]}"
                else (
                    f"--input-format={drv.ShortName}"
                    if t == "--input-format=GTiff"
                    else t
                )
            )
            for t in out
        ]
    return out


def _drop_stored_statistics(gdal, paths) -> None:
    """Remove the STATISTICS_* tags a staged input carries. ``raster info --stats`` returns a band's stored statistics
    instead of computing them, so a tile whose tags came from its source (a clip, a derived tile) would report the
    source's minimum, maximum and mean. Each path is the engine's own staged copy under the call's /vsimem prefix (never
    the caller's bytes), opened as GTiff only; a tile in another format stores no such tag in its bytes and is left as
    it is."""
    for path in paths:
        ds = _open_geotiff(gdal, path, update=True)
        if ds is None:
            continue
        try:
            for k in range(1, ds.RasterCount + 1):
                band = ds.GetRasterBand(k)
                md = band.GetMetadata() or {}
                kept = {n: v for n, v in md.items() if not n.startswith("STATISTICS_")}
                if len(kept) != len(md):
                    band.SetMetadata(kept)
        finally:
            ds.FlushCache()
            ds = None


def _read(gdal, name: str) -> bytes:
    size = gdal.VSIStatL(name).size
    fh = gdal.VSIFOpenL(name, "rb")
    try:
        return bytes(gdal.VSIFReadL(1, size, fh))
    finally:
        gdal.VSIFCloseL(fh)


def _position(
    gdal, osr, sql_name: str, name: str, x: float, y: float, h: float, src_text: str
) -> str:
    """The observer as ``x,y,h`` in the CRS of the input `name`: moved from its own CRS when it has one that differs. Both
    references take the traditional GIS axis order (x east, y north), so the same digits come out on every tier. A point
    that cannot be moved into the input's CRS is a DataError.
    """
    ds = _open_source(gdal, sql_name, name, ("GTiff",))
    try:
        dst = ds.GetSpatialRef()
        if src_text and dst is not None:
            src = argv_mod._osr_reference(src_text)
            dst = dst.Clone()
            for sr in (src, dst):
                sr.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
            if not src.IsSame(dst):
                with osr.ExceptionMgr(useExceptions=True):
                    try:
                        x, y, _z = osr.CoordinateTransformation(
                            src, dst
                        ).TransformPoint(x, y)
                    except RuntimeError as exc:
                        raise DataError(
                            f"{sql_name}: the observer cannot be moved into the raster's CRS ({exc})"
                        ) from None
                if not (math.isfinite(x) and math.isfinite(y)):
                    raise DataError(
                        f"{sql_name}: the observer cannot be moved into the raster's CRS"
                    )
    finally:
        ds = None
    f = argv_mod.fmt_double
    return f"{f(x)},{f(y)},{f(h)}"


def _place_observer(gdal, fn, values, tokens, name: str, command, shown):
    """``(tokens, command)`` with the observer's placeholder replaced by its position in the input's CRS; both as they
    were for a row without an observer."""
    if argv_mod.observer_arg(fn) is None:
        return tokens, command
    obs = argv_mod.observer_of(fn, values)
    pos = _position(gdal, argv_mod._osgeo().osr, fn.sql_name, name, *obs)
    tokens = [t.replace(argv_mod.OBSERVER, pos) for t in tokens]
    return tokens, _shown(" ".join([fn.gdal.alg] + tokens), shown)


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
    that CRS: the result is the input as it is, not a warp of it. The CRSs are compared as CRSs (``core.crs.crs_equal``),
    so a source whose WKT carries no authority node, or spells the CRS differently, is still the target. A tile with no
    CRS is warped.
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
        from databricks.labs.gbx.core.crs import crs_equal

        return crs_equal(have.ExportToWkt(), target)
    finally:
        ds = None


def _warp_grid(gdal, sql_name: str, name: str, tokens, drivers=("GTiff",)):
    """The grid GDAL's reprojection picks for the input `name` before any ``--size`` or ``--resolution`` is applied
    (``gdal.SuggestedWarpOutput``, the call the algorithm makes): a ``WarpGrid`` in the units of the target CRS
    (``--output-crs``, else the input's), or None where GDAL gives none. Only the header is read. It accounts for a
    rotated or GCP-georeferenced tile and for a target CRS that changes the pixel count, which the input's own size and
    north-up pixel size do not.
    """
    target = next(
        (t.split("=", 1)[1] for t in tokens if t.startswith("--output-crs=")), None
    )
    ds = _open_source(gdal, sql_name, name, drivers)
    try:
        try:
            got = gdal.SuggestedWarpOutput(
                ds, [] if target is None else [f"DST_SRS={target}"]
            )
        except (
            Exception
        ):  # noqa: BLE001 - no grid GDAL can suggest: the estimate falls back to the input's size
            return None
        if got is None:
            return None
        gt = got.geotransform
        return argv_mod.WarpGrid(
            int(got.width), int(got.height), abs(gt[1]), abs(gt[5])
        )
    finally:
        ds = None


def _mask_flags(gdal, ds) -> tuple:
    """``(own, declared)`` of the open GeoTIFF `ds`: whether it has a mask band of its own (not an alpha band: JPEG-compressed
    imagery often has one), and whether it declares NoData or has an alpha band. Neither reads a pixel.

    A GeoTIFF has one mask for the whole dataset (its internal mask supports only ``GMF_PER_DATASET``) and one NoData
    value for all its bands (the TIFF tag holds a single value), so band 1 speaks for every band: a source whose bands
    differ cannot reach the engine in a GeoTIFF, and the translation to GeoTIFF of one that has them (a per-band mask in
    another driver) keeps band 1's.
    """
    bands = [ds.GetRasterBand(i + 1) for i in range(ds.RasterCount)]
    own = bands[0].GetMaskFlags() == gdal.GMF_PER_DATASET
    declared = any(b.GetNoDataValue() is not None for b in bands) or any(
        b.GetColorInterpretation() == gdal.GCI_AlphaBand for b in bands
    )
    return own, declared


def _carries_mask(gdal, ds) -> bool:
    """Whether a warp of `ds` carries a mask band (see ``_mask_to_carry``)."""
    own, declared = _mask_flags(gdal, ds)
    return own or not declared


def _mask_to_carry(gdal, ds):
    """What a warp of the open source `ds` must carry as a mask, as ``(valid, inverted)``, else None.

    The source's own mask band is carried: ``valid`` is its 255/0 bytes (the dataset's mask, see ``_mask_flags``). A source
    that declares no NoData in any band and has no alpha band gets its footprint as a mask: GDAL's warp fills the pixels
    no source pixel reaches with 0, which reads as a valid value, and GeoBrix never makes a value where data is missing.
    ``valid`` is then all 255, and ``inverted`` is False: a pixel the warp does not reach is masked. With a NoData value
    (or an alpha band) the pixels outside the footprint are already NoData (or transparent), so only a source's own mask
    is carried, ``inverted``: a pixel no source pixel reaches stays as the warp left it.
    """
    import numpy as np

    w, h = ds.RasterXSize, ds.RasterYSize
    own, declared = _mask_flags(gdal, ds)
    if own:
        valid = np.frombuffer(
            ds.GetRasterBand(1).GetMaskBand().ReadRaster(0, 0, w, h), "u1"
        )
    elif declared:
        return None
    else:
        valid = np.full(w * h, 255, "u1")
    return valid, declared


def _carry_mask(gdal, tokens, src: str, out: str, prefix: str, shown) -> None:
    """The output's own mask band, from the source (see ``_mask_to_carry``): the mask is warped like the pixels, as data
    (nearest), by the same arguments. The output gets no mask band when every pixel of it is valid; the bands stay as
    they are (no alpha band is added)."""
    import numpy as np

    ds = _open_geotiff(gdal, src)
    try:
        carry = _mask_to_carry(gdal, ds)
        if carry is None:
            return
        valid, inverted = carry
        w, h = ds.RasterXSize, ds.RasterYSize
        # a pixel the mask warp does not reach is 0: valid pixels are written 255 (or, inverted, masked ones 255)
        coverage = np.where(valid == 0, 255, 0) if inverted else valid
        if inverted and not coverage.any():
            return
        mem = gdal.GetDriverByName("MEM").Create("", w, h, 1, gdal.GDT_Byte)
        if ds.GetGCPCount():
            # a tile georeferenced by ground control points: the mask warps on the same points, not on a default grid
            mem.SetGCPs(ds.GetGCPs(), ds.GetGCPSpatialRef())
        else:
            mem.SetGeoTransform(ds.GetGeoTransform())
            mem.SetProjection(ds.GetProjection())
        mem.GetRasterBand(1).WriteRaster(0, 0, w, h, coverage.astype("u1").tobytes())
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
        out_valid = np.where(warped == 0, 255, 0) if inverted else warped
        if out_valid.all():
            return
        with gdal.config_options({"GDAL_TIFF_INTERNAL_MASK": "YES"}, thread_local=True):
            od = _open_geotiff(gdal, out, update=True)
            if (od.RasterXSize, od.RasterYSize) != (wd.RasterXSize, wd.RasterYSize):
                raise DataError(
                    "the source's mask band does not warp onto the output grid"
                )
            od.CreateMaskBand(gdal.GMF_PER_DATASET)
            od.GetRasterBand(1).GetMaskBand().WriteRaster(
                0, 0, od.RasterXSize, od.RasterYSize, out_valid.astype("u1").tobytes()
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


SCRATCH_PREFIX = "gbx_spec_"
_swept = False


def _output_name(block, prefix: str):
    """``(the output's name, the folder it needs removed or None)``: a GeoTIFF under the call's /vsimem prefix, or for a
    streamed features output a GeoPackage in a new folder on local disk, which holds what GDAL has written so memory does
    not."""
    if block.out_kind == "features" and block.out_stream:
        from databricks.labs.gbx.pyrx.core.local_temp import (
            gc_stale_local_temp,
            new_local_temp_dir,
        )

        global _swept
        if (
            not _swept
        ):  # a worker ended with os._exit leaves its folder: sweep the old ones once a process
            _swept = True
            gc_stale_local_temp(SCRATCH_PREFIX)
        scratch = new_local_temp_dir(SCRATCH_PREFIX)
        return f"{scratch}/out.gpkg", scratch
    return f"{prefix}/out.tif", None


def _cleanup(gdal, prefix: str, scratch: Optional[str]) -> None:
    """Remove the call's /vsimem files and its local folder (None when a stream took it over)."""
    _remove(gdal, prefix)
    if scratch is not None:
        shutil.rmtree(scratch, ignore_errors=True)


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
    out = list(_layer_rows(lyr, fields, fix))
    del lyr, ds
    return out


_READ = {
    "DOUBLE": "GetFieldAsDouble",
    "INT": "GetFieldAsInteger",
    "BIGINT": "GetFieldAsInteger64",
    "STRING": "GetFieldAsString",
}


def _layer_rows(lyr, fields, fix=None):
    """The features of the layer `lyr` one at a time, as (ISO WKB, field values...) tuples."""
    fix = fix or {}
    for f in lyr:
        g = f.GetGeometryRef()
        yield (None if g is None else bytes(g.ExportToIsoWkb()),) + tuple(
            (
                fix.get(src, _same)(getattr(f, _READ[t])(src))
                if f.IsFieldSetAndNotNull(src)
                else None
            )
            for _name, t, src in fields
        )


class FeatureStream:
    """The features of a streamed output, read one at a time from the GeoPackage GDAL wrote on this worker's local disk.

    A feature whose value is NaN or infinite is left out, as a NoData pixel is: a NaN pixel is not data, and its polygon is
    a hole in its neighbours', as a masked pixel's would be. (The file stores a NaN as NULL.) The file and its folder are
    removed when the stream ends, is closed or is collected: the caller iterates it once and calls ``close``.
    """

    def __init__(self, gdal, path: str, scratch: str, fields):
        self._gdal, self._path, self._scratch, self._fields = (
            gdal,
            path,
            scratch,
            fields,
        )
        self._used = False

    def __iter__(self):
        if self._used:
            raise RuntimeError("a feature stream is read once")
        self._used = True
        gdal = self._gdal
        ds = gdal.OpenEx(
            self._path,
            gdal.OF_VECTOR | gdal.OF_READONLY,
            allowed_drivers=["GPKG"],
        )
        try:
            lyr = ds.GetLayer(0) if ds is not None else None
            if lyr is None:
                raise DataError(
                    gdal.GetLastErrorMsg() or "GDAL's output could not be read back"
                )
            doubles = [
                i + 1 for i, (_n, t, _s) in enumerate(self._fields) if t == "DOUBLE"
            ]
            for row in _layer_rows(lyr, self._fields):
                # the file stores a NaN as NULL
                if all(row[i] is not None and math.isfinite(row[i]) for i in doubles):
                    yield row
        finally:
            lyr = ds = None
            self.close()

    def close(self) -> None:
        """Remove the file and its folder (safe to call again)."""
        scratch, self._scratch = self._scratch, None
        if scratch is not None:
            shutil.rmtree(scratch, ignore_errors=True)

    def __del__(self):
        self.close()


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


def _fill_bands(
    gdal, alg: str, tokens, src: str, out: str, prefix: str, shown, refused, bands: int
) -> None:
    """A per-band algorithm (``GdalSpec.per_band``) over an input of several bands. GDAL's fill writes the one band it is
    given, so the engine runs it once for each band (``--band=k``) and writes each result into the same band of a copy of
    the input made with the call's creation options: every other property of the tile (NoData, colour interpretation,
    georeferencing, the bands the algorithm does not touch) is the input's. ``out`` holds band 1's result on entry and
    the whole tile on return.
    """
    first = f"{prefix}/band_1.tif"
    gdal.Rename(out, first)
    co = [t.split("=", 1)[1] for t in tokens if t.startswith("--creation-option=")]
    gdal.ErrorReset()
    sds = _open_geotiff(gdal, src)
    try:
        copy = gdal.Translate(
            out, sds, options=gdal.TranslateOptions(format="GTiff", creationOptions=co)
        )
    finally:
        sds = None
    if copy is None:
        raise DataError(gdal.GetLastErrorMsg() or "the tile could not be copied")
    try:
        for k in range(1, bands + 1):
            part = first if k == 1 else f"{prefix}/band_{k}.tif"
            if k > 1:
                toks = [
                    (
                        f"--band={k}"
                        if t == "--band=1"
                        else f"--output={part}" if t.startswith("--output=") else t
                    )
                    for t in tokens
                ]
                _execute(gdal, alg, toks, shown, refused)
            _copy_band(
                gdal, part, copy.GetRasterBand(k), copy.RasterXSize, copy.RasterYSize
            )
            gdal.Unlink(part)
        copy.FlushCache()
    finally:
        copy = None


def _unmask_filled(
    gdal, alg: str, tokens, src: str, out: str, prefix: str, shown, refused
) -> None:
    """A source whose validity is its own mask band (not a NoData value): GDAL's fill reads that mask and leaves it as it was,
    so a pixel it filled would still read as masked. The output mask is made ``input valid OR filled by GDAL``; a pixel the
    search did not reach stays masked.

    Which pixels GDAL fills is found by GDAL: the same fill (same ``--max-distance``, no smoothing) over a Byte raster of 1
    where the input is valid and NoData (0) elsewhere. A pixel the fill reaches comes back 1, so the answer needs no
    comparison of values, which a filled value equal to the masked one would defeat.
    """
    import numpy as np

    sds = _open_geotiff(gdal, src)
    try:
        if sds.GetRasterBand(1).GetMaskFlags() != gdal.GMF_PER_DATASET:
            return
        w, h = sds.RasterXSize, sds.RasterYSize
        valid = np.frombuffer(
            sds.GetRasterBand(1).GetMaskBand().ReadRaster(0, 0, w, h), "u1"
        )
        gt = sds.GetGeoTransform()
    finally:
        sds = None
    if valid.all():
        return
    mem = gdal.GetDriverByName("MEM").Create("", w, h, 1, gdal.GDT_Byte)
    mem.SetGeoTransform(gt)
    band = mem.GetRasterBand(1)
    band.SetNoDataValue(0)
    band.WriteRaster(0, 0, w, h, (valid != 0).astype("u1").tobytes())
    cover_in, cover_out = f"{prefix}/cover_in.tif", f"{prefix}/cover_out.tif"
    gdal.GetDriverByName("GTiff").CreateCopy(cover_in, mem)
    mem = band = None
    toks = [
        (
            f"--input={cover_in}"
            if t.startswith("--input=")
            else (
                f"--output={cover_out}"
                if t.startswith("--output=")
                else (
                    "--smoothing-iterations=0"
                    if t.startswith("--smoothing-iterations=")
                    else t
                )
            )
        )
        for t in tokens
        if not t.startswith("--creation-option=")
    ]
    _execute(gdal, alg, toks, shown, refused)
    cds = _open_geotiff(gdal, cover_out)
    try:
        filled = np.frombuffer(cds.GetRasterBand(1).ReadRaster(0, 0, w, h), "u1") != 0
    finally:
        cds = None
    mask = np.where((valid != 0) | filled, 255, 0).astype("u1").tobytes()
    with gdal.config_options({"GDAL_TIFF_INTERNAL_MASK": "YES"}, thread_local=True):
        od = _open_geotiff(gdal, out, update=True)
        try:
            if od.GetRasterBand(1).GetMaskFlags() != gdal.GMF_PER_DATASET:
                od.CreateMaskBand(gdal.GMF_PER_DATASET)
            od.GetRasterBand(1).GetMaskBand().WriteRaster(0, 0, w, h, mask)
            od.FlushCache()
        finally:
            od = None


def _alpha_into_mask(gdal, src: str, band: int) -> None:
    """A tile whose last band is an alpha band that GDAL does not read as the mask of `band` (GDAL derives a mask from alpha
    only for 2 and 4 bands, and not over a declared NoData): the pixels the algorithm leaves out are made the tile's own
    mask band, ``valid for the band AND opaque``. The tile's transparent pixels are no data on every level, as its NoData
    pixels are. The alpha band itself is judged by its own mask, as GDAL judges it.
    """
    import numpy as np

    ds = _open_geotiff(gdal, src)
    try:
        n = ds.RasterCount
        alpha, data = ds.GetRasterBand(n), ds.GetRasterBand(band)
        if (
            n < 2
            or band == n
            or alpha.GetColorInterpretation() != gdal.GCI_AlphaBand
            or data.GetMaskFlags() & gdal.GMF_ALPHA
        ):
            return
        w, h = ds.RasterXSize, ds.RasterYSize
        valid = np.frombuffer(data.GetMaskBand().ReadRaster(0, 0, w, h), "u1") != 0
        opaque = (
            np.frombuffer(alpha.ReadRaster(0, 0, w, h, buf_type=gdal.GDT_Float64), "f8")
            > 0
        )  # 0, negative and NaN are transparent
        mask = np.where(valid & opaque, 255, 0).astype("u1").tobytes()
    finally:
        ds = alpha = data = None
    with gdal.config_options({"GDAL_TIFF_INTERNAL_MASK": "YES"}, thread_local=True):
        od = _open_geotiff(gdal, src, update=True)
        try:
            if od.GetRasterBand(1).GetMaskFlags() != gdal.GMF_PER_DATASET:
                od.CreateMaskBand(gdal.GMF_PER_DATASET)
            od.GetRasterBand(1).GetMaskBand().WriteRaster(0, 0, w, h, mask)
            od.FlushCache()
        finally:
            od = None


def _prepare_input(gdal, fn, tokens, props, names, prefix: str, shown) -> None:
    """The checks and input changes one algorithm needs before it runs: a map-algebra expression is checked on blank
    inputs, and polygonize reads a transparent pixel as masked (``_alpha_into_mask``).
    """
    alg = fn.gdal.alg
    if alg == "raster calc":
        _check_expression(gdal, fn, tokens, props, names, prefix, shown)
    elif alg == "raster polygonize":
        _alpha_into_mask(
            gdal, names[0], int(argv_mod._token_value(tokens, "band") or 1)
        )


def _copy_band(gdal, part: str, band, w: int, h: int) -> None:
    """The pixels of band 1 of the GeoTIFF `part`, written into `band` (same size and type)."""
    ds = _open_geotiff(gdal, part)
    try:
        if (
            band.WriteRaster(0, 0, w, h, ds.GetRasterBand(1).ReadRaster(0, 0, w, h))
            != 0
        ):
            raise DataError(gdal.GetLastErrorMsg() or "a band could not be written")
    finally:
        ds = None


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


def _stage(gdal, fn, rasters, names, prefix, shown, section, src_sizes) -> tuple:
    """``(sources, drivers, props)`` of the call's inputs, written under its prefix. A raster with the GeoTIFF magic is
    written as ``in<i>.tif``. Any other is read under the light bytes rule (a driver that can name other files is
    refused) as ``src<i>.bin``, and translated to GeoTIFF once the budget holds (its props are its header's):
    `sources` maps its index to that name. `src_sizes` marks a raster that is a decimated read of its window.
    """
    block = fn.gdal
    sources, payloads = {}, {}
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
    describes = block.out_kind == "json"
    for i, src in sources.items():
        _bytes_source(gdal, fn.sql_name, i, src, payloads[i], drivers)
    props = [
        (
            _props(gdal, fn.sql_name, sources[i], block.needs_crs, drivers, describes)
            if i in sources
            else _props(gdal, fn.sql_name, n, block.needs_crs, no_band_ok=describes)
        )
        for i, n in enumerate(names)
    ]
    if src_sizes:
        props = [
            (dataclasses.replace(p, src_width=ss[0], src_height=ss[1]) if ss else p)
            for p, ss in zip(props, src_sizes)
        ]
    return sources, drivers, props


def _judge_budget(
    gdal, fn, tokens, props, names, sources, drivers, decoded, budget
) -> None:
    """DataError (with a Fix) unless the call's inputs and its estimated output fit the decoded budget at once (spec
    4.1). A reprojection's output grid is the one GDAL picks (``_warp_grid``), read from the header.
    """
    grid = None
    if fn.gdal.alg == "raster reproject":
        grid = (
            _warp_grid(gdal, fn.sql_name, sources[0], tokens, drivers)
            if 0 in sources
            else _warp_grid(gdal, fn.sql_name, names[0], tokens)
        )
    need = decoded + argv_mod.estimate_output(fn, tokens, props, grid)
    if budget and need > budget:
        raise DataError(
            f"{fn.sql_name}: the call needs {need // 2**20} MiB decoded (input and output), over this "
            f"compute's {budget // 2**20} MiB. Fix: retile the tile first (gbx_rst_retile), or ask for a "
            "smaller output"
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
    out, scratch = _output_name(block, prefix)
    placeholders = (
        ["<input>"]
        if block.inputs[0][1] == "raster"
        else [f"<input{i}>" for i in range(len(rasters))]
    )
    shown = {**dict(zip(names, placeholders)), out: "<output>"}
    command = None
    with engine_settings(), _gdal_status() as gdal:
        try:
            sources, drivers, props = _stage(
                gdal, fn, rasters, names, prefix, shown, section, src_sizes
            )
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
            command = _shown(" ".join([block.alg] + tokens), shown)
            fmt = next(
                t.split("=", 1)[1] for t in tokens if t.startswith("--output-format=")
            )
            if fmt != output_format(block):
                raise ParamError(
                    f"{fn.sql_name}: the engine does not write {fmt} for a {block.out_kind} output"
                )
            data = argv_mod.data_bound(fn, values)

            def refused(msg):
                return (DataError if data else ParamError)(f"{fn.sql_name}: {msg}")

            # A tile that is already in the CRS the row asks for comes back as its own bytes, before the budget is judged
            # (nothing is decoded or written) and before a translation. A tile read in another driver is judged after it
            # is translated: the bytes returned are the GeoTIFF's.
            if 0 not in sources and _keeps_input(gdal, fn, tokens, names[0]):
                return Result(_read(gdal, names[0]), None, command)
            _judge_budget(
                gdal, fn, tokens, props, names, sources, drivers, decoded, budget
            )
            if block.out_kind == "json":
                # a JSON row describes the tile as it is: in its own driver, not translated (a container has no band
                # to translate)
                tokens = _describe_in_place(gdal, tokens, names, sources, drivers)
                command = _shown(" ".join([block.alg] + tokens), shown)
                _drop_stored_statistics(
                    gdal, [sources.get(i, names[i]) for i in range(len(names))]
                )
            else:
                for i, src in sources.items():
                    _to_gtiff(gdal, fn.sql_name, src, names[i], drivers)
            if 0 in sources and _keeps_input(gdal, fn, tokens, names[0]):
                return Result(_read(gdal, names[0]), None, command)
            _prepare_input(gdal, fn, tokens, props, names, prefix, shown)
            tokens, command = _place_observer(
                gdal, fn, values, tokens, names[0], command, shown
            )
            if block.out_kind == "json":
                text = _execute(
                    gdal,
                    block.alg,
                    tokens,
                    shown,
                    refused,
                    lambda alg: str(alg["output-string"]),
                )
                return Result(None, None, command, text=_shown(text, shown))
            if block.out_kind == "features" and block.out_stream:
                _execute(gdal, block.alg, tokens, shown, refused)
                stream = FeatureStream(gdal, out, scratch, block.out_fields)
                scratch = None  # the stream removes it
                return Result(None, None, command, features=stream)
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
            if block.per_band and props[0].bands > 1:
                _fill_bands(
                    gdal,
                    block.alg,
                    tokens,
                    names[0],
                    out,
                    prefix,
                    shown,
                    refused,
                    props[0].bands,
                )
            if block.alg == "raster fill-nodata":
                _unmask_filled(
                    gdal, block.alg, tokens, names[0], out, prefix, shown, refused
                )
            if block.alg == "raster aspect" and "--zero-for-flat" in tokens:
                _remask_zero_for_flat(gdal, names[0], out)
            if block.alg == "raster reproject":
                _carry_mask(gdal, tokens, names[0], out, prefix, shown)
            return Result(_read(gdal, out), None, command)
        except DataError as exc:
            return Result(None, _shown(str(exc), shown), command)
        finally:
            _cleanup(gdal, prefix, scratch)
