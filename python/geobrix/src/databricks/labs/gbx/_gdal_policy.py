"""The process-level defence against crafted data: the one choke point for GeoBrix's GDAL.

A table or a Volume another principal wrote can name any file, and a VRT, a tile index or a pipeline in it can name
more. GDAL opens them through the process's driver registry, with no driver list of its own. So the defence is set
where GeoBrix first touches GDAL, not per call:

- ``prepare()`` runs when the package is imported (``databricks/labs/gbx/__init__.py``, right after the native payload
  is served). It imports nothing from GDAL (the light tier's lazy-import contract holds). It puts ``GDAL_SKIP`` (the
  refused list, added to a user's value) and the two VRT settings in the process environment. GDAL reads ``GDAL_SKIP``
  when it registers its drivers, so a rasterio or osgeo that registers them after this import (rasterio does at its
  first open or ``rasterio.Env``, osgeo and pyogrio at their import) never registers a refused one. Where a GDAL is
  loaded already (a libgdal is mapped into the process, or a GDAL binding is imported where ``/proc`` is absent), it
  runs ``activate()`` at once, which imports rasterio and osgeo to compare their libgdal.
- ``activate()`` runs at the first GeoBrix use that needs GDAL (an open made through ``open_data``, the engine's GDAL,
  ``state()``). It is idempotent and thread-safe. Where the payload's osgeo shares rasterio's libgdal ("full"), it
  deregisters any refused driver still registered and sets ``GDAL_VRT_ENABLE_RAWRASTERBAND=NO`` and
  ``GDAL_VRT_ENABLE_PYTHON=NO`` for the process. Without it ("partial": a PyPI rasterio, a dev host), the settings go
  through rasterio's config; a refused driver cannot be deregistered there, so if rasterio registered its drivers
  before GeoBrix was imported, ``activate()`` warns once, names the drivers and the fix.
- ``open_data(fp, mode, **kw)`` is the only way GeoBrix opens a raster by rasterio, and ``open_bytes(data)`` the only
  way it opens raster bytes. Every read is an open with the allowed drivers as GDAL's driver list, never with the
  whole registry. In full mode a named file is first identified with the allowed drivers and opened with the one
  identified, so identify and open cannot disagree. A file no allowed driver reads is refused with the policy's
  reason; a missing file stays the binding's own I/O error.
- ``pyogrio()`` returns pyogrio after ``activate()``: GeoBrix's vector reads take it from there.

One switch turns the whole defence off: ``GBX_GDAL_DRIVER_POLICY=off`` in the environment, which this module reads
(a Python process reads no Spark conf); the JVM's ``GDALManager`` reads the same variable, or
``spark.databricks.labs.gbx.gdal.driver_policy=off`` in the cluster's Spark config.
"""

from __future__ import annotations

import contextlib
import functools
import os
import re
import sys
import threading
import warnings
from typing import Dict, Optional

from databricks.labs.gbx import manifest

ENV = "GBX_GDAL_DRIVER_POLICY"
SETTINGS = {"GDAL_VRT_ENABLE_RAWRASTERBAND": "NO", "GDAL_VRT_ENABLE_PYTHON": "NO"}
# The phrase every refusal of this policy ends with (the rows read it).
REFUSED_PHRASE = "which GeoBrix does not open"

# GDAL's messages for a source the policy stopped, and what GeoBrix calls it. Each is GDAL 3.13.3's own text. "not
# recognized" is the message for a driver that is not registered (a nested source), so it is read only where a
# nested source can be the cause (nested=True): a data-named file is judged by its identify, not by a message.
_NESTED_REFUSALS = (
    (
        "not recognized as being in a supported file format",
        "a source GDAL has no driver for here",
        True,
    ),
    ("VRTRawRasterBand support has been disabled", "a VRT raw band", False),
    ("explicitly disabled", "a VRT Python pixel function", False),
    ("uses inline code in the VRT", "a VRT Python pixel function", False),
)

# GDAL's message for a file no driver in the open's list reads, and for a missing one (GDAL 3.13.3's own text).
_NOT_RECOGNIZED = "not recognized as being in a supported file format"
_MISSING = "No such file or directory"

_LOCK = threading.RLock()
_STATE: Optional[Dict] = None
_PREPARED = False
# The connection prefixes of the refused drivers ("GTI:" ...), compared without case: GDAL names the prefix of a driver
# it does not have as a missing file, so the policy's reason is read from that message. Seeded from the refused names
# at prepare() (a skipped driver is never registered, so its own prefix cannot be read); a deregistered driver's own
# prefix is added.
_DEREGISTERED_PREFIXES: set = set()


def gap_warning(by_binding) -> str:
    """The warning of a partial process where a GDAL binding registered refused drivers before GeoBrix was imported:
    ``by_binding`` maps the binding (rasterio, pyogrio) to the refused drivers its GDAL has.
    """
    found = "; ".join(
        f"{b}: {', '.join(names)}" for b, names in sorted(by_binding.items())
    )
    return (
        f"GeoBrix's GDAL driver policy cannot remove these drivers from this process ({found}). They were registered "
        "before databricks.labs.gbx was imported (rasterio registers GDAL's drivers at its first open or "
        "rasterio.Env, pyogrio at its import). GeoBrix opens each raster file and each tile's bytes with the allowed "
        "drivers only, but a source that a VRT, an index or a vector file names is opened through that binding's "
        "whole registry, so a file another principal wrote can still reach them. Fix: import databricks.labs.gbx "
        "before rasterio opens anything and before pyogrio is imported, or install the GeoBrix env wheel for the "
        "runtime (its vendored bindings are the ones the policy fully covers)."
    )


class DriverPolicyError(ValueError):
    """A raster the process-level driver policy refuses. The message names the file, the refused kind, and the
    policy's reason (``REFUSED_PHRASE``)."""


class RemotePathError(DriverPolicyError):
    """A path that comes from data (a tile's path, a tile index's row, a VRT's source, a vector table's path) and
    names a remote location the remote-path gate refuses. The message names the location, the switch and the fix.
    """


# The remote-path gate: what a path that comes from data may name. Local and FUSE paths (no scheme: /Volumes,
# /Workspace, /dbfs, a relative path; the dbfs: and file: spellings), the cloud object stores, the archive wrappers
# around an allowed path, and GeoBrix's own /vsimem/. Anything else that names a location (/vsicurl/, http(s)://,
# ftp://, another GDAL virtual file system), at any depth of a wrapper, is refused unless REMOTE_ENV allows it.
REMOTE_ENV = "GBX_ALLOW_REMOTE_PATHS"
_DATA_SCHEMES = frozenset(
    {"s3", "s3a", "abfss", "gs", "wasbs", "zip", "tar", "file", "dbfs"}
)
_DATA_VSI = frozenset({"s3", "az", "adls", "gs", "mem", "zip", "tar"})
_SCHEME = re.compile(r"(?<![a-z0-9+.\-])([a-z][a-z0-9+.\-]*)://", re.IGNORECASE)
# a GDAL virtual file system prefix where a file name starts: the start, or after a wrapper's / { " ' : , or a space
_VSI = re.compile(r"(?:^|(?<=[/{\"':,\s]))/vsi([a-z0-9_]+)", re.IGNORECASE)


def remote_paths_allowed() -> bool:
    """True when the remote-path switch (``GBX_ALLOW_REMOTE_PATHS``) is on in this process's environment."""
    return os.environ.get(REMOTE_ENV, "").strip().lower() in ("1", "true", "yes", "on")


def remote_location(path) -> Optional[str]:
    """The location a path names that the remote-path gate refuses (``/vsicurl/``, ``https://`` ...), else None.
    A wrapper is judged by every path it wraps (``/vsizip//vsicurl/...`` names ``/vsicurl/``).
    """
    text = path if isinstance(path, str) else os.fspath(path)
    for m in _VSI.finditer(text):
        if m.group(1).lower() not in _DATA_VSI:
            return f"/vsi{m.group(1).lower()}/"
    for m in _SCHEME.finditer(text):
        scheme = m.group(1).lower()
        if any(part not in _DATA_SCHEMES for part in scheme.split("+")):
            return f"{scheme}://"
    return None


def data_path(path, what: str = "a path from data"):
    """``path``, when it is one GeoBrix opens for a path that comes from data; ``RemotePathError`` when it names a
    remote location (``remote_location``) and the switch is off. The one gate for those paths: a virtual tile's path
    (``open_tile``'s resolver, the engine, vizx), a ``manifest`` or tile-index row, a VRT's member, the
    ``vector_file_read`` table's path. A path the caller passes itself (``.load(path)``, ``rst_fromfile``) is the
    caller's choice and is not judged here."""
    if path is None or remote_paths_allowed():
        return path
    where = remote_location(path)
    if where is None:
        return path
    raise RemotePathError(
        f"{what}: {os.fspath(path)!r} names a remote location ({where}), which GeoBrix does not open for a path that "
        "comes from data (a tile's path, a tile index's row, a VRT's source, a vector table's path). Fix: copy the "
        "data to a Volume or a cloud object store (s3://, abfss://, gs://), or, for data you trust, set "
        f"{REMOTE_ENV}=true in the environment of the processes that read it"
    )


def enabled() -> bool:
    """False when the one switch is set to off in the environment."""
    return os.environ.get(ENV, "").strip().lower() != "off"


@functools.lru_cache(maxsize=1)
def allowed_names() -> tuple:
    """Every raster driver GeoBrix opens: the manifest's classification, allowed (never refused)."""
    return tuple(manifest.load().gdal.allowed_drivers)


@functools.lru_cache(maxsize=1)
def refused_names() -> tuple:
    """The drivers the policy refuses: deregistered (full) or skipped by the JVM at GDAL's start."""
    return tuple(manifest.load().gdal.refused_drivers)


def chain_text(exc: BaseException) -> str:
    """The messages of an exception and of what it was raised after. rasterio reports a failed read as 'Read failed.
    See previous exception' with GDAL's own message in the exception before it, so the reason is read from both.
    """
    parts, cur = [], exc
    while cur is not None and len(parts) < 8:
        parts.append(str(cur))
        cur = cur.__cause__ or cur.__context__
    return " | ".join(parts)


def policy_reason(message: str, nested: bool = True) -> Optional[str]:
    """What the policy refused, for a GDAL message raised while a raster was opened or read; None when the message is
    not a refusal of the policy (a missing file, a corrupt one)."""
    if nested and _MISSING in message and _names_a_refused_connection(message):
        # a refused driver's connection string (GTI:/path) is no file to GDAL: it reports it as missing
        return "a source GDAL has no driver for here"
    for needle, what, needs_nested in _NESTED_REFUSALS:
        if needle in message and (nested or not needs_nested):
            return what
    return None


def _prefix_pattern(prefixes) -> Optional["re.Pattern"]:
    if not prefixes:
        return None
    alts = "|".join(re.escape(p) for p in sorted(prefixes, key=len, reverse=True))
    # a name starts at the start of the text, or after a space, a quote, a backtick, a bracket or the chain's |
    return re.compile(rf"(?:^|(?<=[\s'\"`(|]))(?:{alts})", re.IGNORECASE)


def _names_a_refused_connection(text: str, at_start: bool = False) -> bool:
    """Whether ``text`` names a refused driver's connection string (``GTI:``, ``WMS:``, ``EEDAI:`` ...) where a file
    name starts (``at_start``: at the start of ``text`` itself). A file whose name merely ends like one
    (``missing.mrf``, ``tiles_wms``, ``a.gti``) does not."""
    if at_start:
        low = text.lower()
        # a URL (http://...) is no connection string, whatever its scheme spells
        return any(
            low.startswith(p.lower()) and low[len(p) : len(p) + 2] != "//"
            for p in _DEREGISTERED_PREFIXES
        )
    rx = _prefix_pattern(frozenset(_DEREGISTERED_PREFIXES))
    return rx is not None and rx.search(text) is not None


# The GDAL bindings that register GDAL's drivers when they load, for a process with no /proc/self/maps.
_GDAL_BINDINGS = ("rasterio", "osgeo", "pyogrio", "fiona")


def _gdal_loaded() -> bool:
    """Whether a GDAL is in the process already: a libgdal mapped (Linux ``/proc/self/maps``, which sees a binding
    whatever its module name), else a GDAL binding imported."""
    from databricks.labs.gbx import _osgeo

    mapped = _osgeo.mapped_libgdal()
    if mapped is not None:
        return bool(mapped)
    return any(m in sys.modules for m in _GDAL_BINDINGS)


def prepare() -> None:
    """The registration-time part of the policy, applied at package import (see the module docstring). Idempotent.
    It imports no GDAL binding itself, and sets the environment their driver registration reads; only where a GDAL is
    loaded already does it run ``activate()``, which imports rasterio and osgeo.
    """
    global _PREPARED
    with _LOCK:
        if _PREPARED:
            return
        _PREPARED = True
        if not enabled():
            return
        _DEREGISTERED_PREFIXES.update(f"{name}:" for name in refused_names())
        _skip_at_registration()
        if _gdal_loaded():
            # a GDAL is loaded already (rasterio, osgeo, pyogrio, fiona ...), and may have registered its drivers: the
            # skip cannot reach those, so the activation (the deregistration, or the partial-mode warning) runs now
            activate()


def activate() -> Dict:
    """The process's defence, applied once (see the module docstring). Returns the state: ``mode`` (off, full,
    partial or none), ``deregistered`` (the names removed) and ``settings`` (the process settings set); a partial
    state also names ``registered_refused``, the refused drivers rasterio had registered before GeoBrix ran.
    """
    global _STATE
    prepare()
    with _LOCK:
        if _STATE is None:
            _STATE = _apply()
        return _STATE


def state() -> Dict:
    """The state ``activate()`` settled on (activating it first if needed)."""
    return activate()


def pyogrio():
    """pyogrio, with the policy active first: GeoBrix's vector reads take the module from here, so the refused drivers
    are gone before their first open."""
    activate()
    import pyogrio as module

    return module


def _skip_at_registration() -> None:
    """GDAL_SKIP names the refused drivers, added to a user's value (a user's value cannot remove one), and the two VRT
    settings go in the environment, where GDAL's driver registration and its config lookups read them.
    """
    names = {t for t in re.split(r"[,\s]+", os.environ.get("GDAL_SKIP", "")) if t}
    os.environ["GDAL_SKIP"] = ",".join(sorted(names | set(refused_names())))
    os.environ.update(SETTINGS)


def _registered_refused() -> Dict[str, list]:
    """The refused drivers each GDAL binding of this process has registered, by binding: rasterio's registry, and
    pyogrio's when pyogrio is imported (a PyPI pyogrio bundles a GDAL of its own, and registers every driver at its
    import). Empty unless a binding registered before ``prepare()`` ran."""
    import rasterio

    refused = set(refused_names())
    with rasterio.Env() as env:
        found = {"rasterio": sorted(refused & set(env.drivers()))}
    ogr = sys.modules.get("pyogrio")
    if ogr is not None:
        try:
            found["pyogrio"] = sorted(refused & set(ogr.list_drivers()))
        except (
            Exception
        ):  # noqa: BLE001 - a pyogrio that cannot list its drivers names none
            pass
    return {b: names for b, names in found.items() if names}


def _deregister_mapped(refused) -> Dict[str, list]:
    """The refused drivers removed, by ``GDALDeregisterDriver(GDALGetDriverByName(name))`` through ctypes, from each
    libgdal this process maps (Linux ``/proc/self/maps``): a binding that bundles a GDAL of its own (a PyPI pyogrio,
    rasterio or fiona) and registered its drivers before GeoBrix was imported loses the refused ones too. Each library
    is reached with ``RTLD_NOLOAD`` (nothing new is loaded); one that is not loaded, or has no such symbols, is skipped.
    Off Linux (no maps) nothing is removed here, and the partial-mode warning names what stays.
    """
    import ctypes

    from databricks.labs.gbx import _osgeo

    removed: Dict[str, list] = {}
    for path in _osgeo.mapped_libgdal() or ():
        try:
            lib = ctypes.CDLL(path, mode=os.RTLD_NOLOAD | os.RTLD_LAZY)
            get, deregister = lib.GDALGetDriverByName, lib.GDALDeregisterDriver
        except (OSError, AttributeError):
            continue
        get.restype, get.argtypes = ctypes.c_void_p, [ctypes.c_char_p]
        deregister.restype, deregister.argtypes = None, [ctypes.c_void_p]
        gone = []
        for name in refused:
            handle = get(name.encode())
            if handle:
                deregister(handle)
                gone.append(name)
        if gone:
            removed[path] = gone
    return removed


def _apply() -> Dict:
    if not enabled():
        return {"mode": "off", "deregistered": [], "settings": []}
    try:
        from databricks.labs.gbx import _osgeo

        report = _osgeo._same_libgdal_report()
    except (
        Exception
    ) as exc:  # noqa: BLE001 - no rasterio at all: nothing to defend (the UC sandbox, a manifest user)
        return {
            "mode": "none",
            "deregistered": [],
            "settings": [],
            "reason": f"no rasterio: {exc}",
        }
    if report.get("shared"):
        from osgeo import gdal

        deregistered = []
        for name in refused_names():
            drv = gdal.GetDriverByName(name)
            if drv is not None:
                prefix = drv.GetMetadataItem("DMD_CONNECTION_PREFIX")
                if prefix:
                    _DEREGISTERED_PREFIXES.add(prefix)
                drv.Deregister()
                deregistered.append(name)
        for key, value in SETTINGS.items():
            gdal.SetConfigOption(key, value)
        # another GDAL in the process (a PyPI pyogrio beside the env wheel's bindings) loses them too
        foreign = _deregister_mapped(refused_names())
        return {
            "mode": "full",
            "deregistered": deregistered,
            "deregistered_by": foreign,
            "settings": sorted(SETTINGS),
        }
    import rasterio.env as rio_env

    for key, value in SETTINGS.items():
        rio_env.set_gdal_config(key, value)
    foreign = _deregister_mapped(refused_names())
    by_binding = _registered_refused()
    if by_binding:
        # the warning points at the caller's code: frames in this package (and the import machinery) are skipped
        warnings.warn(
            gap_warning(by_binding),
            RuntimeWarning,
            stacklevel=2,
            skip_file_prefixes=(os.path.dirname(os.path.abspath(__file__)),),
        )
    return {
        "mode": "partial",
        "deregistered": sorted(set().union(*foreign.values())),
        "deregistered_by": foreign,
        "settings": sorted(SETTINGS),
        "registered_refused": sorted(set().union(*by_binding.values())),
        "registered_refused_by": by_binding,
        "reason": report.get("vsimem_error")
        or report.get("error")
        or "osgeo is another GDAL",
    }


def _identify(path: str) -> Optional[str]:
    """The name of the allowed driver that identifies ``path``, or None (header read only; nothing is opened)."""
    from osgeo import gdal

    with gdal.ExceptionMgr(useExceptions=False):
        drv = gdal.IdentifyDriverEx(
            path, gdal.OF_RASTER, allowed_drivers=list(allowed_names())
        )
    return None if drv is None else drv.ShortName


def _exists(path: str) -> bool:
    return os.path.exists(path)


def _not_a_raster(what: str, driver: str) -> str:
    return (
        f"{what}: is not a raster any driver GeoBrix opens reads (it is a {driver} raster, a driver the policy "
        f"refuses), {REFUSED_PHRASE}"
    )


# How a refused driver's own file starts, for the refusal's message: an open the policy restricted fails with GDAL's
# "not recognized" whatever stopped it, so a refusal names a refused driver only when the bytes are that driver's; any
# other unreadable file or tile (a corrupt one, a format GDAL does not read) keeps the binding's own I/O error.
_REFUSED_SIGNATURES = (
    (b"gdal_streamed_alg", "GDALG"),
    (b"<gdaltileindexdataset", "GTI"),
    (b"<kml", "KMLSUPEROVERLAY"),
    (b"<mrf_meta>", "MRF"),
    (b"<gdal_wms>", "WMS"),
    (b"<gdal_wmts>", "WMTS"),
    (b"<wcs_gdal>", "WCS"),
    (b"<cacheinfo", "ESRIC"),
    (b'"stac_version"', "STACTA"),
)


def _head(fp, path: Optional[str]) -> bytes:
    """The first bytes of what an open was given (a named local file, a MemoryFile, a file-like), else b''."""
    try:
        if path is not None:
            if not _exists(path):
                return b""
            with open(path, "rb") as fh:
                return fh.read(4096)
        if hasattr(fp, "getbuffer"):  # a MemoryFile
            return bytes(fp.getbuffer()[:4096])
        if hasattr(fp, "read") and hasattr(fp, "seek") and hasattr(fp, "tell"):
            pos = fp.tell()
            try:
                return fp.read(4096)
            finally:
                fp.seek(pos)
    except Exception:  # noqa: BLE001 - an unreadable head names no refused driver
        return b""
    return b""


def _refused_format(head: bytes) -> Optional[str]:
    """The refused driver whose file ``head`` starts like, else None."""
    low = head.lower()
    for marker, driver in _REFUSED_SIGNATURES:
        if marker in low:
            return driver
    return None


def _refusal(
    what: str, path: Optional[str], exc: BaseException, fp=None
) -> Optional[DriverPolicyError]:
    """The policy's refusal for an error of an open restricted to the allowed drivers, or None (the binding's own
    error stays: a missing file, a corrupt one, a format GDAL does not read)."""
    text = chain_text(exc)
    if _NOT_RECOGNIZED in text and (path is None or path in text):
        # no allowed driver reads it: the policy's refusal only when the bytes are a refused driver's own
        driver = _refused_format(_head(fp, path))
        if driver is None:
            return None
        return DriverPolicyError(f"{_not_a_raster(what, driver)} (GDAL: {text})")
    if _MISSING in text and path is not None and not _exists(path):
        if _names_a_refused_connection(path, at_start=True):
            # GTI:/x, WMS:http://..., EEDAI:...: a refused driver's connection string is no file to GDAL
            return DriverPolicyError(
                f"{what}: names a source GDAL has no driver for here (a refused driver's connection), "
                f"{REFUSED_PHRASE} (GDAL: {text})"
            )
        # a missing file stays the binding's own I/O error (an OSError, which the read retries catch), even when its
        # name ends like a refused driver's prefix (missing.mrf, tiles_wms, a.gti)
        return None
    reason = policy_reason(text, nested=True)
    if reason is None:
        return None
    return DriverPolicyError(f"{what}: reads {reason}, {REFUSED_PHRASE} (GDAL: {text})")


def _requested_drivers(driver, what: str) -> list:
    """The drivers an open may use: the caller's (each must be allowed), else every allowed driver."""
    allowed = allowed_names()
    if driver is None:
        return list(allowed)
    names = [driver] if isinstance(driver, str) else list(driver)
    refused = [n for n in names if n not in allowed]
    if refused or not names:
        named = ", ".join(map(str, refused)) or repr(driver)
        raise DriverPolicyError(
            f"{what}: driver {named} is one GeoBrix does not open, {REFUSED_PHRASE}"
        )
    return names


def _read_with(fp, drivers: list, kwargs: dict):
    """rasterio's read open of ``fp`` with ``drivers`` as GDAL's driver list. ``rasterio.open`` takes one driver name
    only; every read form of it ends in ``DatasetReader`` (directly, or through ``MemoryFile.open``), which takes the
    list, so this is that same open with the list passed through."""
    import rasterio.io as rio_io
    from rasterio.env import ensure_env_with_credentials

    file_types = tuple(
        t for t in (rio_io.MemoryFile, getattr(rio_io, "FilePath", None)) if t
    )
    if isinstance(fp, file_types):
        return fp.open(driver=drivers, **kwargs)
    if hasattr(fp, "read"):
        # a file-like: as rasterio 1.4+'s open, its bytes go in a MemoryFile that the dataset owns (closed with it).
        # rasterio 1.3's open streamed a file-like through FilePath instead; this reads it whole and seeks back, so a
        # non-seekable stream is not supported here (no GeoBrix caller passes one).
        memfile = rio_io.MemoryFile(fp.read())
        fp.seek(0)
        ds = memfile.open(driver=drivers, **kwargs)
        ds._env.enter_context(memfile)
        return ds
    from rasterio._path import _parse_path

    def _reader(name, **kw):
        return rio_io.DatasetReader(_parse_path(name), driver=drivers, **kw)

    return ensure_env_with_credentials(_reader)(os.fspath(fp), **kwargs)


def _open_restricted(fp, mode: str, drivers: list, kwargs: dict):
    """An open of ``fp`` that can use ``drivers`` only (``mode`` is ``r`` or ``r+``)."""
    import rasterio

    if len(drivers) == 1:
        return rasterio.open(fp, mode, driver=drivers[0], **kwargs)
    if mode == "r":
        return _read_with(fp, drivers, kwargs)
    # an update takes one driver: the one a read restricted to the allowed drivers opens it with
    with _read_with(fp, drivers, {}) as probe:
        name = probe.driver
    return rasterio.open(fp, mode, driver=name, **kwargs)


def open_data(fp, mode: str = "r", **kwargs):
    """``rasterio.open`` for GeoBrix, under the process's driver policy. Every read (``r``, ``r+``) opens with the
    allowed drivers as GDAL's driver list: the identified one in full mode, for a named file; the whole allowed list
    otherwise (a remote or ``/vsi`` name, a ``MemoryFile``, a file-like, a partial process). A ``driver=`` the policy
    refuses, and a file no allowed driver reads, raise ``DriverPolicyError``. A write (``w``) is ``rasterio.open``
    after the activation, as is every open with the switch off."""
    import rasterio

    state_now = activate()
    if not mode.startswith("r") or state_now["mode"] == "off":
        return rasterio.open(fp, mode, **kwargs)
    path = os.fspath(fp) if isinstance(fp, (str, os.PathLike)) else None
    what = path or "the raster bytes"
    explicit = kwargs.pop("driver", None)
    drivers = _requested_drivers(explicit, what)
    if explicit is None and path is not None and state_now["mode"] == "full":
        name = _identify(path)
        if name is not None:
            drivers = [name]
        elif _exists(path):
            driver = _refused_format(_head(None, path))
            if driver is not None:
                raise DriverPolicyError(_not_a_raster(what, driver))
            # no allowed driver identifies it, and it is no refused driver's: the open below gives GDAL's own error
    try:
        ds = _open_restricted(fp, mode, drivers, kwargs)
    except DriverPolicyError:
        raise
    except Exception as exc:
        refusal = _refusal(what, path, exc, fp)
        if refusal is not None:
            raise refusal from None
        raise
    if ds.driver not in allowed_names():
        # defence in depth: an open restricted to the allowed drivers reports one of them
        ds.close()
        raise DriverPolicyError(
            f"{what}: opened by driver {ds.driver}, {REFUSED_PHRASE}"
        )
    return ds


def open_bytes(data, **kwargs):
    """A read of raster bytes under the driver policy: ``data`` is bytes (a tile's raster, an intermediate GeoBrix
    wrote) or a ``MemoryFile``. The open uses the allowed drivers as GDAL's driver list and checks the driver it
    reports, as ``open_data`` does. Bytes are held in a ``MemoryFile`` that the returned dataset owns (closed with
    it); a ``MemoryFile`` passed in stays the caller's. Use it as ``with open_bytes(raw) as ds:``.
    """
    import rasterio.io as rio_io

    owned = not isinstance(data, rio_io.MemoryFile)
    memfile = rio_io.MemoryFile(data) if owned else data
    try:
        if activate()["mode"] == "off":
            # GDAL as GDAL has it, through the MemoryFile's own open (rasterio 1.3's open would take the file-like path)
            ds = memfile.open(**kwargs)
        else:
            ds = open_data(memfile, "r", **kwargs)
    except BaseException:
        if owned:
            memfile.close()
        raise
    if owned:
        ds._env.enter_context(memfile)
    return ds


@contextlib.contextmanager
def refusals_named(what: str):
    """Around the reads of a raster GeoBrix opened: a GDAL failure that is the policy's (a nested source it stopped, a
    raw band, a Python pixel function) raises ``DriverPolicyError`` with the reason; any other error passes through.
    """
    try:
        yield
    except DriverPolicyError:
        raise
    except Exception as exc:
        text = chain_text(exc)
        reason = policy_reason(text, nested=True)
        if reason is None:
            raise
        raise DriverPolicyError(
            f"{what}: reads {reason}, {REFUSED_PHRASE} (GDAL: {text})"
        ) from None
