"""The process-level defence against crafted data: the one choke point for GeoBrix's GDAL.

A table or a Volume another principal wrote can name any file, and a VRT, a tile index or a pipeline in it can name
more. GDAL opens them through the process's driver registry, with no driver list of its own. So the defence is set
where GeoBrix first touches GDAL, not per call:

- ``prepare()`` runs when the package is imported (``databricks/labs/gbx/__init__.py``, right after the native payload
  is served). It imports nothing from GDAL (the light tier's lazy-import contract holds). It puts ``GDAL_SKIP`` (the
  refused list, added to a user's value) and the two VRT settings in the process environment. GDAL reads ``GDAL_SKIP``
  when it registers its drivers, so a rasterio or osgeo that registers them after this import (rasterio does at its
  first open or ``rasterio.Env``, osgeo at its import) never registers a refused one. Where rasterio or osgeo is
  loaded already, it runs ``activate()`` at once (the binding is imported, so this costs no lazy import).
- ``activate()`` runs at the first GeoBrix use that needs GDAL (an open made through ``open_data``, the engine's GDAL,
  ``state()``). It is idempotent and thread-safe. Where the payload's osgeo shares rasterio's libgdal ("full"), it
  deregisters any refused driver still registered and sets ``GDAL_VRT_ENABLE_RAWRASTERBAND=NO`` and
  ``GDAL_VRT_ENABLE_PYTHON=NO`` for the process. Without it ("partial": a PyPI rasterio, a dev host), the settings go
  through rasterio's config; a refused driver cannot be deregistered there, so if rasterio registered its drivers
  before GeoBrix was imported, ``activate()`` warns once, names the drivers and the fix.
- ``open_data(fp, mode, **kw)`` is the only way GeoBrix opens a raster by rasterio. Every read is an open with the
  allowed drivers as GDAL's driver list, never with the whole registry. In full mode a named file is first identified
  with the allowed drivers and opened with the one identified, so identify and open cannot disagree. A file no allowed
  driver reads is refused with the policy's reason.

One switch turns the whole defence off, on both tiers: ``GBX_GDAL_DRIVER_POLICY=off`` in the environment, or
``spark.databricks.labs.gbx.gdal.driver_policy=off`` in the session conf (the JVM's ``GDALManager`` reads it).
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
CONF = "spark.databricks.labs.gbx.gdal.driver_policy"
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

# GDAL's message for a file no driver in the open's list reads (GDAL 3.13.3's own text).
_NOT_RECOGNIZED = "not recognized as being in a supported file format"

_LOCK = threading.RLock()
_STATE: Optional[Dict] = None
_PREPARED = False
# The connection prefixes of the refused drivers ("GTI:" ...), compared without case: GDAL names the prefix of a driver
# it does not have as a missing file, so the policy's reason is read from that message. Seeded from the refused names
# at prepare() (a skipped driver is never registered, so its own prefix cannot be read); a deregistered driver's own
# prefix is added.
_DEREGISTERED_PREFIXES: set = set()


def gap_warning(names) -> str:
    """The warning of a partial process where rasterio registered refused drivers before GeoBrix was imported."""
    return (
        "GeoBrix's GDAL driver policy cannot remove these drivers from this process: "
        f"{', '.join(names)}. rasterio registered GDAL's drivers (an open, or a rasterio.Env) before "
        "databricks.labs.gbx was imported, so a VRT or an index another principal wrote can still reach them; "
        "GeoBrix's own opens still use only the allowed drivers. Fix: import databricks.labs.gbx before rasterio "
        "opens anything, or install the GeoBrix env wheel for the runtime (its vendored rasterio is the one the "
        "policy fully covers)."
    )


class DriverPolicyError(ValueError):
    """A raster the process-level driver policy refuses. The message names the file, the refused kind, and the
    policy's reason (``REFUSED_PHRASE``)."""


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
    low = message.lower()
    if nested and any(
        "No such file or directory" in message and p.lower() in low
        for p in _DEREGISTERED_PREFIXES
    ):
        # a deregistered driver's name (GTI:/path) is no file to GDAL: it reports it as missing
        return "a source GDAL has no driver for here"
    for needle, what, needs_nested in _NESTED_REFUSALS:
        if needle in message and (nested or not needs_nested):
            return what
    return None


def prepare() -> None:
    """The registration-time part of the policy, applied at package import (see the module docstring). Idempotent. It
    imports neither rasterio nor osgeo: it sets the environment their driver registration reads.
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
        if any(m in sys.modules for m in ("rasterio", "osgeo")):
            # a binding is loaded already, and may have registered GDAL's drivers: the skip cannot reach those, so
            # the activation (the deregistration, or the partial-mode warning) runs now, not at the first open
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


def _skip_at_registration() -> None:
    """GDAL_SKIP names the refused drivers, added to a user's value (a user's value cannot remove one), and the two VRT
    settings go in the environment, where GDAL's driver registration and its config lookups read them.
    """
    names = {t for t in re.split(r"[,\s]+", os.environ.get("GDAL_SKIP", "")) if t}
    os.environ["GDAL_SKIP"] = ",".join(sorted(names | set(refused_names())))
    os.environ.update(SETTINGS)


def _registered_refused() -> list:
    """The refused drivers rasterio's GDAL has registered: none, unless it registered before ``prepare()`` ran."""
    import rasterio

    with rasterio.Env() as env:
        registered = set(env.drivers())
    return sorted(set(refused_names()) & registered)


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
        return {
            "mode": "full",
            "deregistered": deregistered,
            "settings": sorted(SETTINGS),
        }
    import rasterio.env as rio_env

    for key, value in SETTINGS.items():
        rio_env.set_gdal_config(key, value)
    present = _registered_refused()
    if present:
        warnings.warn(gap_warning(present), RuntimeWarning, stacklevel=2)
    return {
        "mode": "partial",
        "deregistered": [],
        "settings": sorted(SETTINGS),
        "registered_refused": present,
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


def _not_a_raster(what: str) -> str:
    return (
        f"{what}: is not a raster any driver GeoBrix opens reads (a refused driver, or a format GDAL does not read), "
        f"{REFUSED_PHRASE}"
    )


def _refusal(
    what: str, path: Optional[str], exc: BaseException
) -> Optional[DriverPolicyError]:
    """The policy's refusal for an error of an open restricted to the allowed drivers, or None (a missing file, a
    corrupt one). GDAL's "not recognized" about the opened name itself means no allowed driver reads it.
    """
    text = chain_text(exc)
    if _NOT_RECOGNIZED in text and (path is None or path in text):
        return DriverPolicyError(f"{_not_a_raster(what)} (GDAL: {text})")
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
        # a file-like: as rasterio.open, its bytes go in a MemoryFile that the dataset owns (closed with it)
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
    what = path or repr(fp)
    explicit = kwargs.pop("driver", None)
    drivers = _requested_drivers(explicit, what)
    if explicit is None and path is not None and state_now["mode"] == "full":
        name = _identify(path)
        if name is not None:
            drivers = [name]
        elif _exists(path):
            raise DriverPolicyError(_not_a_raster(what))
    try:
        ds = _open_restricted(fp, mode, drivers, kwargs)
    except DriverPolicyError:
        raise
    except Exception as exc:
        refusal = _refusal(what, path, exc)
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
