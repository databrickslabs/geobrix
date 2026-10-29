"""The process-level defence against crafted data (AP-43): the one choke point for GeoBrix's GDAL.

A table or a Volume another principal wrote can name any file, and a VRT, a tile index or a pipeline in it can name
more. GDAL opens them through the process's driver registry, with no driver list of its own. So the defence is set
where GeoBrix first touches GDAL, not per call:

- ``activate()`` runs when the package is imported (``databricks/labs/gbx/__init__.py``, right after the native
  payload is served), and again at every open made through ``open_data`` and at the engine's GDAL. It is idempotent
  and thread-safe. Where the payload's osgeo shares rasterio's libgdal ("full"), it deregisters every refused
  driver (``manifest.gdal.raster_drivers.refused``) and sets ``GDAL_VRT_ENABLE_RAWRASTERBAND=NO`` and
  ``GDAL_VRT_ENABLE_PYTHON=NO`` for the process. Without it ("partial": rasterio alone, a dev host), the settings go
  through rasterio's config, and the drivers stay registered: a refused driver is then not stopped at the open.
- ``open_data(fp, mode, **kw)`` is the only way GeoBrix opens a raster by rasterio. In read mode it identifies the
  file with the allowed drivers and opens it with the one driver identified, so identify and open cannot disagree.
  A file no allowed driver reads is refused with the policy's reason.

One switch turns the whole defence off, on both tiers: ``GBX_GDAL_DRIVER_POLICY=off`` in the environment, or
``spark.databricks.labs.gbx.gdal.driver_policy=off`` in the session conf (the JVM's ``GDALManager`` reads it).
"""

from __future__ import annotations

import contextlib
import functools
import os
import threading
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

_LOCK = threading.RLock()
_STATE: Optional[Dict] = None
_DEREGISTERED_PREFIXES: set = set()


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
    if nested and any(
        "No such file or directory" in message and p in message
        for p in _DEREGISTERED_PREFIXES
    ):
        # a deregistered driver's name (GTI:/path) is no file to GDAL: it reports it as missing
        return "a source GDAL has no driver for here"
    for needle, what, needs_nested in _NESTED_REFUSALS:
        if needle in message and (nested or not needs_nested):
            return what
    return None


def activate() -> Dict:
    """The process's defence, applied once (see the module docstring). Returns the state: ``mode`` (off, full,
    partial or none), ``deregistered`` (the names removed) and ``settings`` (the process settings set).
    """
    global _STATE
    with _LOCK:
        if _STATE is None:
            _STATE = _apply()
        return _STATE


def state() -> Dict:
    """The state ``activate()`` settled on (activating it first if needed)."""
    return activate()


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
    return {
        "mode": "partial",
        "deregistered": [],
        "settings": sorted(SETTINGS),
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


def _refusal(what: str, exc: BaseException) -> Optional[DriverPolicyError]:
    text = chain_text(exc)
    reason = policy_reason(text, nested=False)
    if reason is None:
        return None
    return DriverPolicyError(f"{what}: reads {reason}, {REFUSED_PHRASE} (GDAL: {text})")


def open_data(fp, mode: str = "r", **kwargs):
    """``rasterio.open`` for GeoBrix, under the process's driver policy. In read mode the file is identified with
    the allowed drivers and opened with the one identified (so an open cannot pick a driver the identify refused); a
    file no allowed driver reads, and a file the policy's settings refuse, raise ``DriverPolicyError``. In write mode
    the call is ``rasterio.open`` after the activation."""
    import rasterio

    state_now = activate()
    path = os.fspath(fp) if isinstance(fp, (str, os.PathLike)) else None
    if mode.startswith("r") and path is not None and state_now["mode"] == "full":
        driver = kwargs.get("driver")
        if driver is None:
            name = _identify(path)
            if name is None and _exists(path):
                raise DriverPolicyError(
                    f"{path}: is not a raster any driver GeoBrix opens reads (a refused driver, or a format GDAL "
                    f"does not read), {REFUSED_PHRASE}"
                )
            if name is not None:
                kwargs["driver"] = name
        elif driver not in allowed_names():
            raise DriverPolicyError(
                f"{path}: driver {driver} is one GeoBrix does not open, {REFUSED_PHRASE}"
            )
    try:
        return rasterio.open(fp, mode, **kwargs)
    except DriverPolicyError:
        raise
    except Exception as exc:
        refusal = _refusal(path or repr(fp), exc)
        if refusal is not None:
            raise refusal from None
        raise


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
