"""Keeps pycolmap's native (glog) output to warnings and errors unless the caller asks for more.

pycolmap logs every registered image, matcher and SIFT extractor at INFO, hundreds of lines per
run. ``pycolmap.logging.minloglevel`` (pycolmap 4.2.1: an int property on ``pycolmap.logging``,
``Level.INFO=0, WARNING=1, ERROR=2, FATAL=3``) is the supported runtime switch. It is per process,
so it is set where the pycolmap work runs: the driver, the matching worker, and the per-image
extraction child.

Set ``GBX_PYCOLMAP_VERBOSE=1`` to leave the native log untouched.
"""

import contextlib
import os

VERBOSE_ENV = "GBX_PYCOLMAP_VERBOSE"
WARNING = 1  # pycolmap.logging.Level.WARNING


def _env_verbose() -> bool:
    return os.environ.get(VERBOSE_ENV, "").strip().lower() in ("1", "true", "yes", "on")


def quiet_native_log(pycolmap, verbose=False):
    """Limit ``pycolmap``'s native log to warnings and errors; return the previous level.

    Returns ``None`` (nothing changed) when ``verbose`` or ``GBX_PYCOLMAP_VERBOSE`` asks for the
    full log, when the level is already stricter, or when this pycolmap has no ``logging`` API.
    """
    if verbose or _env_verbose():
        return None
    log = getattr(pycolmap, "logging", None)
    if log is None:
        return None
    prev = int(log.minloglevel)
    if prev >= WARNING:
        return None
    log.minloglevel = WARNING
    return prev


def restore_native_log(pycolmap, prev) -> None:
    """Put back the level ``quiet_native_log`` returned (a no-op for ``None``)."""
    if prev is not None:
        pycolmap.logging.minloglevel = prev


@contextlib.contextmanager
def native_log(pycolmap, verbose=False):
    """Quiet the native log for the block, then restore the caller's level."""
    prev = quiet_native_log(pycolmap, verbose)
    try:
        yield
    finally:
        restore_native_log(pycolmap, prev)
