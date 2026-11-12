"""Keeps pycolmap's native (glog) output to warnings and errors unless the caller asks for more.

pycolmap logs every registered image, matcher and SIFT extractor at INFO, hundreds of lines per
run. ``pycolmap.logging.minloglevel`` (pycolmap 4.2.1: an int property on ``pycolmap.logging``,
``Level.INFO=0, WARNING=1, ERROR=2, FATAL=3``) is the supported runtime switch. It is per process,
so it is set where the pycolmap work runs: the driver, the matching worker, and the per-image
extraction child.

Within one process the level is a single flag shared by every thread, and ``dense_mvs_pool`` runs
several patch-match calls on pool threads at once. Scopes therefore overlap: the level is saved by
the first scope in, and put back only when the last one out. (A per-scope save/restore lets the
first call to finish switch INFO back on under the others.)

Set ``GBX_PYCOLMAP_VERBOSE=1`` to leave the native log untouched.
"""

import contextlib
import os
import threading

VERBOSE_ENV = "GBX_PYCOLMAP_VERBOSE"
WARNING = 1  # pycolmap.logging.Level.WARNING

_LOCK = threading.Lock()
_ACTIVE = {}  # id(pycolmap.logging) -> [open scopes, level to restore]


def _env_verbose() -> bool:
    return os.environ.get(VERBOSE_ENV, "").strip().lower() in ("1", "true", "yes", "on")


def quiet_native_log(pycolmap, verbose=False):
    """Limit ``pycolmap``'s native log to warnings and errors; return the level to restore.

    Returns ``None`` (nothing changed, nothing to restore) when ``verbose`` or
    ``GBX_PYCOLMAP_VERBOSE`` asks for the full log, when the level is already stricter, or when
    this pycolmap has no ``logging`` API. Every non-``None`` return must be handed to
    ``restore_native_log``; the level goes back when the last overlapping scope restores.
    """
    if verbose or _env_verbose():
        return None
    log = getattr(pycolmap, "logging", None)
    if log is None:
        return None
    with _LOCK:
        entry = _ACTIVE.get(id(log))
        if entry is None:
            prev = int(log.minloglevel)
            if prev >= WARNING:
                return None
            log.minloglevel = WARNING
            entry = _ACTIVE[id(log)] = [0, prev]
        entry[0] += 1
        return entry[1]


def restore_native_log(pycolmap, prev) -> None:
    """Close a scope opened by ``quiet_native_log`` (a no-op for ``None``)."""
    if prev is None:
        return
    log = pycolmap.logging
    with _LOCK:
        entry = _ACTIVE.get(id(log))
        if entry is None:
            log.minloglevel = prev
            return
        entry[0] -= 1
        if entry[0] <= 0:
            del _ACTIVE[id(log)]
            log.minloglevel = entry[1]


def quiet_native_log_for_process(pycolmap, verbose=False) -> None:
    """Limit the native log for the rest of this process (a worker that never restores it)."""
    if verbose or _env_verbose():
        return
    log = getattr(pycolmap, "logging", None)
    if log is None:
        return
    with _LOCK:
        if int(log.minloglevel) < WARNING:
            log.minloglevel = WARNING


@contextlib.contextmanager
def native_log(pycolmap, verbose=False):
    """Quiet the native log for the block, then restore the caller's level."""
    prev = quiet_native_log(pycolmap, verbose)
    try:
        yield
    finally:
        restore_native_log(pycolmap, prev)
