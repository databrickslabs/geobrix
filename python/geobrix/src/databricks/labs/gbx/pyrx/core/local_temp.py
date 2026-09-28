"""Canonical local-disk temp for the product source tree (tier-neutral, stdlib-only).

One base resolver (`local_temp_root`) + atomic create helpers so every product `src/`
site roots its scratch in the same, relocatable place. Distinct from the Volume-parent
scratch in `ds/_scratch.py` (which stages under a UC Volume). See
`.claude/rules/writer-scaffolding.md` and the local-temp standardization spec.
"""

import os
import shutil
import tempfile
import time
import unicodedata
import warnings

#: A write that has not committed within this window is presumed dead; its temp is
#: reclaimable. The floor is the longest *static* phase of a live write -- the
#: driver-merge step, during which no new fragments are added so the dir's mtime stops
#: advancing (file_gdb 782k merges for ~8 min). One hour leaves a wide margin above that
#: yet reclaims hard-killed orphans promptly. Do not drop below ~30 min. (Relocated
#: verbatim from ds/_scratch.py, which now imports it from here.)
DEFAULT_STALE_TTL_SECONDS = 60 * 60

_ENV = "GBX_LOCAL_TEMP_DIR"


def _sanitize(raw: str) -> str:
    # Strip surrounding whitespace and any BOM / zero-width / invisible unicode.
    s = raw.strip().strip("﻿​‌‍⁠")
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Cf").strip()
    return s


def local_temp_root() -> str:
    """Resolve the base dir for geobrix local-disk temp.

    `GBX_LOCAL_TEMP_DIR` (sanitized) if set and usable, else `tempfile.gettempdir()`.
    A set-but-unusable value warns once and falls back (never breaks temp I/O).
    """
    raw = os.environ.get(_ENV)
    if not raw:
        return tempfile.gettempdir()
    root = _sanitize(raw)
    if not root:
        return tempfile.gettempdir()
    try:
        os.makedirs(root, exist_ok=True)
        if not os.path.isdir(root):
            raise OSError(f"{root} is not a directory")
        return root
    except OSError as e:
        warnings.warn(
            f"{_ENV}={root!r} is unusable ({e}); falling back to {tempfile.gettempdir()}",
            RuntimeWarning,
            stacklevel=2,
        )
        return tempfile.gettempdir()


def new_local_temp_dir(prefix: str) -> str:
    """Atomically create and return a unique local-disk temp DIR under local_temp_root().
    Caller owns cleanup (try/finally: shutil.rmtree; gc_stale_local_temp for orphans)."""
    return tempfile.mkdtemp(prefix=prefix, dir=local_temp_root())


def new_local_temp_file(suffix: str = "", prefix: str = "gbx_") -> str:
    """Atomically create a unique empty local-disk temp FILE under local_temp_root();
    returns its path (the fd is closed). Caller owns unlink/cleanup."""
    fd, path = tempfile.mkstemp(suffix=suffix, prefix=prefix, dir=local_temp_root())
    os.close(fd)
    return path


def gc_stale_local_temp(prefix: str, ttl_seconds: int = DEFAULT_STALE_TTL_SECONDS) -> None:
    """Best-effort GC of stale prefixed local-temp dirs under local_temp_root().
    Age-based; never raises. (Relocated from ds/_scratch.py; now follows the base.)"""
    root = local_temp_root()
    now = time.time()
    try:
        names = os.listdir(root)
    except OSError:
        return
    for name in names:
        if not name.startswith(prefix):
            continue
        p = os.path.join(root, name)
        try:
            if os.path.isdir(p) and (now - os.path.getmtime(p)) > ttl_seconds:
                shutil.rmtree(p, ignore_errors=True)
        except OSError:
            continue
