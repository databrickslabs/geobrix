"""Canonical local-disk temp for the product source tree (tier-neutral, stdlib-only).

One base resolver (`local_temp_root`) + atomic create helpers so every product `src/`
site roots its scratch in the same, relocatable place. Distinct from the Volume-parent
scratch in `ds/_scratch.py` (which stages under a UC Volume). See
`.claude/rules/writer-scaffolding.md` and the local-temp standardization spec.
"""

import atexit
import os
import shutil
import tempfile
import threading
import time
import unicodedata
import uuid
import warnings
from collections import OrderedDict
from contextlib import contextmanager
from typing import Callable, Dict, Hashable, Iterable, List, Optional

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
    A set-but-unusable value warns and falls back (never breaks temp I/O).

    Re-resolved per call (not cached) so a runtime env change takes effect; the
    makedirs(exist_ok) on an existing dir is a cheap stat.
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
    Caller owns cleanup (try/finally: shutil.rmtree; gc_stale_local_temp for orphans).
    """
    return tempfile.mkdtemp(prefix=prefix, dir=local_temp_root())


def new_local_temp_file(suffix: str = "", prefix: str = "gbx_") -> str:
    """Atomically create a unique empty local-disk temp FILE under local_temp_root();
    returns its path (the fd is closed). Caller owns unlink/cleanup."""
    fd, path = tempfile.mkstemp(suffix=suffix, prefix=prefix, dir=local_temp_root())
    os.close(fd)
    return path


def new_staged_local_file(source_name: str) -> str:
    """Atomically create a unique empty local-disk file named ``<uuid hex>_<random><source_name>`` under
    local_temp_root(), the name :func:`copy_source_with_sidecars` stages a file and its sidecars under (the shared
    prefix is how :func:`remove_staged` finds the sidecars). Returns its path; the caller owns removal.
    """
    fd, path = tempfile.mkstemp(
        prefix=f"{uuid.uuid4().hex}_", suffix=source_name, dir=local_temp_root()
    )
    os.close(fd)
    return path


def gc_stale_local_temp(
    prefix: str,
    ttl_seconds: int = DEFAULT_STALE_TTL_SECONDS,
    pid_owned: bool = False,
) -> None:
    """Best-effort GC of stale prefixed local-temp dirs under local_temp_root().
    Age-based; never raises. (Relocated from ds/_scratch.py; now follows the base.)

    With ``pid_owned`` the dirs are named ``<prefix><pid>_...`` (as :class:`StageCache` names its dir): one whose owner
    process is alive is kept however old it is (a folder that is only read for an hour is not stale), and one whose owner
    is gone is removed. A name that carries no pid falls back to the age."""
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
            if not os.path.isdir(p):
                continue
            if pid_owned:
                stale = _stale_by_owner(prefix, name, p, now, ttl_seconds)
            else:
                stale = (now - os.path.getmtime(p)) > ttl_seconds
            if stale:
                shutil.rmtree(p, ignore_errors=True)
        except OSError:
            continue


# ---------------------------------------------------------------------------
# Worker-local staging cache (light readers that need a seekable local copy)
# ---------------------------------------------------------------------------

_STAGE_CAP_ENV = "GBX_STAGE_CACHE_MB"


def _stage_cap_bytes(cache_dir: str, cached_bytes: int) -> int:
    """Byte cap for one process's staging cache: ``GBX_STAGE_CACHE_MB`` if set,
    else half the disk space available to the cache (free + already cached)."""
    raw = os.environ.get(_STAGE_CAP_ENV, "").strip()
    if raw:
        return int(float(raw) * 1024 * 1024)
    return (shutil.disk_usage(cache_dir).free + cached_bytes) // 2


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except OSError:  # e.g. PermissionError: exists, owned by another user
        return True
    return True


def _stale_by_owner(prefix: str, name: str, path: str, now: float, ttl: int) -> bool:
    """Whether the dir *name* (``<prefix><pid>_...``) is an orphan: its owner process is gone. A name with no pid in it
    is judged by its age."""
    owner = name[len(prefix) :].split("_", 1)[0]
    if owner.isdigit():
        return int(owner) != os.getpid() and not _pid_alive(int(owner))
    return (now - os.path.getmtime(path)) > ttl


# A staged copy is named ``<uuid hex>_<source name>``; the sidecars staged with it carry the same prefix.
_STAGE_PREFIX_LEN = 33


def _staged_sidecars(local: str) -> List[str]:
    """The sidecar copies staged beside the staged file *local* (same ``<uuid hex>_`` prefix, other names)."""
    folder, name = os.path.split(local)
    prefix = name[:_STAGE_PREFIX_LEN]
    if len(prefix) < _STAGE_PREFIX_LEN or not prefix.endswith("_"):
        return []
    try:
        return [
            os.path.join(folder, n)
            for n in os.listdir(folder)
            if n.startswith(prefix) and n != name
        ]
    except OSError:
        return []


def _remove_local(local: str, sidecars: Iterable[str] = ()) -> None:
    """Remove a staged copy and the *sidecars* staged with it (a directory is removed whole)."""
    if os.path.isdir(local):
        shutil.rmtree(local, ignore_errors=True)
        return
    for path in [*sidecars, local]:
        try:
            os.remove(path)
        except OSError:
            pass


def remove_staged(local: str) -> None:
    """Remove a staged copy that no cache keeps books for (the Volume fallback copy of
    ``_stage_local_if_needed``) with the sidecars staged beside it, found by their shared prefix.
    """
    _remove_local(local, _staged_sidecars(local))


def _path_bytes(path: str, sidecars: Iterable[str] = ()) -> int:
    if os.path.isdir(path):
        return sum(
            os.path.getsize(os.path.join(d, f))
            for d, _, fs in os.walk(path)
            for f in fs
        )
    return os.path.getsize(path) + sum(
        os.path.getsize(p) for p in sidecars if os.path.isfile(p)
    )


def source_signature(path: str) -> tuple:
    """``(size, mtime_ns)`` of a file, or of a directory's contents (total size,
    newest mtime) -- changes when the source is replaced or rewritten in place."""
    if os.path.isdir(path):
        size, newest = 0, os.stat(path).st_mtime_ns
        for d, _, fs in os.walk(path):
            for f in fs:
                st = os.stat(os.path.join(d, f))
                size += st.st_size
                newest = max(newest, st.st_mtime_ns)
        return (size, newest)
    st = os.stat(path)
    return (st.st_size, st.st_mtime_ns)


def copy_source_sequential(source_path: str, local_path: str) -> None:
    """Copy one file in a single sequential pass (a ``StageCache`` ``copy_fn``).

    FUSE-safe: UC Volumes do not support per-window seeks. UC Volume FUSE can
    transiently raise FileNotFoundError / OSError even when the file exists
    (eventual-consistency lag after a write), so the copy is retried up to 10x.
    """
    # Lazy: the ds package imports this module.
    from databricks.labs.gbx.ds import _listing

    def _do_stage() -> None:
        with open(source_path, "rb") as src, open(local_path, "wb") as dst:
            shutil.copyfileobj(src, dst, length=8 * 1024 * 1024)

    _listing._retry_transient(_do_stage)


def copy_source_with_sidecars(source_path: str, local_path: str) -> List[str]:
    """:func:`copy_source_sequential` of the file, then of the sidecar files beside it (a ``StageCache`` ``copy_fn``
    for a raster, whose staged copy has to open as the source does). Returns the staged sidecar paths, which the
    cache keeps with the entry (to size and to remove it) instead of listing the folder.
    """
    try:
        copy_source_sequential(source_path, local_path)
        return _copy_sidecars(source_path, local_path)
    except BaseException:
        # a copy that failed is not in the cache's books: take what it staged (the file and the sidecars already copied)
        remove_staged(local_path)
        raise


def _copy_sidecars(source_path: str, local_path: str) -> List[str]:
    """Copy the sidecar files beside *source_path* (``pyrx._env.copied_sidecar_files``) next to the staged *local_path*,
    named as the staged file is: the staged name is a prefix plus the source's name, and a sidecar keeps the rest of its
    own name, so GDAL finds it by the same rule as at the source. A staged copy that lost them would open without the
    CRS or NoData they hold, or not at all (an ESRI ``.bil`` without its ``.hdr``). Returns the paths copied.
    """
    from databricks.labs.gbx.ds import _listing
    from databricks.labs.gbx.pyrx import _env

    src_name, local_name = os.path.basename(source_path), os.path.basename(local_path)
    if not local_name.endswith(src_name):
        return []
    prefix = local_name[: len(local_name) - len(src_name)]
    folder = os.path.dirname(local_path)
    copied: List[str] = []
    for side in _env.copied_sidecar_files(source_path):
        dest = os.path.join(folder, prefix + os.path.basename(side))

        def _do_copy(side=side, dest=dest) -> None:
            with open(side, "rb") as src, open(dest, "wb") as dst:
                shutil.copyfileobj(src, dst, length=8 * 1024 * 1024)

        _listing._retry_transient(_do_copy)
        copied.append(dest)
    return copied


def raster_source_signature(path: str) -> tuple:
    """:func:`source_signature` of a raster file together with the identity (name, size, mtime) of each sidecar a staged
    copy carries (``_env.copied_sidecar_files``): an ``.aux.xml`` or ``.hdr`` edited beside an unchanged file changes
    the CRS or NoData GDAL reads from the copy, so the copy has to be staged again."""
    from databricks.labs.gbx.pyrx import _env

    sides = []
    for side in _env.copied_sidecar_files(path):
        st = os.stat(side)
        sides.append((os.path.basename(side), st.st_size, st.st_mtime_ns))
    return (source_signature(path), tuple(sides))


def retried_raster_source_signature(path: str) -> tuple:
    """:func:`raster_source_signature` with the transient retry of :func:`retried_source_signature` (the
    ``StageCache`` ``signature_fn`` of the raster reader)."""
    from databricks.labs.gbx.ds import _listing

    return _listing._retry_transient(lambda: raster_source_signature(path))


def retried_source_signature(path: str) -> tuple:
    """:func:`source_signature` with the same transient retry as
    :func:`copy_source_sequential` (a ``StageCache`` ``signature_fn``), so a source
    replaced at the same path is staged again."""
    from databricks.labs.gbx.ds import _listing

    return _listing._retry_transient(lambda: source_signature(path))


class StageCache:
    """Process-wide cache of worker-local copies of source files.

    Readers whose formats need seeked I/O copy each source to local disk once
    (``copy_fn(src, dst)``, a sequential FUSE-safe copy) and reuse it for every
    task on the same worker process. Two guards keep the copies from filling the
    disk:

    - The cache directory is named ``<prefix><pid>_...``. PySpark ends its worker
      processes with ``os._exit``, so the ``atexit`` cleanup does not run; the first
      use in each process therefore removes ``<prefix>`` directories whose owner
      process no longer exists (and unowned ones older than
      ``DEFAULT_STALE_TTL_SECONDS``).
    - Cached bytes are capped (``_stage_cap_bytes``): after a new copy is made,
      the least recently used copies not currently in use are evicted until the
      cache fits. A copy in use is never evicted; a source larger than the cap is
      still staged, alone.

    With a ``signature_fn`` (e.g. :func:`retried_source_signature`), each use
    compares the source's signature with the one taken when it was staged; a source
    replaced at the same path is staged again, and the superseded copy is removed once
    no reader is using it.
    """

    def __init__(
        self,
        prefix: str,
        copy_fn: Callable[[str, str], Optional[List[str]]],
        signature_fn: Optional[Callable[[str], Hashable]] = None,
    ):
        self._prefix = prefix
        self._copy = copy_fn
        self._signature = signature_fn
        self._lock = threading.Lock()
        self._dir: Optional[str] = None
        self._entries: "OrderedDict[str, str]" = (
            OrderedDict()
        )  # source -> local (LRU order)
        self._sizes: Dict[str, int] = {}
        self._sigs: Dict[str, Hashable] = {}
        # local -> the sidecar copies staged with it (what a ``copy_fn`` returned), so sizing and removing a copy
        # needs no listing of the folder under the lock
        self._sidecars: Dict[str, List[str]] = {}
        self._in_use: Dict[str, int] = {}  # local -> active users
        self._retired: List[str] = []  # superseded copies still in use

    @contextmanager
    def use(self, source_path: str):
        """Yield a local copy of ``source_path``; it is not evicted while in use."""
        sig = self._signature(source_path) if self._signature else None
        with self._lock:
            local = self._get_or_stage(source_path, sig)
            self._in_use[local] = self._in_use.get(local, 0) + 1
        try:
            yield local
        finally:
            with self._lock:
                self._in_use[local] -= 1
                if not self._in_use[local]:
                    del self._in_use[local]
                    if local in self._retired:
                        self._retired.remove(local)
                        self._remove(local)

    def _get_or_stage(self, source_path: str, sig: Hashable = None) -> str:
        if source_path in self._entries:
            if self._sigs.get(source_path) == sig:
                self._entries.move_to_end(source_path)
                return self._entries[source_path]
            self._supersede(source_path)
        cache_dir = self._ensure_dir()
        basename = os.path.basename(source_path.rstrip("/")) or "source"
        # uuid keeps two sources that share a basename apart on disk.
        local = os.path.join(cache_dir, f"{uuid.uuid4().hex}_{basename}")
        # Only copy_fn touches the source (readers wrap it in their transient
        # retry); the size comes from the local copy, then idle copies are evicted.
        sidecars = list(self._copy(source_path, local) or ())
        self._entries[source_path] = local
        self._sidecars[local] = sidecars
        self._sizes[source_path] = _path_bytes(local, sidecars)
        self._sigs[source_path] = sig
        self._evict_to_cap(cache_dir, keep=source_path)
        return local

    def _evict_to_cap(self, cache_dir: str, keep: str) -> None:
        cached = sum(self._sizes.values())
        cap = _stage_cap_bytes(cache_dir, cached)
        for src in list(self._entries):  # least recently used first
            if cached <= cap:
                return
            if src == keep or self._in_use.get(self._entries[src], 0):
                continue
            local = self._entries.pop(src)
            cached -= self._sizes.pop(src)
            self._sigs.pop(src, None)
            self._remove(local)

    def _supersede(self, source_path: str) -> None:
        """Drop the cached copy of a source that changed; defer the delete while
        a reader is still using it."""
        local = self._entries.pop(source_path)
        self._sizes.pop(source_path, None)
        self._sigs.pop(source_path, None)
        if self._in_use.get(local, 0):
            self._retired.append(local)
        else:
            self._remove(local)

    def _remove(self, local: str) -> None:
        _remove_local(local, self._sidecars.pop(local, ()))

    def _ensure_dir(self) -> str:
        if self._dir is None:
            self._sweep_orphans()
            self._dir = new_local_temp_dir(f"{self._prefix}{os.getpid()}_")
            atexit.register(self._cleanup)
        return self._dir

    def _sweep_orphans(self) -> None:
        root = local_temp_root()
        try:
            names = os.listdir(root)
        except OSError:
            return
        now = time.time()
        for name in names:
            if not name.startswith(self._prefix):
                continue
            path = os.path.join(root, name)
            try:
                stale = _stale_by_owner(
                    self._prefix, name, path, now, DEFAULT_STALE_TTL_SECONDS
                )
                if stale and os.path.isdir(path):
                    shutil.rmtree(path, ignore_errors=True)
            except OSError:
                continue

    def _cleanup(self) -> None:
        if self._dir and os.path.isdir(self._dir):
            shutil.rmtree(self._dir, ignore_errors=True)
        self._dir = None
        self._entries.clear()
        self._sizes.clear()
        self._sigs.clear()
        self._sidecars.clear()
        self._in_use.clear()
        self._retired.clear()
