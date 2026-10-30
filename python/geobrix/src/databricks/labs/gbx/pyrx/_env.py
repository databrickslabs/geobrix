"""GDAL/PROJ environment configuration for the bundled rasterio wheel.

rasterio's manylinux/macOS wheels ship their own GDAL + PROJ data. We point
GDAL_DATA / PROJ_DATA at those bundled paths *only if unset*, so pyrx does not
collide with any cluster-level GDAL installed by the heavyweight init script.
Registered PROJ grid dirs are merged with the PROJ data into one directory, which
becomes PROJ_DATA (rasterio reads PROJ_DATA as a single path; see _proj_data_dir).
"""

import hashlib
import os
import shutil
import stat
import threading
import time
import warnings
from typing import Dict, List, Optional, Sequence, Tuple


def _bundled_gdal_data() -> Optional[str]:
    """Return rasterio's bundled GDAL data dir, else the env wheel's payload share (``_native/share/gdal``), or None.

    Use GDALDataFinder().search() (a deterministic filesystem search) rather than
    get_gdal_data(), which reflects GDAL's *runtime* config state — that returns
    None once another component has initialized/torn down GDAL in the same
    process, so it is unreliable when many rasters are opened across a session.
    This mirrors the PROJDataFinder().search() approach used for PROJ.
    """
    try:
        from rasterio._env import GDALDataFinder

        path = GDALDataFinder().search()
        if path and os.path.isdir(path):
            return path
    except Exception:
        pass
    # The env wheel's rasterio ships no GDAL data: its GDAL reads the payload's share (_native/share/gdal), the same
    # directory the activation points GDAL_DATA at.
    from databricks.labs.gbx import _native

    native = _native.native_dir()
    share = os.path.join(os.path.dirname(native), "share", "gdal") if native else None
    return share if share and os.path.isdir(share) else None


def _bundled_proj_data() -> Optional[str]:
    """Return rasterio's bundled PROJ data dir, or None.

    rasterio._env.PROJDataFinder().search() returns a single directory string.
    Verify that proj.db exists in the directory to avoid setting PROJ_DATA to an
    invalid path that would shadow PROJ's default search paths.
    """
    try:
        from rasterio._env import PROJDataFinder

        path = PROJDataFinder().search()
        if (
            path
            and os.path.isdir(path)
            and os.path.exists(os.path.join(path, "proj.db"))
        ):
            return path
        return None
    except Exception:
        return None


def _search_order(base: str, extra: Sequence[str]) -> List[str]:
    """`extra` dirs ahead of `base` (either may be os.pathsep-joined), de-duped, in order."""
    parts: List[str] = []
    for d in list(extra) + ([base] if base else []):
        for seg in d.split(os.pathsep) if d else []:
            if seg and seg not in parts:
                parts.append(seg)
    return parts


# One PROJ data directory per (search order) for this process, and the base each was
# built from, so a later call that finds its own merged dir in PROJ_DATA unwraps it
# instead of nesting it.
_PROJ_DATA_DIRS: Dict[Tuple[str, ...], str] = {}
_PROJ_DATA_BASES: Dict[str, str] = {}
# A directory that could not be listed leaves its files out of the merged directory. That
# build is reused only until the time here (then rebuilt, which lists again): per-row
# callers must not repeat the listing retries, and a dir that was unreachable once (FUSE)
# must not stay missing for the life of the process.
_PROJ_DATA_RETRY_AT: Dict[Tuple[str, ...], float] = {}
_UNLISTABLE_RETRY_S = 60.0
_WARNED_UNLISTABLE: set = set()  # dirs already named in a warning (once per process)
# Keys whose directory is being (re)built right now; the others keep using the build
# they have instead of waiting for it. Guarded by the lock below, which is held only to
# read and publish: listing a dir can sit in retries for ~20 s.
_PROJ_DATA_BUILDING: set = set()
_PROJ_DATA_LOCK = threading.Lock()
_MERGED_PREFIX = "gbx_proj_data_"


def _now() -> float:
    return time.monotonic()


def _usable_merged_dir(path: str, uid: Optional[int]) -> bool:
    """A real directory (not a symlink) owned by this user: another user's pre-made dir
    of the same name is never trusted."""
    try:
        st = os.lstat(path)
    except OSError:
        return False
    return stat.S_ISDIR(st.st_mode) and (uid is None or st.st_uid == uid)


def _list_dir(d: str) -> Optional[List[str]]:
    """Sorted entry names of `d`; None if it still cannot be listed after the transient
    retries every FUSE read gets (a UC Volume can miss for a moment even when healthy).

    A dir that stays unlistable is named in one warning per process: its files are then
    missing from PROJ_DATA, which silently costs accuracy (a grid is not applied). Such a
    dir is tried once on later rebuilds (a quick miss), not retried for ~20 s again.
    """
    from databricks.labs.gbx.ds import _listing

    once = {"attempts": 1} if d in _WARNED_UNLISTABLE else {}
    try:
        return sorted(_listing._retry_transient(lambda: os.listdir(d), **once))
    except OSError as e:
        if d not in _WARNED_UNLISTABLE:
            _WARNED_UNLISTABLE.add(d)
            warnings.warn(
                f"register_proj_grids: could not list the PROJ data directory {d!r} "
                f"({e}); its files are left out of PROJ_DATA, so grids in it are not "
                f"applied by the raster functions. Listing it again in "
                f"{_UNLISTABLE_RETRY_S:g} s.",
                RuntimeWarning,
                stacklevel=5,
            )
        return None


def _merged_entries(search: Sequence[str]) -> Tuple[Dict[str, str], List[str]]:
    """`name -> target` for every entry of `search`'s dirs (first dir to hold a name wins
    it, which is PROJ's own order for a multi-directory search path), and the dirs that
    could not be listed."""
    entries: Dict[str, str] = {}
    unlisted: List[str] = []
    for d in search:
        names = _list_dir(d)
        if names is None:
            unlisted.append(d)
            continue
        for name in names:
            entries.setdefault(name, os.path.abspath(os.path.join(d, name)))
    return entries, unlisted


def _build_merged_proj_dir(search: Sequence[str], entries: Dict[str, str]) -> str:
    """Create (or reuse) ONE directory of symlinks to `entries` (see _merged_entries).

    The directory name hashes the search order and the entries, so it is shared by every
    process of the same user on the node and is rebuilt when a grid dir gains files. It
    is built under a temp name and renamed into place, so readers never see it
    half-populated. Raises OSError if it cannot be built (e.g. no symlink support).
    """
    from databricks.labs.gbx.pyrx.core.local_temp import (
        local_temp_root,
        new_local_temp_dir,
    )

    uid = os.getuid() if hasattr(os, "getuid") else None
    key = "\0".join(list(search) + [f"{n}={t}" for n, t in sorted(entries.items())])
    digest = hashlib.sha256(key.encode("utf-8", "surrogateescape")).hexdigest()[:16]
    final = os.path.join(local_temp_root(), f"{_MERGED_PREFIX}{uid or 0}_{digest}")
    if _usable_merged_dir(final, uid):
        return final

    tmp = new_local_temp_dir(_MERGED_PREFIX + "build_")
    try:
        for name, target in entries.items():
            os.symlink(target, os.path.join(tmp, name))
        try:
            os.rename(tmp, final)
            return final
        except OSError:
            # Another process won the rename (reuse its dir), or the name is taken by a
            # directory this user does not own (keep the private one just built).
            if _usable_merged_dir(final, uid):
                shutil.rmtree(tmp, ignore_errors=True)
                return final
            return tmp
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise


def _proj_data_dir(base: str, extra_proj_dirs: Sequence[str]) -> Optional[str]:
    """The single directory to export as PROJ_DATA: `extra_proj_dirs` ahead of `base`.

    rasterio hands PROJ_DATA to GDAL as ONE search path (no os.pathsep split) every time
    a rasterio Env starts, so an os.pathsep-joined value is a directory that does not
    exist: proj.db is lost for every thread that opens it afterwards ("Cannot find
    proj.db") and the registered grids are not found either. A merged directory of
    symlinks gives every reader of PROJ_DATA (rasterio, pyogrio, PROJ) one real path.

    Returns None when `base` is unknown: exporting the grid dirs alone would hide the
    proj.db PROJ finds on its own. Falls back to `base` if the merged dir cannot be built.
    """
    if not base:
        warnings.warn(
            "register_proj_grids: the PROJ data directory is unknown (PROJ_DATA/PROJ_LIB "
            "unset and no bundled rasterio PROJ data found), so the registered grid "
            "directories are not added to PROJ_DATA for the raster functions.",
            RuntimeWarning,
            stacklevel=3,
        )
        return None
    search = _search_order(base, extra_proj_dirs)
    if len(search) == 1:
        return search[0]
    key = tuple(search)
    with _PROJ_DATA_LOCK:
        hit = _PROJ_DATA_DIRS.get(key)
        retry_at = _PROJ_DATA_RETRY_AT.get(key)
        usable = bool(hit) and (hit == base or os.path.isdir(hit))
        # Within the retry window, or while another caller relists, keep the build we
        # have: a degraded one still holds proj.db and every dir that did list.
        if usable and (
            retry_at is None or _now() < retry_at or key in _PROJ_DATA_BUILDING
        ):
            return hit
        _PROJ_DATA_BUILDING.add(key)
    try:
        # Outside the lock: the listing retries sleep for ~20 s on a dir that is down.
        entries, unlisted = _merged_entries(search)
        try:
            path = _build_merged_proj_dir(search, entries)
        except OSError as e:
            warnings.warn(
                f"register_proj_grids: could not build a merged PROJ data directory "
                f"({e}); the raster functions will not see the registered grid "
                f"directories {list(extra_proj_dirs)}.",
                RuntimeWarning,
                stacklevel=3,
            )
            path = base  # cached too: per-row callers must not retry the build
    except BaseException:
        with _PROJ_DATA_LOCK:
            _PROJ_DATA_BUILDING.discard(key)
        raise
    with _PROJ_DATA_LOCK:
        _PROJ_DATA_BUILDING.discard(key)
        _PROJ_DATA_DIRS[key] = path
        if unlisted:
            _PROJ_DATA_RETRY_AT[key] = _now() + _UNLISTABLE_RETRY_S
        else:
            _PROJ_DATA_RETRY_AT.pop(key, None)
        if path != base:
            _PROJ_DATA_BASES[path] = base
    return path


def _set_osr_search_paths(dirs: Sequence[str]) -> None:
    """Set GDAL's PROJ search path on the process's default PROJ context (osr.SetPROJSearchPaths), so a grid directory
    registered after the context was built reaches the raster transforms. Only when osgeo is rasterio's GDAL: another
    GDAL's context is not this process's."""
    try:
        from databricks.labs.gbx import _osgeo

        if not _osgeo._same_libgdal_report().get("shared"):
            return
        from osgeo import osr
    except (
        Exception
    ):  # noqa: BLE001 - no payload osgeo: the environment variable is all there is
        return
    osr.SetPROJSearchPaths(list(dirs))


def _register_pyproj_search_dirs(extra_proj_dirs: Sequence[str]) -> None:
    """Add custom grid dirs to pyproj's PROJ search path (best-effort, idempotent).

    pyproj resolves and caches its PROJ data dir at import and installs it on the proj
    context explicitly, so a ``PROJ_DATA`` env var changed *afterwards* is ignored for
    grids loaded through a pyproj ``Transformer``. Registering the dirs via
    ``pyproj.datadir`` is what makes an in-process transform find a user grid. (The
    GDAL/rasterio side has the same limit: a PROJ context that already exists keeps the
    search path it was built with, so ``configure_gdal_env`` also sets it with
    ``osr.SetPROJSearchPaths``.)

    Dirs are APPENDED (never prepended): pyproj's own data dir stays first so proj.db
    resolution is untouched, and PROJ searches grid files across all search paths
    regardless of order (grid filenames are unique, so there is no override contest).
    Only fires when a dir is genuinely new, so repeated per-row calls are cheap.
    """
    try:
        from pyproj.datadir import get_data_dir, set_data_dir
    except Exception:
        return
    try:
        current = get_data_dir() or ""
    except Exception:
        return
    parts = [p for p in current.split(os.pathsep) if p]
    added = [d for d in extra_proj_dirs if d and d not in parts]
    if added:
        set_data_dir(os.pathsep.join(parts + added))


def configure_gdal_env(extra_proj_dirs: Optional[Sequence[str]] = None) -> None:
    """Idempotently set GDAL_DATA / PROJ_DATA, adding custom grid dirs.

    extra_proj_dirs:
      - None  -> read the driver-side registry (correct only on the driver).
      - list  -> use exactly these (what worker UDF closures pass); [] = none.
    Custom dirs are merged ahead of the base PROJ data (so user grids win) into ONE
    directory exported as PROJ_DATA; see _proj_data_dir for why it is not a joined list.
    Registered dirs are also pushed into pyproj's search path so the light vector
    (pyproj) transform path finds them — the env var alone does not reach pyproj.
    """
    if not os.environ.get("GDAL_DATA"):
        p = _bundled_gdal_data()
        if p:
            os.environ["GDAL_DATA"] = p

    if extra_proj_dirs is None:
        from databricks.labs.gbx.core import proj_grids

        extra_proj_dirs = proj_grids.get_registered_dirs()

    current = os.environ.get("PROJ_DATA") or os.environ.get("PROJ_LIB") or ""
    base = _PROJ_DATA_BASES.get(current, current)  # unwrap a merged dir set earlier
    if not base:
        p = _bundled_proj_data()
        if p:
            base = p

    if extra_proj_dirs:
        merged = _proj_data_dir(base, extra_proj_dirs)
        if merged:
            os.environ["PROJ_DATA"] = merged
            _set_osr_search_paths([merged])
        _register_pyproj_search_dirs(extra_proj_dirs)
    elif base and not os.environ.get("PROJ_DATA") and not os.environ.get("PROJ_LIB"):
        os.environ["PROJ_DATA"] = base


def assert_rasterio_available() -> Tuple[str, str]:
    """Return (gdal_version, rasterio_version); raise a clear error if missing."""
    try:
        import rasterio
    except ImportError as e:  # pragma: no cover - exercised only without the extra
        raise ImportError(
            "pyrx requires rasterio. Install the env wheel for the runtime (on Databricks) or "
            "pip install geobrix (off Databricks)"
        ) from e
    return rasterio.__gdal_version__, rasterio.__version__
