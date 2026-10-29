"""GDAL/PROJ environment configuration for the bundled rasterio wheel.

rasterio's manylinux/macOS wheels ship their own GDAL + PROJ data. We point
GDAL_DATA / PROJ_DATA at those bundled paths *only if unset*, so pyrx does not
collide with any cluster-level GDAL installed by the heavyweight init script, and
PROJ_DATA also when it names no proj.db this process can read (see configure_gdal_env).
Registered PROJ grid dirs are merged with the PROJ data into one directory, which
becomes PROJ_DATA (rasterio reads PROJ_DATA as a single path; see _proj_data_dir).
"""

import hashlib
import logging
import os
import shutil
import stat
import threading
import time
import warnings
from contextlib import contextmanager
from typing import Dict, List, Optional, Sequence, Tuple

_log = logging.getLogger(__name__)


def _bundled_gdal_data() -> Optional[str]:
    """Return rasterio's bundled GDAL data dir, or None.

    Use GDALDataFinder().search() (a deterministic filesystem search) rather than
    get_gdal_data(), which reflects GDAL's *runtime* config state — that returns
    None once another component has initialized/torn down GDAL in the same
    process, so it is unreliable when many rasters are opened across a session.
    This mirrors the PROJDataFinder().search() approach used for PROJ.
    """
    try:
        from rasterio._env import GDALDataFinder

        path = GDALDataFinder().search()
        return path if path and os.path.isdir(path) else None
    except Exception:
        return None


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


def _readable_proj_data(value: str) -> bool:
    """True when a directory of ``value`` (``os.pathsep``-joined) holds a ``proj.db`` this
    process can read."""
    for d in (value or "").split(os.pathsep):
        db = os.path.join(d, "proj.db")
        if d and os.path.isfile(db) and os.access(db, os.R_OK):
            return True
    return False


def _set_rasterio_proj_search_path(path: str) -> None:
    """Point rasterio's GDAL at ``path`` now: rasterio applies ``PROJ_DATA`` as GDAL's PROJ
    search path when it is imported (and again when its first ``rasterio.Env`` of the
    process starts), so a value replaced after the import still holds for a CRS built
    outside an Env."""
    try:
        from rasterio._env import set_proj_data_search_path
    except Exception:
        return
    set_proj_data_search_path(path)


# The PROJ search path configure_gdal_env last gave rasterio's GDAL (None: never).
_RASTERIO_PROJ_PATH: Optional[str] = None


def _point_rasterio_at(path: str) -> None:
    """``_set_rasterio_proj_search_path(path)`` once per new value: every PROJ_DATA that
    configure_gdal_env exports (the bundled data in place of an unreadable dir, a merged
    dir of registered grids) must reach rasterio's search path too, or a transform built
    without a new rasterio.Env misses the registered grids and PROJ takes a lower-accuracy
    operation, or none. Not repeated for the same value (configure_gdal_env runs per row).
    """
    global _RASTERIO_PROJ_PATH
    if path and path != _RASTERIO_PROJ_PATH:
        _set_rasterio_proj_search_path(path)
        _RASTERIO_PROJ_PATH = path


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


def _register_pyproj_search_dirs(extra_proj_dirs: Sequence[str]) -> None:
    """Add custom grid dirs to pyproj's PROJ search path (best-effort, idempotent).

    Setting ``PROJ_DATA`` in the environment is enough for the GDAL/rasterio raster
    path, but NOT for pyproj — the light vector transform engine. pyproj resolves and
    caches its PROJ data dir at import and installs it on the proj context explicitly,
    so a ``PROJ_DATA`` env var changed *afterwards* is ignored for grids loaded through
    a pyproj ``Transformer``. Registering the dirs via ``pyproj.datadir`` is what makes
    an in-process transform actually find a user grid.

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


# To find the sidecar files of a raster it opens (external overviews .ovr, masks .msk,
# .aux.xml, world files), GDAL lists the file's folder by default. Over a Volume's FUSE
# mount that listing is slow and grows with the folder: 0.3 s to 10 s for one open in a
# folder of 745 GeoTIFFs on a Serverless worker. configure_gdal_env sets the option to
# TRUE for the process: GDAL then reads no listing and checks each sidecar by name
# (GDALOpenInfo leaves the sibling list NULL; CPLCheckForFile, GDALReadWorldFile2 and the
# .aux.xml lookup stat each candidate, lower- and upper-case extension), so sidecars are
# still found, at 5 to 7 name checks an open, which on Serverless FUSE cost about what the
# listing did. open_raster goes one step further for the light tier's own opens of local
# and Volume files: it lists each folder once per process (_FolderListings) and opens a
# file the listing shows no sidecar for with EMPTY_DIR (GDAL checks nothing), a file with
# sidecars GDAL finds by name with TRUE, and anything else with NO (GDAL's own listing,
# which matches names case-insensitively). A value set by the user, NO included, is kept
# and decides every open. GDAL reads the option at each open, and a rasterio.Env value
# wins over the environment variable.
_READDIR_ON_OPEN = "GDAL_DISABLE_READDIR_ON_OPEN"
_READDIR_PROCESS_VALUE = "TRUE"
# True once configure_gdal_env set the option (it was not set by the user): only then does
# open_raster choose a value per open.
_READDIR_SET_HERE = False

# The sidecar suffixes GDAL looks for by exact name when it has no sibling list (TRUE): the
# file's name plus a suffix, and the file's stem with another extension. From GDAL 3.13
# (the same in 3.12): .aux.xml (gcore/gdalpamdataset.cpp:898-909); .ovr/.OVR
# (gcore/gdaldefaultoverviews.cpp:267-284); .msk/.MSK (:1614-1631); .aux/.AUX
# (:313-325, gcore/gdal_misc.cpp:4541-4572 and :4660-4671); world files, the derived first+last+"w"
# extension, extension+"w" and "wld", each lower then upper case (gcore/gdal_misc.cpp:
# 2680-2771; frmts/gtiff/gtiffdataset_read.cpp:6096-6106); .tab (gdal_misc.cpp:2465-2500),
# .xml and .hdr (gtiffdataset_read.cpp:4219-4262, 4281, 6741).
_NAME_SIDECARS = (".aux.xml", ".ovr", ".OVR", ".msk", ".MSK", ".aux", ".AUX")
_STEM_SIDECAR_EXTS = ("wld", "tab", "xml", "hdr", "aux")
# The vendor metadata files GDAL's metadata readers look for under names not made from the
# raster's stem (gcore/mdreader/reader_*.cpp): summary.txt (ALOS), *_MTL.txt (Landsat),
# DIM_*.XML and RPC_*.XML (Pleiades), METADATA.DIM (SPOT), *.pass and *.rpc (EROS), and
# the .txt / .hdr files ALOS derives. A folder holding any of them is left to GDAL's own
# listing (NO), as without GeoBrix. The readers' stem-named files (*.IMD, *.RPB, *.XML,
# *.PVL, *_rpc.txt, *_METADATA.XML) are candidates of the raster itself.
_VENDOR_METADATA_EXTS = frozenset((".txt", ".dim", ".pass", ".rpc", ".hdr"))
_VENDOR_METADATA_PREFIXES = ("dim_", "rpc_")
# The only extensions a file may have to be opened with EMPTY_DIR: formats whose GDAL driver
# opens no other file but the sidecars the listing already checks (named after the file:
# .aux.xml, .ovr, .msk, world files, ...): GTiff, PNG, JPEG, JPEG 2000, netCDF, HDF4/HDF5
# (HDF-EOS), GRIB (its .idx index is named after the file, so it is a candidate). A file of
# any other kind keeps TRUE: a VRT or GTI opens its sources, some while it opens (a warped
# VRT: under EMPTY_DIR its source lost its external .ovr), an .xml or .safe or .json can be
# a dataset that names other files, an HFA .img has its .rrd/.ige, a .zarr opens its chunks.
_EMPTY_DIR_EXTS = frozenset(
    (
        ".tif",
        ".tiff",
        ".png",
        ".jpg",
        ".jpeg",
        ".jp2",
        ".nc",
        ".h5",
        ".he5",
        ".hdf",
        ".grib",
        ".grb",
        ".grb2",
        ".grib2",
    )
)


def _vendor_metadata(low_name: str) -> bool:
    """True for a (lower-cased) folder entry a GDAL metadata reader may look for."""
    ext = os.path.splitext(low_name)[1]
    if ext in _VENDOR_METADATA_EXTS:
        return True
    return ext == ".xml" and low_name.startswith(_VENDOR_METADATA_PREFIXES)


def _stat_sidecars(name: str) -> frozenset:
    """Every sidecar name GDAL checks by exact name for the file *name* (see above)."""
    stem, dot, ext = name.rpartition(".")
    if not dot:
        stem, ext = name, ""
    exts = list(_STEM_SIDECAR_EXTS)
    if len(ext) >= 2:
        exts += [ext[0] + ext[-1] + "w", ext + "w"]
    names = {name + s for s in _NAME_SIDECARS}
    for e in exts:
        names.update((f"{stem}.{e.lower()}", f"{stem}.{e.upper()}"))
    return frozenset(names)


def readdir_mode_for(name: str, folder_names) -> str:
    """The GDAL_DISABLE_READDIR_ON_OPEN value an open of the file *name* takes, given the
    names in its folder: ``EMPTY_DIR`` when nothing in the folder can be a sidecar of it,
    ``TRUE`` when every candidate is a name GDAL checks by exact name, else ``NO``.

    A candidate is a folder entry, other than the file, whose name starts, in any case,
    with the file's stem and then ``.`` or ``_``. A candidate GDAL would not find by exact
    name (``x.Tfw`` beside ``x.tif``, ``x.png``, ``x_rpc.txt``) or a vendor metadata file
    anywhere in the folder leaves the open to GDAL's own listing (``NO``). With no
    candidate, only a file of an allowlisted single-file format (_EMPTY_DIR_EXTS) gets
    ``EMPTY_DIR``; any other gets ``TRUE``.
    """
    stem = name.rpartition(".")[0] or name
    prefix = stem.lower()
    candidates = []
    vendor = False
    for other in folder_names:
        if other == name:
            continue
        low = other.lower()
        if low.startswith(prefix) and low[len(prefix) : len(prefix) + 1] in (".", "_"):
            candidates.append(other)
        if _vendor_metadata(low):
            vendor = True
    if vendor:
        return "NO"
    if not candidates:
        # EMPTY_DIR only for a format that opens no other file (_EMPTY_DIR_EXTS): the files
        # a VRT, GTI or other multi-file format opens while it opens would take EMPTY_DIR
        # too and lose their own sidecars. Any other kind checks its sidecars by name.
        ext = os.path.splitext(name)[1].lower()
        return "EMPTY_DIR" if ext in _EMPTY_DIR_EXTS else "TRUE"
    exact = _stat_sidecars(name)
    return "TRUE" if all(c in exact for c in candidates) else "NO"


# A folder held as larger than GDAL's own listing limit (see listing_cap), and a folder that
# could not be listed (retried after _UNLISTABLE_LISTING_RETRY_S).
_TOO_LARGE = object()
_UNLISTABLE = object()
_UNLISTABLE_LISTING_RETRY_S = 60.0
# A file missing from a held listing lists its folder again at most this often. A listing
# of a folder at the cap (1,000 names) cost 0.25-0.9 s on a Serverless worker; once per
# 10 s keeps that under a tenth of the time of a worker that opens new files of a growing
# folder all the time, and a file written meanwhile is opened with TRUE (by-name checks:
# correct, just not the faster EMPTY_DIR) until the next listing.
_RELIST_INTERVAL_S = 10.0


def _gdal_atoi(raw) -> int:
    """``atoi`` as GDAL applies it to a config option: leading spaces, an optional sign,
    the leading digits; 0 when there are none ("5.5" -> 5, "12x" -> 12, "abc" -> 0)."""
    text = str(raw).lstrip()
    sign = 1
    if text[:1] in ("+", "-"):
        sign = -1 if text[0] == "-" else 1
        text = text[1:]
    digits = ""
    for ch in text:
        if ch not in "0123456789":
            break
        digits += ch
    return sign * int(digits) if digits else 0


def listing_cap() -> Optional[int]:
    """GDAL's own limit on a folder listing, GDAL_READDIR_LIMIT_ON_OPEN (1000 unless set),
    read as GDAL reads it (atoi): past it GDAL drops the listing and checks sidecars by
    name (gcore/gdalopeninfo.cpp, GetSiblingFiles). None for no limit, a value of 0 or less
    (as GDAL's ``nMaxFiles > 0`` test treats it, so "abc" is no limit)."""
    raw = None
    try:
        from rasterio.env import get_gdal_config

        raw = get_gdal_config("GDAL_READDIR_LIMIT_ON_OPEN", normalize=False)
    except Exception:  # noqa: BLE001
        raw = os.environ.get("GDAL_READDIR_LIMIT_ON_OPEN")
    cap = 1000 if raw is None else _gdal_atoi(raw)
    return cap if cap > 0 else None


# A folder is listed once this worker has opened this many of its files: a folder it opens
# one file of (one scene a folder) is never listed.
_LIST_AFTER_OPENS = 2


class _FolderListings:
    """The names in each folder, listed once per process (os.scandir), most recent last.

    A folder is listed once this worker has opened ``list_after_opens`` distinct files of
    it; before that its files are opened with the process TRUE (by-name checks), and a
    folder the worker opens one file of (however often) is never listed. The listing is
    not moved to a background thread: on a Volume an open in the folder waits for a listing
    in progress there anyway (a fresh-file open took the 14.4-15.4 s of a 40,000-file
    listing running in another thread), and the FUSE mount fetches the whole folder before
    it returns the first name, so a scan stopped at the cap costs as much as a full one.

    Keyed by the folder's resolved path (resolved once per folder string: a FUSE stat costs
    milliseconds; at most ``4 * max_folders`` folder strings and open records are kept). At
    most ``max_folders`` listings are held, the least recently used dropped first. A folder
    with more names than GDAL's own listing limit (listing_cap), found by a listing or told
    by the reader (mark_too_large), goes into its own set of at most ``max_too_large``
    folders: its files keep TRUE, as GDAL itself checks by name past that limit, and it is
    not listed again. A folder that cannot be listed is not tried again for
    ``_UNLISTABLE_LISTING_RETRY_S``. A file missing from a held listing lists the folder
    again, at most once per ``relist_interval_s``; a sidecar written after the folder was
    listed is not seen until the folder is listed again (a file missing from it, or a new
    worker).
    """

    def __init__(
        self,
        max_folders: int = 64,
        relist_interval_s: float = _RELIST_INTERVAL_S,
        list_after_opens: int = _LIST_AFTER_OPENS,
        max_too_large: int = 1024,
    ):
        from collections import OrderedDict

        self.max_folders = max_folders
        self.relist_interval_s = relist_interval_s
        self.list_after_opens = list_after_opens
        self.max_too_large = max_too_large
        # key -> (frozenset of names | _UNLISTABLE, time listed)
        self._names: "OrderedDict[str, tuple]" = OrderedDict()
        self._too_large: "OrderedDict[str, None]" = OrderedDict()
        self._resolved: "OrderedDict[str, str]" = OrderedDict()
        self._opened: "OrderedDict[str, set]" = OrderedDict()
        self._lock = threading.Lock()
        self.listings = 0

    def clear(self) -> None:
        with self._lock:
            self._names.clear()
            self._too_large.clear()
            self._resolved.clear()
            self._opened.clear()

    def _list(self, folder: str):
        cap = listing_cap()
        names = []
        try:
            with os.scandir(folder) as it:
                for entry in it:
                    names.append(entry.name)
                    if cap is not None and len(names) > cap:
                        self.listings += 1
                        return _TOO_LARGE
        except OSError:
            return None
        self.listings += 1
        return frozenset(names)

    def _key(self, folder: str) -> str:
        with self._lock:
            key = self._resolved.get(folder)
            if key is not None:
                self._resolved.move_to_end(folder)
                return key
        key = os.path.realpath(folder)
        with self._lock:
            self._resolved[folder] = key
            while len(self._resolved) > 4 * self.max_folders:
                self._resolved.popitem(last=False)
        return key

    def _hold_too_large(self, key: str) -> None:
        # with self._lock held
        self._too_large[key] = None
        self._too_large.move_to_end(key)
        while len(self._too_large) > self.max_too_large:
            self._too_large.popitem(last=False)
        self._names.pop(key, None)

    def _store(self, key: str, folder: str):
        names = self._list(folder)
        with self._lock:
            if names is _TOO_LARGE:
                self._hold_too_large(key)
                return _TOO_LARGE
            self._names[key] = (_UNLISTABLE if names is None else names, _now())
            self._names.move_to_end(key)
            while len(self._names) > self.max_folders:
                self._names.popitem(last=False)
        return names

    def mark_too_large(self, folder: str) -> None:
        """Hold *folder* as too large without listing it (see note_folder_files), unless a
        listing of it is held already: a real listing outranks a hint."""
        key = self._key(folder)
        with self._lock:
            held = self._names.get(key)
            if held is not None and held[0] is not _UNLISTABLE:
                self._names.move_to_end(key)
                return
            self._hold_too_large(key)

    def names(self, folder: str, must_hold: str):
        """The names held for *folder* (a frozenset), ``_TOO_LARGE``, or None (not listed
        yet, or cannot be listed). Lists the folder at the ``list_after_opens``-th distinct
        file of it this worker opens, and again when *must_hold* is missing from the names
        held and the folder was listed at least ``relist_interval_s`` ago."""
        key = self._key(folder)
        with self._lock:
            if key in self._too_large:
                self._too_large.move_to_end(key)
                return _TOO_LARGE
            held = self._names.get(key)
            if held is not None:
                self._names.move_to_end(key)
            else:
                opened = self._opened.get(key)
                if opened is None:
                    opened = self._opened[key] = set()
                if len(opened) < self.list_after_opens:
                    opened.add(must_hold)
                self._opened.move_to_end(key)
                while len(self._opened) > 4 * self.max_folders:
                    self._opened.popitem(last=False)
                distinct = len(opened)
        if held is None:
            if distinct < self.list_after_opens:
                return None
            names = self._store(key, folder)
            return names if names is _TOO_LARGE or names else None
        names, listed_at = held
        if names is _UNLISTABLE:
            if _now() - listed_at < _UNLISTABLE_LISTING_RETRY_S:
                return None
            names = self._store(key, folder)
            return names if names is _TOO_LARGE or names else None
        if must_hold in names:
            return names
        if _now() - listed_at >= self.relist_interval_s:
            relisted = self._store(key, folder)
            if relisted is _TOO_LARGE or relisted:
                return relisted
        return names  # without must_hold the caller keeps TRUE for this open


_LISTINGS = _FolderListings()

# The tile metadata key the raster_gbx reader stamps on each virtual tile it emits: the
# number of files it found in the folder of the tile's path when it listed the folder at
# planning time (only files the reader accepted, so never more than the folder holds).
FOLDER_FILES_KEY = "path_folder_files"


def note_folder_files(path, count) -> None:
    """Take *count*, a tile's ``path_folder_files``, as a hint about the folder of *path*.

    The value is data (a tile row can carry anything), so it is only ever allowed to choose
    between two correct modes: a count over the listing cap (listing_cap, the same
    GDAL_READDIR_LIMIT_ON_OPEN) holds the folder as too large without listing it, and its
    files keep TRUE (by-name checks), as GDAL itself does past that limit; anything else
    (a count at or under the cap, none, or one that is neither a non-negative int nor a
    plain ASCII decimal string) does nothing, so
    the folder is listed as without a hint and its listing decides per file. A hint never
    yields EMPTY_DIR: only a real listing of the folder can (readdir_mode_for).
    """
    if isinstance(count, int) and not isinstance(count, bool):
        n = count  # the reader's own count (partition.folder_files)
    else:
        text = count.strip() if isinstance(count, str) else None
        if not text or not (text.isascii() and text.isdigit()):
            return  # not a plain decimal count (None, float, "inf", unicode): no hint
        n = int(text)
    if n < 0:
        return
    cap = listing_cap()
    if cap is None or n <= cap:
        return
    try:
        p = os.fspath(path)
    except TypeError:
        return
    if not isinstance(p, str) or not _is_plain_file_path(p):
        return
    _LISTINGS.mark_too_large(os.path.dirname(os.path.abspath(p)))


def _is_plain_file_path(path: str) -> bool:
    """A local or Volume file path; not a GDAL /vsi path, URL or DRIVER:... string."""
    if path.startswith("/vsi") or "://" in path:
        return False
    head = path.split("/", 1)[0]
    return ":" not in head


def _user_env_sets_readdir() -> bool:
    """True when a rasterio.Env the caller entered names GDAL_DISABLE_READDIR_ON_OPEN."""
    try:
        from rasterio.env import getenv, hasenv

        return hasenv() and _READDIR_ON_OPEN in getenv()
    except Exception:  # noqa: BLE001
        return False


def readdir_mode(path) -> Optional[str]:
    """The GDAL_DISABLE_READDIR_ON_OPEN value for one light-tier open of *path*, or None to
    leave the open to the current setting. None when the user set the option (in the
    environment before configure_gdal_env ran, any value; in a rasterio.Env around the
    call; or as a GDAL config option since), for a cloud, /vsi, DRIVER: or non-path
    *path* (TRUE, no listing), when the thread-local setter cannot be reached, for a folder
    that cannot be listed or is larger than GDAL's listing limit, and for a file missing
    from its folder's listing. See _READDIR_ON_OPEN and readdir_mode_for."""
    if not _READDIR_SET_HERE or _user_env_sets_readdir():
        return None
    try:
        from rasterio.env import get_gdal_config

        if get_gdal_config(_READDIR_ON_OPEN, normalize=False) != _READDIR_PROCESS_VALUE:
            return None  # set by the user since (an environment variable or an option)
    except Exception:  # noqa: BLE001 - no rasterio: nothing to decide
        return None
    try:
        p = os.fspath(path)
    except TypeError:  # a file-like object: rasterio.open takes it, nothing to list
        return None
    if not isinstance(p, str) or not _is_plain_file_path(p):
        return None
    if _thread_config() is None:
        return None  # no per-open value can be set: do not list for nothing
    folder, name = os.path.split(os.path.abspath(p))
    names = _LISTINGS.names(folder, name)
    if names is None or names is _TOO_LARGE or name not in names:
        return None
    return readdir_mode_for(name, names)


# CPLSetThreadLocalConfigOption / CPLGetThreadLocalConfigOption of the libgdal rasterio
# uses, through ctypes: (setter, getter), False when they cannot be reached, None before the
# first look. A rasterio.Env is no use for a per-open value: on exit the outermost Env sets
# the value it found on entry as a process-wide GDAL config option (rasterio/env.py,
# Env.__exit__ with _discovered_options), and from then on that option shadows the
# environment variable, including a value the user sets later. In the main thread
# rasterio's own set_gdal_config is process-wide too.
_THREAD_CONFIG = None


def _thread_config():
    """``(set, get)`` for this thread's GDAL config options, or None (see _THREAD_CONFIG).
    Checked once: a value set through it must be what rasterio's GDAL reads."""
    global _THREAD_CONFIG
    if _THREAD_CONFIG is None:
        _THREAD_CONFIG = False
        try:
            import ctypes

            from rasterio.env import get_gdal_config

            from databricks.labs.gbx.pyrx.core.analysis import _load_libgdal

            lib = _load_libgdal()
            setter = lib.CPLSetThreadLocalConfigOption
            setter.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
            setter.restype = None
            getter = lib.CPLGetThreadLocalConfigOption
            getter.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
            getter.restype = ctypes.c_char_p
            setter(b"GBX_THREAD_CONFIG_CHECK", b"1")
            try:
                same_gdal = (
                    get_gdal_config("GBX_THREAD_CONFIG_CHECK", normalize=False) == "1"
                )
            finally:
                setter(b"GBX_THREAD_CONFIG_CHECK", None)
            if same_gdal:
                _THREAD_CONFIG = (setter, getter)
            else:
                _log.debug(
                    "GDAL_DISABLE_READDIR_ON_OPEN per open: off (the libgdal reached "
                    "through ctypes is not rasterio's); opens check sidecars by name"
                )
        except (
            Exception
        ) as exc:  # noqa: BLE001 - no reachable libgdal: no per-open value
            _THREAD_CONFIG = False
            _log.debug(
                "GDAL_DISABLE_READDIR_ON_OPEN per open: off (%s: %s); opens check "
                "sidecars by name",
                type(exc).__name__,
                exc,
            )
    return _THREAD_CONFIG or None


@contextmanager
def readdir_option(mode: Optional[str]):
    """Run the block with GDAL_DISABLE_READDIR_ON_OPEN = *mode* for this thread only, and
    restore the thread's previous value after it. ``None`` leaves the setting alone. When
    the thread-local setter cannot be reached the block runs under the current setting:
    a by-name sidecar check (TRUE) in place of the value asked for."""
    tc = _thread_config() if mode is not None else None
    if tc is None:
        yield
        return
    setter, getter = tc
    key = _READDIR_ON_OPEN.encode()
    prev = getter(key, None)
    setter(key, mode.encode())
    try:
        yield
    finally:
        setter(key, prev)


def open_raster(path, *args, **kwargs):
    """``rasterio.open(path, ...)`` with GDAL_DISABLE_READDIR_ON_OPEN chosen for this file
    (readdir_mode), for this open only: GDAL decides which sidecars to look for when it
    opens the file, so the dataset returned keeps that choice."""
    import rasterio

    with readdir_option(readdir_mode(path)):
        return rasterio.open(path, *args, **kwargs)


def configure_gdal_env(extra_proj_dirs: Optional[Sequence[str]] = None) -> None:
    """Idempotently set GDAL_DATA / PROJ_DATA, adding custom grid dirs.

    extra_proj_dirs:
      - None  -> read the driver-side registry (correct only on the driver).
      - list  -> use exactly these (what worker UDF closures pass); [] = none.
    Custom dirs are merged ahead of the base PROJ data (so user grids win) into ONE
    directory exported as PROJ_DATA; see _proj_data_dir for why it is not a joined list.
    Registered dirs are also pushed into pyproj's search path so the light vector
    (pyproj) transform path finds them — the env var alone does not reach pyproj.
    A PROJ_DATA / PROJ_LIB that holds no proj.db this process can read is replaced by
    the bundled PROJ data (a readable one is never changed).

    GDAL_DISABLE_READDIR_ON_OPEN is set to TRUE unless it is set already (see
    _READDIR_ON_OPEN).
    """
    global _READDIR_SET_HERE
    if _READDIR_ON_OPEN not in os.environ:
        os.environ[_READDIR_ON_OPEN] = _READDIR_PROCESS_VALUE
        _READDIR_SET_HERE = True

    if not os.environ.get("GDAL_DATA"):
        p = _bundled_gdal_data()
        if p:
            os.environ["GDAL_DATA"] = p

    if extra_proj_dirs is None:
        from databricks.labs.gbx.core import proj_grids

        extra_proj_dirs = proj_grids.get_registered_dirs()

    current = os.environ.get("PROJ_DATA") or os.environ.get("PROJ_LIB") or ""
    base = _PROJ_DATA_BASES.get(current, current)  # unwrap a merged dir set earlier
    if base and not _readable_proj_data(base):
        # A PROJ data dir this process cannot read is no PROJ data at all: rasterio would
        # take it as GDAL's PROJ search path and fail every CRS lookup ("Cannot find
        # proj.db"). DBR 19 exports PROJ_DATA=/databricks/native/proj-data, which the
        # unprivileged notebook user of a standard-access cluster cannot read. Use the
        # bundled data instead, here and in the search path rasterio set at its import.
        p = _bundled_proj_data()
        if p:
            os.environ["PROJ_DATA"] = base = p
            _point_rasterio_at(p)
    if not base:
        p = _bundled_proj_data()
        if p:
            base = p

    if extra_proj_dirs:
        merged = _proj_data_dir(base, extra_proj_dirs)
        if merged:
            os.environ["PROJ_DATA"] = merged
            # rasterio applied the PROJ_DATA of its import (or the bundled data above) as
            # GDAL's search path; the merged dir holds the registered grids as well
            _point_rasterio_at(merged)
        _register_pyproj_search_dirs(extra_proj_dirs)
    elif base and not os.environ.get("PROJ_DATA") and not os.environ.get("PROJ_LIB"):
        os.environ["PROJ_DATA"] = base


def assert_rasterio_available() -> Tuple[str, str]:
    """Return (gdal_version, rasterio_version); raise a clear error if missing."""
    try:
        import rasterio
    except ImportError as e:  # pragma: no cover - exercised only without the extra
        raise ImportError(
            "pyrx requires rasterio. Install with: pip install 'geobrix[light_env6]'"
        ) from e
    return rasterio.__gdal_version__, rasterio.__version__
