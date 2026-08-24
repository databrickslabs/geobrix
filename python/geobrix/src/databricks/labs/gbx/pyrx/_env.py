"""GDAL/PROJ environment configuration for the bundled rasterio wheel.

rasterio's manylinux/macOS wheels ship their own GDAL + PROJ data. We point
GDAL_DATA / PROJ_DATA at those bundled paths *only if unset*, so pyrx does not
collide with any cluster-level GDAL installed by the heavyweight init script.
"""

import os
from typing import Optional, Sequence, Tuple


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
    """
    try:
        from rasterio._env import PROJDataFinder

        path = PROJDataFinder().search()
        return path if path and os.path.isdir(path) else None
    except Exception:
        return None


def _prepend_unique(base: str, extra: Sequence[str]) -> str:
    """Colon-join `extra` dirs ahead of `base`, de-duped, preserving order."""
    parts = []
    for d in list(extra) + ([base] if base else []):
        for seg in d.split(os.pathsep) if d else []:
            if seg and seg not in parts:
                parts.append(seg)
    return os.pathsep.join(parts)


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


def configure_gdal_env(extra_proj_dirs: Optional[Sequence[str]] = None) -> None:
    """Idempotently set GDAL_DATA / PROJ_DATA, prepending custom grid dirs.

    extra_proj_dirs:
      - None  -> read the driver-side registry (correct only on the driver).
      - list  -> use exactly these (what worker UDF closures pass); [] = none.
    Custom dirs are prepended ahead of the bundled PROJ data so user grids win.
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

    base = os.environ.get("PROJ_DATA") or os.environ.get("PROJ_LIB") or ""
    if not base:
        p = _bundled_proj_data()
        if p:
            base = p

    if extra_proj_dirs:
        os.environ["PROJ_DATA"] = _prepend_unique(base, extra_proj_dirs)
        _register_pyproj_search_dirs(extra_proj_dirs)
    elif base and not os.environ.get("PROJ_DATA") and not os.environ.get("PROJ_LIB"):
        os.environ["PROJ_DATA"] = base


def assert_rasterio_available() -> Tuple[str, str]:
    """Return (gdal_version, rasterio_version); raise a clear error if missing."""
    try:
        import rasterio
    except ImportError as e:  # pragma: no cover - exercised only without the extra
        raise ImportError(
            "pyrx requires rasterio. Install with: pip install 'geobrix[light]'"
        ) from e
    return rasterio.__gdal_version__, rasterio.__version__
