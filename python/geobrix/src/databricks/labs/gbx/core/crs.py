"""CRS string/int resolution — the one place the SRID resolution rule lives (light tier).

An integer SRID is classified against the AUTHORITATIVE PROJ code registries, not the
lenient ``CRS.from_epsg`` (which succeeds for ESRI codes and mislabels them ``EPSG``).
Rule: ``n`` in the EPSG code set -> ``EPSG:<n>``; else in the ESRI set -> ``ESRI:<n>``;
else raise (a code in neither registry is invalid). The code sets come from PROJ's
``proj.db`` via ``pyproj.database.get_codes`` and are built once + cached.
"""

import threading
from collections import OrderedDict
from functools import lru_cache
from typing import Optional, Union

from rasterio.crs import CRS

# 120 WGS84 UTM zones (EPSG 326xx N + 327xx S) + 4326/27700/3857 + headroom.
# A workload touching every UTM zone plus the common CRSes never evicts.
_TRANSFORMER_CACHE_SIZE = 128

# Transformers/CRS objects are NOT thread-safe for concurrent use, and executors
# run multiple Spark tasks per JVM. Keep one LRU cache per worker thread — reuse
# within a thread's row loop, zero cross-thread contention (no lock on the hot path).
_thread_local = threading.local()

# CRS PJTypes a numeric SRID could name (projected / geographic / geocentric / compound).
_CRS_PJTYPES = (
    "PROJECTED_CRS",
    "GEOGRAPHIC_2D_CRS",
    "GEOGRAPHIC_3D_CRS",
    "GEOCENTRIC_CRS",
    "COMPOUND_CRS",
)


@lru_cache(maxsize=1)
def _authority_codes(authority: str) -> frozenset:
    """Authoritative set of code strings for an authority ('EPSG'/'ESRI') from proj.db."""
    from pyproj.database import get_codes
    from pyproj.enums import PJType

    codes: set = set()
    for name in _CRS_PJTYPES:
        try:
            codes |= set(
                get_codes(authority, getattr(PJType, name), allow_deprecated=True)
            )
        except Exception:
            # A PJType unsupported by the installed pyproj is skipped; the others suffice.
            continue
    return frozenset(codes)


def _epsg_codes() -> frozenset:
    return _authority_codes("EPSG")


def _esri_codes() -> frozenset:
    return _authority_codes("ESRI")


def _is_intlike(value) -> bool:
    # Accept Python int, numpy integer (e.g. shapely.get_srid -> np.int32), and
    # int-castable strings. bool is an int subclass but not a SRID — exclude it.
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    try:
        import numpy as _np

        if isinstance(value, _np.integer):
            return True
    except Exception:
        pass
    if isinstance(value, str):
        try:
            int(value.strip())
            return True
        except (ValueError, AttributeError):
            return False
    return False


def resolve_crs(value: Union[int, str]) -> CRS:
    """Resolve a SRID int or CRS string to a rasterio CRS.

    Integer (or int-castable string): classified via the authoritative PROJ code sets —
    EPSG code -> ``EPSG:<n>``; else ESRI code -> ``ESRI:<n>``; else ``ValueError``.
    Any other string: parsed as a CRS definition (``EPSG:x`` / ``ESRI:x`` / WKT / PROJ4).
    Raises for a code in neither registry, or for unparseable garbage — intended (this is
    the apply moment; callers guard ``0``/absent as "no CRS" before calling).
    """
    if _is_intlike(value):
        n = str(int(str(value).strip()))
        if n in _epsg_codes():
            return CRS.from_epsg(int(n))
        if n in _esri_codes():
            return CRS.from_authority("ESRI", int(n))
        raise ValueError(f"resolve_crs: {n} is not a valid EPSG or ESRI code")
    return CRS.from_user_input(value)


def crs_to_canonical(crs: Optional[CRS]) -> Optional[str]:
    """Authority string ('EPSG:4326'/'ESRI:54008') when available, else WKT.

    None-safe: returns None when crs is None.
    """
    if crs is None:
        return None
    auth = crs.to_authority()  # (name, code) or None
    if auth:
        return f"{auth[0]}:{auth[1]}"
    return crs.to_wkt()


def _transformer_cache() -> "OrderedDict":
    cache = getattr(_thread_local, "transformers", None)
    if cache is None:
        cache = OrderedDict()
        _thread_local.transformers = cache
    return cache


def _as_crs(value) -> CRS:
    """A CRS object passes through; anything else is resolved via resolve_crs."""
    return value if isinstance(value, CRS) else resolve_crs(value)


def get_transformer(src, dst):
    """Thread-local, LRU-bounded ``pyproj.Transformer`` keyed by canonical CRS pair.

    ``src``/``dst`` may each be an int SRID, a CRS string, or a CRS object. Built
    with ``always_xy=True`` (lon/lat axis order — a classic silent-wrong-answer
    source if unpinned). Equivalent spellings (``4326`` / ``"4326"`` /
    ``"EPSG:4326"``) resolve to the same canonical key and share one transformer.
    """
    from pyproj import Transformer

    src_crs = _as_crs(src)
    dst_crs = _as_crs(dst)
    key = (crs_to_canonical(src_crs), crs_to_canonical(dst_crs))
    cache = _transformer_cache()
    tr = cache.get(key)
    if tr is None:
        tr = Transformer.from_crs(src_crs, dst_crs, always_xy=True)
        cache[key] = tr
        if len(cache) > _TRANSFORMER_CACHE_SIZE:
            cache.popitem(last=False)  # evict oldest (LRU)
    else:
        cache.move_to_end(key)
    return tr


def resolve_source_crs(embedded_srid, srid=None, crs=None) -> Optional[CRS]:
    """Rule 1 (per-geom) source-CRS resolution.

    Precedence: an embedded SRID (from EWKB/EWKT) always wins; else the single
    explicit ``srid`` or ``crs`` param (both set -> error); else ``None``
    (CRS-less). The explicit param is a per-geom fallback for plain WKB/WKT — a
    geometry carrying an embedded SRID ignores the param (mixed-column safe), no
    error. Never raises except the both-params conflict + an unresolvable string.
    """
    if embedded_srid and int(embedded_srid) > 0:
        return resolve_crs(int(embedded_srid))
    if srid is not None and crs is not None:
        raise ValueError("resolve_source_crs: provide srid OR crs, not both")
    if crs is not None:
        return resolve_crs(crs)
    if srid is not None:
        return resolve_crs(srid)
    return None
