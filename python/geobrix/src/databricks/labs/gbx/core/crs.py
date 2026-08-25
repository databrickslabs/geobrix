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


def authority_srid_of(crs: Optional[CRS]) -> Optional[int]:
    """Integer authority code a geometry can carry as its SRID, or None.

    Returns ``n`` for a CRS whose authority code is numeric (``EPSG:4326`` -> 4326,
    ``ESRI:54008`` -> 54008). Returns ``None`` for:

    - **authority-less** CRS — raw WKT / PROJ4 definitions that match no registry entry
      at full confidence;
    - **non-numeric** authority codes — e.g. ``OGC:CRS84``, ``IGNF:LAMB93``: real,
      resolvable CRSes whose code is not an integer and therefore cannot be stored in a
      geometry's SRID slot.

    ``confidence_threshold=100`` (vs. the ``to_authority`` default of 70) is deliberate
    and load-bearing: a geometry SRID is an **exact integer identity**, so a
    70%-confidence fuzzy match must never be silently written into a geometry. Without
    the strict threshold, PROJ's fuzzy matcher maps ``+proj=utm +zone=33 +datum=WGS84``
    onto ``EPSG:32633`` — a guess, stamped as fact, and indistinguishable downstream from
    an SRID the user actually asserted. GDAL's ``GetAuthorityName``/``GetAuthorityCode``
    (the heavyweight tier's rule) does no fuzzy matching at all, so the strict threshold
    is also what makes the two tiers agree on which CRSes are stampable.

    This is the single home for the "what SRID can a geometry carry for this CRS?"
    question — the mirror of the heavyweight ``SpatialRefOps.authoritySridOf``.
    """
    if crs is None:
        return None
    auth = crs.to_authority(confidence_threshold=100)  # (name, code) or None
    if not auth:
        return None
    try:
        return int(auth[1])
    except (TypeError, ValueError):
        return None


def crs_to_canonical(crs: Optional[CRS]) -> Optional[str]:
    """Authority string ('EPSG:4326'/'ESRI:54008') when available, else WKT.

    None-safe: returns None when crs is None.

    Uses PROJ's default match confidence, so a PROJ4/WKT definition that closely
    resembles a registry CRS canonicalizes to that authority string. That is the right
    answer for a *display / provenance* name (it is what the shipped raster surface
    reports for such a CRS) but the WRONG answer for a cache key — see
    :func:`_transformer_key`, which is deliberately stricter.
    """
    if crs is None:
        return None
    auth = crs.to_authority()  # (name, code) or None
    if auth:
        return f"{auth[0]}:{auth[1]}"
    return crs.to_wkt()


def _transformer_key(crs: CRS) -> str:
    """Cache key that identifies a CRS EXACTLY — never a fuzzy-matched near-neighbour.

    Deliberately NOT :func:`crs_to_canonical`. That function answers "what should this
    CRS be *called*?" at PROJ's default 70% match confidence, which makes two
    genuinely different CRSes share one name: the Dutch-RD PROJ4 string
    ``+proj=sterea +lat_0=52.156… +ellps=bessel +towgs84=0,0,0,0,0,0,0`` canonicalizes
    to ``EPSG:28992``, but its null datum shift puts coordinates ~177 m away from real
    EPSG:28992. Keyed on the canonical name, whichever of the two was requested FIRST
    won the cache entry and silently answered for the other — so the correct
    ``EPSG:28992`` request could return the PROJ4 CRS's coordinates, and vice versa.
    Cache-order-dependent georeferencing, in both directions.

    Keying at full confidence keeps them distinct: an exactly-identified CRS keys on its
    authority string, anything else on its full WKT. Equivalent *spellings* of one CRS
    (``4326`` / ``"4326"`` / ``"EPSG:4326"``) still collapse to one key, so the cache
    hit rate on the hot path is unchanged; only genuinely-different CRSes are separated.

    The tradeoff is a long (WKT) key for an authority-less CRS. That is the same
    tradeoff the heavyweight tier already makes — GDAL's ``crsToCanonical`` returns full
    WKT for a CRS with no authority node, and ``transformPlan`` keys on it — and a
    correct answer at the cost of a longer dict key is the right trade.
    """
    auth = crs.to_authority(confidence_threshold=100)
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
    ``"EPSG:4326"``) resolve to the same key and share one transformer.

    The key comes from :func:`_transformer_key`, NOT :func:`crs_to_canonical`: the
    transformer selected here decides the actual coordinate output, so two CRSes that
    merely *look* alike must never share an entry. See ``_transformer_key`` for the
    177 m failure that keying on the canonical name produced.
    """
    from pyproj import Transformer

    src_crs = _as_crs(src)
    dst_crs = _as_crs(dst)
    key = (_transformer_key(src_crs), _transformer_key(dst_crs))
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


def in_target_domain(lonlat_coords, target_crs) -> "Optional[bool]":
    """Is every lon/lat coordinate inside target_crs's area_of_use bbox?

    lonlat_coords: (N,2) ndarray of (lon, lat) in EPSG:4326 degrees.
    Returns True (all inside), False (any outside -> out-of-domain, incl. straddling),
    or None when target_crs has no area_of_use metadata (caller skips the check —
    never NULL what cannot be disproved). Empty input -> True (no coords to reject).
    """
    aou = getattr(target_crs, "area_of_use", None)
    if aou is None or aou.bounds is None:
        return None
    west, south, east, north = aou.bounds
    if lonlat_coords.shape[0] == 0:
        return True
    lon = lonlat_coords[:, 0]
    lat = lonlat_coords[:, 1]
    inside = (lon >= west) & (lon <= east) & (lat >= south) & (lat <= north)
    return bool(inside.all())


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
