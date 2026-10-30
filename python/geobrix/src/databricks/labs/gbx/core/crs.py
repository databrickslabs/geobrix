"""CRS string/int resolution — the one place the SRID resolution rule lives (light tier).

An integer SRID is classified against the AUTHORITATIVE PROJ code registries, not the
lenient ``CRS.from_epsg`` (which succeeds for ESRI codes and mislabels them ``EPSG``).
Rule: ``n`` in the EPSG code set -> ``EPSG:<n>``; else in the ESRI set -> ``ESRI:<n>``;
else raise (a code in neither registry is invalid). The code sets come from PROJ's
``proj.db`` via ``pyproj.database.get_codes`` and are built once + cached.
"""

import re
import threading
from collections import OrderedDict
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional, Tuple, Union

import numpy as np
from rasterio.crs import CRS

from databricks.labs.gbx.pyrx._env import configure_gdal_env

# Every light CRS lookup comes through this module, on the driver as in the tasks, so make
# PROJ's data findable before the first one: a runtime can export a PROJ_DATA this process
# cannot read (DBR 19 standard access), and rasterio took it as its search path at import.
configure_gdal_env()

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


@lru_cache(maxsize=None)
def _crs_codes(authority: str) -> frozenset:
    """Every CRS code (any kind, vertical included) ``authority`` has in PROJ's database, upper-cased."""
    from pyproj.database import get_codes
    from pyproj.enums import PJType

    try:
        return frozenset(
            c.upper() for c in get_codes(authority, PJType.CRS, allow_deprecated=True)
        )
    except Exception:  # an authority PROJ does not have
        return frozenset()


def _authority_known(value: str) -> None:
    """Raise unless each AUTHORITY:CODE ``value`` names (each part of a compound one) is a CRS in PROJ's database.
    GDAL's parse, which rasterio's ``from_authority`` uses, reads a code it cannot resolve as a file name; the JVM
    parses with file and network access off, so both tiers refuse it."""
    auth, _, rest = value.partition(":")
    for part in rest.lstrip(":").split("+"):
        a, sep, code = part.partition(":")
        a, code = (a, code.lstrip(":")) if sep else (auth, part)
        if code.upper() not in _crs_codes(a.upper()):
            raise ValueError(
                f"resolve_crs: '{value}' names {a}:{code}, which is no CRS in PROJ's database; GDAL would read it "
                "as a file name, and a CRS value is never read from a file. Fix: pass EPSG:<code> (or another "
                "AUTHORITY:CODE PROJ knows), an integer SRID, WKT, PROJJSON or a +proj string"
            )


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


# The CRS strings resolve_crs parses, besides an int SRID. GDAL's universal parser reads a file when a string
# names one (a relative name included), and PROJ opens a grid named by path, so anything else is refused: a CRS
# value never reaches a file or the network. The JVM's SpatialRefOps.resolveCrs applies the same rule.
# AUTHORITY:CODE, and a compound one (EPSG:4326+5773, EPSG:4326+EPSG:5773: a horizontal CRS plus a vertical one)
_AUTH_CODE = re.compile(
    r"[A-Za-z][A-Za-z0-9_]*::?[A-Za-z0-9_.\-]+(?:\+(?:[A-Za-z][A-Za-z0-9_]*::?)?[A-Za-z0-9_.\-]+)*"
)
_URL = re.compile(
    r"[A-Za-z][A-Za-z0-9+.\-]*://"
)  # a value that IS a URL (PROJJSON's $schema is not one)
_WKT_START = re.compile(r"[A-Za-z][A-Za-z0-9_]*\s*\[")
_URN_PREFIXES = ("urn:ogc:def:crs:", "urn:ogc:def:crs,crs:", "urn:x-ogc:def:crs:")
_NAMED_CRS = frozenset({"WGS84", "WGS72", "NAD27", "NAD83"})
# Grid (and init file) parameters, which PROJ opens by name: a bare name is looked up on PROJ's search path, the
# registered grid directories included; a path is refused.
_PROJ_GRID_PARAM = re.compile(
    r"\+?\b(nadgrids|geoidgrids|grids|init)\s*=\s*([^\s\"\]]+)", re.I
)
_WKT_GRID_PARAM = re.compile(
    r'(?:PROJ4_GRIDS"\s*,\s*"|PARAMETERFILE\s*\[\s*"[^"]*"\s*,\s*")([^"]*)"', re.I
)
_JSON_STRING_VALUE = re.compile(r'"value"\s*:\s*"([^"]*)"')
CRS_FIX = "Fix: pass EPSG:<code> (or another AUTHORITY:CODE), an integer SRID, WKT, PROJJSON or a +proj string"


def _names_a_path(value: str) -> bool:
    """True for a grid value (or a comma list of them) that names a file by path rather than by grid name."""
    items = [i.strip().lstrip("@") for i in value.split(",")]
    return any("/" in i or "\\" in i or i.startswith(".") for i in items if i)


def crs_text_kind(value: str) -> str:
    """The kind of CRS string ``value`` is: ``auth`` (``EPSG:4326``), ``wkt``, ``projjson``, ``proj``, ``urn`` or
    ``named`` (``WGS84``). Raises ValueError, naming what the value is and with a ``Fix:``, for anything else (a
    file name, a URL) and for a grid or init file named by path; such a value is never read.
    """
    v = value.strip()
    if not v:
        raise ValueError(f"resolve_crs: the CRS value is empty. {CRS_FIX}")
    if _URL.match(v) or v.lower().startswith("www."):
        raise ValueError(
            f"resolve_crs: '{value}' is a URL; a CRS value is never fetched. {CRS_FIX}"
        )
    if v.startswith("{"):
        kind = "projjson"
        grids = _JSON_STRING_VALUE.findall(v)
    elif v.startswith("+") or "+proj=" in v:
        kind = "proj"
        grids = []
    elif _WKT_START.match(v):
        kind = "wkt"
        grids = _WKT_GRID_PARAM.findall(v)
    elif v.lower().startswith(_URN_PREFIXES):
        return "urn"
    elif v.upper() in _NAMED_CRS:
        return "named"
    elif _AUTH_CODE.fullmatch(v):
        return "auth"
    else:
        raise ValueError(
            f"resolve_crs: '{value}' is not a CRS GeoBrix reads (it looks like a file or CRS name); a CRS "
            f"value is never read from a file. {CRS_FIX}"
        )
    grids += [g for _, g in _PROJ_GRID_PARAM.findall(v)]
    bad = [g for g in grids if _names_a_path(g)]
    if bad:
        raise ValueError(
            f"resolve_crs: '{value}' names a grid or init file by path ({bad[0]}); a CRS value never names a "
            "file. Fix: register the grid's directory with register_proj_grids and name the grid alone, or "
            "pass EPSG:<code>"
        )
    return kind


def resolve_crs(value: Union[int, str]) -> CRS:
    """Resolve a SRID int or CRS string to a rasterio CRS.

    Integer (or int-castable string): classified via the authoritative PROJ code sets —
    EPSG code -> ``EPSG:<n>``; else ESRI code -> ``ESRI:<n>``; else ``ValueError``.
    Any other string is one of the kinds ``crs_text_kind`` accepts (``AUTH:CODE``, WKT, PROJJSON, a PROJ string,
    an OGC URN, ``WGS84``-style names), parsed by that kind's own importer, so it is never read from a file or a
    URL; anything else raises with a ``Fix:``. Raises for a code in neither registry, or for unparseable
    garbage — intended (this is the apply moment; callers guard ``0``/absent as "no CRS" before calling).
    """
    if _is_intlike(value):
        n = str(int(str(value).strip()))
        if n in _epsg_codes():
            return CRS.from_epsg(int(n))
        if n in _esri_codes():
            return CRS.from_authority("ESRI", int(n))
        raise ValueError(f"resolve_crs: {n} is not a valid EPSG or ESRI code")
    if isinstance(value, CRS):
        return value
    if not isinstance(value, str):
        if isinstance(value, dict):  # PROJ parameters as a mapping
            crs_text_kind(" ".join(f"+{k}={v}" for k, v in value.items()))
        return CRS.from_user_input(value)
    kind = crs_text_kind(value)
    v = value.strip()
    if kind == "auth":
        _authority_known(v)
        auth, _, code = v.partition(":")
        return CRS.from_authority(auth.upper(), code.lstrip(":"))
    if kind == "wkt":
        return CRS.from_wkt(v)
    if kind == "proj":
        return CRS.from_proj4(v)
    # PROJJSON, an OGC URN or a named CRS: GDAL parses these before it would try a file.
    return CRS.from_user_input(v)


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


def to_pyproj_crs(crs) -> "pyproj.CRS":  # noqa: F821
    """Bridge rasterio CRS / authority string / WKT / int to a **pyproj** CRS.

    Uses ``from_authority`` at 100 % confidence when an authority is recognized,
    falling back to ``from_wkt``.  This is the proven pattern from
    ``pyvx/_crs.py:581-585``, promoted to a canonical helper.

    **Never for storage** — use :func:`crs_to_canonical` for provenance strings.
    Consumers that need pyproj-only properties (``is_geographic``, ``ellipsoid``,
    ``area_of_use``, ``laspy`` header CRS) call this.
    """
    import pyproj as _pyproj

    rio = _as_crs(crs)
    _auth = rio.to_authority(confidence_threshold=100)
    if _auth:
        return _pyproj.CRS.from_authority(*_auth)
    return _pyproj.CRS.from_wkt(rio.to_wkt())


def area_m2(geom, crs) -> float:
    """Area of ``geom`` in SQUARE METERS, whatever ``crs`` it is expressed in.

    ``segment_raster``'s polygons carry the source raster's CRS. For an orthomosaic COG
    that CRS is frequently GEOGRAPHIC (e.g. EPSG:4326), where ``geom.area`` is in square
    DEGREES -- ~10 orders of magnitude off the square-meter scale ``min_area`` is
    expressed in, so a metric ``min_area`` compared against a raw ``.area`` would
    silently drop EVERY polygon (a whole drone orthomosaic spans ~1e-5 sq degrees, far
    under a 10 m^2 threshold). So: for a geographic CRS take the geodesic area on the
    CRS's own ellipsoid (pyproj ``Geod.geometry_area_perimeter``); for a projected CRS
    take the planar ``geom.area`` scaled by the CRS's linear-unit-to-meter factor (1.0
    for the meter-based UTM / planar CRS geobrix emits). ``crs=None`` -> planar area
    as-is. Uses the canonical ``core.crs.to_pyproj_crs`` bridge (never bare CRS.from_*).
    """
    if crs is None:
        return geom.area
    pcrs = to_pyproj_crs(crs)
    if pcrs.is_geographic:
        geod = pcrs.get_geod()
        if geod is not None:
            area, _ = geod.geometry_area_perimeter(geom)
            return abs(area)
        return geom.area
    try:
        factor = pcrs.axis_info[0].unit_conversion_factor or 1.0
    except (IndexError, AttributeError):
        factor = 1.0
    return geom.area * (factor**2)


def crs_equal(a, b) -> bool:
    """Semantic CRS equality: normalize both via :func:`to_pyproj_crs`, compare with pyproj ``.equals()``.

    None-safe: two ``None`` values are equal; ``None`` vs any CRS is ``False``.
    Replaces the inline ``_ProjCRS.from_user_input(a).equals(b)`` pattern
    (e.g. ``pyrx/core/agg.py:485-487``).
    """
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    try:
        return to_pyproj_crs(a).equals(to_pyproj_crs(b))
    except Exception:
        return str(a) == str(b)


def crs_to_proj4(crs: Optional[CRS]) -> Optional[str]:
    """OGR-parity PROJ4 string for output columns — **never for storage**.

    Routes rasterio CRS → pyproj via :func:`to_pyproj_crs` and calls
    ``.to_proj4()``.  The pyproj PROJ4 deprecation ``UserWarning`` is
    suppressed centrally here so callers never see it.

    For canonical storage / provenance strings use :func:`crs_to_canonical`.
    """
    if crs is None:
        return None
    import warnings

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            result = to_pyproj_crs(crs).to_proj4()
        return result or None
    except Exception:
        return None


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


def utm_epsg_for(lon: float, lat: float) -> int:
    """WGS84 UTM zone EPSG code for a representative lon/lat.

    Returns 326zz (northern hemisphere) or 327zz (southern), where zz is the
    UTM zone 1-60. Used to reproject a local scene to its metric UTM zone.
    """
    zone = int((float(lon) + 180.0) / 6.0) + 1
    zone = max(1, min(60, zone))
    return (32600 if float(lat) >= 0.0 else 32700) + zone


# Equirectangular local-tangent-plane approximation of degrees-latitude in metres.
_M_PER_DEG_LAT = 111320.0


def lonlat_to_enu(lon, lat, ref_lat, ref_lon):
    """Local-tangent-plane (equirectangular) ENU east/north metres from lon/lat,
    relative to (ref_lat, ref_lon). Scalars or numpy arrays."""
    cos_ref = np.cos(np.radians(ref_lat))
    east = (np.asarray(lon, dtype="float64") - ref_lon) * _M_PER_DEG_LAT * cos_ref
    north = (np.asarray(lat, dtype="float64") - ref_lat) * _M_PER_DEG_LAT
    return east, north


def enu_to_lonlat(east, north, ref_lat, ref_lon):
    """Inverse of lonlat_to_enu."""
    cos_ref = np.cos(np.radians(ref_lat))
    lon = ref_lon + np.asarray(east, dtype="float64") / (_M_PER_DEG_LAT * cos_ref)
    lat = ref_lat + np.asarray(north, dtype="float64") / _M_PER_DEG_LAT
    return lon, lat


# --- UC surface: CRS forms, decided from what the product st_* functions accept -----------------------
# UC functions accept an INTEGER SRID or an `EPSG:<n>` / `ESRI:<n>` string, converted to its integer
# BEFORE any product st_* call (the product casts strings to INT; with ANSI off a bad one is a silent
# NULL). Everything else (WKT, PROJJSON, PROJ4, URN/URL, other authorities, custom) is refused with a
# Fix:. Outputs carry the integer SRID. This table is the single place that decision lives.
UC_CRS_FORMS: Tuple[str, ...] = ("srid_int", "epsg_code", "esri_code")
# A code has at most 9 digits: a longer digit run is refused with the Fix, never handed to int().
_UC_CODE = re.compile(r"^(EPSG|ESRI):([0-9]{1,9})$", re.IGNORECASE)
_UC_DIGITS = re.compile(r"^[0-9]{1,9}$")


@dataclass(frozen=True)
class UcCrs:
    authority: str  # "EPSG" | "ESRI"
    srid: int  # what product st_* calls and outputs carry

    @property
    def code(self) -> str:
        """For GeoBrix's own kernels (rasterio/pyproj), never for product st_* calls."""
        return f"{self.authority}:{self.srid}"


def _known(authority: str, srid: int) -> bool:
    # The authoritative PROJ registries, as resolve_crs uses them: "EPSG:102003" is not an EPSG code.
    return srid > 0 and str(srid) in _authority_codes(authority)


def _uc_srid_int(value) -> Optional[UcCrs]:
    if isinstance(value, bool):
        return None
    if (isinstance(value, int) and value < 10**9) or (
        isinstance(value, str) and _UC_DIGITS.match(value.strip())
    ):
        n = int(value)
        # An int names one code space; EPSG first, as resolve_crs does.
        for auth in ("EPSG", "ESRI"):
            if _known(auth, n):
                return UcCrs(auth, n)
    return None


def _uc_code(auth: str):
    def parse(value) -> Optional[UcCrs]:
        m = _UC_CODE.match(value.strip()) if isinstance(value, str) else None
        if m and m.group(1).upper() == auth and _known(auth, int(m.group(2))):
            return UcCrs(auth, int(m.group(2)))
        return None

    return parse


_UC_CRS_VALIDATORS = {
    "srid_int": _uc_srid_int,
    "epsg_code": _uc_code("EPSG"),
    "esri_code": _uc_code("ESRI"),
}
_UC_FORM_TEXT = {
    "srid_int": "an SRID integer",
    "epsg_code": "'EPSG:<n>'",
    "esri_code": "'ESRI:<n>'",
}


def parse_uc_crs(value, *, fn: str) -> UcCrs:
    """The restricted CRS of the UC surface (see UC_CRS_FORMS). Raises ValueError with a Fix:, never returns NULL."""
    for form in UC_CRS_FORMS:
        out = _UC_CRS_VALIDATORS[form](value)
        if out is not None:
            return out
    forms = ", ".join(_UC_FORM_TEXT[f] for f in UC_CRS_FORMS)
    shown = repr(value)
    shown = shown if len(shown) <= 60 else shown[:57] + "..."
    raise ValueError(
        f"{fn}: the UC function takes a CRS as {forms}, for a code PROJ knows; got {shown}. "
        "A full CRS definition (WKT, PROJJSON, PROJ4, URN or custom) stays outside UC. "
        f"Fix: use the session function {fn} (after databricks.labs.gbx.register(spark)) or the Python API."
    )
