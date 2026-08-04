"""CRS string/int resolution — the one place the SRID resolution rule lives (light tier).

An integer SRID is classified against the AUTHORITATIVE PROJ code registries, not the
lenient ``CRS.from_epsg`` (which succeeds for ESRI codes and mislabels them ``EPSG``).
Rule: ``n`` in the EPSG code set -> ``EPSG:<n>``; else in the ESRI set -> ``ESRI:<n>``;
else raise (a code in neither registry is invalid). The code sets come from PROJ's
``proj.db`` via ``pyproj.database.get_codes`` and are built once + cached.
"""

from functools import lru_cache
from typing import Optional, Union

from rasterio.crs import CRS

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
