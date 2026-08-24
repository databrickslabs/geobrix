"""Geometry validity ops (distributed shapely, light tier).

Single-geometry OGC-SFS validity: repair (``st_makevalid``) and diagnose
(``explain_validity``). WKB in/out, CRS preserved (never reprojected). See
docs/docs/api/geometry-validity-antimeridian.mdx.
"""

import json
import re
from typing import Optional

from shapely import get_srid, is_valid_reason, make_valid, set_srid, to_wkb

from ._geom import parse_geom

_LEVELS = ("structure", "linework")

# GEOS reason prefix (SFS violation category) -> stable GeoBrix code.
# structural 1-9, self-intersection 10-19, ring-continuity 20-29; unmapped -> None.
_REASON_CODE = {
    "Valid Geometry": 0,
    "Too few points in geometry component": 1,
    "Invalid coordinate": 2,
    "Ring not closed": 3,
    "Self-intersection": 10,
    "Ring Self-intersection": 11,
    "Hole lies outside shell": 20,
    "Holes are nested": 21,
    "Interior is disconnected": 22,
    "Nested shells": 23,
    "Duplicate rings": 24,
}

_LOC_RE = re.compile(r"\[\s*([-\d.eE+]+)\s+([-\d.eE+]+)\s*\]")


def _finish(g, srid: int) -> bytes:
    """Re-stamp original SRID and emit EWKB (SRID preserved, no reprojection)."""
    if srid:
        g = set_srid(g, srid)
    return to_wkb(g, include_srid=True)


def st_makevalid(geom, level="linework") -> Optional[bytes]:
    """Repair a geometry to OGC-SFS validity (shapely make_valid). BINARY out."""
    g = parse_geom(geom)
    if g is None:
        return None
    lv = (level or "linework").lower()
    if lv not in _LEVELS:
        raise ValueError(
            f"gbx_st_makevalid: level must be one of {_LEVELS} "
            f"('full'/'cleaning' arrive in a later release); got {level!r}"
        )
    srid = get_srid(g)
    return _finish(make_valid(g, method=lv, keep_collapsed=True), srid)


def explain_validity(geom) -> Optional[str]:
    """Diagnose SFS validity: JSON {valid, reason, code, location}. STRING out.

    ``valid`` + ``reason`` are always present (GEOS never fails to produce a
    reason string); ``code`` (reason-prefix -> stable code) and ``location``
    (GEOS embeds ``Reason[x y]``) are best-effort and ``null`` when GEOS omits
    them. Nothing throws.
    """
    g = parse_geom(geom)
    if g is None:
        return None
    reason = is_valid_reason(g)  # "Valid Geometry" or "<Reason>[x y]"
    prefix = reason.split("[", 1)[0].strip()
    m = _LOC_RE.search(reason)
    location = f"POINT({m.group(1)} {m.group(2)})" if m else None
    return json.dumps(
        {
            "valid": reason == "Valid Geometry",
            "reason": reason,
            "code": _REASON_CODE.get(prefix),
            "location": location,
        }
    )


def _udf_st_makevalid(geom, level=None) -> Optional[bytes]:
    """SQL UDF callable for gbx_st_makevalid (BINARY return)."""
    return st_makevalid(geom, level if level is not None else "linework")


def _udf_st_explainvalidity(geom) -> Optional[str]:
    """SQL UDF callable for gbx_st_explainvalidity (STRING/JSON return)."""
    return explain_validity(geom)
