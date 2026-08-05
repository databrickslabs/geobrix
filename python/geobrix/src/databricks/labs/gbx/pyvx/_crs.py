"""VectorX CRS functions for the pyvx light tier (Serverless-safe).

Implements ``st_crs``, ``st_setcrs``, and ``st_transformcrs`` for vector
geometries — the analogue of the raster ``rst_crs`` / ``rst_setcrs`` /
``rst_transformcrs`` family in pyrx.

## Design — two layers

### Python core layer (medium-preserving)
``st_crs``, ``st_setcrs``, and ``st_transformcrs`` preserve the encoding medium
of their input:

- bytes / bytearray input (WKB, EWKB) -> bytes output (WKB, EWKB)
- str input (WKT, EWKT) -> str output (WKT, EWKT)

This lets Python callers and encoding-matrix tests operate with minimal
round-trips and without implicit WKB coercion.

### SQL/UDF layer (WKB-normalised)
The registered SQL UDFs ``gbx_st_crs`` (STRING) and ``gbx_st_setcrs`` /
``gbx_st_transformcrs`` (BINARY) follow a single fixed Spark return type,
which is the SQL-surface contract:

- ``gbx_st_crs`` returns STRING (canonical authority string, or NULL).
- ``gbx_st_setcrs`` / ``gbx_st_transformcrs`` return BINARY (WKB/EWKB bytes
  regardless of whether the input was text). SQL callers always work in WKB.

The private helpers ``_udf_st_setcrs``, ``_udf_st_transformcrs`` are the
scalar-UDF callables registered with Spark; they call the medium-preserving
core and coerce the result to the fixed SQL return type.

## CRS handling rules (mirrors raster tier, geometry-specific semantics)

- A geometry stores ONLY an integer SRID (in EWKB / EWKT).
- ``st_crs`` reads that integer and classifies it via ``resolve_crs`` /
  ``crs_to_canonical`` (authoritative PROJ rule: ESRI-range codes come back as
  ``ESRI:<n>``, not ``EPSG:<n>``).
- ``st_setcrs`` stamps an integer SRID; authority-less CRS (WKT / PROJ4 with no
  EPSG/ESRI authority) cannot be stored in an int, so it raises ``ValueError``.
- ``st_transformcrs`` reprojects coordinates.  If the source CRS is unresolvable
  (no embedded SRID, no explicit ``source_crs``, or the SRID/string is not in
  any known registry) it returns the input UNCHANGED (never-error invariant).
  Authority-less *target* CRS is allowed; it yields a plain geometry with SRID
  cleared.

Serverless-safe: no ``spark.conf.set``, no ``_jvm``, no ``.rdd``.
"""

from typing import Optional, Union

from shapely import get_srid, set_srid, to_wkb, to_wkt
from shapely.ops import transform

from databricks.labs.gbx.core.crs import (
    crs_to_canonical,
    get_transformer,
    resolve_crs,
)

from ._geom import parse_geom

# ---------------------------------------------------------------------------
# Encoding classifier
# ---------------------------------------------------------------------------


def _is_text(x) -> bool:
    """Return True if ``x`` is a str (WKT / EWKT), False for bytes/bytearray."""
    return isinstance(x, str)


# ---------------------------------------------------------------------------
# Core medium-preserving functions
# ---------------------------------------------------------------------------


def st_crs(geom) -> Optional[str]:
    """Return the canonical CRS string for a geometry's embedded SRID, or None.

    Reads the integer SRID from EWKB / EWKT; classifies it via the authoritative
    PROJ code sets (EPSG or ESRI). Returns ``None`` for plain WKB / plain WKT
    (no embedded SRID), None inputs, and any unresolvable SRID.

    Args:
        geom: WKB / EWKB bytes, or WKT / EWKT string.

    Returns:
        Canonical CRS string (``'EPSG:4326'``, ``'ESRI:54008'``, …) or ``None``.
    """
    if geom is None:
        return None
    g = parse_geom(geom)
    if g is None:
        return None
    s = int(get_srid(g))
    if s <= 0:
        return None
    try:
        return crs_to_canonical(resolve_crs(s))
    except Exception:
        return None


def st_setcrs(geom, crs: Union[int, str]) -> Union[bytes, str]:
    """Stamp a new CRS on a geometry without reprojecting (medium-preserving).

    Assigns the EPSG/ESRI SRID to the geometry.  Authority-less CRS (WKT /
    PROJ4 strings that resolve to no EPSG or ESRI authority) raise ``ValueError``
    because a geometry can only store an integer SRID.

    Encoding is preserved:
    - bytes in (WKB / EWKB) -> EWKB bytes out.
    - str in (WKT / EWKT) -> EWKT str out (``SRID=<n>;<WKT>``).

    Coordinates are preserved exactly (``rounding_precision=-1``).

    Args:
        geom: WKB / EWKB bytes, or WKT / EWKT string. None returns None.
        crs: EPSG/ESRI authority string (``'EPSG:4326'``), integer SRID, or
            any string resolvable by ``resolve_crs``. WKT / PROJ4 are rejected.

    Returns:
        Geometry with stamped SRID in the same encoding as the input.

    Raises:
        ValueError: if ``crs`` resolves to an authority-less CRS (no EPSG/ESRI
            code — cannot be stored as an integer SRID).
    """
    if geom is None:
        return None
    text = _is_text(geom)
    g = parse_geom(geom)
    if g is None:
        return None
    c = resolve_crs(crs)
    auth = c.to_authority()  # (name, code) tuple or None
    if auth is None:
        raise ValueError(
            "st_setcrs: cannot stamp an authority-less CRS (WKT / PROJ4) onto a "
            "geometry — a geometry SRID must be an EPSG or ESRI integer code. "
            f"Resolved CRS: {c.to_wkt()[:120]!r}"
        )
    code = int(auth[1])
    g = set_srid(g, code)
    if text:
        return f"SRID={code};{to_wkt(g, rounding_precision=-1)}"
    return to_wkb(g, include_srid=True)


def st_transformcrs(
    geom,
    target_crs: Union[int, str],
    source_crs: Optional[Union[int, str]] = None,
) -> Union[bytes, str]:
    """Reproject a geometry to ``target_crs`` (medium-preserving).

    Source CRS resolution order:
    1. Embedded SRID from the geometry (EWKB / EWKT).
    2. Explicit ``source_crs`` parameter (for plain WKB / WKT inputs).
    3. No source CRS resolvable -> return the input UNCHANGED (never-error
       invariant: a plain geometry without a CRS cannot be reprojected; an
       unresolvable SRID or source_crs string also degrades gracefully).

    Output encoding follows the input medium (bytes -> bytes, str -> str).
    Output SRID:
    - Authority-coded target (``EPSG:n`` / ``ESRI:n``) -> SRID ``n`` stamped
      (E-form in the input medium).
    - Authority-less target (WKT / PROJ4) -> SRID cleared (plain form).

    Coordinates are preserved exactly (``rounding_precision=-1``).

    Args:
        geom: WKB / EWKB bytes, or WKT / EWKT string.
        target_crs: target CRS as EPSG/ESRI string, int SRID, WKT, or PROJ4.
        source_crs: explicit source CRS override for plain (SRID-less) inputs.

    Returns:
        Reprojected geometry in the same encoding as the input, or the input
        unchanged when no source CRS is resolvable.
    """
    if geom is None:
        return None
    text = _is_text(geom)
    g = parse_geom(geom)
    if g is None:
        return geom

    # Resolve source CRS — never-error: any resolution failure -> return unchanged.
    embedded_srid = int(get_srid(g))
    src = None
    if embedded_srid > 0:
        try:
            src = resolve_crs(embedded_srid)
        except Exception:
            # Unresolvable embedded SRID — degrade gracefully, return unchanged.
            return geom
    elif source_crs is not None:
        try:
            src = resolve_crs(source_crs)
        except Exception:
            # Unresolvable explicit source_crs — degrade gracefully.
            return geom

    if src is None:
        # No source CRS at all (plain geometry, no explicit override).
        return geom

    tgt = resolve_crs(target_crs)
    tr = get_transformer(src, tgt)
    g_proj = transform(tr.transform, g)

    # Determine output SRID from target authority.
    tgt_auth = tgt.to_authority()  # (name, code) or None
    if tgt_auth is not None:
        tgt_srid = int(tgt_auth[1])
        g_proj = set_srid(g_proj, tgt_srid)
        if text:
            return f"SRID={tgt_srid};{to_wkt(g_proj, rounding_precision=-1)}"
        return to_wkb(g_proj, include_srid=True)
    else:
        # Authority-less target: clear SRID.
        g_proj = set_srid(g_proj, 0)
        if text:
            return to_wkt(g_proj, rounding_precision=-1)
        return to_wkb(g_proj)


# ---------------------------------------------------------------------------
# SQL/UDF layer — WKB-normalised callables for Spark registration
# ---------------------------------------------------------------------------


def _udf_st_setcrs(geom, crs) -> Optional[bytes]:
    """SQL UDF callable for gbx_st_setcrs (BINARY return type).

    Coerces the result to EWKB bytes regardless of input medium — SQL callers
    always receive WKB.  Authority-less CRS raises (per core contract).
    None geom or crs returns None.
    """
    if geom is None or crs is None:
        return None
    result = st_setcrs(geom, crs)
    if result is None:
        return None
    if isinstance(result, str):
        # Text input was processed medium-preserving -> convert to EWKB for SQL.
        # The EWKT result from st_setcrs already has the SRID embedded; parse and
        # re-encode as EWKB.
        g = parse_geom(result)
        srid = int(get_srid(g))
        return to_wkb(g, include_srid=srid > 0)
    return bytes(result)


def _udf_st_transformcrs(geom, target_crs, source_crs=None) -> Optional[bytes]:
    """SQL UDF callable for gbx_st_transformcrs (BINARY return type).

    Coerces the result to WKB/EWKB bytes regardless of input medium — SQL callers
    always receive WKB.  Returns None if geom or target_crs is None; returns
    input bytes unchanged when no source CRS is resolvable.
    """
    if geom is None or target_crs is None:
        return None
    result = st_transformcrs(geom, target_crs, source_crs)
    if isinstance(result, str):
        # Text-input round-trip: convert EWKT/WKT back to (E)WKB for SQL surface.
        g = parse_geom(result)
        if g is None:
            return None
        srid = int(get_srid(g))
        return to_wkb(g, include_srid=srid > 0)
    if result is None:
        return None
    return bytes(result)
