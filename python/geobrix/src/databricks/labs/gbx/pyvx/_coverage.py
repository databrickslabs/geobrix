"""Polygon-coverage validity ops (distributed shapely, light tier).

A *coverage* is a set of polygons that partition a region with shared edges
(admin boundaries, parcels). Coverage validity is a set-level question that
single-geometry validity cannot answer (overlaps / gaps / misaligned edges).
Two SQL grouped-aggregates + one Python-API DataFrame helper. WKB in/out,
SRID preserved. See docs/docs/api/coverage-validity.mdx.
"""

from typing import Iterable, List, Optional, Tuple

from shapely import (
    MultiLineString,
    coverage_invalid_edges,
    coverage_is_valid,
    coverage_simplify,
    get_srid,
    set_srid,
    to_wkb,
    union_all,
)

from ._geom import parse_geom

_PARSE_ERR = (
    "coverage: every geometry must parse to a polygon "
    "(a coverage cannot contain null, empty, or unparseable members)"
)


def _finish(g, srid: int) -> bytes:
    """Re-stamp original SRID and emit EWKB (SRID preserved, no reprojection)."""
    if srid:
        g = set_srid(g, srid)
    return to_wkb(g, include_srid=True)


def _parse_group(geoms: Iterable) -> Tuple[List, int]:
    """Parse an iterable of geometry inputs (WKB/EWKB/WKT/EWKT) → (geoms, srid).

    Drops None-returning inputs (SQL NULL / empty); a malformed geometry raises
    ValueError — a coverage must not contain corrupt members.
    SRID taken from the first parseable geom.
    """
    parsed: List = []
    srid = 0
    for gb in geoms:
        try:
            g = parse_geom(gb)
        except Exception as exc:
            raise ValueError(_PARSE_ERR) from exc
        if g is not None:
            if not parsed:
                srid = get_srid(g)
            parsed.append(g)
    return parsed, srid


def coverage_is_valid_agg(geoms: Iterable, gap_width: float = 0.0) -> Optional[bool]:
    """Is this set of polygons a valid coverage? (overlaps invalid; gaps thinner
    than gap_width flagged). None on an empty/degenerate group."""
    parsed, _ = _parse_group(geoms)
    if len(parsed) < 1:
        return None
    return bool(coverage_is_valid(parsed, gap_width=float(gap_width)))


def coverage_invalid_edges_agg(
    geoms: Iterable, gap_width: float = 0.0
) -> Optional[bytes]:
    """The coverage's invalid edges unioned into one geometry (EWKB, SRID-preserved).
    Empty geometry when the coverage is clean; None on an empty group."""
    parsed, srid = _parse_group(geoms)
    if len(parsed) < 1:
        return None
    edges = coverage_invalid_edges(parsed, gap_width=float(gap_width))
    non_empty = [e for e in edges if e is not None and not e.is_empty]
    merged = union_all(non_empty) if non_empty else MultiLineString([])
    return _finish(merged, srid)


def coverage_simplify_pdf(pdf, geom_col, tolerance, simplify_boundary, out_col):
    """applyInPandas group body: simplify a whole coverage (topology-preserving),
    writing EWKB into out_col. N rows in → N rows out; all columns preserved."""
    geoms = []
    for gb in pdf[geom_col].tolist():
        try:
            g = parse_geom(gb)
        except Exception as exc:
            raise ValueError(_PARSE_ERR) from exc
        geoms.append(g)
    if any(g is None for g in geoms):
        raise ValueError(_PARSE_ERR)
    srid = get_srid(geoms[0]) if geoms else 0
    simp = coverage_simplify(
        geoms, float(tolerance), simplify_boundary=bool(simplify_boundary)
    )
    out = pdf.copy()
    out[out_col] = [_finish(g, srid) for g in simp]
    return out
