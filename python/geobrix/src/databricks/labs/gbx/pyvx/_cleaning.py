"""Geometry cleaning / normalization ops (distributed shapely, light tier).

These operate on ALREADY-VALID geometry to improve quality/precision — a
distinct capability from validity (see geometry-validity). WKB in/out, CRS
preserved. See docs/docs/api/geometry-cleaning.mdx.
"""

from typing import Optional

from shapely import (
    get_srid,
    node,
    remove_repeated_points,
    set_precision,
    set_srid,
    simplify,
    snap,
    to_wkb,
)

from ._geom import parse_geom


def _finish(g, srid: int) -> bytes:
    """Re-stamp original SRID and emit EWKB (SRID preserved, no reprojection)."""
    if srid:
        g = set_srid(g, srid)
    return to_wkb(g, include_srid=True)


def st_simplifypreservetopology(geom, tolerance) -> Optional[bytes]:
    """Douglas-Peucker simplify that preserves topology (shapely preserve_topology=True).

    The topology-preserving counterpart to the product's `st_simplify` (which can
    split or collapse a geometry). BINARY out, SRID preserved.
    """
    g = parse_geom(geom)
    if g is None:
        return None
    srid = get_srid(g)
    return _finish(simplify(g, float(tolerance), preserve_topology=True), srid)


def st_removerepeatedpoints(geom, tolerance=0.0) -> Optional[bytes]:
    """Remove consecutive duplicate (or within-tolerance near-duplicate) vertices."""
    g = parse_geom(geom)
    if g is None:
        return None
    srid = get_srid(g)
    return _finish(remove_repeated_points(g, float(tolerance)), srid)


def st_reduceprecision(geom, grid_size) -> Optional[bytes]:
    """Snap coordinates to a grid of `grid_size` (GEOS precision model; a.k.a. snap-to-grid).

    Uses shapely set_precision(mode="valid_output") so the result stays valid.
    """
    g = parse_geom(geom)
    if g is None:
        return None
    srid = get_srid(g)
    return _finish(set_precision(g, float(grid_size), mode="valid_output"), srid)


def st_node(geom) -> Optional[bytes]:
    """Node a linework: split at all self/pairwise intersections (returns noded lines)."""
    g = parse_geom(geom)
    if g is None:
        return None
    srid = get_srid(g)
    return _finish(node(g), srid)


def st_snap(geom, reference, tolerance) -> Optional[bytes]:
    """Snap `geom`'s vertices onto `reference` where within `tolerance`."""
    g = parse_geom(geom)
    ref = parse_geom(reference)
    if g is None or ref is None:
        return None
    srid = get_srid(g)
    return _finish(snap(g, ref, float(tolerance)), srid)


def _udf_st_simplifypreservetopology(geom, tolerance) -> Optional[bytes]:
    if tolerance is None:
        return None
    return st_simplifypreservetopology(geom, tolerance)


def _udf_st_removerepeatedpoints(geom, tolerance=None) -> Optional[bytes]:
    return st_removerepeatedpoints(geom, tolerance if tolerance is not None else 0.0)


def _udf_st_reduceprecision(geom, grid_size) -> Optional[bytes]:
    if grid_size is None:
        return None
    return st_reduceprecision(geom, grid_size)


def _udf_st_node(geom) -> Optional[bytes]:
    return st_node(geom)


def _udf_st_snap(geom, reference, tolerance) -> Optional[bytes]:
    if tolerance is None:
        return None
    return st_snap(geom, reference, tolerance)
