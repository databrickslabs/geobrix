"""Pure-Python TIN engine for the pyvx light tier (Serverless-safe).

scipy Delaunay + Sloan constraint recovery (constrained, no Steiner points),
Z-snap to breaklines, barycentric Z interpolation, and grid generators.
Heavy parity target: vectorx.jts.InterpolateElevation (mode="constrained").
"""

from typing import List, Sequence

import numpy as np
from scipy.spatial import Delaunay, QhullError


def _merge_vertices(pts: np.ndarray, tol: float) -> np.ndarray:
    """Drop coincident XY vertices, keeping the first occurrence.

    With ``tol > 0`` near-coincident vertices (within tol) snap to one
    representative; with ``tol <= 0`` only EXACT duplicates (rounded at machine
    eps) are dropped so coincident breakline/mass-point coords don't double-site
    the Delaunay input. Keep-first preserves the Z of whichever row comes first.
    """
    if len(pts) == 0:
        return pts
    scale = tol if tol > 0.0 else 1e-12
    keys = np.round(pts[:, :2] / scale).astype(np.int64)
    _, idx = np.unique(keys, axis=0, return_index=True)
    return pts[np.sort(idx)]


def triangulate(
    points: np.ndarray,
    breaklines: Sequence[np.ndarray],
    merge_tolerance: float,
    snap_tolerance: float,
) -> List[np.ndarray]:
    """Constrained Delaunay over XYZ points. Returns a list of (3,3) XYZ triangles.

    breaklines: sequence of (N,2|3) constraint polylines whose segments are forced
    as triangle edges (Sloan recovery). Empty -> plain Delaunay.

    Every breakline coordinate is added as a triangulation site (matching heavy
    ``JTSConformingDelaunayTriangulationBuilder.create`` -> ``createVertices``), so
    a breakline endpoint that isn't a mass point still becomes a vertex and the
    constraint can be enforced. Breaklines are prepended (sites-first) so the
    keep-first dedup retains breakline Z on coincident coordinates.
    """
    pts = np.asarray(points, dtype=float)
    if breaklines:
        bl_coords = []
        for bl in breaklines:
            b = np.asarray(bl, dtype=float)
            if len(b) == 0:
                continue  # empty LineString -> no constraint coords, skip
            if b.shape[1] < 3:
                b = np.column_stack([b[:, 0], b[:, 1], np.zeros(len(b))])
            bl_coords.append(b[:, :3])
        pts = np.vstack(
            bl_coords + [pts]
        )  # breaklines first -> keep-first keeps breakline Z
    pts = _merge_vertices(pts, merge_tolerance)
    if len(pts) < 3:
        return []
    try:
        tri = Delaunay(pts[:, :2])
    except QhullError:
        # Collinear/degenerate input ("Initial simplex is flat"): no 2D simplex
        # exists. Heavy returns gracefully (no triangles); match it.
        return []
    simplices = tri.simplices.copy()
    if breaklines:
        simplices = _recover_constraints(pts[:, :2], simplices, tri, breaklines)
    z = pts[:, 2]
    out = [np.column_stack([pts[s, 0], pts[s, 1], z[s]]) for s in simplices]
    if snap_tolerance > 0.0 and breaklines:
        out = _zsnap(out, breaklines, snap_tolerance)
    return out


def _orient2d(a, b, c) -> float:
    """>0 if a->b->c is counter-clockwise."""
    return (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])


def _segments_intersect(p1, p2, p3, p4) -> bool:
    d1 = _orient2d(p3, p4, p1)
    d2 = _orient2d(p3, p4, p2)
    d3 = _orient2d(p1, p2, p3)
    d4 = _orient2d(p1, p2, p4)
    return ((d1 > 0) != (d2 > 0)) and ((d3 > 0) != (d4 > 0))


def _vertex_index(xy: np.ndarray, p, tol=1e-9) -> int:
    d = np.hypot(xy[:, 0] - p[0], xy[:, 1] - p[1])
    i = int(np.argmin(d))
    return i if d[i] <= max(tol, 1e-9) else -1


def _recover_constraints(xy: np.ndarray, simplices: np.ndarray, tri, breaklines):
    """Sloan recovery: force each breakline segment to be a triangle edge via
    convex-quad edge flips. Bounded flip budget; raises on non-termination."""
    triangles = [list(s) for s in simplices.tolist()]

    def edges_of(t):
        return [
            frozenset((t[0], t[1])),
            frozenset((t[1], t[2])),
            frozenset((t[2], t[0])),
        ]

    def build_adj():
        adj = {}
        for ti, t in enumerate(triangles):
            for e in edges_of(t):
                adj.setdefault(e, []).append(ti)
        return adj

    for bl in breaklines:
        seg_pts = np.asarray(bl, dtype=float)
        for k in range(len(seg_pts) - 1):
            ia = _vertex_index(xy, seg_pts[k])
            ib = _vertex_index(xy, seg_pts[k + 1])
            if ia < 0 or ib < 0 or ia == ib:
                continue
            target = frozenset((ia, ib))
            budget = 50 * len(triangles) + 100
            while budget > 0:
                budget -= 1
                # Build adjacency once per iteration (membership test + flip scan
                # both read it), not twice.
                adj = build_adj()
                if target in adj:
                    break  # constraint already an edge
                flipped = False
                for e, ts in adj.items():
                    if len(ts) != 2:
                        continue
                    u, v = tuple(e)
                    if not _segments_intersect(xy[ia], xy[ib], xy[u], xy[v]):
                        continue
                    t0, t1 = triangles[ts[0]], triangles[ts[1]]
                    w0 = next(x for x in t0 if x not in e)
                    w1 = next(x for x in t1 if x not in e)
                    if not _segments_intersect(xy[w0], xy[w1], xy[u], xy[v]):
                        continue  # quad not convex -> flip would be invalid
                    # Strict-progress guard (guarantees termination): only flip when
                    # the NEW diagonal (w0,w1) no longer crosses the constraint, so
                    # each flip strictly reduces the crossing-edge count. Without
                    # this the new edge can re-cross and oscillate back next pass.
                    new_edge = frozenset((w0, w1))
                    if new_edge != target and _segments_intersect(
                        xy[ia], xy[ib], xy[w0], xy[w1]
                    ):
                        continue
                    triangles[ts[0]] = [w0, w1, u]
                    triangles[ts[1]] = [w0, w1, v]
                    flipped = True
                    break
                if not flipped:
                    # A full scan found NO convex-flippable intersecting edge: a
                    # genuine stall (recovering this segment would need a Steiner
                    # point). Leave it -- the parity posture accepts "where
                    # recoverable" -- and move on to the next segment.
                    break
            if budget <= 0:
                raise RuntimeError("Sloan constraint recovery did not terminate")
    return np.array(triangles, dtype=np.int64)


def grid_bbox(xmin, ymin, xmax, ymax, width_px, height_px):
    """Yield (x, y) cell centers, column-major (matches heavy pointGridBBox)."""
    xres = (xmax - xmin) / width_px
    yres = (ymax - ymin) / height_px
    for i in range(int(width_px)):
        for j in range(int(height_px)):
            yield (xmin + (i + 0.5) * xres, ymin + (j + 0.5) * yres)


def grid_geom(origin_x, origin_y, cols, rows, cell_x, cell_y):
    """Yield (x, y) cell centers from origin + cell sizes (matches pointGridOrigin).
    cell_y may be negative (y-down)."""
    for i in range(int(cols)):
        for j in range(int(rows)):
            yield (origin_x + (i + 0.5) * cell_x, origin_y + (j + 0.5) * cell_y)


def interpolate_z(triangles, x, y):
    """Barycentric Z at (x,y) within the TIN. None if outside all triangles."""
    p = np.array([x, y])
    # Relative tolerance on the normalized barycentric coords: invariant to coord
    # magnitude, so a point dead-on an edge at BNG/UTM scales (~1e5-1e6) is NOT
    # spuriously dropped by an absolute epsilon that vanishes against the area.
    eps = 1e-9
    for t in triangles:
        a, b, c = t[0, :2], t[1, :2], t[2, :2]
        d = _orient2d(a, b, c)
        if d == 0.0:
            continue
        l1 = _orient2d(p, b, c) / d
        l2 = _orient2d(a, p, c) / d
        l3 = 1.0 - l1 - l2
        if l1 >= -eps and l2 >= -eps and l3 >= -eps:
            z = l1 * t[0, 2] + l2 * t[1, 2] + l3 * t[2, 2]
            return None if np.isnan(z) else float(z)
    return None


def _zsnap(triangles, breaklines, tol):
    """Overwrite vertex Z by linear interpolation along any constraint line
    within tol (mirrors heavy LengthIndexedLine post-process)."""
    out = []
    for t in triangles:
        t = t.copy()
        for vi in range(3):
            p = t[vi]
            for bl in breaklines:
                bl = np.asarray(bl, dtype=float)
                if bl.shape[1] < 3:
                    continue
                for k in range(len(bl) - 1):
                    a, b = bl[k], bl[k + 1]
                    ab = b[:2] - a[:2]
                    L2 = float(ab @ ab)
                    if L2 == 0.0:
                        continue
                    s = float((p[:2] - a[:2]) @ ab) / L2
                    if 0.0 <= s <= 1.0:
                        proj = a[:2] + s * ab
                        if np.hypot(*(p[:2] - proj)) <= tol:
                            t[vi, 2] = a[2] + s * (b[2] - a[2])
                    else:
                        # Endpoint-cap region: a vertex just beyond an endpoint
                        # (s<0 or s>1) is missed by the clamped projection but is
                        # caught by heavy's circular buffer (point.buffer(tol)).
                        # Snap to whichever endpoint is within tol.
                        if np.hypot(*(p[:2] - a[:2])) <= tol:
                            t[vi, 2] = a[2]
                        elif np.hypot(*(p[:2] - b[:2])) <= tol:
                            t[vi, 2] = b[2]
        out.append(t)
    return out


# --- the rows of the TIN functions: the one core of the session UDTFs and the Unity Catalog routines ---------------------
def geoms_from_array(arr):
    """Decode an ARRAY<BINARY|STRING> of geometries via the shared parse_geom contract (WKB/EWKB/WKT/EWKT); a NULL or
    empty element is dropped."""
    from ._geom import parse_geom

    out = []
    for g in arr or []:
        geom = parse_geom(g)
        if geom is not None:
            out.append(geom)
    return out


def all_xyz(geom):
    """Recursively flatten any geometry to a list of (x, y, z) coordinate tuples.

    Handles multi-part geometries (MULTIPOINT/MULTILINESTRING/etc. expose ``.geoms``, not ``.coords``) and pads 2D
    coordinates with z=0.0. Used to extract triangulation/elevation sites from the points array, so a single MULTIPOINT
    element doesn't crash on a missing ``.coords``.
    """
    if hasattr(geom, "geoms"):
        return [c for g in geom.geoms for c in all_xyz(g)]
    return [(c[0], c[1], c[2] if len(c) == 3 else 0.0) for c in geom.coords]


def validate_mode(mode):
    """``'constrained'`` (the default) or raise: ``'conforming'`` is JVM-only, any other word is a bad argument."""
    m = (mode or "constrained").lower()
    if m == "conforming":
        raise NotImplementedError(
            "mode='conforming' (Steiner-point conforming Delaunay) is heavy-only; "
            "use the heavyweight vectorx tier, or mode='constrained' in light."
        )
    if m != "constrained":
        raise ValueError(f"mode must be 'constrained' or 'conforming'; got {mode!r}")
    return m


def sites(points, breaklines):
    """``(coords, breaklines)`` as arrays from the two geometry arrays: the (x, y, z) of every point (None when there is
    none) and one coordinate array per break line."""
    pt_geoms = geoms_from_array(points)
    if not pt_geoms:
        return None, []
    coords = np.array([xyz for g in pt_geoms for xyz in all_xyz(g)], dtype=float)
    return coords, [
        np.array(g.coords, dtype=float) for g in geoms_from_array(breaklines)
    ]


def triangle_wkbs(points, breaklines, merge_tolerance, snap_tolerance, parsed=None):
    """The 2D WKB polygon of every triangle of the constrained Delaunay triangulation, one at a time. ``parsed`` is the
    ``(coords, break lines)`` of :func:`sites` when the caller has them already."""
    from shapely import to_wkb
    from shapely.geometry import Polygon

    coords, bls = parsed or sites(points, breaklines)
    if coords is None:
        return
    for t in triangulate(coords, bls, float(merge_tolerance), float(snap_tolerance)):
        yield to_wkb(Polygon([(p[0], p[1]) for p in t]))


def elevation_wkbs(
    points,
    breaklines,
    merge_tolerance,
    snap_tolerance,
    cell_iter,
    srid,
    flavor="extended",
    parsed=None,
):
    """The WKB point (with Z) at the centre of every grid cell inside the surface, in the order of ``cell_iter``; a cell
    outside the hull gives none. ``flavor="iso"`` writes type 1001 for a point with Z (the form the warehouse parses).
    """
    from shapely import set_srid, to_wkb
    from shapely.geometry import Point

    coords, bls = parsed or sites(points, breaklines)
    if coords is None:
        return
    tris = triangulate(coords, bls, float(merge_tolerance), float(snap_tolerance))
    for x, y in cell_iter:
        z = interpolate_z(tris, x, y)
        if z is None:
            continue
        p = Point(x, y, z)
        if srid:
            p = set_srid(p, int(srid))
        yield to_wkb(p, output_dimension=3, flavor=flavor)
