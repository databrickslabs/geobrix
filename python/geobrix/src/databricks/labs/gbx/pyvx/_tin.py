"""Pure-Python TIN engine for the pyvx light tier (Serverless-safe).

The shared pre-step (:func:`prepare_sites`: the Z of 2D coordinates and the snap of ``snap_tolerance``), then either
mode: ``constrained`` (scipy Delaunay + Sloan constraint recovery, no Steiner points) or ``conforming`` (JTS 1.20's
conforming Delaunay, ``_conforming``); then barycentric Z interpolation and the grid generators.
Heavy parity target: vectorx.jts.InterpolateElevation (both modes).
"""

import math
from typing import List, Sequence

import numpy as np
from scipy.spatial import Delaunay, QhullError

from ._jmath import hypot

MODES = ("constrained", "conforming")


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


def _xyz(arr) -> np.ndarray:
    """``arr`` as an (N, 3) float array whose missing Z (a 2D coordinate, or NaN) is 0."""
    a = np.asarray(arr, dtype=float)
    if a.size == 0:
        return np.zeros((0, 3))
    a = a.reshape(len(a), -1)
    if a.shape[1] < 3:
        a = np.column_stack([a[:, 0], a[:, 1], np.zeros(len(a))])
    else:
        a = a[:, :3].copy()
    a[np.isnan(a[:, 2]), 2] = 0.0
    return a


def _point_segment_distance(px, py, ax, ay, bx, by) -> float:
    """``org.locationtech.jts.algorithm.Distance.pointToSegment``, operation for operation (the JVM's
    ``InterpolateElevation.pointToSegment``)."""
    if ax == bx and ay == by:
        return hypot(px - ax, py - ay)
    len2 = (bx - ax) * (bx - ax) + (by - ay) * (by - ay)
    r = ((px - ax) * (bx - ax) + (py - ay) * (by - ay)) / len2
    if r <= 0.0:
        return hypot(px - ax, py - ay)
    if r >= 1.0:
        return hypot(px - bx, py - by)
    s = ((ay - py) * (bx - ax) - (ax - px) * (by - ay)) / len2
    return abs(s) * math.sqrt(len2)


def _project(p, a, b):
    """``(r, point)``: the point of segment a-b nearest p, with the segment's Z interpolated there, and its projection
    factor ``r`` (``InterpolateElevation.project``)."""
    if a[0] == b[0] and a[1] == b[1]:
        return 0.0, a
    len2 = (b[0] - a[0]) * (b[0] - a[0]) + (b[1] - a[1]) * (b[1] - a[1])
    r = ((p[0] - a[0]) * (b[0] - a[0]) + (p[1] - a[1]) * (b[1] - a[1])) / len2
    if r <= 0.0:
        return 0.0, a
    if r >= 1.0:
        return 1.0, b
    return r, (
        a[0] + r * (b[0] - a[0]),
        a[1] + r * (b[1] - a[1]),
        a[2] + r * (b[2] - a[2]),
    )


class _VertexIndex:
    """The break line vertices, to ask whether a point is within ``tol`` of one (``tol <= 0``: the same X and Y)."""

    def __init__(self, tol: float):
        self.tol = tol
        self.cells = {}
        self.exact = set()

    def _cell(self, x, y):
        return math.floor(x / self.tol), math.floor(y / self.tol)

    def add(self, c):
        if self.tol > 0.0:
            self.cells.setdefault(self._cell(c[0], c[1]), []).append(c)
        else:
            self.exact.add((c[0] + 0.0, c[1] + 0.0))

    def near(self, c) -> bool:
        if not self.tol > 0.0:
            return (c[0] + 0.0, c[1] + 0.0) in self.exact
        kx, ky = self._cell(c[0], c[1])
        tol = self.tol
        for i in range(-2, 3):
            for j in range(-2, 3):
                for v in self.cells.get((kx + i, ky + j), ()):
                    if hypot(c[0] - v[0], c[1] - v[1]) <= tol:
                        return True
        return False


def prepare_sites(points, breaklines, merge_tolerance, snap_tolerance):
    """``(mass points, break lines)``: the sites of both modes, as an (N, 3) array and a list of (M, 3) arrays. This is
    the one statement of the steps; the JVM level (``InterpolateElevation.prepareSites``) does the same operations in
    the same order, so the two agree bit for bit:

    1. Z: a coordinate without Z (a 2D point or break line, or a NaN Z) takes Z 0, in both modes.
    2. When ``snap_tolerance`` > 0 and a break line has a segment:

       a. the mass points are merged as the constrained mode merges sites (``_merge_vertices``, keep the first);
       b. each mass point, in order, finds the break line segments within ``snap_tolerance`` (the segment's envelope
          grown by the tolerance holds the point, then the 2D point-to-segment distance, JTS's formula with Java's
          ``Math.hypot``, is at most the tolerance). The nearest wins; on an exact tie the first line in input order,
          then the first segment along it;
       c. the point moves to its projection on that segment, with the segment's Z interpolated linearly there (the
          projection factor clamped to the segment, so a point past an end goes to the end);
       d. the projected point is inserted into the line as a new vertex between the segment's ends, ordered along the
          segment, so it becomes a constraint vertex and an edge end: no zero-area triangle. A projection within
          ``merge_tolerance`` of a break line vertex already there (an end, an interior vertex or a point snapped
          before it) merges into that vertex instead; with ``merge_tolerance`` 0 only the same X and Y merges;
       e. a snapped point leaves the mass points.
    3. Then the mode triangulates the mass points left and the break lines.
    """
    pts = _xyz(points)
    lines = [_xyz(b) for b in breaklines if np.asarray(b).size]
    if not (snap_tolerance > 0.0) or not any(len(b) >= 2 for b in lines):
        return pts, lines
    tol = float(snap_tolerance)
    pts = _merge_vertices(pts, merge_tolerance)
    segs = []  # (a, b, line, index along the line), in input order
    for li, b in enumerate(lines):
        cs = [tuple(r) for r in b.tolist()]
        segs.extend((cs[k], cs[k + 1], li, k) for k in range(len(cs) - 1))
    import shapely

    A = np.array([s[0] for s in segs], dtype=float)
    B = np.array([s[1] for s in segs], dtype=float)
    minx, maxx = np.minimum(A[:, 0], B[:, 0]), np.maximum(A[:, 0], B[:, 0])
    miny, maxy = np.minimum(A[:, 1], B[:, 1]), np.maximum(A[:, 1], B[:, 1])
    reach = 2.0 * tol  # the query only finds candidates: the test below decides
    tree = shapely.STRtree(shapely.box(minx, miny, maxx, maxy))
    qi, si = tree.query(
        shapely.box(
            pts[:, 0] - reach, pts[:, 1] - reach, pts[:, 0] + reach, pts[:, 1] + reach
        )
    )
    px, py = pts[qi, 0], pts[qi, 1]
    ok = (
        (minx[si] - tol <= px)
        & (px <= maxx[si] + tol)
        & (miny[si] - tol <= py)
        & (py <= maxy[si] + tol)
    )
    qi, si = qi[ok], si[ok]
    order = np.lexsort((si, qi))
    qi, si = qi[order].tolist(), si[order].tolist()

    vertices = _VertexIndex(float(merge_tolerance))
    for b in lines:
        for c in b.tolist():
            vertices.add(c)
    inserted = [[] for _ in segs]
    snapped = np.zeros(len(pts), dtype=bool)
    rows = pts.tolist()
    k = 0
    while k < len(qi):
        i = qi[k]
        p = rows[i]
        best, best_d = -1, 0.0
        while k < len(qi) and qi[k] == i:
            a, b = segs[si[k]][0], segs[si[k]][1]
            d = _point_segment_distance(p[0], p[1], a[0], a[1], b[0], b[1])
            if d <= tol and (best < 0 or d < best_d):
                best, best_d = si[k], d
            k += 1
        if best < 0:
            continue
        snapped[i] = True
        r, q = _project(p, segs[best][0], segs[best][1])
        if not vertices.near(q):
            vertices.add(q)
            inserted[best].append((r, q))
    out, s = [], 0
    for b in lines:
        cs = b.tolist()
        row = []
        for j, c in enumerate(cs):
            row.append(c)
            if j < len(cs) - 1:
                row.extend(q for _, q in sorted(inserted[s], key=lambda t: t[0]))
                s += 1
        out.append(np.array(row, dtype=float).reshape(-1, 3))
    return pts[~snapped], out


def validate_finder(split_point_finder) -> str:
    """``'NONENCROACHING'`` or ``'MIDPOINT'`` (any case), upper-cased, or raise, as the JVM level does in both modes."""
    from ._conforming import FINDERS

    f = str(split_point_finder).upper()
    if f not in FINDERS:
        raise ValueError(
            f"split_point_finder must be 'NONENCROACHING' or 'MIDPOINT'; got {split_point_finder!r}"
        )
    return f


def triangulate(
    points: np.ndarray,
    breaklines: Sequence[np.ndarray],
    merge_tolerance: float,
    snap_tolerance: float,
    mode: str = "constrained",
    split_point_finder: str = "NONENCROACHING",
) -> List[np.ndarray]:
    """The triangulation of the XYZ points and break lines, as a list of (3, 3) XYZ triangles: the shared pre-step
    (:func:`prepare_sites`), then the mode. ``breaklines`` are (N, 2|3) polylines; empty -> plain Delaunay.
    """
    m = validate_mode(mode)
    finder = validate_finder(split_point_finder)
    pts, lines = prepare_sites(points, breaklines, merge_tolerance, snap_tolerance)
    if m == "conforming":
        from . import _conforming

        return _conforming.triangulate(pts, lines, float(merge_tolerance), finder)
    return _triangulate_constrained(pts, lines, float(merge_tolerance))


def _triangulate_constrained(pts, lines, merge_tolerance) -> List[np.ndarray]:
    """Constrained Delaunay: every break line coordinate is a site (breaklines first, so the keep-first merge keeps
    their Z on a coincident coordinate, as heavy ``triangulateConstrained``), scipy's Delaunay, then Sloan recovery
    forces each break line segment to be an edge where it can."""
    if lines:
        pts = np.vstack(lines + [pts])
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
    if lines:
        simplices = _recover_constraints(pts[:, :2], simplices, tri, lines)
    z = pts[:, 2]
    return [np.column_stack([pts[s, 0], pts[s, 1], z[s]]) for s in simplices]


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
    """``'constrained'`` (the default) or ``'conforming'``, lower-cased, or raise for any other word."""
    m = (mode or "constrained").lower()
    if m not in MODES:
        raise ValueError(f"mode must be 'constrained' or 'conforming'; got {mode!r}")
    return m


def _linear_parts(geom):
    """The line strings (and polygon rings) of a geometry, in order, as JTS's ``LinearComponentExtracter`` finds them."""
    if hasattr(geom, "geoms"):
        return [p for g in geom.geoms for p in _linear_parts(g)]
    if hasattr(geom, "exterior"):
        return [geom.exterior, *geom.interiors] if not geom.is_empty else []
    return [geom] if hasattr(geom, "coords") and geom.geom_type != "Point" else []


def sites(points, breaklines):
    """``(coords, breaklines)`` as arrays from the two geometry arrays: the (x, y, z) of every point (None when there is
    none) and one coordinate array per break line (a multi-line element gives one per part).
    """
    pt_geoms = geoms_from_array(points)
    if not pt_geoms:
        return None, []
    coords = np.array([xyz for g in pt_geoms for xyz in all_xyz(g)], dtype=float)
    return coords, [
        np.array(part.coords, dtype=float)
        for g in geoms_from_array(breaklines)
        for part in _linear_parts(g)
    ]


def triangle_wkbs(
    points,
    breaklines,
    merge_tolerance,
    snap_tolerance,
    mode="constrained",
    split_point_finder="NONENCROACHING",
    parsed=None,
):
    """The 2D WKB polygon of every triangle of the triangulation (:func:`triangulate`), one at a time. ``parsed`` is the
    ``(coords, break lines)`` of :func:`sites` when the caller has them already."""
    from shapely import to_wkb
    from shapely.geometry import Polygon

    coords, bls = parsed or sites(points, breaklines)
    if coords is None:
        return
    for t in triangulate(
        coords,
        bls,
        float(merge_tolerance),
        float(snap_tolerance),
        mode,
        split_point_finder,
    ):
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
    mode="constrained",
    split_point_finder="NONENCROACHING",
):
    """The WKB point (with Z) at the centre of every grid cell inside the surface, in the order of ``cell_iter``; a cell
    outside the hull gives none. ``flavor="iso"`` writes type 1001 for a point with Z (the form the warehouse parses).
    """
    from shapely import set_srid, to_wkb
    from shapely.geometry import Point

    coords, bls = parsed or sites(points, breaklines)
    if coords is None:
        return
    tris = triangulate(
        coords,
        bls,
        float(merge_tolerance),
        float(snap_tolerance),
        mode,
        split_point_finder,
    )
    for x, y in cell_iter:
        z = interpolate_z(tris, x, y)
        if z is None:
            continue
        p = Point(x, y, z)
        if srid:
            p = set_srid(p, int(srid))
        yield to_wkb(p, output_dimension=3, flavor=flavor)
