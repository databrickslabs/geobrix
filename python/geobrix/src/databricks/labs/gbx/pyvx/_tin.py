"""Pure-Python TIN engine for the pyvx light tier (Serverless-safe).

The shared pre-step (:func:`prepare_sites`: the Z of 2D coordinates, the merge of ``merge_tolerance`` and the snap of
``snap_tolerance``), then either mode: ``constrained`` (JTS's Delaunay triangulation, from scipy where it is certified
unique, + Sloan constraint recovery, no Steiner points) or ``conforming`` (JTS 1.20's conforming Delaunay,
``_conforming``); then barycentric Z interpolation and the grid generators.
Heavy parity target: vectorx.jts.InterpolateElevation (both modes).
"""

import math
from collections import deque
from typing import List, Sequence

import numpy as np

from ._jmath import hypot

MODES = ("constrained", "conforming")


_I64_MAX = 2**63 - 1

# The message of a constrained triangulation that cannot make a break line segment an edge; the JVM level raises the
# same text (InterpolateElevation.scala).
CONSTRAINED_CROSSING = (
    "mode 'constrained' could not make every break line segment a triangle edge: break lines cross, or a line "
    "crosses itself. Fix: split the break lines where they cross"
)


# The message of a constrained triangulation whose point location does not converge (JTS's LocateFailureException,
# seen with sites very close together); the JVM level raises the same text.
CONSTRAINED_LOCATE_FAILED = (
    "mode 'constrained' could not triangulate the sites: locating a point did not converge, as with sites very "
    "close together. Fix: merge close sites with a merge_tolerance above 0"
)


class ConstrainedError(ValueError):
    """The constrained triangulation gave up: a break line segment cannot be made an edge (``CONSTRAINED_CROSSING``),
    or locating a site did not converge (``CONSTRAINED_LOCATE_FAILED``)."""


def _unique_xy(pts: np.ndarray) -> np.ndarray:
    """``pts`` less each row whose X and Y an earlier row has (-0.0 is 0.0): the site list of the constrained mode,
    where a vertex two break lines share comes twice. A no-op on anything else, as :func:`prepare_sites` merged it.
    """
    if len(pts) == 0:
        return pts
    _, idx = np.unique(pts[:, :2] + 0.0, axis=0, return_index=True)
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


def _jlong_floor(q: float) -> int:
    """Java's ``Math.floor(q).toLong``: NaN is 0, out of range saturates."""
    if q != q:
        return 0
    if q >= 9.223372036854775807e18:
        return _I64_MAX
    if q <= -9.223372036854775808e18:
        return -_I64_MAX - 1
    return int(math.floor(q))


class _SiteIndex:
    """The coordinates kept so far, to find the one a new coordinate merges into: the nearest within ``tol`` (Java's
    ``Math.hypot``), the lesser (x, y) on an exact tie, as JTS's ``KdTree`` picks it (``_conforming.KdTree``); with
    ``tol <= 0`` the one with the same X and Y. A grid of ``tol`` cells finds the same coordinate as the tree.
    """

    def __init__(self, tol: float):
        self.tol = tol
        self.cells = {}
        self.exact = {}

    def _cell(self, x, y):
        return _jlong_floor(x / self.tol), _jlong_floor(y / self.tol)

    def add(self, c):
        if self.tol > 0.0:
            self.cells.setdefault(self._cell(c[0], c[1]), []).append(c)
        else:
            self.exact[(c[0] + 0.0, c[1] + 0.0)] = c

    def match(self, c):
        """The kept coordinate ``c`` merges into, or None."""
        if not self.tol > 0.0:
            return self.exact.get((c[0] + 0.0, c[1] + 0.0))
        kx, ky = self._cell(c[0], c[1])
        tol = self.tol
        best, best_d = None, 0.0
        for i in range(-2, 3):
            for j in range(-2, 3):
                for v in self.cells.get((kx + i, ky + j), ()):
                    d = hypot(c[0] - v[0], c[1] - v[1])
                    if d <= tol and (
                        best is None
                        or d < best_d
                        or (d == best_d and (v[0], v[1]) < (best[0], best[1]))
                    ):
                        best, best_d = v, d
        return best


def _merge(points: np.ndarray, lines, tol: float):
    """``(mass points, break lines)`` after the one merge of ``merge_tolerance`` (step 2 of :func:`prepare_sites`)."""
    index = _SiteIndex(tol)
    out, lone = [], []
    for b in lines:
        row, new = [], []
        for c in b.tolist():
            m = index.match(c)
            if m is None:
                index.add(c)
                new.append(c)
                m = c
            if not row or row[-1] is not m:
                row.append(m)
        if len(row) >= 2:
            out.append(np.array(row, dtype=float))
        else:
            lone.extend(
                new
            )  # a line merged into one coordinate: a point, if it is no other line's
    kept = []
    for c in points.tolist():
        if index.match(c) is None:
            index.add(c)
            kept.append(c)
    return np.array(lone + kept, dtype=float).reshape(-1, 3), out


def prepare_sites(points, breaklines, merge_tolerance, snap_tolerance):
    """``(mass points, break lines)``: the sites of both modes, as an (N, 3) array and a list of (M, 3) arrays. This is
    the one statement of the steps; the JVM level (``InterpolateElevation.prepareSites``) does the same operations in
    the same order, so the two agree bit for bit:

    1. Z: a coordinate without Z (a 2D point or break line, or a NaN Z) takes Z 0, in both modes.
    2. Merge, once, whether or not anything snaps: the break line vertices (line by line, along each line), then the
       mass points, each in order, join the nearest coordinate kept before them within ``merge_tolerance``, or are
       kept. It is JTS's ``KdTree`` rule: the first kept wins, the nearest on a choice, the lesser (x, y) on an exact
       tie; with ``merge_tolerance`` 0 only the same X and Y merge. So a mass point near a line vertex merges into
       the vertex, the line's coordinate and Z winning. A line vertex that merges takes the kept coordinate's place
       in its line (a repeat in a row is dropped); a line left with one coordinate is a mass point.
    3. Snap, when ``snap_tolerance`` > 0 and a break line has a segment:

       a. each mass point, in order, finds the break line segments within ``snap_tolerance`` (the segment's envelope
          grown by the tolerance holds the point, then the 2D point-to-segment distance, JTS's formula with Java's
          ``Math.hypot``, is at most the tolerance). The nearest wins; on an exact tie the first line in input order,
          then the first segment along it;
       b. the point moves to its projection on that segment, with the segment's Z interpolated linearly there (the
          projection factor clamped to the segment, so a point past an end goes to the end);
       c. the projected point is inserted into the line as a new vertex between the segment's ends, ordered along the
          segment, so it becomes a constraint vertex and an edge end: no zero-area triangle. A projection within
          ``merge_tolerance`` of a break line vertex already there (an end, an interior vertex or a point snapped
          before it) merges into that vertex instead, by the rule of step 2;
       d. a snapped point leaves the mass points;
       e. a mass point left within ``merge_tolerance`` of a vertex inserted by the snap merges into it, the line's
          coordinate winning, as in step 2 (only possible where ``merge_tolerance`` is above ``snap_tolerance``).
    4. Then the mode triangulates the mass points left and the break lines.
    """
    tol = float(merge_tolerance)
    pts, lines = _merge(
        _xyz(points), [_xyz(b) for b in breaklines if np.asarray(b).size], tol
    )
    if not (snap_tolerance > 0.0) or not lines or not len(pts):
        return pts, lines
    snap = float(snap_tolerance)
    segs = []  # (a, b, line, index along the line), in input order
    for li, b in enumerate(lines):
        cs = [tuple(r) for r in b.tolist()]
        segs.extend((cs[k], cs[k + 1], li, k) for k in range(len(cs) - 1))
    import shapely

    A = np.array([s[0] for s in segs], dtype=float)
    B = np.array([s[1] for s in segs], dtype=float)
    minx, maxx = np.minimum(A[:, 0], B[:, 0]), np.maximum(A[:, 0], B[:, 0])
    miny, maxy = np.minimum(A[:, 1], B[:, 1]), np.maximum(A[:, 1], B[:, 1])
    reach = 2.0 * snap  # the query only finds candidates: the test below decides
    tree = shapely.STRtree(shapely.box(minx, miny, maxx, maxy))
    qi, si = tree.query(
        shapely.box(
            pts[:, 0] - reach, pts[:, 1] - reach, pts[:, 0] + reach, pts[:, 1] + reach
        )
    )
    px, py = pts[qi, 0], pts[qi, 1]
    ok = (
        (minx[si] - snap <= px)
        & (px <= maxx[si] + snap)
        & (miny[si] - snap <= py)
        & (py <= maxy[si] + snap)
    )
    qi, si = qi[ok], si[ok]
    order = np.lexsort((si, qi))
    qi, si = qi[order].tolist(), si[order].tolist()

    vertices = _SiteIndex(tol)
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
            if d <= snap and (best < 0 or d < best_d):
                best, best_d = si[k], d
            k += 1
        if best < 0:
            continue
        snapped[i] = True
        r, q = _project(p, segs[best][0], segs[best][1])
        if vertices.match(q) is None:
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
    added = _SiteIndex(tol)
    for q in (q for ins in inserted for _, q in ins):
        added.add(q)
    keep = ~snapped
    if any(inserted):
        keep &= np.array([added.match(c) is None for c in rows], dtype=bool)
    return pts[keep], out


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
    return _triangulate_constrained(pts, lines)


def _triangulate_constrained(pts, lines) -> List[np.ndarray]:
    """Constrained Delaunay over the merged sites of :func:`prepare_sites` (no second merge): every break line
    coordinate is a site (breaklines first), the Delaunay triangulation the JVM level's ``DelaunayTriangulationBuilder``
    makes (:func:`_delaunay`), then Sloan recovery makes each break line segment an edge, or raises
    :class:`ConstrainedError`."""
    if lines:
        pts = _unique_xy(np.vstack(lines + [pts]))
    if len(pts) < 3:
        return []
    simplices = _delaunay(pts[:, :2])
    if lines and len(simplices):
        where = {(x + 0.0, y + 0.0): i for i, (x, y) in enumerate(pts[:, :2].tolist())}
        segments = [
            (where[(a[0] + 0.0, a[1] + 0.0)], where[(b[0] + 0.0, b[1] + 0.0)])
            for ln in lines
            for a, b in zip(ln.tolist(), ln.tolist()[1:])
        ]
        simplices = _recover_constraints(pts[:, :2], simplices, segments)
    return list(pts[:, :3][simplices])


def _delaunay(xy: np.ndarray) -> np.ndarray:
    """The triangles (site index triples) of the JVM level's ``DelaunayTriangulationBuilder`` over ``xy``, the same
    hybrid as the conforming mode: scipy's (Qhull's, run from the sites' corner, where it keeps close points far from
    0) when they are certified to be the unique Delaunay triangulation (``_conforming.unique_delaunay``), which is so
    in general position; otherwise (co-circular sites, such as a regular grid, or sites Qhull leaves out) the port of
    JTS's incremental triangulation (:func:`_jts_delaunay`), which makes JTS's own choice among the ties. Where this
    process has the Rust kernel, JTS's triangulation comes from it on every input: it is faster than scipy.
    """
    from ._conforming import _rust, unique_delaunay

    lo, hi = xy.min(axis=0), xy.max(axis=0)
    env = (float(lo[0]), float(lo[1]), float(hi[0]), float(hi[1]))
    s = unique_delaunay(xy, origin=lo) if _rust() is None else None
    return _jts_delaunay(xy, env) if s is None else s


def _jts_delaunay(xy: np.ndarray, env) -> np.ndarray:
    """The triangles (site index triples) the JVM level's ``DelaunayTriangulationBuilder`` makes of ``xy``: the sites
    sorted by (x, y), inserted into JTS's incremental triangulation framed on their envelope ``env``, tolerance 0 (the
    Rust kernel, or the ``_quadedge`` port without a Rust build). Raises :class:`ConstrainedError`
    (``CONSTRAINED_LOCATE_FAILED``) where JTS's point location fails, as the JVM level does.
    """
    from ._conforming import ConformingError, jts_delaunay

    order = np.lexsort((xy[:, 1], xy[:, 0]))
    try:
        tris = jts_delaunay(xy[order], env, 0.0)
    except ConformingError:
        raise ConstrainedError(CONSTRAINED_LOCATE_FAILED) from None
    return order[tris]


def _orient2d(a, b, c) -> float:
    """>0 if a->b->c is counter-clockwise."""
    return (b[0] - a[0]) * (c[1] - a[1]) - (b[1] - a[1]) * (c[0] - a[0])


def _crosses(p1, p2, p3, p4) -> bool:
    """True iff the segments p1-p2 and p3-p4 cross at one point inside both (no end on the other's line)."""
    d1 = _orient2d(p3, p4, p1)
    d2 = _orient2d(p3, p4, p2)
    d3 = _orient2d(p1, p2, p3)
    d4 = _orient2d(p1, p2, p4)
    return ((d1 > 0.0 and d2 < 0.0) or (d1 < 0.0 and d2 > 0.0)) and (
        (d3 > 0.0 and d4 < 0.0) or (d3 < 0.0 and d4 > 0.0)
    )


def _ek(i: int, j: int):
    return (i, j) if i < j else (j, i)


def _recover_constraints(xy: np.ndarray, simplices: np.ndarray, segments):
    """Sloan's recovery (1993): each segment ``(i, j)`` (site indices), in order, becomes a triangle edge.

    The edges the segment crosses, found by walking from its first end, go in a queue; each is flipped when its
    quadrilateral is convex and goes back to the queue's end when not, or when its new diagonal still crosses. A site
    exactly on the segment splits it there, so the edge chain through the site is the segment. A segment that crosses
    one made an edge before it (crossing break lines) raises :class:`ConstrainedError`. The JVM level
    (``InterpolateElevation.recoverConstraints``) does the same steps in the same order.
    """
    rec = _Recovery(xy, simplices)
    for ia, ib in segments:
        rec.enforce(ia, ib)
    return rec.tris


class _EdgeTris:
    """Edge ``(i, j)``, ``i < j`` -> the list of the triangles on it, the lower index first. An edge's list is read
    from the initial triangles when first used, so only the edges the recovery touches cost Python objects; a flip
    reads every edge whose triangles it changes before changing them."""

    def __init__(self, simplices: np.ndarray, n: int):
        a, b = simplices, np.roll(
            simplices, -1, axis=1
        )  # edge k of triangle t: t[k] -> t[k + 1]
        lo, hi = np.minimum(a, b).ravel(), np.maximum(a, b).ravel()
        self.n = n
        key = lo * n + hi
        order = np.argsort(
            key, kind="stable"
        )  # the triangles of an edge stay in index order
        self.keys = key[order]
        self.tris = (order // 3).astype(np.int64)
        self.over = {}
        self.dead = set()

    def _base(self, e):
        k = e[0] * self.n + e[1]
        i, j = np.searchsorted(self.keys, [k, k + 1])
        return self.tris[i:j].tolist() if j > i else None

    def __contains__(self, e) -> bool:
        if e in self.over:
            return True
        return e not in self.dead and self._base(e) is not None

    def __getitem__(self, e):
        v = self.over.get(e)
        if v is None:
            v = None if e in self.dead else self._base(e)
            if v is None:
                raise KeyError(e)
            self.over[e] = v
        return v

    def __setitem__(self, e, v):
        self.over[e] = v
        self.dead.discard(e)

    def __delitem__(self, e):
        if e not in self:
            raise KeyError(e)
        self.over.pop(e, None)
        self.dead.add(e)


class _VertTris:
    """Site -> the set of the triangles around it, made (in index order) from the initial triangles when first used;
    a flip reads the four sets it changes before changing them."""

    def __init__(self, simplices: np.ndarray):
        v = simplices.ravel()
        order = np.argsort(v, kind="stable")
        self.keys = v[order]
        self.tris = (order // 3).astype(np.int64)
        self.over = {}

    def get(self, v, default=None):
        s = self.over.get(v)
        if s is None:
            i, j = np.searchsorted(self.keys, [v, v + 1])
            if j == i:
                return default
            s = self.over[v] = set(self.tris[i:j].tolist())
        return s

    def __getitem__(self, v):
        s = self.get(v)
        if s is None:
            raise KeyError(v)
        return s


class _Recovery:
    """The triangles of :func:`_recover_constraints`, their edge and vertex maps, and the edges made constraints."""

    def __init__(self, xy, simplices):
        self.xy = xy
        self.tris = np.array(simplices, dtype=np.int64).reshape(-1, 3)
        self.edge_tris = _EdgeTris(self.tris, len(xy))
        self.vert_tris = _VertTris(self.tris)
        self.fixed = set()

    def _third(self, ti, u, v):
        return next(w for w in self.tris[ti].tolist() if w != u and w != v)

    def enforce(self, ia, ib):
        """Make ``ia``-``ib`` an edge, or a chain of edges through the sites on it."""
        a = ia
        while a != ib:
            if _ek(a, ib) in self.edge_tris:
                self.fixed.add(_ek(a, ib))
                return
            r, crossed = self._walk(a, ib)
            end = ib if r is None else r
            self._sloan(a, end, crossed)
            self.fixed.add(_ek(a, end))
            a = end

    def _start(self, ia, ib):
        """``(site, start)``: a site strictly inside a-b next to a, or the triangle around a whose far edge a-b
        crosses, as ``(triangle, p, q)``."""
        xy = self.xy
        A, B = xy[ia], xy[ib]
        dx, dy = B[0] - A[0], B[1] - A[1]
        reach = dx * dx + dy * dy
        start = None
        for ti in self.vert_tris.get(ia, ()):
            p, q = [w for w in self.tris[ti].tolist() if w != ia]
            for w in (p, q):
                P = xy[w]
                if _orient2d(A, B, P) == 0.0:
                    along = (P[0] - A[0]) * dx + (P[1] - A[1]) * dy
                    if 0.0 < along < reach:
                        return w, None
            if _crosses(A, B, xy[p], xy[q]):
                start = (ti, p, q)
        if start is None:
            raise ConstrainedError(CONSTRAINED_CROSSING)
        return None, start

    def _walk(self, ia, ib):
        """``(site, crossed)``: the edges a-b crosses from a, in order, up to b (site None) or to the first site
        strictly inside a-b."""
        site, start = self._start(ia, ib)
        if site is not None:
            return site, []
        xy = self.xy
        A, B = xy[ia], xy[ib]
        ti, p, q = start
        crossed = []
        for _ in range(len(self.tris) + 1):
            e = _ek(p, q)
            crossed.append(e)
            ts = self.edge_tris[e]
            if len(ts) != 2:
                raise ConstrainedError(CONSTRAINED_CROSSING)
            tj = ts[1] if ts[0] == ti else ts[0]
            r = self._third(tj, p, q)
            if r == ib:
                return None, crossed
            o = _orient2d(A, B, xy[r])
            if o == 0.0:
                return r, crossed
            if (o > 0.0) == (_orient2d(A, B, xy[p]) > 0.0):
                p = r
            else:
                q = r
            ti = tj
        raise ConstrainedError(CONSTRAINED_CROSSING)

    def _flip(self, u, v, w0, w1, t0, t1):
        """Replace the edge u-v of triangles t0 (u, v, w0) and t1 (u, v, w1) with w0-w1."""
        et, vt = self.edge_tris, self.vert_tris
        del et[(u, v)]
        self.tris[t0] = [w0, w1, u]
        self.tris[t1] = [w0, w1, v]
        e = et[_ek(v, w0)]
        e[e.index(t0)] = t1
        e = et[_ek(u, w1)]
        e[e.index(t1)] = t0
        et[_ek(w0, w1)] = [t0, t1]
        vt[v].discard(t0)
        vt[w1].add(t0)
        vt[u].discard(t1)
        vt[w0].add(t1)

    def _sloan(self, ia, ib, crossed):
        if any(e in self.fixed for e in crossed):
            raise ConstrainedError(CONSTRAINED_CROSSING)
        xy = self.xy
        A, B = xy[ia], xy[ib]
        queue = deque(crossed)
        budget = 10 * (len(crossed) + 1) ** 2 + 100
        while queue:
            budget -= 1
            if budget < 0:
                raise ConstrainedError(CONSTRAINED_CROSSING)
            u, v = queue.popleft()
            t0, t1 = self.edge_tris[(u, v)]
            w0, w1 = self._third(t0, u, v), self._third(t1, u, v)
            if not _crosses(xy[w0], xy[w1], xy[u], xy[v]):
                queue.append((u, v))  # not convex: try again after the others
                continue
            self._flip(u, v, w0, w1, t0, t1)
            if _crosses(A, B, xy[w0], xy[w1]):
                queue.append(_ek(w0, w1))


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


_EPS_BARY = 1e-9  # interpolate_z's tolerance on the barycentric coordinates
_CHUNK = 65536  # grid cells per batch


def interpolate_zs(tris: np.ndarray, xs: np.ndarray, ys: np.ndarray) -> np.ndarray:
    """:func:`interpolate_z` for many points at once: ``tris`` is an (M, 3, 3) array, the result the Z at each
    (xs[i], ys[i]), NaN where it gives None. The same float operations in the same order, and the same triangle (the
    first in ``tris`` that holds the point); an index of the triangles only skips the ones too far to hold it.
    """
    import shapely

    xs, ys = np.asarray(xs, dtype=float), np.asarray(ys, dtype=float)
    out = np.full(len(xs), np.nan)
    if not len(tris) or not len(xs):
        return out
    A, B, C = tris[:, 0, :2], tris[:, 1, :2], tris[:, 2, :2]
    d = (B[:, 0] - A[:, 0]) * (C[:, 1] - A[:, 1]) - (B[:, 1] - A[:, 1]) * (
        C[:, 0] - A[:, 0]
    )
    lo, hi = tris[:, :, :2].min(axis=1), tris[:, :, :2].max(axis=1)
    # A point the test takes is within twice a triangle's size of it, even where rounding flips the sign of a
    # sliver's area (barycentric coordinates in [-1, 0] then pass); the margin keeps every such triangle a candidate.
    m = 2.0 * (hi - lo).max(axis=1)
    tree = shapely.STRtree(
        shapely.box(lo[:, 0] - m, lo[:, 1] - m, hi[:, 0] + m, hi[:, 1] + m)
    )
    for s in range(0, len(xs), _CHUNK):
        px, py = xs[s : s + _CHUNK], ys[s : s + _CHUNK]
        pi, ti = tree.query(shapely.points(px, py))
        x, y = px[pi], py[pi]
        a0, a1, b0, b1, c0, c1 = (
            A[ti, 0],
            A[ti, 1],
            B[ti, 0],
            B[ti, 1],
            C[ti, 0],
            C[ti, 1],
        )
        dt = d[ti]
        with np.errstate(divide="ignore", invalid="ignore"):
            l1 = ((b0 - x) * (c1 - y) - (b1 - y) * (c0 - x)) / dt
            l2 = ((x - a0) * (c1 - a1) - (y - a1) * (c0 - a0)) / dt
            l3 = 1.0 - l1 - l2
        ok = (dt != 0.0) & (l1 >= -_EPS_BARY) & (l2 >= -_EPS_BARY) & (l3 >= -_EPS_BARY)
        pi, ti, l1, l2, l3 = pi[ok], ti[ok], l1[ok], l2[ok], l3[ok]
        first = np.lexsort((ti, pi))  # per point, the first triangle in list order
        keep = np.ones(len(first), dtype=bool)
        keep[1:] = pi[first][1:] != pi[first][:-1]
        k = first[keep]
        t = tris[ti[k]]
        out[s + pi[k]] = l1[k] * t[:, 0, 2] + l2[k] * t[:, 1, 2] + l3[k] * t[:, 2, 2]
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
    import shapely

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
    for s in range(0, len(tris), _CHUNK):
        part = np.asarray(tris[s : s + _CHUNK])[:, :, :2]
        yield from shapely.to_wkb(shapely.polygons(part)).tolist()


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
    import itertools

    import shapely

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
    tri_arr = np.asarray(tris, dtype=float).reshape(-1, 3, 3)
    cells = iter(cell_iter)
    while True:
        xy = np.array(list(itertools.islice(cells, _CHUNK)), dtype=float).reshape(-1, 2)
        if not len(xy):
            return
        z = interpolate_zs(tri_arr, xy[:, 0], xy[:, 1])
        hit = ~np.isnan(z)
        pts = shapely.points(xy[hit, 0], xy[hit, 1], z[hit])
        if srid:
            pts = shapely.set_srid(pts, int(srid))
        yield from shapely.to_wkb(pts, output_dimension=3, flavor=flavor).tolist()
