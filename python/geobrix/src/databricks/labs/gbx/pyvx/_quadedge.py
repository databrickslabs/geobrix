"""A port of JTS 1.20's incremental Delaunay triangulation, for the conforming mode's exact fallback.

``QuadEdgeSubdivision`` + ``IncrementalDelaunayTriangulator`` (``forceConvex`` on, as in JTS 1.20) +
``LastFoundQuadEdgeLocator``: the same predicates, the same walk and the same flip order, so inserting the same sites
in the same order gives the JVM's triangles, the choice among co-circular points included. It is slow (pure Python),
so ``_conforming`` uses it only when it cannot prove the Delaunay triangulation unique.

A vertex is an int indexing the ``X``/``Y`` lists; 0, 1 and 2 are the frame.
"""

import math
from fractions import Fraction

from ._jmath import hypot

FRAME = 3  # vertices 0..2 are the frame triangle


class LocateFailureError(RuntimeError):
    """JTS's ``LocateFailureException``: the walk to a point did not converge (seen with crossing break lines)."""


class _QE:
    """One quarter of a quad-edge: ``rot`` (None once deleted), ``next`` (the origin ring) and ``v`` (its origin)."""

    __slots__ = ("rot", "next", "v")


def _make_edge(o, d):
    q0, q1, q2, q3 = _QE(), _QE(), _QE(), _QE()
    q0.rot, q1.rot, q2.rot, q3.rot = q1, q2, q3, q0
    q0.next, q1.next, q2.next, q3.next = q0, q3, q2, q1
    q0.v, q2.v = o, d
    q1.v = q3.v = None
    return q0


def _splice(a, b):
    alpha = a.next.rot
    beta = b.next.rot
    t1, t2, t3, t4 = b.next, a.next, beta.next, alpha.next
    a.next, b.next, alpha.next, beta.next = t1, t2, t3, t4


def _sym(e):
    return e.rot.rot


def _oprev(e):
    return e.rot.next.rot


def _lnext(e):
    return e.rot.rot.rot.next.rot  # invRot().oNext().rot()


def _dprev(e):
    return e.rot.rot.rot.next.rot.rot.rot  # invRot().oNext().invRot()


def _lprev(e):
    return e.next.rot.rot


def _dest(e):
    return e.rot.rot.v


def _swap(e):
    a = _oprev(e)
    b = _oprev(_sym(e))
    _splice(e, a)
    _splice(_sym(e), b)
    _splice(e, _lnext(a))
    _splice(_sym(e), _lnext(b))
    e.v = _dest(a)
    _sym(e).v = _dest(b)


def _orientation_index(ax, ay, bx, by, cx, cy) -> int:
    """``CGAlgorithmsDD.orientationIndex``: JTS's floating-point filter, then the exact sign (JTS uses double-double
    there; an exact rational gives the same sign)."""
    detleft = (ax - cx) * (by - cy)
    detright = (ay - cy) * (bx - cx)
    det = detleft - detright
    if detleft > 0.0:
        if detright <= 0.0:
            return (det > 0) - (det < 0)
        detsum = detleft + detright
    elif detleft < 0.0:
        if detright >= 0.0:
            return (det > 0) - (det < 0)
        detsum = -detleft - detright
    else:
        return (det > 0) - (det < 0)
    errbound = 1e-15 * detsum
    if det >= errbound or -det >= errbound:
        return (det > 0) - (det < 0)
    d = (Fraction(bx) - Fraction(ax)) * (Fraction(cy) - Fraction(by)) - (
        Fraction(by) - Fraction(ay)
    ) * (Fraction(cx) - Fraction(bx))
    return (d > 0) - (d < 0)


class Subdivision:
    """``QuadEdgeSubdivision`` with its incremental Delaunay insertion."""

    def __init__(self, env, tol, X, Y):
        """``env`` is ``(minx, miny, maxx, maxy)``, JTS's ``computeAreaEnv``; ``X``/``Y`` hold three free slots (0..2)
        for the frame, followed by the sites."""
        minx, miny, maxx, maxy = env
        offset = max(maxx - minx, maxy - miny) * 10.0
        X[0], Y[0] = (maxx + minx) / 2.0, maxy + offset
        X[1], Y[1] = minx - offset, miny - offset
        X[2], Y[2] = maxx + offset, miny - offset
        self.X, self.Y = X, Y
        self.tol = tol
        self.edge_tol = tol / 1000.0
        self.n_edges = 0
        self.edges = (
            []
        )  # every primary edge ever made: the triangles are read from the live ones
        ea = self._new_edge(0, 1)
        eb = self._new_edge(1, 2)
        _splice(_sym(ea), eb)
        ec = self._new_edge(2, 0)
        _splice(_sym(eb), ec)
        _splice(_sym(ec), ea)
        self.last = ea

    def _new_edge(self, o, d):
        q = _make_edge(o, d)
        self.n_edges += 1
        self.edges.append(q)
        return q

    def _connect(self, a, b):
        e = self._new_edge(_dest(a), b.v)
        _splice(e, _lnext(a))
        _splice(_sym(e), b)
        return e

    def _delete(self, e):
        _splice(e, _oprev(e))
        _splice(_sym(e), _oprev(_sym(e)))
        self.n_edges -= 1
        es, er = _sym(e), e.rot
        eir = er.rot.rot
        e.rot = es.rot = er.rot = eir.rot = None

    def _ccw(self, a, b, c) -> bool:
        """``Vertex.isCCW``: the plain floating-point determinant."""
        X, Y = self.X, self.Y
        ax, ay = X[a], Y[a]
        return (X[b] - ax) * (Y[c] - ay) - (Y[b] - ay) * (X[c] - ax) > 0

    def _right_of(self, v, e) -> bool:
        return self._ccw(v, _dest(e), e.v)

    def _locate(self, v):
        """``LastFoundQuadEdgeLocator`` + ``locateFromEdge``: walk from the last edge found."""
        e = self.last
        if e.rot is None:  # LastFoundQuadEdgeLocator.init: the first live edge
            e = next(q for q in self.edges if q.rot is not None)
        X, Y = self.X, self.Y
        vx, vy = X[v], Y[v]
        it = 0
        max_it = self.n_edges
        while True:
            it += 1
            if it > max_it:
                raise LocateFailureError("Locate failed to converge")
            o, d = e.v, _dest(e)
            if (vx == X[o] and vy == Y[o]) or (vx == X[d] and vy == Y[d]):
                break
            if self._right_of(v, e):
                e = _sym(e)
            elif not self._right_of(v, e.next):
                e = e.next
            elif not self._right_of(v, _dprev(e)):
                e = _dprev(e)
            else:
                break
        self.last = e
        return e

    def _in_circle(self, p, a, b, c) -> bool:
        """``Vertex.isInCircle`` = ``TrianglePredicate.isInCircleNormalized(a, b, c, p)``."""
        X, Y = self.X, self.Y
        px, py = X[p], Y[p]
        adx, ady = X[a] - px, Y[a] - py
        bdx, bdy = X[b] - px, Y[b] - py
        cdx, cdy = X[c] - px, Y[c] - py
        abdet = adx * bdy - bdx * ady
        bcdet = bdx * cdy - cdx * bdy
        cadet = cdx * ady - adx * cdy
        alift = adx * adx + ady * ady
        blift = bdx * bdx + bdy * bdy
        clift = cdx * cdx + cdy * cdy
        return alift * bcdet + blift * cadet + clift * abdet > 0

    def _is_concave_at_origin(self, e) -> bool:
        X, Y = self.X, self.Y
        o = e.v
        p = _dest(_oprev(e))
        q = _dest(e.next)
        return _orientation_index(X[p], Y[p], X[q], Y[q], X[o], Y[o]) == 1

    def _is_concave_boundary(self, e) -> bool:
        if _dest(e) < FRAME:
            return self._is_concave_at_origin(e)
        if e.v < FRAME:
            return self._is_concave_at_origin(_sym(e))
        return False

    @staticmethod
    def _between_frame_and_inserted(e, v) -> bool:
        adj1 = _dest(e.next)
        adj2 = _dest(_oprev(e))
        return (adj1 == v and adj2 < FRAME) or (adj2 == v and adj1 < FRAME)

    def _on_edge(self, e, v) -> bool:
        """``isOnEdge``: ``LineSegment.distance(p) < tol / 1000``."""
        X, Y = self.X, self.Y
        ax, ay, bx, by = X[e.v], Y[e.v], X[_dest(e)], Y[_dest(e)]
        px, py = X[v], Y[v]
        if ax == bx and ay == by:
            return hypot(px - ax, py - ay) < self.edge_tol
        len2 = (bx - ax) * (bx - ax) + (by - ay) * (by - ay)
        r = ((px - ax) * (bx - ax) + (py - ay) * (by - ay)) / len2
        if r <= 0.0:
            d = hypot(px - ax, py - ay)
        elif r >= 1.0:
            d = hypot(px - bx, py - by)
        else:
            s = ((ay - py) * (bx - ax) - (ax - px) * (by - ay)) / len2
            d = abs(s) * math.sqrt(len2)
        return d < self.edge_tol

    def insert_site(self, v):
        """``IncrementalDelaunayTriangulator.insertSite``."""
        e = self._locate(v)
        X, Y = self.X, self.Y
        if self.tol > 0.0:
            o, d = e.v, _dest(e)
            if (
                hypot(X[v] - X[o], Y[v] - Y[o]) < self.tol
                or hypot(X[v] - X[d], Y[v] - Y[d]) < self.tol
            ):
                return e
            if self._on_edge(e, v):
                e = _oprev(e)
                self._delete(e.next)
        base = self._new_edge(e.v, v)
        _splice(base, e)
        start = base
        while True:
            base = self._connect(e, _sym(base))
            e = _oprev(base)
            if _lnext(e) is start:
                break
        while True:
            t = _oprev(e)
            td = _dest(t)
            flip = self._right_of(td, e) and self._in_circle(v, e.v, td, _dest(e))
            if self._is_concave_boundary(e):
                flip = True
            elif self._between_frame_and_inserted(e, v):
                flip = False
            if flip:
                _swap(e)
                e = _oprev(e)
            elif e.next is start:
                return base
            else:
                e = _lprev(e.next)

    def triangles(self):
        """Every triangle without a frame vertex, as a sorted vertex triple (``getTriangles(false)``)."""
        out = set()
        for q in self.edges:
            if q.rot is None:
                continue
            for e in (q, _sym(q)):
                e1 = _lnext(e)
                e2 = _lnext(e1)
                if _lnext(e2) is not e:
                    continue
                a, b, c = e.v, e1.v, e2.v
                if a < FRAME or b < FRAME or c < FRAME:
                    continue
                out.add(tuple(sorted((a, b, c))))
        return out
