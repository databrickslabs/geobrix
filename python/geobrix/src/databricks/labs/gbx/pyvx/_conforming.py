"""The conforming mode of the TIN functions: JTS 1.20's ``ConformingDelaunayTriangulator``, in pure Python.

The JVM level runs ``InterpolateElevation.triangulateConforming`` -> ``JTSConformingDelaunayTriangulationBuilder``:

- the sites are the mass points sorted by (x, y), exact 2D repeats dropped (``extractUniqueCoordinates``), less every
  coordinate that is a break line vertex;
- the constraint vertices are the values of a ``java.util.HashMap`` keyed by the break line coordinates, in the map's
  own iteration order (a repeated coordinate keeps its first slot and takes the Z of its last occurrence);
- the segments are each break line's consecutive coordinate pairs, in order.

JTS inserts every site into a ``KdTree`` (``merge_tolerance`` snaps a site onto a node already there) and into the
incremental Delaunay triangulation, then the constraint vertices, then splits each segment whose diametral circle holds
a vertex (the one closest to the midpoint; the first found on a tie) until no segment is split, at most 99 passes.
Each split point takes the Z of the segment it splits (``LineZSplitPointFinder`` on the JVM).

The final triangulation is the Delaunay triangulation of the final vertex set, so only the vertex set needs the
faithful emulation. The triangles come from scipy (Qhull) when they are provably the unique Delaunay triangulation of
those vertices, which is so for points in general position; otherwise (co-circular points, such as a regular grid)
from ``_quadedge``, the port of JTS's incremental triangulation, which makes JTS's own choice among the ties.
"""

import math
import struct
from typing import List, Optional, Sequence, Tuple

import numpy as np

from ._jmath import hypot

MAX_SPLIT_ITER = 99  # ConformingDelaunayTriangulator.MAX_SPLIT_ITER
FINDERS = ("NONENCROACHING", "MIDPOINT")

# The messages of the two ways JTS gives up; the JVM level raises the same text (InterpolateElevation.scala).
TOO_MANY_SPLITS = (
    "mode 'conforming' could not fit the triangulation to the break lines: too many splitting "
    "iterations (the limit is 99). Fix: split crossing break lines where they cross, move break lines "
    "apart where they meet at a sharp angle, or use mode 'constrained'"
)
LOCATE_FAILED = (
    "mode 'conforming' could not fit the triangulation to the break lines: locating a point did not "
    "converge. Fix: split crossing break lines where they cross, or use mode 'constrained'"
)


class ConformingError(ValueError):
    """The conforming triangulation gave up, as JTS does (``ConstraintEnforcementException`` or
    ``LocateFailureException``); the message ends with a Fix."""


Coord = Tuple[float, float, float]
_D = struct.Struct("<d")
_Q = struct.Struct("<Q")


# ----------------------------------------------------------------------------------- java.util.HashMap iteration order
def _java_double_hash(x: float) -> int:
    """``Double.hashCode``: the two words of ``doubleToLongBits`` XOR-ed (NaN canonical)."""
    bits = 0x7FF8000000000000 if math.isnan(x) else _Q.unpack(_D.pack(x))[0]
    return (bits ^ (bits >> 32)) & 0xFFFFFFFF


def _java_coord_hash(x: float, y: float) -> int:
    """``org.locationtech.jts.geom.Coordinate.hashCode`` as an unsigned 32-bit int."""
    r = (37 * 17 + _java_double_hash(x)) & 0xFFFFFFFF
    return (37 * r + _java_double_hash(y)) & 0xFFFFFFFF


def hashmap_order(keys: Sequence[Tuple[float, float]]) -> List[int]:
    """The indices of ``keys`` (distinct, put in this order) in the iteration order of a ``java.util.HashMap`` (default
    capacity 16, load factor 0.75): by bucket at the final capacity, then by insertion (a resize keeps the order of a
    bucket's nodes). As in ``putVal``, a bucket that reaches 9 nodes below capacity 64 doubles the table
    (``treeifyBin``), and then a size over the threshold doubles it again.

    Not modelled: a tree bin (9 nodes in one bucket at capacity 64 or more), whose iteration order is not its insertion
    order. Its order here is the insertion order, so the constraint vertices could then enter the KdTree in another
    order than on the JVM. That changes the result only where the order matters: an exact distance tie, a
    ``merge_tolerance`` snap between constraint vertices, or co-circular points. Java's own notes put a bucket of 8 at
    6e-8 of buckets for well-spread hashes; the keys are the break line vertices, rarely more than a few thousand.
    """
    spread = []
    for x, y in keys:
        h = _java_coord_hash(x, y)
        spread.append(h ^ (h >> 16))
    cap = 16
    counts = {}

    def recount(n):
        out = {}
        for hh in spread[:n]:
            bb = hh & (cap - 1)
            out[bb] = out.get(bb, 0) + 1
        return out

    for i, h in enumerate(spread):
        b = h & (cap - 1)
        counts[b] = counts.get(b, 0) + 1
        if counts[b] >= 9 and cap < 64:  # treeifyBin resizes a small table
            cap *= 2
            counts = recount(i + 1)
        if i + 1 > int(cap * 0.75):  # ++size > threshold
            cap *= 2
            counts = recount(i + 1)
    return sorted(range(len(keys)), key=lambda i: (spread[i] & (cap - 1), i))


def _key(x: float, y: float):
    """A HashMap key: Java's equal hash AND equals2D, so 0.0 and -0.0 are two keys."""
    return (x, y, math.copysign(1.0, x), math.copysign(1.0, y))


# --------------------------------------------------------------------------------------------------- the JTS KdTree
class KdTree:
    """``org.locationtech.jts.index.kdtree.KdTree``: unbalanced, X and Y levels alternating, a point within ``tol``
    of a node snaps onto it. A node is the list ``[x, y, left, right, coord]``."""

    __slots__ = ("root", "tol")

    def __init__(self, tol: float):
        self.root = None
        self.tol = tol

    def insert(self, c: Coord) -> bool:
        """Insert ``c``; ``True`` when it made a new node (``False``: it snapped onto one)."""
        x, y = c[0], c[1]
        if self.root is None:
            self.root = [x, y, None, None, c]
            return True
        tol = self.tol
        if tol > 0.0 and self._best_match(x, y) is not None:
            return False
        cur = self.root
        leaf = cur
        is_x = True
        less = True
        while cur is not None:
            dx = x - cur[0]
            dy = y - cur[1]
            if (dx == 0.0 and dy == 0.0) if tol == 0.0 else hypot(dx, dy) <= tol:
                return False
            less = (x < cur[0]) if is_x else (y < cur[1])
            leaf = cur
            cur = cur[2] if less else cur[3]
            is_x = not is_x
        node = [x, y, None, None, c]
        if less:
            leaf[2] = node
        else:
            leaf[3] = node
        return True

    def query(self, minx: float, miny: float, maxx: float, maxy: float) -> list:
        """The nodes inside the closed envelope, in JTS's (in-order, iterative) traversal order."""
        out = []
        stack = []
        cur = self.root
        is_x = True
        while True:
            if cur is not None:
                stack.append((cur, is_x))
                if (minx < cur[0]) if is_x else (miny < cur[1]):
                    cur = cur[2]
                    if cur is not None:
                        is_x = not is_x
                else:
                    cur = None
            elif stack:
                cur, is_x = stack.pop()
                cx, cy = cur[0], cur[1]
                if minx <= cx <= maxx and miny <= cy <= maxy:
                    out.append(cur)
                if (cur[0] <= maxx) if is_x else (cur[1] <= maxy):
                    cur = cur[3]
                    if cur is not None:
                        is_x = not is_x
                else:
                    cur = None
            else:
                return out

    def _best_match(self, x: float, y: float):
        """``BestMatchVisitor``: the nearest node within ``tol``, the lesser coordinate on a tie."""
        tol = self.tol
        best = None
        best_d = 0.0
        for nd in self.query(x - tol, y - tol, x + tol, y + tol):
            d = hypot(x - nd[0], y - nd[1])
            if d > tol:
                continue
            if (
                best is None
                or d < best_d
                or (d == best_d and (nd[0], nd[1]) <= (best[0], best[1]))
            ):
                best, best_d = nd, d
        return best


# ------------------------------------------------------------------------------------------------------- split points
def _split_midpoint(p0: Coord, p1: Coord, enc: Coord) -> Tuple[float, float]:
    """``MidpointSplitPointFinder``."""
    return (p0[0] + p1[0]) / 2, (p0[1] + p1[1]) / 2


def _split_nonencroaching(p0: Coord, p1: Coord, enc: Coord) -> Tuple[float, float]:
    """``NonEncroachingSplitPointFinder``: the encroaching point projected onto the segment, kept away from the
    endpoints (``SplitSegment``)."""
    x0, y0, x1, y1 = p0[0], p0[1], p1[0], p1[1]
    ex, ey = enc[0], enc[1]
    seg_len = hypot(x0 - x1, y0 - y1)
    mid_len = seg_len / 2
    # LineSegment.project: an endpoint (2D-equal) comes back as itself
    if (ex == x0 and ey == y0) or (ex == x1 and ey == y1):
        px, py = ex, ey
    else:
        dx = x1 - x0
        dy = y1 - y0
        len2 = dx * dx + dy * dy
        r = math.nan if len2 <= 0.0 else ((ex - x0) * dx + (ey - y0) * dy) / len2
        px = x0 + r * (x1 - x0)
        py = y0 + r * (y1 - y0)
    non_enc_diam = hypot(px - ex, py - ey) * 2 * 0.8
    min_len = non_enc_diam if non_enc_diam <= mid_len else mid_len
    min_frac = min_len / seg_len
    if hypot(px - x0, py - y0) < min_len:
        return x0 + min_frac * (x1 - x0), y0 + min_frac * (y1 - y0)
    if hypot(px - x1, py - y1) < min_len:
        return x1 - min_frac * (x1 - x0), y1 - min_frac * (y1 - y0)
    return px, py


_FINDERS = {"MIDPOINT": _split_midpoint, "NONENCROACHING": _split_nonencroaching}


def _split_point(finder, p0: Coord, p1: Coord, enc: Coord) -> Coord:
    """The finder's point, with the Z of the segment there (``LineZSplitPointFinder``: the same operations)."""
    sx, sy = finder(p0, p1, enc)
    dx = p1[0] - p0[0]
    dy = p1[1] - p0[1]
    r = ((sx - p0[0]) * dx + (sy - p0[1]) * dy) / (dx * dx + dy * dy)
    return (sx, sy, p0[2] + r * (p1[2] - p0[2]))


# -------------------------------------------------------------------------------------------------- the vertex set
def _area_env(coords: Sequence[Coord]):
    """``computeBoundingBox``: the envelope of the sites and constraint vertices, grown by 20% of its larger side."""
    xs = [c[0] for c in coords]
    ys = [c[1] for c in coords]
    minx, maxx, miny, maxy = min(xs), max(xs), min(ys), max(ys)
    delta = max((maxx - minx) * 0.2, (maxy - miny) * 0.2)
    return (minx - delta, miny - delta, maxx + delta, maxy + delta)


def _unique_sites(points) -> np.ndarray:
    """``extractUniqueCoordinates``: a stable sort by x then y, exact 2D repeats dropped (the first kept)."""
    pts = np.asarray(points, dtype=float).reshape(-1, 3)
    sp = pts[np.lexsort((pts[:, 1], pts[:, 0]))]
    if len(sp):
        keep = np.ones(len(sp), dtype=bool)
        keep[1:] = (sp[1:, 0] != sp[:-1, 0]) | (sp[1:, 1] != sp[:-1, 1])
        sp = sp[keep]
    return sp


def _constraints(lines):
    """``(constraint map, constraint vertices in HashMap order, segments)`` of the break lines with a segment (a line
    string has none or at least two points)."""
    cmap = {}  # key -> [slot, coordinate]
    keys: List[Tuple[float, float]] = []
    segments: List[Tuple[Coord, Coord]] = []
    for ln in lines:
        if len(ln) < 2:
            continue
        cs = [(float(r[0]), float(r[1]), float(r[2])) for r in ln]
        for c in cs:
            k = _key(c[0], c[1])
            if k in cmap:
                cmap[k][1] = c  # put() replaces the value in place: the last Z wins
            else:
                cmap[k] = [len(keys), c]
                keys.append((c[0], c[1]))
        segments.extend(zip(cs, cs[1:]))
    seg_vertices = [cmap[_key(*keys[i])][1] for i in hashmap_order(keys)]
    return cmap, seg_vertices, segments


def _encroacher(kdt: KdTree, p0: Coord, p1: Coord):
    """``findNonGabrielPoint``: the node strictly inside the segment's diametral circle closest to its midpoint (the
    first found on a tie), or None."""
    px, py, qx, qy = p0[0], p0[1], p1[0], p1[1]
    mx, my = (px + qx) / 2.0, (py + qy) / 2.0
    rad = hypot(px - mx, py - my)
    enc = None
    min_d = 0.0
    for nd in kdt.query(mx - rad, my - rad, mx + rad, my + rad):
        tx, ty = nd[0], nd[1]
        if (tx == px and ty == py) or (tx == qx and ty == qy):
            continue
        d = hypot(mx - tx, my - ty)
        if d < rad and (enc is None or d < min_d):
            enc, min_d = nd, d
    return enc


def _enforce(kdt: KdTree, segments, split, insert, stats: Optional[dict]) -> None:
    """``enforceConstraints``: split every segment with an encroaching vertex, pass after pass, at most 99."""
    count = 0
    total = 0
    while True:
        splits = 0
        new_segments = []
        removed = set()
        for si, (p0, p1) in enumerate(segments):
            enc = _encroacher(kdt, p0, p1)
            if enc is None:
                continue
            sv = _split_point(split, p0, p1, enc[4])
            insert(sv)
            new_segments.append((p0, sv))
            new_segments.append((sv, p1))
            removed.add(si)
            splits += 1
        if removed:
            segments = [s for i, s in enumerate(segments) if i not in removed]
            segments += new_segments
        total += splits
        count += 1
        if not (splits > 0 and count < MAX_SPLIT_ITER):
            break
    if stats is not None:
        stats.update(passes=count, splits=total)
    if count == MAX_SPLIT_ITER:
        raise ConformingError(TOO_MANY_SPLITS)


def conforming_vertices(
    points: np.ndarray,
    lines: Sequence[np.ndarray],
    merge_tolerance: float,
    finder: str = "NONENCROACHING",
    stats: Optional[dict] = None,
):
    """``(vertices, area envelope)``: the (x, y, z) of every vertex of the conforming triangulation, in JTS's insertion
    order, and the envelope its incremental triangulation is framed in (None without vertices). ``points`` is (N, 3)
    and ``lines`` (M, 3) arrays, as ``_tin.prepare_sites`` leaves them. Raises :class:`ConformingError` after 99
    splitting passes."""
    split = _FINDERS[finder]
    cmap, seg_vertices, segments = _constraints(lines)
    initial = [  # createSiteVertices
        (x, y, z)
        for x, y, z in _unique_sites(points).tolist()
        if _key(x, y) not in cmap
    ]
    if not initial and not seg_vertices:
        return [], None
    env = _area_env(initial + seg_vertices)

    kdt = KdTree(merge_tolerance)
    verts: List[Coord] = []

    def insert(c: Coord):
        if kdt.insert(c):
            verts.append(c)

    for c in initial:
        insert(c)
    if segments:
        for c in seg_vertices:
            insert(c)
        _enforce(kdt, segments, split, insert, stats)
    return verts, env


# --------------------------------------------------------------------------------------------- the triangles of them
_EPS = 2.0**-53
_ICC_ERR = (10.0 + 96.0 * _EPS) * _EPS  # Shewchuk's incircle error bound
_CCW_ERR = (3.0 + 16.0 * _EPS) * _EPS  # and orientation


def _incircle(A, B, C, p):
    """``(det, error bound)`` of JTS's ``isInCircleNormalized(A, B, C, p)`` for rows of triangles (any orientation:
    each is put counter-clockwise first) and points: ``det > bound`` is surely inside, ``det < -bound`` surely
    outside."""
    ori = (B[:, 0] - A[:, 0]) * (C[:, 1] - A[:, 1]) - (B[:, 1] - A[:, 1]) * (
        C[:, 0] - A[:, 0]
    )
    flip = (ori < 0)[:, None]
    B, C = np.where(flip, C, B), np.where(flip, B, C)
    adx, ady = A[:, 0] - p[:, 0], A[:, 1] - p[:, 1]
    bdx, bdy = B[:, 0] - p[:, 0], B[:, 1] - p[:, 1]
    cdx, cdy = C[:, 0] - p[:, 0], C[:, 1] - p[:, 1]
    bdxcdy, cdxbdy = bdx * cdy, cdx * bdy
    cdxady, adxcdy = cdx * ady, adx * cdy
    adxbdy, bdxady = adx * bdy, bdx * ady
    alift = adx * adx + ady * ady
    blift = bdx * bdx + bdy * bdy
    clift = cdx * cdx + cdy * cdy
    det = (
        alift * (bdxcdy - cdxbdy)
        + blift * (cdxady - adxcdy)
        + clift * (adxbdy - bdxady)
    )
    perm = (
        (np.abs(bdxcdy) + np.abs(cdxbdy)) * alift
        + (np.abs(cdxady) + np.abs(adxcdy)) * blift
        + (np.abs(adxbdy) + np.abs(bdxady)) * clift
    )
    return det, _ICC_ERR * perm


def _orient_sign(a, b, c) -> np.ndarray:
    """The sign of the orientation of each row triple a, b, c (+1 counter-clockwise), 0 where the floating-point
    determinant cannot certify it (Shewchuk's error bound)."""
    left = (b[:, 0] - a[:, 0]) * (c[:, 1] - a[:, 1])
    right = (b[:, 1] - a[:, 1]) * (c[:, 0] - a[:, 0])
    det = left - right
    sure = np.abs(det) > _CCW_ERR * (np.abs(left) + np.abs(right))
    return np.where(sure, np.sign(det), 0.0)


def _valid_triangulation(xy: np.ndarray, s: np.ndarray, nb: np.ndarray) -> bool:
    """True when the triangles ``s`` (counter-clockwise, ``nb[t, k]`` the triangle across from vertex k, -1 on the
    hull) PROVABLY tile the convex hull of ``xy`` once: every interior edge is walked in opposite directions by its
    two triangles (they lie on either side of it), and the hull edges form one cycle that turns strictly left at
    every vertex, once around. With every triangle counter-clockwise, that covers each point of the hull once.
    """
    ti, k = np.nonzero(nb >= 0)
    tj = nb[ti, k]
    # the edge of ti across from k is s[ti, k+1] -> s[ti, k+2]; its twin in tj must run the other way
    m = np.argmax(nb[tj] == ti[:, None], axis=1)
    if not np.all(nb[tj, m] == ti):
        return False
    if not (
        np.array_equal(s[ti, (k + 1) % 3], s[tj, (m + 2) % 3])
        and np.array_equal(s[ti, (k + 2) % 3], s[tj, (m + 1) % 3])
    ):
        return False
    hi, hk = np.nonzero(nb < 0)
    org, dst = s[hi, (hk + 1) % 3], s[hi, (hk + 2) % 3]  # the hull, counter-clockwise
    if len(org) < 3 or len(np.unique(org)) != len(org):
        return False
    if 2 * len(xy) - (3 * len(s) + len(org)) + 2 * len(s) != 2:  # a disk: V - E + F = 1
        return False
    nxt = np.full(len(xy), -1, dtype=np.int64)
    nxt[org] = dst
    v, n = int(org[0]), 0
    while True:
        v = int(nxt[v])
        n += 1
        if v < 0 or n > len(org):
            return False
        if v == org[0]:
            break
    if n != len(org):
        return False
    p, q, r = xy[org], xy[dst], xy[nxt[dst]]
    if np.any(_orient_sign(p, q, r) <= 0):
        return False
    u, w = q - p, r - q
    turn = np.arctan2(u[:, 0] * w[:, 1] - u[:, 1] * w[:, 0], (u * w).sum(axis=1))
    return bool(turn.sum() < 3.0 * math.pi)  # 2*pi once around; 4*pi or more twice


def unique_delaunay(xy: np.ndarray, origin=None) -> Optional[np.ndarray]:
    """scipy's (Qhull's) Delaunay simplices of ``xy`` when they are PROVABLY the unique Delaunay triangulation, else
    None: every vertex used, every triangle oriented beyond the floating-point error bound, the triangles a valid
    tiling of the convex hull (:func:`_valid_triangulation`, whose hull turns strictly at every vertex), and every
    interior edge strictly locally Delaunay beyond the error bound. The incircle expression is JTS's
    ``isInCircleNormalized``, so a certified sign is the sign JTS computes.

    JTS 1.20 builds the triangulation inside a frame triangle, but its ``forceConvex`` rules (a hull edge never flips
    towards the frame, a concave hull corner is filled) give the Delaunay triangulation of the sites themselves, even
    where a frame vertex lies in the circle of a thin hull triangle. Checked against the JVM on 735 point sets in each
    mode (such a hull triangle in 439 of them in the constrained mode, 343 in the conforming mode): the certified
    triangles were JTS's every time. The oracle rows ``tri_thin_hull_triangle_*`` pin one such input.

    ``origin`` (a point) runs Qhull on ``xy - origin``, as it keeps close points far from 0 that way; the proof is on
    ``xy`` itself, so the rounding of the shift cannot change the answer.
    """
    from scipy.spatial import Delaunay, QhullError

    try:
        tri = Delaunay(xy if origin is None else xy - origin)
    except QhullError:
        return None
    s = tri.simplices
    if len(getattr(tri, "coplanar", [])) or len(np.unique(s)) != len(xy):
        return None
    ori = _orient_sign(xy[s[:, 0]], xy[s[:, 1]], xy[s[:, 2]])
    if np.any(ori == 0.0):
        return None
    s, nb = s.copy(), tri.neighbors.copy()
    # each triangle counter-clockwise: swap vertices 1 and 2, and the neighbours across from them
    cw = ori < 0
    s[cw, 1], s[cw, 2] = s[cw, 2], s[cw, 1]
    nb[cw, 1], nb[cw, 2] = nb[cw, 2], nb[cw, 1]
    if not _valid_triangulation(xy, s, nb):
        return None
    ti, k = np.nonzero(nb >= 0)
    once = ti < nb[ti, k]  # each interior edge once
    ti, k = ti[once], k[once]
    opp = s[nb[ti, k]]
    # the vertex of ti across the shared edge must be strictly OUTSIDE the circle of its neighbour
    det, bound = _incircle(xy[opp[:, 0]], xy[opp[:, 1]], xy[opp[:, 2]], xy[s[ti, k]])
    if np.any(det > -bound):
        return None
    return s


def jts_delaunay(verts: Sequence[Coord], env, tol: float) -> np.ndarray:
    """The triangles (vertex index triples) JTS's incremental triangulation makes inserting ``verts`` in order.
    Raises :class:`ConformingError` where JTS's point location fails."""
    from ._quadedge import FRAME, LocateFailureError, Subdivision

    X = [0.0] * FRAME + [c[0] for c in verts]
    Y = [0.0] * FRAME + [c[1] for c in verts]
    sd = Subdivision(env, tol, X, Y)
    try:
        for v in range(FRAME, len(X)):
            sd.insert_site(v)
    except LocateFailureError:
        raise ConformingError(LOCATE_FAILED) from None
    return np.array(sorted(sd.triangles()), dtype=np.int64).reshape(-1, 3) - FRAME


def triangulate(
    points: np.ndarray,
    lines: Sequence[np.ndarray],
    merge_tolerance: float,
    finder: str = "NONENCROACHING",
    delaunay: str = "hybrid",
    stats: Optional[dict] = None,
) -> List[np.ndarray]:
    """The conforming triangulation as a list of (3, 3) XYZ triangles. ``delaunay="hybrid"`` (the default) takes
    scipy's triangles when they are certified unique and the JTS port otherwise; ``"jts"`` always takes the port.
    """
    verts, env = conforming_vertices(
        points, lines, float(merge_tolerance), finder, stats
    )
    if len(verts) < 3:
        return []
    va = np.array(verts, dtype=float)
    simplices = (
        unique_delaunay(va[:, :2], origin=va[:, :2].min(axis=0))
        if delaunay == "hybrid"
        else None
    )
    if stats is not None:
        stats["fallback"] = simplices is None
    if simplices is None:
        simplices = jts_delaunay(verts, env, float(merge_tolerance))
    return [va[s] for s in simplices]
