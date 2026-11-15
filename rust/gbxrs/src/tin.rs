//! The TIN kernels: a port of JTS 1.20's incremental Delaunay triangulation and of the KdTree its conforming
//! triangulation snaps and splits with. They are the same algorithms as the pure-Python ports `pyvx/_quadedge.py`
//! and `pyvx/_conforming.py` (`KdTree`), step for step and with the same floating-point operations in the same order,
//! so inserting the same sites in the same order gives the JVM's triangles, the choice among co-circular points
//! included.
//!
//! `QuadEdgeSubdivision` + `IncrementalDelaunayTriangulator` (`forceConvex` on, as in JTS 1.20) +
//! `LastFoundQuadEdgeLocator`. A quad-edge is four quarter edges `4q..4q+3` (`rot` is the next one round), each with
//! its `next` (the origin ring); a primary quarter (`r` = 0 or 2) has a vertex, an index into `x`/`y`, where 0, 1 and
//! 2 are the frame. A deleted quad-edge is no longer live, as JTS's `rot` set to null.

use crate::jmath::hypot;

/// Vertices 0..2 are the frame triangle.
pub const FRAME: u32 = 3;
const NONE: u32 = u32::MAX;

/// JTS's `LocateFailureException`: the walk to a point did not converge (seen with crossing break lines).
#[derive(Debug, PartialEq, Eq)]
pub struct LocateFailure;

#[inline]
fn rot(e: u32) -> u32 {
    (e & !3) | ((e + 1) & 3)
}

#[inline]
fn sym(e: u32) -> u32 {
    (e & !3) | ((e + 2) & 3)
}

#[inline]
fn inv_rot(e: u32) -> u32 {
    (e & !3) | ((e + 3) & 3)
}

#[inline]
fn sign(d: f64) -> i32 {
    (d > 0.0) as i32 - (d < 0.0) as i32
}

/// `a + b` as the exact pair (sum, error).
#[inline]
fn two_sum(a: f64, b: f64) -> (f64, f64) {
    let x = a + b;
    let bv = x - a;
    let av = x - bv;
    (x, (a - av) + (b - bv))
}

/// `a - b` as the exact pair (difference, error), smaller first.
#[inline]
fn two_diff(a: f64, b: f64) -> [f64; 2] {
    let x = a - b;
    let bv = a - x;
    let av = x + bv;
    [(a - av) + (bv - b), x]
}

/// `a * b` as the exact pair, smaller first (the error from a fused multiply-add, which rounds once).
#[inline]
fn two_product(a: f64, b: f64) -> [f64; 2] {
    let p = a * b;
    [a.mul_add(b, -p), p]
}

/// The exact sign of `(bx - ax) * (cy - by) - (by - ay) * (cx - bx)`: Shewchuk's expansion arithmetic, every term
/// grown into one non-overlapping expansion whose largest component has the sign of the sum.
fn exact_orientation_sign(ax: f64, ay: f64, bx: f64, by: f64, cx: f64, cy: f64) -> i32 {
    let (dx1, dy2, dy1, dx2) = (
        two_diff(bx, ax),
        two_diff(cy, by),
        two_diff(by, ay),
        two_diff(cx, bx),
    );
    let mut terms = [0.0f64; 16];
    let mut n = 0;
    for (u, v, s) in [(&dx1, &dy2, 1.0), (&dy1, &dx2, -1.0)] {
        for a in u {
            for b in v {
                for p in two_product(*a, *b) {
                    terms[n] = s * p;
                    n += 1;
                }
            }
        }
    }
    let mut e: Vec<f64> = Vec::with_capacity(17);
    for t in terms {
        // GROW-EXPANSION: add one double to a non-overlapping expansion (increasing magnitude), exactly
        let mut q = t;
        for c in e.iter_mut() {
            let (s, h) = two_sum(q, *c);
            *c = h;
            q = s;
        }
        e.push(q);
        e.retain(|c| *c != 0.0);
    }
    e.last().map_or(0, |c| sign(*c))
}

/// `CGAlgorithmsDD.orientationIndex`: JTS's floating-point filter, then the exact sign (JTS uses double-double
/// there; the exact sign is the Python port's, an exact rational).
pub fn orientation_index(ax: f64, ay: f64, bx: f64, by: f64, cx: f64, cy: f64) -> i32 {
    let detleft = (ax - cx) * (by - cy);
    let detright = (ay - cy) * (bx - cx);
    let det = detleft - detright;
    let detsum = if detleft > 0.0 {
        if detright <= 0.0 {
            return sign(det);
        }
        detleft + detright
    } else if detleft < 0.0 {
        if detright >= 0.0 {
            return sign(det);
        }
        -detleft - detright
    } else {
        return sign(det);
    };
    let errbound = 1e-15 * detsum;
    if det >= errbound || -det >= errbound {
        return sign(det);
    }
    exact_orientation_sign(ax, ay, bx, by, cx, cy)
}

/// `QuadEdgeSubdivision` with its incremental Delaunay insertion.
pub struct Subdivision {
    x: Vec<f64>,
    y: Vec<f64>,
    next: Vec<u32>,
    vert: Vec<u32>,
    live: Vec<bool>,
    /// no quad-edge below it is live (deletion only ever removes, and new quad-edges go at the end)
    first_live: usize,
    n_edges: usize,
    tol: f64,
    edge_tol: f64,
    last: u32,
}

impl Subdivision {
    /// `env` is `(minx, miny, maxx, maxy)`, JTS's `computeAreaEnv`; the sites are vertices 3.. in `xs`/`ys` order.
    pub fn new(env: (f64, f64, f64, f64), tol: f64, xs: &[f64], ys: &[f64]) -> Self {
        let (minx, miny, maxx, maxy) = env;
        let (dx, dy) = (maxx - minx, maxy - miny);
        let offset = (if dy > dx { dy } else { dx }) * 10.0; // Python's max(): the first unless the second is greater
        let mut x = Vec::with_capacity(xs.len() + 3);
        let mut y = Vec::with_capacity(ys.len() + 3);
        x.extend_from_slice(&[(maxx + minx) / 2.0, minx - offset, maxx + offset]);
        y.extend_from_slice(&[maxy + offset, miny - offset, miny - offset]);
        x.extend_from_slice(xs);
        y.extend_from_slice(ys);
        let cap = 3 * xs.len() + 8;
        let mut sd = Subdivision {
            x,
            y,
            next: Vec::with_capacity(4 * cap),
            vert: Vec::with_capacity(4 * cap),
            live: Vec::with_capacity(cap),
            first_live: 0,
            n_edges: 0,
            tol,
            edge_tol: tol / 1000.0,
            last: 0,
        };
        let ea = sd.new_edge(0, 1);
        let eb = sd.new_edge(1, 2);
        sd.splice(sym(ea), eb);
        let ec = sd.new_edge(2, 0);
        sd.splice(sym(eb), ec);
        sd.splice(sym(ec), ea);
        sd.last = ea;
        sd
    }

    /// The number of vertices, the frame's three included.
    pub fn n_vertices(&self) -> usize {
        self.x.len()
    }

    #[inline]
    fn onext(&self, e: u32) -> u32 {
        self.next[e as usize]
    }

    #[inline]
    fn oprev(&self, e: u32) -> u32 {
        rot(self.onext(rot(e)))
    }

    #[inline]
    fn lnext(&self, e: u32) -> u32 {
        rot(self.onext(inv_rot(e))) // invRot().oNext().rot()
    }

    #[inline]
    fn dprev(&self, e: u32) -> u32 {
        inv_rot(self.onext(inv_rot(e))) // invRot().oNext().invRot()
    }

    #[inline]
    fn lprev(&self, e: u32) -> u32 {
        sym(self.onext(e))
    }

    #[inline]
    fn orig(&self, e: u32) -> u32 {
        self.vert[e as usize]
    }

    #[inline]
    fn dest(&self, e: u32) -> u32 {
        self.vert[sym(e) as usize]
    }

    fn new_edge(&mut self, o: u32, d: u32) -> u32 {
        let base = self.next.len() as u32;
        self.next
            .extend_from_slice(&[base, base + 3, base + 2, base + 1]);
        self.vert.extend_from_slice(&[o, NONE, d, NONE]);
        self.live.push(true);
        self.n_edges += 1;
        base
    }

    fn splice(&mut self, a: u32, b: u32) {
        let alpha = rot(self.onext(a));
        let beta = rot(self.onext(b));
        let (t1, t2, t3, t4) = (
            self.onext(b),
            self.onext(a),
            self.onext(beta),
            self.onext(alpha),
        );
        self.next[a as usize] = t1;
        self.next[b as usize] = t2;
        self.next[alpha as usize] = t3;
        self.next[beta as usize] = t4;
    }

    fn swap(&mut self, e: u32) {
        let a = self.oprev(e);
        let b = self.oprev(sym(e));
        self.splice(e, a);
        self.splice(sym(e), b);
        let la = self.lnext(a);
        self.splice(e, la);
        let lb = self.lnext(b); // after the third splice, as JTS's `swap` evaluates it
        self.splice(sym(e), lb);
        self.vert[e as usize] = self.dest(a);
        self.vert[sym(e) as usize] = self.dest(b);
    }

    fn connect(&mut self, a: u32, b: u32) -> u32 {
        let e = self.new_edge(self.dest(a), self.orig(b));
        let la = self.lnext(a);
        self.splice(e, la);
        self.splice(sym(e), b);
        e
    }

    fn delete(&mut self, e: u32) {
        let p = self.oprev(e);
        self.splice(e, p);
        let ps = self.oprev(sym(e));
        self.splice(sym(e), ps);
        self.n_edges -= 1;
        self.live[(e >> 2) as usize] = false;
    }

    /// `Vertex.isCCW`: the plain floating-point determinant.
    #[inline]
    fn ccw(&self, a: u32, b: u32, c: u32) -> bool {
        let (x, y) = (&self.x, &self.y);
        let (ax, ay) = (x[a as usize], y[a as usize]);
        (x[b as usize] - ax) * (y[c as usize] - ay) - (y[b as usize] - ay) * (x[c as usize] - ax)
            > 0.0
    }

    #[inline]
    fn right_of(&self, v: u32, e: u32) -> bool {
        self.ccw(v, self.dest(e), self.orig(e))
    }

    /// `LastFoundQuadEdgeLocator` + `locateFromEdge`: walk from the last edge found (from the first live edge when
    /// that one was deleted, as JTS's locator restarts).
    fn locate(&mut self, v: u32) -> Result<u32, LocateFailure> {
        let mut e = self.last;
        if !self.live[(e >> 2) as usize] {
            while !self.live[self.first_live] {
                self.first_live += 1;
            }
            e = (self.first_live as u32) << 2;
        }
        let (vx, vy) = (self.x[v as usize], self.y[v as usize]);
        let max_it = self.n_edges;
        let mut it = 0usize;
        loop {
            it += 1;
            if it > max_it {
                return Err(LocateFailure);
            }
            let (o, d) = (self.orig(e) as usize, self.dest(e) as usize);
            if (vx == self.x[o] && vy == self.y[o]) || (vx == self.x[d] && vy == self.y[d]) {
                break;
            }
            if self.right_of(v, e) {
                e = sym(e);
            } else if !self.right_of(v, self.onext(e)) {
                e = self.onext(e);
            } else if !self.right_of(v, self.dprev(e)) {
                e = self.dprev(e);
            } else {
                break;
            }
        }
        self.last = e;
        Ok(e)
    }

    /// `Vertex.isInCircle` = `TrianglePredicate.isInCircleNormalized(a, b, c, p)`.
    #[inline]
    fn in_circle(&self, p: u32, a: u32, b: u32, c: u32) -> bool {
        let (x, y) = (&self.x, &self.y);
        let (px, py) = (x[p as usize], y[p as usize]);
        let (adx, ady) = (x[a as usize] - px, y[a as usize] - py);
        let (bdx, bdy) = (x[b as usize] - px, y[b as usize] - py);
        let (cdx, cdy) = (x[c as usize] - px, y[c as usize] - py);
        let abdet = adx * bdy - bdx * ady;
        let bcdet = bdx * cdy - cdx * bdy;
        let cadet = cdx * ady - adx * cdy;
        let alift = adx * adx + ady * ady;
        let blift = bdx * bdx + bdy * bdy;
        let clift = cdx * cdx + cdy * cdy;
        alift * bcdet + blift * cadet + clift * abdet > 0.0
    }

    fn is_concave_at_origin(&self, e: u32) -> bool {
        let o = self.orig(e) as usize;
        let p = self.dest(self.oprev(e)) as usize;
        let q = self.dest(self.onext(e)) as usize;
        let (x, y) = (&self.x, &self.y);
        orientation_index(x[p], y[p], x[q], y[q], x[o], y[o]) == 1
    }

    fn is_concave_boundary(&self, e: u32) -> bool {
        if self.dest(e) < FRAME {
            return self.is_concave_at_origin(e);
        }
        if self.orig(e) < FRAME {
            return self.is_concave_at_origin(sym(e));
        }
        false
    }

    fn between_frame_and_inserted(&self, e: u32, v: u32) -> bool {
        let adj1 = self.dest(self.onext(e));
        let adj2 = self.dest(self.oprev(e));
        (adj1 == v && adj2 < FRAME) || (adj2 == v && adj1 < FRAME)
    }

    /// `isOnEdge`: `LineSegment.distance(p) < tol / 1000`.
    fn on_edge(&self, e: u32, v: u32) -> bool {
        let (x, y) = (&self.x, &self.y);
        let (o, d, v) = (self.orig(e) as usize, self.dest(e) as usize, v as usize);
        let (ax, ay, bx, by) = (x[o], y[o], x[d], y[d]);
        let (px, py) = (x[v], y[v]);
        if ax == bx && ay == by {
            return hypot(px - ax, py - ay) < self.edge_tol;
        }
        let len2 = (bx - ax) * (bx - ax) + (by - ay) * (by - ay);
        let r = ((px - ax) * (bx - ax) + (py - ay) * (by - ay)) / len2;
        let d = if r <= 0.0 {
            hypot(px - ax, py - ay)
        } else if r >= 1.0 {
            hypot(px - bx, py - by)
        } else {
            let s = ((ay - py) * (bx - ax) - (ax - px) * (by - ay)) / len2;
            s.abs() * len2.sqrt()
        };
        d < self.edge_tol
    }

    /// `IncrementalDelaunayTriangulator.insertSite` of vertex `v`.
    pub fn insert_site(&mut self, v: u32) -> Result<(), LocateFailure> {
        let mut e = self.locate(v)?;
        if self.tol > 0.0 {
            let (o, d, vi) = (self.orig(e) as usize, self.dest(e) as usize, v as usize);
            let (x, y) = (&self.x, &self.y);
            if hypot(x[vi] - x[o], y[vi] - y[o]) < self.tol
                || hypot(x[vi] - x[d], y[vi] - y[d]) < self.tol
            {
                return Ok(());
            }
            if self.on_edge(e, v) {
                e = self.oprev(e);
                let n = self.onext(e);
                self.delete(n);
            }
        }
        let mut base = self.new_edge(self.orig(e), v);
        self.splice(base, e);
        let start = base;
        loop {
            base = self.connect(e, sym(base));
            e = self.oprev(base);
            if self.lnext(e) == start {
                break;
            }
        }
        loop {
            let t = self.oprev(e);
            let td = self.dest(t);
            let mut flip =
                self.right_of(td, e) && self.in_circle(v, self.orig(e), td, self.dest(e));
            if self.is_concave_boundary(e) {
                flip = true;
            } else if self.between_frame_and_inserted(e, v) {
                flip = false;
            }
            if flip {
                self.swap(e);
                e = self.oprev(e);
            } else if self.onext(e) == start {
                return Ok(());
            } else {
                e = self.lprev(self.onext(e));
            }
        }
    }

    /// Every triangle without a frame vertex, as an ascending vertex triple, in ascending order
    /// (`getTriangles(false)`). Vertices are numbered as in `new`: the first site is 3.
    pub fn triangles(&self) -> Vec<[u32; 3]> {
        let mut out = Vec::with_capacity(2 * self.x.len());
        for (q, live) in self.live.iter().enumerate() {
            if !live {
                continue;
            }
            let q0 = (q as u32) << 2;
            for e in [q0, sym(q0)] {
                let e1 = self.lnext(e);
                let e2 = self.lnext(e1);
                if self.lnext(e2) != e {
                    continue;
                }
                let mut t = [self.orig(e), self.orig(e1), self.orig(e2)];
                if t.iter().any(|v| *v < FRAME) {
                    continue;
                }
                t.sort_unstable();
                out.push(t);
            }
        }
        out.sort_unstable();
        out.dedup();
        out
    }
}

/// The triangles JTS's incremental triangulation makes inserting the sites `xs`/`ys` in order, framed on `env` with
/// tolerance `tol`: ascending triples of site indices (the first site is 0), in ascending order.
pub fn delaunay(
    xs: &[f64],
    ys: &[f64],
    env: (f64, f64, f64, f64),
    tol: f64,
) -> Result<Vec<[u32; 3]>, LocateFailure> {
    let mut sd = Subdivision::new(env, tol, xs, ys);
    for v in FRAME..sd.n_vertices() as u32 {
        sd.insert_site(v)?;
    }
    let mut tris = sd.triangles();
    for t in tris.iter_mut() {
        for v in t.iter_mut() {
            *v -= FRAME;
        }
    }
    Ok(tris)
}

/// `org.locationtech.jts.index.kdtree.KdTree`, as `pyvx/_conforming.KdTree`: unbalanced, X and Y levels
/// alternating, a point within `tol` of a node snaps onto it. A node's id is its insertion rank (the root is 0).
pub struct KdTree {
    tol: f64,
    x: Vec<f64>,
    y: Vec<f64>,
    left: Vec<u32>,
    right: Vec<u32>,
    stack: Vec<(u32, bool)>,
}

impl KdTree {
    pub fn new(tol: f64) -> Self {
        KdTree {
            tol,
            x: Vec::new(),
            y: Vec::new(),
            left: Vec::new(),
            right: Vec::new(),
            stack: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.x.len()
    }

    /// The node's coordinates.
    pub fn node(&self, id: u32) -> (f64, f64) {
        (self.x[id as usize], self.y[id as usize])
    }

    fn push(&mut self, x: f64, y: f64) -> u32 {
        self.x.push(x);
        self.y.push(y);
        self.left.push(NONE);
        self.right.push(NONE);
        (self.x.len() - 1) as u32
    }

    /// Insert (x, y); true when it made a new node (false: it snapped onto one).
    pub fn insert(&mut self, x: f64, y: f64) -> bool {
        if self.x.is_empty() {
            self.push(x, y);
            return true;
        }
        let tol = self.tol;
        if tol > 0.0 && self.has_match(x, y) {
            return false;
        }
        let mut cur = 0u32;
        let mut leaf = cur;
        let mut is_x = true;
        let mut less = true;
        while cur != NONE {
            let (cx, cy) = self.node(cur);
            let (dx, dy) = (x - cx, y - cy);
            let same = if tol == 0.0 {
                dx == 0.0 && dy == 0.0
            } else {
                hypot(dx, dy) <= tol
            };
            if same {
                return false;
            }
            less = if is_x { x < cx } else { y < cy };
            leaf = cur;
            cur = if less {
                self.left[cur as usize]
            } else {
                self.right[cur as usize]
            };
            is_x = !is_x;
        }
        let node = self.push(x, y);
        if less {
            self.left[leaf as usize] = node;
        } else {
            self.right[leaf as usize] = node;
        }
        true
    }

    /// The nodes inside the closed envelope, in JTS's (in-order, iterative) traversal order, appended to `out`.
    pub fn query(&mut self, minx: f64, miny: f64, maxx: f64, maxy: f64, out: &mut Vec<u32>) {
        let mut stack = std::mem::take(&mut self.stack);
        stack.clear();
        let mut cur = if self.x.is_empty() { NONE } else { 0 };
        let mut is_x = true;
        loop {
            if cur != NONE {
                stack.push((cur, is_x));
                let (cx, cy) = self.node(cur);
                if if is_x { minx < cx } else { miny < cy } {
                    cur = self.left[cur as usize];
                    if cur != NONE {
                        is_x = !is_x;
                    }
                } else {
                    cur = NONE;
                }
            } else if let Some((c, ix)) = stack.pop() {
                is_x = ix;
                let (cx, cy) = self.node(c);
                if minx <= cx && cx <= maxx && miny <= cy && cy <= maxy {
                    out.push(c);
                }
                if if is_x { cx <= maxx } else { cy <= maxy } {
                    cur = self.right[c as usize];
                    if cur != NONE {
                        is_x = !is_x;
                    }
                } else {
                    cur = NONE;
                }
            } else {
                break;
            }
        }
        self.stack = stack;
    }

    /// Whether `BestMatchVisitor` finds a node within `tol` of (x, y): only that there is one matters to `insert`.
    fn has_match(&mut self, x: f64, y: f64) -> bool {
        let tol = self.tol;
        let mut found = Vec::new();
        self.query(x - tol, y - tol, x + tol, y + tol, &mut found);
        found.iter().any(|id| {
            let (nx, ny) = self.node(*id);
            hypot(x - nx, y - ny) <= tol
        })
    }

    /// `findNonGabrielPoint` of the segment p-q: the node strictly inside its diametral circle closest to its midpoint
    /// (the first found on a tie), or None.
    pub fn encroacher(
        &mut self,
        px: f64,
        py: f64,
        qx: f64,
        qy: f64,
        buf: &mut Vec<u32>,
    ) -> Option<u32> {
        let (mx, my) = ((px + qx) / 2.0, (py + qy) / 2.0);
        let rad = hypot(px - mx, py - my);
        buf.clear();
        self.query(mx - rad, my - rad, mx + rad, my + rad, buf);
        let mut enc = None;
        let mut min_d = 0.0;
        for id in buf.iter() {
            let (tx, ty) = self.node(*id);
            if (tx == px && ty == py) || (tx == qx && ty == qy) {
                continue;
            }
            let d = hypot(mx - tx, my - ty);
            if d < rad && (enc.is_none() || d < min_d) {
                enc = Some(*id);
                min_d = d;
            }
        }
        enc
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A small deterministic generator (the crate has no dependencies).
    struct Lcg(u64);
    impl Lcg {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.0 >> 11
        }
        fn int(&mut self, bound: i64) -> i64 {
            (self.next() % (2 * bound as u64 + 1)) as i64 - bound
        }
        fn unit(&mut self) -> f64 {
            self.next() as f64 / (1u64 << 53) as f64
        }
    }

    /// The exact sign against integer arithmetic (i128), on near-collinear integer points the filter cannot decide.
    #[test]
    fn exact_orientation_matches_integer_arithmetic() {
        let mut r = Lcg(7);
        let mut undecided = 0;
        for _ in 0..20000 {
            let s = 1i64 << 50;
            let (ax, ay, bx, by) = (r.int(s), r.int(s), r.int(s), r.int(s));
            let k = r.int(4);
            let (cx, cy) = (
                ax + k * (bx - ax) / 4 + r.int(2),
                ay + k * (by - ay) / 4 + r.int(2),
            );
            let f = |v: i64| v as f64;
            if [ax, ay, bx, by, cx, cy].iter().any(|v| f(*v) as i64 != *v) {
                continue;
            }
            let want = {
                let d =
                    (bx - ax) as i128 * (cy - by) as i128 - (by - ay) as i128 * (cx - bx) as i128;
                d.signum() as i32
            };
            let got = exact_orientation_sign(f(ax), f(ay), f(bx), f(by), f(cx), f(cy));
            assert_eq!(got, want, "({ax},{ay}) ({bx},{by}) ({cx},{cy})");
            let (detleft, detright) = (
                (f(ax) - f(cx)) * (f(by) - f(cy)),
                (f(ay) - f(cy)) * (f(bx) - f(cx)),
            );
            let det = detleft - detright;
            if det.abs() < 1e-15 * (detleft.abs() + detright.abs()) {
                undecided += 1;
            }
            // the filtered predicate: (a - c) x (b - c) has the sign of (b - a) x (c - b)
            assert_eq!(
                orientation_index(f(ax), f(ay), f(bx), f(by), f(cx), f(cy)),
                want
            );
        }
        assert!(undecided > 100, "{undecided} cases reached the exact sign");
        // fractional coordinates: exactly collinear on a binary grid, and one unit in the last place off it
        assert_eq!(
            exact_orientation_sign(0.1, 0.1, 0.2, 0.2, 0.30000000000000004, 0.30000000000000004),
            0
        );
        assert_ne!(
            exact_orientation_sign(0.1, 0.1, 0.2, 0.2, 0.3, 0.30000000000000004),
            0
        );
    }

    fn check_delaunay(xs: &[f64], ys: &[f64], tris: &[[u32; 3]]) {
        // Euler: a triangulation of n points with h on the hull has 2n - 2 - h triangles; and each is Delaunay-empty
        let n = xs.len();
        for t in tris {
            for v in 0..n as u32 {
                if t.contains(&v) {
                    continue;
                }
                let [a, b, c] = t.map(|i| i as usize);
                let ori = (xs[b] - xs[a]) * (ys[c] - ys[a]) - (ys[b] - ys[a]) * (xs[c] - xs[a]);
                let (b, c) = if ori > 0.0 { (b, c) } else { (c, b) };
                let p = v as usize;
                let (adx, ady, bdx, bdy, cdx, cdy) = (
                    xs[a] - xs[p],
                    ys[a] - ys[p],
                    xs[b] - xs[p],
                    ys[b] - ys[p],
                    xs[c] - xs[p],
                    ys[c] - ys[p],
                );
                let det = (adx * adx + ady * ady) * (bdx * cdy - cdx * bdy)
                    + (bdx * bdx + bdy * bdy) * (cdx * ady - adx * cdy)
                    + (cdx * cdx + cdy * cdy) * (adx * bdy - bdx * ady);
                assert!(det <= 1e-6, "site {p} inside the circle of {t:?}");
            }
        }
    }

    #[test]
    fn random_sites_give_a_delaunay_triangulation() {
        let mut r = Lcg(11);
        let n = 300;
        let mut pts: Vec<(f64, f64)> = (0..n)
            .map(|_| (r.unit() * 100.0, r.unit() * 100.0))
            .collect();
        pts.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let (xs, ys): (Vec<f64>, Vec<f64>) = pts.into_iter().unzip();
        let env = (0.0, 0.0, 100.0, 100.0);
        let tris = delaunay(&xs, &ys, env, 0.0).unwrap();
        assert!(tris.len() > n && tris.len() < 2 * n);
        check_delaunay(&xs, &ys, &tris);
        for t in &tris {
            assert!(t[0] < t[1] && t[1] < t[2]);
        }
        assert!(tris.windows(2).all(|w| w[0] < w[1]));
    }

    /// A 3x3 grid: every unit square is split by one diagonal (8 triangles), whichever JTS picks.
    #[test]
    fn grid_ties_give_two_triangles_per_cell() {
        let (mut xs, mut ys) = (Vec::new(), Vec::new());
        for i in 0..3 {
            for j in 0..3 {
                xs.push(i as f64);
                ys.push(j as f64);
            }
        }
        let tris = delaunay(&xs, &ys, (0.0, 0.0, 2.0, 2.0), 0.0).unwrap();
        assert_eq!(tris.len(), 8);
    }

    #[test]
    fn a_tolerance_drops_a_site_near_another() {
        let xs = [0.0, 0.0, 1.0, 1.0, 0.5, 0.5000001];
        let ys = [0.0, 1.0, 0.0, 1.0, 0.5, 0.5];
        let tris = delaunay(&xs, &ys, (0.0, 0.0, 1.0, 1.0), 0.001).unwrap();
        assert!(tris.iter().all(|t| !t.contains(&5)));
        assert_eq!(tris.len(), 4);
    }

    #[test]
    fn kdtree_snaps_and_queries_in_order() {
        let mut t = KdTree::new(0.0);
        assert!(t.insert(5.0, 5.0));
        assert!(t.insert(2.0, 7.0));
        assert!(t.insert(8.0, 1.0));
        assert!(!t.insert(2.0, 7.0));
        assert!(t.insert(3.0, 3.0));
        let mut out = Vec::new();
        t.query(0.0, 0.0, 10.0, 10.0, &mut out);
        assert_eq!(out.len(), 4);
        out.clear();
        t.query(1.0, 6.0, 3.0, 8.0, &mut out);
        assert_eq!(out, vec![1]);
        let mut s = KdTree::new(0.5);
        assert!(s.insert(0.0, 0.0));
        assert!(s.insert(1.0, 0.0));
        assert!(!s.insert(0.3, 0.3));
        assert!(!s.insert(1.2, -0.2));
        assert_eq!(s.len(), 2);
        let mut buf = Vec::new();
        assert_eq!(t.encroacher(1.0, 2.0, 5.0, 4.0, &mut buf), Some(3));
        assert_eq!(t.encroacher(0.0, 0.0, 0.1, 0.1, &mut buf), None);
    }
}
