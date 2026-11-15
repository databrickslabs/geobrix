package com.databricks.labs.gbx.vectorx.jts

/** Delaunay triangulation and Z interpolation for DTM. Used by the VectorX generators. */
import org.locationtech.jts.geom.util.{LinearComponentExtracter, PolygonExtracter}
import org.locationtech.jts.geom._
import org.locationtech.jts.index.strtree.STRtree
import org.locationtech.jts.triangulate.{ConstraintEnforcementException, DelaunayTriangulationBuilder}
import org.locationtech.jts.triangulate.quadedge.LocateFailureException

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

/** Delaunay triangulation from points and breaklines; interpolates Z at grid points and builds point grids. */
object InterpolateElevation {

    /** Builds triangulation from multipoint and breaklines, then interpolates Z for each grid point. Each break
     *  line geometry is split into parts as [[triangulate]] splits it (see [[prepareSites]]). */
    def interpolate(
        multipoint: MultiPoint,
        breaklines: Seq[Geometry],
        gridPoints: MultiPoint,
        mergeTolerance: Double,
        snapTolerance: Double,
        splitPointFinder: Option[TriangulationSplitPointTypeEnum.Value] = None,
        mode: String = "constrained"
    ): Seq[Point] = {
        val triangles = triangulate(multipoint, breaklines, mergeTolerance, snapTolerance, splitPointFinder, mode)

        val tree = new STRtree(4)
        triangles.foreach(p => tree.insert(p.getEnvelopeInternal, p))

        val pointsSeq = (0 until gridPoints.getNumGeometries)
            .map(i => gridPoints.getGeometryN(i))
            .collect { case p: org.locationtech.jts.geom.Point => p }
        pointsSeq
            .map(p => {
                p -> tree
                    .query(p.getEnvelopeInternal)
                    .asScala
                    .map(_.asInstanceOf[Polygon])
                    .find(_.intersects(p))
            })
            .toMap
            .collect({ case (pt, Some(ply)) => pt -> ply })
            .flatMap({ case (point: Point, poly: Polygon) =>
                val polyCoords = poly.getCoordinates
                val tri = new Triangle(polyCoords(0), polyCoords(1), polyCoords(2))
                val z = tri.interpolateZ(point.getCoordinate)
                if (z.isNaN) {
                    None // cell with degenerate triangle -> caller treats as no_data
                } else {
                    val ip = JTS.point(new Coordinate(point.getX, point.getY, z))
                    ip.setSRID(multipoint.getSRID)
                    Some(ip)
                }
            })
            .toSeq
    }

    /** Returns Delaunay triangles from multiPoint and optional breaklines.
     *
     *  `mode` selects how breakline constraints are honoured:
     *    - `"constrained"` (default) — no Steiner points. Every breakline coordinate is added as a
     *      triangulation site, an unconstrained Delaunay is built, then Sloan edge-flip recovery
     *      makes each breakline segment a triangle edge; crossing break lines raise
     *      [[ConstrainedCrossing]]. Mirrors the pyvx light tier (`pyvx/_tin.py`).
     *    - `"conforming"` — JTS `ConformingDelaunayTriangulator` (may insert Steiner points to make the
     *      triangulation conform to the constraints). Mirrors `pyvx/_conforming.py`.
     *
     *  The mode word is taken in any case.
     *
     *  Both modes first run [[prepareSites]] (Z of 2D coordinates, the merge of `mergeTolerance`, then the snap of
     *  `snapTolerance`), the steps documented once in `pyvx/_tin.py` `prepare_sites`.
     */
    def triangulate(
        multiPoint: Geometry,
        breaklines: Seq[Geometry],
        mergeTolerance: Double,
        snapTolerance: Double,
        splitPointFinder: Option[TriangulationSplitPointTypeEnum.Value] = None,
        mode: String = "constrained"
    ): Seq[Geometry] = {
        // the word in any case, as the Python and UC levels take it (`pyvx/_tin.py` `validate_mode`)
        val m = Option(mode).map(_.toLowerCase(java.util.Locale.ROOT)).orNull // a null mode raises below
        if (m != "conforming" && m != "constrained") {
            throw new IllegalArgumentException(s"mode must be 'constrained' or 'conforming'; got '$mode'")
        }
        val (points, lines) = prepareSites(multiPoint, breaklines, mergeTolerance, snapTolerance)
        val gf = multiPoint.getFactory
        val triangles =
            if (m == "conforming") triangulateConforming(points, lines, mergeTolerance, splitPointFinder, gf)
            else triangulateConstrained(points, lines, gf)
        triangles.map(canonicalTriangle(_, gf))
    }

    /** The triangle written one way on every level: counter-clockwise (the exact sign of its area) and from its lowest
     *  vertex, the least X then the least Y (`pyvx/_conforming.py` `canonical_rings`). So its WKB is the same bytes on
     *  every level. Anything but a triangle is returned as it is. */
    private[jts] def canonicalTriangle(t: Geometry, gf: GeometryFactory): Geometry = {
        val cs = t.getCoordinates
        if (!t.isInstanceOf[Polygon] || cs.length != 4) return t
        val v =
            if (orientationSign(cs(0), cs(1), cs(2)) < 0) Array(cs(0), cs(2), cs(1))
            else Array(cs(0), cs(1), cs(2))
        var k = 0
        for (j <- 1 to 2) {
            if (v(j).x < v(k).x || (v(j).x == v(k).x && v(j).y < v(k).y)) k = j
        }
        val a = v(k)
        gf.createPolygon(Array(a, v((k + 1) % 3), v((k + 2) % 3), new Coordinate(a)))
    }

    /** The exact sign of `(b - a) x (c - a)`: the floating-point determinant where Shewchuk's error bound certifies it,
     *  else exact decimal arithmetic. */
    private[jts] def orientationSign(a: Coordinate, b: Coordinate, c: Coordinate): Int = {
        val left = (b.x - a.x) * (c.y - a.y)
        val right = (b.y - a.y) * (c.x - a.x)
        val det = left - right
        val eps = Math.ulp(1.0) / 2.0
        if (Math.abs(det) > (3.0 + 16.0 * eps) * eps * (Math.abs(left) + Math.abs(right))) {
            return if (det > 0.0) 1 else -1
        }
        def d(x: Double) = new java.math.BigDecimal(x)
        d(b.x).subtract(d(a.x)).multiply(d(c.y).subtract(d(a.y)))
            .subtract(d(b.y).subtract(d(a.y)).multiply(d(c.x).subtract(d(a.x))))
            .signum()
    }

    /** The message of a conforming triangulation JTS stops after its 99 splitting passes. */
    val ConformingTooManySplits: String =
        "mode 'conforming' could not fit the triangulation to the break lines: too many splitting " +
            "iterations (the limit is 99). Fix: split crossing break lines where they cross, move break lines " +
            "apart where they meet at a sharp angle, or use mode 'constrained'"

    /** The message of a conforming triangulation whose point location does not converge. */
    val ConformingLocateFailed: String =
        "mode 'conforming' could not fit the triangulation to the break lines: locating a point did not " +
            "converge. Fix: split crossing break lines where they cross, or use mode 'constrained'"

    /** Conforming path: JTS ConformingDelaunayTriangulator (may insert Steiner points). */
    private def triangulateConforming(
        points: Array[Coordinate],
        lines: Seq[Array[Coordinate]],
        mergeTolerance: Double,
        splitPointFinder: Option[TriangulationSplitPointTypeEnum.Value],
        gf: GeometryFactory
    ): Seq[Geometry] = {
        val triangulator = JTSConformingDelaunayTriangulationBuilder(gf.createMultiPointFromCoords(points))
        val segLines = lines.filter(_.length >= 2)
        if (segLines.nonEmpty) {
            triangulator.setConstraints(gf.createMultiLineString(segLines.map(l => gf.createLineString(l)).toArray))
        }
        triangulator.setTolerance(mergeTolerance)
        splitPointFinder.foreach(triangulator.setSplitPointFinder)

        val trianglesGeomCollection =
            try triangulator.getTriangles
            catch {
                case e: ConstraintEnforcementException => throw new IllegalArgumentException(ConformingTooManySplits, e)
                case e: LocateFailureException         => throw new IllegalArgumentException(ConformingLocateFailed, e)
            }
        PolygonExtracter.getPolygons(trianglesGeomCollection).asScala.map(_.asInstanceOf[Geometry]).toSeq
    }

    /** Constrained (no-Steiner) path over the merged sites of [[prepareSites]] (no second merge), as the pyvx light
     *  `_triangulate_constrained`:
     *
     *  1. Site set = breakline coords (first) ++ mass points, a coordinate two lines share kept once.
     *  2. Unconstrained Delaunay over the XY of all sites (JTS `DelaunayTriangulationBuilder`); a point location
     *     that does not converge raises [[ConstrainedLocateFailed]].
     *  3. Sloan recovery ([[recoverConstraints]]): each breakline segment becomes a triangle edge, or it raises.
     */
    private def triangulateConstrained(
        points: Array[Coordinate],
        lines: Seq[Array[Coordinate]],
        gf: GeometryFactory
    ): Seq[Geometry] = {
        // --- 1. site coordinate set: breaklines first, then mass points, each XY once ---
        val sites = uniqueXY(lines.flatten ++ points.toSeq)
        if (sites.length < 3) return Seq.empty[Geometry]
        val where = scala.collection.mutable.HashMap[(Double, Double), Int]()
        sites.indices.foreach(i => where((sites(i).x + 0.0, sites(i).y + 0.0)) = i)
        def at(c: Coordinate): Int = where.getOrElse((c.x + 0.0, c.y + 0.0), -1)

        // --- 2. initial unconstrained Delaunay over XY ---
        val builder = new DelaunayTriangulationBuilder()
        builder.setSites(java.util.Arrays.asList(sites: _*))
        val triGeomColl =
            try builder.getTriangles(gf)
            catch { case e: LocateFailureException => throw new IllegalArgumentException(ConstrainedLocateFailed, e) }
        val initialPolys =
            PolygonExtracter.getPolygons(triGeomColl).asScala.map(_.asInstanceOf[Polygon]).toSeq

        // Map each triangle's vertices back to site indices (the same XY as a site).
        var triangles: Array[Array[Int]] = initialPolys.flatMap { poly =>
            val cs = poly.getCoordinates
            // exterior ring is 4 coords (closing repeat); take the first 3 distinct
            val idx = Array(at(cs(0)), at(cs(1)), at(cs(2)))
            if (idx.contains(-1)) None else Some(idx)
        }.toArray

        // --- 3. Sloan constraint recovery ---
        if (lines.nonEmpty && triangles.nonEmpty) {
            val segments = lines.flatMap(l => (0 until l.length - 1).map(k => (at(l(k)), at(l(k + 1)))))
            triangles = recoverConstraints(sites, triangles, segments)
        }

        // --- triangle polygons carrying site Z ---
        triangles.toSeq.map { t =>
            val ring = Array(sites(t(0)), sites(t(1)), sites(t(2)), sites(t(0)))
            gf.createPolygon(ring)
        }
    }

    /** A copy of `c` whose missing Z (NaN: a 2D point or line) is 0, as the constrained mode always did. */
    private def withZ(c: Coordinate): Coordinate =
        new Coordinate(c.x, c.y, if (java.lang.Double.isNaN(c.getZ)) 0.0 else c.getZ)

    /** The shared pre-step of both modes: `(mass points, break lines)`, each break line one coordinate array.
     *
     *  Every coordinate without Z takes Z 0. Then the one merge of `mergeTolerance` ([[merge]]), whether or not
     *  anything snaps. Then, when `snapTolerance` > 0 and a break line has a segment, [[snapToBreaklines]] moves each
     *  mass point within reach onto its nearest break line. A mirror of `pyvx/_tin.py` `prepare_sites`, whose
     *  docstring states the rules.
     */
    private[jts] def prepareSites(
        multiPoint: Geometry,
        breaklines: Seq[Geometry],
        mergeTolerance: Double,
        snapTolerance: Double
    ): (Array[Coordinate], Seq[Array[Coordinate]]) = {
        val (points, lines) = merge(
          multiPoint.getCoordinates.map(withZ),
          breaklines
              .flatMap(bl => LinearComponentExtracter.getLines(bl).asScala.map(_.asInstanceOf[LineString]))
              .filter(!_.isEmpty)
              .map(_.getCoordinates.map(withZ)),
          mergeTolerance
        )
        if (snapTolerance > 0.0 && lines.nonEmpty && points.nonEmpty) {
            snapToBreaklines(points, lines, mergeTolerance, snapTolerance)
        } else {
            (points, lines)
        }
    }

    /** The one merge of `tol` (step 2 of `prepare_sites`): the break line vertices, line by line, then the mass
     *  points, each joins the nearest coordinate kept before it within `tol` ([[SiteIndex]]) or is kept. A line vertex
     *  that merges takes the kept coordinate's place (a repeat in a row is dropped); a line left with one coordinate
     *  gives that coordinate to the mass points, before them. */
    private def merge(
        points: Array[Coordinate],
        lines: Seq[Array[Coordinate]],
        tol: Double
    ): (Array[Coordinate], Seq[Array[Coordinate]]) = {
        val index = new SiteIndex(tol)
        val out = ArrayBuffer[Array[Coordinate]]()
        val lone = ArrayBuffer[Coordinate]()
        lines.foreach { l =>
            val row = ArrayBuffer[Coordinate]()
            val added = ArrayBuffer[Coordinate]()
            l.foreach { c =>
                var m = index.matchOf(c)
                if (m == null) {
                    index.add(c)
                    added += c
                    m = c
                }
                if (row.isEmpty || (row.last ne m)) row += m
            }
            if (row.length >= 2) out += row.toArray
            else lone ++= added
        }
        val kept = ArrayBuffer[Coordinate]()
        points.foreach { c =>
            if (index.matchOf(c) == null) {
                index.add(c)
                kept += c
            }
        }
        ((lone ++ kept).toArray, out.toSeq)
    }

    /** `org.locationtech.jts.algorithm.Distance.pointToSegment`, spelled out so the Python level can repeat each
     *  operation in the same order (`pyvx/_tin.py` `_point_segment_distance`). */
    private[jts] def pointToSegment(px: Double, py: Double, ax: Double, ay: Double, bx: Double, by: Double): Double = {
        if (ax == bx && ay == by) return Math.hypot(px - ax, py - ay)
        val len2 = (bx - ax) * (bx - ax) + (by - ay) * (by - ay)
        val r = ((px - ax) * (bx - ax) + (py - ay) * (by - ay)) / len2
        if (r <= 0.0) return Math.hypot(px - ax, py - ay)
        if (r >= 1.0) return Math.hypot(px - bx, py - by)
        val s = ((ay - py) * (bx - ax) - (ax - px) * (by - ay)) / len2
        Math.abs(s) * Math.sqrt(len2)
    }

    /** The point of segment a-b nearest p, with the segment's Z interpolated there, and its projection factor. */
    private[jts] def project(p: Coordinate, a: Coordinate, b: Coordinate): (Double, Coordinate) = {
        if (a.x == b.x && a.y == b.y) return (0.0, a)
        val len2 = (b.x - a.x) * (b.x - a.x) + (b.y - a.y) * (b.y - a.y)
        val r = ((p.x - a.x) * (b.x - a.x) + (p.y - a.y) * (b.y - a.y)) / len2
        if (r <= 0.0) (0.0, a)
        else if (r >= 1.0) (1.0, b)
        else (r, new Coordinate(a.x + r * (b.x - a.x), a.y + r * (b.y - a.y), a.getZ + r * (b.getZ - a.getZ)))
    }

    /** The coordinates kept so far, to find the one a new coordinate merges into: the nearest within `tol`
     *  (`Math.hypot`), the lesser (x, y) on an exact tie, as JTS's `KdTree` picks it; with `tol` <= 0 the one with the
     *  same X and Y. A grid of `tol` cells finds the same coordinate as the tree. Mirrors light `_SiteIndex`. */
    private final class SiteIndex(tol: Double) {
        private val exact = scala.collection.mutable.HashMap[(Double, Double), Coordinate]()
        private val cells = scala.collection.mutable.HashMap[(Long, Long), ArrayBuffer[Coordinate]]()
        private def key(v: Double): Long = Math.floor(v / tol).toLong

        def add(c: Coordinate): Unit =
            if (tol > 0.0) cells.getOrElseUpdate((key(c.x), key(c.y)), ArrayBuffer[Coordinate]()) += c
            else exact((c.x + 0.0, c.y + 0.0)) = c

        /** The kept coordinate `c` merges into, or null. */
        def matchOf(c: Coordinate): Coordinate =
            if (tol > 0.0) {
                val kx = key(c.x)
                val ky = key(c.y)
                var best: Coordinate = null
                var bestD = 0.0
                for (i <- -2 to 2; j <- -2 to 2) {
                    cells.get((kx + i, ky + j)).foreach(_.foreach { v =>
                        val d = Math.hypot(c.x - v.x, c.y - v.y)
                        if (d <= tol && (best == null || d < bestD ||
                                (d == bestD && (v.x < best.x || (v.x == best.x && v.y < best.y))))) {
                            best = v
                            bestD = d
                        }
                    })
                }
                best
            } else {
                exact.getOrElse((c.x + 0.0, c.y + 0.0), null)
            }
    }

    /** Moves each mass point within `snapTol` of a break line onto the nearest one (see `prepare_sites`): returns the
     *  mass points left (less those within `mergeTol` of an inserted vertex) and the break lines with the snapped
     *  points inserted as vertices. */
    private def snapToBreaklines(
        points: Array[Coordinate],
        lines: Seq[Array[Coordinate]],
        mergeTol: Double,
        snapTol: Double
    ): (Array[Coordinate], Seq[Array[Coordinate]]) = {
        // the segments in input order: line by line, then along each line
        val segs = lines.flatMap(l => (0 until l.length - 1).map(k => (l(k), l(k + 1)))).toArray
        val tree = new STRtree()
        segs.indices.foreach { s =>
            val (a, b) = segs(s)
            tree.insert(new Envelope(a.x, b.x, a.y, b.y), Int.box(s))
        }
        val vertices = new SiteIndex(mergeTol)
        lines.foreach(_.foreach(vertices.add))
        val inserted = Array.fill(segs.length)(ArrayBuffer[(Double, Coordinate)]())
        val kept = ArrayBuffer[Coordinate]()
        val reach = 2.0 * snapTol // the query box only finds candidates: the test below decides
        points.foreach { p =>
            val candidates = tree
                .query(new Envelope(p.x - reach, p.x + reach, p.y - reach, p.y + reach))
                .asScala
                .map(_.asInstanceOf[Integer].intValue)
                .sorted
            var best = -1
            var bestD = 0.0
            candidates.foreach { s =>
                val (a, b) = segs(s)
                if (
                  Math.min(a.x, b.x) - snapTol <= p.x && p.x <= Math.max(a.x, b.x) + snapTol &&
                  Math.min(a.y, b.y) - snapTol <= p.y && p.y <= Math.max(a.y, b.y) + snapTol
                ) {
                    val d = pointToSegment(p.x, p.y, a.x, a.y, b.x, b.y)
                    if (d <= snapTol && (best < 0 || d < bestD)) {
                        best = s
                        bestD = d
                    }
                }
            }
            if (best < 0) {
                kept += p
            } else {
                val (a, b) = segs(best)
                val (r, q) = project(p, a, b)
                if (vertices.matchOf(q) == null) {
                    vertices.add(q)
                    inserted(best) += ((r, q))
                }
            }
        }
        var s = 0
        val out = lines.map { l =>
            val buf = ArrayBuffer[Coordinate]()
            l.indices.foreach { k =>
                buf += l(k)
                if (k < l.length - 1) {
                    buf ++= inserted(s).sortBy(_._1).map(_._2)
                    s += 1
                }
            }
            buf.toArray
        }
        // a mass point left within mergeTol of a vertex the snap inserted merges into it (light step 3e)
        val added = new SiteIndex(mergeTol)
        inserted.foreach(_.foreach { case (_, q) => added.add(q) })
        (if (inserted.exists(_.nonEmpty)) kept.filter(c => added.matchOf(c) == null).toArray else kept.toArray, out)
    }

    /** `coords` less each one whose X and Y an earlier one has (-0.0 is 0.0): the site list of the constrained mode,
     *  where a vertex two break lines share comes twice. Mirrors light `_unique_xy`. */
    private def uniqueXY(coords: Seq[Coordinate]): Array[Coordinate] = {
        val seen = scala.collection.mutable.HashSet[(Double, Double)]()
        coords.filter(c => seen.add((c.x + 0.0, c.y + 0.0))).toArray
    }

    /** The message of a constrained triangulation that cannot make a break line segment an edge (the light
     *  `CONSTRAINED_CROSSING`). */
    val ConstrainedCrossing: String =
        "mode 'constrained' could not make every break line segment a triangle edge: break lines cross, or a line " +
            "crosses itself. Fix: split the break lines where they cross"

    /** The message of a constrained triangulation whose point location does not converge (the light
     *  `CONSTRAINED_LOCATE_FAILED`). */
    val ConstrainedLocateFailed: String =
        "mode 'constrained' could not triangulate the sites: locating a point did not converge, as with sites very " +
            "close together. Fix: merge close sites with a merge_tolerance above 0"

    /** >0 if a->b->c is counter-clockwise (2D orientation). Mirrors light `_orient2d`. */
    private def orient2d(a: Coordinate, b: Coordinate, c: Coordinate): Double =
        (b.x - a.x) * (c.y - a.y) - (b.y - a.y) * (c.x - a.x)

    /** True iff the segments p1-p2 and p3-p4 cross at one point inside both (no end on the other's line). Mirrors
     *  light `_crosses`. */
    private def crosses(p1: Coordinate, p2: Coordinate, p3: Coordinate, p4: Coordinate): Boolean = {
        val d1 = orient2d(p3, p4, p1)
        val d2 = orient2d(p3, p4, p2)
        val d3 = orient2d(p1, p2, p3)
        val d4 = orient2d(p1, p2, p4)
        ((d1 > 0.0 && d2 < 0.0) || (d1 < 0.0 && d2 > 0.0)) && ((d3 > 0.0 && d4 < 0.0) || (d3 < 0.0 && d4 > 0.0))
    }

    /** Sloan's recovery (1993): each segment (site indices), in order, becomes a triangle edge. A site exactly on a
     *  segment splits it there; a segment that crosses one made an edge before it raises [[ConstrainedCrossing]]. The
     *  same steps in the same order as light `_recover_constraints`, whose docstring states them. */
    private[jts] def recoverConstraints(
        xy: Array[Coordinate],
        initial: Array[Array[Int]],
        segments: Seq[(Int, Int)]
    ): Array[Array[Int]] = {
        import scala.collection.mutable
        val tris = initial.map(_.clone())
        def ek(i: Int, j: Int): (Int, Int) = if (i < j) (i, j) else (j, i)
        val edgeTris = mutable.HashMap[(Int, Int), ArrayBuffer[Int]]()
        val vertTris = mutable.HashMap[Int, mutable.LinkedHashSet[Int]]()
        tris.indices.foreach { ti =>
            val t = tris(ti)
            (0 until 3).foreach { k =>
                edgeTris.getOrElseUpdate(ek(t(k), t((k + 1) % 3)), ArrayBuffer[Int]()) += ti
                vertTris.getOrElseUpdate(t(k), mutable.LinkedHashSet[Int]()) += ti
            }
        }
        val fixed = mutable.HashSet[(Int, Int)]()
        def fail() = throw new IllegalArgumentException(ConstrainedCrossing)
        def third(ti: Int, u: Int, v: Int): Int = tris(ti).find(w => w != u && w != v).get

        /** `(site, crossed)`: the edges a-b crosses from a, in order, up to b (site -1) or to the first site strictly
         *  inside a-b. */
        def walk(ia: Int, ib: Int): (Int, ArrayBuffer[(Int, Int)]) = {
            val a = xy(ia)
            val b = xy(ib)
            val dx = b.x - a.x
            val dy = b.y - a.y
            val reach = dx * dx + dy * dy
            var start: (Int, Int, Int) = null
            vertTris.getOrElse(ia, mutable.LinkedHashSet[Int]()).foreach { ti =>
                val pq = tris(ti).filter(_ != ia)
                pq.foreach { w =>
                    val p = xy(w)
                    if (orient2d(a, b, p) == 0.0) {
                        val along = (p.x - a.x) * dx + (p.y - a.y) * dy
                        if (0.0 < along && along < reach) return (w, ArrayBuffer[(Int, Int)]())
                    }
                }
                if (crosses(a, b, xy(pq(0)), xy(pq(1)))) start = (ti, pq(0), pq(1))
            }
            if (start == null) fail()
            var (ti, p, q) = start
            val crossed = ArrayBuffer[(Int, Int)]()
            var steps = 0
            while (steps <= tris.length) {
                steps += 1
                val e = ek(p, q)
                crossed += e
                val ts = edgeTris(e)
                if (ts.length != 2) fail()
                val tj = if (ts(0) == ti) ts(1) else ts(0)
                val r = third(tj, p, q)
                if (r == ib) return (-1, crossed)
                val o = orient2d(a, b, xy(r))
                if (o == 0.0) return (r, crossed)
                if ((o > 0.0) == (orient2d(a, b, xy(p)) > 0.0)) p = r else q = r
                ti = tj
            }
            fail()
        }

        def flip(u: Int, v: Int, w0: Int, w1: Int, t0: Int, t1: Int): Unit = {
            edgeTris.remove((u, v))
            tris(t0) = Array(w0, w1, u)
            tris(t1) = Array(w0, w1, v)
            val e0 = edgeTris(ek(v, w0))
            e0(e0.indexOf(t0)) = t1
            val e1 = edgeTris(ek(u, w1))
            e1(e1.indexOf(t1)) = t0
            edgeTris(ek(w0, w1)) = ArrayBuffer(t0, t1)
            vertTris(v) -= t0
            vertTris(w1) += t0
            vertTris(u) -= t1
            vertTris(w0) += t1
        }

        def sloan(ia: Int, ib: Int, crossed: ArrayBuffer[(Int, Int)]): Unit = {
            if (crossed.exists(fixed.contains)) fail()
            val a = xy(ia)
            val b = xy(ib)
            val queue = mutable.Queue[(Int, Int)](crossed.toSeq: _*)
            var budget = 10L * (crossed.length + 1).toLong * (crossed.length + 1).toLong + 100L
            while (queue.nonEmpty) {
                budget -= 1
                if (budget < 0) fail()
                val (u, v) = queue.dequeue()
                val ts = edgeTris((u, v))
                val t0 = ts(0)
                val t1 = ts(1)
                val w0 = third(t0, u, v)
                val w1 = third(t1, u, v)
                if (!crosses(xy(w0), xy(w1), xy(u), xy(v))) {
                    queue.enqueue((u, v)) // not convex: try again after the others
                } else {
                    flip(u, v, w0, w1, t0, t1)
                    if (crosses(a, b, xy(w0), xy(w1))) queue.enqueue(ek(w0, w1))
                }
            }
        }

        segments.foreach { case (ia, ib) =>
            var a = ia
            var done = false
            while (!done && a != ib) {
                if (edgeTris.contains(ek(a, ib))) {
                    fixed += ek(a, ib)
                    done = true
                } else {
                    val (r, crossed) = walk(a, ib)
                    val end = if (r < 0) ib else r
                    sloan(a, end, crossed)
                    fixed += ek(a, end)
                    a = end
                }
            }
        }
        tris
    }

    /** Regular grid of cell-center points over a bbox.
     *  Ordering: column-major (x index varies slowest, y index varies fastest).
     *  Cell size is derived: xRes = (xmax-xmin)/widthPx, yRes = (ymax-ymin)/heightPx.
     *  Centers: x = xmin + (i + 0.5)*xRes, y = ymin + (j + 0.5)*yRes.
     */
    def pointGridBBox(
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        widthPx: Int, heightPx: Int, srid: Int
    ): MultiPoint = {
        val xRes = (xmax - xmin) / widthPx
        val yRes = (ymax - ymin) / heightPx
        val pts = for (i <- 0 until widthPx; j <- 0 until heightPx) yield {
            val x = xmin + (i + 0.5) * xRes
            val y = ymin + (j + 0.5) * yRes
            val p = JTS.point(new Coordinate(x, y))
            p.setSRID(srid)
            p
        }
        val mp = JTS.multiPoint(pts.toArray)
        mp.setSRID(srid)
        mp
    }

    /** Grid of cell-center points from an origin corner + cell counts + per-cell sizes.
     *  Centers: x = originX + (i + 0.5)*cellSizeX, y = originY + (j + 0.5)*cellSizeY.
     *  cellSizeY may be negative (y-down). Column-major (x slowest, y fastest).
     */
    def pointGridOrigin(
        originX: Double, originY: Double, cols: Int, rows: Int,
        cellSizeX: Double, cellSizeY: Double, srid: Int
    ): MultiPoint = {
        val pts = for (i <- 0 until cols; j <- 0 until rows) yield {
            val p = JTS.point(new Coordinate(originX + (i + 0.5) * cellSizeX, originY + (j + 0.5) * cellSizeY))
            p.setSRID(srid); p
        }
        val mp = JTS.multiPoint(pts.toArray); mp.setSRID(srid); mp
    }

}
