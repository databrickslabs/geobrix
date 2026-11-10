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

    /** Builds triangulation from multipoint and breaklines, then interpolates Z for each grid point. */
    def interpolate(
        multipoint: MultiPoint,
        breaklines: Seq[LineString],
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
     *      forces each breakline segment to be a triangle edge where the quad is convex (un-recoverable
     *      segments are left as-is). Mirrors the pyvx light tier (`pyvx/_tin.py`).
     *    - `"conforming"` — JTS `ConformingDelaunayTriangulator` (may insert Steiner points to make the
     *      triangulation conform to the constraints). Mirrors `pyvx/_conforming.py`.
     *
     *  Both modes first run [[prepareSites]] (Z of 2D coordinates, then the snap of `snapTolerance`), the steps
     *  documented once in `pyvx/_tin.py` `prepare_sites`.
     */
    def triangulate(
        multiPoint: Geometry,
        breaklines: Seq[Geometry],
        mergeTolerance: Double,
        snapTolerance: Double,
        splitPointFinder: Option[TriangulationSplitPointTypeEnum.Value] = None,
        mode: String = "constrained"
    ): Seq[Geometry] = {
        if (mode != "conforming" && mode != "constrained") {
            throw new IllegalArgumentException(s"mode must be 'constrained' or 'conforming'; got '$mode'")
        }
        val (points, lines) = prepareSites(multiPoint, breaklines, mergeTolerance, snapTolerance)
        val gf = multiPoint.getFactory
        if (mode == "conforming") triangulateConforming(points, lines, mergeTolerance, splitPointFinder, gf)
        else triangulateConstrained(points, lines, mergeTolerance, gf)
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

    /** Constrained (no-Steiner) path. Faithful port of the pyvx light `triangulate`:
     *
     *  1. Site set = breakline coords (first, so keep-first dedup retains breakline Z) ++ mass points;
     *     dedup by XY (keep-first).
     *  2. Unconstrained Delaunay over the XY of all sites (JTS `DelaunayTriangulationBuilder`).
     *  3. Sloan edge-flip recovery: force each breakline segment to be a triangle edge.
     */
    private def triangulateConstrained(
        points: Array[Coordinate],
        lines: Seq[Array[Coordinate]],
        mergeTolerance: Double,
        gf: GeometryFactory
    ): Seq[Geometry] = {
        // --- 1. site coordinate set: breaklines first, then mass points, dedup by XY (keep-first) ---
        val sites = dedupByXY(lines.flatten ++ points.toSeq, mergeTolerance)
        if (sites.length < 3) return Seq.empty[Geometry]

        // --- 2. initial unconstrained Delaunay over XY ---
        val builder = new DelaunayTriangulationBuilder()
        builder.setSites(java.util.Arrays.asList(sites: _*))
        val triGeomColl = builder.getTriangles(gf)
        val initialPolys =
            PolygonExtracter.getPolygons(triGeomColl).asScala.map(_.asInstanceOf[Polygon]).toSeq

        // Map each triangle's vertices back to site indices (XY match against the site set).
        var triangles: Array[Array[Int]] = initialPolys.flatMap { poly =>
            val cs = poly.getCoordinates
            // exterior ring is 4 coords (closing repeat); take the first 3 distinct
            val idx = Array(vertexIndex(sites, cs(0)), vertexIndex(sites, cs(1)), vertexIndex(sites, cs(2)))
            if (idx.contains(-1)) None else Some(idx)
        }.toArray

        // --- 3. Sloan constraint recovery ---
        if (lines.nonEmpty) {
            triangles = recoverConstraints(sites, triangles, lines)
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
     *  Every coordinate without Z takes Z 0. When `snapTolerance` > 0 and a break line has a segment, the mass
     *  points are merged (`dedupByXY`), then [[snapToBreaklines]] moves each one within reach onto its nearest break
     *  line. A mirror of `pyvx/_tin.py` `prepare_sites`, whose docstring states the rules.
     */
    private[jts] def prepareSites(
        multiPoint: Geometry,
        breaklines: Seq[Geometry],
        mergeTolerance: Double,
        snapTolerance: Double
    ): (Array[Coordinate], Seq[Array[Coordinate]]) = {
        val points = multiPoint.getCoordinates.map(withZ)
        val lines = breaklines
            .flatMap(bl => LinearComponentExtracter.getLines(bl).asScala.map(_.asInstanceOf[LineString]))
            .filter(!_.isEmpty)
            .map(_.getCoordinates.map(withZ))
        if (snapTolerance > 0.0 && lines.exists(_.length >= 2)) {
            snapToBreaklines(dedupByXY(points.toSeq, mergeTolerance), lines, mergeTolerance, snapTolerance)
        } else {
            (points, lines)
        }
    }

    /** `org.locationtech.jts.algorithm.Distance.pointToSegment`, spelled out so the Python level can repeat each
     *  operation in the same order (`pyvx/_tin.py` `_point_segment_distance`). */
    private def pointToSegment(px: Double, py: Double, ax: Double, ay: Double, bx: Double, by: Double): Double = {
        if (ax == bx && ay == by) return Math.hypot(px - ax, py - ay)
        val len2 = (bx - ax) * (bx - ax) + (by - ay) * (by - ay)
        val r = ((px - ax) * (bx - ax) + (py - ay) * (by - ay)) / len2
        if (r <= 0.0) return Math.hypot(px - ax, py - ay)
        if (r >= 1.0) return Math.hypot(px - bx, py - by)
        val s = ((ay - py) * (bx - ax) - (ax - px) * (by - ay)) / len2
        Math.abs(s) * Math.sqrt(len2)
    }

    /** The point of segment a-b nearest p, with the segment's Z interpolated there, and its projection factor. */
    private def project(p: Coordinate, a: Coordinate, b: Coordinate): (Double, Coordinate) = {
        if (a.x == b.x && a.y == b.y) return (0.0, a)
        val len2 = (b.x - a.x) * (b.x - a.x) + (b.y - a.y) * (b.y - a.y)
        val r = ((p.x - a.x) * (b.x - a.x) + (p.y - a.y) * (b.y - a.y)) / len2
        if (r <= 0.0) (0.0, a)
        else if (r >= 1.0) (1.0, b)
        else (r, new Coordinate(a.x + r * (b.x - a.x), a.y + r * (b.y - a.y), a.getZ + r * (b.getZ - a.getZ)))
    }

    /** The break line vertices, to ask whether a point is within `tol` of one (`tol` <= 0: the same X and Y). */
    private final class VertexIndex(tol: Double) {
        private val exact = scala.collection.mutable.HashSet[(Double, Double)]()
        private val cells = scala.collection.mutable.HashMap[(Long, Long), ArrayBuffer[Coordinate]]()
        private def key(v: Double): Long = Math.floor(v / tol).toLong

        def add(c: Coordinate): Unit =
            if (tol > 0.0) cells.getOrElseUpdate((key(c.x), key(c.y)), ArrayBuffer[Coordinate]()) += c
            else exact += ((c.x + 0.0, c.y + 0.0))

        def near(c: Coordinate): Boolean =
            if (tol > 0.0) {
                val kx = key(c.x)
                val ky = key(c.y)
                (-2 to 2).exists(i =>
                    (-2 to 2).exists(j =>
                        cells.get((kx + i, ky + j)).exists(_.exists(v => Math.hypot(c.x - v.x, c.y - v.y) <= tol))
                    )
                )
            } else {
                exact.contains((c.x + 0.0, c.y + 0.0))
            }
    }

    /** Moves each mass point within `snapTol` of a break line onto the nearest one (see `prepare_sites`): returns the
     *  mass points left and the break lines with the snapped points inserted as vertices. */
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
        val vertices = new VertexIndex(mergeTol)
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
                if (!vertices.near(q)) {
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
        (kept.toArray, out)
    }

    /** Drop coincident XY coordinates, keeping the first occurrence (preserves breakline Z).
     *  `tol > 0` merges the coordinates in one cell of the `tol` grid (rounded half to even; `toLong`
     *  saturates); `tol <= 0` drops only coordinates with the same X and Y. Mirrors light `_merge_vertices`. */
    private def dedupByXY(coords: Seq[Coordinate], tol: Double): Array[Coordinate] = {
        val seen = scala.collection.mutable.HashSet[(Any, Any)]()
        val out = scala.collection.mutable.ArrayBuffer[Coordinate]()
        coords.foreach { c =>
            val key: (Any, Any) =
                if (tol > 0.0) (math.rint(c.x / tol).toLong, math.rint(c.y / tol).toLong)
                else (c.x + 0.0, c.y + 0.0)
            if (!seen.contains(key)) {
                seen += key
                out += c
            }
        }
        out.toArray
    }

    /** >0 if a->b->c is counter-clockwise (2D orientation). Mirrors light `_orient2d`. */
    private def orient2d(a: Coordinate, b: Coordinate, c: Coordinate): Double =
        (b.x - a.x) * (c.y - a.y) - (b.y - a.y) * (c.x - a.x)

    /** True iff the open segments (p1,p2) and (p3,p4) properly cross. Mirrors light `_segments_intersect`. */
    private def segmentsIntersect(p1: Coordinate, p2: Coordinate, p3: Coordinate, p4: Coordinate): Boolean = {
        val d1 = orient2d(p3, p4, p1)
        val d2 = orient2d(p3, p4, p2)
        val d3 = orient2d(p1, p2, p3)
        val d4 = orient2d(p1, p2, p4)
        ((d1 > 0) != (d2 > 0)) && ((d3 > 0) != (d4 > 0))
    }

    /** Index of the site nearest `p` in XY, or -1 if none within tolerance. Mirrors light `_vertex_index`. */
    private def vertexIndex(sites: Array[Coordinate], p: Coordinate, tol: Double = 1e-9): Int = {
        var best = -1
        var bestD = Double.MaxValue
        var i = 0
        while (i < sites.length) {
            val dx = sites(i).x - p.x
            val dy = sites(i).y - p.y
            val d = math.hypot(dx, dy)
            if (d < bestD) { bestD = d; best = i }
            i += 1
        }
        if (best >= 0 && bestD <= math.max(tol, 1e-9)) best else -1
    }

    /** Sloan recovery: force each breakline segment to be a triangle edge via convex-quad edge flips.
     *  Bounded flip budget; raises on non-termination. Faithful port of light `_recover_constraints`. */
    private def recoverConstraints(
        sites: Array[Coordinate],
        initial: Array[Array[Int]],
        breaklines: Seq[Array[Coordinate]]
    ): Array[Array[Int]] = {
        val triangles = scala.collection.mutable.ArrayBuffer[Array[Int]](initial.map(_.clone()): _*)

        def edgesOf(t: Array[Int]): Seq[Set[Int]] =
            Seq(Set(t(0), t(1)), Set(t(1), t(2)), Set(t(2), t(0)))

        def buildAdj(): scala.collection.mutable.LinkedHashMap[Set[Int], scala.collection.mutable.ArrayBuffer[Int]] = {
            val adj = scala.collection.mutable.LinkedHashMap[Set[Int], scala.collection.mutable.ArrayBuffer[Int]]()
            var ti = 0
            while (ti < triangles.length) {
                edgesOf(triangles(ti)).foreach { e =>
                    adj.getOrElseUpdate(e, scala.collection.mutable.ArrayBuffer[Int]()) += ti
                }
                ti += 1
            }
            adj
        }

        breaklines.foreach { segPts =>
            var k = 0
            while (k < segPts.length - 1) {
                val ia = vertexIndex(sites, segPts(k))
                val ib = vertexIndex(sites, segPts(k + 1))
                if (ia >= 0 && ib >= 0 && ia != ib) {
                    val target = Set(ia, ib)
                    var budget = 50 * triangles.length + 100
                    var done = false
                    while (budget > 0 && !done) {
                        budget -= 1
                        val adj = buildAdj()
                        if (adj.contains(target)) {
                            done = true
                        } else {
                            var flipped = false
                            val it = adj.iterator
                            while (it.hasNext && !flipped) {
                                val (e, ts) = it.next()
                                if (ts.length == 2) {
                                    val ev = e.toSeq
                                    val u = ev(0)
                                    val v = ev(1)
                                    if (segmentsIntersect(sites(ia), sites(ib), sites(u), sites(v))) {
                                        val t0 = triangles(ts(0))
                                        val t1 = triangles(ts(1))
                                        val w0 = t0.find(!e.contains(_)).get
                                        val w1 = t1.find(!e.contains(_)).get
                                        if (segmentsIntersect(sites(w0), sites(w1), sites(u), sites(v))) {
                                            // strict-progress guard: only flip when the NEW diagonal no
                                            // longer crosses the constraint (guarantees termination).
                                            val newEdge = Set(w0, w1)
                                            if (newEdge == target ||
                                                !segmentsIntersect(sites(ia), sites(ib), sites(w0), sites(w1))) {
                                                triangles(ts(0)) = Array(w0, w1, u)
                                                triangles(ts(1)) = Array(w0, w1, v)
                                                flipped = true
                                            }
                                        }
                                    }
                                }
                            }
                            if (!flipped) done = true // genuine stall: leave segment un-recovered (no Steiner)
                        }
                    }
                    if (budget <= 0) throw new RuntimeException("Sloan constraint recovery did not terminate")
                }
                k += 1
            }
        }
        triangles.toArray
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
