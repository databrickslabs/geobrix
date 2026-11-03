package com.databricks.labs.gbx.vectorx.jts

/** Delaunay triangulation and Z interpolation for DTM. Used by the VectorX generators. */
import org.locationtech.jts.geom.util.{LinearComponentExtracter, PolygonExtracter}
import org.locationtech.jts.geom._
import org.locationtech.jts.index.strtree.STRtree
import org.locationtech.jts.linearref.LengthIndexedLine
import org.locationtech.jts.triangulate.DelaunayTriangulationBuilder

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
     *      triangulation conform to the constraints).
     *
     *  Both paths Z-snap vertices lying within `snapTolerance` of a constraint line.
     */
    def triangulate(
        multiPoint: Geometry,
        breaklines: Seq[Geometry],
        mergeTolerance: Double,
        snapTolerance: Double,
        splitPointFinder: Option[TriangulationSplitPointTypeEnum.Value] = None,
        mode: String = "constrained"
    ): Seq[Geometry] = mode match {
        case "conforming"  => triangulateConforming(multiPoint, breaklines, mergeTolerance, snapTolerance, splitPointFinder)
        case "constrained" => triangulateConstrained(multiPoint, breaklines, mergeTolerance, snapTolerance)
        case other         => throw new IllegalArgumentException(
            s"mode must be 'constrained' or 'conforming'; got '$other'")
    }

    /** Conforming path: JTS ConformingDelaunayTriangulator (may insert Steiner points). */
    private def triangulateConforming(
        multiPoint: Geometry,
        breaklines: Seq[Geometry],
        mergeTolerance: Double,
        snapTolerance: Double,
        splitPointFinder: Option[TriangulationSplitPointTypeEnum.Value]
    ): Seq[Geometry] = {
        val multiLineString = JTS.multiLineString(breaklines)
        val triangulator = JTSConformingDelaunayTriangulationBuilder(multiPoint)
        if (breaklines.nonEmpty) triangulator.setConstraints(multiLineString)
        triangulator.setTolerance(mergeTolerance)
        splitPointFinder.foreach(triangulator.setSplitPointFinder)

        val trianglesGeomCollection = triangulator.getTriangles
        val trianglePolygons = PolygonExtracter.getPolygons(trianglesGeomCollection).asScala.map(_.asInstanceOf[Polygon])

        val postProcessedTrianglePolygons = postProcessTriangulation(trianglePolygons.toSeq, multiLineString, snapTolerance)
        postProcessedTrianglePolygons
    }

    /** Constrained (no-Steiner) path. Faithful port of the pyvx light `triangulate`:
     *
     *  1. Site set = breakline coords (first, so keep-first dedup retains breakline Z) ++ multipoint
     *     coords; XY→Z=0 for 2D breakline coords; dedup by XY (keep-first).
     *  2. Unconstrained Delaunay over the XY of all sites (JTS `DelaunayTriangulationBuilder`).
     *  3. Sloan edge-flip recovery: force each breakline segment to be a triangle edge.
     *  4. Z-snap vertices within `snapTolerance` of a constraint line (reuses `postProcessTriangulation`).
     */
    private def triangulateConstrained(
        multiPoint: Geometry,
        breaklines: Seq[Geometry],
        mergeTolerance: Double,
        snapTolerance: Double
    ): Seq[Geometry] = {
        val gf = multiPoint.getFactory

        // --- 1. site coordinate set: breaklines first, then mass points, dedup by XY (keep-first) ---
        val blCoords: Seq[Coordinate] = breaklines.flatMap { bl =>
            bl.getCoordinates.map { c =>
                val z = if (java.lang.Double.isNaN(c.getZ)) 0.0 else c.getZ
                new Coordinate(c.x, c.y, z)
            }
        }
        val mpCoords: Seq[Coordinate] = multiPoint.getCoordinates.toSeq
        val sites = dedupByXY(blCoords ++ mpCoords, mergeTolerance)
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
        if (breaklines.nonEmpty) {
            triangles = recoverConstraints(sites, triangles, breaklines)
        }

        // --- build triangle polygons carrying site Z ---
        val polys: Seq[Geometry] = triangles.toSeq.map { t =>
            val ring = Array(sites(t(0)), sites(t(1)), sites(t(2)), sites(t(0)))
            gf.createPolygon(ring)
        }

        // --- 4. Z-snap (reuse the conforming post-process; equivalent to light _zsnap) ---
        if (snapTolerance > 0.0 && breaklines.nonEmpty) {
            val mls = JTS.multiLineString(breaklines)
            postProcessTriangulation(polys.map(_.asInstanceOf[Polygon]), mls, snapTolerance)
        } else {
            polys
        }
    }

    /** Drop coincident XY coordinates, keeping the first occurrence (preserves breakline Z).
     *  `tol > 0` snaps near-coincident coords to one representative; `tol <= 0` drops only exact
     *  duplicates (machine-eps grid). Mirrors light `_merge_vertices`. */
    private def dedupByXY(coords: Seq[Coordinate], tol: Double): Array[Coordinate] = {
        val scale = if (tol > 0.0) tol else 1e-12
        val seen = scala.collection.mutable.HashSet[(Long, Long)]()
        val out = scala.collection.mutable.ArrayBuffer[Coordinate]()
        coords.foreach { c =>
            val key = (math.rint(c.x / scale).toLong, math.rint(c.y / scale).toLong)
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
        breaklines: Seq[Geometry]
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

        breaklines.foreach { bl =>
            val segPts = bl.getCoordinates
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

    /** Snaps triangle vertices to constraint lines within tolerance and rebuilds polygons. */
    private def postProcessTriangulation(
        trianglePolygons: Seq[Polygon],
        constraintLineGeom: Geometry,
        tolerance: Double
    ): Seq[Polygon] = {
        val geomFact = constraintLineGeom.getFactory

        val constraintLines = LinearComponentExtracter
            .getLines(constraintLineGeom)
            .iterator()
            .asScala
            .toSeq
            .map(_.asInstanceOf[LineString])

        val constraintLinesTree = new STRtree(4)
        constraintLines.foreach(l => constraintLinesTree.insert(l.getEnvelopeInternal, l))

        trianglePolygons.map(t => {
            val coords = t.getCoordinates.map(c => {
                /*
                 * overwrite the z values for every coordinate lying
                 * within a fraction of the value of `tolerance`.
                 */
                val coordPoint = geomFact.createPoint(c)
                val originatingLineString = constraintLinesTree
                    .query(new Envelope(c))
                    .iterator()
                    .asScala
                    .toSeq
                    .map(_.asInstanceOf[LineString])
                    .find(l => l.intersects(coordPoint.buffer(tolerance)))
                originatingLineString match {
                    case Some(l) =>
                        val indexedLine = new LengthIndexedLine(l)
                        val index = indexedLine.indexOf(c)
                        indexedLine.extractPoint(index)
                    case None    => c
                }
            })
            geomFact.createPolygon(coords)
        })
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
