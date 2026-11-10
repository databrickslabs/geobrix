package com.databricks.labs.gbx.vectorx.jts

import org.locationtech.jts.geom.util.LinearComponentExtracter
import org.locationtech.jts.geom.{Coordinate, CoordinateList, Envelope, Geometry, LineString}
import org.locationtech.jts.triangulate._
import org.locationtech.jts.triangulate.quadedge.QuadEdgeSubdivision

import java.util
import java.util.Locale

/** Split-point strategy for conforming Delaunay triangulation. */
object TriangulationSplitPointTypeEnum extends Enumeration {
    val MIDPOINT: TriangulationSplitPointTypeEnum.Value = Value("MIDPOINT")
    val NONENCROACHING: TriangulationSplitPointTypeEnum.Value = Value("NONENCROACHING")

    def fromString(value: String): TriangulationSplitPointTypeEnum.Value =
        TriangulationSplitPointTypeEnum.values
            .find(_.toString == value.toUpperCase(Locale.ROOT))
            .getOrElse(
              throw new Error(
                s"Invalid mode for triangulation split point type: $value." +
                    s" Must be one of ${TriangulationSplitPointTypeEnum.values.mkString(",")}"
              )
            )
}

/** Places a split point with `inner`, then gives it the Z of the segment it splits, interpolated linearly at its
 *  projection factor on the segment (`r = ((x - x0) * dx + (y - y0) * dy) / (dx * dx + dy * dy)`, `z = z0 + r * (z1 - z0)`).
 *  JTS leaves a midpoint (and a point placed near an endpoint) without Z, and a projected point with the Z of the point
 *  that encroached; the surface along a break line is the line's own. The Python level does the same operations in the
 *  same order (`pyvx/_conforming.py`), so both agree bit for bit.
 */
final class LineZSplitPointFinder(inner: ConstraintSplitPointFinder) extends ConstraintSplitPointFinder {
    override def findSplitPoint(seg: Segment, encroachPt: Coordinate): Coordinate = {
        val p = inner.findSplitPoint(seg, encroachPt)
        val a = seg.getStart
        val b = seg.getEnd
        val dx = b.x - a.x
        val dy = b.y - a.y
        val r = ((p.x - a.x) * dx + (p.y - a.y) * dy) / (dx * dx + dy * dy)
        new Coordinate(p.x, p.y, a.getZ + r * (b.getZ - a.getZ))
    }
}

/** Builds a conforming Delaunay triangulation from a geometry (and optional constraint lines). Used by InterpolateElevation. */
class JTSConformingDelaunayTriangulationBuilder(geom: Geometry) {

    /** Tolerance for constraint enforcement. */
    var tolerance: Double = 0.0
    /** Maps coordinates to constraint vertices for the triangulator. */
    val constraintVertexMap = new util.HashMap[Coordinate, ConstraintVertex]
    /** Optional line geometry to enforce as edges in the triangulation. */
    var constraintLines: Geometry = _
    /** How to split encroaching segments (midpoint vs non-encroaching). */
    var splitPointFinder: ConstraintSplitPointFinder = _

    /** Unique coordinates from the input geometry (sites for Delaunay). */
    def siteCoords: CoordinateList = DelaunayTriangulationBuilder.extractUniqueCoordinates(geom)

    /** Envelope of the site coordinates. */
    def siteEnv: Envelope = DelaunayTriangulationBuilder.envelope(siteCoords)

    /** Builds constraint vertices for coordinates not already in constraintVertexMap. */
    private def createSiteVertices(coords: CoordinateList): util.ArrayList[ConstraintVertex] = {
        val vertices = new util.ArrayList[ConstraintVertex]
        coords.toCoordinateArray
            .filter(coord => !constraintVertexMap.containsKey(coord))
            .map(new ConstraintVertex(_))
            .foreach(v => {
                vertices.add(v)
            })
        vertices
    }

    /** Sets the tolerance used by the conforming triangulator. */
    def setTolerance(tolerance: Double): Unit = {
        this.tolerance = tolerance
    }

    /** Sets the line geometry to enforce as constraints. */
    def setConstraints(constraintLines: Geometry): Unit = {
        this.constraintLines = constraintLines
    }

    /** Sets the split-point strategy (midpoint or non-encroaching). */
    def setSplitPointFinder(splitPointFinder: TriangulationSplitPointTypeEnum.Value): Unit = {
        this.splitPointFinder = splitPointFinder match {
            case TriangulationSplitPointTypeEnum.MIDPOINT       => new MidpointSplitPointFinder
            case TriangulationSplitPointTypeEnum.NONENCROACHING => new NonEncroachingSplitPointFinder
        }
    }

    /** Adds all coordinates of geom to constraintVertexMap as ConstraintVertices. */
    def createVertices(geom: Geometry): Unit = {
        geom.getCoordinates.foreach(coord => {
            val v = new ConstraintVertex(coord)
            constraintVertexMap.put(coord, v)
        })
    }

    /** Extracts line segments from geometry (LinearComponentExtracter) into a list of Segments. */
    def createConstraintSegments(geometry: Geometry): util.ArrayList[Segment] = {
        val constraintSegs = new util.ArrayList[Segment]
        LinearComponentExtracter
            .getLines(geometry)
            .toArray
            .map(_.asInstanceOf[LineString])
            .filter(!_.isEmpty)
            .foreach(l => createConstraintSegments(l, constraintSegs))
        constraintSegs
    }

    /** Appends segments from consecutive coordinates of line to constraintSegs. */
    def createConstraintSegments(line: LineString, constraintSegs: util.ArrayList[Segment]): Unit = {
        val coords = line.getCoordinates
        coords
            .zip(coords.tail)
            .map(c => new Segment(c._1, c._2))
            .foreach(constraintSegs.add)
    }

    /** Builds the conforming Delaunay subdivision (sites + optional constraints). */
    def create(): QuadEdgeSubdivision = {
        var segments: util.ArrayList[Segment] = null
        if (constraintLines != null) {
            siteEnv.expandToInclude(constraintLines.getEnvelopeInternal)
            createVertices(constraintLines)
            segments = createConstraintSegments(constraintLines)
        }
        val sites = createSiteVertices(siteCoords)
        val cdt = new ConformingDelaunayTriangulator(sites, tolerance)
        // The caller's rule, or JTS's own default (NonEncroachingSplitPointFinder), places each extra point; either way
        // the point takes the Z of the break line segment it splits.
        val finder = Option(splitPointFinder).getOrElse(new NonEncroachingSplitPointFinder)
        cdt.setSplitPointFinder(new LineZSplitPointFinder(finder))

        cdt.setConstraints(segments, new util.ArrayList(constraintVertexMap.values()))
        cdt.formInitialDelaunay()
        if (constraintLines != null) { cdt.enforceConstraints() }
        cdt.getSubdivision
    }

    /** Returns the triangulation as a JTS geometry (collection of triangles). */
    def getTriangles: Geometry = {
        val subdiv = create()
        subdiv.getTriangles(geom.getFactory)
    }

}

object JTSConformingDelaunayTriangulationBuilder {
    /** Builds a new triangulation builder for the given geometry. */
    def apply(geom: Geometry): JTSConformingDelaunayTriangulationBuilder = new JTSConformingDelaunayTriangulationBuilder(geom)
}
