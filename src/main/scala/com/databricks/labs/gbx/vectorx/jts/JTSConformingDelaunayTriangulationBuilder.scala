package com.databricks.labs.gbx.vectorx.jts

import com.databricks.labs.gbx.rasterx.operations.InterpolateElevation.TriangulationSplitPointTypeEnum
import org.locationtech.jts.geom.util.LinearComponentExtracter
import org.locationtech.jts.geom.{Coordinate, CoordinateList, Envelope, Geometry, LineString}
import org.locationtech.jts.triangulate._
import org.locationtech.jts.triangulate.quadedge.QuadEdgeSubdivision

import java.util

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
