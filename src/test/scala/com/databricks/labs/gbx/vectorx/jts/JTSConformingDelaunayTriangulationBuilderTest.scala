package com.databricks.labs.gbx.vectorx.jts

import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class JTSConformingDelaunayTriangulationBuilderTest extends AnyFunSuite {

    val gf = new GeometryFactory()

    // ====== Construction ======

    test("should construct with Geometry") {
        val point = gf.createPoint(new Coordinate(0.0, 0.0))
        val builder = new JTSConformingDelaunayTriangulationBuilder(point)
        builder should not be null
    }

    test("companion object apply should construct builder") {
        val point = gf.createPoint(new Coordinate(0.0, 0.0))
        val builder = JTSConformingDelaunayTriangulationBuilder(point)
        builder should not be null
        builder shouldBe a[JTSConformingDelaunayTriangulationBuilder]
    }

    // ====== Property Access ======

    test("siteCoords should return CoordinateList") {
        val point = gf.createPoint(new Coordinate(1.0, 2.0))
        val builder = JTSConformingDelaunayTriangulationBuilder(point)
        val coords = builder.siteCoords
        coords should not be null
        coords.size() should be > 0
    }

    test("siteEnv should return Envelope") {
        val point = gf.createPoint(new Coordinate(1.0, 2.0))
        val builder = JTSConformingDelaunayTriangulationBuilder(point)
        val env = builder.siteEnv
        env should not be null
    }

    test("constraintVertexMap should be initialized") {
        val point = gf.createPoint(new Coordinate(0.0, 0.0))
        val builder = JTSConformingDelaunayTriangulationBuilder(point)
        builder.constraintVertexMap should not be null
    }

    // ====== Setters ======

    test("setTolerance should accept tolerance value") {
        val point = gf.createPoint(new Coordinate(0.0, 0.0))
        val builder = JTSConformingDelaunayTriangulationBuilder(point)
        noException should be thrownBy builder.setTolerance(0.001)
        builder.tolerance shouldBe 0.001
    }

    test("setConstraints should accept LineString") {
        val point = gf.createPoint(new Coordinate(0.0, 0.0))
        val line = gf.createLineString(Array(
          new Coordinate(0.0, 0.0),
          new Coordinate(1.0, 1.0)
        ))
        val builder = JTSConformingDelaunayTriangulationBuilder(point)
        noException should be thrownBy builder.setConstraints(line)
        builder.constraintLines should not be null
    }

    test("setSplitPointFinder should accept MIDPOINT") {
        import com.databricks.labs.gbx.rasterx.operations.InterpolateElevation.TriangulationSplitPointTypeEnum
        val point = gf.createPoint(new Coordinate(0.0, 0.0))
        val builder = JTSConformingDelaunayTriangulationBuilder(point)
        noException should be thrownBy builder.setSplitPointFinder(TriangulationSplitPointTypeEnum.MIDPOINT)
        builder.splitPointFinder should not be null
    }

    test("setSplitPointFinder should accept NONENCROACHING") {
        import com.databricks.labs.gbx.rasterx.operations.InterpolateElevation.TriangulationSplitPointTypeEnum
        val point = gf.createPoint(new Coordinate(0.0, 0.0))
        val builder = JTSConformingDelaunayTriangulationBuilder(point)
        noException should be thrownBy builder.setSplitPointFinder(TriangulationSplitPointTypeEnum.NONENCROACHING)
        builder.splitPointFinder should not be null
    }

    // ====== createVertices ======

    test("createVertices should process geometry") {
        val line = gf.createLineString(Array(
          new Coordinate(0.0, 0.0),
          new Coordinate(1.0, 1.0)
        ))
        val builder = JTSConformingDelaunayTriangulationBuilder(line)
        noException should be thrownBy builder.createVertices(line)
        builder.constraintVertexMap.size() should be > 0
    }

    // ====== createConstraintSegments ======

    test("createConstraintSegments should create segments from LineString") {
        val line = gf.createLineString(Array(
          new Coordinate(0.0, 0.0),
          new Coordinate(1.0, 1.0),
          new Coordinate(2.0, 2.0)
        ))
        val builder = JTSConformingDelaunayTriangulationBuilder(line)
        val segments = builder.createConstraintSegments(line)
        segments should not be null
        segments.size() shouldBe 2
    }

    test("createConstraintSegments should handle Polygon") {
        val poly = gf.createPolygon(Array(
          new Coordinate(0.0, 0.0),
          new Coordinate(1.0, 0.0),
          new Coordinate(1.0, 1.0),
          new Coordinate(0.0, 1.0),
          new Coordinate(0.0, 0.0)
        ))
        val builder = JTSConformingDelaunayTriangulationBuilder(poly)
        val segments = builder.createConstraintSegments(poly)
        segments should not be null
        segments.size() should be > 0
    }

    test("createConstraintSegments should skip empty LineStrings") {
        val emptyLine = gf.createLineString()
        val builder = JTSConformingDelaunayTriangulationBuilder(emptyLine)
        val segments = builder.createConstraintSegments(emptyLine)
        segments should not be null
        segments.size() shouldBe 0
    }

    // ====== create() - Basic Structure Tests ======

    test("create should return QuadEdgeSubdivision for simple geometry") {
        val coords = Array(
          new Coordinate(0.0, 0.0),
          new Coordinate(1.0, 0.0),
          new Coordinate(0.5, 1.0)
        )
        val multiPoint = gf.createMultiPointFromCoords(coords)
        val builder = JTSConformingDelaunayTriangulationBuilder(multiPoint)
        val subdiv = builder.create()
        subdiv should not be null
    }

    test("create should handle constrained triangulation") {
        val coords = Array(
          new Coordinate(0.0, 0.0),
          new Coordinate(1.0, 0.0),
          new Coordinate(0.5, 1.0)
        )
        val multiPoint = gf.createMultiPointFromCoords(coords)
        val constraints = gf.createLineString(Array(
          new Coordinate(0.0, 0.0),
          new Coordinate(1.0, 0.0)
        ))
        val builder = JTSConformingDelaunayTriangulationBuilder(multiPoint)
        builder.setConstraints(constraints)
        val subdiv = builder.create()
        subdiv should not be null
    }

    // ====== getTriangles ======

    test("getTriangles should return Geometry") {
        val coords = Array(
          new Coordinate(0.0, 0.0),
          new Coordinate(1.0, 0.0),
          new Coordinate(0.5, 1.0)
        )
        val multiPoint = gf.createMultiPointFromCoords(coords)
        val builder = JTSConformingDelaunayTriangulationBuilder(multiPoint)
        val triangles = builder.getTriangles
        triangles should not be null
    }

}
