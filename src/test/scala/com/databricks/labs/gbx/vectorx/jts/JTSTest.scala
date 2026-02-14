package com.databricks.labs.gbx.vectorx.jts

import org.locationtech.jts.geom.{Coordinate, Point, Polygon}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class JTSTest extends AnyFunSuite {

    // ====== Point Creation ======

    test("point should create Point from x, y coordinates") {
        val pt = JTS.point(1.0, 2.0)
        pt should not be null
        pt shouldBe a[Point]
        pt.getX shouldBe 1.0
        pt.getY shouldBe 2.0
    }

    test("point should create Point from Coordinate") {
        val coord = new Coordinate(3.0, 4.0)
        val pt = JTS.point(coord)
        pt should not be null
        pt shouldBe a[Point]
        pt.getX shouldBe 3.0
        pt.getY shouldBe 4.0
    }

    test("point should handle negative coordinates") {
        val pt = JTS.point(-10.5, -20.3)
        pt.getX shouldBe -10.5
        pt.getY shouldBe -20.3
    }

    // ====== Coordinate Creation ======

    test("coordinatesFromXYs should create Coordinate") {
        val coord = JTS.coordinatesFromXYs(5.5, 6.6)
        coord should not be null
        coord shouldBe a[Coordinate]
        coord.getX shouldBe 5.5
        coord.getY shouldBe 6.6
    }

    // ====== Polygon Creation ======

    test("polygonFromPoints should create Polygon from Points") {
        val points = Array(
          JTS.point(0.0, 0.0),
          JTS.point(1.0, 0.0),
          JTS.point(1.0, 1.0),
          JTS.point(0.0, 1.0),
          JTS.point(0.0, 0.0)
        )
        val poly = JTS.polygonFromPoints(points)
        poly should not be null
        poly shouldBe a[Polygon]
        poly.getNumPoints shouldBe 5
    }

    test("polygonFromCoords should create Polygon from Coordinates") {
        val coords = Array(
          new Coordinate(0.0, 0.0),
          new Coordinate(1.0, 0.0),
          new Coordinate(1.0, 1.0),
          new Coordinate(0.0, 1.0),
          new Coordinate(0.0, 0.0)
        )
        val poly = JTS.polygonFromCoords(coords)
        poly should not be null
        poly shouldBe a[Polygon]
    }

    test("polygonFromXYs should create Polygon from coordinate tuples") {
        val xys = Array(
          (0.0, 0.0),
          (1.0, 0.0),
          (1.0, 1.0),
          (0.0, 1.0),
          (0.0, 0.0)
        )
        val poly = JTS.polygonFromXYs(xys)
        poly should not be null
        poly shouldBe a[Polygon]
        poly.getNumPoints shouldBe 5
    }

    // ====== MultiPolygon Creation ======

    test("multiPolygonFromXYs should create MultiPolygon") {
        val polygons = Array(
          Array((0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)),
          Array((2.0, 2.0), (3.0, 2.0), (3.0, 3.0), (2.0, 3.0), (2.0, 2.0))
        )
        val multiPoly = JTS.multiPolygonFromXYs(polygons)
        multiPoly should not be null
        multiPoly.getNumGeometries shouldBe 2
    }

    test("multiPolygonFromXYs should handle single polygon") {
        val polygons = Array(
          Array((0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0))
        )
        val multiPoly = JTS.multiPolygonFromXYs(polygons)
        multiPoly should not be null
        multiPoly.getNumGeometries shouldBe 1
    }

    // ====== LineString Creation ======

    test("lineStringXYs should create LineString from buffer") {
        import scala.collection.mutable
        val xys = mutable.Buffer((0.0, 0.0), (1.0, 1.0), (2.0, 2.0))
        val line = JTS.lineStringXYs(xys)
        line should not be null
        line.getNumPoints shouldBe 3
    }

    test("lineStringXYs should handle two points") {
        import scala.collection.mutable
        val xys = mutable.Buffer((0.0, 0.0), (1.0, 1.0))
        val line = JTS.lineStringXYs(xys)
        line should not be null
        line.getNumPoints shouldBe 2
    }

    // ====== MultiLineString Creation ======

    test("multiLineString should create empty MultiLineString for empty sequence") {
        val multiLine = JTS.multiLineString(Seq.empty)
        multiLine should not be null
        multiLine.isEmpty shouldBe true
    }

    test("multiLineString should create MultiLineString from LineStrings") {
        import scala.collection.mutable
        val line1 = JTS.lineStringXYs(mutable.Buffer((0.0, 0.0), (1.0, 1.0)))
        val line2 = JTS.lineStringXYs(mutable.Buffer((2.0, 2.0), (3.0, 3.0)))
        val multiLine = JTS.multiLineString(Seq(line1, line2))
        multiLine should not be null
        multiLine.getNumGeometries shouldBe 2
    }

    // ====== MultiPoint Creation ======

    test("multiPoint should create MultiPoint from Points") {
        val points: Array[org.locationtech.jts.geom.Geometry] = Array(JTS.point(0.0, 0.0), JTS.point(1.0, 1.0))
        val multiPoint = JTS.multiPoint(points)
        multiPoint should not be null
        multiPoint.getNumPoints shouldBe 2
    }

    test("multiPoint should handle single point") {
        val points: Array[org.locationtech.jts.geom.Geometry] = Array(JTS.point(0.0, 0.0))
        val multiPoint = JTS.multiPoint(points)
        multiPoint should not be null
        multiPoint.getNumPoints shouldBe 1
    }

    // ====== Geometry Operations ======

    test("translate should move geometry") {
        val pt = JTS.point(1.0, 2.0)
        val translated = JTS.translate(10.0, 20.0, pt)
        translated should not be null
        translated.getCoordinate.getX shouldBe 11.0
        translated.getCoordinate.getY shouldBe 22.0
    }

    test("translate should handle negative offsets") {
        val pt = JTS.point(10.0, 20.0)
        val translated = JTS.translate(-5.0, -10.0, pt)
        translated.getCoordinate.getX shouldBe 5.0
        translated.getCoordinate.getY shouldBe 10.0
    }

    test("anyPoint should extract point from geometry") {
        val poly = JTS.polygonFromXYs(Array((0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)))
        val pt = JTS.anyPoint(poly)
        pt should not be null
        pt shouldBe a[Point]
    }

    test("simplify should simplify geometry with tolerance") {
        val poly = JTS.polygonFromXYs(Array(
          (0.0, 0.0), (0.5, 0.1), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)
        ))
        poly.setSRID(4326)
        val simplified = JTS.simplify(poly, 0.2)
        simplified should not be null
        simplified.getSRID shouldBe 4326
    }

    // ====== WKT Operations ======

    test("fromWKT should parse POINT") {
        val geom = JTS.fromWKT("POINT (1 2)")
        geom should not be null
        geom shouldBe a[Point]
    }

    test("fromWKT should parse POLYGON") {
        val geom = JTS.fromWKT("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))")
        geom should not be null
        geom shouldBe a[Polygon]
    }

    test("toWKT should convert Point to WKT") {
        val pt = JTS.point(1.0, 2.0)
        val wkt = JTS.toWKT(pt)
        wkt should not be null
        wkt should include("POINT")
    }

    test("toWKT should convert Polygon to WKT") {
        val poly = JTS.polygonFromXYs(Array((0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)))
        val wkt = JTS.toWKT(poly)
        wkt should not be null
        wkt should include("POLYGON")
    }

    test("emptyPolygon should create empty POLYGON") {
        val empty = JTS.emptyPolygon
        empty should not be null
        empty.isEmpty shouldBe true
    }

    // ====== WKB Operations ======

    test("toWKB should convert geometry to bytes") {
        val pt = JTS.point(1.0, 2.0)
        val wkb = JTS.toWKB(pt)
        wkb should not be null
        wkb.length should be > 0
    }

    test("fromWKB should parse geometry from bytes") {
        val pt = JTS.point(1.0, 2.0)
        val wkb = JTS.toWKB(pt)
        val parsed = JTS.fromWKB(wkb)
        parsed should not be null
        parsed shouldBe a[Point]
    }

    test("fromWKB and toWKB should round-trip") {
        val poly = JTS.polygonFromXYs(Array((0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)))
        val wkb = JTS.toWKB(poly)
        val parsed = JTS.fromWKB(wkb)
        parsed should not be null
        parsed shouldBe a[Polygon]
    }

}
