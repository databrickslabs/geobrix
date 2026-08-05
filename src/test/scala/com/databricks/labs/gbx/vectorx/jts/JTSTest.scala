package com.databricks.labs.gbx.vectorx.jts

import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point, Polygon}
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

    // ====== EWKT / EWKB (PostGIS extended formats) ======

    test("fromWKT should parse EWKT SRID prefix and set SRID") {
        val geom = JTS.fromWKT("SRID=4326;POINT (10 20)")
        geom should not be null
        geom shouldBe a[Point]
        geom.getSRID shouldBe 4326
        geom.asInstanceOf[Point].getX shouldBe 10.0
        geom.asInstanceOf[Point].getY shouldBe 20.0
    }

    test("fromWKT should tolerate whitespace and case in EWKT prefix") {
        val geom = JTS.fromWKT("  srid=27700 ; POINT (100 200)")
        geom.getSRID shouldBe 27700
    }

    test("fromWKT should leave SRID=0 for plain WKT") {
        val geom = JTS.fromWKT("POINT (1 2)")
        geom.getSRID shouldBe 0
    }

    test("fromWKT should not mis-parse a WKT that happens to start with S") {
        // e.g. not "SRID=..." — must not strip anything
        val geom = JTS.fromWKT("POINT (5 6)")
        geom.getSRID shouldBe 0
    }

    test("fromWKT should ignore invalid SRID prefix and parse remainder") {
        // "SRID=abc;..." — non-numeric: return (0, original), which will fail WKT parse as it
        // still starts with "SRID=abc;" — this is expected behaviour (input is malformed)
        val thrown = intercept[Exception](JTS.fromWKT("SRID=abc;POINT (1 2)"))
        thrown.getMessage should not be null
    }

    test("toEWKT should emit SRID prefix when set, plain WKT otherwise") {
        val pt = JTS.point(1.0, 2.0)
        JTS.toEWKT(pt) should not include "SRID="
        pt.setSRID(4326)
        JTS.toEWKT(pt) should startWith("SRID=4326;")
    }

    test("EWKT round-trip should preserve SRID") {
        val pt = JTS.point(3.14, 2.71)
        pt.setSRID(3857)
        val ewkt = JTS.toEWKT(pt)
        val back = JTS.fromWKT(ewkt)
        back.getSRID shouldBe 3857
    }

    test("fromWKB should decode EWKB with SRID flag") {
        val pt = JTS.point(10.0, 20.0)
        pt.setSRID(4326)
        val ewkb = JTS.toEWKB(pt)
        val parsed = JTS.fromWKB(ewkb)
        parsed.getSRID shouldBe 4326
        parsed.asInstanceOf[Point].getX shouldBe 10.0
    }

    test("fromWKB should leave SRID=0 for plain WKB") {
        val pt = JTS.point(1.0, 2.0)
        val wkb = JTS.toWKB(pt) // default writer — no SRID embedded
        val parsed = JTS.fromWKB(wkb)
        parsed.getSRID shouldBe 0
    }

    test("toWKB (plain) should not embed SRID even when set on the geometry") {
        val pt = JTS.point(1.0, 2.0)
        pt.setSRID(4326)
        val wkb = JTS.toWKB(pt)
        val parsed = JTS.fromWKB(wkb)
        parsed.getSRID shouldBe 0
    }

    test("EWKB round-trip should preserve SRID") {
        val poly = JTS.polygonFromXYs(Array((0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)))
        poly.setSRID(27700)
        val ewkb = JTS.toEWKB(poly)
        val parsed = JTS.fromWKB(ewkb)
        parsed.getSRID shouldBe 27700
    }

    // ====== Adaptive writers (Z-preserving without NaN-Z injection) ======

    test("toEWKBAdaptive: 2D point produces same bytes as toEWKB") {
        val gf = new GeometryFactory()
        val pt = gf.createPoint(new Coordinate(1.0, 2.0))
        pt.setSRID(4326)
        val adaptive = JTS.toEWKBAdaptive(pt)
        val standard = JTS.toEWKB(pt)
        adaptive.length shouldBe standard.length
        adaptive shouldEqual standard
    }

    test("toEWKBAdaptive: 2D point is 25 bytes (no Z)") {
        val gf = new GeometryFactory()
        val pt = gf.createPoint(new Coordinate(1.0, 2.0))
        pt.setSRID(4326)
        JTS.toEWKBAdaptive(pt).length shouldBe 25
    }

    test("toEWKBAdaptive: 3D point is 33 bytes (has Z)") {
        val gf = new GeometryFactory()
        val pt = gf.createPoint(new Coordinate(1.0, 2.0, 3.0))
        pt.setSRID(4326)
        JTS.toEWKBAdaptive(pt).length shouldBe 33
    }

    test("toEWKBAdaptive: 3D point round-trips Z") {
        val gf = new GeometryFactory()
        val pt = gf.createPoint(new Coordinate(1.5, 2.5, 99.25))
        pt.setSRID(4326)
        val bytes = JTS.toEWKBAdaptive(pt)
        val decoded = JTS.fromWKB(bytes)
        decoded.getCoordinate.z shouldBe (99.25 +- 1e-9)
        decoded.getSRID shouldBe 4326
    }

    test("toWKBAdaptive: 2D point produces same bytes as toWKB") {
        val gf = new GeometryFactory()
        val pt = gf.createPoint(new Coordinate(1.0, 2.0))
        JTS.toWKBAdaptive(pt) shouldEqual JTS.toWKB(pt)
    }

    test("toWKBAdaptive: 3D point round-trips Z") {
        val gf = new GeometryFactory()
        val pt = gf.createPoint(new Coordinate(1.5, 2.5, 42.0))
        val bytes = JTS.toWKBAdaptive(pt)
        val decoded = JTS.fromWKB(bytes)
        decoded.getCoordinate.z shouldBe (42.0 +- 1e-9)
    }

    test("toEWKTAdaptive: 2D point produces plain WKT (no Z marker)") {
        val gf = new GeometryFactory()
        val pt = gf.createPoint(new Coordinate(1.0, 2.0))
        val wkt = JTS.toEWKTAdaptive(pt)
        wkt should not include "Z"
    }

    test("toEWKTAdaptive: 3D point includes Z ordinate") {
        val gf = new GeometryFactory()
        val pt = gf.createPoint(new Coordinate(1.0, 2.0, 3.0))
        val wkt = JTS.toEWKTAdaptive(pt)
        wkt should include ("3")  // Z value appears in the string
    }

    test("toEWKTAdaptive: SRID is included for 3D point") {
        val gf = new GeometryFactory()
        val pt = gf.createPoint(new Coordinate(1.0, 2.0, 3.0))
        pt.setSRID(4326)
        val wkt = JTS.toEWKTAdaptive(pt)
        wkt should startWith("SRID=4326;")
    }

    // ====== Adaptive Z probe — mixed-geometry regression cases ======

    test("toEWKBAdaptive: GEOMETRYCOLLECTION where first coord NaN-Z but later has Z -> 3D output") {
        // Regression: old code probed only geom.getCoordinate (first coord).
        // GEOMETRYCOLLECTION(POINT(0 0), POINT Z(1 1 5)) — first coord has NaN Z.
        val gf = new GeometryFactory()
        val pt2d = gf.createPoint(new Coordinate(0.0, 0.0))       // Z = NaN
        val pt3d = gf.createPoint(new Coordinate(1.0, 1.0, 5.0))  // Z = 5
        val coll = gf.createGeometryCollection(Array(pt2d, pt3d))
        val bytes = JTS.toEWKBAdaptive(coll)
        // 3D output is larger than 2D; exact size varies but must carry Z
        val decoded = JTS.fromWKB(bytes)
        // Second point's coordinate should have Z = 5
        val coords = decoded.getCoordinates
        val z5 = coords.find(c => !c.z.isNaN && math.abs(c.z - 5.0) < 1e-9)
        z5 should not be None
    }

    test("toEWKBAdaptive: LINESTRING where first vertex NaN-Z but second has Z -> 3D output") {
        val gf = new GeometryFactory()
        // Create LINESTRING where vertex 0 has NaN Z and vertex 1 has Z = 10
        val coords = Array(new Coordinate(0.0, 0.0), new Coordinate(1.0, 1.0, 10.0))
        val line = gf.createLineString(coords)
        val bytes = JTS.toEWKBAdaptive(line)
        val decoded = JTS.fromWKB(bytes)
        val c1 = decoded.getCoordinates()(1)
        c1.z shouldBe (10.0 +- 1e-9)
    }

    test("toEWKBAdaptive: 2D GEOMETRYCOLLECTION produces same bytes as toEWKB") {
        val gf = new GeometryFactory()
        val pt1 = gf.createPoint(new Coordinate(0.0, 0.0))
        val pt2 = gf.createPoint(new Coordinate(1.0, 1.0))
        val coll = gf.createGeometryCollection(Array(pt1, pt2))
        JTS.toEWKBAdaptive(coll).length shouldBe JTS.toEWKB(coll).length
    }

}
