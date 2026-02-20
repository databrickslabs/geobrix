package com.databricks.labs.gbx.gridx.grid

import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.Geometry
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Tests for H3 grid (pure methods, geometry, polyfill). */
class H3Test extends AnyFunSuite {

    // -------- Pure / constants --------
    test("H3.name should be H3") {
        H3.name shouldBe "H3"
    }

    test("H3.crsID should be 4326") {
        H3.crsID shouldBe 4326
    }

    test("H3.isCylindrical should be true") {
        H3.isCylindrical shouldBe true
    }

    test("H3.resolutions should be 0 to 15") {
        H3.resolutions shouldBe (0 to 15).toSet
    }

    test("H3.edgeLength should decrease as resolution increases") {
        val e0 = H3.edgeLength(0)
        val e5 = H3.edgeLength(5)
        val e15 = H3.edgeLength(15)
        assert(e0 > e5)
        assert(e5 > e15)
        assert(e0 > 0)
        assert(e15 > 0)
    }

    test("H3.getResolution with Int should return the value in range") {
        H3.getResolution(0) shouldBe 0
        H3.getResolution(15) shouldBe 15
        H3.getResolution(7) shouldBe 7
    }

    test("H3.getResolution with String should parse") {
        H3.getResolution("0") shouldBe 0
        H3.getResolution("15") shouldBe 15
    }

    test("H3.getResolution with UTF8String should parse") {
        H3.getResolution(UTF8String.fromString("10")) shouldBe 10
    }

    test("H3.getResolution below 0 should throw") {
        assertThrows[IllegalStateException](H3.getResolution(-1))
        assertThrows[IllegalStateException](H3.getResolution("-1"))
    }

    test("H3.getResolution above 15 should throw") {
        assertThrows[IllegalStateException](H3.getResolution(16))
        assertThrows[IllegalStateException](H3.getResolution("16"))
    }

    test("H3.getResolution with invalid type should throw") {
        assertThrows[IllegalArgumentException](H3.getResolution(Map.empty))
        assertThrows[IllegalArgumentException](H3.getResolution(Seq(1)))
    }

    test("H3.getResolutionStr should return string of resolution") {
        H3.getResolutionStr(0) shouldBe "0"
        H3.getResolutionStr(15) shouldBe "15"
    }

    // -------- pointToCellID / format / parse / distance --------
    test("H3.pointToCellID should return cell at (lon, lat) for resolution") {
        val cell = H3.pointToCellID(0.0, 0.0, 5)
        cell should not be 0L
    }

    test("H3.format and parse should round-trip") {
        val cell = H3.pointToCellID(-122.4, 37.7, 9)
        val addr = H3.format(cell)
        addr should not be empty
        H3.parse(addr) shouldBe cell
    }

    test("H3.distance between same cell should be 0") {
        val cell = H3.pointToCellID(0.0, 0.0, 5)
        H3.distance(cell, cell) shouldBe 0L
    }

    test("H3.distance between adjacent cells should be 1") {
        val cell = H3.pointToCellID(0.0, 0.0, 5)
        val ring = H3.kLoop(cell, 1)
        ring should not be empty
        ring.foreach(c => H3.distance(cell, c) shouldBe 1L)
    }

    // -------- kRing / kLoop --------
    test("H3.kRing should include center and neighbors") {
        val cell = H3.pointToCellID(0.0, 0.0, 5)
        val ring = H3.kRing(cell, 1)
        ring should contain(cell)
        ring.size should be > 1
    }

    test("H3.kLoop at 0 should return only center") {
        val cell = H3.pointToCellID(0.0, 0.0, 5)
        val loop = H3.kLoop(cell, 0)
        loop should contain(cell)
        loop.size shouldBe 1
    }

    test("H3.kLoop at 1 should return cells at distance 1") {
        val cell = H3.pointToCellID(0.0, 0.0, 5)
        val loop = H3.kLoop(cell, 1)
        loop should not contain cell
        loop.foreach(c => H3.distance(cell, c) shouldBe 1L)
    }

    // -------- cellIdToGeometry / cellIdToCenter / cellIdToBoundary --------
    test("H3.cellIdToGeometry should return polygon with SRID 4326") {
        val cell = H3.pointToCellID(0.0, 0.0, 5)
        val geom = H3.cellIdToGeometry(cell)
        geom should not be null
        geom.getSRID shouldBe 4326
        geom.getGeometryType shouldBe "Polygon"
    }

    test("H3.cellIdToCenter should return coordinate (lat, lng)") {
        val cell = H3.pointToCellID(0.0, 0.0, 5)
        val coord = H3.cellIdToCenter(cell)
        coord should not be null
        math.abs(coord.x) should be < 1.0
        math.abs(coord.y) should be < 1.0
    }

    test("H3.cellIdToBoundary should return closed ring") {
        val cell = H3.pointToCellID(0.0, 0.0, 5)
        val boundary = H3.cellIdToBoundary(cell)
        boundary should not be empty
        boundary.size should be >= 6
    }

    // -------- getBufferRadius (Polygon) --------
    test("H3.getBufferRadius for Polygon should return positive radius") {
        val poly = JTS.polygonFromXYs(Array((0.0, 0.0), (0.01, 0.0), (0.01, 0.01), (0.0, 0.01), (0.0, 0.0)))
        poly.setSRID(4326)
        val r = H3.getBufferRadius(poly, 7)
        r should be >= 0.0
    }

    // -------- polyfill --------
    test("H3.polyfill with empty geometry should return empty seq") {
        val empty = JTS.polygonFromXYs(Array((0.0, 0.0), (0.0, 0.0), (0.0, 0.0), (0.0, 0.0), (0.0, 0.0)))
        empty.setSRID(4326)
        H3.polyfill(empty, 5) shouldBe Seq.empty
    }

    test("H3.polyfill with small polygon should return non-empty cells") {
        val poly = JTS.polygonFromXYs(Array((-122.4, 37.7), (-122.3, 37.7), (-122.3, 37.8), (-122.4, 37.8), (-122.4, 37.7)))
        poly.setSRID(4326)
        val cells = H3.polyfill(poly, 7)
        cells should not be empty
    }

    // -------- crossesAntiMeridian / makeSafeGeometry --------
    test("H3.crossesAntiMeridian for geometry not crossing antimeridian should be false") {
        val poly = JTS.polygonFromXYs(Array((1.0, 1.0), (2.0, 1.0), (2.0, 2.0), (1.0, 2.0), (1.0, 1.0)))
        H3.crossesAntiMeridian(poly) shouldBe false
    }

    test("H3.makeSafeGeometry for non-crossing geometry should return same geometry") {
        val poly = JTS.polygonFromXYs(Array((1.0, 1.0), (2.0, 1.0), (2.0, 2.0), (1.0, 2.0), (1.0, 1.0)))
        val out = H3.makeSafeGeometry(poly)
        out should not be null
        out.getNumPoints shouldBe poly.getNumPoints
    }

    test("H3.extent should be world polygon") {
        val ext = H3.extent
        ext should not be null
        ext.getEnvelopeInternal.getMinX shouldBe -180.0
        ext.getEnvelopeInternal.getMaxX shouldBe 180.0
        ext.getEnvelopeInternal.getMinY shouldBe -90.0
        ext.getEnvelopeInternal.getMaxY shouldBe 90.0
    }
}
