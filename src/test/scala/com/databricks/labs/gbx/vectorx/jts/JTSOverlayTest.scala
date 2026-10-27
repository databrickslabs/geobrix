package com.databricks.labs.gbx.vectorx.jts

import org.locationtech.jts.geom.{Geometry, GeometryFactory, PrecisionModel}
import org.scalatest.funsuite.AnyFunSuite

class JTSOverlayTest extends AnyFunSuite {

    private def read(wkt: String): Geometry = JTS.fromWKT(wkt)

    private val square = read("POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))")

    test("ordinary geometries retain JTS intersection and difference semantics") {
        val other = read("POLYGON ((2 0, 6 0, 6 4, 2 4, 2 0))")
        assert(JTS.intersection(square, other).equalsTopo(square.intersection(other)))
        assert(JTS.difference(square, other).equalsTopo(square.difference(other)))
    }

    test("intersection handles nested collections on either side and preserves mixed dimensions") {
        val collection = read(
          "GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (1 1), LINESTRING (2 1, 2 3)), " +
              "POLYGON ((3 1, 5 1, 5 2, 3 2, 3 1)))")
        val expected = read(
          "GEOMETRYCOLLECTION (POINT (1 1), LINESTRING (2 1, 2 3), " +
              "POLYGON ((3 1, 4 1, 4 2, 3 2, 3 1)))")
        for (result <- Seq(JTS.intersection(collection, square), JTS.intersection(square, collection))) {
            assert(result.getNumGeometries == 3)
            assert(result.norm().equalsExact(expected.norm()))
        }
        val covering = read("GEOMETRYCOLLECTION (POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0)))")
        assert(JTS.intersection(collection, covering).norm().equalsExact(expected.norm()))
    }

    test("difference removes boundary lines and points from polygon collections") {
        val polygon = "POLYGON ((1 1, 3 1, 3 3, 1 3, 1 1))"
        for (fragment <- Seq("LINESTRING (0 1, 0 3)", "POINT (0 2)")) {
            val collection = read(s"GEOMETRYCOLLECTION ($polygon, $fragment)")
            val result = JTS.difference(collection, square.getBoundary)
            assert(result.getGeometryType == "Polygon")
            assert(result.equalsTopo(read(polygon)))
        }
    }

    test("right-hand collection members are subtracted cumulatively without swapping operands") {
        val cutters = read(
          "GEOMETRYCOLLECTION (POLYGON ((0 0, 1 0, 1 4, 0 4, 0 0)), " +
              "GEOMETRYCOLLECTION (POLYGON ((3 0, 4 0, 4 4, 3 4, 3 0))))")
        val result = JTS.difference(square, cutters)
        assert(result.equalsTopo(read("POLYGON ((1 0, 3 0, 3 4, 1 4, 1 0))")))
        assert(JTS.difference(cutters, square).isEmpty)
        val left = read("GEOMETRYCOLLECTION (POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0)))")
        assert(JTS.difference(left, cutters).equalsTopo(result))
    }

    test("compaction unions overlapping polygon members and drops empty members") {
        val collection = read(
          "GEOMETRYCOLLECTION (POLYGON EMPTY, POLYGON ((0 0, 3 0, 3 4, 0 4, 0 0)), " +
              "POLYGON ((1 0, 4 0, 4 4, 1 4, 1 0)))")
        val result = JTS.intersection(collection, square)
        assert(result.getGeometryType == "Polygon")
        assert(result.equalsTopo(square))
    }

    test("empty collections are identities for subtraction and annihilate intersection") {
        val empty = read("GEOMETRYCOLLECTION EMPTY")
        assert(JTS.intersection(square, empty).isEmpty)
        assert(JTS.intersection(empty, square).isEmpty)
        assert(JTS.difference(empty, square).isEmpty)
        assert(JTS.difference(square, empty).equalsTopo(square))
    }

    test("overlay helpers preserve the left factory precision model and SRID without mutating inputs") {
        val factory = new GeometryFactory(new PrecisionModel(1000.0), 27700)
        val left = factory.createGeometry(read("GEOMETRYCOLLECTION (POINT (1 1), POINT (2 2))"))
        left.setSRID(4326)
        val original = left.copy()
        for (result <- Seq(JTS.intersection(left, square), JTS.difference(left, square.getBoundary))) {
            assert(result.getSRID == 4326)
            assert(result.getFactory eq factory)
            assert(result.getPrecisionModel.getScale == 1000.0)
        }
        assert(left.equalsExact(original))
        assert(left.getSRID == 4326)
        assert(JTS.intersection(left.getGeometryN(0), square).getSRID == left.getGeometryN(0).getSRID)
    }
}
