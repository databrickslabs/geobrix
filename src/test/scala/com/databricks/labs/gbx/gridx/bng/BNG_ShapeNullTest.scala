package com.databricks.labs.gbx.gridx.bng

import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite

class BNG_ShapeNullTest extends AnyFunSuite {
    private def u(s: String) = UTF8String.fromString(s)

    // --- data degrades to null ---

    test("PointAsCell returns null for an unparseable geometry (data)") {
        assert(BNG_PointAsCell.eval(u("NOT WKT"), 3) == null)
    }
    test("PointAsCell WKB overload returns null for garbage bytes (data)") {
        assert(BNG_PointAsCell.eval(Array[Byte](1, 2, 3), 3) == null)
    }
    test("KRing returns null array for a malformed cell id (data)") {
        assert(BNG_KRing.eval(u("!!"), 1) == null)
    }
    test("Polyfill returns null for an unparseable geometry (data)") {
        assert(BNG_Polyfill.eval(u("NOT WKT"), 3) == null)
    }
    test("Polyfill WKB overload returns null for garbage bytes (data)") {
        assert(BNG_Polyfill.eval(Array[Byte](1, 2, 3), 3) == null)
    }
    test("Tessellate returns null struct for an unparseable geometry (data)") {
        assert(BNG_Tessellate.eval(u("NOT WKT"), 3, true) == null)
    }

    // --- parameter raises ---

    test("PointAsCell still RAISES for an unsupported resolution (parameter)") {
        assertThrows[Exception](BNG_PointAsCell.eval(u("POINT (530000 180000)"), u("bogus-res")))
    }
    test("GeometryKRing still RAISES for an unsupported resolution (parameter)") {
        assertThrows[Exception](BNG_GeometryKRing.eval(u("POINT (530000 180000)"), u("bogus-res"), 1))
    }
    test("Polyfill still RAISES for an unsupported resolution (parameter)") {
        assertThrows[Exception](BNG_Polyfill.eval(u("POLYGON ((530000 180000, 531000 180000, 531000 181000, 530000 181000, 530000 180000))"), u("bogus-res")))
    }
    test("Tessellate still RAISES for an unsupported resolution (parameter)") {
        assertThrows[Exception](BNG_Tessellate.eval(u("POLYGON ((530000 180000, 531000 180000, 531000 181000, 530000 181000, 530000 180000))"), u("bogus-res"), true))
    }
}
