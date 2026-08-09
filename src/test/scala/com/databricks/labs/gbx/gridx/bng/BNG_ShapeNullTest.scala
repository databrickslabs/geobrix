package com.databricks.labs.gbx.gridx.bng

import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite

class BNG_ShapeNullTest extends AnyFunSuite {
    private def u(s: String) = UTF8String.fromString(s)

    test("PointAsCell returns null for an unparseable geometry (data)") {
        assert(BNG_PointAsCell.eval(u("NOT WKT"), 3) == null)
    }
    test("PointAsCell still RAISES for an unsupported resolution (parameter)") {
        assertThrows[Exception](BNG_PointAsCell.eval(u("POINT (530000 180000)"), u("bogus-res")))
    }
    test("KRing returns null array for a malformed cell id (data)") {
        assert(BNG_KRing.eval(u("!!"), 1) == null)
    }
    test("Polyfill returns null for an unparseable geometry (data)") {
        assert(BNG_Polyfill.eval(u("NOT WKT"), 3) == null)
    }
    test("Tessellate returns null struct for an unparseable geometry (data)") {
        assert(BNG_Tessellate.eval(u("NOT WKT"), 3, true) == null)
    }
}
