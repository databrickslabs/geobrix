package com.databricks.labs.gbx.gridx.bng

import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite

class BNG_AccessorNullTest extends AnyFunSuite {
    private def u(s: String) = UTF8String.fromString(s)

    test("BNG_AsWKB returns null for a malformed cell id") {
        assert(BNG_AsWKB.eval(u("!!")) == null)
    }
    test("BNG_CellArea returns null (boxed) for a malformed cell id") {
        assert(BNG_CellArea.eval(u("!!")) == null)
    }
    test("BNG_Distance returns null (boxed) when either cell id is malformed") {
        assert(BNG_Distance.eval(u("!!"), u("TL")) == null)
    }
    test("BNG_CellArea still computes a real value for a valid cell") {
        assert(BNG_CellArea.eval(u("TL")) != null)
    }
}
