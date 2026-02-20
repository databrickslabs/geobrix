package com.databricks.labs.gbx.vectorx.jts.legacy

import org.apache.spark.sql.catalyst.util.ArrayData
import org.locationtech.jts.geom.Coordinate
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class InternalCoordTest extends AnyFunSuite {

    // ====== Construction ======

    test("InternalCoord should accept 2D coordinates") {
        val coord = InternalCoord(Seq(1.0, 2.0))
        coord.coords should have size 2
        coord.coords(0) shouldBe 1.0
        coord.coords(1) shouldBe 2.0
    }

    test("InternalCoord should accept 3D coordinates") {
        val coord = InternalCoord(Seq(1.0, 2.0, 3.0))
        coord.coords should have size 3
        coord.coords(0) shouldBe 1.0
        coord.coords(1) shouldBe 2.0
        coord.coords(2) shouldBe 3.0
    }

    // ====== toCoordinate ======

    test("toCoordinate should convert 2D to JTS Coordinate") {
        val coord = InternalCoord(Seq(1.0, 2.0))
        val jtsCoord = coord.toCoordinate
        jtsCoord should not be null
        jtsCoord shouldBe a[Coordinate]
        jtsCoord.getX shouldBe 1.0
        jtsCoord.getY shouldBe 2.0
    }

    test("toCoordinate should convert 3D to JTS Coordinate") {
        val coord = InternalCoord(Seq(1.0, 2.0, 3.0))
        val jtsCoord = coord.toCoordinate
        jtsCoord should not be null
        jtsCoord.getX shouldBe 1.0
        jtsCoord.getY shouldBe 2.0
        jtsCoord.getZ shouldBe 3.0
    }

    // ====== serialize ======

    test("serialize should convert to ArrayData") {
        val coord = InternalCoord(Seq(1.0, 2.0))
        val arrayData = coord.serialize
        arrayData should not be null
        arrayData shouldBe a[ArrayData]
        arrayData.numElements() shouldBe 2
    }

    test("serialize should preserve 3D coordinates") {
        val coord = InternalCoord(Seq(1.0, 2.0, 3.0))
        val arrayData = coord.serialize
        arrayData.numElements() shouldBe 3
    }

    // ====== Companion object: apply(Coordinate) ======

    test("apply should construct from 2D JTS Coordinate") {
        val jtsCoord = new Coordinate(1.0, 2.0)
        val coord = InternalCoord(jtsCoord)
        coord.coords should have size 2
        coord.coords(0) shouldBe 1.0
        coord.coords(1) shouldBe 2.0
    }

    test("apply should construct from 3D JTS Coordinate") {
        val jtsCoord = new Coordinate(1.0, 2.0, 3.0)
        val coord = InternalCoord(jtsCoord)
        coord.coords should have size 3
        coord.coords(0) shouldBe 1.0
        coord.coords(1) shouldBe 2.0
        coord.coords(2) shouldBe 3.0
    }

    test("apply should handle NaN Z coordinate as 2D") {
        val jtsCoord = new Coordinate(1.0, 2.0)
        // JTS Coordinate without Z has NaN for Z
        jtsCoord.getZ.isNaN shouldBe true
        val coord = InternalCoord(jtsCoord)
        coord.coords should have size 2
    }

    // ====== Companion object: apply(ArrayData) ======

    test("apply should construct from 2D ArrayData") {
        val arrayData = ArrayData.toArrayData(Array(1.0, 2.0))
        val coord = InternalCoord(arrayData)
        coord.coords should have size 2
        coord.coords(0) shouldBe 1.0
        coord.coords(1) shouldBe 2.0
    }

    test("apply should construct from 3D ArrayData") {
        val arrayData = ArrayData.toArrayData(Array(1.0, 2.0, 3.0))
        val coord = InternalCoord(arrayData)
        coord.coords should have size 3
        coord.coords(0) shouldBe 1.0
        coord.coords(1) shouldBe 2.0
        coord.coords(2) shouldBe 3.0
    }

    // ====== Round-trip ======

    test("should round-trip through JTS Coordinate") {
        val original = InternalCoord(Seq(1.0, 2.0, 3.0))
        val jtsCoord = original.toCoordinate
        val restored = InternalCoord(jtsCoord)
        restored.coords should have size 3
        restored.coords(0) shouldBe 1.0
        restored.coords(1) shouldBe 2.0
        restored.coords(2) shouldBe 3.0
    }

    test("should round-trip through ArrayData") {
        val original = InternalCoord(Seq(1.0, 2.0))
        val arrayData = original.serialize
        val restored = InternalCoord(arrayData)
        restored.coords should have size 2
        restored.coords(0) shouldBe 1.0
        restored.coords(1) shouldBe 2.0
    }

}
