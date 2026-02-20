package com.databricks.labs.gbx.vectorx.jts

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class GeometryTypeEnumTest extends AnyFunSuite {

    // ====== Enum Values ======

    test("INVALID should have id 0") {
        GeometryTypeEnum.INVALID.id shouldBe 0
        GeometryTypeEnum.INVALID.toString shouldBe "INVALID"
    }

    test("POINT should have id 1") {
        GeometryTypeEnum.POINT.id shouldBe 1
        GeometryTypeEnum.POINT.toString shouldBe "POINT"
    }

    test("MULTIPOINT should have id 2") {
        GeometryTypeEnum.MULTIPOINT.id shouldBe 2
        GeometryTypeEnum.MULTIPOINT.toString shouldBe "MULTIPOINT"
    }

    test("LINESTRING should have id 3") {
        GeometryTypeEnum.LINESTRING.id shouldBe 3
        GeometryTypeEnum.LINESTRING.toString shouldBe "LINESTRING"
    }

    test("MULTILINESTRING should have id 4") {
        GeometryTypeEnum.MULTILINESTRING.id shouldBe 4
        GeometryTypeEnum.MULTILINESTRING.toString shouldBe "MULTILINESTRING"
    }

    test("POLYGON should have id 5") {
        GeometryTypeEnum.POLYGON.id shouldBe 5
        GeometryTypeEnum.POLYGON.toString shouldBe "POLYGON"
    }

    test("MULTIPOLYGON should have id 6") {
        GeometryTypeEnum.MULTIPOLYGON.id shouldBe 6
        GeometryTypeEnum.MULTIPOLYGON.toString shouldBe "MULTIPOLYGON"
    }

    test("LINEARRING should have id 7") {
        GeometryTypeEnum.LINEARRING.id shouldBe 7
        GeometryTypeEnum.LINEARRING.toString shouldBe "LINEARRING"
    }

    test("GEOMETRYCOLLECTION should have id 8") {
        GeometryTypeEnum.GEOMETRYCOLLECTION.id shouldBe 8
        GeometryTypeEnum.GEOMETRYCOLLECTION.toString shouldBe "GEOMETRYCOLLECTION"
    }

    // ====== apply(tpe: String) ======

    test("apply should return POINT for 'POINT'") {
        GeometryTypeEnum.apply("POINT") shouldBe GeometryTypeEnum.POINT
    }

    test("apply should be case insensitive") {
        GeometryTypeEnum.apply("point") shouldBe GeometryTypeEnum.POINT
        GeometryTypeEnum.apply("Point") shouldBe GeometryTypeEnum.POINT
        GeometryTypeEnum.apply("POINT") shouldBe GeometryTypeEnum.POINT
    }

    test("apply should return POLYGON for 'POLYGON'") {
        GeometryTypeEnum.apply("POLYGON") shouldBe GeometryTypeEnum.POLYGON
    }

    test("apply should return LINESTRING for 'LINESTRING'") {
        GeometryTypeEnum.apply("LINESTRING") shouldBe GeometryTypeEnum.LINESTRING
    }

    test("apply should return INVALID for unknown type") {
        GeometryTypeEnum.apply("UNKNOWN") shouldBe GeometryTypeEnum.INVALID
    }

    test("apply should return INVALID for empty string") {
        GeometryTypeEnum.apply("") shouldBe GeometryTypeEnum.INVALID
    }

    // ====== fromTypeId(typeId: Int) ======

    test("fromTypeId should return POINT for id 1") {
        GeometryTypeEnum.fromTypeId(1) shouldBe GeometryTypeEnum.POINT
    }

    test("fromTypeId should return POLYGON for id 5") {
        GeometryTypeEnum.fromTypeId(5) shouldBe GeometryTypeEnum.POLYGON
    }

    test("fromTypeId should return MULTIPOLYGON for id 6") {
        GeometryTypeEnum.fromTypeId(6) shouldBe GeometryTypeEnum.MULTIPOLYGON
    }

    test("fromTypeId should return LINEARRING for id 7") {
        GeometryTypeEnum.fromTypeId(7) shouldBe GeometryTypeEnum.LINEARRING
    }

    test("fromTypeId should return GEOMETRYCOLLECTION for id 8") {
        GeometryTypeEnum.fromTypeId(8) shouldBe GeometryTypeEnum.GEOMETRYCOLLECTION
    }

    test("fromTypeId should return INVALID for unknown id") {
        GeometryTypeEnum.fromTypeId(99) shouldBe GeometryTypeEnum.INVALID
    }

    test("fromTypeId should return INVALID for negative id") {
        GeometryTypeEnum.fromTypeId(-1) shouldBe GeometryTypeEnum.INVALID
    }

    // ====== All Values ======

    test("values should contain all 9 geometry types") {
        GeometryTypeEnum.values.size shouldBe 9
    }

    test("values should contain POINT") {
        GeometryTypeEnum.values should contain(GeometryTypeEnum.POINT)
    }

    test("values should contain all types") {
        val values = GeometryTypeEnum.values
        values should contain(GeometryTypeEnum.INVALID)
        values should contain(GeometryTypeEnum.POINT)
        values should contain(GeometryTypeEnum.MULTIPOINT)
        values should contain(GeometryTypeEnum.LINESTRING)
        values should contain(GeometryTypeEnum.MULTILINESTRING)
        values should contain(GeometryTypeEnum.POLYGON)
        values should contain(GeometryTypeEnum.MULTIPOLYGON)
        values should contain(GeometryTypeEnum.LINEARRING)
        values should contain(GeometryTypeEnum.GEOMETRYCOLLECTION)
    }

}
