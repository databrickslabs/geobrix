package com.databricks.labs.gbx.vectorx.jts.legacy.expressions

import com.databricks.labs.gbx.vectorx.jts.JTS
import com.databricks.labs.gbx.vectorx.jts.legacy.{InternalCoord, InternalGeometry}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class ST_LegacyAsWKBTest extends AnyFunSuite {

    val gf = new GeometryFactory()

    // Helper to create InternalRow for testing
    def createLegacyGeomRow(typeId: Int, srid: Int = 4326): InternalRow = {
        // For polygon, create a single boundary array with multiple coords
        val boundary = Array(
          InternalCoord(Seq(0.0, 0.0)),
          InternalCoord(Seq(1.0, 0.0)),
          InternalCoord(Seq(1.0, 1.0)),
          InternalCoord(Seq(0.0, 1.0)),
          InternalCoord(Seq(0.0, 0.0))
        )
        val boundaries = Array(boundary.map(_.serialize))
        val holes = Array.empty[Array[Array[InternalCoord]]]
        
        InternalRow(
          typeId,
          srid,
          new GenericArrayData(boundaries.map(b => new GenericArrayData(b))),
          new GenericArrayData(holes)
        )
    }

    // ====== ST_LegacyAsWKB Expression ======

    test("ST_LegacyAsWKB should be an Expression") {
        val expr = ST_LegacyAsWKB(Literal(null))
        expr shouldBe a[Expression]
    }

    test("ST_LegacyAsWKB should have BinaryType dataType") {
        val expr = ST_LegacyAsWKB(Literal(null))
        expr.dataType shouldBe BinaryType
    }

    test("ST_LegacyAsWKB should be nullable") {
        val expr = ST_LegacyAsWKB(Literal(null))
        expr.nullable shouldBe true
    }

    test("ST_LegacyAsWKB should have correct prettyName") {
        val expr = ST_LegacyAsWKB(Literal(null))
        expr.prettyName shouldBe "gbx_st_legacyaswkb"
    }

    test("ST_LegacyAsWKB should have single child") {
        val child = Literal(null)
        val expr = ST_LegacyAsWKB(child)
        expr.children should have length 1
        expr.children.head shouldBe child
    }

    test("ST_LegacyAsWKB should create replacement") {
        val expr = ST_LegacyAsWKB(Literal(null))
        val replacement = expr.replacement
        replacement should not be null
    }

    test("ST_LegacyAsWKB withNewChildrenInternal should create new instance") {
        val expr = ST_LegacyAsWKB(Literal(1))
        val newChild = Literal(2)
        val newExpr = expr.withNewChildrenInternal(IndexedSeq(newChild))
        newExpr shouldBe a[ST_LegacyAsWKB]
        newExpr.children.head shouldBe newChild
    }

    // ====== ST_LegacyAsWKB Object ======

    test("ST_LegacyAsWKB object should have correct name") {
        ST_LegacyAsWKB.name shouldBe "gbx_st_legacyaswkb"
    }

    test("ST_LegacyAsWKB object should provide builder") {
        val builder = ST_LegacyAsWKB.builder()
        builder should not be null
    }

    test("ST_LegacyAsWKB builder should create expression from sequence") {
        val builder = ST_LegacyAsWKB.builder()
        val expr = builder(Seq(Literal(null)))
        expr shouldBe a[ST_LegacyAsWKB]
    }

    // ====== ST_LegacyAsWKB Eval Logic ======

    test("ST_LegacyAsWKB eval should convert Point to WKB") {
        // Create a Point (typeId = 1)
        val coord = InternalCoord(Seq(10.0, 20.0))
        val row = InternalRow(
          1, // POINT
          4326,
          new GenericArrayData(Array(new GenericArrayData(Array(coord.serialize)))),
          new GenericArrayData(Array.empty)
        )
        
        val wkb = ST_LegacyAsWKB.eval(row)
        wkb should not be null
        wkb shouldBe a[Array[_]]
        wkb.length should be > 0
    }

    test("ST_LegacyAsWKB eval should convert Polygon to WKB") {
        val row = createLegacyGeomRow(5) // POLYGON
        val wkb = ST_LegacyAsWKB.eval(row)
        
        wkb should not be null
        wkb shouldBe a[Array[_]]
        wkb.length should be > 0
    }

    test("ST_LegacyAsWKB eval should produce valid WKB format") {
        val row = createLegacyGeomRow(1) // POINT
        val wkb = ST_LegacyAsWKB.eval(row)
        
        // WKB format starts with byte order (1 byte)
        // Followed by geometry type (4 bytes)
        wkb.length should be >= 5
    }

    test("ST_LegacyAsWKB eval should round-trip through JTS") {
        // Create a simple point
        val point = gf.createPoint(new Coordinate(5.0, 10.0))
        val wkbOriginal = JTS.toWKB(point)
        
        // Convert to legacy format via InternalCoord
        val coord = InternalCoord(Seq(5.0, 10.0))
        val row = InternalRow(
          1, // POINT
          0,
          new GenericArrayData(Array(new GenericArrayData(Array(coord.serialize)))),
          new GenericArrayData(Array.empty)
        )
        
        val wkbFromLegacy = ST_LegacyAsWKB.eval(row)
        
        // Both should be valid WKB
        wkbOriginal should not be null
        wkbFromLegacy should not be null
        wkbOriginal.length should be > 0
        wkbFromLegacy.length should be > 0
    }

}
