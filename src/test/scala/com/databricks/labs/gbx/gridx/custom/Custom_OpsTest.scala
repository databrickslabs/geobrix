package com.databricks.labs.gbx.gridx.custom

import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.apache.spark.sql.types.{BinaryType, IntegerType, LongType}
import org.scalatest.matchers.should.Matchers._

class Custom_OpsTest extends PlanTest with SilentSparkSession {

    // ------------------------------------------------------------------
    // Helper: build the grid-spec InternalRow for (0,100,0,100,2,10,10,32633)
    // ------------------------------------------------------------------
    private def buildGridRow(): InternalRow = {
        val gridExpr = Custom_Grid(
            Literal(0L,    LongType),
            Literal(100L,  LongType),
            Literal(0L,    LongType),
            Literal(100L,  LongType),
            Literal(2,     IntegerType),
            Literal(10,    IntegerType),
            Literal(10,    IntegerType),
            Literal(32633, IntegerType)
        )
        gridExpr.eval(InternalRow.empty).asInstanceOf[InternalRow]
    }

    // ------------------------------------------------------------------
    // pointascell -> cellaswkb round-trip: cell [0,10]x[0,10]
    // ------------------------------------------------------------------
    test("Custom_PointAsCell should encode point (5,5) and Custom_AsWKB should return [0,10]x[0,10] envelope") {
        val gridRow = buildGridRow()
        val gridLit = Literal.create(gridRow, Custom_GridSpec.gridStructType)

        val pointWkb   = JTS.toWKB(JTS.point(5.0, 5.0))
        val pointLit   = Literal.create(pointWkb, BinaryType)
        val resLit     = Literal(0, IntegerType)

        val cellExpr = Custom_PointAsCell(pointLit, gridLit, resLit)
        val cell     = cellExpr.eval(InternalRow.empty).asInstanceOf[Long]

        val wkbExpr  = Custom_AsWKB(Literal(cell), gridLit)
        val wkbBytes = wkbExpr.eval(InternalRow.empty).asInstanceOf[Array[Byte]]

        val geom     = JTS.fromWKB(wkbBytes)
        val env      = geom.getEnvelopeInternal

        env.getMinX shouldBe 0.0  +- 1e-9
        env.getMaxX shouldBe 10.0 +- 1e-9
        env.getMinY shouldBe 0.0  +- 1e-9
        env.getMaxY shouldBe 10.0 +- 1e-9
    }

    // ------------------------------------------------------------------
    // cellaswkt: result starts with POLYGON
    // ------------------------------------------------------------------
    test("Custom_AsWKT should return a POLYGON string for a valid cell") {
        val gridRow = buildGridRow()
        val gridLit = Literal.create(gridRow, Custom_GridSpec.gridStructType)

        val pointWkb = JTS.toWKB(JTS.point(5.0, 5.0))
        val pointLit = Literal.create(pointWkb, BinaryType)
        val resLit   = Literal(0, IntegerType)

        val cell    = Custom_PointAsCell(pointLit, gridLit, resLit).eval(InternalRow.empty).asInstanceOf[Long]
        val wktExpr = Custom_AsWKT(Literal(cell), gridLit)
        val result  = wktExpr.eval(InternalRow.empty).asInstanceOf[org.apache.spark.unsafe.types.UTF8String]

        result should not be null
        result.toString should startWith("POLYGON")
    }

    // ------------------------------------------------------------------
    // centroid: Point at (5,5) +- 1e-9
    // ------------------------------------------------------------------
    test("Custom_Centroid should return WKB point at center (5,5) for cell containing (5,5)") {
        val gridRow = buildGridRow()
        val gridLit = Literal.create(gridRow, Custom_GridSpec.gridStructType)

        val pointWkb = JTS.toWKB(JTS.point(5.0, 5.0))
        val pointLit = Literal.create(pointWkb, BinaryType)
        val resLit   = Literal(0, IntegerType)

        val cell         = Custom_PointAsCell(pointLit, gridLit, resLit).eval(InternalRow.empty).asInstanceOf[Long]
        val centroidExpr = Custom_Centroid(Literal(cell), gridLit)
        val centWkb      = centroidExpr.eval(InternalRow.empty).asInstanceOf[Array[Byte]]

        val centGeom = JTS.fromWKB(centWkb)
        val coord    = centGeom.getCoordinate

        coord.x shouldBe 5.0 +- 1e-9
        coord.y shouldBe 5.0 +- 1e-9
    }

    // ------------------------------------------------------------------
    // a cell id given as INT (a SQL literal such as 45) is the same cell as its BIGINT
    // ------------------------------------------------------------------
    test("the custom cell functions take a cell id given as INT, as light does") {
        val gridLit = Literal.create(buildGridRow(), Custom_GridSpec.gridStructType)
        val cell    = Custom_PointAsCell(
            Literal.create(JTS.toWKB(JTS.point(55.0, 45.0)), BinaryType), gridLit, Literal(0, IntegerType)
        ).eval(InternalRow.empty).asInstanceOf[Long]
        cell.isValidInt shouldBe true
        val asInt  = Literal(cell.toInt, IntegerType)
        val asLong = Literal(cell, LongType)
        val k      = Literal(1, IntegerType)
        def ring(e: org.apache.spark.sql.catalyst.expressions.Expression) =
            e.eval(InternalRow.empty).asInstanceOf[org.apache.spark.sql.catalyst.util.ArrayData].toLongArray().toSeq
        ring(Custom_KRing(asInt, gridLit, k)) shouldBe ring(Custom_KRing(asLong, gridLit, k))
        ring(Custom_KRing(asInt, gridLit, k)) should have size 9
        ring(Custom_KLoop(asInt, gridLit, k)) shouldBe ring(Custom_KLoop(asLong, gridLit, k))
        Custom_AsWKT(asInt, gridLit).eval(InternalRow.empty) shouldBe Custom_AsWKT(asLong, gridLit).eval(InternalRow.empty)
        Custom_AsWKB(asInt, gridLit).eval(InternalRow.empty).asInstanceOf[Array[Byte]] shouldBe
            Custom_AsWKB(asLong, gridLit).eval(InternalRow.empty).asInstanceOf[Array[Byte]]
        Custom_Centroid(asInt, gridLit).eval(InternalRow.empty).asInstanceOf[Array[Byte]] shouldBe
            Custom_Centroid(asLong, gridLit).eval(InternalRow.empty).asInstanceOf[Array[Byte]]
        Custom_Distance(asInt, gridLit, asLong).eval(InternalRow.empty) shouldBe 0L
    }

}
