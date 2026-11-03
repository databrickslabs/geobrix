package com.databricks.labs.gbx.gridx.custom

import com.databricks.labs.gbx.kernels.TestL1
import com.databricks.labs.gbx.gridx.grid.{CustomGridSystem, GridConf}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.scalatest.matchers.should.Matchers._

/** Tests for Custom_Distance (gbx_custom_distance) expression.
  * gbx_custom_distance returns the Chebyshev grid-ring distance between two custom cells:
  * max(|dx|, |dy|) in cell-position units = minimum k such that b ∈ kRing(a, k).
  */
class Custom_DistanceTest extends PlanTest with SilentSparkSession {

    private val gridConf = GridConf(
        boundXMin     = 0L,
        boundXMax     = 100L,
        boundYMin     = 0L,
        boundYMax     = 100L,
        cellSplits    = 2,
        rootCellSizeX = 10,
        rootCellSizeY = 10,
        crsID         = Some(32633)
    )
    private val sys = CustomGridSystem(gridConf)

    private def buildGridLit(): Literal = {
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
        val gridRow = gridExpr.eval(InternalRow.empty).asInstanceOf[InternalRow]
        Literal.create(gridRow, Custom_GridSpec.gridStructType)
    }

    test("gbx_custom_distance: distance of a cell to itself is 0") {
        val gridLit = buildGridLit()
        val cell    = sys.pointToCellID(15.0, 15.0, 0)

        val result = Custom_Distance(Literal(cell), gridLit, Literal(cell)).eval(InternalRow.empty)
        assert(result != null)
        result.asInstanceOf[java.lang.Long].longValue shouldBe 0L
    }

    test("gbx_custom_distance: adjacent cells have distance 1 (axis-aligned, Chebyshev=Manhattan=1)") {
        val gridLit = buildGridLit()
        // cell at (15,15,0) pos=(1,1); adjacent at (25,15,0) pos=(2,1) -> dx=1,dy=0 -> Chebyshev=1
        val cellA = sys.pointToCellID(15.0, 15.0, 0)
        val cellB = sys.pointToCellID(25.0, 15.0, 0)

        val result = Custom_Distance(Literal(cellA), gridLit, Literal(cellB)).eval(InternalRow.empty)
        assert(result != null)
        result.asInstanceOf[java.lang.Long].longValue shouldBe 1L
    }

    test("gbx_custom_distance: diagonal pair dx=1,dy=1 -> Chebyshev=1 (not Manhattan=2)") {
        val gridLit = buildGridLit()
        // pos=(1,1) to pos=(2,2) -> dx=1, dy=1 -> Chebyshev=max(1,1)=1; Manhattan would give 2
        val cellA = sys.pointToCellID(15.0, 15.0, 0)
        val cellB = sys.pointToCellID(25.0, 25.0, 0)

        val result = Custom_Distance(Literal(cellA), gridLit, Literal(cellB)).eval(InternalRow.empty)
        assert(result != null)
        result.asInstanceOf[java.lang.Long].longValue shouldBe 1L
    }

    test("gbx_custom_distance: diagonal pair dx=2,dy=1 -> Chebyshev=2 (not Manhattan=3)") {
        val gridLit = buildGridLit()
        // pos=(1,1) to pos=(3,2) -> dx=2, dy=1 -> Chebyshev=max(2,1)=2; Manhattan would give 3
        val cellA = sys.pointToCellID(15.0, 15.0, 0)
        val cellB = sys.pointToCellID(35.0, 25.0, 0)

        val result = Custom_Distance(Literal(cellA), gridLit, Literal(cellB)).eval(InternalRow.empty)
        assert(result != null)
        result.asInstanceOf[java.lang.Long].longValue shouldBe 2L
    }

    test("gbx_custom_distance: distance is symmetric") {
        val gridLit  = buildGridLit()
        val gridLit2 = buildGridLit()
        val cellA = sys.pointToCellID(15.0, 15.0, 0)
        val cellB = sys.pointToCellID(45.0, 35.0, 0)  // pos=(4,3): dx=3, dy=2 -> Chebyshev=3

        val dAB = Custom_Distance(Literal(cellA), gridLit,  Literal(cellB)).eval(InternalRow.empty).asInstanceOf[java.lang.Long].longValue
        val dBA = Custom_Distance(Literal(cellB), gridLit2, Literal(cellA)).eval(InternalRow.empty).asInstanceOf[java.lang.Long].longValue
        dAB shouldBe dBA
        dAB shouldBe 3L  // Chebyshev=max(3,2)=3
    }

    test("gbx_custom_distance: distance(a,b)==k means b is in kRing(a,k) but not kRing(a,k-1)") {
        // diagonal (dx=1,dy=1): Chebyshev=1, so b must be in kRing(a,1)
        val cellA = sys.pointToCellID(15.0, 15.0, 0)
        val cellB = sys.pointToCellID(25.0, 25.0, 0)
        val dist  = sys.distance(cellA, cellB)
        dist shouldBe 1L
        sys.kRing(cellA, 1) should contain(cellB)
        sys.kRing(cellA, 0) should not contain(cellB)
    }

    test("gbx_custom_distance: registers and runs as SQL function via functions.register") {
        TestL1.installAll(spark)
        val cell = sys.pointToCellID(15.0, 15.0, 0)
        val row  = spark.sql(
            s"""SELECT gbx_custom_distance(
               |  ${cell}L,
               |  gbx_custom_grid(0L, 100L, 0L, 100L, 2, 10, 10, 32633),
               |  ${cell}L
               |)""".stripMargin
        ).head()
        row.getLong(0) shouldBe 0L
    }

}
