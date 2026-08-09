package com.databricks.labs.gbx.gridx.custom

import com.databricks.labs.gbx.gridx.grid.{CustomGridSystem, GridConf}
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.apache.spark.sql.types.{BinaryType, IntegerType, LongType}

/** Covers the param/data split:
  *   - PARAMETER (resolution, k, grid-spec) -> always raises even through expression .eval()
  *   - DATA (NaN/out-of-bounds coords, bad geometry bytes) -> null (not a raise)
  */
class Custom_DegradeTest extends PlanTest with SilentSparkSession {

    // ─────────────────────────────────────────────────────────────────
    // Direct-method tests (CustomGridSystem.pointToCellIdOrNull)
    // Grid: 0..1_000_000, cellSplits=10 → maxResolution=8
    // ─────────────────────────────────────────────────────────────────
    private def sys = CustomGridSystem(GridConf(
        boundXMin     = 0L,
        boundXMax     = 1000000L,
        boundYMin     = 0L,
        boundYMax     = 1000000L,
        cellSplits    = 10,
        rootCellSizeX = 100000,
        rootCellSizeY = 100000
    ))

    test("pointToCellIdOrNull returns null for out-of-bounds x (data)") {
        assert(sys.pointToCellIdOrNull(-5.0, 500000.0, 0) == null)
    }

    test("pointToCellIdOrNull returns null for NaN y (data)") {
        assert(sys.pointToCellIdOrNull(500000.0, Double.NaN, 0) == null)
    }

    test("pointToCellIdOrNull still RAISES for resolution over max (parameter)") {
        assertThrows[IllegalStateException](sys.pointToCellIdOrNull(500000.0, 500000.0, 99))
    }

    test("pointToCellIdOrNull returns a cell for an in-bounds point") {
        assert(sys.pointToCellIdOrNull(500000.0, 500000.0, 0) != null)
    }

    // ─────────────────────────────────────────────────────────────────
    // Expression-level tests: Custom_PointAsCell and Custom_Polyfill
    // Grid: (0,100, 0,100, cellSplits=2, rootX=10, rootY=10, srid=-1)
    //   maxResolution = 20; resolution=99 is over-max.
    // ─────────────────────────────────────────────────────────────────
    private def buildGridLit(): Literal = {
        val gridRow = Custom_Grid(
            Literal(0L,   LongType),
            Literal(100L, LongType),
            Literal(0L,   LongType),
            Literal(100L, LongType),
            Literal(2,    IntegerType),
            Literal(10,   IntegerType),
            Literal(10,   IntegerType),
            Literal(-1,   IntegerType)
        ).eval(InternalRow.empty).asInstanceOf[InternalRow]
        Literal.create(gridRow, Custom_GridSpec.gridStructType)
    }

    // Custom_PointAsCell: bad resolution must raise through .eval() (parameter, not null)
    test("Custom_PointAsCell.eval raises for over-max resolution (expression-level parameter check)") {
        val gridLit  = buildGridLit()
        val pointWkb = JTS.toWKB(JTS.point(5.0, 5.0))
        val pointLit = Literal.create(pointWkb, BinaryType)
        val resLit   = Literal(99, IntegerType)

        val expr = Custom_PointAsCell(pointLit, gridLit, resLit)
        assertThrows[IllegalStateException](expr.eval(InternalRow.empty))
    }

    // Custom_PointAsCell: valid input returns non-null (data path works normally)
    test("Custom_PointAsCell.eval returns a cell for a valid in-bounds point (expression-level)") {
        val gridLit  = buildGridLit()
        val pointWkb = JTS.toWKB(JTS.point(5.0, 5.0))
        val pointLit = Literal.create(pointWkb, BinaryType)
        val resLit   = Literal(0, IntegerType)

        val result = Custom_PointAsCell(pointLit, gridLit, resLit).eval(InternalRow.empty)
        assert(result != null)
    }

    // Custom_PointAsCell: out-of-bounds coord returns null (data, not a raise)
    test("Custom_PointAsCell.eval returns null for out-of-bounds point coordinate (expression-level data)") {
        val gridLit  = buildGridLit()
        val pointWkb = JTS.toWKB(JTS.point(-5.0, 5.0))  // x out of [0,100)
        val pointLit = Literal.create(pointWkb, BinaryType)
        val resLit   = Literal(0, IntegerType)

        val result = Custom_PointAsCell(pointLit, gridLit, resLit).eval(InternalRow.empty)
        assert(result == null)
    }

    // Custom_Polyfill: bad resolution must raise through .eval() (parameter, not null)
    test("Custom_Polyfill.eval raises for over-max resolution (expression-level parameter check)") {
        val gridLit  = buildGridLit()
        val polyWkb  = JTS.toWKB(JTS.fromWKT("POLYGON ((0 0, 30 0, 30 30, 0 30, 0 0))"))
        val geomLit  = Literal.create(polyWkb, BinaryType)
        val resLit   = Literal(99, IntegerType)

        val expr = Custom_Polyfill(geomLit, gridLit, resLit)
        assertThrows[IllegalStateException](expr.eval(InternalRow.empty))
    }

    // Custom_Polyfill: valid input returns non-null array (data path works normally)
    test("Custom_Polyfill.eval returns non-null array for valid geometry and resolution (expression-level)") {
        val gridLit  = buildGridLit()
        val polyWkb  = JTS.toWKB(JTS.fromWKT("POLYGON ((0 0, 30 0, 30 30, 0 30, 0 0))"))
        val geomLit  = Literal.create(polyWkb, BinaryType)
        val resLit   = Literal(0, IntegerType)

        val result = Custom_Polyfill(geomLit, gridLit, resLit).eval(InternalRow.empty)
        assert(result != null)
    }

}
