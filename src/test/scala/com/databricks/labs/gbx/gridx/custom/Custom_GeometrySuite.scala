package com.databricks.labs.gbx.gridx.custom

import com.databricks.labs.gbx.gridx.grid.{CustomGridSystem, GeomDilation, GridConf}
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Unit tests for Custom_GeometryKRing, Custom_GeometryKLoop, and their Explode variants.
  *
  * Fixture grid: 0..1,000,000 × 0..1,000,000, cell_splits=2, root_size=1000 (res-0 cells 1 000-unit squares).
  *
  * Simple fixture: 5 000 × 5 000 box at (530 000, 180 000) → 5×5=25 cells at res 0.
  *
  * Holed fixture: 60 km outer box (500 000..560 000 × 100 000..160 000) with an interior hole
  * inset 500 units from the cell-grid lines (510 500..549 500 × 110 500..149 500).
  * At res 0 (1 000-unit cells):
  *  - h_border: cells straddling the hole edge (e.g. cell [510 000..511 000]^2 partially overlaps)
  *  - h_core:   38 × 38 = 1 444 cells for x∈[511..548], y∈[111..148] — fully inside the hole
  * The h_core is non-empty so hole-in/hole-out genuinely exercise inward fill.
  */
class Custom_GeometrySuite extends AnyFunSuite {

    // ------------------------------------------------------------------
    // Grid fixture
    // ------------------------------------------------------------------
    private val GRID_CONF = GridConf(
        boundXMin     = 0L,
        boundXMax     = 1_000_000L,
        boundYMin     = 0L,
        boundYMax     = 1_000_000L,
        cellSplits    = 2,
        rootCellSizeX = 1000,
        rootCellSizeY = 1000,
        crsID         = None
    )
    private val SYS = CustomGridSystem(GRID_CONF)

    // ------------------------------------------------------------------
    // Geometry fixtures
    // ------------------------------------------------------------------
    // Simple polygon: 4 600 × 4 600 box at (530 200, 180 200) → NOT aligned to cell
    // boundaries so cells on the edges straddle the polygon boundary (p_border non-empty)
    // and interior cells are fully contained (p_core non-empty).
    // At res 0 (1 000-unit cells) polyfill covers ~25 cells; k=1 boundary-out is larger.
    private val SIMPLE_WKT = "POLYGON((530200 180200, 534800 180200, 534800 184800, 530200 184800, 530200 180200))"
    private val SIMPLE_WKB = JTS.toWKB(JTS.fromWKT(SIMPLE_WKT))
    private val SIMPLE_RES = 0

    // Holed: 60 km outer, hole inset 500 units from cell-grid lines.
    private val HOLED_WKT =
        "POLYGON((500000 100000, 560000 100000, 560000 160000, 500000 160000, 500000 100000), " +
        "(510500 110500, 549500 110500, 549500 149500, 510500 149500, 510500 110500))"
    private val HOLED_WKB = JTS.toWKB(JTS.fromWKT(HOLED_WKT))
    private val HOLED_RES = 0

    // ------------------------------------------------------------------
    // Basic properties on the simple fixture
    // ------------------------------------------------------------------

    test("Custom_GeometryKRing — boundary-out (coveras) k=0 is the full straddling band") {
        // LOCKED: coveras boundary-out k0 = the full coveras band (sCover - sCore), with the
        // outer-perimeter fallback when the band is empty (grid-aligned geom).
        val geom  = JTS.fromWKB(SIMPLE_WKB)
        val cls   = GeomDilation.classify(SYS, geom, SIMPLE_RES)
        val band  = cls.sCover -- cls.sCore
        val expect = if (band.nonEmpty) band else GeomDilation.outerPerimeter(cls.sCover, SYS)
        val k0Out = SYS.geometryKRing(geom, SIMPLE_RES, 0, GeomDilation.DEFAULT_MODE)
        k0Out.nonEmpty shouldBe true
        k0Out shouldBe expect
    }

    test("Custom_GeometryKRing — boundary-out excludes the interior (sCore)") {
        // boundary-out = full band (k0) + outward band; the geom INTERIOR (sCore) excluded.
        val geom = JTS.fromWKB(SIMPLE_WKB)
        val k1   = SYS.geometryKRing(geom, SIMPLE_RES, 1, GeomDilation.DEFAULT_MODE)
        val cls  = GeomDilation.classify(SYS, geom, SIMPLE_RES)
        k1.size should be > 0
        k1.intersect(cls.sCore).isEmpty shouldBe true
    }

    test("Custom_GeometryKLoop — loop == ring diff") {
        val geom = JTS.fromWKB(SIMPLE_WKB)
        val r2   = SYS.geometryKRing(geom, SIMPLE_RES, 2, GeomDilation.DEFAULT_MODE)
        val r1   = SYS.geometryKRing(geom, SIMPLE_RES, 1, GeomDilation.DEFAULT_MODE)
        val loop = SYS.geometryKLoop(geom, SIMPLE_RES, 2, GeomDilation.DEFAULT_MODE)
        loop shouldBe (r2 diff r1)
    }

    test("Custom_GeometryKRing — returns LongType cell ids") {
        val geom  = JTS.fromWKB(SIMPLE_WKB)
        val cells = SYS.geometryKRing(geom, SIMPLE_RES, 1, GeomDilation.DEFAULT_MODE)
        cells.size should be > 0
        // All cell IDs at res 0 decode to resolution 0.
        val ID_BITS = 56
        cells.foreach(c => assert((c >> ID_BITS).toInt == SIMPLE_RES))
    }

    // ------------------------------------------------------------------
    // Expression eval (CodegenFallback path)
    // ------------------------------------------------------------------

    test("Custom_GeometryKRing expression eval — WKB returns non-null ArrayData") {
        import org.apache.spark.sql.catalyst.InternalRow
        import org.apache.spark.sql.catalyst.expressions.Literal
        import org.apache.spark.sql.catalyst.util.ArrayData
        import org.apache.spark.sql.types.{BinaryType, IntegerType, LongType}

        val gridRow = Custom_Grid(
            Literal(0L, LongType), Literal(1_000_000L, LongType),
            Literal(0L, LongType), Literal(1_000_000L, LongType),
            Literal(2, IntegerType), Literal(1000, IntegerType), Literal(1000, IntegerType),
            Literal(-1, IntegerType)
        ).eval(InternalRow.empty).asInstanceOf[InternalRow]

        val expr = Custom_GeometryKRing(
            Literal(SIMPLE_WKB, BinaryType),
            Literal.create(gridRow, Custom_GridSpec.gridStructType),
            Literal(SIMPLE_RES, IntegerType),
            Literal(1, IntegerType),
            Literal(GeomDilation.DEFAULT_MODE),
            Literal(GeomDilation.DEFAULT_COVERAGE)
        )
        val result = expr.eval(InternalRow.empty)
        assert(result != null)
        assert(result.asInstanceOf[ArrayData].numElements() > 0)
    }

    test("Custom_GeometryKRing expression eval — WKT returns same cells as WKB") {
        import org.apache.spark.sql.catalyst.InternalRow
        import org.apache.spark.sql.catalyst.expressions.Literal
        import org.apache.spark.sql.catalyst.util.ArrayData
        import org.apache.spark.sql.types.{BinaryType, IntegerType, LongType, StringType}
        import org.apache.spark.unsafe.types.UTF8String

        val gridRow = Custom_Grid(
            Literal(0L, LongType), Literal(1_000_000L, LongType),
            Literal(0L, LongType), Literal(1_000_000L, LongType),
            Literal(2, IntegerType), Literal(1000, IntegerType), Literal(1000, IntegerType),
            Literal(-1, IntegerType)
        ).eval(InternalRow.empty).asInstanceOf[InternalRow]

        val exprWKT = Custom_GeometryKRing(
            Literal(UTF8String.fromString(SIMPLE_WKT), StringType),
            Literal.create(gridRow, Custom_GridSpec.gridStructType),
            Literal(SIMPLE_RES, IntegerType),
            Literal(1, IntegerType),
            Literal(GeomDilation.DEFAULT_MODE),
            Literal(GeomDilation.DEFAULT_COVERAGE)
        )
        val exprWKB = Custom_GeometryKRing(
            Literal(SIMPLE_WKB, BinaryType),
            Literal.create(gridRow, Custom_GridSpec.gridStructType),
            Literal(SIMPLE_RES, IntegerType),
            Literal(1, IntegerType),
            Literal(GeomDilation.DEFAULT_MODE),
            Literal(GeomDilation.DEFAULT_COVERAGE)
        )

        val wktResult = exprWKT.eval(InternalRow.empty).asInstanceOf[ArrayData]
        val wkbResult = exprWKB.eval(InternalRow.empty).asInstanceOf[ArrayData]

        assert(wktResult != null, "WKT eval returned null")
        assert(wkbResult != null, "WKB eval returned null")

        val wktCells = (0 until wktResult.numElements()).map(wktResult.getLong(_)).toSet
        val wkbCells = (0 until wkbResult.numElements()).map(wkbResult.getLong(_)).toSet
        assert(
          wktCells == wkbCells,
          s"WKT and WKB eval paths differ: WKT=${wktCells.size} WKB=${wkbCells.size} " +
          s"wkt_only=${(wktCells diff wkbCells).take(5)} wkb_only=${(wkbCells diff wktCells).take(5)}"
        )
    }

    test("Custom_GeometryKLoop expression eval — WKB returns non-null ArrayData") {
        import org.apache.spark.sql.catalyst.InternalRow
        import org.apache.spark.sql.catalyst.expressions.Literal
        import org.apache.spark.sql.catalyst.util.ArrayData
        import org.apache.spark.sql.types.{BinaryType, IntegerType, LongType}

        val gridRow = Custom_Grid(
            Literal(0L, LongType), Literal(1_000_000L, LongType),
            Literal(0L, LongType), Literal(1_000_000L, LongType),
            Literal(2, IntegerType), Literal(1000, IntegerType), Literal(1000, IntegerType),
            Literal(-1, IntegerType)
        ).eval(InternalRow.empty).asInstanceOf[InternalRow]

        val expr = Custom_GeometryKLoop(
            Literal(SIMPLE_WKB, BinaryType),
            Literal.create(gridRow, Custom_GridSpec.gridStructType),
            Literal(SIMPLE_RES, IntegerType),
            Literal(1, IntegerType),
            Literal(GeomDilation.DEFAULT_MODE),
            Literal(GeomDilation.DEFAULT_COVERAGE)
        )
        val result = expr.eval(InternalRow.empty)
        assert(result != null)
        assert(result.asInstanceOf[ArrayData].numElements() > 0)
    }

    // ------------------------------------------------------------------
    // Holed fixture correctness gate
    // ------------------------------------------------------------------

    test("holed fixture — hCore is non-empty (fixture correctness gate)") {
        val geom = JTS.fromWKT(HOLED_WKT)
        val cls  = GeomDilation.classify(SYS, geom, HOLED_RES)
        cls.hCore.size should be > 0
    }

    test("Custom_GeometryKRing — all 6 modes produce correct-resolution cells") {
        val geom    = JTS.fromWKT(HOLED_WKT)
        val ID_BITS = 56
        GeomDilation.MODES.foreach { mode =>
            val cells = SYS.geometryKRing(geom, HOLED_RES, 1, mode)
            cells.foreach(c => assert((c >> ID_BITS).toInt == HOLED_RES))
        }
    }

    test("Custom_GeometryKLoop — all 6 modes on holed polygon produce correct-resolution cells") {
        val geom    = JTS.fromWKT(HOLED_WKT)
        val ID_BITS = 56
        GeomDilation.MODES.foreach { mode =>
            val cells = SYS.geometryKLoop(geom, HOLED_RES, 1, mode)
            cells.foreach(c => assert((c >> ID_BITS).toInt == HOLED_RES))
        }
    }

    test("hole-in mode — reaches hCore after sufficient dilation steps") {
        val geom = JTS.fromWKT(HOLED_WKT)
        val cls  = GeomDilation.classify(SYS, geom, HOLED_RES)
        cls.hCore.size should be > 0
        val expanded = GeomDilation.expand("ring", 3, "hole-in", SYS, geom, HOLED_RES)
        // All result cells must be within the hole region.
        expanded.subsetOf(cls.hCover) shouldBe true
        // After 3 steps inward from h_border, must reach some hCore cells.
        expanded.intersect(cls.hCore).nonEmpty shouldBe true
        // hole-in must not reach cells only in the solid (pCore \ hCover).
        val solidOnly = cls.pCore diff cls.hCover
        expanded.intersect(solidOnly) shouldBe empty
    }

    test("Custom_GeometryKRing — unknown mode raises") {
        val geom = JTS.fromWKB(SIMPLE_WKB)
        assertThrows[IllegalArgumentException] {
            SYS.geometryKRing(geom, SIMPLE_RES, 1, "BOGUS")
        }
    }

    test("Custom_GeometryKLoop — unknown mode raises") {
        val geom = JTS.fromWKB(SIMPLE_WKB)
        assertThrows[IllegalArgumentException] {
            SYS.geometryKLoop(geom, SIMPLE_RES, 1, "BOGUS")
        }
    }

    test("Custom_GeometryKRing — null geom eval returns null") {
        import org.apache.spark.sql.catalyst.InternalRow
        import org.apache.spark.sql.catalyst.expressions.Literal
        import org.apache.spark.sql.types.{BinaryType, IntegerType, LongType}

        val gridRow = Custom_Grid(
            Literal(0L, LongType), Literal(1_000_000L, LongType),
            Literal(0L, LongType), Literal(1_000_000L, LongType),
            Literal(2, IntegerType), Literal(1000, IntegerType), Literal(1000, IntegerType),
            Literal(-1, IntegerType)
        ).eval(InternalRow.empty).asInstanceOf[InternalRow]

        val expr = Custom_GeometryKRing(
            Literal(null, BinaryType),
            Literal.create(gridRow, Custom_GridSpec.gridStructType),
            Literal(SIMPLE_RES, IntegerType),
            Literal(1, IntegerType),
            Literal(GeomDilation.DEFAULT_MODE),
            Literal(GeomDilation.DEFAULT_COVERAGE)
        )
        assert(expr.eval(InternalRow.empty) == null)
    }

}
