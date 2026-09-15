package com.databricks.labs.gbx.gridx.quadbin

import com.databricks.labs.gbx.gridx.grid.{GeomDilation, Quadbin}
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Unit tests for Quadbin_GeometryKRing, Quadbin_GeometryKLoop, and their Explode variants.
  *
  * Simple fixture: NYC box (-73.99, 40.71 → -73.95, 40.75) at resolution 12.
  *
  * Holed fixture: large east-US box (-76, 38 → -72, 43) with an interior 2°×3° hole
  * (-75, 39 → -73, 42) at resolution 10 (cells ≈0.35° wide, ~5.7 cells across the hole).
  * At res 10 the hole spans ~6 x-tiles × ~9 y-tiles; cells well inside the hole
  * (x ∈ [299..303], y ∈ [381..390]) are fully contained by the hole polygon, so hCore is
  * non-empty. This ensures that hole-in/hole-out modes exercise genuine inward fill, not
  * just h_border traversal.
  */
class Quadbin_GeometrySuite extends AnyFunSuite {

    private val NYC_WKT  = "POLYGON((-73.99 40.71, -73.95 40.71, -73.95 40.75, -73.99 40.75, -73.99 40.71))"
    private val NYC_WKB  = JTS.toWKB(JTS.fromWKT(NYC_WKT))
    private val RES      = 12

    // Holed polygon: large east-US box with a 2°×3° interior hole at res 10.
    // Hole spans ~6 cells wide × ~9 cells tall → hCore is non-empty (verified below).
    private val HOLED_WKT =
        "POLYGON((-76 38, -72 38, -72 43, -76 43, -76 38), " +
        "(-75 39, -73 39, -73 42, -75 42, -75 39))"
    private val HOLED_WKB = JTS.toWKB(JTS.fromWKT(HOLED_WKT))
    private val HOLED_RES = 10  // cells ≈0.35°; 2°×3° hole spans ~6×9 tiles → hCore non-empty

    test("Quadbin_GeometryKRing — boundary-out (coveras) k=0 is the full straddling band") {
        // LOCKED: coveras boundary-out k0 = the full coveras band (sCover - sCore), with the
        // outer-perimeter fallback when the band is empty (grid-aligned geom).
        val geom   = JTS.fromWKB(NYC_WKB)
        val cls    = GeomDilation.classify(Quadbin, geom, RES)
        val band   = cls.sCover -- cls.sCore
        val expect = if (band.nonEmpty) band else GeomDilation.outerPerimeter(cls.sCover, Quadbin)
        val k0Out  = Quadbin_GeometryKRing.execute(geom, RES, 0, GeomDilation.DEFAULT_MODE)
        k0Out.nonEmpty shouldBe true
        k0Out shouldBe expect
    }

    test("Quadbin_GeometryKRing — boundary-out excludes the interior (sCore)") {
        // boundary-out = full band (k0) + outward band; the geom INTERIOR (fully-contained
        // sCore) is never returned.
        val geom = JTS.fromWKB(NYC_WKB)
        val k1   = Quadbin_GeometryKRing.execute(geom, RES, 1, GeomDilation.DEFAULT_MODE)
        val cls  = GeomDilation.classify(Quadbin, geom, RES)
        k1.size should be > 0
        k1.intersect(cls.sCore).isEmpty shouldBe true
    }

    test("Quadbin_GeometryKLoop — loop == ring diff") {
        val geom = JTS.fromWKB(NYC_WKB)
        val r2   = Quadbin_GeometryKRing.execute(geom, RES, 2, GeomDilation.DEFAULT_MODE)
        val r1   = Quadbin_GeometryKRing.execute(geom, RES, 1, GeomDilation.DEFAULT_MODE)
        val loop = Quadbin_GeometryKLoop.execute(geom, RES, 2, GeomDilation.DEFAULT_MODE)
        loop shouldBe (r2 diff r1)
    }

    test("Quadbin_GeometryKRing — returns LongType cell ids (all at correct resolution)") {
        val geom  = JTS.fromWKB(NYC_WKB)
        val cells = Quadbin_GeometryKRing.execute(geom, RES, 1, GeomDilation.DEFAULT_MODE)
        cells.size should be > 0
        cells.foreach(c => Quadbin.resolution(c) shouldBe RES)
    }

    test("Quadbin_GeometryKRing — eval(WKB) non-null and consistent with execute") {
        val geom   = JTS.fromWKB(NYC_WKB)
        val expect = Quadbin_GeometryKRing.execute(geom, RES, 1, GeomDilation.DEFAULT_MODE).toArray.sorted
        val arr    = Quadbin_GeometryKRing.eval(NYC_WKB, RES, 1)
        arr should not be null
        arr.toObjectArray(org.apache.spark.sql.types.LongType).map(_.asInstanceOf[Long]).sorted shouldBe expect
    }

    test("Quadbin_GeometryKLoop — eval(WKB) non-null and consistent with execute") {
        val geom   = JTS.fromWKB(NYC_WKB)
        val expect = Quadbin_GeometryKLoop.execute(geom, RES, 1, GeomDilation.DEFAULT_MODE).toArray.sorted
        val arr    = Quadbin_GeometryKLoop.eval(NYC_WKB, RES, 1)
        arr should not be null
        arr.toObjectArray(org.apache.spark.sql.types.LongType).map(_.asInstanceOf[Long]).sorted shouldBe expect
    }

    test("holed fixture — hCore is non-empty (fixture correctness gate)") {
        // Asserts the fixture is adequate: the 2°×3° hole at res 10 contains at least
        // one quadbin cell fully inside the hole polygon. If this fails, the hole modes
        // below only exercise h_border traversal (not genuine inward fill).
        val geom = JTS.fromWKT(HOLED_WKT)
        val cls  = GeomDilation.classify(Quadbin, geom, HOLED_RES)
        cls.hCore.size should be > 0
    }

    test("Quadbin_GeometryKRing — all 6 modes produce correct-resolution cells (no exception)") {
        val geom = JTS.fromWKT(HOLED_WKT)
        GeomDilation.MODES.foreach { mode =>
            val cells = Quadbin_GeometryKRing.execute(geom, HOLED_RES, 1, mode)
            cells.foreach(c => Quadbin.resolution(c) shouldBe HOLED_RES)
        }
    }

    test("Quadbin_GeometryKLoop — all 6 modes on holed polygon: no exception") {
        val geom = JTS.fromWKT(HOLED_WKT)
        GeomDilation.MODES.foreach { mode =>
            val cells = Quadbin_GeometryKLoop.execute(geom, HOLED_RES, 1, mode)
            cells.foreach(c => Quadbin.resolution(c) shouldBe HOLED_RES)
        }
    }

    test("hole-in mode — reaches hCore after sufficient dilation steps") {
        // hole-in fills inward from the hole boundary. After k=3 steps the expansion
        // should include cells fully inside the hole (hCore), proving the engine
        // genuinely traverses the interior rather than stopping at h_border.
        // Also asserts the result is wholly within hCover (no cells outside the hole).
        val geom = JTS.fromWKT(HOLED_WKT)
        val cls  = GeomDilation.classify(Quadbin, geom, HOLED_RES)
        // Fixture gate already checked; fail clearly if hCore somehow became empty.
        cls.hCore.size should be > 0
        val expanded = GeomDilation.expand("ring", 3, "hole-in", Quadbin, geom, HOLED_RES)
        // All result cells must be within the hole region (hCover).
        expanded.subsetOf(cls.hCover) shouldBe true
        // After 3 steps inward from h_border, we must reach some hCore cells.
        expanded.intersect(cls.hCore).nonEmpty shouldBe true
        // hole-in must not reach cells inside the solid (pCore \ hCover).
        val solidOnly = cls.pCore diff cls.hCover
        expanded.intersect(solidOnly) shouldBe empty
    }

    test("Quadbin_GeometryKRing — unknown mode raises") {
        val geom = JTS.fromWKB(NYC_WKB)
        assertThrows[IllegalArgumentException] {
            Quadbin_GeometryKRing.execute(geom, RES, 1, "BOGUS")
        }
    }

    test("Quadbin_GeometryKLoop — unknown mode raises") {
        val geom = JTS.fromWKB(NYC_WKB)
        assertThrows[IllegalArgumentException] {
            Quadbin_GeometryKLoop.execute(geom, RES, 1, "BOGUS")
        }
    }

    test("Quadbin_GeometryKRing — null geom eval returns null") {
        val arr = Quadbin_GeometryKRing.eval(null.asInstanceOf[Array[Byte]], RES, 1)
        arr shouldBe null
    }

    test("Quadbin_GeometryKLoop — null geom eval returns null") {
        val arr = Quadbin_GeometryKLoop.eval(null.asInstanceOf[Array[Byte]], RES, 1)
        arr shouldBe null
    }

}
