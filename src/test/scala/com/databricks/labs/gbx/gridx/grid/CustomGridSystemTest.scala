package com.databricks.labs.gbx.gridx.grid

import com.databricks.labs.gbx.vectorx.jts.JTS
import org.locationtech.jts.geom.Coordinate
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CustomGridSystemTest extends AnyFunSuite with Matchers {

    val conf = GridConf(
      boundXMin = 0,
      boundXMax = 100,
      boundYMin = 0,
      boundYMax = 100,
      cellSplits = 2,
      rootCellSizeX = 10,
      rootCellSizeY = 10,
      crsID = Some(32633)
    )
    val g = CustomGridSystem(conf)

    // res 0: cellWidth=10, totalCellsX=10, rootCellCountX=ceil(100/10)=10

    test("pointToCellID res0 at (5,5) has resolution 0 and envelope [0,10]x[0,10]") {
        val id = g.pointToCellID(5.0, 5.0, 0)
        g.getCellResolution(id) shouldBe 0
        val env = g.cellIdToGeometry(id).getEnvelopeInternal
        env.getMinX shouldBe 0.0 +- 1e-9
        env.getMaxX shouldBe 10.0 +- 1e-9
        env.getMinY shouldBe 0.0 +- 1e-9
        env.getMaxY shouldBe 10.0 +- 1e-9
    }

    test("pointToCellID res0 at (15,25) has envelope [10,20]x[20,30]") {
        val id = g.pointToCellID(15.0, 25.0, 0)
        val env = g.cellIdToGeometry(id).getEnvelopeInternal
        env.getMinX shouldBe 10.0 +- 1e-9
        env.getMaxX shouldBe 20.0 +- 1e-9
        env.getMinY shouldBe 20.0 +- 1e-9
        env.getMaxY shouldBe 30.0 +- 1e-9
    }

    test("pointToCellID res1 at (2.5,2.5) has resolution 1 and envelope [0,5]x[0,5]") {
        // res1: cellWidth=10/2^1=5
        val id = g.pointToCellID(2.5, 2.5, 1)
        g.getCellResolution(id) shouldBe 1
        val env = g.cellIdToGeometry(id).getEnvelopeInternal
        env.getMinX shouldBe 0.0 +- 1e-9
        env.getMaxX shouldBe 5.0 +- 1e-9
        env.getMinY shouldBe 0.0 +- 1e-9
        env.getMaxY shouldBe 5.0 +- 1e-9
    }

    test("cellIdToCenter at res0 (5,5) is approximately (5,5)") {
        val id = g.pointToCellID(5.0, 5.0, 0)
        val center: Coordinate = g.cellIdToCenter(id)
        center.x shouldBe 5.0 +- 1e-9
        center.y shouldBe 5.0 +- 1e-9
    }

    test("polyfill POLYGON((0 0, 30 0, 30 30, 0 30, 0 0)) at res0 returns 9 cells all within [0,30]x[0,30]") {
        val poly = JTS.fromWKT("POLYGON ((0 0, 30 0, 30 30, 0 30, 0 0))")
        val cells = g.polyfill(poly, 0)
        cells should have size 9
        cells.foreach { cellId =>
            val env = g.cellIdToGeometry(cellId).getEnvelopeInternal
            env.getMinX should be >= 0.0
            env.getMaxX should be <= 30.0
            env.getMinY should be >= 0.0
            env.getMaxY should be <= 30.0
        }
    }

    test("kRing at res0 (15,15) with k=1 returns 9 entries including center") {
        // cellPosX=1, cellPosY=1 => interior 3x3 ring
        val center = g.pointToCellID(15.0, 15.0, 0)
        val ring = g.kRing(center, 1)
        ring should have size 9
        ring should contain(center)
    }

    // the 10 x 10 cells at res 0 run from position 0 to 9 on each axis
    private def pos(id: Long): (Long, Long) = {
        val p = g.getCellPosition(id)
        (g.getCellPositionX(p, 0), g.getCellPositionY(p, 0))
    }

    test("kRing at the east edge holds only the cells that exist, none wrapped into the next row") {
        val east = g.pointToCellID(95.0, 55.0, 0) // (9, 5)
        val ring = g.kRing(east, 1)
        ring.map(pos).toSet shouldBe (for (x <- 8L to 9L; y <- 4L to 6L) yield (x, y)).toSet
        ring should have size 6
    }

    test("kRing at the north edge names no cell outside the grid") {
        val north = g.pointToCellID(55.0, 95.0, 0) // (5, 9)
        val ring = g.kRing(north, 1)
        ring.map(pos).toSet shouldBe (for (x <- 4L to 6L; y <- 8L to 9L) yield (x, y)).toSet
        ring.foreach(c => g.getCellPosition(c) should be < 100L)
    }

    test("kRing and kLoop at the north-east corner hold the four and three cells of the grid") {
        val corner = g.pointToCellID(95.0, 95.0, 0) // (9, 9)
        g.kRing(corner, 1).map(pos).toSet shouldBe Set((8L, 8L), (8L, 9L), (9L, 8L), (9L, 9L))
        g.kLoop(corner, 1).map(pos).toSet shouldBe Set((8L, 8L), (8L, 9L), (9L, 8L))
        g.kLoop(corner, 2).map(pos).toSet shouldBe Set((7L, 7L), (7L, 8L), (7L, 9L), (8L, 7L), (9L, 7L))
    }

    test("pointToCellID rejects a NaN X coordinate via the NaN guard") {
        val ex = the[IllegalStateException] thrownBy g.pointToCellID(Double.NaN, 5.0, 0)
        ex.getMessage should include("NaN coordinates are not supported")
    }

    test("pointToCellID rejects a NaN Y coordinate via the NaN guard") {
        // Resolved decision 3: the guard formerly read `!x.isNaN && !x.isNaN`, so a NaN Y
        // was never caught by the NaN guard. Pre-fix a NaN Y is only (incidentally) rejected
        // by the later Y-bounds `require` (NaN comparisons are false), surfacing the wrong
        // message. The fix makes the NaN guard itself catch a NaN Y -> assert that message.
        val ex = the[IllegalStateException] thrownBy g.pointToCellID(5.0, Double.NaN, 0)
        ex.getMessage should include("NaN coordinates are not supported")
    }

    // --- GridSystem conformance ---

    test("CustomGridSystem conforms to GridSystem trait") {
        val gTrait: GridSystem = CustomGridSystem(GridConf(
            boundXMin    = 0,
            boundXMax    = 100,
            boundYMin    = 0,
            boundYMax    = 100,
            cellSplits   = 2,
            rootCellSizeX = 10,
            rootCellSizeY = 10,
            crsID        = Some(27700)
        ))
        gTrait.name shouldBe "CUSTOM"
        gTrait.crsSrid shouldBe 27700
        val cell = gTrait.pointToCellID(50.0, 50.0, 0)
        gTrait.cellIdToGeometry(cell).getEnvelopeInternal.getMinX should be <= 50.0
    }

    test("coveringCandidateCells includes all cells whose square overlaps the bbox") {
        // resolution 0: cell size 10x10; bbox straddles four cell corners at (10,10)
        val bbox = JTS.fromWKT("POLYGON ((8 8, 12 8, 12 12, 8 12, 8 8))")
        val candidates = g.coveringCandidateCells(bbox, 0)
        candidates should not be empty
        // each of the four cells that the bbox overlaps must be in the candidate set;
        // identify each cell by a point clearly inside it
        Seq((5.0, 5.0), (15.0, 5.0), (5.0, 15.0), (15.0, 15.0)).foreach { case (px, py) =>
            val cellId = g.pointToCellID(px, py, 0)
            candidates should contain(cellId)
        }
    }

}
