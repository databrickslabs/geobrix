package com.databricks.labs.gbx.gridx.grid

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Tests for Quadbin grid system, focusing on GridSystem conformance. */
class QuadbinTest extends AnyFunSuite {

    test("Quadbin conforms to GridSystem; cellIdToGeometry matches cellBbox") {
        val g: GridSystem = Quadbin
        g.name shouldBe "QUADBIN"
        g.crsSrid shouldBe 4326
        val cell = g.pointToCellID(-0.1276, 51.5074, 10)   // delegates to pointToCell
        val (xmin, ymin, xmax, ymax) = Quadbin.cellBbox(cell)
        val env = g.cellIdToGeometry(cell).getEnvelopeInternal
        env.getMinX shouldBe (xmin +- 1e-9)
        env.getMaxX shouldBe (xmax +- 1e-9)
        env.getMinY shouldBe (ymin +- 1e-9)
        env.getMaxY shouldBe (ymax +- 1e-9)
    }

    test("Quadbin.polyfill returns cells intersecting the input geometry") {
        val g: GridSystem = Quadbin
        val poly = g.cellIdToGeometry(g.pointToCellID(-0.13, 51.5, 12))
        g.polyfill(poly, 12) should contain (g.pointToCellID(-0.13, 51.5, 12))
    }

    test("Quadbin.coveringCandidateCells returns non-empty set containing the cell for a small bbox") {
        val g: GridSystem = Quadbin
        val center = g.pointToCellID(-0.13, 51.5, 10)
        val bbox = g.cellIdToGeometry(center)
        val covering = g.coveringCandidateCells(bbox, 10)
        covering should not be empty
        covering should contain (center)
    }

    test("Quadbin.resolutions spans 0..MAX_RESOLUTION") {
        Quadbin.resolutions shouldBe (0 to Quadbin.MAX_RESOLUTION).toSet
    }

    test("Quadbin.renderCellId returns the Long (default)") {
        val cell = Quadbin.pointToCell(-0.13, 51.5, 10)
        Quadbin.renderCellId(cell) shouldBe cell
    }

    // The latitudes at which the zoom-10 web-mercator tile row k starts, as doubles. The fast formula's tan, cos
    // and log differ between platforms by an ulp, so a pixel centre ON such a latitude used to land one row away
    // from the lightweight tier's on some of them. The row of the double is decided exactly: 17, 119 and 510 here
    // (the doubles lie just north of the edges 18 and 120 and just south of 511 -- the row below -- by 4e-14,
    // 2e-14 and 6e-17 of a row).
    private val edgeRows = Seq((18, 17L), (120, 119L), (511, 510L))

    for ((k, row) <- edgeRows) {
        test(s"Quadbin.lonLatToTile decides the row of the double at the edge of row $k exactly: $row") {
            val n   = 1 << 10
            val lat = math.toDegrees(math.atan(math.sinh(math.Pi * (1.0 - 2.0 * k / n))))
            Quadbin.lonLatToTile(10.0, lat, 10)._2 shouldBe row
        }
    }

    test("Quadbin.lonLatToTile away from a row edge keeps the fast formula") {
        // London: nowhere near an edge
        val (x, y) = Quadbin.lonLatToTile(-0.1276, 51.5074, 10)
        (x, y) shouldBe ((511L, 340L))
    }
}
