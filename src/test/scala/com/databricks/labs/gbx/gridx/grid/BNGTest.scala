package com.databricks.labs.gbx.gridx.grid

import com.databricks.labs.gbx.vectorx.jts.JTS
import org.locationtech.jts.io.WKTReader
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Conformance tests: BNG implements GridSystem correctly. */
class BNGTest extends AnyFunSuite {

    test("BNG conforms to GridSystem and renders cell ids as strings") {
        val g: GridSystem = BNG
        g.name shouldBe "BNG"
        g.crsSrid shouldBe 27700
        val cell = g.pointToCellID(530000.0, 180000.0, BNG.getResolution("1km"))
        g.renderCellId(cell) shouldBe a[String]
        g.renderCellId(cell) shouldBe BNG.format(cell)
        g.polyfill(g.cellIdToGeometry(cell), BNG.getResolution("1km")) should not be empty
    }

    test("bng geometryKRing boundary-out (mode) equals legacy no-mode") {
        val wkt = new WKTReader()
        val g = wkt.read("POLYGON((529000 179000,529000 182000,532000 182000,532000 179000,529000 179000))")
        assert(BNG.geometryKRing(g, 3, 1, "boundary-out") == BNG.geometryKRing(g, 3, 1))
    }

    test("bng geometryKRing boundary-out expands outward while boundary-in stays inside") {
        val wkt = new WKTReader()
        val g = wkt.read("POLYGON((528000 178000,528000 184000,534000 184000,534000 178000,528000 178000))")
        val cls = GeomDilation.classify(BNG, g, 3)
        val out = BNG.geometryKRing(g, 3, 1, "boundary-out")
        val inn = BNG.geometryKRing(g, 3, 1, "boundary-in")
        // LOCKED: boundary-out expands OUTWARD (reaches cells outside the geom); boundary-in stays
        // within the covering set. This geom is grid-aligned (no straddling band), so k0 falls back
        // to the outer ring of sCover — the sCore-exclusion is a non-aligned property, checked in
        // the *Geometry suites on non-aligned geoms, not here.
        assert(inn.nonEmpty, "boundary-in must be non-empty")
        assert(inn.subsetOf(cls.pCover), "boundary-in must stay within the covering set")
        assert(out.exists(c => !cls.sCover.contains(c)), "boundary-out must reach cells outside the geom")
    }

    test("bng geometryKRing boundary-out expands outward for aligned geom (alignment-robustness)") {
        val wkt = new WKTReader()
        // 3 km × 3 km box with corners exactly on 1 km BNG grid lines → no straddling cells.
        // The old straddling-cell (pBorder) seed produced no outward expansion for such geoms.
        // With the perimeter fix boundary-out expands; the boundary ring (k0) is non-empty and the
        // geom INTERIOR is excluded (any covering cell in the ring is on the boundary ring k0).
        val g   = wkt.read("POLYGON((529000 179000,529000 182000,532000 182000,532000 179000,529000 179000))")
        val cls = GeomDilation.classify(BNG, g, 3)
        assert(cls.pBorder.isEmpty, "test precondition: aligned polygon must have no straddling cells (pBorder empty)")
        val k0   = BNG.geometryKRing(g, 3, 0)
        val ring = BNG.geometryKRing(g, 3, 1)
        assert(k0.nonEmpty, "boundary-out k0 (full band / fallback ring) must be non-empty when pBorder is empty")
        assert(ring.nonEmpty,
          "boundary-out must expand outward even when pBorder is empty (alignment-robustness)")
        assert(ring.exists(c => !cls.sCover.contains(c)), "boundary-out reaches cells outside the geom")
    }

    test("bng geometryKLoop boundary-out outward shell for aligned geom") {
        val wkt = new WKTReader()
        val g   = wkt.read("POLYGON((529000 179000,529000 182000,532000 182000,532000 179000,529000 179000))")
        val r1  = BNG.geometryKRing(g, 3, 1)
        val r0  = BNG.geometryKRing(g, 3, 0)
        val l1  = BNG.geometryKLoop(g, 3, 1)
        // kLoop must equal ring(k) \ ring(k-1) for the BNG standalone methods
        assert(l1 == r1.diff(r0), "bng geometryKLoop must equal ring(1) diff ring(0)")
    }

    test("BNG.coveringCandidateCells returns non-empty Seq[Long] for a GB bbox") {
        val g: GridSystem = BNG
        // 2 km x 2 km bbox around central London in EPSG:27700
        val bbox = JTS.polygonFromXYs(
          Array(
            (529000.0, 179000.0),
            (531000.0, 179000.0),
            (531000.0, 181000.0),
            (529000.0, 181000.0),
            (529000.0, 179000.0)
          )
        )
        bbox.setSRID(27700)
        val candidates = g.coveringCandidateCells(bbox, BNG.getResolution("1km"))
        candidates should not be empty
        // All returned ids are Long (not strings)
        val centerCell = BNG.pointToCellID(530000.0, 180000.0, BNG.getResolution("1km"))
        candidates should contain(centerCell)
    }
}
