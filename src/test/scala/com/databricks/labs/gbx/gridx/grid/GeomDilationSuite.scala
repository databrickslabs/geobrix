// src/test/scala/com/databricks/labs/gbx/gridx/grid/GeomDilationSuite.scala
package com.databricks.labs.gbx.gridx.grid

import org.scalatest.funsuite.AnyFunSuite
import org.locationtech.jts.io.WKTReader

class GeomDilationSuite extends AnyFunSuite {
  private val wkt = new WKTReader()
  // Quadbin is a concrete GridSystem with kLoop/polyfill/cellIdToGeometry.
  private val grid = Quadbin
  private val res = 12

  test("boundary-out (coveras): k0 is the full straddling band; interior EXCLUDED") {
    // LOCKED: coveras boundary-out k0 = the full coveras straddling band (sCover - sCore).
    // The geom INTERIOR (fully-contained sCore) is never returned; k>=1 adds the outward band.
    val g = wkt.read("POLYGON((-1 -1, -1 1, 1 1, 1 -1, -1 -1))")
    val cls = GeomDilation.classify(grid, g, res)
    val fullBand = cls.sCover -- cls.sCore
    val k0Out = GeomDilation.expand("loop", 0, "boundary-out", grid, g, res)
    assert(k0Out == fullBand && k0Out.nonEmpty, "boundary-out k0 = full coveras straddling band")
    val ring = GeomDilation.expand("ring", 1, "boundary-out", grid, g, res)
    assert(ring.nonEmpty, "boundary-out k=1 must be non-empty (full band + outward band)")
    assert(ring.intersect(cls.sCore).isEmpty, "boundary-out must EXCLUDE the interior (sCore)")
  }

  test("hole modes empty when no holes") {
    val g = wkt.read("POLYGON((-1 -1, -1 1, 1 1, 1 -1, -1 -1))")
    assert(GeomDilation.expand("ring", 3, "hole-in", grid, g, res).isEmpty)
  }

  test("loop k equals ring(k) minus ring(k-1)") {
    val g = wkt.read("POLYGON((-2 -2, -2 2, 2 2, 2 -2, -2 -2))")
    val r3 = GeomDilation.expand("ring", 3, "boundary-out", grid, g, res)
    val r2 = GeomDilation.expand("ring", 2, "boundary-out", grid, g, res)
    val l3 = GeomDilation.expand("loop", 3, "boundary-out", grid, g, res)
    assert(l3 == (r3 diff r2))
  }

  test("boundary-in stays inside geom and respects a hole") {
    val g = wkt.read("POLYGON((-3 -3,-3 3,3 3,3 -3,-3 -3),(-1 -1,-1 1,1 1,1 -1,-1 -1))")
    val cls = GeomDilation.classify(grid, g, res)
    // Precondition: fix A must produce a non-empty hCore (polyfill SOLID fills hole interior)
    assert(cls.hCore.nonEmpty, "hCore must be non-empty after classify polyfills the solid")
    val r = GeomDilation.expand("ring", 2, "boundary-in", grid, g, res)
    assert(r.subsetOf(cls.pCore union cls.pBorder))
    assert(r.intersect(cls.hCore).isEmpty)
  }

  test("hole-in fills hole interior") {
    val g = wkt.read("POLYGON((-3 -3,-3 3,3 3,3 -3,-3 -3),(-1 -1,-1 1,1 1,1 -1,-1 -1))")
    val cls = GeomDilation.classify(grid, g, res)
    assert(cls.hCore.nonEmpty, "hCore must be non-empty (precondition for hole-in)")
    val r = GeomDilation.expand("ring", 20, "hole-in", grid, g, res)
    assert(cls.hCore.subsetOf(r), "hole-in ring must include all hCore cells")
    assert(r.intersect(cls.pCore).isEmpty, "hole-in must not enter the solid")
  }

  test("boundary-out is alignment-robust — non-empty for grid-aligned solid") {
    // A polygon whose corners snap exactly to Quadbin cell boundaries has zero straddling
    // cells (pBorder = ∅).  The old pBorder seed produced no expansion; the perimeter fix
    // makes the boundary ring (k0) and the outward band non-empty regardless of alignment.
    val g = wkt.read("POLYGON((-1 -1,-1 1,1 1,1 -1,-1 -1))")
    val cls  = GeomDilation.classify(grid, g, res)
    val k0   = GeomDilation.expand("loop", 0, "boundary-out", grid, g, res)
    val ring = GeomDilation.expand("ring", 1, "boundary-out", grid, g, res)
    assert(k0.nonEmpty,
      "boundary-out k0 (full band / fallback ring) must be non-empty even when pBorder is empty")
    assert(ring.nonEmpty, "boundary-out must expand outward (alignment-robustness)")
    assert(ring.intersect(cls.sCore).isEmpty, "boundary-out excludes the interior (sCore)")
  }

  test("boundary-out hole is excluded — perimeter seed never includes hole-rim cells") {
    // For a holed polygon the outer perimeter of sCover lies on the outer ring only.
    // boundary-out must not expand from the hole rim (which was the Layer-1 / old pBorder bug).
    val g   = wkt.read("POLYGON((-3 -3,-3 3,3 3,3 -3,-3 -3),(-1 -1,-1 1,1 1,1 -1,-1 -1))")
    val cls = GeomDilation.classify(grid, g, res)
    assert(cls.hCore.nonEmpty, "fixture must have a non-empty hole interior (hCore)")
    val ring = GeomDilation.expand("ring", 1, "boundary-out", grid, g, res)
    // hole interior cells must NOT appear in a boundary-out expansion
    assert(ring.intersect(cls.hCore).isEmpty,
      "boundary-out must not fill hole interior cells")
  }

  test("unknown mode throws") {
    val g = wkt.read("POLYGON((0 0,0 1,1 1,1 0,0 0))")
    assertThrows[IllegalArgumentException](GeomDilation.expand("ring", 1, "sideways", grid, g, res))
  }

  test("unknown kind throws") {
    val g = wkt.read("POLYGON((0 0,0 1,1 1,1 0,0 0))")
    assertThrows[IllegalArgumentException](GeomDilation.expand("disk", 1, "boundary-out", grid, g, res))
  }
}
