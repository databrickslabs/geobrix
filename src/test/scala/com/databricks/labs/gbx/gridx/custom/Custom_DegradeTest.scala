package com.databricks.labs.gbx.gridx.custom

import com.databricks.labs.gbx.gridx.grid.{CustomGridSystem, GridConf}
import org.scalatest.funsuite.AnyFunSuite

class Custom_DegradeTest extends AnyFunSuite {

    // 0..1_000_000 grid; cellSplits=10 → bitsPerResolution=7, maxResolution=min(20,8)=8.
    // Resolution 0 is valid; resolution 99 exceeds 8 (parameter → raises).
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

}
