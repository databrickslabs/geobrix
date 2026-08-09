package com.databricks.labs.gbx.gridx.quadbin

import com.databricks.labs.gbx.gridx.grid.Quadbin
import org.scalatest.funsuite.AnyFunSuite

class Quadbin_ClampTest extends AnyFunSuite {

    test("out-of-range latitude is clamped, not NULL'd (documented behavior)") {
        // lat 89 is beyond the web-mercator limit; it must clamp to the +85.05 tile,
        // producing the SAME cell as lat 85.05112878, not an error or a different cell.
        val clamped = Quadbin.lonLatToTile(10.0, 89.0, 10)
        val atLimit = Quadbin.lonLatToTile(10.0, 85.05112878, 10)
        assert(clamped == atLimit)
    }

    test("resolution out of range still RAISES (parameter)") {
        assertThrows[IllegalArgumentException](Quadbin.pointToCell(10.0, 50.0, 99))
    }
}
