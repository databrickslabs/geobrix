package com.databricks.labs.gbx.gridx.grid

import org.scalatest.funsuite.AnyFunSuite

class BNGParseTest extends AnyFunSuite {

    test("parseOrNull returns a cell id for a valid BNG string") {
        // TL is a valid 100km grid-square prefix (London area).
        assert(BNG.parseOrNull("TL") != null)
    }

    test("parseOrNull returns null for an unrecognised prefix (no throw)") {
        assert(BNG.parseOrNull("!!") == null)
    }

    test("parseOrNull returns null for a non-digit body (no NumberFormatException)") {
        // Valid prefix, garbage digits — parse() would throw NumberFormatException here.
        assert(BNG.parseOrNull("TLxy") == null)
    }

    test("parse still throws on a bad prefix (raising variant preserved)") {
        assertThrows[IllegalArgumentException](BNG.parse("!!"))
    }

    // The six 500 km squares (one letter), by the south-west corner of each.
    private val SQUARES500 = Seq("S" -> ((0, 0)), "T" -> ((500000, 0)), "N" -> ((0, 500000)),
        "O" -> ((500000, 500000)), "H" -> ((0, 1000000)), "J" -> ((500000, 1000000)))

    test("a one-letter name is its own 500 km square, and formats back to the name") {
        SQUARES500.foreach { case (name, (x, y)) =>
            val id = BNG.parse(name)
            assert(BNG.format(id) == name, name)
            val env = BNG.cellIdToGeometry(id).getEnvelopeInternal
            assert((env.getMinX, env.getMinY, env.getMaxX, env.getMaxY) ==
                (x.toDouble, y.toDouble, x + 500000.0, y + 500000.0), name)
            assert(BNG.area(id) == 250000.0, name)
        }
    }

    test("a point anywhere in a 500 km square is that square's cell") {
        SQUARES500.foreach { case (name, (x, y)) =>
            Seq((x, y), (x + 250000, y + 250000), (x + 499999, y + 499999)).foreach { case (e, n) =>
                assert(BNG.format(BNG.pointToCellID(e.toDouble, n.toDouble, -1)) == name, s"$name at $e $n")
                assert(BNG.pointToCellID(e.toDouble, n.toDouble, -1) == BNG.parse(name), name)
            }
        }
    }

    test("a 500 km cell rings around its own square") {
        val n = BNG.parse("N")
        assert(BNG.kRing(n, 0) == Seq(n))
        val loop = BNG.kLoop(n, 1).map(BNG.format).toSet
        assert(Set("S", "H", "O", "T", "J").subsetOf(loop))
        assert(!loop.contains("N"))
    }
}
