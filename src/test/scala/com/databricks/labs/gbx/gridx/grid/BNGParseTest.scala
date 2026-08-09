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
}
