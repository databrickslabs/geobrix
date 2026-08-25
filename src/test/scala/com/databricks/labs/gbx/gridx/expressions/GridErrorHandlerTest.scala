package com.databricks.labs.gbx.gridx.expressions

import org.scalatest.funsuite.AnyFunSuite

class GridErrorHandlerTest extends AnyFunSuite {

    test("safeEval returns the body value when no exception") {
        assert(GridErrorHandler.safeEval("fallback")("ok") == "ok")
    }

    test("safeEval returns nullValue on a NonFatal throw") {
        val r: String = GridErrorHandler.safeEval[String](null)(throw new IllegalArgumentException("bad data"))
        assert(r == null)
    }

    test("safeEval returns boxed null for a numeric shape on throw") {
        val r: java.lang.Long = GridErrorHandler.safeEval[java.lang.Long](null)(throw new NumberFormatException("x"))
        assert(r == null)
    }

    test("safeEval rethrows a fatal error (StackOverflowError)") {
        assertThrows[StackOverflowError] {
            GridErrorHandler.safeEval[String](null)(throw new StackOverflowError())
        }
    }
}
