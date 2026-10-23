package com.databricks.labs.gbx.gridx.bng

import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class BNG_FunctionsTest extends AnyFunSuite {

    test("scalar-literal overloads should accept plain values for non-Column args") {
        // Int resolution overloads
        functions.bng_eastnorthasbng(col("e"), col("n"), 1) should not be null
        functions.bng_pointascell(col("pt"), 1) should not be null
        functions.bng_polyfill(col("geom"), 1) should not be null
        functions.bng_tessellate(col("geom"), 1) should not be null

        // String resolution overloads
        functions.bng_eastnorthasbng(col("e"), col("n"), "1km") should not be null
        functions.bng_pointascell(col("pt"), "1km") should not be null
        functions.bng_polyfill(col("geom"), "1km") should not be null
        functions.bng_tessellate(col("geom"), "1km") should not be null
        functions.bng_tessellate(col("geom"), 1, keepCoreGeom = true) should not be null
        functions.bng_tessellate(col("geom"), "1km", keepCoreGeom = false) should not be null

        // k-ring / k-loop Int overloads
        functions.bng_kloop(col("cellId"), 2) should not be null
        functions.bng_kring(col("cellId"), 2) should not be null
        functions.bng_geomkloop(col("geom"), 1, 2) should not be null
        functions.bng_geomkring(col("geom"), "1km", 2) should not be null

        // Generators
        functions.bng_geomkloopexplode(col("geom"), 1, 2) should not be null
        functions.bng_geomkringexplode(col("geom"), "1km", 2) should not be null
        functions.bng_kloopexplode(col("cellId"), 2) should not be null
        functions.bng_kringexplode(col("cellId"), 2) should not be null
        functions.bng_tessellateexplode(col("geom"), 1) should not be null
        functions.bng_tessellateexplode(col("geom"), "1km") should not be null
    }

}
