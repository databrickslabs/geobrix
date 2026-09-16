package com.databricks.labs.gbx.rasterx

import com.databricks.labs.gbx.rasterx.expressions.RST_Chm
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SilentSparkSession
import org.scalatest.matchers.should.Matchers._

/**
  * Guards that rasterx.functions.register(spark) installs gbx_rst_chm in the
  * session function registry, and that the rst_chm Column wrapper delegates to
  * the correct SQL name.
  */
class RST_ChmRegistrationTest extends PlanTest with SilentSparkSession {

    test("functions.register(spark) installs gbx_rst_chm in the session function registry") {
        functions.register(spark)
        spark.sessionState.functionRegistry
            .lookupFunction(FunctionIdentifier("gbx_rst_chm")) should not be empty
    }

    test("rst_chm Column wrapper delegates to RST_Chm.name") {
        val result = functions.rst_chm(col("dsm"), col("dem"))
        result should not be null
        result.toString should include(RST_Chm.name)
    }

}
