package com.databricks.labs.gbx.rasterx

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.scalatest.matchers.should.Matchers._

/**
  * Guards that rasterx.functions.register(spark) installs gbx_rst_chm in the
  * session function registry
  */
class RST_ChmRegistrationTest extends PlanTest with SilentSparkSession {

    test("functions.register(spark) installs gbx_rst_chm in the session function registry") {
        functions.register(spark)
        spark.sessionState.functionRegistry
            .lookupFunction(FunctionIdentifier("gbx_rst_chm")) should not be empty
    }

}
