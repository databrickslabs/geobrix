package com.databricks.labs.gbx.rasterx

import com.databricks.labs.gbx.rasterx.expressions.vector.RST_Isoband
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SilentSparkSession
import org.scalatest.matchers.should.Matchers._

/**
  * Guards that rasterx.functions.register(spark) installs gbx_rst_isoband in the
  * session function registry, and that the rst_isoband Column wrapper delegates to
  * the correct SQL name.
  */
class RST_IsobandRegistrationTest extends PlanTest with SilentSparkSession {

    test("functions.register(spark) installs gbx_rst_isoband in the session function registry") {
        functions.register(spark)
        spark.sessionState.functionRegistry
            .lookupFunction(FunctionIdentifier("gbx_rst_isoband")) should not be empty
    }

    test("rst_isoband Column wrapper delegates to RST_Isoband.name") {
        val result = functions.rst_isoband(col("tile"), col("breaks"))
        result should not be null
        result.toString should include(RST_Isoband.name)
    }

}
