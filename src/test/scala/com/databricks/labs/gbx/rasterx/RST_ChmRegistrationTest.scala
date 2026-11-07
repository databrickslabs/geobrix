package com.databricks.labs.gbx.rasterx

import com.databricks.labs.gbx.kernels.L1Installer
import com.databricks.labs.gbx.kernels.TestL1
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.scalatest.matchers.should.Matchers._

/**
  * Guards that TestL1.installAll(spark) installs gbx_rst_chm in the
  * session function registry
  */
class RST_ChmRegistrationTest extends PlanTest with SilentSparkSession {

    test("TestL1.installAll(spark) installs gbx_rst_chm in the session function registry") {
        TestL1.installAll(spark)
        spark.sessionState.functionRegistry
            .lookupFunction(L1Installer.sessionFunctionId("gbx_rst_chm")) should not be empty
    }

}
