package com.databricks.labs.gbx.rasterx

import com.databricks.labs.gbx.rasterx.expressions.RST_BinPoints
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SilentSparkSession
import org.scalatest.matchers.should.Matchers._

/**
  * Guards that rasterx.functions.register(spark) installs gbx_rst_binpoints in the
  * session function registry, and that the rst_binpoints Column wrappers delegate to
  * the correct SQL name.
  */
class RST_BinPointsRegistrationTest extends PlanTest with SilentSparkSession {

    test("functions.register(spark) installs gbx_rst_binpoints in the session function registry") {
        functions.register(spark)
        spark.sessionState.functionRegistry
            .lookupFunction(FunctionIdentifier("gbx_rst_binpoints")) should not be empty
    }

    test("rst_binpoints 10-arg Column wrapper delegates to RST_BinPoints.name") {
        val result = functions.rst_binpoints(
            col("x_array"), col("y_array"), col("z_array"),
            col("xmin"), col("ymin"), col("xmax"), col("ymax"),
            col("width_px"), col("height_px"), col("srid")
        )
        result should not be null
        result.toString should include(RST_BinPoints.name)
    }

    test("rst_binpoints 11-arg Column wrapper delegates to RST_BinPoints.name") {
        val result = functions.rst_binpoints(
            col("x_array"), col("y_array"), col("z_array"),
            col("xmin"), col("ymin"), col("xmax"), col("ymax"),
            col("width_px"), col("height_px"), col("srid"),
            col("statistic")
        )
        result should not be null
        result.toString should include(RST_BinPoints.name)
    }

}
