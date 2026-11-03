package com.databricks.labs.gbx.rasterx.parity

import com.databricks.labs.gbx.kernels.L1Installer
import com.databricks.labs.gbx.rasterx.expressions.agg.{
  RST_BNG_RasterizeAgg, RST_H3_RasterizeAgg, RST_Quadbin_RasterizeAgg
}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.apache.spark.sql.types.BinaryType
import org.scalatest.matchers.should.Matchers._

import scala.util.Try

/**
  * Guards the public SQL surface for the grid rasterize aggregates.
  *
  * Enumerates the 3 rasterize_agg gbx_* names and asserts that each installs in the session's
  * FunctionRegistry via L1Installer — the primitive the Python manager uses. Also verifies
  * that every builder accepts its documented arity (rasterize_agg: 12 args).
  *
  * Tessellate generators are tracked separately and are not part of this check.
  */
class RegistrationInvarianceTest extends PlanTest with SilentSparkSession {

  // All 3 rasterize_agg companions.
  private val rasterizeAggCompanions = Seq(
    RST_H3_RasterizeAgg, RST_Quadbin_RasterizeAgg, RST_BNG_RasterizeAgg
  )

  test("all 3 grid rasterize_agg gbx_* names register unchanged") {
    val expected = Seq(
      "gbx_rst_h3_rasterize_agg",
      "gbx_rst_quadbin_rasterize_agg",
      "gbx_rst_bng_rasterize_agg"
    )

    rasterizeAggCompanions.foreach { c =>
      L1Installer.install(spark, c.name, c.getClass.getName.stripSuffix("$")) shouldBe "ok"
    }

    expected.foreach { name =>
      withClue(s"$name must resolve in registry") {
        spark.sessionState.functionRegistry.lookupFunction(FunctionIdentifier(name)) should not be empty
      }
    }
  }

  test("rasterize_agg builders accept 12 args") {
    val args12 = Seq.fill(12)(Literal.create(null, BinaryType))
    rasterizeAggCompanions.foreach { c =>
      val attempt = Try(c.builder()(args12))
      withClue(s"${c.name} builder should accept 12 args") {
        attempt.isSuccess shouldBe true
      }
    }
  }

  test("rasterize_agg builders reject wrong arity") {
    val args1 = Seq(Literal.create(null, BinaryType))
    rasterizeAggCompanions.foreach { c =>
      val attempt = Try(c.builder()(args1))
      withClue(s"${c.name} builder should reject 1 arg") {
        attempt.isFailure shouldBe true
        attempt.failed.get shouldBe an[IllegalArgumentException]
      }
    }
  }
}
