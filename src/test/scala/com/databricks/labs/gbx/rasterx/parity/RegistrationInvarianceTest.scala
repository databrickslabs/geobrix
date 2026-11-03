package com.databricks.labs.gbx.rasterx.parity

import com.databricks.labs.gbx.kernels.L1Installer
import com.databricks.labs.gbx.rasterx.expressions.agg.{
  RST_BNG_RasterizeAgg, RST_H3_RasterizeAgg, RST_Quadbin_RasterizeAgg
}
import com.databricks.labs.gbx.rasterx.expressions.grid._
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.apache.spark.sql.types.{BinaryType, IntegerType}
import org.scalatest.matchers.should.Matchers._

import scala.util.Try

/**
  * Guards the public SQL surface for grid-raster functions after Stage 1 refactoring.
  *
  * Enumerates the 27 grid-raster gbx_* names (24 rastertogrid + 3 rasterize_agg)
  * and asserts that each installs in the session's FunctionRegistry via L1Installer —
  * the primitive the Python manager uses. Also verifies
  * that every builder accepts its documented arity:
  *   rastertogrid: 2 args (tile, resolution)
  *   rasterize_agg: 12 args
  *
  * Tessellate generators are tracked separately and are not part of this check.
  */
class RegistrationInvarianceTest extends PlanTest with SilentSparkSession {

  // All 24 rastertogrid companions (the companions TestL1 installs).
  private val rastertogridCompanions = Seq(
    RST_H3_RasterToGridAvg,       RST_H3_RasterToGridCount,
    RST_H3_RasterToGridMax,       RST_H3_RasterToGridMin,
    RST_H3_RasterToGridMedian,    RST_H3_RasterToGridSum,
    RST_H3_RasterToGridVariance,  RST_H3_RasterToGridStddev,
    RST_Quadbin_RasterToGridAvg,  RST_Quadbin_RasterToGridCount,
    RST_Quadbin_RasterToGridMax,  RST_Quadbin_RasterToGridMin,
    RST_Quadbin_RasterToGridMedian, RST_Quadbin_RasterToGridSum,
    RST_Quadbin_RasterToGridVariance, RST_Quadbin_RasterToGridStddev,
    RST_BNG_RasterToGridAvg,      RST_BNG_RasterToGridCount,
    RST_BNG_RasterToGridMax,      RST_BNG_RasterToGridMin,
    RST_BNG_RasterToGridMedian,   RST_BNG_RasterToGridSum,
    RST_BNG_RasterToGridVariance, RST_BNG_RasterToGridStddev
  )

  // All 3 rasterize_agg companions.
  private val rasterizeAggCompanions = Seq(
    RST_H3_RasterizeAgg, RST_Quadbin_RasterizeAgg, RST_BNG_RasterizeAgg
  )

  test("all 27 grid-raster gbx_* names register unchanged") {
    val expected = Seq(
      "gbx_rst_h3_rastertogridavg",       "gbx_rst_h3_rastertogridcount",
      "gbx_rst_h3_rastertogridmax",       "gbx_rst_h3_rastertogridmin",
      "gbx_rst_h3_rastertogridmedian",    "gbx_rst_h3_rastertogridsum",
      "gbx_rst_h3_rastertogridvariance",  "gbx_rst_h3_rastertogridstddev",
      "gbx_rst_quadbin_rastertogridavg",  "gbx_rst_quadbin_rastertogridcount",
      "gbx_rst_quadbin_rastertogridmax",  "gbx_rst_quadbin_rastertogridmin",
      "gbx_rst_quadbin_rastertogridmedian", "gbx_rst_quadbin_rastertogridsum",
      "gbx_rst_quadbin_rastertogridvariance", "gbx_rst_quadbin_rastertogridstddev",
      "gbx_rst_bng_rastertogridavg",      "gbx_rst_bng_rastertogridcount",
      "gbx_rst_bng_rastertogridmax",      "gbx_rst_bng_rastertogridmin",
      "gbx_rst_bng_rastertogridmedian",   "gbx_rst_bng_rastertogridsum",
      "gbx_rst_bng_rastertogridvariance", "gbx_rst_bng_rastertogridstddev",
      "gbx_rst_h3_rasterize_agg",
      "gbx_rst_quadbin_rasterize_agg",
      "gbx_rst_bng_rasterize_agg"
    )
    expected.size shouldBe 27 // 24 rastertogrid + 3 rasterize_agg; tessellate generators counted separately

    (rastertogridCompanions ++ rasterizeAggCompanions).foreach { c =>
      L1Installer.install(spark, c.name, c.getClass.getName.stripSuffix("$")) shouldBe "ok"
    }

    expected.foreach { name =>
      withClue(s"$name must resolve in registry") {
        spark.sessionState.functionRegistry.lookupFunction(FunctionIdentifier(name)) should not be empty
      }
    }
  }

  test("each rastertogrid companion name matches expected gbx_rst prefix") {
    val names = rastertogridCompanions.map(_.name)
    names.size shouldBe 24
    names.foreach { n =>
      withClue(s"$n should carry gbx_rst_ prefix") {
        n should startWith("gbx_rst_")
      }
      withClue(s"$n should carry _rastertogrid suffix") {
        n should include("rastertogrid")
      }
    }
  }

  test("rastertogrid builders accept 2 args (tile, resolution)") {
    val tile = Literal.create(null, BinaryType)
    val res  = Literal.create(2, IntegerType)
    rastertogridCompanions.foreach { c =>
      val attempt = Try(c.builder()(Seq(tile, res)))
      withClue(s"${c.name} builder should accept 2 args") {
        attempt.isSuccess shouldBe true
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
