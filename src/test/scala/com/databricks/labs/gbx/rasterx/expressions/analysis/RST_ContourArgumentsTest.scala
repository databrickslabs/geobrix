package com.databricks.labs.gbx.rasterx.expressions.analysis

import com.databricks.labs.gbx.kernels.TestL1
import com.databricks.labs.gbx.rasterx.functions._
import com.databricks.labs.gbx.udfs
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession
import org.scalatest.matchers.should.Matchers._

/** `gbx_rst_contour` on the JAR level raises for a call that cannot make a contour (no levels and an interval that is
  * not above 0 and finite, or a blank value field), as the lightweight level does. Those are mistakes in the call, not
  * bad rows of data, so they are never the NULL an unreadable tile gives. */
class RST_ContourArgumentsTest extends PlanTest with SilentSparkSession {

    private lazy val modisPath = this.getClass.getResource("/modis/").toString
    private val b01 = "MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF"

    private def validTile(): DataFrame = {
        val sc = spark
        import sc.implicits._
        TestL1.installAll(spark)
        Seq((1, s"$modisPath/$b01")).toDF("id", "path").withColumn("raster", udfs.rasterFromPath(col("path")))
    }

    private def raised(c: Column): String = {
        val ex = intercept[Throwable](validTile().select(c.as("c")).collect())
        LazyList.iterate(Option(ex))(_.flatMap(t => Option(t.getCause))).takeWhile(_.isDefined).flatMap(_.toSeq)
            .map(t => String.valueOf(t.getMessage)).mkString(" | ")
    }

    private val noLevels = array().cast("array<double>")

    test("no levels and an interval of 0, a negative or an infinite one raises, naming the value") {
        Seq(0.0, -1.0, Double.PositiveInfinity).foreach { i =>
            raised(rst_contour(col("raster"), noLevels, lit(i))) should include("interval must be > 0 and finite")
        }
    }

    test("a blank value field raises") {
        raised(rst_contour(col("raster"), array(lit(1.0)), lit(0.0), lit(0.0), lit(" "))) should include("attr_field must be non-empty")
    }

    test("levels with no interval still contour, and a NULL tile is still NULL") {
        val ok = validTile().select(rst_contour(col("raster"), array(lit(100.0)), lit(0.0)).as("c")).collect()
        ok.length shouldBe 1
        val tileType = validTile().schema("raster").dataType
        val nullTile = validTile().select(rst_contour(lit(null).cast(tileType), noLevels, lit(0.0)).as("c")).collect()
        nullTile.head.isNullAt(0) shouldBe true
    }
}
