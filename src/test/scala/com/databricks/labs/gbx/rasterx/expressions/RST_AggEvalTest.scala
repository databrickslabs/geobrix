package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.kernels.TestL1
import com.databricks.labs.gbx.udfs
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession
import org.scalatest.matchers.should.Matchers._

/**
  * The array-of-tiles JVM form (`gbx_rst_frombands`) given a single tile column instead of an array, as when an
  * aggregate across rows was meant. The tile aggregates have no JVM form: they run on the Python level.
  */
class RST_AggEvalTest extends PlanTest with SilentSparkSession {

    test("rst_frombands on a single tile column raises a friendly error naming the expected ARRAY<tile>") {
        // Regression for the notebook error `.selectExpr("gbx_rst_merge(tile) AS tile")`, now on the array
        // form that keeps a JVM level. The non-agg form expects ARRAY<tile>; passing a single tile struct
        // previously produced a raw ClassCastException from inside Catalyst analysis. With
        // RST_ExpressionUtil.arrayOfTileRasterType in place we see an IllegalArgumentException with a
        // message that names the function and the actual type received.
        val sc = spark
        import sc.implicits._
        TestL1.installAll(spark)

        val tifPath = this.getClass.getResource("/modis/").toString
        val df = Seq(
          s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF"
        ).toDF("path").withColumn("tile", udfs.rasterFromPath(col("path")))

        val thrown = intercept[Throwable] {
            df.selectExpr("gbx_rst_frombands(tile) AS tile").collect()
        }
        // Spark wraps analysis-time IllegalArgumentException so the actual
        // class can be either IllegalArgumentException or one of Spark's
        // catalyst-wrapper types — what matters is that our diagnostic
        // message survives in the chain.
        val joined = LazyList
            .iterate(Option(thrown))(_.flatMap(t => Option(t.getCause)))
            .takeWhile(_.isDefined)
            .flatMap(_.map(_.getMessage).filter(_ != null))
            .mkString(" || ")
        joined should include ("gbx_rst_frombands expects ARRAY<tile>")
        joined should include ("collect_list(tile)")
    }

    test("rst_frombands names the SQL function in its skipped-member note") {
        // A member whose bytes do not open is dropped and noted in last_error; the note is worded as the
        // Python and UC levels word it, so the text matches whichever level ran.
        val sc = spark
        import sc.implicits._
        TestL1.installAll(spark)

        val tifPath = this.getClass.getResource("/modis/").toString
        val df = Seq(
          s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF"
        ).toDF("path").withColumn("tile", udfs.rasterFromPath(col("path")))
            .withColumn("bad", col("tile").withField("raster", lit(Array[Byte](1, 2, 3))))

        val out = df.selectExpr("gbx_rst_frombands(array(tile, bad)) AS tile")
            .selectExpr("tile.raster IS NOT NULL AS has_raster", "tile.metadata['last_error'] AS note")
            .collect()
        out.head.getBoolean(0) shouldBe true
        out.head.getString(1) shouldBe "gbx_rst_frombands: skipped 1 corrupt input tile(s)"
    }

}
