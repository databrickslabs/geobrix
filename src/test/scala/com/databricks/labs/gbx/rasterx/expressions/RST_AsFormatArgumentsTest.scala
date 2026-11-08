package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.kernels.TestL1
import com.databricks.labs.gbx.rasterx.functions._
import com.databricks.labs.gbx.udfs
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession
import org.scalatest.matchers.should.Matchers._

/** `gbx_rst_asformat` on the JAR level refuses a format whose bytes GeoBrix does not open as a tile (VRT and the rest
  * of the driver policy's bytes-refused class: formats that can name other files), as the lightweight level does,
  * instead of returning a tile that cannot be opened again. */
class RST_AsFormatArgumentsTest extends PlanTest with SilentSparkSession {

    private lazy val modisPath = this.getClass.getResource("/modis/").toString
    private val b01 = "MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF"

    private def validTile(): DataFrame = {
        val sc = spark
        import sc.implicits._
        TestL1.installAll(spark)
        Seq((1, s"$modisPath/$b01")).toDF("id", "path").withColumn("raster", udfs.rasterFromPath(col("path")))
    }

    private def raised(df: DataFrame): String = {
        val ex = intercept[Throwable](df.collect())
        LazyList.iterate(Option(ex))(_.flatMap(t => Option(t.getCause))).takeWhile(_.isDefined).flatMap(_.toSeq)
            .map(t => String.valueOf(t.getMessage)).mkString(" | ")
    }

    test("a VRT target is refused, any case, naming the format") {
        Seq("VRT", "vrt").foreach { f =>
            val msg = raised(validTile().select(rst_asformat(col("raster"), f).as("t")))
            msg should include("does not open as a tile")
            msg should include(f)
        }
    }

    test("a GeoTIFF target still converts") {
        val rows = validTile().select(rst_asformat(col("raster"), "GTiff").as("t")).select(rst_width(col("t")).as("w")).collect()
        rows.head.isNullAt(0) shouldBe false
    }
}
