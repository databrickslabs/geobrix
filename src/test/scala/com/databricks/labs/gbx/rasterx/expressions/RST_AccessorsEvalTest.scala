package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.kernels.TestL1
import com.databricks.labs.gbx.rasterx.functions
import com.databricks.labs.gbx.udfs
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession
import org.scalatest.matchers.should.Matchers._

class RST_AccessorsEvalTest extends PlanTest with SilentSparkSession {

    test("RST_AccessorsEvalTest should evaluate expressions on raster columns") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        TestL1.installAll(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        def runQuery(df: DataFrame): Unit = {
            val base = df.cache()
            base.collect()
            Seq(
              rst_boundingbox(col("raster")),
              rst_numbands(col("raster")),
              rst_upperlefty(col("raster")),
              rst_width(col("raster"))
            ).foreach(f => base.select(f).collect())
            base.unpersist()
        }

        val df1: DataFrame = Seq(
          (1, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF"),
          (2, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B02.TIF"),
          (3, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B03.TIF")
        ).toDF("id", "path")
            .withColumn("raster", udfs.rasterFromPath(col("path")))

        noException should be thrownBy runQuery(df1)

        val df2: DataFrame = spark.read
            .format("binaryFile")
            .load(tifPath)
            .withColumn("raster", rst_fromcontent(col("content"), lit("GTiff")))

        noException should be thrownBy runQuery(df2)

    }

    test("RST_Width returns NULL (not 0) on a corrupt raster") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        TestL1.installAll(spark)
        // Wrap garbage bytes as a tile struct via rst_fromcontent; GDAL open fails -> safeEval swallows to null.
        val df = Seq(Array[Byte](1, 2, 3, 4)).toDF("content")
            .withColumn("raster", rst_fromcontent(col("content"), lit("GTiff")))
        val res = df.select(rst_width(col("raster")).as("w")).collect()
        assert(res.head.get(0) == null, "corrupt raster width must be NULL, not the 0 sentinel")
    }

}
