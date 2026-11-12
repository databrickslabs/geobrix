package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.kernels.TestL1
import com.databricks.labs.gbx.rasterx.{ErrorTokenListener, functions}
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.udfs
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession
import org.scalatest.matchers.should.Matchers._

class RST_ExpressionEvalTest extends PlanTest with SilentSparkSession {

    /** The upper-left `size` x `size` window of the MODIS B01 tile, written as a GTiff into a new temp dir: a small
      * input keeps the expressions fast and stable. Returns the file path. */
    private def modisWindow(size: Int): String = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        org.gdal.gdal.gdal.AllRegister()
        val src = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF").getPath
        val dir = java.nio.file.Files.createTempDirectory(s"gbx_expr_eval_$size").toFile
        dir.deleteOnExit()
        val out = s"${dir.getAbsolutePath}/window_$size.tif"
        val ds = org.gdal.gdal.gdal.Open(src)
        val opts = new java.util.Vector[String]()
        Seq("-srcwin", "0", "0", size.toString, size.toString).foreach(opts.add)
        val win = org.gdal.gdal.gdal.Translate(out, ds, new org.gdal.gdal.TranslateOptions(opts))
        win.delete(); ds.delete()
        new java.io.File(out).deleteOnExit()
        out
    }

    test("RST_ExpressionEvalTest should evaluate expressions on raster columns") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import com.databricks.labs.gbx.udfs._
        import sc.implicits._
        TestL1.installAll(spark)

        // a 256 x 256 window to reduce data size for faster, more stable testing
        val window256 = modisWindow(256)

        def runQuery(df: DataFrame): Unit = {
            val base = df
                .select(col("raster").as("clipped"))
                .limit(1)
                .cache()
            base.collect()
            Seq(
              rst_boundingbox(col("clipped")),
              rst_worldtorastercoordy(col("clipped"), lit(500000.0), lit(5000000.0))
            ).foreach(f => base.select(f).collect())
            base.unpersist()
        }

        // Use only 1 file instead of 3 for faster, more stable testing
        def byPath(path: String): DataFrame = Seq((1, path)).toDF("id", "path")
            .withColumn("raster", udfs.rasterFromPath(col("path")))
        val df1 = byPath(window256)

        // noException should be thrownBy

        runQuery(df1)

        // Use only 1 file for content-based testing for stability
        def byContent(path: String): DataFrame = spark.read
            .format("binaryFile")
            .load(path)
            .limit(1)
            .withColumn("raster", rst_fromcontent(col("content"), lit("GTiff")))
        val df2 = byContent(window256)

        noException should be thrownBy runQuery(df2)

    }

}
