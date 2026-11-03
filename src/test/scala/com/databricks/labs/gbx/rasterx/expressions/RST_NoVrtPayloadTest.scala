package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.kernels.TestL1
import com.databricks.labs.gbx.rasterx.functions
import com.databricks.labs.gbx.udfs
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions.{array, call_function, col, lit}
import org.apache.spark.sql.test.SilentSparkSession
import org.scalatest.matchers.should.Matchers._

/**
  * Self-contained-payload regression: tile.raster bytes must be reachable from any
  * executor — VRT XML referencing a /vsimem/ tempfile on the producing executor is
  * not. Three RasterX operations (MergeRasters, MergeBands, PixelCombineRasters)
  * historically left their result metadata claiming driver=VRT (the input dataset's
  * driver) even though their on-disk output was GTiff; that mis-tagging propagated
  * through RasterDriver.writeToBytes and caused the on-wire payload to be VRT XML.
  *
  * This test asserts the post-fix invariant: for every function that flows through
  * those three operations, the serialized tile.raster bytes start with the GTiff
  * magic and never with `<VRTDataset`, and tile.metadata("driver") reports "GTiff".
  *
  * Functions exercised (one per VRT-using internal operation):
  *   - rst_merge_agg  → MergeRasters
  *   - rst_frombands  → MergeBands
  *   - rst_combineavg → PixelCombineRasters (CombineAVG → PixelCombineRasters)
  *
  * Adding a new function that touches any of those three operations? Add it here.
  */
class RST_NoVrtPayloadTest extends PlanTest with SilentSparkSession {

    /** Both endians of TIFF magic; we ship as standard little-endian GTiff but accept either. */
    private val gtiffMagicLE: Array[Byte] = Array(0x49.toByte, 0x49.toByte, 0x2a.toByte, 0x00.toByte) // "II*\0"
    private val gtiffMagicBE: Array[Byte] = Array(0x4d.toByte, 0x4d.toByte, 0x00.toByte, 0x2a.toByte) // "MM\0*"
    private val vrtMagic: String = "<VRTDataset"

    private def startsWith(bytes: Array[Byte], prefix: Array[Byte]): Boolean =
        bytes != null && bytes.length >= prefix.length && bytes.zip(prefix).forall { case (a, b) => a == b }

    private def assertSelfContained(bytes: Array[Byte], driver: String, label: String): Unit = {
        bytes should not be null
        bytes.length should be > 0
        val head = new String(bytes.take(vrtMagic.length), "US-ASCII")
        withClue(s"$label: tile.raster starts with VRT XML — should be a materialized self-contained raster: ") {
            head should not equal vrtMagic
        }
        withClue(s"$label: tile.raster does not start with GTiff magic bytes (got first 4 bytes as hex: ${bytes.take(4).map("%02x".format(_)).mkString})") {
            (startsWith(bytes, gtiffMagicLE) || startsWith(bytes, gtiffMagicBE)) shouldBe true
        }
        withClue(s"$label: tile.metadata(\"driver\") should be GTiff, not VRT: ") {
            driver should not equal "VRT"
        }
    }

    test("rst_merge_agg returns self-contained GTiff bytes (no VRT payload)") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        TestL1.installAll(spark)

        val tifPath = this.getClass.getResource("/modis/").toString
        val df = Seq(
          s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF",
          s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B02.TIF",
          s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B03.TIF"
        ).toDF("path")
            .withColumn("tile", udfs.rasterFromPath(col("path")))
            .groupBy(lit(1).alias("g"))
            .agg(call_function("gbx_rst_merge_agg", col("tile")).alias("merged"))
            .select(
              col("merged.raster").alias("raster"),
              col("merged.metadata").alias("metadata")
            )

        val row = df.collect().head
        val bytes = row.getAs[Array[Byte]]("raster")
        val driver = row.getAs[Map[String, String]]("metadata").getOrElse("driver", "")
        assertSelfContained(bytes, driver, "rst_merge_agg")
    }

    test("rst_frombands returns self-contained GTiff bytes (no VRT payload)") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        TestL1.installAll(spark)

        val tifPath = this.getClass.getResource("/modis/").toString
        val df = Seq(1).toDF("id")
            .withColumn("b1", udfs.rasterFromPath(lit(s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")))
            .withColumn("b2", udfs.rasterFromPath(lit(s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B02.TIF")))
            .withColumn("b3", udfs.rasterFromPath(lit(s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B03.TIF")))
            .withColumn("stacked", rst_frombands(array(col("b1"), col("b2"), col("b3"))))
            .select(
              col("stacked.raster").alias("raster"),
              col("stacked.metadata").alias("metadata")
            )

        val row = df.collect().head
        val bytes = row.getAs[Array[Byte]]("raster")
        val driver = row.getAs[Map[String, String]]("metadata").getOrElse("driver", "")
        assertSelfContained(bytes, driver, "rst_frombands")
    }

    test("rst_combineavg_agg returns self-contained GTiff bytes (no VRT payload)") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        TestL1.installAll(spark)

        val tifPath = this.getClass.getResource("/modis/").toString
        // No rst_clip warmup needed: PixelCombineRasters.combine now
        // pre-creates NodeFilePathUtil.rootPath itself, so combineavg_agg
        // is safe as the first op in a fresh JVM.
        val df = Seq(
          s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF",
          s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B02.TIF",
          s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B03.TIF"
        ).toDF("path")
            .withColumn("tile", udfs.rasterFromPath(col("path")))
            .groupBy(lit(1).alias("g"))
            .agg(rst_combineavg_agg(col("tile")).alias("avg"))
            .select(
              col("avg.raster").alias("raster"),
              col("avg.metadata").alias("metadata")
            )

        val row = df.collect().head
        val bytes = row.getAs[Array[Byte]]("raster")
        val driver = row.getAs[Map[String, String]]("metadata").getOrElse("driver", "")
        assertSelfContained(bytes, driver, "rst_combineavg_agg")
    }

}
